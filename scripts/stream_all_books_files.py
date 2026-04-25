from __future__ import annotations

import argparse
import asyncio
import json
import lzma
import random
import shutil
import sys
import tarfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


GAMMA_API = "https://gamma-api.polymarket.com"
MARKET_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
HIGH_XZ_PRESET = 9 | lzma.PRESET_EXTREME


@dataclass(slots=True)
class RunStats:
    markets_total: int = 0
    assets_total: int = 0
    chunks_total: int = 0
    markets_written: int = 0
    ws_messages: int = 0
    ws_book_entries: int = 0
    ws_price_change_entries: int = 0
    books_bytes: int = 0
    price_change_bytes: int = 0
    event_messages: int = 0
    new_markets_seen: int = 0
    new_markets_added: int = 0
    resolved_markets_seen: int = 0
    resolved_markets_archived: int = 0


@dataclass(slots=True)
class AssetMeta:
    asset_id: str
    asset_index: int
    outcome: str
    tick_size: str


@dataclass(slots=True)
class MarketMeta:
    market_id: str
    gamma_market_id: str
    question: str
    slug: str
    event_slug: str
    condition_id: str
    outcomes: list[str]
    assets: list[AssetMeta]


@dataclass(slots=True)
class CollectorState:
    data_root: Path
    meta_path: Path
    market_catalog: dict[str, MarketMeta]
    asset_to_market: dict[str, str]
    shard_assets: list[set[str]]
    shard_commands: list[asyncio.Queue]
    resolved_markets: set[str]
    meta_lock: asyncio.Lock


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Listen to all Polymarket books and store them in "
            "all_market_meta.json + <market>/books.jsonl."
        )
    )
    parser.add_argument("--data-root", default="data_all_books_jsonl_live", help="Output root")
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=1800,
        help="Run duration; use 0 or a negative value to keep running",
    )
    parser.add_argument("--page-limit", type=int, default=500, help="Gamma page size")
    parser.add_argument("--chunk-size", type=int, default=500, help="Assets per websocket connection")
    parser.add_argument(
        "--proxy-url",
        default="",
        help="Optional outbound proxy URL, for example http://127.0.0.1:7890 or socks5h://127.0.0.1:7890",
    )
    parser.add_argument("--open-timeout", type=float, default=20.0, help="WebSocket open timeout")
    parser.add_argument("--recv-timeout", type=float, default=30.0, help="WebSocket receive timeout")
    parser.add_argument(
        "--idle-reconnect-seconds",
        type=float,
        default=90.0,
        help="Reconnect a websocket if no usable book messages arrive for this long.",
    )
    parser.add_argument("--queue-maxsize", type=int, default=10000, help="Writer queue size")
    parser.add_argument("--summary-file", default="run_summary.json", help="Summary path under state/")
    parser.add_argument(
        "--event-anchor-assets",
        type=int,
        default=3,
        help="How many placeholder assets the dedicated event websocket subscribes to.",
    )
    return parser.parse_args()


def parse_token_ids(token_ids: Any) -> list[str]:
    if isinstance(token_ids, str):
        try:
            parsed = json.loads(token_ids)
        except json.JSONDecodeError:
            parsed = [token_ids]
    else:
        parsed = token_ids or []
    return [str(item) for item in parsed if item]


def parse_outcomes(outcomes: Any) -> list[str]:
    if isinstance(outcomes, str):
        try:
            parsed = json.loads(outcomes)
        except json.JSONDecodeError:
            parsed = [outcomes]
    elif isinstance(outcomes, list):
        parsed = outcomes
    else:
        parsed = []
    return [str(item) for item in parsed]


def build_market_meta(item: dict[str, Any]) -> MarketMeta:
    asset_ids = parse_token_ids(item.get("clobTokenIds") or item.get("clob_token_ids") or item.get("assets_ids"))
    outcomes = parse_outcomes(item.get("outcomes"))
    tick_size = str(
        item.get("orderPriceMinTickSize")
        or item.get("order_price_min_tick_size")
        or item.get("minimum_tick_size")
        or "0.01"
    )
    assets: list[AssetMeta] = []

    for slot, asset_id in enumerate(asset_ids):
        outcome = outcomes[slot] if slot < len(outcomes) else ""
        assets.append(
            AssetMeta(
                asset_id=asset_id,
                asset_index=slot + 1,
                outcome=outcome,
                tick_size=tick_size,
            )
        )

    condition_id = str(item.get("conditionId") or item.get("condition_id") or "")
    gamma_market_id = str(item.get("id") or "")
    market_id = str(item.get("market") or condition_id or gamma_market_id)

    return MarketMeta(
        market_id=market_id,
        gamma_market_id=gamma_market_id,
        question=str(item.get("question") or ""),
        slug=str(item.get("slug") or ""),
        event_slug=str(item.get("eventSlug") or item.get("event_slug") or ""),
        condition_id=condition_id or market_id,
        outcomes=outcomes,
        assets=assets,
    )


def fetch_markets_and_assets(*, page_limit: int, proxy_url: str = "") -> tuple[dict[str, MarketMeta], list[str]]:
    session = requests.Session()
    session.headers.update({"User-Agent": "polymarket-collector-mvp/0.1"})
    if proxy_url:
        session.proxies.update({"http": proxy_url, "https": proxy_url})
    market_catalog: dict[str, MarketMeta] = {}
    asset_ids: list[str] = []
    offset = 0

    while True:
        response = session.get(
            f"{GAMMA_API}/markets",
            params={
                "limit": page_limit,
                "offset": offset,
                "active": "true",
                "closed": "false",
                "archived": "false",
            },
            timeout=30,
        )
        response.raise_for_status()
        page = response.json()
        if not page:
            break

        for item in page:
            market_meta = build_market_meta(item)
            if not market_meta.market_id or market_meta.market_id in market_catalog:
                continue
            market_catalog[market_meta.market_id] = market_meta
            asset_ids.extend(asset.asset_id for asset in market_meta.assets)

        if len(page) < page_limit:
            break
        offset += page_limit

    return market_catalog, list(dict.fromkeys(asset_ids))


async def fetch_markets_and_assets_with_retry(
    *, page_limit: int, stop_at: float | None, proxy_url: str = ""
) -> tuple[dict[str, MarketMeta], list[str]]:
    failure_streak = 0
    while True:
        try:
            return await asyncio.to_thread(fetch_markets_and_assets, page_limit=page_limit, proxy_url=proxy_url)
        except requests.RequestException:
            failure_streak += 1
            delay = min(60.0, 2.0 * (2 ** min(failure_streak, 5))) + random.uniform(0.0, 1.0)
            if stop_at is not None and asyncio.get_running_loop().time() + delay >= stop_at:
                raise
            await asyncio.sleep(delay)


def chunked(values: list[str], chunk_size: int) -> list[list[str]]:
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


def normalize_books_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict) and item.get("asset_id") and item.get("market")]
    if isinstance(payload, dict) and payload.get("asset_id") and payload.get("market"):
        return [payload]
    return []


def normalize_market_events_payload(payload: Any) -> list[dict[str, Any]]:
    items = normalize_books_payload(payload)
    normalized: list[dict[str, Any]] = []
    for item in items:
        event_type = str(item.get("event_type") or "")
        if not event_type and ("bids" in item or "asks" in item):
            item = dict(item)
            item["event_type"] = "book"
            normalized.append(item)
            continue
        if event_type in {"book", "price_change"}:
            normalized.append(item)
    return normalized


def normalize_payload_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def market_dir(root: Path, market_id: str) -> Path:
    return root / market_id


def compact_levels(levels: Any) -> list[list[str]]:
    if not isinstance(levels, list):
        return []
    compact: list[list[str]] = []
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = level.get("price")
        size = level.get("size")
        if price in (None, "") or size in (None, ""):
            continue
        compact.append([str(price), str(size)])
    return compact


def compact_book_record(*, asset_index: int, book: dict[str, Any]) -> dict[str, Any]:
    record: dict[str, Any] = {
        "i": asset_index,
        "t": str(book.get("timestamp") or ""),
        "b": compact_levels(book.get("bids")),
        "a": compact_levels(book.get("asks")),
    }
    book_hash = book.get("hash")
    if book_hash not in (None, ""):
        record["h"] = str(book_hash)
    return record


def compact_price_change_record(*, asset_index: int, price_change: dict[str, Any]) -> dict[str, Any]:
    record: dict[str, Any] = {
        "i": asset_index,
        "t": str(price_change.get("timestamp") or ""),
    }
    for key, value in price_change.items():
        if key in {"asset_id", "market", "event_type", "timestamp"}:
            continue
        if value in (None, ""):
            continue
        record[key] = value
    return record


def build_asset_to_market(market_catalog: dict[str, MarketMeta]) -> dict[str, str]:
    asset_to_market: dict[str, str] = {}
    for market_id, market_meta in market_catalog.items():
        for asset in market_meta.assets:
            asset_to_market[asset.asset_id] = market_id
    return asset_to_market


def write_all_market_meta(meta_path: Path, market_catalog: dict[str, MarketMeta]) -> None:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "layout_version": 5,
        "storage_format": "per-market append-only books.jsonl + price_changes.jsonl",
        "markets": [
            {
                "market": market_meta.market_id,
                "gamma_market_id": market_meta.gamma_market_id,
                "question": market_meta.question,
                "slug": market_meta.slug,
                "event_slug": market_meta.event_slug,
                "condition_id": market_meta.condition_id,
                "assets": [
                    {
                        "asset_index": asset.asset_index,
                        "asset_id": asset.asset_id,
                        "outcome": asset.outcome,
                        "tick_size": asset.tick_size,
                    }
                    for asset in market_meta.assets
                ],
            }
            for market_meta in market_catalog.values()
        ],
    }
    meta_path.write_text(json.dumps(payload, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")


async def persist_market_catalog(state: CollectorState) -> None:
    async with state.meta_lock:
        snapshot = dict(state.market_catalog)
        await asyncio.to_thread(write_all_market_meta, state.meta_path, snapshot)

def choose_shard_index(shard_assets: list[set[str]]) -> int:
    return min(range(len(shard_assets)), key=lambda idx: len(shard_assets[idx]))


async def add_new_markets(state: CollectorState, market_metas: list[MarketMeta], stats: RunStats) -> int:
    pending_by_shard: dict[int, list[str]] = {}
    added_markets = 0

    for market_meta in market_metas:
        if not market_meta.market_id or market_meta.market_id in state.market_catalog or market_meta.market_id in state.resolved_markets:
            continue
        if not market_meta.assets:
            continue

        state.market_catalog[market_meta.market_id] = market_meta
        added_markets += 1

        for asset in market_meta.assets:
            if asset.asset_id in state.asset_to_market:
                continue
            shard_index = choose_shard_index(state.shard_assets)
            state.shard_assets[shard_index].add(asset.asset_id)
            state.asset_to_market[asset.asset_id] = market_meta.market_id
            pending_by_shard.setdefault(shard_index, []).append(asset.asset_id)

    if not added_markets:
        return 0

    await persist_market_catalog(state)
    for shard_index, asset_ids in pending_by_shard.items():
        await state.shard_commands[shard_index].put({"op": "subscribe", "asset_ids": asset_ids})

    stats.markets_total = len(state.market_catalog)
    stats.assets_total = len(state.asset_to_market)
    stats.new_markets_added += added_markets
    return added_markets


async def upsert_market_meta(
    *,
    state: CollectorState,
    market_meta: MarketMeta,
    stats: RunStats,
) -> bool:
    if not market_meta.market_id or market_meta.market_id in state.resolved_markets:
        return False

    pending_by_shard: dict[int, list[str]] = {}
    changed = False
    existing = state.market_catalog.get(market_meta.market_id)

    if existing is None:
        if not market_meta.assets:
            return False
        state.market_catalog[market_meta.market_id] = market_meta
        changed = True
        for asset in market_meta.assets:
            if asset.asset_id in state.asset_to_market:
                continue
            shard_index = choose_shard_index(state.shard_assets)
            state.shard_assets[shard_index].add(asset.asset_id)
            state.asset_to_market[asset.asset_id] = market_meta.market_id
            pending_by_shard.setdefault(shard_index, []).append(asset.asset_id)
        stats.new_markets_added += 1
    else:
        existing_assets = {asset.asset_id: asset for asset in existing.assets}
        merged_assets = list(existing.assets)
        next_index = max((asset.asset_index for asset in merged_assets), default=0) + 1

        for asset in market_meta.assets:
            current = existing_assets.get(asset.asset_id)
            if current is not None:
                current.outcome = asset.outcome or current.outcome
                current.tick_size = asset.tick_size or current.tick_size
            else:
                new_asset = AssetMeta(
                    asset_id=asset.asset_id,
                    asset_index=next_index,
                    outcome=asset.outcome,
                    tick_size=asset.tick_size,
                )
                merged_assets.append(new_asset)
                existing_assets[new_asset.asset_id] = new_asset
                next_index += 1
                changed = True
                shard_index = choose_shard_index(state.shard_assets)
                state.shard_assets[shard_index].add(new_asset.asset_id)
                state.asset_to_market[new_asset.asset_id] = market_meta.market_id
                pending_by_shard.setdefault(shard_index, []).append(new_asset.asset_id)

        updated = MarketMeta(
            market_id=existing.market_id,
            gamma_market_id=market_meta.gamma_market_id or existing.gamma_market_id,
            question=market_meta.question or existing.question,
            slug=market_meta.slug or existing.slug,
            event_slug=market_meta.event_slug or existing.event_slug,
            condition_id=market_meta.condition_id or existing.condition_id,
            outcomes=market_meta.outcomes or existing.outcomes,
            assets=merged_assets,
        )
        if (
            updated.gamma_market_id != existing.gamma_market_id
            or updated.question != existing.question
            or updated.slug != existing.slug
            or updated.event_slug != existing.event_slug
            or updated.condition_id != existing.condition_id
            or updated.outcomes != existing.outcomes
            or len(updated.assets) != len(existing.assets)
        ):
            changed = True
        state.market_catalog[market_meta.market_id] = updated

    if not changed and not pending_by_shard:
        return False

    await persist_market_catalog(state)
    for shard_index, asset_ids in pending_by_shard.items():
        await state.shard_commands[shard_index].put({"op": "subscribe", "asset_ids": asset_ids})

    stats.markets_total = len(state.market_catalog)
    stats.assets_total = len(state.asset_to_market)
    return True


def build_market_meta_from_event(item: dict[str, Any]) -> MarketMeta | None:
    market_meta = build_market_meta(item)
    if not market_meta.market_id or not market_meta.assets:
        return None
    return market_meta


def archive_resolved_market_sync(
    *,
    data_root: Path,
    market_meta: MarketMeta,
    event_payload: dict[str, Any],
) -> None:
    market_id = market_meta.market_id
    market_path = data_root / market_id
    resolved_root = data_root / "resolved_market"
    resolved_root.mkdir(parents=True, exist_ok=True)
    archive_path = resolved_root / f"{market_id}.tar.xz"
    if archive_path.exists():
        if market_path.exists():
            shutil.rmtree(market_path, ignore_errors=True)
        return

    with tarfile.open(archive_path, mode="w:xz", preset=HIGH_XZ_PRESET) as tar:
        if market_path.exists():
            tar.add(market_path, arcname=market_id, recursive=True)

    if market_path.exists():
        shutil.rmtree(market_path, ignore_errors=True)


async def archive_resolved_market(
    *,
    state: CollectorState,
    market_meta: MarketMeta,
    event_payload: dict[str, Any],
    stats: RunStats,
) -> None:
    await asyncio.to_thread(
        archive_resolved_market_sync,
        data_root=state.data_root,
        market_meta=market_meta,
        event_payload=event_payload,
    )
    stats.resolved_markets_archived += 1


async def handle_market_resolved(
    *,
    state: CollectorState,
    event_payload: dict[str, Any],
    stats: RunStats,
) -> None:
    market_id = str(event_payload.get("market") or event_payload.get("condition_id") or event_payload.get("id") or "")
    if not market_id or market_id in state.resolved_markets:
        return

    market_meta = state.market_catalog.pop(market_id, None)
    if market_meta is None:
        return

    state.resolved_markets.add(market_id)
    pending_by_shard: dict[int, list[str]] = {}

    for asset in market_meta.assets:
        state.asset_to_market.pop(asset.asset_id, None)
        for shard_index, assets in enumerate(state.shard_assets):
            if asset.asset_id in assets:
                assets.remove(asset.asset_id)
                pending_by_shard.setdefault(shard_index, []).append(asset.asset_id)
                break

    await persist_market_catalog(state)
    for shard_index, asset_ids in pending_by_shard.items():
        await state.shard_commands[shard_index].put({"op": "unsubscribe", "asset_ids": asset_ids})

    stats.markets_total = len(state.market_catalog)
    stats.assets_total = len(state.asset_to_market)
    await archive_resolved_market(state=state, market_meta=market_meta, event_payload=event_payload, stats=stats)


async def writer_task(
    queue: asyncio.Queue,
    state: CollectorState,
    stats: RunStats,
) -> None:
    known_markets: set[str] = set()

    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        events: list[dict[str, Any]] = item
        grouped: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            market_id = str(event.get("market") or "")
            asset_id = str(event.get("asset_id") or "")
            if not market_id or not asset_id:
                continue
            if market_id in state.resolved_markets or market_id not in state.market_catalog:
                continue
            grouped.setdefault(market_id, []).append(event)

        for market_id, market_events in grouped.items():
            market_meta = state.market_catalog.get(market_id)
            if market_meta is None:
                continue

            asset_by_id = {asset.asset_id: asset for asset in market_meta.assets}
            mdir = market_dir(state.data_root, market_id)
            mdir.mkdir(parents=True, exist_ok=True)

            if market_id not in known_markets:
                known_markets.add(market_id)
                stats.markets_written += 1

            book_lines: list[str] = []
            price_change_lines: list[str] = []
            for event in market_events:
                asset_id = str(event.get("asset_id") or "")
                asset_meta = asset_by_id.get(asset_id)
                if asset_meta is None:
                    continue
                event_type = str(event.get("event_type") or "")
                if event_type == "book":
                    compact = compact_book_record(asset_index=asset_meta.asset_index, book=event)
                    book_lines.append(json.dumps(compact, ensure_ascii=False, separators=(",", ":")))
                elif event_type == "price_change":
                    compact = compact_price_change_record(asset_index=asset_meta.asset_index, price_change=event)
                    price_change_lines.append(json.dumps(compact, ensure_ascii=False, separators=(",", ":")))

            if book_lines:
                blob = ("\n".join(book_lines) + "\n").encode("utf-8")
                with (mdir / "books.jsonl").open("ab") as f:
                    f.write(blob)
                stats.books_bytes += len(blob)

            if price_change_lines:
                blob = ("\n".join(price_change_lines) + "\n").encode("utf-8")
                with (mdir / "price_changes.jsonl").open("ab") as f:
                    f.write(blob)
                stats.price_change_bytes += len(blob)

        queue.task_done()


async def process_shard_commands(
    *,
    ws: Any,
    command_queue: asyncio.Queue,
    current_assets: set[str],
) -> None:
    while True:
        try:
            command = command_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        if not isinstance(command, dict):
            command_queue.task_done()
            continue

        asset_ids = [str(asset_id) for asset_id in command.get("asset_ids", []) if asset_id]
        op = str(command.get("op") or "")

        if op == "subscribe" and asset_ids:
            new_assets = [asset_id for asset_id in asset_ids if asset_id not in current_assets]
            if new_assets:
                current_assets.update(new_assets)
                await ws.send(
                    json.dumps(
                        {
                            "assets_ids": new_assets,
                            "operation": "subscribe",
                        }
                    )
                )
        elif op == "unsubscribe" and asset_ids:
            existing_assets = [asset_id for asset_id in asset_ids if asset_id in current_assets]
            if existing_assets:
                for asset_id in existing_assets:
                    current_assets.discard(asset_id)
                await ws.send(
                    json.dumps(
                        {
                            "assets_ids": existing_assets,
                            "operation": "unsubscribe",
                        }
                    )
                )

        command_queue.task_done()


async def listen_shard(
    *,
    shard_index: int,
    initial_assets: list[str],
    command_queue: asyncio.Queue,
    queue: asyncio.Queue,
    stats: RunStats,
    stop_at: float | None,
    open_timeout: float,
    recv_timeout: float,
    idle_reconnect_seconds: float,
    proxy_url: str,
) -> None:
    current_assets = set(initial_assets)
    failure_streak = 0

    while stop_at is None or asyncio.get_running_loop().time() < stop_at:
        try:
            async with connect(
                MARKET_WSS,
                proxy=proxy_url or None,
                open_timeout=open_timeout,
                ping_interval=20,
                ping_timeout=20,
                max_size=None,
            ) as ws:
                failure_streak = 0
                if current_assets:
                    await ws.send(
                        json.dumps(
                            {
                                "assets_ids": sorted(current_assets),
                                "type": "market",
                                "custom_feature_enabled": False,
                            }
                        )
                    )

                last_books_at = asyncio.get_running_loop().time()
                while stop_at is None or asyncio.get_running_loop().time() < stop_at:
                    await process_shard_commands(ws=ws, command_queue=command_queue, current_assets=current_assets)
                    try:
                        raw_message = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                    except TimeoutError:
                        await ws.ping()
                        if asyncio.get_running_loop().time() - last_books_at >= idle_reconnect_seconds:
                            break
                        continue

                    try:
                        payload = json.loads(raw_message)
                    except json.JSONDecodeError:
                        continue

                    events = normalize_market_events_payload(payload)
                    if not events:
                        continue

                    await queue.put(events)
                    stats.ws_messages += 1
                    stats.ws_book_entries += sum(1 for item in events if item.get("event_type") == "book")
                    stats.ws_price_change_entries += sum(1 for item in events if item.get("event_type") == "price_change")
                    last_books_at = asyncio.get_running_loop().time()
        except (ConnectionClosed, OSError, TimeoutError):
            failure_streak += 1
            delay = min(30.0, 1.5 * (2 ** min(failure_streak, 5))) + random.uniform(0.0, 1.0)
            await asyncio.sleep(delay)
        except Exception:
            failure_streak += 1
            delay = min(30.0, 1.5 * (2 ** min(failure_streak, 5))) + random.uniform(0.0, 1.0)
            await asyncio.sleep(delay)


async def listen_event_connection(
    *,
    anchor_assets: list[str],
    event_queue: asyncio.Queue,
    stats: RunStats,
    stop_at: float | None,
    open_timeout: float,
    recv_timeout: float,
    idle_reconnect_seconds: float,
    proxy_url: str,
) -> None:
    failure_streak = 0
    subscribe_message = json.dumps(
        {
            "assets_ids": anchor_assets,
            "type": "market",
            "custom_feature_enabled": True,
        }
    )

    while stop_at is None or asyncio.get_running_loop().time() < stop_at:
        try:
            async with connect(
                MARKET_WSS,
                proxy=proxy_url or None,
                open_timeout=open_timeout,
                ping_interval=20,
                ping_timeout=20,
                max_size=None,
            ) as ws:
                failure_streak = 0
                await ws.send(subscribe_message)
                last_event_at = asyncio.get_running_loop().time()

                while stop_at is None or asyncio.get_running_loop().time() < stop_at:
                    try:
                        raw_message = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                    except TimeoutError:
                        await ws.ping()
                        if asyncio.get_running_loop().time() - last_event_at >= idle_reconnect_seconds:
                            break
                        continue

                    try:
                        payload = json.loads(raw_message)
                    except json.JSONDecodeError:
                        continue

                    items = normalize_payload_items(payload)
                    if not items:
                        continue

                    for item in items:
                        event_type = str(item.get("event_type") or "")
                        if event_type == "new_market":
                            stats.event_messages += 1
                            stats.new_markets_seen += 1
                            await event_queue.put(item)
                            last_event_at = asyncio.get_running_loop().time()
                        elif event_type == "market_resolved":
                            stats.event_messages += 1
                            stats.resolved_markets_seen += 1
                            await event_queue.put(item)
                            last_event_at = asyncio.get_running_loop().time()
        except (ConnectionClosed, OSError, TimeoutError):
            failure_streak += 1
            delay = min(30.0, 1.5 * (2 ** min(failure_streak, 5))) + random.uniform(0.0, 1.0)
            await asyncio.sleep(delay)
        except Exception:
            failure_streak += 1
            delay = min(30.0, 1.5 * (2 ** min(failure_streak, 5))) + random.uniform(0.0, 1.0)
            await asyncio.sleep(delay)


async def event_processor_task(
    *,
    event_queue: asyncio.Queue,
    state: CollectorState,
    stats: RunStats,
) -> None:
    while True:
        item = await event_queue.get()
        if item is None:
            event_queue.task_done()
            break

        event_type = str(item.get("event_type") or "")
        if event_type == "market_resolved":
            await handle_market_resolved(state=state, event_payload=item, stats=stats)
            event_queue.task_done()
            continue

        if event_type == "new_market":
            provisional = build_market_meta_from_event(item)
            if provisional is not None:
                await upsert_market_meta(
                    state=state,
                    market_meta=provisional,
                    stats=stats,
                )
            event_queue.task_done()
            continue

        event_queue.task_done()


def choose_event_anchor_assets(market_catalog: dict[str, MarketMeta], count: int) -> list[str]:
    anchors: list[str] = []
    for market_meta in market_catalog.values():
        for asset in market_meta.assets:
            anchors.append(asset.asset_id)
            if len(anchors) >= count:
                return anchors
    return anchors


async def main_async(args: argparse.Namespace) -> None:
    loop = asyncio.get_running_loop()
    default_exception_handler = loop.get_exception_handler()

    def quiet_websocket_bug(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        exc = context.get("exception")
        message = context.get("message", "")
        if (
            isinstance(exc, AttributeError)
            and "recv_messages" in str(exc)
            and "Connection.connection_lost" in message
        ):
            return
        if default_exception_handler is not None:
            default_exception_handler(_loop, context)
        else:
            _loop.default_exception_handler(context)

    loop.set_exception_handler(quiet_websocket_bug)

    stop_at = None if args.duration_seconds <= 0 else asyncio.get_running_loop().time() + args.duration_seconds
    market_catalog, asset_ids = await fetch_markets_and_assets_with_retry(
        page_limit=args.page_limit,
        stop_at=stop_at,
        proxy_url=args.proxy_url,
    )
    chunks = chunked(asset_ids, args.chunk_size)
    stats = RunStats(
        markets_total=len(market_catalog),
        assets_total=len(asset_ids),
        chunks_total=len(chunks),
    )

    data_root = Path(args.data_root)
    meta_path = data_root / "all_market_meta.json"
    await asyncio.to_thread(write_all_market_meta, meta_path, market_catalog)

    shard_assets = [set(chunk) for chunk in chunks]
    shard_commands = [asyncio.Queue() for _ in chunks]
    state = CollectorState(
        data_root=data_root,
        meta_path=meta_path,
        market_catalog=market_catalog,
        asset_to_market=build_asset_to_market(market_catalog),
        shard_assets=shard_assets,
        shard_commands=shard_commands,
        resolved_markets=set(),
        meta_lock=asyncio.Lock(),
    )

    summary_path = data_root / "state" / args.summary_file
    queue: asyncio.Queue = asyncio.Queue(maxsize=args.queue_maxsize)
    event_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

    writer = asyncio.create_task(writer_task(queue, state, stats))
    event_processor = asyncio.create_task(
        event_processor_task(
            event_queue=event_queue,
            state=state,
            stats=stats,
        )
    )

    listeners = [
        asyncio.create_task(
            listen_shard(
                shard_index=idx,
                initial_assets=chunk,
                command_queue=shard_commands[idx],
                queue=queue,
                stats=stats,
                stop_at=stop_at,
                open_timeout=args.open_timeout,
                recv_timeout=args.recv_timeout,
                idle_reconnect_seconds=args.idle_reconnect_seconds,
                proxy_url=args.proxy_url,
            )
        )
        for idx, chunk in enumerate(chunks)
    ]

    anchor_assets = choose_event_anchor_assets(market_catalog, max(1, args.event_anchor_assets))
    event_listener = asyncio.create_task(
        listen_event_connection(
            anchor_assets=anchor_assets,
            event_queue=event_queue,
            stats=stats,
            stop_at=stop_at,
            open_timeout=args.open_timeout,
            recv_timeout=args.recv_timeout,
            idle_reconnect_seconds=args.idle_reconnect_seconds,
            proxy_url=args.proxy_url,
        )
    )

    try:
        await asyncio.gather(*listeners, event_listener)
    finally:
        await queue.put(None)
        await event_queue.put(None)
        await queue.join()
        await event_queue.join()
        await writer
        await event_processor

    total_bytes = 0
    if data_root.exists():
        total_bytes = sum(path.stat().st_size for path in data_root.rglob("*") if path.is_file())

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts_finished": datetime.now(UTC).isoformat(),
        "markets_total": stats.markets_total,
        "assets_total": stats.assets_total,
        "chunks_total": stats.chunks_total,
        "markets_written": stats.markets_written,
        "ws_messages": stats.ws_messages,
        "ws_book_entries": stats.ws_book_entries,
        "books_bytes": stats.books_bytes,
        "ws_price_change_entries": stats.ws_price_change_entries,
        "price_change_bytes": stats.price_change_bytes,
        "event_messages": stats.event_messages,
        "new_markets_seen": stats.new_markets_seen,
        "new_markets_added": stats.new_markets_added,
        "resolved_markets_seen": stats.resolved_markets_seen,
        "resolved_markets_archived": stats.resolved_markets_archived,
        "data_root": str(data_root),
        "data_total_bytes": total_bytes,
    }
    summary_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=True))


def main() -> int:
    args = parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_async(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
