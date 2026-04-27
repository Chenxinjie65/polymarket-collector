from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import lzma
import random
import shutil
import sys
import tarfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


GAMMA_API = "https://gamma-api.polymarket.com"
MARKET_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PRICE_SCALE = 1_000_000
SIZE_SCALE = 100
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
    market_index: int
    write_shard: int
    resolved: bool
    resolved_at: str
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
    meta_journal_path: Path
    market_catalog: dict[str, MarketMeta]
    asset_to_shard: dict[str, int]
    shard_loads: list[int]
    shard_commands: list[asyncio.Queue]
    resolved_markets: set[str]
    next_market_index: int
    write_shards: int
    meta_lock: asyncio.Lock


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Listen to all Polymarket books and store them in "
            "all_market_meta.json + hourly shard jsonl files."
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
    parser.add_argument("--chunk-size", type=int, default=2000, help="Assets per websocket connection")
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
    parser.add_argument("--queue-maxsize", type=int, default=256, help="Writer queue size")
    parser.add_argument(
        "--ws-max-queue",
        type=int,
        default=1,
        help="Max queued websocket messages per connection before applying backpressure.",
    )
    parser.add_argument(
        "--write-shards",
        type=int,
        default=64,
        help="Fixed shard count for hourly output files.",
    )
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


def scale_decimal(value: Any, scale: int) -> int | None:
    if value in (None, ""):
        return None
    try:
        scaled = (Decimal(str(value)) * scale).to_integral_value()
    except (InvalidOperation, ValueError):
        return None
    return int(scaled)


def parse_timestamp_ms(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return max(0, int(str(value)))
    except (TypeError, ValueError):
        return 0


def stable_shard_index(key: str, shard_count: int) -> int:
    normalized_count = max(1, shard_count)
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % normalized_count


def timestamp_to_hour_bucket(timestamp_ms: int) -> tuple[str, str]:
    if timestamp_ms > 0:
        bucket_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    else:
        bucket_time = datetime.now(UTC)
    return bucket_time.strftime("%Y-%m-%d"), bucket_time.strftime("%H")


def sharded_output_path(data_root: Path, dataset: str, timestamp_ms: int, shard_index: int) -> Path:
    dt_value, hour_value = timestamp_to_hour_bucket(timestamp_ms)
    return data_root / dataset / f"dt={dt_value}" / f"hour={hour_value}" / f"shard-{shard_index:04d}.jsonl"


def initialize_market_storage(market_catalog: dict[str, MarketMeta], write_shards: int) -> int:
    next_market_index = 1
    for market_id in sorted(market_catalog):
        market_meta = market_catalog[market_id]
        market_meta.market_index = next_market_index
        market_meta.write_shard = stable_shard_index(market_id, write_shards)
        next_market_index += 1
    return next_market_index


def market_meta_to_dict(market_meta: MarketMeta) -> dict[str, Any]:
    return {
        "market": market_meta.market_id,
        "market_index": market_meta.market_index,
        "write_shard": market_meta.write_shard,
        "resolved": market_meta.resolved,
        "resolved_at": market_meta.resolved_at,
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


def market_meta_from_dict(item: dict[str, Any], write_shards: int) -> MarketMeta | None:
    market_id = str(item.get("market") or "")
    if not market_id:
        return None

    assets: list[AssetMeta] = []
    for asset in item.get("assets") or []:
        asset_id = str(asset.get("asset_id") or "")
        if not asset_id:
            continue
        try:
            asset_index = int(asset.get("asset_index") or 0)
        except (TypeError, ValueError):
            asset_index = 0
        if asset_index <= 0:
            continue
        assets.append(
            AssetMeta(
                asset_id=asset_id,
                asset_index=asset_index,
                outcome=str(asset.get("outcome") or ""),
                tick_size=str(asset.get("tick_size") or "0.01"),
            )
        )

    try:
        market_index = int(item.get("market_index") or 0)
    except (TypeError, ValueError):
        market_index = 0
    try:
        write_shard = int(item.get("write_shard") or stable_shard_index(market_id, write_shards))
    except (TypeError, ValueError):
        write_shard = stable_shard_index(market_id, write_shards)

    return MarketMeta(
        market_id=market_id,
        market_index=max(0, market_index),
        write_shard=write_shard,
        resolved=bool(item.get("resolved", False)),
        resolved_at=str(item.get("resolved_at") or ""),
        gamma_market_id=str(item.get("gamma_market_id") or ""),
        question=str(item.get("question") or ""),
        slug=str(item.get("slug") or ""),
        event_slug=str(item.get("event_slug") or ""),
        condition_id=str(item.get("condition_id") or market_id),
        outcomes=[],
        assets=assets,
    )


def _finalize_loaded_market_catalog(markets: list[dict[str, Any]], write_shards: int) -> tuple[dict[str, MarketMeta], int]:
    market_catalog: dict[str, MarketMeta] = {}
    max_market_index = 0
    for item in markets:
        if not isinstance(item, dict):
            continue
        market_meta = market_meta_from_dict(item, write_shards)
        if market_meta is None:
            continue
        if market_meta.market_index <= 0:
            max_market_index += 1
            market_meta.market_index = max_market_index
        else:
            max_market_index = max(max_market_index, market_meta.market_index)
        market_catalog[market_meta.market_id] = market_meta

    return market_catalog, max_market_index + 1


def load_existing_market_catalog(meta_path: Path, journal_path: Path, write_shards: int) -> tuple[dict[str, MarketMeta], int]:
    if meta_path.exists():
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        markets = payload.get("markets")
        if isinstance(markets, list):
            return _finalize_loaded_market_catalog(markets, write_shards)

    if journal_path.exists():
        latest_by_market: dict[str, dict[str, Any]] = {}
        with journal_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                market_payload = payload.get("market")
                if not isinstance(market_payload, dict):
                    continue
                market_id = str(market_payload.get("market") or "")
                if not market_id:
                    continue
                latest_by_market[market_id] = market_payload
        if latest_by_market:
            return _finalize_loaded_market_catalog(list(latest_by_market.values()), write_shards)

    return {}, 1


def merge_market_catalog(
    *,
    existing_catalog: dict[str, MarketMeta],
    fetched_catalog: dict[str, MarketMeta],
    write_shards: int,
    next_market_index: int,
) -> tuple[dict[str, MarketMeta], int]:
    merged_catalog = dict(existing_catalog)

    for market_id, fetched_meta in fetched_catalog.items():
        existing = merged_catalog.get(market_id)
        if existing is None:
            fetched_meta.market_index = next_market_index
            fetched_meta.write_shard = stable_shard_index(market_id, write_shards)
            fetched_meta.resolved = False
            fetched_meta.resolved_at = ""
            merged_catalog[market_id] = fetched_meta
            next_market_index += 1
            continue

        existing_assets = {asset.asset_id: asset for asset in existing.assets}
        merged_assets = list(existing.assets)
        next_asset_index = max((asset.asset_index for asset in merged_assets), default=0) + 1

        for fetched_asset in fetched_meta.assets:
            current_asset = existing_assets.get(fetched_asset.asset_id)
            if current_asset is not None:
                current_asset.outcome = fetched_asset.outcome or current_asset.outcome
                current_asset.tick_size = fetched_asset.tick_size or current_asset.tick_size
            else:
                merged_assets.append(
                    AssetMeta(
                        asset_id=fetched_asset.asset_id,
                        asset_index=next_asset_index,
                        outcome=fetched_asset.outcome,
                        tick_size=fetched_asset.tick_size,
                    )
                )
                next_asset_index += 1

        merged_catalog[market_id] = MarketMeta(
            market_id=existing.market_id,
            market_index=existing.market_index,
            write_shard=existing.write_shard,
            resolved=existing.resolved,
            resolved_at=existing.resolved_at,
            gamma_market_id=fetched_meta.gamma_market_id or existing.gamma_market_id,
            question=fetched_meta.question or existing.question,
            slug=fetched_meta.slug or existing.slug,
            event_slug=fetched_meta.event_slug or existing.event_slug,
            condition_id=fetched_meta.condition_id or existing.condition_id,
            outcomes=fetched_meta.outcomes or existing.outcomes,
            assets=merged_assets,
        )

    return merged_catalog, next_market_index


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
        market_index=0,
        write_shard=0,
        resolved=False,
        resolved_at="",
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
    normalized: list[dict[str, Any]] = []
    for item in normalize_payload_items(payload):
        event_type = str(item.get("event_type") or "")

        if not event_type and item.get("asset_id") and item.get("market") and ("bids" in item or "asks" in item):
            book_item = dict(item)
            book_item["event_type"] = "book"
            normalized.append(book_item)
            continue

        if event_type == "book":
            if item.get("asset_id") and item.get("market"):
                normalized.append(item)
            continue

        if event_type == "price_change":
            market_id = str(item.get("market") or "")
            timestamp = str(item.get("timestamp") or "")
            price_changes = item.get("price_changes")
            if not market_id or not isinstance(price_changes, list):
                continue
            for change in price_changes:
                if not isinstance(change, dict):
                    continue
                asset_id = str(change.get("asset_id") or "")
                if not asset_id:
                    continue
                expanded = dict(change)
                expanded["event_type"] = "price_change"
                expanded["market"] = market_id
                if timestamp and expanded.get("timestamp") in (None, ""):
                    expanded["timestamp"] = timestamp
                normalized.append(expanded)
    return normalized


def normalize_payload_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def compact_levels(levels: Any) -> list[list[int]]:
    if not isinstance(levels, list):
        return []
    compact: list[list[int]] = []
    for level in levels:
        if not isinstance(level, dict):
            continue
        scaled_price = scale_decimal(level.get("price"), PRICE_SCALE)
        scaled_size = scale_decimal(level.get("size"), SIZE_SCALE)
        if scaled_price is None or scaled_size is None:
            continue
        compact.append([scaled_price, scaled_size])
    return compact


def encode_side(value: Any) -> int | None:
    side = str(value or "").upper()
    if side == "BUY":
        return 1
    if side == "SELL":
        return 2
    return None


def compact_book_record(*, market_index: int, asset_index: int, book: dict[str, Any]) -> dict[str, Any]:
    record: dict[str, Any] = {
        "m": market_index,
        "i": asset_index,
        "t": parse_timestamp_ms(book.get("timestamp")),
        "b": compact_levels(book.get("bids")),
        "a": compact_levels(book.get("asks")),
    }
    return record


def compact_price_change_record(*, market_index: int, asset_index: int, price_change: dict[str, Any]) -> dict[str, Any]:
    record: dict[str, Any] = {
        "m": market_index,
        "i": asset_index,
        "t": parse_timestamp_ms(price_change.get("timestamp")),
    }
    short_key_map = {
        "price": "p",
        "size": "s",
        "side": "y",
        "best_bid": "bb",
        "best_ask": "ba",
    }
    scale_map = {
        "price": PRICE_SCALE,
        "size": SIZE_SCALE,
        "best_bid": PRICE_SCALE,
        "best_ask": PRICE_SCALE,
    }
    for key, value in price_change.items():
        if key in {"asset_id", "market", "event_type", "timestamp"}:
            continue
        if value in (None, ""):
            continue
        if key == "side":
            encoded_side = encode_side(value)
            if encoded_side is not None:
                record["y"] = encoded_side
            continue
        short_key = short_key_map.get(key, key)
        scale = scale_map.get(key)
        if scale is not None:
            scaled = scale_decimal(value, scale)
            if scaled is not None:
                record[short_key] = scaled
            continue
        record[short_key] = value
    return record


def build_asset_to_shard(chunks: list[list[str]]) -> tuple[dict[str, int], list[int]]:
    asset_to_shard: dict[str, int] = {}
    shard_loads: list[int] = []
    for shard_index, chunk in enumerate(chunks):
        shard_loads.append(len(chunk))
        for asset_id in chunk:
            asset_to_shard[asset_id] = shard_index
    return asset_to_shard, shard_loads


def append_market_catalog_event(journal_path: Path, op: str, market_meta: MarketMeta) -> None:
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts_utc": datetime.now(UTC).isoformat(),
        "op": op,
        "market": market_meta_to_dict(market_meta),
    }
    with journal_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
        handle.write("\n")


def bootstrap_market_catalog_journal(journal_path: Path, market_catalog: dict[str, MarketMeta]) -> None:
    if journal_path.exists() and journal_path.stat().st_size > 0:
        return
    for market_meta in sorted(market_catalog.values(), key=lambda item: item.market_index):
        append_market_catalog_event(journal_path, "bootstrap_market", market_meta)


def write_all_market_meta(meta_path: Path, market_catalog: dict[str, MarketMeta], write_shards: int) -> None:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "layout_version": 7,
        "storage_format": "hourly shard jsonl with compact integer encoding",
        "price_scale": PRICE_SCALE,
        "size_scale": SIZE_SCALE,
        "write_shards": max(1, write_shards),
        "markets": [
            market_meta_to_dict(market_meta)
            for market_meta in sorted(market_catalog.values(), key=lambda item: item.market_index)
        ],
    }
    meta_path.write_text(json.dumps(payload, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")


async def persist_market_catalog(state: CollectorState) -> None:
    async with state.meta_lock:
        snapshot = dict(state.market_catalog)
        await asyncio.to_thread(write_all_market_meta, state.meta_path, snapshot, state.write_shards)


def choose_shard_index(shard_loads: list[int]) -> int:
    return min(range(len(shard_loads)), key=shard_loads.__getitem__)


async def add_new_markets(state: CollectorState, market_metas: list[MarketMeta], stats: RunStats) -> int:
    pending_by_shard: dict[int, list[str]] = {}
    added_markets = 0

    for market_meta in market_metas:
        if not market_meta.market_id or market_meta.market_id in state.market_catalog or market_meta.market_id in state.resolved_markets:
            continue
        if not market_meta.assets:
            continue
        market_meta.market_index = state.next_market_index
        market_meta.write_shard = stable_shard_index(market_meta.market_id, state.write_shards)
        market_meta.resolved = False
        market_meta.resolved_at = ""
        state.next_market_index += 1

        state.market_catalog[market_meta.market_id] = market_meta
        added_markets += 1
        await asyncio.to_thread(append_market_catalog_event, state.meta_journal_path, "add_market", market_meta)

        for asset in market_meta.assets:
            if asset.asset_id in state.asset_to_shard:
                continue
            shard_index = choose_shard_index(state.shard_loads)
            state.shard_loads[shard_index] += 1
            state.asset_to_shard[asset.asset_id] = shard_index
            pending_by_shard.setdefault(shard_index, []).append(asset.asset_id)

    if not added_markets:
        return 0

    await persist_market_catalog(state)
    for shard_index, asset_ids in pending_by_shard.items():
        await state.shard_commands[shard_index].put({"op": "subscribe", "asset_ids": asset_ids})

    stats.markets_total = len(state.market_catalog)
    stats.assets_total = len(state.asset_to_shard)
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
        market_meta.market_index = state.next_market_index
        market_meta.write_shard = stable_shard_index(market_meta.market_id, state.write_shards)
        market_meta.resolved = False
        market_meta.resolved_at = ""
        state.next_market_index += 1
        state.market_catalog[market_meta.market_id] = market_meta
        changed = True
        for asset in market_meta.assets:
            if asset.asset_id in state.asset_to_shard:
                continue
            shard_index = choose_shard_index(state.shard_loads)
            state.shard_loads[shard_index] += 1
            state.asset_to_shard[asset.asset_id] = shard_index
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
                shard_index = choose_shard_index(state.shard_loads)
                state.shard_loads[shard_index] += 1
                state.asset_to_shard[new_asset.asset_id] = shard_index
                pending_by_shard.setdefault(shard_index, []).append(new_asset.asset_id)

        updated = MarketMeta(
            market_id=existing.market_id,
            market_index=existing.market_index,
            write_shard=existing.write_shard,
            resolved=existing.resolved,
            resolved_at=existing.resolved_at,
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

    await asyncio.to_thread(append_market_catalog_event, state.meta_journal_path, "upsert_market", state.market_catalog[market_meta.market_id])
    await persist_market_catalog(state)
    for shard_index, asset_ids in pending_by_shard.items():
        await state.shard_commands[shard_index].put({"op": "subscribe", "asset_ids": asset_ids})

    stats.markets_total = len(state.market_catalog)
    stats.assets_total = len(state.asset_to_shard)
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
    marker_path = resolved_root / f"{market_id}.json"

    if market_path.exists():
        if not archive_path.exists():
            with tarfile.open(archive_path, mode="w:xz", preset=HIGH_XZ_PRESET) as tar:
                tar.add(market_path, arcname=market_id, recursive=True)
        shutil.rmtree(market_path, ignore_errors=True)

    marker_payload = {
        "market": market_id,
        "market_index": market_meta.market_index,
        "write_shard": market_meta.write_shard,
        "resolved_at": datetime.now(UTC).isoformat(),
        "layout_version": 7,
        "note": "shared hourly shard files stay in place; this marker only stops future collection",
        "event": event_payload,
    }
    marker_path.write_text(json.dumps(marker_payload, ensure_ascii=True, indent=2), encoding="utf-8")


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

    market_meta = state.market_catalog.get(market_id)
    if market_meta is None:
        return

    market_meta.resolved = True
    market_meta.resolved_at = datetime.now(UTC).isoformat()
    state.resolved_markets.add(market_id)
    pending_by_shard: dict[int, list[str]] = {}

    for asset in market_meta.assets:
        shard_index = state.asset_to_shard.pop(asset.asset_id, None)
        if shard_index is None:
            continue
        state.shard_loads[shard_index] = max(0, state.shard_loads[shard_index] - 1)
        pending_by_shard.setdefault(shard_index, []).append(asset.asset_id)

    await asyncio.to_thread(append_market_catalog_event, state.meta_journal_path, "resolve_market", market_meta)
    await persist_market_catalog(state)
    for shard_index, asset_ids in pending_by_shard.items():
        await state.shard_commands[shard_index].put({"op": "unsubscribe", "asset_ids": asset_ids})

    stats.markets_total = len(state.market_catalog)
    stats.assets_total = len(state.asset_to_shard)
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
        lines_by_path: dict[Path, list[str]] = {}
        asset_maps_by_market: dict[str, dict[str, AssetMeta]] = {}

        for event in events:
            market_id = str(event.get("market") or "")
            asset_id = str(event.get("asset_id") or "")
            if not market_id or not asset_id:
                continue

            market_meta = state.market_catalog.get(market_id)
            if market_meta is None or market_id in state.resolved_markets:
                continue

            asset_by_id = asset_maps_by_market.get(market_id)
            if asset_by_id is None:
                asset_by_id = {asset.asset_id: asset for asset in market_meta.assets}
                asset_maps_by_market[market_id] = asset_by_id
            asset_meta = asset_by_id.get(asset_id)
            if asset_meta is None:
                continue

            if market_id not in known_markets:
                known_markets.add(market_id)
                stats.markets_written += 1

            event_type = str(event.get("event_type") or "")
            timestamp_ms = parse_timestamp_ms(event.get("timestamp"))
            if event_type == "book":
                compact = compact_book_record(
                    market_index=market_meta.market_index,
                    asset_index=asset_meta.asset_index,
                    book=event,
                )
                path = sharded_output_path(state.data_root, "books", timestamp_ms, market_meta.write_shard)
                lines_by_path.setdefault(path, []).append(
                    json.dumps(compact, ensure_ascii=False, separators=(",", ":"))
                )
            elif event_type == "price_change":
                compact = compact_price_change_record(
                    market_index=market_meta.market_index,
                    asset_index=asset_meta.asset_index,
                    price_change=event,
                )
                path = sharded_output_path(state.data_root, "price_changes", timestamp_ms, market_meta.write_shard)
                lines_by_path.setdefault(path, []).append(
                    json.dumps(compact, ensure_ascii=False, separators=(",", ":"))
                )

        for path, lines in lines_by_path.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            blob = ("\n".join(lines) + "\n").encode("utf-8")
            with path.open("ab") as f:
                f.write(blob)
            if path.parts[-4] == "books":
                stats.books_bytes += len(blob)
            else:
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
    ws_max_queue: int,
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
                max_queue=ws_max_queue,
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
    ws_max_queue: int,
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
                max_queue=ws_max_queue,
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
    fetched_catalog, asset_ids = await fetch_markets_and_assets_with_retry(
        page_limit=args.page_limit,
        stop_at=stop_at,
        proxy_url=args.proxy_url,
    )
    data_root = Path(args.data_root)
    meta_path = data_root / "all_market_meta.json"
    meta_journal_path = data_root / "state" / "market_catalog.jsonl"

    existing_catalog, next_market_index = await asyncio.to_thread(
        load_existing_market_catalog,
        meta_path,
        meta_journal_path,
        args.write_shards,
    )
    market_catalog, next_market_index = merge_market_catalog(
        existing_catalog=existing_catalog,
        fetched_catalog=fetched_catalog,
        write_shards=args.write_shards,
        next_market_index=next_market_index,
    )
    chunks = chunked(asset_ids, args.chunk_size)
    stats = RunStats(
        markets_total=len(market_catalog),
        assets_total=len(asset_ids),
        chunks_total=len(chunks),
    )
    await asyncio.to_thread(write_all_market_meta, meta_path, market_catalog, args.write_shards)
    await asyncio.to_thread(bootstrap_market_catalog_journal, meta_journal_path, market_catalog)

    asset_to_shard, shard_loads = build_asset_to_shard(chunks)
    shard_commands = [asyncio.Queue() for _ in chunks]
    state = CollectorState(
        data_root=data_root,
        meta_path=meta_path,
        meta_journal_path=meta_journal_path,
        market_catalog=market_catalog,
        asset_to_shard=asset_to_shard,
        shard_loads=shard_loads,
        shard_commands=shard_commands,
        resolved_markets={market_id for market_id, market_meta in market_catalog.items() if market_meta.resolved},
        next_market_index=next_market_index,
        write_shards=max(1, args.write_shards),
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
                ws_max_queue=args.ws_max_queue,
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
            ws_max_queue=args.ws_max_queue,
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
