from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


MARKET_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Subscribe to a tiny set of assets with custom_feature_enabled=true and keep listening "
            "until a real market_resolved event is captured."
        )
    )
    parser.add_argument(
        "--data-root",
        default="data_all_books_jsonl_live",
        help="Data root containing all_market_meta.json and state/",
    )
    parser.add_argument(
        "--asset-count",
        type=int,
        default=3,
        help="How many assets to subscribe to for the probe",
    )
    parser.add_argument(
        "--open-timeout",
        type=float,
        default=20.0,
        help="WebSocket open timeout",
    )
    parser.add_argument(
        "--recv-timeout",
        type=float,
        default=30.0,
        help="WebSocket receive timeout",
    )
    parser.add_argument(
        "--state-subdir",
        default="state/market_resolved_probe",
        help="State output directory under data-root",
    )
    return parser.parse_args()


def load_probe_assets(meta_path: Path, asset_count: int) -> list[dict[str, str]]:
    payload = json.loads(meta_path.read_text(encoding="utf-8"))
    picked: list[dict[str, str]] = []
    for market in payload.get("markets", []):
        assets = market.get("assets") or []
        if not assets:
            continue
        picked.append(
            {
                "market": str(market.get("market") or ""),
                "question": str(market.get("question") or ""),
                "asset_id": str(assets[0].get("asset_id") or ""),
            }
        )
        if len(picked) >= asset_count:
            break
    return [item for item in picked if item["asset_id"]]


def extract_event_asset_ids(item: dict[str, Any]) -> list[str]:
    asset_ids: list[str] = []
    for key in ("assets_ids", "clob_token_ids"):
        val = item.get(key)
        if isinstance(val, list):
            asset_ids.extend(str(x) for x in val if x)
        elif isinstance(val, str) and val:
            try:
                parsed = json.loads(val)
            except json.JSONDecodeError:
                parsed = val
            if isinstance(parsed, list):
                asset_ids.extend(str(x) for x in parsed if x)
            else:
                asset_ids.append(str(val))
    return asset_ids


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def write_latest(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def main_async(args: argparse.Namespace) -> int:
    data_root = Path(args.data_root)
    if not data_root.is_absolute():
        data_root = Path(__file__).resolve().parent.parent / data_root

    state_dir = data_root / args.state_subdir
    state_dir.mkdir(parents=True, exist_ok=True)
    meta_path = data_root / "all_market_meta.json"
    selected_assets = load_probe_assets(meta_path, args.asset_count)
    asset_ids = [item["asset_id"] for item in selected_assets]
    subscribed = set(asset_ids)

    config_path = state_dir / "config.json"
    latest_path = state_dir / "latest.json"
    events_path = state_dir / "events.jsonl"
    result_path = state_dir / "captured_market_resolved.json"

    config_path.write_text(
        json.dumps(
            {
                "ts_started": datetime.now(UTC).isoformat(),
                "subscribed_assets": selected_assets,
                "ws": MARKET_WSS,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    counts = {
        "book": 0,
        "price_change": 0,
        "best_bid_ask": 0,
        "last_trade_price": 0,
        "new_market": 0,
        "market_resolved": 0,
        "other": 0,
        "reconnects": 0,
    }

    subscribe_message = json.dumps(
        {
            "assets_ids": asset_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
    )

    while True:
        try:
            async with connect(
                MARKET_WSS,
                open_timeout=args.open_timeout,
                ping_interval=20,
                ping_timeout=20,
                max_size=None,
            ) as ws:
                await ws.send(subscribe_message)
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=args.recv_timeout)
                    except TimeoutError:
                        await ws.ping()
                        write_latest(
                            latest_path,
                            {
                                "ts_iso": datetime.now(UTC).isoformat(),
                                "status": "idle_ping",
                                "counts": counts,
                            },
                        )
                        continue

                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    items = payload if isinstance(payload, list) else [payload]
                    write_latest(
                        latest_path,
                        {
                            "ts_iso": datetime.now(UTC).isoformat(),
                            "status": "receiving",
                            "counts": counts,
                            "batch_items": len(items),
                        },
                    )

                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        event_type = item.get("event_type")
                        if event_type in counts:
                            counts[event_type] += 1
                        elif item.get("asset_id") and item.get("market"):
                            counts["book"] += 1
                            continue
                        else:
                            counts["other"] += 1

                        if event_type not in ("new_market", "market_resolved"):
                            continue

                        event_asset_ids = extract_event_asset_ids(item)
                        relation = "unknown"
                        if event_asset_ids:
                            relation = (
                                "intersects_subscribed"
                                if subscribed.intersection(event_asset_ids)
                                else "disjoint_from_subscribed"
                            )
                        event_payload = {
                            "ts_probe": datetime.now(UTC).isoformat(),
                            "event_type": event_type,
                            "timestamp": item.get("timestamp"),
                            "market": item.get("market") or item.get("condition_id") or item.get("id"),
                            "question": item.get("question"),
                            "winning_asset_id": item.get("winning_asset_id"),
                            "winning_outcome": item.get("winning_outcome"),
                            "asset_ids": event_asset_ids,
                            "relation_to_subscribed_assets": relation,
                            "raw": item,
                            "counts": counts.copy(),
                        }
                        append_jsonl(events_path, event_payload)
                        write_latest(latest_path, event_payload)

                        if event_type == "market_resolved":
                            result_path.write_text(json.dumps(event_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                            return 0
        except (ConnectionClosed, OSError, TimeoutError):
            counts["reconnects"] += 1
            write_latest(
                latest_path,
                {
                    "ts_iso": datetime.now(UTC).isoformat(),
                    "status": "reconnecting",
                    "counts": counts,
                },
            )
            await asyncio.sleep(min(30, 2 + counts["reconnects"]))


def main() -> int:
    args = parse_args()
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
