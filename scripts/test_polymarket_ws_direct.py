from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect


MARKET_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_ASSET_ID = "19852850164094540668760898516189342174175377693880304142110125820656108135966"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test Polymarket market websocket with direct connection only (no proxy)."
    )
    parser.add_argument(
        "--asset-id",
        default=DEFAULT_ASSET_ID,
        help="Asset id to subscribe to. Defaults to a known sample asset.",
    )
    parser.add_argument(
        "--open-timeout",
        type=float,
        default=20.0,
        help="WebSocket open timeout in seconds.",
    )
    parser.add_argument(
        "--recv-timeout",
        type=float,
        default=20.0,
        help="How long to wait for the first message.",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Optional path to write the result JSON.",
    )
    return parser.parse_args()


def clear_proxy_env() -> dict[str, str]:
    removed: dict[str, str] = {}
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        value = os.environ.pop(key, None)
        if value is not None:
            removed[key] = value
    return removed


def json_preview(payload: Any, limit: int = 500) -> str:
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(text) <= limit:
        return text
    return text[:limit] + "...(truncated)"


async def main_async(args: argparse.Namespace) -> int:
    removed_proxy_env = clear_proxy_env()
    started = time.perf_counter()
    subscribe_message = {
        "assets_ids": [args.asset_id],
        "type": "market",
    }
    result: dict[str, Any] = {
        "ok": False,
        "ws": MARKET_WSS,
        "asset_id": args.asset_id,
        "proxy_mode": "direct_only",
        "env_proxy_removed": removed_proxy_env,
        "open_timeout": args.open_timeout,
        "recv_timeout": args.recv_timeout,
    }

    try:
        async with connect(
            MARKET_WSS,
            proxy=None,
            open_timeout=args.open_timeout,
            ping_interval=20,
            ping_timeout=20,
            max_size=None,
        ) as ws:
            result["handshake_seconds"] = round(time.perf_counter() - started, 3)
            await ws.send(json.dumps(subscribe_message))
            raw_message = await asyncio.wait_for(ws.recv(), timeout=args.recv_timeout)
            parsed = json.loads(raw_message)

            result["ok"] = True
            result["message_type"] = type(parsed).__name__
            result["message_preview"] = json_preview(parsed)
            if isinstance(parsed, list):
                result["items"] = len(parsed)
                if parsed and isinstance(parsed[0], dict):
                    result["first_event_type"] = parsed[0].get("event_type", "")
            elif isinstance(parsed, dict):
                result["first_event_type"] = parsed.get("event_type", "")
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        result["error"] = str(exc)

    if args.output_file:
        output_path = Path(args.output_file)
        if not output_path.is_absolute():
            output_path = Path.cwd() / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


def main() -> int:
    args = parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
