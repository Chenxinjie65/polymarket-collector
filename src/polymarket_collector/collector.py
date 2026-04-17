from __future__ import annotations

import gzip
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

try:
    import websocket
except ImportError:  # pragma: no cover - optional at runtime
    websocket = None

try:
    from py_clob_client.client import ClobClient as _PyClobClient
    from py_clob_client.clob_types import BookParams as _BookParams
except ImportError:  # pragma: no cover - optional at runtime
    _PyClobClient = None
    _BookParams = None

try:
    from py_clob_clients.client import ClobClient as _PyClobClientAlt
    from py_clob_clients.clob_types import BookParams as _BookParamsAlt
except ImportError:  # pragma: no cover - optional at runtime
    _PyClobClientAlt = None
    _BookParamsAlt = None


GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
MARKET_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
MAX_TRADE_MARKET_QUERY_CHARS = 3000


@dataclass(slots=True)
class CollectorConfig:
    data_root: Path
    timeout_seconds: float = 20.0
    user_agent: str = "polymarket-collector-mvp/0.1"
    clob_driver: str = "raw"  # supported: raw, pyclob
    bucket_seconds: int = 3600
    writer_node_id: str = "standalone"


class JsonlGzWriter:
    def __init__(self, root: Path, *, bucket_seconds: int, node_id: str) -> None:
        self.root = root
        self.bucket_seconds = max(1, int(bucket_seconds))
        self.node_id = _safe_node_id(node_id)

    def write(self, source: str, records: list[dict[str, Any]]) -> Path | None:
        if not records:
            return None

        bucket_start = self._bucket_start(records[0])
        directory = (
            self.root
            / "raw"
            / f"source={source}"
            / f"dt={bucket_start:%Y-%m-%d}"
            / f"hour={bucket_start:%H}"
        )
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"bucket_start={bucket_start:%Y%m%dT%H%M%SZ}_node={self.node_id}.jsonl.gz"

        with gzip.open(path, "at", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=True, separators=(",", ":")))
                handle.write("\n")

        return path

    def _bucket_start(self, record: dict[str, Any]) -> datetime:
        ts_value = record.get("ts_ingest")
        if isinstance(ts_value, str):
            dt = _parse_iso_datetime(ts_value)
        else:
            dt = datetime.now(UTC)
        epoch = int(dt.timestamp())
        bucket_epoch = (epoch // self.bucket_seconds) * self.bucket_seconds
        return datetime.fromtimestamp(bucket_epoch, tz=UTC)


class PolymarketCollector:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        self.writer = JsonlGzWriter(
            config.data_root,
            bucket_seconds=config.bucket_seconds,
            node_id=config.writer_node_id,
        )
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.user_agent})
        self._pyclob = _build_pyclob_client(config.clob_driver)

    def discover_markets(
        self,
        *,
        limit: int = 100,
        active: bool = True,
        closed: bool = False,
        archived: bool = False,
    ) -> tuple[list[dict[str, Any]], Path | None]:
        response = self.session.get(
            f"{GAMMA_API}/markets",
            params={
                "limit": limit,
                "active": str(active).lower(),
                "closed": str(closed).lower(),
                "archived": str(archived).lower(),
            },
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        markets = response.json()
        wrapped = [self._wrap_record("gamma_markets", market) for market in markets]
        path = self.writer.write("gamma_markets", wrapped)
        return markets, path

    def discover_markets_all_pages(
        self,
        *,
        page_limit: int = 500,
        active: bool = True,
        closed: bool = False,
        archived: bool = False,
        max_pages: int = 200,
    ) -> list[dict[str, Any]]:
        markets: list[dict[str, Any]] = []
        offset = 0
        seen_ids: set[str] = set()
        page_count = 0

        while page_count < max_pages:
            response = self.session.get(
                f"{GAMMA_API}/markets",
                params={
                    "limit": page_limit,
                    "offset": offset,
                    "active": str(active).lower(),
                    "closed": str(closed).lower(),
                    "archived": str(archived).lower(),
                },
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            page = response.json()
            if not page:
                break

            dedup_page = []
            for item in page:
                market_id = item.get("id")
                key = str(market_id) if market_id is not None else json.dumps(item, sort_keys=True)
                if key in seen_ids:
                    continue
                seen_ids.add(key)
                dedup_page.append(item)

            markets.extend(dedup_page)
            page_count += 1

            if len(page) < page_limit:
                break
            offset += page_limit

        wrapped = [self._wrap_record("gamma_markets", market) for market in markets]
        self.writer.write("gamma_markets", wrapped)
        return markets

    def discover_events(
        self,
        *,
        limit: int = 100,
        active: bool = True,
        closed: bool = False,
        archived: bool = False,
    ) -> tuple[list[dict[str, Any]], Path | None]:
        response = self.session.get(
            f"{GAMMA_API}/events",
            params={
                "limit": limit,
                "active": str(active).lower(),
                "closed": str(closed).lower(),
                "archived": str(archived).lower(),
            },
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        events = response.json()
        wrapped = [self._wrap_record("gamma_events", event) for event in events]
        path = self.writer.write("gamma_events", wrapped)
        return events, path

    def fetch_trades(
        self,
        *,
        condition_ids: list[str] | None = None,
        limit: int = 500,
        taker_only: bool = False,
    ) -> tuple[list[dict[str, Any]], Path | None]:
        market_batches = _chunk_joined_values(
            values=condition_ids or [],
            max_joined_chars=MAX_TRADE_MARKET_QUERY_CHARS,
        )
        if not market_batches:
            market_batches = [None]

        trades: list[dict[str, Any]] = []
        for market_batch in market_batches:
            params: dict[str, Any] = {"limit": limit, "takerOnly": str(taker_only).lower()}
            if market_batch:
                params["market"] = ",".join(market_batch)

            response = self.session.get(
                f"{DATA_API}/trades",
                params=params,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            trades.extend(response.json())

        wrapped = [self._wrap_record("data_trades", trade) for trade in trades]
        path = self.writer.write("data_trades", wrapped)
        return trades, path

    def fetch_books(self, *, token_ids: list[str]) -> tuple[list[dict[str, Any]], Path | None]:
        if self.config.clob_driver == "pyclob":
            books = self._fetch_books_pyclob(token_ids=token_ids)
            wrapped = [self._wrap_record("clob_books", book) for book in books]
            path = self.writer.write("clob_books", wrapped)
            return books, path

        payload = [{"token_id": token_id} for token_id in token_ids]
        response = self.session.post(
            f"{CLOB_API}/books",
            json=payload,
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        books = response.json()
        wrapped = [self._wrap_record("clob_books", book) for book in books]
        path = self.writer.write("clob_books", wrapped)
        return books, path

    def fetch_midpoints(self, *, token_ids: list[str]) -> tuple[dict[str, Any], Path | None]:
        payload = [{"token_id": token_id} for token_id in token_ids]
        response = self.session.post(
            f"{CLOB_API}/midpoints",
            json=payload,
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        midpoints = response.json()
        wrapped = [self._wrap_record("clob_midpoints", {"token_ids": token_ids, "result": midpoints})]
        path = self.writer.write("clob_midpoints", wrapped)
        return midpoints, path

    def fetch_spreads(self, *, token_ids: list[str]) -> tuple[dict[str, Any], Path | None]:
        payload = [{"token_id": token_id} for token_id in token_ids]
        response = self.session.post(
            f"{CLOB_API}/spreads",
            json=payload,
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        spreads = response.json()
        wrapped = [self._wrap_record("clob_spreads", {"token_ids": token_ids, "result": spreads})]
        path = self.writer.write("clob_spreads", wrapped)
        return spreads, path

    def fetch_batch_prices_history(
        self,
        *,
        token_ids: list[str],
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str = "1h",
        fidelity: int = 1,
    ) -> tuple[dict[str, Any], Path | None]:
        payload: dict[str, Any] = {
            "markets": token_ids,
            "interval": interval,
            "fidelity": fidelity,
        }
        if start_ts is not None:
            payload["start_ts"] = start_ts
        if end_ts is not None:
            payload["end_ts"] = end_ts

        response = self.session.post(
            f"{CLOB_API}/batch-prices-history",
            json=payload,
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        history = response.json()
        wrapped = [self._wrap_record("clob_batch_prices_history", {"request": payload, "result": history})]
        path = self.writer.write("clob_batch_prices_history", wrapped)
        return history, path

    def fetch_open_interest(
        self,
        *,
        condition_ids: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], Path | None]:
        params: dict[str, Any] = {}
        if condition_ids:
            params["market"] = condition_ids
        response = self.session.get(
            f"{DATA_API}/oi",
            params=params or None,
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        values = response.json()
        wrapped = [self._wrap_record("data_oi", item) for item in values]
        path = self.writer.write("data_oi", wrapped)
        return values, path

    def fetch_holders(
        self,
        *,
        condition_ids: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], Path | None]:
        params: dict[str, Any] = {}
        if condition_ids:
            params["market"] = condition_ids
        response = self.session.get(
            f"{DATA_API}/holders",
            params=params or None,
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        holders = response.json()
        wrapped = [self._wrap_record("data_holders", item) for item in holders]
        path = self.writer.write("data_holders", wrapped)
        return holders, path

    def _fetch_books_pyclob(self, *, token_ids: list[str]) -> list[dict[str, Any]]:
        if self._pyclob is None:
            raise RuntimeError(
                "clob_driver=pyclob requires py-clob-client. Install it with "
                "`python -m pip install py-clob-client`."
            )
        if not token_ids:
            return []

        book_params = [_build_book_param(token_id) for token_id in token_ids]
        books = self._pyclob.get_order_books(book_params)
        return [_to_plain_object(item) for item in books]

    def stream_market(
        self,
        *,
        asset_ids: list[str],
        duration_seconds: int = 60,
        custom_feature_enabled: bool = True,
        flush_every_messages: int = 50,
        flush_every_seconds: int = 5,
    ) -> int:
        if websocket is None:
            raise RuntimeError(
                "websocket-client is not installed. Install dependencies from requirements.txt first."
            )

        ws = websocket.create_connection(MARKET_WSS, timeout=self.config.timeout_seconds)
        try:
            ws.settimeout(min(self.config.timeout_seconds, 5))
            subscribe_message = {
                "assets_ids": asset_ids,
                "type": "market",
                "custom_feature_enabled": custom_feature_enabled,
            }
            ws.send(json.dumps(subscribe_message))

            start = time.monotonic()
            message_count = 0
            heartbeat_at = start
            last_flush_at = start
            pending_records: list[dict[str, Any]] = []

            while time.monotonic() - start < duration_seconds:
                if time.monotonic() - heartbeat_at >= 10:
                    ws.send("PING")
                    heartbeat_at = time.monotonic()

                try:
                    raw_message = ws.recv()
                except websocket.WebSocketTimeoutException:
                    raw_message = None

                if raw_message is not None:
                    message_count += 1
                    wrapped = self._wrap_ws_message("ws_market", raw_message)
                    pending_records.append(wrapped)

                should_flush = (
                    len(pending_records) >= flush_every_messages
                    or time.monotonic() - last_flush_at >= flush_every_seconds
                )
                if should_flush and pending_records:
                    self.writer.write("ws_market", pending_records)
                    pending_records = []
                    last_flush_at = time.monotonic()

            if pending_records:
                self.writer.write("ws_market", pending_records)
            return message_count
        finally:
            ws.close()

    @staticmethod
    def extract_condition_ids(markets: list[dict[str, Any]]) -> list[str]:
        values = [market.get("conditionId") for market in markets]
        return [value for value in values if isinstance(value, str) and value]

    @staticmethod
    def extract_asset_ids(markets: list[dict[str, Any]], *, max_assets: int | None = None) -> list[str]:
        asset_ids: list[str] = []
        for market in markets:
            token_ids = market.get("clobTokenIds")
            normalized = _normalize_token_ids(token_ids)
            asset_ids.extend(normalized)

        deduped = list(dict.fromkeys(asset_ids))
        if max_assets is not None:
            return deduped[:max_assets]
        return deduped

    def _wrap_record(self, source: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "source": source,
            "ts_ingest": datetime.now(UTC).isoformat(),
            "payload": payload,
        }

    def _wrap_ws_message(self, source: str, raw_message: str) -> dict[str, Any]:
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            payload = {"raw_message": raw_message}

        return {
            "source": source,
            "ts_ingest": datetime.now(UTC).isoformat(),
            "payload": payload,
        }


def _normalize_token_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(parsed, list):
            return [str(item) for item in parsed if item]
    return []


def _chunk_joined_values(*, values: list[str], max_joined_chars: int) -> list[list[str]]:
    if not values:
        return []

    chunks: list[list[str]] = []
    current: list[str] = []
    current_len = 0

    for value in values:
        item = str(value)
        item_len = len(item) if not current else len(item) + 1
        if current and current_len + item_len > max_joined_chars:
            chunks.append(current)
            current = [item]
            current_len = len(item)
            continue
        current.append(item)
        current_len += item_len

    if current:
        chunks.append(current)
    return chunks


def _build_pyclob_client(clob_driver: str):
    if clob_driver != "pyclob":
        return None

    client_cls = _PyClobClient or _PyClobClientAlt
    if client_cls is None:
        return None
    return client_cls(CLOB_API)


def _build_book_param(token_id: str):
    cls = _BookParams or _BookParamsAlt
    if cls is None:
        raise RuntimeError(
            "BookParams class not found. Ensure py-clob-client is installed correctly."
        )
    return cls(token_id=token_id)


def _safe_node_id(value: str) -> str:
    text = (value or "standalone").strip()
    if not text:
        return "standalone"
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in text)


def _parse_iso_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _to_plain_object(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _to_plain_object(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_plain_object(v) for v in value]

    for method_name in ("model_dump", "dict"):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                dumped = method()
                return _to_plain_object(dumped)
            except TypeError:
                pass

    if hasattr(value, "__dict__"):
        raw = {k: v for k, v in vars(value).items() if not k.startswith("_")}
        if raw:
            return _to_plain_object(raw)

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
