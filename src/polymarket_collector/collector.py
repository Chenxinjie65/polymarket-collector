from __future__ import annotations

import gzip
import json
import threading
import time
from collections.abc import Callable
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
MAX_TRADE_PAGE_LIMIT = 500
# The public trades API becomes unreliable at higher offsets; keep a safer default.
MAX_TRADE_PAGE_OFFSET = 3000
MAX_DATA_MARKETS_BATCH_SIZE = 100
MAX_CLOB_BOOKS_BATCH_SIZE = 100
MAX_CLOB_MIDPOINTS_BATCH_SIZE = 200
MAX_CLOB_SPREADS_BATCH_SIZE = 200
# /batch-prices-history currently accepts at most 20 market ids per request.
MAX_CLOB_BATCH_HISTORY_BATCH_SIZE = 20


@dataclass(slots=True)
class CollectorConfig:
    data_root: Path
    timeout_seconds: float = 20.0
    user_agent: str = "polymarket-collector-mvp/0.1"
    clob_driver: str = "raw"  # supported: raw, pyclob
    bucket_seconds: int = 3600
    writer_node_id: str = "standalone"
    http_max_retries: int = 5
    http_backoff_base_seconds: float = 0.5
    http_max_backoff_seconds: float = 8.0
    raw_sources: tuple[str, ...] | None = None
    write_gamma_markets_raw: bool = False
    write_gamma_events_raw: bool = False


class JsonlGzWriter:
    _global_write_lock = threading.Lock()

    def __init__(
        self,
        root: Path,
        *,
        bucket_seconds: int,
        node_id: str,
        raw_sources: tuple[str, ...] | None = None,
    ) -> None:
        self.root = root
        self.bucket_seconds = max(1, int(bucket_seconds))
        self.node_id = _safe_node_id(node_id)
        self.raw_sources = set(raw_sources) if raw_sources else None

    def write(self, source: str, records: list[dict[str, Any]]) -> Path | None:
        if self.raw_sources is not None and source not in self.raw_sources:
            return None
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

        with self._global_write_lock:
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
            raw_sources=config.raw_sources,
        )
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.user_agent})
        self._pyclob = _build_pyclob_client(config.clob_driver)

    def _request_with_retries(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json_payload: dict[str, Any] | list[dict[str, Any]] | None = None,
    ) -> requests.Response:
        retries = max(0, int(self.config.http_max_retries))
        max_attempts = retries + 1
        transient_statuses = {429, 500, 502, 503, 504}

        for attempt in range(max_attempts):
            try:
                if method == "GET":
                    response = self.session.get(
                        url,
                        params=params,
                        timeout=self.config.timeout_seconds,
                    )
                elif method == "POST":
                    response = self.session.post(
                        url,
                        json=json_payload,
                        timeout=self.config.timeout_seconds,
                    )
                else:  # pragma: no cover - defensive branch
                    raise ValueError(f"unsupported method: {method}")
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt >= retries:
                    raise
                self._sleep_before_retry(attempt=attempt, response=None)
                continue

            if response.status_code in transient_statuses and attempt < retries:
                self._sleep_before_retry(attempt=attempt, response=response)
                continue
            return response

        raise RuntimeError("unreachable retry loop state")

    def _sleep_before_retry(self, *, attempt: int, response: requests.Response | None) -> None:
        retry_after = _parse_retry_after_seconds(response)
        if retry_after is not None:
            time.sleep(min(retry_after, max(0.0, self.config.http_max_backoff_seconds)))
            return

        base = max(0.01, float(self.config.http_backoff_base_seconds))
        cap = max(base, float(self.config.http_max_backoff_seconds))
        backoff = min(cap, base * (2**attempt))
        time.sleep(backoff)

    def discover_markets(
        self,
        *,
        limit: int = 100,
        active: bool = True,
        closed: bool = False,
        archived: bool = False,
    ) -> tuple[list[dict[str, Any]], Path | None]:
        response = self._request_with_retries(
            method="GET",
            url=f"{GAMMA_API}/markets",
            params={
                "limit": limit,
                "active": str(active).lower(),
                "closed": str(closed).lower(),
                "archived": str(archived).lower(),
            },
        )
        response.raise_for_status()
        markets = response.json()
        path = None
        if self.config.write_gamma_markets_raw:
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
            response = self._request_with_retries(
                method="GET",
                url=f"{GAMMA_API}/markets",
                params={
                    "limit": page_limit,
                    "offset": offset,
                    "active": str(active).lower(),
                    "closed": str(closed).lower(),
                    "archived": str(archived).lower(),
                },
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

        if self.config.write_gamma_markets_raw:
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
        response = self._request_with_retries(
            method="GET",
            url=f"{GAMMA_API}/events",
            params={
                "limit": limit,
                "active": str(active).lower(),
                "closed": str(closed).lower(),
                "archived": str(archived).lower(),
            },
        )
        response.raise_for_status()
        events = response.json()
        path = None
        if self.config.write_gamma_events_raw:
            wrapped = [self._wrap_record("gamma_events", event) for event in events]
            path = self.writer.write("gamma_events", wrapped)
        return events, path

    def discover_events_all_pages(
        self,
        *,
        page_limit: int = 500,
        active: bool = True,
        closed: bool = False,
        archived: bool = False,
        max_pages: int = 200,
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        offset = 0
        seen_ids: set[str] = set()
        page_count = 0

        while page_count < max_pages:
            response = self._request_with_retries(
                method="GET",
                url=f"{GAMMA_API}/events",
                params={
                    "limit": page_limit,
                    "offset": offset,
                    "active": str(active).lower(),
                    "closed": str(closed).lower(),
                    "archived": str(archived).lower(),
                },
            )
            response.raise_for_status()
            page = response.json()
            if not page:
                break

            dedup_page = []
            for item in page:
                event_id = item.get("id")
                key = str(event_id) if event_id is not None else json.dumps(item, sort_keys=True)
                if key in seen_ids:
                    continue
                seen_ids.add(key)
                dedup_page.append(item)

            events.extend(dedup_page)
            page_count += 1

            if len(page) < page_limit:
                break
            offset += page_limit

        if self.config.write_gamma_events_raw:
            wrapped = [self._wrap_record("gamma_events", event) for event in events]
            self.writer.write("gamma_events", wrapped)
        return events

    def fetch_trades(
        self,
        *,
        condition_ids: list[str] | None = None,
        limit: int = MAX_TRADE_PAGE_LIMIT,
        taker_only: bool = False,
    ) -> tuple[list[dict[str, Any]], Path | None]:
        trades = self.fetch_trades_page(
            condition_ids=condition_ids,
            limit=limit,
            offset=0,
            taker_only=taker_only,
        )
        wrapped = [self._wrap_record("data_trades", trade) for trade in trades]
        path = self.writer.write("data_trades", wrapped)
        return trades, path

    def fetch_trades_page(
        self,
        *,
        condition_ids: list[str] | None = None,
        limit: int = MAX_TRADE_PAGE_LIMIT,
        offset: int = 0,
        taker_only: bool = False,
    ) -> list[dict[str, Any]]:
        market_batches = _chunk_joined_values(
            values=condition_ids or [],
            max_joined_chars=MAX_TRADE_MARKET_QUERY_CHARS,
        )
        if not market_batches:
            market_batches = [None]

        trades: list[dict[str, Any]] = []
        for market_batch in market_batches:
            params: dict[str, Any] = {
                "limit": limit,
                "offset": offset,
                "takerOnly": str(taker_only).lower(),
            }
            if market_batch:
                params["market"] = ",".join(market_batch)

            response = self._request_with_retries(
                method="GET",
                url=f"{DATA_API}/trades",
                params=params,
            )
            response.raise_for_status()
            trades.extend(response.json())

        return trades

    def fetch_trades_incremental(
        self,
        *,
        condition_ids: list[str],
        frontier_by_condition: dict[str, Any] | None = None,
        page_limit: int = MAX_TRADE_PAGE_LIMIT,
        max_offset: int = MAX_TRADE_PAGE_OFFSET,
        taker_only: bool = False,
    ) -> tuple[list[dict[str, Any]], Path | None, dict[str, Any], bool]:
        frontier = _normalize_trade_frontier_state(frontier_by_condition)
        page_size = min(max(1, page_limit), MAX_TRADE_PAGE_LIMIT)
        max_page_offset = max(0, min(max_offset, MAX_TRADE_PAGE_OFFSET))
        trades: list[dict[str, Any]] = []
        hit_offset_cap = False

        market_batches = _chunk_joined_values(
            values=condition_ids,
            max_joined_chars=MAX_TRADE_MARKET_QUERY_CHARS,
        )
        if not market_batches:
            return [], None, frontier, False

        for market_batch in market_batches:
            offset = 0
            while True:
                page = self.fetch_trades_page(
                    condition_ids=market_batch,
                    limit=page_size,
                    offset=offset,
                    taker_only=taker_only,
                )
                if not page:
                    break

                new_page_trades = [
                    trade for trade in page if _is_new_trade_record(trade=trade, frontier=frontier)
                ]
                if new_page_trades:
                    trades.extend(new_page_trades)
                    frontier = _update_trade_frontier(frontier=frontier, trades=new_page_trades)

                if len(page) < page_size:
                    break
                if not new_page_trades:
                    break

                next_offset = offset + page_size
                if next_offset > max_page_offset:
                    hit_offset_cap = True
                    break
                offset = next_offset

        wrapped = [self._wrap_record("data_trades", trade) for trade in trades]
        path = self.writer.write("data_trades", wrapped)
        return trades, path, frontier, hit_offset_cap

    def fetch_books(self, *, token_ids: list[str]) -> tuple[list[dict[str, Any]], Path | None]:
        books: list[dict[str, Any]] = []
        for token_batch in _chunk_values(values=token_ids, max_batch_size=MAX_CLOB_BOOKS_BATCH_SIZE):
            if self.config.clob_driver == "pyclob":
                books.extend(self._fetch_books_pyclob(token_ids=token_batch))
                continue

            payload = [{"token_id": token_id} for token_id in token_batch]
            response = self._request_with_retries(
                method="POST",
                url=f"{CLOB_API}/books",
                json_payload=payload,
            )
            response.raise_for_status()
            books.extend(response.json())

        wrapped = [self._wrap_record("clob_books", book) for book in books]
        path = self.writer.write("clob_books", wrapped)
        return books, path

    def fetch_midpoints(self, *, token_ids: list[str]) -> tuple[dict[str, Any], Path | None]:
        midpoints: dict[str, Any] = {}
        for token_batch in _chunk_values(values=token_ids, max_batch_size=MAX_CLOB_MIDPOINTS_BATCH_SIZE):
            payload = [{"token_id": token_id} for token_id in token_batch]
            response = self._request_with_retries(
                method="POST",
                url=f"{CLOB_API}/midpoints",
                json_payload=payload,
            )
            response.raise_for_status()
            midpoints.update(response.json())

        wrapped = [self._wrap_record("clob_midpoints", {"token_ids": token_ids, "result": midpoints})]
        path = self.writer.write("clob_midpoints", wrapped)
        return midpoints, path

    def fetch_spreads(self, *, token_ids: list[str]) -> tuple[dict[str, Any], Path | None]:
        spreads: dict[str, Any] = {}
        for token_batch in _chunk_values(values=token_ids, max_batch_size=MAX_CLOB_SPREADS_BATCH_SIZE):
            payload = [{"token_id": token_id} for token_id in token_batch]
            response = self._request_with_retries(
                method="POST",
                url=f"{CLOB_API}/spreads",
                json_payload=payload,
            )
            response.raise_for_status()
            spreads.update(response.json())

        wrapped = [self._wrap_record("clob_spreads", {"token_ids": token_ids, "result": spreads})]
        path = self.writer.write("clob_spreads", wrapped)
        return spreads, path

    def fetch_batch_prices_history(
        self,
        *,
        token_ids: list[str],
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str | None = "1h",
        fidelity: int = 1,
    ) -> tuple[dict[str, Any], Path | None]:
        history: dict[str, Any] = {}
        for token_batch in _chunk_values(values=token_ids, max_batch_size=MAX_CLOB_BATCH_HISTORY_BATCH_SIZE):
            batch_result = self._fetch_batch_prices_history_chunk(
                token_ids=token_batch,
                start_ts=start_ts,
                end_ts=end_ts,
                interval=interval,
                fidelity=fidelity,
            )
            history = _merge_dict_results(history, batch_result)

        payload = _build_batch_prices_history_payload(
            token_ids=token_ids,
            start_ts=start_ts,
            end_ts=end_ts,
            interval=interval,
            fidelity=fidelity,
        )
        wrapped = [self._wrap_record("clob_batch_prices_history", {"request": payload, "result": history})]
        path = self.writer.write("clob_batch_prices_history", wrapped)
        return history, path

    def _fetch_batch_prices_history_chunk(
        self,
        *,
        token_ids: list[str],
        start_ts: int | None,
        end_ts: int | None,
        interval: str | None,
        fidelity: int,
    ) -> dict[str, Any]:
        payload = _build_batch_prices_history_payload(
            token_ids=token_ids,
            start_ts=start_ts,
            end_ts=end_ts,
            interval=interval,
            fidelity=fidelity,
        )

        response = self._request_with_retries(
            method="POST",
            url=f"{CLOB_API}/batch-prices-history",
            json_payload=payload,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError:
            if _is_absolute_history_request(start_ts=start_ts, end_ts=end_ts) and "interval" in payload:
                retry_payload = dict(payload)
                retry_payload.pop("interval", None)
                retry_response = self._request_with_retries(
                    method="POST",
                    url=f"{CLOB_API}/batch-prices-history",
                    json_payload=retry_payload,
                )
                try:
                    retry_response.raise_for_status()
                except requests.HTTPError:
                    response = retry_response
                else:
                    response = retry_response
            if response.status_code < 400:
                payload = response.json()
                if isinstance(payload, dict) and isinstance(payload.get("history"), dict):
                    return payload["history"]
                return payload
            # If payload constraints change upstream, split the chunk recursively
            # so one bad batch does not fail the entire collection cycle.
            if response.status_code == 400 and len(token_ids) > 1:
                middle = len(token_ids) // 2
                left = self._fetch_batch_prices_history_chunk(
                    token_ids=token_ids[:middle],
                    start_ts=start_ts,
                    end_ts=end_ts,
                    interval=interval,
                    fidelity=fidelity,
                )
                right = self._fetch_batch_prices_history_chunk(
                    token_ids=token_ids[middle:],
                    start_ts=start_ts,
                    end_ts=end_ts,
                    interval=interval,
                    fidelity=fidelity,
                )
                return _merge_dict_results(left, right)
            if response.status_code == 400 and len(token_ids) == 1:
                # Some assets can be unsupported for this endpoint.
                # Skip single bad assets and keep partial results flowing.
                return {}
            raise
        payload = response.json()
        if isinstance(payload, dict) and isinstance(payload.get("history"), dict):
            return payload["history"]
        return payload

    def fetch_open_interest(
        self,
        *,
        condition_ids: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], Path | None]:
        market_batches = _chunk_values(values=condition_ids or [], max_batch_size=MAX_DATA_MARKETS_BATCH_SIZE)
        if not market_batches:
            market_batches = [None]

        values: list[dict[str, Any]] = []
        for market_batch in market_batches:
            params: dict[str, Any] = {}
            if market_batch:
                params["market"] = market_batch
            response = self._request_with_retries(
                method="GET",
                url=f"{DATA_API}/oi",
                params=params or None,
            )
            response.raise_for_status()
            values.extend(response.json())

        wrapped = [self._wrap_record("data_oi", item) for item in values]
        path = self.writer.write("data_oi", wrapped)
        return values, path

    def fetch_holders(
        self,
        *,
        condition_ids: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], Path | None]:
        market_batches = _chunk_values(values=condition_ids or [], max_batch_size=MAX_DATA_MARKETS_BATCH_SIZE)
        if not market_batches:
            market_batches = [None]

        holders: list[dict[str, Any]] = []
        for market_batch in market_batches:
            params: dict[str, Any] = {}
            if market_batch:
                params["market"] = market_batch
            response = self._request_with_retries(
                method="GET",
                url=f"{DATA_API}/holders",
                params=params or None,
            )
            response.raise_for_status()
            holders.extend(response.json())

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
        subscribe_batch_size: int = 500,
        on_new_market: Callable[[dict[str, Any]], None] | None = None,
        on_market_resolved: Callable[[dict[str, Any]], None] | None = None,
    ) -> int:
        if websocket is None:
            raise RuntimeError(
                "websocket-client is not installed. Install dependencies from requirements.txt first."
            )

        subscribed_asset_ids = set(asset_ids)
        ws = websocket.create_connection(MARKET_WSS, timeout=self.config.timeout_seconds)
        try:
            ws.settimeout(min(self.config.timeout_seconds, 5))
            for batch in _chunk_values(values=asset_ids, max_batch_size=max(1, subscribe_batch_size)):
                subscribe_message = {
                    "assets_ids": batch,
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
                    payload = wrapped["payload"]
                    for event in _iter_ws_market_events(payload):
                        event_type = event.get("event_type")
                        if event_type == "new_market":
                            new_assets = _extract_ws_assets_ids(event)
                            new_subscriptions = [asset_id for asset_id in new_assets if asset_id not in subscribed_asset_ids]
                            if new_subscriptions:
                                for batch in _chunk_values(
                                    values=new_subscriptions,
                                    max_batch_size=max(1, subscribe_batch_size),
                                ):
                                    ws.send(
                                        json.dumps(
                                            {
                                                "assets_ids": batch,
                                                "operation": "subscribe",
                                                "custom_feature_enabled": custom_feature_enabled,
                                            }
                                        )
                                    )
                                subscribed_asset_ids.update(new_subscriptions)
                            if on_new_market is not None:
                                on_new_market(event)
                        elif event_type == "market_resolved":
                            resolved_assets = _extract_ws_assets_ids(event)
                            unsubscribe_assets = [asset_id for asset_id in resolved_assets if asset_id in subscribed_asset_ids]
                            if unsubscribe_assets:
                                for batch in _chunk_values(
                                    values=unsubscribe_assets,
                                    max_batch_size=max(1, subscribe_batch_size),
                                ):
                                    ws.send(
                                        json.dumps(
                                            {
                                                "assets_ids": batch,
                                                "operation": "unsubscribe",
                                            }
                                        )
                                    )
                                subscribed_asset_ids.difference_update(unsubscribe_assets)
                            if on_market_resolved is not None:
                                on_market_resolved(event)

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
        if max_assets is not None and max_assets > 0:
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


def _iter_ws_market_events(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _extract_ws_assets_ids(event: dict[str, Any]) -> list[str]:
    direct = event.get("assets_ids")
    if isinstance(direct, list):
        return [str(item) for item in direct if item]

    asset_id = event.get("asset_id")
    if isinstance(asset_id, str) and asset_id:
        return [asset_id]

    changes = event.get("price_changes")
    if isinstance(changes, list):
        values = []
        for item in changes:
            if not isinstance(item, dict):
                continue
            change_asset_id = item.get("asset_id")
            if isinstance(change_asset_id, str) and change_asset_id:
                values.append(change_asset_id)
        return list(dict.fromkeys(values))
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


def _chunk_values(*, values: list[str], max_batch_size: int) -> list[list[str]]:
    if not values:
        return []
    size = max(1, max_batch_size)
    return [values[index : index + size] for index in range(0, len(values), size)]


def _normalize_trade_frontier_state(frontier_by_condition: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for condition_id, raw in (frontier_by_condition or {}).items():
        if not isinstance(condition_id, str) or not condition_id:
            continue
        if not isinstance(raw, dict):
            continue
        max_timestamp = raw.get("max_timestamp")
        if not isinstance(max_timestamp, int):
            continue
        keys_at_max_timestamp = raw.get("keys_at_max_timestamp") or []
        normalized[condition_id] = {
            "max_timestamp": max_timestamp,
            "keys_at_max_timestamp": sorted(
                str(key)
                for key in keys_at_max_timestamp
                if isinstance(key, str) and key
            ),
        }
    return normalized


def _is_new_trade_record(*, trade: dict[str, Any], frontier: dict[str, dict[str, Any]]) -> bool:
    condition_id = trade.get("conditionId")
    if not isinstance(condition_id, str) or not condition_id:
        return True

    timestamp = trade.get("timestamp")
    if not isinstance(timestamp, int):
        return True

    state = frontier.get(condition_id)
    if state is None:
        return True
    if timestamp > state["max_timestamp"]:
        return True
    if timestamp < state["max_timestamp"]:
        return False
    return _trade_record_key(trade) not in set(state["keys_at_max_timestamp"])


def _update_trade_frontier(
    *,
    frontier: dict[str, dict[str, Any]],
    trades: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    updated = {
        condition_id: {
            "max_timestamp": int(state["max_timestamp"]),
            "keys_at_max_timestamp": list(state["keys_at_max_timestamp"]),
        }
        for condition_id, state in frontier.items()
    }

    grouped: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        condition_id = trade.get("conditionId")
        timestamp = trade.get("timestamp")
        if not isinstance(condition_id, str) or not condition_id or not isinstance(timestamp, int):
            continue
        grouped.setdefault(condition_id, []).append(trade)

    for condition_id, condition_trades in grouped.items():
        max_timestamp = max(int(trade["timestamp"]) for trade in condition_trades)
        new_keys = {
            _trade_record_key(trade)
            for trade in condition_trades
            if int(trade["timestamp"]) == max_timestamp
        }

        existing = updated.get(condition_id)
        if existing is None or max_timestamp > existing["max_timestamp"]:
            updated[condition_id] = {
                "max_timestamp": max_timestamp,
                "keys_at_max_timestamp": sorted(new_keys),
            }
            continue
        if max_timestamp == existing["max_timestamp"]:
            merged_keys = set(existing["keys_at_max_timestamp"])
            merged_keys.update(new_keys)
            existing["keys_at_max_timestamp"] = sorted(merged_keys)

    return updated


def _trade_record_key(trade: dict[str, Any]) -> str:
    return "|".join(
        [
            str(trade.get("transactionHash", "")),
            str(trade.get("asset", "")),
            str(trade.get("conditionId", "")),
            str(trade.get("side", "")),
            str(trade.get("timestamp", "")),
            str(trade.get("price", "")),
            str(trade.get("size", "")),
        ]
    )


def _build_batch_prices_history_payload(
    *,
    token_ids: list[str],
    start_ts: int | None,
    end_ts: int | None,
    interval: str | None,
    fidelity: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "markets": token_ids,
        "fidelity": fidelity,
    }
    if start_ts is not None:
        payload["start_ts"] = start_ts
    if end_ts is not None:
        payload["end_ts"] = end_ts

    normalized_interval = _normalize_batch_prices_history_interval(
        interval=interval,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    if normalized_interval is not None:
        payload["interval"] = normalized_interval
    return payload


def _normalize_batch_prices_history_interval(
    *,
    interval: str | None,
    start_ts: int | None,
    end_ts: int | None,
) -> str | None:
    text = (interval or "").strip()
    if _is_absolute_history_request(start_ts=start_ts, end_ts=end_ts):
        # Polymarket treats interval values such as 1m/1h/1d as window selectors.
        # When callers provide an explicit timestamp range, use the full-range mode
        # instead of mixing relative intervals with absolute bounds.
        return "all"
    return text or None


def _is_absolute_history_request(*, start_ts: int | None, end_ts: int | None) -> bool:
    return start_ts is not None or end_ts is not None


def _merge_dict_results(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    if not left:
        return dict(right)

    merged = dict(left)
    for key, value in right.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            nested = dict(merged[key])
            nested.update(value)
            merged[key] = nested
            continue
        merged[key] = value
    return merged


def _parse_retry_after_seconds(response: requests.Response | None) -> float | None:
    if response is None:
        return None
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        value = float(raw.strip())
    except ValueError:
        return None
    if value < 0:
        return None
    return value


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
