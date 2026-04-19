from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from polymarket_collector.dedup import _extract_record_key


@dataclass(slots=True)
class ParquetBuildStats:
    sources: list[str]
    files_scanned: int = 0
    files_written: int = 0
    files_skipped: int = 0
    records_read: int = 0
    records_written: int = 0
    records_dropped_as_duplicates: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "sources": self.sources,
            "files_scanned": self.files_scanned,
            "files_written": self.files_written,
            "files_skipped": self.files_skipped,
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_dropped_as_duplicates": self.records_dropped_as_duplicates,
        }


def build_parquet_from_raw(
    *,
    data_root: Path,
    sources: list[str],
    start: datetime | None = None,
    end: datetime | None = None,
    overwrite: bool = False,
    dedup: bool = True,
) -> ParquetBuildStats:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError(
            "build-parquet requires pyarrow. Install with: python -m pip install -e '.[parquet]'"
        ) from exc

    normalized_sources = _resolve_sources(data_root=data_root, sources=sources)
    stats = ParquetBuildStats(sources=normalized_sources)

    for source in normalized_sources:
        for raw_path in _iter_source_raw_files(data_root=data_root, source=source, start=start, end=end):
            stats.files_scanned += 1
            target_path = _target_parquet_path(data_root=data_root, raw_path=raw_path)
            if target_path.exists() and not overwrite:
                stats.files_skipped += 1
                continue

            target_path.parent.mkdir(parents=True, exist_ok=True)
            rows, dropped = _read_rows_for_parquet(raw_path=raw_path, source=source, dedup=dedup)
            stats.records_read += len(rows) + dropped
            stats.records_written += len(rows)
            stats.records_dropped_as_duplicates += dropped

            table = pa.Table.from_pylist(rows) if rows else pa.table({"source": [], "ts_ingest": [], "dedup_key": [], "payload_json": []})
            pq.write_table(table, target_path, compression="zstd")
            stats.files_written += 1

    return stats


def build_normalized_parquet_from_raw(
    *,
    data_root: Path,
    sources: list[str],
    start: datetime | None = None,
    end: datetime | None = None,
    overwrite: bool = False,
    dedup: bool = True,
) -> ParquetBuildStats:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError(
            "build-parquet-normalized requires pyarrow. Install with: python -m pip install -e '.[parquet]'"
        ) from exc

    normalized_sources = _resolve_normalized_sources(data_root=data_root, sources=sources)
    stats = ParquetBuildStats(sources=normalized_sources)

    for source in normalized_sources:
        for raw_path in _iter_source_raw_files(data_root=data_root, source=source, start=start, end=end):
            stats.files_scanned += 1
            target_path = _target_normalized_parquet_path(data_root=data_root, raw_path=raw_path)
            if target_path.exists() and not overwrite:
                stats.files_skipped += 1
                continue

            target_path.parent.mkdir(parents=True, exist_ok=True)
            rows, dropped = _read_rows_for_normalized_parquet(raw_path=raw_path, source=source, dedup=dedup)
            stats.records_read += len(rows) + dropped
            stats.records_written += len(rows)
            stats.records_dropped_as_duplicates += dropped

            table = pa.Table.from_pylist(rows) if rows else pa.table(_empty_normalized_columns(source))
            pq.write_table(table, target_path, compression="zstd", use_dictionary=True)
            stats.files_written += 1

    return stats


def _resolve_sources(*, data_root: Path, sources: list[str]) -> list[str]:
    normalized = [item.strip() for item in sources if item.strip()]
    if not normalized:
        return []
    if any(item == "all" for item in normalized):
        raw_root = data_root / "raw"
        if not raw_root.exists():
            return []
        return sorted(
            part.split("=", 1)[1]
            for part in (path.name for path in raw_root.iterdir() if path.is_dir() and path.name.startswith("source="))
        )
    return sorted(set(normalized))


def _resolve_normalized_sources(*, data_root: Path, sources: list[str]) -> list[str]:
    supported = {"data_trades", "ws_market"}
    resolved = _resolve_sources(data_root=data_root, sources=sources)
    return [source for source in resolved if source in supported]


def _iter_source_raw_files(
    *,
    data_root: Path,
    source: str,
    start: datetime | None,
    end: datetime | None,
):
    base = data_root / "raw" / f"source={source}"
    if not base.exists():
        return

    start_utc = start.astimezone(UTC) if start else None
    end_utc = end.astimezone(UTC) if end else None

    for path in sorted(base.rglob("*.jsonl.gz")):
        hour = _extract_partition_hour(path)
        if hour is None:
            continue
        if start_utc and hour < start_utc:
            continue
        if end_utc and hour > end_utc:
            continue
        yield path


def _target_parquet_path(*, data_root: Path, raw_path: Path) -> Path:
    rel = raw_path.relative_to(data_root / "raw")
    return (data_root / "warehouse" / rel).with_suffix("").with_suffix(".parquet")


def _target_normalized_parquet_path(*, data_root: Path, raw_path: Path) -> Path:
    rel = raw_path.relative_to(data_root / "raw")
    return (data_root / "warehouse_normalized" / rel).with_suffix("").with_suffix(".parquet")


def _read_rows_for_parquet(*, raw_path: Path, source: str, dedup: bool) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    dropped = 0

    with gzip.open(raw_path, "rt", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError:
                continue

            payload = record.get("payload")
            key = _extract_record_key(source=source, payload=payload)
            if dedup and key in seen:
                dropped += 1
                continue
            seen.add(key)
            rows.append(
                {
                    "source": record.get("source", source),
                    "ts_ingest": record.get("ts_ingest", ""),
                    "dedup_key": key,
                    "payload_json": json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
                }
            )

    return rows, dropped


def _read_rows_for_normalized_parquet(
    *,
    raw_path: Path,
    source: str,
    dedup: bool,
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    dropped = 0

    with gzip.open(raw_path, "rt", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError:
                continue

            if source == "data_trades":
                normalized = _normalize_data_trade_record(record)
            elif source == "ws_market":
                normalized = _normalize_ws_market_record(record)
            else:
                normalized = []

            for row in normalized:
                key = row.pop("_dedup_key")
                if dedup and key in seen:
                    dropped += 1
                    continue
                seen.add(key)
                rows.append(row)

    return rows, dropped


def _normalize_data_trade_record(record: dict[str, Any]) -> list[dict[str, Any]]:
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return []

    return [
        {
            "_dedup_key": _extract_record_key(source="data_trades", payload=payload),
            "source": record.get("source", "data_trades"),
            "ts_ingest": str(record.get("ts_ingest", "")),
            "timestamp": _as_int(payload.get("timestamp")),
            "condition_id": _as_str(payload.get("conditionId")),
            "asset_id": _as_str(payload.get("asset")),
            "side": _as_str(payload.get("side")),
            "price": _as_float(payload.get("price")),
            "size": _as_float(payload.get("size")),
            "transaction_hash": _as_str(payload.get("transactionHash")),
            "trade_id": _as_str(payload.get("id") or payload.get("trade_id")),
            "maker_address": _as_str(payload.get("maker")),
            "taker_address": _as_str(payload.get("taker") or payload.get("proxyWallet")),
        }
    ]


def _normalize_ws_market_record(record: dict[str, Any]) -> list[dict[str, Any]]:
    payload = record.get("payload")
    events = _iter_ws_events(payload)
    if not events:
        return []

    normalized_rows: list[dict[str, Any]] = []
    for event in events:
        event_type = _as_str(event.get("event_type"))
        condition_id = _as_str(event.get("condition_id") or event.get("market"))
        event_ts = _as_int(event.get("timestamp"))
        side = _as_str(event.get("side"))
        base_asset_ids = _extract_event_asset_ids(event)
        price_changes = event.get("price_changes")

        if isinstance(price_changes, list) and price_changes:
            for change in price_changes:
                if not isinstance(change, dict):
                    continue
                asset_id = _as_str(change.get("asset_id")) or (base_asset_ids[0] if base_asset_ids else "")
                normalized_rows.append(
                    {
                        "_dedup_key": _extract_record_key(source="ws_market", payload=change)
                        + "|"
                        + _as_str(event_ts)
                        + "|"
                        + condition_id
                        + "|"
                        + event_type,
                        "source": record.get("source", "ws_market"),
                        "ts_ingest": str(record.get("ts_ingest", "")),
                        "event_type": event_type,
                        "condition_id": condition_id,
                        "asset_id": asset_id,
                        "side": side,
                        "timestamp": event_ts,
                        "price": _as_float(change.get("price")),
                        "size": _as_float(change.get("size")),
                        "best_bid": _as_float(change.get("best_bid")),
                        "best_ask": _as_float(change.get("best_ask")),
                    }
                )
            continue

        fallback_assets = base_asset_ids if base_asset_ids else [""]
        for asset_id in fallback_assets:
            normalized_rows.append(
                {
                    "_dedup_key": _extract_record_key(source="ws_market", payload=event) + "|" + asset_id,
                    "source": record.get("source", "ws_market"),
                    "ts_ingest": str(record.get("ts_ingest", "")),
                    "event_type": event_type,
                    "condition_id": condition_id,
                    "asset_id": asset_id,
                    "side": side,
                    "timestamp": event_ts,
                    "price": _as_float(event.get("price")),
                    "size": _as_float(event.get("size")),
                    "best_bid": _as_float(event.get("best_bid")),
                    "best_ask": _as_float(event.get("best_ask")),
                }
            )

    return normalized_rows


def _iter_ws_events(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _extract_event_asset_ids(event: dict[str, Any]) -> list[str]:
    direct = event.get("assets_ids")
    if isinstance(direct, list):
        values = [_as_str(item) for item in direct]
        return [item for item in values if item]

    asset_id = _as_str(event.get("asset_id"))
    if asset_id:
        return [asset_id]
    return []


def _empty_normalized_columns(source: str) -> dict[str, list[Any]]:
    if source == "data_trades":
        return {
            "source": [],
            "ts_ingest": [],
            "timestamp": [],
            "condition_id": [],
            "asset_id": [],
            "side": [],
            "price": [],
            "size": [],
            "transaction_hash": [],
            "trade_id": [],
            "maker_address": [],
            "taker_address": [],
        }
    return {
        "source": [],
        "ts_ingest": [],
        "event_type": [],
        "condition_id": [],
        "asset_id": [],
        "side": [],
        "timestamp": [],
        "price": [],
        "size": [],
        "best_bid": [],
        "best_ask": [],
    }


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _extract_partition_hour(path: Path) -> datetime | None:
    try:
        dt_part = next(part for part in path.parts if part.startswith("dt="))
        hour_part = next(part for part in path.parts if part.startswith("hour="))
        dt_text = dt_part.split("=", 1)[1]
        hour_text = hour_part.split("=", 1)[1]
        parsed = datetime.strptime(f"{dt_text} {hour_text}", "%Y-%m-%d %H")
        return parsed.replace(tzinfo=UTC)
    except (StopIteration, ValueError):
        return None
