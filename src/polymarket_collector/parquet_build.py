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
