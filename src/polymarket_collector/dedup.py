from __future__ import annotations

import gzip
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class DedupReport:
    source: str
    total_records: int
    unique_records: int
    duplicate_records: int
    duplicate_ratio: float
    files_scanned: int
    start: str
    end: str
    top_duplicate_keys: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "total_records": self.total_records,
            "unique_records": self.unique_records,
            "duplicate_records": self.duplicate_records,
            "duplicate_ratio": self.duplicate_ratio,
            "files_scanned": self.files_scanned,
            "start": self.start,
            "end": self.end,
            "top_duplicate_keys": self.top_duplicate_keys,
        }


def build_dedup_report(
    *,
    data_root: Path,
    source: str,
    start: datetime,
    end: datetime,
    top_n: int = 20,
) -> DedupReport:
    files = list(iter_raw_files(data_root=data_root, source=source, start=start, end=end))
    key_counter: Counter[str] = Counter()

    for path in files:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                payload = record.get("payload", {})
                key = _extract_record_key(source=source, payload=payload)
                key_counter[key] += 1

    total = sum(key_counter.values())
    unique = len(key_counter)
    duplicates = total - unique
    ratio = (duplicates / total) if total else 0.0
    top_duplicates = [
        {"key": key, "count": count}
        for key, count in key_counter.most_common()
        if count > 1
    ][:top_n]

    return DedupReport(
        source=source,
        total_records=total,
        unique_records=unique,
        duplicate_records=duplicates,
        duplicate_ratio=round(ratio, 8),
        files_scanned=len(files),
        start=start.astimezone(UTC).isoformat(),
        end=end.astimezone(UTC).isoformat(),
        top_duplicate_keys=top_duplicates,
    )


def iter_raw_files(
    *,
    data_root: Path,
    source: str,
    start: datetime,
    end: datetime,
):
    base = data_root / "raw" / f"source={source}"
    if not base.exists():
        return

    start = start.astimezone(UTC)
    end = end.astimezone(UTC)
    for path in sorted(base.rglob("*.jsonl.gz")):
        hour = _extract_partition_hour(path)
        if hour is None:
            continue
        if start <= hour <= end:
            yield path


def _extract_partition_hour(path: Path) -> datetime | None:
    # .../source=.../dt=YYYY-MM-DD/hour=HH/file.jsonl.gz
    try:
        dt_part = next(part for part in path.parts if part.startswith("dt="))
        hour_part = next(part for part in path.parts if part.startswith("hour="))
        dt_text = dt_part.split("=", 1)[1]
        hour_text = hour_part.split("=", 1)[1]
        parsed = datetime.strptime(f"{dt_text} {hour_text}", "%Y-%m-%d %H")
        return parsed.replace(tzinfo=UTC)
    except (StopIteration, ValueError):
        return None


def _extract_record_key(*, source: str, payload: Any) -> str:
    if not isinstance(payload, dict):
        return _hash_payload(payload)

    if source == "data_trades":
        for key in ("id", "trade_id", "transactionHash"):
            value = payload.get(key)
            if value:
                return f"trade:{value}"
        fallback = (
            payload.get("timestamp"),
            payload.get("proxyWallet"),
            payload.get("asset"),
            payload.get("price"),
            payload.get("size"),
            payload.get("side"),
        )
        return "trade_fallback:" + "|".join(_safe_part(v) for v in fallback)

    if source == "clob_books":
        if payload.get("hash"):
            return f"book_hash:{payload['hash']}"
        fallback = (payload.get("asset_id"), payload.get("timestamp"))
        return "book_fallback:" + "|".join(_safe_part(v) for v in fallback)

    if source == "gamma_markets":
        fallback = (payload.get("id"), payload.get("updatedAt"), payload.get("endDate"))
        return "market_fallback:" + "|".join(_safe_part(v) for v in fallback)

    if source == "ws_market":
        if payload.get("event_type"):
            fallback = (
                payload.get("event_type"),
                payload.get("market") or payload.get("condition_id"),
                payload.get("asset_id"),
                payload.get("timestamp"),
                payload.get("price"),
                payload.get("size"),
                payload.get("best_bid"),
                payload.get("best_ask"),
            )
            return "ws_event:" + "|".join(_safe_part(v) for v in fallback)
        return "ws_payload:" + _hash_payload(payload)

    return f"{source}:{_hash_payload(payload)}"


def _hash_payload(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _safe_part(value: Any) -> str:
    return "" if value is None else str(value)
