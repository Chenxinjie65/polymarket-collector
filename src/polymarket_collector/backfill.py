from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class BackfillStats:
    files_copied: int = 0
    files_skipped_existing: int = 0
    files_missing_on_source: int = 0
    bytes_copied: int = 0
    partitions_scanned: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "files_copied": self.files_copied,
            "files_skipped_existing": self.files_skipped_existing,
            "files_missing_on_source": self.files_missing_on_source,
            "bytes_copied": self.bytes_copied,
            "partitions_scanned": self.partitions_scanned,
        }


def backfill_raw_partitions(
    *,
    source_root: Path,
    target_root: Path,
    start: datetime,
    end: datetime,
    sources: list[str],
    dry_run: bool = False,
) -> BackfillStats:
    if end < start:
        raise ValueError("end must be greater than or equal to start")

    stats = BackfillStats()
    cursor = _to_hour_floor(start)
    end_hour = _to_hour_floor(end)

    while cursor <= end_hour:
        for source in sources:
            stats.partitions_scanned += 1
            src_dirs = _partition_dirs(source_root, source, cursor)
            if not src_dirs:
                stats.files_missing_on_source += 1
                continue

            found_files = False
            for src_dir in src_dirs:
                files = sorted(src_dir.glob("*.jsonl.gz"))
                if not files:
                    continue
                found_files = True
                dst_dir = _target_partition_dir(
                    source_root=source_root,
                    target_root=target_root,
                    src_dir=src_dir,
                )
                if not dry_run:
                    dst_dir.mkdir(parents=True, exist_ok=True)

                for src_file in files:
                    dst_file = dst_dir / src_file.name
                    if dst_file.exists():
                        stats.files_skipped_existing += 1
                        continue
                    if dry_run:
                        stats.files_copied += 1
                        stats.bytes_copied += src_file.stat().st_size
                        continue
                    shutil.copy2(src_file, dst_file)
                    stats.files_copied += 1
                    stats.bytes_copied += dst_file.stat().st_size

            if not found_files:
                stats.files_missing_on_source += 1

        cursor += timedelta(hours=1)

    return stats


def _partition_dir(root: Path, source: str, hour: datetime) -> Path:
    return (
        root
        / "raw"
        / f"source={source}"
        / f"dt={hour:%Y-%m-%d}"
        / f"hour={hour:%H}"
    )


def _partition_dirs(root: Path, source: str, hour: datetime) -> list[Path]:
    base = root / "raw" / f"source={source}"
    if not base.exists():
        return []
    if source != "ws_market":
        path = _partition_dir(root, source, hour)
        return [path] if path.exists() else []
    pattern = f"**/dt={hour:%Y-%m-%d}/hour={hour:%H}"
    return sorted(path for path in base.glob(pattern) if path.is_dir())


def _target_partition_dir(*, source_root: Path, target_root: Path, src_dir: Path) -> Path:
    relative = src_dir.relative_to(source_root / "raw")
    return target_root / "raw" / relative


def _to_hour_floor(value: datetime) -> datetime:
    value = value.astimezone(UTC)
    return value.replace(minute=0, second=0, microsecond=0)
