from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class FinalizeStats:
    files_compressed: int = 0
    files_skipped_up_to_date: int = 0
    files_skipped_open_hour: int = 0
    bytes_in: int = 0
    bytes_out: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "files_compressed": self.files_compressed,
            "files_skipped_up_to_date": self.files_skipped_up_to_date,
            "files_skipped_open_hour": self.files_skipped_open_hour,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compress completed hourly books/price_changes shard files into finalized/*.jsonl.gz "
            "so downstream sync only pulls sealed compressed partitions."
        )
    )
    parser.add_argument("--data-root", default="data_all_books_jsonl_live", help="Collector data root")
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Dataset to finalize. Repeatable. Defaults to books and price_changes.",
    )
    parser.add_argument(
        "--compression-level",
        type=int,
        default=6,
        help="gzip compression level, 1-9",
    )
    parser.add_argument(
        "--delete-source",
        action="store_true",
        help="Delete source .jsonl after successful compression.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report planned compression without writing files.",
    )
    parser.add_argument(
        "--state-file",
        default="finalize_hourly_shards_latest.json",
        help="Summary JSON path under <data-root>/state/transfers/",
    )
    return parser.parse_args()


def parse_partition_hour(path: Path) -> datetime | None:
    try:
        dt_part = next(part for part in path.parts if part.startswith("dt="))
        hour_part = next(part for part in path.parts if part.startswith("hour="))
        dt_text = dt_part.split("=", 1)[1]
        hour_text = hour_part.split("=", 1)[1]
        parsed = datetime.strptime(f"{dt_text} {hour_text}", "%Y-%m-%d %H")
        return parsed.replace(tzinfo=UTC)
    except (StopIteration, ValueError):
        return None


def iter_source_files(data_root: Path, datasets: list[str]):
    for dataset in datasets:
        base = data_root / dataset
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.jsonl")):
            yield dataset, path


def compress_file(*, src: Path, dst: Path, compression_level: int) -> int:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp_dst = dst.with_suffix(dst.suffix + ".tmp")
    with src.open("rb") as src_handle, gzip.open(tmp_dst, "wb", compresslevel=compression_level) as dst_handle:
        shutil.copyfileobj(src_handle, dst_handle)
    os.replace(tmp_dst, dst)
    return dst.stat().st_size


def finalize_shards(
    *,
    data_root: Path,
    datasets: list[str],
    compression_level: int,
    delete_source: bool,
    dry_run: bool,
) -> FinalizeStats:
    stats = FinalizeStats()
    open_hour_floor = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)

    for dataset, src_path in iter_source_files(data_root, datasets):
        partition_hour = parse_partition_hour(src_path)
        if partition_hour is None:
            continue
        if partition_hour >= open_hour_floor:
            stats.files_skipped_open_hour += 1
            continue

        dst_path = data_root / "finalized" / dataset / src_path.relative_to(data_root / dataset)
        dst_path = dst_path.with_suffix(dst_path.suffix + ".gz")

        src_stat = src_path.stat()
        if dst_path.exists():
            dst_stat = dst_path.stat()
            if dst_stat.st_mtime >= src_stat.st_mtime and dst_stat.st_size > 0:
                stats.files_skipped_up_to_date += 1
                continue

        stats.bytes_in += src_stat.st_size
        if dry_run:
            stats.files_compressed += 1
            continue

        out_size = compress_file(src=src_path, dst=dst_path, compression_level=compression_level)
        stats.files_compressed += 1
        stats.bytes_out += out_size

        if delete_source:
            src_path.unlink()

    return stats


def main() -> int:
    args = parse_args()
    data_root = Path(args.data_root)
    datasets = args.dataset or ["books", "price_changes"]
    stats = finalize_shards(
        data_root=data_root,
        datasets=datasets,
        compression_level=max(1, min(9, args.compression_level)),
        delete_source=args.delete_source,
        dry_run=args.dry_run,
    )

    state_dir = data_root / "state" / "transfers"
    state_dir.mkdir(parents=True, exist_ok=True)
    summary_path = state_dir / args.state_file
    payload = {
        "ts_utc": datetime.now(UTC).isoformat(),
        "data_root": str(data_root),
        "datasets": datasets,
        "dry_run": args.dry_run,
        "delete_source": args.delete_source,
        "compression_level": args.compression_level,
        **stats.as_dict(),
    }
    summary_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
