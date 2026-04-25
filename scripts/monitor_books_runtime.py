from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil


DEFAULT_DATA_ROOT = Path("data_all_books_jsonl_live")


@dataclass(slots=True)
class Sample:
    ts_iso: str
    cpu_percent: float
    rss_bytes: int
    vms_bytes: int
    read_bytes: int
    write_bytes: int
    delta_read_bytes: int
    delta_write_bytes: int
    dir_total_bytes: int
    delta_dir_bytes: int
    thread_count: int
    handle_count: int | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Monitor the Polymarket books collector process and record CPU, memory, "
            "process disk I/O, and output directory growth."
        )
    )
    parser.add_argument(
        "--pid",
        type=int,
        help="Target process id. If omitted, read from --pid-file.",
    )
    parser.add_argument(
        "--pid-file",
        default=str(DEFAULT_DATA_ROOT / "state" / "stream.pid"),
        help="PID file to read when --pid is omitted.",
    )
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help="Collector output root to measure on-disk growth.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=10.0,
        help="Sampling interval in seconds.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=0.0,
        help="Monitor duration in seconds. Use 0 or negative to run until the process exits.",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Output directory for samples.csv and summary.json. Defaults to <data-root>/state/monitor.",
    )
    return parser.parse_args()


def resolve_pid(args: argparse.Namespace) -> int:
    if args.pid is not None:
        return args.pid
    pid_path = Path(args.pid_file)
    if not pid_path.exists():
        raise FileNotFoundError(f"PID file not found: {pid_path}")
    raw = pid_path.read_text(encoding="utf-8").strip()
    if not raw:
        raise ValueError(f"PID file is empty: {pid_path}")
    return int(raw)


def safe_handle_count(proc: psutil.Process) -> int | None:
    try:
        return proc.num_handles()
    except (AttributeError, psutil.AccessDenied):
        return None


def directory_total_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def write_samples_csv(path: Path, samples: list[Sample]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "ts_iso",
                "cpu_percent",
                "rss_bytes",
                "vms_bytes",
                "read_bytes",
                "write_bytes",
                "delta_read_bytes",
                "delta_write_bytes",
                "dir_total_bytes",
                "delta_dir_bytes",
                "thread_count",
                "handle_count",
            ]
        )
        for sample in samples:
            writer.writerow(
                [
                    sample.ts_iso,
                    f"{sample.cpu_percent:.2f}",
                    sample.rss_bytes,
                    sample.vms_bytes,
                    sample.read_bytes,
                    sample.write_bytes,
                    sample.delta_read_bytes,
                    sample.delta_write_bytes,
                    sample.dir_total_bytes,
                    sample.delta_dir_bytes,
                    sample.thread_count,
                    "" if sample.handle_count is None else sample.handle_count,
                ]
            )


def append_sample_csv(path: Path, sample: Sample) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(
                [
                    "ts_iso",
                    "cpu_percent",
                    "rss_bytes",
                    "vms_bytes",
                    "read_bytes",
                    "write_bytes",
                    "delta_read_bytes",
                    "delta_write_bytes",
                    "dir_total_bytes",
                    "delta_dir_bytes",
                    "thread_count",
                    "handle_count",
                ]
            )
        writer.writerow(
            [
                sample.ts_iso,
                f"{sample.cpu_percent:.2f}",
                sample.rss_bytes,
                sample.vms_bytes,
                sample.read_bytes,
                sample.write_bytes,
                sample.delta_read_bytes,
                sample.delta_write_bytes,
                sample.dir_total_bytes,
                sample.delta_dir_bytes,
                sample.thread_count,
                "" if sample.handle_count is None else sample.handle_count,
            ]
        )


def summarize(samples: list[Sample], *, pid: int, data_root: Path, interval_seconds: float) -> dict[str, Any]:
    if not samples:
        return {
            "pid": pid,
            "data_root": str(data_root),
            "sample_count": 0,
            "interval_seconds": interval_seconds,
        }

    cpu_values = [sample.cpu_percent for sample in samples]
    rss_values = [sample.rss_bytes for sample in samples]
    dir_values = [sample.dir_total_bytes for sample in samples]
    delta_write_values = [sample.delta_write_bytes for sample in samples]
    delta_dir_values = [sample.delta_dir_bytes for sample in samples]
    thread_values = [sample.thread_count for sample in samples]
    handle_values = [sample.handle_count for sample in samples if sample.handle_count is not None]

    runtime_seconds = max(0.0, (len(samples) - 1) * interval_seconds)
    total_proc_write = samples[-1].write_bytes - samples[0].write_bytes
    total_dir_growth = samples[-1].dir_total_bytes - samples[0].dir_total_bytes

    summary = {
        "pid": pid,
        "data_root": str(data_root),
        "sample_count": len(samples),
        "interval_seconds": interval_seconds,
        "ts_first": samples[0].ts_iso,
        "ts_last": samples[-1].ts_iso,
        "runtime_seconds_estimate": runtime_seconds,
        "cpu_percent": {
            "avg": round(statistics.fmean(cpu_values), 2),
            "max": round(max(cpu_values), 2),
        },
        "rss_bytes": {
            "avg": int(statistics.fmean(rss_values)),
            "max": max(rss_values),
            "last": samples[-1].rss_bytes,
        },
        "process_io_bytes": {
            "read_total_delta": samples[-1].read_bytes - samples[0].read_bytes,
            "write_total_delta": total_proc_write,
            "write_avg_per_sec": 0.0 if runtime_seconds <= 0 else total_proc_write / runtime_seconds,
            "write_max_per_interval": max(delta_write_values),
        },
        "data_dir_bytes": {
            "first": samples[0].dir_total_bytes,
            "last": samples[-1].dir_total_bytes,
            "growth_total": total_dir_growth,
            "growth_avg_per_sec": 0.0 if runtime_seconds <= 0 else total_dir_growth / runtime_seconds,
            "growth_max_per_interval": max(delta_dir_values),
        },
        "thread_count": {
            "avg": round(statistics.fmean(thread_values), 2),
            "max": max(thread_values),
            "last": samples[-1].thread_count,
        },
    }

    if handle_values:
        summary["handle_count"] = {
            "avg": round(statistics.fmean(handle_values), 2),
            "max": max(handle_values),
            "last": handle_values[-1],
        }

    return summary


def main() -> int:
    args = parse_args()
    data_root = Path(args.data_root)
    output_dir = Path(args.output_dir) if args.output_dir else data_root / "state" / "monitor"
    output_dir.mkdir(parents=True, exist_ok=True)
    samples_path = output_dir / "samples.csv"
    latest_path = output_dir / "latest.json"
    summary_path = output_dir / "summary.json"

    start_monotonic = time.monotonic()
    stop_at = None if args.duration_seconds <= 0 else start_monotonic + args.duration_seconds
    samples: list[Sample] = []
    summary_pid = args.pid if args.pid is not None else -1
    prev_read_bytes = 0
    prev_write_bytes = 0
    prev_dir_total_bytes = 0
    first_sample = True
    current_pid: int | None = None
    proc: psutil.Process | None = None

    while True:
        if stop_at is not None and time.monotonic() >= stop_at:
            break

        try:
            pid = resolve_pid(args)
            summary_pid = pid
        except (FileNotFoundError, ValueError):
            time.sleep(args.interval_seconds)
            continue

        if proc is None or current_pid != pid:
            try:
                proc = psutil.Process(pid)
            except psutil.NoSuchProcess:
                time.sleep(args.interval_seconds)
                continue
            proc.cpu_percent(interval=None)
            current_pid = pid
            prev_read_bytes = 0
            prev_write_bytes = 0
            first_sample = True

        if not proc.is_running():
            proc = None
            current_pid = None
            time.sleep(args.interval_seconds)
            continue

        try:
            with proc.oneshot():
                io = proc.io_counters()
                mem = proc.memory_info()
                cpu = proc.cpu_percent(interval=None)
                threads = proc.num_threads()
                handles = safe_handle_count(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            break

        dir_total = directory_total_bytes(data_root)
        ts_iso = datetime.now(UTC).isoformat()

        if first_sample:
            delta_read_bytes = 0
            delta_write_bytes = 0
            delta_dir_bytes = 0
            first_sample = False
        else:
            delta_read_bytes = io.read_bytes - prev_read_bytes
            delta_write_bytes = io.write_bytes - prev_write_bytes
            delta_dir_bytes = dir_total - prev_dir_total_bytes

        samples.append(
            Sample(
                ts_iso=ts_iso,
                cpu_percent=cpu,
                rss_bytes=mem.rss,
                vms_bytes=mem.vms,
                read_bytes=io.read_bytes,
                write_bytes=io.write_bytes,
                delta_read_bytes=delta_read_bytes,
                delta_write_bytes=delta_write_bytes,
                dir_total_bytes=dir_total,
                delta_dir_bytes=delta_dir_bytes,
                thread_count=threads,
                handle_count=handles,
            )
        )
        append_sample_csv(samples_path, samples[-1])
        latest_path.write_text(
            json.dumps(
                {
                    "ts_iso": samples[-1].ts_iso,
                    "cpu_percent": samples[-1].cpu_percent,
                    "rss_bytes": samples[-1].rss_bytes,
                    "vms_bytes": samples[-1].vms_bytes,
                    "read_bytes": samples[-1].read_bytes,
                    "write_bytes": samples[-1].write_bytes,
                    "delta_read_bytes": samples[-1].delta_read_bytes,
                    "delta_write_bytes": samples[-1].delta_write_bytes,
                    "dir_total_bytes": samples[-1].dir_total_bytes,
                    "delta_dir_bytes": samples[-1].delta_dir_bytes,
                    "thread_count": samples[-1].thread_count,
                    "handle_count": samples[-1].handle_count,
                },
                ensure_ascii=True,
                indent=2,
            ),
            encoding="utf-8",
        )

        prev_read_bytes = io.read_bytes
        prev_write_bytes = io.write_bytes
        prev_dir_total_bytes = dir_total

        print(
            json.dumps(
                {
                    "ts": ts_iso,
                    "cpu_percent": round(cpu, 2),
                    "rss_mb": round(mem.rss / (1024 * 1024), 2),
                    "proc_write_delta_bytes": delta_write_bytes,
                    "dir_growth_delta_bytes": delta_dir_bytes,
                    "dir_total_bytes": dir_total,
                    "threads": threads,
                    "handles": handles,
                },
                ensure_ascii=True,
            ),
            flush=True,
        )

        sleep_for = args.interval_seconds
        if stop_at is not None:
            sleep_for = min(sleep_for, max(0.0, stop_at - time.monotonic()))
        if sleep_for <= 0:
            break
        time.sleep(sleep_for)

    summary = summarize(samples, pid=summary_pid, data_root=data_root, interval_seconds=args.interval_seconds)
    summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "samples_csv": str(samples_path),
                "latest_json": str(latest_path),
                "summary_json": str(summary_path),
            },
            ensure_ascii=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
