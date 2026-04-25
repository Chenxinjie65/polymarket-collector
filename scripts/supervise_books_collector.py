from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Supervise and restart the books collector.")
    parser.add_argument("--data-root", default="data_all_books_jsonl_live", help="Collector data root")
    parser.add_argument(
        "--collector-script",
        default="scripts/stream_all_books_files.py",
        help="Collector script path, relative to repo root or absolute.",
    )
    parser.add_argument("--duration-seconds", type=int, default=0, help="Collector run duration per launch")
    parser.add_argument("--restart-delay-seconds", type=float, default=5.0, help="Initial restart delay")
    parser.add_argument("--max-restart-delay-seconds", type=float, default=60.0, help="Maximum restart delay")
    parser.add_argument(
        "--stable-reset-seconds",
        type=float,
        default=300.0,
        help="If a run survives this long, reset restart backoff to the initial delay",
    )
    parser.add_argument(
        "--idle-reconnect-seconds",
        type=float,
        default=90.0,
        help="Forwarded to the collector to force reconnect after idle time",
    )
    return parser.parse_args()


def append_log(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True) + "\n")


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    data_root = Path(args.data_root)
    if not data_root.is_absolute():
        data_root = repo_root / data_root
    state_dir = data_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)

    supervisor_pid_path = state_dir / "supervisor.pid"
    stream_pid_path = state_dir / "stream.pid"
    supervisor_log = state_dir / "supervisor.log"
    stream_stdout = state_dir / "stream_stdout.log"
    stream_stderr = state_dir / "stream_stderr.log"
    collector_script = Path(args.collector_script)
    if not collector_script.is_absolute():
        collector_script = repo_root / collector_script

    supervisor_pid_path.write_text(str(os.getpid()), encoding="utf-8")

    base_delay = max(1.0, args.restart_delay_seconds)
    delay = base_delay
    create_no_window = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    while True:
        started_at = time.monotonic()
        append_log(
            supervisor_log,
            {
                "ts": datetime.now(UTC).isoformat(),
                "event": "launch",
                "delay_seconds": delay,
            },
        )
        with stream_stdout.open("ab") as stdout_f, stream_stderr.open("ab") as stderr_f:
            proc = subprocess.Popen(
                [
                    sys.executable,
                    str(collector_script),
                    "--data-root",
                    str(data_root),
                    "--duration-seconds",
                    str(args.duration_seconds),
                    "--idle-reconnect-seconds",
                    str(args.idle_reconnect_seconds),
                ],
                cwd=repo_root,
                stdout=stdout_f,
                stderr=stderr_f,
                creationflags=create_no_window,
            )
            stream_pid_path.write_text(str(proc.pid), encoding="utf-8")
            exit_code = proc.wait()

        runtime = time.monotonic() - started_at
        append_log(
            supervisor_log,
            {
                "ts": datetime.now(UTC).isoformat(),
                "event": "exit",
                "pid": proc.pid,
                "exit_code": exit_code,
                "runtime_seconds": round(runtime, 2),
            },
        )

        if runtime >= args.stable_reset_seconds:
            delay = base_delay
        else:
            delay = min(args.max_restart_delay_seconds, max(base_delay, delay * 2))

        time.sleep(delay)


if __name__ == "__main__":
    raise SystemExit(main())
