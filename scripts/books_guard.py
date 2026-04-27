from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import psutil


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Keep the books supervisor and monitor running.")
    parser.add_argument("--data-root", default="data_all_books_jsonl_live", help="Collector data root")
    parser.add_argument(
        "--collector-script",
        default="scripts/stream_all_books_files.py",
        help="Collector script path forwarded to the supervisor.",
    )
    parser.add_argument(
        "--check-interval-seconds",
        type=float,
        default=30.0,
        help="How often to verify the stack is running",
    )
    parser.add_argument("--duration-seconds", type=int, default=0, help="Forwarded to the collector supervisor")
    parser.add_argument("--restart-delay-seconds", type=float, default=5.0, help="Forwarded to the collector supervisor")
    parser.add_argument(
        "--max-restart-delay-seconds",
        type=float,
        default=60.0,
        help="Forwarded to the collector supervisor",
    )
    parser.add_argument(
        "--stable-reset-seconds",
        type=float,
        default=300.0,
        help="Forwarded to the collector supervisor",
    )
    parser.add_argument(
        "--idle-reconnect-seconds",
        type=float,
        default=90.0,
        help="Forwarded to the collector supervisor",
    )
    parser.add_argument(
        "--monitor-interval-seconds",
        type=float,
        default=10.0,
        help="Forwarded to the runtime monitor",
    )
    parser.add_argument(
        "--monitor-duration-seconds",
        type=float,
        default=0.0,
        help="Forwarded to the runtime monitor",
    )
    parser.add_argument(
        "--collector-arg",
        action="append",
        default=[],
        help="Extra argument forwarded to the collector process through the supervisor. Repeat for multiple arguments.",
    )
    return parser.parse_args()


def append_log(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=True) + "\n")


def read_pid(path: Path) -> int | None:
    if not path.exists():
        return None
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def pid_running(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        proc = psutil.Process(pid)
        return proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def launch_python_process(repo_root: Path, script_path: Path, arguments: list[str], *, stdout: Path, stderr: Path) -> int:
    stdout.parent.mkdir(parents=True, exist_ok=True)
    stderr.parent.mkdir(parents=True, exist_ok=True)
    create_no_window = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    with stdout.open("ab") as stdout_f, stderr.open("ab") as stderr_f:
        proc = subprocess.Popen(
            [
                sys.executable,
                str(script_path),
                *arguments,
            ],
            cwd=repo_root,
            stdout=stdout_f,
            stderr=stderr_f,
            creationflags=create_no_window,
        )
    return proc.pid


def ensure_started(
    *,
    running: bool,
    component: str,
    repo_root: Path,
    state_dir: Path,
    log_path: Path,
    script_path: Path,
    script_args: list[str],
) -> None:
    if running:
        return
    launcher_pid = launch_python_process(
        repo_root,
        script_path,
        script_args,
        stdout=state_dir / f"{component}_launcher_stdout.log",
        stderr=state_dir / f"{component}_launcher_stderr.log",
    )
    append_log(
        log_path,
        {
            "ts": datetime.now(UTC).isoformat(),
            "event": f"start_{component}",
            "launcher_pid": launcher_pid,
            "script": str(script_path),
        },
    )


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    data_root = Path(args.data_root)
    if not data_root.is_absolute():
        data_root = repo_root / data_root

    state_dir = data_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    guard_pid_path = state_dir / "guard.pid"
    guard_log_path = state_dir / "guard.log"
    supervisor_pid_path = state_dir / "supervisor.pid"
    stream_pid_path = state_dir / "stream.pid"
    monitor_pid_path = state_dir / "monitor.pid"

    existing_guard_pid = read_pid(guard_pid_path)
    if pid_running(existing_guard_pid) and existing_guard_pid != os.getpid():
        append_log(
            guard_log_path,
            {
                "ts": datetime.now(UTC).isoformat(),
                "event": "guard_already_running",
                "pid": existing_guard_pid,
            },
        )
        return 0

    guard_pid_path.write_text(str(os.getpid()), encoding="utf-8")
    append_log(
        guard_log_path,
        {
            "ts": datetime.now(UTC).isoformat(),
            "event": "guard_started",
            "pid": os.getpid(),
            "data_root": str(data_root),
        },
    )

    collector_args = [
        "--data-root",
        str(data_root),
        "--collector-script",
        str(args.collector_script),
        "--duration-seconds",
        str(args.duration_seconds),
        "--restart-delay-seconds",
        str(args.restart_delay_seconds),
        "--max-restart-delay-seconds",
        str(args.max_restart_delay_seconds),
        "--stable-reset-seconds",
        str(args.stable_reset_seconds),
        "--idle-reconnect-seconds",
        str(args.idle_reconnect_seconds),
    ]
    for extra_arg in args.collector_arg:
        collector_args.append(f"--collector-arg={extra_arg}")
    monitor_args = [
        "--data-root",
        str(data_root),
        "--pid-file",
        str(stream_pid_path),
        "--self-pid-file",
        str(monitor_pid_path),
        "--interval-seconds",
        str(args.monitor_interval_seconds),
        "--duration-seconds",
        str(args.monitor_duration_seconds),
    ]

    while True:
        try:
            ensure_started(
                running=pid_running(read_pid(supervisor_pid_path)),
                component="collector",
                repo_root=repo_root,
                state_dir=state_dir,
                log_path=guard_log_path,
                script_path=repo_root / "scripts" / "supervise_books_collector.py",
                script_args=collector_args,
            )
            ensure_started(
                running=pid_running(read_pid(monitor_pid_path)),
                component="monitor",
                repo_root=repo_root,
                state_dir=state_dir,
                log_path=guard_log_path,
                script_path=repo_root / "scripts" / "monitor_books_runtime.py",
                script_args=monitor_args,
            )
        except Exception as exc:
            append_log(
                guard_log_path,
                {
                    "ts": datetime.now(UTC).isoformat(),
                    "event": "guard_error",
                    "error": repr(exc),
                },
            )
        time.sleep(max(5.0, args.check_interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
