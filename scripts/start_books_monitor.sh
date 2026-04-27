#!/usr/bin/env bash
set -euo pipefail

data_root="data_all_books_jsonl_live"
interval_seconds="10"
duration_seconds="0"
python_exe=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-root)
      data_root="$2"
      shift 2
      ;;
    --interval-seconds)
      interval_seconds="$2"
      shift 2
      ;;
    --duration-seconds)
      duration_seconds="$2"
      shift 2
      ;;
    --python-exe)
      python_exe="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: start_books_monitor.sh [options]
  --data-root PATH
  --interval-seconds N
  --duration-seconds N
  --python-exe PATH
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -z "$python_exe" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    python_exe="$(command -v python3)"
  else
    python_exe="$(command -v python)"
  fi
fi

if [[ "$data_root" = /* ]]; then
  data_root_abs="$data_root"
else
  data_root_abs="$repo_root/$data_root"
fi

state_dir="$data_root_abs/state"
mkdir -p "$state_dir"
stdout="$state_dir/monitor_stdout.log"
stderr="$state_dir/monitor_stderr.log"
monitor_pid_path="$state_dir/monitor.pid"

if [[ -f "$monitor_pid_path" ]]; then
  existing_pid="$(head -n 1 "$monitor_pid_path" || true)"
  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "Monitor is already running with PID $existing_pid" >&2
    exit 1
  fi
fi

"$python_exe" "scripts/monitor_books_runtime.py" \
  --data-root "$data_root" \
  --interval-seconds "$interval_seconds" \
  --duration-seconds "$duration_seconds" \
  --self-pid-file "$monitor_pid_path" \
  >>"$stdout" 2>>"$stderr" &
proc_pid=$!
echo "$proc_pid" >"$monitor_pid_path"

cat <<EOF
{"pid":$proc_pid,"python":"$python_exe","data_root":"$data_root_abs","stdout":"$stdout","stderr":"$stderr"}
EOF
