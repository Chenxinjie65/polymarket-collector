#!/usr/bin/env bash
set -euo pipefail

data_root="data_all_books_jsonl_live"
collector_script="scripts/stream_all_books_files.py"
duration_seconds="0"
restart_delay_seconds="5"
max_restart_delay_seconds="60"
stable_reset_seconds="300"
idle_reconnect_seconds="90"
python_exe=""
collector_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-root)
      data_root="$2"
      shift 2
      ;;
    --collector-script)
      collector_script="$2"
      shift 2
      ;;
    --duration-seconds)
      duration_seconds="$2"
      shift 2
      ;;
    --restart-delay-seconds)
      restart_delay_seconds="$2"
      shift 2
      ;;
    --max-restart-delay-seconds)
      max_restart_delay_seconds="$2"
      shift 2
      ;;
    --stable-reset-seconds)
      stable_reset_seconds="$2"
      shift 2
      ;;
    --idle-reconnect-seconds)
      idle_reconnect_seconds="$2"
      shift 2
      ;;
    --python-exe)
      python_exe="$2"
      shift 2
      ;;
    --collector-arg)
      collector_args+=("$2")
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: start_books_collector.sh [options]
  --data-root PATH
  --collector-script PATH
  --duration-seconds N
  --restart-delay-seconds N
  --max-restart-delay-seconds N
  --stable-reset-seconds N
  --idle-reconnect-seconds N
  --python-exe PATH
  --collector-arg ARG
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
stdout="$state_dir/supervisor_stdout.log"
stderr="$state_dir/supervisor_stderr.log"
supervisor_pid_path="$state_dir/supervisor.pid"
stream_pid_path="$state_dir/stream.pid"

if [[ -f "$supervisor_pid_path" ]]; then
  existing_pid="$(head -n 1 "$supervisor_pid_path" || true)"
  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "Collector supervisor is already running with PID $existing_pid" >&2
    exit 1
  fi
fi

cmd=(
  "$python_exe"
  "scripts/supervise_books_collector.py"
  "--data-root" "$data_root"
  "--collector-script" "$collector_script"
  "--duration-seconds" "$duration_seconds"
  "--restart-delay-seconds" "$restart_delay_seconds"
  "--max-restart-delay-seconds" "$max_restart_delay_seconds"
  "--stable-reset-seconds" "$stable_reset_seconds"
  "--idle-reconnect-seconds" "$idle_reconnect_seconds"
)

for extra_arg in "${collector_args[@]}"; do
  cmd+=("--collector-arg" "$extra_arg")
done

"${cmd[@]}" >>"$stdout" 2>>"$stderr" &
proc_pid=$!
echo "$proc_pid" >"$supervisor_pid_path"

cat <<EOF
{"pid":$proc_pid,"python":"$python_exe","data_root":"$data_root_abs","supervisor_pid":$proc_pid,"stream_pid_path":"$stream_pid_path","stdout":"$stdout","stderr":"$stderr"}
EOF
