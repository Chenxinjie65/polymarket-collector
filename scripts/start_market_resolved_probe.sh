#!/usr/bin/env bash
set -euo pipefail

data_root="data_all_books_jsonl_live"
asset_count="3"
python_exe=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-root)
      data_root="$2"
      shift 2
      ;;
    --asset-count)
      asset_count="$2"
      shift 2
      ;;
    --python-exe)
      python_exe="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: start_market_resolved_probe.sh [options]
  --data-root PATH
  --asset-count N
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

state_dir="$data_root_abs/state/market_resolved_probe"
mkdir -p "$state_dir"
stdout="$state_dir/probe_stdout.log"
stderr="$state_dir/probe_stderr.log"
probe_pid_path="$state_dir/probe.pid"

if [[ -f "$probe_pid_path" ]]; then
  existing_pid="$(head -n 1 "$probe_pid_path" || true)"
  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "market_resolved probe is already running with PID $existing_pid" >&2
    exit 1
  fi
fi

"$python_exe" "scripts/probe_market_resolved.py" \
  --data-root "$data_root" \
  --asset-count "$asset_count" \
  >>"$stdout" 2>>"$stderr" &
proc_pid=$!
echo "$proc_pid" >"$probe_pid_path"

cat <<EOF
{"pid":$proc_pid,"python":"$python_exe","data_root":"$data_root_abs","stdout":"$stdout","stderr":"$stderr"}
EOF
