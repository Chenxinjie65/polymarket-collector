#!/usr/bin/env bash
set -euo pipefail

data_root="data_all_books_jsonl_live"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-root)
      data_root="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: stop_books_stack.sh [--data-root PATH]
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
if [[ "$data_root" = /* ]]; then
  data_root_abs="$data_root"
else
  data_root_abs="$repo_root/$data_root"
fi

state_dir="$data_root_abs/state"
pid_files=(
  "$state_dir/guard.pid"
  "$state_dir/monitor.pid"
  "$state_dir/supervisor.pid"
  "$state_dir/stream.pid"
  "$state_dir/market_resolved_probe/probe.pid"
)

stopped=()
for pid_file in "${pid_files[@]}"; do
  [[ -f "$pid_file" ]] || continue
  pid_value="$(head -n 1 "$pid_file" || true)"
  [[ -n "$pid_value" ]] || continue
  if kill -0 "$pid_value" 2>/dev/null; then
    kill "$pid_value" 2>/dev/null || true
    for _ in {1..20}; do
      if ! kill -0 "$pid_value" 2>/dev/null; then
        break
      fi
      sleep 0.25
    done
    if kill -0 "$pid_value" 2>/dev/null; then
      kill -9 "$pid_value" 2>/dev/null || true
    fi
  fi
  stopped+=("$(basename "$pid_file")")
done

json_stopped="[]"
if [[ ${#stopped[@]} -gt 0 ]]; then
  json_stopped="["
  for item in "${stopped[@]}"; do
    if [[ "$json_stopped" != "[" ]]; then
      json_stopped+=","
    fi
    json_stopped+="\"$item\""
  done
  json_stopped+="]"
fi

printf '{"data_root":"%s","stopped":%s}\n' "$data_root_abs" "$json_stopped"
