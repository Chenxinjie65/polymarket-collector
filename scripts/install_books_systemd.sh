#!/usr/bin/env bash
set -euo pipefail

service_name="polymarket-books-guard"
data_root="data_all_books_jsonl_live"
collector_script="scripts/stream_all_books_files.py"
check_interval_seconds="30"
duration_seconds="0"
restart_delay_seconds="5"
max_restart_delay_seconds="60"
stable_reset_seconds="300"
idle_reconnect_seconds="90"
monitor_interval_seconds="10"
monitor_duration_seconds="0"
python_exe=""
collector_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service-name)
      service_name="$2"
      shift 2
      ;;
    --data-root)
      data_root="$2"
      shift 2
      ;;
    --collector-script)
      collector_script="$2"
      shift 2
      ;;
    --check-interval-seconds)
      check_interval_seconds="$2"
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
    --monitor-interval-seconds)
      monitor_interval_seconds="$2"
      shift 2
      ;;
    --monitor-duration-seconds)
      monitor_duration_seconds="$2"
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
Usage: install_books_systemd.sh [options]
  --service-name NAME
  --data-root PATH
  --collector-script PATH
  --check-interval-seconds N
  --duration-seconds N
  --restart-delay-seconds N
  --max-restart-delay-seconds N
  --stable-reset-seconds N
  --idle-reconnect-seconds N
  --monitor-interval-seconds N
  --monitor-duration-seconds N
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

if [[ -z "$python_exe" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    python_exe="$(command -v python3)"
  else
    python_exe="$(command -v python)"
  fi
fi

cmd=(
  "$python_exe"
  "$repo_root/scripts/books_guard.py"
  "--data-root" "$data_root"
  "--collector-script" "$collector_script"
  "--check-interval-seconds" "$check_interval_seconds"
  "--duration-seconds" "$duration_seconds"
  "--restart-delay-seconds" "$restart_delay_seconds"
  "--max-restart-delay-seconds" "$max_restart_delay_seconds"
  "--stable-reset-seconds" "$stable_reset_seconds"
  "--idle-reconnect-seconds" "$idle_reconnect_seconds"
  "--monitor-interval-seconds" "$monitor_interval_seconds"
  "--monitor-duration-seconds" "$monitor_duration_seconds"
)

for extra_arg in "${collector_args[@]}"; do
  cmd+=("--collector-arg=$extra_arg")
done

quoted_cmd="$(printf '%q ' "${cmd[@]}")"
quoted_cmd="${quoted_cmd% }"
unit_path="/etc/systemd/system/${service_name}.service"

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "Run as root to install a systemd service." >&2
  exit 1
fi

cat >"$unit_path" <<EOF
[Unit]
Description=Polymarket books guard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$repo_root
ExecStart=/usr/bin/env bash -lc 'exec $quoted_cmd'
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "${service_name}.service"

cat <<EOF
{"service":"${service_name}.service","unit_path":"$unit_path","status":"enabled"}
EOF
