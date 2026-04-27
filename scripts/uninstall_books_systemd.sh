#!/usr/bin/env bash
set -euo pipefail

service_name="polymarket-books-guard"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service-name)
      service_name="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: uninstall_books_systemd.sh [--service-name NAME]
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

unit_path="/etc/systemd/system/${service_name}.service"

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "Run as root to uninstall a systemd service." >&2
  exit 1
fi

systemctl disable --now "${service_name}.service" 2>/dev/null || true
rm -f "$unit_path"
systemctl daemon-reload

cat <<EOF
{"service":"${service_name}.service","unit_path":"$unit_path","status":"removed"}
EOF
