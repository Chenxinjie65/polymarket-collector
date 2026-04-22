#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

PID_FILE="${PM_PID_FILE:-.primary.pid}"
LOG_FILE="${PM_LOG_FILE:-run_primary_12h_rich.log}"

is_running() {
  if [[ ! -f "${PID_FILE}" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [[ -z "${pid}" ]]; then
    return 1
  fi
  if ps -p "${pid}" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

cmd_start() {
  if is_running; then
    echo "already running: pid=$(cat "${PID_FILE}")"
    return 0
  fi
  bash "${SCRIPT_DIR}/start_primary.sh"
}

cmd_stop() {
  if ! is_running; then
    echo "not running"
    return 0
  fi
  local pid
  pid="$(cat "${PID_FILE}")"
  echo "stopping pid=${pid}"
  kill "${pid}" || true
  sleep 2
  if ps -p "${pid}" >/dev/null 2>&1; then
    echo "force stopping pid=${pid}"
    kill -9 "${pid}" || true
  fi
}

cmd_status() {
  if is_running; then
    local pid
    pid="$(cat "${PID_FILE}")"
    echo "running: pid=${pid}"
    ps -p "${pid}" -o pid,etimes,cmd
    return 0
  fi
  echo "not running"
}

cmd_restart() {
  cmd_stop
  cmd_start
}

cmd_logs() {
  if [[ ! -f "${LOG_FILE}" ]]; then
    echo "log file not found: ${REPO_ROOT}/${LOG_FILE}"
    return 1
  fi
  tail -n "${TAIL_LINES:-100}" "${LOG_FILE}"
}

usage() {
  cat <<EOF
usage: bash scripts/collector_ctl.sh <start|stop|status|restart|logs>

env overrides:
  PM_PID_FILE (default: .primary.pid)
  PM_LOG_FILE (default: run_primary_12h_rich.log)
  TAIL_LINES  (default: 100, for logs command)
EOF
}

main() {
  local cmd="${1:-}"
  case "${cmd}" in
    start) cmd_start ;;
    stop) cmd_stop ;;
    status) cmd_status ;;
    restart) cmd_restart ;;
    logs) cmd_logs ;;
    *) usage; exit 1 ;;
  esac
}

main "${@}"

