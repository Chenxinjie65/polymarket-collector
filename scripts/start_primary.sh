#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

if [[ ! -d ".venv" ]]; then
  echo "error: .venv not found in ${REPO_ROOT}" >&2
  exit 1
fi

# shellcheck source=/dev/null
. ".venv/bin/activate"

if ! python -c "import polymarket_collector" >/dev/null 2>&1; then
  echo "info: installing local package into .venv"
  python -m pip install -e ".[pyclob]"
fi

DATA_ROOT="${PM_DATA_ROOT:-data}"
BUCKET_SECONDS="${PM_BUCKET_SECONDS:-3600}"
WRITER_NODE_ID="${PM_WRITER_NODE_ID:-cloud-test}"
NODE_ID="${PM_NODE_ID:-cloud-test}"
INTERVAL_SECONDS="${PM_INTERVAL_SECONDS:-60}"
DURATION_SECONDS="${PM_DURATION_SECONDS:-43200}"
PAGE_LIMIT="${PM_PAGE_LIMIT:-500}"
MAX_MARKETS_FOR_TRADES="${PM_MAX_MARKETS_FOR_TRADES:-0}"
TRADE_PAGE_LIMIT="${PM_TRADE_PAGE_LIMIT:-500}"
TRADE_MAX_OFFSET="${PM_TRADE_MAX_OFFSET:-10000}"
MAX_MARKETS_FOR_OI_HOLDERS="${PM_MAX_MARKETS_FOR_OI_HOLDERS:-0}"
MAX_ASSETS_FOR_BOOKS="${PM_MAX_ASSETS_FOR_BOOKS:-0}"
HOT_ASSETS_FOR_BOOKS="${PM_HOT_ASSETS_FOR_BOOKS:-0}"
HOT_SNAPSHOT_INTERVAL_SECONDS="${PM_HOT_SNAPSHOT_INTERVAL_SECONDS:-300}"
COLD_SNAPSHOT_INTERVAL_SECONDS="${PM_COLD_SNAPSHOT_INTERVAL_SECONDS:-300}"
MAX_ASSETS_FOR_HISTORY="${PM_MAX_ASSETS_FOR_HISTORY:-0}"
HISTORY_SNAPSHOT_INTERVAL_SECONDS="${PM_HISTORY_SNAPSHOT_INTERVAL_SECONDS:-0}"
HISTORY_WINDOW_SECONDS="${PM_HISTORY_WINDOW_SECONDS:-900}"
HISTORY_INTERVAL="${PM_HISTORY_INTERVAL:-all}"
HISTORY_FIDELITY="${PM_HISTORY_FIDELITY:-1}"
MAX_ASSETS_FOR_WS="${PM_MAX_ASSETS_FOR_WS:-0}"
WS_DURATION_SECONDS="${PM_WS_DURATION_SECONDS:-55}"
NEW_MARKET_BACKFILL_SECONDS="${PM_NEW_MARKET_BACKFILL_SECONDS:-0}"
FREEZE_TRACKED_MARKETS="${PM_FREEZE_TRACKED_MARKETS:-1}"
FULL_TRADES_FOR_TRACKED_MARKETS="${PM_FULL_TRADES_FOR_TRACKED_MARKETS:-1}"
COLLECT_MIDPOINTS="${PM_COLLECT_MIDPOINTS:-0}"
COLLECT_SPREADS="${PM_COLLECT_SPREADS:-0}"
LOG_FILE="${PM_LOG_FILE:-run_primary_12h_rich.log}"
PID_FILE="${PM_PID_FILE:-.primary.pid}"
KILL_EXISTING="${PM_KILL_EXISTING:-1}"

if [[ "${KILL_EXISTING}" == "1" ]]; then
  existing_pids="$(ps -ef | grep 'python -m polymarket_collector' | grep 'run-primary' | grep -v grep | awk '{print $2}' || true)"
  if [[ -n "${existing_pids}" ]]; then
    echo "info: stopping existing run-primary pids: ${existing_pids}"
    # shellcheck disable=SC2086
    kill ${existing_pids} || true
    sleep 2
    alive_pids="$(ps -ef | grep 'python -m polymarket_collector' | grep 'run-primary' | grep -v grep | awk '{print $2}' || true)"
    if [[ -n "${alive_pids}" ]]; then
      echo "info: force stopping pids: ${alive_pids}"
      # shellcheck disable=SC2086
      kill -9 ${alive_pids} || true
    fi
  fi
fi

mkdir -p "${DATA_ROOT}"

freeze_tracked_markets_flag=()
if [[ "${FREEZE_TRACKED_MARKETS}" == "1" ]]; then
  freeze_tracked_markets_flag+=(--freeze-tracked-markets)
fi

full_trades_for_tracked_markets_flag=()
if [[ "${FULL_TRADES_FOR_TRACKED_MARKETS}" == "1" ]]; then
  full_trades_for_tracked_markets_flag+=(--full-trades-for-tracked-markets)
fi

collect_midpoints_flag=()
if [[ "${COLLECT_MIDPOINTS}" == "1" ]]; then
  collect_midpoints_flag+=(--collect-midpoints)
fi

collect_spreads_flag=()
if [[ "${COLLECT_SPREADS}" == "1" ]]; then
  collect_spreads_flag+=(--collect-spreads)
fi

nohup python -m polymarket_collector \
  --data-root "${DATA_ROOT}" \
  --bucket-seconds "${BUCKET_SECONDS}" \
  --writer-node-id "${WRITER_NODE_ID}" \
  --clob-driver pyclob \
  run-primary \
  --node-id "${NODE_ID}" \
  --interval-seconds "${INTERVAL_SECONDS}" \
  --duration-seconds "${DURATION_SECONDS}" \
  --discover-all-pages \
  --page-limit "${PAGE_LIMIT}" \
  --max-markets-for-trades "${MAX_MARKETS_FOR_TRADES}" \
  --trade-page-limit "${TRADE_PAGE_LIMIT}" \
  --trade-max-offset "${TRADE_MAX_OFFSET}" \
  --max-markets-for-oi-holders "${MAX_MARKETS_FOR_OI_HOLDERS}" \
  --max-assets-for-books "${MAX_ASSETS_FOR_BOOKS}" \
  --hot-assets-for-books "${HOT_ASSETS_FOR_BOOKS}" \
  --hot-snapshot-interval-seconds "${HOT_SNAPSHOT_INTERVAL_SECONDS}" \
  --cold-snapshot-interval-seconds "${COLD_SNAPSHOT_INTERVAL_SECONDS}" \
  --max-assets-for-history "${MAX_ASSETS_FOR_HISTORY}" \
  --history-snapshot-interval-seconds "${HISTORY_SNAPSHOT_INTERVAL_SECONDS}" \
  --history-window-seconds "${HISTORY_WINDOW_SECONDS}" \
  --history-interval "${HISTORY_INTERVAL}" \
  --history-fidelity "${HISTORY_FIDELITY}" \
  --max-assets-for-ws "${MAX_ASSETS_FOR_WS}" \
  --ws-duration-seconds "${WS_DURATION_SECONDS}" \
  --new-market-backfill-seconds "${NEW_MARKET_BACKFILL_SECONDS}" \
  "${freeze_tracked_markets_flag[@]}" \
  "${full_trades_for_tracked_markets_flag[@]}" \
  "${collect_midpoints_flag[@]}" \
  "${collect_spreads_flag[@]}" \
  > "${LOG_FILE}" 2>&1 &

pid="$!"
echo "${pid}" > "${PID_FILE}"

echo "started: pid=${pid}"
echo "log: ${REPO_ROOT}/${LOG_FILE}"
echo "pid file: ${REPO_ROOT}/${PID_FILE}"
echo "data root: ${REPO_ROOT}/${DATA_ROOT}"
