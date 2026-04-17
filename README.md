# Polymarket Collector MVP

基于 Polymarket 公开接口的数据采集器，目标是稳定写入原始层（`jsonl.gz`），并支持后续去重、回填、Parquet 构建和主备切换。

当前代码能力（以 `main` 分支为准）：

- Gamma API：`discover-markets`、`discover-events`
- Data API：`fetch-trades`、`fetch-oi`、`fetch-holders`
- CLOB API：`fetch-books`、`fetch-midpoints`、`fetch-spreads`、`fetch-prices-history`
- WebSocket：`stream-market`
- 运行态：`run-primary`、`run-backup`、heartbeat/failover
- 工具链：`backfill-from-dir`、`dedup-report`、`build-parquet`

## 安装

如果从 GitHub 克隆，先初始化 submodule：

```bash
git submodule update --init --recursive
```

创建虚拟环境并安装：

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
```

如需官方 CLOB Python 客户端（用于 `--clob-driver pyclob`）：

```bash
python -m pip install -e ".[pyclob]"
```

如需 Parquet 构建：

```bash
python -m pip install -e ".[parquet]"
```

## 常用命令

快速拉取并采集：

```bash
python -m polymarket_collector discover-markets --limit 50
python -m polymarket_collector discover-events --limit 50
python -m polymarket_collector fetch-trades --markets-file latest
python -m polymarket_collector fetch-books --markets-file latest --max-assets 20
python -m polymarket_collector fetch-midpoints --markets-file latest --max-assets 20
python -m polymarket_collector fetch-spreads --markets-file latest --max-assets 20
python -m polymarket_collector fetch-prices-history --markets-file latest --max-assets 20 --interval 1h
python -m polymarket_collector fetch-oi --markets-file latest --max-markets 20
python -m polymarket_collector fetch-holders --markets-file latest --max-markets 20
```

使用官方 CLOB 驱动抓盘口：

```bash
python -m polymarket_collector --clob-driver pyclob fetch-books --markets-file latest --max-assets 20
```

运行态与运维工具：

```bash
python -m polymarket_collector heartbeat --node-id local-primary --role primary --status ok
python -m polymarket_collector check-failover --primary-node-id local-primary --max-stale-seconds 180 --write-state
python -m polymarket_collector backfill-from-dir --from-root /mnt/cloud-buffer/data --start 2026-04-17T00:00:00Z --end 2026-04-17T06:00:00Z
python -m polymarket_collector dedup-report --source data_trades --start 2026-04-17T00:00:00Z --end 2026-04-17T06:00:00Z
python -m polymarket_collector build-parquet --sources all
```

## 主循环模式

短时验证：

```bash
python -m polymarket_collector run-primary \
  --node-id local-primary \
  --interval-seconds 60 \
  --duration-seconds 120 \
  --market-limit 10 \
  --max-markets-for-trades 5 \
  --max-markets-for-oi-holders 5 \
  --max-assets-for-books 5 \
  --hot-assets-for-books 3 \
  --hot-snapshot-interval-seconds 20 \
  --cold-snapshot-interval-seconds 60 \
  --max-assets-for-history 5 \
  --history-snapshot-interval-seconds 60 \
  --history-window-seconds 600 \
  --max-assets-for-ws 5 \
  --ws-duration-seconds 20
```

全量分页扫描 + 富采集：

```bash
python -m polymarket_collector run-primary \
  --node-id local-primary \
  --interval-seconds 300 \
  --discover-all-pages \
  --freeze-tracked-markets \
  --full-trades-for-tracked-markets \
  --page-limit 500 \
  --max-markets-for-trades 500 \
  --trade-page-limit 500 \
  --trade-max-offset 10000 \
  --max-markets-for-oi-holders 1000 \
  --max-assets-for-books 2000 \
  --hot-assets-for-books 300 \
  --hot-snapshot-interval-seconds 30 \
  --cold-snapshot-interval-seconds 300 \
  --max-assets-for-history 1000 \
  --history-snapshot-interval-seconds 1800 \
  --history-window-seconds 3600 \
  --history-interval all \
  --new-market-backfill-seconds 1800
```

备节点模式：

```bash
python -m polymarket_collector run-backup \
  --node-id cloud-backup \
  --primary-node-id local-primary \
  --check-interval-seconds 30 \
  --max-stale-seconds 90 \
  --duration-seconds 120
```

## 一键启动脚本（移交流程重点）

脚本位置：`scripts/start_primary.sh`  
作用：激活 `.venv`、必要时自动安装本地包、可选停止旧 `run-primary` 进程、后台启动新进程并输出 `pid/log/data_root`。

直接启动：

```bash
bash scripts/start_primary.sh
```

常见覆盖参数：

```bash
PM_DATA_ROOT=data_run4 PM_DURATION_SECONDS=21600 PM_LOG_FILE=run_primary_run4.log bash scripts/start_primary.sh
```

脚本默认值（与代码一致）：

- `PM_DATA_ROOT=data`
- `PM_BUCKET_SECONDS=3600`
- `PM_WRITER_NODE_ID=cloud-test`
- `PM_NODE_ID=cloud-test`
- `PM_INTERVAL_SECONDS=60`
- `PM_DURATION_SECONDS=43200`（12 小时）
- `PM_PAGE_LIMIT=500`
- `PM_MAX_MARKETS_FOR_TRADES=500`
- `PM_TRADE_PAGE_LIMIT=500`
- `PM_TRADE_MAX_OFFSET=10000`
- `PM_MAX_MARKETS_FOR_OI_HOLDERS=1000`
- `PM_MAX_ASSETS_FOR_BOOKS=5000`
- `PM_HOT_ASSETS_FOR_BOOKS=1500`
- `PM_HOT_SNAPSHOT_INTERVAL_SECONDS=20`
- `PM_COLD_SNAPSHOT_INTERVAL_SECONDS=120`
- `PM_MAX_ASSETS_FOR_HISTORY=1500`
- `PM_HISTORY_SNAPSHOT_INTERVAL_SECONDS=900`
- `PM_HISTORY_WINDOW_SECONDS=900`
- `PM_HISTORY_INTERVAL=all`
- `PM_HISTORY_FIDELITY=1`
- `PM_MAX_ASSETS_FOR_WS=800`
- `PM_WS_DURATION_SECONDS=55`
- `PM_NEW_MARKET_BACKFILL_SECONDS=3600`
- `PM_FREEZE_TRACKED_MARKETS=1`
- `PM_FULL_TRADES_FOR_TRACKED_MARKETS=1`
- `PM_LOG_FILE=run_primary_12h_rich.log`
- `PM_PID_FILE=.primary.pid`
- `PM_KILL_EXISTING=1`

更新并重启（建议用于线上移交）：

```bash
git pull
. .venv/bin/activate
python -m pip install -e ".[pyclob]"
bash scripts/start_primary.sh
```

停止当前进程（按 pid 文件）：

```bash
kill "$(cat .primary.pid)"
```

## 健康检查与验收

进程与日志：

```bash
ps -ef | grep 'python -m polymarket_collector' | grep run-primary | grep -v grep
tail -n 100 run_primary_12h_rich.log
grep -E "ERROR|Traceback|failed:" run_primary_12h_rich.log | tail -n 20
```

heartbeat：

```bash
cat data_run3/state/heartbeat_cloud-test.json
```

建议重点关注字段：

- `status` 应为 `ok`
- `collection_warnings`、`bootstrap_warnings` 应为空
- `history_snapshot_asset_count` 应持续大于 0
- `books_hot_snapshot_count`、`books_cold_snapshot_count` 与配置规模一致

数据写入：

```bash
find data_run3/raw -maxdepth 1 -mindepth 1 -type d | sort
find data_run3/raw/source=clob_batch_prices_history -type f | tail
du -sb data_run3/raw
```

`du -sb` 会持续增长；`du -sh` 可能因单位取整短时间不变化，属于正常现象。

## 数据布局

原始层路径：

```text
data/raw/source=<source>/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
```

运行态状态文件：

```text
<data-root>/latest_markets.json
<data-root>/state/heartbeat_<node>.json
<data-root>/state/failover_state.json
<data-root>/state/market_universe.json
<data-root>/state/tracked_market_selection.json
<data-root>/state/trade_frontier.json
<data-root>/state/reports/dedup_report_*.json
```

已写入的主要源：

```text
gamma_markets
gamma_events
data_trades
data_oi
data_holders
clob_books
clob_midpoints
clob_spreads
clob_batch_prices_history
ws_market
```

## 重要实现约束

- `--clob-driver pyclob` 只影响 CLOB 相关调用；缺依赖会快速失败。
- 盘口采样采用冷热分层频率，降低全量抓取压力。
- `--freeze-tracked-markets` 会把跟踪集合持久化到 `tracked_market_selection.json`：保留仍然 active 的旧市场顺序，自动追加新 active 市场，自动移除 inactive/closed 市场。
- `--full-trades-for-tracked-markets` 会对固定市场集合做分页增量同步，并把已追到的交易前沿写进 `trade_frontier.json`，用于重启后续抓。
- `clob_batch_prices_history` 对时间参数有约束：当请求带 `start_ts/end_ts` 时，采集器会规范化 `interval` 为 `all`，并在必要时重试不带 `interval` 的请求。
- writer 使用固定时间桶（默认 `3600` 秒）；跨机主备建议统一 `bucket_seconds` 和 `writer_node_id`。
- `build-parquet` 从 `data/raw` 读取并写入 `data/warehouse`，已有文件默认跳过，`--overwrite` 可覆盖。

## 最近关键变更（按提交记录）

- `HEAD`：新增固定跟踪市场集合和交易增量全量抓取前沿状态。
- `ee2b379`：修复历史价格参数规范化，避免 `/batch-prices-history` 400。
- `ef5d85c`：修复一键脚本在“无旧进程”场景下的退出问题。
- `695a2de`：新增 `scripts/start_primary.sh` 一键启动脚本。
- `ca7b038`：单资产历史价格 400 时容错，避免整轮采集失败。
- `f0edd09`：历史价格分批与递归拆分策略完善。
- `b336907`：扩展运行态采集（events、oi/holders、history）和 heartbeat 指标。
- `6326a3d`：CLOB 快照请求批处理。
- `36ef375`：trade 请求批处理。
