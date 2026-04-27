# Polymarket Collector MVP

Minimal Polymarket public data collector focused on proving the data path works.

Current capabilities:

- discover active markets from Gamma API
- fetch recent trades from Data API
- fetch order book snapshots from CLOB API
- optionally stream public market WebSocket events
- write raw data to time-partitioned `jsonl.gz` files

## Quick start

If you cloned this repository from GitHub, initialize submodules first:

```bash
git submodule update --init --recursive
```

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
```

If you want to use the official CLOB Python client driver:

```bash
python -m pip install -e ".[pyclob]"
```

If you also want parquet conversion:

```bash
python -m pip install -e ".[parquet]"
```

Discover active markets and save them:

```bash
python -m polymarket_collector discover-markets --limit 50
```

Fetch recent trades for the discovered markets:

```bash
python -m polymarket_collector fetch-trades --markets-file latest
```

Fetch active events:

```bash
python -m polymarket_collector discover-events --limit 50
```

Fetch order book snapshots for the same markets:

```bash
python -m polymarket_collector fetch-books --markets-file latest --max-assets 20
```

Fetch high-value pricing/microstructure snapshots:

```bash
python -m polymarket_collector fetch-midpoints --markets-file latest --max-assets 20
python -m polymarket_collector fetch-spreads --markets-file latest --max-assets 20
python -m polymarket_collector fetch-prices-history --markets-file latest --max-assets 20 --interval 1h
```

Fetch market participation structure:

```bash
python -m polymarket_collector fetch-oi --markets-file latest --max-markets 20
python -m polymarket_collector fetch-holders --markets-file latest --max-markets 20
```

Use the official CLOB driver for book fetches:

```bash
python -m polymarket_collector --clob-driver pyclob fetch-books --markets-file latest --max-assets 20
```

Stream public market WebSocket events for the first 10 discovered assets:

```bash
python -m polymarket_collector stream-market --markets-file latest --max-assets 10
```

Run the books collector stack on Linux with the native shell wrappers:

```bash
./scripts/start_books_guard.sh \
  --data-root /var/lib/polymarket-books \
  --collector-arg "--write-shards" \
  --collector-arg "64"
```

Stop the Linux books stack:

```bash
./scripts/stop_books_stack.sh --data-root /var/lib/polymarket-books
```

Install the books guard as a `systemd` service on Linux:

```bash
sudo ./scripts/install_books_systemd.sh \
  --data-root /var/lib/polymarket-books \
  --collector-arg "--write-shards" \
  --collector-arg "64"
```

Pull cloud-collected data back to a local machine over SSH:

```bash
./scripts/pull_cloud_data.sh \
  --remote collector@example.com:/var/lib/polymarket-books \
  --local-root /data/polymarket-books
```

Finalize completed hourly shard files into one compressed package per hour (`finalized/hourly/.../bundle.tar.gz`) on the cloud node:

```bash
python3 scripts/finalize_hourly_shards.py \
  --data-root /var/lib/polymarket-books \
  --delete-source
```

If the local machine should only download sealed compressed files:

```bash
./scripts/pull_cloud_data.sh \
  --remote collector@example.com:/var/lib/polymarket-books \
  --local-root /data/polymarket-books \
  --compressed-only
```

By default, pulling `finalized/*.jsonl.gz` writes remote ack files under
`state/transfers/acks/finalized/*.ack.json` and then deletes the corresponding
remote compressed files. Use `--no-ack-delete-finalized` to disable that behavior.

Write one heartbeat (primary node example):

```bash
python -m polymarket_collector heartbeat --node-id local-primary --role primary --status ok
```

Evaluate failover status from primary heartbeat:

```bash
python -m polymarket_collector check-failover --primary-node-id local-primary --max-stale-seconds 180 --write-state
```

Incremental backfill from a cloud buffer directory for a specific outage window:

```bash
python -m polymarket_collector backfill-from-dir \
  --from-root /mnt/cloud-buffer/data \
  --start 2026-04-17T00:00:00Z \
  --end 2026-04-17T06:00:00Z \
  --sources data_trades,ws_market,clob_books
```

Build a duplicate-rate report for one source and time window:

```bash
python -m polymarket_collector dedup-report \
  --source data_trades \
  --start 2026-04-17T00:00:00Z \
  --end 2026-04-17T06:00:00Z
```

Build parquet warehouse files from raw jsonl.gz (incremental by default):

```bash
python -m polymarket_collector build-parquet --sources all
```

Build parquet only for a window/source:

```bash
python -m polymarket_collector build-parquet \
  --sources data_trades,clob_books,ws_market \
  --start 2026-04-17T00:00:00Z \
  --end 2026-04-17T06:00:00Z
```

Run primary collector loop (example: one short cycle for testing):

```bash
python -m polymarket_collector run-primary \
  --node-id local-primary \
  --interval-seconds 60 \
  --duration-seconds 70 \
  --market-limit 10 \
  --max-markets-for-trades 5 \
  --max-assets-for-books 5 \
  --hot-assets-for-books 3 \
  --hot-snapshot-interval-seconds 20 \
  --cold-snapshot-interval-seconds 60 \
  --max-assets-for-ws 5 \
  --ws-duration-seconds 20
```

Enable automatic full active-market scan and new-market bootstrap:

```bash
python -m polymarket_collector run-primary \
  --node-id local-primary \
  --interval-seconds 300 \
  --discover-all-pages \
  --page-limit 500 \
  --max-assets-for-books 2000 \
  --hot-assets-for-books 300 \
  --hot-snapshot-interval-seconds 30 \
  --cold-snapshot-interval-seconds 300 \
  --new-market-backfill-seconds 1800
```

Run backup failover loop (example: monitor and collect only when primary is stale):

```bash
python -m polymarket_collector run-backup \
  --node-id cloud-backup \
  --primary-node-id local-primary \
  --check-interval-seconds 30 \
  --max-stale-seconds 90 \
  --duration-seconds 120 \
  --market-limit 10 \
  --max-markets-for-trades 5 \
  --max-assets-for-books 5 \
  --hot-assets-for-books 3 \
  --hot-snapshot-interval-seconds 20 \
  --cold-snapshot-interval-seconds 60 \
  --max-assets-for-ws 5 \
  --ws-duration-seconds 20
```

`run-primary` and `run-backup` now persist market universe state at:

```text
<data-root>/state/market_universe.json
```

New market detection works by diffing latest `conditionId` and `clobTokenIds` against this state.

Data is written under:

```text
data/raw/source=<source>/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
```

## Notes

- This MVP stores raw (`jsonl.gz`) and can build parquet warehouse files via `build-parquet`.
- It uses only public endpoints and public WebSocket channels.
- It is designed to be easy to move to a cloud server later.
- The failover tools assume local and cloud collectors use the same partition convention.
- `--clob-driver pyclob` only affects CLOB calls; if the extra dependency is not installed, the command fails fast.
- Book snapshots are now tiered by frequency: hot assets use `--hot-snapshot-interval-seconds`, cold assets use `--cold-snapshot-interval-seconds`.
- Writer now uses fixed time buckets (default `--bucket-seconds 3600`). Use `--bucket-seconds 86400` for daily bucket files.
- Use the same `--bucket-seconds` and `--writer-node-id` convention on both machines for clean failover sync.
- `build-parquet` reads from `data/raw` and writes mirrored parquet paths into `data/warehouse`; existing parquet files are skipped unless `--overwrite` is set.
