# Polymarket Data Platform Implementation Plan

## 1. Goal

Build a production-grade Polymarket data platform that:

- continuously ingests public Polymarket market data
- stores data in a form suitable for research, model training, and commercial delivery
- links off-chain trade records with on-chain activity where confidence is high enough
- supports future expansion into analytics, user profiling, factor products, and customer-facing services

This project should not be designed as a one-off crawler. It should be designed as a layered data platform with clear boundaries between ingestion, normalization, analytics, and distribution.

## 2. Product Principles

- Use official public APIs and documented blockchain data sources as the primary source of truth.
- Separate raw data retention from normalized data products.
- Prefer append-only datasets and versioned schemas over in-place mutation.
- Design for batch delivery first, then add query services later.
- Preserve replayability and auditability so datasets can be regenerated if parsing logic changes.
- Keep infrastructure simple in the first iteration, but choose formats and interfaces that scale.
- Avoid tight coupling between ingestion logic and downstream analytics.

## 3. High-Level Architecture

The platform should be organized into six layers:

1. Source layer
- Polymarket public APIs
- Polymarket public market data endpoints
- Polymarket public order/trade data endpoints
- Blockchain event sources and indexers

2. Ingestion layer
- scheduled pull jobs for REST resources
- streaming consumers for trade/orderbook feeds where needed
- chain event ingestion jobs
- retry, backoff, deduplication, and checkpoint logic

3. Raw data layer
- immutable source captures
- minimally transformed payload storage
- enough metadata to replay and reprocess later

4. Standardized data layer
- typed, normalized, schema-controlled datasets
- clean facts and dimensions for research and downstream products

5. Enrichment layer
- trade-to-address matching
- market state reconstruction
- address/entity-level feature generation
- derived metrics, factors, and labels

6. Delivery layer
- customer-ready Parquet packages
- manifests, schema docs, and changelogs
- optional future query API and analytics services

### 3.1 Runtime Topology (Cost-Optimized)

Use a two-node setup by default:

- local node as the primary collector
  - continuously ingests and writes to local disks
  - serves as the long-term archive source
- cloud node as an emergency buffer
  - ingests in parallel but keeps only a short retention window (recommended 3-7 days)
  - does not perform continuous full downstream sync in normal operation
- failover backfill workflow
  - only when the local node is offline, pull missing cloud partitions for the outage window
  - run deduplication and integrity checks after backfill

Goal: turn cloud egress from a continuous cost into an occasional failure-recovery cost.

## 4. Repository and Project Structure

Recommended structure:

```text
polymarket-data-platform/
├── README.md
├── docs/
│   ├── architecture.md
│   ├── schemas/
│   ├── operations.md
│   └── product_definitions.md
├── config/
│   ├── base.yaml
│   ├── prod.yaml
│   └── datasets.yaml
├── src/
│   ├── collectors/
│   ├── parsers/
│   ├── writers/
│   ├── pipelines/
│   ├── matching/
│   ├── features/
│   ├── quality/
│   ├── manifests/
│   └── cli/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── scripts/
├── state/
├── logs/
└── data/
```

Guidelines:

- Keep source-specific logic inside `collectors/`.
- Keep schema normalization inside `parsers/`.
- Keep file-format and partition-writing logic inside `writers/`.
- Keep matching and profiling logic isolated from ingestion.
- Keep all dataset definitions explicit and versioned.

## 5. Storage Best Practices

### 5.1 Storage Layers

Use three storage layers from day one:

#### Raw

Purpose:
- replayability
- auditability
- recovery from parser bugs
- source verification

Format:
- Current MVP: `jsonl.gz` (gzip-compressed JSONL)
- Optional future optimization: `JSONL.zst`

Contents:
- raw API responses for key endpoints
- raw WebSocket messages for trades and optional orderbook streams
- raw chain event records from the indexer/query source

Rules:
- append-only
- never mutate old files
- partition by source and time

Current MVP layout:

```text
data/raw/source=gamma_markets/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=gamma_events/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=data_trades/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=clob_books/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=clob_midpoints/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=clob_spreads/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=clob_batch_prices_history/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=data_oi/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=data_holders/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
data/raw/source=ws_market/dt=YYYY-MM-DD/hour=HH/bucket_start=YYYYMMDDTHHMMSSZ_node=<node>.jsonl.gz
```

#### Silver

Purpose:
- normalized, typed, analyst-friendly canonical data

Format:
- `Parquet` with `zstd`

Contents:
- dimensions and facts with stable schemas

Rules:
- append-only partitions
- schema version tracked
- derived from raw plus deterministic parsing

#### Gold

Purpose:
- customer delivery
- packaged, documented, quality-checked datasets

Format:
- `Parquet` with `zstd`

Contents:
- curated datasets
- feature tables
- address/entity profile tables
- factor and analytics outputs

Rules:
- only publish validated datasets
- include manifest, schema, dictionary, and release notes

### 5.2 Why Parquet

Parquet should be the main delivery format because:

- quant teams and market makers can read it directly from Python, Spark, DuckDB, Polars, and ClickHouse
- it compresses repetitive tabular data well
- it supports column pruning and fast filtering
- it is much better than CSV and plain JSON for large-scale historical data delivery

### 5.3 Compression

- Use `zstd` for Parquet.
- Use `zstd` or `gzip` for raw JSONL.
- Prefer `zstd` when operationally convenient because it offers better compression and performance.

### 5.4 Partition Strategy

Recommended base partition scheme:

- `dataset=<name>/dt=YYYY-MM-DD/hour=HH/`

For high-volume tables, add a bucket key:

- `market_bucket=<nn>`
- `address_bucket=<nn>`

This prevents hot directories and too many files in one partition path.

### 5.5 File Size Targets

Avoid tiny files. Target:

- 64 MB to 512 MB per Parquet file for delivery-grade datasets
- smaller files are acceptable in early testing, but production should compact them

## 6. Core Datasets

The first release should define the following canonical datasets.

### 6.1 Dimension Tables

#### `markets`

Fields:
- `market_id`
- `event_id`
- `condition_id`
- `question`
- `slug`
- `description`
- `category`
- `tags`
- `status`
- `active`
- `closed`
- `resolved`
- `start_time`
- `end_time`
- `resolution_time`
- `created_at`
- `updated_at`
- `schema_version`
- `ts_ingest`

Notes:
- treat this as a slowly changing dimension
- preserve historical changes

#### `tokens`

Fields:
- `token_id`
- `market_id`
- `outcome`
- `side_index`
- `condition_id`
- `created_at`
- `ts_ingest`

### 6.2 Fact Tables

#### `price_snapshots`

Fields:
- `ts_event`
- `ts_ingest`
- `market_id`
- `token_id`
- `mid_price`
- `best_bid`
- `best_ask`
- `spread`
- `source`
- `ingest_run_id`

Recommended cadence:
- start with 1-minute snapshots
- optionally add 5-second or 15-second snapshots later for selected markets

#### `trades`

Fields:
- `trade_id`
- `ts_event`
- `ts_ingest`
- `market_id`
- `token_id`
- `price`
- `size`
- `notional`
- `side`
- `taker_order_id`
- `transaction_hash`
- `source`
- `ingest_run_id`

If maker-side detail is available:
- store maker order references in a child dataset or normalized mapping table

#### `order_fills_onchain`

Fields:
- `block_number`
- `block_timestamp`
- `transaction_hash`
- `log_index`
- `order_hash`
- `maker_address`
- `taker_address`
- `token_id`
- `maker_amount_filled`
- `taker_amount_filled`
- `fee`
- `source`
- `ingest_run_id`

#### `orderbook_l2`

Fields:
- `ts_event`
- `ts_ingest`
- `market_id`
- `token_id`
- `side`
- `price`
- `size`
- `level`
- `snapshot_id`

Guideline:
- do not enable full-market continuous L2 capture in the first production version unless clearly justified by a customer need

### 6.3 Matching and Enrichment Tables

#### `trade_order_links`

Purpose:
- map trade records to one or more order hashes

Fields:
- `trade_id`
- `order_hash`
- `role`
- `link_confidence`
- `matched_via`
- `ts_ingest`

#### `trade_address_links`

Purpose:
- map trades to one or more chain addresses

Fields:
- `trade_id`
- `order_hash`
- `maker_address`
- `taker_address`
- `link_confidence`
- `match_status`
- `match_notes`
- `ts_ingest`

#### `address_profiles`

Purpose:
- daily or periodic snapshots of address-level features

Fields:
- `address`
- `profile_date`
- `total_trades`
- `total_volume`
- `realized_pnl_proxy`
- `avg_holding_time`
- `maker_ratio`
- `taker_ratio`
- `win_rate_proxy`
- `early_entry_score`
- `market_diversity_score`
- `recency_score`
- `cluster_id`
- `profile_version`

## 7. Phase Plan

## Phase 0: Foundations

### Objective

Set up the project so future work does not create architectural debt.

### Tasks

- initialize a Python project with clear package structure
- define config loading strategy
- define dataset naming conventions
- define schema versioning conventions
- define partitioning conventions
- define state management conventions
- define log format and observability metrics
- define environment separation for local, staging, and production

### Deliverables

- project skeleton
- configuration files
- base schema registry
- local runbook
- production runbook

### Acceptance Criteria

- a new engineer can understand where ingestion, parsing, matching, and delivery logic belong
- no storage paths or dataset names are ad hoc

## Phase 1: Stable Ingestion MVP

### Objective

Continuously collect the highest-value market data with minimal operational complexity.

### Scope

Include:
- markets and events metadata
- periodic price snapshots
- trade history or trade stream
- chain event ingestion for fill-related events

Exclude:
- full historical backfill for every source
- complete long-term orderbook depth capture
- customer APIs

### Tasks

- implement market discovery collector
- implement metadata refresh loop
- implement periodic price snapshot collector
- implement trade collector
- implement chain event collector
- implement retries, timeout handling, and backoff
- implement checkpoint storage
- implement raw writer
- implement structured logging
- implement health heartbeat
- implement primary/backup heartbeat monitoring and failover state flags
- implement time-windowed incremental cloud backfill script
- implement post-backfill deduplication and gap validation

### Deliverables

- continuously running ingestion pipeline
- raw data partitioned by source and time
- basic operational metrics
- minimal failover and backfill tooling

### Acceptance Criteria

- process can run continuously for at least 72 hours
- no silent failures
- restart does not destroy state
- raw data is replayable
- cloud buffer continues during local outage and missing windows can be incrementally backfilled

## Phase 2: Standardized Data Lake

### Objective

Transform raw payloads into stable, research-grade datasets.

### Tasks

- define canonical schemas for all core tables
- build raw-to-silver parsers
- implement deterministic deduplication
- implement SCD logic for market metadata
- normalize timestamps to UTC
- compute notional and basic derived fields
- write partitioned Parquet datasets
- generate manifests per partition
- generate field dictionaries

### Deliverables

- silver datasets for `markets`, `tokens`, `price_snapshots`, `trades`, `order_fills_onchain`
- schema registry files
- manifest generation

### Acceptance Criteria

- repeated reprocessing of the same raw data produces consistent silver outputs
- schemas are explicit and versioned
- core tables are queryable with DuckDB or Polars without extra cleanup

## Phase 3: Trade-to-Address Matching

### Objective

Build a high-confidence linkage layer between off-chain trade records and on-chain addresses.

### Matching Strategy

Do not assume a perfect one-to-one match.

Primary matching keys:
- `taker_order_id`
- maker order IDs where available
- `order_hash`
- `transaction_hash`
- `token_id`
- `market_id`
- timestamp proximity
- amount and price consistency

### Tasks

- normalize API-side order references
- normalize chain-side order fill events
- define one-to-one, one-to-many, and ambiguous matching cases
- implement confidence scoring
- store unmatched and low-confidence cases separately
- generate audit samples for manual review

### Match Status Categories

- `exact`
- `high_confidence`
- `ambiguous`
- `unmatched`
- `invalid`

### Deliverables

- `trade_order_links`
- `trade_address_links`
- confidence scoring logic
- matching QA report

### Acceptance Criteria

- linkage process is reproducible
- ambiguous cases are explicitly labeled
- exact and high-confidence matches can be separated cleanly for downstream products

## Phase 4: Address Profiling and Feature Engineering

### Objective

Turn linked address data into commercially useful analytics.

### Feature Families

- activity features
- size and turnover features
- maker/taker behavior
- market timing behavior
- event-category preferences
- holding-duration proxies
- directional consistency
- recency-weighted performance proxies
- concentration and diversification metrics
- response-to-news or volatility-event behavior

### Tasks

- define feature generation windows: 1 day, 7 day, 30 day, all-time
- build address-level aggregates
- build market-level participant summaries
- build address clusters where justified
- define feature versioning
- create daily feature snapshots

### Deliverables

- `address_profiles`
- `market_participant_stats`
- `address_feature_snapshots`

### Acceptance Criteria

- features are reproducible from silver datasets
- feature definitions are documented and versioned
- outputs are usable directly by quant research teams

## Phase 5: Customer Delivery Layer

### Objective

Package data as a product customers can reliably consume.

### Tasks

- define release cadence: daily full pack plus optional hourly incrementals
- build release manifests
- build schema docs and changelogs
- build package validation checks
- define customer folder structure
- define licensing and data usage notice templates
- produce sample notebooks or query examples

### Deliverables

- gold datasets
- release manifests
- schema packages
- sample usage documentation

### Acceptance Criteria

- customers can ingest data without reverse-engineering field meanings
- every release can be validated via checksum and manifest
- schema changes are trackable

## Phase 6: Commercial Extensions

### Objective

Expand from data delivery into higher-value products and services.

### Potential Products

- smart money feeds
- address watchlists
- event-specific participant flows
- market-maker behavior analytics
- factor libraries for model input
- anomaly alerts
- risk dashboards
- premium historical bundles

### Potential Services

- query API
- filtered exports by market, address, category, or time
- custom research reports
- backtest-ready factor bundles
- daily signal subscriptions
- webhook or feed delivery for premium customers

### Acceptance Criteria

- new products are built on top of stable canonical datasets
- no product requires changing ingestion architecture fundamentally

## 8. Operational Best Practices

### 8.1 Runtime

- run collectors as supervised services using `systemd` or containers
- use one clear process per responsibility where possible
- keep ingestion jobs stateless except for explicit checkpoint state

### 8.2 State and Checkpoints

- store checkpoints in a dedicated `state/` area
- write state atomically
- never mix state files with deliverable datasets
- primary local node writes a heartbeat file for cloud-side health detection

### 8.3 Observability

Track at minimum:

- last successful ingestion timestamp
- events collected per minute
- raw files written per hour
- silver partitions written per hour
- retries and reconnects
- duplicate ratio
- unmatched trade ratio
- disk usage
- end-to-end latency

### 8.4 Alerting

Alert when:

- no data arrives for a defined interval
- retries exceed threshold
- disk free space drops below threshold
- partition generation fails
- schema mismatch appears
- unmatched linkage rate spikes

### 8.5 Retention

Recommended starting policy:

- raw hot storage: 7 to 30 days
- silver: long retention
- gold: long retention with release immutability
- logs: rotate aggressively

For primary/backup mode:

- local: keep `raw/silver/gold` long-term
- cloud: keep short-lived `raw` (recommended 3-7 days) and required release cache only
- enforce cloud partition cleanup to cap recurring storage cost

### 8.6 Backups

- back up manifests, schemas, and gold datasets daily
- retain enough raw data to rebuild recent silver/gold partitions
- verify restore procedures, not just backups

### 8.7 Sync and Backfill Policy

- partition convention
  - fixed `source + dt + hour` partitioning
  - file rotation triggered by time/size to avoid small-file explosions
- sync policy
  - no continuous full cloud-to-local sync in normal operation
  - sync only missing time-window partitions
- dedup policy
  - deduplicate with business keys (`trade_id`, `order_hash+log_index`, stream sequence keys)
  - produce a dedup report per backfill run
- integrity policy
  - validate row counts, time ranges, and checksums per partition after backfill
  - failed partitions are queued for re-fetch

## 9. Data Quality Best Practices

Implement automated checks for:

- null-rate anomalies
- invalid timestamps
- duplicate primary keys
- negative or impossible amounts
- unexpected price ranges
- partition completeness
- schema drift
- event ordering anomalies

Each dataset release should have:

- row counts
- min and max timestamps
- distinct market and token counts
- duplicate counts
- anomaly flags

## 10. Trade-to-Address Matching Guidance

This is commercially valuable, but should be treated as a scored inference layer.

Best practices:

- do not market ambiguous matches as ground truth
- keep confidence scores and evidence fields
- separate exact matches from inferred matches
- preserve all matching inputs for auditability
- treat proxy/funder/wallet-composition patterns carefully
- build the matching layer so it can be re-run when new heuristics are developed

Recommended output classes:

- exact on-chain match
- strong order-hash-based match
- strong transaction-plus-amount match
- low-confidence inferred match
- unresolved

## 11. Commercial Data Product Strategy

Do not stop at selling raw data dumps.

Best commercial packaging:

### Tier 1: Core Market Data

- market metadata
- price snapshots
- trades
- optional selected orderbook data

### Tier 2: Linked Address Data

- trade-address linkage
- address participation by market
- active address summaries
- participant flow views

### Tier 3: Analytics and Factors

- smart money scores
- address behavior factors
- event momentum participation
- signal-ready features
- anomaly and crowding indicators

### Tier 4: Services

- API access
- research support
- custom filtered bundles
- bespoke dashboards

## 12. Development Priorities

Build in this order:

1. reliable ingestion
2. clean schemas and Parquet outputs
3. matching layer
4. feature layer
5. customer packaging
6. premium services

This order minimizes wasted effort and creates useful outputs at every step.

## 13. Recommended First Release Scope

The first production release should include:

- `markets`
- `tokens`
- `price_snapshots`
- `trades`
- `order_fills_onchain`
- `trade_order_links`
- `trade_address_links`

It should not yet include:

- full continuous depth capture for every market
- customer-facing API service
- complex entity clustering beyond address-level linkage

## 14. Success Criteria

The project is on track when:

- ingestion runs stably for weeks, not hours
- raw data can be replayed into identical silver outputs
- customers can consume gold datasets without custom cleanup
- trade-address linking produces a meaningful exact/high-confidence segment
- new features can be added without redesigning the storage model

## 15. Next Implementation Step

After this planning document, the next concrete implementation task should be:

- scaffold the repository
- define the initial dataset schemas
- implement the ingestion MVP for markets, prices, trades, and chain fills
- write raw and silver outputs using the storage conventions in this document
