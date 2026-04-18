from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from polymarket_collector.backfill import backfill_raw_partitions
from polymarket_collector.collector import CollectorConfig, PolymarketCollector
from polymarket_collector.dedup import build_dedup_report
from polymarket_collector.parquet_build import build_parquet_from_raw
from polymarket_collector.runtime import run_backup_loop, run_primary_loop
from polymarket_collector.state_ops import (
    evaluate_failover,
    heartbeat_loop,
    write_failover_state,
    write_heartbeat,
)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    writer_node_id = args.writer_node_id
    if writer_node_id == "auto":
        writer_node_id = getattr(args, "node_id", "standalone")

    collector = PolymarketCollector(
        CollectorConfig(
            data_root=Path(args.data_root),
            timeout_seconds=args.timeout_seconds,
            clob_driver=args.clob_driver,
            bucket_seconds=args.bucket_seconds,
            writer_node_id=writer_node_id,
        )
    )

    if args.command == "discover-markets":
        markets, path = collector.discover_markets(limit=args.limit)
        _write_latest_markets(Path(args.data_root), markets)
        print(f"fetched_markets={len(markets)}")
        if path:
            print(f"written={path}")
        return 0

    if args.command == "discover-events":
        events, path = collector.discover_events(limit=args.limit)
        print(f"fetched_events={len(events)}")
        if path:
            print(f"written={path}")
        return 0

    if args.command == "fetch-trades":
        markets = _load_markets_file(Path(args.data_root), args.markets_file)
        condition_ids = _limit_values(collector.extract_condition_ids(markets), args.max_markets)
        trades, path = collector.fetch_trades(
            condition_ids=condition_ids or None,
            limit=args.limit,
            taker_only=args.taker_only,
        )
        print(f"fetched_trades={len(trades)}")
        if path:
            print(f"written={path}")
        return 0

    if args.command == "fetch-books":
        markets = _load_markets_file(Path(args.data_root), args.markets_file)
        asset_ids = collector.extract_asset_ids(markets, max_assets=args.max_assets)
        books, path = collector.fetch_books(token_ids=asset_ids)
        print(f"fetched_books={len(books)}")
        if path:
            print(f"written={path}")
        return 0

    if args.command == "fetch-midpoints":
        markets = _load_markets_file(Path(args.data_root), args.markets_file)
        asset_ids = collector.extract_asset_ids(markets, max_assets=args.max_assets)
        result, path = collector.fetch_midpoints(token_ids=asset_ids)
        print(f"fetched_midpoints_keys={len(result) if isinstance(result, dict) else 0}")
        if path:
            print(f"written={path}")
        return 0

    if args.command == "fetch-spreads":
        markets = _load_markets_file(Path(args.data_root), args.markets_file)
        asset_ids = collector.extract_asset_ids(markets, max_assets=args.max_assets)
        result, path = collector.fetch_spreads(token_ids=asset_ids)
        print(f"fetched_spreads_keys={len(result) if isinstance(result, dict) else 0}")
        if path:
            print(f"written={path}")
        return 0

    if args.command == "fetch-prices-history":
        markets = _load_markets_file(Path(args.data_root), args.markets_file)
        asset_ids = collector.extract_asset_ids(markets, max_assets=args.max_assets)
        result, path = collector.fetch_batch_prices_history(
            token_ids=asset_ids,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            interval=args.interval,
            fidelity=args.fidelity,
        )
        size = len(result) if isinstance(result, dict) else 0
        print(f"fetched_prices_history_keys={size}")
        if path:
            print(f"written={path}")
        return 0

    if args.command == "fetch-oi":
        markets = _load_markets_file(Path(args.data_root), args.markets_file)
        condition_ids = _limit_values(collector.extract_condition_ids(markets), args.max_markets)
        values, path = collector.fetch_open_interest(condition_ids=condition_ids or None)
        print(f"fetched_oi={len(values)}")
        if path:
            print(f"written={path}")
        return 0

    if args.command == "fetch-holders":
        markets = _load_markets_file(Path(args.data_root), args.markets_file)
        condition_ids = _limit_values(collector.extract_condition_ids(markets), args.max_markets)
        values, path = collector.fetch_holders(condition_ids=condition_ids or None)
        print(f"fetched_holders={len(values)}")
        if path:
            print(f"written={path}")
        return 0

    if args.command == "stream-market":
        markets = _load_markets_file(Path(args.data_root), args.markets_file)
        asset_ids = collector.extract_asset_ids(markets, max_assets=args.max_assets)
        message_count = collector.stream_market(
            asset_ids=asset_ids,
            duration_seconds=args.duration_seconds,
        )
        print(f"streamed_messages={message_count}")
        return 0

    if args.command == "heartbeat":
        extra = _parse_json_object(args.extra_json)
        path = write_heartbeat(
            data_root=Path(args.data_root),
            node_id=args.node_id,
            role=args.role,
            status=args.status,
            extra=extra,
        )
        print(f"written={path}")
        return 0

    if args.command == "heartbeat-loop":
        extra = _parse_json_object(args.extra_json)
        heartbeat_loop(
            data_root=Path(args.data_root),
            node_id=args.node_id,
            role=args.role,
            status=args.status,
            interval_seconds=args.interval_seconds,
            duration_seconds=args.duration_seconds,
            extra=extra,
        )
        print("heartbeat_loop_completed=true")
        return 0

    if args.command == "check-failover":
        state = evaluate_failover(
            data_root=Path(args.data_root),
            primary_node_id=args.primary_node_id,
            max_stale_seconds=args.max_stale_seconds,
        )
        print(json.dumps(state, ensure_ascii=True))
        if args.write_state:
            path = write_failover_state(data_root=Path(args.data_root), state=state)
            print(f"written={path}")
        return 0

    if args.command == "backfill-from-dir":
        start = _parse_iso_datetime(args.start)
        end = _parse_iso_datetime(args.end)
        stats = backfill_raw_partitions(
            source_root=Path(args.from_root),
            target_root=Path(args.data_root),
            start=start,
            end=end,
            sources=_parse_sources(args.sources),
            dry_run=args.dry_run,
        )
        report = stats.as_dict()
        report["dry_run"] = args.dry_run
        print(json.dumps(report, ensure_ascii=True))
        return 0

    if args.command == "dedup-report":
        start = _parse_iso_datetime(args.start)
        end = _parse_iso_datetime(args.end)
        report = build_dedup_report(
            data_root=Path(args.data_root),
            source=args.source,
            start=start,
            end=end,
            top_n=args.top_n,
        )
        output_path = _write_report(Path(args.data_root), report.as_dict(), prefix="dedup_report")
        print(json.dumps(report.as_dict(), ensure_ascii=True))
        print(f"written={output_path}")
        return 0

    if args.command == "build-parquet":
        start = _parse_iso_datetime(args.start) if args.start else None
        end = _parse_iso_datetime(args.end) if args.end else None
        try:
            stats = build_parquet_from_raw(
                data_root=Path(args.data_root),
                sources=_parse_sources(args.sources),
                start=start,
                end=end,
                overwrite=args.overwrite,
                dedup=not args.no_dedup,
            )
        except RuntimeError as exc:
            print(str(exc))
            return 1
        print(json.dumps(stats.as_dict(), ensure_ascii=True))
        return 0

    if args.command == "run-primary":
        run_primary_loop(
            collector=collector,
            data_root=Path(args.data_root),
            node_id=args.node_id,
            interval_seconds=args.interval_seconds,
            duration_seconds=args.duration_seconds,
            market_limit=args.market_limit,
            max_markets_for_trades=args.max_markets_for_trades,
            max_markets_for_oi_holders=args.max_markets_for_oi_holders,
            max_assets_for_books=args.max_assets_for_books,
            hot_assets_for_books=args.hot_assets_for_books,
            hot_snapshot_interval_seconds=args.hot_snapshot_interval_seconds,
            cold_snapshot_interval_seconds=args.cold_snapshot_interval_seconds,
            max_assets_for_history=args.max_assets_for_history,
            history_snapshot_interval_seconds=args.history_snapshot_interval_seconds,
            history_window_seconds=args.history_window_seconds,
            history_interval=args.history_interval,
            history_fidelity=args.history_fidelity,
            max_assets_for_ws=args.max_assets_for_ws,
            ws_duration_seconds=args.ws_duration_seconds,
            discover_all_pages=args.discover_all_pages,
            page_limit=args.page_limit,
            new_market_backfill_seconds=args.new_market_backfill_seconds,
            freeze_tracked_markets=args.freeze_tracked_markets,
            full_trades_for_tracked_markets=args.full_trades_for_tracked_markets,
            trade_page_limit=args.trade_page_limit,
            trade_max_offset=args.trade_max_offset,
            collect_midpoints=args.collect_midpoints,
            collect_spreads=args.collect_spreads,
        )
        print("run_primary_completed=true")
        return 0

    if args.command == "run-backup":
        run_backup_loop(
            collector=collector,
            data_root=Path(args.data_root),
            node_id=args.node_id,
            primary_node_id=args.primary_node_id,
            check_interval_seconds=args.check_interval_seconds,
            max_stale_seconds=args.max_stale_seconds,
            duration_seconds=args.duration_seconds,
            market_limit=args.market_limit,
            max_markets_for_trades=args.max_markets_for_trades,
            max_markets_for_oi_holders=args.max_markets_for_oi_holders,
            max_assets_for_books=args.max_assets_for_books,
            hot_assets_for_books=args.hot_assets_for_books,
            hot_snapshot_interval_seconds=args.hot_snapshot_interval_seconds,
            cold_snapshot_interval_seconds=args.cold_snapshot_interval_seconds,
            max_assets_for_history=args.max_assets_for_history,
            history_snapshot_interval_seconds=args.history_snapshot_interval_seconds,
            history_window_seconds=args.history_window_seconds,
            history_interval=args.history_interval,
            history_fidelity=args.history_fidelity,
            max_assets_for_ws=args.max_assets_for_ws,
            ws_duration_seconds=args.ws_duration_seconds,
            discover_all_pages=args.discover_all_pages,
            page_limit=args.page_limit,
            new_market_backfill_seconds=args.new_market_backfill_seconds,
            freeze_tracked_markets=args.freeze_tracked_markets,
            full_trades_for_tracked_markets=args.full_trades_for_tracked_markets,
            trade_page_limit=args.trade_page_limit,
            trade_max_offset=args.trade_max_offset,
            collect_midpoints=args.collect_midpoints,
            collect_spreads=args.collect_spreads,
        )
        print("run_backup_completed=true")
        return 0

    raise ValueError(f"unknown command: {args.command}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Minimal Polymarket public data collector")
    parser.add_argument("--data-root", default="data", help="Root directory for output data")
    parser.add_argument("--timeout-seconds", type=float, default=20.0, help="HTTP/WS timeout")
    parser.add_argument(
        "--bucket-seconds",
        type=int,
        default=3600,
        help="Fixed write bucket size in seconds (3600=hourly, 86400=daily)",
    )
    parser.add_argument(
        "--writer-node-id",
        default="auto",
        help="Writer node id embedded in bucket filename; 'auto' uses subcommand --node-id when available",
    )
    parser.add_argument(
        "--clob-driver",
        choices=("raw", "pyclob"),
        default="raw",
        help="CLOB client implementation: raw HTTP or official py-clob-client",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    discover = subparsers.add_parser("discover-markets", help="Fetch active markets")
    discover.add_argument("--limit", type=int, default=50, help="Number of active markets to fetch")

    events = subparsers.add_parser("discover-events", help="Fetch active events")
    events.add_argument("--limit", type=int, default=50, help="Number of active events to fetch")

    trades = subparsers.add_parser("fetch-trades", help="Fetch recent trades")
    trades.add_argument("--markets-file", default="latest", help="Path to markets JSON or 'latest'")
    trades.add_argument("--limit", type=int, default=500, help="Trade API limit")
    trades.add_argument(
        "--max-markets",
        type=int,
        default=20,
        help="Maximum number of market condition IDs to query in one call; <=0 means all",
    )
    trades.add_argument(
        "--taker-only",
        action="store_true",
        help="Set Data API takerOnly=true",
    )

    books = subparsers.add_parser("fetch-books", help="Fetch order book snapshots")
    books.add_argument("--markets-file", default="latest", help="Path to markets JSON or 'latest'")
    books.add_argument("--max-assets", type=int, default=20, help="Maximum number of asset IDs; <=0 means all")

    midpoints = subparsers.add_parser("fetch-midpoints", help="Fetch midpoint snapshots")
    midpoints.add_argument("--markets-file", default="latest", help="Path to markets JSON or 'latest'")
    midpoints.add_argument("--max-assets", type=int, default=20, help="Maximum number of asset IDs; <=0 means all")

    spreads = subparsers.add_parser("fetch-spreads", help="Fetch spread snapshots")
    spreads.add_argument("--markets-file", default="latest", help="Path to markets JSON or 'latest'")
    spreads.add_argument("--max-assets", type=int, default=20, help="Maximum number of asset IDs; <=0 means all")

    history = subparsers.add_parser("fetch-prices-history", help="Fetch batch price history")
    history.add_argument("--markets-file", default="latest", help="Path to markets JSON or 'latest'")
    history.add_argument("--max-assets", type=int, default=20, help="Maximum number of asset IDs; <=0 means all")
    history.add_argument("--start-ts", type=int, default=None, help="Unix start timestamp")
    history.add_argument("--end-ts", type=int, default=None, help="Unix end timestamp")
    history.add_argument(
        "--interval",
        default="1h",
        help="History interval. When start/end timestamps are provided, the collector normalizes this to all.",
    )
    history.add_argument("--fidelity", type=int, default=1, help="History fidelity")

    oi = subparsers.add_parser("fetch-oi", help="Fetch open interest")
    oi.add_argument("--markets-file", default="latest", help="Path to markets JSON or 'latest'")
    oi.add_argument("--max-markets", type=int, default=20, help="Maximum market condition IDs; <=0 means all")

    holders = subparsers.add_parser("fetch-holders", help="Fetch holders")
    holders.add_argument("--markets-file", default="latest", help="Path to markets JSON or 'latest'")
    holders.add_argument("--max-markets", type=int, default=20, help="Maximum market condition IDs; <=0 means all")

    stream = subparsers.add_parser("stream-market", help="Stream public market WebSocket")
    stream.add_argument("--markets-file", default="latest", help="Path to markets JSON or 'latest'")
    stream.add_argument("--max-assets", type=int, default=10, help="Maximum number of asset IDs; <=0 means all")
    stream.add_argument("--duration-seconds", type=int, default=60, help="Streaming duration")

    hb = subparsers.add_parser("heartbeat", help="Write one heartbeat state file")
    hb.add_argument("--node-id", default="local-primary", help="Node identifier")
    hb.add_argument("--role", default="primary", choices=("primary", "backup"), help="Node role")
    hb.add_argument("--status", default="ok", help="Node health status")
    hb.add_argument("--extra-json", default="{}", help="Optional JSON object with metadata")

    hb_loop = subparsers.add_parser("heartbeat-loop", help="Write heartbeat periodically")
    hb_loop.add_argument("--node-id", default="local-primary", help="Node identifier")
    hb_loop.add_argument("--role", default="primary", choices=("primary", "backup"), help="Node role")
    hb_loop.add_argument("--status", default="ok", help="Node health status")
    hb_loop.add_argument("--interval-seconds", type=int, default=60, help="Heartbeat interval")
    hb_loop.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="Loop duration; 0 means run forever",
    )
    hb_loop.add_argument("--extra-json", default="{}", help="Optional JSON object with metadata")

    failover = subparsers.add_parser("check-failover", help="Evaluate primary heartbeat staleness")
    failover.add_argument("--primary-node-id", default="local-primary", help="Primary node id")
    failover.add_argument(
        "--max-stale-seconds",
        type=int,
        default=180,
        help="Staleness threshold for primary heartbeat",
    )
    failover.add_argument(
        "--write-state",
        action="store_true",
        help="Write failover evaluation result to state/failover_state.json",
    )

    backfill = subparsers.add_parser("backfill-from-dir", help="Incremental raw backfill from source dir")
    backfill.add_argument("--from-root", required=True, help="Source root (cloud buffer directory)")
    backfill.add_argument("--start", required=True, help="Start time ISO8601, e.g. 2026-04-17T00:00:00Z")
    backfill.add_argument("--end", required=True, help="End time ISO8601, e.g. 2026-04-17T06:00:00Z")
    backfill.add_argument(
        "--sources",
        default=(
            "gamma_markets,gamma_events,data_trades,data_oi,data_holders,"
            "clob_books,clob_midpoints,clob_spreads,clob_batch_prices_history,ws_market"
        ),
        help="Comma-separated sources",
    )
    backfill.add_argument("--dry-run", action="store_true", help="Only report planned copy operations")

    dedup = subparsers.add_parser("dedup-report", help="Build duplicate-rate report for a source")
    dedup.add_argument(
        "--source",
        required=True,
        choices=(
            "gamma_markets",
            "gamma_events",
            "data_trades",
            "data_oi",
            "data_holders",
            "clob_books",
            "clob_midpoints",
            "clob_spreads",
            "clob_batch_prices_history",
            "ws_market",
        ),
        help="Raw source name",
    )
    dedup.add_argument("--start", required=True, help="Start time ISO8601")
    dedup.add_argument("--end", required=True, help="End time ISO8601")
    dedup.add_argument("--top-n", type=int, default=20, help="Number of top duplicate keys")

    parquet_cmd = subparsers.add_parser("build-parquet", help="Build parquet files from raw jsonl.gz")
    parquet_cmd.add_argument(
        "--sources",
        default="all",
        help="Comma-separated sources; use 'all' to scan every source in data/raw",
    )
    parquet_cmd.add_argument("--start", default=None, help="Optional start time ISO8601")
    parquet_cmd.add_argument("--end", default=None, help="Optional end time ISO8601")
    parquet_cmd.add_argument("--overwrite", action="store_true", help="Rewrite existing parquet files")
    parquet_cmd.add_argument("--no-dedup", action="store_true", help="Disable per-file dedup during build")

    primary = subparsers.add_parser("run-primary", help="Run primary collector loop with heartbeat")
    primary.add_argument("--node-id", default="local-primary", help="Primary node id")
    primary.add_argument("--interval-seconds", type=int, default=300, help="Cycle interval")
    primary.add_argument("--duration-seconds", type=int, default=0, help="Loop duration; 0 means forever")
    primary.add_argument("--market-limit", type=int, default=50, help="Markets fetched per cycle")
    primary.add_argument(
        "--max-markets-for-trades",
        type=int,
        default=20,
        help="Max condition IDs used in one trade fetch; <=0 means all tracked markets",
    )
    primary.add_argument(
        "--max-markets-for-oi-holders",
        type=int,
        default=300,
        help="Max condition IDs used for open-interest and holder snapshots each cycle; <=0 means all tracked markets",
    )
    primary.add_argument(
        "--max-assets-for-books",
        type=int,
        default=20,
        help="Max assets for book snapshots; <=0 means all tracked assets",
    )
    primary.add_argument(
        "--hot-assets-for-books",
        type=int,
        default=10,
        help="Hot-asset count within max-assets-for-books (higher snapshot frequency)",
    )
    primary.add_argument(
        "--hot-snapshot-interval-seconds",
        type=int,
        default=60,
        help="Snapshot interval for hot assets",
    )
    primary.add_argument(
        "--cold-snapshot-interval-seconds",
        type=int,
        default=300,
        help="Snapshot interval for cold assets",
    )
    primary.add_argument(
        "--max-assets-for-history",
        type=int,
        default=0,
        help="Max assets used for periodic price-history snapshots; <=0 means all tracked assets",
    )
    primary.add_argument(
        "--history-snapshot-interval-seconds",
        type=int,
        default=0,
        help="Snapshot interval for batch price history; set 0 to disable",
    )
    primary.add_argument(
        "--history-window-seconds",
        type=int,
        default=3600,
        help="Lookback window for each batch price-history snapshot",
    )
    primary.add_argument(
        "--history-interval",
        default="all",
        help="Interval passed to batch price history. Use all for timestamp-bounded snapshots; fidelity controls resolution.",
    )
    primary.add_argument(
        "--history-fidelity",
        type=int,
        default=1,
        help="Fidelity passed to batch price history",
    )
    primary.add_argument("--max-assets-for-ws", type=int, default=0, help="Max assets for WS stream; <=0 means all")
    primary.add_argument("--ws-duration-seconds", type=int, default=60, help="WS duration per cycle")
    primary.add_argument(
        "--discover-all-pages",
        action="store_true",
        help="Scan all active market pages each cycle",
    )
    primary.add_argument("--page-limit", type=int, default=500, help="Gamma page size when scanning all pages")
    primary.add_argument(
        "--new-market-backfill-seconds",
        type=int,
        default=0,
        help="History window used to bootstrap newly discovered tokens",
    )
    primary.add_argument(
        "--freeze-tracked-markets",
        action="store_true",
        help="Persist tracked condition IDs and asset IDs across cycles, appending new active markets and dropping inactive ones while preserving order",
    )
    primary.add_argument(
        "--full-trades-for-tracked-markets",
        action="store_true",
        help="Paginate trades for tracked markets and persist trade-frontier state for incremental full capture",
    )
    primary.add_argument(
        "--trade-page-limit",
        type=int,
        default=500,
        help="Trade page size for recent fetches or incremental pagination",
    )
    primary.add_argument(
        "--trade-max-offset",
        type=int,
        default=10000,
        help="Maximum offset walked during one incremental trade sync",
    )
    primary.add_argument(
        "--collect-midpoints",
        action="store_true",
        help="Collect midpoint snapshots together with book snapshots",
    )
    primary.add_argument(
        "--collect-spreads",
        action="store_true",
        help="Collect spread snapshots together with book snapshots",
    )

    backup = subparsers.add_parser("run-backup", help="Run backup failover loop")
    backup.add_argument("--node-id", default="cloud-backup", help="Backup node id")
    backup.add_argument("--primary-node-id", default="local-primary", help="Primary node id to monitor")
    backup.add_argument("--check-interval-seconds", type=int, default=60, help="Failover check interval")
    backup.add_argument("--max-stale-seconds", type=int, default=180, help="Primary staleness threshold")
    backup.add_argument("--duration-seconds", type=int, default=0, help="Loop duration; 0 means forever")
    backup.add_argument("--market-limit", type=int, default=50, help="Markets fetched per failover cycle")
    backup.add_argument(
        "--max-markets-for-trades",
        type=int,
        default=20,
        help="Max condition IDs used in one trade fetch; <=0 means all tracked markets",
    )
    backup.add_argument(
        "--max-markets-for-oi-holders",
        type=int,
        default=300,
        help="Max condition IDs used for open-interest and holder snapshots each active cycle; <=0 means all tracked markets",
    )
    backup.add_argument(
        "--max-assets-for-books",
        type=int,
        default=20,
        help="Max assets for book snapshots; <=0 means all tracked assets",
    )
    backup.add_argument(
        "--hot-assets-for-books",
        type=int,
        default=10,
        help="Hot-asset count within max-assets-for-books (higher snapshot frequency)",
    )
    backup.add_argument(
        "--hot-snapshot-interval-seconds",
        type=int,
        default=60,
        help="Snapshot interval for hot assets when backup is active",
    )
    backup.add_argument(
        "--cold-snapshot-interval-seconds",
        type=int,
        default=300,
        help="Snapshot interval for cold assets when backup is active",
    )
    backup.add_argument(
        "--max-assets-for-history",
        type=int,
        default=0,
        help="Max assets used for periodic price-history snapshots when backup is active; <=0 means all tracked assets",
    )
    backup.add_argument(
        "--history-snapshot-interval-seconds",
        type=int,
        default=0,
        help="Snapshot interval for batch price history when backup is active; set 0 to disable",
    )
    backup.add_argument(
        "--history-window-seconds",
        type=int,
        default=3600,
        help="Lookback window for each backup price-history snapshot",
    )
    backup.add_argument(
        "--history-interval",
        default="all",
        help="Interval passed to backup batch price history. Use all for timestamp-bounded snapshots; fidelity controls resolution.",
    )
    backup.add_argument(
        "--history-fidelity",
        type=int,
        default=1,
        help="Fidelity passed to backup batch price history",
    )
    backup.add_argument("--max-assets-for-ws", type=int, default=0, help="Max assets for WS stream; <=0 means all")
    backup.add_argument("--ws-duration-seconds", type=int, default=60, help="WS duration per active cycle")
    backup.add_argument(
        "--discover-all-pages",
        action="store_true",
        help="Scan all active market pages when backup is active",
    )
    backup.add_argument("--page-limit", type=int, default=500, help="Gamma page size when scanning all pages")
    backup.add_argument(
        "--new-market-backfill-seconds",
        type=int,
        default=0,
        help="History window used to bootstrap newly discovered tokens",
    )
    backup.add_argument(
        "--freeze-tracked-markets",
        action="store_true",
        help="Persist tracked condition IDs and asset IDs across cycles, appending new active markets and dropping inactive ones while preserving order",
    )
    backup.add_argument(
        "--full-trades-for-tracked-markets",
        action="store_true",
        help="Paginate trades for tracked markets and persist trade-frontier state for incremental full capture",
    )
    backup.add_argument(
        "--trade-page-limit",
        type=int,
        default=500,
        help="Trade page size for recent fetches or incremental pagination",
    )
    backup.add_argument(
        "--trade-max-offset",
        type=int,
        default=10000,
        help="Maximum offset walked during one incremental trade sync",
    )
    backup.add_argument(
        "--collect-midpoints",
        action="store_true",
        help="Collect midpoint snapshots together with book snapshots",
    )
    backup.add_argument(
        "--collect-spreads",
        action="store_true",
        help="Collect spread snapshots together with book snapshots",
    )

    return parser


def _write_latest_markets(data_root: Path, markets: list[dict[str, Any]]) -> Path:
    data_root.mkdir(parents=True, exist_ok=True)
    latest_path = data_root / "latest_markets.json"
    latest_path.write_text(json.dumps(markets, ensure_ascii=True, indent=2), encoding="utf-8")
    return latest_path


def _load_markets_file(data_root: Path, markets_file: str) -> list[dict[str, Any]]:
    if markets_file == "latest":
        path = data_root / "latest_markets.json"
    else:
        path = Path(markets_file)

    if not path.exists():
        raise FileNotFoundError(
            f"markets file not found: {path}. Run discover-markets first or pass --markets-file."
        )

    return json.loads(path.read_text(encoding="utf-8"))


def _parse_sources(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_iso_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _parse_json_object(value: str) -> dict[str, Any]:
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("--extra-json must be a JSON object")
    return payload


def _limit_values(values: list[str], limit: int) -> list[str]:
    if limit <= 0:
        return list(values)
    return values[:limit]


def _write_report(data_root: Path, payload: dict[str, Any], *, prefix: str) -> Path:
    reports_dir = data_root / "state" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)
    path = reports_dir / f"{prefix}_{now:%Y%m%dT%H%M%S}.json"
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return path
