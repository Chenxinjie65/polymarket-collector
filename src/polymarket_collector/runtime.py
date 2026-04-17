from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from polymarket_collector.collector import PolymarketCollector
from polymarket_collector.state_ops import evaluate_failover, write_heartbeat


@dataclass(slots=True)
class UniverseState:
    condition_ids: set[str]
    asset_ids: set[str]


@dataclass(slots=True)
class SnapshotScheduleState:
    last_hot_snapshot_at: float = 0.0
    last_cold_snapshot_at: float = 0.0


def run_primary_loop(
    *,
    collector: PolymarketCollector,
    data_root: Path,
    node_id: str,
    interval_seconds: int,
    duration_seconds: int,
    market_limit: int,
    max_markets_for_trades: int,
    max_assets_for_books: int,
    hot_assets_for_books: int,
    hot_snapshot_interval_seconds: int,
    cold_snapshot_interval_seconds: int,
    max_assets_for_ws: int,
    ws_duration_seconds: int,
    discover_all_pages: bool,
    page_limit: int,
    new_market_backfill_seconds: int,
) -> None:
    universe_state = _load_universe_state(data_root)
    snapshot_state = SnapshotScheduleState()
    start = time.monotonic()
    while True:
        cycle_started = time.monotonic()
        try:
            markets = _discover_markets_for_cycle(
                collector=collector,
                discover_all_pages=discover_all_pages,
                market_limit=market_limit,
                page_limit=page_limit,
            )
            _write_latest_markets(data_root, markets)
            cycle_state = _collect_cycle(
                collector=collector,
                markets=markets,
                previous_state=universe_state,
                max_markets_for_trades=max_markets_for_trades,
                max_assets_for_books=max_assets_for_books,
                hot_assets_for_books=hot_assets_for_books,
                hot_snapshot_interval_seconds=hot_snapshot_interval_seconds,
                cold_snapshot_interval_seconds=cold_snapshot_interval_seconds,
                max_assets_for_ws=max_assets_for_ws,
                ws_duration_seconds=ws_duration_seconds,
                new_market_backfill_seconds=new_market_backfill_seconds,
                snapshot_state=snapshot_state,
            )
            universe_state = cycle_state["current_state"]
            _save_universe_state(data_root, universe_state)

            write_heartbeat(
                data_root=data_root,
                node_id=node_id,
                role="primary",
                status="ok",
                extra={
                    "mode": "collecting",
                    "cycle_ts": datetime.now(UTC).isoformat(),
                    "markets_count": len(markets),
                    "condition_ids_count": len(universe_state.condition_ids),
                    "asset_ids_count": len(universe_state.asset_ids),
                    "new_condition_ids_count": cycle_state["new_condition_ids_count"],
                    "new_asset_ids_count": cycle_state["new_asset_ids_count"],
                    "books_hot_snapshot_count": cycle_state["books_hot_snapshot_count"],
                    "books_cold_snapshot_count": cycle_state["books_cold_snapshot_count"],
                    "bootstrap_warnings": cycle_state["bootstrap_warnings"],
                },
            )
        except Exception as exc:  # noqa: BLE001
            write_heartbeat(
                data_root=data_root,
                node_id=node_id,
                role="primary",
                status="error",
                extra={
                    "mode": "collecting",
                    "error": str(exc),
                    "cycle_ts": datetime.now(UTC).isoformat(),
                },
            )

        if duration_seconds > 0 and (time.monotonic() - start) >= duration_seconds:
            return

        elapsed = time.monotonic() - cycle_started
        sleep_seconds = max(0, interval_seconds - int(elapsed))
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def run_backup_loop(
    *,
    collector: PolymarketCollector,
    data_root: Path,
    node_id: str,
    primary_node_id: str,
    check_interval_seconds: int,
    max_stale_seconds: int,
    duration_seconds: int,
    market_limit: int,
    max_markets_for_trades: int,
    max_assets_for_books: int,
    hot_assets_for_books: int,
    hot_snapshot_interval_seconds: int,
    cold_snapshot_interval_seconds: int,
    max_assets_for_ws: int,
    ws_duration_seconds: int,
    discover_all_pages: bool,
    page_limit: int,
    new_market_backfill_seconds: int,
) -> None:
    universe_state = _load_universe_state(data_root)
    snapshot_state = SnapshotScheduleState()
    start = time.monotonic()
    while True:
        cycle_started = time.monotonic()
        try:
            decision = evaluate_failover(
                data_root=data_root,
                primary_node_id=primary_node_id,
                max_stale_seconds=max_stale_seconds,
            )
            if decision["recommend_backup_collect"]:
                markets = _discover_markets_for_cycle(
                    collector=collector,
                    discover_all_pages=discover_all_pages,
                    market_limit=market_limit,
                    page_limit=page_limit,
                )
                _write_latest_markets(data_root, markets)
                cycle_state = _collect_cycle(
                    collector=collector,
                    markets=markets,
                    previous_state=universe_state,
                    max_markets_for_trades=max_markets_for_trades,
                    max_assets_for_books=max_assets_for_books,
                    hot_assets_for_books=hot_assets_for_books,
                    hot_snapshot_interval_seconds=hot_snapshot_interval_seconds,
                    cold_snapshot_interval_seconds=cold_snapshot_interval_seconds,
                    max_assets_for_ws=max_assets_for_ws,
                    ws_duration_seconds=ws_duration_seconds,
                    new_market_backfill_seconds=new_market_backfill_seconds,
                    snapshot_state=snapshot_state,
                )
                universe_state = cycle_state["current_state"]
                _save_universe_state(data_root, universe_state)

                write_heartbeat(
                    data_root=data_root,
                    node_id=node_id,
                    role="backup",
                    status="ok",
                    extra={
                        "mode": "active",
                        "failover_decision": decision,
                        "cycle_ts": datetime.now(UTC).isoformat(),
                        "markets_count": len(markets),
                        "condition_ids_count": len(universe_state.condition_ids),
                        "asset_ids_count": len(universe_state.asset_ids),
                        "new_condition_ids_count": cycle_state["new_condition_ids_count"],
                        "new_asset_ids_count": cycle_state["new_asset_ids_count"],
                        "books_hot_snapshot_count": cycle_state["books_hot_snapshot_count"],
                        "books_cold_snapshot_count": cycle_state["books_cold_snapshot_count"],
                        "bootstrap_warnings": cycle_state["bootstrap_warnings"],
                    },
                )
            else:
                write_heartbeat(
                    data_root=data_root,
                    node_id=node_id,
                    role="backup",
                    status="ok",
                    extra={
                        "mode": "standby",
                        "failover_decision": decision,
                        "cycle_ts": datetime.now(UTC).isoformat(),
                    },
                )
        except Exception as exc:  # noqa: BLE001
            write_heartbeat(
                data_root=data_root,
                node_id=node_id,
                role="backup",
                status="error",
                extra={
                    "mode": "standby",
                    "error": str(exc),
                    "cycle_ts": datetime.now(UTC).isoformat(),
                },
            )

        if duration_seconds > 0 and (time.monotonic() - start) >= duration_seconds:
            return

        elapsed = time.monotonic() - cycle_started
        sleep_seconds = max(0, check_interval_seconds - int(elapsed))
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def _write_latest_markets(data_root: Path, markets: list[dict[str, Any]]) -> Path:
    data_root.mkdir(parents=True, exist_ok=True)
    latest_path = data_root / "latest_markets.json"
    latest_path.write_text(json.dumps(markets, ensure_ascii=True, indent=2), encoding="utf-8")
    return latest_path


def _discover_markets_for_cycle(
    *,
    collector: PolymarketCollector,
    discover_all_pages: bool,
    market_limit: int,
    page_limit: int,
) -> list[dict[str, Any]]:
    if discover_all_pages:
        return collector.discover_markets_all_pages(page_limit=page_limit)
    markets, _ = collector.discover_markets(limit=market_limit)
    return markets


def _collect_cycle(
    *,
    collector: PolymarketCollector,
    markets: list[dict[str, Any]],
    previous_state: UniverseState,
    max_markets_for_trades: int,
    max_assets_for_books: int,
    hot_assets_for_books: int,
    hot_snapshot_interval_seconds: int,
    cold_snapshot_interval_seconds: int,
    max_assets_for_ws: int,
    ws_duration_seconds: int,
    new_market_backfill_seconds: int,
    snapshot_state: SnapshotScheduleState,
) -> dict[str, Any]:
    condition_ids = collector.extract_condition_ids(markets)
    asset_ids = collector.extract_asset_ids(markets)

    current_state = UniverseState(condition_ids=set(condition_ids), asset_ids=set(asset_ids))
    new_condition_ids = sorted(current_state.condition_ids - previous_state.condition_ids)
    new_asset_ids = sorted(current_state.asset_ids - previous_state.asset_ids)

    # 1) Normal cycle collection for current known universe.
    collector.fetch_trades(
        condition_ids=condition_ids[:max_markets_for_trades] or None,
        limit=500,
        taker_only=False,
    )
    books_hot_snapshot_count, books_cold_snapshot_count = _collect_tiered_book_snapshots(
        collector=collector,
        asset_ids=asset_ids,
        max_assets_for_books=max_assets_for_books,
        hot_assets_for_books=hot_assets_for_books,
        hot_snapshot_interval_seconds=hot_snapshot_interval_seconds,
        cold_snapshot_interval_seconds=cold_snapshot_interval_seconds,
        snapshot_state=snapshot_state,
    )

    if asset_ids[:max_assets_for_ws]:
        collector.stream_market(
            asset_ids=asset_ids[:max_assets_for_ws],
            duration_seconds=ws_duration_seconds,
        )

    # 2) Bootstrap collection for newly discovered markets/tokens.
    if new_condition_ids:
        collector.fetch_trades(
            condition_ids=new_condition_ids[:max_markets_for_trades],
            limit=500,
            taker_only=False,
        )

    bootstrap_warnings: list[str] = []
    if new_asset_ids:
        bootstrap_assets = new_asset_ids[:max_assets_for_books]
        collector.fetch_books(token_ids=bootstrap_assets)
        collector.fetch_midpoints(token_ids=bootstrap_assets)
        collector.fetch_spreads(token_ids=bootstrap_assets)

        now_ts = int(datetime.now(UTC).timestamp())
        start_ts = now_ts - max(new_market_backfill_seconds, 60)
        try:
            collector.fetch_batch_prices_history(
                token_ids=bootstrap_assets,
                start_ts=start_ts,
                end_ts=now_ts,
                interval="1m",
                fidelity=1,
            )
        except Exception as exc:  # noqa: BLE001
            bootstrap_warnings.append(f"batch_prices_history_failed:{exc}")

    return {
        "current_state": current_state,
        "new_condition_ids_count": len(new_condition_ids),
        "new_asset_ids_count": len(new_asset_ids),
        "books_hot_snapshot_count": books_hot_snapshot_count,
        "books_cold_snapshot_count": books_cold_snapshot_count,
        "bootstrap_warnings": bootstrap_warnings,
    }


def _collect_tiered_book_snapshots(
    *,
    collector: PolymarketCollector,
    asset_ids: list[str],
    max_assets_for_books: int,
    hot_assets_for_books: int,
    hot_snapshot_interval_seconds: int,
    cold_snapshot_interval_seconds: int,
    snapshot_state: SnapshotScheduleState,
) -> tuple[int, int]:
    assets = asset_ids[:max_assets_for_books]
    if not assets:
        return 0, 0

    hot_count = max(0, min(hot_assets_for_books, len(assets)))
    hot_assets = assets[:hot_count]
    cold_assets = assets[hot_count:]

    now = time.monotonic()
    hot_due = bool(hot_assets) and (
        snapshot_state.last_hot_snapshot_at <= 0
        or now - snapshot_state.last_hot_snapshot_at >= max(hot_snapshot_interval_seconds, 1)
    )
    cold_due = bool(cold_assets) and (
        snapshot_state.last_cold_snapshot_at <= 0
        or now - snapshot_state.last_cold_snapshot_at >= max(cold_snapshot_interval_seconds, 1)
    )

    if hot_due:
        collector.fetch_books(token_ids=hot_assets)
        collector.fetch_midpoints(token_ids=hot_assets)
        collector.fetch_spreads(token_ids=hot_assets)
        snapshot_state.last_hot_snapshot_at = now

    if cold_due:
        collector.fetch_books(token_ids=cold_assets)
        collector.fetch_midpoints(token_ids=cold_assets)
        collector.fetch_spreads(token_ids=cold_assets)
        snapshot_state.last_cold_snapshot_at = now

    return len(hot_assets) if hot_due else 0, len(cold_assets) if cold_due else 0


def _load_universe_state(data_root: Path) -> UniverseState:
    path = data_root / "state" / "market_universe.json"
    if not path.exists():
        return UniverseState(condition_ids=set(), asset_ids=set())
    payload = json.loads(path.read_text(encoding="utf-8"))
    condition_ids = payload.get("condition_ids") or []
    asset_ids = payload.get("asset_ids") or []
    return UniverseState(condition_ids=set(condition_ids), asset_ids=set(asset_ids))


def _save_universe_state(data_root: Path, state: UniverseState) -> Path:
    state_dir = data_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / "market_universe.json"
    payload = {
        "ts": datetime.now(UTC).isoformat(),
        "condition_ids": sorted(state.condition_ids),
        "asset_ids": sorted(state.asset_ids),
    }
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return path
