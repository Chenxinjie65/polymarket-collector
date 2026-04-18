from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
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
    last_history_snapshot_at: float = 0.0


@dataclass(slots=True)
class TrackedMarketSelection:
    condition_ids: list[str]
    asset_ids: list[str]
    added_condition_ids: list[str] = field(default_factory=list)
    removed_condition_ids: list[str] = field(default_factory=list)
    added_asset_ids: list[str] = field(default_factory=list)
    removed_asset_ids: list[str] = field(default_factory=list)


def run_primary_loop(
    *,
    collector: PolymarketCollector,
    data_root: Path,
    node_id: str,
    interval_seconds: int,
    duration_seconds: int,
    market_limit: int,
    max_markets_for_trades: int,
    max_markets_for_oi_holders: int,
    max_assets_for_books: int,
    hot_assets_for_books: int,
    hot_snapshot_interval_seconds: int,
    cold_snapshot_interval_seconds: int,
    max_assets_for_history: int,
    history_snapshot_interval_seconds: int,
    history_window_seconds: int,
    history_interval: str,
    history_fidelity: int,
    max_assets_for_ws: int,
    ws_duration_seconds: int,
    discover_all_pages: bool,
    page_limit: int,
    new_market_backfill_seconds: int,
    freeze_tracked_markets: bool,
    full_trades_for_tracked_markets: bool,
    trade_page_limit: int,
    trade_max_offset: int,
    collect_midpoints: bool,
    collect_spreads: bool,
) -> None:
    universe_state = _load_universe_state(data_root)
    snapshot_state = SnapshotScheduleState()
    trade_frontier = _load_trade_frontier_state(data_root)
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
            tracked_selection = _resolve_tracked_market_selection(
                data_root=data_root,
                markets=markets,
                freeze_tracked_markets=freeze_tracked_markets,
            )
            event_count, event_warnings = _discover_events_for_cycle(
                collector=collector,
                data_root=data_root,
                discover_all_pages=discover_all_pages,
                market_limit=market_limit,
                page_limit=page_limit,
            )
            cycle_state = _collect_cycle(
                collector=collector,
                markets=markets,
                previous_state=universe_state,
                max_markets_for_trades=max_markets_for_trades,
                max_markets_for_oi_holders=max_markets_for_oi_holders,
                max_assets_for_books=max_assets_for_books,
                hot_assets_for_books=hot_assets_for_books,
                hot_snapshot_interval_seconds=hot_snapshot_interval_seconds,
                cold_snapshot_interval_seconds=cold_snapshot_interval_seconds,
                max_assets_for_history=max_assets_for_history,
                history_snapshot_interval_seconds=history_snapshot_interval_seconds,
                history_window_seconds=history_window_seconds,
                history_interval=history_interval,
                history_fidelity=history_fidelity,
                max_assets_for_ws=max_assets_for_ws,
                ws_duration_seconds=ws_duration_seconds,
                new_market_backfill_seconds=new_market_backfill_seconds,
                snapshot_state=snapshot_state,
                tracked_selection=tracked_selection,
                freeze_tracked_markets=freeze_tracked_markets,
                full_trades_for_tracked_markets=full_trades_for_tracked_markets,
                trade_page_limit=trade_page_limit,
                trade_max_offset=trade_max_offset,
                trade_frontier=trade_frontier,
                collect_midpoints=collect_midpoints,
                collect_spreads=collect_spreads,
            )
            universe_state = cycle_state["current_state"]
            trade_frontier = cycle_state["trade_frontier"]
            _save_universe_state(data_root, universe_state)
            _save_tracked_market_selection(data_root, cycle_state["tracked_selection"])
            _save_trade_frontier_state(data_root, trade_frontier)

            write_heartbeat(
                data_root=data_root,
                node_id=node_id,
                role="primary",
                status="ok",
                extra={
                    "mode": "collecting",
                    "cycle_ts": datetime.now(UTC).isoformat(),
                    "markets_count": len(markets),
                    "events_count": event_count,
                    "condition_ids_count": len(universe_state.condition_ids),
                    "asset_ids_count": len(universe_state.asset_ids),
                    "new_condition_ids_count": cycle_state["new_condition_ids_count"],
                    "new_asset_ids_count": cycle_state["new_asset_ids_count"],
                    "oi_holders_market_count": cycle_state["oi_holders_market_count"],
                    "books_hot_snapshot_count": cycle_state["books_hot_snapshot_count"],
                    "books_cold_snapshot_count": cycle_state["books_cold_snapshot_count"],
                    "history_snapshot_asset_count": cycle_state["history_snapshot_asset_count"],
                    "tracked_market_selection_mode": "persistent" if freeze_tracked_markets else "dynamic",
                    "tracked_condition_ids_count": cycle_state["tracked_condition_ids_count"],
                    "tracked_asset_ids_count": cycle_state["tracked_asset_ids_count"],
                    "tracked_added_condition_ids_count": cycle_state["tracked_added_condition_ids_count"],
                    "tracked_removed_condition_ids_count": cycle_state["tracked_removed_condition_ids_count"],
                    "tracked_added_asset_ids_count": cycle_state["tracked_added_asset_ids_count"],
                    "tracked_removed_asset_ids_count": cycle_state["tracked_removed_asset_ids_count"],
                    "trade_frontier_condition_count": len(trade_frontier),
                    "collection_warnings": event_warnings + cycle_state["collection_warnings"],
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
    max_markets_for_oi_holders: int,
    max_assets_for_books: int,
    hot_assets_for_books: int,
    hot_snapshot_interval_seconds: int,
    cold_snapshot_interval_seconds: int,
    max_assets_for_history: int,
    history_snapshot_interval_seconds: int,
    history_window_seconds: int,
    history_interval: str,
    history_fidelity: int,
    max_assets_for_ws: int,
    ws_duration_seconds: int,
    discover_all_pages: bool,
    page_limit: int,
    new_market_backfill_seconds: int,
    freeze_tracked_markets: bool,
    full_trades_for_tracked_markets: bool,
    trade_page_limit: int,
    trade_max_offset: int,
    collect_midpoints: bool,
    collect_spreads: bool,
) -> None:
    universe_state = _load_universe_state(data_root)
    snapshot_state = SnapshotScheduleState()
    trade_frontier = _load_trade_frontier_state(data_root)
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
                tracked_selection = _resolve_tracked_market_selection(
                    data_root=data_root,
                    markets=markets,
                    freeze_tracked_markets=freeze_tracked_markets,
                )
                event_count, event_warnings = _discover_events_for_cycle(
                    collector=collector,
                    data_root=data_root,
                    discover_all_pages=discover_all_pages,
                    market_limit=market_limit,
                    page_limit=page_limit,
                )
                cycle_state = _collect_cycle(
                    collector=collector,
                    markets=markets,
                    previous_state=universe_state,
                    max_markets_for_trades=max_markets_for_trades,
                    max_markets_for_oi_holders=max_markets_for_oi_holders,
                    max_assets_for_books=max_assets_for_books,
                    hot_assets_for_books=hot_assets_for_books,
                    hot_snapshot_interval_seconds=hot_snapshot_interval_seconds,
                    cold_snapshot_interval_seconds=cold_snapshot_interval_seconds,
                    max_assets_for_history=max_assets_for_history,
                    history_snapshot_interval_seconds=history_snapshot_interval_seconds,
                    history_window_seconds=history_window_seconds,
                    history_interval=history_interval,
                    history_fidelity=history_fidelity,
                    max_assets_for_ws=max_assets_for_ws,
                    ws_duration_seconds=ws_duration_seconds,
                    new_market_backfill_seconds=new_market_backfill_seconds,
                    snapshot_state=snapshot_state,
                    tracked_selection=tracked_selection,
                    freeze_tracked_markets=freeze_tracked_markets,
                    full_trades_for_tracked_markets=full_trades_for_tracked_markets,
                    trade_page_limit=trade_page_limit,
                    trade_max_offset=trade_max_offset,
                    trade_frontier=trade_frontier,
                    collect_midpoints=collect_midpoints,
                    collect_spreads=collect_spreads,
                )
                universe_state = cycle_state["current_state"]
                trade_frontier = cycle_state["trade_frontier"]
                _save_universe_state(data_root, universe_state)
                _save_tracked_market_selection(data_root, cycle_state["tracked_selection"])
                _save_trade_frontier_state(data_root, trade_frontier)

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
                        "events_count": event_count,
                        "condition_ids_count": len(universe_state.condition_ids),
                        "asset_ids_count": len(universe_state.asset_ids),
                        "new_condition_ids_count": cycle_state["new_condition_ids_count"],
                        "new_asset_ids_count": cycle_state["new_asset_ids_count"],
                        "oi_holders_market_count": cycle_state["oi_holders_market_count"],
                        "books_hot_snapshot_count": cycle_state["books_hot_snapshot_count"],
                        "books_cold_snapshot_count": cycle_state["books_cold_snapshot_count"],
                        "history_snapshot_asset_count": cycle_state["history_snapshot_asset_count"],
                        "tracked_market_selection_mode": "persistent" if freeze_tracked_markets else "dynamic",
                        "tracked_condition_ids_count": cycle_state["tracked_condition_ids_count"],
                        "tracked_asset_ids_count": cycle_state["tracked_asset_ids_count"],
                        "tracked_added_condition_ids_count": cycle_state["tracked_added_condition_ids_count"],
                        "tracked_removed_condition_ids_count": cycle_state["tracked_removed_condition_ids_count"],
                        "tracked_added_asset_ids_count": cycle_state["tracked_added_asset_ids_count"],
                        "tracked_removed_asset_ids_count": cycle_state["tracked_removed_asset_ids_count"],
                        "trade_frontier_condition_count": len(trade_frontier),
                        "collection_warnings": event_warnings + cycle_state["collection_warnings"],
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


def _write_latest_events(data_root: Path, events: list[dict[str, Any]]) -> Path:
    data_root.mkdir(parents=True, exist_ok=True)
    latest_path = data_root / "latest_events.json"
    latest_path.write_text(json.dumps(events, ensure_ascii=True, indent=2), encoding="utf-8")
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


def _discover_events_for_cycle(
    *,
    collector: PolymarketCollector,
    data_root: Path,
    discover_all_pages: bool,
    market_limit: int,
    page_limit: int,
) -> tuple[int, list[str]]:
    try:
        if discover_all_pages:
            events = collector.discover_events_all_pages(page_limit=page_limit)
        else:
            events, _ = collector.discover_events(limit=market_limit)
        _write_latest_events(data_root, events)
        return len(events), []
    except Exception as exc:  # noqa: BLE001
        return 0, [f"discover_events_failed:{exc}"]


def _collect_cycle(
    *,
    collector: PolymarketCollector,
    markets: list[dict[str, Any]],
    previous_state: UniverseState,
    max_markets_for_trades: int,
    max_markets_for_oi_holders: int,
    max_assets_for_books: int,
    hot_assets_for_books: int,
    hot_snapshot_interval_seconds: int,
    cold_snapshot_interval_seconds: int,
    max_assets_for_history: int,
    history_snapshot_interval_seconds: int,
    history_window_seconds: int,
    history_interval: str,
    history_fidelity: int,
    max_assets_for_ws: int,
    ws_duration_seconds: int,
    new_market_backfill_seconds: int,
    snapshot_state: SnapshotScheduleState,
    tracked_selection: TrackedMarketSelection,
    freeze_tracked_markets: bool,
    full_trades_for_tracked_markets: bool,
    trade_page_limit: int,
    trade_max_offset: int,
    trade_frontier: dict[str, Any],
    collect_midpoints: bool,
    collect_spreads: bool,
) -> dict[str, Any]:
    condition_ids = collector.extract_condition_ids(markets)
    asset_ids = collector.extract_asset_ids(markets)
    tracked_condition_ids = tracked_selection.condition_ids
    tracked_asset_ids = tracked_selection.asset_ids
    tracked_added_condition_ids = tracked_selection.added_condition_ids
    tracked_added_asset_ids = tracked_selection.added_asset_ids

    current_state = UniverseState(condition_ids=set(condition_ids), asset_ids=set(asset_ids))
    new_condition_ids = sorted(current_state.condition_ids - previous_state.condition_ids)
    new_asset_ids = sorted(current_state.asset_ids - previous_state.asset_ids)
    collection_warnings: list[str] = []

    # 1) Normal cycle collection for current known universe.
    try:
        tracked_trade_condition_ids = tracked_condition_ids[:max_markets_for_trades]
        if tracked_trade_condition_ids:
            if full_trades_for_tracked_markets:
                _, _, trade_frontier, hit_trade_offset_cap = collector.fetch_trades_incremental(
                    condition_ids=tracked_trade_condition_ids,
                    frontier_by_condition=trade_frontier,
                    page_limit=trade_page_limit,
                    max_offset=trade_max_offset,
                    taker_only=False,
                )
                if hit_trade_offset_cap:
                    collection_warnings.append("fetch_trades_reached_offset_cap")
            else:
                collector.fetch_trades(
                    condition_ids=tracked_trade_condition_ids,
                    limit=trade_page_limit,
                    taker_only=False,
                )
    except Exception as exc:  # noqa: BLE001
        collection_warnings.append(f"fetch_trades_failed:{exc}")

    try:
        books_hot_snapshot_count, books_cold_snapshot_count = _collect_tiered_book_snapshots(
            collector=collector,
            asset_ids=tracked_asset_ids,
            max_assets_for_books=max_assets_for_books,
            hot_assets_for_books=hot_assets_for_books,
            hot_snapshot_interval_seconds=hot_snapshot_interval_seconds,
            cold_snapshot_interval_seconds=cold_snapshot_interval_seconds,
            snapshot_state=snapshot_state,
            collect_midpoints=collect_midpoints,
            collect_spreads=collect_spreads,
        )
    except Exception as exc:  # noqa: BLE001
        books_hot_snapshot_count = 0
        books_cold_snapshot_count = 0
        collection_warnings.append(f"book_snapshot_failed:{exc}")

    oi_holders_market_count = max(0, min(max_markets_for_oi_holders, len(tracked_condition_ids)))
    if oi_holders_market_count > 0:
        oi_condition_ids = tracked_condition_ids[:oi_holders_market_count]
        try:
            collector.fetch_open_interest(condition_ids=oi_condition_ids)
        except Exception as exc:  # noqa: BLE001
            collection_warnings.append(f"fetch_open_interest_failed:{exc}")

        try:
            collector.fetch_holders(condition_ids=oi_condition_ids)
        except Exception as exc:  # noqa: BLE001
            collection_warnings.append(f"fetch_holders_failed:{exc}")

    try:
        history_snapshot_asset_count = _collect_history_snapshots(
            collector=collector,
            asset_ids=tracked_asset_ids,
            max_assets_for_history=max_assets_for_history,
            history_snapshot_interval_seconds=history_snapshot_interval_seconds,
            history_window_seconds=history_window_seconds,
            history_interval=history_interval,
            history_fidelity=history_fidelity,
            snapshot_state=snapshot_state,
        )
    except Exception as exc:  # noqa: BLE001
        history_snapshot_asset_count = 0
        collection_warnings.append(f"history_snapshot_failed:{exc}")

    if tracked_asset_ids[:max_assets_for_ws]:
        try:
            collector.stream_market(
                asset_ids=tracked_asset_ids[:max_assets_for_ws],
                duration_seconds=ws_duration_seconds,
                on_new_market=lambda event: _update_tracked_selection_from_ws_event(
                    selection=tracked_selection,
                    event=event,
                    add=True,
                ),
                on_market_resolved=lambda event: _update_tracked_selection_from_ws_event(
                    selection=tracked_selection,
                    event=event,
                    add=False,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            collection_warnings.append(f"stream_market_failed:{exc}")

    # 2) Bootstrap collection for newly discovered markets/tokens.
    bootstrap_trade_condition_ids = tracked_added_condition_ids if freeze_tracked_markets else new_condition_ids
    if bootstrap_trade_condition_ids:
        try:
            bootstrap_trade_condition_ids = bootstrap_trade_condition_ids[:max_markets_for_trades]
            if full_trades_for_tracked_markets:
                _, _, trade_frontier, hit_trade_offset_cap = collector.fetch_trades_incremental(
                    condition_ids=bootstrap_trade_condition_ids,
                    frontier_by_condition=trade_frontier,
                    page_limit=trade_page_limit,
                    max_offset=trade_max_offset,
                    taker_only=False,
                )
                if hit_trade_offset_cap:
                    collection_warnings.append("bootstrap_fetch_trades_reached_offset_cap")
            else:
                collector.fetch_trades(
                    condition_ids=bootstrap_trade_condition_ids,
                    limit=trade_page_limit,
                    taker_only=False,
                )
        except Exception as exc:  # noqa: BLE001
            collection_warnings.append(f"bootstrap_fetch_trades_failed:{exc}")

    bootstrap_warnings: list[str] = []
    bootstrap_asset_ids = tracked_added_asset_ids if freeze_tracked_markets else new_asset_ids
    if bootstrap_asset_ids:
        bootstrap_assets = bootstrap_asset_ids[:max_assets_for_books]
        try:
            collector.fetch_books(token_ids=bootstrap_assets)
            if collect_midpoints:
                collector.fetch_midpoints(token_ids=bootstrap_assets)
            if collect_spreads:
                collector.fetch_spreads(token_ids=bootstrap_assets)
        except Exception as exc:  # noqa: BLE001
            bootstrap_warnings.append(f"bootstrap_books_failed:{exc}")

        if max_assets_for_history > 0 and new_market_backfill_seconds > 0:
            now_ts = int(datetime.now(UTC).timestamp())
            start_ts = now_ts - max(new_market_backfill_seconds, 60)
            try:
                collector.fetch_batch_prices_history(
                    token_ids=bootstrap_assets[:max_assets_for_history],
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
        "oi_holders_market_count": oi_holders_market_count,
        "books_hot_snapshot_count": books_hot_snapshot_count,
        "books_cold_snapshot_count": books_cold_snapshot_count,
        "history_snapshot_asset_count": history_snapshot_asset_count,
        "tracked_condition_ids_count": len(tracked_selection.condition_ids),
        "tracked_asset_ids_count": len(tracked_selection.asset_ids),
        "tracked_added_condition_ids_count": len(tracked_selection.added_condition_ids),
        "tracked_removed_condition_ids_count": len(tracked_selection.removed_condition_ids),
        "tracked_added_asset_ids_count": len(tracked_selection.added_asset_ids),
        "tracked_removed_asset_ids_count": len(tracked_selection.removed_asset_ids),
        "collection_warnings": collection_warnings,
        "bootstrap_warnings": bootstrap_warnings,
        "trade_frontier": trade_frontier,
        "tracked_selection": tracked_selection,
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
    collect_midpoints: bool,
    collect_spreads: bool,
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
        if collect_midpoints:
            collector.fetch_midpoints(token_ids=hot_assets)
        if collect_spreads:
            collector.fetch_spreads(token_ids=hot_assets)
        snapshot_state.last_hot_snapshot_at = now

    if cold_due:
        collector.fetch_books(token_ids=cold_assets)
        if collect_midpoints:
            collector.fetch_midpoints(token_ids=cold_assets)
        if collect_spreads:
            collector.fetch_spreads(token_ids=cold_assets)
        snapshot_state.last_cold_snapshot_at = now

    return len(hot_assets) if hot_due else 0, len(cold_assets) if cold_due else 0


def _collect_history_snapshots(
    *,
    collector: PolymarketCollector,
    asset_ids: list[str],
    max_assets_for_history: int,
    history_snapshot_interval_seconds: int,
    history_window_seconds: int,
    history_interval: str,
    history_fidelity: int,
    snapshot_state: SnapshotScheduleState,
) -> int:
    assets = asset_ids[:max_assets_for_history]
    if not assets or history_snapshot_interval_seconds <= 0:
        return 0

    now = time.monotonic()
    due = (
        snapshot_state.last_history_snapshot_at <= 0
        or now - snapshot_state.last_history_snapshot_at >= max(history_snapshot_interval_seconds, 1)
    )
    if not due:
        return 0

    now_ts = int(datetime.now(UTC).timestamp())
    start_ts = now_ts - max(history_window_seconds, 60)
    collector.fetch_batch_prices_history(
        token_ids=assets,
        start_ts=start_ts,
        end_ts=now_ts,
        interval=history_interval,
        fidelity=history_fidelity,
    )
    snapshot_state.last_history_snapshot_at = now
    return len(assets)


def _load_universe_state(data_root: Path) -> UniverseState:
    path = data_root / "state" / "market_universe.json"
    if not path.exists():
        return UniverseState(condition_ids=set(), asset_ids=set())
    payload = json.loads(path.read_text(encoding="utf-8"))
    condition_ids = payload.get("condition_ids") or []
    asset_ids = payload.get("asset_ids") or []
    return UniverseState(condition_ids=set(condition_ids), asset_ids=set(asset_ids))


def _resolve_tracked_market_selection(
    *,
    data_root: Path,
    markets: list[dict[str, Any]],
    freeze_tracked_markets: bool,
) -> TrackedMarketSelection:
    if not freeze_tracked_markets:
        return TrackedMarketSelection(
            condition_ids=PolymarketCollector.extract_condition_ids(markets),
            asset_ids=PolymarketCollector.extract_asset_ids(markets),
        )

    active_condition_ids = PolymarketCollector.extract_condition_ids(markets)
    active_asset_ids = PolymarketCollector.extract_asset_ids(markets)
    existing = _load_tracked_market_selection(data_root)

    if existing is None:
        selection = TrackedMarketSelection(
            condition_ids=active_condition_ids,
            asset_ids=active_asset_ids,
            added_condition_ids=active_condition_ids,
            added_asset_ids=active_asset_ids,
        )
        _save_tracked_market_selection(data_root, selection)
        return selection

    selection = _merge_tracked_market_selection(
        existing=existing,
        active_condition_ids=active_condition_ids,
        active_asset_ids=active_asset_ids,
    )
    _save_tracked_market_selection(data_root, selection)
    return selection


def _load_tracked_market_selection(data_root: Path) -> TrackedMarketSelection | None:
    path = data_root / "state" / "tracked_market_selection.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    condition_ids = payload.get("condition_ids") or []
    asset_ids = payload.get("asset_ids") or []
    return TrackedMarketSelection(
        condition_ids=[str(item) for item in condition_ids if item],
        asset_ids=[str(item) for item in asset_ids if item],
    )


def _merge_tracked_market_selection(
    *,
    existing: TrackedMarketSelection,
    active_condition_ids: list[str],
    active_asset_ids: list[str],
) -> TrackedMarketSelection:
    next_condition_ids, added_condition_ids, removed_condition_ids = _merge_persistent_ids(
        existing_ids=existing.condition_ids,
        active_ids=active_condition_ids,
    )
    next_asset_ids, added_asset_ids, removed_asset_ids = _merge_persistent_ids(
        existing_ids=existing.asset_ids,
        active_ids=active_asset_ids,
    )
    return TrackedMarketSelection(
        condition_ids=next_condition_ids,
        asset_ids=next_asset_ids,
        added_condition_ids=added_condition_ids,
        removed_condition_ids=removed_condition_ids,
        added_asset_ids=added_asset_ids,
        removed_asset_ids=removed_asset_ids,
    )


def _merge_persistent_ids(*, existing_ids: list[str], active_ids: list[str]) -> tuple[list[str], list[str], list[str]]:
    active_set = set(active_ids)
    existing_set = set(existing_ids)
    kept = [item for item in existing_ids if item in active_set]
    removed = [item for item in existing_ids if item not in active_set]
    added = [item for item in active_ids if item not in existing_set]
    return kept + added, added, removed


def _update_tracked_selection_from_ws_event(
    *,
    selection: TrackedMarketSelection,
    event: dict[str, Any],
    add: bool,
) -> None:
    condition_id = _extract_ws_condition_id(event)
    asset_ids = _extract_ws_asset_ids(event)
    if add:
        _append_tracked_ids(selection.condition_ids, [condition_id] if condition_id else [])
        _append_tracked_ids(selection.asset_ids, asset_ids)
        _append_tracked_ids(selection.added_condition_ids, [condition_id] if condition_id else [])
        _append_tracked_ids(selection.added_asset_ids, asset_ids)
        if condition_id:
            _remove_tracked_ids(selection.removed_condition_ids, [condition_id])
        _remove_tracked_ids(selection.removed_asset_ids, asset_ids)
        return

    _remove_tracked_ids(selection.condition_ids, [condition_id] if condition_id else [])
    _remove_tracked_ids(selection.asset_ids, asset_ids)
    _append_tracked_ids(selection.removed_condition_ids, [condition_id] if condition_id else [])
    _append_tracked_ids(selection.removed_asset_ids, asset_ids)
    if condition_id:
        _remove_tracked_ids(selection.added_condition_ids, [condition_id])
    _remove_tracked_ids(selection.added_asset_ids, asset_ids)


def _extract_ws_condition_id(event: dict[str, Any]) -> str | None:
    for key in ("condition_id", "market"):
        value = event.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _extract_ws_asset_ids(event: dict[str, Any]) -> list[str]:
    values = event.get("assets_ids")
    if isinstance(values, list):
        return [str(item) for item in values if item]

    token_ids = event.get("clob_token_ids")
    if isinstance(token_ids, list):
        return [str(item) for item in token_ids if item]

    asset_id = event.get("asset_id")
    if isinstance(asset_id, str) and asset_id:
        return [asset_id]
    return []


def _append_tracked_ids(target: list[str], values: list[str]) -> None:
    seen = set(target)
    for value in values:
        if not value or value in seen:
            continue
        target.append(value)
        seen.add(value)


def _remove_tracked_ids(target: list[str], values: list[str]) -> None:
    if not values:
        return
    to_remove = set(value for value in values if value)
    if not to_remove:
        return
    target[:] = [item for item in target if item not in to_remove]


def _save_tracked_market_selection(data_root: Path, selection: TrackedMarketSelection) -> Path:
    state_dir = data_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / "tracked_market_selection.json"
    payload = {
        "ts_initialized": datetime.now(UTC).isoformat(),
        "condition_ids": selection.condition_ids,
        "asset_ids": selection.asset_ids,
    }
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return path


def _load_trade_frontier_state(data_root: Path) -> dict[str, Any]:
    path = data_root / "state" / "trade_frontier.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    frontier = payload.get("frontier_by_condition")
    return frontier if isinstance(frontier, dict) else {}


def _save_trade_frontier_state(data_root: Path, frontier: dict[str, Any]) -> Path:
    state_dir = data_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / "trade_frontier.json"
    payload = {
        "ts": datetime.now(UTC).isoformat(),
        "frontier_by_condition": frontier,
    }
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return path


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
