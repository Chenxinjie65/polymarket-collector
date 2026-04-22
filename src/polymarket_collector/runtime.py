from __future__ import annotations

import concurrent.futures
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from polymarket_collector.collector import CollectorConfig, PolymarketCollector
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


class _BackgroundWsCollector:
    def __init__(
        self,
        *,
        collector_config: CollectorConfig,
        data_root: Path,
        worker_count: int,
        max_assets_for_ws: int,
        ws_duration_seconds: int,
        flush_every_messages: int,
        flush_every_seconds: int,
        subscribe_batch_size: int,
    ) -> None:
        self.collector_config = collector_config
        self.data_root = data_root
        self.worker_count = max(1, worker_count)
        self.max_assets_for_ws = max_assets_for_ws
        self.ws_duration_seconds = max(1, ws_duration_seconds)
        self.flush_every_messages = max(1, flush_every_messages)
        self.flush_every_seconds = max(1, flush_every_seconds)
        self.subscribe_batch_size = max(1, subscribe_batch_size)
        self._selection: TrackedMarketSelection | None = None
        self._selection_lock = threading.Lock()
        self._worker_errors: list[str | None] = [None for _ in range(self.worker_count)]
        self._error_lock = threading.Lock()
        self._worker_message_counts: list[int] = [0 for _ in range(self.worker_count)]
        self._stats_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._threads = [
            threading.Thread(
                target=self._run_worker,
                args=(worker_index,),
                name=f"polymarket-ws-{worker_index}",
                daemon=True,
            )
            for worker_index in range(self.worker_count)
        ]

    def start(self) -> None:
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        for thread in self._threads:
            thread.join(timeout=self.ws_duration_seconds + 5)

    def update_selection(self, selection: TrackedMarketSelection) -> None:
        with self._selection_lock:
            self._selection = _clone_tracked_market_selection(selection)

    def snapshot_selection(self) -> TrackedMarketSelection | None:
        with self._selection_lock:
            if self._selection is None:
                return None
            return _clone_tracked_market_selection(self._selection)

    def last_error(self) -> str | None:
        with self._error_lock:
            errors = [error for error in self._worker_errors if error]
        return ";".join(errors) if errors else None

    def _set_worker_error(self, worker_index: int, value: str | None) -> None:
        with self._error_lock:
            self._worker_errors[worker_index] = value

    def consume_stats(self) -> dict[str, int]:
        with self._stats_lock:
            total_messages = sum(self._worker_message_counts)
            self._worker_message_counts = [0 for _ in range(self.worker_count)]
        return {"messages_in_cycle": total_messages}

    def _run_worker(self, worker_index: int) -> None:
        collector = PolymarketCollector(self.collector_config)
        while not self._stop_event.is_set():
            selection = self.snapshot_selection()
            if selection is None:
                self._stop_event.wait(1)
                continue

            asset_ids = _shard_items(
                values=_limit_items(selection.asset_ids, self.max_assets_for_ws),
                shard_count=self.worker_count,
                shard_index=worker_index,
            )
            if not asset_ids:
                self._stop_event.wait(1)
                continue

            try:
                message_count = collector.stream_market(
                    asset_ids=asset_ids,
                    duration_seconds=self.ws_duration_seconds,
                    flush_every_messages=self.flush_every_messages,
                    flush_every_seconds=self.flush_every_seconds,
                    subscribe_batch_size=self.subscribe_batch_size,
                    on_new_market=lambda event: self._handle_event(event=event, add=True),
                    on_market_resolved=lambda event: self._handle_event(event=event, add=False),
                )
                with self._stats_lock:
                    self._worker_message_counts[worker_index] += max(0, int(message_count))
                self._set_worker_error(worker_index, None)
            except Exception as exc:  # noqa: BLE001
                self._set_worker_error(worker_index, f"background_stream_market_failed[{worker_index}]:{exc}")
                if self._stop_event.wait(5):
                    return

    def _handle_event(self, *, event: dict[str, Any], add: bool) -> None:
        with self._selection_lock:
            if self._selection is None:
                return
            _update_tracked_selection_from_ws_event(
                selection=self._selection,
                event=event,
                add=add,
            )
            updated_selection = _clone_tracked_market_selection(self._selection)
        _save_tracked_market_selection(self.data_root, updated_selection)


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
    ws_worker_count: int,
    ws_flush_every_messages: int,
    ws_flush_every_seconds: int,
    ws_subscribe_batch_size: int,
    rest_worker_count: int,
    trade_worker_count: int,
    discover_all_pages: bool,
    page_limit: int,
    include_closed_markets: bool,
    include_archived_markets: bool,
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
    ws_collector = _BackgroundWsCollector(
        collector_config=collector.config,
        data_root=data_root,
        worker_count=ws_worker_count,
        max_assets_for_ws=max_assets_for_ws,
        ws_duration_seconds=ws_duration_seconds,
        flush_every_messages=ws_flush_every_messages,
        flush_every_seconds=ws_flush_every_seconds,
        subscribe_batch_size=ws_subscribe_batch_size,
    )
    ws_collector.start()
    previous_ws_raw_bytes = _source_raw_size_bytes(data_root=data_root, source="ws_market")
    try:
        while True:
            cycle_started = time.monotonic()
            cycle_ts = datetime.now(UTC).isoformat()
            try:
                markets = _discover_markets_for_cycle(
                    collector=collector,
                    discover_all_pages=discover_all_pages,
                    market_limit=market_limit,
                    page_limit=page_limit,
                    include_closed_markets=include_closed_markets,
                    include_archived_markets=include_archived_markets,
                )
                _write_latest_markets(data_root, markets)
                tracked_selection = _resolve_tracked_market_selection(
                    data_root=data_root,
                    markets=markets,
                    freeze_tracked_markets=freeze_tracked_markets,
                )
                ws_collector.update_selection(tracked_selection)
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
                    collect_ws_inline=False,
                    rest_worker_count=rest_worker_count,
                    trade_worker_count=trade_worker_count,
                    on_trade_frontier_update=lambda frontier: _save_trade_frontier_state(data_root, frontier),
                )
                universe_state = cycle_state["current_state"]
                trade_frontier = cycle_state["trade_frontier"]
                persisted_tracked_selection = ws_collector.snapshot_selection() or cycle_state["tracked_selection"]
                collection_warnings = event_warnings + cycle_state["collection_warnings"]
                ws_error = ws_collector.last_error()
                if ws_error:
                    collection_warnings.append(ws_error)
                ws_stats = ws_collector.consume_stats()
                cycle_elapsed_seconds = max(0.0, time.monotonic() - cycle_started)
                current_ws_raw_bytes = _source_raw_size_bytes(data_root=data_root, source="ws_market")
                ws_raw_growth_bytes = max(0, current_ws_raw_bytes - previous_ws_raw_bytes)
                previous_ws_raw_bytes = current_ws_raw_bytes
                if ws_stats["messages_in_cycle"] <= 0 and ws_raw_growth_bytes <= 0:
                    collection_warnings.append("ws_market_no_growth_in_cycle")

                _save_universe_state(data_root, universe_state)
                _save_tracked_market_selection(data_root, persisted_tracked_selection)
                _save_trade_frontier_state(data_root, trade_frontier)
                quality_payload = {
                    "role": "primary",
                    "node_id": node_id,
                    "cycle_ts": cycle_ts,
                    "cycle_elapsed_seconds": round(cycle_elapsed_seconds, 3),
                    "markets_count": len(markets),
                    "events_count": event_count,
                    "ws_messages_in_cycle": ws_stats["messages_in_cycle"],
                    "ws_raw_growth_bytes": ws_raw_growth_bytes,
                    "ws_raw_total_bytes": current_ws_raw_bytes,
                    "collection_warning_count": len(collection_warnings),
                    "bootstrap_warning_count": len(cycle_state["bootstrap_warnings"]),
                }
                report_path = _write_runtime_quality_report(data_root=data_root, payload=quality_payload)
                if collection_warnings:
                    _append_runtime_alert(
                        data_root=data_root,
                        payload={
                            "ts": cycle_ts,
                            "role": "primary",
                            "node_id": node_id,
                            "collection_warnings": collection_warnings,
                        },
                    )

                write_heartbeat(
                    data_root=data_root,
                    node_id=node_id,
                    role="primary",
                    status="ok",
                    extra={
                        "mode": "collecting",
                        "cycle_ts": cycle_ts,
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
                        "tracked_condition_ids_count": len(persisted_tracked_selection.condition_ids),
                        "tracked_asset_ids_count": len(persisted_tracked_selection.asset_ids),
                        "tracked_added_condition_ids_count": len(persisted_tracked_selection.added_condition_ids),
                        "tracked_removed_condition_ids_count": len(persisted_tracked_selection.removed_condition_ids),
                        "tracked_added_asset_ids_count": len(persisted_tracked_selection.added_asset_ids),
                        "tracked_removed_asset_ids_count": len(persisted_tracked_selection.removed_asset_ids),
                        "trade_frontier_condition_count": len(trade_frontier),
                        "ws_messages_in_cycle": ws_stats["messages_in_cycle"],
                        "ws_raw_growth_bytes": ws_raw_growth_bytes,
                        "quality_report_path": str(report_path),
                        "collection_warnings": collection_warnings,
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
    finally:
        ws_collector.stop()


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
    ws_worker_count: int,
    ws_flush_every_messages: int,
    ws_flush_every_seconds: int,
    ws_subscribe_batch_size: int,
    rest_worker_count: int,
    trade_worker_count: int,
    discover_all_pages: bool,
    page_limit: int,
    include_closed_markets: bool,
    include_archived_markets: bool,
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
    ws_collector: _BackgroundWsCollector | None = None
    previous_ws_raw_bytes = _source_raw_size_bytes(data_root=data_root, source="ws_market")
    try:
        while True:
            cycle_started = time.monotonic()
            cycle_ts = datetime.now(UTC).isoformat()
            try:
                decision = evaluate_failover(
                    data_root=data_root,
                    primary_node_id=primary_node_id,
                    max_stale_seconds=max_stale_seconds,
                )
                if decision["recommend_backup_collect"]:
                    if ws_collector is None:
                        ws_collector = _BackgroundWsCollector(
                            collector_config=collector.config,
                            data_root=data_root,
                            worker_count=ws_worker_count,
                            max_assets_for_ws=max_assets_for_ws,
                            ws_duration_seconds=ws_duration_seconds,
                            flush_every_messages=ws_flush_every_messages,
                            flush_every_seconds=ws_flush_every_seconds,
                            subscribe_batch_size=ws_subscribe_batch_size,
                        )
                        ws_collector.start()
                    markets = _discover_markets_for_cycle(
                        collector=collector,
                        discover_all_pages=discover_all_pages,
                        market_limit=market_limit,
                        page_limit=page_limit,
                        include_closed_markets=include_closed_markets,
                        include_archived_markets=include_archived_markets,
                    )
                    _write_latest_markets(data_root, markets)
                    tracked_selection = _resolve_tracked_market_selection(
                        data_root=data_root,
                        markets=markets,
                        freeze_tracked_markets=freeze_tracked_markets,
                    )
                    ws_collector.update_selection(tracked_selection)
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
                        collect_ws_inline=False,
                        rest_worker_count=rest_worker_count,
                        trade_worker_count=trade_worker_count,
                        on_trade_frontier_update=lambda frontier: _save_trade_frontier_state(data_root, frontier),
                    )
                    universe_state = cycle_state["current_state"]
                    trade_frontier = cycle_state["trade_frontier"]
                    persisted_tracked_selection = ws_collector.snapshot_selection() or cycle_state["tracked_selection"]
                    collection_warnings = event_warnings + cycle_state["collection_warnings"]
                    ws_error = ws_collector.last_error()
                    if ws_error:
                        collection_warnings.append(ws_error)
                    ws_stats = ws_collector.consume_stats()
                    cycle_elapsed_seconds = max(0.0, time.monotonic() - cycle_started)
                    current_ws_raw_bytes = _source_raw_size_bytes(data_root=data_root, source="ws_market")
                    ws_raw_growth_bytes = max(0, current_ws_raw_bytes - previous_ws_raw_bytes)
                    previous_ws_raw_bytes = current_ws_raw_bytes
                    if ws_stats["messages_in_cycle"] <= 0 and ws_raw_growth_bytes <= 0:
                        collection_warnings.append("ws_market_no_growth_in_cycle")

                    _save_universe_state(data_root, universe_state)
                    _save_tracked_market_selection(data_root, persisted_tracked_selection)
                    _save_trade_frontier_state(data_root, trade_frontier)
                    quality_payload = {
                        "role": "backup_active",
                        "node_id": node_id,
                        "cycle_ts": cycle_ts,
                        "cycle_elapsed_seconds": round(cycle_elapsed_seconds, 3),
                        "markets_count": len(markets),
                        "events_count": event_count,
                        "ws_messages_in_cycle": ws_stats["messages_in_cycle"],
                        "ws_raw_growth_bytes": ws_raw_growth_bytes,
                        "ws_raw_total_bytes": current_ws_raw_bytes,
                        "collection_warning_count": len(collection_warnings),
                        "bootstrap_warning_count": len(cycle_state["bootstrap_warnings"]),
                    }
                    report_path = _write_runtime_quality_report(data_root=data_root, payload=quality_payload)
                    if collection_warnings:
                        _append_runtime_alert(
                            data_root=data_root,
                            payload={
                                "ts": cycle_ts,
                                "role": "backup",
                                "node_id": node_id,
                                "collection_warnings": collection_warnings,
                            },
                        )

                    write_heartbeat(
                        data_root=data_root,
                        node_id=node_id,
                        role="backup",
                        status="ok",
                        extra={
                            "mode": "active",
                            "failover_decision": decision,
                            "cycle_ts": cycle_ts,
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
                            "tracked_condition_ids_count": len(persisted_tracked_selection.condition_ids),
                            "tracked_asset_ids_count": len(persisted_tracked_selection.asset_ids),
                            "tracked_added_condition_ids_count": len(persisted_tracked_selection.added_condition_ids),
                            "tracked_removed_condition_ids_count": len(persisted_tracked_selection.removed_condition_ids),
                            "tracked_added_asset_ids_count": len(persisted_tracked_selection.added_asset_ids),
                            "tracked_removed_asset_ids_count": len(persisted_tracked_selection.removed_asset_ids),
                            "trade_frontier_condition_count": len(trade_frontier),
                            "ws_messages_in_cycle": ws_stats["messages_in_cycle"],
                            "ws_raw_growth_bytes": ws_raw_growth_bytes,
                            "quality_report_path": str(report_path),
                            "collection_warnings": collection_warnings,
                            "bootstrap_warnings": cycle_state["bootstrap_warnings"],
                        },
                    )
                else:
                    if ws_collector is not None:
                        ws_collector.stop()
                        ws_collector = None
                    write_heartbeat(
                        data_root=data_root,
                        node_id=node_id,
                        role="backup",
                        status="ok",
                        extra={
                            "mode": "standby",
                            "failover_decision": decision,
                            "cycle_ts": cycle_ts,
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
    finally:
        if ws_collector is not None:
            ws_collector.stop()


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
    include_closed_markets: bool,
    include_archived_markets: bool,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def _append(items: list[dict[str, Any]]) -> None:
        for item in items:
            market_id = item.get("id")
            key = str(market_id) if market_id is not None else json.dumps(item, ensure_ascii=True, sort_keys=True)
            if key in seen_ids:
                continue
            seen_ids.add(key)
            merged.append(item)

    if discover_all_pages:
        _append(
            collector.discover_markets_all_pages(
                page_limit=page_limit,
                active=True,
                closed=False,
                archived=False,
            )
        )
        if include_closed_markets:
            _append(
                collector.discover_markets_all_pages(
                    page_limit=page_limit,
                    active=False,
                    closed=True,
                    archived=False,
                )
            )
        if include_archived_markets:
            _append(
                collector.discover_markets_all_pages(
                    page_limit=page_limit,
                    active=False,
                    closed=False,
                    archived=True,
                )
            )
        return merged

    active_markets, _ = collector.discover_markets(
        limit=market_limit,
        active=True,
        closed=False,
        archived=False,
    )
    _append(active_markets)
    if include_closed_markets:
        closed_markets, _ = collector.discover_markets(
            limit=market_limit,
            active=False,
            closed=True,
            archived=False,
        )
        _append(closed_markets)
    if include_archived_markets:
        archived_markets, _ = collector.discover_markets(
            limit=market_limit,
            active=False,
            closed=False,
            archived=True,
        )
        _append(archived_markets)
    return merged


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
    collect_ws_inline: bool = True,
    rest_worker_count: int = 4,
    trade_worker_count: int = 4,
    on_trade_frontier_update: Any | None = None,
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
    collector_factory = _build_parallel_collector_factory(collector)

    # 1) Normal cycle collection for current known universe.
    tracked_trade_condition_ids = _limit_items(tracked_condition_ids, max_markets_for_trades)
    trade_frontier, hit_trade_offset_cap, trade_warnings = _collect_trade_batches(
        collector_factory=collector_factory,
        condition_ids=tracked_trade_condition_ids,
        full_trades_for_tracked_markets=full_trades_for_tracked_markets,
        trade_page_limit=trade_page_limit,
        trade_max_offset=trade_max_offset,
        trade_frontier=trade_frontier,
        trade_worker_count=trade_worker_count,
        warning_prefix="fetch_trades",
        on_trade_frontier_update=on_trade_frontier_update,
    )
    collection_warnings.extend(trade_warnings)
    if hit_trade_offset_cap:
        collection_warnings.append("fetch_trades_reached_offset_cap")

    oi_condition_ids = _limit_items(tracked_condition_ids, max_markets_for_oi_holders)
    oi_holders_market_count = len(oi_condition_ids)
    books_hot_snapshot_count = 0
    books_cold_snapshot_count = 0
    history_snapshot_asset_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, rest_worker_count)) as executor:
        futures: dict[str, concurrent.futures.Future[Any]] = {
            "books": executor.submit(
                _collect_tiered_book_snapshots,
                collector_factory=collector_factory,
                asset_ids=tracked_asset_ids,
                max_assets_for_books=max_assets_for_books,
                hot_assets_for_books=hot_assets_for_books,
                hot_snapshot_interval_seconds=hot_snapshot_interval_seconds,
                cold_snapshot_interval_seconds=cold_snapshot_interval_seconds,
                snapshot_state=snapshot_state,
                collect_midpoints=collect_midpoints,
                collect_spreads=collect_spreads,
            ),
            "history": executor.submit(
                _collect_history_snapshots,
                collector_factory=collector_factory,
                asset_ids=tracked_asset_ids,
                max_assets_for_history=max_assets_for_history,
                history_snapshot_interval_seconds=history_snapshot_interval_seconds,
                history_window_seconds=history_window_seconds,
                history_interval=history_interval,
                history_fidelity=history_fidelity,
                snapshot_state=snapshot_state,
            ),
        }
        if oi_holders_market_count > 0:
            futures["oi"] = executor.submit(
                _run_open_interest_snapshot,
                collector_factory=collector_factory,
                condition_ids=oi_condition_ids,
            )
            futures["holders"] = executor.submit(
                _run_holders_snapshot,
                collector_factory=collector_factory,
                condition_ids=oi_condition_ids,
            )

        for name, future in futures.items():
            try:
                result = future.result()
            except Exception as exc:  # noqa: BLE001
                if name == "books":
                    collection_warnings.append(f"book_snapshot_failed:{exc}")
                elif name == "history":
                    collection_warnings.append(f"history_snapshot_failed:{exc}")
                elif name == "oi":
                    collection_warnings.append(f"fetch_open_interest_failed:{exc}")
                elif name == "holders":
                    collection_warnings.append(f"fetch_holders_failed:{exc}")
                continue

            if name == "books":
                books_hot_snapshot_count, books_cold_snapshot_count = result
            elif name == "history":
                history_snapshot_asset_count = result

    ws_asset_ids = _limit_items(tracked_asset_ids, max_assets_for_ws)
    if collect_ws_inline and ws_asset_ids:
        try:
            collector.stream_market(
                asset_ids=ws_asset_ids,
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
        bootstrap_trade_condition_ids = _limit_items(bootstrap_trade_condition_ids, max_markets_for_trades)
        trade_frontier, bootstrap_hit_trade_offset_cap, bootstrap_trade_warnings = _collect_trade_batches(
            collector_factory=collector_factory,
            condition_ids=bootstrap_trade_condition_ids,
            full_trades_for_tracked_markets=full_trades_for_tracked_markets,
            trade_page_limit=trade_page_limit,
            trade_max_offset=trade_max_offset,
            trade_frontier=trade_frontier,
            trade_worker_count=trade_worker_count,
            warning_prefix="bootstrap_fetch_trades",
            on_trade_frontier_update=on_trade_frontier_update,
        )
        collection_warnings.extend(bootstrap_trade_warnings)
        if bootstrap_hit_trade_offset_cap:
            collection_warnings.append("bootstrap_fetch_trades_reached_offset_cap")

    bootstrap_warnings: list[str] = []
    bootstrap_asset_ids = tracked_added_asset_ids if freeze_tracked_markets else new_asset_ids
    if bootstrap_asset_ids:
        bootstrap_assets = _limit_items(bootstrap_asset_ids, max_assets_for_books)
        bootstrap_history_assets = _limit_items(bootstrap_assets, max_assets_for_history)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(rest_worker_count, 2))) as executor:
            futures = {
                "bootstrap_books": executor.submit(
                    _collect_bootstrap_books,
                    collector_factory=collector_factory,
                    bootstrap_assets=bootstrap_assets,
                    collect_midpoints=collect_midpoints,
                    collect_spreads=collect_spreads,
                )
            }
            if bootstrap_history_assets and new_market_backfill_seconds > 0:
                now_ts = int(datetime.now(UTC).timestamp())
                start_ts = now_ts - max(new_market_backfill_seconds, 60)
                futures["bootstrap_history"] = executor.submit(
                    _collect_bootstrap_history,
                    collector_factory=collector_factory,
                    token_ids=bootstrap_history_assets,
                    start_ts=start_ts,
                    end_ts=now_ts,
                )

            for name, future in futures.items():
                try:
                    future.result()
                except Exception as exc:  # noqa: BLE001
                    if name == "bootstrap_books":
                        bootstrap_warnings.append(f"bootstrap_books_failed:{exc}")
                    else:
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


def _clone_tracked_market_selection(selection: TrackedMarketSelection) -> TrackedMarketSelection:
    return TrackedMarketSelection(
        condition_ids=list(selection.condition_ids),
        asset_ids=list(selection.asset_ids),
        added_condition_ids=list(selection.added_condition_ids),
        removed_condition_ids=list(selection.removed_condition_ids),
        added_asset_ids=list(selection.added_asset_ids),
        removed_asset_ids=list(selection.removed_asset_ids),
    )


def _collect_tiered_book_snapshots(
    *,
    collector_factory: Any,
    asset_ids: list[str],
    max_assets_for_books: int,
    hot_assets_for_books: int,
    hot_snapshot_interval_seconds: int,
    cold_snapshot_interval_seconds: int,
    snapshot_state: SnapshotScheduleState,
    collect_midpoints: bool,
    collect_spreads: bool,
) -> tuple[int, int]:
    assets = _limit_items(asset_ids, max_assets_for_books)
    if not assets:
        return 0, 0
    collector = collector_factory()

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


def _collect_bootstrap_books(
    *,
    collector_factory: Any,
    bootstrap_assets: list[str],
    collect_midpoints: bool,
    collect_spreads: bool,
) -> None:
    if not bootstrap_assets:
        return
    collector = collector_factory()
    collector.fetch_books(token_ids=bootstrap_assets)
    if collect_midpoints:
        collector.fetch_midpoints(token_ids=bootstrap_assets)
    if collect_spreads:
        collector.fetch_spreads(token_ids=bootstrap_assets)


def _collect_history_snapshots(
    *,
    collector_factory: Any,
    asset_ids: list[str],
    max_assets_for_history: int,
    history_snapshot_interval_seconds: int,
    history_window_seconds: int,
    history_interval: str,
    history_fidelity: int,
    snapshot_state: SnapshotScheduleState,
) -> int:
    assets = _limit_items(asset_ids, max_assets_for_history)
    if not assets or history_snapshot_interval_seconds <= 0:
        return 0
    collector = collector_factory()

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


def _build_parallel_collector_factory(collector: Any) -> Any:
    config = getattr(collector, "config", None)
    if isinstance(config, CollectorConfig):
        return lambda: PolymarketCollector(config)
    return lambda: collector


def _collect_trade_batches(
    *,
    collector_factory: Any,
    condition_ids: list[str],
    full_trades_for_tracked_markets: bool,
    trade_page_limit: int,
    trade_max_offset: int,
    trade_frontier: dict[str, Any],
    trade_worker_count: int,
    warning_prefix: str,
    on_trade_frontier_update: Any | None,
) -> tuple[dict[str, Any], bool, list[str]]:
    if not condition_ids:
        return trade_frontier, False, []

    warnings: list[str] = []
    hit_trade_offset_cap = False

    trade_batches = [
        batch
        for batch in (
            _shard_items(values=condition_ids, shard_count=max(1, trade_worker_count), shard_index=worker_index)
            for worker_index in range(max(1, trade_worker_count))
        )
        if batch
    ]
    if not trade_batches:
        return trade_frontier, False, []

    frontier_state = dict(trade_frontier)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(len(trade_batches), trade_worker_count))) as executor:
        futures: dict[concurrent.futures.Future[Any], list[str]] = {}
        for batch in trade_batches:
            if full_trades_for_tracked_markets:
                batch_frontier = {
                    condition_id: frontier_state[condition_id]
                    for condition_id in batch
                    if condition_id in frontier_state
                }
                future = executor.submit(
                    _run_incremental_trade_batch,
                    collector_factory=collector_factory,
                    condition_ids=batch,
                    frontier_by_condition=batch_frontier,
                    trade_page_limit=trade_page_limit,
                    trade_max_offset=trade_max_offset,
                )
            else:
                future = executor.submit(
                    _run_recent_trade_batch,
                    collector_factory=collector_factory,
                    condition_ids=batch,
                    trade_page_limit=trade_page_limit,
                )
            futures[future] = batch

        for future in concurrent.futures.as_completed(futures):
            try:
                batch_frontier, batch_hit_offset_cap = future.result()
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"{warning_prefix}_failed:{exc}")
                continue

            hit_trade_offset_cap = hit_trade_offset_cap or batch_hit_offset_cap
            if not full_trades_for_tracked_markets:
                continue

            frontier_state.update(batch_frontier)
            if on_trade_frontier_update is not None:
                try:
                    on_trade_frontier_update(frontier_state)
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"{warning_prefix}_checkpoint_failed:{exc}")

    return frontier_state, hit_trade_offset_cap, warnings


def _run_incremental_trade_batch(
    *,
    collector_factory: Any,
    condition_ids: list[str],
    frontier_by_condition: dict[str, Any],
    trade_page_limit: int,
    trade_max_offset: int,
) -> tuple[dict[str, Any], bool]:
    collector = collector_factory()
    _, _, batch_frontier, hit_trade_offset_cap = collector.fetch_trades_incremental(
        condition_ids=condition_ids,
        frontier_by_condition=frontier_by_condition,
        page_limit=trade_page_limit,
        max_offset=trade_max_offset,
        taker_only=False,
    )
    return batch_frontier, hit_trade_offset_cap


def _run_recent_trade_batch(
    *,
    collector_factory: Any,
    condition_ids: list[str],
    trade_page_limit: int,
) -> tuple[dict[str, Any], bool]:
    collector = collector_factory()
    collector.fetch_trades(
        condition_ids=condition_ids,
        limit=trade_page_limit,
        taker_only=False,
    )
    return {}, False


def _run_open_interest_snapshot(*, collector_factory: Any, condition_ids: list[str]) -> tuple[list[dict[str, Any]], Path | None]:
    collector = collector_factory()
    return collector.fetch_open_interest(condition_ids=condition_ids)


def _run_holders_snapshot(*, collector_factory: Any, condition_ids: list[str]) -> tuple[list[dict[str, Any]], Path | None]:
    collector = collector_factory()
    return collector.fetch_holders(condition_ids=condition_ids)


def _collect_bootstrap_history(
    *,
    collector_factory: Any,
    token_ids: list[str],
    start_ts: int,
    end_ts: int,
) -> tuple[dict[str, Any], Path | None]:
    collector = collector_factory()
    return collector.fetch_batch_prices_history(
        token_ids=token_ids,
        start_ts=start_ts,
        end_ts=end_ts,
        interval="1m",
        fidelity=1,
    )


def _limit_items(values: list[str], limit: int) -> list[str]:
    if limit <= 0:
        return list(values)
    return values[:limit]


def _shard_items(*, values: list[str], shard_count: int, shard_index: int) -> list[str]:
    normalized_shard_count = max(1, shard_count)
    normalized_shard_index = max(0, shard_index)
    return list(values[normalized_shard_index::normalized_shard_count])


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


def _source_raw_size_bytes(*, data_root: Path, source: str) -> int:
    source_root = data_root / "raw" / f"source={source}"
    if not source_root.exists():
        return 0
    total = 0
    for path in source_root.rglob("*.jsonl.gz"):
        if path.is_file():
            total += path.stat().st_size
    return total


def _write_runtime_quality_report(*, data_root: Path, payload: dict[str, Any]) -> Path:
    reports_dir = data_root / "state" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC)
    path = reports_dir / f"runtime_quality_{ts:%Y%m%dT%H%M%S}.json"
    latest_path = reports_dir / "runtime_quality_latest.json"
    text = json.dumps(payload, ensure_ascii=True, indent=2)
    path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    return path


def _append_runtime_alert(*, data_root: Path, payload: dict[str, Any]) -> Path:
    reports_dir = data_root / "state" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / "runtime_alerts.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
        handle.write("\n")
    return path
