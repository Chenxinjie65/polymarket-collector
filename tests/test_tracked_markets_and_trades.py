from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

import requests

from polymarket_collector.collector import CollectorConfig, PolymarketCollector
from polymarket_collector.runtime import (
    SnapshotScheduleState,
    TrackedMarketSelection,
    UniverseState,
    _collect_cycle,
    _resolve_tracked_market_selection,
    _update_tracked_selection_from_ws_event,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: list[dict[str, object]] | dict[str, object]) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload, ensure_ascii=True)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error", response=self)

    def json(self) -> list[dict[str, object]] | dict[str, object]:
        return self._payload


class GetOnlySession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, *, params: dict[str, object] | None, timeout: float) -> FakeResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        if not self._responses:
            raise AssertionError("unexpected GET request")
        return self._responses.pop(0)


class TrackedMarketsAndTradesTests(unittest.TestCase):
    def test_tracked_market_selection_is_persistent_and_updates_with_active_set(self) -> None:
        markets_a = [
            {"conditionId": "cond-a", "clobTokenIds": ["tok-a-1", "tok-a-2"]},
            {"conditionId": "cond-b", "clobTokenIds": ["tok-b-1", "tok-b-2"]},
        ]
        markets_b = [
            {"conditionId": "cond-b", "clobTokenIds": ["tok-b-1", "tok-b-2"]},
            {"conditionId": "cond-x", "clobTokenIds": ["tok-x-1", "tok-x-2"]},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir)

            first = _resolve_tracked_market_selection(
                data_root=data_root,
                markets=markets_a,
                freeze_tracked_markets=True,
            )
            second = _resolve_tracked_market_selection(
                data_root=data_root,
                markets=markets_b,
                freeze_tracked_markets=True,
            )

            self.assertEqual(first.condition_ids, ["cond-a", "cond-b"])
            self.assertEqual(first.asset_ids, ["tok-a-1", "tok-a-2", "tok-b-1", "tok-b-2"])
            self.assertEqual(second.condition_ids, ["cond-b", "cond-x"])
            self.assertEqual(second.asset_ids, ["tok-b-1", "tok-b-2", "tok-x-1", "tok-x-2"])
            self.assertEqual(second.added_condition_ids, ["cond-x"])
            self.assertEqual(second.removed_condition_ids, ["cond-a"])
            self.assertEqual(second.added_asset_ids, ["tok-x-1", "tok-x-2"])
            self.assertEqual(second.removed_asset_ids, ["tok-a-1", "tok-a-2"])

    def test_fetch_trades_incremental_updates_frontier_and_writes_only_new_trades(self) -> None:
        page_0 = [
            {
                "conditionId": "cond-a",
                "asset": "tok-a-1",
                "side": "BUY",
                "timestamp": 200,
                "price": 0.45,
                "size": 10,
                "transactionHash": "tx-new",
            },
            {
                "conditionId": "cond-a",
                "asset": "tok-a-1",
                "side": "BUY",
                "timestamp": 150,
                "price": 0.4,
                "size": 5,
                "transactionHash": "tx-old",
            },
        ]
        page_500 = [
            {
                "conditionId": "cond-a",
                "asset": "tok-a-1",
                "side": "BUY",
                "timestamp": 100,
                "price": 0.35,
                "size": 2,
                "transactionHash": "tx-older",
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            collector = PolymarketCollector(CollectorConfig(data_root=Path(tmpdir)))
            collector.session = GetOnlySession(
                [
                    FakeResponse(200, page_0),
                    FakeResponse(200, page_500),
                ]
            )

            trades, path, frontier, hit_offset_cap = collector.fetch_trades_incremental(
                condition_ids=["cond-a"],
                frontier_by_condition={
                    "cond-a": {
                        "max_timestamp": 150,
                        "keys_at_max_timestamp": [
                            "tx-old|tok-a-1|cond-a|BUY|150|0.4|5",
                        ],
                    }
                },
                page_limit=2,
                max_offset=1000,
                taker_only=False,
            )

            self.assertFalse(hit_offset_cap)
            self.assertEqual(
                trades,
                [
                    {
                        "conditionId": "cond-a",
                        "asset": "tok-a-1",
                        "side": "BUY",
                        "timestamp": 200,
                        "price": 0.45,
                        "size": 10,
                        "transactionHash": "tx-new",
                    }
                ],
            )
            self.assertEqual(collector.session.calls[0]["params"], {"limit": 2, "offset": 0, "takerOnly": "false", "market": "cond-a"})
            self.assertEqual(collector.session.calls[1]["params"], {"limit": 2, "offset": 2, "takerOnly": "false", "market": "cond-a"})
            self.assertEqual(
                frontier,
                {
                    "cond-a": {
                        "max_timestamp": 200,
                        "keys_at_max_timestamp": [
                            "tx-new|tok-a-1|cond-a|BUY|200|0.45|10",
                        ],
                    }
                },
            )

            self.assertIsNotNone(path)
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                written = [json.loads(line) for line in handle]
            self.assertEqual(len(written), 1)
            self.assertEqual(written[0]["payload"]["transactionHash"], "tx-new")

    def test_collect_cycle_skips_history_midpoints_and_spreads_when_disabled(self) -> None:
        class DummyCollector:
            def __init__(self) -> None:
                self.calls: list[tuple[str, list[str] | None]] = []

            @staticmethod
            def extract_condition_ids(markets: list[dict[str, object]]) -> list[str]:
                return [str(m["conditionId"]) for m in markets]

            @staticmethod
            def extract_asset_ids(markets: list[dict[str, object]]) -> list[str]:
                values: list[str] = []
                for market in markets:
                    values.extend(list(market["clobTokenIds"]))
                return values

            def fetch_trades_incremental(self, **kwargs):
                self.calls.append(("fetch_trades_incremental", list(kwargs["condition_ids"])))
                return [], None, {}, False

            def fetch_open_interest(self, **kwargs):
                self.calls.append(("fetch_open_interest", list(kwargs["condition_ids"])))

            def fetch_holders(self, **kwargs):
                self.calls.append(("fetch_holders", list(kwargs["condition_ids"])))

            def fetch_books(self, **kwargs):
                self.calls.append(("fetch_books", list(kwargs["token_ids"])))

            def fetch_midpoints(self, **kwargs):
                self.calls.append(("fetch_midpoints", list(kwargs["token_ids"])))

            def fetch_spreads(self, **kwargs):
                self.calls.append(("fetch_spreads", list(kwargs["token_ids"])))

            def fetch_batch_prices_history(self, **kwargs):
                self.calls.append(("fetch_batch_prices_history", list(kwargs["token_ids"])))

            def stream_market(self, **kwargs):
                self.calls.append(("stream_market", list(kwargs["asset_ids"])))

        collector = DummyCollector()
        markets = [{"conditionId": "cond-a", "clobTokenIds": ["tok-a-1", "tok-a-2"]}]
        result = _collect_cycle(
            collector=collector,
            markets=markets,
            previous_state=UniverseState(condition_ids=set(), asset_ids=set()),
            max_markets_for_trades=10,
            max_markets_for_oi_holders=10,
            max_assets_for_books=10,
            hot_assets_for_books=0,
            hot_snapshot_interval_seconds=300,
            cold_snapshot_interval_seconds=300,
            max_assets_for_history=0,
            history_snapshot_interval_seconds=0,
            history_window_seconds=900,
            history_interval="all",
            history_fidelity=1,
            max_assets_for_ws=10,
            ws_duration_seconds=5,
            new_market_backfill_seconds=0,
            snapshot_state=SnapshotScheduleState(),
            tracked_selection=TrackedMarketSelection(
                condition_ids=["cond-a"],
                asset_ids=["tok-a-1", "tok-a-2"],
                added_condition_ids=["cond-a"],
                added_asset_ids=["tok-a-1", "tok-a-2"],
            ),
            freeze_tracked_markets=True,
            full_trades_for_tracked_markets=True,
            trade_page_limit=500,
            trade_max_offset=10000,
            trade_frontier={},
            collect_midpoints=False,
            collect_spreads=False,
        )

        self.assertEqual(result["history_snapshot_asset_count"], 0)
        call_names = [name for name, _ in collector.calls]
        self.assertNotIn("fetch_midpoints", call_names)
        self.assertNotIn("fetch_spreads", call_names)
        self.assertNotIn("fetch_batch_prices_history", call_names)

    def test_ws_event_updates_tracked_selection_immediately(self) -> None:
        selection = TrackedMarketSelection(
            condition_ids=["cond-a"],
            asset_ids=["tok-a-1", "tok-a-2"],
        )

        _update_tracked_selection_from_ws_event(
            selection=selection,
            event={
                "event_type": "new_market",
                "condition_id": "cond-b",
                "assets_ids": ["tok-b-1", "tok-b-2"],
            },
            add=True,
        )
        self.assertEqual(selection.condition_ids, ["cond-a", "cond-b"])
        self.assertEqual(selection.asset_ids, ["tok-a-1", "tok-a-2", "tok-b-1", "tok-b-2"])

        _update_tracked_selection_from_ws_event(
            selection=selection,
            event={
                "event_type": "market_resolved",
                "condition_id": "cond-b",
                "assets_ids": ["tok-b-1", "tok-b-2"],
            },
            add=False,
        )
        self.assertEqual(selection.condition_ids, ["cond-a"])
        self.assertEqual(selection.asset_ids, ["tok-a-1", "tok-a-2"])


if __name__ == "__main__":
    unittest.main()
