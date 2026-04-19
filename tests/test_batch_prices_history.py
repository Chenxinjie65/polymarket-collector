from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

import requests

from polymarket_collector.cli import build_parser
from polymarket_collector.collector import CollectorConfig, PolymarketCollector
from polymarket_collector.parquet_build import _read_rows_for_normalized_parquet


class FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, object]) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload, ensure_ascii=True)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error", response=self)

    def json(self) -> dict[str, object]:
        return self._payload


class RecordingSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def post(self, url: str, *, json: dict[str, object], timeout: float) -> FakeResponse:
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        if not self._responses:
            raise AssertionError("unexpected POST request")
        return self._responses.pop(0)


class BatchPricesHistoryTests(unittest.TestCase):
    def test_absolute_window_uses_all_then_retries_without_interval(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            collector = PolymarketCollector(CollectorConfig(data_root=Path(tmpdir)))
            collector.session = RecordingSession(
                [
                    FakeResponse(400, {"error": "bad request"}),
                    FakeResponse(200, {"history": {"token-1": [{"t": 10, "p": 0.42}]}}),
                ]
            )

            history, path = collector.fetch_batch_prices_history(
                token_ids=["token-1"],
                start_ts=100,
                end_ts=200,
                interval="1m",
                fidelity=1,
            )

            self.assertEqual(history, {"token-1": [{"t": 10, "p": 0.42}]})
            self.assertIsNotNone(path)
            self.assertEqual(
                collector.session.calls[0]["json"],
                {
                    "markets": ["token-1"],
                    "fidelity": 1,
                    "start_ts": 100,
                    "end_ts": 200,
                    "interval": "all",
                },
            )
            self.assertEqual(
                collector.session.calls[1]["json"],
                {
                    "markets": ["token-1"],
                    "fidelity": 1,
                    "start_ts": 100,
                    "end_ts": 200,
                },
            )

            with gzip.open(path, "rt", encoding="utf-8") as handle:
                written = json.loads(handle.readline())
            self.assertEqual(
                written["payload"]["request"],
                {
                    "markets": ["token-1"],
                    "fidelity": 1,
                    "start_ts": 100,
                    "end_ts": 200,
                    "interval": "all",
                },
            )

    def test_relative_window_keeps_requested_interval(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            collector = PolymarketCollector(CollectorConfig(data_root=Path(tmpdir)))
            collector.session = RecordingSession(
                [
                    FakeResponse(200, {"history": {"token-1": [{"t": 10, "p": 0.42}]}}),
                ]
            )

            history, _ = collector.fetch_batch_prices_history(
                token_ids=["token-1"],
                interval="1h",
                fidelity=5,
            )

            self.assertEqual(history, {"token-1": [{"t": 10, "p": 0.42}]})
            self.assertEqual(
                collector.session.calls[0]["json"],
                {
                    "markets": ["token-1"],
                    "fidelity": 5,
                    "interval": "1h",
                },
            )

    def test_run_primary_defaults_history_interval_to_all(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["run-primary"])
        self.assertEqual(args.history_interval, "all")
        self.assertEqual(args.max_assets_for_history, 0)
        self.assertEqual(args.max_assets_for_ws, 0)
        self.assertEqual(args.history_snapshot_interval_seconds, 0)
        self.assertEqual(args.new_market_backfill_seconds, 0)
        self.assertEqual(args.trade_max_offset, 3000)
        self.assertEqual(args.ws_worker_count, 4)
        self.assertEqual(args.ws_flush_every_messages, 50)
        self.assertEqual(args.ws_flush_every_seconds, 5)
        self.assertEqual(args.ws_subscribe_batch_size, 500)
        self.assertEqual(args.rest_worker_count, 4)
        self.assertEqual(args.trade_worker_count, 4)
        self.assertFalse(args.collect_midpoints)
        self.assertFalse(args.collect_spreads)

    def test_normalized_ws_market_keeps_each_asset_from_multi_asset_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = Path(tmpdir) / "ws.jsonl.gz"
            record = {
                "source": "ws_market",
                "ts_ingest": "2026-04-19T00:00:00+00:00",
                "payload": {
                    "event_type": "new_market",
                    "condition_id": "cond-a",
                    "assets_ids": ["tok-a-1", "tok-a-2"],
                    "timestamp": "123",
                },
            }
            with gzip.open(raw_path, "wt", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=True))
                handle.write("\n")

            rows, dropped = _read_rows_for_normalized_parquet(
                raw_path=raw_path,
                source="ws_market",
                dedup=True,
            )

            self.assertEqual(dropped, 0)
            self.assertEqual([row["asset_id"] for row in rows], ["tok-a-1", "tok-a-2"])


if __name__ == "__main__":
    unittest.main()
