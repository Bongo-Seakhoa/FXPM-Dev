import json
import os
import shutil
import unittest
import zlib
from datetime import datetime
import uuid

import pandas as pd

from pm_dashboard.analytics import build_analytics_payload, load_trade_history, reconstruct_trade_outcomes


def _encode_magic(symbol: str, timeframe: str, regime: str) -> int:
    key = f"{symbol}|{timeframe}|{regime}"
    return zlib.crc32(key.encode("utf-8")) & 0x7FFFFFFF


class DashboardTradeEnrichmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pm_root = os.path.join(os.getcwd(), f".tmp_dashboard_trade_{uuid.uuid4().hex}")
        os.makedirs(self.pm_root, exist_ok=True)
        os.makedirs(os.path.join(self.pm_root, "pm_outputs"), exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.pm_root, ignore_errors=True)

    def _write_trades(self, rows) -> None:
        path = os.path.join(self.pm_root, "pm_outputs", "trades_20260209_000000.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(rows, handle)

    def _write_pm_configs(self, cfg) -> None:
        path = os.path.join(self.pm_root, "pm_configs.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(cfg, handle)

    def test_load_trade_history_enriches_from_magic(self) -> None:
        self._write_pm_configs(
            {
                "EURUSD": {
                    "symbol": "EURUSD",
                    "timeframe": "H1",
                    "strategy_name": "EMACrossoverStrategy",
                    "regime_configs": {
                        "M30": {
                            "TREND": {"strategy_name": "MomentumBurstStrategy"}
                        }
                    },
                }
            }
        )
        magic = _encode_magic("EURUSD", "M30", "TREND")
        self._write_trades(
            [
                {
                    "timestamp": "2026-02-09T00:00:00",
                    "symbol": "eurusd",
                    "direction": "LONG",
                    "volume": 0.01,
                    "price": 1.1,
                    "sl": 1.09,
                    "tp": 1.11,
                    "magic": magic,
                    "status": "EXECUTED",
                }
            ]
        )

        trades = load_trade_history(self.pm_root, max_files=10)
        self.assertEqual(len(trades), 1)
        trade = trades[0]

        self.assertEqual(trade.get("symbol"), "EURUSD")
        self.assertEqual(trade.get("timeframe"), "M30")
        self.assertEqual(trade.get("regime"), "TREND")
        self.assertEqual(trade.get("strategy"), "MomentumBurstStrategy")
        self.assertIn("_parsed_timestamp", trade)

    def test_load_trade_history_falls_back_to_symbol_config(self) -> None:
        self._write_pm_configs(
            {
                "GBPUSD": {
                    "symbol": "GBPUSD",
                    "timeframe": "H4",
                    "strategy_name": "EMACrossoverStrategy",
                    "regime_configs": {
                        "H4": {
                            "TREND": {"strategy_name": "KeltnerBreakoutStrategy"}
                        }
                    },
                }
            }
        )
        self._write_trades(
            [
                {
                    "timestamp": "2026-02-09T01:00:00",
                    "symbol": "GBPUSD",
                    "direction": "SHORT",
                    "volume": 0.01,
                    "price": 1.25,
                    "sl": 1.26,
                    "tp": 1.24,
                    "status": "EXECUTED",
                }
            ]
        )

        trades = load_trade_history(self.pm_root, max_files=10)
        self.assertEqual(len(trades), 1)
        trade = trades[0]

        self.assertEqual(trade.get("timeframe"), "H4")
        self.assertEqual(trade.get("regime"), "TREND")
        self.assertEqual(trade.get("strategy"), "KeltnerBreakoutStrategy")

    def test_reconstruct_trade_outcomes_defaults_missing_timeframe_to_m5(self) -> None:
        entry_time = datetime(2026, 2, 9, 2, 0, 0)
        bars = pd.DataFrame(
            {
                "Open": [1.1000],
                "High": [1.1110],
                "Low": [1.0990],
                "Close": [1.1100],
            },
            index=[entry_time + pd.Timedelta(minutes=5)],
        )

        calls = []

        def _loader(symbol, timeframe, start, end):
            calls.append((symbol, timeframe, start, end))
            return bars

        reconstructed = reconstruct_trade_outcomes(
            [
                {
                    "timestamp": entry_time.isoformat(),
                    "symbol": "EURUSD",
                    "direction": "LONG",
                    "volume": 0.01,
                    "price": 1.1000,
                    "sl": 1.0950,
                    "tp": 1.1100,
                }
            ],
            _loader,
            max_trades=10,
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1], "M5")
        self.assertEqual(len(reconstructed), 1)
        self.assertEqual(reconstructed[0].get("close_reason"), "TP_HIT")

    def test_reconstruct_trade_outcomes_batches_by_symbol_and_timeframe(self) -> None:
        entry_a = datetime(2026, 2, 9, 2, 0, 0)
        entry_b = datetime(2026, 2, 10, 2, 0, 0)
        bars = pd.DataFrame(
            {
                "Open": [1.1000, 1.1005, 1.1010, 1.1015],
                "High": [1.1110, 1.1110, 1.1120, 1.1130],
                "Low": [1.0990, 1.0995, 1.1000, 1.1005],
                "Close": [1.1100, 1.1105, 1.1110, 1.1120],
            },
            index=[
                entry_a + pd.Timedelta(minutes=5),
                entry_a + pd.Timedelta(minutes=10),
                entry_b + pd.Timedelta(minutes=5),
                entry_b + pd.Timedelta(minutes=10),
            ],
        )

        calls = []

        def _loader(symbol, timeframe, start, end):
            calls.append((symbol, timeframe, start, end))
            return bars

        reconstructed = reconstruct_trade_outcomes(
            [
                {
                    "timestamp": entry_a.isoformat(),
                    "symbol": "EURUSD",
                    "timeframe": "M5",
                    "direction": "LONG",
                    "volume": 0.01,
                    "price": 1.1000,
                    "sl": 1.0950,
                    "tp": 1.1100,
                },
                {
                    "timestamp": entry_b.isoformat(),
                    "symbol": "EURUSD",
                    "timeframe": "M5",
                    "direction": "LONG",
                    "volume": 0.01,
                    "price": 1.1010,
                    "sl": 1.0960,
                    "tp": 1.1120,
                },
            ],
            _loader,
            max_trades=10,
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "EURUSD")
        self.assertEqual(calls[0][1], "M5")
        self.assertEqual(len(reconstructed), 2)

    def test_analytics_uses_realized_trades_only(self) -> None:
        self._write_trades(
            [
                {
                    "timestamp": "2026-02-09T03:00:00",
                    "symbol": "EURUSD",
                    "direction": "LONG",
                    "price": 1.1,
                    "sl": 1.09,
                    "tp": 1.12,
                    "status": "OPEN",
                },
                {
                    "timestamp": "2026-02-09T04:00:00",
                    "symbol": "EURUSD",
                    "direction": "LONG",
                    "price": 1.1,
                    "sl": 1.09,
                    "tp": 1.12,
                    "status": "CLOSED",
                    "close_reason": "TP_HIT",
                    "pnl": 15.0,
                },
            ]
        )

        trades = load_trade_history(self.pm_root, max_files=10)
        self.assertEqual(len(trades), 2)
        self.assertFalse(next(item for item in trades if item.get("status") == "OPEN").get("realized"))
        self.assertTrue(next(item for item in trades if item.get("status") == "CLOSED").get("realized"))

        payload = build_analytics_payload(self.pm_root, initial_capital=10000.0)
        self.assertEqual(payload["realized_trades_loaded"], 1)
        self.assertEqual(payload["metrics"]["total_trades"], 1)
        self.assertAlmostEqual(payload["metrics"]["total_pnl"], 15.0, places=2)

    def test_no_data_payload_includes_extended_risk_metric_keys(self) -> None:
        payload = build_analytics_payload(self.pm_root, initial_capital=10000.0)
        metrics = payload["metrics"]

        self.assertFalse(payload["has_data"])
        self.assertIn("drawdown_duration", metrics)
        self.assertIn("recovery_time", metrics)
        self.assertIn("ulcer_index", metrics)
        self.assertEqual(metrics["drawdown_duration"], 0)
        self.assertEqual(metrics["recovery_time"], 0)
        self.assertEqual(metrics["ulcer_index"], 0.0)


if __name__ == "__main__":
    unittest.main()
