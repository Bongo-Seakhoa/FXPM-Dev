import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parents[1] / "agent_audit" / "performance" / "scripts"
sys.path.insert(0, str(SCRIPT_DIR))

import run_promoted_cell_replay as replay_tools
from run_promoted_cell_replay import gate_signals_to_cell_regime, iter_promoted_cells
from shadow_selection_audit import iter_cells


class PromotedCellEnumerationTests(unittest.TestCase):
    def test_symbol_level_promoted_winner_is_not_skipped_when_regime_configs_exist(self):
        ledger = {
            "EURUSD": {
                "symbol": "EURUSD",
                "promotion_status": "PROMOTED",
                "strategy_name": "SymbolWinnerStrategy",
                "timeframe": "H1",
                "parameters": {"x": 1},
                "regime_configs": {
                    "H1": {
                        "TREND": {"promotion_status": "NO_TRADE", "strategy_name": "NO_TRADE"}
                    }
                },
            }
        }

        replay_cells = list(iter_promoted_cells(ledger))
        shadow_cells = list(iter_cells(ledger))

        self.assertEqual(len(replay_cells), 1)
        self.assertEqual(replay_cells[0]["symbol"], "EURUSD")
        self.assertEqual(replay_cells[0]["timeframe"], "H1")
        self.assertEqual(replay_cells[0]["regime"], "SYMBOL_LEVEL")
        self.assertEqual(replay_cells[0]["promotion_source"], "symbol_level")
        self.assertEqual(len(shadow_cells), 1)
        self.assertEqual(shadow_cells[0]["promotion_source"], "symbol_level")

    def test_symbol_and_regime_promoted_winner_is_deduped_to_regime_cell(self):
        ledger = {
            "FORD": {
                "symbol": "FORD",
                "promotion_status": "PROMOTED",
                "strategy_name": "DoubleEMATrendStrategy",
                "timeframe": "H4",
                "parameters": {"symbol_level": True},
                "regime_configs": {
                    "H4": {
                        "CHOP": {
                            "promotion_status": "PROMOTED",
                            "strategy_name": "DoubleEMATrendStrategy",
                            "parameters": {"leaf": True},
                            "val_metrics": {"total_trades": 16},
                        }
                    }
                },
            }
        }

        replay_cells = list(iter_promoted_cells(ledger))
        shadow_cells = list(iter_cells(ledger))

        self.assertEqual(len(replay_cells), 1)
        self.assertEqual(replay_cells[0]["symbol"], "FORD")
        self.assertEqual(replay_cells[0]["timeframe"], "H4")
        self.assertEqual(replay_cells[0]["regime"], "CHOP")
        self.assertEqual(replay_cells[0]["promotion_source"], "symbol_and_regime")
        self.assertEqual(replay_cells[0]["parameters"], {"leaf": True})
        self.assertEqual(len(shadow_cells), 1)
        self.assertEqual(shadow_cells[0]["promotion_source"], "symbol_and_regime")
        self.assertEqual(shadow_cells[0]["val_metrics"]["total_trades"], 16)


class RegimeReplayParityTests(unittest.TestCase):
    def test_regime_leaf_only_keeps_signals_from_its_live_regime(self):
        index = pd.date_range("2026-01-01", periods=6, freq="h")
        features = pd.DataFrame(
            {"REGIME_LIVE": ["TREND", "CHOP", "TREND", "RANGE", "CHOP", "TREND"]},
            index=index,
        )
        signals = pd.Series([1, -1, 1, -1, 1, -1], index=index)

        filtered, column, matching = gate_signals_to_cell_regime(signals, features, "CHOP")

        self.assertEqual(column, "REGIME_LIVE")
        self.assertEqual(matching, 2)
        self.assertEqual(filtered.tolist(), [0, -1, 0, 0, 1, 0])

    def test_symbol_level_winner_remains_unconditional(self):
        index = pd.date_range("2026-01-01", periods=3, freq="h")
        features = pd.DataFrame({"Close": [1.0, 1.1, 1.2]}, index=index)
        signals = pd.Series([1, 0, -1], index=index)

        filtered, column, matching = gate_signals_to_cell_regime(
            signals, features, "SYMBOL_LEVEL"
        )

        self.assertEqual(column, "unconditional")
        self.assertEqual(matching, 3)
        self.assertEqual(filtered.tolist(), signals.tolist())

    def test_regime_leaf_rejects_non_live_detector_column(self):
        index = pd.date_range("2026-01-01", periods=3, freq="h")
        features = pd.DataFrame({"REGIME": ["CHOP"] * 3}, index=index)
        signals = pd.Series([1, 0, -1], index=index)

        with self.assertRaisesRegex(ValueError, "requires_REGIME_LIVE"):
            gate_signals_to_cell_regime(signals, features, "CHOP")


class ActivatedNativeReplayTests(unittest.TestCase):
    @staticmethod
    def _frame(rows=200):
        index = pd.date_range("2025-01-01", periods=rows, freq="5min", tz="UTC")
        return pd.DataFrame(
            {
                "Open": [1.0] * rows,
                "High": [1.1] * rows,
                "Low": [0.9] * rows,
                "Close": [1.0] * rows,
                "Volume": [1] * rows,
            },
            index=index,
        )

    @staticmethod
    def _meta(symbol, timeframe):
        return {
            "source_kind": "broker_native",
            "symbol": symbol,
            "timeframe": timeframe,
            "sha256": "a" * 64,
            "provenance_valid": True,
            "provenance_errors": [],
            "timestamp_semantics": "bar_open_utc",
            "activation_id": "activation-test",
            "activation_manifest_sha256": "b" * 64,
            "activation_entry": {
                "status": "active",
                "symbol": symbol,
                "timeframe": timeframe,
            },
        }

    def test_every_supported_cell_requests_its_activated_native_timeframe(self):
        loader = MagicMock()
        frame = self._frame()

        for timeframe in ("M5", "M15", "M30", "H1", "H4", "D1"):
            with self.subTest(timeframe=timeframe):
                loader.reset_mock()
                loader.load_native_timeframe.return_value = (
                    frame,
                    self._meta("EURUSD", timeframe),
                )
                loaded, evidence, error = replay_tools.load_activated_native_replay_frame(
                    loader, "eurusd", timeframe
                )

                self.assertIs(loaded, frame)
                self.assertEqual(error, "")
                self.assertEqual(evidence["native_timeframe"], timeframe)
                loader.load_native_timeframe.assert_called_once_with("EURUSD", timeframe)
                loader.load_symbol.assert_not_called()
                loader.resample.assert_not_called()

    def test_h4_promoted_cell_replay_never_uses_m5_or_resample(self):
        loader = MagicMock()
        frame = self._frame()
        loader.load_native_timeframe.return_value = (
            frame,
            self._meta("EURUSD", "H4"),
        )
        loader.load_symbol.side_effect = AssertionError("legacy M5 load is forbidden")
        loader.resample.side_effect = AssertionError("replay resampling is forbidden")
        features = frame.copy()
        features["REGIME_LIVE"] = ["CHOP", "TREND"] * 100
        strategy = MagicMock()
        strategy.generate_signals.return_value = pd.Series(1, index=features.index)
        backtester = MagicMock()
        backtester.run.return_value = {
            "trades": [],
            "total_trades": 0,
            "total_pnl": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "profit_factor": 0.0,
        }
        cfg = SimpleNamespace(initial_capital=10000.0, regime_params_file="regime.json")
        cell = {
            "symbol": "EURUSD",
            "timeframe": "H4",
            "regime": "CHOP",
            "strategy_name": "NativeProbeStrategy",
            "parameters": {},
        }

        with patch.object(
            replay_tools.FeatureComputer, "compute_all", return_value=features
        ), patch.object(
            replay_tools.StrategyRegistry, "get", return_value=strategy
        ), patch.object(
            replay_tools, "Backtester", return_value=backtester
        ), patch.object(
            replay_tools, "get_instrument_spec", return_value=object()
        ):
            row = replay_tools.replay_cell(cfg, loader, cell)

        self.assertEqual(row["error"], "")
        self.assertEqual(row["native_timeframe"], "H4")
        self.assertEqual(row["native_activation_id"], "activation-test")
        self.assertEqual(row["regime_filter_column"], "REGIME_LIVE")
        loader.load_native_timeframe.assert_called_once_with("EURUSD", "H4")
        loader.load_symbol.assert_not_called()
        loader.resample.assert_not_called()
        replay_signals = backtester.run.call_args.args[1]
        self.assertEqual(int(replay_signals.abs().sum()), 100)
        self.assertTrue((features.loc[replay_signals.ne(0), "REGIME_LIVE"] == "CHOP").all())

    def test_missing_native_activation_fails_before_features(self):
        loader = MagicMock()
        loader.load_native_timeframe.return_value = (
            None,
            {
                "source_kind": "missing",
                "symbol": "EURUSD",
                "timeframe": "D1",
                "provenance_valid": False,
                "provenance_errors": ["NATIVE_ACTIVE_CELL_MISSING"],
            },
        )
        cfg = SimpleNamespace(initial_capital=10000.0, regime_params_file="regime.json")
        cell = {
            "symbol": "EURUSD",
            "timeframe": "D1",
            "regime": "TREND",
            "strategy_name": "NativeProbeStrategy",
            "parameters": {},
        }

        with patch.object(replay_tools.FeatureComputer, "compute_all") as compute:
            row = replay_tools.replay_cell(cfg, loader, cell)

        self.assertEqual(row["error"], "native_activation_unavailable")
        self.assertIn("NATIVE_ACTIVE_CELL_MISSING", row["native_source_errors"])
        compute.assert_not_called()
        loader.load_symbol.assert_not_called()
        loader.resample.assert_not_called()

    def test_unpinned_or_invalid_native_source_fails_closed(self):
        loader = MagicMock()
        frame = self._frame()
        invalid = self._meta("EURUSD", "H1")
        invalid["activation_id"] = ""
        loader.load_native_timeframe.return_value = (frame, invalid)

        _frame, _evidence, error = replay_tools.load_activated_native_replay_frame(
            loader, "EURUSD", "H1"
        )

        self.assertIsNone(_frame)
        self.assertEqual(error, "native_activation_unpinned")
        loader.load_symbol.assert_not_called()
        loader.resample.assert_not_called()


if __name__ == "__main__":
    unittest.main()
