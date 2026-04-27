import logging
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from pm_main import LiveTrader
from pm_pipeline import RegimeConfig, SymbolConfig


class DummyMT5:
    def get_bars(self, symbol, timeframe, count=200):
        idx = pd.date_range("2026-02-04 00:00:00", periods=3, freq="h")
        return pd.DataFrame({"open": [1.0, 1.1, 1.2]}, index=idx)


class DummyStrategy:
    def generate_signals(self, features: pd.DataFrame, symbol: str):
        return pd.Series([0, 1, 1], index=features.index)


def make_trader():
    trader = LiveTrader.__new__(LiveTrader)
    trader.mt5 = DummyMT5()
    trader.pipeline_config = SimpleNamespace(
        live_bars_count=200,
        live_min_bars=2,
        regime_params_file="regime_params.json",
        regime_freshness_decay=1.0,
        regime_min_val_profit_factor=1.0,
        regime_min_val_return_pct=0.0,
        fx_val_max_drawdown=35.0,
        live_loop_trigger_mode="scheduled",
    )
    trader._candidate_cache = {}
    trader._last_bar_times = {}
    trader._cache_hits = 0
    trader._cache_misses = 0
    trader._max_cache_size = 100
    trader.logger = logging.getLogger("test_winners_only")
    return trader


def make_features():
    idx = pd.date_range("2026-02-04 00:00:00", periods=3, freq="h")
    return pd.DataFrame(
        {
            "REGIME": ["TREND", "TREND", "TREND"],
            "REGIME_STRENGTH": [0.8, 0.8, 0.8],
        },
        index=idx,
    )


class WinnersOnlyTests(unittest.TestCase):
    @patch("pm_main.FeatureComputer.compute_all")
    @patch("pm_strategies.StrategyRegistry.get")
    def test_winners_only_skips_timeframe_without_winner(self, mock_get, mock_compute):
        mock_get.return_value = DummyStrategy()
        mock_compute.return_value = make_features()

        valid_cfg = RegimeConfig(
            strategy_name="TrendStrategy",
            parameters={},
            quality_score=0.8,
            train_metrics={},
            val_metrics={"profit_factor": 2.0, "total_return_pct": 10.0, "max_drawdown_pct": 5.0},
        )

        symbol_cfg = SymbolConfig(
            symbol="TONUSD",
            regime_configs={
                "H1": {"TREND": valid_cfg},
                "M5": {"RANGE": valid_cfg},  # no TREND winner for M5
            },
            default_config=valid_cfg,
        )

        trader = make_trader()
        candidates, stats = trader._evaluate_regime_candidates("TONUSD", "TONUSD", symbol_cfg)

        timeframes = {c["timeframe"] for c in candidates}
        self.assertEqual(timeframes, {"H1"})
        self.assertGreaterEqual(stats.get("no_winner", 0), 1)

    @patch("pm_main.FeatureComputer.compute_all")
    @patch("pm_strategies.StrategyRegistry.get")
    def test_default_config_not_used_when_winner_fails_gate(self, mock_get, mock_compute):
        mock_get.return_value = DummyStrategy()
        mock_compute.return_value = make_features()

        bad_cfg = RegimeConfig(
            strategy_name="BadStrategy",
            parameters={},
            quality_score=0.5,
            train_metrics={},
            val_metrics={"profit_factor": 0.5, "total_return_pct": -5.0, "max_drawdown_pct": 50.0},
        )
        default_cfg = RegimeConfig(
            strategy_name="DefaultStrategy",
            parameters={},
            quality_score=0.9,
            train_metrics={},
            val_metrics={"profit_factor": 2.0, "total_return_pct": 10.0, "max_drawdown_pct": 5.0},
        )

        symbol_cfg = SymbolConfig(
            symbol="TONUSD",
            regime_configs={"M5": {"TREND": bad_cfg}},
            default_config=default_cfg,
        )

        trader = make_trader()
        candidates, stats = trader._evaluate_regime_candidates("TONUSD", "TONUSD", symbol_cfg)

        self.assertEqual(len(candidates), 0)
        self.assertGreaterEqual(stats.get("winner_failed_gate", 0), 1)
        mock_get.assert_not_called()

    @patch("pm_main.FeatureComputer.compute_all")
    @patch("pm_strategies.StrategyRegistry.get")
    def test_winners_only_skips_stale_timeframe_before_bar_load(self, mock_get, mock_compute):
        valid_cfg = RegimeConfig(
            strategy_name="TrendStrategy",
            parameters={},
            quality_score=0.8,
            train_metrics={},
            val_metrics={"profit_factor": 2.0, "total_return_pct": 10.0, "max_drawdown_pct": 5.0},
        )

        symbol_cfg = SymbolConfig(
            symbol="TONUSD",
            regime_configs={"H1": {"TREND": valid_cfg}},
            default_config=valid_cfg,
        )

        trader = make_trader()
        trader.mt5 = MagicMock()
        trader.mt5.get_bars.side_effect = AssertionError("stale timeframe should not load bars")
        trader._last_bar_times["TONUSD_H1"] = pd.Timestamp.now()

        candidates, stats = trader._evaluate_regime_candidates("TONUSD", "TONUSD", symbol_cfg)

        self.assertEqual(candidates, [])
        self.assertEqual(stats.get("timeframes_skipped_stale", 0), 1)
        mock_compute.assert_not_called()
        mock_get.assert_not_called()

    @patch("pm_main.FeatureComputer.compute_all")
    @patch("pm_strategies.StrategyRegistry.get")
    def test_zero_quality_score_is_preserved_in_live_selection(self, mock_get, mock_compute):
        mock_get.return_value = DummyStrategy()
        mock_compute.return_value = make_features()

        valid_cfg = RegimeConfig(
            strategy_name="TrendStrategy",
            parameters={},
            quality_score=0.0,
            train_metrics={},
            val_metrics={"profit_factor": 2.0, "total_return_pct": 10.0, "max_drawdown_pct": 5.0},
        )

        symbol_cfg = SymbolConfig(
            symbol="TONUSD",
            regime_configs={"H1": {"TREND": valid_cfg}},
            default_config=valid_cfg,
        )

        trader = make_trader()
        candidates, _stats = trader._evaluate_regime_candidates("TONUSD", "TONUSD", symbol_cfg)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["quality_score"], 0.0)
        self.assertEqual(candidates[0]["selection_score"], 0.0)


if __name__ == "__main__":
    unittest.main()
