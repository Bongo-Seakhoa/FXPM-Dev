"""Tests for regime-aware TP multipliers as a downstream execution layer."""
import math
import unittest

import numpy as np
import pandas as pd

import pm_strategies
from pm_core import Backtester, PipelineConfig, get_instrument_spec
from pm_strategies import (
    BaseStrategy,
    _DEFAULT_REGIME_TP_MULTIPLIERS,
    get_regime_tp_multipliers,
    set_regime_tp_multipliers,
)


def _make_features(n: int = 60, regime_live: str = "TREND") -> pd.DataFrame:
    rng = np.random.default_rng(3)
    close = 1.10 + rng.normal(0, 0.001, n).cumsum()
    high = close + 0.0005
    low = close - 0.0005
    df = pd.DataFrame({
        "Open": close,
        "High": high,
        "Low": low,
        "Close": close,
        "Volume": np.full(n, 1000),
        # ATR_14 must be strictly positive so calculate_stops produces
        # non-floor TP we can inspect the multiplier against.
        "ATR_14": np.full(n, 0.0025),
        "REGIME_LIVE": [regime_live] * n,
    })
    return df


def _make_flat_features(n: int = 60, regime_live: str = "TREND") -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=n, freq="h")
    return pd.DataFrame({
        "Open": np.full(n, 1.1000),
        "High": np.full(n, 1.1004),
        "Low": np.full(n, 1.0996),
        "Close": np.full(n, 1.1000),
        "Volume": np.full(n, 1000),
        "ATR_14": np.full(n, 0.0025),
        "REGIME_LIVE": [regime_live] * n,
    }, index=index)


class _MinimalStrategy(BaseStrategy):
    """Strategy with fixed sl/tp ATR multiples so we can isolate the regime
    multiplier in the TP output."""

    def __init__(self):
        super().__init__(sl_atr_mult=2.0, tp_atr_mult=3.0)

    @property
    def name(self) -> str:
        return "_tp_multiplier_probe"

    @property
    def category(self) -> str:
        return "test"

    def get_default_params(self):
        return {"sl_atr_mult": 2.0, "tp_atr_mult": 3.0}

    def generate_signals(self, features):
        return pd.Series(0, index=features.index, dtype=int)


class TradeIntentRegimeTpMultiplierTests(unittest.TestCase):
    def setUp(self):
        # Defensive: each test resets to module defaults so overrides don't leak.
        set_regime_tp_multipliers(None)
        self.strat = _MinimalStrategy()

    def tearDown(self):
        set_regime_tp_multipliers(None)

    def _base_tp(self) -> float:
        """TP without regime multiplier applied (hand-computed baseline)."""
        # ATR=0.0025 on EURUSD pip_size=0.0001 → atr_pips=25, tp = 25 * 3 = 75.
        return 75.0

    def _intent_tp(self, df: pd.DataFrame) -> float:
        intent = self.strat.build_trade_intent(
            df,
            symbol="EURUSD",
            timeframe="H1",
            regime="TREND",
            signal=1,
            bar_index=len(df) - 1,
        )
        return float(intent.take_profit_pips)

    def test_calculate_stops_remains_raw_stop_primitive(self):
        df = _make_features(regime_live="TREND")
        _, tp = self.strat.calculate_stops(df, signal=1, symbol="EURUSD", bar_index=len(df) - 1)
        self.assertAlmostEqual(tp, self._base_tp(), places=4)

    def test_backtester_uses_trade_intent_exit_surface(self):
        df = _make_flat_features(regime_live="TREND")
        signals = pd.Series(0, index=df.index, dtype=int)
        signals.iloc[20] = 1
        config = PipelineConfig(
            use_spread=False,
            use_commission=False,
            use_slippage=False,
            regime_tp_multipliers={"TREND": 1.25},
        )
        spec = get_instrument_spec("EURUSD")
        result = Backtester(config).run(
            df,
            signals,
            "EURUSD",
            self.strat,
            spec=spec,
            timeframe="H1",
            warmup_bars=14,
        )
        self.assertEqual(len(result["trades"]), 1)
        trade = result["trades"][0]
        tp_distance = spec.price_to_pips(trade["take_profit"] - trade["entry_price"])
        self.assertAlmostEqual(tp_distance, self._base_tp() * 1.25, places=4)

    def test_trend_inflates_tp(self):
        df = _make_features(regime_live="TREND")
        self.assertAlmostEqual(self._intent_tp(df), self._base_tp() * 1.25, places=4)

    def test_breakout_inflates_tp(self):
        df = _make_features(regime_live="BREAKOUT")
        self.assertAlmostEqual(self._intent_tp(df), self._base_tp() * 1.15, places=4)

    def test_range_shrinks_tp(self):
        df = _make_features(regime_live="RANGE")
        self.assertAlmostEqual(self._intent_tp(df), self._base_tp() * 0.85, places=4)

    def test_chop_shrinks_tp(self):
        df = _make_features(regime_live="CHOP")
        self.assertAlmostEqual(self._intent_tp(df), self._base_tp() * 0.75, places=4)

    def test_missing_regime_column_is_no_op(self):
        df = _make_features(regime_live="TREND").drop(columns=["REGIME_LIVE"])
        self.assertAlmostEqual(self._intent_tp(df), self._base_tp(), places=4)

    def test_falls_back_to_regime_column_when_live_absent(self):
        df = _make_features(regime_live="TREND").drop(columns=["REGIME_LIVE"])
        df["REGIME"] = "TREND"
        self.assertAlmostEqual(self._intent_tp(df), self._base_tp() * 1.25, places=4)

    def test_unknown_regime_label_is_no_op(self):
        df = _make_features(regime_live="UNDISCOVERED_REGIME")
        self.assertAlmostEqual(self._intent_tp(df), self._base_tp(), places=4)

    def test_nan_regime_value_is_no_op(self):
        df = _make_features(regime_live="TREND")
        df["REGIME_LIVE"] = np.nan
        self.assertAlmostEqual(self._intent_tp(df), self._base_tp(), places=4)

    def test_case_insensitive_label_match(self):
        df = _make_features(regime_live="trend")
        self.assertAlmostEqual(self._intent_tp(df), self._base_tp() * 1.25, places=4)

    def test_tp_floor_is_still_enforced(self):
        df = _make_features(regime_live="CHOP")
        df.loc[:, "ATR_14"] = 0.00001  # 0.1 pips on EURUSD
        self.assertGreaterEqual(self._intent_tp(df), 10.0)


class OperatorOverrideTests(unittest.TestCase):
    """Operator override via set_regime_tp_multipliers() behaves as spec'd."""

    def setUp(self):
        set_regime_tp_multipliers(None)

    def tearDown(self):
        set_regime_tp_multipliers(None)

    def test_default_table_matches_findings(self):
        self.assertEqual(
            _DEFAULT_REGIME_TP_MULTIPLIERS,
            {"TREND": 1.25, "BREAKOUT": 1.15, "RANGE": 0.85, "CHOP": 0.75},
        )

    def test_override_replaces_active_table(self):
        set_regime_tp_multipliers({"TREND": 2.0, "CHOP": 0.5})
        active = get_regime_tp_multipliers()
        self.assertEqual(active, {"TREND": 2.0, "CHOP": 0.5})

    def test_override_upper_cases_keys(self):
        set_regime_tp_multipliers({"trend": 1.4})
        self.assertIn("TREND", get_regime_tp_multipliers())

    def test_override_drops_non_positive_and_non_numeric(self):
        set_regime_tp_multipliers({"TREND": 1.5, "CHOP": 0, "RANGE": -1.0, "BREAKOUT": "abc"})
        self.assertEqual(get_regime_tp_multipliers(), {"TREND": 1.5})

    def test_empty_override_resets_to_defaults(self):
        set_regime_tp_multipliers({"TREND": 1.5})
        set_regime_tp_multipliers({})
        self.assertEqual(get_regime_tp_multipliers(), _DEFAULT_REGIME_TP_MULTIPLIERS)

    def test_none_override_resets_to_defaults(self):
        set_regime_tp_multipliers({"TREND": 1.5})
        set_regime_tp_multipliers(None)
        self.assertEqual(get_regime_tp_multipliers(), _DEFAULT_REGIME_TP_MULTIPLIERS)

    def test_all_invalid_override_falls_back_to_defaults(self):
        # Every entry is invalid (non-positive / non-numeric) → treat as empty.
        set_regime_tp_multipliers({"TREND": -1, "CHOP": None, "RANGE": "nope"})
        self.assertEqual(get_regime_tp_multipliers(), _DEFAULT_REGIME_TP_MULTIPLIERS)


class PositionConfigWiringTests(unittest.TestCase):
    """PositionConfig.regime_tp_multipliers carries the table into the app."""

    def test_position_config_default_matches_spec(self):
        from pm_position import PositionConfig
        cfg = PositionConfig()
        self.assertEqual(
            cfg.regime_tp_multipliers,
            {"TREND": 1.25, "BREAKOUT": 1.15, "RANGE": 0.85, "CHOP": 0.75},
        )

    def test_position_config_defaults_are_isolated_per_instance(self):
        """field(default_factory=...) should give each PositionConfig its own
        dict so mutating one config does not leak into another."""
        from pm_position import PositionConfig
        a = PositionConfig()
        b = PositionConfig()
        a.regime_tp_multipliers["TREND"] = 99.0
        self.assertEqual(b.regime_tp_multipliers["TREND"], 1.25)

    def test_app_copies_position_multipliers_to_pipeline_config(self):
        from types import SimpleNamespace
        from pm_main import FXPortfolioManagerApp
        from pm_position import PositionConfig

        pipeline_config = PipelineConfig(regime_tp_multipliers={"TREND": 1.0})
        position_config = PositionConfig(regime_tp_multipliers={"TREND": 2.0})
        app = FXPortfolioManagerApp(
            symbols=["EURUSD"],
            pipeline_config=pipeline_config,
            position_config=position_config,
            mt5_config=SimpleNamespace(),
        )
        self.assertEqual(app.pipeline_config.regime_tp_multipliers, {"TREND": 2.0})


if __name__ == "__main__":
    unittest.main()
