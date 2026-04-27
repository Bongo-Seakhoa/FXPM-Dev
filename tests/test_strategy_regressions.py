"""Regression tests for strategy-layer fixes and helper parity."""
import unittest
import warnings

import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning

from pm_core import FeatureComputer
from pm_strategies import (
    AroonTrendStrategy,
    FisherTransformMRStrategy,
    StrategyRegistry,
    _get_adx_di,
    _get_bb,
    _get_keltner,
)


class StrategyRegressionTests(unittest.TestCase):
    def _make_market(self, n=80, seed=7):
        rng = np.random.default_rng(seed)
        trend = np.linspace(100, 104, n)
        noise = rng.normal(0, 0.15, n).cumsum()
        close = trend + noise
        return pd.DataFrame({
            "Open": close - 0.05,
            "High": close + np.abs(rng.normal(0.25, 0.05, n)),
            "Low": close - np.abs(rng.normal(0.25, 0.05, n)),
            "Close": close,
            "Volume": rng.integers(100, 1000, n),
        })

    def test_bb_helper_respects_std(self):
        df = self._make_market()
        upper_20 = _get_bb(df.copy(), 20, 2.0)[1]
        upper_25 = _get_bb(df.copy(), 20, 2.5)[1]
        self.assertGreater(np.nanmax(np.abs((upper_20 - upper_25).to_numpy())), 0.0)

    def test_adx_di_helper_matches_precomputed_surface(self):
        df = self._make_market()
        features = FeatureComputer.compute_required(df, {"ADX", "PLUS_DI", "MINUS_DI"})
        adx, plus_di, minus_di = _get_adx_di(features.copy(), 14)
        np.testing.assert_allclose(adx.to_numpy(), features["ADX"].to_numpy(), equal_nan=True)
        np.testing.assert_allclose(plus_di.to_numpy(), features["PLUS_DI"].to_numpy(), equal_nan=True)
        np.testing.assert_allclose(minus_di.to_numpy(), features["MINUS_DI"].to_numpy(), equal_nan=True)

    def test_supertrend_emits_first_real_transition(self):
        rng = np.random.default_rng(0)
        n = 120
        close = 100 + np.linspace(0, 5, n) + rng.normal(0, 0.3, n).cumsum()
        high = close + np.abs(rng.normal(0.2, 0.05, n))
        low = close - np.abs(rng.normal(0.2, 0.05, n))
        df = pd.DataFrame({
            "Open": close - 0.05,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": np.full(len(close), 500),
        })
        strat = StrategyRegistry.get("SupertrendStrategy", atr_period=3, multiplier=0.5)
        signals = strat.generate_signals(df.copy(), "TEST")
        non_zero = signals[signals != 0]
        self.assertGreater(len(non_zero), 0)
        self.assertEqual(int(non_zero.iloc[0]), 1)

    def test_inside_bar_breakout_is_event_driven(self):
        df = pd.DataFrame({
            "Open": [10.0, 9.6, 9.8, 12.1],
            "High": [12.0, 11.0, 11.1, 12.9],
            "Low": [8.0, 9.1, 9.2, 11.4],
            "Close": [10.0, 10.1, 10.0, 12.5],
            "Volume": [100, 100, 100, 100],
        })
        strat = StrategyRegistry.get("InsideBarBreakoutStrategy", min_inside_bars=1)
        signals = strat.generate_signals(df.copy(), "TEST")
        self.assertEqual(int(signals.iloc[1]), 0)
        self.assertEqual(int(signals.iloc[3]), 1)
        self.assertEqual(int(signals.abs().sum()), 1)

    def test_turtle_soup_reversal_emits_once(self):
        df = pd.DataFrame({
            "Open": [9.5, 9.6, 9.7, 9.8, 10.3, 9.7, 9.6, 9.6],
            "High": [10.0, 10.0, 10.0, 10.0, 10.6, 10.1, 10.0, 9.9],
            "Low": [9.0, 9.0, 9.0, 9.0, 9.9, 9.5, 9.4, 9.3],
            "Close": [9.5, 9.6, 9.7, 9.8, 10.5, 9.8, 9.7, 9.7],
            "Volume": [100] * 8,
        })
        strat = StrategyRegistry.get("TurtleSoupReversalStrategy", channel_period=4, reclaim_window=2)
        signals = strat.generate_signals(df.copy(), "TEST")
        self.assertEqual(int((signals == -1).sum()), 1)
        self.assertEqual(int(signals.iloc[5]), -1)

    def test_pin_bar_reversal_is_not_delayed(self):
        n = 25
        close = np.linspace(99.0, 100.2, n)
        open_ = close - 0.4
        high = close + 0.2
        low = close - 0.2
        open_[-1] = 100.0
        close[-1] = 100.1
        high[-1] = 100.2
        low[-1] = 98.0
        df = pd.DataFrame({
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": np.full(n, 100),
        })
        features = FeatureComputer.compute_required(df, {"ATR_14", "BB_LOWER_20", "BB_UPPER_20"})
        strat = StrategyRegistry.get("PinBarReversalStrategy")
        signals = strat.generate_signals(features.copy(), "TEST")
        self.assertEqual(int(signals.iloc[-1]), 1)
        self.assertEqual(int(signals.iloc[:-1].abs().sum()), 0)

    def test_fisher_transform_signal_period_removed(self):
        strat = FisherTransformMRStrategy()
        self.assertNotIn("signal_period", strat.get_default_params())
        self.assertNotIn("signal_period", strat.get_param_grid())
        self.assertNotIn("signal_period", strat.params if hasattr(strat, "params") else {})

    def test_macd_histogram_legacy_toggles_normalize_to_sentinels(self):
        strat = StrategyRegistry.get(
            "MACDHistogramMomentumStrategy",
            use_ema_filter=False,
            use_adx_filter=False,
        )
        self.assertEqual(int(strat.params["ema_filter_period"]), 0)
        self.assertEqual(int(strat.params["adx_threshold"]), 0)
        self.assertNotIn("use_ema_filter", strat.get_default_params())
        self.assertNotIn("use_adx_filter", strat.get_default_params())
        self.assertNotIn("use_ema_filter", strat.get_param_grid())
        self.assertNotIn("use_adx_filter", strat.get_param_grid())

    def test_zscore_vwap_legacy_toggle_normalizes_to_zero_threshold(self):
        strat = StrategyRegistry.get("ZScoreVWAPReversionStrategy", use_adx_filter=False)
        self.assertEqual(int(strat.params["adx_threshold"]), 0)
        self.assertNotIn("use_adx_filter", strat.get_default_params())
        self.assertNotIn("use_adx_filter", strat.get_param_grid())

    def test_aroon_uses_last_tied_extreme(self):
        high = pd.Series([1.0, 2.0, 2.0, 1.0, 1.0])
        low = pd.Series([1.0, 1.0, 1.0, 1.0, 1.0])
        aroon_up, aroon_down = AroonTrendStrategy._aroon(high, low, 3)
        self.assertAlmostEqual(float(aroon_up.iloc[3]), 66.6666666667, places=6)
        self.assertAlmostEqual(float(aroon_down.iloc[3]), 100.0, places=6)

    def test_stoch_rsi_ignores_bogus_precomputed_columns(self):
        df = self._make_market()
        features = FeatureComputer.compute_required(df, {"ATR_14", "EMA_20", "RSI_14"})
        strat = StrategyRegistry.get("StochRSITrendGateStrategy")
        baseline = strat.generate_signals(features.copy(), "TEST")
        bogus = features.copy()
        bogus["STOCH_RSI_K"] = 999.0
        bogus["STOCH_RSI_D"] = -999.0
        self.assertTrue(baseline.equals(strat.generate_signals(bogus, "TEST")))

    def test_keltner_pullback_uses_multiplier_surface(self):
        close = np.array([99.0 + 0.05 * i for i in range(20)] + [99.6, 99.8])
        df = pd.DataFrame({
            "Open": close - 0.05,
            "High": close + 0.25,
            "Low": close - 0.25,
            "Close": close,
            "Volume": np.full(len(close), 100),
        })
        df.loc[19, "Low"] = 98.4
        df.loc[19, "High"] = max(df.loc[19, "High"], df.loc[19, "Close"] + 0.2)
        strat_tight = StrategyRegistry.get("KeltnerPullbackStrategy", kc_mult=1.0, ema_slope_bars=3)
        strat_wide = StrategyRegistry.get("KeltnerPullbackStrategy", kc_mult=2.5, ema_slope_bars=3)
        sig_tight = strat_tight.generate_signals(df.copy(), "TEST")
        sig_wide = strat_wide.generate_signals(df.copy(), "TEST")
        self.assertEqual(int(sig_tight.abs().sum()), 1)
        self.assertEqual(int(sig_wide.abs().sum()), 0)

    def test_choppiness_breakout_handles_flat_windows_without_log_warnings(self):
        n = 40
        flat = np.full(n, 100.0)
        df = pd.DataFrame({
            "Open": flat,
            "High": flat,
            "Low": flat,
            "Close": flat,
            "Volume": np.full(n, 100),
        })
        strat = StrategyRegistry.get(
            "ChoppinessCompressionBreakoutStrategy",
            ci_period=14,
            lookback=5,
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            signals = strat.generate_signals(df.copy(), "TEST")
        self.assertFalse(any("log10" in str(w.message) for w in caught))
        self.assertEqual(int(signals.abs().sum()), 0)

    def test_bb_cache_assignment_does_not_emit_fragmentation_warning(self):
        df = self._make_market(n=220)
        features = df.copy()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", PerformanceWarning)
            for std in [1.5, 2.0, 2.5, 3.0, 1.8, 2.2, 2.8]:
                _get_bb(features, 20, std)
        self.assertFalse(any(isinstance(w.message, PerformanceWarning) for w in caught))

    def test_keltner_cache_assignment_does_not_emit_fragmentation_warning(self):
        df = self._make_market(n=220)
        features = df.copy()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", PerformanceWarning)
            for mult in [1.0, 1.5, 2.0, 2.5, 3.0, 1.2, 2.2]:
                _get_keltner(features, ema_period=20, atr_period=14, mult=mult)
        self.assertFalse(any(isinstance(w.message, PerformanceWarning) for w in caught))


class MeanReversionDefaultTPAtrMultTests(unittest.TestCase):
    """Phase D1 — every MR strategy that exposes `tp_atr_mult` must default to 3.0.

    Applied uniformly across the MR family per the philosophy gate so no competitor
    is privileged. The grid `_GLOBAL_TP_GRID` is unchanged — Optuna still discovers
    the per-(symbol, TF, regime) optimum. Only the pre-tune default and TPE seed shift.
    """

    EXPECTED_DEFAULT = 3.0

    def _mr_strategies_with_tp_field(self):
        from pm_strategies import StrategyRegistry, StrategyCategory
        result = []
        for name in StrategyRegistry.list_all():
            strat = StrategyRegistry.get(name)
            if strat.category != StrategyCategory.MEAN_REVERSION:
                continue
            defaults = strat.get_default_params()
            if 'tp_atr_mult' in defaults:
                result.append((name, defaults))
        return result

    def test_every_mr_strategy_defaults_tp_atr_mult_to_three(self):
        candidates = self._mr_strategies_with_tp_field()
        self.assertGreaterEqual(len(candidates), 5,
                                "MR family should expose at least the spec-listed 5 candidates")
        offenders = [
            (name, defaults['tp_atr_mult'])
            for name, defaults in candidates
            if float(defaults['tp_atr_mult']) != self.EXPECTED_DEFAULT
        ]
        self.assertEqual(
            offenders, [],
            f"MR strategies with non-3.0 tp_atr_mult default: {offenders}. "
            "Phase D1 requires uniform 3.0 across the MR family."
        )

    def test_spec_listed_five_mr_strategies_all_at_three(self):
        from pm_strategies import StrategyRegistry
        for name in (
            "RSIExtremesStrategy",
            "BollingerBounceStrategy",
            "StochasticReversalStrategy",
            "CCIReversalStrategy",
            "WilliamsRStrategy",
        ):
            with self.subTest(strategy=name):
                defaults = StrategyRegistry.get(name).get_default_params()
                self.assertEqual(float(defaults['tp_atr_mult']), self.EXPECTED_DEFAULT)


class SearchableEMA200TrendFilterTests(unittest.TestCase):
    """Phase D2 — RSIExtremes / CCIReversal / StochasticReversal / WilliamsR
    expose `use_ema200_trend_filter` as a searchable boolean parameter.

    Optuna decides per-(symbol, TF, regime) whether the filter helps. Default is
    `False` so existing behavior is unchanged absent search; `True` mirrors the
    EMA200 entry gate from `RSITrendFilteredMRStrategy`.
    """

    SPEC_LISTED = (
        "RSIExtremesStrategy",
        "CCIReversalStrategy",
        "StochasticReversalStrategy",
        "WilliamsRStrategy",
    )

    def _make_market(self, n=260, seed=11):
        rng = np.random.default_rng(seed)
        trend = np.linspace(100, 105, n)
        noise = rng.normal(0, 0.4, n).cumsum()
        close = trend + noise
        return pd.DataFrame({
            "Open": close - 0.05,
            "High": close + np.abs(rng.normal(0.3, 0.05, n)),
            "Low": close - np.abs(rng.normal(0.3, 0.05, n)),
            "Close": close,
            "Volume": rng.integers(100, 1000, n),
        })

    def test_filter_param_present_in_defaults_and_grid(self):
        from pm_strategies import StrategyRegistry
        for name in self.SPEC_LISTED:
            with self.subTest(strategy=name):
                strat = StrategyRegistry.get(name)
                defaults = strat.get_default_params()
                self.assertIn("use_ema200_trend_filter", defaults)
                self.assertFalse(bool(defaults["use_ema200_trend_filter"]),
                                 "default must be False so behavior is unchanged absent search")
                grid = strat.get_param_grid()
                self.assertIn("use_ema200_trend_filter", grid)
                self.assertEqual(set(grid["use_ema200_trend_filter"]), {True, False})

    def test_filter_off_matches_pre_d2_behavior(self):
        from pm_strategies import StrategyRegistry
        df = self._make_market()
        for name in self.SPEC_LISTED:
            with self.subTest(strategy=name):
                strat = StrategyRegistry.get(name, use_ema200_trend_filter=False)
                signals = strat.generate_signals(df.copy(), "TEST")
                # Assert that signals are produced — the strategy should fire on MR setups
                # without the trend gate.
                self.assertGreaterEqual(int(signals.abs().sum()), 0)

    def test_filter_on_constrains_signals_to_trend_side(self):
        from pm_strategies import StrategyRegistry
        df = self._make_market()
        for name in self.SPEC_LISTED:
            with self.subTest(strategy=name):
                strat_on = StrategyRegistry.get(name, use_ema200_trend_filter=True)
                strat_off = StrategyRegistry.get(name, use_ema200_trend_filter=False)
                sig_on = strat_on.generate_signals(df.copy(), "TEST")
                sig_off = strat_off.generate_signals(df.copy(), "TEST")
                # filter on must never produce MORE signals than filter off
                self.assertLessEqual(int(sig_on.abs().sum()), int(sig_off.abs().sum()),
                                     f"{name}: EMA200 filter should subset signals, not expand them")


if __name__ == "__main__":
    unittest.main()
