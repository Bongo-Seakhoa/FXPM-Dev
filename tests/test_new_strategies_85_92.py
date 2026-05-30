"""Tests for Package L structure, VWAP, volume, and efficiency-ratio strategies."""

import numpy as np
import pandas as pd
import pytest

import pm_strategies
from pm_strategies import (
    AnchoredVWAPReclaimStrategy,
    EfficiencyRatioMeanReversionStrategy,
    EfficiencyRatioTrendStrategy,
    FractalChannelBreakoutStrategy,
    OpeningRangeRetestStrategy,
    SessionVWAPPullbackTrendStrategy,
    StrategyCategory,
    StrategyRegistry,
    SwingFailureReversalStrategy,
    VolumeClimaxReversalStrategy,
    _GLOBAL_SL_GRID,
    _GLOBAL_TP_GRID,
)

TEST_SYMBOL = "EURUSD"

PACKAGE_L_STRATEGIES = [
    "FractalChannelBreakoutStrategy",
    "SwingFailureReversalStrategy",
    "AnchoredVWAPReclaimStrategy",
    "VolumeClimaxReversalStrategy",
    "EfficiencyRatioTrendStrategy",
    "EfficiencyRatioMeanReversionStrategy",
    "OpeningRangeRetestStrategy",
    "SessionVWAPPullbackTrendStrategy",
]

EXPECTED_CATEGORIES = {
    "FractalChannelBreakoutStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "SwingFailureReversalStrategy": StrategyCategory.MEAN_REVERSION,
    "AnchoredVWAPReclaimStrategy": StrategyCategory.TREND_FOLLOWING,
    "VolumeClimaxReversalStrategy": StrategyCategory.MEAN_REVERSION,
    "EfficiencyRatioTrendStrategy": StrategyCategory.TREND_FOLLOWING,
    "EfficiencyRatioMeanReversionStrategy": StrategyCategory.MEAN_REVERSION,
    "OpeningRangeRetestStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "SessionVWAPPullbackTrendStrategy": StrategyCategory.TREND_FOLLOWING,
}


def _make_features(n: int = 1200, seed: int = 1001) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    idx = pd.date_range("2025-01-01 00:00:00", periods=n, freq="h")
    trend = np.linspace(0, 0.04, n)
    cycles = np.sin(np.linspace(0, 42, n)) * 0.003
    close = 1.1000 + trend + cycles + np.cumsum(rng.randn(n) * 0.0008)
    open_ = close + rng.randn(n) * 0.0004
    high = np.maximum(open_, close) + rng.uniform(0.0002, 0.0018, n)
    low = np.minimum(open_, close) - rng.uniform(0.0002, 0.0018, n)
    volume = rng.randint(100, 9000, n).astype(float)
    df = pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close,
         "Volume": volume, "RealVolume": volume},
        index=idx,
    )
    tr = np.maximum(df["High"] - df["Low"],
                    np.maximum((df["High"] - df["Close"].shift(1)).abs(),
                               (df["Low"] - df["Close"].shift(1)).abs()))
    for period in (10, 14, 20):
        df[f"ATR_{period}"] = tr.rolling(period).mean()
    return df


class TestPackageLRegistry:
    def test_all_registered(self):
        names = set(StrategyRegistry.list_all())
        for name in PACKAGE_L_STRATEGIES:
            assert name in names

    @pytest.mark.parametrize("name", PACKAGE_L_STRATEGIES)
    def test_category(self, name):
        assert StrategyRegistry.get(name).category == EXPECTED_CATEGORIES[name]


class TestPackageLSignalContract:
    @pytest.mark.parametrize("name", PACKAGE_L_STRATEGIES)
    def test_signal_values_in_range(self, name):
        features = _make_features()
        signals = StrategyRegistry.get(name).generate_signals(features.copy(), TEST_SYMBOL)
        assert isinstance(signals, pd.Series)
        assert len(signals) == len(features)
        assert signals.index.equals(features.index)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    @pytest.mark.parametrize("name", PACKAGE_L_STRATEGIES)
    def test_no_lookahead_truncation(self, name):
        features = _make_features(n=1100, seed=808)
        instance = StrategyRegistry.get(name)
        full = instance.generate_signals(features.copy(), TEST_SYMBOL)
        cutoff = 700
        trunc = instance.generate_signals(features.iloc[:cutoff].copy(), TEST_SYMBOL)
        np.testing.assert_array_equal(
            full.iloc[:cutoff].values, trunc.values,
            err_msg=f"{name} has lookahead-sensitive signals",
        )

    def test_opening_range_retest_uses_prior_breakout_only(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=60, freq="h")
        close = np.full(len(idx), 100.0)
        high = close + 0.5
        low = close - 0.5
        close[30] = 102.0
        high[30] = 102.2
        low[30] = 101.8
        close[31] = 101.5
        high[31] = 101.8
        low[31] = 100.4
        features = pd.DataFrame(
            {
                "Open": close,
                "High": high,
                "Low": low,
                "Close": close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        sig = OpeningRangeRetestStrategy(
            session_start_hour=0,
            range_bars=3,
            retest_window=6,
            retest_atr=0.5,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[30] == 0
        assert sig.iloc[31] == 1

    def test_opening_range_retest_does_not_carry_breakout_across_sessions(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=48, freq="h")
        close = np.full(len(idx), 100.0)
        high = close + 0.5
        low = close - 0.5
        close[20] = 102.0
        high[20] = 102.2
        low[20] = 101.8
        close[24] = 101.5
        high[24] = 101.8
        low[24] = 100.4
        features = pd.DataFrame(
            {
                "Open": close,
                "High": high,
                "Low": low,
                "Close": close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        sig = OpeningRangeRetestStrategy(
            session_start_hour=0,
            range_bars=3,
            retest_window=12,
            retest_atr=0.5,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[24] == 0

    def test_opening_range_retest_emits_first_retest_only(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=60, freq="h")
        close = np.full(len(idx), 100.0)
        high = close + 0.5
        low = close - 0.5
        close[30] = 102.0
        high[30] = 102.2
        low[30] = 101.8
        close[31:34] = 101.5
        high[31:34] = 101.8
        low[31:34] = 100.4
        features = pd.DataFrame(
            {
                "Open": close,
                "High": high,
                "Low": low,
                "Close": close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        sig = OpeningRangeRetestStrategy(
            session_start_hour=0,
            range_bars=3,
            retest_window=6,
            retest_atr=0.5,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[31] == 1
        assert sig.iloc[32:34].abs().sum() == 0

    def test_fractal_breakout_waits_for_confirmed_pivot(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=40, freq="h")
        close = np.linspace(100.0, 100.4, len(idx))
        high = close + 0.2
        low = close - 0.2
        high[20] = 105.0
        close[20] = 104.5
        close[23] = 106.0
        high[23] = 106.2
        features = pd.DataFrame(
            {
                "Open": close,
                "High": high,
                "Low": low,
                "Close": close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 0.5,
            },
            index=idx,
        )
        sig = FractalChannelBreakoutStrategy(wing_bars=1, atr_buffer=0.0).generate_signals(
            features.copy(), TEST_SYMBOL
        )
        assert sig.iloc[20] == 0
        assert sig.iloc[23] == 1

    def test_anchored_vwap_reclaim_does_not_use_prior_session_flush(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=48, freq="h")
        close = np.full(len(idx), 100.0)
        close[23] = 95.0
        close[24] = 101.0
        features = pd.DataFrame(
            {
                "Open": close,
                "High": close + 0.5,
                "Low": close - 2.0,
                "Close": close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        sig = AnchoredVWAPReclaimStrategy(
            session_start_hour=0,
            ema_period=2,
            flush_atr=0.0,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[24] == 0

    def test_session_vwap_pullback_does_not_use_prior_session_pullback(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=48, freq="h")
        close = np.full(len(idx), 100.0)
        close[22] = 99.0
        close[23] = 99.5
        close[24] = 103.0
        features = pd.DataFrame(
            {
                "Open": close,
                "High": close + 0.5,
                "Low": close - 1.0,
                "Close": close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        sig = SessionVWAPPullbackTrendStrategy(
            session_start_hour=0,
            ema_period=2,
            slope_bars=1,
            pullback_atr=1.0,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[24] == 0

    def test_swing_failure_reversal_closes_back_inside_prior_range(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=40, freq="h")
        close = np.full(len(idx), 100.0)
        high = close + 0.5
        low = close - 0.5
        low[25] = 98.0
        close[25] = 99.8
        features = pd.DataFrame(
            {
                "Open": close,
                "High": high,
                "Low": low,
                "Close": close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        sig = SwingFailureReversalStrategy(
            lookback=10,
            sweep_atr=0.0,
            close_back_atr=0.0,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[25] == 1

    def test_volume_climax_reversal_requires_volume_range_and_rejection(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=60, freq="h")
        close = np.full(len(idx), 100.0)
        open_ = close.copy()
        high = close + 0.5
        low = close - 0.5
        volume = np.full(len(idx), 1000.0)
        open_[40] = 99.0
        close[40] = 101.0
        high[40] = 101.2
        low[40] = 95.0
        volume[40] = 5000.0
        features = pd.DataFrame(
            {
                "Open": open_,
                "High": high,
                "Low": low,
                "Close": close,
                "Volume": volume,
                "RealVolume": volume,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        sig = VolumeClimaxReversalStrategy(
            vol_lookback=10,
            volume_mult=2.0,
            range_atr=2.0,
            wick_frac=0.4,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[40] == 1

    def test_efficiency_ratio_strategies_emit_state_transitions_not_every_bar(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=80, freq="h")
        trend_close = np.r_[np.full(30, 100.0), np.linspace(100.0, 120.0, 50)]
        trend_features = pd.DataFrame(
            {
                "Open": trend_close,
                "High": trend_close + 0.5,
                "Low": trend_close - 0.5,
                "Close": trend_close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        trend_sig = EfficiencyRatioTrendStrategy(
            er_period=3,
            min_er=0.1,
            ema_period=3,
            slope_bars=1,
        ).generate_signals(trend_features.copy(), TEST_SYMBOL)
        assert trend_sig.iloc[35:45].eq(1).sum() <= 1

        mr_close = np.full(80, 100.0)
        mr_close[40:43] = 96.0
        mr_features = pd.DataFrame(
            {
                "Open": mr_close,
                "High": mr_close + 0.5,
                "Low": mr_close - 0.5,
                "Close": mr_close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        mr_sig = EfficiencyRatioMeanReversionStrategy(
            er_period=3,
            max_er=1.0,
            z_period=10,
            z_entry=1.0,
        ).generate_signals(mr_features.copy(), TEST_SYMBOL)
        assert mr_sig.iloc[40:43].eq(1).sum() <= 1


class TestPackageLParamGridAndStops:
    @pytest.mark.parametrize("name", PACKAGE_L_STRATEGIES)
    def test_sl_tp_standardized(self, name):
        grid = StrategyRegistry.get(name).get_param_grid()
        assert grid["sl_atr_mult"] == _GLOBAL_SL_GRID
        assert grid["tp_atr_mult"] == _GLOBAL_TP_GRID

    @pytest.mark.parametrize("name", PACKAGE_L_STRATEGIES)
    def test_defaults_in_grid(self, name):
        instance = StrategyRegistry.get(name)
        defaults = instance.get_default_params()
        grid = instance.get_param_grid()
        for key, val in defaults.items():
            if key in {"sl_atr_mult", "tp_atr_mult", "structural_sl_lookback", "use_structural_sl"}:
                continue
            if key in grid:
                assert val in grid[key], f"{name}: default {key}={val} missing from grid"

    @pytest.mark.parametrize("name", PACKAGE_L_STRATEGIES)
    def test_calculate_stops_valid(self, name):
        features = _make_features()
        instance = StrategyRegistry.get(name)
        sl, tp = instance.calculate_stops(features, 1, TEST_SYMBOL, bar_index=len(features) - 1)
        assert isinstance(sl, float) and isinstance(tp, float)
        assert sl > 0 and tp > 0

    def test_package_l_in_all_exports(self):
        for name in PACKAGE_L_STRATEGIES:
            assert name in pm_strategies.__all__

    def test_instantiation_smoke(self):
        features = _make_features()
        for cls in (FractalChannelBreakoutStrategy, SwingFailureReversalStrategy,
                    AnchoredVWAPReclaimStrategy, VolumeClimaxReversalStrategy,
                    EfficiencyRatioTrendStrategy, EfficiencyRatioMeanReversionStrategy,
                    OpeningRangeRetestStrategy, SessionVWAPPullbackTrendStrategy):
            sig = cls().generate_signals(features.copy(), TEST_SYMBOL)
            assert len(sig) == len(features)
            assert not sig.isna().any()
