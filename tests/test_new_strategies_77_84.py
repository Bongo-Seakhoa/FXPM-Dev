"""Tests for Package K time/session/calendar and carry-aware strategies."""

import numpy as np
import pandas as pd
import pytest

from pm_strategies import (
    CarryFilteredPullbackStrategy,
    DayOfWeekBiasStrategy,
    IntradayRangeFadeStrategy,
    OvernightDriftStrategy,
    SessionCloseReversionStrategy,
    StrategyCategory,
    StrategyRegistry,
    SwapCarryTrendStrategy,
    TimeOfDayMomentumStrategy,
    TurnOfMonthStrategy,
    _GLOBAL_SL_GRID,
    _GLOBAL_TP_GRID,
)

TEST_SYMBOL = "EURUSD"

PACKAGE_K_STRATEGIES = [
    "TimeOfDayMomentumStrategy",
    "SessionCloseReversionStrategy",
    "DayOfWeekBiasStrategy",
    "TurnOfMonthStrategy",
    "OvernightDriftStrategy",
    "IntradayRangeFadeStrategy",
    "SwapCarryTrendStrategy",
    "CarryFilteredPullbackStrategy",
]

EXPECTED_CATEGORIES = {
    "TimeOfDayMomentumStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "SessionCloseReversionStrategy": StrategyCategory.MEAN_REVERSION,
    "DayOfWeekBiasStrategy": StrategyCategory.TREND_FOLLOWING,
    "TurnOfMonthStrategy": StrategyCategory.TREND_FOLLOWING,
    "OvernightDriftStrategy": StrategyCategory.TREND_FOLLOWING,
    "IntradayRangeFadeStrategy": StrategyCategory.MEAN_REVERSION,
    "SwapCarryTrendStrategy": StrategyCategory.TREND_FOLLOWING,
    "CarryFilteredPullbackStrategy": StrategyCategory.TREND_FOLLOWING,
}


def _make_features(n: int = 1200, seed: int = 909) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    idx = pd.date_range("2025-01-01 00:00:00", periods=n, freq="h")
    trend = np.linspace(0, 0.03, n)
    cycles = np.sin(np.linspace(0, 40, n)) * 0.003
    close = 1.0800 + trend + cycles + np.cumsum(rng.randn(n) * 0.0009)
    open_ = close + rng.randn(n) * 0.0005
    high = np.maximum(open_, close) + rng.uniform(0.0002, 0.0020, n)
    low = np.minimum(open_, close) - rng.uniform(0.0002, 0.0020, n)
    volume = rng.randint(100, 8000, n).astype(float)
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


class TestPackageKRegistry:
    def test_all_registered(self):
        names = set(StrategyRegistry.list_all())
        for name in PACKAGE_K_STRATEGIES:
            assert name in names

    @pytest.mark.parametrize("name", PACKAGE_K_STRATEGIES)
    def test_category(self, name):
        assert StrategyRegistry.get(name).category == EXPECTED_CATEGORIES[name]


class TestPackageKSignalContract:
    @pytest.mark.parametrize("name", PACKAGE_K_STRATEGIES)
    def test_signal_values_in_range(self, name):
        features = _make_features()
        signals = StrategyRegistry.get(name).generate_signals(features.copy(), TEST_SYMBOL)
        assert isinstance(signals, pd.Series)
        assert len(signals) == len(features)
        assert signals.index.equals(features.index)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    @pytest.mark.parametrize("name", PACKAGE_K_STRATEGIES)
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

    def test_carry_direction_gates_eurusd_short(self):
        # EURUSD default spec carry favors short (swap_long < swap_short) -> no long signals.
        features = _make_features()
        sig = SwapCarryTrendStrategy().generate_signals(features.copy(), TEST_SYMBOL)
        assert (sig == 1).sum() == 0


class TestPackageKParamGridAndStops:
    @pytest.mark.parametrize("name", PACKAGE_K_STRATEGIES)
    def test_sl_tp_standardized(self, name):
        grid = StrategyRegistry.get(name).get_param_grid()
        assert grid["sl_atr_mult"] == _GLOBAL_SL_GRID
        assert grid["tp_atr_mult"] == _GLOBAL_TP_GRID

    @pytest.mark.parametrize("name", PACKAGE_K_STRATEGIES)
    def test_defaults_in_grid(self, name):
        instance = StrategyRegistry.get(name)
        defaults = instance.get_default_params()
        grid = instance.get_param_grid()
        for key, val in defaults.items():
            if key in {"sl_atr_mult", "tp_atr_mult", "structural_sl_lookback", "use_structural_sl"}:
                continue
            if key in grid:
                assert val in grid[key], f"{name}: default {key}={val} missing from grid"

    @pytest.mark.parametrize("name", PACKAGE_K_STRATEGIES)
    def test_calculate_stops_valid(self, name):
        features = _make_features()
        instance = StrategyRegistry.get(name)
        sl, tp = instance.calculate_stops(features, 1, TEST_SYMBOL, bar_index=len(features) - 1)
        assert isinstance(sl, float) and isinstance(tp, float)
        assert sl > 0 and tp > 0

    def test_instantiation_smoke(self):
        features = _make_features()
        for cls in (TimeOfDayMomentumStrategy, SessionCloseReversionStrategy, DayOfWeekBiasStrategy,
                    TurnOfMonthStrategy, OvernightDriftStrategy, IntradayRangeFadeStrategy,
                    SwapCarryTrendStrategy, CarryFilteredPullbackStrategy):
            sig = cls().generate_signals(features.copy(), TEST_SYMBOL)
            assert len(sig) == len(features)
            assert not sig.isna().any()
