"""Tests for Package K time/session/calendar and carry-aware strategies."""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import pm_strategies
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

TIME_INDEX_STRATEGIES = [
    "TimeOfDayMomentumStrategy",
    "SessionCloseReversionStrategy",
    "DayOfWeekBiasStrategy",
    "TurnOfMonthStrategy",
    "OvernightDriftStrategy",
    "IntradayRangeFadeStrategy",
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

    @pytest.mark.parametrize("name", TIME_INDEX_STRATEGIES)
    def test_time_calendar_strategies_flat_without_datetime_index(self, name):
        features = _make_features().reset_index(drop=True)
        sig = StrategyRegistry.get(name).generate_signals(features.copy(), TEST_SYMBOL)
        assert (sig != 0).sum() == 0

    def test_carry_direction_gates_eurusd_short(self):
        # EURUSD default spec carry favors short (swap_long < swap_short) -> no long signals.
        features = _make_features()
        sig = SwapCarryTrendStrategy().generate_signals(features.copy(), TEST_SYMBOL)
        assert (sig == 1).sum() == 0

    def test_carry_direction_requires_positive_swap(self, monkeypatch):
        monkeypatch.setattr(
            pm_strategies,
            "get_instrument_spec",
            lambda _symbol: SimpleNamespace(swap_long=-10.0, swap_short=-1.0),
        )
        features = _make_features()
        assert pm_strategies._carry_direction("BTCUSD", 0.5) == 0
        sig = SwapCarryTrendStrategy(min_swap_diff=0.5).generate_signals(features.copy(), "BTCUSD")
        assert (sig != 0).sum() == 0

    def test_overnight_drift_enters_once_per_entry_hour_on_intraday_data(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=192, freq="15min")
        close = 1.1000 + np.arange(len(idx)) * 0.0001
        features = pd.DataFrame(
            {
                "Open": close - 0.00002,
                "High": close + 0.0001,
                "Low": close - 0.0001,
                "Close": close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 0.0001,
            },
            index=idx,
        )
        sig = OvernightDriftStrategy(entry_hour=21, min_move_atr=0.0).generate_signals(
            features.copy(), TEST_SYMBOL
        )
        entry_hour_signals = sig[sig.index.hour == 21]
        per_day = entry_hour_signals.ne(0).groupby(entry_hour_signals.index.date).sum()
        assert per_day.max() == 1

    def test_intraday_range_fade_does_not_carry_previous_session_range(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=48, freq="h")
        close = np.full(len(idx), 100.0)
        close[24:] = 200.0
        features = pd.DataFrame(
            {
                "Open": close,
                "High": close + 1.0,
                "Low": close - 1.0,
                "Close": close,
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        sig = IntradayRangeFadeStrategy(
            session_start_hour=0,
            quiet_start_hour=0,
            quiet_end_hour=1,
            ext_atr=0.0,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[24] == 0


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

    def test_time_of_day_momentum_grid_has_no_empty_windows(self):
        grid = TimeOfDayMomentumStrategy().get_param_grid()
        for start_hour in grid["start_hour"]:
            for end_hour in grid["end_hour"]:
                assert start_hour < end_hour

    def test_package_k_in_all_exports(self):
        for name in PACKAGE_K_STRATEGIES:
            assert name in pm_strategies.__all__

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
