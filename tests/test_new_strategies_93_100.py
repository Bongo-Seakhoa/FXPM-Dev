"""Tests for Package M level, ADR, and deterministic statistical strategies."""

import warnings

import numpy as np
import pandas as pd
import pytest

import pm_strategies
from pm_strategies import (
    ADRExpansionBreakoutStrategy,
    AutocorrelationReversalStrategy,
    DarvasBoxBreakoutStrategy,
    HeadAndShouldersNecklineBreakStrategy,
    PreviousWeekRangeBreakoutStrategy,
    PriorDayRangeReclaimStrategy,
    StrategyCategory,
    StrategyRegistry,
    VarianceRatioTrendStrategy,
    WeeklyMonthlyLevelRejectionStrategy,
    _GLOBAL_SL_GRID,
    _GLOBAL_TP_GRID,
)

TEST_SYMBOL = "EURUSD"

PACKAGE_M_STRATEGIES = [
    "DarvasBoxBreakoutStrategy",
    "ADRExpansionBreakoutStrategy",
    "PriorDayRangeReclaimStrategy",
    "WeeklyMonthlyLevelRejectionStrategy",
    "PreviousWeekRangeBreakoutStrategy",
    "HeadAndShouldersNecklineBreakStrategy",
    "VarianceRatioTrendStrategy",
    "AutocorrelationReversalStrategy",
]

DATETIME_REQUIRED_STRATEGIES = [
    "ADRExpansionBreakoutStrategy",
    "PriorDayRangeReclaimStrategy",
    "WeeklyMonthlyLevelRejectionStrategy",
    "PreviousWeekRangeBreakoutStrategy",
]

EXPECTED_CATEGORIES = {
    "DarvasBoxBreakoutStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "ADRExpansionBreakoutStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "PriorDayRangeReclaimStrategy": StrategyCategory.MEAN_REVERSION,
    "WeeklyMonthlyLevelRejectionStrategy": StrategyCategory.MEAN_REVERSION,
    "PreviousWeekRangeBreakoutStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "HeadAndShouldersNecklineBreakStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "VarianceRatioTrendStrategy": StrategyCategory.TREND_FOLLOWING,
    "AutocorrelationReversalStrategy": StrategyCategory.MEAN_REVERSION,
}


def _make_features(n: int = 1200, seed: int = 1101, freq: str = "h") -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    idx = pd.date_range("2025-01-01 00:00:00", periods=n, freq=freq)
    trend = np.linspace(0, 0.04, n)
    cycles = np.sin(np.linspace(0, 50, n)) * 0.003
    close = 1.0900 + trend + cycles + np.cumsum(rng.randn(n) * 0.0008)
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


def test_timezone_aware_period_helpers_do_not_warn():
    idx = pd.date_range("2025-01-01 00:00:00", periods=48, freq="h", tz="UTC")
    close = np.full(len(idx), 100.0)
    features = pd.DataFrame(
        {"Open": close, "High": close + 1.0, "Low": close - 1.0, "Close": close,
         "Volume": 1000.0, "RealVolume": 1000.0, "ATR_14": 1.0},
        index=idx,
    )
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        pm_strategies._previous_period_levels(features, "W")
        pm_strategies._previous_day_context(features, 5)
    assert not record


class TestPackageMRegistry:
    def test_all_registered(self):
        names = set(StrategyRegistry.list_all())
        for name in PACKAGE_M_STRATEGIES:
            assert name in names

    @pytest.mark.parametrize("name", PACKAGE_M_STRATEGIES)
    def test_category(self, name):
        assert StrategyRegistry.get(name).category == EXPECTED_CATEGORIES[name]


class TestPackageMSignalContract:
    @pytest.mark.parametrize("name", PACKAGE_M_STRATEGIES)
    def test_signal_values_in_range(self, name):
        features = _make_features()
        signals = StrategyRegistry.get(name).generate_signals(features.copy(), TEST_SYMBOL)
        assert isinstance(signals, pd.Series)
        assert len(signals) == len(features)
        assert signals.index.equals(features.index)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    @pytest.mark.parametrize("name", PACKAGE_M_STRATEGIES)
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

    @pytest.mark.parametrize("name", DATETIME_REQUIRED_STRATEGIES)
    def test_level_strategies_flat_without_datetime_index(self, name):
        features = _make_features().reset_index(drop=True)
        sig = StrategyRegistry.get(name).generate_signals(features.copy(), TEST_SYMBOL)
        assert (sig != 0).sum() == 0

    def test_darvas_box_breakout_requires_compact_prior_box(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=40, freq="h")
        close = np.full(len(idx), 100.0)
        high = close + 0.4
        low = close - 0.4
        close[25] = 102.0
        high[25] = 102.2
        features = pd.DataFrame(
            {"Open": close, "High": high, "Low": low, "Close": close,
             "Volume": 1000.0, "RealVolume": 1000.0, "ATR_14": 1.0},
            index=idx,
        )
        sig = DarvasBoxBreakoutStrategy(
            lookback=10,
            max_box_atr=2.0,
            breakout_atr=0.0,
            volume_mult=0.0,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[25] == 1

    def test_prior_day_reclaim_uses_previous_day_levels(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=72, freq="h")
        close = np.full(len(idx), 100.0)
        high = close + 0.5
        low = close - 0.5
        low[50] = 98.0
        close[50] = 99.8
        features = pd.DataFrame(
            {"Open": close, "High": high, "Low": low, "Close": close,
             "Volume": 1000.0, "RealVolume": 1000.0, "ATR_14": 1.0},
            index=idx,
        )
        sig = PriorDayRangeReclaimStrategy(sweep_atr=0.0).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[50] == 1

    def test_prior_day_reclaim_emits_once_per_day_per_side(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=72, freq="h")
        close = np.full(len(idx), 100.0)
        high = close + 0.5
        low = close - 0.5
        for i in (50, 51):
            low[i] = 98.0
            close[i] = 99.8
        features = pd.DataFrame(
            {"Open": close, "High": high, "Low": low, "Close": close,
             "Volume": 1000.0, "RealVolume": 1000.0, "ATR_14": 1.0},
            index=idx,
        )
        sig = PriorDayRangeReclaimStrategy(sweep_atr=0.0).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[50] == 1
        assert sig.iloc[51] == 0

    def test_weekly_level_rejection_fades_prior_week_sweep(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=35, freq="D")
        close = np.full(len(idx), 100.0)
        high = close + 0.5
        low = close - 0.5
        high[22] = 102.0
        close[22] = 100.2
        features = pd.DataFrame(
            {"Open": close, "High": high, "Low": low, "Close": close,
             "Volume": 1000.0, "RealVolume": 1000.0, "ATR_14": 1.0},
            index=idx,
        )
        sig = WeeklyMonthlyLevelRejectionStrategy(
            period_type="W",
            sweep_atr=0.0,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[22] == -1

    def test_weekly_level_rejection_emits_once_per_period_per_side(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=35, freq="D")
        close = np.full(len(idx), 100.0)
        high = close + 0.5
        low = close - 0.5
        for i in (22, 23):
            high[i] = 102.0
            close[i] = 100.2
        features = pd.DataFrame(
            {"Open": close, "High": high, "Low": low, "Close": close,
             "Volume": 1000.0, "RealVolume": 1000.0, "ATR_14": 1.0},
            index=idx,
        )
        sig = WeeklyMonthlyLevelRejectionStrategy(
            period_type="W",
            sweep_atr=0.0,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[22] == -1
        assert sig.iloc[23] == 0

    def test_weekly_levels_keep_sunday_with_next_broker_week(self):
        idx = pd.to_datetime(
            [
                "2025-01-05 21:00:00",
                "2025-01-06 00:00:00",
                "2025-01-12 21:00:00",
                "2025-01-13 00:00:00",
            ],
            utc=True,
        )
        features = pd.DataFrame(
            {
                "Open": [100.0, 100.0, 120.0, 100.0],
                "High": [101.0, 102.0, 150.0, 100.5],
                "Low": [99.0, 98.0, 119.0, 99.5],
                "Close": [100.0, 100.0, 120.0, 100.0],
                "Volume": 1000.0,
                "RealVolume": 1000.0,
                "ATR_14": 1.0,
            },
            index=idx,
        )
        prev_high, _prev_low = pm_strategies._previous_period_levels(features, "W")
        assert prev_high.iloc[3] == 102.0

    def test_head_and_shoulders_breaks_after_confirmed_right_shoulder(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=80, freq="h")
        close = np.full(len(idx), 100.0)
        high = close + 0.5
        low = close - 0.5
        high[30] = 105.0
        high[40] = 110.0
        high[50] = 105.2
        low[35] = 98.0
        low[45] = 98.2
        close[55] = 97.0
        low[55] = 96.5
        features = pd.DataFrame(
            {"Open": close, "High": high, "Low": low, "Close": close,
             "Volume": 1000.0, "RealVolume": 1000.0, "ATR_14": 1.0},
            index=idx,
        )
        sig = HeadAndShouldersNecklineBreakStrategy(
            wing_bars=1,
            shoulder_tolerance_atr=1.0,
            neckline_buffer_atr=0.0,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[50] == 0
        assert sig.iloc[55] == -1

    def test_statistical_regime_strategies_emit_transitions_not_every_bar(self):
        idx = pd.date_range("2025-01-01 00:00:00", periods=120, freq="h")
        close = np.r_[np.full(50, 100.0), np.linspace(100.0, 130.0, 70)]
        features = pd.DataFrame(
            {"Open": close, "High": close + 0.5, "Low": close - 0.5, "Close": close,
             "Volume": 1000.0, "RealVolume": 1000.0, "ATR_14": 1.0},
            index=idx,
        )
        sig = VarianceRatioTrendStrategy(
            vr_period=10,
            lag=2,
            min_vr=0.5,
            ema_period=3,
        ).generate_signals(features.copy(), TEST_SYMBOL)
        assert sig.iloc[60:80].eq(1).sum() <= 1

        shock = np.full(120, 100.0)
        shock[70:73] = 96.0
        ac_features = pd.DataFrame(
            {"Open": shock, "High": shock + 0.5, "Low": shock - 0.5, "Close": shock,
             "Volume": 1000.0, "RealVolume": 1000.0, "ATR_14": 1.0},
            index=idx,
        )
        ac_sig = AutocorrelationReversalStrategy(
            corr_period=10,
            lag=1,
            max_autocorr=1.0,
            z_period=10,
            z_entry=1.0,
        ).generate_signals(ac_features.copy(), TEST_SYMBOL)
        assert ac_sig.iloc[70:73].eq(1).sum() <= 1


class TestPackageMParamGridAndStops:
    @pytest.mark.parametrize("name", PACKAGE_M_STRATEGIES)
    def test_sl_tp_standardized(self, name):
        grid = StrategyRegistry.get(name).get_param_grid()
        assert grid["sl_atr_mult"] == _GLOBAL_SL_GRID
        assert grid["tp_atr_mult"] == _GLOBAL_TP_GRID

    @pytest.mark.parametrize("name", PACKAGE_M_STRATEGIES)
    def test_defaults_in_grid(self, name):
        instance = StrategyRegistry.get(name)
        defaults = instance.get_default_params()
        grid = instance.get_param_grid()
        for key, val in defaults.items():
            if key in {"sl_atr_mult", "tp_atr_mult", "structural_sl_lookback", "use_structural_sl"}:
                continue
            if key in grid:
                assert val in grid[key], f"{name}: default {key}={val} missing from grid"

    @pytest.mark.parametrize("name", PACKAGE_M_STRATEGIES)
    def test_calculate_stops_valid(self, name):
        features = _make_features()
        instance = StrategyRegistry.get(name)
        sl, tp = instance.calculate_stops(features, 1, TEST_SYMBOL, bar_index=len(features) - 1)
        assert isinstance(sl, float) and isinstance(tp, float)
        assert sl > 0 and tp > 0

    def test_package_m_in_all_exports(self):
        for name in PACKAGE_M_STRATEGIES:
            assert name in pm_strategies.__all__
