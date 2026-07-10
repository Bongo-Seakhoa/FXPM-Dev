"""Tests for Package H strategy expansion."""

import numpy as np
import pandas as pd
import pytest

from pm_strategies import (
    BollingerBandWalkStrategy,
    DoubleEMATrendStrategy,
    GapContinuationStrategy,
    HeikinAshiTrendStrategy,
    OpeningRangeBreakoutStrategy,
    PercentileChannelReversionStrategy,
    RSI2TrendPullbackStrategy,
    StrategyCategory,
    StrategyRegistry,
    VWAPTrendContinuationStrategy,
    _GLOBAL_SL_GRID,
    _GLOBAL_TP_GRID,
)


TEST_SYMBOL = "EURUSD"

PACKAGE_H_STRATEGIES = [
    "OpeningRangeBreakoutStrategy",
    "GapContinuationStrategy",
    "VWAPTrendContinuationStrategy",
    "RSI2TrendPullbackStrategy",
    "DoubleEMATrendStrategy",
    "HeikinAshiTrendStrategy",
    "BollingerBandWalkStrategy",
    "PercentileChannelReversionStrategy",
]

EXPECTED_CATEGORIES = {
    "OpeningRangeBreakoutStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "GapContinuationStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "VWAPTrendContinuationStrategy": StrategyCategory.TREND_FOLLOWING,
    "RSI2TrendPullbackStrategy": StrategyCategory.MEAN_REVERSION,
    "DoubleEMATrendStrategy": StrategyCategory.TREND_FOLLOWING,
    "HeikinAshiTrendStrategy": StrategyCategory.TREND_FOLLOWING,
    "BollingerBandWalkStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "PercentileChannelReversionStrategy": StrategyCategory.MEAN_REVERSION,
}

EXPECTED_STRATEGY_COMBOS = {
    "OpeningRangeBreakoutStrategy": 324,
    "GapContinuationStrategy": 216,
    "VWAPTrendContinuationStrategy": 324,
    "RSI2TrendPullbackStrategy": 288,
    "DoubleEMATrendStrategy": 216,
    "HeikinAshiTrendStrategy": 72,
    "BollingerBandWalkStrategy": 324,
    "PercentileChannelReversionStrategy": 162,
}

SL_TP_COMBOS = len(_GLOBAL_SL_GRID) * len(_GLOBAL_TP_GRID)


def _make_features(n: int = 720, seed: int = 101) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    idx = pd.date_range("2025-01-06 00:00:00", periods=n, freq="h")
    drift = np.sin(np.linspace(0, 18, n)) * 0.0015
    trend = np.linspace(0, 0.025, n)
    close = 1.1000 + trend + drift + np.cumsum(rng.randn(n) * 0.0008)
    open_ = close + rng.randn(n) * 0.0003
    high = np.maximum(open_, close) + rng.uniform(0.0002, 0.0015, n)
    low = np.minimum(open_, close) - rng.uniform(0.0002, 0.0015, n)
    volume = rng.randint(100, 5000, n).astype(float)

    df = pd.DataFrame(
        {
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
            "RealVolume": volume,
        },
        index=idx,
    )
    tr = np.maximum(
        df["High"] - df["Low"],
        np.maximum(
            (df["High"] - df["Close"].shift(1)).abs(),
            (df["Low"] - df["Close"].shift(1)).abs(),
        ),
    )
    df["ATR_14"] = tr.rolling(14).mean()
    df["ADX"] = 25.0
    return df


class TestPackageHRegistry:
    def test_all_package_h_strategies_registered(self):
        names = set(StrategyRegistry.list_all())
        for name in PACKAGE_H_STRATEGIES:
            assert name in names

    @pytest.mark.parametrize("name", PACKAGE_H_STRATEGIES)
    def test_category_assignment(self, name):
        assert StrategyRegistry.get(name).category == EXPECTED_CATEGORIES[name]


class TestPackageHSignalContract:
    @pytest.mark.parametrize("name", PACKAGE_H_STRATEGIES)
    def test_signal_values_in_range(self, name):
        features = _make_features()
        instance = StrategyRegistry.get(name)
        signals = instance.generate_signals(features.copy(), TEST_SYMBOL)

        assert isinstance(signals, pd.Series)
        assert len(signals) == len(features)
        assert signals.index.equals(features.index)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    @pytest.mark.parametrize("name", PACKAGE_H_STRATEGIES)
    def test_no_lookahead_truncation(self, name):
        features = _make_features(n=900, seed=17)
        instance = StrategyRegistry.get(name)
        signals_full = instance.generate_signals(features.copy(), TEST_SYMBOL)

        cutoff = 600
        signals_trunc = instance.generate_signals(features.iloc[:cutoff].copy(), TEST_SYMBOL)
        np.testing.assert_array_equal(
            signals_full.iloc[:cutoff].values,
            signals_trunc.values,
            err_msg=f"{name} has lookahead-sensitive signals",
        )


class TestPackageHParamGrid:
    @pytest.mark.parametrize("name", PACKAGE_H_STRATEGIES)
    def test_sl_tp_standardized(self, name):
        grid = StrategyRegistry.get(name).get_param_grid()
        assert grid["sl_atr_mult"] == _GLOBAL_SL_GRID
        assert grid["tp_atr_mult"] == _GLOBAL_TP_GRID

    @pytest.mark.parametrize("name", PACKAGE_H_STRATEGIES)
    def test_strategy_combo_count(self, name):
        grid = StrategyRegistry.get(name).get_param_grid()
        combos = 1
        for key, values in grid.items():
            if key in {"sl_atr_mult", "tp_atr_mult"}:
                continue
            combos *= len(values)
        assert combos == EXPECTED_STRATEGY_COMBOS[name]
        assert combos * SL_TP_COMBOS > 0

    @pytest.mark.parametrize("name", PACKAGE_H_STRATEGIES)
    def test_default_params_in_grid(self, name):
        instance = StrategyRegistry.get(name)
        defaults = instance.get_default_params()
        grid = instance.get_param_grid()
        for key, default_val in defaults.items():
            if key in {"sl_atr_mult", "tp_atr_mult", "structural_sl_lookback"}:
                continue
            if key in grid:
                assert default_val in grid[key], f"{name}: default {key} missing from grid"


class TestPackageHStopsAndInstantiation:
    @pytest.mark.parametrize("name", PACKAGE_H_STRATEGIES)
    def test_calculate_stops_returns_valid_when_strategy_signals(self, name):
        features = _make_features()
        instance = StrategyRegistry.get(name)
        signals = instance.generate_signals(features.copy(), TEST_SYMBOL)
        nonzero = signals[signals != 0]
        if len(nonzero) == 0:
            pytest.skip(f"{name} produced no signals on synthetic data")

        idx = nonzero.index[0]
        direction = int(nonzero.iloc[0])
        bar_idx = features.index.get_loc(idx)
        sl, tp = instance.calculate_stops(features, direction, TEST_SYMBOL, bar_index=bar_idx)

        assert isinstance(sl, float)
        assert isinstance(tp, float)
        assert sl > 0
        assert tp > 0

    def test_direct_instantiation_smoke(self):
        features = _make_features()
        instances = [
            OpeningRangeBreakoutStrategy(),
            GapContinuationStrategy(),
            VWAPTrendContinuationStrategy(),
            RSI2TrendPullbackStrategy(),
            DoubleEMATrendStrategy(),
            HeikinAshiTrendStrategy(),
            BollingerBandWalkStrategy(),
            PercentileChannelReversionStrategy(),
        ]
        for instance in instances:
            signals = instance.generate_signals(features.copy(), TEST_SYMBOL)
            assert len(signals) == len(features)
            assert not signals.isna().any()
