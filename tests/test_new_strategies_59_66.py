"""Tests for Package I conventional candlestick-pattern strategies."""

import numpy as np
import pandas as pd
import pytest

from pm_strategies import (
    DojiBreakoutStrategy,
    HaramiReversalStrategy,
    MarubozuMomentumStrategy,
    MorningStarEveningStarStrategy,
    PiercingDarkCloudStrategy,
    StrategyCategory,
    StrategyRegistry,
    ThreeBarReversalStrategy,
    ThreeSoldiersCrowsStrategy,
    TweezerReversalStrategy,
    _GLOBAL_SL_GRID,
    _GLOBAL_TP_GRID,
)


TEST_SYMBOL = "EURUSD"

PACKAGE_I_STRATEGIES = [
    "MorningStarEveningStarStrategy",
    "PiercingDarkCloudStrategy",
    "HaramiReversalStrategy",
    "TweezerReversalStrategy",
    "ThreeBarReversalStrategy",
    "DojiBreakoutStrategy",
    "MarubozuMomentumStrategy",
    "ThreeSoldiersCrowsStrategy",
]

EXPECTED_CATEGORIES = {
    "MorningStarEveningStarStrategy": StrategyCategory.MEAN_REVERSION,
    "PiercingDarkCloudStrategy": StrategyCategory.MEAN_REVERSION,
    "HaramiReversalStrategy": StrategyCategory.MEAN_REVERSION,
    "TweezerReversalStrategy": StrategyCategory.MEAN_REVERSION,
    "ThreeBarReversalStrategy": StrategyCategory.MEAN_REVERSION,
    "DojiBreakoutStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "MarubozuMomentumStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
    "ThreeSoldiersCrowsStrategy": StrategyCategory.TREND_FOLLOWING,
}

EXPECTED_STRATEGY_COMBOS = {
    "MorningStarEveningStarStrategy": 486,
    "PiercingDarkCloudStrategy": 162,
    "HaramiReversalStrategy": 162,
    "TweezerReversalStrategy": 162,
    "ThreeBarReversalStrategy": 243,
    "DojiBreakoutStrategy": 486,
    "MarubozuMomentumStrategy": 486,
    "ThreeSoldiersCrowsStrategy": 324,
}

SL_TP_COMBOS = len(_GLOBAL_SL_GRID) * len(_GLOBAL_TP_GRID)


def _make_features(n: int = 900, seed: int = 303) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    idx = pd.date_range("2025-02-03 00:00:00", periods=n, freq="h")
    cycles = np.sin(np.linspace(0, 32, n)) * 0.002
    close = 1.1200 + cycles + np.cumsum(rng.randn(n) * 0.0009)
    open_ = close + rng.randn(n) * 0.00045
    high = np.maximum(open_, close) + rng.uniform(0.0001, 0.0018, n)
    low = np.minimum(open_, close) - rng.uniform(0.0001, 0.0018, n)
    volume = rng.randint(100, 6000, n).astype(float)
    df = pd.DataFrame(
        {
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
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
    df["ATR_10"] = tr.rolling(10).mean()
    df["ATR_14"] = tr.rolling(14).mean()
    df["ATR_20"] = tr.rolling(20).mean()
    df["ADX"] = 22.0
    return df


class TestPackageIRegistry:
    def test_all_package_i_strategies_registered(self):
        names = set(StrategyRegistry.list_all())
        for name in PACKAGE_I_STRATEGIES:
            assert name in names

    @pytest.mark.parametrize("name", PACKAGE_I_STRATEGIES)
    def test_category_assignment(self, name):
        assert StrategyRegistry.get(name).category == EXPECTED_CATEGORIES[name]


class TestPackageISignalContract:
    @pytest.mark.parametrize("name", PACKAGE_I_STRATEGIES)
    def test_signal_values_in_range(self, name):
        features = _make_features()
        instance = StrategyRegistry.get(name)
        signals = instance.generate_signals(features.copy(), TEST_SYMBOL)

        assert isinstance(signals, pd.Series)
        assert len(signals) == len(features)
        assert signals.index.equals(features.index)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    @pytest.mark.parametrize("name", PACKAGE_I_STRATEGIES)
    def test_no_lookahead_truncation(self, name):
        features = _make_features(n=1000, seed=404)
        instance = StrategyRegistry.get(name)
        signals_full = instance.generate_signals(features.copy(), TEST_SYMBOL)

        cutoff = 650
        signals_trunc = instance.generate_signals(features.iloc[:cutoff].copy(), TEST_SYMBOL)
        np.testing.assert_array_equal(
            signals_full.iloc[:cutoff].values,
            signals_trunc.values,
            err_msg=f"{name} has lookahead-sensitive signals",
        )


class TestPackageIParamGrid:
    @pytest.mark.parametrize("name", PACKAGE_I_STRATEGIES)
    def test_sl_tp_standardized(self, name):
        grid = StrategyRegistry.get(name).get_param_grid()
        assert grid["sl_atr_mult"] == _GLOBAL_SL_GRID
        assert grid["tp_atr_mult"] == _GLOBAL_TP_GRID

    @pytest.mark.parametrize("name", PACKAGE_I_STRATEGIES)
    def test_strategy_combo_count(self, name):
        grid = StrategyRegistry.get(name).get_param_grid()
        combos = 1
        for key, values in grid.items():
            if key in {"sl_atr_mult", "tp_atr_mult"}:
                continue
            combos *= len(values)
        assert combos == EXPECTED_STRATEGY_COMBOS[name]
        assert combos * SL_TP_COMBOS > 0

    @pytest.mark.parametrize("name", PACKAGE_I_STRATEGIES)
    def test_default_params_in_grid(self, name):
        instance = StrategyRegistry.get(name)
        defaults = instance.get_default_params()
        grid = instance.get_param_grid()
        for key, default_val in defaults.items():
            if key in {"sl_atr_mult", "tp_atr_mult", "structural_sl_lookback"}:
                continue
            if key in grid:
                assert default_val in grid[key], f"{name}: default {key} missing from grid"


class TestPackageIStopsAndInstantiation:
    @pytest.mark.parametrize("name", PACKAGE_I_STRATEGIES)
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
            MorningStarEveningStarStrategy(),
            PiercingDarkCloudStrategy(),
            HaramiReversalStrategy(),
            TweezerReversalStrategy(),
            ThreeBarReversalStrategy(),
            DojiBreakoutStrategy(),
            MarubozuMomentumStrategy(),
            ThreeSoldiersCrowsStrategy(),
        ]
        for instance in instances:
            signals = instance.generate_signals(features.copy(), TEST_SYMBOL)
            assert len(signals) == len(features)
            assert not signals.isna().any()
