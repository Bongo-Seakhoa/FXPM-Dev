"""Tests for the current strategy-registry expansion beyond the original 42 set.

This repository currently ships a 47-strategy roster with these additional
strategies validated here:

- VortexTrendStrategy
- TRIXSignalStrategy
- RelativeVigorIndexStrategy
- VIDYABandTrendStrategy
- ChoppinessCompressionBreakoutStrategy
"""

import numpy as np
import pandas as pd
import pytest

from pm_strategies import (
    StrategyCategory,
    StrategyRegistry,
    VortexTrendStrategy,
    TRIXSignalStrategy,
    RelativeVigorIndexStrategy,
    VIDYABandTrendStrategy,
    ChoppinessCompressionBreakoutStrategy,
    _GLOBAL_SL_GRID,
    _GLOBAL_TP_GRID,
)

TEST_SYMBOL = "EURUSD"

NEW_STRATEGY_NAMES = [
    "VortexTrendStrategy",
    "TRIXSignalStrategy",
    "RelativeVigorIndexStrategy",
    "VIDYABandTrendStrategy",
    "ChoppinessCompressionBreakoutStrategy",
]

EXPECTED_CATEGORIES = {
    "VortexTrendStrategy": StrategyCategory.TREND_FOLLOWING,
    "TRIXSignalStrategy": StrategyCategory.TREND_FOLLOWING,
    "RelativeVigorIndexStrategy": StrategyCategory.TREND_FOLLOWING,
    "VIDYABandTrendStrategy": StrategyCategory.TREND_FOLLOWING,
    "ChoppinessCompressionBreakoutStrategy": StrategyCategory.BREAKOUT_MOMENTUM,
}

EXPECTED_STRATEGY_COMBOS = {
    # Counts include D5 `use_structural_sl: [True, False]` (×2) on
    # trend/breakout strategies — opt-in searched per (symbol, TF, regime).
    "VortexTrendStrategy": 10,
    "TRIXSignalStrategy": 24,
    "RelativeVigorIndexStrategy": 10,
    "VIDYABandTrendStrategy": 108,
    "ChoppinessCompressionBreakoutStrategy": 144,
}

SL_TP_COMBOS = len(_GLOBAL_SL_GRID) * len(_GLOBAL_TP_GRID)


def _make_features(n: int = 500, seed: int = 42) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2023-01-01", periods=n, freq="4h")

    close = 1.1000 + np.cumsum(rng.randn(n) * 0.001)
    high = close + rng.uniform(0.0005, 0.003, n)
    low = close - rng.uniform(0.0005, 0.003, n)
    open_ = close + rng.randn(n) * 0.001
    volume = rng.randint(100, 10000, n).astype(float)

    df = pd.DataFrame(
        {
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
        },
        index=dates,
    )

    tr = np.maximum(
        df["High"] - df["Low"],
        np.maximum(
            (df["High"] - df["Close"].shift(1)).abs(),
            (df["Low"] - df["Close"].shift(1)).abs(),
        ),
    )
    df["ATR_14"] = tr.rolling(14).mean()
    return df


class TestRegistryExpansion:
    def test_total_count_is_47(self):
        assert StrategyRegistry.count() == 47

    def test_all_current_expansion_strategies_registered(self):
        names = set(StrategyRegistry.list_all())
        for name in NEW_STRATEGY_NAMES:
            assert name in names, f"{name} missing from registry"

    def test_category_counts(self):
        trend = StrategyRegistry.list_by_category(StrategyCategory.TREND_FOLLOWING)
        mr = StrategyRegistry.list_by_category(StrategyCategory.MEAN_REVERSION)
        breakout = StrategyRegistry.list_by_category(StrategyCategory.BREAKOUT_MOMENTUM)
        assert len(trend) == 18
        assert len(mr) == 17
        assert len(breakout) == 12

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_category_assignment(self, name):
        instance = StrategyRegistry.get(name)
        assert instance.category == EXPECTED_CATEGORIES[name]


class TestSignalContract:
    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_signal_values_in_range(self, name):
        features = _make_features()
        instance = StrategyRegistry.get(name)
        signals = instance.generate_signals(features.copy(), TEST_SYMBOL)

        assert isinstance(signals, pd.Series)
        assert len(signals) == len(features)
        assert signals.index.equals(features.index)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_no_lookahead_truncation(self, name):
        features = _make_features(n=600, seed=99)
        instance = StrategyRegistry.get(name)
        signals_full = instance.generate_signals(features.copy(), TEST_SYMBOL)

        cutoff = 400
        signals_trunc = instance.generate_signals(features.iloc[:cutoff].copy(), TEST_SYMBOL)
        np.testing.assert_array_equal(
            signals_full.iloc[:cutoff].values,
            signals_trunc.values,
            err_msg=f"{name} has lookahead-sensitive signals",
        )


class TestParamGrid:
    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_sl_tp_standardized(self, name):
        instance = StrategyRegistry.get(name)
        grid = instance.get_param_grid()
        assert grid["sl_atr_mult"] == _GLOBAL_SL_GRID
        assert grid["tp_atr_mult"] == _GLOBAL_TP_GRID

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_strategy_combo_count(self, name):
        instance = StrategyRegistry.get(name)
        grid = instance.get_param_grid()
        combos = 1
        for key, values in grid.items():
            if key in {"sl_atr_mult", "tp_atr_mult"}:
                continue
            combos *= len(values)
        assert combos == EXPECTED_STRATEGY_COMBOS[name]

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_total_combo_count(self, name):
        assert EXPECTED_STRATEGY_COMBOS[name] * SL_TP_COMBOS > 0

    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_default_params_in_grid(self, name):
        instance = StrategyRegistry.get(name)
        defaults = instance.get_default_params()
        grid = instance.get_param_grid()
        for key, default_val in defaults.items():
            if key in {"sl_atr_mult", "tp_atr_mult"}:
                continue
            if key in grid:
                assert default_val in grid[key], f"{name}: default {key} missing from grid"


class TestStopCalculation:
    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_calculate_stops_returns_valid(self, name):
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


class TestSmokeWithNonDefaultParams:
    @pytest.mark.parametrize("name", NEW_STRATEGY_NAMES)
    def test_non_default_params_smoke(self, name):
        features = _make_features(n=600, seed=7)
        instance = StrategyRegistry.get(name)
        grid = instance.get_param_grid()
        params = {}
        for key, values in grid.items():
            if key in {"sl_atr_mult", "tp_atr_mult"}:
                continue
            params[key] = values[-1]
        custom = StrategyRegistry.get(name, **params)
        signals = custom.generate_signals(features.copy(), TEST_SYMBOL)
        assert not signals.isna().any()
        assert set(signals.unique()).issubset({-1, 0, 1})


def test_direct_instantiation_smoke():
    features = _make_features()
    instances = [
        VortexTrendStrategy(),
        TRIXSignalStrategy(),
        RelativeVigorIndexStrategy(),
        VIDYABandTrendStrategy(),
        ChoppinessCompressionBreakoutStrategy(),
    ]
    for instance in instances:
        signals = instance.generate_signals(features.copy(), TEST_SYMBOL)
        assert len(signals) == len(features)
        assert not signals.isna().any()
