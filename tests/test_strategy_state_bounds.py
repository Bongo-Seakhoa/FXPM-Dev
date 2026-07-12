"""Regression tests for bounded discrete state in structure strategies."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pm_strategies import (
    FractalChannelBreakoutStrategy,
    HeadAndShouldersNecklineBreakStrategy,
    InsideBarBreakoutStrategy,
    VolumeClimaxReversalStrategy,
)


TEST_SYMBOL = "EURUSD"


def _flat_features(n: int) -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=n, freq="h", tz="UTC")
    close = np.full(n, 100.0)
    return pd.DataFrame(
        {
            "Open": close.copy(),
            "High": close + 0.5,
            "Low": close - 0.5,
            "Close": close,
            "Volume": 1_000.0,
            "RealVolume": 1_000.0,
            "ATR_14": 1.0,
        },
        index=index,
    )


def _inside_bar_features(breakout_bar: int, n: int = 80) -> pd.DataFrame:
    features = _flat_features(n)
    features.iloc[20, features.columns.get_loc("High")] = 105.0
    features.iloc[20, features.columns.get_loc("Low")] = 95.0
    features.iloc[21, features.columns.get_loc("High")] = 104.0
    features.iloc[21, features.columns.get_loc("Low")] = 96.0
    features.iloc[breakout_bar, features.columns.get_loc("Close")] = 106.0
    features.iloc[breakout_bar, features.columns.get_loc("High")] = 106.5
    return features


def _fractal_features(
    *,
    pivot_bar: int,
    breakout_bar: int,
    n: int,
) -> pd.DataFrame:
    features = _flat_features(n)
    features.iloc[pivot_bar, features.columns.get_loc("High")] = 105.0
    features.iloc[breakout_bar, features.columns.get_loc("Close")] = 106.0
    features.iloc[breakout_bar, features.columns.get_loc("High")] = 106.5
    return features


def _head_shoulders_features(
    *,
    direction: str,
    centers: tuple[int, int, int],
    breakout_bar: int,
    n: int = 140,
) -> pd.DataFrame:
    features = _flat_features(n)
    a, b, c = centers
    if direction == "short":
        for bar, value in zip(centers, (105.0, 110.0, 105.2), strict=True):
            features.iloc[bar, features.columns.get_loc("High")] = value
        features.iloc[(a + b) // 2, features.columns.get_loc("Low")] = 98.0
        features.iloc[(b + c) // 2, features.columns.get_loc("Low")] = 98.2
        features.iloc[breakout_bar, features.columns.get_loc("Close")] = 97.0
        features.iloc[breakout_bar, features.columns.get_loc("Low")] = 96.5
    elif direction == "long":
        for bar, value in zip(centers, (95.0, 90.0, 95.2), strict=True):
            features.iloc[bar, features.columns.get_loc("Low")] = value
        features.iloc[(a + b) // 2, features.columns.get_loc("High")] = 102.0
        features.iloc[(b + c) // 2, features.columns.get_loc("High")] = 101.8
        features.iloc[breakout_bar, features.columns.get_loc("Close")] = 103.0
        features.iloc[breakout_bar, features.columns.get_loc("High")] = 103.5
    else:  # pragma: no cover - test helper guard
        raise ValueError(direction)
    return features


def _head_shoulders(direction: str, max_age: int) -> HeadAndShouldersNecklineBreakStrategy:
    return HeadAndShouldersNecklineBreakStrategy(
        wing_bars=1,
        shoulder_tolerance_atr=1.0,
        neckline_buffer_atr=0.0,
        pattern_max_age_bars=max_age,
    )


def test_inside_bar_setup_expires_before_stale_breakout() -> None:
    features = _inside_bar_features(breakout_bar=30)

    bounded = InsideBarBreakoutStrategy(
        min_inside_bars=1,
        setup_max_age_bars=3,
    ).generate_signals(features.copy(), TEST_SYMBOL)
    recent = InsideBarBreakoutStrategy(
        min_inside_bars=1,
        setup_max_age_bars=20,
    ).generate_signals(features.copy(), TEST_SYMBOL)

    assert bounded.iloc[30] == 0
    assert recent.iloc[30] == 1


def test_inside_bar_setup_age_is_inclusive_and_deterministic() -> None:
    at_limit = _inside_bar_features(breakout_bar=24)
    after_limit = _inside_bar_features(breakout_bar=25)
    strategy = InsideBarBreakoutStrategy(min_inside_bars=1, setup_max_age_bars=3)

    assert strategy.generate_signals(at_limit, TEST_SYMBOL).iloc[24] == 1
    assert strategy.generate_signals(after_limit, TEST_SYMBOL).iloc[25] == 0


def test_fractal_channel_closes_494_bar_stale_pivot_counterexample() -> None:
    # The pivot at bar 20 confirms at bar 22 for wing=1. The breakout at
    # bar 516 is therefore 494 bars after confirmation.
    features = _fractal_features(pivot_bar=20, breakout_bar=516, n=520)
    bounded = FractalChannelBreakoutStrategy(
        wing_bars=1,
        atr_buffer=0.0,
    ).generate_signals(features.copy(), TEST_SYMBOL)
    reproduced = FractalChannelBreakoutStrategy(
        wing_bars=1,
        atr_buffer=0.0,
        pivot_max_age_bars=600,
    ).generate_signals(features.copy(), TEST_SYMBOL)

    assert reproduced.iloc[516] == 1
    assert bounded.iloc[516] == 0


def test_fractal_channel_recent_pivot_still_triggers() -> None:
    features = _fractal_features(pivot_bar=60, breakout_bar=70, n=90)
    signals = FractalChannelBreakoutStrategy(
        wing_bars=1,
        atr_buffer=0.0,
        pivot_max_age_bars=10,
    ).generate_signals(features, TEST_SYMBOL)

    assert signals.iloc[70] == 1


@pytest.mark.parametrize("direction, expected", [("short", -1), ("long", 1)])
def test_head_shoulders_expires_ancient_constituent_pivots(
    direction: str,
    expected: int,
) -> None:
    features = _head_shoulders_features(
        direction=direction,
        centers=(10, 20, 60),
        breakout_bar=65,
    )
    bounded = _head_shoulders(direction, max_age=30).generate_signals(
        features.copy(), TEST_SYMBOL
    )
    reproduced = _head_shoulders(direction, max_age=100).generate_signals(
        features.copy(), TEST_SYMBOL
    )

    assert reproduced.iloc[65] == expected
    assert bounded.iloc[65] == 0


@pytest.mark.parametrize("direction, expected", [("short", -1), ("long", 1)])
def test_head_shoulders_expires_formed_pattern_from_oldest_pivot(
    direction: str,
    expected: int,
) -> None:
    stale = _head_shoulders_features(
        direction=direction,
        centers=(10, 20, 30),
        breakout_bar=43,
    )
    recent = _head_shoulders_features(
        direction=direction,
        centers=(10, 20, 30),
        breakout_bar=40,
    )
    strategy = _head_shoulders(direction, max_age=30)

    assert strategy.generate_signals(stale, TEST_SYMBOL).iloc[43] == 0
    assert strategy.generate_signals(recent, TEST_SYMBOL).iloc[40] == expected


def test_inside_bar_full_and_sufficient_prefix_outputs_match() -> None:
    features = _inside_bar_features(breakout_bar=30)
    strategy = InsideBarBreakoutStrategy(min_inside_bars=1, setup_max_age_bars=10)
    full = strategy.generate_signals(features.copy(), TEST_SYMBOL)
    prefixed = strategy.generate_signals(features.iloc[16:].copy(), TEST_SYMBOL)

    pd.testing.assert_series_equal(full.iloc[30:], prefixed.loc[features.index[30:]])
    assert full.iloc[30] == 1


def test_fractal_full_and_sufficient_prefix_outputs_match() -> None:
    features = _fractal_features(pivot_bar=60, breakout_bar=70, n=100)
    strategy = FractalChannelBreakoutStrategy(
        wing_bars=1,
        atr_buffer=0.0,
        pivot_max_age_bars=10,
    )
    full = strategy.generate_signals(features.copy(), TEST_SYMBOL)
    # 32 bars covers max age, pivot locality, and the strategy warm-up.
    prefixed = strategy.generate_signals(features.iloc[38:].copy(), TEST_SYMBOL)

    pd.testing.assert_series_equal(full.iloc[70:], prefixed.loc[features.index[70:]])
    assert full.iloc[70] == 1


def test_head_shoulders_full_and_sufficient_prefix_outputs_match() -> None:
    features = _head_shoulders_features(
        direction="short",
        centers=(50, 60, 70),
        breakout_bar=80,
    )
    strategy = _head_shoulders("short", max_age=30)
    full = strategy.generate_signals(features.copy(), TEST_SYMBOL)
    # 52 bars covers the state horizon, pivot locality, and strategy warm-up.
    prefixed = strategy.generate_signals(features.iloc[28:].copy(), TEST_SYMBOL)

    pd.testing.assert_series_equal(full.iloc[80:], prefixed.loc[features.index[80:]])
    assert full.iloc[80] == -1


@pytest.mark.parametrize(
    "strategy_cls, parameter, default",
    [
        (InsideBarBreakoutStrategy, "setup_max_age_bars", 20),
        (FractalChannelBreakoutStrategy, "pivot_max_age_bars", 100),
        (HeadAndShouldersNecklineBreakStrategy, "pattern_max_age_bars", 100),
    ],
)
def test_state_age_defaults_are_explicit_and_optimizable(
    strategy_cls: type,
    parameter: str,
    default: int,
) -> None:
    strategy = strategy_cls()
    grid = strategy.get_param_grid()[parameter]

    assert strategy.params[parameter] == default
    assert default in grid
    assert len(set(grid)) >= 3
    assert all(isinstance(value, int) and value > 0 for value in grid)


def test_unused_ema_feature_requests_are_removed() -> None:
    assert FractalChannelBreakoutStrategy(ema_period=987).get_required_features() == {"ATR_14"}
    assert VolumeClimaxReversalStrategy(ema_period=987).get_required_features() == {"ATR_14"}
