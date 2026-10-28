import importlib.util
import unittest
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pm_strategies import (
    AroonTrendStrategy,
    KaufmanAMATrendStrategy,
    _detect_swing_points,
)


def _naive_detect_swing_points(series: pd.Series, order: int):
    swing_highs = pd.Series(False, index=series.index)
    swing_lows = pd.Series(False, index=series.index)
    vals = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    for i in range(order, len(vals) - order):
        window = vals[i - order:i + order + 1]
        if np.all(np.isnan(window)):
            continue
        if vals[i] == np.nanmax(window):
            swing_highs.iloc[i + order] = True
        if vals[i] == np.nanmin(window):
            swing_lows.iloc[i + order] = True
    return swing_highs, swing_lows


def _naive_aroon(high: pd.Series, low: pd.Series, period: int):
    aroon_up = pd.Series(np.nan, index=high.index)
    aroon_down = pd.Series(np.nan, index=low.index)
    high_vals = high.to_numpy(dtype=float)
    low_vals = low.to_numpy(dtype=float)
    for i in range(period, len(high_vals)):
        window_h = high_vals[i - period:i + 1]
        window_l = low_vals[i - period:i + 1]
        if np.all(np.isnan(window_h)) or np.all(np.isnan(window_l)):
            continue
        bars_since_high = period - int(np.where(window_h == np.nanmax(window_h))[0][-1])
        bars_since_low = period - int(np.where(window_l == np.nanmin(window_l))[0][-1])
        aroon_up.iat[i] = 100.0 * (period - bars_since_high) / period
        aroon_down.iat[i] = 100.0 * (period - bars_since_low) / period
    return aroon_up, aroon_down


def _naive_kaufman_ama(close: np.ndarray, er_period: int, fast_period: int, slow_period: int):
    n = len(close)
    ama = np.full(n, np.nan)
    fast_sc = 2.0 / (fast_period + 1)
    slow_sc = 2.0 / (slow_period + 1)
    if er_period >= n:
        return ama
    ama[er_period] = close[er_period]
    for i in range(er_period + 1, n):
        direction = abs(close[i] - close[i - er_period])
        volatility = 0.0
        for j in range(i - er_period + 1, i + 1):
            volatility += abs(close[j] - close[j - 1])
        er = direction / volatility if volatility > 0 else 0.0
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        ama[i] = ama[i - 1] + sc * (close[i] - ama[i - 1])
    return ama


@lru_cache(maxsize=1)
def _stage_a_oracle():
    path = (Path(__file__).resolve().parents[1] / "audit_reports/fxpm_workshop_20261007"
            / "coupled_strategy_domain_trim_verification/stage_a_oracle.py")
    spec = importlib.util.spec_from_file_location("fxpm_stage_a_oracle", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("index_kind", ["utc", "range", "nonmonotonic"])
@pytest.mark.parametrize("case", [
    "finite", "flat", "missing-open", "missing-close", "missing-both",
    "nan-high-low", "numeric-strings", "nullable", "empty",
])
def test_candle_parts_matches_original_outputs_and_preserves_input(case, index_kind):
    oracle = _stage_a_oracle()
    oracle.assert_candle_equal(oracle.candle_frame(case, index_kind))


@pytest.mark.parametrize("ordinal", range(33))
def test_candle_callers_preserve_original_default_and_variant_signals(ordinal):
    oracle = _stage_a_oracle()
    name, parameters = oracle.candle_parameter_cases()[ordinal]
    oracle.assert_signal_equal(name, parameters, oracle.market_frame(),
                               label=f"original roster {ordinal}:{name}")


@pytest.mark.parametrize("inside", [1, 2, 3])
@pytest.mark.parametrize("age", [5, 10, 20, 40])
def test_inside_bar_matches_original_for_every_entry_domain_pair(inside, age):
    oracle = _stage_a_oracle()
    for label, frame in oracle.inside_edge_frames(inside, age):
        oracle.assert_signal_equal("InsideBarBreakoutStrategy", {
            "min_inside_bars": inside, "setup_max_age_bars": age,
        }, frame, label=f"inside={inside},age={age}:{label}")


@pytest.mark.parametrize("channel", [10, 15, 20, 25])
@pytest.mark.parametrize("window", [1, 2, 3])
def test_turtle_soup_matches_original_for_every_entry_domain_pair(channel, window):
    oracle = _stage_a_oracle()
    for label, frame in oracle.turtle_edge_frames(channel, window):
        oracle.assert_signal_equal("TurtleSoupReversalStrategy", {
            "channel_period": channel, "reclaim_window": window,
        }, frame, label=f"channel={channel},window={window}:{label}")


@pytest.mark.parametrize("copy_on_write", [False, True])
@pytest.mark.parametrize("name", ["InsideBarBreakoutStrategy", "TurtleSoupReversalStrategy"])
def test_structure_signal_arrays_preserve_copy_on_write_and_nullable_errors(name, copy_on_write):
    oracle = _stage_a_oracle()
    with pd.option_context("mode.copy_on_write", copy_on_write):
        oracle.assert_signal_equal(name, {}, oracle.market_frame(),
                                   label=f"{name}:copy_on_write={copy_on_write}")
        frame = oracle.market_frame(80)
        for column in ("Open", "High", "Low", "Close"):
            frame[column] = frame[column].astype("Float64")
        if name == "InsideBarBreakoutStrategy":
            frame.loc[frame.index[1], "High"] = pd.NA
        else:
            frame.loc[frame.index[30], "Close"] = pd.NA
        strategy = getattr(oracle.pm_strategies, name)()
        original_input, current_input = frame.copy(deep=True), frame.copy(deep=True)
        with pytest.raises(TypeError) as original_error:
            oracle.original_functions()[name](strategy, original_input, "TEST")
        with pytest.raises(type(original_error.value)) as current_error:
            strategy.generate_signals(current_input, "TEST")
        assert str(current_error.value) == str(original_error.value)
        pd.testing.assert_frame_equal(original_input, frame, check_exact=True)
        pd.testing.assert_frame_equal(current_input, frame, check_exact=True)


class StrategyHelperParityTests(unittest.TestCase):
    def test_detect_swing_points_matches_naive_loop(self):
        series = pd.Series([1.0, 2.0, np.nan, 4.0, 4.0, 2.0, 5.0, 1.0, 1.0, 3.0])
        high_fast, low_fast = _detect_swing_points(series, order=2)
        high_naive, low_naive = _naive_detect_swing_points(series, order=2)

        self.assertTrue(high_fast.equals(high_naive))
        self.assertTrue(low_fast.equals(low_naive))

    def test_aroon_matches_naive_loop(self):
        high = pd.Series([1, 2, 4, 4, 3, 5, 2, 6, 6, 4], dtype=float)
        low = pd.Series([0, 1, 1, 2, 1, 2, 0, 1, 2, 1], dtype=float)

        up_fast, down_fast = AroonTrendStrategy._aroon(high, low, period=4)
        up_naive, down_naive = _naive_aroon(high, low, period=4)

        np.testing.assert_allclose(up_fast.to_numpy(dtype=float), up_naive.to_numpy(dtype=float), equal_nan=True)
        np.testing.assert_allclose(down_fast.to_numpy(dtype=float), down_naive.to_numpy(dtype=float), equal_nan=True)

    def test_kaufman_ama_matches_naive_loop(self):
        close = np.array([100.0, 100.5, 101.0, 100.8, 101.2, 101.5, 101.3, 101.9, 102.1, 102.4])
        fast = KaufmanAMATrendStrategy._kaufman_ama(close, er_period=3, fast_period=2, slow_period=10)
        naive = _naive_kaufman_ama(close, er_period=3, fast_period=2, slow_period=10)
        np.testing.assert_allclose(fast, naive, equal_nan=True)


if __name__ == "__main__":
    unittest.main()
