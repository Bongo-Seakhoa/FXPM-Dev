"""Invented software-only Noise101 fixtures; no market/financial evidence."""
import numpy as np
import pandas as pd
import pytest

from pm_current_refit_parameters import build_parameter_domain
from pm_strategies import NoiseBandBreakoutStrategy, resolve_strategy_parameters


@pytest.mark.parametrize("timeframe,minutes,default,checks", [
    ("M5", 5, 6, [3, 6, 12, 24]),
    ("M15", 15, 2, [1, 2, 4, 8]),
    ("M30", 30, 1, [1, 2, 4]),
    ("H1", 60, 1, [1, 2]),
    ("H4", 240, 1, [1]),
    ("D1", 1440, 1, [1]),
])
def test_noise101_resolves_physical_cadence_before_domain(timeframe, minutes, default, checks):
    strategy = NoiseBandBreakoutStrategy()
    defaults, grid, groups = resolve_strategy_parameters(strategy, timeframe)
    domain = build_parameter_domain(defaults, grid, coupled_groups=groups)
    assert domain["defaults"]["bar_minutes"] == minutes
    assert domain["defaults"]["check_bars"] == default
    assert domain["defaults"]["sl_atr_mult"] == domain["defaults"]["tp_atr_mult"] == 2.0
    axes = {axis["names"][0]: [row[0] for row in axis["choices"]]
            for axis in domain["axes"]}
    assert axes["check_bars"] == checks
    assert "bar_minutes" not in axes and "vwap_exit" not in domain["defaults"]


def test_noise101_never_guesses_missing_timeframe():
    strategy = NoiseBandBreakoutStrategy()
    with pytest.raises(ValueError, match="NOISE101_TIMEFRAME_REQUIRED"):
        resolve_strategy_parameters(strategy, None)
    with pytest.raises(ValueError, match="NOISE101_TIMEFRAME_REQUIRED"):
        strategy.get_param_grid()
    with pytest.raises(ValueError, match="NOISE101_TIMEFRAME_REQUIRED"):
        strategy.generate_signals(pd.DataFrame(), "US100")


def _invented_sessions(missing_slots):
    frames = []
    for number in range(9):
        index = pd.date_range(pd.Timestamp("2025-01-06 16:30") + pd.Timedelta(days=number),
                              periods=78, freq="5min")
        close = np.full(78, 100.0)
        close[5] = 101.0 if number < 8 else 101.1
        frame = pd.DataFrame({"Open": 100.0, "Close": close}, index=index)
        if number in missing_slots:
            frame = frame.drop(index[5])
        frames.append(frame)
    return pd.concat(frames)


@pytest.mark.parametrize("missing_slots,expected", [((2, 3), 1), ((2, 3, 4), 0)])
def test_noise101_uses_last_session_keys_not_last_available_slots(missing_slots, expected):
    frame = _invented_sessions(missing_slots)
    original = frame.copy(deep=True)
    defaults, _, _ = resolve_strategy_parameters(NoiseBandBreakoutStrategy(), "M5")
    strategy = NoiseBandBreakoutStrategy(**{**defaults, "lookback": 7})
    result = strategy.generate_signals(frame, "US100")
    check_time = pd.Timestamp("2025-01-14 16:55")
    assert result.loc[check_time] == expected
    assert result.loc["2025-01-14"].sum() == expected
    future = frame.copy(deep=True)
    future.loc[future.index > check_time, "Close"] = 10000.0
    pd.testing.assert_series_equal(result.loc[:check_time],
                                   strategy.generate_signals(future, "US100").loc[:check_time])
    pd.testing.assert_frame_equal(frame, original)


def test_noise101_nominal_six_checks_does_not_count_future_rows():
    defaults, _, _ = resolve_strategy_parameters(NoiseBandBreakoutStrategy(), "M30")
    strategy = NoiseBandBreakoutStrategy(**{**defaults, "check_bars": 4, "lookback": 7})
    index = pd.date_range("2025-01-01", periods=30 * 48, freq="30min")
    frame = pd.DataFrame({"Open": 100.0, "Close": np.arange(len(index)) + 100.0}, index=index)
    assert not strategy.generate_signals(frame, "US100").any()


def test_noise101_resolver_refuses_persisted_cross_timeframe_binding():
    defaults, _, _ = resolve_strategy_parameters(NoiseBandBreakoutStrategy(), "M5")
    strategy = NoiseBandBreakoutStrategy(**defaults)
    with pytest.raises(ValueError, match="NOISE101_TIMEFRAME_BINDING_MISMATCH"):
        resolve_strategy_parameters(strategy, "M15")


def test_defaults_only_binding_never_constructs_unused_domain():
    class DefaultsOnlyNoise(NoiseBandBreakoutStrategy):
        def get_param_grid_for_timeframe(self, timeframe):
            raise AssertionError("unused grid was constructed")

        def get_param_couplings(self):
            raise AssertionError("unused couplings were constructed")

    defaults, grid, groups = resolve_strategy_parameters(DefaultsOnlyNoise(), "M5", defaults_only=True)
    assert defaults["bar_minutes"] == 5 and defaults["check_bars"] == 6
    assert grid is groups is None
