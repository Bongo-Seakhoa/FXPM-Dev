from __future__ import annotations

import itertools

import numpy as np
import pandas as pd
import pytest

from pm_state_contract import DEFAULT_AUTHORITY
from pm_strategies import StrategyRegistry


def _market_frame(seed: int, rows: int = 1_200) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    x = np.arange(rows, dtype=float)
    increments = 0.0002 * np.sin(x / 5.0) + rng.normal(0.0, 0.0009, rows)
    close = 1.1 + np.cumsum(increments)
    prior = np.r_[close[0], close[:-1]]
    spread = 0.0003 + rng.uniform(0.0001, 0.0010, rows)
    index = pd.date_range("2020-01-01", periods=rows, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "Open": prior,
            "High": np.maximum(close, prior) + spread,
            "Low": np.minimum(close, prior) - spread,
            "Close": close,
            "Volume": rng.integers(1, 10_000, rows).astype(float),
            # The state lane isolates strategy state. ATR's independently
            # declared finite dependency is supplied identically on both runs.
            "ATR_14": np.full(rows, 0.0008, dtype=float),
        },
        index=index,
    )


def _local_bound(name: str, params: dict[str, object]) -> int:
    resolution = DEFAULT_AUTHORITY.resolve_cell(
        name,
        parameters=params,
        timeframe="M5",
        include_regime_controller=False,
    )
    assert resolution.admissible, resolution.reason_codes
    node = next(item for item in resolution.nodes if item.name == f"strategy:{name}")
    assert node.kind == "FINITE_WINDOW"
    assert node.local_context_bars is not None
    return node.local_context_bars


FINITE_CASES = (
    (
        "AccumulationDistributionBreakoutStrategy",
        {"lookback": 10, "adl_slope_bars": 5, "breakout_buffer_atr": 0.0},
    ),
    (
        "AccumulationDistributionBreakoutStrategy",
        {"lookback": 40, "adl_slope_bars": 20, "breakout_buffer_atr": 0.1},
    ),
    (
        "FractalChannelBreakoutStrategy",
        {"wing_bars": 2, "atr_buffer": 0.0, "pivot_max_age_bars": 50},
    ),
    (
        "FractalChannelBreakoutStrategy",
        {"wing_bars": 5, "atr_buffer": 0.25, "pivot_max_age_bars": 200},
    ),
    (
        "OBVDivergenceStrategy",
        {"swing_order": 3, "obv_smooth": 0},
    ),
    (
        "OBVDivergenceStrategy",
        {"swing_order": 7, "obv_smooth": 10},
    ),
)


@pytest.mark.parametrize(("name", "params"), FINITE_CASES)
@pytest.mark.parametrize("seed", (3, 29))
def test_randomized_suffix_replay_matches_full_history_after_declared_bound(
    name: str,
    params: dict[str, object],
    seed: int,
) -> None:
    frame = _market_frame(seed)
    strategy = StrategyRegistry.get(name, **params)
    full = strategy.generate_signals(frame.copy(), "EURUSD")
    assert full.abs().sum() > 0
    bound = _local_bound(name, params)

    for cut in (97, 311, 577):
        suffix = strategy.generate_signals(frame.iloc[cut:].copy(), "EURUSD")
        comparison_start = cut + bound
        pd.testing.assert_series_equal(
            full.iloc[comparison_start:],
            suffix.iloc[bound:],
            check_names=False,
            check_dtype=True,
        )


@pytest.mark.parametrize(("name", "params"), FINITE_CASES)
def test_adversarial_prefix_values_cannot_change_post_bound_signals(
    name: str,
    params: dict[str, object],
) -> None:
    original = _market_frame(71)
    hostile = original.copy()
    cut = 500
    prefix_index = hostile.index[:cut]
    saw = np.where(np.arange(cut) % 2 == 0, -1.0, 1.0)
    hostile.loc[prefix_index, "Close"] = 100.0 + 40.0 * saw
    hostile.loc[prefix_index, "Open"] = 100.0 - 40.0 * saw
    hostile.loc[prefix_index, "High"] = 200.0 + np.arange(cut)
    hostile.loc[prefix_index, "Low"] = -200.0 - np.arange(cut)
    hostile.loc[prefix_index, "Volume"] = np.geomspace(1.0, 1e12, cut)

    strategy = StrategyRegistry.get(name, **params)
    baseline = strategy.generate_signals(original, "EURUSD")
    perturbed = strategy.generate_signals(hostile, "EURUSD")
    bound = _local_bound(name, params)
    pd.testing.assert_series_equal(
        baseline.iloc[cut + bound :],
        perturbed.iloc[cut + bound :],
        check_names=False,
        check_dtype=True,
    )


def test_adl_finite_delta_is_equivalent_to_legacy_cumulative_slope_on_candidate_domain() -> None:
    frame = _market_frame(113)
    strategy = StrategyRegistry.get("AccumulationDistributionBreakoutStrategy")
    money_flow = strategy._adl_money_flow(frame)
    # Preserve the legacy cumsum(skipna=True) behavior even for an interior
    # missing observation: it contributes no flow, while a missing comparison
    # endpoint remains ineligible.
    money_flow.iloc[[101, 307, 701]] = np.nan
    cumulative = money_flow.cumsum()
    grid = strategy.get_param_grid()

    for slope_bars, lookback in itertools.product(
        grid["adl_slope_bars"], grid["lookback"]
    ):
        legacy_up = cumulative > cumulative.shift(slope_bars)
        legacy_down = cumulative < cumulative.shift(slope_bars)
        finite_delta = strategy._finite_adl_delta(money_flow, slope_bars)
        pd.testing.assert_series_equal(
            legacy_up.fillna(False),
            (finite_delta > 0.0).fillna(False),
            check_names=False,
        )
        pd.testing.assert_series_equal(
            legacy_down.fillna(False),
            (finite_delta < 0.0).fillna(False),
            check_names=False,
        )
        assert _local_bound(
            "AccumulationDistributionBreakoutStrategy",
            {"adl_slope_bars": slope_bars, "lookback": lookback},
        ) >= max(slope_bars, lookback) + 1


def test_every_disputed_grid_axis_combination_has_a_finite_declared_bound() -> None:
    axes = {
        "AccumulationDistributionBreakoutStrategy": ("lookback", "adl_slope_bars"),
        "FractalChannelBreakoutStrategy": ("wing_bars", "pivot_max_age_bars"),
        "OBVDivergenceStrategy": ("swing_order", "obv_smooth"),
    }
    for name, keys in axes.items():
        strategy = StrategyRegistry.get(name)
        grid = strategy.get_param_grid()
        for values in itertools.product(*(grid[key] for key in keys)):
            params = dict(zip(keys, values))
            assert _local_bound(name, params) > 0
