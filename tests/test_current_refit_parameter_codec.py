from __future__ import annotations

import copy

import numpy as np
import pytest

import pm_pipeline as pipeline
from pm_current_refit_artifacts import (
    CurrentRefitArtifactError,
    _normalized_parameters,
    build_current_refit_search_artifacts_v2,
    canonical_artifact_bytes,
)
from pm_current_refit_parameters import (
    CurrentRefitParameterCodecError,
    canonical_parameter_bytes,
    canonicalize_parameter_mapping,
    decode_parameter_mapping,
)
from pm_strategies import StrategyRegistry


REGIME_STRATEGY = "RegimeTransitionBreakoutStrategy"


def _regime_artifacts(parameters):
    return build_current_refit_search_artifacts_v2(
        symbol="EURUSD",
        timeframe="H1",
        regime="BREAKOUT",
        strategy_id=REGIME_STRATEGY,
        parameters=parameters,
        lookback_kind="NATIVE_OBSERVATIONS",
        lookback_value=1_000,
        fit_start_utc="2021-01-01T00:00:00Z",
        fit_end_utc="2026-01-02T00:00:00Z",
        latest_closed_bar_utc="2026-01-01T23:00:00Z",
        source_snapshot_sha256="a" * 64,
        source_bar_count=1_000,
        seed=20260714,
    )


def test_all_registered_defaults_and_grid_choices_round_trip_exactly() -> None:
    names = StrategyRegistry.list_all()
    assert names
    for strategy_id in names:
        base = StrategyRegistry.get(strategy_id)
        defaults = base.get_default_params()
        candidates = [defaults]
        for name, choices in base.get_param_grid().items():
            for choice in choices:
                candidate = copy.deepcopy(defaults)
                candidate[name] = choice
                candidates.append(candidate)

        for candidate in candidates:
            wire = _normalized_parameters(strategy_id, candidate)
            assert wire == pipeline._current_refit_json_parameters(candidate)
            runtime = decode_parameter_mapping(wire, defaults)
            fitted = StrategyRegistry.get(strategy_id, **runtime)
            assert canonical_parameter_bytes(fitted.get_params()) == canonical_parameter_bytes(wire)
            for name, value in runtime.items():
                if type(defaults[name]) in (tuple, list):
                    assert type(value) is type(defaults[name])


def test_regime_transition_tuple_and_list_inputs_have_one_artifact_identity() -> None:
    tuple_parameters = StrategyRegistry.get(REGIME_STRATEGY).get_params()
    list_parameters = copy.deepcopy(tuple_parameters)
    list_parameters["source_regimes"] = list(list_parameters["source_regimes"])
    list_parameters["target_regimes"] = list(list_parameters["target_regimes"])

    from_tuple = _regime_artifacts(tuple_parameters)
    from_list = _regime_artifacts(list_parameters)
    assert from_tuple == from_list
    assert canonical_parameter_bytes(tuple_parameters) == canonical_parameter_bytes(list_parameters)
    assert from_tuple.trial_definition["parameters"]["source_regimes"] == ["CHOP", "RANGE"]
    assert from_tuple.trial_definition["parameters"]["target_regimes"] == ["BREAKOUT"]

    _, process_sha = pipeline._current_refit_validate_process(
        from_tuple.winner_process,
        strategy_id=REGIME_STRATEGY,
    )
    request = {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "fit_start_utc": from_tuple.horizon_process["fit_start_utc"],
        "fit_end_utc": from_tuple.horizon_process["fit_end_utc"],
        "latest_closed_bar_utc": from_tuple.horizon_process["latest_closed_bar_utc"],
    }
    _, horizon_sha = pipeline._current_refit_validate_horizon(
        from_tuple.horizon_process,
        request=request,
        source_sha256="a" * 64,
        source_bar_count=1_000,
    )
    trial, wire = pipeline._current_refit_validate_trial(
        from_tuple.trial_definition,
        strategy_id=REGIME_STRATEGY,
        process_sha256=process_sha,
        horizon_sha256=horizon_sha,
    )
    runtime = decode_parameter_mapping(
        wire, StrategyRegistry.get(REGIME_STRATEGY).get_default_params()
    )
    assert type(runtime["source_regimes"]) is tuple
    assert type(runtime["target_regimes"]) is tuple
    assert StrategyRegistry.get(REGIME_STRATEGY, **runtime).get_params() == tuple_parameters
    assert canonical_artifact_bytes(trial) == pipeline._current_refit_canonical_bytes(trial)


@pytest.mark.parametrize(
    "parameters",
    [
        {"value": float("nan")},
        {"value": float("inf")},
        {"value": float("-inf")},
        {"value": 2**63},
        {"value": -(2**63) - 1},
        {"value": []},
        {"value": ["x"] * 65},
        {"value": [["nested"]]},
        {"value": ("x", ("nested",))},
        {"value": {"nested": "mapping"}},
        {"value": {"set"}},
        {"value": "\ud800"},
        {1: "non-string-key"},
    ],
)
def test_hostile_parameter_values_fail_closed(parameters) -> None:
    with pytest.raises(CurrentRefitParameterCodecError):
        canonicalize_parameter_mapping(parameters)


@pytest.mark.parametrize(
    ("wire", "templates"),
    [
        ({"period": [20]}, {"period": 20}),
        ({"regimes": "BREAKOUT"}, {"regimes": ("BREAKOUT",)}),
        ({"regimes": [1]}, {"regimes": ("BREAKOUT",)}),
        ({"regimes": ("BREAKOUT",)}, {"regimes": ("BREAKOUT",)}),
        ({"ratio": 2}, {"ratio": 2.0}),
        ({"ratio": 2.0}, {"ratio": 2}),
        ({"ratio": -0.0}, {"ratio": 0.0}),
        ({"unknown": 1}, {"period": 1}),
    ],
)
def test_wire_shape_and_scalar_type_splices_fail_closed(wire, templates) -> None:
    with pytest.raises(CurrentRefitParameterCodecError):
        decode_parameter_mapping(wire, templates)


def test_regime_sequence_normalization_and_non_grid_shapes_are_rejected() -> None:
    parameters = StrategyRegistry.get(REGIME_STRATEGY).get_params()
    lower_case = copy.deepcopy(parameters)
    lower_case["target_regimes"] = ["breakout"]
    with pytest.raises(CurrentRefitArtifactError, match="PARAMETERS_NOT_FULL_CANONICAL_SET"):
        _regime_artifacts(lower_case)

    outside_grid = copy.deepcopy(parameters)
    outside_grid["source_regimes"] = ["CHOP", "TREND"]
    with pytest.raises(CurrentRefitArtifactError, match="PARAMETER_VALUE_OUTSIDE_PROCESS"):
        _regime_artifacts(outside_grid)


def test_numpy_grid_scalars_and_key_order_have_stable_bytes() -> None:
    left = {"period": np.int64(20), "ratio": np.float64(2.0)}
    right = {"ratio": 2.0, "period": 20}
    assert canonical_parameter_bytes(left) == canonical_parameter_bytes(right)
    assert canonicalize_parameter_mapping(left) == {"period": 20, "ratio": 2.0}
