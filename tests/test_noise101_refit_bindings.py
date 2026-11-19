"""Invented software-only refit bindings; no market data or financial claims."""
import pytest

from pm_current_refit_artifacts import (
    CurrentRefitArtifactError,
    build_current_refit_search_artifacts_v2,
)
from pm_pipeline import _current_refit_validate_trial
from pm_strategies import StrategyRegistry, resolve_strategy_parameters


def _artifacts(strategy_id, timeframe, parameters):
    return build_current_refit_search_artifacts_v2(
        symbol="US100", timeframe=timeframe, regime="TREND", strategy_id=strategy_id,
        parameters=parameters, lookback_kind="NATIVE_OBSERVATIONS", lookback_value=2000,
        fit_start_utc="2025-01-01T00:00:00+00:00", fit_end_utc="2025-02-01T00:00:00+00:00",
        latest_closed_bar_utc="2025-01-31T23:45:00+00:00",
        source_snapshot_sha256="a" * 64, source_bar_count=2000, seed=7,
    )


@pytest.mark.parametrize("strategy_id", ["NoiseBandBreakoutStrategy", "EMACrossoverStrategy"])
def test_public_refit_artifacts_and_trial_validator_share_cell_timeframe(strategy_id):
    defaults, _, _ = resolve_strategy_parameters(StrategyRegistry.get(strategy_id), "M15")
    artifacts = _artifacts(strategy_id, "M15", defaults)
    trial, parameters = _current_refit_validate_trial(
        artifacts.trial_definition, strategy_id=strategy_id,
        process_sha256=artifacts.winner_process_sha256,
        horizon_sha256=artifacts.horizon_process_sha256, timeframe="M15",
    )
    assert parameters == defaults
    assert trial["parameters"] == artifacts.trial_definition["parameters"]
    assert artifacts.horizon_process["timeframe"] == "M15"


def test_public_refit_artifacts_refuse_noise_parameters_from_other_timeframe():
    defaults, _, _ = resolve_strategy_parameters(StrategyRegistry.get("NoiseBandBreakoutStrategy"), "M5")
    with pytest.raises(CurrentRefitArtifactError, match="PARAMETER_FIXED_DEFAULT_MISMATCH"):
        _artifacts("NoiseBandBreakoutStrategy", "M15", defaults)
