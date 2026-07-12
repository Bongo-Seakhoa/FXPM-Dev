import math

import numpy as np

from pm_trade_power import (
    TradePowerPolicy,
    adjusted_alpha,
    derive_trade_power_requirement,
    robust_r_dispersion,
)


def _policy(**overrides):
    values = {
        "minimum_economic_edge_r": 0.25,
        "maximum_ci_half_width_r": 0.25,
        "target_power": 0.80,
        "alpha_train": 0.05,
        "alpha_tuning": 0.05,
        "alpha_final": 0.05,
    }
    values.update(overrides)
    return TradePowerPolicy(**values)


def test_worked_single_trial_requirement_matches_power_formula():
    requirement = derive_trade_power_requirement(
        sigma_r_robust=1.2,
        effective_trials=1,
        policy=_policy(),
    )

    assert requirement.passed
    assert requirement.required_effective_train_trades == 143
    assert requirement.required_effective_tuning_trades == 89
    assert requirement.required_effective_final_trades == 89


def test_search_multiplicity_tightens_train_requirement_only():
    one = derive_trade_power_requirement(
        sigma_r_robust=1.0,
        effective_trials=1,
        policy=_policy(),
    )
    many = derive_trade_power_requirement(
        sigma_r_robust=1.0,
        effective_trials=500,
        policy=_policy(),
    )

    assert many.alpha_train_adjusted < one.alpha_train_adjusted
    assert many.required_effective_train_trades > one.required_effective_train_trades
    assert many.required_effective_tuning_trades == one.required_effective_tuning_trades


def test_sidak_and_bonferroni_are_valid_and_conservative():
    sidak = adjusted_alpha(0.05, 100, method="sidak_effective_trials")
    bonferroni = adjusted_alpha(0.05, 100, method="bonferroni_effective_trials")

    assert 0.0 < bonferroni <= sidak < 0.05
    assert math.isclose(bonferroni, 0.0005)


def test_robust_dispersion_uses_mad_and_rejects_corruption():
    rng = np.random.default_rng(4)
    values = rng.normal(0.1, 1.0, size=2_000)
    values[-1] = 100.0
    sigma = robust_r_dispersion(values)

    assert sigma is not None
    assert 0.8 < sigma < 1.2
    assert robust_r_dispersion([0.0, float("nan"), 1.0]) is None
    assert robust_r_dispersion([1.0] * 20) is None


def test_invalid_calibration_fails_closed_without_hidden_defaults():
    missing_edge = derive_trade_power_requirement(
        sigma_r_robust=1.0,
        effective_trials=10,
        policy=_policy(minimum_economic_edge_r=0.0),
    )
    missing_sigma = derive_trade_power_requirement(
        sigma_r_robust=None,
        effective_trials=10,
        policy=_policy(),
    )
    missing_trials = derive_trade_power_requirement(
        sigma_r_robust=1.0,
        effective_trials=0,
        policy=_policy(),
    )

    assert not missing_edge.passed
    assert "MINIMUM_ECONOMIC_EDGE_R_INVALID" in missing_edge.reason_codes
    assert not missing_sigma.passed
    assert "PILOT_SIGMA_R_INVALID" in missing_sigma.reason_codes
    assert not missing_trials.passed
    assert "EFFECTIVE_TRIALS_INVALID" in missing_trials.reason_codes
