from __future__ import annotations

from copy import deepcopy

import pytest

import pm_cap_calibration as cap
from pm_cap_replay_producer import capital_policy_for_point
from pm_cap_calibration import (
    BLOCKED_NO_NUMERIC_AUTHORITY,
    PRODUCTION_NEUTRAL_CONCENTRATION_TOTALS_PCT,
    PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT,
    CapCalibrationConstraints,
    build_frozen_production_candidate_roster,
    calibrate_total_stop_risk_cap,
)
from pm_optimization_authority import _capital_execution_errors
from pm_portfolio_replay import PortfolioPolicy


def _point(total: float) -> dict[str, float]:
    factor = min(total, 2.0)
    symbol = min(factor, 1.0)
    return {
        "max_total_stop_risk_pct": total,
        "max_factor_stop_risk_pct": factor,
        "max_same_symbol_stop_risk_pct": symbol,
        "max_margin_utilization_pct": 50.0,
        "min_margin_level_pct": 200.0,
        "min_free_margin_pct": 50.0,
        "gap_stress_multiple": 2.5,
    }


def _points() -> list[dict[str, float]]:
    return [_point(total) for total in PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT]


def test_frozen_roster_includes_medium_and_high_risk_candidates_only() -> None:
    points = _points()
    roster = build_frozen_production_candidate_roster(points)

    assert {row["max_total_stop_risk_pct"] for row in roster} == set(
        PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT
    )
    assert {33.25, 50.0, 75.0, 90.0}.issubset(
        {row["max_total_stop_risk_pct"] for row in roster}
    )
    assert [row["policy_id"] for row in roster] == sorted(
        row["policy_id"] for row in roster
    )
    assert len(roster) == len(points) + len(
        PRODUCTION_NEUTRAL_CONCENTRATION_TOTALS_PCT
    )
    for total in PRODUCTION_NEUTRAL_CONCENTRATION_TOTALS_PCT:
        at_total = [
            row for row in roster if row["max_total_stop_risk_pct"] == total
        ]
        assert any(
            row["max_factor_stop_risk_pct"] == total
            and row["max_same_symbol_stop_risk_pct"] == total
            for row in at_total
        )
        assert any(
            row["max_factor_stop_risk_pct"] < total
            or row["max_same_symbol_stop_risk_pct"] < total
            for row in at_total
        )
        neutral = next(
            row
            for row in at_total
            if row["max_factor_stop_risk_pct"] == total
            and row["max_same_symbol_stop_risk_pct"] == total
        )
        replay_policy = capital_policy_for_point(PortfolioPolicy(), neutral)
        assert replay_policy.enforce_pair_correlation is False
        assert (
            replay_policy.max_open_risk_pct
            == replay_policy.max_factor_group_open_risk_pct
            == replay_policy.max_same_symbol_open_risk_pct
            == total
        )
    assert all(set(row) == {
        "policy_id",
        "max_total_stop_risk_pct",
        "max_factor_stop_risk_pct",
        "max_same_symbol_stop_risk_pct",
        "max_margin_utilization_pct",
        "min_margin_level_pct",
        "min_free_margin_pct",
        "gap_stress_multiple",
    } for row in roster)

    points[0]["max_total_stop_risk_pct"] = 99.0
    assert {row["max_total_stop_risk_pct"] for row in roster} == set(
        PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT
    )


def test_frozen_roster_rejects_missing_or_non_live_candidate_axes() -> None:
    missing = _points()[:-1]
    with pytest.raises(ValueError, match="candidate coverage is incomplete"):
        build_frozen_production_candidate_roster(missing)

    misaligned = _points()
    misaligned[-1]["max_factor_stop_risk_pct"] = 1.1
    with pytest.raises(ValueError, match="0.25% live quantum"):
        build_frozen_production_candidate_roster(misaligned)

    duplicate = _points()
    duplicate.append(deepcopy(duplicate[-1]))
    with pytest.raises(ValueError, match="duplicated"):
        build_frozen_production_candidate_roster(duplicate)

    numeric_alias = _points()
    alias = deepcopy(numeric_alias[-1])
    alias["max_margin_utilization_pct"] = 50
    numeric_alias.append(alias)
    with pytest.raises(ValueError, match="duplicated"):
        build_frozen_production_candidate_roster(numeric_alias)


def test_neutral_counterparts_are_matched_idempotent_and_bounded() -> None:
    points = _points()
    for total in PRODUCTION_NEUTRAL_CONCENTRATION_TOTALS_PCT:
        limited = next(
            point for point in points if point["max_total_stop_risk_pct"] == total
        )
        neutral = deepcopy(limited)
        neutral["max_factor_stop_risk_pct"] = total
        neutral["max_same_symbol_stop_risk_pct"] = total
        points.append(neutral)

    roster = build_frozen_production_candidate_roster(points)
    assert len(roster) == len(points)

    no_limited_90 = [
        point
        for point in points
        if not (
            point["max_total_stop_risk_pct"] == 90.0
            and (
                point["max_factor_stop_risk_pct"] < 90.0
                or point["max_same_symbol_stop_risk_pct"] < 90.0
            )
        )
    ]
    with pytest.raises(ValueError, match="concentration-limited.*90"):
        build_frozen_production_candidate_roster(no_limited_90)

    full = _points()
    for ordinal in range(64 - len(full)):
        variant = deepcopy(full[0])
        variant["gap_stress_multiple"] = 3.0 + ordinal / 100.0
        full.append(variant)
    assert len(full) == 64
    with pytest.raises(ValueError, match="exceeds its bound after.*neutral"):
        build_frozen_production_candidate_roster(full)


def test_neutral_axes_do_not_need_impossible_duplicate_cap_saturation() -> None:
    roster = build_frozen_production_candidate_roster(_points())
    neutral = next(
        row
        for row in roster
        if row["max_total_stop_risk_pct"] == 90.0
        and row["max_factor_stop_risk_pct"] == 90.0
        and row["max_same_symbol_stop_risk_pct"] == 90.0
    )
    limited = next(
        row
        for row in roster
        if row["max_total_stop_risk_pct"] == 90.0
        and row["max_factor_stop_risk_pct"] < 90.0
    )
    constraints = CapCalibrationConstraints()
    summary = {
        "accepted_positions": constraints.minimum_accepted_positions,
        "hac_effective_observations": max(
            constraints.minimum_hac_effective_observations,
            constraints.minimum_exposure_effective_observations,
        ),
        "dependence_block_count": constraints.minimum_paired_policy_observations,
        "near_cap_observations": {
            "total": constraints.minimum_near_cap_observations,
            "factor": 0,
            "same_symbol": 0,
        },
        "mtm_drawdown_ucb_pct": 0.0,
        "initial_stop_drawdown_ucb_pct": 0.0,
        "joint_gap_loss_ucb_pct": 0.0,
        "factor_gap_loss_ucb_pct": 0.0,
        "margin_utilization_ucb_pct": 0.0,
        "margin_level_lcb_pct": cap.FLAT_MARGIN_LEVEL_PCT,
        "free_margin_lcb_pct": 100.0,
    }

    neutral_failures = cap._safety_failures(summary, neutral, constraints)
    assert "FACTOR_CAP_NOT_IDENTIFIED" not in neutral_failures
    assert "SAME_SYMBOL_CAP_NOT_IDENTIFIED" not in neutral_failures

    limited_failures = cap._safety_failures(summary, limited, constraints)
    assert "FACTOR_CAP_NOT_IDENTIFIED" in limited_failures
    assert "SAME_SYMBOL_CAP_NOT_IDENTIFIED" in limited_failures


def test_roster_cannot_select_a_cap_without_authenticated_evidence() -> None:
    artifact = calibrate_total_stop_risk_cap(
        candidates=build_frozen_production_candidate_roster(_points()),
        base_policies={},
        evaluation_start="2020-01-01T00:00:00Z",
        evaluation_end="2026-01-01T00:00:00Z",
        lineage={},
        constraints=CapCalibrationConstraints(),
    )

    assert artifact["selection"]["status"] == BLOCKED_NO_NUMERIC_AUTHORITY
    assert artifact["selection"]["selected_policy_point_id"] is None
    assert artifact["selection"]["selected_capital_envelope"] is None
    assert "CAP_PRECOMMITMENT_MISSING" in artifact["selection"][
        "identification_failures"
    ]


def test_live_capital_execution_verifier_accepts_real_margin_code() -> None:
    envelope = {
        "max_total_stop_risk_pct": 8.0,
        "max_factor_stop_risk_pct": 2.0,
        "max_same_symbol_stop_risk_pct": 1.0,
        "max_margin_utilization_pct": 50.0,
        "min_margin_level_pct": 200.0,
        "min_free_margin_pct": 50.0,
        "gap_stress_multiple": 2.5,
    }

    assert _capital_execution_errors(envelope) == []
