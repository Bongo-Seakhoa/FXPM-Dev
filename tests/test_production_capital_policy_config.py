from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path

import pm_cap_calibration as cap
import pm_capital_authority_factory as authority
from pm_core import PipelineConfig


ROOT = Path(__file__).resolve().parents[1]


def _config() -> dict:
    return json.loads((ROOT / "config.json").read_text(encoding="utf-8"))


def test_shipped_capital_policy_is_strict_demo_only_and_uses_evidence_floors() -> None:
    policy = authority.load_capital_authority_policy(_config())

    assert policy.permitted_deployment_scopes == (authority.DEMO_PROBATION_SCOPE,)
    assert (
        policy.authorization_max_ttl_seconds == authority.MAX_AUTHORIZATION_TTL_SECONDS
    )
    assert asdict(policy.constraints) == asdict(cap.CapCalibrationConstraints())
    assert "production_capital_authority" not in PipelineConfig.__dataclass_fields__


def test_shipped_capital_roster_is_outcome_blind_and_concentration_neutral() -> None:
    policy = authority.load_capital_authority_policy(_config())
    rows = list(policy.candidate_policies)

    assert len(rows) == 30
    assert [row["policy_id"] for row in rows] == sorted(
        row["policy_id"] for row in rows
    )
    assert {row["max_total_stop_risk_pct"] for row in rows} == set(
        cap.PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT
    )

    for total in cap.PRODUCTION_TOTAL_STOP_RISK_CANDIDATES_PCT:
        at_total = [row for row in rows if row["max_total_stop_risk_pct"] == total]
        assert len(at_total) == 3
        assert {
            (row["max_factor_stop_risk_pct"], row["max_same_symbol_stop_risk_pct"])
            for row in at_total
        } == {
            (min(total, 2.0), min(total, 1.0)),
            (total, min(total, 1.0)),
            (total, total),
        }
        assert all(row["max_margin_utilization_pct"] == 50.0 for row in at_total)
        assert all(row["min_margin_level_pct"] == 200.0 for row in at_total)
        assert all(row["min_free_margin_pct"] == 50.0 for row in at_total)
        assert all(row["gap_stress_multiple"] == 2.5 for row in at_total)


def test_static_legacy_limits_cannot_collapse_the_frozen_candidate_roster() -> None:
    config = _config()
    pipeline = config["pipeline"]
    policy = authority.load_capital_authority_policy(config)

    assert (
        pipeline["max_total_open_risk_pct"],
        pipeline["max_factor_group_open_risk_pct"],
        pipeline["max_combined_risk_pct"],
    ) == (8.0, 2.0, 1.0)
    assert any(
        (
            row["max_total_stop_risk_pct"],
            row["max_factor_stop_risk_pct"],
            row["max_same_symbol_stop_risk_pct"],
        )
        != (8.0, 2.0, 1.0)
        for row in policy.candidate_policies
    )
    assert (
        len(policy.candidate_policies) <= policy.constraints.maximum_candidate_policies
    )
