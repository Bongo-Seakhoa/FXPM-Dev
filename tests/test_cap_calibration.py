from __future__ import annotations

from dataclasses import asdict, replace

import pytest

from pm_cap_calibration import (
    CAP_SWEEP_PCT,
    CapCalibrationConstraints,
    calibrate_total_stop_risk_cap,
    cap_neutral_config_sha256,
    seal_cap_calibration,
    validate_cap_calibration_artifact,
)
from pm_portfolio_replay import CandidateTrade, ExitLeg, PortfolioPolicy
from pm_portfolio_replay import PORTFOLIO_REPLAY_SCHEMA_VERSION


def candidate() -> CandidateTrade:
    return CandidateTrade(
        candidate_id="tuning-candidate",
        cell_id="EURUSD|H1|TREND|Test",
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_name="Test",
        promotion_status="PROMOTED",
        asset_class="fx",
        risk_groups=(),
        entry_time="2020-06-01",
        direction=1,
        stop_risk_per_lot=100.0,
        margin_per_lot=100.0,
        volume_min=0.01,
        volume_max=100.0,
        volume_step=0.01,
        priority_score=1.0,
        legs=(ExitLeg("2020-06-02", 1.0, 10.0),),
        marks=(),
    )


def policy() -> PortfolioPolicy:
    return PortfolioPolicy(
        starting_balance=10_000.0,
        target_risk_per_trade_pct=0.25,
        max_risk_per_trade_pct=0.50,
        max_open_risk_pct=8.0,
        max_concurrent_positions=200,
        allow_d1_plus_lower_tf=True,
        max_same_symbol_open_risk_pct=1.0,
        max_factor_group_open_risk_pct=2.0,
        max_correlated_cluster_risk_pct=2.0,
        max_margin_utilization_pct=50.0,
        min_margin_level_pct=200.0,
        min_free_margin_pct=50.0,
    )


def synthetic_summary(cap: float) -> dict:
    feasible = cap <= 12.0
    return {
        "schema_version": PORTFOLIO_REPLAY_SCHEMA_VERSION,
        "starting_balance": 10_000.0,
        "accepted_positions": 150,
        "profit_factor": 1.20,
        "return_pct": 5.0,
        "max_mark_to_market_drawdown_pct": 10.0 if feasible else 25.0,
        "max_initial_stop_stress_drawdown_pct": 12.0 if feasible else 30.0,
        "mtm_candidate_coverage_pct": 100.0,
        "observed_ruin": False,
        "max_open_risk_pct": cap * 0.80,
        "max_admission_total_stop_risk_pct": cap * 0.80,
        "exploratory_sequence_bootstrap": {
            "daily_return_observations": 500,
            "ruin_probability_wilson_95pct": [0.0, 1.0],
            "p95_max_drawdown_pct": 15.0,
        },
    }


def test_complete_tuning_sweep_selects_highest_feasible_without_lockbox(monkeypatch):
    def fake_run(self, _candidates):
        return {
            "policy": asdict(self.policy),
            "summary": synthetic_summary(self.policy.max_open_risk_pct),
        }

    monkeypatch.setattr(
        "pm_portfolio_replay.SynchronizedPortfolioSimulator.run",
        fake_run,
    )
    lineage = {"cap_neutral_config_sha256": "a" * 64, "ledger_sha256": "b" * 64}
    artifact = calibrate_total_stop_risk_cap(
        candidates=[candidate()],
        base_policies={10_000.0: policy()},
        evaluation_start="2020-01-01",
        evaluation_end="2022-01-01",
        lineage=lineage,
        constraints=CapCalibrationConstraints(),
    )
    assert artifact["partition"] == "tuning_validation"
    assert artifact["sweep_caps_pct"] == list(CAP_SWEEP_PCT)
    assert len(CAP_SWEEP_PCT) == 349
    assert CAP_SWEEP_PCT[:4] == (3.0, 3.25, 3.5, 3.75)
    assert CAP_SWEEP_PCT[-1] == 90.0
    assert artifact["selection"]["selected_max_total_stop_risk_pct"] == 8.0
    cap_above_gap_budget = next(
        row for row in artifact["cap_records"] if row["cap_pct"] == 8.25
    )
    assert (
        "10000.00:DETERMINISTIC_GAP_STRESS_TOO_HIGH"
        in cap_above_gap_budget["failures"]
    )
    assert validate_cap_calibration_artifact(
        artifact,
        expected_lineage=lineage,
        expected_selected_cap_pct=8.0,
    ) == []


def test_validator_rejects_lockbox_partition_and_non_highest_selection(monkeypatch):
    def fake_run(self, _candidates):
        return {"policy": asdict(self.policy), "summary": synthetic_summary(self.policy.max_open_risk_pct)}

    monkeypatch.setattr("pm_portfolio_replay.SynchronizedPortfolioSimulator.run", fake_run)
    lineage = {"cap_neutral_config_sha256": "a" * 64}
    artifact = calibrate_total_stop_risk_cap(
        candidates=[candidate()],
        base_policies={10_000.0: policy()},
        evaluation_start="2020-01-01",
        evaluation_end="2022-01-01",
        lineage=lineage,
        constraints=CapCalibrationConstraints(),
    )
    artifact["partition"] = "final_lockbox"
    artifact["selection"]["selected_max_total_stop_risk_pct"] = 7.0
    artifact = seal_cap_calibration(artifact)
    errors = validate_cap_calibration_artifact(
        artifact,
        expected_lineage=lineage,
        expected_selected_cap_pct=8.0,
    )
    assert "CAP_CALIBRATION_NOT_TUNING_ONLY" in errors
    assert "CAP_CALIBRATION_NOT_HIGHEST_FEASIBLE" in errors
    assert "CAP_CALIBRATION_CONFIG_CAP_MISMATCH" in errors


def test_cap_neutral_config_hash_ignores_selected_cap_and_artifact_pointer():
    first = {
        "pipeline": {
            "max_total_open_risk_pct": 8.0,
            "production_cap_calibration_path": "",
            "production_cap_calibration_sha256": "",
            "production_prelockbox_roster_path": "",
            "production_prelockbox_roster_sha256": "",
            "production_portfolio_replay_evidence_path": "",
            "winner_ledger_path": "pm_configs.json",
            "production_promotion_authorization_path": "",
            "production_promotion_authorization_sha256": "",
            "manual_new_entries_enabled": False,
            "max_factor_group_open_risk_pct": 2.0,
        }
    }
    second = {
        "pipeline": {
            "max_total_open_risk_pct": 12.0,
            "production_cap_calibration_path": "artifact.json",
            "production_cap_calibration_sha256": "f" * 64,
            "production_prelockbox_roster_path": "prelockbox.json",
            "production_prelockbox_roster_sha256": "e" * 64,
            "production_portfolio_replay_evidence_path": "final.json",
            "winner_ledger_path": "funded_winner_ledger.json",
            "production_promotion_authorization_path": "authorization.json",
            "production_promotion_authorization_sha256": "d" * 64,
            "manual_new_entries_enabled": True,
            "max_factor_group_open_risk_pct": 2.0,
        }
    }
    assert cap_neutral_config_sha256(first) == cap_neutral_config_sha256(second)


def test_gap_and_ruin_constraints_cannot_be_weakened():
    with pytest.raises(ValueError, match="gap multiple"):
        CapCalibrationConstraints(initial_stop_gap_multiple=2.49).validate()
    with pytest.raises(ValueError, match="1 percentage point"):
        CapCalibrationConstraints(
            maximum_bootstrap_ruin_upper_95pct=1.01
        ).validate()
    with pytest.raises(ValueError, match="500 bootstrap"):
        CapCalibrationConstraints(
            minimum_bootstrap_daily_return_observations=499
        ).validate()


def test_bootstrap_daily_observation_depth_fails_closed(monkeypatch):
    def fake_run(self, _candidates):
        summary = synthetic_summary(self.policy.max_open_risk_pct)
        summary["exploratory_sequence_bootstrap"][
            "daily_return_observations"
        ] = 499
        return {"policy": asdict(self.policy), "summary": summary}

    monkeypatch.setattr(
        "pm_portfolio_replay.SynchronizedPortfolioSimulator.run", fake_run
    )
    artifact = calibrate_total_stop_risk_cap(
        candidates=[candidate()],
        base_policies={10_000.0: policy()},
        evaluation_start="2020-01-01",
        evaluation_end="2022-01-01",
        lineage={"cap_neutral_config_sha256": "a" * 64},
        constraints=CapCalibrationConstraints(),
    )
    assert artifact["selection"]["selected_max_total_stop_risk_pct"] is None
    assert (
        "10000.00:BOOTSTRAP_DAILY_OBSERVATIONS_TOO_LOW"
        in artifact["cap_records"][0]["failures"]
    )


@pytest.mark.parametrize("boundary", ["entry", "exit"])
def test_cap_calibration_rejects_candidate_at_exclusive_end(boundary):
    original = candidate()
    end = "2022-01-01"
    if boundary == "entry":
        bounded = replace(
            original,
            entry_time=end,
            legs=(ExitLeg("2022-01-02", 1.0, 10.0),),
        )
    else:
        bounded = replace(
            original,
            legs=(ExitLeg(end, 1.0, 10.0),),
        )
    with pytest.raises(ValueError, match="escapes the tuning-validation window"):
        calibrate_total_stop_risk_cap(
            candidates=[bounded],
            base_policies={10_000.0: policy()},
            evaluation_start="2020-01-01",
            evaluation_end=end,
            lineage={"cap_neutral_config_sha256": "a" * 64},
            constraints=CapCalibrationConstraints(),
        )


def test_bootstrap_p95_drawdown_rejects_cap_even_when_ruin_bound_passes(monkeypatch):
    def fake_run(self, _candidates):
        summary = synthetic_summary(self.policy.max_open_risk_pct)
        summary["max_mark_to_market_drawdown_pct"] = 10.0
        summary["max_initial_stop_stress_drawdown_pct"] = 10.0
        summary["exploratory_sequence_bootstrap"]["p95_max_drawdown_pct"] = (
            25.0 if self.policy.max_open_risk_pct >= 7.0 else 15.0
        )
        return {"policy": asdict(self.policy), "summary": summary}

    monkeypatch.setattr("pm_portfolio_replay.SynchronizedPortfolioSimulator.run", fake_run)
    artifact = calibrate_total_stop_risk_cap(
        candidates=[candidate()],
        base_policies={10_000.0: policy()},
        evaluation_start="2020-01-01",
        evaluation_end="2022-01-01",
        lineage={"cap_neutral_config_sha256": "a" * 64},
        constraints=CapCalibrationConstraints(),
    )
    assert artifact["selection"]["selected_max_total_stop_risk_pct"] == 6.75
    cap_seven = next(row for row in artifact["cap_records"] if row["cap_pct"] == 7.0)
    assert "10000.00:BOOTSTRAP_P95_DRAWDOWN_TOO_HIGH" in cap_seven["failures"]


def test_bootstrap_ruin_upper_bound_is_in_percentage_points_and_capped_at_one(monkeypatch):
    def fake_run(self, _candidates):
        summary = synthetic_summary(self.policy.max_open_risk_pct)
        summary["exploratory_sequence_bootstrap"][
            "ruin_probability_wilson_95pct"
        ] = [0.0, 1.01]
        return {"policy": asdict(self.policy), "summary": summary}

    monkeypatch.setattr(
        "pm_portfolio_replay.SynchronizedPortfolioSimulator.run", fake_run
    )
    artifact = calibrate_total_stop_risk_cap(
        candidates=[candidate()],
        base_policies={10_000.0: policy()},
        evaluation_start="2020-01-01",
        evaluation_end="2022-01-01",
        lineage={"cap_neutral_config_sha256": "a" * 64},
        constraints=CapCalibrationConstraints(),
    )
    assert artifact["selection"]["selected_max_total_stop_risk_pct"] is None
    cap_three = artifact["cap_records"][0]
    assert "10000.00:BOOTSTRAP_RUIN_TOO_HIGH" in cap_three["failures"]


def test_calibration_identifies_cap_from_conservative_admission_not_path_drift(monkeypatch):
    def fake_run(self, _candidates):
        summary = synthetic_summary(self.policy.max_open_risk_pct)
        # An adverse move after admission can make fresh-equity path risk much
        # larger without exercising a different allocation ceiling.
        summary["max_open_risk_pct"] = 80.0
        return {"policy": asdict(self.policy), "summary": summary}

    monkeypatch.setattr(
        "pm_portfolio_replay.SynchronizedPortfolioSimulator.run", fake_run
    )
    artifact = calibrate_total_stop_risk_cap(
        candidates=[candidate()],
        base_policies={10_000.0: policy()},
        evaluation_start="2020-01-01",
        evaluation_end="2022-01-01",
        lineage={"cap_neutral_config_sha256": "a" * 64},
        constraints=CapCalibrationConstraints(),
    )
    assert artifact["selection"]["selected_max_total_stop_risk_pct"] == 8.0


def test_validator_fails_closed_when_required_summary_metric_is_missing(monkeypatch):
    def fake_run(self, _candidates):
        return {
            "policy": asdict(self.policy),
            "summary": synthetic_summary(self.policy.max_open_risk_pct),
        }

    monkeypatch.setattr("pm_portfolio_replay.SynchronizedPortfolioSimulator.run", fake_run)
    lineage = {"cap_neutral_config_sha256": "a" * 64}
    artifact = calibrate_total_stop_risk_cap(
        candidates=[candidate()],
        base_policies={10_000.0: policy()},
        evaluation_start="2020-01-01",
        evaluation_end="2022-01-01",
        lineage=lineage,
        constraints=CapCalibrationConstraints(),
    )
    selected_record = next(
        row for row in artifact["cap_records"] if row["cap_pct"] == 8.0
    )
    selected_record["scenarios"][0]["summary"].pop("return_pct")
    artifact = seal_cap_calibration(artifact)
    errors = validate_cap_calibration_artifact(
        artifact,
        expected_lineage=lineage,
        expected_selected_cap_pct=8.0,
    )
    assert "CAP_CALIBRATION_FEASIBILITY_MISMATCH:8.00" in errors


def test_executable_code_contract_change_invalidates_frozen_cap(monkeypatch):
    def fake_run(self, _candidates):
        return {
            "policy": asdict(self.policy),
            "summary": synthetic_summary(self.policy.max_open_risk_pct),
        }

    monkeypatch.setattr("pm_portfolio_replay.SynchronizedPortfolioSimulator.run", fake_run)
    lineage = {
        "artifact_contract_sha256": "a" * 64,
        "deployment_contract_sha256": "b" * 64,
        "critical_module_code_contract_sha256": "c" * 64,
    }
    artifact = calibrate_total_stop_risk_cap(
        candidates=[candidate()],
        base_policies={10_000.0: policy()},
        evaluation_start="2020-01-01",
        evaluation_end="2022-01-01",
        lineage=lineage,
        constraints=CapCalibrationConstraints(),
    )
    changed_code_lineage = {
        **lineage,
        "critical_module_code_contract_sha256": "d" * 64,
    }
    errors = validate_cap_calibration_artifact(
        artifact,
        expected_lineage=changed_code_lineage,
        expected_selected_cap_pct=8.0,
    )
    assert (
        "CAP_CALIBRATION_LINEAGE_MISMATCH:critical_module_code_contract_sha256"
        in errors
    )
