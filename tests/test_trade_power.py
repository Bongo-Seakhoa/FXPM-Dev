import math
import json
import copy

import numpy as np
import pandas as pd
import pytest

from pm_search_outcomes import SearchOutcomeKind
from pm_trade_power import (
    TradePowerPolicy,
    adjusted_alpha,
    build_trade_sample_evidence,
    calibrate_train_trade_power,
    derive_trade_power_requirement,
    evaluate_trade_power_partition,
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
    nonnumeric_policy = derive_trade_power_requirement(
        sigma_r_robust=1.0,
        effective_trials=float("nan"),
        policy=_policy(
            minimum_economic_edge_r=None,
            maximum_ci_half_width_r="bad",
        ),
    )

    assert not missing_edge.passed
    assert "MINIMUM_ECONOMIC_EDGE_R_INVALID" in missing_edge.reason_codes
    assert not missing_sigma.passed
    assert "PILOT_SIGMA_R_INVALID" in missing_sigma.reason_codes
    assert not missing_trials.passed
    assert "EFFECTIVE_TRIALS_INVALID" in missing_trials.reason_codes
    assert not nonnumeric_policy.passed
    assert nonnumeric_policy.minimum_economic_edge_r == 0.0
    assert nonnumeric_policy.maximum_ci_half_width_r == 0.0
    json.dumps(nonnumeric_policy.to_dict(), allow_nan=False)


def _trade_records(values, *, overlap=False, timeframe=None, fake_r_multiple=None):
    base = pd.Timestamp("2010-01-01", tz="UTC")
    records = []
    for index, value in enumerate(values):
        entry = base if overlap else base + pd.Timedelta(days=2 * index)
        exit_ = base + pd.Timedelta(days=1) if overlap else entry + pd.Timedelta(days=1)
        record = {
            "pnl_dollars": float(value) * 100.0,
            "risk_amount": 100.0,
            "entry_time": entry,
            "exit_time": exit_,
            "trade_record_schema_version": "2026-06-18-mae-mfe-v1",
        }
        if timeframe is not None:
            record["timeframe"] = timeframe
        if fake_r_multiple is not None:
            record["r_multiple"] = fake_r_multiple
        records.append(record)
    return records


def _source_contract(
    *,
    provenance="AUTHENTICATED",
    status="AVAILABLE",
    binding_overrides=None,
    historical_rates_authenticated=True,
    stress_approved=False,
    stress_immutable=False,
    broker_swap_disabled=False,
):
    swap = {
        "contract_version": "causal_swap_accrual_v1",
        "status": status,
        "provenance_status": provenance,
        "execution_state_compounding": True,
        "source_provenance_authenticated": provenance == "AUTHENTICATED",
        "historical_rates_authenticated": historical_rates_authenticated,
        "stress_policy_explicitly_approved": stress_approved,
        "stress_policy_immutable": stress_immutable,
        "stress_policy_sha256": "b" * 64 if stress_immutable else "",
        "broker_swap_disabled": broker_swap_disabled,
        "timestamp_source": "_BAR_CLOSE_TS" if status == "AVAILABLE" else None,
    }
    source_binding = {
        "price_activation_manifest_sha256": "1" * 64,
        "terms_activation_manifest_sha256": "2" * 64,
        "terms_snapshot_sha256": "3" * 64,
        "historical_rates_sha256": "4" * 64,
        "broker_disabled_proof_sha256": "5" * 64,
    }
    source_binding.update(dict(binding_overrides or {}))
    return {
        "schema_version": "fxpm-trade-evidence-source-v1",
        "source_binding": source_binding,
        "trade_record_schema_version": "2026-06-18-mae-mfe-v1",
        "execution_cost_timing_contract": {
            "entry_slippage": "entry_slippage_parity_v1",
            "swap_accrual": "causal_swap_accrual_v1",
        },
        "swap_accrual_contract": swap,
    }


def _ar1(count, phi, seed=19):
    rng = np.random.default_rng(seed)
    innovations = rng.normal(0.0, math.sqrt(max(1e-9, 1.0 - phi * phi)), count)
    values = np.zeros(count, dtype=np.float64)
    for index in range(1, count):
        values[index] = phi * values[index - 1] + innovations[index]
    return values


def test_empty_underpowered_and_zero_variance_are_honest_ineligibility():
    empty = build_trade_sample_evidence([], source_contract=_source_contract())
    thin = build_trade_sample_evidence(
        _trade_records([0.5, -0.2, 0.1]), source_contract=_source_contract()
    )
    constant = build_trade_sample_evidence(
        _trade_records([0.5] * 20), source_contract=_source_contract()
    )

    assert empty.outcome_kind is SearchOutcomeKind.EXPECTED_INELIGIBLE
    assert empty.reason_codes == ("NO_TRADES",)
    assert thin.outcome_kind is SearchOutcomeKind.EXPECTED_INELIGIBLE
    assert "INSUFFICIENT_RAW_OBSERVATIONS" in thin.reason_codes
    assert constant.outcome_kind is SearchOutcomeKind.EXPECTED_INELIGIBLE
    assert "VARIANCE_UNAVAILABLE" in constant.reason_codes


@pytest.mark.parametrize(
    "mutator",
    [
        lambda row: row.pop("pnl_dollars"),
        lambda row: row.update(pnl_dollars=float("nan")),
        lambda row: row.update(risk_amount=0.0),
        lambda row: row.pop("entry_time"),
        lambda row: row.update(exit_time=pd.Timestamp("2000-01-01", tz="UTC")),
    ],
)
def test_any_malformed_positive_trade_record_is_infrastructure_failure(mutator):
    records = _trade_records(np.linspace(-1.0, 1.0, 20))
    mutator(records[7])

    evidence = build_trade_sample_evidence(
        records, source_contract=_source_contract()
    )

    assert evidence.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert evidence.status == "INFRA_FAILURE"
    assert evidence.reason_codes == ("MALFORMED_POSITIVE_TRADE_EVIDENCE",)
    assert evidence.usable_trades == 0


def test_non_sequence_trade_payload_is_infrastructure_failure():
    evidence = build_trade_sample_evidence(
        {"pnl_dollars": 10.0}, source_contract=_source_contract()
    )
    invalid_minimum = build_trade_sample_evidence(
        [], source_contract=_source_contract(), minimum_observations="bad"
    )

    assert evidence.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert evidence.reason_codes == ("TRADE_RECORDS_INVALID",)
    assert invalid_minimum.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert invalid_minimum.reason_codes == ("MINIMUM_OBSERVATIONS_INVALID",)


def test_source_contract_is_mandatory_and_exact_execution_tokens_are_binding():
    records = _trade_records(np.linspace(-1.0, 1.0, 20))
    missing = build_trade_sample_evidence(records)
    missing_even_when_empty = build_trade_sample_evidence([])
    legacy = _source_contract()
    legacy["execution_cost_timing_contract"]["entry_slippage"] = "legacy_v0"
    stale_execution = build_trade_sample_evidence(
        records, source_contract=legacy
    )
    stale_schema = _source_contract()
    stale_schema["trade_record_schema_version"] = "legacy_trade_schema"
    stale_schema_result = build_trade_sample_evidence(
        records, source_contract=stale_schema
    )
    fabricated = _source_contract()
    fabricated.pop("schema_version")
    fabricated_result = build_trade_sample_evidence(
        records, source_contract=fabricated
    )

    assert missing.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert missing.reason_codes == ("SOURCE_CONTRACT_MISSING",)
    assert missing_even_when_empty.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert missing_even_when_empty.reason_codes == ("SOURCE_CONTRACT_MISSING",)
    assert stale_execution.reason_codes == (
        "SOURCE_CONTRACT_EXECUTION_TIMING_INVALID",
    )
    assert stale_schema_result.reason_codes == (
        "SOURCE_CONTRACT_TRADE_SCHEMA_INVALID",
    )
    assert fabricated_result.reason_codes == ("SOURCE_CONTRACT_SCHEMA_INVALID",)


def test_current_snapshot_only_and_legacy_swap_provenance_are_rejected():
    records = _trade_records(np.linspace(-1.0, 1.0, 20))
    snapshot_only = build_trade_sample_evidence(
        records,
        source_contract=_source_contract(historical_rates_authenticated=False),
    )
    legacy = build_trade_sample_evidence(
        records,
        source_contract=_source_contract(provenance="LEGACY_UNTRUSTED"),
    )

    assert snapshot_only.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert snapshot_only.reason_codes == (
        "SOURCE_CONTRACT_SWAP_PROVENANCE_INVALID",
    )
    assert legacy.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert legacy.reason_codes == (
        "SOURCE_CONTRACT_SWAP_PROVENANCE_INVALID",
    )


def test_only_immutable_explicit_stress_or_authenticated_broker_disabled_are_alternatives():
    records = _trade_records(np.linspace(-1.0, 1.0, 20))
    approved_stress = build_trade_sample_evidence(
        records,
        source_contract=_source_contract(
            provenance="STRESS_APPROVED",
            stress_approved=True,
            stress_immutable=True,
        ),
    )
    mutable_stress = build_trade_sample_evidence(
        records,
        source_contract=_source_contract(
            provenance="STRESS_APPROVED",
            stress_approved=True,
            stress_immutable=False,
        ),
    )
    broker_disabled = build_trade_sample_evidence(
        records,
        source_contract=_source_contract(
            status="DISABLED",
            broker_swap_disabled=True,
        ),
    )
    config_disabled_only = build_trade_sample_evidence(
        records,
        source_contract=_source_contract(
            status="DISABLED",
            broker_swap_disabled=False,
        ),
    )

    assert approved_stress.passed
    assert (
        approved_stress.source_contract.historical_financing_provenance
        == "IMMUTABLE_STRESS_APPROVED"
    )
    assert mutable_stress.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert broker_disabled.passed
    assert (
        broker_disabled.source_contract.swap_provenance_class
        == "AUTHENTICATED_BROKER_DISABLED"
    )
    assert config_disabled_only.outcome_kind is SearchOutcomeKind.INFRA_FAILURE


def test_source_bindings_and_every_nonempty_trade_schema_are_verified():
    records = _trade_records(np.linspace(-1.0, 1.0, 20))
    malformed_binding = build_trade_sample_evidence(
        records,
        source_contract=_source_contract(
            binding_overrides={"terms_snapshot_sha256": "fabricated"}
        ),
    )
    wrong_trade_schema_records = copy.deepcopy(records)
    wrong_trade_schema_records[4]["trade_record_schema_version"] = "legacy"
    wrong_trade_schema = build_trade_sample_evidence(
        wrong_trade_schema_records,
        source_contract=_source_contract(),
    )

    assert malformed_binding.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert malformed_binding.reason_codes == ("SOURCE_CONTRACT_BINDING_INVALID",)
    assert wrong_trade_schema.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert wrong_trade_schema.reason_codes == ("TRADE_RECORD_SCHEMA_MISMATCH",)


def test_net_r_is_always_recomputed_from_post_cost_pnl_and_risk():
    values = np.linspace(-0.8, 1.2, 40)
    honest = build_trade_sample_evidence(
        _trade_records(values), source_contract=_source_contract()
    )
    fabricated = build_trade_sample_evidence(
        _trade_records(values, fake_r_multiple=999_999.0),
        source_contract=_source_contract(),
    )

    assert honest.passed and fabricated.passed
    assert honest.mean_net_r == fabricated.mean_net_r
    assert honest.net_r_values_sha256 == fabricated.net_r_values_sha256
    assert honest.sample_sha256 == fabricated.sample_sha256


def test_serial_dependence_and_overlapping_holds_reduce_effective_trades():
    independent = build_trade_sample_evidence(
        _trade_records(_ar1(600, 0.0, seed=31)),
        source_contract=_source_contract(),
    )
    persistent = build_trade_sample_evidence(
        _trade_records(_ar1(600, 0.92, seed=31)),
        source_contract=_source_contract(),
    )
    overlapping = build_trade_sample_evidence(
        _trade_records(_ar1(600, 0.0, seed=31), overlap=True),
        source_contract=_source_contract(),
    )

    assert independent.passed and persistent.passed and overlapping.passed
    assert persistent.effective_trades < independent.effective_trades
    assert overlapping.effective_trades == 1.0
    assert (
        overlapping.dependence_evidence["overlap_cluster_count"] == 1
    )


def test_train_calibration_freezes_sigma_multiplicity_and_all_partition_thresholds():
    values = _ar1(900, 0.0, seed=101) + 0.30
    decision = calibrate_train_trade_power(
        _trade_records(values),
        effective_trials=25,
        policy=_policy(),
        source_contract=_source_contract(),
    )

    assert decision.passed
    requirement = decision.frozen_requirement
    assert requirement is not None and requirement.frozen
    assert requirement.calibration_partition == "optimizer_train"
    assert requirement.calibration_sample_sha256 == decision.sample_evidence.sample_sha256
    assert requirement.effective_trials == 25.0
    assert requirement.multiplicity_method == "raw_trial_count_conservative_upper_bound_v1"
    assert requirement.multiple_testing_adjustment == "sidak_effective_trials"
    assert requirement.pilot_dispersion_method == "mad_then_sample_std_r"
    assert requirement.policy_schema_version == "fxpm-cell-power-v1"
    assert requirement.entry_slippage_contract_version == "entry_slippage_parity_v1"
    assert requirement.swap_accrual_contract_version == "causal_swap_accrual_v1"
    assert requirement.trade_record_schema_version == "2026-06-18-mae-mfe-v1"
    assert requirement.swap_provenance_class == "AUTHENTICATED"
    assert (
        requirement.historical_financing_provenance
        == "AUTHENTICATED_HISTORICAL_RATES"
    )
    assert len(requirement.calibration_source_contract_sha256) == 64
    assert requirement.required_effective_train_trades > 0
    assert requirement.required_effective_tuning_trades > 0
    assert requirement.required_effective_final_trades > 0


def test_tuning_and_lockbox_can_only_consume_not_recalibrate_frozen_requirement():
    train_values = _ar1(1_000, 0.0, seed=5) + 0.30
    train = calibrate_train_trade_power(
        _trade_records(train_values),
        effective_trials=10,
        policy=_policy(),
        source_contract=_source_contract(),
    )
    assert train.frozen_requirement is not None
    frozen_before = train.frozen_requirement.to_dict()

    tuning = evaluate_trade_power_partition(
        _trade_records(_ar1(1_000, 0.0, seed=6) + 0.15),
        frozen_requirement=train.frozen_requirement,
        partition="tuning_validation",
        source_contract=_source_contract(),
    )
    lockbox = evaluate_trade_power_partition(
        _trade_records((_ar1(1_000, 0.0, seed=7) * 4.0) - 2.0),
        frozen_requirement=train.frozen_requirement,
        partition="final_lockbox",
        source_contract=_source_contract(),
    )

    assert tuning.requirement_recalibrated is False
    assert lockbox.requirement_recalibrated is False
    assert tuning.frozen_requirement.to_dict() == frozen_before
    assert lockbox.frozen_requirement.to_dict() == frozen_before
    assert tuning.required_effective_trades == frozen_before[
        "required_effective_tuning_trades"
    ]
    assert lockbox.required_effective_trades == frozen_before[
        "required_effective_final_trades"
    ]


def test_partition_source_contract_drift_is_infrastructure_failure():
    train_source = _source_contract()
    train = calibrate_train_trade_power(
        _trade_records(_ar1(1_000, 0.0, seed=90) + 0.3),
        effective_trials=5,
        policy=_policy(),
        source_contract=train_source,
    )
    drifted_source = _source_contract(
        binding_overrides={"terms_snapshot_sha256": "9" * 64}
    )
    decision = evaluate_trade_power_partition(
        _trade_records(_ar1(1_000, 0.0, seed=91) + 0.2),
        frozen_requirement=train.frozen_requirement,
        partition="tuning_validation",
        source_contract=drifted_source,
    )
    missing = evaluate_trade_power_partition(
        _trade_records(_ar1(1_000, 0.0, seed=91) + 0.2),
        frozen_requirement=train.frozen_requirement,
        partition="tuning_validation",
    )

    assert decision.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert decision.reason_codes == ("SOURCE_CONTRACT_DRIFT",)
    assert missing.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert missing.reason_codes == ("SOURCE_CONTRACT_MISSING",)


def test_overlapping_holds_can_fail_power_despite_large_raw_trade_count():
    train = calibrate_train_trade_power(
        _trade_records(_ar1(800, 0.0, seed=17) + 0.3),
        effective_trials=1,
        policy=_policy(),
        source_contract=_source_contract(),
    )
    assert train.frozen_requirement is not None

    overlap = evaluate_trade_power_partition(
        _trade_records(_ar1(800, 0.0, seed=18), overlap=True),
        frozen_requirement=train.frozen_requirement,
        partition="tuning_validation",
        source_contract=_source_contract(),
    )

    assert not overlap.passed
    assert overlap.outcome_kind is SearchOutcomeKind.EXPECTED_INELIGIBLE
    assert overlap.observed_effective_trades == 1.0
    assert overlap.sample_evidence.raw_trade_records == 800


def test_unfrozen_or_wrong_partition_requirements_fail_as_infrastructure():
    unfrozen = derive_trade_power_requirement(
        sigma_r_robust=1.0,
        effective_trials=1,
        policy=_policy(),
    )
    invalid_requirement = evaluate_trade_power_partition(
        _trade_records(_ar1(100, 0.0)),
        frozen_requirement=unfrozen,
        partition="tuning_validation",
        source_contract=_source_contract(),
    )
    invalid_partition = evaluate_trade_power_partition(
        _trade_records(_ar1(100, 0.0)),
        frozen_requirement=unfrozen,
        partition="optimizer_train",
        source_contract=_source_contract(),
    )

    assert invalid_requirement.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert invalid_requirement.reason_codes == ("FROZEN_REQUIREMENT_INVALID",)
    assert invalid_partition.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert invalid_partition.reason_codes == ("PARTITION_INVALID",)


def test_invalid_power_policy_is_infrastructure_not_no_trade():
    decision = calibrate_train_trade_power(
        _trade_records(_ar1(200, 0.0)),
        effective_trials=10,
        policy=_policy(minimum_economic_edge_r=0.0),
        source_contract=_source_contract(),
    )

    assert decision.outcome_kind is SearchOutcomeKind.INFRA_FAILURE
    assert "MINIMUM_ECONOMIC_EDGE_R_INVALID" in decision.reason_codes


def test_same_samples_are_timeframe_invariant_and_artifacts_are_compact():
    values = _ar1(700, 0.0, seed=55) + 0.2
    m5 = calibrate_train_trade_power(
        _trade_records(values, timeframe="M5"),
        effective_trials=5,
        policy=_policy(),
        source_contract=_source_contract(),
    )
    d1 = calibrate_train_trade_power(
        _trade_records(values, timeframe="D1"),
        effective_trials=5,
        policy=_policy(),
        source_contract=_source_contract(),
    )

    assert m5.status == d1.status
    assert m5.observed_effective_trades == d1.observed_effective_trades
    assert m5.required_effective_trades == d1.required_effective_trades
    assert m5.sample_evidence.sample_sha256 == d1.sample_evidence.sample_sha256
    assert m5.frozen_requirement.to_dict() == d1.frozen_requirement.to_dict()

    payload = m5.to_dict()
    encoded = json.dumps(payload, sort_keys=True)
    assert "net_r_values" not in payload["sample_evidence"]
    assert "trades" not in payload["sample_evidence"]
    assert len(payload["sample_evidence"]["net_r_values_sha256"]) == 64
    assert len(payload["sample_evidence"]["holding_intervals_sha256"]) == 64
    compact_source = payload["sample_evidence"]["source_contract"]
    assert compact_source["entry_slippage_contract_version"] == (
        "entry_slippage_parity_v1"
    )
    assert compact_source["swap_accrual_contract_version"] == (
        "causal_swap_accrual_v1"
    )
    assert len(compact_source["source_contract_sha256"]) == 64
    assert "source_binding" not in compact_source
    assert "historical_rates_sha256" not in compact_source
    assert len(encoded) < 12_000
