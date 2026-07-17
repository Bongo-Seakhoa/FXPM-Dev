import copy
from datetime import datetime, timedelta
import inspect

import pm_model_lifecycle as lifecycle
import pm_specialist_arbitration as arbitration
from pm_model_lifecycle import (
    ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION,
    CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
    CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    CURRENT_REFIT_RECEIPT_VERSION,
    build_typed_receipt,
    canonical_receipt_sha256,
    compute_current_refit_cell_id,
    compute_current_winner_roster_sha256,
)
from pm_specialist_arbitration import (
    ARBITRATION_CONTRACT_VERSION,
    ARBITRATION_POLICY_VERSION,
    POLICY_RECEIPT_TYPE,
    REGIME_ORDER,
    SIGNAL_RECEIPT_TYPE,
    SIGNAL_SNAPSHOT_RECEIPT_VERSION,
    TIMEFRAME_ORDER,
    arbitration_contract_snapshot,
    rank_specialist_surfaces,
    verify_arbitration_decision,
)


CUTOFF = "2026-07-13T00:00:00+00:00"
SIGNAL_AS_OF = "2026-07-13T20:00:00+00:00"


def _digest(label):
    return canonical_receipt_sha256({"label": label})


def _process_decision():
    return {
        "schema": "opaque-causal-schema-not-pinned-by-lifecycle",
        "authority_version": "causal-v3-test",
        "authority_scope": "OFFLINE_PROCESS_VALIDATION_ONLY",
        "permitted_use": "OFFLINE_PROCESS_VALIDATION_EVIDENCE_INPUT",
        "explicit_non_authorities": [
            "CURRENT_REFIT",
            "MODEL_PROMOTION",
            "CAPITAL_ALLOCATION",
            "FUNDED_LIVE_TRADING",
            "LIVE_TRADING",
            "PROFITABILITY_CLAIM",
        ],
        "process_id": "causal-process-v3-accepted",
        "process_definition_sha256": _digest("causal-definition"),
        "policy_sha256": _digest("causal-policy"),
        "precommit_receipt_sha256": _digest("causal-precommit"),
        "evidence_receipt_sha256": _digest("causal-evidence"),
        "reconciliation_receipt_sha256": _digest("causal-reconciliation"),
        "evaluated_at_utc": "2026-07-12T11:00:00+00:00",
        "metrics": {
            "anchor_count": 4,
            "total_calendar_days": 1_460,
            "completed_trade_count": 480,
            "effective_dependence_blocks": "42.5",
            "sample_size_hac_floor_calendar_days": 34,
            "primary_hac_bandwidth_calendar_days": 34,
            "sensitivity_hac_bandwidth_calendar_days": 68,
            "most_influential_block_calendar_days": 35,
            "small_cluster_degrees_of_freedom": 3,
            "small_cluster_one_sided_critical_value": "2.353",
        },
        "gates": {"primary_hac": True, "sensitivity_hac": True, "influence": True},
        "decision": "OFFLINE_PROCESS_VALIDATION_PASS",
        "failure_reasons": [],
    }


def _winner_roster():
    return [
        {
            "symbol": "EURUSD",
            "timeframe": timeframe,
            "regime": "TREND",
            "strategy_id": f"Current{timeframe}Expert",
            "winner_process_candidate_id": f"winner-{timeframe.lower()}",
            "winner_process_sha256": _digest(f"winner-process-{timeframe}"),
            "winner_horizon_process_sha256": _digest(f"current-horizon-{timeframe}"),
            "winner_trial_definition_sha256": _digest(f"current-trial-{timeframe}"),
            "winner_refit_recipe_sha256": _digest(f"current-recipe-{timeframe}"),
            "winner_decision_sha256": _digest(f"winner-decision-{timeframe}"),
            "winner_book_entry_sha256": _digest(f"book-entry-{timeframe}"),
            "regime_fit_receipt_sha256": _digest(f"regime-fit-{timeframe}"),
            "regime_detector_record_sha256": _digest(
                f"regime-detector-record-{timeframe}"
            ),
            "regime_detector_parameters_sha256": _digest(
                f"regime-detector-parameters-{timeframe}"
            ),
            "regime_detector_code_sha256": _digest(
                f"regime-detector-code-{timeframe}"
            ),
        }
        for timeframe in ("M5", "M15", "D1")
    ]


def _generation_sha(payload):
    return canonical_receipt_sha256(
        {name: value for name, value in payload.items() if name != "generation_sha256"}
    )


def _base_generation(proposed=("M5", "D1"), no_signal=()):
    process = _process_decision()
    process_sha = canonical_receipt_sha256(process)
    roster = _winner_roster()
    roster_sha = compute_current_winner_roster_sha256(roster)
    regime_params_sha = _digest("current-regime-params")
    book_payload = {
        "schema_version": ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION,
        "generation_id": "current-full-process-generation-20260713",
        "historical_process_decision_sha256": process_sha,
        "process_id": process["process_id"],
        "process_definition_sha256": process["process_definition_sha256"],
        "generation_sha256": "0" * 64,
        "current_cutoff_utc": CUTOFF,
        "maximum_input_timestamp_utc": "2026-07-12T23:55:00+00:00",
        "source_snapshot_bundle_sha256": _digest("source-bundle"),
        "regime_params_sha256": regime_params_sha,
        "horizon_decisions_sha256": _digest("current-horizon-decisions"),
        "full_cell_outcome_roster_sha256": _digest("all-current-cell-outcomes"),
        "selected_winner_roster_sha256": roster_sha,
        "trial_ledger_bundle_sha256": _digest("all-current-trial-ledgers"),
        "winner_bundle_sha256": _digest("winner-bundle"),
        "output_book_sha256": _digest("current-process-winner-book"),
        "book_multiplicity_receipt_sha256": _digest("book-multiplicity"),
        "full_process_execution_trace_sha256": _digest("current-process-trace"),
        "started_at_utc": "2026-07-13T00:00:01+00:00",
        "completed_at_utc": "2026-07-13T02:00:00+00:00",
        "status": "COMPLETE",
        "selection_scope": "FULL_CURRENT_PROCESS_ALL_PRECOMMITTED_CELLS",
        "precommitted_cell_count": 2_400,
        "selected_winner_count": len(roster),
        "attempts": 1,
        "identical_frozen_process_executed": True,
        "post_cutoff_data_used": False,
        "complete_precommitted_roster_processed": True,
        "current_winner_selection_completed": True,
        "historical_anchor_winner_inputs_used": False,
        "survivor_only_reoptimization": False,
        "parameter_level_oos_evidence_claimed": False,
        "funded_live_authority": False,
    }
    book_payload["generation_sha256"] = _generation_sha(book_payload)
    book_receipt = build_typed_receipt(
        book_payload,
        receipt_type=CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
    )
    book_receipt_sha = canonical_receipt_sha256(book_receipt)
    cells = []
    latest_refit_bar = {
        "M5": "2026-07-12T23:55:00+00:00",
        "M15": "2026-07-12T23:45:00+00:00",
        "D1": "2026-07-12T00:00:00+00:00",
    }
    fit_start = {
        "M5": "2024-01-01T00:00:00+00:00",
        "M15": "2022-01-01T00:00:00+00:00",
        "D1": "2010-01-01T00:00:00+00:00",
    }
    for winner in roster:
        timeframe = winner["timeframe"]
        identity = {
            "historical_process_decision_sha256": process_sha,
            "accepted_book_generation_receipt_sha256": book_receipt_sha,
            **winner,
            "fit_start_utc": fit_start[timeframe],
            "fit_end_utc": CUTOFF,
            "latest_closed_bar_utc": latest_refit_bar[timeframe],
            "source_snapshot_sha256": _digest(f"source-{timeframe}"),
            "parameter_sha256": _digest(f"parameters-{timeframe}"),
            "output_artifact_sha256": _digest(f"artifact-{timeframe}"),
        }
        cells.append(
            {
                "current_refit_cell_id": compute_current_refit_cell_id(identity),
                **identity,
                "execution_trace_sha256": _digest(f"trace-{timeframe}"),
                "attempts": 1,
                "status": "COMPLETE",
                "refit_process_frozen": True,
                "parameter_level_oos_evidence_claimed": False,
            }
        )
    authorization = {
        "authorization_id": "current-refit-20260713",
        "historical_process_decision_sha256": process_sha,
        "process_id": process["process_id"],
        "process_definition_sha256": process["process_definition_sha256"],
        "accepted_book_generation_receipt_sha256": book_receipt_sha,
        "accepted_book_generation_sha256": book_payload["generation_sha256"],
        "current_cutoff_utc": CUTOFF,
        "issued_at_utc": "2026-07-13T02:01:00+00:00",
        "source_snapshot_bundle_sha256": book_payload["source_snapshot_bundle_sha256"],
        "regime_params_sha256": regime_params_sha,
        "horizon_decisions_sha256": book_payload["horizon_decisions_sha256"],
        "selected_winner_roster_sha256": roster_sha,
        "one_touch_nonce_sha256": _digest("one-touch"),
    }
    authorization_sha = canonical_receipt_sha256(authorization)
    execution = {
        "authorization_sha256": authorization_sha,
        "process_definition_sha256": process["process_definition_sha256"],
        "accepted_book_generation_sha256": book_payload["generation_sha256"],
        "started_at_utc": "2026-07-13T02:02:00+00:00",
        "completed_at_utc": "2026-07-13T03:00:00+00:00",
        "current_cutoff_utc": CUTOFF,
        "output_book_sha256": _digest("current-output-book"),
        "cell_execution_bundle_sha256": canonical_receipt_sha256(
            {"cell_executions": cells}
        ),
        "execution_trace_sha256": _digest("global-trace"),
        "cell_executions": cells,
        "attempts": 1,
        "status": "COMPLETE",
        "refit_process_frozen": True,
        "post_authorization_policy_change": False,
        "parameter_level_oos_evidence_claimed": False,
    }
    refit_payload = {
        "schema_version": CURRENT_REFIT_RECEIPT_VERSION,
        "authorization": authorization,
        "authorization_sha256": authorization_sha,
        "execution": execution,
        "execution_sha256": canonical_receipt_sha256(execution),
    }
    refit_receipt = build_typed_receipt(
        refit_payload,
        receipt_type=CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    )
    refit_receipt_sha = canonical_receipt_sha256(refit_receipt)
    runtime_calibration_bundle = _digest("current-runtime-calibration-bundle")
    shared_regime_function = _digest("one-shared-regime-function")
    shared_utility_function = _digest("one-shared-utility-function")
    cell_policies = []
    for timeframe in TIMEFRAME_ORDER:
        for regime in REGIME_ORDER:
            cell_policies.append(
                {
                    "timeframe": timeframe,
                    "regime": regime,
                    "edge_decay_evidence_sha256": _digest(
                        f"edge-decay-{timeframe}-{regime}"
                    ),
                    "calibration_receipt_sha256": _digest(
                        f"calibration-{timeframe}-{regime}"
                    ),
                    "threshold_selection_receipt_sha256": _digest(
                        f"thresholds-{timeframe}-{regime}"
                    ),
                    "calibrated_at_utc": "2026-07-13T04:00:00+00:00",
                    "calibration_fit_end_utc": CUTOFF,
                    "maximum_calibration_input_utc": "2026-07-12T23:55:00+00:00",
                    "valid_from_utc": CUTOFF,
                    "valid_through_utc": "2026-08-13T00:00:00+00:00",
                    "retrain_due_utc": "2026-08-13T00:00:00+00:00",
                    "status": "CALIBRATED",
                    "regime_probability_method": "OOS_CALIBRATED_PROBABILITY",
                    "minimum_regime_probability": "0.6",
                    "minimum_conditional_survival_probability": "0.55",
                    "minimum_conditional_net_r_utility": "0",
                    "thresholds_selected_inside_historical_anchors": True,
                    "rerun_in_current_process": True,
                    "parameter_level_oos_evidence_claimed": False,
                }
            )
    policy_payload = {
        "schema_version": ARBITRATION_POLICY_VERSION,
        "policy_id": "causal-current-runtime-policy-20260713",
        "symbol": "EURUSD",
        "historical_process_decision_sha256": process_sha,
        "process_id": process["process_id"],
        "process_definition_sha256": process["process_definition_sha256"],
        "accepted_book_generation_receipt_sha256": book_receipt_sha,
        "accepted_book_generation_sha256": book_payload["generation_sha256"],
        "current_refit_receipt_sha256": refit_receipt_sha,
        "causal_policy_sha256": process["policy_sha256"],
        "causal_precommit_receipt_sha256": process["precommit_receipt_sha256"],
        "causal_evidence_receipt_sha256": process["evidence_receipt_sha256"],
        "causal_reconciliation_receipt_sha256": process[
            "reconciliation_receipt_sha256"
        ],
        "runtime_policy_definition_sha256": _digest("runtime-policy-definition"),
        "runtime_calibration_bundle_sha256": runtime_calibration_bundle,
        "shared_regime_function_sha256": shared_regime_function,
        "shared_utility_function_sha256": shared_utility_function,
        "closed_bar_contract_sha256": _digest("closed-bar-contract"),
        "portfolio_state_contract_sha256": _digest("portfolio-state-contract"),
        "decision_hierarchy": [
            "CALIBRATED_REGIME_VALIDITY_AND_CONDITIONAL_SURVIVAL",
            "ACTUAL_SIGNAL_PRESENCE",
            "COMMON_CONDITIONAL_NET_R_UTILITY",
            "EVIDENCE_STRENGTH_EXACT_UTILITY_TIE_ONLY",
        ],
        "utility_unit": "NET_R_PER_ONE_RISK_UNIT",
        "selection_method": "MAX_NET_R_THEN_EVIDENCE_EXACT_TIES_ONLY",
        "cell_calibration_policies": cell_policies,
        "process_validated_calibration_procedure": True,
        "current_calibration_rerun": True,
        "current_signal_only": True,
        "capital_authority": False,
        "order_authority": False,
        "funded_live_authority": False,
        "profitability_claim": False,
    }
    policy_receipt = build_typed_receipt(
        policy_payload,
        receipt_type=POLICY_RECEIPT_TYPE,
    )
    cell_by_timeframe = {cell["timeframe"]: cell for cell in cells}
    portfolio_sha = _digest("portfolio-state-snapshot")
    state_values = {
        "M5": ("0.8", "0.75", "0.25", "0.7", "SELL", 3_600),
        "M15": ("0.85", "0.8", "0.1", "0.8", "BUY", 7_200),
        "D1": ("0.75", "0.7", "0.2", "0.9", "BUY", 172_800),
    }
    signal_time = datetime.fromisoformat(SIGNAL_AS_OF)
    states = []
    for timeframe in TIMEFRAME_ORDER:
        cell = cell_by_timeframe.get(timeframe)
        regime = "TREND"
        cell_policy = next(
            item
            for item in cell_policies
            if item["timeframe"] == timeframe and item["regime"] == regime
        )
        common = {
            "timeframe": timeframe,
            "latest_closed_bar_utc": (
                "2026-07-12T00:00:00+00:00"
                if timeframe == "D1"
                else "2026-07-13T19:55:00+00:00"
            ),
            "state_calculated_at_utc": SIGNAL_AS_OF,
            "regime": regime,
            "closed_bar_only": True,
            "partial_current_bar_used": False,
            "shared_runtime_path": True,
            "regime_state_receipt_sha256": _digest(f"regime-state-{timeframe}"),
            "feature_contract_sha256": _digest(f"feature-contract-{timeframe}"),
            "source_snapshot_sha256": _digest(f"live-source-{timeframe}"),
            "regime_params_sha256": regime_params_sha,
            "shared_regime_function_sha256": shared_regime_function,
            "calibration_receipt_sha256": cell_policy["calibration_receipt_sha256"],
            "runtime_calibration_bundle_sha256": runtime_calibration_bundle,
            "regime_probability_method": "OOS_CALIBRATED_PROBABILITY",
            "calibrated_regime_probability": "0.8",
            "current_regime_elapsed_seconds": 10_800,
            "shared_utility_function_sha256": shared_utility_function,
            "portfolio_state_snapshot_sha256": portfolio_sha,
            "parameter_level_oos_evidence": False,
        }
        if cell is None:
            states.append(
                {
                    **common,
                    "proposal_status": "NO_CURRENT_WINNER",
                    "current_refit_cell_id": None,
                    "direction": None,
                    "signal_evidence_sha256": None,
                    "expert_holding_horizon_seconds": None,
                    "holding_horizon_source_receipt_sha256": None,
                    "survival_horizon_end_utc": None,
                    "conditional_survival_probability": None,
                    "utility_calculation_receipt_sha256": None,
                    "conditional_net_r_utility": None,
                    "evidence_strength": None,
                    "evidence_strength_receipt_sha256": None,
                    "current_cost_snapshot_sha256": None,
                    "exposure_correlation_snapshot_sha256": None,
                    "order_state_opportunity_cost_snapshot_sha256": None,
                }
            )
            continue
        probability, survival, utility, strength, direction, holding = state_values[
            timeframe
        ]
        status = (
            "PROPOSED"
            if timeframe in proposed
            else "NO_SIGNAL"
            if timeframe in no_signal or timeframe not in proposed
            else "NO_SIGNAL"
        )
        horizon_end = (signal_time + timedelta(seconds=holding)).isoformat()
        expert = {
            **common,
            "proposal_status": status,
            "current_refit_cell_id": cell["current_refit_cell_id"],
            "signal_evidence_sha256": _digest(f"signal-evidence-{timeframe}"),
            "expert_holding_horizon_seconds": holding,
            "holding_horizon_source_receipt_sha256": _digest(
                f"holding-horizon-source-{timeframe}"
            ),
            "survival_horizon_end_utc": horizon_end,
            "conditional_survival_probability": survival,
            "calibrated_regime_probability": probability,
        }
        if status == "PROPOSED":
            expert.update(
                {
                    "direction": direction,
                    "utility_calculation_receipt_sha256": _digest(
                        f"utility-{timeframe}"
                    ),
                    "conditional_net_r_utility": utility,
                    "evidence_strength": strength,
                    "evidence_strength_receipt_sha256": _digest(
                        f"evidence-strength-{timeframe}"
                    ),
                    "current_cost_snapshot_sha256": _digest(f"cost-{timeframe}"),
                    "exposure_correlation_snapshot_sha256": _digest(
                        f"exposure-{timeframe}"
                    ),
                    "order_state_opportunity_cost_snapshot_sha256": _digest(
                        f"order-state-{timeframe}"
                    ),
                }
            )
        else:
            expert.update(
                {
                    "direction": None,
                    "utility_calculation_receipt_sha256": None,
                    "conditional_net_r_utility": None,
                    "evidence_strength": None,
                    "evidence_strength_receipt_sha256": None,
                    "current_cost_snapshot_sha256": None,
                    "exposure_correlation_snapshot_sha256": None,
                    "order_state_opportunity_cost_snapshot_sha256": None,
                }
            )
        states.append(expert)
    signal_payload = {
        "schema_version": SIGNAL_SNAPSHOT_RECEIPT_VERSION,
        "symbol": "EURUSD",
        "signal_as_of_utc": SIGNAL_AS_OF,
        "historical_process_decision_sha256": process_sha,
        "accepted_book_generation_receipt_sha256": book_receipt_sha,
        "current_refit_receipt_sha256": refit_receipt_sha,
        "market_snapshot_sha256": _digest("market-snapshot"),
        "market_data_freshness_evidence_sha256": _digest("market-freshness"),
        "market_data_current": True,
        "portfolio_state_snapshot_sha256": portfolio_sha,
        "portfolio_state_current": True,
        "timeframe_states": states,
    }
    signal_receipt = build_typed_receipt(
        signal_payload,
        receipt_type=SIGNAL_RECEIPT_TYPE,
    )
    request = {
        "contract_version": ARBITRATION_CONTRACT_VERSION,
        "current_cutoff_utc": CUTOFF,
        "historical_process_decision": process,
        "accepted_book_generation_receipt": book_receipt,
        "current_refit_receipt": refit_receipt,
        "policy_receipt": policy_receipt,
        "signal_snapshot_receipt": signal_receipt,
    }
    return {
        "request": request,
        "policy_payload": policy_payload,
        "signal_payload": signal_payload,
        "cells": cells,
    }


def _reseal_policy(context):
    context["request"]["policy_receipt"] = build_typed_receipt(
        context["policy_payload"],
        receipt_type=POLICY_RECEIPT_TYPE,
    )


def _reseal_signal(context):
    context["request"]["signal_snapshot_receipt"] = build_typed_receipt(
        context["signal_payload"],
        receipt_type=SIGNAL_RECEIPT_TYPE,
    )


def _state(context, timeframe):
    return next(
        item
        for item in context["signal_payload"]["timeframe_states"]
        if item["timeframe"] == timeframe
    )


def _cell_policy(context, timeframe, regime="TREND"):
    return next(
        item
        for item in context["policy_payload"]["cell_calibration_policies"]
        if item["timeframe"] == timeframe and item["regime"] == regime
    )


def _set_no_current_winner(state):
    state["proposal_status"] = "NO_CURRENT_WINNER"
    for name in (
        "current_refit_cell_id",
        "direction",
        "signal_evidence_sha256",
        "expert_holding_horizon_seconds",
        "holding_horizon_source_receipt_sha256",
        "survival_horizon_end_utc",
        "conditional_survival_probability",
        "utility_calculation_receipt_sha256",
        "conditional_net_r_utility",
        "evidence_strength",
        "evidence_strength_receipt_sha256",
        "current_cost_snapshot_sha256",
        "exposure_correlation_snapshot_sha256",
        "order_state_opportunity_cost_snapshot_sha256",
    ):
        state[name] = None


def test_contract_is_constructible_and_contains_no_fake_signing_or_postselection_tournament():
    snapshot = arbitration_contract_snapshot()
    assert (
        arbitration.CURRENT_REFIT_RECEIPT_VERSION
        == lifecycle.CURRENT_REFIT_RECEIPT_VERSION
    )
    assert snapshot["current_refit_receipt_version"] == CURRENT_REFIT_RECEIPT_VERSION
    assert snapshot["historical_current_winner_return_tournament"] is False
    assert snapshot["raw_regime_margin_or_dwell_ratio"] is False
    assert snapshot["magic_utility_epsilon"] is None
    assert snapshot["symmetric_signing_or_verification_keys"] is False
    assert (
        "verification_keys"
        not in inspect.signature(rank_specialist_surfaces).parameters
    )
    assert "signing_key" not in inspect.signature(build_typed_receipt).parameters
    context = _base_generation()
    assert "candidate_evidence" not in context["request"]
    assert "daily_net_returns" not in str(context["request"])


def test_m5_and_d1_advisories_can_coexist_with_conflicting_directions():
    context = _base_generation(proposed=("M5", "D1"))
    decision = rank_specialist_surfaces(context["request"])
    result = decision.to_dict()
    assert result["status"] == "ADVISORY_READY", result
    assert len(result["advisory_candidate_ids"]) == 2
    assert {item["structural_slot"] for item in result["candidate_advisories"]} >= {
        "D1_STRUCTURAL",
        "LOWER_STRUCTURAL",
    }
    proposed = [
        item for item in result["candidate_advisories"] if item["direction"] is not None
    ]
    assert {item["direction"] for item in proposed} == {"BUY", "SELL"}
    assert "D1_AND_LOWER_STRUCTURAL_SPECIALISTS_MAY_COEXIST" in result["reason_codes"]
    assert result["capital_authority"] is False
    assert result["order_authority"] is False
    assert result["funded_live_authority"] is False
    assert verify_arbitration_decision(decision, context["request"])


def test_lower_slot_uses_common_net_r_before_evidence_strength():
    context = _base_generation(proposed=("M5", "M15"))
    result = rank_specialist_surfaces(context["request"]).to_dict()
    lower = next(
        item
        for item in result["structural_allocations"]
        if item["structural_slot"] == "LOWER_STRUCTURAL"
    )
    m5 = next(cell for cell in context["cells"] if cell["timeframe"] == "M5")
    assert lower["advisory_candidate_id"] == m5["current_refit_cell_id"]
    assert lower["status"] == "MAXIMUM_COMMON_NET_R_ADVISORY"
    assert lower["evidence_strength_tie_break_used"] is False


def test_evidence_strength_is_only_exact_utility_tie_break_and_exact_double_tie_is_unresolved():
    context = _base_generation(proposed=("M5", "M15"))
    _state(context, "M15")["conditional_net_r_utility"] = "0.25"
    _reseal_signal(context)
    first = rank_specialist_surfaces(context["request"]).to_dict()
    lower = next(
        item
        for item in first["structural_allocations"]
        if item["structural_slot"] == "LOWER_STRUCTURAL"
    )
    m15 = next(cell for cell in context["cells"] if cell["timeframe"] == "M15")
    assert lower["advisory_candidate_id"] == m15["current_refit_cell_id"]
    assert lower["evidence_strength_tie_break_used"] is True

    _state(context, "M15")["evidence_strength"] = "0.7"
    _reseal_signal(context)
    tied = rank_specialist_surfaces(context["request"]).to_dict()
    lower = next(
        item
        for item in tied["structural_allocations"]
        if item["structural_slot"] == "LOWER_STRUCTURAL"
    )
    assert tied["status"] == "ADVISORY_INDETERMINATE"
    assert lower["advisory_candidate_id"] is None
    assert len(lower["exact_tied_candidate_ids"]) == 2
    assert lower["magic_utility_epsilon_used"] is False


def test_hierarchy_rejects_regime_then_survival_then_signal_then_net_r():
    regime = _base_generation(proposed=("M5",))
    _state(regime, "M5")["calibrated_regime_probability"] = "0.5"
    _reseal_signal(regime)
    assert (
        _state_reason(regime) == "REGIME_PROBABILITY_BELOW_PROCESS_VALIDATED_THRESHOLD"
    )

    survival = _base_generation(proposed=("M5",))
    _state(survival, "M5")["conditional_survival_probability"] = "0.5"
    _reseal_signal(survival)
    assert (
        _state_reason(survival)
        == "CONDITIONAL_SURVIVAL_BELOW_PROCESS_VALIDATED_THRESHOLD"
    )

    no_signal = _base_generation(proposed=(), no_signal=("M5",))
    assert _state_reason(no_signal) == "NO_ACTUAL_SIGNAL"

    net_r = _base_generation(proposed=("M5",))
    _state(net_r, "M5")["conditional_net_r_utility"] = "-0.01"
    _reseal_signal(net_r)
    assert _state_reason(net_r) == "NET_R_BELOW_PROCESS_VALIDATED_THRESHOLD"


def _state_reason(context, timeframe="M5"):
    result = rank_specialist_surfaces(context["request"]).to_dict()
    return next(
        item["eligibility_reason"]
        for item in result["timeframe_evaluations"]
        if item["timeframe"] == timeframe
    )


def test_thresholds_are_process_calibrated_policy_values_not_module_constants():
    context = _base_generation(proposed=("M5",))
    _cell_policy(context, "M5")["minimum_regime_probability"] = "0.9"
    _reseal_policy(context)
    result = rank_specialist_surfaces(context["request"]).to_dict()
    assert result["status"] == "NO_ACTION"
    assert (
        _state_reason(context) == "REGIME_PROBABILITY_BELOW_PROCESS_VALIDATED_THRESHOLD"
    )


def test_stale_cell_calibration_fails_closed_from_edge_decay_evidence():
    context = _base_generation(proposed=("M5",))
    policy = _cell_policy(context, "M5")
    policy["valid_through_utc"] = "2026-07-13T19:00:00+00:00"
    policy["retrain_due_utc"] = policy["valid_through_utc"]
    _reseal_policy(context)
    result = rank_specialist_surfaces(context["request"]).to_dict()
    assert result["status"] == "REJECTED"
    assert result["reason_codes"] == [
        "CURRENT_CALIBRATION_STALE_BY_EDGE_DECAY_EVIDENCE"
    ]


def test_shared_function_closed_bar_and_partial_bar_parity_fail_closed():
    wrong_function = _base_generation(proposed=("M5",))
    _state(wrong_function, "M5")["shared_regime_function_sha256"] = _digest(
        "second-path"
    )
    _reseal_signal(wrong_function)
    assert (
        rank_specialist_surfaces(wrong_function["request"]).to_dict()["status"]
        == "REJECTED"
    )

    partial = _base_generation(proposed=("M5",))
    _state(partial, "M5")["partial_current_bar_used"] = True
    _reseal_signal(partial)
    result = rank_specialist_surfaces(partial["request"]).to_dict()
    assert result["reason_codes"] == ["SIGNAL_RUNTIME_PARITY_OR_CLOSED_BAR_INVALID"]


def test_raw_margin_or_dwell_score_cannot_enter_exact_signal_schema():
    context = _base_generation(proposed=("M5",))
    _state(context, "M5")["raw_classifier_margin"] = "12"
    _reseal_signal(context)
    result = rank_specialist_surfaces(context["request"]).to_dict()
    assert result["status"] == "REJECTED"
    assert result["reason_codes"] == ["SIGNAL_STATE_SCHEMA_INVALID"]


def test_no_current_winner_is_honest_non_action_not_fabricated_no_trade():
    context = _base_generation(proposed=())
    result = rank_specialist_surfaces(context["request"]).to_dict()
    missing = next(
        item for item in result["timeframe_evaluations"] if item["timeframe"] == "M30"
    )
    assert missing["proposal_status"] == "NO_CURRENT_WINNER"
    assert missing["eligibility_reason"] == "NO_CURRENT_WINNER"
    assert missing["current_refit_cell_id"] is None


def test_no_current_winner_cannot_hide_a_cell_present_in_current_book():
    context = _base_generation(proposed=())
    _set_no_current_winner(_state(context, "M5"))
    _reseal_signal(context)
    result = rank_specialist_surfaces(context["request"]).to_dict()
    assert result["reason_codes"] == ["NO_CURRENT_WINNER_CONTRADICTS_CURRENT_BOOK"]


def test_cell_calibration_and_current_state_receipts_cannot_be_relabelled():
    policy_reuse = _base_generation()
    policies = policy_reuse["policy_payload"]["cell_calibration_policies"]
    policies[1]["calibration_receipt_sha256"] = policies[0][
        "calibration_receipt_sha256"
    ]
    _reseal_policy(policy_reuse)
    result = rank_specialist_surfaces(policy_reuse["request"]).to_dict()
    assert result["reason_codes"] == ["POLICY_CELL_CALIBRATION_RECEIPT_REUSED"]

    state_reuse = _base_generation(proposed=("M5", "M15"))
    _state(state_reuse, "M15")["utility_calculation_receipt_sha256"] = _state(
        state_reuse, "M5"
    )["utility_calculation_receipt_sha256"]
    _reseal_signal(state_reuse)
    result = rank_specialist_surfaces(state_reuse["request"]).to_dict()
    assert result["reason_codes"] == ["SIGNAL_UTILITY_RECEIPT_REUSED"]


def test_conditional_survival_horizon_binds_measured_expert_holding_period():
    context = _base_generation(proposed=("M5",))
    _state(context, "M5")["survival_horizon_end_utc"] = SIGNAL_AS_OF
    _reseal_signal(context)
    result = rank_specialist_surfaces(context["request"]).to_dict()
    assert result["reason_codes"] == ["SIGNAL_SURVIVAL_HORIZON_MISMATCH"]


def test_receipt_splices_current_cutoff_conflict_and_future_signal_fail_closed():
    book_tamper = _base_generation()
    book_tamper["request"]["accepted_book_generation_receipt"]["payload_sha256"] = (
        "0" * 64
    )
    assert (
        rank_specialist_surfaces(book_tamper["request"]).to_dict()["status"]
        == "REJECTED"
    )

    policy_type = _base_generation()
    policy_type["request"]["policy_receipt"]["receipt_type"] = "fxpm.wrong-type"
    assert (
        rank_specialist_surfaces(policy_type["request"]).to_dict()["status"]
        == "REJECTED"
    )

    cutoff = _base_generation()
    cutoff["request"]["current_cutoff_utc"] = "2026-07-12T00:00:00+00:00"
    assert rank_specialist_surfaces(cutoff["request"]).to_dict()["status"] == "REJECTED"

    future = _base_generation()
    future["signal_payload"]["signal_as_of_utc"] = "2099-07-13T20:00:00+00:00"
    _reseal_signal(future)
    result = rank_specialist_surfaces(future["request"]).to_dict()
    assert result["reason_codes"] == ["SIGNAL_TRUSTED_CLOCK_IN_FUTURE"]


def test_canonical_decimal_text_rejects_floats_and_noncanonical_trailing_zeros():
    floating = _base_generation(proposed=("M5",))
    _state(floating, "M5")["conditional_net_r_utility"] = 0.25
    _reseal_signal(floating)
    result = rank_specialist_surfaces(floating["request"]).to_dict()
    assert result["reason_codes"] == [
        "FLOAT_INPUT_FORBIDDEN_USE_CANONICAL_DECIMAL_TEXT"
    ]

    trailing = _base_generation(proposed=("M5",))
    _state(trailing, "M5")["conditional_net_r_utility"] = "0.250"
    _reseal_signal(trailing)
    result = rank_specialist_surfaces(trailing["request"]).to_dict()
    assert result["reason_codes"] == ["SIGNAL_NET_R_UTILITY_INVALID"]


def test_policy_requires_exact_24_cell_order_and_current_rerun_provenance():
    context = _base_generation()
    assert len(context["policy_payload"]["cell_calibration_policies"]) == 24
    context["policy_payload"]["cell_calibration_policies"][0:2] = reversed(
        context["policy_payload"]["cell_calibration_policies"][0:2]
    )
    _reseal_policy(context)
    result = rank_specialist_surfaces(context["request"]).to_dict()
    assert result["reason_codes"] == ["POLICY_CELL_CALIBRATION_ORDER_INVALID"]

    current = _base_generation()
    current["policy_payload"]["current_calibration_rerun"] = False
    _reseal_policy(current)
    result = rank_specialist_surfaces(current["request"]).to_dict()
    assert result["reason_codes"] == ["POLICY_AUTHORITY_OR_CALIBRATION_SCOPE_INVALID"]


def test_failed_causal_gate_cannot_enter_current_advisory():
    context = _base_generation()
    context["request"]["historical_process_decision"]["gates"]["primary_hac"] = False
    result = rank_specialist_surfaces(context["request"]).to_dict()
    assert result["status"] == "REJECTED"
    assert result["reason_codes"] == ["CURRENT_REFIT_LINEAGE_INVALID"]


def test_decision_replay_rejects_output_mutation_and_request_is_deterministic():
    context = _base_generation(proposed=("M5", "D1"))
    first = rank_specialist_surfaces(context["request"])
    second = rank_specialist_surfaces(copy.deepcopy(context["request"]))
    assert first.to_dict() == second.to_dict()
    tampered = first.to_dict()
    tampered["capital_authority"] = True
    assert not verify_arbitration_decision(tampered, context["request"])


def test_input_bounds_fail_closed_before_large_roster_work():
    context = _base_generation()
    context["request"]["oversized"] = [None] * 8_193
    result = rank_specialist_surfaces(context["request"]).to_dict()
    assert result["status"] == "REJECTED"
    assert result["reason_codes"] == ["INPUT_LIST_LIMIT_EXCEEDED"]
