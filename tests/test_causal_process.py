from __future__ import annotations

import copy
import hashlib
import inspect
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext

import pytest

import pm_causal_process as process
import pm_model_lifecycle as lifecycle


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _h(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _forward_lineage(plans: list[dict], precommit_sha256: str) -> dict:
    anchor_ids = [plan["anchor_id"] for plan in plans]
    roles = (
        "EVENT_LEDGER",
        "FILL_LEDGER",
        "MTM_LEDGER",
        "COST_LEDGER",
        "EXPOSURE_LEDGER",
        "MARGIN_LEDGER",
    )
    descriptors = {
        anchor_id: {
            role: {
                "schema_version": process.PINNED_AUTHORITY_ARTIFACT_DESCRIPTOR_SCHEMA,
                "relative_path": f"artifacts/{anchor_id}-{role.lower()}.json",
                "sha256": _h(f"forward-descriptor:{anchor_id}:{role}"),
                "size_bytes": 1,
                "format": process.PINNED_AUTHORITY_CANONICAL_JSON_FORMAT,
            }
            for role in roles
        }
        for anchor_id in anchor_ids
    }
    descriptor_roots = {
        anchor_id: _sha(
            {
                "schema": "fxpm.causal-forward-oos-anchor-descriptor-roster.v1",
                "anchor_id": anchor_id,
                "anchor_ordinal": ordinal,
                "ledger_descriptors": descriptors[anchor_id],
            }
        )
        for ordinal, anchor_id in enumerate(anchor_ids)
    }
    metadata = {
        anchor_id: {
            "schema": "fxpm.test-forward-anchor-production-metadata.v1",
            "anchor_id": anchor_id,
        }
        for anchor_id in anchor_ids
    }
    metadata_roots = {
        anchor_id: _sha(
            {
                "schema": "fxpm.causal-forward-oos-anchor-production-metadata.v1",
                "anchor_id": anchor_id,
                "anchor_ordinal": ordinal,
                "production_metadata": metadata[anchor_id],
            }
        )
        for ordinal, anchor_id in enumerate(anchor_ids)
    }
    return {
        "schema": process.FORWARD_OOS_REPLAY_LINEAGE_SCHEMA,
        "production_contract_version": (
            process.PINNED_FORWARD_OOS_PRODUCTION_CONTRACT_VERSION
        ),
        "producer_receipt_schema_version": (
            process.PINNED_FORWARD_OOS_PRODUCER_RECEIPT_SCHEMA
        ),
        "producer_receipt_sha256": _h("forward-producer-receipt"),
        "producer_receipt_descriptor_sha256": _h("forward-producer-receipt"),
        "forward_precommit_schema_version": (
            process.PINNED_FORWARD_OOS_PRECOMMIT_SCHEMA
        ),
        "forward_precommit_receipt_sha256": _h("forward-precommit"),
        "forward_precommit_descriptor_sha256": _h("forward-precommit"),
        "generation_id": "causal-fixture-generation",
        "generation_context_sha256": _h("generation-context"),
        "native_universe_sha256": _h("native-universe"),
        "config_sha256": _h("config"),
        "historical_precommit_receipt_sha256": _h("historical-precommit"),
        "historical_run_receipt_sha256": _h("historical-run"),
        "historical_run_payload_sha256": _h("historical-run-payload"),
        "causal_precommit_payload_sha256": precommit_sha256,
        "code_identity_sha256": _h("forward-code-identity"),
        "deployment_binding_sha256": _h("forward-deployment-binding"),
        "evidence_access_started_at_utc": "2026-07-13T10:02:00Z",
        "execution_started_at_utc": "2026-07-13T10:03:00Z",
        "execution_completed_at_utc": "2026-07-13T10:20:00Z",
        "reconciled_at_utc": "2026-07-13T10:22:00Z",
        "ordered_anchor_ids": anchor_ids,
        "ordered_anchor_descriptor_roster_sha256": _sha(
            [
                {
                    "anchor_id": anchor_id,
                    "anchor_descriptor_roster_sha256": descriptor_roots[anchor_id],
                }
                for anchor_id in anchor_ids
            ]
        ),
        "anchor_descriptor_roster_sha256_by_id": descriptor_roots,
        "anchor_ledger_descriptors_by_id": descriptors,
        "ordered_anchor_production_metadata_roster_sha256": _sha(
            [
                {
                    "anchor_id": anchor_id,
                    "anchor_production_metadata_sha256": metadata_roots[anchor_id],
                }
                for anchor_id in anchor_ids
            ]
        ),
        "anchor_production_metadata_sha256_by_id": metadata_roots,
        "anchor_production_metadata_by_id": metadata,
        "causal_binding_followup_completed": True,
        "confers_current_refit_capital_readiness_publication_or_trading_authority": False,
    }


def test_forward_oos_lineage_replays_nonlexicographic_anchor_order_and_exact_roles() -> (
    None
):
    plans = [
        {"anchor_id": value} for value in ("z-first", "a-second", "m-third", "b-fourth")
    ]
    precommit_sha = _h("nonlex-causal-precommit")
    lineage = _forward_lineage(plans, precommit_sha)
    reloaded = json.loads(_canonical(lineage).decode("utf-8"))
    assert (
        process._validate_forward_oos_replay_lineage(reloaded, plans, precommit_sha)
        == reloaded
    )

    attacked = copy.deepcopy(reloaded)
    del attacked["anchor_ledger_descriptors_by_id"]["a-second"]["COST_LEDGER"]
    with pytest.raises(process.ProcessVerificationError):
        process._validate_forward_oos_replay_lineage(attacked, plans, precommit_sha)


def _detector_parameters() -> dict[str, int | float]:
    return {
        "k_confirm": 3,
        "gap_min": 0.1,
        "k_hold": 5,
        "adx_period": 14,
        "atr_period": 14,
        "bb_period": 20,
        "bb_std": 2.0,
        "efficiency_period": 10,
        "bb_squeeze_lookback": 50,
        "bb_squeeze_percentile": 20.0,
        "atr_lookback": 100,
        "structure_period": 20,
        "structure_atr_mult": 0.1,
        "trend_adx_weight": 0.4,
        "trend_efficiency_weight": 0.35,
        "trend_slope_weight": 0.25,
        "range_adx_weight": 0.35,
        "range_efficiency_weight": 0.3,
        "range_containment_weight": 0.35,
        "breakout_squeeze_weight": 0.35,
        "breakout_atr_weight": 0.3,
        "breakout_structure_weight": 0.35,
        "chop_adx_weight": 0.3,
        "chop_efficiency_weight": 0.3,
        "chop_whipsaw_weight": 0.4,
        "adx_trend_threshold": 25.0,
    }


def _risk_policy() -> dict:
    holding = {"M5": 2, "M15": 3, "M30": 4, "H1": 7, "H4": 14, "D1": 30}
    return {
        "schema": "fxpm-historical-process-risk-policy-v1",
        "portfolio_return_basis": "SYNCHRONIZED_DAILY_NET_MTM_EQUITY_RETURN",
        "maximum_holding_calendar_days_by_timeframe": holding,
        "execution_overlap_allowance_calendar_days": 5,
        "minimum_purge_calendar_days": 35,
    }


def _cost_policy() -> dict:
    return {
        "schema": "fxpm-historical-process-cost-policy-v2",
        "base_lane": "GROSS_MTM_PLUS_OBSERVED_EXECUTION_COST_DEBITS",
        "stress_lane": "SAME_EVENTS_WITH_COST_DEBITS_NOT_LESS_SEVERE_THAN_BASE",
        "require_same_gross_event_stream": True,
        "require_daily_gross_cost_decomposition": True,
    }


def _regime_fit_policy() -> dict:
    return {
        "schema": "fxpm-historical-regime-fit-policy-v3",
        "fit_unit": "ONE_DECISION_PER_ANCHOR_SYMBOL_TIMEFRAME",
        "regime_fit_contract_version": process.PINNED_REGIME_FIT_CONTRACT_VERSION,
        "detector_record_schema_version": process.REGIME_DETECTOR_RECORD_SCHEMA_VERSION,
        "detector_ledger_record_schema_version": (
            process.REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION
        ),
        "exact_detector_parameters_embedded": True,
        "mutable_regime_parameter_sidecar_is_authority": False,
        "required_fit_decision_count_per_anchor": len(process.SYMBOLS)
        * len(process.TIMEFRAMES),
        "cell_reference_rule": (
            "ALL_FOUR_REGIME_CELLS_REFERENCE_ONE_REGIME_FIT_DECISION"
        ),
        "terminal_source_semantics": "ONE_SHARED_TERMINAL_NO_FIT_DECISION",
        "detector_candidate_roster_rule": (
            "PRECOMMITTED_BEFORE_NESTED_INNER_VALIDATION_ACCESS"
        ),
        "selected_controller_components": "FAMILY_FEATURES_AND_THRESHOLD_POLICY",
        "latent_parameter_fit_inputs": ("CAUSAL_MARKET_DATA_AND_MARKET_FEATURES_ONLY"),
        "latent_parameter_fit_strategy_utility_permitted": False,
        "controller_identifiability_gate": (
            "MODEL_FAMILY_DERIVED_EFFECTIVE_STATE_OBSERVATIONS_TRANSITIONS_AND_POSTERIOR_SEPARATION"
        ),
        "fixed_state_occupancy_floor_permitted": False,
        "possible_regime_slots_are_forced_observations": False,
        "regime_observation_scope": "EXACT_REGIME_LIVE_TRAIN_SUPPORT_RECEIPT",
        "regime_unobserved_rule": "EXACT_ZERO_TRAIN_LABEL_COUNT_ONLY",
        "regime_unobserved_attempt_count": 0,
        "regime_unobserved_winner_request_permitted": False,
        "expert_holding_horizon_support_veto_permitted": False,
        "rare_regime_no_local_winner_semantics": (
            "POSITIVE_TRAIN_LABEL_COUNT_USES_CELL_LOCAL_NO_LOCAL_WINNER_WITHOUT_GROUP_VETO"
        ),
        "positive_regime_insufficient_evidence_semantics": (
            "AUTHENTICATED_PRECOMMITTED_GEOMETRY_SHORTFALL_IS_COMPLETE_NON_ECONOMIC_ABSTENTION"
        ),
        "controller_selection_scope": (
            "PURGED_TIME_ORDERED_NESTED_INNER_VALIDATION_ONLY"
        ),
        "controller_selection_unit": (
            "ONE_SHARED_CONTROLLER_ACROSS_FOUR_REGIME_EXPERTS"
        ),
        "controller_selection_utility": (
            "SYNCHRONIZED_FOUR_EXPERT_POST_COST_LOG_GROWTH"
        ),
        "controller_selection_strategy_utility_permitted": True,
        "controller_selection_outer_oos_permitted": False,
        "controller_selection_calendar": (
            "FULL_SYNCHRONIZED_CALENDAR_INCLUDING_INACTIVE_DAYS_AND_ALL_FOUR_ROUTED_STATES"
        ),
        "collapsed_state_labels_permitted": False,
        "economic_unsupervised_utility_tie_break": "SIMPLER_BASELINE_CONTROLLER",
        "multiplicity_accounting_rule": (
            "ALL_DETECTOR_CANDIDATE_X_EXPERT_TRIAL_ATTEMPTS_IN_FOUR_FULL_ATTEMPT_LEDGERS"
        ),
        "outer_evidence_role": (
            "UNTOUCHED_JOINT_SHARED_CONTROLLER_AND_FOUR_EXPERTS_PROCESS_VALIDATION"
        ),
    }


def _dates(start_text: str, end_text: str) -> list[str]:
    start = date.fromisoformat(start_text)
    end = date.fromisoformat(end_text)
    return [
        (start + timedelta(days=index)).isoformat()
        for index in range((end - start).days + 1)
    ]


def _centered_returns(seed: int, count: int, mean: float, sigma: float) -> list[str]:
    generator = random.Random(seed)
    noise = [generator.gauss(0.0, sigma) for _ in range(count)]
    center = sum(noise) / count
    return [format(value - center + mean, ".12f") for value in noise]


def _centered_block_pattern(
    count: int, *, block: int, mean: float, sigma: float
) -> list[str]:
    raw = [
        mean + (sigma if (index // block) % 2 == 0 else -sigma)
        for index in range(count)
    ]
    center = sum(raw) / count
    return [format(value - center + mean, ".12f") for value in raw]


def _phase_centered_block_pattern(
    count: int,
    *,
    block: int,
    phase: int,
    mean: Decimal,
    amplitude: Decimal,
) -> list[str]:
    raw = [
        mean + (amplitude if ((index + phase) // block) % 2 == 0 else -amplitude)
        for index in range(count)
    ]
    center = sum(raw, Decimal(0)) / Decimal(count)
    return [format(value - center + mean, ".12f") for value in raw]


def _train_support(
    anchor_id: str,
    symbol: str,
    timeframe: str,
    counts: dict[str, int],
    cutoff: str,
) -> dict:
    eligible_rows = sum(counts.values())
    observed = [regime for regime in process.REGIMES if counts[regime] > 0]
    unobserved = [regime for regime in process.REGIMES if counts[regime] == 0]
    body = {
        "schema_version": process.REGIME_TRAIN_SUPPORT_SCHEMA_VERSION,
        "symbol": symbol,
        "timeframe": timeframe,
        "surface": "REGIME_LIVE",
        "warmup_policy": process.REGIME_TRAIN_SUPPORT_WARMUP_POLICY,
        "label_order": list(process.REGIMES),
        "eligible_rows": eligible_rows,
        "counts": counts,
        "observed_regimes": observed,
        "unobserved_regimes": unobserved,
        "support_state_by_regime": {
            regime: "EMPIRICALLY_OBSERVED" if counts[regime] > 0 else "UNOBSERVED"
            for regime in process.REGIMES
        },
        "first_ts": None if eligible_rows == 0 else "2018-01-01T00:00:00+00:00",
        "last_ts": (
            None
            if eligible_rows == 0
            else datetime.fromisoformat(cutoff.replace("Z", "+00:00")).isoformat()
        ),
        "vector_encoding": process.REGIME_TRAIN_SUPPORT_VECTOR_ENCODING,
        "vector_sha256": (
            hashlib.sha256(b"fxpm.regime-live-labels-nul/v1\x00").hexdigest()
            if eligible_rows == 0
            else _h(f"{anchor_id}:{symbol}:{timeframe}:REGIME_LIVE-vector:{counts}")
        ),
        "bindings": {
            "symbol": symbol,
            "timeframe": timeframe,
            "activation_id": f"{anchor_id}:{symbol}:{timeframe}:activation",
            "activation_manifest_sha256": _h(f"{anchor_id}:activation-manifest"),
            "source_sha256": _h(f"{anchor_id}:{symbol}:{timeframe}:native-source"),
            "train_window_sha256": _h(f"{anchor_id}:{symbol}:{timeframe}:train-window"),
            "params_sha256": _sha(_detector_parameters()),
            "detector_code_sha256": _h("regime-detector-code"),
        },
    }
    body["receipt_sha256"] = _sha(body)
    return body


def _detector_ledger_record(
    anchor_id: str,
    cutoff: str,
    symbol: str,
    timeframe: str,
    source_receipt_sha256: str,
    regime_fit_receipt_sha256: str,
    support: dict,
) -> dict:
    bindings = support["bindings"]
    parameters = _detector_parameters()
    parameters_text = _canonical(parameters).decode("ascii")
    record_body = {
        "schema_version": process.REGIME_DETECTOR_RECORD_SCHEMA_VERSION,
        "symbol": symbol,
        "timeframe": timeframe,
        "source_group_id": _sha(
            {
                "identity_contract": "FXPM_SYMBOL_TIMEFRAME_SOURCE_GROUP_V1",
                "symbol": symbol,
                "timeframe": timeframe,
            }
        ),
        "source_group_decision_sha256": source_receipt_sha256,
        "source_contract_sha256": _h(f"{anchor_id}:source-contract"),
        "source_group_evidence_sha256": _h(
            f"{anchor_id}:{symbol}:{timeframe}:source-evidence"
        ),
        "activation_id": bindings["activation_id"],
        "activation_manifest_sha256": bindings["activation_manifest_sha256"],
        "native_source_sha256": bindings["source_sha256"],
        "fit_cutoff_utc": datetime.fromisoformat(
            cutoff.replace("Z", "+00:00")
        ).isoformat(),
        "detector_version": "fxpm.market-regime-detector/v1",
        "detector_code_sha256": bindings["detector_code_sha256"],
        "parameters_canonical_json": parameters_text,
        "parameters_sha256": hashlib.sha256(
            parameters_text.encode("ascii")
        ).hexdigest(),
    }
    detector_record = {**record_body, "record_sha256": _sha(record_body)}
    ledger_body = {
        "schema_version": process.REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION,
        "regime_fit_contract_version": process.PINNED_REGIME_FIT_CONTRACT_VERSION,
        "regime_fit_receipt_sha256": regime_fit_receipt_sha256,
        "detector_record": detector_record,
        "detector_record_sha256": detector_record["record_sha256"],
    }
    return {**ledger_body, "ledger_record_sha256": _sha(ledger_body)}


def _refresh_detector_ledger_record(value: dict) -> None:
    record = value["detector_record"]
    record.pop("record_sha256", None)
    record["parameters_sha256"] = hashlib.sha256(
        record["parameters_canonical_json"].encode("ascii")
    ).hexdigest()
    record["record_sha256"] = _sha(record)
    value["detector_record_sha256"] = record["record_sha256"]
    value.pop("ledger_record_sha256", None)
    value["ledger_record_sha256"] = _sha(value)


def _cell(anchor_id: str, cutoff: str, index: int) -> dict:
    symbol, timeframe, regime = process.expected_cell_roster()[index]
    winner = index % 400 == 0
    status = "WINNER" if winner else process.NO_LOCAL_WINNER
    label_counts = {"TREND": 4, "RANGE": 3, "BREAKOUT": 2, "CHOP": 1}
    train_support = _train_support(anchor_id, symbol, timeframe, label_counts, cutoff)
    source_receipt = _h(f"{anchor_id}:{symbol}:{timeframe}:source")
    regime_receipt = _h(f"{anchor_id}:{symbol}:{timeframe}:regime-fit-decision")
    detector_record = _detector_ledger_record(
        anchor_id,
        cutoff,
        symbol,
        timeframe,
        source_receipt,
        regime_receipt,
        train_support,
    )
    decision = {
        "schema": "fxpm-anchor-local-cell-decision-v6",
        "anchor_id": anchor_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "selection_cutoff_utc": cutoff,
        "status": status,
        "source_state": "AVAILABLE",
        "source_terminal_reason": None,
        "source_receipt_sha256": source_receipt,
        "regime_receipt_sha256": regime_receipt,
        "regime_detector_ledger_record_sha256": detector_record["ledger_record_sha256"],
        "train_regime_support_receipt_sha256": train_support["receipt_sha256"],
        "horizon_receipt_sha256": _h(f"{anchor_id}:{index}:horizon"),
        "full_attempt_ledger_sha256": _h(f"{anchor_id}:{index}:attempts"),
        "state_decision_receipt_sha256": _h(f"{anchor_id}:{index}:state"),
        "insufficient_evidence_binding": None,
        "winner_selection_contract_version": (
            process.PINNED_WINNER_SELECTION_CONTRACT_VERSION
        ),
        "winner_evidence_ledger_contract_version": (
            process.PINNED_WINNER_EVIDENCE_LEDGER_VERSION
        ),
        "winner_request_sha256": _h(f"{anchor_id}:{index}:winner-request"),
        "winner_evidence_ledger_payload_sha256": _h(
            f"{anchor_id}:{index}:winner-evidence-v3"
        ),
        "training_feature_end_utc": cutoff,
        "training_label_end_utc": cutoff,
        "refit_market_data_end_utc": cutoff if winner else None,
        "strategy_code_sha256": _h(f"{anchor_id}:{index}:strategy") if winner else None,
        "trial_definition_sha256": _h(f"{anchor_id}:{index}:trial") if winner else None,
        "parameters_sha256": _h(f"{anchor_id}:{index}:parameters") if winner else None,
        "winner_decision_receipt_sha256": _h(f"{anchor_id}:{index}:winner-decision"),
        "anchor_refit_receipt_sha256": _h(f"{anchor_id}:{index}:refit")
        if winner
        else None,
    }
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": regime,
        "status": status,
        "search_complete": True,
        "attempts_terminal": True,
        "technical_failure": False,
        "terminal_source_unavailable": False,
        "attempt_count": 3,
        "selection_as_of_utc": cutoff,
        "market_data_end_utc": cutoff,
        "cell_decision": decision,
        "cell_decision_sha256": _sha(decision),
    }


def _set_group_regime_support(
    anchor: dict, offset: int, counts: dict[str, int]
) -> None:
    group = anchor["book_cells"][offset : offset + len(process.REGIMES)]
    symbol = group[0]["symbol"]
    timeframe = group[0]["timeframe"]
    support = _train_support(
        anchor["anchor_id"], symbol, timeframe, counts, anchor["selection_cutoff_utc"]
    )
    anchor["train_regime_supports"][offset // len(process.REGIMES)] = support
    for cell in group:
        decision = cell["cell_decision"]
        decision["train_regime_support_receipt_sha256"] = support["receipt_sha256"]
        if counts[cell["regime"]] == 0:
            cell["status"] = process.REGIME_UNOBSERVED
            cell["attempt_count"] = 0
            decision["status"] = process.REGIME_UNOBSERVED
            decision["refit_market_data_end_utc"] = None
            for key in (
                "horizon_receipt_sha256",
                "full_attempt_ledger_sha256",
                "strategy_code_sha256",
                "trial_definition_sha256",
                "parameters_sha256",
                "winner_request_sha256",
                "winner_evidence_ledger_payload_sha256",
                "winner_decision_receipt_sha256",
                "anchor_refit_receipt_sha256",
            ):
                decision[key] = None
            decision["winner_selection_contract_version"] = None
            decision["winner_evidence_ledger_contract_version"] = None
        cell["cell_decision_sha256"] = _sha(decision)
    _set_activity_ledger_from_vectors(anchor)


def _set_terminal_technical_group(anchor: dict, offset: int) -> None:
    group = anchor["book_cells"][offset : offset + len(process.REGIMES)]
    anchor["train_regime_supports"][offset // len(process.REGIMES)] = None
    anchor["regime_detector_records"][offset // len(process.REGIMES)] = None
    for cell in group:
        cell["status"] = process.TERMINAL_TECHNICAL
        cell["technical_failure"] = False
        cell["terminal_source_unavailable"] = False
        cell["attempt_count"] = 0
        cell["market_data_end_utc"] = None
        decision = cell["cell_decision"]
        decision["status"] = process.TERMINAL_TECHNICAL
        decision["source_state"] = process.TERMINAL_TECHNICAL_SOURCE_STATE
        decision["source_terminal_reason"] = process.TERMINAL_TECHNICAL_REASON
        decision["regime_detector_ledger_record_sha256"] = None
        decision["train_regime_support_receipt_sha256"] = None
        decision["training_feature_end_utc"] = None
        decision["training_label_end_utc"] = None
        decision["refit_market_data_end_utc"] = None
        decision["insufficient_evidence_binding"] = None
        for key in (
            "horizon_receipt_sha256",
            "full_attempt_ledger_sha256",
            "strategy_code_sha256",
            "trial_definition_sha256",
            "parameters_sha256",
            "winner_request_sha256",
            "winner_evidence_ledger_payload_sha256",
            "winner_decision_receipt_sha256",
            "anchor_refit_receipt_sha256",
        ):
            decision[key] = None
        decision["winner_selection_contract_version"] = None
        decision["winner_evidence_ledger_contract_version"] = None
        cell["cell_decision_sha256"] = _sha(decision)


def _set_insufficient_cell(anchor: dict, index: int, *, postsearch: bool) -> None:
    cell = anchor["book_cells"][index]
    decision = cell["cell_decision"]
    planned = 3
    attempts = planned if postsearch else 0
    stage = (
        process.INSUFFICIENT_POSTSEARCH_STAGE
        if postsearch
        else process.INSUFFICIENT_PRESEARCH_STAGE
    )
    reason = (
        process.INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON
        if postsearch
        else process.INSUFFICIENT_PRESEARCH_BOUNDARY_REASON
    )
    horizon_receipt = decision["horizon_receipt_sha256"]
    attempt_receipt = decision["full_attempt_ledger_sha256"] if postsearch else None
    binding = {
        "schema_version": process.INSUFFICIENT_EVIDENCE_BINDING_SCHEMA_VERSION,
        "terminal_stage": stage,
        "reason_code": reason,
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "source_prefix_receipt_sha256": _h(
            f"{anchor['anchor_id']}:{index}:source-prefix"
        ),
        "source_prefix_content_sha256": _h(
            f"{anchor['anchor_id']}:{index}:source-content"
        ),
        "source_prefix_rows": 50_000,
        "horizon_plan_receipt_sha256": horizon_receipt,
        "horizon_policy_release_sha256": _h("horizon-policy-release"),
        "dependency_policy_receipt_sha256": _h(
            f"{anchor['anchor_id']}:{index}:dependency-policy"
        ),
        "dependency_geometry_sha256": _h(
            f"{anchor['anchor_id']}:{index}:dependency-geometry"
        ),
        "candidate_registry_sha256": _h(
            f"{anchor['anchor_id']}:{index}:candidate-registry"
        ),
        "trial_registry_sha256": _h(f"{anchor['anchor_id']}:{index}:trial-registry"),
        "horizon_attempt_roster_sha256": _h(
            f"{anchor['anchor_id']}:{index}:horizon-attempts"
        ),
        "evidence_roster_sha256": _h(f"{anchor['anchor_id']}:{index}:evidence-roster"),
        "causal_fold_geometry_sha256": _h(
            f"{anchor['anchor_id']}:{index}:fold-geometry"
        ),
        "causal_fold_count": 4 if postsearch else 0,
        "planned_attempt_count": planned,
        "process_rejected_attempt_count": attempts,
        "process_rejection_request_sha256": (
            _h(f"{anchor['anchor_id']}:{index}:rejection-request")
            if postsearch
            else None
        ),
        "process_rejection_attempt_ledger_sha256": attempt_receipt,
        "process_rejection_evidence_ledger_sha256": (
            _h(f"{anchor['anchor_id']}:{index}:rejection-evidence")
            if postsearch
            else None
        ),
        "process_rejection_decision_sha256": (
            _h(f"{anchor['anchor_id']}:{index}:rejection-decision")
            if postsearch
            else None
        ),
        "horizon_winner_cross_binding_schema_version": (
            process.HORIZON_WINNER_CROSS_BINDING_SCHEMA_VERSION if postsearch else None
        ),
        "horizon_winner_cross_binding_content_sha256": (
            _h(f"{anchor['anchor_id']}:{index}:cross-binding") if postsearch else None
        ),
        "horizon_winner_cross_binding_mapping_sha256": (
            _h(f"{anchor['anchor_id']}:{index}:cross-mapping") if postsearch else None
        ),
        "winner_raw_production_schema_version": (
            process.WINNER_RAW_EVIDENCE_PRODUCTION_SCHEMA_VERSION
            if postsearch
            else None
        ),
        "winner_raw_production_receipt_sha256": (
            _h(f"{anchor['anchor_id']}:{index}:raw-production") if postsearch else None
        ),
    }
    cell["status"] = process.INSUFFICIENT_EVIDENCE
    cell["attempt_count"] = attempts
    decision["status"] = process.INSUFFICIENT_EVIDENCE
    decision["insufficient_evidence_binding"] = binding
    decision["full_attempt_ledger_sha256"] = attempt_receipt
    decision["winner_selection_contract_version"] = None
    decision["winner_evidence_ledger_contract_version"] = None
    decision["winner_request_sha256"] = None
    decision["winner_evidence_ledger_payload_sha256"] = None
    decision["strategy_code_sha256"] = None
    decision["trial_definition_sha256"] = None
    decision["parameters_sha256"] = None
    decision["winner_decision_receipt_sha256"] = None
    decision["anchor_refit_receipt_sha256"] = None
    decision["refit_market_data_end_utc"] = None
    cell["cell_decision_sha256"] = _sha(decision)
    _set_activity_ledger_from_vectors(anchor)


def _refresh_anchor_hashes(anchor: dict, *, derive_activity: bool = True) -> None:
    anchor["book_sha256"] = _sha(
        {
            "cells": anchor["book_cells"],
            "train_regime_supports": anchor["train_regime_supports"],
            "regime_detector_records": anchor["regime_detector_records"],
        }
    )
    anchor["component_outputs"]["exact_2400_cell_book_sha256"] = anchor["book_sha256"]
    anchor["calendar_sha256"] = _sha(
        {
            "calendar_semantics": "EVERY_UTC_DATE_INCLUSIVE",
            "dates": anchor["calendar_dates"],
        }
    )
    anchor["gross_vector_sha256"] = _sha(
        {
            "lane": "GROSS_MTM",
            "dates": anchor["calendar_dates"],
            "daily_values": anchor["daily_gross_mtm_returns"],
        }
    )
    anchor["base_cost_vector_sha256"] = _sha(
        {
            "lane": "BASE_COST_DEBIT",
            "dates": anchor["calendar_dates"],
            "daily_values": anchor["daily_base_cost_returns"],
        }
    )
    anchor["stress_cost_vector_sha256"] = _sha(
        {
            "lane": "STRESS_COST_DEBIT",
            "dates": anchor["calendar_dates"],
            "daily_values": anchor["daily_stress_cost_returns"],
        }
    )
    anchor["base_vector_sha256"] = _sha(
        {
            "lane": "BASE",
            "dates": anchor["calendar_dates"],
            "daily_values": anchor["daily_base_net_mtm_returns"],
        }
    )
    anchor["stress_vector_sha256"] = _sha(
        {
            "lane": "STRESS",
            "dates": anchor["calendar_dates"],
            "daily_values": anchor["daily_stress_net_mtm_returns"],
        }
    )
    if derive_activity:
        gross = [Decimal(value) for value in anchor["daily_gross_mtm_returns"]]
        base_cost = [Decimal(value) for value in anchor["daily_base_cost_returns"]]
        stress_cost = [Decimal(value) for value in anchor["daily_stress_cost_returns"]]
        (
            completed_outcomes,
            observed_holding,
            ledger_identities,
            risk_diagnostics,
            activity_sha256,
        ) = process._validate_activity_ledger(
            anchor,
            anchor["calendar_dates"],
            gross,
            base_cost,
            stress_cost,
        )
        anchor["activity_ledger_sha256"] = activity_sha256
        anchor["completed_trade_count"] = len(completed_outcomes)
        anchor["completed_trade_outcomes"] = [
            process._decimal_text(value) for value in completed_outcomes
        ]
        anchor["ledger_identities"] = ledger_identities
        anchor["risk_diagnostics"] = risk_diagnostics
        anchor["dependence_diagnostics"][
            "maximum_observed_holding_calendar_days_by_timeframe"
        ] = observed_holding
        anchor["dependence_diagnostics"]["maximum_observed_holding_calendar_days"] = (
            max(observed_holding.values())
        )
    anchor["trade_vector_sha256"] = _sha(
        {
            "lane": "COMPLETED_POSITION_NET_EQUITY_CONTRIBUTIONS_IN_FORWARD_WINDOW",
            "completed_position_net_equity_contributions": anchor[
                "completed_trade_outcomes"
            ],
        }
    )
    outputs = anchor["component_outputs"]
    outputs.update(process._expected_lineage_outputs(anchor))
    outputs["exact_2400_cell_book_sha256"] = anchor["book_sha256"]
    ledgers = anchor["ledger_identities"]
    risk_output = _sha(
        {
            "schema": "fxpm-anchor-risk-execution-v1",
            "anchor_id": anchor["anchor_id"],
            "risk_policy_sha256": _sha(_risk_policy()),
            "exposure_ledger_sha256": ledgers["exposure_ledger_sha256"],
            "open_position_ledger_sha256": ledgers["open_position_ledger_sha256"],
            "risk_diagnostics": anchor["risk_diagnostics"],
            "dependence_diagnostics": anchor["dependence_diagnostics"],
        }
    )
    cost_output = _sha(
        {
            "schema": "fxpm-anchor-cost-execution-v1",
            "anchor_id": anchor["anchor_id"],
            "cost_policy_sha256": _sha(_cost_policy()),
            "cost_ledger_sha256": ledgers["cost_ledger_sha256"],
            "gross_vector_sha256": anchor["gross_vector_sha256"],
            "base_cost_vector_sha256": anchor["base_cost_vector_sha256"],
            "stress_cost_vector_sha256": anchor["stress_cost_vector_sha256"],
            "base_vector_sha256": anchor["base_vector_sha256"],
            "stress_vector_sha256": anchor["stress_vector_sha256"],
        }
    )
    replay_output = _sha(
        {
            "schema": "fxpm-anchor-synchronized-replay-v1",
            "anchor_id": anchor["anchor_id"],
            "forward_oos_replay_anchor_lineage_sha256": anchor[
                "forward_oos_replay_anchor_lineage_sha256"
            ],
            "book_sha256": anchor["book_sha256"],
            "calendar_sha256": anchor["calendar_sha256"],
            "event_ledger_sha256": ledgers["event_ledger_sha256"],
            "fill_ledger_sha256": ledgers["fill_ledger_sha256"],
            "gross_vector_sha256": anchor["gross_vector_sha256"],
            "base_vector_sha256": anchor["base_vector_sha256"],
            "stress_vector_sha256": anchor["stress_vector_sha256"],
        }
    )
    outputs["risk_execution_sha256"] = risk_output
    outputs["cost_execution_sha256"] = cost_output
    outputs["synchronized_replay_sha256"] = replay_output
    lineage_outputs = process._expected_lineage_outputs(anchor)
    outputs["reconciliation_input_sha256"] = _sha(
        {
            "schema": "fxpm-anchor-reconciliation-input-v1",
            "anchor_id": anchor["anchor_id"],
            "lineage_outputs": lineage_outputs,
            "risk_execution_sha256": risk_output,
            "cost_execution_sha256": cost_output,
            "synchronized_replay_sha256": replay_output,
            "forward_oos_replay_anchor_lineage_sha256": anchor[
                "forward_oos_replay_anchor_lineage_sha256"
            ],
            "ledger_identities": ledgers,
        }
    )


def _set_net_vectors(anchor: dict, base: list[str], stress: list[str]) -> None:
    if len(base) != len(stress):
        raise AssertionError("base and stress fixtures must have equal length")
    gross: list[str] = []
    base_cost: list[str] = []
    stress_cost: list[str] = []
    normalized_base: list[str] = []
    normalized_stress: list[str] = []
    for base_text, stress_text in zip(base, stress, strict=True):
        base_value = Decimal(base_text)
        stress_value = Decimal(stress_text)
        if stress_value > base_value:
            raise AssertionError("stress fixture cannot exceed base")
        gross_value = base_value + Decimal("0.0002")
        gross.append(format(gross_value, ".12f"))
        base_cost.append("-0.000200000000")
        stress_cost.append(format(stress_value - gross_value, ".12f"))
        normalized_base.append(format(base_value, ".12f"))
        normalized_stress.append(format(stress_value, ".12f"))
    anchor["daily_gross_mtm_returns"] = gross
    anchor["daily_base_cost_returns"] = base_cost
    anchor["daily_stress_cost_returns"] = stress_cost
    anchor["daily_base_net_mtm_returns"] = normalized_base
    anchor["daily_stress_net_mtm_returns"] = normalized_stress
    _set_activity_ledger_from_vectors(anchor)


def _set_activity_ledger_from_vectors(anchor: dict) -> None:
    winner = next(cell for cell in anchor["book_cells"] if cell["status"] == "WINNER")
    opening_primitive_keys = (
        "position_id",
        "cell_decision_sha256",
        "timeframe",
        "opened_at_utc",
        "side",
        "quantity",
        "contract_multiplier",
        "stop_price",
        "broker_fill_id",
        "reference_mid_price",
        "reference_bid_price",
        "reference_ask_price",
        "base_fill_price",
        "stress_fill_price",
        "base_commission_cash",
        "stress_commission_cash",
        "base_fee_cash",
        "stress_fee_cash",
    )
    closing_primitive_keys = (
        "position_id",
        "closed_at_utc",
        "broker_fill_id",
        "reference_mid_price",
        "reference_bid_price",
        "reference_ask_price",
        "base_fill_price",
        "stress_fill_price",
        "base_commission_cash",
        "stress_commission_cash",
        "base_fee_cash",
        "stress_fee_cash",
    )
    equity = Decimal("1000")
    opening_mid = Decimal("100")
    days = []
    for index, date_text in enumerate(anchor["calendar_dates"]):
        position_id = f"{anchor['anchor_id']}:position:{index:04d}"
        base_cost_cash = -Decimal(anchor["daily_base_cost_returns"][index]) * equity
        stress_cost_cash = -Decimal(anchor["daily_stress_cost_returns"][index]) * equity
        base_half_cost = base_cost_cash / Decimal(2)
        stress_half_cost = stress_cost_cash / Decimal(2)
        mark_price = (
            opening_mid + Decimal(anchor["daily_gross_mtm_returns"][index]) * equity
        )
        entry = {
            "position_id": position_id,
            "cell_decision_sha256": winner["cell_decision_sha256"],
            "timeframe": winner["timeframe"],
            "opened_at_utc": f"{date_text}T00:01:00Z",
            "side": "LONG",
            "quantity": "1",
            "contract_multiplier": "1",
            "stop_price": "0.01",
            "broker_fill_id": f"{position_id}:open",
            "reference_mid_price": format(opening_mid, ".12f"),
            "reference_bid_price": format(opening_mid - Decimal("0.01"), ".12f"),
            "reference_ask_price": format(opening_mid + Decimal("0.01"), ".12f"),
            "base_fill_price": format(opening_mid + base_half_cost, ".12f"),
            "stress_fill_price": format(opening_mid + stress_half_cost, ".12f"),
            "base_commission_cash": "0",
            "stress_commission_cash": "0",
            "base_fee_cash": "0",
            "stress_fee_cash": "0",
        }
        entry["opening_fill_payload_sha256"] = _sha(
            {
                "schema": "fxpm-position-opening-fill-payload-v2",
                "anchor_id": anchor["anchor_id"],
                "fill": {key: entry[key] for key in opening_primitive_keys},
            }
        )
        entry["opening_fill_receipt_sha256"] = _h(f"{position_id}:opening-fill-receipt")
        mark = {
            "position_id": position_id,
            "marked_at_utc": f"{date_text}T23:57:00Z",
            "mark_price": format(mark_price, ".12f"),
            "mark_bid_price": format(mark_price - Decimal("0.01"), ".12f"),
            "mark_ask_price": format(mark_price + Decimal("0.01"), ".12f"),
            "base_financing_cash": "0",
            "stress_financing_cash": "0",
        }
        mark["market_mark_payload_sha256"] = _sha(
            {
                "schema": "fxpm-position-market-mark-payload-v2",
                "anchor_id": anchor["anchor_id"],
                "date": date_text,
                "position_id": position_id,
                "marked_at_utc": mark["marked_at_utc"],
                "mark_price": mark["mark_price"],
                "mark_bid_price": mark["mark_bid_price"],
                "mark_ask_price": mark["mark_ask_price"],
            }
        )
        mark["market_mark_receipt_sha256"] = _h(f"{position_id}:market-mark-receipt")
        exit_fill = {
            "position_id": position_id,
            "closed_at_utc": f"{date_text}T23:58:00Z",
            "broker_fill_id": f"{position_id}:close",
            "reference_mid_price": mark["mark_price"],
            "reference_bid_price": mark["mark_bid_price"],
            "reference_ask_price": mark["mark_ask_price"],
            "base_fill_price": format(mark_price - base_half_cost, ".12f"),
            "stress_fill_price": format(mark_price - stress_half_cost, ".12f"),
            "base_commission_cash": "0",
            "stress_commission_cash": "0",
            "base_fee_cash": "0",
            "stress_fee_cash": "0",
        }
        exit_fill["closing_fill_payload_sha256"] = _sha(
            {
                "schema": "fxpm-position-closing-fill-payload-v2",
                "anchor_id": anchor["anchor_id"],
                "fill": {key: exit_fill[key] for key in closing_primitive_keys},
            }
        )
        exit_fill["closing_fill_receipt_sha256"] = _h(
            f"{position_id}:closing-fill-receipt"
        )
        days.append(
            {
                "date": date_text,
                "equity_basis_cash": format(equity, ".12f"),
                "entries": [entry],
                "position_marks": [mark],
                "exits": [exit_fill],
            }
        )
    anchor["activity_ledger"] = {
        "schema": "fxpm-anchor-economic-position-activity-ledger-v2",
        "initial_positions": [],
        "days": days,
    }


def _refresh_opening_fill_payload(anchor: dict, fill: dict) -> None:
    keys = (
        "position_id",
        "cell_decision_sha256",
        "timeframe",
        "opened_at_utc",
        "side",
        "quantity",
        "contract_multiplier",
        "stop_price",
        "broker_fill_id",
        "reference_mid_price",
        "reference_bid_price",
        "reference_ask_price",
        "base_fill_price",
        "stress_fill_price",
        "base_commission_cash",
        "stress_commission_cash",
        "base_fee_cash",
        "stress_fee_cash",
    )
    fill["opening_fill_payload_sha256"] = _sha(
        {
            "schema": "fxpm-position-opening-fill-payload-v2",
            "anchor_id": anchor["anchor_id"],
            "fill": {key: fill[key] for key in keys},
        }
    )


def _refresh_closing_fill_payload(anchor: dict, fill: dict) -> None:
    keys = (
        "position_id",
        "closed_at_utc",
        "broker_fill_id",
        "reference_mid_price",
        "reference_bid_price",
        "reference_ask_price",
        "base_fill_price",
        "stress_fill_price",
        "base_commission_cash",
        "stress_commission_cash",
        "base_fee_cash",
        "stress_fee_cash",
    )
    fill["closing_fill_payload_sha256"] = _sha(
        {
            "schema": "fxpm-position-closing-fill-payload-v2",
            "anchor_id": anchor["anchor_id"],
            "fill": {key: fill[key] for key in keys},
        }
    )


def _anchor(plan: dict, index: int, forward_lineage: dict) -> dict:
    anchor_id = plan["anchor_id"]
    cutoff = plan["selection_cutoff_utc"]
    dates = _dates(plan["forward_start_date"], plan["forward_end_date"])
    base = _centered_returns(
        100 + index,
        len(dates),
        0.0012 + index * 0.00002,
        0.003,
    )
    stress = [format(Decimal(value) - Decimal("0.00015"), ".12f") for value in base]
    cells = [
        _cell(anchor_id, cutoff, cell_index) for cell_index in range(process.CELL_COUNT)
    ]
    support_roster = [
        _train_support(
            anchor_id,
            cells[offset]["symbol"],
            cells[offset]["timeframe"],
            {"TREND": 4, "RANGE": 3, "BREAKOUT": 2, "CHOP": 1},
            cutoff,
        )
        for offset in range(0, process.CELL_COUNT, len(process.REGIMES))
    ]
    detector_roster = [
        _detector_ledger_record(
            anchor_id,
            cutoff,
            cells[offset]["symbol"],
            cells[offset]["timeframe"],
            cells[offset]["cell_decision"]["source_receipt_sha256"],
            cells[offset]["cell_decision"]["regime_receipt_sha256"],
            support_roster[offset // len(process.REGIMES)],
        )
        for offset in range(0, process.CELL_COUNT, len(process.REGIMES))
    ]
    anchor = {
        "anchor_id": anchor_id,
        "selection_cutoff_utc": cutoff,
        "forward_start_date": plan["forward_start_date"],
        "forward_end_date": plan["forward_end_date"],
        "forward_oos_replay_anchor_lineage_sha256": (
            process._forward_oos_anchor_lineage_sha256(forward_lineage, anchor_id)
        ),
        "historical_selection_mode": "ANCHOR_LOCAL_AS_OF_ONLY",
        "cutoff_locality": {key: cutoff for key in process.CUTOFF_LOCALITY_KEYS},
        "component_outputs": {key: "" for key in process.ANCHOR_OUTPUT_KEYS},
        "book_cells": cells,
        "train_regime_supports": support_roster,
        "regime_detector_records": detector_roster,
        "book_sha256": "",
        "calendar_dates": dates,
        "calendar_sha256": "",
        "daily_gross_mtm_returns": [],
        "gross_vector_sha256": "",
        "daily_base_cost_returns": [],
        "base_cost_vector_sha256": "",
        "daily_stress_cost_returns": [],
        "stress_cost_vector_sha256": "",
        "daily_base_net_mtm_returns": [],
        "base_vector_sha256": "",
        "daily_stress_net_mtm_returns": [],
        "stress_vector_sha256": "",
        "activity_ledger": {},
        "activity_ledger_sha256": "",
        "completed_trade_count": 0,
        "completed_trade_outcomes": [],
        "trade_vector_sha256": "",
        "dependence_diagnostics": {
            "risk_policy_sha256": _sha(_risk_policy()),
            "inference_unit": "SYNCHRONIZED_DAILY_NET_MTM_EQUITY_RETURN",
            "maximum_observed_holding_calendar_days_by_timeframe": {
                timeframe: 0 for timeframe in process.TIMEFRAMES
            },
            "maximum_observed_holding_calendar_days": 0,
            "execution_overlap_allowance_calendar_days": 5,
            "holding_overlap_hac_floor_calendar_days": 34,
            "sample_size_hac_floor_calendar_days": 6,
            "primary_hac_bandwidth_calendar_days": 34,
            "sensitivity_hac_bandwidth_calendar_days": 68,
        },
        "risk_diagnostics": {},
        "ledger_identities": {
            key: _h(f"{anchor_id}:ledger:{key}") for key in process.LEDGER_IDENTITY_KEYS
        },
    }
    _set_net_vectors(anchor, base, stress)
    _refresh_anchor_hashes(anchor)
    return anchor


def _reconciliation_anchor(anchor: dict) -> dict:
    return {
        "anchor_id": anchor["anchor_id"],
        "evidence_anchor_sha256": _sha(anchor),
        "forward_oos_replay_anchor_lineage_sha256": anchor[
            "forward_oos_replay_anchor_lineage_sha256"
        ],
        "component_outputs": copy.deepcopy(anchor["component_outputs"]),
        "ledger_identities": copy.deepcopy(anchor["ledger_identities"]),
        "calendar_dates": copy.deepcopy(anchor["calendar_dates"]),
        "daily_gross_mtm_returns": copy.deepcopy(anchor["daily_gross_mtm_returns"]),
        "daily_base_cost_returns": copy.deepcopy(anchor["daily_base_cost_returns"]),
        "daily_stress_cost_returns": copy.deepcopy(anchor["daily_stress_cost_returns"]),
        "daily_base_net_mtm_returns": copy.deepcopy(
            anchor["daily_base_net_mtm_returns"]
        ),
        "daily_stress_net_mtm_returns": copy.deepcopy(
            anchor["daily_stress_net_mtm_returns"]
        ),
        "activity_ledger": copy.deepcopy(anchor["activity_ledger"]),
        "activity_ledger_sha256": anchor["activity_ledger_sha256"],
        "calendar_sha256": anchor["calendar_sha256"],
        "gross_vector_sha256": anchor["gross_vector_sha256"],
        "base_cost_vector_sha256": anchor["base_cost_vector_sha256"],
        "stress_cost_vector_sha256": anchor["stress_cost_vector_sha256"],
        "base_vector_sha256": anchor["base_vector_sha256"],
        "stress_vector_sha256": anchor["stress_vector_sha256"],
        "completed_trade_count": anchor["completed_trade_count"],
        "completed_trade_outcomes": copy.deepcopy(anchor["completed_trade_outcomes"]),
        "trade_vector_sha256": anchor["trade_vector_sha256"],
        "dependence_diagnostics": copy.deepcopy(anchor["dependence_diagnostics"]),
        "risk_diagnostics": copy.deepcopy(anchor["risk_diagnostics"]),
        "events_complete": True,
        "fills_complete": True,
        "costs_complete": True,
        "exposures_complete": True,
        "open_position_valuation_complete": True,
        "calendar_complete": True,
        "net_mtm_reconciled": True,
    }


@dataclass
class Corpus:
    precommit_payload: dict
    evidence_payload: dict
    reconciliation_payload: dict

    def refresh_reconciliation(self) -> None:
        self.reconciliation_payload = {
            "schema": process.RECONCILIATION_SCHEMA,
            "process_id": self.precommit_payload["process_id"],
            "precommit_payload_sha256": _sha(self.precommit_payload),
            "evidence_payload_sha256": _sha(self.evidence_payload),
            "process_definition_sha256": self.precommit_payload[
                "process_definition_sha256"
            ],
            "reconciled_at_utc": "2026-07-13T10:22:00Z",
            "forward_oos_replay_lineage": copy.deepcopy(
                self.evidence_payload["forward_oos_replay_lineage"]
            ),
            "anchors": [
                _reconciliation_anchor(anchor)
                for anchor in self.evidence_payload["anchors"]
            ],
        }


def _build_corpus() -> Corpus:
    plans = [
        {
            "anchor_id": "anchor-2019",
            "selection_cutoff_utc": "2018-11-27T00:00:00Z",
            "forward_start_date": "2019-01-01",
            "forward_end_date": "2019-12-31",
            "purge_calendar_days": 35,
        },
        {
            "anchor_id": "anchor-2021",
            "selection_cutoff_utc": "2020-11-27T00:00:00Z",
            "forward_start_date": "2021-01-01",
            "forward_end_date": "2021-12-31",
            "purge_calendar_days": 35,
        },
        {
            "anchor_id": "anchor-2023",
            "selection_cutoff_utc": "2022-11-27T00:00:00Z",
            "forward_start_date": "2023-01-01",
            "forward_end_date": "2023-12-31",
            "purge_calendar_days": 35,
        },
        {
            "anchor_id": "anchor-2024",
            "selection_cutoff_utc": "2024-01-02T00:00:00Z",
            "forward_start_date": "2024-02-06",
            "forward_end_date": "2025-02-04",
            "purge_calendar_days": 35,
        },
    ]
    definition = {
        "schema": "fxpm-frozen-historical-institution-process-v8",
        "symbols": list(process.SYMBOLS),
        "timeframes": list(process.TIMEFRAMES),
        "regimes": list(process.REGIMES),
        "cell_count": process.CELL_COUNT,
        "book_multiplicity_contract_version": (
            process.PINNED_BOOK_MULTIPLICITY_CONTRACT_VERSION
        ),
        "winner_selection_contract_version": (
            process.PINNED_WINNER_SELECTION_CONTRACT_VERSION
        ),
        "winner_evidence_ledger_version": (
            process.PINNED_WINNER_EVIDENCE_LEDGER_VERSION
        ),
        "regime_fit_contract_version": process.PINNED_REGIME_FIT_CONTRACT_VERSION,
        "regime_detector_record_schema_version": (
            process.REGIME_DETECTOR_RECORD_SCHEMA_VERSION
        ),
        "regime_detector_ledger_record_schema_version": (
            process.REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION
        ),
        "component_identities": {
            key: _h(f"component:{key}") for key in process.COMPONENT_IDENTITY_KEYS
        },
        "policy_identities": {
            key: (
                _sha(_risk_policy())
                if key == "risk_policy_sha256"
                else _sha(_cost_policy())
                if key == "cost_model_policy_sha256"
                else _sha(_regime_fit_policy())
                if key == "regime_fit_policy_sha256"
                else _h(f"policy:{key}")
            )
            for key in process.POLICY_IDENTITY_KEYS
        },
        "regime_fit_policy": _regime_fit_policy(),
        "risk_policy": _risk_policy(),
        "cost_policy": _cost_policy(),
        "anchor_plan": plans,
        "scope": "OFFLINE_PROCESS_VALIDATION_ONLY",
        "forbidden_authorities": [
            "CURRENT_REFIT",
            "MODEL_PROMOTION",
            "CAPITAL_ALLOCATION",
            "FUNDED_LIVE_TRADING",
            "LIVE_TRADING",
            "PROFITABILITY_CLAIM",
        ],
    }
    precommit_payload = {
        "schema": process.PRECOMMIT_SCHEMA,
        "process_id": "historical-institution-process-2026-07-13",
        "authorized_at_utc": "2026-07-13T10:00:00Z",
        "evidence_access_not_before_utc": "2026-07-13T10:01:00Z",
        "evidence_access_deadline_utc": "2026-07-13T12:00:00Z",
        "policy": process.pinned_process_policy(),
        "policy_sha256": _sha(process.pinned_process_policy()),
        "process_definition": definition,
        "process_definition_sha256": _sha(definition),
    }
    precommit_sha = _sha(precommit_payload)
    forward_lineage = _forward_lineage(plans, precommit_sha)
    evidence_payload = {
        "schema": process.EVIDENCE_SCHEMA,
        "process_id": precommit_payload["process_id"],
        "precommit_payload_sha256": precommit_sha,
        "process_definition_sha256": precommit_payload["process_definition_sha256"],
        "evidence_access_started_at_utc": "2026-07-13T10:02:00Z",
        "execution_started_at_utc": "2026-07-13T10:03:00Z",
        "execution_completed_at_utc": "2026-07-13T10:20:00Z",
        "forward_oos_replay_lineage": forward_lineage,
        "anchors": [
            _anchor(plan, index, forward_lineage) for index, plan in enumerate(plans)
        ],
    }
    reconciliation_payload = {
        "schema": process.RECONCILIATION_SCHEMA,
        "process_id": precommit_payload["process_id"],
        "precommit_payload_sha256": _sha(precommit_payload),
        "evidence_payload_sha256": _sha(evidence_payload),
        "process_definition_sha256": precommit_payload["process_definition_sha256"],
        "reconciled_at_utc": "2026-07-13T10:22:00Z",
        "forward_oos_replay_lineage": copy.deepcopy(forward_lineage),
        "anchors": [
            _reconciliation_anchor(anchor) for anchor in evidence_payload["anchors"]
        ],
    }
    return Corpus(
        precommit_payload,
        evidence_payload,
        reconciliation_payload,
    )


@pytest.fixture(scope="module")
def valid_corpus() -> Corpus:
    return _build_corpus()


def _owned(corpus: Corpus) -> Corpus:
    return Corpus(
        copy.deepcopy(corpus.precommit_payload),
        copy.deepcopy(corpus.evidence_payload),
        copy.deepcopy(corpus.reconciliation_payload),
    )


def _validate_anchor_book_only(anchor: dict) -> str:
    return process._validate_cell_book(
        anchor["book_cells"],
        anchor["train_regime_supports"],
        anchor["regime_detector_records"],
        anchor["selection_cutoff_utc"],
        anchor["anchor_id"],
        process._EvidenceIdentityRegistry(),
    )


def _evaluate(corpus: Corpus) -> process.HistoricalProcessDecision:
    return process.evaluate_historical_process(
        corpus.precommit_payload,
        corpus.evidence_payload,
        corpus.reconciliation_payload,
    )


def test_plain_exported_payloads_construct_and_2026_clocks_govern_old_cutoffs(
    valid_corpus,
) -> None:
    payloads = (
        valid_corpus.precommit_payload,
        valid_corpus.evidence_payload,
        valid_corpus.reconciliation_payload,
    )
    assert all(type(payload) is dict for payload in payloads)
    assert tuple(payload["schema"] for payload in payloads) == (
        process.PRECOMMIT_SCHEMA,
        process.EVIDENCE_SCHEMA,
        process.RECONCILIATION_SCHEMA,
    )
    decision = _evaluate(valid_corpus)
    body = decision.body()
    assert body["decision"] == "OFFLINE_PROCESS_VALIDATION_PASS"
    assert body["authority_scope"] == "OFFLINE_PROCESS_VALIDATION_ONLY"
    assert body["permitted_use"] == "OFFLINE_PROCESS_VALIDATION_EVIDENCE_INPUT"
    assert "external_replay_status" not in body
    assert body["evaluated_at_utc"].startswith("2026-")
    policy = process.pinned_process_policy()
    assert policy["book_multiplicity_contract_version"] == (
        "fxpm.book-multiplicity.v13"
    )
    assert policy["winner_selection_contract_version"] == ("fxpm.winner-selection.v13")
    assert policy["winner_evidence_ledger_version"] == (
        "fxpm.winner-evidence-ledger.v4"
    )
    assert policy["regime_fit_contract_version"] == "fxpm.historical-regime-fit.v4"
    assert policy["regime_detector_record_schema_version"] == (
        "fxpm.regime-detector-record/v1"
    )
    assert policy["validation_target"] == (
        "FROZEN_FULL_SELECTION_REFIT_ARBITRATION_PROCESS"
    )
    assert policy["outer_evidence_geometry"] == (
        "REPEATED_LATER_SYNCHRONIZED_AS_OF_FORWARD_ANCHORS"
    )
    assert policy["interior_holdout_permitted_use"] == (
        "INNER_SELECTION_ONLY_NEVER_FINAL_CAUSAL_PROOF"
    )
    assert valid_corpus.precommit_payload["process_definition"]["anchor_plan"][0][
        "selection_cutoff_utc"
    ].startswith("2018-")
    assert valid_corpus.precommit_payload["process_definition"]["anchor_plan"][-1][
        "selection_cutoff_utc"
    ].startswith("2024-")
    assert body["precommit_receipt_sha256"] == _sha(valid_corpus.precommit_payload)
    assert body["evidence_receipt_sha256"] == _sha(valid_corpus.evidence_payload)
    assert body["reconciliation_receipt_sha256"] == _sha(
        valid_corpus.reconciliation_payload
    )
    assert "CURRENT_REFIT" in body["explicit_non_authorities"]
    assert float(body["metrics"]["pooled_base_log_growth_mean_diagnostic"]) > 0
    assert body["confirmatory_inference_status"] == "PASS"
    variance_candidates = [
        Decimal(body["metrics"]["stress_log_growth_primary_hac_variance"]),
        Decimal(body["metrics"]["stress_log_growth_sensitivity_hac_variance"]),
        Decimal(body["metrics"]["stress_log_growth_anchor_cluster_variance"]),
        Decimal(body["metrics"]["stress_log_growth_daily_ips_variance"]),
    ]
    selected_variance = Decimal(body["metrics"]["stress_log_growth_selected_variance"])
    assert selected_variance == max(variance_candidates)
    assert selected_variance < sum(variance_candidates, Decimal(0))
    assert process.verify_historical_process_decision(
        decision,
        valid_corpus.precommit_payload,
        valid_corpus.evidence_payload,
        valid_corpus.reconciliation_payload,
    )
    assert "effective_dependence_blocks_diagnostic" not in body["metrics"]
    lifecycle_view = lifecycle._validate_historical_process_decision(
        body,
        verification_time=datetime.fromisoformat(
            body["evaluated_at_utc"].replace("Z", "+00:00")
        ),
    )
    assert lifecycle_view["process_id"] == body["process_id"]


@pytest.mark.parametrize("field", ["Platinum", "Palladium"])
def test_mixed_case_symbol_roster_is_exact(valid_corpus, field: str) -> None:
    corpus = _owned(valid_corpus)
    symbols = corpus.precommit_payload["process_definition"]["symbols"]
    symbols[symbols.index(field)] = field.upper()
    corpus.precommit_payload["process_definition_sha256"] = _sha(
        corpus.precommit_payload["process_definition"]
    )
    with pytest.raises(process.ProcessVerificationError, match="mixed-case"):
        _evaluate(corpus)


def test_fewer_than_four_anchors_is_rejected(valid_corpus) -> None:
    corpus = _owned(valid_corpus)
    corpus.precommit_payload["process_definition"]["anchor_plan"].pop()
    corpus.precommit_payload["process_definition_sha256"] = _sha(
        corpus.precommit_payload["process_definition"]
    )
    with pytest.raises(process.ProcessVerificationError, match="anchor count"):
        _evaluate(corpus)


@pytest.mark.parametrize("failure", ["embargo", "overlap"])
def test_anchor_embargo_and_overlap_are_rejected(valid_corpus, failure: str) -> None:
    corpus = _owned(valid_corpus)
    plan = corpus.precommit_payload["process_definition"]["anchor_plan"]
    if failure == "embargo":
        plan[1]["forward_start_date"] = "2020-12-29"
    else:
        plan[1]["selection_cutoff_utc"] = "2019-12-31T00:00:00Z"
        plan[1]["forward_start_date"] = "2019-12-31"
    corpus.precommit_payload["process_definition_sha256"] = _sha(
        corpus.precommit_payload["process_definition"]
    )
    with pytest.raises(process.ProcessVerificationError):
        _evaluate(corpus)


def test_phase_reversal_and_future_market_anchor_are_rejected(valid_corpus) -> None:
    reversed_phase = _owned(valid_corpus)
    reversed_phase.evidence_payload["evidence_access_started_at_utc"] = (
        "2026-07-13T09:59:00Z"
    )
    # Preserve the sealed Forward-v2 clock binding so this probe reaches the
    # independent authorization/access phase-order check it is intended to
    # exercise.  A mismatched forward receipt is already covered separately.
    reversed_phase.evidence_payload["forward_oos_replay_lineage"][
        "evidence_access_started_at_utc"
    ] = "2026-07-13T09:59:00Z"
    reversed_phase.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="phase clocks"):
        _evaluate(reversed_phase)

    future_market = _owned(valid_corpus)
    plan = future_market.precommit_payload["process_definition"]["anchor_plan"][-1]
    plan["selection_cutoff_utc"] = "2026-07-14T00:00:00Z"
    plan["forward_start_date"] = "2026-08-18"
    plan["forward_end_date"] = "2027-08-17"
    future_market.precommit_payload["process_definition_sha256"] = _sha(
        future_market.precommit_payload["process_definition"]
    )
    future_precommit_sha = _sha(future_market.precommit_payload)
    future_market.evidence_payload["precommit_payload_sha256"] = future_precommit_sha
    future_market.evidence_payload["forward_oos_replay_lineage"][
        "causal_precommit_payload_sha256"
    ] = future_precommit_sha
    future_market.evidence_payload["process_definition_sha256"] = (
        future_market.precommit_payload["process_definition_sha256"]
    )
    # Geometry is updated so only the real-vs-market access boundary rejects it.
    anchor = future_market.evidence_payload["anchors"][-1]
    for key in ("selection_cutoff_utc", "forward_start_date", "forward_end_date"):
        anchor[key] = plan[key]
    future_market.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError, match="must precede real evidence access"
    ):
        _evaluate(future_market)


@pytest.mark.parametrize("mode", ["missing", "duplicate"])
def test_exact_2400_cell_book_is_mandatory(valid_corpus, mode: str) -> None:
    corpus = _owned(valid_corpus)
    cells = corpus.evidence_payload["anchors"][0]["book_cells"]
    if mode == "missing":
        cells.pop()
    else:
        cells[-1] = copy.deepcopy(cells[-2])
    corpus.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="2400|roster"):
        _evaluate(corpus)


def test_terminal_source_unavailable_is_valid_but_technical_partial_is_not(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    anchor["train_regime_supports"][1] = None
    anchor["regime_detector_records"][1] = None
    for cell in anchor["book_cells"][4:8]:
        cell["status"] = "SOURCE_UNAVAILABLE"
        cell["terminal_source_unavailable"] = True
        cell["attempt_count"] = 0
        cell["market_data_end_utc"] = None
        decision = cell["cell_decision"]
        decision["status"] = "SOURCE_UNAVAILABLE"
        decision["source_state"] = "AUTHENTIC_TERMINAL_UNAVAILABLE"
        decision["source_terminal_reason"] = "BROKER_HISTORY_TERMINAL_ABSENCE"
        decision["training_feature_end_utc"] = None
        decision["training_label_end_utc"] = None
        decision["refit_market_data_end_utc"] = None
        decision["train_regime_support_receipt_sha256"] = None
        decision["regime_detector_ledger_record_sha256"] = None
        for key in (
            "horizon_receipt_sha256",
            "full_attempt_ledger_sha256",
            "strategy_code_sha256",
            "trial_definition_sha256",
            "parameters_sha256",
            "winner_request_sha256",
            "winner_evidence_ledger_payload_sha256",
            "winner_decision_receipt_sha256",
            "anchor_refit_receipt_sha256",
        ):
            decision[key] = None
        decision["winner_selection_contract_version"] = None
        decision["winner_evidence_ledger_contract_version"] = None
        cell["cell_decision_sha256"] = _sha(decision)
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    assert _evaluate(corpus).body()["decision"] == "OFFLINE_PROCESS_VALIDATION_PASS"

    technical = _owned(corpus)
    technical.evidence_payload["anchors"][0]["book_cells"][7]["technical_failure"] = (
        True
    )
    technical.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="technical"):
        _evaluate(technical)


def test_authenticated_terminal_technical_group_is_valid_distinct_lineage(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    _set_terminal_technical_group(anchor, 4)
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    decision = _evaluate(corpus)
    assert decision.body()["decision"] == "OFFLINE_PROCESS_VALIDATION_PASS"
    group = anchor["book_cells"][4:8]
    assert {cell["status"] for cell in group} == {process.TERMINAL_TECHNICAL}
    assert {cell["cell_decision"]["source_state"] for cell in group} == {
        process.TERMINAL_TECHNICAL_SOURCE_STATE
    }
    assert all(cell["technical_failure"] is False for cell in group)
    assert all(cell["terminal_source_unavailable"] is False for cell in group)

    relabeled = _owned(corpus)
    hostile = relabeled.evidence_payload["anchors"][0]
    for cell in hostile["book_cells"][4:8]:
        cell["cell_decision"]["source_state"] = "AUTHENTIC_TERMINAL_UNAVAILABLE"
        cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
    _refresh_anchor_hashes(hostile)
    relabeled.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="terminal-technical"):
        _evaluate(relabeled)


@pytest.mark.parametrize(
    ("field", "source_index", "target_index"),
    (
        ("horizon_receipt_sha256", 0, 1),
        ("full_attempt_ledger_sha256", 0, 1),
        ("winner_request_sha256", 0, 1),
        ("winner_evidence_ledger_payload_sha256", 0, 1),
        ("winner_decision_receipt_sha256", 0, 400),
    ),
)
def test_cell_specific_receipts_are_unique_within_each_anchor(
    valid_corpus,
    field: str,
    source_index: int,
    target_index: int,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    source = anchor["book_cells"][source_index]["cell_decision"]
    target = anchor["book_cells"][target_index]
    target["cell_decision"][field] = source[field]
    target["cell_decision_sha256"] = _sha(target["cell_decision"])
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError,
        match="reused",
    ):
        _evaluate(corpus)


def test_strategy_code_trial_and_parameter_values_may_repeat_across_cells(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    winners = [cell for cell in anchor["book_cells"] if cell["status"] == "WINNER"]
    source = winners[0]["cell_decision"]
    target_cell = winners[1]
    target = target_cell["cell_decision"]
    for key in (
        "strategy_code_sha256",
        "trial_definition_sha256",
        "parameters_sha256",
    ):
        target[key] = source[key]
    target_cell["cell_decision_sha256"] = _sha(target)
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    assert _evaluate(corpus).body()["decision"] == "OFFLINE_PROCESS_VALIDATION_PASS"


def test_insufficient_evidence_is_complete_nonteconomic_and_preserves_sibling_process(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    for ordinal, anchor in enumerate(corpus.evidence_payload["anchors"]):
        _set_insufficient_cell(anchor, 1, postsearch=bool(ordinal % 2))
        _refresh_anchor_hashes(anchor)
        assert _validate_anchor_book_only(anchor) == anchor["book_sha256"]
        assert anchor["book_cells"][0]["status"] == "WINNER"
        assert anchor["book_cells"][1]["status"] == process.INSUFFICIENT_EVIDENCE
    corpus.refresh_reconciliation()
    decision = _evaluate(corpus)
    assert decision.body()["decision"] == "OFFLINE_PROCESS_VALIDATION_PASS"
    policy = process.pinned_process_policy()
    assert policy["insufficient_evidence_is_complete_nontechnical_state"] is True
    assert policy["insufficient_evidence_is_non_economic_abstention"] is True
    assert policy["insufficient_evidence_does_not_veto_sibling_or_anchor"] is True


def test_insufficient_evidence_relabel_omission_and_binding_attacks_fail_closed(
    valid_corpus,
) -> None:
    base = copy.deepcopy(valid_corpus.evidence_payload["anchors"][0])
    _set_insufficient_cell(base, 1, postsearch=False)
    assert _validate_anchor_book_only(base)

    mutations: list[tuple[str, callable]] = [
        (
            "attempt-count",
            lambda anchor: anchor["book_cells"][1].update({"attempt_count": 1}),
        ),
        (
            "winner-leaf",
            lambda anchor: anchor["book_cells"][1]["cell_decision"].update(
                {"winner_request_sha256": _h("laundered-winner")}
            ),
        ),
        (
            "geometry",
            lambda anchor: anchor["book_cells"][1]["cell_decision"][
                "insufficient_evidence_binding"
            ].update({"causal_fold_count": 4}),
        ),
        (
            "economic-relabel",
            lambda anchor: (
                anchor["book_cells"][1].update({"status": process.NO_LOCAL_WINNER}),
                anchor["book_cells"][1]["cell_decision"].update(
                    {"status": process.NO_LOCAL_WINNER}
                ),
            ),
        ),
        (
            "source-relabel",
            lambda anchor: anchor["book_cells"][1]["cell_decision"].update(
                {"source_state": "AUTHENTIC_TERMINAL_UNAVAILABLE"}
            ),
        ),
        (
            "technical-laundering",
            lambda anchor: anchor["book_cells"][1].update({"technical_failure": True}),
        ),
    ]
    for _, mutate in mutations:
        attacked = copy.deepcopy(base)
        mutate(attacked)
        cell = attacked["book_cells"][1]
        cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
        with pytest.raises(process.ProcessVerificationError):
            _validate_anchor_book_only(attacked)

    omitted = copy.deepcopy(base)
    omitted["book_cells"].pop(1)
    with pytest.raises(process.ProcessVerificationError, match="exactly 2400"):
        _validate_anchor_book_only(omitted)

    reused = copy.deepcopy(base)
    _set_insufficient_cell(reused, 2, postsearch=False)
    reused_binding = reused["book_cells"][2]["cell_decision"][
        "insufficient_evidence_binding"
    ]
    reused_binding["source_prefix_receipt_sha256"] = reused["book_cells"][1][
        "cell_decision"
    ]["insufficient_evidence_binding"]["source_prefix_receipt_sha256"]
    reused["book_cells"][2]["cell_decision_sha256"] = _sha(
        reused["book_cells"][2]["cell_decision"]
    )
    with pytest.raises(process.ProcessVerificationError, match="reused"):
        _validate_anchor_book_only(reused)

    zero_support = copy.deepcopy(base)
    _set_group_regime_support(
        zero_support,
        0,
        {"TREND": 4, "RANGE": 0, "BREAKOUT": 2, "CHOP": 1},
    )
    zero_support["book_cells"][1]["cell_decision"]["horizon_receipt_sha256"] = _h(
        "zero-support-horizon"
    )
    _set_insufficient_cell(zero_support, 1, postsearch=False)
    with pytest.raises(
        process.ProcessVerificationError,
        match="insufficient-evidence cell terminal state is inconsistent",
    ):
        _validate_anchor_book_only(zero_support)


def test_cell_specific_identity_cannot_cross_receipt_domains(valid_corpus) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    source = anchor["book_cells"][0]["cell_decision"]
    target = anchor["book_cells"][1]
    target["cell_decision"]["horizon_receipt_sha256"] = source[
        "full_attempt_ledger_sha256"
    ]
    target["cell_decision_sha256"] = _sha(target["cell_decision"])
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError,
        match="semantic evidence identity is reused",
    ):
        _evaluate(corpus)


def test_source_receipt_is_shared_only_by_one_canonical_four_regime_group(
    valid_corpus,
) -> None:
    split = _owned(valid_corpus)
    anchor = split.evidence_payload["anchors"][0]
    cell = anchor["book_cells"][1]
    cell["cell_decision"]["source_receipt_sha256"] = _h("split-source-group")
    cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
    _refresh_anchor_hashes(anchor)
    split.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="not canonical"):
        _evaluate(split)

    cross_group = _owned(valid_corpus)
    anchor = cross_group.evidence_payload["anchors"][0]
    shared = anchor["book_cells"][0]["cell_decision"]["source_receipt_sha256"]
    for cell in anchor["book_cells"][4:8]:
        cell["cell_decision"]["source_receipt_sha256"] = shared
        cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
    _refresh_anchor_hashes(anchor)
    cross_group.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError,
        match="semantic evidence identity is reused|source or fit receipt is spliced",
    ):
        _evaluate(cross_group)

    cross_domain = _owned(valid_corpus)
    anchor = cross_domain.evidence_payload["anchors"][0]
    cell_specific = anchor["book_cells"][0]["cell_decision"]["horizon_receipt_sha256"]
    for cell in anchor["book_cells"][4:8]:
        cell["cell_decision"]["source_receipt_sha256"] = cell_specific
        cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
    _refresh_anchor_hashes(anchor)
    cross_domain.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError,
        match="semantic evidence identity is reused|source or fit receipt is spliced",
    ):
        _evaluate(cross_domain)


def test_complete_regime_fit_roster_binds_shared_nested_utility_selection(
    valid_corpus,
) -> None:
    expected_group_count = len(process.SYMBOLS) * len(process.TIMEFRAMES)
    for anchor in valid_corpus.evidence_payload["anchors"]:
        assert len(anchor["regime_detector_records"]) == expected_group_count
        groups = [
            anchor["book_cells"][offset : offset + len(process.REGIMES)]
            for offset in range(0, process.CELL_COUNT, len(process.REGIMES))
        ]
        assert len(groups) == expected_group_count == 600
        assert all(
            len({cell["cell_decision"]["regime_receipt_sha256"] for cell in group}) == 1
            for group in groups
        )
        assert all(
            len(
                {
                    cell["cell_decision"]["regime_detector_ledger_record_sha256"]
                    for cell in group
                }
            )
            == 1
            for group in groups
        )
    regime_policy = valid_corpus.precommit_payload["process_definition"][
        "regime_fit_policy"
    ]
    assert regime_policy["latent_parameter_fit_strategy_utility_permitted"] is False
    assert regime_policy["exact_detector_parameters_embedded"] is True
    assert regime_policy["mutable_regime_parameter_sidecar_is_authority"] is False
    assert regime_policy["controller_selection_strategy_utility_permitted"] is True
    assert regime_policy["controller_selection_utility"] == (
        "SYNCHRONIZED_FOUR_EXPERT_POST_COST_LOG_GROWTH"
    )
    assert regime_policy["controller_selection_outer_oos_permitted"] is False
    assert regime_policy["fixed_state_occupancy_floor_permitted"] is False
    assert regime_policy["possible_regime_slots_are_forced_observations"] is False
    assert regime_policy["regime_unobserved_rule"] == (
        "EXACT_ZERO_TRAIN_LABEL_COUNT_ONLY"
    )
    assert regime_policy["regime_unobserved_attempt_count"] == 0
    assert regime_policy["regime_unobserved_winner_request_permitted"] is False
    assert regime_policy["expert_holding_horizon_support_veto_permitted"] is False
    assert regime_policy["collapsed_state_labels_permitted"] is False
    assert regime_policy["controller_identifiability_gate"].startswith(
        "MODEL_FAMILY_DERIVED_EFFECTIVE_STATE_OBSERVATIONS_TRANSITIONS"
    )
    assert regime_policy["controller_selection_calendar"].startswith(
        "FULL_SYNCHRONIZED_CALENDAR_INCLUDING_INACTIVE_DAYS"
    )
    assert (
        regime_policy["economic_unsupervised_utility_tie_break"]
        == "SIMPLER_BASELINE_CONTROLLER"
    )
    assert regime_policy["multiplicity_accounting_rule"].startswith(
        "ALL_DETECTOR_CANDIDATE_X_EXPERT_TRIAL_ATTEMPTS"
    )

    independent = _owned(valid_corpus)
    anchor = independent.evidence_payload["anchors"][0]
    for index, cell in enumerate(anchor["book_cells"][4:8]):
        cell["cell_decision"]["regime_receipt_sha256"] = _h(
            f"independently-tuned-regime-controller:{index}"
        )
        cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
    _refresh_anchor_hashes(anchor)
    independent.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="shared RegimeFit"):
        _evaluate(independent)

    cross_group = _owned(valid_corpus)
    anchor = cross_group.evidence_payload["anchors"][0]
    reused_fit = anchor["book_cells"][0]["cell_decision"]["regime_receipt_sha256"]
    for cell in anchor["book_cells"][4:8]:
        cell["cell_decision"]["regime_receipt_sha256"] = reused_fit
        cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
    _refresh_anchor_hashes(anchor)
    cross_group.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError, match="semantic evidence identity is reused"
    ):
        _evaluate(cross_group)

    latent_leak = _owned(valid_corpus)
    definition = latent_leak.precommit_payload["process_definition"]
    regime_policy = definition["regime_fit_policy"]
    regime_policy["latent_parameter_fit_strategy_utility_permitted"] = True
    definition["policy_identities"]["regime_fit_policy_sha256"] = _sha(regime_policy)
    latent_leak.precommit_payload["process_definition_sha256"] = _sha(definition)
    with pytest.raises(process.ProcessVerificationError, match="nested-selection"):
        _evaluate(latent_leak)

    price_only_selection = _owned(valid_corpus)
    definition = price_only_selection.precommit_payload["process_definition"]
    regime_policy = definition["regime_fit_policy"]
    regime_policy["controller_selection_utility"] = (
        "PRICE_ONLY_SEPARATION_AND_PERSISTENCE"
    )
    definition["policy_identities"]["regime_fit_policy_sha256"] = _sha(regime_policy)
    price_only_selection.precommit_payload["process_definition_sha256"] = _sha(
        definition
    )
    with pytest.raises(process.ProcessVerificationError, match="nested-selection"):
        _evaluate(price_only_selection)

    hidden_multiplicity = _owned(valid_corpus)
    definition = hidden_multiplicity.precommit_payload["process_definition"]
    regime_policy = definition["regime_fit_policy"]
    regime_policy["multiplicity_accounting_rule"] = "SELECTED_DETECTOR_ONLY"
    definition["policy_identities"]["regime_fit_policy_sha256"] = _sha(regime_policy)
    hidden_multiplicity.precommit_payload["process_definition_sha256"] = _sha(
        definition
    )
    with pytest.raises(process.ProcessVerificationError, match="nested-selection"):
        _evaluate(hidden_multiplicity)

    fixed_occupancy = _owned(valid_corpus)
    definition = fixed_occupancy.precommit_payload["process_definition"]
    regime_policy = definition["regime_fit_policy"]
    regime_policy["fixed_state_occupancy_floor_permitted"] = True
    definition["policy_identities"]["regime_fit_policy_sha256"] = _sha(regime_policy)
    fixed_occupancy.precommit_payload["process_definition_sha256"] = _sha(definition)
    with pytest.raises(process.ProcessVerificationError, match="nested-selection"):
        _evaluate(fixed_occupancy)

    collapsed_labels = _owned(valid_corpus)
    definition = collapsed_labels.precommit_payload["process_definition"]
    regime_policy = definition["regime_fit_policy"]
    regime_policy["collapsed_state_labels_permitted"] = True
    definition["policy_identities"]["regime_fit_policy_sha256"] = _sha(regime_policy)
    collapsed_labels.precommit_payload["process_definition_sha256"] = _sha(definition)
    with pytest.raises(process.ProcessVerificationError, match="nested-selection"):
        _evaluate(collapsed_labels)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("numeric_alias", "parameter payload"),
        ("source_snapshot", "source or support splice"),
        ("cutoff", "not anchor-local"),
        ("version_relabel", "record hash mismatch"),
    ),
)
def test_regime_detector_record_rejects_hostile_mutations(
    valid_corpus, mutation: str, message: str
) -> None:
    anchor = valid_corpus.evidence_payload["anchors"][0]
    record = copy.deepcopy(anchor["regime_detector_records"][0])
    support = anchor["train_regime_supports"][0]
    if mutation == "numeric_alias":
        parameters = json.loads(record["detector_record"]["parameters_canonical_json"])
        parameters["k_confirm"] = True
        record["detector_record"]["parameters_canonical_json"] = _canonical(
            parameters
        ).decode("ascii")
        _refresh_detector_ledger_record(record)
    elif mutation == "source_snapshot":
        record["detector_record"]["native_source_sha256"] = _h("spliced-source")
        _refresh_detector_ledger_record(record)
    elif mutation == "cutoff":
        record["detector_record"]["fit_cutoff_utc"] = "2018-11-26T00:00:00+00:00"
        _refresh_detector_ledger_record(record)
    else:
        record["detector_record"]["detector_version"] = "relabelled"
    with pytest.raises(process.ProcessVerificationError, match=message):
        process._validate_regime_detector_ledger_record(
            record,
            anchor["book_cells"][0]["symbol"],
            anchor["book_cells"][0]["timeframe"],
            anchor["selection_cutoff_utc"],
            support,
        )


@pytest.mark.parametrize(
    ("field", "bad"),
    (
        ("k_confirm", 65),
        ("k_hold", 257),
        ("adx_period", 513),
        ("bb_squeeze_lookback", 10_001),
        ("gap_min", 1.0001),
        ("bb_squeeze_percentile", 100.1),
        ("adx_trend_threshold", -0.1),
        ("bb_std", 0.0),
        ("structure_atr_mult", 10.1),
        ("trend_adx_weight", 1.1),
        ("trend_adx_weight", 0.39),
        ("gap_min", float("inf")),
    ),
)
def test_causal_detector_parameter_domain_matches_direct_runtime(
    field: str, bad: int | float
) -> None:
    parameters = _detector_parameters()
    parameters[field] = bad
    with pytest.raises(process.ProcessVerificationError, match="regime detector"):
        process._validate_regime_detector_parameters(parameters)


def test_causal_detector_parameter_runtime_boundaries_are_accepted() -> None:
    parameters = _detector_parameters()
    parameters.update(process.REGIME_DETECTOR_INTEGER_MAXIMA)
    parameters.update(
        {
            "gap_min": 1.0,
            "bb_squeeze_percentile": 100.0,
            "adx_trend_threshold": 100.0,
            "bb_std": 10.0,
            "structure_atr_mult": 10.0,
        }
    )
    assert process._validate_regime_detector_parameters(parameters) == parameters


def test_regime_detector_record_omission_and_fit_receipt_splice_fail_closed(
    valid_corpus,
) -> None:
    omitted = _owned(valid_corpus)
    anchor = omitted.evidence_payload["anchors"][0]
    anchor["regime_detector_records"][0] = None
    _refresh_anchor_hashes(anchor)
    omitted.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError, match="immutable regime detector"
    ):
        _evaluate(omitted)

    spliced = _owned(valid_corpus)
    anchor = spliced.evidence_payload["anchors"][0]
    record = anchor["regime_detector_records"][1]
    record["regime_fit_receipt_sha256"] = _h("spliced-regime-fit")
    _refresh_detector_ledger_record(record)
    for cell in anchor["book_cells"][4:8]:
        cell["cell_decision"]["regime_detector_ledger_record_sha256"] = record[
            "ledger_record_sha256"
        ]
        cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
    _refresh_anchor_hashes(anchor)
    spliced.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError, match="fit receipt is spliced"
    ):
        _evaluate(spliced)


def test_rare_regime_no_local_winner_does_not_veto_shared_controller_group(
    valid_corpus,
) -> None:
    group = valid_corpus.evidence_payload["anchors"][0]["book_cells"][:4]
    assert [cell["status"] for cell in group] == [
        "WINNER",
        process.NO_LOCAL_WINNER,
        process.NO_LOCAL_WINNER,
        process.NO_LOCAL_WINNER,
    ]
    assert len({cell["cell_decision"]["regime_receipt_sha256"] for cell in group}) == 1
    support = valid_corpus.evidence_payload["anchors"][0]["train_regime_supports"][0]
    assert support["counts"]["CHOP"] == 1
    assert _evaluate(valid_corpus).body()["decision"] == (
        "OFFLINE_PROCESS_VALIDATION_PASS"
    )


@pytest.mark.parametrize(
    ("field", "stale"),
    (
        ("winner_selection_contract_version", "fxpm.winner-selection.v10"),
        (
            "winner_evidence_ledger_contract_version",
            "fxpm.winner-evidence-ledger.v2",
        ),
    ),
)
def test_stale_winner_contract_cannot_enter_causal_book(
    valid_corpus, field: str, stale: str
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    target = anchor["book_cells"][1]
    target["cell_decision"][field] = stale
    target["cell_decision_sha256"] = _sha(target["cell_decision"])
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="contract pin"):
        _evaluate(corpus)


def test_no_local_winner_requires_exact_v11_receipt_chain(valid_corpus) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    target = anchor["book_cells"][1]
    target["cell_decision"]["winner_evidence_ledger_payload_sha256"] = None
    target["cell_decision_sha256"] = _sha(target["cell_decision"])
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError, match="SHA-256|unsupported JSON"
    ):
        _evaluate(corpus)


@pytest.mark.parametrize("hostile", [True, 1, 1.0, "0" * 63])
def test_winner_receipt_chain_rejects_bool_numeric_and_malformed_values(
    valid_corpus, hostile
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    target = anchor["book_cells"][1]
    target["cell_decision"]["winner_request_sha256"] = hostile
    target["cell_decision_sha256"] = _sha(target["cell_decision"])
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError, match="SHA-256|unsupported JSON"
    ):
        _evaluate(corpus)


@pytest.mark.parametrize(
    ("field", "stale"),
    (
        ("winner_selection_contract_version", "fxpm.winner-selection.v10"),
        ("regime_fit_contract_version", "fxpm.historical-regime-fit.v2"),
        ("regime_detector_record_schema_version", "fxpm.regime-detector-record/v0"),
        (
            "regime_detector_ledger_record_schema_version",
            "fxpm.regime-detector-ledger-record/v0",
        ),
    ),
)
def test_process_definition_cannot_relabel_book_winner_or_regime_contracts(
    valid_corpus, field: str, stale: str
) -> None:
    corpus = _owned(valid_corpus)
    definition = corpus.precommit_payload["process_definition"]
    definition[field] = stale
    corpus.precommit_payload["process_definition_sha256"] = _sha(definition)
    with pytest.raises(process.ProcessVerificationError, match="contract pin"):
        _evaluate(corpus)


def test_exact_zero_train_labels_create_complete_unobserved_possible_slot(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    _set_group_regime_support(
        anchor,
        0,
        {"TREND": 4, "RANGE": 3, "BREAKOUT": 2, "CHOP": 0},
    )
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    assert _evaluate(corpus).body()["decision"] == "OFFLINE_PROCESS_VALIDATION_PASS"
    absent = anchor["book_cells"][3]
    assert len(anchor["book_cells"]) == process.CELL_COUNT
    assert absent["status"] == process.REGIME_UNOBSERVED
    assert absent["attempt_count"] == 0
    assert absent["technical_failure"] is False
    assert absent["terminal_source_unavailable"] is False
    assert absent["cell_decision"]["horizon_receipt_sha256"] is None
    assert absent["cell_decision"]["winner_selection_contract_version"] is None
    assert absent["cell_decision"]["winner_request_sha256"] is None
    assert absent["cell_decision"]["winner_evidence_ledger_payload_sha256"] is None
    assert absent["cell_decision"]["winner_decision_receipt_sha256"] is None
    assert absent["cell_decision"]["anchor_refit_receipt_sha256"] is None
    assert (
        absent["cell_decision"]["regime_detector_ledger_record_sha256"]
        == (anchor["regime_detector_records"][0]["ledger_record_sha256"])
    )


def test_all_zero_train_support_is_a_complete_unobserved_group(valid_corpus) -> None:
    anchor = _owned(valid_corpus).evidence_payload["anchors"][0]
    _set_group_regime_support(
        anchor,
        0,
        {"TREND": 0, "RANGE": 0, "BREAKOUT": 0, "CHOP": 0},
    )

    assert len(_validate_anchor_book_only(anchor)) == 64
    support = anchor["train_regime_supports"][0]
    assert support["eligible_rows"] == 0
    assert support["observed_regimes"] == []
    assert support["first_ts"] is None
    assert support["last_ts"] is None
    assert support["vector_sha256"] == hashlib.sha256(
        b"fxpm.regime-live-labels-nul/v1\x00"
    ).hexdigest()
    group = anchor["book_cells"][: len(process.REGIMES)]
    assert all(cell["status"] == process.REGIME_UNOBSERVED for cell in group)
    assert all(cell["attempt_count"] == 0 for cell in group)
    assert all(
        cell["cell_decision"]["winner_request_sha256"] is None
        and cell["cell_decision"]["winner_decision_receipt_sha256"] is None
        and cell["cell_decision"]["anchor_refit_receipt_sha256"] is None
        for cell in group
    )

    support["first_ts"] = anchor["selection_cutoff_utc"]
    support.pop("receipt_sha256")
    support["receipt_sha256"] = _sha(support)
    with pytest.raises(
        process.ProcessVerificationError,
        match="empty train regime support timestamps",
    ):
        _validate_anchor_book_only(anchor)


def test_unobserved_slot_requires_exact_support_and_cannot_be_omitted(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    _set_group_regime_support(
        anchor,
        0,
        {"TREND": 4, "RANGE": 3, "BREAKOUT": 2, "CHOP": 0},
    )
    anchor["train_regime_supports"][0] = None
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="support"):
        _evaluate(corpus)

    omitted = _owned(valid_corpus)
    omitted.evidence_payload["anchors"][0]["book_cells"].pop(3)
    omitted.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="2400"):
        _evaluate(omitted)


def test_rehashed_positive_support_cannot_relabel_unobserved_slot(valid_corpus) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    _set_group_regime_support(
        anchor,
        0,
        {"TREND": 4, "RANGE": 3, "BREAKOUT": 2, "CHOP": 0},
    )
    group = anchor["book_cells"][:4]
    support = copy.deepcopy(anchor["train_regime_supports"][0])
    support["counts"]["TREND"] = 3
    support["counts"]["CHOP"] = 1
    support["observed_regimes"] = list(process.REGIMES)
    support["unobserved_regimes"] = []
    support["support_state_by_regime"]["CHOP"] = "EMPIRICALLY_OBSERVED"
    support["vector_sha256"] = _h("relabelled-positive-support-vector")
    support.pop("receipt_sha256")
    support["receipt_sha256"] = _sha(support)
    anchor["train_regime_supports"][0] = support
    for cell in group:
        decision = cell["cell_decision"]
        decision["train_regime_support_receipt_sha256"] = support["receipt_sha256"]
        cell["cell_decision_sha256"] = _sha(decision)
    _set_activity_ledger_from_vectors(anchor)
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="unobserved"):
        _evaluate(corpus)


def test_winner_receipt_cannot_be_laundered_as_an_opening_fill_receipt(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    winner_receipt = anchor["book_cells"][0]["cell_decision"][
        "winner_decision_receipt_sha256"
    ]
    opening_fill = anchor["activity_ledger"]["days"][0]["entries"][0]
    opening_fill["opening_fill_receipt_sha256"] = winner_receipt
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError, match="semantic evidence identity is reused"
    ):
        _evaluate(corpus)


@pytest.mark.parametrize(
    "cell_receipt_key",
    (
        "regime_receipt_sha256",
        "horizon_receipt_sha256",
        "full_attempt_ledger_sha256",
    ),
)
def test_artifact_receipt_cannot_be_laundered_as_market_mark_provenance(
    valid_corpus,
    cell_receipt_key: str,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    mark_receipt = anchor["activity_ledger"]["days"][0]["position_marks"][0][
        "market_mark_receipt_sha256"
    ]
    target = anchor["book_cells"][1]
    targets = (
        anchor["book_cells"][4:8]
        if cell_receipt_key == "regime_receipt_sha256"
        else [target]
    )
    for target_cell in targets:
        target_cell["cell_decision"][cell_receipt_key] = mark_receipt
        target_cell["cell_decision_sha256"] = _sha(target_cell["cell_decision"])
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError,
        match="semantic evidence identity is reused|source or fit receipt is spliced",
    ):
        _evaluate(corpus)


def test_canonical_source_receipt_cannot_be_laundered_as_fill_provenance(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    opening_receipt = anchor["activity_ledger"]["days"][0]["entries"][0][
        "opening_fill_receipt_sha256"
    ]
    for cell in anchor["book_cells"][4:8]:
        cell["cell_decision"]["source_receipt_sha256"] = opening_receipt
        cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError,
        match="semantic evidence identity is reused|source or fit receipt is spliced",
    ):
        _evaluate(corpus)


@pytest.mark.parametrize(
    ("identity_container", "identity_key"),
    (
        ("top_level", "precommit_payload_sha256"),
        ("immutable_definition", "process_definition_sha256"),
        ("component_outputs", "source_manifest_sha256"),
        ("ledger_identities", "calendar_completeness_sha256"),
        ("anchor", "gross_vector_sha256"),
    ),
)
def test_fill_receipt_cannot_reuse_top_level_or_derived_evidence_identity(
    valid_corpus,
    identity_container: str,
    identity_key: str,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    if identity_container == "top_level":
        reused_identity = _sha(corpus.precommit_payload)
    elif identity_container == "immutable_definition":
        reused_identity = corpus.precommit_payload[identity_key]
    elif identity_container == "anchor":
        reused_identity = anchor[identity_key]
    else:
        reused_identity = anchor[identity_container][identity_key]
    anchor["activity_ledger"]["days"][0]["entries"][0][
        "opening_fill_receipt_sha256"
    ] = reused_identity
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError, match="semantic evidence identity is reused"
    ):
        _evaluate(corpus)


def test_repeatable_strategy_content_cannot_be_laundered_as_fill_provenance(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    strategy_identity = anchor["book_cells"][0]["cell_decision"]["strategy_code_sha256"]
    anchor["activity_ledger"]["days"][0]["entries"][0][
        "opening_fill_receipt_sha256"
    ] = strategy_identity
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError, match="semantic evidence identity is reused"
    ):
        _evaluate(corpus)


@pytest.mark.parametrize(
    ("source_key", "target_key"),
    (
        ("strategy_code_sha256", "parameters_sha256"),
        ("trial_definition_sha256", "strategy_code_sha256"),
    ),
)
def test_repeatable_specialist_content_cannot_cross_incompatible_domains(
    valid_corpus,
    source_key: str,
    target_key: str,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    winner = anchor["book_cells"][400]
    winner["cell_decision"][target_key] = winner["cell_decision"][source_key]
    winner["cell_decision_sha256"] = _sha(winner["cell_decision"])
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError, match="semantic evidence identity is reused"
    ):
        _evaluate(corpus)


def test_component_executable_content_cannot_alias_specialist_strategy_content(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    component_identity = next(
        iter(
            corpus.precommit_payload["process_definition"][
                "component_identities"
            ].values()
        )
    )
    anchor = corpus.evidence_payload["anchors"][0]
    winner = anchor["book_cells"][400]
    winner["cell_decision"]["strategy_code_sha256"] = component_identity
    winner["cell_decision_sha256"] = _sha(winner["cell_decision"])
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError, match="semantic evidence identity is reused"
    ):
        _evaluate(corpus)


def test_reconciliation_anchor_identity_cannot_be_laundered_as_fill_provenance(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    reconciliation_identity = corpus.reconciliation_payload["anchors"][0][
        "evidence_anchor_sha256"
    ]
    anchor = corpus.evidence_payload["anchors"][1]
    anchor["activity_ledger"]["days"][0]["entries"][0][
        "opening_fill_receipt_sha256"
    ] = reconciliation_identity
    _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError, match="semantic evidence identity is reused"
    ):
        _evaluate(corpus)


def test_current_survivor_and_anchor_receipt_reuse_are_rejected(valid_corpus) -> None:
    relabel = _owned(valid_corpus)
    relabel.evidence_payload["anchors"][0]["historical_selection_mode"] = (
        "CURRENT_SURVIVOR_RELABELED"
    )
    relabel.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="current-survivor"):
        _evaluate(relabel)

    reused = _owned(valid_corpus)
    first = reused.evidence_payload["anchors"][0]["book_cells"][0]["cell_decision"]
    later_cell = reused.evidence_payload["anchors"][1]["book_cells"][0]
    later_cell["cell_decision"]["state_decision_receipt_sha256"] = first[
        "state_decision_receipt_sha256"
    ]
    later_cell["cell_decision"]["winner_decision_receipt_sha256"] = first[
        "winner_decision_receipt_sha256"
    ]
    later_cell["cell_decision"]["anchor_refit_receipt_sha256"] = first[
        "anchor_refit_receipt_sha256"
    ]
    later_cell["cell_decision_sha256"] = _sha(later_cell["cell_decision"])
    _refresh_anchor_hashes(reused.evidence_payload["anchors"][1], derive_activity=False)
    reused.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="reused"):
        _evaluate(reused)

    activity_reuse = _owned(valid_corpus)
    first_fill = activity_reuse.evidence_payload["anchors"][0]["activity_ledger"][
        "days"
    ][0]["entries"][0]
    later_anchor = activity_reuse.evidence_payload["anchors"][1]
    later_fill = later_anchor["activity_ledger"]["days"][0]["entries"][0]
    later_fill["opening_fill_receipt_sha256"] = first_fill[
        "opening_fill_receipt_sha256"
    ]
    _refresh_anchor_hashes(later_anchor)
    activity_reuse.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError, match="semantic evidence identity is reused"
    ):
        _evaluate(activity_reuse)


def test_omitted_calendar_day_and_scalar_only_evidence_are_rejected(
    valid_corpus,
) -> None:
    omitted = _owned(valid_corpus)
    anchor = omitted.evidence_payload["anchors"][0]
    anchor["calendar_dates"].pop(100)
    anchor["daily_gross_mtm_returns"].pop(100)
    anchor["daily_base_cost_returns"].pop(100)
    anchor["daily_stress_cost_returns"].pop(100)
    anchor["daily_base_net_mtm_returns"].pop(100)
    anchor["daily_stress_net_mtm_returns"].pop(100)
    _refresh_anchor_hashes(anchor, derive_activity=False)
    omitted.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="omits"):
        _evaluate(omitted)

    scalar = _owned(valid_corpus)
    scalar.evidence_payload["anchors"][0]["daily_base_net_mtm_returns"] = "0.001"
    scalar.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="vectors"):
        _evaluate(scalar)


def test_independent_reconciliation_binds_actual_vectors_and_ledgers(
    valid_corpus,
) -> None:
    vector_splice = _owned(valid_corpus)
    vector_splice.reconciliation_payload["anchors"][0]["daily_base_net_mtm_returns"][
        0
    ] = "0.5"
    with pytest.raises(process.ProcessVerificationError, match="exact dates, values"):
        _evaluate(vector_splice)

    ledger_splice = _owned(valid_corpus)
    ledger_splice.reconciliation_payload["anchors"][0]["ledger_identities"][
        "fill_ledger_sha256"
    ] = _h("altered-fill-ledger")
    with pytest.raises(process.ProcessVerificationError, match="exact dates, values"):
        _evaluate(ledger_splice)


def test_trade_vector_is_exact_and_must_be_derived_from_position_activity(
    valid_corpus,
) -> None:
    missing_vector = _owned(valid_corpus)
    anchor = missing_vector.evidence_payload["anchors"][0]
    anchor["completed_trade_outcomes"] = []
    missing_vector.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError, match="completed-trade vector"
    ):
        _evaluate(missing_vector)

    rescaled = _owned(valid_corpus)
    anchor = rescaled.evidence_payload["anchors"][0]
    anchor["completed_trade_outcomes"] = [
        format(float(value) * 2.0, ".12f")
        for value in anchor["completed_trade_outcomes"]
    ]
    _refresh_anchor_hashes(anchor, derive_activity=False)
    rescaled.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="not derived"):
        _evaluate(rescaled)

    contradictory = _owned(valid_corpus)
    anchor = contradictory.evidence_payload["anchors"][0]
    anchor["completed_trade_outcomes"] = [
        format(-abs(Decimal(value)), ".12f")
        for value in anchor["completed_trade_outcomes"]
    ]
    _refresh_anchor_hashes(anchor, derive_activity=False)
    contradictory.refresh_reconciliation()
    assert sum(Decimal(value) for value in anchor["daily_base_net_mtm_returns"]) > 0
    assert sum(Decimal(value) for value in anchor["completed_trade_outcomes"]) < 0
    with pytest.raises(process.ProcessVerificationError, match="not derived"):
        _evaluate(contradictory)


def test_zero_completed_positions_cannot_supply_nonzero_mtm_evidence(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    count = len(anchor["calendar_dates"])
    zeros = ["0.000000000000"] * count
    anchor["daily_gross_mtm_returns"] = list(zeros)
    anchor["daily_base_cost_returns"] = list(zeros)
    anchor["daily_stress_cost_returns"] = list(zeros)
    anchor["daily_base_net_mtm_returns"] = list(zeros)
    anchor["daily_stress_net_mtm_returns"] = list(zeros)
    anchor["daily_gross_mtm_returns"][-1] = "0.001200000000"
    anchor["daily_base_cost_returns"][-1] = "-0.000200000000"
    anchor["daily_stress_cost_returns"][-1] = "-0.000300000000"
    anchor["daily_base_net_mtm_returns"][-1] = "0.001000000000"
    anchor["daily_stress_net_mtm_returns"][-1] = "0.000900000000"
    _set_activity_ledger_from_vectors(anchor)
    for day in anchor["activity_ledger"]["days"][:-1]:
        day["entries"] = []
        day["position_marks"] = []
        day["exits"] = []
    final_day = anchor["activity_ledger"]["days"][-1]
    entry = final_day["entries"][0]
    entry["base_fill_price"] = "100.200000000000"
    entry["stress_fill_price"] = "100.300000000000"
    _refresh_opening_fill_payload(anchor, entry)
    final_day["exits"] = []
    _refresh_anchor_hashes(anchor)
    assert anchor["completed_trade_count"] == 0
    corpus.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError,
        match="nonzero MTM evidence requires at least one completed position",
    ):
        _evaluate(corpus)


def test_carried_position_and_risk_cost_evidence_must_replay_from_activity(
    valid_corpus,
) -> None:
    carried = _owned(valid_corpus)
    anchor = carried.evidence_payload["anchors"][0]
    first_day = anchor["activity_ledger"]["days"][0]
    position = copy.deepcopy(first_day["entries"][0])
    prior_date = (
        date.fromisoformat(anchor["calendar_dates"][0]) - timedelta(days=1)
    ).isoformat()
    position["opened_at_utc"] = f"{prior_date}T00:01:00Z"
    _refresh_opening_fill_payload(anchor, position)
    position["forward_window_open_mark_price"] = position["reference_mid_price"]
    position["forward_window_open_mark_payload_sha256"] = _sha(
        {
            "schema": "fxpm-forward-window-opening-mark-payload-v1",
            "anchor_id": anchor["anchor_id"],
            "position_id": position["position_id"],
            "forward_start_date": anchor["calendar_dates"][0],
            "mark_price": position["forward_window_open_mark_price"],
        }
    )
    position["forward_window_open_mark_receipt_sha256"] = _h(
        f"{position['position_id']}:forward-opening-mark-receipt"
    )
    anchor["activity_ledger"]["initial_positions"] = [position]
    first_day["entries"] = []
    equity = Decimal(first_day["equity_basis_cash"])
    mark_price = Decimal(first_day["position_marks"][0]["mark_price"])
    exit_fill = first_day["exits"][0]
    base_cost_cash = -Decimal(anchor["daily_base_cost_returns"][0]) * equity
    stress_cost_cash = -Decimal(anchor["daily_stress_cost_returns"][0]) * equity
    exit_fill["base_fill_price"] = format(mark_price - base_cost_cash, ".12f")
    exit_fill["stress_fill_price"] = format(mark_price - stress_cost_cash, ".12f")
    _refresh_closing_fill_payload(anchor, exit_fill)
    _refresh_anchor_hashes(anchor)
    carried.refresh_reconciliation()
    assert _evaluate(carried).body()["decision"] == "OFFLINE_PROCESS_VALIDATION_PASS"
    assert (
        anchor["dependence_diagnostics"][
            "maximum_observed_holding_calendar_days_by_timeframe"
        ]["M5"]
        == 2
    )
    assert anchor["risk_diagnostics"]["maximum_concurrent_open_positions"] == 1
    assert (
        Decimal(anchor["risk_diagnostics"]["maximum_gross_exposure_equity_multiple"])
        > 0
    )
    assert Decimal(anchor["risk_diagnostics"]["maximum_open_risk_equity_fraction"]) > 0
    assert position["opening_fill_payload_sha256"]
    assert position["forward_window_open_mark_receipt_sha256"]

    fabricated_cost = _owned(valid_corpus)
    anchor = fabricated_cost.evidence_payload["anchors"][0]
    entry = anchor["activity_ledger"]["days"][0]["entries"][0]
    entry["base_fill_price"] = format(
        Decimal(entry["base_fill_price"]) + Decimal("0.01"), ".12f"
    )
    _refresh_opening_fill_payload(anchor, entry)
    anchor["activity_ledger_sha256"] = _sha(anchor["activity_ledger"])
    fabricated_cost.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="does not reconcile"):
        _evaluate(fabricated_cost)

    fabricated_risk = _owned(valid_corpus)
    anchor = fabricated_risk.evidence_payload["anchors"][0]
    for day in anchor["activity_ledger"]["days"]:
        entry = day["entries"][0]
        entry["stop_price"] = "80"
        _refresh_opening_fill_payload(anchor, entry)
    _, _, ledgers, _, activity_sha = process._validate_activity_ledger(
        anchor,
        anchor["calendar_dates"],
        [Decimal(value) for value in anchor["daily_gross_mtm_returns"]],
        [Decimal(value) for value in anchor["daily_base_cost_returns"]],
        [Decimal(value) for value in anchor["daily_stress_cost_returns"]],
    )
    anchor["activity_ledger_sha256"] = activity_sha
    anchor["ledger_identities"] = ledgers
    _refresh_anchor_hashes(anchor, derive_activity=False)
    fabricated_risk.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="risk diagnostics"):
        _evaluate(fabricated_risk)

    bare_carried = _owned(valid_corpus)
    anchor = bare_carried.evidence_payload["anchors"][0]
    day = anchor["activity_ledger"]["days"][0]
    entry = day["entries"][0]
    anchor["activity_ledger"]["initial_positions"] = [
        {
            "position_id": entry["position_id"],
            "cell_decision_sha256": entry["cell_decision_sha256"],
            "timeframe": entry["timeframe"],
            "opened_on_date": (
                date.fromisoformat(anchor["calendar_dates"][0]) - timedelta(days=1)
            ).isoformat(),
        }
    ]
    day["entries"] = []
    bare_carried.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="keys mismatch"):
        _evaluate(bare_carried)


def test_policy_and_process_definition_hash_tampering_fail_closed(
    valid_corpus,
) -> None:
    policy = _owned(valid_corpus)
    policy.precommit_payload["policy"]["minimum_anchor_count"] = 3
    policy.precommit_payload["policy_sha256"] = _sha(policy.precommit_payload["policy"])
    with pytest.raises(process.ProcessVerificationError, match="pinned policy"):
        _evaluate(policy)

    component = _owned(valid_corpus)
    component.precommit_payload["process_definition"]["component_identities"][
        "activation_contract_code_sha256"
    ] = _h("altered-component")
    with pytest.raises(
        process.ProcessVerificationError, match="process definition hash mismatch"
    ):
        _evaluate(component)


@pytest.mark.parametrize("payload_index", (0, 1, 2))
def test_public_payloads_require_exact_builtin_dicts(
    valid_corpus,
    payload_index: int,
) -> None:
    class DictSubclass(dict):
        pass

    payloads = [
        copy.deepcopy(valid_corpus.precommit_payload),
        copy.deepcopy(valid_corpus.evidence_payload),
        copy.deepcopy(valid_corpus.reconciliation_payload),
    ]
    payloads[payload_index] = DictSubclass(payloads[payload_index])
    with pytest.raises(process.ProcessVerificationError, match="exact built-in dict"):
        process.evaluate_historical_process(*payloads)


@pytest.mark.parametrize(
    ("location", "alias"),
    (
        pytest.param("cell_count", float(process.CELL_COUNT), id="float-cell-count"),
        pytest.param("cell_count", True, id="bool-cell-count"),
        pytest.param("attempt_count", 3.0, id="float-attempt-count"),
        pytest.param("attempt_count", True, id="bool-attempt-count"),
    ),
)
def test_float_and_bool_count_aliases_fail_at_public_construction(
    valid_corpus,
    location: str,
    alias: object,
) -> None:
    corpus = _owned(valid_corpus)
    if location == "cell_count":
        definition = corpus.precommit_payload["process_definition"]
        definition["cell_count"] = alias
        corpus.precommit_payload["process_definition_sha256"] = _sha(definition)
    else:
        anchor = corpus.evidence_payload["anchors"][0]
        anchor["book_cells"][1]["attempt_count"] = alias
        _refresh_anchor_hashes(anchor)
        corpus.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError,
        match="unsupported JSON value type: float|cell_count|attempt_count",
    ):
        _evaluate(corpus)


def test_input_bounds_fail_closed(valid_corpus) -> None:
    oversized = _owned(valid_corpus)
    oversized.evidence_payload["anchors"][0]["anchor_id"] = "x" * 5_000
    with pytest.raises(process.ProcessVerificationError, match="string exceeds"):
        _evaluate(oversized)


def test_extreme_economic_primitives_fail_closed_at_public_api(
    valid_corpus,
) -> None:
    extreme = _owned(valid_corpus)
    anchor = extreme.evidence_payload["anchors"][0]
    day = anchor["activity_ledger"]["days"][0]
    entry = day["entries"][0]
    entry["quantity"] = "1000000000000000"
    entry["contract_multiplier"] = "1000000000000000"
    day["equity_basis_cash"] = "0.000000000001"
    _refresh_opening_fill_payload(anchor, entry)
    anchor["activity_ledger_sha256"] = _sha(anchor["activity_ledger"])
    extreme.refresh_reconciliation()

    with pytest.raises(
        process.ProcessVerificationError,
        match="magnitude|numeric capacity|pinned precision",
    ):
        _evaluate(extreme)


@pytest.mark.parametrize(("depth", "container"), ((2_000, "mapping"), (5_000, "list")))
def test_deep_inputs_reject_iteratively_without_python_recursion(
    depth: int,
    container: str,
) -> None:
    nested: object = "leaf"
    for _ in range(depth):
        nested = {"child": nested} if container == "mapping" else [nested]
    with pytest.raises(process.ProcessVerificationError, match="depth bound"):
        process.canonical_sha256({"nested": nested})


def test_negative_newest_anchor_mean_rejects_without_per_anchor_lcb_conjunction(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    newest = corpus.evidence_payload["anchors"][-1]
    count = len(newest["calendar_dates"])
    _set_net_vectors(newest, ["-0.0001"] * count, ["-0.0001"] * count)
    _refresh_anchor_hashes(newest)
    corpus.refresh_reconciliation()
    body = _evaluate(corpus).body()
    assert body["decision"] == "OFFLINE_PROCESS_VALIDATION_FAIL"
    assert body["failure_reasons"] == [
        "newest_anchor_stress_log_growth_mean_above_zero"
    ]
    assert process.pinned_process_policy()["per_anchor_hac_lcb_gate"] is False


def test_equal_numeric_vectors_with_unique_evidence_are_statistically_evaluated(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    for anchor in corpus.evidence_payload["anchors"]:
        count = len(anchor["calendar_dates"])
        base = [
            "0.011" if (index // 35) % 2 == 0 else "-0.009" for index in range(count)
        ]
        stress = [format(Decimal(value) - Decimal("0.0002"), ".12f") for value in base]
        _set_net_vectors(anchor, base, stress)
        _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    body = _evaluate(corpus).body()
    assert body["decision"] == "OFFLINE_PROCESS_VALIDATION_FAIL"
    assert body["gates"]["base_between_anchor_log_growth_variation"] is False
    assert body["gates"]["stress_between_anchor_log_growth_variation"] is False
    assert body["gates"]["base_distinct_anchor_log_growth_means"] is False
    assert body["gates"]["stress_distinct_anchor_log_growth_means"] is False


def test_precommitted_fixed_hac_bandwidths_feed_one_max_variance_gate(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    for anchor_index, anchor in enumerate(corpus.evidence_payload["anchors"]):
        count = len(anchor["calendar_dates"])
        base = _centered_block_pattern(
            count, block=20, mean=0.0005 + anchor_index * 0.000001, sigma=0.01
        )
        stress = [format(Decimal(value) - Decimal("0.00015"), ".12f") for value in base]
        _set_net_vectors(anchor, base, stress)
        _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    body = _evaluate(corpus).body()
    assert body["metrics"]["primary_hac_bandwidth_calendar_days"] == 34
    assert body["metrics"]["sensitivity_hac_bandwidth_calendar_days"] == 68
    metrics = body["metrics"]
    assert (
        metrics["stress_log_growth_primary_hac_variance"]
        != metrics["stress_log_growth_sensitivity_hac_variance"]
    )
    selected = Decimal(metrics["stress_log_growth_selected_variance"])
    assert selected >= Decimal(metrics["stress_log_growth_primary_hac_variance"])
    assert selected >= Decimal(metrics["stress_log_growth_sensitivity_hac_variance"])
    assert metrics["fixed_hac_role"] == (
        "VARIANCE_CANDIDATES_AND_DIAGNOSTICS_NOT_SEPARATE_TESTS"
    )


def test_180_day_dependence_cannot_bypass_the_max_variance_estimator(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    for anchor_index, (anchor, phase) in enumerate(
        zip(corpus.evidence_payload["anchors"], (0, 7, 19, 31), strict=True)
    ):
        count = len(anchor["calendar_dates"])
        base = _phase_centered_block_pattern(
            count,
            block=180,
            phase=phase,
            mean=Decimal("0.002") + Decimal(anchor_index) * Decimal("0.000001"),
            amplitude=Decimal("0.005"),
        )
        stress = [format(Decimal(value) - Decimal("0.0001"), ".12f") for value in base]
        _set_net_vectors(anchor, base, stress)
        _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    body = _evaluate(corpus).body()
    metrics = body["metrics"]
    selected = Decimal(metrics["stress_log_growth_selected_variance"])
    primary = Decimal(metrics["stress_log_growth_primary_hac_variance"])
    sensitivity = Decimal(metrics["stress_log_growth_sensitivity_hac_variance"])
    assert sensitivity > primary
    assert selected >= sensitivity
    assert body["decision"] != "OFFLINE_PROCESS_VALIDATION_PASS"


def test_one_profitable_day_per_anchor_cannot_create_process_evidence(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    for anchor_index, anchor in enumerate(corpus.evidence_payload["anchors"]):
        count = len(anchor["calendar_dates"])
        base = ["0"] * count
        base[100 + anchor_index] = format(
            Decimal("0.2") + Decimal(anchor_index) * Decimal("0.01"),
            ".12f",
        )
        _set_net_vectors(anchor, base, list(base))
        _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    body = _evaluate(corpus).body()
    assert body["decision"] == "OFFLINE_PROCESS_VALIDATION_FAIL"
    assert body["confirmatory_inference_status"] == "FAIL"
    assert "stress_log_growth_confirmatory_lcb_above_zero" in body["failure_reasons"]
    assert (
        Decimal(
            body["metrics"][
                "pooled_base_log_growth_mean_after_top_block_removal_diagnostic"
            ]
        )
        == 0
    )


def test_positive_arithmetic_mean_cannot_authorize_compounding_loss(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    for anchor_index, anchor in enumerate(corpus.evidence_payload["anchors"]):
        count = len(anchor["calendar_dates"])
        gain = Decimal("0.05000") + Decimal(anchor_index) * Decimal("0.00002")
        loss = Decimal("-0.04900")
        base = [
            format(gain if index % 2 == 0 else loss, ".12f") for index in range(count)
        ]
        random.Random(900 + anchor_index).shuffle(base)
        stress = [format(Decimal(value) - Decimal("0.0001"), ".12f") for value in base]
        _set_net_vectors(anchor, base, stress)
        _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()

    body = _evaluate(corpus).body()
    metrics = body["metrics"]
    assert Decimal(metrics["pooled_stress_arithmetic_return_mean_diagnostic"]) > 0
    assert Decimal(metrics["pooled_stress_log_growth_mean"]) < 0
    assert body["confirmatory_inference_status"] == "FAIL"
    assert body["decision"] == "OFFLINE_PROCESS_VALIDATION_FAIL"
    assert "stress_log_growth_confirmatory_lcb_above_zero" in body["failure_reasons"]


def test_v4_calibration_retains_power_without_null_false_passes() -> None:
    def evaluate_seed(seed: int, daily_log_growth: float) -> str:
        generator = random.Random(seed)
        vectors: list[list[Decimal]] = []
        with localcontext() as context:
            context.prec = 50
            for _ in range(4):
                vector = [
                    Decimal(str(generator.gauss(daily_log_growth, 0.01))).exp()
                    - Decimal(1)
                    for _ in range(365)
                ]
                vectors.append(vector)
        return process._compute_decision_body(
            process_id="deterministic-calibration",
            process_definition_sha256="a" * 64,
            precommit_receipt_sha256="b" * 64,
            evidence_receipt_sha256="c" * 64,
            reconciliation_receipt_sha256="d" * 64,
            reconciliation_time=datetime(2026, 7, 13),
            anchors=[{"completed_trade_count": 1} for _ in range(4)],
            base_vectors=vectors,
            stress_vectors=vectors,
        )["decision"]

    strong_edge_passes = sum(
        evaluate_seed(seed, 0.001) == "OFFLINE_PROCESS_VALIDATION_PASS"
        for seed in range(10)
    )
    null_passes = sum(
        evaluate_seed(seed, 0.0) == "OFFLINE_PROCESS_VALIDATION_PASS"
        for seed in range(10)
    )
    assert strong_edge_passes >= 7
    assert null_passes <= 1


def test_daily_ips_is_anchor_local_initial_positive_and_monotone() -> None:
    generator = random.Random(2)
    vectors = [
        [Decimal(str(generator.gauss(0.0, 0.01))) for _ in range(120)] for _ in range(4)
    ]
    lrv, variance, unresolved, valid_pair_count = process._daily_ips_variance(
        vectors, 67
    )

    centered = [
        [value - sum(vector, Decimal(0)) / Decimal(len(vector)) for value in vector]
        for vector in vectors
    ]
    total = sum(len(vector) for vector in centered)
    gammas = [
        sum(
            (
                sum(
                    (
                        vector[index] * vector[index - lag]
                        for index in range(lag, len(vector))
                    ),
                    Decimal(0),
                )
                for vector in centered
            ),
            Decimal(0),
        )
        / Decimal(total)
        for lag in range(68)
    ]
    raw_pairs = [gammas[index] + gammas[index + 1] for index in range(0, 68, 2)]
    first_nonpositive = next(
        index for index, value in enumerate(raw_pairs) if value <= 0
    )
    monotone_pairs: list[Decimal] = []
    for raw_pair in raw_pairs[:first_nonpositive]:
        monotone_pairs.append(
            raw_pair if not monotone_pairs else min(monotone_pairs[-1], raw_pair)
        )
    expected_lrv = max(
        Decimal(0),
        -gammas[0] + Decimal(2) * sum(monotone_pairs, Decimal(0)),
    )

    flattened = [value for vector in centered for value in vector]
    cross_boundary_gammas = [
        sum(
            (
                flattened[index] * flattened[index - lag]
                for index in range(lag, len(flattened))
            ),
            Decimal(0),
        )
        / Decimal(total)
        for lag in range(68)
    ]
    cross_boundary_pairs = [
        cross_boundary_gammas[index] + cross_boundary_gammas[index + 1]
        for index in range(0, 68, 2)
    ]
    cross_boundary_stop = next(
        index for index, value in enumerate(cross_boundary_pairs) if value <= 0
    )
    cross_boundary_monotone: list[Decimal] = []
    for raw_pair in cross_boundary_pairs[:cross_boundary_stop]:
        cross_boundary_monotone.append(
            raw_pair
            if not cross_boundary_monotone
            else min(cross_boundary_monotone[-1], raw_pair)
        )
    wrong_cross_boundary_lrv = max(
        Decimal(0),
        -cross_boundary_gammas[0]
        + Decimal(2) * sum(cross_boundary_monotone, Decimal(0)),
    )

    assert unresolved is False
    assert valid_pair_count == first_nonpositive == 6
    assert raw_pairs[3] > raw_pairs[2]
    assert monotone_pairs[3] == monotone_pairs[2]
    assert lrv == expected_lrv
    assert variance == expected_lrv / Decimal(total)
    assert lrv != wrong_cross_boundary_lrv


def test_empty_anchor_evidence_cannot_create_a_process_pass(valid_corpus) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    count = len(anchor["calendar_dates"])
    zeros = ["0.000000000000"] * count
    anchor["daily_gross_mtm_returns"] = list(zeros)
    anchor["daily_base_cost_returns"] = list(zeros)
    anchor["daily_stress_cost_returns"] = list(zeros)
    anchor["daily_base_net_mtm_returns"] = list(zeros)
    anchor["daily_stress_net_mtm_returns"] = list(zeros)
    anchor["activity_ledger"] = {
        "schema": "fxpm-anchor-economic-position-activity-ledger-v2",
        "initial_positions": [],
        "days": [
            {
                "date": date_text,
                "equity_basis_cash": "1000",
                "entries": [],
                "position_marks": [],
                "exits": [],
            }
            for date_text in anchor["calendar_dates"]
        ],
    }
    _refresh_anchor_hashes(anchor)
    assert anchor["completed_trade_count"] == 0
    corpus.refresh_reconciliation()

    body = _evaluate(corpus).body()
    assert body["decision"] == "OFFLINE_PROCESS_VALIDATION_FAIL"
    assert body["gates"]["base_positive_within_anchor_log_growth_variance"] is False
    assert body["gates"]["stress_positive_within_anchor_log_growth_variance"] is False
    assert "base_positive_within_anchor_log_growth_variance" in body["failure_reasons"]
    assert (
        "stress_positive_within_anchor_log_growth_variance" in body["failure_reasons"]
    )


def test_observed_holding_and_overlap_must_fit_frozen_dependence_policy(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    corpus.evidence_payload["anchors"][0]["dependence_diagnostics"][
        "maximum_observed_holding_calendar_days"
    ] = 31
    corpus.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="holding exceeds"):
        _evaluate(corpus)

    timeframe = _owned(valid_corpus)
    timeframe.evidence_payload["anchors"][0]["dependence_diagnostics"][
        "maximum_observed_holding_calendar_days_by_timeframe"
    ]["M5"] = 3
    timeframe.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError,
        match="M5 holding exceeds|not derived from the position activity ledger",
    ):
        _evaluate(timeframe)


def test_decision_tamper_is_detected_by_deterministic_reverification(
    valid_corpus,
) -> None:
    decision = _evaluate(valid_corpus)
    tampered = decision.body()
    tampered["decision"] = "OFFLINE_PROCESS_VALIDATION_FAIL"
    tampered["failure_reasons"] = ["invented"]
    with pytest.raises(process.ProcessVerificationError, match="tampered"):
        process.verify_historical_process_decision(
            tampered,
            valid_corpus.precommit_payload,
            valid_corpus.evidence_payload,
            valid_corpus.reconciliation_payload,
        )


def test_module_has_no_signature_or_trust_dependency() -> None:
    assert not hasattr(process, "sign_receipt")
    assert not hasattr(process, "generate_authority_key")
    assert not hasattr(process, "AuthorityTrust")
    assert not hasattr(process, "ProcessTrustBundle")
    source = inspect.getsource(process)
    assert "Ed25519" not in source
    assert "cryptography" not in source
    assert "signature_b64" not in source
    assert "trust_bundle" not in source


def test_public_api_has_no_integrity_or_evaluator_override(valid_corpus) -> None:
    evaluate_parameters = tuple(
        inspect.signature(process.evaluate_historical_process).parameters
    )
    verify_parameters = tuple(
        inspect.signature(process.verify_historical_process_decision).parameters
    )
    assert evaluate_parameters == (
        "precommit_payload",
        "evidence_payload",
        "reconciliation_payload",
    )
    assert verify_parameters == (
        "decision",
        "precommit_payload",
        "evidence_payload",
        "reconciliation_payload",
    )
    with pytest.raises(TypeError):
        process.evaluate_historical_process(
            {},
            {},
            {},
            _integrity_check=lambda: None,
            _implementation=lambda *args: None,
        )
    assert not hasattr(process.HistoricalProcessDecision, "from_mapping")


def test_purge_is_derived_from_d1_holding_and_overlap_and_label_locality(
    valid_corpus,
) -> None:
    policy = process.pinned_process_policy()
    assert (
        policy["minimum_purge_calendar_days"]
        == (
            max(policy["maximum_holding_calendar_days_by_timeframe"].values())
            + policy["execution_overlap_allowance_calendar_days"]
        )
        == 35
    )

    short = _owned(valid_corpus)
    short.precommit_payload["process_definition"]["anchor_plan"][0][
        "purge_calendar_days"
    ] = 2
    short.precommit_payload["process_definition_sha256"] = _sha(
        short.precommit_payload["process_definition"]
    )
    with pytest.raises(process.ProcessVerificationError, match="purge"):
        _evaluate(short)

    leaked = _owned(valid_corpus)
    anchor = leaked.evidence_payload["anchors"][0]
    cell = anchor["book_cells"][20]
    assert cell["timeframe"] == "D1"
    leaked_end = "2018-11-28T00:00:00Z"
    cell["market_data_end_utc"] = leaked_end
    cell["cell_decision"]["training_label_end_utc"] = leaked_end
    cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
    _refresh_anchor_hashes(anchor)
    leaked.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError,
        match="after its anchor cutoff|non-causal",
    ):
        _evaluate(leaked)


def test_anchor_cutoff_must_be_strictly_after_prior_half_open_oos_end(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    second = corpus.precommit_payload["process_definition"]["anchor_plan"][1]
    second["selection_cutoff_utc"] = "2020-01-01T00:00:00Z"
    second["forward_start_date"] = "2020-02-05"
    second["forward_end_date"] = "2021-02-03"
    corpus.precommit_payload["process_definition_sha256"] = _sha(
        corpus.precommit_payload["process_definition"]
    )
    with pytest.raises(process.ProcessVerificationError, match="end-exclusive"):
        _evaluate(corpus)


def test_four_anchor_cluster_variance_rejects_z_false_positive(valid_corpus) -> None:
    corpus = _owned(valid_corpus)
    means = [Decimal("0.0001")] * 3 + [Decimal("0.0005")]
    for anchor_index, (anchor, mean) in enumerate(
        zip(corpus.evidence_payload["anchors"], means, strict=True)
    ):
        count = len(anchor["calendar_dates"])
        base = [
            format(
                mean
                + (
                    Decimal("0.0000001")
                    if index == anchor_index
                    else Decimal("-0.0000001")
                    if index == anchor_index + 10
                    else Decimal("0")
                ),
                ".12f",
            )
            for index in range(count)
        ]
        stress = [format(Decimal(value) - Decimal("0.00001"), ".12f") for value in base]
        _set_net_vectors(anchor, base, stress)
        _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    body = _evaluate(corpus).body()
    assert body["decision"] == "OFFLINE_PROCESS_VALIDATION_FAIL"
    assert Decimal(body["metrics"]["stress_log_growth_confirmatory_lcb"]) < 0
    assert "stress_log_growth_confirmatory_lcb_above_zero" in body["failure_reasons"]
    assert body["metrics"]["small_cluster_degrees_of_freedom"] == 3
    assert body["metrics"]["small_cluster_one_sided_critical_value"].startswith(
        "2.353363"
    )


def test_epsilon_zero_variance_zero_trade_book_cannot_pass(valid_corpus) -> None:
    corpus = _owned(valid_corpus)
    for index, anchor in enumerate(corpus.evidence_payload["anchors"], start=1):
        count = len(anchor["calendar_dates"])
        value = format(Decimal(index) * Decimal("0.000000000001"), ".12f")
        _set_net_vectors(anchor, [value] * count, [value] * count)
        anchor["activity_ledger"]["days"] = [
            {
                "date": day,
                "equity_basis_cash": "1000",
                "entries": [],
                "position_marks": [],
                "exits": [],
            }
            for day in anchor["calendar_dates"]
        ]
        anchor["activity_ledger_sha256"] = _sha(anchor["activity_ledger"])
        anchor["completed_trade_count"] = 0
        anchor["completed_trade_outcomes"] = []
        _refresh_anchor_hashes(anchor, derive_activity=False)
    corpus.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="does not reconcile"):
        _evaluate(corpus)


def test_claimed_future_authority_clock_is_rejected_today(valid_corpus) -> None:
    corpus = _owned(valid_corpus)
    corpus.precommit_payload["authorized_at_utc"] = "2100-07-13T10:00:00Z"
    corpus.precommit_payload["evidence_access_not_before_utc"] = "2100-07-13T10:01:00Z"
    corpus.precommit_payload["evidence_access_deadline_utc"] = "2100-07-13T12:00:00Z"
    with pytest.raises(
        process.ProcessVerificationError, match="future trusted timestamp"
    ):
        _evaluate(corpus)


@pytest.mark.parametrize(
    "deadline",
    (
        "2026-07-13T10:21:30Z",
        "2026-07-13T10:20:30Z",
        "2026-07-13T10:19:30Z",
    ),
)
def test_execution_evidence_and_reconciliation_must_meet_deadline(
    valid_corpus,
    deadline: str,
) -> None:
    corpus = _owned(valid_corpus)
    corpus.precommit_payload["evidence_access_deadline_utc"] = deadline
    precommit_sha = _sha(corpus.precommit_payload)
    corpus.evidence_payload["precommit_payload_sha256"] = precommit_sha
    # Keep the issued forward lineage bound to the modified precommit so the
    # test isolates deadline enforcement rather than failing at the earlier
    # receipt-splice boundary.
    corpus.evidence_payload["forward_oos_replay_lineage"][
        "causal_precommit_payload_sha256"
    ] = precommit_sha
    corpus.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="must not exceed"):
        _evaluate(corpus)


def test_decimal_context_is_owned_and_exponent_text_is_rejected(valid_corpus) -> None:
    with localcontext() as context:
        context.prec = 12
        low_precision = _evaluate(valid_corpus)
    with localcontext() as context:
        context.prec = 28
        ordinary_precision = _evaluate(valid_corpus)
    assert low_precision.sha256 == ordinary_precision.sha256

    exponent = _owned(valid_corpus)
    anchor = exponent.evidence_payload["anchors"][0]
    anchor["daily_base_net_mtm_returns"][0] = "1e-12"
    _refresh_anchor_hashes(anchor)
    exponent.refresh_reconciliation()
    with pytest.raises(process.ProcessVerificationError, match="fixed-point"):
        _evaluate(exponent)


def test_control_character_input_is_rejected_before_canonical_amplification() -> None:
    escaped = "\x00" * 4096
    assert len(json.dumps(escaped).encode("utf-8")) > len(escaped) * 5
    with pytest.raises(process.ProcessVerificationError, match="byte bound"):
        process.canonical_sha256({"values": [escaped] * 3_000})


def test_long_memory_phase_variants_do_not_manufacture_process_power(
    valid_corpus,
) -> None:
    corpus = _owned(valid_corpus)
    for anchor_index, (anchor, offset) in enumerate(
        zip(corpus.evidence_payload["anchors"], (0, 20, 40, 60), strict=True)
    ):
        count = len(anchor["calendar_dates"])
        level_shift = Decimal(anchor_index) * Decimal("0.000001")
        base = [
            format(Decimal("0.005") + level_shift, ".12f")
            if ((index + offset) // 180) % 2 == 0
            else format(Decimal("-0.001") + level_shift, ".12f")
            for index in range(count)
        ]
        stress = [format(Decimal(value) - Decimal("0.0001"), ".12f") for value in base]
        _set_net_vectors(anchor, base, stress)
        _refresh_anchor_hashes(anchor)
    corpus.refresh_reconciliation()
    body = _evaluate(corpus).body()
    assert body["decision"] == "OFFLINE_PROCESS_VALIDATION_DEPENDENCE_INSUFFICIENT"
    assert body["confirmatory_inference_status"] == "DEPENDENCE_INSUFFICIENT"
    assert body["insufficiency_reasons"] == [
        "stress_log_growth_ips_positive_sequence_unresolved_through_lag_67"
    ]


def test_risk_cost_and_cell_lineage_are_executable_not_opaque(valid_corpus) -> None:
    risk = _owned(valid_corpus)
    risk.precommit_payload["process_definition"]["risk_policy"][
        "maximum_holding_calendar_days_by_timeframe"
    ]["D1"] = 31
    risk.precommit_payload["process_definition_sha256"] = _sha(
        risk.precommit_payload["process_definition"]
    )
    with pytest.raises(process.ProcessVerificationError, match="frozen policy"):
        _evaluate(risk)

    cost = _owned(valid_corpus)
    anchor = cost.evidence_payload["anchors"][0]
    gross = Decimal(anchor["daily_gross_mtm_returns"][0])
    anchor["daily_stress_cost_returns"][0] = "-0.000100000000"
    anchor["daily_stress_net_mtm_returns"][0] = format(
        gross - Decimal("0.0001"), ".12f"
    )
    _refresh_anchor_hashes(anchor, derive_activity=False)
    cost.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError, match="not at least as severe"
    ):
        _evaluate(cost)

    lineage = _owned(valid_corpus)
    anchor = lineage.evidence_payload["anchors"][0]
    cell = anchor["book_cells"][0]
    cell["cell_decision"]["horizon_receipt_sha256"] = _h("opaque-horizon-splice")
    cell["cell_decision_sha256"] = _sha(cell["cell_decision"])
    anchor["book_sha256"] = _sha(
        {
            "cells": anchor["book_cells"],
            "train_regime_supports": anchor["train_regime_supports"],
            "regime_detector_records": anchor["regime_detector_records"],
        }
    )
    anchor["component_outputs"]["exact_2400_cell_book_sha256"] = anchor["book_sha256"]
    lineage.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError, match="canonical lineage root"
    ):
        _evaluate(lineage)


def test_no_winner_book_cannot_manufacture_returns(valid_corpus) -> None:
    corpus = _owned(valid_corpus)
    anchor = corpus.evidence_payload["anchors"][0]
    for cell in anchor["book_cells"]:
        if cell["status"] != "WINNER":
            continue
        cell["status"] = process.NO_LOCAL_WINNER
        decision = cell["cell_decision"]
        decision["status"] = process.NO_LOCAL_WINNER
        decision["refit_market_data_end_utc"] = None
        for key in (
            "strategy_code_sha256",
            "trial_definition_sha256",
            "parameters_sha256",
            "anchor_refit_receipt_sha256",
        ):
            decision[key] = None
        cell["cell_decision_sha256"] = _sha(decision)
    _refresh_anchor_hashes(anchor, derive_activity=False)
    corpus.refresh_reconciliation()
    with pytest.raises(
        process.ProcessVerificationError,
        match="winning specialist|cannot manufacture",
    ):
        _evaluate(corpus)
