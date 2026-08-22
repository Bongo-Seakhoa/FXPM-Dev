"""Genuine search-time adapter for historical anchor production."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import inspect
import json
import secrets
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_current_refit_artifacts as current_refit
import pm_historical_anchor_orchestration as orchestration
import pm_native_prefix_production as native_prefix
import pm_training_horizon as training_horizon
from pm_search_outcomes import (
    FamilyCellOutcome,
    SearchInfrastructureError,
    SearchOutcomeKind,
    TrialOutcome,
)
import pm_winner_raw_evidence as raw_winner
import pm_winner_selection as winner_selection


CAPTURE_CONTRACT_VERSION = "fxpm.historical-search-capture.v2"
SYMBOL_INDEX_SCHEMA = "fxpm.historical-symbol-production-index.v1"
OPTIMIZER_PAYLOAD_SCHEMA = "fxpm.historical-optimizer-payload.v1"
FAILURE_ARTIFACT_SCHEMA = "fxpm.historical-search-technical-failure.v1"
RAW_DESCRIPTOR_GRAPH_SCHEMA = "fxpm.historical-raw-winner-descriptor-graph.v1"
HORIZON_WINNER_CROSS_BINDING_SCHEMA = "fxpm.historical-horizon-winner-cross-binding.v1"
SOURCE_COUNT_RECEIPT_SCHEMA = "fxpm.historical-attempt-source-count.v1"
RAW_ARTIFACT_NAMESPACE_PREFIX = "historical-anchor"
BOUNDED_RAW_WINNER_CANARY_RECEIPT_SCHEMA = "fxpm.bounded-raw-winner-canary-receipt.v2"
BOUNDED_RAW_WINNER_CANARY_STATE_CONTEXT_ADMISSION_SCHEMA = (
    "fxpm.bounded-raw-winner-canary-state-context-admission.v1"
)
BOUNDED_RAW_WINNER_CANARY_RECEIPT_ARTIFACT_TYPE = (
    "bounded_raw_winner_canary_nonauthorizing_receipt"
)
BOUNDED_RAW_WINNER_CANARY_FAILURE_ARTIFACT_TYPE = (
    "bounded_raw_winner_canary_technical_failure"
)
_BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA_V1 = (
    "fxpm.bounded-raw-winner-canary-failure.v1"
)
BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA = "fxpm.bounded-raw-winner-canary-failure.v2"
BOUNDED_RAW_WINNER_CANARY_FAILURE_DETAIL_LIMIT = 8
BOUNDED_RAW_WINNER_CANARY_FAILURE_DETAIL_BYTES = 256
BOUNDED_RAW_WINNER_CANARY_EXCEPTION_TYPE_BYTES = 128
HISTORICAL_SYMBOL_WORKER_BUNDLE_SCHEMA = (
    "fxpm.historical-symbol-worker-evidence-bundle.v1"
)
HISTORICAL_SYMBOL_WORKER_BUNDLE_MAX_BYTES = 64 * 1024**2

_CANARY_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "authority_scope",
        "status",
        "failure_code",
        "request_sha256",
        "request_nonce",
        "generation_id",
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "anchor_plan_receipt_sha256",
        "activation_id",
        "activation_manifest_sha256",
        "anchor_plan_sha256",
        "anchor",
        "search_process_receipt_sha256",
        "causal_precommit_receipt_sha256",
        "native_authority_verification",
        "symbol",
        "timeframe",
        "regime",
        "regime_selection_rule",
        "cell_plan_receipt_sha256",
        "horizon_plan_receipt_sha256",
        "source_session_receipt_sha256",
        "source_session_entry",
        "plan_permit_receipt_sha256",
        "capture_permit_receipt_sha256",
        "effective_pipeline_config_sha256",
        "attempted_trials_exact",
        "state_context_admission_summary",
        "raw_artifact_namespace",
        "frozen_plan_receipt",
        "frozen_descriptor_graph",
        "horizon_winner_cross_binding",
        "winner_request_entry",
        "winner_decision_entry",
        "raw_production_receipt",
        "production_descriptor_graph",
        "winner_request_sha256",
        "winner_decision_sha256",
        "winner_decision_status",
        "all_process_rejected",
        "raw_winner_completed",
        "standalone_winner_verifier_accepted",
        "technical_failure_entry",
        "book_projection_performed",
        "book_multiplicity_authorized",
        "causal_authorized",
        "current_refit_authorized",
        "capital_authorized",
        "publication_authorized",
        "active_pointer_mutation_authorized",
        "configuration_mutation_authorized",
        "order_authorized",
        "profitability_claimed",
    }
)
_CANARY_RECEIPT_SEAL = object()
_CANARY_STATE_CONTEXT_ADMISSION_FIELDS = frozenset(
    {
        "schema_version",
        "admission_scope",
        "search_process_receipt_sha256",
        "process_semantics_sha256",
        "registry_order_sha256",
        "cell_plan_receipt_sha256",
        "horizon_plan_receipt_sha256",
        "horizon_trial_registry_sha256",
        "horizon_trial_registry",
        "horizon_attempt_roster_sha256",
        "horizon_attempt_roster",
        "state_context_selection_receipt_sha256",
        "state_context_disposition_roster_sha256",
        "state_context_disposition_body_roster_sha256",
        "state_context_disposition_roster",
        "attempt_classification_roster",
        "attempt_classification_roster_sha256",
        "selected_common_context_bars",
        "candidate_boundary_limited",
        "attempt_count_exact",
        "additional_nondefault_attempt_count_exact",
        "state_context_admitted_attempt_count",
        "state_context_shortfall_attempt_count",
        "execution_planned_attempt_count",
        "execution_state_context_rejected_attempt_count",
        "execution_source_boundary_rejected_attempt_count",
        "execution_other_process_rejected_attempt_count",
        "registry_default_strategy_count_exact",
        "registry_default_state_context_admitted_count",
        "registry_default_state_context_shortfall_count",
        "strategy_with_any_state_context_admitted_count",
        "strategy_fully_state_context_admitted_count",
        "strategy_partially_state_context_admitted_count",
        "strategy_fully_state_context_shortfall_count",
        "receipt_sha256",
    }
)
_STATE_CONTEXT_SELECTION_FIELDS = frozenset(
    {
        "schema_version",
        "selection_rule",
        "selected_common_context_bars",
        "candidate_boundary_limited",
        "required_context_roster_sha256",
        "disposition_sha256",
        "disposition_roster_sha256",
        "within_selected_context_count",
        "exceeds_selected_context_count",
        "receipt_sha256",
    }
)
_STATE_CONTEXT_DISPOSITION_FIELDS = frozenset(
    {
        "schema_version",
        "candidate_id",
        "candidate_definition_sha256",
        "state_requirement_sha256",
        "required_context_bars",
        "selected_context_bars",
        "status",
        "reason_code",
        "disposition_sha256",
    }
)

_DESCRIPTOR_FIELDS = frozenset(
    {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
)
_CANARY_FAILURE_V1_FIELDS = frozenset(
    {
        "schema_version",
        "authority_scope",
        "request_sha256",
        "anchor_id",
        "symbol",
        "timeframe",
        "regime",
        "failure_code",
        "attempted_trials_exact",
        "frozen_receipt_sha256",
        "search_outcome",
        "economic_no_trade_or_no_winner",
        "downstream_authority",
    }
)
_CANARY_FAILURE_V2_FIELDS = _CANARY_FAILURE_V1_FIELDS | frozenset(
    {
        "exception_type",
        "failure_details",
        "attempt_ordinal",
        "fold_ordinal",
    }
)
_ARTIFACT_ENTRY_FIELDS = frozenset({"artifact_id", "artifact_type", "descriptor"})
_ATTEMPT_ARTIFACT_KEYS = frozenset(
    {"winner_process", "horizon_process", "trial_definition", "refit_recipe"}
)
_FROZEN_ARTIFACT_TYPES = {
    "precommitment": "winner_precommitment",
    "attempt_ledger": "winner_attempt_ledger",
    "source_contract_seed": "winner_source_contract_seed",
    "base_cost_contract": "winner_base_cost_contract",
    "stress_cost_contract": "winner_stress_cost_contract",
    "cost_stress_adversity_plan": "winner_cost_stress_adversity_plan",
    "risk_unit_contract": "winner_risk_unit_contract",
    "horizon_execution_contracts": "winner_horizon_execution_contracts",
    "trade_power_policy": "winner_trade_power_policy",
    "multiplicity_receipt": "winner_trade_power_multiplicity",
}


class HistoricalProductionAdapterError(ValueError):
    """Fail-closed capture or binding error."""


def _fail(code: str) -> None:
    raise HistoricalProductionAdapterError(code)


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise HistoricalProductionAdapterError("CANONICAL_JSON_INVALID") from exc


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _released_horizon_plan_body(plan: Any, code: str) -> dict[str, Any]:
    """Reopen a genuine sealed Horizon plan through its own authority."""

    try:
        valid = training_horizon._valid_plan(plan)
        body = plan.to_dict()
        receipt = plan.receipt_sha256
    except Exception as exc:
        raise HistoricalProductionAdapterError(code) from exc
    if (
        valid is not True
        or type(body) is not dict
        or type(receipt) is not str
        or _sha256(body) != receipt
    ):
        _fail(code)
    return json.loads(_canonical_bytes(body).decode("ascii"))


def _verify_unobserved_horizon_binding(
    *,
    plan: Any,
    plan_body: Mapping[str, Any],
    source_decision: book.HistoricalSourceGroupDecision,
    fit_decision: book.RegimeFitDecision,
    regime: str,
) -> None:
    """Require the released zero-work plan to bind the exact fitted support."""

    reopened = _released_horizon_plan_body(plan, "UNOBSERVED_HORIZON_PLAN_NOT_GENUINE")
    if reopened != plan_body:
        _fail("UNOBSERVED_HORIZON_PLAN_REOPEN_MISMATCH")
    source = source_decision.to_dict()
    fit = fit_decision.to_dict()
    support = fit.get("train_regime_support")
    binding = reopened.get("regime_support_binding")
    native = reopened.get("native_prefix")
    expected_binding_fields = {
        "schema_version",
        "source_group_decision_sha256",
        "regime_fit_decision_sha256",
        "regime_detector_record_sha256",
        "train_regime_support_receipt_sha256",
        "target_regime",
        "target_regime_count",
        "train_regime_support",
        "binding_sha256",
    }
    if (
        reopened.get("plan_status") != "HISTORICAL_REGIME_UNOBSERVED"
        or type(binding) is not dict
        or set(binding) != expected_binding_fields
        or type(support) is not dict
        or type(native) is not dict
        or binding.get("source_group_decision_sha256") != source_decision.receipt_sha256
        or binding.get("regime_fit_decision_sha256") != fit_decision.receipt_sha256
        or binding.get("regime_detector_record_sha256")
        != fit.get("regime_detector_record_sha256")
        or binding.get("train_regime_support_receipt_sha256")
        != support.get("receipt_sha256")
        or binding.get("target_regime") != regime
        or binding.get("target_regime_count") != 0
        or binding.get("train_regime_support") != support
        or binding.get("binding_sha256")
        != _sha256(
            {name: value for name, value in binding.items() if name != "binding_sha256"}
        )
        or support.get("counts", {}).get(regime) != 0
        or native.get("source_state") != book.AVAILABLE
        or native.get("activation_id") != source.get("activation_id")
        or native.get("activation_manifest_sha256")
        != source.get("activation_manifest_sha256")
        or native.get("artifact_sha256") != source.get("native_source_sha256")
        or any(
            reopened.get(name) != []
            for name in (
                "candidates",
                "trials",
                "candidate_registry",
                "trial_registry",
                "attempts",
                "expected_evaluations",
            )
        )
    ):
        _fail("UNOBSERVED_HORIZON_SUPPORT_BINDING_INVALID")


def _hex64(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        _fail(code)
    return value


def _reason(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value.encode("utf-8")) > 256
        or any(ord(character) < 32 for character in value)
    ):
        _fail(code)
    return value


def _normalized_state_context_selection(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _STATE_CONTEXT_SELECTION_FIELDS:
        _fail("CANARY_STATE_CONTEXT_SELECTION_FIELDS_INVALID")
    body = {key: item for key, item in value.items() if key != "receipt_sha256"}
    disposition_hashes = value.get("disposition_sha256")
    if (
        value.get("schema_version") != "fxpm.production-cell-state-context-selection.v1"
        or value.get("selection_rule")
        != "LARGEST_SOURCE_GEOMETRY_FEASIBLE_COMMON_CONTEXT_DESCENDING"
        or type(value.get("selected_common_context_bars")) is not int
        or value["selected_common_context_bars"] < 1
        or type(value.get("candidate_boundary_limited")) is not bool
        or type(disposition_hashes) is not list
        or not disposition_hashes
        or value.get("receipt_sha256") != _sha256(body)
        or value.get("disposition_roster_sha256") != _sha256(disposition_hashes)
        or type(value.get("within_selected_context_count")) is not int
        or type(value.get("exceeds_selected_context_count")) is not int
        or value["within_selected_context_count"] < 0
        or value["exceeds_selected_context_count"] < 0
        or value["within_selected_context_count"]
        + value["exceeds_selected_context_count"]
        != len(disposition_hashes)
    ):
        _fail("CANARY_STATE_CONTEXT_SELECTION_INVALID")
    _hex64(
        value.get("required_context_roster_sha256"),
        "CANARY_STATE_CONTEXT_REQUIREMENT_ROSTER_INVALID",
    )
    _hex64(
        value.get("disposition_roster_sha256"),
        "CANARY_STATE_CONTEXT_DISPOSITION_ROSTER_INVALID",
    )
    _hex64(
        value.get("receipt_sha256"),
        "CANARY_STATE_CONTEXT_SELECTION_RECEIPT_INVALID",
    )
    for digest in disposition_hashes:
        _hex64(digest, "CANARY_STATE_CONTEXT_DISPOSITION_INVALID")
    return json.loads(_canonical_bytes(value).decode("ascii"))


def _build_canary_state_context_admission_summary(
    *,
    process_body: Mapping[str, Any],
    cell_receipt: Mapping[str, Any],
    horizon_plan_body: Mapping[str, Any],
) -> dict[str, Any]:
    """Derive honest strategy and attempt admission counts from frozen plans."""

    if any(
        type(value) is not dict
        for value in (process_body, cell_receipt, horizon_plan_body)
    ):
        _fail("CANARY_STATE_CONTEXT_ADMISSION_INPUT_INVALID")
    process = dict(process_body)
    cell = dict(cell_receipt)
    horizon = dict(horizon_plan_body)
    process_receipt_sha256 = _sha256(process)
    cell_receipt_sha256 = cell.get("receipt_sha256")
    cell_body = {key: value for key, value in cell.items() if key != "receipt_sha256"}
    horizon_receipt_sha256 = _sha256(horizon)
    semantics = process.get("semantics")
    attempt_policy = (
        semantics.get("attempt_policy") if type(semantics) is dict else None
    )
    registry_order = (
        semantics.get("registry_order") if type(semantics) is dict else None
    )
    if (
        type(semantics) is not dict
        or type(attempt_policy) is not dict
        or type(registry_order) is not list
        or not registry_order
        or any(
            type(strategy_id) is not str or not strategy_id
            for strategy_id in registry_order
        )
        or len(registry_order) != len(set(registry_order))
        or semantics.get("registry_order_sha256") != _sha256(registry_order)
        or semantics.get("strategy_domain_count") != len(registry_order)
        or process.get("process_semantics_sha256") != _sha256(semantics)
        or attempt_policy.get("one_full_default_per_strategy") is not True
        or attempt_policy.get("default_attempt_count") != len(registry_order)
        or type(attempt_policy.get("additional_nondefault_attempt_count")) is not int
        or attempt_policy["additional_nondefault_attempt_count"] < 0
        or type(attempt_policy.get("attempt_count_per_observed_cell")) is not int
        or attempt_policy["attempt_count_per_observed_cell"]
        != len(registry_order) + attempt_policy["additional_nondefault_attempt_count"]
    ):
        _fail("CANARY_STATE_CONTEXT_PROCESS_SEMANTICS_INVALID")
    if (
        cell_receipt_sha256 != _sha256(cell_body)
        or cell.get("schema_version") != "fxpm.production-search-cell-plan-receipt.v2"
        or cell.get("mode") != "BOUNDED_RAW_WINNER_CANARY"
        or cell.get("search_process_receipt_sha256") != process_receipt_sha256
        or cell.get("attempt_count")
        != attempt_policy["attempt_count_per_observed_cell"]
    ):
        _fail("CANARY_STATE_CONTEXT_CELL_PLAN_INVALID")
    horizon_binding = cell.get("horizon_plan_binding")
    if (
        type(horizon_binding) is not dict
        or horizon_binding.get("plan_receipt_sha256") != horizon_receipt_sha256
        or horizon_binding.get("attempt_roster_sha256")
        != horizon.get("attempt_roster_sha256")
        or horizon_binding.get("trial_registry_sha256")
        != horizon.get("trial_registry_sha256")
        or horizon_binding.get("cell") != cell.get("cell")
        or horizon.get("cell") != cell.get("cell")
    ):
        _fail("CANARY_STATE_CONTEXT_HORIZON_BINDING_INVALID")

    selection = _normalized_state_context_selection(cell.get("state_context_selection"))
    trials = horizon.get("trials")
    trial_registry = horizon.get("trial_registry")
    attempts = horizon.get("attempts")
    candidates = horizon.get("candidates")
    default_count = attempt_policy["default_attempt_count"]
    additional_count = attempt_policy["additional_nondefault_attempt_count"]
    attempt_count = default_count + additional_count
    if (
        type(trials) is not list
        or type(trial_registry) is not list
        or type(attempts) is not list
        or type(candidates) is not list
        or len(candidates) != 1
        or len(trials) != attempt_count
        or len(trial_registry) != attempt_count
        or len(attempts) != attempt_count
        or horizon.get("trial_registry_sha256") != _sha256(trial_registry)
        or horizon.get("attempt_roster_sha256") != _sha256(attempts)
        or type(cell.get("boundary_limited")) is not bool
        or selection["candidate_boundary_limited"] is not cell["boundary_limited"]
        or selection["selected_common_context_bars"]
        != (cell.get("dependency_payload") or {}).get("feature_lookback_bars")
    ):
        _fail("CANARY_STATE_CONTEXT_HORIZON_ROSTER_INVALID")
    candidate = candidates[0]
    if type(candidate) is not dict:
        _fail("CANARY_STATE_CONTEXT_CANDIDATE_INVALID")
    candidate_folds = candidate.get("folds")
    expected_plan_status = (
        "HISTORICAL_BOUNDARY_LIMITED"
        if selection["candidate_boundary_limited"]
        else "READY_FOR_HISTORICAL_EVIDENCE"
    )
    if (
        type(candidate_folds) is not list
        or (not candidate_folds) is not selection["candidate_boundary_limited"]
        or horizon.get("plan_status") != expected_plan_status
    ):
        _fail("CANARY_STATE_CONTEXT_CANDIDATE_BOUNDARY_INVALID")
    candidate_id = candidate.get("candidate_id")
    candidate_sha256 = candidate.get("candidate_definition_sha256")
    if type(candidate_id) is not str or not candidate_id:
        _fail("CANARY_STATE_CONTEXT_CANDIDATE_INVALID")
    _hex64(candidate_sha256, "CANARY_STATE_CONTEXT_CANDIDATE_INVALID")

    strategy_ids: list[str] = []
    state_admitted: list[bool] = []
    disposition_hashes: list[str] = []
    execution_planned = 0
    execution_state_rejected = 0
    execution_source_rejected = 0
    execution_other_rejected = 0
    classifications: list[dict[str, Any]] = []
    registry_strategy_ids = set(registry_order)
    seen_trial_ids: set[str] = set()
    for ordinal, (trial, trial_registry_row, attempt) in enumerate(
        zip(trials, trial_registry, attempts, strict=True)
    ):
        if (
            type(trial) is not dict
            or type(trial_registry_row) is not dict
            or type(attempt) is not dict
        ):
            _fail("CANARY_STATE_CONTEXT_TRIAL_INVALID")
        trial_id = trial.get("trial_id")
        strategy_id = trial.get("strategy_id")
        disposition = trial.get("state_context_disposition")
        if (
            type(trial_id) is not str
            or not trial_id
            or trial_id in seen_trial_ids
            or type(strategy_id) is not str
            or not strategy_id
            or type(disposition) is not dict
            or set(disposition) != _STATE_CONTEXT_DISPOSITION_FIELDS
        ):
            _fail("CANARY_STATE_CONTEXT_TRIAL_INVALID")
        seen_trial_ids.add(trial_id)
        disposition_body = {
            key: value
            for key, value in disposition.items()
            if key != "disposition_sha256"
        }
        required = disposition.get("required_context_bars")
        selected = disposition.get("selected_context_bars")
        admitted = (
            type(required) is int
            and required >= 0
            and type(selected) is int
            and selected == selection["selected_common_context_bars"]
            and required <= selected
        )
        expected_status = "PLANNED" if admitted else "PROCESS_REJECTED"
        expected_reason = (
            None if admitted else "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
        )
        disposition_sha256 = disposition.get("disposition_sha256")
        expected_trial_registry_row = {
            "trial_id": trial_id,
            "strategy_id": strategy_id,
            "strategy_release_sha256": trial.get("strategy_release_sha256"),
            "parameter_set_sha256": trial.get("parameter_set_sha256"),
            "trial_definition_sha256": trial.get("trial_definition_sha256"),
            "state_context_disposition_sha256": disposition_sha256,
        }
        if (
            disposition.get("schema_version")
            != "fxpm.production-trial-state-context-disposition.v1"
            or disposition.get("candidate_id") != candidate_id
            or disposition.get("candidate_definition_sha256") != candidate_sha256
            or disposition.get("status") != expected_status
            or disposition.get("reason_code") != expected_reason
            or disposition_sha256 != _sha256(disposition_body)
            or trial_registry_row != expected_trial_registry_row
            or attempt.get("trial_id") != trial_id
            or attempt.get("trial_definition_sha256")
            != trial.get("trial_definition_sha256")
            or attempt.get("strategy_id") != strategy_id
            or attempt.get("strategy_release_sha256")
            != trial.get("strategy_release_sha256")
            or attempt.get("parameter_set_sha256") != trial.get("parameter_set_sha256")
            or attempt.get("candidate_id") != candidate_id
            or attempt.get("candidate_definition_sha256") != candidate_sha256
            or attempt.get("state_context_disposition_sha256") != disposition_sha256
        ):
            _fail("CANARY_STATE_CONTEXT_DISPOSITION_BINDING_INVALID")
        for digest in (
            trial.get("strategy_release_sha256"),
            trial.get("parameter_set_sha256"),
            trial.get("trial_definition_sha256"),
        ):
            _hex64(digest, "CANARY_STATE_CONTEXT_TRIAL_HASH_INVALID")
        _hex64(
            disposition.get("state_requirement_sha256"),
            "CANARY_STATE_CONTEXT_REQUIREMENT_INVALID",
        )
        _hex64(
            disposition_sha256,
            "CANARY_STATE_CONTEXT_DISPOSITION_INVALID",
        )
        execution_status = attempt.get("status")
        execution_reason = attempt.get("reason_code")
        if execution_status == "PLANNED" and execution_reason is None:
            execution_planned += 1
        elif execution_status == "PROCESS_REJECTED":
            if execution_reason == "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL":
                execution_state_rejected += 1
            elif execution_reason == "SOURCE_BOUNDARY_SHORTFALL":
                execution_source_rejected += 1
            else:
                execution_other_rejected += 1
        else:
            _fail("CANARY_STATE_CONTEXT_EXECUTION_STATUS_INVALID")
        strategy_ids.append(strategy_id)
        state_admitted.append(admitted)
        disposition_hashes.append(disposition_sha256)
        classifications.append(
            {
                "ordinal": ordinal,
                "strategy_id": strategy_id,
                "design_role": (
                    "REGISTRY_DEFAULT"
                    if ordinal < default_count
                    else "DETERMINISTIC_NONDEFAULT_GRID_POINT"
                ),
                "state_context_status": expected_status,
                "execution_status": execution_status,
                "execution_reason_code": execution_reason,
                "disposition_sha256": disposition_sha256,
            }
        )
        if ordinal < default_count and strategy_id != registry_order[ordinal]:
            _fail("CANARY_STATE_CONTEXT_DEFAULT_STRATEGY_ORDER_INVALID")
        if ordinal >= default_count and strategy_id not in registry_strategy_ids:
            _fail("CANARY_STATE_CONTEXT_VARIANT_STRATEGY_INVALID")

    state_admitted_count = sum(state_admitted)
    state_shortfall_count = attempt_count - state_admitted_count
    if (
        disposition_hashes != selection["disposition_sha256"]
        or selection["within_selected_context_count"] != state_admitted_count
        or selection["exceeds_selected_context_count"] != state_shortfall_count
        or (
            selection["candidate_boundary_limited"]
            and (
                execution_planned != 0
                or execution_state_rejected != 0
                or execution_source_rejected != attempt_count
                or execution_other_rejected != 0
            )
        )
        or (
            not selection["candidate_boundary_limited"]
            and (
                execution_planned != state_admitted_count
                or execution_state_rejected != state_shortfall_count
                or execution_source_rejected != 0
                or execution_other_rejected != 0
            )
        )
    ):
        _fail("CANARY_STATE_CONTEXT_ADMISSION_COUNTS_INVALID")

    by_strategy: dict[str, list[bool]] = {
        strategy_id: [] for strategy_id in registry_order
    }
    for strategy_id, admitted in zip(strategy_ids, state_admitted, strict=True):
        by_strategy[strategy_id].append(admitted)
    if any(not statuses for statuses in by_strategy.values()):
        _fail("CANARY_STATE_CONTEXT_STRATEGY_COVERAGE_INVALID")
    any_admitted = sum(any(statuses) for statuses in by_strategy.values())
    fully_admitted = sum(all(statuses) for statuses in by_strategy.values())
    partially_admitted = sum(
        any(statuses) and not all(statuses) for statuses in by_strategy.values()
    )
    fully_shortfall = sum(not any(statuses) for statuses in by_strategy.values())
    default_admitted = sum(state_admitted[:default_count])
    disposition_roster = [dict(trial["state_context_disposition"]) for trial in trials]
    body = {
        "schema_version": (BOUNDED_RAW_WINNER_CANARY_STATE_CONTEXT_ADMISSION_SCHEMA),
        "admission_scope": (
            "STATE_CONTEXT_REQUIREMENT_ONLY_CANDIDATE_BOUNDARY_REPORTED_SEPARATELY"
        ),
        "search_process_receipt_sha256": process_receipt_sha256,
        "process_semantics_sha256": process["process_semantics_sha256"],
        "registry_order_sha256": semantics["registry_order_sha256"],
        "cell_plan_receipt_sha256": cell_receipt_sha256,
        "horizon_plan_receipt_sha256": horizon_receipt_sha256,
        "horizon_trial_registry_sha256": horizon["trial_registry_sha256"],
        "horizon_trial_registry": trial_registry,
        "horizon_attempt_roster_sha256": horizon["attempt_roster_sha256"],
        "horizon_attempt_roster": attempts,
        "state_context_selection_receipt_sha256": selection["receipt_sha256"],
        "state_context_disposition_roster_sha256": selection[
            "disposition_roster_sha256"
        ],
        "state_context_disposition_body_roster_sha256": _sha256(disposition_roster),
        "state_context_disposition_roster": disposition_roster,
        "attempt_classification_roster": classifications,
        "attempt_classification_roster_sha256": _sha256(classifications),
        "selected_common_context_bars": selection["selected_common_context_bars"],
        "candidate_boundary_limited": selection["candidate_boundary_limited"],
        "attempt_count_exact": attempt_count,
        "additional_nondefault_attempt_count_exact": additional_count,
        "state_context_admitted_attempt_count": state_admitted_count,
        "state_context_shortfall_attempt_count": state_shortfall_count,
        "execution_planned_attempt_count": execution_planned,
        "execution_state_context_rejected_attempt_count": (execution_state_rejected),
        "execution_source_boundary_rejected_attempt_count": (execution_source_rejected),
        "execution_other_process_rejected_attempt_count": execution_other_rejected,
        "registry_default_strategy_count_exact": default_count,
        "registry_default_state_context_admitted_count": default_admitted,
        "registry_default_state_context_shortfall_count": (
            default_count - default_admitted
        ),
        "strategy_with_any_state_context_admitted_count": any_admitted,
        "strategy_fully_state_context_admitted_count": fully_admitted,
        "strategy_partially_state_context_admitted_count": partially_admitted,
        "strategy_fully_state_context_shortfall_count": fully_shortfall,
    }
    return {**body, "receipt_sha256": _sha256(body)}


def _verified_canary_state_context_admission_summary(
    value: Any,
    *,
    cell_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify the compact receipt against the independently reopened cell plan."""

    if (
        type(value) is not dict
        or set(value) != _CANARY_STATE_CONTEXT_ADMISSION_FIELDS
        or type(cell_receipt) is not dict
    ):
        _fail("CANARY_STATE_CONTEXT_ADMISSION_FIELDS_INVALID")
    body = {key: item for key, item in value.items() if key != "receipt_sha256"}
    selection = _normalized_state_context_selection(
        cell_receipt.get("state_context_selection")
    )
    horizon_binding = cell_receipt.get("horizon_plan_binding")
    if (
        value.get("schema_version")
        != BOUNDED_RAW_WINNER_CANARY_STATE_CONTEXT_ADMISSION_SCHEMA
        or value.get("admission_scope")
        != "STATE_CONTEXT_REQUIREMENT_ONLY_CANDIDATE_BOUNDARY_REPORTED_SEPARATELY"
        or value.get("receipt_sha256") != _sha256(body)
        or value.get("cell_plan_receipt_sha256") != cell_receipt.get("receipt_sha256")
        or value.get("search_process_receipt_sha256")
        != cell_receipt.get("search_process_receipt_sha256")
        or type(horizon_binding) is not dict
        or value.get("horizon_plan_receipt_sha256")
        != horizon_binding.get("plan_receipt_sha256")
        or value.get("horizon_trial_registry_sha256")
        != horizon_binding.get("trial_registry_sha256")
        or value.get("horizon_attempt_roster_sha256")
        != horizon_binding.get("attempt_roster_sha256")
        or value.get("state_context_selection_receipt_sha256")
        != selection["receipt_sha256"]
        or value.get("state_context_disposition_roster_sha256")
        != selection["disposition_roster_sha256"]
        or value.get("selected_common_context_bars")
        != selection["selected_common_context_bars"]
        or value.get("candidate_boundary_limited")
        is not selection["candidate_boundary_limited"]
    ):
        _fail("CANARY_STATE_CONTEXT_ADMISSION_BINDING_INVALID")
    for name in (
        "search_process_receipt_sha256",
        "process_semantics_sha256",
        "registry_order_sha256",
        "cell_plan_receipt_sha256",
        "horizon_plan_receipt_sha256",
        "horizon_trial_registry_sha256",
        "horizon_attempt_roster_sha256",
        "state_context_selection_receipt_sha256",
        "state_context_disposition_roster_sha256",
        "state_context_disposition_body_roster_sha256",
        "attempt_classification_roster_sha256",
        "receipt_sha256",
    ):
        _hex64(value.get(name), "CANARY_STATE_CONTEXT_ADMISSION_HASH_INVALID")
    count_names = (
        "attempt_count_exact",
        "additional_nondefault_attempt_count_exact",
        "state_context_admitted_attempt_count",
        "state_context_shortfall_attempt_count",
        "execution_planned_attempt_count",
        "execution_state_context_rejected_attempt_count",
        "execution_source_boundary_rejected_attempt_count",
        "execution_other_process_rejected_attempt_count",
        "registry_default_strategy_count_exact",
        "registry_default_state_context_admitted_count",
        "registry_default_state_context_shortfall_count",
        "strategy_with_any_state_context_admitted_count",
        "strategy_fully_state_context_admitted_count",
        "strategy_partially_state_context_admitted_count",
        "strategy_fully_state_context_shortfall_count",
    )
    if any(type(value.get(name)) is not int or value[name] < 0 for name in count_names):
        _fail("CANARY_STATE_CONTEXT_ADMISSION_COUNT_INVALID")
    attempt_count = value["attempt_count_exact"]
    strategy_count = value["registry_default_strategy_count_exact"]
    classifications = value.get("attempt_classification_roster")
    trial_registry = value.get("horizon_trial_registry")
    attempts = value.get("horizon_attempt_roster")
    dispositions = value.get("state_context_disposition_roster")
    if (
        type(classifications) is not list
        or type(trial_registry) is not list
        or type(attempts) is not list
        or type(dispositions) is not list
        or len(classifications) != attempt_count
        or len(trial_registry) != attempt_count
        or len(attempts) != attempt_count
        or len(dispositions) != attempt_count
        or value.get("attempt_classification_roster_sha256") != _sha256(classifications)
        or value.get("horizon_trial_registry_sha256") != _sha256(trial_registry)
        or value.get("horizon_attempt_roster_sha256") != _sha256(attempts)
        or value.get("state_context_disposition_body_roster_sha256")
        != _sha256(dispositions)
    ):
        _fail("CANARY_STATE_CONTEXT_CLASSIFICATION_ROSTER_INVALID")
    strategy_ids: list[str] = []
    state_admitted: list[bool] = []
    execution_planned = 0
    execution_state_rejected = 0
    execution_source_rejected = 0
    execution_other_rejected = 0
    disposition_hashes: list[str] = []
    expected_classifications: list[dict[str, Any]] = []
    expected_trial_registry_fields = {
        "trial_id",
        "strategy_id",
        "strategy_release_sha256",
        "parameter_set_sha256",
        "trial_definition_sha256",
        "state_context_disposition_sha256",
    }
    expected_attempt_fields = {
        "attempt_id",
        "candidate_id",
        "candidate_definition_sha256",
        "trial_id",
        "trial_definition_sha256",
        "strategy_id",
        "strategy_release_sha256",
        "parameter_set_sha256",
        "status",
        "reason_code",
        "state_context_disposition_sha256",
    }
    for ordinal, (trial, attempt, disposition) in enumerate(
        zip(trial_registry, attempts, dispositions, strict=True)
    ):
        if (
            type(trial) is not dict
            or set(trial) != expected_trial_registry_fields
            or type(attempt) is not dict
            or set(attempt) != expected_attempt_fields
            or type(disposition) is not dict
            or set(disposition) != _STATE_CONTEXT_DISPOSITION_FIELDS
            or type(trial.get("strategy_id")) is not str
            or not trial["strategy_id"]
        ):
            _fail("CANARY_STATE_CONTEXT_CLASSIFICATION_SOURCE_INVALID")
        disposition_body = {
            key: item
            for key, item in disposition.items()
            if key != "disposition_sha256"
        }
        required = disposition.get("required_context_bars")
        selected = disposition.get("selected_context_bars")
        admitted = (
            type(required) is int
            and required >= 0
            and type(selected) is int
            and selected == selection["selected_common_context_bars"]
            and required <= selected
        )
        state_status = "PLANNED" if admitted else "PROCESS_REJECTED"
        state_reason = None if admitted else "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
        disposition_sha256 = disposition.get("disposition_sha256")
        if selection["candidate_boundary_limited"]:
            expected_execution_status = "PROCESS_REJECTED"
            expected_execution_reason = "SOURCE_BOUNDARY_SHORTFALL"
        else:
            expected_execution_status = state_status
            expected_execution_reason = state_reason
        if (
            disposition.get("schema_version")
            != "fxpm.production-trial-state-context-disposition.v1"
            or disposition.get("status") != state_status
            or disposition.get("reason_code") != state_reason
            or disposition_sha256 != _sha256(disposition_body)
            or trial.get("state_context_disposition_sha256") != disposition_sha256
            or attempt.get("state_context_disposition_sha256") != disposition_sha256
            or attempt.get("trial_id") != trial.get("trial_id")
            or attempt.get("trial_definition_sha256")
            != trial.get("trial_definition_sha256")
            or attempt.get("strategy_id") != trial.get("strategy_id")
            or attempt.get("strategy_release_sha256")
            != trial.get("strategy_release_sha256")
            or attempt.get("parameter_set_sha256") != trial.get("parameter_set_sha256")
            or attempt.get("candidate_id") != disposition.get("candidate_id")
            or attempt.get("candidate_definition_sha256")
            != disposition.get("candidate_definition_sha256")
            or attempt.get("status") != expected_execution_status
            or attempt.get("reason_code") != expected_execution_reason
        ):
            _fail("CANARY_STATE_CONTEXT_CLASSIFICATION_SOURCE_BINDING_INVALID")
        for digest in (
            trial.get("strategy_release_sha256"),
            trial.get("parameter_set_sha256"),
            trial.get("trial_definition_sha256"),
            disposition.get("state_requirement_sha256"),
            disposition_sha256,
        ):
            _hex64(digest, "CANARY_STATE_CONTEXT_CLASSIFICATION_SOURCE_INVALID")
        disposition_hashes.append(disposition_sha256)
        strategy_ids.append(trial["strategy_id"])
        state_admitted.append(admitted)
        if expected_execution_status == "PLANNED":
            execution_planned += 1
        else:
            if expected_execution_reason == "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL":
                execution_state_rejected += 1
            elif expected_execution_reason == "SOURCE_BOUNDARY_SHORTFALL":
                execution_source_rejected += 1
            else:
                execution_other_rejected += 1
        expected_classifications.append(
            {
                "ordinal": ordinal,
                "strategy_id": trial["strategy_id"],
                "design_role": (
                    "REGISTRY_DEFAULT"
                    if ordinal < strategy_count
                    else "DETERMINISTIC_NONDEFAULT_GRID_POINT"
                ),
                "state_context_status": state_status,
                "execution_status": expected_execution_status,
                "execution_reason_code": expected_execution_reason,
                "disposition_sha256": disposition_sha256,
            }
        )
    if classifications != expected_classifications:
        _fail("CANARY_STATE_CONTEXT_CLASSIFICATION_CHANGED")
    default_strategy_ids = strategy_ids[:strategy_count]
    default_strategy_set = set(default_strategy_ids)
    if (
        len(default_strategy_set) != strategy_count
        or _sha256(default_strategy_ids) != value["registry_order_sha256"]
        or any(
            strategy_id not in default_strategy_set
            for strategy_id in strategy_ids[strategy_count:]
        )
        or disposition_hashes != selection["disposition_sha256"]
    ):
        _fail("CANARY_STATE_CONTEXT_CLASSIFICATION_BINDING_INVALID")
    by_strategy: dict[str, list[bool]] = {
        strategy_id: [] for strategy_id in default_strategy_ids
    }
    for strategy_id, admitted in zip(strategy_ids, state_admitted, strict=True):
        by_strategy[strategy_id].append(admitted)
    derived_counts = {
        "state_context_admitted_attempt_count": sum(state_admitted),
        "state_context_shortfall_attempt_count": attempt_count - sum(state_admitted),
        "execution_planned_attempt_count": execution_planned,
        "execution_state_context_rejected_attempt_count": (execution_state_rejected),
        "execution_source_boundary_rejected_attempt_count": (execution_source_rejected),
        "execution_other_process_rejected_attempt_count": execution_other_rejected,
        "registry_default_state_context_admitted_count": sum(
            state_admitted[:strategy_count]
        ),
        "registry_default_state_context_shortfall_count": strategy_count
        - sum(state_admitted[:strategy_count]),
        "strategy_with_any_state_context_admitted_count": sum(
            any(statuses) for statuses in by_strategy.values()
        ),
        "strategy_fully_state_context_admitted_count": sum(
            all(statuses) for statuses in by_strategy.values()
        ),
        "strategy_partially_state_context_admitted_count": sum(
            any(statuses) and not all(statuses) for statuses in by_strategy.values()
        ),
        "strategy_fully_state_context_shortfall_count": sum(
            not any(statuses) for statuses in by_strategy.values()
        ),
    }
    if any(value.get(name) != expected for name, expected in derived_counts.items()):
        _fail("CANARY_STATE_CONTEXT_CLASSIFICATION_COUNTS_INVALID")
    if (
        attempt_count != cell_receipt.get("attempt_count")
        or attempt_count != len(selection["disposition_sha256"])
        or strategy_count <= 0
        or strategy_count > attempt_count
        or value["additional_nondefault_attempt_count_exact"]
        != attempt_count - strategy_count
        or value["state_context_admitted_attempt_count"]
        != selection["within_selected_context_count"]
        or value["state_context_shortfall_attempt_count"]
        != selection["exceeds_selected_context_count"]
        or value["state_context_admitted_attempt_count"]
        + value["state_context_shortfall_attempt_count"]
        != attempt_count
        or value["registry_default_state_context_admitted_count"]
        + value["registry_default_state_context_shortfall_count"]
        != strategy_count
        or value["strategy_fully_state_context_admitted_count"]
        + value["strategy_partially_state_context_admitted_count"]
        + value["strategy_fully_state_context_shortfall_count"]
        != strategy_count
        or value["strategy_with_any_state_context_admitted_count"]
        != value["strategy_fully_state_context_admitted_count"]
        + value["strategy_partially_state_context_admitted_count"]
        or value["execution_planned_attempt_count"]
        + value["execution_state_context_rejected_attempt_count"]
        + value["execution_source_boundary_rejected_attempt_count"]
        + value["execution_other_process_rejected_attempt_count"]
        != attempt_count
    ):
        _fail("CANARY_STATE_CONTEXT_ADMISSION_COUNT_INVALID")
    if selection["candidate_boundary_limited"]:
        valid_execution_counts = (
            value["execution_planned_attempt_count"] == 0
            and value["execution_state_context_rejected_attempt_count"] == 0
            and value["execution_source_boundary_rejected_attempt_count"]
            == attempt_count
            and value["execution_other_process_rejected_attempt_count"] == 0
        )
    else:
        valid_execution_counts = (
            value["execution_planned_attempt_count"]
            == value["state_context_admitted_attempt_count"]
            and value["execution_state_context_rejected_attempt_count"]
            == value["state_context_shortfall_attempt_count"]
            and value["execution_source_boundary_rejected_attempt_count"] == 0
            and value["execution_other_process_rejected_attempt_count"] == 0
        )
    if not valid_execution_counts:
        _fail("CANARY_STATE_CONTEXT_EXECUTION_COUNTS_INVALID")
    return json.loads(_canonical_bytes(value).decode("ascii"))


def _bounded_canary_failure_diagnostic(
    *,
    exception_type: Any,
    failure_details: Any,
    attempt_ordinal: Any,
    fold_ordinal: Any,
    attempted_trials_exact: int,
) -> dict[str, Any]:
    """Validate the small diagnostic carried by a non-authorizing canary failure."""

    if (
        type(attempted_trials_exact) is not int
        or not 0
        <= attempted_trials_exact
        <= winner_selection.MAX_ATTEMPTS
    ):
        _fail("CANARY_FAILURE_ATTEMPT_COUNT_INVALID")
    if exception_type is not None:
        if (
            type(exception_type) is not str
            or not exception_type
            or exception_type != exception_type.strip()
            or len(exception_type.encode("utf-8"))
            > BOUNDED_RAW_WINNER_CANARY_EXCEPTION_TYPE_BYTES
            or not all(character.isprintable() for character in exception_type)
        ):
            _fail("CANARY_FAILURE_EXCEPTION_TYPE_INVALID")
    if type(failure_details) not in {list, tuple} or not (
        0 <= len(failure_details) <= BOUNDED_RAW_WINNER_CANARY_FAILURE_DETAIL_LIMIT
    ):
        _fail("CANARY_FAILURE_DETAILS_INVALID")
    details: list[str] = []
    for value in failure_details:
        if (
            type(value) is not str
            or not value
            or value != value.strip()
            or len(value.encode("utf-8"))
            > BOUNDED_RAW_WINNER_CANARY_FAILURE_DETAIL_BYTES
            or not all(character.isprintable() for character in value)
        ):
            _fail("CANARY_FAILURE_DETAILS_INVALID")
        details.append(value)
    if exception_type is None and details:
        _fail("CANARY_FAILURE_DETAILS_WITHOUT_EXCEPTION")
    if attempt_ordinal is not None and (
        type(attempt_ordinal) is not int
        or not 0 <= attempt_ordinal < attempted_trials_exact
    ):
        _fail("CANARY_FAILURE_ATTEMPT_ORDINAL_INVALID")
    if fold_ordinal is not None and (
        type(fold_ordinal) is not int
        or attempt_ordinal is None
        or not 0 <= fold_ordinal < winner_selection.MAX_FOLDS
    ):
        _fail("CANARY_FAILURE_FOLD_ORDINAL_INVALID")
    return {
        "exception_type": exception_type,
        "failure_details": details,
        "attempt_ordinal": attempt_ordinal,
        "fold_ordinal": fold_ordinal,
    }


def _verified_canary_failure_body(
    value: Any,
    *,
    request_sha256: str,
    anchor_id: str,
    symbol: str,
    timeframe: str,
    regime: str,
    failure_code: str,
    attempted_trials_exact: int,
    frozen_receipt_sha256: str | None,
) -> dict[str, Any]:
    """Reopen an exact v1/v2 canary failure body without granting authority."""

    if type(value) is not dict:
        _fail("CANARY_TECHNICAL_FAILURE_FIELDS_INVALID")
    schema = value.get("schema_version")
    if schema == _BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA_V1:
        expected_fields = _CANARY_FAILURE_V1_FIELDS
    elif schema == BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA:
        expected_fields = _CANARY_FAILURE_V2_FIELDS
    else:
        _fail("CANARY_TECHNICAL_FAILURE_SCHEMA_INVALID")
    if set(value) != expected_fields:
        _fail("CANARY_TECHNICAL_FAILURE_FIELDS_INVALID")
    code = _reason(failure_code, "CANARY_FAILURE_CODE_INVALID")
    if frozen_receipt_sha256 is not None:
        _hex64(
            frozen_receipt_sha256,
            "CANARY_TECHNICAL_FAILURE_FROZEN_RECEIPT_INVALID",
        )
    search_outcome = value.get("search_outcome")
    if search_outcome is not None and type(search_outcome) is not dict:
        _fail("CANARY_TECHNICAL_FAILURE_SEARCH_OUTCOME_INVALID")
    if (
        value.get("authority_scope") != orchestration.BOUNDED_RAW_WINNER_CANARY_SCOPE
        or value.get("request_sha256") != request_sha256
        or value.get("anchor_id") != anchor_id
        or value.get("symbol") != symbol
        or value.get("timeframe") != timeframe
        or value.get("regime") != regime
        or value.get("failure_code") != code
        or value.get("attempted_trials_exact") != attempted_trials_exact
        or value.get("frozen_receipt_sha256") != frozen_receipt_sha256
        or value.get("economic_no_trade_or_no_winner") is not False
        or value.get("downstream_authority") is not False
    ):
        _fail("CANARY_TECHNICAL_FAILURE_BINDING_INVALID")
    if schema == BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA:
        diagnostic = _bounded_canary_failure_diagnostic(
            exception_type=value["exception_type"],
            failure_details=value["failure_details"],
            attempt_ordinal=value["attempt_ordinal"],
            fold_ordinal=value["fold_ordinal"],
            attempted_trials_exact=attempted_trials_exact,
        )
        if any(value[name] != expected for name, expected in diagnostic.items()):
            _fail("CANARY_TECHNICAL_FAILURE_DIAGNOSTIC_INVALID")
    try:
        return json.loads(_canonical_bytes(value).decode("ascii"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise HistoricalProductionAdapterError(
            "CANARY_TECHNICAL_FAILURE_CANONICAL_INVALID"
        ) from exc


def _descriptor(value: Any, code: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _DESCRIPTOR_FIELDS:
        _fail(code)
    if (
        value.get("schema_version") != authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA
        or type(value.get("relative_path")) is not str
        or not value["relative_path"]
        or type(value.get("size_bytes")) is not int
        or value["size_bytes"] <= 0
        or type(value.get("format")) is not str
        or not value["format"]
    ):
        _fail(code)
    _hex64(value.get("sha256"), code)
    return json.loads(_canonical_bytes(value).decode("ascii"))


def _artifact_entry(value: Any, code: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _ARTIFACT_ENTRY_FIELDS:
        _fail(code)
    artifact_id = value.get("artifact_id")
    artifact_type = value.get("artifact_type")
    if (
        type(artifact_id) is not str
        or not artifact_id
        or artifact_id != artifact_id.strip()
        or len(artifact_id.encode("utf-8")) > 1024
        or type(artifact_type) is not str
        or not artifact_type
        or artifact_type != artifact_type.strip()
        or len(artifact_type.encode("utf-8")) > 256
    ):
        _fail(code)
    return {
        "artifact_id": artifact_id,
        "artifact_type": artifact_type,
        "descriptor": _descriptor(value.get("descriptor"), code),
    }


def _reopen_winner_raw_production_receipt(
    context: authority_spool.GenerationSpoolContext,
    descriptor: Mapping[str, Any],
) -> bytes:
    """Reopen exact producer bytes through the spool's bounded safe reader."""

    code = "WINNER_RAW_PRODUCTION_RECEIPT_REOPEN_FAILED"
    normalized = _descriptor(dict(descriptor), code)
    try:
        expected_path = authority_spool._content_relative_path(
            normalized["sha256"], normalized["format"]
        )
        maximum_bytes = min(
            context.limits.max_artifact_bytes,
            context.limits.max_json_bytes,
        )
        if (
            normalized["format"] != raw_winner.ASCII_JSON_FORMAT
            or normalized["relative_path"] != expected_path
            or normalized["size_bytes"] > maximum_bytes
        ):
            _fail(code)
        digest, size, encoded = authority_spool._stream_file(
            context.root,
            normalized["relative_path"],
            maximum_bytes=maximum_bytes,
            expected_size=normalized["size_bytes"],
            collect=True,
            chunk_bytes=context.limits.stream_chunk_bytes,
        )
        if (
            encoded is None
            or digest != normalized["sha256"]
            or size != normalized["size_bytes"]
        ):
            _fail(code)
        body = json.loads(encoded.decode("ascii"))
        if type(body) is not dict or _canonical_bytes(body) != encoded:
            _fail(code)
    except HistoricalProductionAdapterError:
        raise
    except Exception as exc:
        raise HistoricalProductionAdapterError(code) from exc
    return encoded


def _reopen_ascii_json_entry(
    context: authority_spool.GenerationSpoolContext,
    value: Mapping[str, Any],
    *,
    expected_type: str,
    code: str,
) -> dict[str, Any]:
    entry = _artifact_entry(dict(value), code)
    if entry["artifact_type"] != expected_type:
        _fail(code)
    descriptor = entry["descriptor"]
    if descriptor["format"] == raw_winner.ASCII_JSON_FORMAT:
        encoded = _reopen_winner_raw_production_receipt(context, descriptor)
    elif descriptor["format"] == authority_spool.CANONICAL_JSON_FORMAT:
        try:
            expected_path = authority_spool._content_relative_path(
                descriptor["sha256"], descriptor["format"]
            )
            digest, size, encoded = authority_spool._stream_file(
                context.root,
                descriptor["relative_path"],
                maximum_bytes=min(
                    context.limits.max_artifact_bytes,
                    context.limits.max_json_bytes,
                ),
                expected_size=descriptor["size_bytes"],
                collect=True,
                chunk_bytes=context.limits.stream_chunk_bytes,
            )
            if (
                descriptor["relative_path"] != expected_path
                or encoded is None
                or digest != descriptor["sha256"]
                or size != descriptor["size_bytes"]
            ):
                _fail(code)
        except HistoricalProductionAdapterError:
            raise
        except Exception as exc:
            raise HistoricalProductionAdapterError(code) from exc
    else:
        _fail(code)
    try:
        body = json.loads(encoded.decode("ascii"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise HistoricalProductionAdapterError(code) from exc
    if type(body) is not dict:
        _fail(code)
    return body


def _frame_sha256(frame: pd.DataFrame) -> str:
    """Hash the exact fit frame with the authoritative executor domain."""

    try:
        hashed = pd.util.hash_pandas_object(frame, index=True).to_numpy(dtype="<u8")
    except Exception as exc:
        raise HistoricalProductionAdapterError("FIT_FRAME_HASH_FAILED") from exc
    digest = hashlib.sha256()
    digest.update(b"fxpm-authoritative-search-frame-v1\x00")
    digest.update(_canonical_bytes([str(value) for value in frame.columns]))
    digest.update(hashed.tobytes(order="C"))
    return digest.hexdigest()


def _strict_attempt_artifacts(
    raw_attempts: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    plan_body: Mapping[str, Any],
    plan_receipt_sha256: str,
    native_prefix_product: native_prefix.NativePrefixProduct,
) -> tuple[
    tuple[dict[str, Any], ...],
    tuple[dict[str, Any], ...],
    tuple[dict[str, Any], ...],
]:
    """Reopen native bytes and verify every executable attempt before freeze."""

    if type(raw_attempts) not in {list, tuple} or not raw_attempts:
        _fail("ATTEMPT_ARTIFACT_ROSTER_INVALID")
    plan = dict(plan_body)
    prefix = plan.get("native_prefix")
    candidates = plan.get("candidates")
    trials = plan.get("trials")
    planned_attempts = plan.get("attempts")
    if (
        type(prefix) is not dict
        or type(candidates) is not list
        or type(trials) is not list
        or type(planned_attempts) is not list
        or len(planned_attempts) != len(raw_attempts)
        or type(native_prefix_product) is not native_prefix.NativePrefixProduct
    ):
        _fail("ATTEMPT_SOURCE_AUTHORITY_INVALID")
    try:
        frame = native_prefix.open_verified_prefix_frame(native_prefix_product)
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "ATTEMPT_SOURCE_PREFIX_REOPEN_FAILED"
        ) from exc
    if (
        native_prefix_product.source_state != native_prefix.AVAILABLE
        or native_prefix_product.index_sha256 != prefix.get("prefix_index_sha256")
        or dict(native_prefix_product.native_prefix) != prefix
        or plan.get("cell")
        != {"symbol": symbol, "timeframe": timeframe, "regime": regime}
    ):
        _fail("ATTEMPT_SOURCE_PREFIX_BINDING_INVALID")

    candidate_by_id = {
        value.get("candidate_id"): value for value in candidates if type(value) is dict
    }
    trial_by_id = {
        value.get("trial_id"): value for value in trials if type(value) is dict
    }
    trial_by_identity = {
        (
            value.get("strategy_id"),
            value.get("strategy_release_sha256"),
            value.get("parameter_set_sha256"),
        ): value
        for value in trials
        if type(value) is dict
    }
    plan_attempt_by_pair = {
        (value.get("candidate_id"), value.get("trial_id")): value
        for value in planned_attempts
        if type(value) is dict
    }
    if (
        len(candidate_by_id) != len(candidates)
        or len(trial_by_id) != len(trials)
        or len(trial_by_identity) != len(trials)
        or len(plan_attempt_by_pair) != len(planned_attempts)
    ):
        _fail("ATTEMPT_SOURCE_PLAN_ROSTER_INVALID")

    prefix_end = _utc_seconds(
        prefix.get("prefix_end_exclusive_utc"), "ATTEMPT_SOURCE_PREFIX_END_INVALID"
    )
    prefix_latest = _utc_seconds(
        prefix.get("prefix_last_close_utc"), "ATTEMPT_SOURCE_PREFIX_LATEST_INVALID"
    )
    prefix_content_sha256 = _hex64(
        prefix.get("prefix_content_sha256"),
        "ATTEMPT_SOURCE_PREFIX_CONTENT_INVALID",
    )
    opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(
        pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    )
    if opens.isna().any() or closes.isna().any():
        _fail("ATTEMPT_SOURCE_FRAME_TIMESTAMP_INVALID")

    # This module is imported by the authoritative executor, so keep the
    # reverse dependency local.  Reusing the executor's helper is intentional:
    # the planner, executor, and this independent reopen must realize the exact
    # same cutoff-anchored window rather than duplicating calendar arithmetic.
    try:
        import pm_authoritative_search_execution as search_execution
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "ATTEMPT_SOURCE_CUTOFF_HELPER_UNAVAILABLE"
        ) from exc

    fit_end_ts = pd.Timestamp(prefix_end)
    candidate_windows: dict[tuple[str, int], tuple[dict[str, Any], str, int, str]] = {}
    for candidate in candidates:
        if type(candidate) is not dict:
            _fail("ATTEMPT_SOURCE_CANDIDATE_SEMANTIC_IDENTITY_INVALID")
        identity = (candidate.get("lookback_kind"), candidate.get("lookback_value"))
        if (
            type(identity[0]) is not str
            or type(identity[1]) is not int
            or identity in candidate_windows
        ):
            _fail("ATTEMPT_SOURCE_CANDIDATE_SEMANTIC_IDENTITY_INVALID")
        try:
            expected_start_ts, expected_count = (
                search_execution._realize_cutoff_refit_window(
                    opens=opens,
                    closes=closes,
                    fit_end=fit_end_ts,
                    lookback_kind=identity[0],
                    lookback_value=identity[1],
                    state_context_bars=0,
                    boundary_limited=True,
                )
            )
        except search_execution.AuthoritativeSearchExecutionError as exc:
            raise HistoricalProductionAdapterError(
                "ATTEMPT_SOURCE_CUTOFF_REFIT_WINDOW_INVALID"
            ) from exc
        expected_start = _utc_seconds(
            expected_start_ts.isoformat(),
            "ATTEMPT_SOURCE_FIT_START_INVALID",
        )
        mask = (
            (opens >= expected_start_ts) & (opens < fit_end_ts) & (closes <= fit_end_ts)
        )
        selected = frame.loc[mask].copy(deep=True)
        source_bar_count = len(selected)
        if (
            source_bar_count != expected_count
            or not 2 <= source_bar_count <= 10_000_000
            or selected.empty
            or _utc_seconds(
                pd.Timestamp(selected["_BAR_CLOSE_TS"].iloc[-1]).isoformat(),
                "ATTEMPT_SOURCE_SELECTED_LATEST_INVALID",
            )
            != prefix_latest
        ):
            _fail("ATTEMPT_SOURCE_INTERVAL_BINDING_INVALID")
        candidate_windows[identity] = (
            candidate,
            expected_start,
            source_bar_count,
            _frame_sha256(selected),
        )

    normalized: list[dict[str, Any]] = []
    execution_contracts: list[dict[str, Any]] = []
    count_receipts: list[dict[str, Any]] = []
    mapped_attempt_ids: list[str] = []
    for ordinal, raw in enumerate(raw_attempts):
        code = f"ATTEMPT_{ordinal:04d}_EXECUTABLE_ARTIFACT_INVALID"
        if type(raw) is not dict or set(raw) != _ATTEMPT_ARTIFACT_KEYS:
            _fail(code)
        bodies = {name: raw.get(name) for name in _ATTEMPT_ARTIFACT_KEYS}
        if any(type(body) is not dict for body in bodies.values()):
            _fail(code)
        process = bodies["winner_process"]
        horizon = bodies["horizon_process"]
        trial_definition = bodies["trial_definition"]
        recipe = bodies["refit_recipe"]
        assert isinstance(process, dict)
        assert isinstance(horizon, dict)
        assert isinstance(trial_definition, dict)
        assert isinstance(recipe, dict)

        candidate_window = candidate_windows.get(
            (horizon.get("lookback_kind"), horizon.get("lookback_value"))
        )
        parameter_sha256 = _sha256(trial_definition.get("parameters"))
        trial = trial_by_identity.get(
            (
                process.get("strategy_id"),
                process.get("strategy_code_sha256"),
                parameter_sha256,
            )
        )
        if candidate_window is None or trial is None:
            _fail("ATTEMPT_SOURCE_PLAN_MAPPING_INVALID")
        candidate, fit_start, source_bar_count, fit_frame_sha256 = candidate_window
        planned = plan_attempt_by_pair.get(
            (candidate.get("candidate_id"), trial.get("trial_id"))
        )
        if (
            type(planned) is not dict
            or planned.get("attempt_id") in mapped_attempt_ids
            or planned.get("strategy_id") != process.get("strategy_id")
        ):
            _fail("ATTEMPT_SOURCE_PLAN_MAPPING_INVALID")

        if (
            _utc_seconds(
                horizon.get("fit_start_utc"), "ATTEMPT_SOURCE_FIT_START_INVALID"
            )
            != fit_start
            or _utc_seconds(
                horizon.get("fit_end_utc"), "ATTEMPT_SOURCE_FIT_END_INVALID"
            )
            != prefix_end
            or _utc_seconds(
                horizon.get("latest_closed_bar_utc"),
                "ATTEMPT_SOURCE_LATEST_CLOSED_INVALID",
            )
            != prefix_latest
            or horizon.get("source_snapshot_sha256") != prefix_content_sha256
            or horizon.get("source_bar_count") != source_bar_count
        ):
            _fail("ATTEMPT_SOURCE_CUTOFF_REFIT_WINDOW_INVALID")
        lookback_kind = candidate.get("lookback_kind")
        lookback_value = candidate.get("lookback_value")
        if (
            type(lookback_kind) is not str
            or type(lookback_value) is not int
            or (
                lookback_kind == "NATIVE_OBSERVATIONS"
                and lookback_value != source_bar_count
            )
        ):
            _fail("ATTEMPT_SOURCE_LOOKBACK_BINDING_INVALID")
        contract = {
            "lookback_kind": lookback_kind,
            "lookback_value": lookback_value,
            "source_bar_count": source_bar_count,
        }
        receipt_body = {
            "schema_version": SOURCE_COUNT_RECEIPT_SCHEMA,
            "anchor_id": plan["anchor"]["anchor_id"],
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": regime,
            "horizon_plan_receipt_sha256": _hex64(
                plan_receipt_sha256, "ATTEMPT_SOURCE_PLAN_RECEIPT_INVALID"
            ),
            "native_prefix_index_sha256": native_prefix_product.index_sha256,
            "native_prefix_content_sha256": prefix_content_sha256,
            "horizon_attempt_id": planned["attempt_id"],
            "horizon_candidate_id": candidate["candidate_id"],
            "horizon_candidate_definition_sha256": candidate[
                "candidate_definition_sha256"
            ],
            "lookback_kind": lookback_kind,
            "lookback_value": lookback_value,
            "fit_start_utc": fit_start,
            "fit_end_exclusive_utc": prefix_end,
            "latest_closed_bar_utc": prefix_latest,
            "source_bar_count": source_bar_count,
            "fit_frame_sha256": fit_frame_sha256,
        }
        receipt = {**receipt_body, "receipt_sha256": _sha256(receipt_body)}
        try:
            verified = current_refit.verify_current_refit_search_artifacts_bytes_v2(
                winner_process_bytes=_canonical_bytes(process),
                horizon_process_bytes=_canonical_bytes(horizon),
                trial_definition_bytes=_canonical_bytes(trial_definition),
                refit_recipe_bytes=_canonical_bytes(recipe),
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                strategy_id=trial.get("strategy_id"),
                lookback_kind=lookback_kind,
                lookback_value=lookback_value,
                fit_start_utc=fit_start,
                fit_end_utc=prefix_end,
                latest_closed_bar_utc=prefix_latest,
                source_snapshot_sha256=prefix_content_sha256,
                source_bar_count=source_bar_count,
                seed=recipe.get("seed"),
            )
        except Exception as exc:
            raise HistoricalProductionAdapterError(code) from exc
        normalized.append(
            {
                "winner_process": verified.winner_process,
                "horizon_process": verified.horizon_process,
                "trial_definition": verified.trial_definition,
                "refit_recipe": verified.refit_recipe,
            }
        )
        execution_contracts.append(contract)
        count_receipts.append(receipt)
        mapped_attempt_ids.append(planned["attempt_id"])
    if mapped_attempt_ids != [value.get("attempt_id") for value in planned_attempts]:
        _fail("ATTEMPT_SOURCE_PLAN_ORDER_OR_COVERAGE_INVALID")
    return tuple(normalized), tuple(execution_contracts), tuple(count_receipts)


def _raw_namespace_supported(function: Any) -> bool:
    try:
        parameter = inspect.signature(function).parameters.get("artifact_namespace")
    except (TypeError, ValueError):
        return False
    return parameter is not None


def _precommitted_evidence_roles(
    plan_body: Mapping[str, Any], attempt_count: int
) -> tuple[str, ...]:
    """Map the verified Horizon attempt states into Winner's frozen roles."""

    planned_attempts = plan_body.get("attempts")
    if type(planned_attempts) is not list or len(planned_attempts) != attempt_count:
        _fail("PRECOMMITTED_EVIDENCE_ROLE_ROSTER_INVALID")
    roles: list[str] = []
    for attempt in planned_attempts:
        status = attempt.get("status") if type(attempt) is dict else None
        if status == "PLANNED":
            roles.append("EVALUATION_REQUIRED")
        elif status == "PROCESS_REJECTED":
            roles.append("PRESEARCH_PROCESS_REJECTED")
        else:
            _fail("PRECOMMITTED_EVIDENCE_ROLE_HORIZON_SPLICE")
    return tuple(roles)


def _all_precommitted_attempts_process_rejected(
    *,
    request: Any,
    decision: Any,
    attempted_trials_exact: int,
) -> bool:
    """Project Winner v13's role-aware rejected roster without relabelling it."""

    decision_body = decision.to_dict()
    precommitment = request.precommitment.to_dict()
    attempts = request.attempt_ledger.to_dict().get("attempts")
    entries = request.evidence_ledger.to_dict().get("entries")
    fold_plan = precommitment.get("causal_fold_plan")
    if (
        decision_body.get("status") != "NO_PROCESS_WINNER"
        or decision_body.get("winner") is not None
        or type(attempts) is not list
        or type(entries) is not list
        or type(fold_plan) is not list
        or len(attempts) != attempted_trials_exact
        or len(entries) != attempted_trials_exact
        or attempted_trials_exact <= 0
    ):
        return False
    required_pbo_folds = len(fold_plan)
    for attempt, entry in zip(attempts, entries, strict=True):
        if (
            type(attempt) is not dict
            or type(entry) is not dict
            or entry.get("evidence_status") != "PROCESS_REJECTED"
            or entry.get("fold_count") != 0
            or entry.get("folds") != []
        ):
            return False
        role = attempt.get("precommitted_evidence_role")
        if role == "EVALUATION_REQUIRED":
            if (
                entry.get("status_reason") != "EVALUATED_OOS_PROCESS_REJECTED"
                or entry.get("pbo_evidence_source")
                != "COMPLETE_EVALUATION_REQUIRED_FOLDS"
                or entry.get("pbo_fold_count") != required_pbo_folds
                or type(entry.get("pbo_folds")) is not list
                or len(entry["pbo_folds"]) != required_pbo_folds
            ):
                return False
        elif role == "PRESEARCH_PROCESS_REJECTED":
            if (
                entry.get("status_reason") != "PRESEARCH_PROCESS_REJECTED"
                or entry.get("pbo_evidence_source")
                != "NOT_APPLICABLE_PRESEARCH_REJECTION"
                or entry.get("pbo_fold_count") != 0
                or entry.get("pbo_folds") != []
            ):
                return False
        else:
            return False
    return True


def _utc_seconds(value: Any, code: str) -> str:
    if type(value) is not str or not 1 <= len(value) <= 64:
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HistoricalProductionAdapterError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    return (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _cross_binding_body(
    *,
    plan_body: Mapping[str, Any],
    plan_receipt_sha256: str,
    frozen: raw_winner.FrozenWinnerSearchPlan,
    attempt_artifacts: Sequence[Mapping[str, Any]],
    producer_request_sha256: str,
    anchor_id: str,
    artifact_namespace: str,
    source_group_decision_sha256: str,
    regime_fit_decision_sha256: str,
    horizon_execution_contracts: Sequence[Mapping[str, Any]],
    source_count_receipts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Bijection between different Horizon and Winner canonical schemas."""

    plan = dict(plan_body)
    roster_fields = {
        "candidate_registry_sha256": "candidate_registry",
        "trial_registry_sha256": "trial_registry",
        "attempt_roster_sha256": "attempts",
        "evidence_roster_sha256": "expected_evaluations",
    }
    for digest_name, roster_name in roster_fields.items():
        roster = plan.get(roster_name)
        if type(roster) is not list or plan.get(digest_name) != _sha256(roster):
            _fail("HORIZON_COMMITMENT_ROSTER_INVALID")
    if plan.get("plan_status") not in {
        "READY_FOR_HISTORICAL_EVIDENCE",
        "HISTORICAL_BOUNDARY_LIMITED",
    }:
        _fail("HORIZON_PLAN_NOT_SEARCHABLE")
    prefix = plan.get("native_prefix")
    if type(prefix) is not dict or prefix.get("source_state") != "AVAILABLE":
        _fail("HORIZON_NATIVE_PREFIX_INVALID")
    try:
        precommitment = frozen.precommitment
        attempt_ledger = frozen.attempt_ledger
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "WINNER_FROZEN_COMMITMENTS_INVALID"
        ) from exc
    raw_attempts = attempt_ledger.get("attempts")
    raw_folds = precommitment.get("causal_fold_plan")
    horizon_attempts = plan.get("attempts")
    expected_evaluations = plan.get("expected_evaluations")
    candidates = plan.get("candidates")
    trials = plan.get("trials")
    if (
        type(raw_attempts) is not list
        or type(raw_folds) is not list
        or type(horizon_attempts) is not list
        or type(expected_evaluations) is not list
        or type(candidates) is not list
        or type(trials) is not list
        or len(raw_attempts) != len(attempt_artifacts)
        or len(raw_attempts) != len(horizon_attempts)
        or type(horizon_execution_contracts) not in {list, tuple}
        or type(source_count_receipts) not in {list, tuple}
        or len(horizon_execution_contracts) != len(raw_attempts)
        or len(source_count_receipts) != len(raw_attempts)
    ):
        _fail("HORIZON_WINNER_ROSTER_CARDINALITY_MISMATCH")
    candidate_by_id = {
        value.get("candidate_id"): value for value in candidates if type(value) is dict
    }
    trial_by_id = {
        value.get("trial_id"): value for value in trials if type(value) is dict
    }
    trial_by_identity = {
        (
            value.get("strategy_id"),
            value.get("strategy_release_sha256"),
            value.get("parameter_set_sha256"),
        ): value
        for value in trials
        if type(value) is dict
    }
    candidate_by_identity: dict[tuple[str, int], dict[str, Any]] = {}
    for value in candidates:
        if type(value) is not dict:
            _fail("HORIZON_IDENTITY_ROSTER_INVALID")
        identity = (value.get("lookback_kind"), value.get("lookback_value"))
        if (
            type(identity[0]) is not str
            or type(identity[1]) is not int
            or identity in candidate_by_identity
        ):
            _fail("HORIZON_IDENTITY_ROSTER_INVALID")
        candidate_by_identity[identity] = value
    if (
        len(candidate_by_id) != len(candidates)
        or len(candidate_by_identity) != len(candidates)
        or len(trial_by_id) != len(trials)
        or len(trial_by_identity) != len(trials)
    ):
        _fail("HORIZON_IDENTITY_ROSTER_INVALID")
    plan_attempt_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for value in horizon_attempts:
        if type(value) is not dict:
            _fail("HORIZON_ATTEMPT_ROSTER_INVALID")
        pair = (value.get("candidate_id"), value.get("trial_id"))
        if pair in plan_attempt_by_pair:
            _fail("HORIZON_ATTEMPT_ROSTER_INVALID")
        plan_attempt_by_pair[pair] = value

    expected_by_attempt: dict[str, list[dict[str, Any]]] = {}
    for value in expected_evaluations:
        if type(value) is not dict or type(value.get("attempt_id")) is not str:
            _fail("HORIZON_EVIDENCE_ROSTER_INVALID")
        expected_by_attempt.setdefault(value["attempt_id"], []).append(value)

    prefix_end = _utc_seconds(
        prefix.get("prefix_end_exclusive_utc"), "HORIZON_PREFIX_END_INVALID"
    )
    prefix_latest = _utc_seconds(
        prefix.get("prefix_last_close_utc"), "HORIZON_PREFIX_LATEST_INVALID"
    )
    prefix_sha = _hex64(
        prefix.get("prefix_content_sha256"), "HORIZON_PREFIX_SHA256_INVALID"
    )
    policy = plan.get("horizon_policy")
    if (
        type(policy) is not dict
        or policy.get("cost_contract_sha256")
        != precommitment.get("cost_contract_sha256")
        or policy.get("cost_stress_contract_sha256")
        != precommitment.get("cost_stress_contract_sha256")
    ):
        _fail("HORIZON_WINNER_COST_CONTRACT_SPLICE")

    mapping: list[dict[str, Any]] = []
    mapped_horizon_attempts: list[str] = []
    for ordinal, (
        raw_attempt,
        artifacts,
        execution_contract,
        count_receipt,
    ) in enumerate(
        zip(
            raw_attempts,
            attempt_artifacts,
            horizon_execution_contracts,
            source_count_receipts,
            strict=True,
        )
    ):
        if (
            type(raw_attempt) is not dict
            or type(artifacts) is not dict
            or type(execution_contract) is not dict
            or set(execution_contract)
            != {"lookback_kind", "lookback_value", "source_bar_count"}
            or type(count_receipt) is not dict
        ):
            _fail("HORIZON_WINNER_ATTEMPT_INVALID")
        process = artifacts.get("winner_process")
        horizon = artifacts.get("horizon_process")
        trial_definition = artifacts.get("trial_definition")
        recipe = artifacts.get("refit_recipe")
        if any(
            type(value) is not dict
            for value in (process, horizon, trial_definition, recipe)
        ):
            _fail("HORIZON_WINNER_ATTEMPT_INVALID")
        artifact_hashes = {
            "process_sha256": _sha256(process),
            "horizon_process_sha256": _sha256(horizon),
            "trial_definition_sha256": _sha256(trial_definition),
            "refit_recipe_sha256": _sha256(recipe),
        }
        if any(
            raw_attempt.get(name) != value for name, value in artifact_hashes.items()
        ):
            _fail("HORIZON_WINNER_ATTEMPT_ARTIFACT_SPLICE")
        if (
            horizon.get("symbol") != frozen.symbol
            or horizon.get("timeframe") != frozen.timeframe
            or _utc_seconds(horizon.get("fit_end_utc"), "WINNER_FIT_END_INVALID")
            != prefix_end
            or _utc_seconds(
                horizon.get("latest_closed_bar_utc"),
                "WINNER_LATEST_CLOSED_BAR_INVALID",
            )
            != prefix_latest
            or horizon.get("source_snapshot_sha256") != prefix_sha
        ):
            _fail("HORIZON_WINNER_SOURCE_PREFIX_SPLICE")
        fit_start = _utc_seconds(
            horizon.get("fit_start_utc"), "WINNER_FIT_START_INVALID"
        )
        candidate = candidate_by_identity.get(
            (horizon.get("lookback_kind"), horizon.get("lookback_value"))
        )
        if candidate is None:
            _fail("HORIZON_WINNER_CANDIDATE_MAPPING_INVALID")
        parameter_sha = _sha256(trial_definition.get("parameters"))
        trial = trial_by_identity.get(
            (
                process.get("strategy_id"),
                process.get("strategy_code_sha256"),
                parameter_sha,
            )
        )
        if trial is None:
            _fail("HORIZON_WINNER_TRIAL_MAPPING_INVALID")
        planned = plan_attempt_by_pair.get(
            (candidate.get("candidate_id"), trial.get("trial_id"))
        )
        if planned is None or planned.get("attempt_id") in mapped_horizon_attempts:
            _fail("HORIZON_WINNER_ATTEMPT_MAPPING_INVALID")
        if planned.get("strategy_id") != process.get("strategy_id"):
            _fail("HORIZON_WINNER_ATTEMPT_MAPPING_INVALID")
        count_body = dict(count_receipt)
        count_sha256 = count_body.pop("receipt_sha256", None)
        if (
            count_sha256 != _sha256(count_body)
            or count_receipt.get("schema_version") != SOURCE_COUNT_RECEIPT_SCHEMA
            or count_receipt.get("horizon_plan_receipt_sha256") != plan_receipt_sha256
            or count_receipt.get("native_prefix_index_sha256")
            != prefix.get("prefix_index_sha256")
            or count_receipt.get("native_prefix_content_sha256") != prefix_sha
            or count_receipt.get("horizon_attempt_id") != planned.get("attempt_id")
            or count_receipt.get("horizon_candidate_id")
            != candidate.get("candidate_id")
            or count_receipt.get("horizon_candidate_definition_sha256")
            != candidate.get("candidate_definition_sha256")
            or any(
                count_receipt.get(name) != execution_contract.get(name)
                for name in (
                    "lookback_kind",
                    "lookback_value",
                    "source_bar_count",
                )
            )
            or horizon.get("lookback_kind") != execution_contract.get("lookback_kind")
            or horizon.get("lookback_value") != execution_contract.get("lookback_value")
            or horizon.get("source_bar_count")
            != execution_contract.get("source_bar_count")
            or _utc_seconds(
                count_receipt.get("fit_start_utc"),
                "HORIZON_WINNER_SOURCE_COUNT_FIT_START_INVALID",
            )
            != fit_start
            or _utc_seconds(
                count_receipt.get("fit_end_exclusive_utc"),
                "HORIZON_WINNER_SOURCE_COUNT_FIT_END_INVALID",
            )
            != prefix_end
            or _utc_seconds(
                count_receipt.get("latest_closed_bar_utc"),
                "HORIZON_WINNER_SOURCE_COUNT_LATEST_INVALID",
            )
            != prefix_latest
        ):
            _fail("HORIZON_WINNER_SOURCE_COUNT_RECEIPT_INVALID")
        planned_evidence = expected_by_attempt.get(planned.get("attempt_id"), [])
        fold_mapping: list[dict[str, Any]] = []
        if planned.get("status") == "PLANNED":
            causal_evidence = [
                value
                for value in planned_evidence
                if value.get("downstream_causal_eligible") is True
            ]
            if len(causal_evidence) != len(raw_folds):
                _fail("HORIZON_WINNER_FOLD_CARDINALITY_MISMATCH")
            for raw_fold, expected in zip(raw_folds, causal_evidence, strict=True):
                if type(raw_fold) is not dict:
                    _fail("HORIZON_WINNER_FOLD_MAPPING_INVALID")
                start = _utc_seconds(
                    expected.get("validation_start_utc"),
                    "HORIZON_VALIDATION_START_INVALID",
                )
                end = _utc_seconds(
                    expected.get("validation_end_exclusive_utc"),
                    "HORIZON_VALIDATION_END_INVALID",
                )
                start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                if (
                    start_dt.time() != datetime.min.time()
                    or end_dt.time() != datetime.min.time()
                    or raw_fold.get("anchor_as_of_utc") != start
                    or raw_fold.get("oos_start_date_utc") != start_dt.date().isoformat()
                    or raw_fold.get("oos_end_date_utc")
                    != (end_dt.date() - timedelta(days=1)).isoformat()
                    or expected.get("expected_status") != "EVALUATION_REQUIRED"
                ):
                    _fail("HORIZON_WINNER_FOLD_MAPPING_INVALID")
                candidate_fold = next(
                    (
                        value
                        for value in candidate.get("folds", [])
                        if type(value) is dict
                        and value.get("fold_id") == expected.get("fold_id")
                    ),
                    None,
                )
                if type(candidate_fold) is not dict:
                    _fail("HORIZON_WINNER_FOLD_DEFINITION_MISSING")
                fold_mapping.append(
                    {
                        "horizon_fold_id": expected.get("fold_id"),
                        "horizon_fold_definition_sha256": candidate_fold.get(
                            "fold_definition_sha256"
                        ),
                        "horizon_context_sha256": expected.get("context_sha256"),
                        "winner_fold_id": raw_fold.get("fold_id"),
                        "validation_start_utc": start,
                        "validation_end_exclusive_utc": end,
                    }
                )
        elif planned.get("status") == "PROCESS_REJECTED":
            if not planned_evidence or any(
                value.get("expected_status") != "PROCESS_REJECTED"
                for value in planned_evidence
            ):
                _fail("HORIZON_WINNER_TERMINAL_MAPPING_INVALID")
        else:
            _fail("HORIZON_WINNER_ATTEMPT_STATUS_INVALID")
        mapped_horizon_attempts.append(planned["attempt_id"])
        mapping.append(
            {
                "ordinal": ordinal,
                "horizon_attempt_id": planned["attempt_id"],
                "horizon_candidate_id": candidate["candidate_id"],
                "horizon_candidate_definition_sha256": candidate[
                    "candidate_definition_sha256"
                ],
                "horizon_trial_id": trial["trial_id"],
                "horizon_trial_definition_sha256": trial["trial_definition_sha256"],
                "winner_attempt_id": raw_attempt.get("attempt_id"),
                "winner_process_candidate_id": raw_attempt.get("process_candidate_id"),
                "horizon_execution_contract": dict(execution_contract),
                "source_count_receipt": dict(count_receipt),
                **artifact_hashes,
                "fold_mapping": fold_mapping,
                "fold_mapping_sha256": _sha256(fold_mapping),
            }
        )
    expected_order = [value.get("attempt_id") for value in horizon_attempts]
    if mapped_horizon_attempts != expected_order:
        _fail("HORIZON_WINNER_ATTEMPT_ORDER_OR_COVERAGE_INVALID")

    horizon_roster = {
        "plan_receipt_sha256": _hex64(
            plan_receipt_sha256, "HORIZON_PLAN_RECEIPT_INVALID"
        ),
        "source_prefix_receipt_sha256": plan.get("source_prefix_receipt_sha256"),
        **{name: plan[name] for name in roster_fields},
    }
    winner_roster = {
        "precommitment_sha256": _sha256(precommitment),
        "attempt_ledger_sha256": _sha256(attempt_ledger),
        "frozen_receipt_sha256": hashlib.sha256(
            frozen.frozen_receipt_bytes
        ).hexdigest(),
        "process_registry_sha256": precommitment.get("process_registry_sha256"),
        "horizon_registry_sha256": precommitment.get("horizon_registry_sha256"),
        "trial_registry_sha256": precommitment.get("trial_registry_sha256"),
        "attempts_sha256": attempt_ledger.get("attempts_sha256"),
        "causal_fold_plan_sha256": precommitment.get("causal_fold_plan_sha256"),
        "source_contract_sha256": precommitment.get("source_contract_sha256"),
        "cost_contract_sha256": precommitment.get("cost_contract_sha256"),
        "cost_stress_contract_sha256": precommitment.get("cost_stress_contract_sha256"),
    }
    for value in winner_roster.values():
        _hex64(value, "WINNER_CROSS_BINDING_ROSTER_INVALID")
    return {
        "schema_version": HORIZON_WINNER_CROSS_BINDING_SCHEMA,
        "producer_request_sha256": producer_request_sha256,
        "anchor_id": anchor_id,
        "selection_cutoff_utc": plan["anchor"]["selection_cutoff_utc"],
        "process_precommit_receipt_sha256": plan["anchor"][
            "process_precommit_receipt_sha256"
        ],
        "source_group_decision_sha256": source_group_decision_sha256,
        "regime_fit_decision_sha256": regime_fit_decision_sha256,
        "native_prefix_index_sha256": _hex64(
            prefix.get("prefix_index_sha256"),
            "HORIZON_PREFIX_INDEX_SHA256_INVALID",
        ),
        "symbol": frozen.symbol,
        "timeframe": frozen.timeframe,
        "regime": frozen.regime,
        "artifact_namespace": artifact_namespace,
        "horizon_roster": horizon_roster,
        "winner_roster": winner_roster,
        "mapping_count": len(mapping),
        "mapping_sha256": _sha256(mapping),
        "mapping": mapping,
        "bijective_coverage": True,
        "created_before_winner_evidence_access": True,
        "confers_causal_current_live_or_capital_authority": False,
    }


def historical_production_adapter_contract() -> dict[str, Any]:
    """Return the immutable integration contract used by anchor precommitment."""

    return {
        "contract_version": CAPTURE_CONTRACT_VERSION,
        "source": "OptimizationPipeline/RegimeOptimizer search-time events",
        "event_order": [
            "bind_timeframe",
            "freeze_observed_cell",
            "complete_observed_cell_or_record_failure",
            "finish",
        ],
        "timeframes": list(book.TIMEFRAME_ORDER),
        "regimes": list(book.REGIME_ORDER),
        "cells_per_symbol": len(book.TIMEFRAME_ORDER) * len(book.REGIME_ORDER),
        "symbols_per_anchor": len(book.PINNED_SYMBOL_ORDER),
        "single_inflight_cell": True,
        "orchestration_owned_symbol_sink_required": True,
        "orchestration_input": "EXACT_24_SEALED_HORIZON_PLANS_CANONICAL_ORDER",
        "caller_binding_maps_accepted": False,
        "raw_artifact_namespace": (
            f"{RAW_ARTIFACT_NAMESPACE_PREFIX}/{{run_id}}/{{anchor_id}}"
        ),
        "raw_descriptor_graph_registered_before_symbol_seal": True,
        "horizon_winner_cross_binding": (
            "BIJECTIVE_SEMANTIC_MAPPING_OF_DISTINCT_CANONICAL_SCHEMAS"
        ),
        "canonical_executable_attempt_artifacts_strictly_verified": True,
        "sealed_native_prefix_reopened_before_freeze_and_evidence": True,
        "source_bar_count_recomputed_for_exact_fit_interval": True,
        "horizon_execution_contract_frozen_before_evidence": True,
        "raw_vectors_spooled_before_compaction": True,
        "posthoc_evidence_reconstruction": False,
        "spawn_worker_bridge_schema": HISTORICAL_SYMBOL_WORKER_BUNDLE_SCHEMA,
        "spawn_worker_bridge_is_routing_only": True,
        "spawn_worker_bridge_registered_in_symbol_manifest": False,
        "spawn_worker_pickled_authority_objects": False,
        "spawn_worker_parent_local_seal_reconstruction_required": True,
        "spawn_worker_artifacts_verified_before_parent_registration": True,
        "spawn_worker_completion_order_changes_book_input_order": False,
        "missing_cell_default": None,
        "technical_failure_is_economic_no_winner": False,
        "authenticated_terminal_technical_is_complete_abstention": True,
        "authenticated_terminal_technical_is_market_no_trade_verdict": False,
        "source_unavailable_requires_terminal_source_decision": True,
        "regime_unobserved_requires_zero_train_labels": True,
        "regime_unobserved_requires_genuine_horizon_zero_support_binding": True,
        "insufficient_evidence_is_derive_only_from_genuine_horizon_or_winner": True,
        "boundary_limited_zero_search_is_non_economic_insufficient_evidence": True,
        "all_process_rejected_is_non_economic_insufficient_evidence": True,
        "bounded_raw_winner_canary_receipt_schema": (
            BOUNDED_RAW_WINNER_CANARY_RECEIPT_SCHEMA
        ),
        "bounded_raw_winner_canary_state_context_admission_schema": (
            BOUNDED_RAW_WINNER_CANARY_STATE_CONTEXT_ADMISSION_SCHEMA
        ),
        "bounded_raw_winner_canary_reports_registry_default_and_variant_counts": True,
        "bounded_raw_winner_canary_separates_state_and_source_boundary_rejection": True,
        "confers_causal_current_live_or_capital_authority": False,
    }


HISTORICAL_PRODUCER_RELEASE_SHA256 = _sha256(historical_production_adapter_contract())


@dataclass(frozen=True, slots=True)
class FrozenHistoricalCell:
    """One cell frozen before its first decision-relevant evaluation."""

    symbol: str
    timeframe: str
    regime: str
    frozen: raw_winner.FrozenWinnerSearchPlan
    frozen_receipt_descriptor: dict[str, Any]
    frozen_descriptor_graph: dict[str, Any]
    cross_binding_descriptor: dict[str, Any]
    artifact_namespace: str
    _attempt_artifacts: tuple[dict[str, Any], ...]
    _horizon_execution_contracts: tuple[dict[str, Any], ...]
    _source_count_receipts: tuple[dict[str, Any], ...]
    _native_prefix_product: native_prefix.NativePrefixProduct
    _cross_binding_bytes: bytes
    _capture_nonce: str

    @property
    def attempted_trials_exact(self) -> int:
        return int(self.frozen.attempt_ledger["attempt_count"])


@dataclass(frozen=True, slots=True)
class _RawWinnerCellCompletion:
    production: raw_winner.WinnerRawEvidenceProduction
    production_receipt_descriptor: dict[str, Any]
    production_descriptor_graph: dict[str, Any]
    all_process_rejected: bool


@dataclass(frozen=True, slots=True)
class BoundedRawWinnerCanaryReceipt:
    """Content-addressed, non-authorizing terminal receipt for one canary."""

    _body_json: bytes
    _entry_json: bytes
    _symbol_receipt_json: bytes
    _seal: object

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()

    @property
    def artifact_entry(self) -> dict[str, Any]:
        return json.loads(self._entry_json.decode("ascii"))

    @property
    def symbol_receipt(self) -> dict[str, Any]:
        return json.loads(self._symbol_receipt_json.decode("ascii"))


class HistoricalSymbolSearchCapture:
    """Bounded per-symbol sink called at the optimizer's true search sites."""

    def __init__(
        self,
        context: authority_spool.GenerationSpoolContext,
        request: orchestration.AnchorProducerRequest,
        symbol: str,
    ) -> None:
        if type(context) is not authority_spool.GenerationSpoolContext:
            _fail("GENERATION_CONTEXT_INVALID")
        if type(request) is not orchestration.AnchorProducerRequest:
            _fail("ANCHOR_REQUEST_INVALID")
        body = request.to_dict()
        if (
            body.get("schema_version") != orchestration.PRODUCER_REQUEST_SCHEMA
            or body.get("generation_id") != context.generation_id
            or body.get("symbols") != list(book.PINNED_SYMBOL_ORDER)
            or body.get("timeframes") != list(book.TIMEFRAME_ORDER)
            or body.get("regimes") != list(book.REGIME_ORDER)
            or body.get("requires_exact_sparse_state_roster") is not True
            or body.get("possible_regime_slots_are_forced_observations") is not False
            or body.get("confers_causal_or_current_authority") is not False
            or body.get("optimizer_producer_release_sha256")
            != HISTORICAL_PRODUCER_RELEASE_SHA256
        ):
            _fail("ANCHOR_REQUEST_CONTRACT_INVALID")
        observed = authority_spool.reopen_generation_context(
            context.root,
            expected_generation_id=context.generation_id,
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=context.config_sha256,
        )
        if observed.context_sha256 != context.context_sha256:
            _fail("GENERATION_CONTEXT_CHANGED")
        if symbol not in book.PINNED_SYMBOL_ORDER:
            _fail("SYMBOL_OUTSIDE_PINNED_ROSTER")
        self.context = context
        self.request = request
        self.request_body = body
        self.anchor = dict(body["anchor"])
        self.symbol = symbol
        try:
            sink = request.symbol_sink(symbol)
        except Exception as exc:
            raise HistoricalProductionAdapterError(
                "ORCHESTRATION_SYMBOL_SINK_UNAVAILABLE"
            ) from exc
        if (
            type(sink) is not authority_spool.SymbolAuthoritySink
            or sink.symbol != symbol
            or sink.context is not context
            or sink.context.context_sha256 != context.context_sha256
        ):
            _fail("ORCHESTRATION_SYMBOL_SINK_IDENTITY_MISMATCH")
        self.sink = sink
        self.raw_artifact_namespace = (
            f"{RAW_ARTIFACT_NAMESPACE_PREFIX}/{body['run_id']}/"
            f"{self.anchor['anchor_id']}"
        )
        self._bindings: dict[tuple[str, str], dict[str, Any]] = {}
        self._horizon_plan_bodies: dict[tuple[str, str], dict[str, Any]] = {}
        self._horizon_plans: dict[tuple[str, str], Any] = {}
        self._source: dict[str, book.HistoricalSourceGroupDecision] = {}
        self._fit: dict[str, book.RegimeFitDecision] = {}
        self._evidence: dict[tuple[str, str], book.HistoricalBookCellEvidence] = {}
        self._cell_records: dict[tuple[str, str], dict[str, Any]] = {}
        self._active: FrozenHistoricalCell | None = None
        self._finished = False

    def _expected_timeframe(self) -> str:
        ordinal = len(self._source)
        if ordinal >= len(book.TIMEFRAME_ORDER):
            _fail("TIMEFRAME_ROSTER_OVERSIZED")
        return book.TIMEFRAME_ORDER[ordinal]

    def _raw_cell_prefix(self, timeframe: str, regime: str) -> str:
        return f"{self.raw_artifact_namespace}/winner-search/{timeframe}/{regime}"

    def _spool_descriptor_graph(
        self,
        *,
        timeframe: str,
        regime: str,
        phase: str,
        entries: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        if phase not in {"FROZEN", "PRODUCTION"} or type(entries) not in {
            list,
            tuple,
        }:
            _fail("RAW_DESCRIPTOR_GRAPH_INPUT_INVALID")
        expected_prefix = f"{self._raw_cell_prefix(timeframe, regime)}/"
        normalized = [
            _artifact_entry(dict(entry), "RAW_DESCRIPTOR_GRAPH_ENTRY_INVALID")
            for entry in entries
        ]
        normalized.sort(key=lambda item: item["artifact_id"])
        ids = [item["artifact_id"] for item in normalized]
        if (
            not normalized
            or any(not artifact_id.startswith(expected_prefix) for artifact_id in ids)
            or len(ids) != len(set(ids))
            or len(ids) > self.context.limits.max_artifacts_per_symbol
        ):
            _fail("RAW_DESCRIPTOR_GRAPH_ROSTER_INVALID")
        total_bytes = sum(int(item["descriptor"]["size_bytes"]) for item in normalized)
        if total_bytes > self.context.limits.max_symbol_bytes:
            _fail("RAW_DESCRIPTOR_GRAPH_BYTE_BOUND_EXCEEDED")
        body = {
            "schema_version": RAW_DESCRIPTOR_GRAPH_SCHEMA,
            "generation_id": self.context.generation_id,
            "generation_context_sha256": self.context.context_sha256,
            "producer_request_sha256": self.request.receipt_sha256,
            "run_id": self.request_body["run_id"],
            "anchor_id": self.anchor["anchor_id"],
            "symbol": self.symbol,
            "timeframe": timeframe,
            "regime": regime,
            "phase": phase,
            "artifact_namespace": self.raw_artifact_namespace,
            "artifact_count": len(normalized),
            "artifact_total_bytes": total_bytes,
            "artifact_roster_sha256": _sha256(normalized),
            "artifacts": normalized,
            "complete_for_phase": True,
            "confers_causal_current_live_or_capital_authority": False,
        }
        entry = self.sink.put_json(
            f"{self.anchor['anchor_id']}/adapter/{timeframe}/{regime}/"
            f"{phase.lower()}-descriptor-graph",
            "historical_raw_winner_descriptor_graph",
            body,
        )
        return _descriptor(entry["descriptor"], "RAW_DESCRIPTOR_GRAPH_INVALID")

    def _frozen_raw_entries(
        self,
        frozen: raw_winner.FrozenWinnerSearchPlan,
    ) -> tuple[dict[str, Any], ...]:
        try:
            receipt = json.loads(frozen.frozen_receipt_bytes.decode("ascii"))
        except (AttributeError, UnicodeError, json.JSONDecodeError) as exc:
            raise HistoricalProductionAdapterError(
                "WINNER_FROZEN_RECEIPT_INVALID"
            ) from exc
        if (
            getattr(frozen, "artifact_namespace", None) != self.raw_artifact_namespace
            or receipt.get("artifact_namespace") != self.raw_artifact_namespace
        ):
            _fail("WINNER_FROZEN_NAMESPACE_MISMATCH")
        roster = receipt.get("artifact_roster")
        if type(roster) is not dict:
            _fail("WINNER_FROZEN_ARTIFACT_ROSTER_INVALID")
        attempts = frozen.attempt_ledger.get("attempts")
        if type(attempts) is not list:
            _fail("WINNER_FROZEN_ATTEMPT_ROSTER_INVALID")
        expected_names = set(_FROZEN_ARTIFACT_TYPES)
        for attempt in attempts:
            ordinal = attempt.get("ordinal") if type(attempt) is dict else None
            if type(ordinal) is not int or ordinal < 0:
                _fail("WINNER_FROZEN_ATTEMPT_ROSTER_INVALID")
            expected_names.update(
                f"attempt-{ordinal:04d}-{name}" for name in _ATTEMPT_ARTIFACT_KEYS
            )
        if set(roster) != expected_names:
            _fail("WINNER_FROZEN_ARTIFACT_ROSTER_INVALID")
        prefix = f"{self._raw_cell_prefix(frozen.timeframe, frozen.regime)}/frozen"
        entries: list[dict[str, Any]] = []
        for name in sorted(roster):
            if name in _FROZEN_ARTIFACT_TYPES:
                artifact_id = f"{prefix}/{name}"
                artifact_type = _FROZEN_ARTIFACT_TYPES[name]
            else:
                attempt_label, body_name = name.rsplit("-", 1)
                # Artifact body names contain underscores, never hyphens.
                artifact_id = f"{prefix}/{attempt_label}/{body_name}"
                artifact_type = f"winner_attempt_{body_name}"
            entries.append(
                {
                    "artifact_id": artifact_id,
                    "artifact_type": artifact_type,
                    "descriptor": _descriptor(
                        roster[name], "WINNER_FROZEN_ARTIFACT_DESCRIPTOR_INVALID"
                    ),
                }
            )
        try:
            raw_receipt_entry = frozen.frozen_receipt_entry
        except Exception as exc:
            raise HistoricalProductionAdapterError(
                "WINNER_FROZEN_RECEIPT_ENTRY_UNAVAILABLE"
            ) from exc
        receipt_entry = _artifact_entry(
            raw_receipt_entry, "WINNER_FROZEN_RECEIPT_ENTRY_INVALID"
        )
        receipt_descriptor = receipt_entry["descriptor"]
        if (
            receipt_entry["artifact_id"] != f"{prefix}/receipt"
            or receipt_entry["artifact_type"] != "winner_frozen_search_plan_receipt"
            or receipt_descriptor["sha256"]
            != hashlib.sha256(frozen.frozen_receipt_bytes).hexdigest()
            or receipt_descriptor["size_bytes"] != len(frozen.frozen_receipt_bytes)
            or receipt_descriptor["format"] != raw_winner.ASCII_JSON_FORMAT
        ):
            _fail("WINNER_FROZEN_RECEIPT_ENTRY_INVALID")
        entries.append(receipt_entry)
        return tuple(entries)

    def _store_process_state(
        self,
        *,
        timeframe: str,
        regime: str,
        state_kind: str,
        reason_code: str,
        attempted_trials_exact: int,
        artifact_descriptor: Mapping[str, Any] | None = None,
        horizon_plan: Any = None,
        process_rejection_request: Any = None,
        process_rejection_decision: Any = None,
        horizon_winner_cross_binding_bytes: bytes | None = None,
        winner_raw_production_receipt_bytes: bytes | None = None,
    ) -> None:
        key = (timeframe, regime)
        if key in self._evidence:
            _fail("CELL_CAPTURE_DUPLICATED")
        state = book.create_historical_process_state_decision(
            symbol=self.symbol,
            timeframe=timeframe,
            regime=regime,
            state_kind=state_kind,
            reason_code=reason_code,
            source_group_decision=self._source[timeframe],
            regime_fit_decision=self._fit[timeframe],
            attempted_trials_exact=attempted_trials_exact,
            horizon_plan=horizon_plan,
            process_rejection_request=process_rejection_request,
            process_rejection_decision=process_rejection_decision,
            horizon_winner_cross_binding_bytes=horizon_winner_cross_binding_bytes,
            winner_raw_production_receipt_bytes=winner_raw_production_receipt_bytes,
        )
        if state is None:
            _fail("PROCESS_STATE_DECISION_REJECTED")
        state_body = state.to_dict()
        evidence = book.process_state_cell_evidence(
            process_state_decision=state,
            source_group_decision=self._source[timeframe],
            regime_fit_decision=self._fit[timeframe],
            horizon_plan=horizon_plan,
            process_rejection_request=process_rejection_request,
            process_rejection_decision=process_rejection_decision,
            horizon_winner_cross_binding_bytes=horizon_winner_cross_binding_bytes,
            winner_raw_production_receipt_bytes=winner_raw_production_receipt_bytes,
        )
        if evidence is None:
            _fail("PROCESS_STATE_EVIDENCE_REJECTED")
        record: dict[str, Any] = {
            "symbol": self.symbol,
            "timeframe": timeframe,
            "regime": regime,
            "historical_state": state_kind,
            "attempted_trials_exact": attempted_trials_exact,
            "process_state_receipt_sha256": state.receipt_sha256,
            "winner_request_sha256": None,
            "winner_decision_sha256": None,
            "insufficient_evidence_binding": state_body.get(
                "insufficient_evidence_binding"
            ),
            "insufficient_horizon_plan_receipt_sha256": (
                getattr(horizon_plan, "receipt_sha256", None)
            ),
            "process_rejection_request_sha256": (
                getattr(process_rejection_request, "request_sha256", None)
            ),
            "process_rejection_decision_sha256": (
                getattr(process_rejection_decision, "receipt_sha256", None)
            ),
            "raw_artifact_namespace": None,
            "frozen_plan_receipt": None,
            "frozen_descriptor_graph": None,
            "horizon_winner_cross_binding": None,
            "raw_production_receipt": None,
            "production_descriptor_graph": None,
            "technical_failure_artifact": None,
        }
        if artifact_descriptor is not None:
            record["technical_failure_artifact"] = _descriptor(
                dict(artifact_descriptor), "TECHNICAL_FAILURE_DESCRIPTOR_INVALID"
            )
        self._evidence[key] = evidence
        self._cell_records[key] = record

    def bind_timeframe(
        self,
        timeframe: str,
        *,
        horizon_plans: Sequence[Any],
        source_group_decision: book.HistoricalSourceGroupDecision,
        regime_fit_decision: book.RegimeFitDecision,
        terminal_native_timeframe: Any | None = None,
    ) -> None:
        """Bind source, detector fit, support, and four released Horizon plans."""

        if self._finished or self._active is not None:
            _fail("CAPTURE_EVENT_ORDER_INVALID")
        expected = self._expected_timeframe()
        if timeframe != expected:
            _fail("TIMEFRAME_ROSTER_ORDER_INVALID")
        if (
            type(source_group_decision) is not book.HistoricalSourceGroupDecision
            or type(regime_fit_decision) is not book.RegimeFitDecision
            or type(horizon_plans) not in {list, tuple}
            or len(horizon_plans) != len(book.REGIME_ORDER)
        ):
            _fail("TIMEFRAME_CAPTURE_INPUT_INVALID")
        source_body = source_group_decision.to_dict()
        fit_body = regime_fit_decision.to_dict()
        if (
            source_body.get("symbol") != self.symbol
            or source_body.get("timeframe") != timeframe
            or fit_body.get("symbol") != self.symbol
            or fit_body.get("timeframe") != timeframe
            or fit_body.get("source_group_decision_sha256")
            != source_group_decision.receipt_sha256
        ):
            _fail("TIMEFRAME_AUTHORITY_IDENTITY_MISMATCH")
        bindings: dict[str, dict[str, Any]] = {}
        plan_bodies: dict[str, dict[str, Any]] = {}
        for regime, plan in zip(book.REGIME_ORDER, horizon_plans, strict=True):
            binding = orchestration.historical_horizon_plan_binding(plan)
            try:
                plan_body = plan.to_dict()
            except Exception as exc:
                raise HistoricalProductionAdapterError(
                    "HORIZON_PLAN_CAPTURE_CHANGED"
                ) from exc
            if (
                binding.get("cell")
                != {
                    "symbol": self.symbol,
                    "timeframe": timeframe,
                    "regime": regime,
                }
                or binding.get("anchor_id") != self.anchor.get("anchor_id")
                or binding.get("selection_cutoff_utc")
                != self.anchor.get("selection_cutoff_utc")
                or binding.get("process_precommit_receipt_sha256")
                != self.request_body.get("process_precommit_receipt_sha256")
                or type(plan_body) is not dict
                or _sha256(plan_body) != binding.get("plan_receipt_sha256")
            ):
                _fail("HORIZON_PLAN_CAPTURE_MISMATCH")
            bindings[regime] = binding
            plan_bodies[regime] = json.loads(
                _canonical_bytes(plan_body).decode("ascii")
            )
        self._source[timeframe] = source_group_decision
        self._fit[timeframe] = regime_fit_decision
        for regime in book.REGIME_ORDER:
            self._bindings[(timeframe, regime)] = bindings[regime]
            self._horizon_plan_bodies[(timeframe, regime)] = plan_bodies[regime]
        for regime, plan in zip(book.REGIME_ORDER, horizon_plans, strict=True):
            self._horizon_plans[(timeframe, regime)] = plan

        availability = source_body.get("availability")
        fit_state = fit_body.get("fit_state")
        if availability == book.TERMINAL_TECHNICAL:
            # A terminal-technical Book abstention is valid only when the
            # captured plans and Book decisions are the exact projection of
            # one authenticated native-prefix product.  Merely presenting
            # individually well-formed decisions would permit provenance from
            # another activation/cell to be spliced into this capture.
            try:
                import pm_authoritative_search_execution as search_execution
                from pm_core import PipelineConfig

                if (
                    type(terminal_native_timeframe)
                    is not search_execution.TerminalNativeTimeframe
                ):
                    _fail("TERMINAL_TECHNICAL_AUTHORITY_REQUIRED")
                terminal_product = terminal_native_timeframe.product
                reopened_terminal_product = (
                    native_prefix.reverify_activated_native_prefix_product(
                        terminal_product,
                        self.context,
                        require_current_activation=False,
                    )
                )
                fresh_terminal = search_execution.fit_terminal_native_timeframe(
                    product=reopened_terminal_product,
                    config=PipelineConfig(),
                )
            except HistoricalProductionAdapterError:
                raise
            except Exception as exc:
                raise HistoricalProductionAdapterError(
                    "TERMINAL_TECHNICAL_AUTHORITY_REVERIFY_FAILED"
                ) from exc
            terminal_native = dict(reopened_terminal_product.native_prefix)
            terminal_contract = dict(terminal_native_timeframe.terminal_source_contract)
            terminal_policy = dict(terminal_native_timeframe.terminal_regime_fit_policy)
            if (
                terminal_native_timeframe.source_group_decision
                is not source_group_decision
                or terminal_native_timeframe.regime_fit_decision
                is not regime_fit_decision
                or terminal_product.source_state
                != reopened_terminal_product.source_state
                or dict(terminal_product.index) != dict(reopened_terminal_product.index)
                or dict(terminal_product.index_descriptor)
                != dict(reopened_terminal_product.index_descriptor)
                or terminal_product.index_sha256
                != reopened_terminal_product.index_sha256
                or dict(terminal_product.native_prefix)
                != dict(reopened_terminal_product.native_prefix)
                or dict(terminal_product.instrument_snapshot or {})
                != dict(reopened_terminal_product.instrument_snapshot or {})
                or terminal_product.prefix_path != reopened_terminal_product.prefix_path
                or dict(terminal_product.prefix_descriptor or {})
                != dict(reopened_terminal_product.prefix_descriptor or {})
                or tuple(terminal_product.artifact_descriptors)
                != tuple(reopened_terminal_product.artifact_descriptors)
                or terminal_product.source_state != native_prefix.TECHNICAL_TERMINAL
                or terminal_native.get("source_state")
                != native_prefix.TECHNICAL_TERMINAL
                or any(
                    plan_bodies[regime].get("native_prefix") != terminal_native
                    for regime in book.REGIME_ORDER
                )
                or source_body != fresh_terminal.source_group_decision.to_dict()
                or fit_body != fresh_terminal.regime_fit_decision.to_dict()
                or terminal_contract != dict(fresh_terminal.terminal_source_contract)
                or terminal_policy != dict(fresh_terminal.terminal_regime_fit_policy)
                or terminal_native_timeframe.terminal_source_contract_sha256
                != fresh_terminal.terminal_source_contract_sha256
                or _sha256(terminal_contract)
                != terminal_native_timeframe.terminal_source_contract_sha256
                or _sha256(terminal_policy) != fit_body.get("regime_fit_policy_sha256")
                or source_body.get("source_group_evidence_sha256")
                != reopened_terminal_product.index_sha256
                or source_body.get("activation_id")
                != terminal_native.get("activation_id")
                or source_body.get("activation_manifest_sha256")
                != terminal_native.get("activation_manifest_sha256")
                or source_body.get("activation_entry_sha256")
                != terminal_native.get("activation_entry_sha256")
                or source_body.get("terminal_proof_sha256")
                != terminal_native.get("source_terminal_receipt_sha256")
                or source_body.get("reason_code")
                != search_execution.TERMINAL_TECHNICAL_REASON_CODE
                or fit_body.get("reason_code")
                != search_execution.TERMINAL_TECHNICAL_REASON_CODE
            ):
                _fail("TERMINAL_TECHNICAL_AUTHORITY_BINDING_MISMATCH")
            if fit_state != book.REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL or any(
                bindings[regime].get("plan_status") != "HISTORICAL_TERMINAL_TECHNICAL"
                for regime in book.REGIME_ORDER
            ):
                _fail("TERMINAL_TECHNICAL_CAPTURE_CONTRADICTION")
            for regime in book.REGIME_ORDER:
                self._store_process_state(
                    timeframe=timeframe,
                    regime=regime,
                    state_kind=book.TERMINAL_TECHNICAL,
                    reason_code=_reason(
                        fit_body.get("reason_code"),
                        "TERMINAL_TECHNICAL_REASON_INVALID",
                    ),
                    attempted_trials_exact=0,
                )
            return
        if terminal_native_timeframe is not None:
            _fail("UNUSED_TERMINAL_TECHNICAL_AUTHORITY")
        if availability == book.TERMINAL_UNAVAILABLE:
            if fit_state != book.REGIME_NOT_APPLICABLE or any(
                bindings[regime].get("plan_status") != "HISTORICAL_SOURCE_UNAVAILABLE"
                for regime in book.REGIME_ORDER
            ):
                _fail("SOURCE_UNAVAILABLE_CAPTURE_CONTRADICTION")
            for regime in book.REGIME_ORDER:
                self._store_process_state(
                    timeframe=timeframe,
                    regime=regime,
                    state_kind=book.TERMINAL_SOURCE_ABSENCE,
                    reason_code=_reason(
                        fit_body.get("reason_code"), "SOURCE_UNAVAILABLE_REASON_INVALID"
                    ),
                    attempted_trials_exact=0,
                )
            return
        if availability != book.AVAILABLE:
            _fail("SOURCE_AVAILABILITY_INVALID")
        if any(
            bindings[regime].get("plan_status") == "HISTORICAL_SOURCE_UNAVAILABLE"
            for regime in book.REGIME_ORDER
        ):
            _fail("AVAILABLE_SOURCE_HORIZON_CONTRADICTION")
        if fit_state == book.REGIME_FIT_TECHNICAL_FAILURE:
            for regime in book.REGIME_ORDER:
                self._store_process_state(
                    timeframe=timeframe,
                    regime=regime,
                    state_kind=book.TECHNICAL_FAILURE,
                    reason_code=_reason(
                        fit_body.get("reason_code"), "REGIME_FIT_REASON_INVALID"
                    ),
                    attempted_trials_exact=0,
                )
            return
        if fit_state != book.REGIME_FITTED:
            _fail("REGIME_FIT_STATE_INVALID")
        support = fit_body.get("train_regime_support")
        counts = support.get("counts") if isinstance(support, Mapping) else None
        if type(counts) is not dict or set(counts) != set(book.REGIME_ORDER):
            _fail("TRAIN_REGIME_SUPPORT_INVALID")
        for regime in book.REGIME_ORDER:
            count = counts.get(regime)
            if type(count) is not int or count < 0:
                _fail("TRAIN_REGIME_SUPPORT_INVALID")
            status = bindings[regime].get("plan_status")
            if status == "HISTORICAL_TECHNICAL_INCOMPLETE":
                self._store_process_state(
                    timeframe=timeframe,
                    regime=regime,
                    state_kind=book.TECHNICAL_FAILURE,
                    reason_code="HISTORICAL_HORIZON_TECHNICAL_INCOMPLETE",
                    attempted_trials_exact=0,
                )
            elif count == 0:
                if status != "HISTORICAL_REGIME_UNOBSERVED":
                    _fail("UNOBSERVED_REGIME_HORIZON_STATUS_INVALID")
                _verify_unobserved_horizon_binding(
                    plan=self._horizon_plans[(timeframe, regime)],
                    plan_body=self._horizon_plan_bodies[(timeframe, regime)],
                    source_decision=source_group_decision,
                    fit_decision=regime_fit_decision,
                    regime=regime,
                )
                self._store_process_state(
                    timeframe=timeframe,
                    regime=regime,
                    state_kind=book.REGIME_UNOBSERVED,
                    reason_code=book.REGIME_UNOBSERVED_REASON,
                    attempted_trials_exact=0,
                )
            elif status == "HISTORICAL_BOUNDARY_LIMITED":
                self._store_process_state(
                    timeframe=timeframe,
                    regime=regime,
                    state_kind=book.INSUFFICIENT_EVIDENCE,
                    reason_code=book.INSUFFICIENT_PRESEARCH_BOUNDARY_REASON,
                    attempted_trials_exact=0,
                    horizon_plan=self._horizon_plans[(timeframe, regime)],
                )
            elif status != "READY_FOR_HISTORICAL_EVIDENCE":
                _fail("OBSERVED_REGIME_HORIZON_STATUS_INVALID")

    def freeze_observed_cell(
        self,
        timeframe: str,
        regime: str,
        *,
        native_prefix_product: native_prefix.NativePrefixProduct,
        precommitted_at_utc: str,
        attempts_frozen_at_utc: str,
        fold_plan: Sequence[Mapping[str, Any]],
        attempt_artifacts: Sequence[Mapping[str, Any]],
        source_contract_seed: Mapping[str, Any],
        base_cost_contract: Mapping[str, Any],
        stress_cost_contract: Mapping[str, Any],
        cost_stress_adversity_plan: Mapping[str, Any],
        risk_unit_contract: Mapping[str, Any],
    ) -> FrozenHistoricalCell:
        """Freeze the complete cell roster before raw evidence access."""

        key = (timeframe, regime)
        if (
            self._finished
            or self._active is not None
            or key not in self._bindings
            or key in self._evidence
            or timeframe not in self._source
            or regime not in book.REGIME_ORDER
        ):
            _fail("CELL_FREEZE_EVENT_ORDER_INVALID")
        fit_body = self._fit[timeframe].to_dict()
        support = fit_body.get("train_regime_support")
        if (
            fit_body.get("fit_state") != book.REGIME_FITTED
            or not isinstance(support, Mapping)
            or int((support.get("counts") or {}).get(regime, 0)) <= 0
        ):
            _fail("CELL_FREEZE_REQUIRES_OBSERVED_REGIME")
        normalized_attempts, execution_contracts, count_receipts = (
            _strict_attempt_artifacts(
                attempt_artifacts,
                symbol=self.symbol,
                timeframe=timeframe,
                regime=regime,
                plan_body=self._horizon_plan_bodies[key],
                plan_receipt_sha256=self._bindings[key]["plan_receipt_sha256"],
                native_prefix_product=native_prefix_product,
            )
        )
        precommitted_evidence_roles = _precommitted_evidence_roles(
            self._horizon_plan_bodies[key], len(normalized_attempts)
        )
        if not _raw_namespace_supported(
            raw_winner.freeze_winner_search_plan_v3
        ) or not _raw_namespace_supported(raw_winner.produce_winner_v11_raw_evidence):
            _fail("RAW_WINNER_NAMESPACE_UNAVAILABLE")
        try:
            frozen = raw_winner.freeze_winner_search_plan_v3(
                self.sink,
                symbol=self.symbol,
                timeframe=timeframe,
                regime=regime,
                precommitted_at_utc=precommitted_at_utc,
                attempts_frozen_at_utc=attempts_frozen_at_utc,
                fold_plan=fold_plan,
                attempt_artifacts=normalized_attempts,
                precommitted_evidence_roles=precommitted_evidence_roles,
                horizon_execution_contracts=execution_contracts,
                source_contract_seed=source_contract_seed,
                base_cost_contract=base_cost_contract,
                stress_cost_contract=stress_cost_contract,
                cost_stress_adversity_plan=cost_stress_adversity_plan,
                risk_unit_contract=risk_unit_contract,
                artifact_namespace=self.raw_artifact_namespace,
            )
        except TypeError as exc:
            raise HistoricalProductionAdapterError(
                "RAW_WINNER_NAMESPACE_INVOCATION_FAILED"
            ) from exc
        source_sha = self._source[timeframe].to_dict().get("source_contract_sha256")
        if frozen.precommitment.get("source_contract_sha256") != source_sha:
            _fail("WINNER_SOURCE_CONTRACT_SPLICE")
        cross_binding = _cross_binding_body(
            plan_body=self._horizon_plan_bodies[key],
            plan_receipt_sha256=self._bindings[key]["plan_receipt_sha256"],
            frozen=frozen,
            attempt_artifacts=normalized_attempts,
            producer_request_sha256=self.request.receipt_sha256,
            anchor_id=self.anchor["anchor_id"],
            artifact_namespace=self.raw_artifact_namespace,
            source_group_decision_sha256=self._source[timeframe].receipt_sha256,
            regime_fit_decision_sha256=self._fit[timeframe].receipt_sha256,
            horizon_execution_contracts=execution_contracts,
            source_count_receipts=count_receipts,
        )
        cross_binding_bytes = _canonical_bytes(cross_binding)
        cross_binding_entry = self.sink.put_opaque(
            f"{self.anchor['anchor_id']}/adapter/{timeframe}/{regime}/"
            "horizon-winner-cross-binding",
            "historical_horizon_winner_cross_binding",
            (cross_binding_bytes,),
            format=raw_winner.ASCII_JSON_FORMAT,
        )
        cross_binding_descriptor = _descriptor(
            cross_binding_entry["descriptor"],
            "HORIZON_WINNER_CROSS_BINDING_DESCRIPTOR_INVALID",
        )
        frozen_receipt_entry = _artifact_entry(
            frozen.frozen_receipt_entry,
            "WINNER_FROZEN_RECEIPT_ENTRY_INVALID",
        )
        frozen_descriptor = _descriptor(
            frozen_receipt_entry["descriptor"], "FROZEN_RECEIPT_DESCRIPTOR_INVALID"
        )
        frozen_graph = self._spool_descriptor_graph(
            timeframe=timeframe,
            regime=regime,
            phase="FROZEN",
            entries=self._frozen_raw_entries(frozen),
        )
        nonce = _sha256(
            {
                "capture": id(self),
                "anchor_id": self.anchor["anchor_id"],
                "cell": [self.symbol, timeframe, regime],
                "frozen_receipt_sha256": frozen_descriptor["sha256"],
            }
        )
        handle = FrozenHistoricalCell(
            symbol=self.symbol,
            timeframe=timeframe,
            regime=regime,
            frozen=frozen,
            frozen_receipt_descriptor=frozen_descriptor,
            frozen_descriptor_graph=frozen_graph,
            cross_binding_descriptor=cross_binding_descriptor,
            artifact_namespace=self.raw_artifact_namespace,
            _attempt_artifacts=normalized_attempts,
            _horizon_execution_contracts=execution_contracts,
            _source_count_receipts=count_receipts,
            _native_prefix_product=native_prefix_product,
            _cross_binding_bytes=cross_binding_bytes,
            _capture_nonce=nonce,
        )
        self._active = handle
        return handle

    def _require_active(self, handle: FrozenHistoricalCell) -> None:
        if type(handle) is not FrozenHistoricalCell or self._active is not handle:
            _fail("FROZEN_CELL_HANDLE_INVALID")
        if (
            handle.symbol != self.symbol
            or handle.artifact_namespace != self.raw_artifact_namespace
            or (handle.timeframe, handle.regime) in self._evidence
        ):
            _fail("FROZEN_CELL_HANDLE_INVALID")

    def _complete_raw_winner_cell(
        self,
        handle: FrozenHistoricalCell,
        *,
        attempt_evidence: Iterable[Mapping[str, Any]],
        evidence_access_started_at_utc: str,
        evidence_access_completed_at_utc: str,
    ) -> _RawWinnerCellCompletion:
        """Shared production/canary raw completion before any Book projection."""

        self._require_active(handle)
        key = (handle.timeframe, handle.regime)
        (
            replayed_attempts,
            replayed_execution_contracts,
            replayed_count_receipts,
        ) = _strict_attempt_artifacts(
            handle._attempt_artifacts,
            symbol=self.symbol,
            timeframe=handle.timeframe,
            regime=handle.regime,
            plan_body=self._horizon_plan_bodies[key],
            plan_receipt_sha256=self._bindings[key]["plan_receipt_sha256"],
            native_prefix_product=handle._native_prefix_product,
        )
        if (
            _canonical_bytes(replayed_attempts)
            != _canonical_bytes(handle._attempt_artifacts)
            or _canonical_bytes(replayed_execution_contracts)
            != _canonical_bytes(handle._horizon_execution_contracts)
            or _canonical_bytes(replayed_count_receipts)
            != _canonical_bytes(handle._source_count_receipts)
        ):
            _fail("ATTEMPT_SOURCE_AUTHORITY_REPLAY_FAILED")
        replayed_cross_binding = _canonical_bytes(
            _cross_binding_body(
                plan_body=self._horizon_plan_bodies[key],
                plan_receipt_sha256=self._bindings[key]["plan_receipt_sha256"],
                frozen=handle.frozen,
                attempt_artifacts=replayed_attempts,
                producer_request_sha256=self.request.receipt_sha256,
                anchor_id=self.anchor["anchor_id"],
                artifact_namespace=self.raw_artifact_namespace,
                source_group_decision_sha256=self._source[
                    handle.timeframe
                ].receipt_sha256,
                regime_fit_decision_sha256=self._fit[handle.timeframe].receipt_sha256,
                horizon_execution_contracts=replayed_execution_contracts,
                source_count_receipts=replayed_count_receipts,
            )
        )
        if (
            replayed_cross_binding != handle._cross_binding_bytes
            or hashlib.sha256(replayed_cross_binding).hexdigest()
            != handle.cross_binding_descriptor["sha256"]
            or len(replayed_cross_binding)
            != handle.cross_binding_descriptor["size_bytes"]
            or handle.cross_binding_descriptor["format"] != raw_winner.ASCII_JSON_FORMAT
        ):
            _fail("HORIZON_WINNER_CROSS_BINDING_REPLAY_FAILED")
        if not _raw_namespace_supported(raw_winner.produce_winner_v11_raw_evidence):
            _fail("RAW_WINNER_NAMESPACE_UNAVAILABLE")
        try:
            production = raw_winner.produce_winner_v11_raw_evidence(
                self.sink,
                frozen=handle.frozen,
                attempt_evidence=attempt_evidence,
                evidence_access_started_at_utc=evidence_access_started_at_utc,
                evidence_access_completed_at_utc=evidence_access_completed_at_utc,
                provenance_cross_binding_bytes=handle._cross_binding_bytes,
                artifact_namespace=self.raw_artifact_namespace,
            )
        except TypeError as exc:
            raise HistoricalProductionAdapterError(
                "RAW_WINNER_NAMESPACE_INVOCATION_FAILED"
            ) from exc
        production_entry = production.entries.get("production-receipt")
        if type(production_entry) is not dict:
            _fail("WINNER_PRODUCTION_RECEIPT_MISSING")
        production_receipt_descriptor = _descriptor(
            production_entry.get("descriptor"),
            "WINNER_PRODUCTION_RECEIPT_INVALID",
        )
        production_graph = self._spool_descriptor_graph(
            timeframe=handle.timeframe,
            regime=handle.regime,
            phase="PRODUCTION",
            entries=tuple(production.entries.values()),
        )
        all_process_rejected = _all_precommitted_attempts_process_rejected(
            request=production.request,
            decision=production.decision,
            attempted_trials_exact=handle.attempted_trials_exact,
        )
        return _RawWinnerCellCompletion(
            production=production,
            production_receipt_descriptor=production_receipt_descriptor,
            production_descriptor_graph=production_graph,
            all_process_rejected=all_process_rejected,
        )

    def complete_observed_cell(
        self,
        handle: FrozenHistoricalCell,
        *,
        attempt_evidence: Iterable[Mapping[str, Any]],
        evidence_access_started_at_utc: str,
        evidence_access_completed_at_utc: str,
    ) -> raw_winner.WinnerRawEvidenceProduction:
        """Consume un-compacted Backtester lanes and create Winner V11 evidence."""

        key = (handle.timeframe, handle.regime)
        completed = self._complete_raw_winner_cell(
            handle,
            attempt_evidence=attempt_evidence,
            evidence_access_started_at_utc=evidence_access_started_at_utc,
            evidence_access_completed_at_utc=evidence_access_completed_at_utc,
        )
        production = completed.production
        production_receipt_descriptor = completed.production_receipt_descriptor
        production_graph = completed.production_descriptor_graph
        all_process_rejected = completed.all_process_rejected
        if all_process_rejected:
            production_receipt_bytes = _reopen_winner_raw_production_receipt(
                self.context,
                production_receipt_descriptor,
            )
            self._store_process_state(
                timeframe=handle.timeframe,
                regime=handle.regime,
                state_kind=book.INSUFFICIENT_EVIDENCE,
                reason_code=book.INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON,
                attempted_trials_exact=handle.attempted_trials_exact,
                horizon_plan=self._horizon_plans[key],
                process_rejection_request=production.request,
                process_rejection_decision=production.decision,
                horizon_winner_cross_binding_bytes=handle._cross_binding_bytes,
                winner_raw_production_receipt_bytes=production_receipt_bytes,
            )
            record = self._cell_records[key]
            record.update(
                {
                    "raw_artifact_namespace": self.raw_artifact_namespace,
                    "frozen_plan_receipt": handle.frozen_receipt_descriptor,
                    "frozen_descriptor_graph": handle.frozen_descriptor_graph,
                    "horizon_winner_cross_binding": (handle.cross_binding_descriptor),
                    "raw_production_receipt": production_receipt_descriptor,
                    "production_descriptor_graph": production_graph,
                }
            )
            self._active = None
            return production

        evidence = book.winner_cell_evidence(
            production.decision,
            production.request,
            self._source[handle.timeframe],
            self._fit[handle.timeframe],
        )
        if evidence is None:
            _fail("WINNER_BOOK_EVIDENCE_REJECTED")
        self._evidence[key] = evidence
        self._cell_records[key] = {
            "symbol": self.symbol,
            "timeframe": handle.timeframe,
            "regime": handle.regime,
            "historical_state": "WINNER_DECISION",
            "attempted_trials_exact": handle.attempted_trials_exact,
            "process_state_receipt_sha256": None,
            "winner_request_sha256": production.request.request_sha256,
            "winner_decision_sha256": production.decision.receipt_sha256,
            "insufficient_evidence_binding": None,
            "insufficient_horizon_plan_receipt_sha256": None,
            "process_rejection_request_sha256": None,
            "process_rejection_decision_sha256": None,
            "raw_artifact_namespace": self.raw_artifact_namespace,
            "frozen_plan_receipt": handle.frozen_receipt_descriptor,
            "frozen_descriptor_graph": handle.frozen_descriptor_graph,
            "horizon_winner_cross_binding": handle.cross_binding_descriptor,
            "raw_production_receipt": production_receipt_descriptor,
            "production_descriptor_graph": production_graph,
            "technical_failure_artifact": None,
        }
        self._active = None
        return production

    def record_raw_evidence_failure(
        self,
        handle: FrozenHistoricalCell,
        failure: raw_winner.WinnerRawEvidenceIncomplete,
    ) -> None:
        """Persist a typed producer failure without manufacturing NO_WINNER."""

        self._require_active(handle)
        if type(failure) is not raw_winner.WinnerRawEvidenceIncomplete:
            _fail("RAW_EVIDENCE_FAILURE_TYPE_INVALID")
        body = {
            "schema_version": FAILURE_ARTIFACT_SCHEMA,
            "anchor_id": self.anchor["anchor_id"],
            "symbol": self.symbol,
            "timeframe": handle.timeframe,
            "regime": handle.regime,
            "failure_code": _reason(failure.code, "RAW_EVIDENCE_FAILURE_CODE_INVALID"),
            "attempted_trials_exact": handle.attempted_trials_exact,
            "frozen_receipt_sha256": handle.frozen_receipt_descriptor["sha256"],
            "economic_no_winner": False,
        }
        entry = self.sink.put_json(
            f"{self.anchor['anchor_id']}/adapter/{handle.timeframe}/"
            f"{handle.regime}/technical-failure",
            "historical_winner_raw_evidence_failure",
            body,
        )
        self._active = None
        self._store_process_state(
            timeframe=handle.timeframe,
            regime=handle.regime,
            state_kind=book.TECHNICAL_FAILURE,
            reason_code=failure.code,
            attempted_trials_exact=handle.attempted_trials_exact,
            artifact_descriptor=entry["descriptor"],
        )
        self._cell_records[(handle.timeframe, handle.regime)]["frozen_plan_receipt"] = (
            handle.frozen_receipt_descriptor
        )
        self._cell_records[(handle.timeframe, handle.regime)][
            "frozen_descriptor_graph"
        ] = handle.frozen_descriptor_graph
        self._cell_records[(handle.timeframe, handle.regime)][
            "raw_artifact_namespace"
        ] = self.raw_artifact_namespace
        self._cell_records[(handle.timeframe, handle.regime)][
            "horizon_winner_cross_binding"
        ] = handle.cross_binding_descriptor

    def record_search_infrastructure_failure(
        self,
        timeframe: str,
        regime: str,
        *,
        failure: SearchInfrastructureError,
        attempted_trials_exact: int,
    ) -> None:
        """Capture the optimizer's typed failure before Winner raw production."""

        key = (timeframe, regime)
        if (
            self._finished
            or self._active is not None
            or key not in self._bindings
            or key in self._evidence
            or type(failure) is not SearchInfrastructureError
            or type(attempted_trials_exact) is not int
            or attempted_trials_exact < 0
        ):
            _fail("SEARCH_FAILURE_EVENT_INVALID")
        outcome = failure.outcome
        if type(outcome) not in {TrialOutcome, FamilyCellOutcome} or (
            outcome.kind is not SearchOutcomeKind.INFRA_FAILURE
        ):
            _fail("SEARCH_FAILURE_OUTCOME_INVALID")
        if any(
            value not in ("", expected)
            for value, expected in (
                (str(getattr(outcome, "symbol", "")), self.symbol),
                (str(getattr(outcome, "timeframe", "")), timeframe),
                (str(getattr(outcome, "regime", "")), regime),
            )
        ):
            _fail("SEARCH_FAILURE_CONTEXT_MISMATCH")
        body = {
            "schema_version": FAILURE_ARTIFACT_SCHEMA,
            "anchor_id": self.anchor["anchor_id"],
            "symbol": self.symbol,
            "timeframe": timeframe,
            "regime": regime,
            "failure_code": _reason(
                outcome.reason_code, "SEARCH_FAILURE_REASON_INVALID"
            ),
            "attempted_trials_exact": attempted_trials_exact,
            "search_outcome": outcome.to_dict(),
            "economic_no_winner": False,
        }
        entry = self.sink.put_json(
            f"{self.anchor['anchor_id']}/adapter/{timeframe}/{regime}/"
            "search-technical-failure",
            "historical_optimizer_search_failure",
            body,
        )
        self._store_process_state(
            timeframe=timeframe,
            regime=regime,
            state_kind=book.TECHNICAL_FAILURE,
            reason_code=outcome.reason_code,
            attempted_trials_exact=attempted_trials_exact,
            artifact_descriptor=entry["descriptor"],
        )

    def record_unsearched_cell(
        self,
        timeframe: str,
        regime: str,
        *,
        reason_code: str,
    ) -> None:
        """Record an explicit zero-attempt observed cell; missing cells still fail."""

        key = (timeframe, regime)
        if (
            self._finished
            or self._active is not None
            or key not in self._bindings
            or key in self._evidence
        ):
            _fail("UNSEARCHED_EVENT_INVALID")
        self._store_process_state(
            timeframe=timeframe,
            regime=regime,
            state_kind=book.UNSEARCHED,
            reason_code=_reason(reason_code, "UNSEARCHED_REASON_INVALID"),
            attempted_trials_exact=0,
        )

    def finish(self) -> orchestration.AnchorSymbolProduct:
        """Seal exactly 24 cells into one orchestration product."""

        if self._finished or self._active is not None:
            _fail("SYMBOL_CAPTURE_NOT_FINISHABLE")
        if tuple(self._source) != tuple(book.TIMEFRAME_ORDER):
            _fail("TIMEFRAME_ROSTER_INCOMPLETE")
        expected_cells = tuple(
            (timeframe, regime)
            for timeframe in book.TIMEFRAME_ORDER
            for regime in book.REGIME_ORDER
        )
        if set(self._bindings) != set(expected_cells):
            _fail("HORIZON_BINDING_ROSTER_INCOMPLETE")
        if set(self._evidence) != set(expected_cells):
            _fail("BOOK_EVIDENCE_ROSTER_INCOMPLETE")
        if set(self._cell_records) != set(expected_cells):
            _fail("CELL_RECEIPT_ROSTER_INCOMPLETE")
        bindings = tuple(self._bindings[cell] for cell in expected_cells)
        plans = tuple(self._horizon_plans[cell] for cell in expected_cells)
        evidence = tuple(self._evidence[cell] for cell in expected_cells)
        records = [self._cell_records[cell] for cell in expected_cells]
        state_counts: dict[str, int] = {}
        for record in records:
            state = str(record["historical_state"])
            state_counts[state] = state_counts.get(state, 0) + 1
        graph_roots: list[dict[str, Any]] = []
        for timeframe, regime in expected_cells:
            record = self._cell_records[(timeframe, regime)]
            for phase, field, artifact_type in (
                (
                    "frozen-descriptor-graph",
                    "frozen_descriptor_graph",
                    "historical_raw_winner_descriptor_graph",
                ),
                (
                    "horizon-winner-cross-binding",
                    "horizon_winner_cross_binding",
                    "historical_horizon_winner_cross_binding",
                ),
                (
                    "production-descriptor-graph",
                    "production_descriptor_graph",
                    "historical_raw_winner_descriptor_graph",
                ),
            ):
                descriptor = record.get(field)
                if descriptor is None:
                    continue
                graph_roots.append(
                    {
                        "artifact_id": (
                            f"{self.anchor['anchor_id']}/adapter/{timeframe}/"
                            f"{regime}/{phase}"
                        ),
                        "artifact_type": artifact_type,
                        "descriptor": _descriptor(
                            descriptor, "RAW_DESCRIPTOR_GRAPH_ROOT_INVALID"
                        ),
                    }
                )
        graph_roots.sort(key=lambda item: item["artifact_id"])
        graph_root_body = {
            "schema_version": RAW_DESCRIPTOR_GRAPH_SCHEMA,
            "generation_id": self.context.generation_id,
            "generation_context_sha256": self.context.context_sha256,
            "producer_request_sha256": self.request.receipt_sha256,
            "run_id": self.request_body["run_id"],
            "anchor_id": self.anchor["anchor_id"],
            "symbol": self.symbol,
            "phase": "SYMBOL_ROOT",
            "artifact_namespace": self.raw_artifact_namespace,
            "artifact_count": len(graph_roots),
            "artifact_total_bytes": sum(
                item["descriptor"]["size_bytes"] for item in graph_roots
            ),
            "artifact_roster_sha256": _sha256(graph_roots),
            "artifacts": graph_roots,
            "complete_for_phase": True,
            "confers_causal_current_live_or_capital_authority": False,
        }
        graph_root_entry = self.sink.put_json(
            f"{self.anchor['anchor_id']}/adapter/raw-winner-descriptor-graph",
            "historical_raw_winner_descriptor_graph_root",
            graph_root_body,
        )
        graph_root_descriptor = _descriptor(
            graph_root_entry["descriptor"],
            "RAW_DESCRIPTOR_GRAPH_ROOT_INVALID",
        )
        index = {
            "schema_version": SYMBOL_INDEX_SCHEMA,
            "capture_contract_version": CAPTURE_CONTRACT_VERSION,
            "producer_release_sha256": HISTORICAL_PRODUCER_RELEASE_SHA256,
            "generation_id": self.context.generation_id,
            "generation_context_sha256": self.context.context_sha256,
            "producer_request_sha256": self.request.receipt_sha256,
            "anchor": dict(self.anchor),
            "symbol": self.symbol,
            "source_groups": [
                self._source[timeframe].to_dict() for timeframe in book.TIMEFRAME_ORDER
            ],
            "regime_fits": [
                self._fit[timeframe].to_dict() for timeframe in book.TIMEFRAME_ORDER
            ],
            "horizon_bindings": list(bindings),
            "cell_records": records,
            "state_counts": dict(sorted(state_counts.items())),
            "cell_count": len(records),
            "raw_artifact_namespace": self.raw_artifact_namespace,
            "raw_descriptor_graph": graph_root_descriptor,
            "raw_descriptor_graph_root_count": len(graph_roots),
            "raw_descriptor_graph_roster_sha256": _sha256(graph_roots),
            "raw_vectors_spooled_before_compaction": True,
            "posthoc_evidence_reconstruction": False,
            "complete_sparse_state_roster": True,
            "confers_causal_current_live_or_capital_authority": False,
        }
        entry = self.sink.put_json(
            f"{self.anchor['anchor_id']}/adapter/production-index",
            "historical_symbol_production_index",
            index,
        )
        index_descriptor = _descriptor(
            entry["descriptor"], "SYMBOL_INDEX_DESCRIPTOR_INVALID"
        )
        payload = {
            "schema_version": OPTIMIZER_PAYLOAD_SCHEMA,
            "capture_contract_version": CAPTURE_CONTRACT_VERSION,
            "producer_release_sha256": HISTORICAL_PRODUCER_RELEASE_SHA256,
            "generation_context_sha256": self.context.context_sha256,
            "producer_request_sha256": self.request.receipt_sha256,
            "anchor_id": self.anchor["anchor_id"],
            "symbol": self.symbol,
            "cell_count": len(records),
            "state_counts": dict(sorted(state_counts.items())),
            "raw_artifact_namespace": self.raw_artifact_namespace,
            "raw_descriptor_graph": graph_root_descriptor,
            "raw_descriptor_graph_root_count": len(graph_roots),
            "raw_descriptor_graph_roster_sha256": _sha256(graph_roots),
            "production_index": index_descriptor,
            "complete_sparse_state_roster": True,
            "posthoc_evidence_reconstruction": False,
            "confers_causal_current_live_or_capital_authority": False,
        }
        self._finished = True
        return orchestration.AnchorSymbolProduct(
            symbol=self.symbol,
            horizon_plans=plans,
            book_evidence=evidence,
            optimizer_payload=payload,
        )


def _owned_decision_envelope(value: Any, code: str) -> dict[str, Any]:
    """Release a deterministic Book decision without its process-local seal."""

    try:
        payload = value.to_dict()
    except Exception as exc:
        raise HistoricalProductionAdapterError(code) from exc
    if type(payload) is not dict:
        _fail(code)
    body = dict(payload)
    receipt = body.pop("receipt_sha256", None)
    if (
        type(receipt) is not str
        or hashlib.sha256(_canonical_bytes(body)).hexdigest() != receipt
    ):
        _fail(code)
    return {
        "payload": json.loads(_canonical_bytes(body).decode("ascii")),
        "receipt_sha256": receipt,
    }


def _spool_ascii_json_descriptor(
    context: authority_spool.GenerationSpoolContext,
    value: Mapping[str, Any],
    code: str,
) -> dict[str, Any]:
    """Write exact ASCII canonical bytes without registering bridge routing."""

    if not isinstance(value, Mapping):
        _fail(code)
    encoded = _canonical_bytes(dict(value))
    try:
        descriptor = authority_spool.write_opaque_artifact(
            context,
            (encoded,),
            format=raw_winner.ASCII_JSON_FORMAT,
        )
    except Exception as exc:
        raise HistoricalProductionAdapterError(code) from exc
    return _descriptor(descriptor, code)


def _spool_ascii_bytes_descriptor(
    context: authority_spool.GenerationSpoolContext,
    value: bytes,
    code: str,
) -> dict[str, Any]:
    if type(value) is not bytes or not value:
        _fail(code)
    try:
        parsed = json.loads(value.decode("ascii"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise HistoricalProductionAdapterError(code) from exc
    if type(parsed) is not dict or _canonical_bytes(parsed) != value:
        _fail(code)
    try:
        descriptor = authority_spool.write_opaque_artifact(
            context,
            (value,),
            format=raw_winner.ASCII_JSON_FORMAT,
        )
    except Exception as exc:
        raise HistoricalProductionAdapterError(code) from exc
    return _descriptor(descriptor, code)


def _spool_plan_descriptor(
    context: authority_spool.GenerationSpoolContext,
    plan: Any,
) -> dict[str, Any]:
    body = _released_horizon_plan_body(plan, "WORKER_HORIZON_PLAN_INVALID")
    try:
        descriptor = authority_spool.write_canonical_json_artifact(context, body)
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "WORKER_HORIZON_PLAN_SPOOL_FAILED"
        ) from exc
    return {
        "descriptor": _descriptor(
            descriptor,
            "WORKER_HORIZON_PLAN_DESCRIPTOR_INVALID",
        ),
        "receipt_sha256": plan.receipt_sha256,
    }


def spool_historical_symbol_worker_bundle(
    capture: HistoricalSymbolSearchCapture,
) -> dict[str, Any]:
    """Finish one worker capture and persist only a compact canonical bridge.

    No sealed Python object crosses the spawn boundary.  The bridge is routing
    evidence only and is deliberately absent from the final symbol manifest.
    """

    if type(capture) is not HistoricalSymbolSearchCapture:
        _fail("WORKER_CAPTURE_INVALID")
    product = capture.finish()
    plans = tuple(product.horizon_plans)
    evidence_roster = tuple(product.book_evidence)
    expected_cells = tuple(
        (timeframe, regime)
        for timeframe in book.TIMEFRAME_ORDER
        for regime in book.REGIME_ORDER
    )
    if (
        product.symbol != capture.symbol
        or len(plans) != len(expected_cells)
        or len(evidence_roster) != len(expected_cells)
    ):
        _fail("WORKER_PRODUCT_ROSTER_INVALID")

    timeframe_rows: list[dict[str, Any]] = []
    for timeframe in book.TIMEFRAME_ORDER:
        source = capture._source.get(timeframe)  # noqa: SLF001
        fit = capture._fit.get(timeframe)  # noqa: SLF001
        if (
            type(source) is not book.HistoricalSourceGroupDecision
            or type(fit) is not book.RegimeFitDecision
        ):
            _fail("WORKER_TIMEFRAME_AUTHORITY_INVALID")
        timeframe_rows.append(
            {
                "timeframe": timeframe,
                "source_group": _owned_decision_envelope(
                    source,
                    "WORKER_SOURCE_DECISION_INVALID",
                ),
                "regime_fit": _owned_decision_envelope(
                    fit,
                    "WORKER_REGIME_FIT_INVALID",
                ),
            }
        )

    cell_rows: list[dict[str, Any]] = []
    for (timeframe, regime), plan, evidence in zip(
        expected_cells,
        plans,
        evidence_roster,
        strict=True,
    ):
        if (
            type(evidence) is not book.HistoricalBookCellEvidence
            or evidence.symbol != capture.symbol
            or evidence.timeframe != timeframe
            or evidence.regime != regime
        ):
            _fail("WORKER_CELL_EVIDENCE_INVALID")
        plan_binding = _spool_plan_descriptor(capture.context, plan)
        winner_binding = None
        process_binding = None
        if evidence.winner_decision is not None or evidence.winner_request is not None:
            if (
                type(evidence.winner_decision)
                is not winner_selection.WinnerSelectionDecision
                or type(evidence.winner_request)
                is not winner_selection.WinnerSelectionRequest
                or evidence.process_state_decision is not None
            ):
                _fail("WORKER_WINNER_EVIDENCE_INVALID")
            winner_binding = {
                "request": _spool_ascii_json_descriptor(
                    capture.context,
                    evidence.winner_request.to_dict(),
                    "WORKER_WINNER_REQUEST_SPOOL_FAILED",
                ),
                "request_sha256": evidence.winner_request.request_sha256,
                "decision": _spool_ascii_json_descriptor(
                    capture.context,
                    evidence.winner_decision.to_dict(),
                    "WORKER_WINNER_DECISION_SPOOL_FAILED",
                ),
                "decision_sha256": evidence.winner_decision.receipt_sha256,
            }
        else:
            state = evidence.process_state_decision
            if type(state) is not book.HistoricalProcessStateDecision:
                _fail("WORKER_PROCESS_EVIDENCE_INVALID")
            insufficient = None
            optional_values = (
                evidence.insufficient_horizon_plan,
                evidence.insufficient_process_rejection_request,
                evidence.insufficient_process_rejection_decision,
                evidence.insufficient_horizon_winner_cross_binding_bytes,
                evidence.insufficient_winner_raw_production_receipt_bytes,
            )
            if state.state_kind == book.INSUFFICIENT_EVIDENCE:
                (
                    insufficient_plan,
                    rejection_request,
                    rejection_decision,
                    cross_bytes,
                    production_bytes,
                ) = optional_values
                if insufficient_plan is None:
                    _fail("WORKER_INSUFFICIENT_EVIDENCE_INVALID")
                if insufficient_plan.receipt_sha256 != plan.receipt_sha256:
                    _fail("WORKER_INSUFFICIENT_HORIZON_SPLICE")
                raw_values = (
                    rejection_request,
                    rejection_decision,
                    cross_bytes,
                    production_bytes,
                )
                if all(value is None for value in raw_values):
                    insufficient = {
                        "request": None,
                        "request_sha256": None,
                        "decision": None,
                        "decision_sha256": None,
                        "cross_binding": None,
                        "production_receipt": None,
                    }
                elif (
                    type(rejection_request) is winner_selection.WinnerSelectionRequest
                    and type(rejection_decision)
                    is winner_selection.WinnerSelectionDecision
                    and type(cross_bytes) is bytes
                    and type(production_bytes) is bytes
                ):
                    insufficient = {
                        "request": _spool_ascii_json_descriptor(
                            capture.context,
                            rejection_request.to_dict(),
                            "WORKER_REJECTION_REQUEST_SPOOL_FAILED",
                        ),
                        "request_sha256": rejection_request.request_sha256,
                        "decision": _spool_ascii_json_descriptor(
                            capture.context,
                            rejection_decision.to_dict(),
                            "WORKER_REJECTION_DECISION_SPOOL_FAILED",
                        ),
                        "decision_sha256": rejection_decision.receipt_sha256,
                        "cross_binding": _spool_ascii_bytes_descriptor(
                            capture.context,
                            cross_bytes,
                            "WORKER_CROSS_BINDING_SPOOL_FAILED",
                        ),
                        "production_receipt": _spool_ascii_bytes_descriptor(
                            capture.context,
                            production_bytes,
                            "WORKER_PRODUCTION_RECEIPT_SPOOL_FAILED",
                        ),
                    }
                else:
                    _fail("WORKER_INSUFFICIENT_EVIDENCE_INVALID")
            elif any(value is not None for value in optional_values):
                _fail("WORKER_NONINSUFFICIENT_EVIDENCE_CARRIES_RAW_BINDING")
            process_binding = {
                "decision": _owned_decision_envelope(
                    state,
                    "WORKER_PROCESS_DECISION_INVALID",
                ),
                "insufficient": insufficient,
            }
        cell_rows.append(
            {
                "timeframe": timeframe,
                "regime": regime,
                "horizon_plan": plan_binding,
                "winner": winner_binding,
                "process_state": process_binding,
            }
        )

    artifact_roster = capture.sink.artifact_roster()
    normalized_roster = [
        _artifact_entry(entry, "WORKER_ARTIFACT_ROSTER_INVALID")
        for entry in artifact_roster
    ]
    if normalized_roster != sorted(
        normalized_roster, key=lambda item: item["artifact_id"]
    ) or len({item["artifact_id"] for item in normalized_roster}) != len(
        normalized_roster
    ):
        _fail("WORKER_ARTIFACT_ROSTER_INVALID")
    request_body = capture.request.to_dict()
    bundle = {
        "schema_version": HISTORICAL_SYMBOL_WORKER_BUNDLE_SCHEMA,
        "generation_id": capture.context.generation_id,
        "generation_context_sha256": capture.context.context_sha256,
        "producer_request_sha256": capture.request.receipt_sha256,
        "run_id": request_body["run_id"],
        "anchor": dict(capture.anchor),
        "symbol": capture.symbol,
        "timeframe_order": list(book.TIMEFRAME_ORDER),
        "regime_order": list(book.REGIME_ORDER),
        "timeframes": timeframe_rows,
        "cells": cell_rows,
        "optimizer_payload": product.optimizer_payload,
        "artifact_count": len(normalized_roster),
        "artifact_total_bytes": sum(
            int(entry["descriptor"]["size_bytes"]) for entry in normalized_roster
        ),
        "artifact_roster_sha256": _sha256(normalized_roster),
        "artifacts": normalized_roster,
        "bridge_registered_in_symbol_manifest": False,
        "confers_causal_current_live_or_capital_authority": False,
    }
    try:
        descriptor = authority_spool.write_canonical_json_artifact(
            capture.context,
            bundle,
        )
    except Exception as exc:
        raise HistoricalProductionAdapterError("WORKER_BUNDLE_SPOOL_FAILED") from exc
    normalized = _descriptor(descriptor, "WORKER_BUNDLE_DESCRIPTOR_INVALID")
    if normalized["size_bytes"] > HISTORICAL_SYMBOL_WORKER_BUNDLE_MAX_BYTES:
        _fail("WORKER_BUNDLE_SIZE_BOUND_EXCEEDED")
    return normalized


def _reopen_worker_descriptor_bytes(
    context: authority_spool.GenerationSpoolContext,
    descriptor: Mapping[str, Any],
    *,
    maximum_bytes: int,
    expected_format: str,
    code: str,
) -> bytes:
    normalized = _descriptor(dict(descriptor), code)
    if (
        normalized["format"] != expected_format
        or normalized["size_bytes"] > maximum_bytes
    ):
        _fail(code)
    try:
        expected_path = authority_spool._content_relative_path(  # noqa: SLF001
            normalized["sha256"],
            normalized["format"],
        )
        digest, size, encoded = authority_spool._stream_file(  # noqa: SLF001
            context.root,
            normalized["relative_path"],
            maximum_bytes=maximum_bytes,
            expected_size=normalized["size_bytes"],
            collect=True,
            chunk_bytes=context.limits.stream_chunk_bytes,
        )
    except HistoricalProductionAdapterError:
        raise
    except Exception as exc:
        raise HistoricalProductionAdapterError(code) from exc
    if (
        normalized["relative_path"] != expected_path
        or encoded is None
        or digest != normalized["sha256"]
        or size != normalized["size_bytes"]
    ):
        _fail(code)
    return encoded


def _reopen_worker_json_descriptor(
    context: authority_spool.GenerationSpoolContext,
    descriptor: Mapping[str, Any],
    *,
    maximum_bytes: int,
    code: str,
) -> dict[str, Any]:
    encoded = _reopen_worker_descriptor_bytes(
        context,
        descriptor,
        maximum_bytes=maximum_bytes,
        expected_format=authority_spool.CANONICAL_JSON_FORMAT,
        code=code,
    )
    try:
        value = authority_spool._strict_json_bytes(encoded, maximum_bytes)  # noqa: SLF001
    except Exception as exc:
        raise HistoricalProductionAdapterError(code) from exc
    if type(value) is not dict:
        _fail(code)
    return value


def _reopen_worker_ascii_json_descriptor(
    context: authority_spool.GenerationSpoolContext,
    descriptor: Mapping[str, Any],
    *,
    maximum_bytes: int,
    code: str,
) -> tuple[dict[str, Any], bytes]:
    encoded = _reopen_worker_descriptor_bytes(
        context,
        descriptor,
        maximum_bytes=maximum_bytes,
        expected_format=raw_winner.ASCII_JSON_FORMAT,
        code=code,
    )
    try:
        value = json.loads(encoded.decode("ascii"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise HistoricalProductionAdapterError(code) from exc
    if type(value) is not dict or _canonical_bytes(value) != encoded:
        _fail(code)
    return value, encoded


def _envelope_payload(value: Any, code: str) -> dict[str, Any]:
    if (
        type(value) is not dict
        or set(value) != {"payload", "receipt_sha256"}
        or type(value.get("payload")) is not dict
        or hashlib.sha256(_canonical_bytes(value["payload"])).hexdigest()
        != value.get("receipt_sha256")
    ):
        _fail(code)
    return value["payload"]


def _rebuild_source_decision(value: Any) -> book.HistoricalSourceGroupDecision:
    payload = _envelope_payload(value, "WORKER_SOURCE_DECISION_INVALID")
    decision = book.create_historical_source_group_decision(
        symbol=payload.get("symbol"),
        timeframe=payload.get("timeframe"),
        availability=payload.get("availability"),
        reason_code=payload.get("reason_code"),
        source_contract_sha256=payload.get("source_contract_sha256"),
        source_group_evidence_sha256=payload.get("source_group_evidence_sha256"),
        acquisition_attempts_exact=payload.get("acquisition_attempts_exact"),
        activation_id=payload.get("activation_id"),
        activation_manifest_sha256=payload.get("activation_manifest_sha256"),
        activation_entry_sha256=payload.get("activation_entry_sha256"),
        native_source_sha256=payload.get("native_source_sha256"),
        terminal_proof_sha256=payload.get("terminal_proof_sha256"),
    )
    if (
        decision is None
        or _owned_decision_envelope(
            decision,
            "WORKER_SOURCE_DECISION_CHANGED",
        )
        != value
    ):
        _fail("WORKER_SOURCE_DECISION_CHANGED")
    return decision


def _rebuild_regime_fit_decision(
    value: Any,
    source: book.HistoricalSourceGroupDecision,
) -> book.RegimeFitDecision:
    payload = _envelope_payload(value, "WORKER_REGIME_FIT_INVALID")
    decision = book.create_regime_fit_decision(
        source_group_decision=source,
        fit_state=payload.get("fit_state"),
        reason_code=payload.get("reason_code"),
        regime_fit_policy_sha256=payload.get("regime_fit_policy_sha256"),
        detector_process_sha256=payload.get("detector_process_sha256"),
        regime_fit_evidence_sha256=payload.get("regime_fit_evidence_sha256"),
        four_regime_mapping_sha256=payload.get("four_regime_mapping_sha256"),
        regime_params_sha256=payload.get("regime_params_sha256"),
        regime_detector_record=payload.get("regime_detector_record"),
        train_regime_support=payload.get("train_regime_support"),
    )
    if (
        decision is None
        or _owned_decision_envelope(
            decision,
            "WORKER_REGIME_FIT_CHANGED",
        )
        != value
    ):
        _fail("WORKER_REGIME_FIT_CHANGED")
    return decision


def _rebuild_horizon_plan(
    context: authority_spool.GenerationSpoolContext,
    value: Any,
) -> Any:
    if type(value) is not dict or set(value) != {"descriptor", "receipt_sha256"}:
        _fail("WORKER_HORIZON_PLAN_INVALID")
    body = _reopen_worker_json_descriptor(
        context,
        value["descriptor"],
        maximum_bytes=context.limits.max_json_bytes,
        code="WORKER_HORIZON_PLAN_REOPEN_FAILED",
    )
    unobserved = "regime_support_binding" in body
    request = {
        "schema_version": (
            training_horizon._UNOBSERVED_REQUEST_SCHEMA  # noqa: SLF001
            if unobserved
            else training_horizon._REQUEST_SCHEMA  # noqa: SLF001
        ),
        "run_id": body.get("run_id"),
        "anchor": body.get("anchor"),
        "cell": body.get("cell"),
        "native_prefix": body.get("native_prefix"),
        "dependency_policy": body.get("dependency_policy"),
        "horizon_policy": body.get("horizon_policy"),
        "candidates": body.get("candidates"),
        "trials": body.get("trials"),
    }
    if unobserved:
        request["regime_support_binding"] = body.get("regime_support_binding")
    try:
        plan = training_horizon.build_historical_horizon_plan(request)
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "WORKER_HORIZON_PLAN_REBUILD_FAILED"
        ) from exc
    if plan.to_dict() != body or plan.receipt_sha256 != value.get("receipt_sha256"):
        _fail("WORKER_HORIZON_PLAN_CHANGED")
    return plan


def _rebuild_winner_pair(
    context: authority_spool.GenerationSpoolContext,
    value: Any,
    *,
    symbol: str,
    timeframe: str,
    regime: str,
) -> tuple[
    winner_selection.WinnerSelectionRequest,
    winner_selection.WinnerSelectionDecision,
]:
    if type(value) is not dict or set(value) != {
        "request",
        "request_sha256",
        "decision",
        "decision_sha256",
    }:
        _fail("WORKER_WINNER_BINDING_INVALID")
    request_body, _ = _reopen_worker_ascii_json_descriptor(
        context,
        value["request"],
        maximum_bytes=winner_selection.MAX_REQUEST_BYTES,
        code="WORKER_WINNER_REQUEST_REOPEN_FAILED",
    )
    if set(request_body) != {
        "contract_version",
        "precommitment",
        "attempt_ledger",
        "evidence_ledger",
    }:
        _fail("WORKER_WINNER_REQUEST_INVALID")
    wrappers: list[dict[str, Any]] = []
    for name in ("precommitment", "attempt_ledger", "evidence_ledger"):
        wrapper = request_body.get(name)
        if (
            type(wrapper) is not dict
            or set(wrapper) != {"payload", "payload_sha256"}
            or type(wrapper.get("payload")) is not dict
            or _sha256(wrapper["payload"]) != wrapper.get("payload_sha256")
        ):
            _fail("WORKER_WINNER_REQUEST_INVALID")
        wrappers.append(wrapper["payload"])
    request = winner_selection.create_winner_selection_request(*wrappers)
    if (
        request is None
        or request.to_dict() != request_body
        or request.request_sha256 != value.get("request_sha256")
    ):
        _fail("WORKER_WINNER_REQUEST_CHANGED")
    decision_body, _ = _reopen_worker_ascii_json_descriptor(
        context,
        value["decision"],
        maximum_bytes=winner_selection.MAX_DECISION_BYTES,
        code="WORKER_WINNER_DECISION_REOPEN_FAILED",
    )
    decision = winner_selection.evaluate_historical_cell_winner(request)
    if (
        type(decision) is not winner_selection.WinnerSelectionDecision
        or decision.to_dict() != decision_body
        or decision.receipt_sha256 != value.get("decision_sha256")
    ):
        _fail("WORKER_WINNER_DECISION_CHANGED")
    cell = decision_body.get("cell")
    if (
        type(cell) is not dict
        or cell.get("symbol") != symbol
        or cell.get("timeframe") != timeframe
        or cell.get("regime") != regime
    ):
        _fail("WORKER_WINNER_CELL_SPLICE")
    return request, decision


def _verify_worker_production_receipt(
    *,
    context: authority_spool.GenerationSpoolContext,
    descriptor: Mapping[str, Any],
    cross_descriptor: Mapping[str, Any],
    cross_bytes: bytes,
    request_descriptor: Mapping[str, Any],
    decision_descriptor: Mapping[str, Any],
    request: winner_selection.WinnerSelectionRequest,
    decision: winner_selection.WinnerSelectionDecision,
    symbol: str,
    timeframe: str,
    regime: str,
) -> bytes:
    body, encoded = _reopen_worker_ascii_json_descriptor(
        context,
        descriptor,
        maximum_bytes=context.limits.max_json_bytes,
        code="WORKER_PRODUCTION_RECEIPT_REOPEN_FAILED",
    )
    roster = body.get("artifact_roster")
    if (
        body.get("schema_version") != raw_winner.PRODUCTION_SCHEMA
        or body.get("generation_context_sha256") != context.context_sha256
        or body.get("symbol") != symbol
        or body.get("timeframe") != timeframe
        or body.get("regime") != regime
        or body.get("request_sha256") != request.request_sha256
        or body.get("decision_receipt_sha256") != decision.receipt_sha256
        or body.get("provenance_cross_binding_sha256")
        != hashlib.sha256(cross_bytes).hexdigest()
        or type(roster) is not dict
        or roster.get("winner-request")
        != _descriptor(
            dict(request_descriptor),
            "WORKER_WINNER_REQUEST_DESCRIPTOR_INVALID",
        )
        or roster.get("winner-decision")
        != _descriptor(
            dict(decision_descriptor),
            "WORKER_WINNER_DECISION_DESCRIPTOR_INVALID",
        )
        or _descriptor(
            dict(cross_descriptor),
            "WORKER_CROSS_BINDING_DESCRIPTOR_INVALID",
        )["sha256"]
        != hashlib.sha256(cross_bytes).hexdigest()
    ):
        _fail("WORKER_PRODUCTION_RECEIPT_SPLICE")
    return encoded


def _verify_worker_index_projection(
    *,
    context: authority_spool.GenerationSpoolContext,
    request_body: Mapping[str, Any],
    request_sha256: str,
    symbol: str,
    plans: Sequence[Any],
    evidence_roster: Sequence[book.HistoricalBookCellEvidence],
    artifacts: Sequence[Mapping[str, Any]],
    optimizer_payload: Mapping[str, Any],
) -> None:
    """Bind reconstructed Book inputs to the worker's final production index."""

    anchor = request_body.get("anchor")
    anchor_id = anchor.get("anchor_id") if type(anchor) is dict else None
    expected_id = f"{anchor_id}/adapter/production-index"
    matches = [
        entry
        for entry in artifacts
        if entry.get("artifact_id") == expected_id
        and entry.get("artifact_type") == "historical_symbol_production_index"
    ]
    if len(matches) != 1:
        _fail("WORKER_PRODUCTION_INDEX_MISSING")
    index_entry = _artifact_entry(
        dict(matches[0]),
        "WORKER_PRODUCTION_INDEX_ENTRY_INVALID",
    )
    index = _reopen_worker_json_descriptor(
        context,
        index_entry["descriptor"],
        maximum_bytes=context.limits.max_json_bytes,
        code="WORKER_PRODUCTION_INDEX_REOPEN_FAILED",
    )
    expected_cells = tuple(
        (timeframe, regime)
        for timeframe in book.TIMEFRAME_ORDER
        for regime in book.REGIME_ORDER
    )
    if len(plans) != len(expected_cells) or len(evidence_roster) != len(expected_cells):
        _fail("WORKER_PRODUCTION_INDEX_ROSTER_INVALID")
    source_groups: list[dict[str, Any]] = []
    regime_fits: list[dict[str, Any]] = []
    offset = 0
    for timeframe in book.TIMEFRAME_ORDER:
        evidence = evidence_roster[offset]
        if evidence.timeframe != timeframe:
            _fail("WORKER_PRODUCTION_INDEX_TIMEFRAME_ORDER_INVALID")
        source_groups.append(evidence.source_group_decision.to_dict())
        regime_fits.append(evidence.regime_fit_decision.to_dict())
        offset += len(book.REGIME_ORDER)
    try:
        horizon_bindings = [
            orchestration.historical_horizon_plan_binding(plan) for plan in plans
        ]
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "WORKER_PRODUCTION_INDEX_HORIZON_REOPEN_FAILED"
        ) from exc
    records = index.get("cell_records")
    if (
        index.get("schema_version") != SYMBOL_INDEX_SCHEMA
        or index.get("capture_contract_version") != CAPTURE_CONTRACT_VERSION
        or index.get("producer_release_sha256") != HISTORICAL_PRODUCER_RELEASE_SHA256
        or index.get("generation_id") != context.generation_id
        or index.get("generation_context_sha256") != context.context_sha256
        or index.get("producer_request_sha256") != request_sha256
        or index.get("anchor") != anchor
        or index.get("symbol") != symbol
        or index.get("source_groups") != source_groups
        or index.get("regime_fits") != regime_fits
        or index.get("horizon_bindings") != horizon_bindings
        or type(records) is not list
        or len(records) != len(expected_cells)
        or index.get("cell_count") != len(expected_cells)
        or index.get("complete_sparse_state_roster") is not True
        or index.get("posthoc_evidence_reconstruction") is not False
        or index.get("confers_causal_current_live_or_capital_authority") is not False
    ):
        _fail("WORKER_PRODUCTION_INDEX_PROJECTION_CHANGED")
    state_counts: dict[str, int] = {}
    for (timeframe, regime), evidence, record in zip(
        expected_cells,
        evidence_roster,
        records,
        strict=True,
    ):
        if (
            type(record) is not dict
            or record.get("symbol") != symbol
            or record.get("timeframe") != timeframe
            or record.get("regime") != regime
        ):
            _fail("WORKER_PRODUCTION_INDEX_CELL_CHANGED")
        if evidence.winner_decision is not None:
            expected_state = "WINNER_DECISION"
            if (
                type(evidence.winner_request)
                is not winner_selection.WinnerSelectionRequest
                or record.get("process_state_receipt_sha256") is not None
                or record.get("winner_request_sha256")
                != evidence.winner_request.request_sha256
                or record.get("winner_decision_sha256")
                != evidence.winner_decision.receipt_sha256
            ):
                _fail("WORKER_PRODUCTION_INDEX_WINNER_CHANGED")
        else:
            decision = evidence.process_state_decision
            if type(decision) is not book.HistoricalProcessStateDecision:
                _fail("WORKER_PRODUCTION_INDEX_PROCESS_CHANGED")
            expected_state = decision.state_kind
            if (
                record.get("historical_state") != expected_state
                or record.get("process_state_receipt_sha256") != decision.receipt_sha256
                or record.get("winner_request_sha256") is not None
                or record.get("winner_decision_sha256") is not None
                or record.get("insufficient_horizon_plan_receipt_sha256")
                != getattr(evidence.insufficient_horizon_plan, "receipt_sha256", None)
                or record.get("process_rejection_request_sha256")
                != getattr(
                    evidence.insufficient_process_rejection_request,
                    "request_sha256",
                    None,
                )
                or record.get("process_rejection_decision_sha256")
                != getattr(
                    evidence.insufficient_process_rejection_decision,
                    "receipt_sha256",
                    None,
                )
            ):
                _fail("WORKER_PRODUCTION_INDEX_PROCESS_CHANGED")
        if record.get("historical_state") != expected_state:
            _fail("WORKER_PRODUCTION_INDEX_CELL_STATE_CHANGED")
        state_counts[expected_state] = state_counts.get(expected_state, 0) + 1
    if index.get("state_counts") != dict(sorted(state_counts.items())):
        _fail("WORKER_PRODUCTION_INDEX_STATE_COUNTS_CHANGED")
    expected_optimizer_fields = {
        "schema_version",
        "capture_contract_version",
        "producer_release_sha256",
        "generation_context_sha256",
        "producer_request_sha256",
        "anchor_id",
        "symbol",
        "cell_count",
        "state_counts",
        "raw_artifact_namespace",
        "raw_descriptor_graph",
        "raw_descriptor_graph_root_count",
        "raw_descriptor_graph_roster_sha256",
        "production_index",
        "complete_sparse_state_roster",
        "posthoc_evidence_reconstruction",
        "confers_causal_current_live_or_capital_authority",
    }
    if (
        set(optimizer_payload) != expected_optimizer_fields
        or optimizer_payload.get("schema_version") != OPTIMIZER_PAYLOAD_SCHEMA
        or optimizer_payload.get("capture_contract_version") != CAPTURE_CONTRACT_VERSION
        or optimizer_payload.get("producer_release_sha256")
        != HISTORICAL_PRODUCER_RELEASE_SHA256
        or optimizer_payload.get("generation_context_sha256") != context.context_sha256
        or optimizer_payload.get("producer_request_sha256") != request_sha256
        or optimizer_payload.get("anchor_id") != anchor_id
        or optimizer_payload.get("symbol") != symbol
        or optimizer_payload.get("cell_count") != len(expected_cells)
        or optimizer_payload.get("state_counts") != dict(sorted(state_counts.items()))
        or optimizer_payload.get("production_index") != index_entry["descriptor"]
        or optimizer_payload.get("raw_descriptor_graph")
        != index.get("raw_descriptor_graph")
        or optimizer_payload.get("complete_sparse_state_roster") is not True
        or optimizer_payload.get("posthoc_evidence_reconstruction") is not False
        or optimizer_payload.get("confers_causal_current_live_or_capital_authority")
        is not False
    ):
        _fail("WORKER_OPTIMIZER_PAYLOAD_PROJECTION_CHANGED")


def reopen_historical_symbol_worker_bundle(
    context: authority_spool.GenerationSpoolContext,
    request: orchestration.AnchorProducerRequest,
    descriptor: Mapping[str, Any],
) -> orchestration.AnchorSymbolProduct:
    """Rebuild process-local seals, then attach the verified worker roster."""

    if (
        type(context) is not authority_spool.GenerationSpoolContext
        or type(request) is not orchestration.AnchorProducerRequest
    ):
        _fail("WORKER_BUNDLE_REOPEN_INPUT_INVALID")
    try:
        observed = authority_spool.reopen_generation_context(
            context.root,
            expected_generation_id=context.generation_id,
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=context.config_sha256,
        )
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "WORKER_GENERATION_CONTEXT_REOPEN_FAILED"
        ) from exc
    if observed.context_sha256 != context.context_sha256:
        _fail("WORKER_GENERATION_CONTEXT_CHANGED")
    bundle = _reopen_worker_json_descriptor(
        context,
        descriptor,
        maximum_bytes=min(
            HISTORICAL_SYMBOL_WORKER_BUNDLE_MAX_BYTES,
            context.limits.max_json_bytes,
        ),
        code="WORKER_BUNDLE_REOPEN_FAILED",
    )
    expected_fields = {
        "schema_version",
        "generation_id",
        "generation_context_sha256",
        "producer_request_sha256",
        "run_id",
        "anchor",
        "symbol",
        "timeframe_order",
        "regime_order",
        "timeframes",
        "cells",
        "optimizer_payload",
        "artifact_count",
        "artifact_total_bytes",
        "artifact_roster_sha256",
        "artifacts",
        "bridge_registered_in_symbol_manifest",
        "confers_causal_current_live_or_capital_authority",
    }
    request_body = request.to_dict()
    symbol = bundle.get("symbol")
    if (
        set(bundle) != expected_fields
        or bundle.get("schema_version") != HISTORICAL_SYMBOL_WORKER_BUNDLE_SCHEMA
        or bundle.get("generation_id") != context.generation_id
        or bundle.get("generation_context_sha256") != context.context_sha256
        or bundle.get("producer_request_sha256") != request.receipt_sha256
        or request_body.get("schema_version") != orchestration.PRODUCER_REQUEST_SCHEMA
        or bundle.get("run_id") != request_body.get("run_id")
        or bundle.get("anchor") != request_body.get("anchor")
        or type(symbol) is not str
        or symbol not in book.PINNED_SYMBOL_ORDER
        or bundle.get("timeframe_order") != list(book.TIMEFRAME_ORDER)
        or bundle.get("regime_order") != list(book.REGIME_ORDER)
        or bundle.get("bridge_registered_in_symbol_manifest") is not False
        or bundle.get("confers_causal_current_live_or_capital_authority") is not False
    ):
        _fail("WORKER_BUNDLE_BINDING_INVALID")

    timeframe_rows = bundle.get("timeframes")
    cell_rows = bundle.get("cells")
    if (
        type(timeframe_rows) is not list
        or len(timeframe_rows) != len(book.TIMEFRAME_ORDER)
        or type(cell_rows) is not list
        or len(cell_rows) != len(book.TIMEFRAME_ORDER) * len(book.REGIME_ORDER)
    ):
        _fail("WORKER_BUNDLE_ROSTER_INVALID")
    authority: dict[
        str,
        tuple[
            book.HistoricalSourceGroupDecision,
            book.RegimeFitDecision,
        ],
    ] = {}
    for timeframe, row in zip(book.TIMEFRAME_ORDER, timeframe_rows, strict=True):
        if (
            type(row) is not dict
            or set(row) != {"timeframe", "source_group", "regime_fit"}
            or row.get("timeframe") != timeframe
        ):
            _fail("WORKER_TIMEFRAME_BINDING_INVALID")
        source = _rebuild_source_decision(row["source_group"])
        fit = _rebuild_regime_fit_decision(row["regime_fit"], source)
        source_body = source.to_dict()
        fit_body = fit.to_dict()
        if (
            source_body.get("symbol") != symbol
            or source_body.get("timeframe") != timeframe
            or fit_body.get("symbol") != symbol
            or fit_body.get("timeframe") != timeframe
        ):
            _fail("WORKER_TIMEFRAME_AUTHORITY_SPLICE")
        authority[timeframe] = (source, fit)

    plans: list[Any] = []
    evidence_roster: list[book.HistoricalBookCellEvidence] = []
    expected_cells = tuple(
        (timeframe, regime)
        for timeframe in book.TIMEFRAME_ORDER
        for regime in book.REGIME_ORDER
    )
    expected_cell_fields = {
        "timeframe",
        "regime",
        "horizon_plan",
        "winner",
        "process_state",
    }
    for (timeframe, regime), row in zip(expected_cells, cell_rows, strict=True):
        if (
            type(row) is not dict
            or set(row) != expected_cell_fields
            or row.get("timeframe") != timeframe
            or row.get("regime") != regime
            or (row.get("winner") is None) == (row.get("process_state") is None)
        ):
            _fail("WORKER_CELL_BINDING_INVALID")
        plan = _rebuild_horizon_plan(context, row["horizon_plan"])
        plan_body = plan.to_dict()
        plan_anchor = plan_body.get("anchor")
        request_anchor = request_body.get("anchor")
        if (
            plan_body.get("cell")
            != {"symbol": symbol, "timeframe": timeframe, "regime": regime}
            or type(plan_anchor) is not dict
            or type(request_anchor) is not dict
            or plan_anchor.get("anchor_id") != request_anchor.get("anchor_id")
            or plan_anchor.get("selection_cutoff_utc")
            != request_anchor.get("selection_cutoff_utc")
            or plan_anchor.get("process_precommit_receipt_sha256")
            != request_body.get("process_precommit_receipt_sha256")
        ):
            _fail("WORKER_HORIZON_PLAN_SPLICE")
        source, fit = authority[timeframe]
        if row["winner"] is not None:
            winner_request, winner_decision = _rebuild_winner_pair(
                context,
                row["winner"],
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
            )
            evidence = book.winner_cell_evidence(
                winner_decision,
                winner_request,
                source,
                fit,
            )
        else:
            process_binding = row["process_state"]
            if type(process_binding) is not dict or set(process_binding) != {
                "decision",
                "insufficient",
            }:
                _fail("WORKER_PROCESS_BINDING_INVALID")
            process_payload = _envelope_payload(
                process_binding["decision"],
                "WORKER_PROCESS_DECISION_INVALID",
            )
            insufficient = process_binding["insufficient"]
            rejection_request = None
            rejection_decision = None
            cross_bytes = None
            production_bytes = None
            insufficient_plan = None
            if insufficient is not None:
                if type(insufficient) is not dict or set(insufficient) != {
                    "request",
                    "request_sha256",
                    "decision",
                    "decision_sha256",
                    "cross_binding",
                    "production_receipt",
                }:
                    _fail("WORKER_INSUFFICIENT_BINDING_INVALID")
                raw_values = tuple(
                    insufficient[name]
                    for name in (
                        "request",
                        "request_sha256",
                        "decision",
                        "decision_sha256",
                        "cross_binding",
                        "production_receipt",
                    )
                )
                if not all(value is None for value in raw_values):
                    if any(value is None for value in raw_values):
                        _fail("WORKER_INSUFFICIENT_BINDING_PARTIAL")
                    rejection_request, rejection_decision = _rebuild_winner_pair(
                        context,
                        {
                            "request": insufficient["request"],
                            "request_sha256": insufficient["request_sha256"],
                            "decision": insufficient["decision"],
                            "decision_sha256": insufficient["decision_sha256"],
                        },
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                    )
                    _, cross_bytes = _reopen_worker_ascii_json_descriptor(
                        context,
                        insufficient["cross_binding"],
                        maximum_bytes=context.limits.max_json_bytes,
                        code="WORKER_CROSS_BINDING_REOPEN_FAILED",
                    )
                    production_bytes = _verify_worker_production_receipt(
                        context=context,
                        descriptor=insufficient["production_receipt"],
                        cross_descriptor=insufficient["cross_binding"],
                        cross_bytes=cross_bytes,
                        request_descriptor=insufficient["request"],
                        decision_descriptor=insufficient["decision"],
                        request=rejection_request,
                        decision=rejection_decision,
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                    )
                insufficient_plan = plan
            state_cell = process_payload.get("cell")
            if (
                type(state_cell) is not dict
                or state_cell.get("symbol") != symbol
                or state_cell.get("timeframe") != timeframe
                or state_cell.get("regime") != regime
                or (process_payload.get("state_kind") == book.INSUFFICIENT_EVIDENCE)
                != (insufficient is not None)
            ):
                _fail("WORKER_PROCESS_CELL_SPLICE")
            process_decision = book.create_historical_process_state_decision(
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                state_kind=process_payload.get("state_kind"),
                reason_code=process_payload.get("reason_code"),
                source_group_decision=source,
                regime_fit_decision=fit,
                attempted_trials_exact=process_payload.get("attempted_trials_exact"),
                horizon_plan=insufficient_plan,
                process_rejection_request=rejection_request,
                process_rejection_decision=rejection_decision,
                horizon_winner_cross_binding_bytes=cross_bytes,
                winner_raw_production_receipt_bytes=production_bytes,
            )
            if (
                process_decision is None
                or _owned_decision_envelope(
                    process_decision,
                    "WORKER_PROCESS_DECISION_CHANGED",
                )
                != process_binding["decision"]
            ):
                _fail("WORKER_PROCESS_DECISION_CHANGED")
            evidence = book.process_state_cell_evidence(
                process_state_decision=process_decision,
                source_group_decision=source,
                regime_fit_decision=fit,
                horizon_plan=insufficient_plan,
                process_rejection_request=rejection_request,
                process_rejection_decision=rejection_decision,
                horizon_winner_cross_binding_bytes=cross_bytes,
                winner_raw_production_receipt_bytes=production_bytes,
            )
        if (
            type(evidence) is not book.HistoricalBookCellEvidence
            or evidence.symbol != symbol
            or evidence.timeframe != timeframe
            or evidence.regime != regime
        ):
            _fail("WORKER_CELL_EVIDENCE_REVERIFY_FAILED")
        plans.append(plan)
        evidence_roster.append(evidence)

    artifacts = bundle.get("artifacts")
    if type(artifacts) is not list:
        _fail("WORKER_ARTIFACT_ROSTER_INVALID")
    normalized_artifacts = [
        _artifact_entry(entry, "WORKER_ARTIFACT_ROSTER_INVALID") for entry in artifacts
    ]
    total_bytes = sum(
        int(entry["descriptor"]["size_bytes"]) for entry in normalized_artifacts
    )
    if (
        normalized_artifacts
        != sorted(normalized_artifacts, key=lambda item: item["artifact_id"])
        or len({entry["artifact_id"] for entry in normalized_artifacts})
        != len(normalized_artifacts)
        or bundle.get("artifact_count") != len(normalized_artifacts)
        or bundle.get("artifact_total_bytes") != total_bytes
        or bundle.get("artifact_roster_sha256") != _sha256(normalized_artifacts)
        or len(normalized_artifacts) > context.limits.max_artifacts_per_symbol
        or total_bytes > context.limits.max_symbol_bytes
    ):
        _fail("WORKER_ARTIFACT_ROSTER_INVALID")
    try:
        for entry in normalized_artifacts:
            authority_spool._verify_artifact_file(  # noqa: SLF001
                context,
                entry["descriptor"],
            )
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "WORKER_ARTIFACT_REVERIFY_FAILED"
        ) from exc
    optimizer_payload = bundle.get("optimizer_payload")
    if type(optimizer_payload) is not dict:
        _fail("WORKER_OPTIMIZER_PAYLOAD_INVALID")
    _verify_worker_index_projection(
        context=context,
        request_body=request_body,
        request_sha256=request.receipt_sha256,
        symbol=symbol,
        plans=plans,
        evidence_roster=evidence_roster,
        artifacts=normalized_artifacts,
        optimizer_payload=optimizer_payload,
    )

    # All bridge and authority reconstruction has completed before the parent
    # orchestration sink is changed.  A later anchor failure cannot publish it.
    try:
        sink = request.symbol_sink(symbol)
        if (
            type(sink) is not authority_spool.SymbolAuthoritySink
            or sink.context is not context
            or sink.symbol != symbol
        ):
            _fail("WORKER_PARENT_SYMBOL_SINK_INVALID")
        for entry in normalized_artifacts:
            sink.register_artifact(
                entry["artifact_id"],
                entry["artifact_type"],
                entry["descriptor"],
            )
    except HistoricalProductionAdapterError:
        raise
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "WORKER_PARENT_ARTIFACT_REGISTRATION_FAILED"
        ) from exc
    return orchestration.AnchorSymbolProduct(
        symbol=symbol,
        horizon_plans=tuple(plans),
        book_evidence=tuple(evidence_roster),
        optimizer_payload=json.loads(
            _canonical_bytes(optimizer_payload).decode("ascii")
        ),
    )


class BoundedRawWinnerCanaryCapture(HistoricalSymbolSearchCapture):
    """Exact one-cell capture that terminates before every Book operation."""

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        _fail("CANARY_CAPTURE_USE_VERIFIED_OPENER")

    def freeze_observed_cell(
        self,
        *args: Any,
        cost_stress_adversity_plan: Mapping[str, Any],
        **kwargs: Any,
    ) -> FrozenHistoricalCell:
        _verify_bounded_raw_winner_canary_capture(self)
        return super().freeze_observed_cell(
            *args,
            cost_stress_adversity_plan=cost_stress_adversity_plan,
            **kwargs,
        )

    def _failure_entry(
        self,
        *,
        failure_code: str,
        attempted_trials_exact: int,
        handle: FrozenHistoricalCell | None = None,
        search_outcome: Mapping[str, Any] | None = None,
        exception_type: str | None = None,
        failure_details: Sequence[str] = (),
        attempt_ordinal: int | None = None,
        fold_ordinal: int | None = None,
    ) -> dict[str, Any]:
        _verify_bounded_raw_winner_canary_capture(self)
        code = _reason(failure_code, "CANARY_FAILURE_CODE_INVALID")
        diagnostic = _bounded_canary_failure_diagnostic(
            exception_type=exception_type,
            failure_details=failure_details,
            attempt_ordinal=attempt_ordinal,
            fold_ordinal=fold_ordinal,
            attempted_trials_exact=attempted_trials_exact,
        )
        frozen_receipt_sha256 = (
            handle.frozen_receipt_descriptor["sha256"] if handle is not None else None
        )
        body = {
            "schema_version": BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA,
            "authority_scope": orchestration.BOUNDED_RAW_WINNER_CANARY_SCOPE,
            "request_sha256": self.request.receipt_sha256,
            "anchor_id": self.anchor["anchor_id"],
            "symbol": self.symbol,
            "timeframe": self.cell_plan.timeframe,
            "regime": self.regime,
            "failure_code": code,
            "attempted_trials_exact": attempted_trials_exact,
            "frozen_receipt_sha256": frozen_receipt_sha256,
            "search_outcome": (
                dict(search_outcome) if search_outcome is not None else None
            ),
            **diagnostic,
            "economic_no_trade_or_no_winner": False,
            "downstream_authority": False,
        }
        body = _verified_canary_failure_body(
            body,
            request_sha256=self.request.receipt_sha256,
            anchor_id=self.anchor["anchor_id"],
            symbol=self.symbol,
            timeframe=self.cell_plan.timeframe,
            regime=self.regime,
            failure_code=code,
            attempted_trials_exact=attempted_trials_exact,
            frozen_receipt_sha256=frozen_receipt_sha256,
        )
        return self.sink.put_json(
            f"{self.anchor['anchor_id']}/canary/{self.cell_plan.timeframe}/"
            f"{self.regime}/technical-failure",
            BOUNDED_RAW_WINNER_CANARY_FAILURE_ARTIFACT_TYPE,
            body,
        )

    def _seal_canary_receipt(
        self,
        *,
        status: str,
        failure_code: str | None,
        attempted_trials_exact: int,
        handle: FrozenHistoricalCell | None = None,
        completed: _RawWinnerCellCompletion | None = None,
        technical_failure_entry: Mapping[str, Any] | None = None,
    ) -> BoundedRawWinnerCanaryReceipt:
        _verify_bounded_raw_winner_canary_capture(self)
        if self._finished or self._canary_receipt is not None:
            _fail("CANARY_RECEIPT_ALREADY_SEALED")
        if type(status) is not str or not status:
            _fail("CANARY_RECEIPT_STATUS_INVALID")
        if failure_code is not None:
            _reason(failure_code, "CANARY_RECEIPT_FAILURE_CODE_INVALID")
        request_body = self.request_body
        production = completed.production if completed is not None else None
        standalone_accepted = bool(
            production is not None
            and winner_selection.verify_historical_cell_winner_decision(
                production.decision, production.request
            )
        )
        if production is not None and not standalone_accepted:
            _fail("CANARY_STANDALONE_WINNER_REVERIFY_FAILED")
        state_context_admission_summary = _build_canary_state_context_admission_summary(
            process_body=self.process.to_dict(),
            cell_receipt=self.cell_plan.cell_receipt,
            horizon_plan_body=_released_horizon_plan_body(
                self.cell_plan.horizon_plan,
                "CANARY_STATE_CONTEXT_HORIZON_PLAN_NOT_GENUINE",
            ),
        )
        if (
            state_context_admission_summary["attempt_count_exact"]
            != request_body["attempt_count_exact"]
        ):
            _fail("CANARY_STATE_CONTEXT_ATTEMPT_COUNT_INVALID")
        winner_request_entry = (
            _artifact_entry(
                production.entries.get("winner-request"),
                "CANARY_WINNER_REQUEST_ENTRY_INVALID",
            )
            if production is not None
            else None
        )
        winner_decision_entry = (
            _artifact_entry(
                production.entries.get("winner-decision"),
                "CANARY_WINNER_DECISION_ENTRY_INVALID",
            )
            if production is not None
            else None
        )
        body = {
            "schema_version": BOUNDED_RAW_WINNER_CANARY_RECEIPT_SCHEMA,
            "authority_scope": orchestration.BOUNDED_RAW_WINNER_CANARY_SCOPE,
            "status": status,
            "failure_code": failure_code,
            "request_sha256": self.request.receipt_sha256,
            "request_nonce": request_body["request_nonce"],
            "generation_id": self.context.generation_id,
            "native_universe_sha256": self.context.native_universe_sha256,
            "config_sha256": self.context.config_sha256,
            "generation_context_sha256": self.context.context_sha256,
            "anchor_plan_receipt_sha256": request_body["anchor_plan_receipt_sha256"],
            "activation_id": request_body["activation_id"],
            "activation_manifest_sha256": request_body["activation_manifest_sha256"],
            "anchor_plan_sha256": request_body["anchor_plan_sha256"],
            "anchor": dict(self.anchor),
            "search_process_receipt_sha256": request_body[
                "search_process_receipt_sha256"
            ],
            "causal_precommit_receipt_sha256": request_body[
                "causal_precommit_receipt_sha256"
            ],
            "native_authority_verification": request_body[
                "native_authority_verification"
            ],
            "symbol": self.symbol,
            "timeframe": self.cell_plan.timeframe,
            "regime": self.regime,
            "regime_selection_rule": request_body["regime_selection_rule"],
            "cell_plan_receipt_sha256": self.cell_plan.receipt_sha256,
            "horizon_plan_receipt_sha256": self.cell_plan.horizon_plan.receipt_sha256,
            "source_session_receipt_sha256": self.source_session.receipt_sha256,
            "source_session_entry": self.source_session.spool_entry,
            "plan_permit_receipt_sha256": self.plan_permit.receipt_sha256,
            "capture_permit_receipt_sha256": self.capture_permit.receipt_sha256,
            "effective_pipeline_config_sha256": (
                self.frozen_pipeline_config.effective_config_sha256
            ),
            "attempted_trials_exact": attempted_trials_exact,
            "state_context_admission_summary": state_context_admission_summary,
            "raw_artifact_namespace": self.raw_artifact_namespace,
            "frozen_plan_receipt": (
                handle.frozen_receipt_descriptor if handle is not None else None
            ),
            "frozen_descriptor_graph": (
                handle.frozen_descriptor_graph if handle is not None else None
            ),
            "horizon_winner_cross_binding": (
                handle.cross_binding_descriptor if handle is not None else None
            ),
            "winner_request_entry": winner_request_entry,
            "winner_decision_entry": winner_decision_entry,
            "raw_production_receipt": (
                completed.production_receipt_descriptor
                if completed is not None
                else None
            ),
            "production_descriptor_graph": (
                completed.production_descriptor_graph if completed is not None else None
            ),
            "winner_request_sha256": (
                production.request.request_sha256 if production is not None else None
            ),
            "winner_decision_sha256": (
                production.decision.receipt_sha256 if production is not None else None
            ),
            "winner_decision_status": (
                production.decision.status if production is not None else None
            ),
            "all_process_rejected": (
                completed.all_process_rejected if completed is not None else False
            ),
            "raw_winner_completed": production is not None,
            "standalone_winner_verifier_accepted": standalone_accepted,
            "technical_failure_entry": (
                _artifact_entry(
                    dict(technical_failure_entry),
                    "CANARY_TECHNICAL_FAILURE_ENTRY_INVALID",
                )
                if technical_failure_entry is not None
                else None
            ),
            "book_projection_performed": False,
            "book_multiplicity_authorized": False,
            "causal_authorized": False,
            "current_refit_authorized": False,
            "capital_authorized": False,
            "publication_authorized": False,
            "active_pointer_mutation_authorized": False,
            "configuration_mutation_authorized": False,
            "order_authorized": False,
            "profitability_claimed": False,
        }
        body_json = _canonical_bytes(body)
        artifact_id = (
            f"{self.anchor['anchor_id']}/canary/{self.cell_plan.timeframe}/"
            f"{self.regime}/non-authorizing-receipt"
        )
        entry = self.sink.put_json(
            artifact_id,
            BOUNDED_RAW_WINNER_CANARY_RECEIPT_ARTIFACT_TYPE,
            body,
        )
        symbol_receipt = self.sink.seal()
        receipt = BoundedRawWinnerCanaryReceipt(
            body_json,
            _canonical_bytes(entry),
            _canonical_bytes(symbol_receipt),
            _CANARY_RECEIPT_SEAL,
        )
        verify_bounded_raw_winner_canary_receipt(
            self.context,
            request=self.request,
            receipt=receipt,
        )
        self._active = None
        self._finished = True
        self._canary_receipt = receipt
        return receipt

    def complete_observed_cell(
        self,
        handle: FrozenHistoricalCell,
        *,
        attempt_evidence: Iterable[Mapping[str, Any]],
        evidence_access_started_at_utc: str,
        evidence_access_completed_at_utc: str,
    ) -> raw_winner.WinnerRawEvidenceProduction:
        _verify_bounded_raw_winner_canary_capture(self)
        completed = self._complete_raw_winner_cell(
            handle,
            attempt_evidence=attempt_evidence,
            evidence_access_started_at_utc=evidence_access_started_at_utc,
            evidence_access_completed_at_utc=evidence_access_completed_at_utc,
        )
        self._seal_canary_receipt(
            status=(
                "RAW_WINNER_VERIFIED_ALL_PROCESS_REJECTED"
                if completed.all_process_rejected
                else "RAW_WINNER_VERIFIED"
            ),
            failure_code=None,
            attempted_trials_exact=handle.attempted_trials_exact,
            handle=handle,
            completed=completed,
        )
        return completed.production

    def record_raw_evidence_failure(
        self,
        handle: FrozenHistoricalCell,
        failure: raw_winner.WinnerRawEvidenceIncomplete,
        *,
        exception_type: str | None = None,
        failure_details: Sequence[str] = (),
        attempt_ordinal: int | None = None,
        fold_ordinal: int | None = None,
    ) -> None:
        _verify_bounded_raw_winner_canary_capture(self)
        self._require_active(handle)
        if type(failure) is not raw_winner.WinnerRawEvidenceIncomplete:
            _fail("CANARY_RAW_EVIDENCE_FAILURE_TYPE_INVALID")
        entry = self._failure_entry(
            failure_code=failure.code,
            attempted_trials_exact=handle.attempted_trials_exact,
            handle=handle,
            exception_type=(
                type(failure).__name__ if exception_type is None else exception_type
            ),
            failure_details=failure_details,
            attempt_ordinal=attempt_ordinal,
            fold_ordinal=fold_ordinal,
        )
        self._seal_canary_receipt(
            status="RAW_WINNER_TECHNICAL_FAILURE",
            failure_code=failure.code,
            attempted_trials_exact=handle.attempted_trials_exact,
            handle=handle,
            technical_failure_entry=entry,
        )

    def record_search_infrastructure_failure(
        self,
        timeframe: str,
        regime: str,
        *,
        failure: SearchInfrastructureError,
        attempted_trials_exact: int,
    ) -> None:
        _verify_bounded_raw_winner_canary_capture(self)
        if (
            self._finished
            or self._active is not None
            or timeframe != self.cell_plan.timeframe
            or regime != self.regime
            or type(failure) is not SearchInfrastructureError
        ):
            _fail("CANARY_SEARCH_FAILURE_EVENT_INVALID")
        outcome = failure.outcome
        if type(outcome) not in {TrialOutcome, FamilyCellOutcome} or (
            outcome.kind is not SearchOutcomeKind.INFRA_FAILURE
        ):
            _fail("CANARY_SEARCH_FAILURE_OUTCOME_INVALID")
        if any(
            value not in ("", expected)
            for value, expected in (
                (str(getattr(outcome, "symbol", "")), self.symbol),
                (str(getattr(outcome, "timeframe", "")), timeframe),
                (str(getattr(outcome, "regime", "")), regime),
            )
        ):
            _fail("CANARY_SEARCH_FAILURE_CONTEXT_MISMATCH")
        entry = self._failure_entry(
            failure_code=outcome.reason_code,
            attempted_trials_exact=attempted_trials_exact,
            search_outcome=outcome.to_dict(),
        )
        self._seal_canary_receipt(
            status="SEARCH_INFRASTRUCTURE_FAILURE",
            failure_code=outcome.reason_code,
            attempted_trials_exact=attempted_trials_exact,
            technical_failure_entry=entry,
        )

    def record_unsearched_cell(
        self,
        timeframe: str,
        regime: str,
        *,
        reason_code: str,
    ) -> None:
        _verify_bounded_raw_winner_canary_capture(self)
        if (
            self._finished
            or self._active is not None
            or timeframe != self.cell_plan.timeframe
            or regime != self.regime
        ):
            _fail("CANARY_UNSEARCHED_EVENT_INVALID")
        code = _reason(reason_code, "CANARY_UNSEARCHED_REASON_INVALID")
        entry = self._failure_entry(
            failure_code=code,
            attempted_trials_exact=0,
        )
        self._seal_canary_receipt(
            status="UNSEARCHED_SOURCE_BOUNDARY_SHORTFALL",
            failure_code=code,
            attempted_trials_exact=0,
            technical_failure_entry=entry,
        )

    def finish(self) -> orchestration.AnchorSymbolProduct:
        _verify_bounded_raw_winner_canary_capture(self)
        _fail("CANARY_BOOK_FINISH_FORBIDDEN")

    @property
    def verified_fitted_timeframe(self) -> Any:
        _verify_bounded_raw_winner_canary_capture(self)
        return self.fitted_timeframe

    @property
    def verified_pipeline_config(self) -> Any:
        _verify_bounded_raw_winner_canary_capture(self)
        return self.pipeline_config

    @property
    def canary_receipt(self) -> BoundedRawWinnerCanaryReceipt:
        _verify_bounded_raw_winner_canary_capture(self)
        if self._canary_receipt is None:
            _fail("CANARY_RECEIPT_NOT_AVAILABLE")
        return self._canary_receipt


def _validated_bounded_raw_winner_canary_capture_inputs(
    context: authority_spool.GenerationSpoolContext,
    *,
    request: orchestration.BoundedRawWinnerCanaryRequest,
    process: Any,
    plan: Any,
    source_session: Any,
    plan_permit: Any,
    capture_permit: Any,
) -> tuple[
    dict[str, Any],
    authority_spool.SymbolAuthoritySink,
    Any,
    Any,
    Any,
]:
    """Consume exact executor capabilities and reverify the planned cell."""

    try:
        import pm_authoritative_search_execution as search_execution
        import pm_production_search_planner as search_planner

        if (
            type(context) is not authority_spool.GenerationSpoolContext
            or type(request) is not orchestration.BoundedRawWinnerCanaryRequest
            or type(process) is not search_planner.FrozenProductionSearchProcess
            or type(plan) is not search_planner.ProductionCellSearchPlan
        ):
            _fail("CANARY_CAPTURE_INPUT_TYPE_INVALID")
        (
            request_body,
            sink,
            verified_source_session,
            pipeline_config,
            frozen_pipeline_config,
        ) = search_execution._consume_bounded_raw_winner_canary_capture_permit(
            context,
            request=request,
            process=process,
            source_session=source_session,
            plan_permit=plan_permit,
            plan=plan,
            capture_permit=capture_permit,
        )
        fitted = verified_source_session._fitted
        search_planner.verify_frozen_production_search_process(context, process)
        search_planner.verify_production_cell_search_plan(
            context,
            process=process,
            sink=sink,
            plan=plan,
        )
    except HistoricalProductionAdapterError:
        raise
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "CANARY_CAPTURE_PREREQUISITE_REVERIFY_FAILED"
        ) from exc

    source_body = fitted.source_group_decision.to_dict()
    fit_body = fitted.regime_fit_decision.to_dict()
    support = fitted.train_regime_support
    counts = support.get("counts") if type(support) is dict else None
    if (
        type(counts) is not dict
        or set(counts) != set(book.REGIME_ORDER)
        or any(
            type(counts[regime]) is not int
            or type(counts[regime]) is bool
            or counts[regime] < 0
            for regime in book.REGIME_ORDER
        )
    ):
        _fail("CANARY_CAPTURE_SUPPORT_INVALID")
    observed = [regime for regime in book.REGIME_ORDER if counts[regime] > 0]
    if not observed or plan.regime != observed[0]:
        _fail("CANARY_CAPTURE_REGIME_SELECTION_INVALID")
    binding = orchestration.historical_horizon_plan_binding(plan.horizon_plan)
    horizon_body = _released_horizon_plan_body(
        plan.horizon_plan, "CANARY_HORIZON_PLAN_NOT_GENUINE"
    )
    plan_receipt = plan.cell_receipt
    if (
        plan.mode != "BOUNDED_RAW_WINNER_CANARY"
        or plan.symbol != request_body["symbol"]
        or plan.timeframe != request_body["timeframe"]
        or plan.attempt_count != request_body["attempt_count_exact"]
        or plan.search_process_receipt_sha256
        != request_body["search_process_receipt_sha256"]
        or plan.causal_precommit_receipt_sha256
        != request_body["causal_precommit_receipt_sha256"]
        or plan_receipt.get("artifact_namespace") != request_body["artifact_namespace"]
        or plan_receipt.get("attempt_count") != request_body["attempt_count_exact"]
        or binding.get("cell")
        != {
            "symbol": request_body["symbol"],
            "timeframe": request_body["timeframe"],
            "regime": plan.regime,
        }
        or binding.get("anchor_id") != request_body["anchor"]["anchor_id"]
        or binding.get("selection_cutoff_utc")
        != request_body["anchor"]["selection_cutoff_utc"]
        or binding.get("process_precommit_receipt_sha256")
        != request_body["causal_precommit_receipt_sha256"]
        or binding.get("plan_receipt_sha256") != plan.horizon_plan.receipt_sha256
        or horizon_body.get("plan_status")
        not in {"READY_FOR_HISTORICAL_EVIDENCE", "HISTORICAL_BOUNDARY_LIMITED"}
        or source_body.get("availability") != book.AVAILABLE
        or source_body.get("symbol") != request_body["symbol"]
        or source_body.get("timeframe") != request_body["timeframe"]
        or fit_body.get("fit_state") != book.REGIME_FITTED
        or fit_body.get("source_group_decision_sha256")
        != fitted.source_group_decision.receipt_sha256
        or fit_body.get("train_regime_support") != support
    ):
        _fail("CANARY_CAPTURE_CELL_BINDING_INVALID")
    return (
        request_body,
        sink,
        verified_source_session,
        pipeline_config,
        frozen_pipeline_config,
    )


def _make_bounded_raw_winner_canary_capture_authority():
    capture_key = secrets.token_bytes(32)

    def binding_payload(value: Any) -> bytes:
        try:
            key = (str(value.cell_plan.timeframe), str(value.cell_plan.regime))
            expected_binding = orchestration.historical_horizon_plan_binding(
                value.cell_plan.horizon_plan
            )
            expected_plan_body = _released_horizon_plan_body(
                value.cell_plan.horizon_plan,
                "CANARY_HORIZON_PLAN_NOT_GENUINE",
            )
            if (
                value._bindings != {key: expected_binding}
                or value._horizon_plan_bodies != {key: expected_plan_body}
                or value._horizon_plans != {key: value.cell_plan.horizon_plan}
                or value._source
                != {key[0]: value.fitted_timeframe.source_group_decision}
                or value._fit != {key[0]: value.fitted_timeframe.regime_fit_decision}
                or value.source_session._fitted is not value.fitted_timeframe
                or value.source_session._pipeline_config is not value.pipeline_config
                or value.source_session._frozen_pipeline_config
                is not value.frozen_pipeline_config
            ):
                _fail("CANARY_CAPTURE_INTERNAL_BINDING_CHANGED")
            body = {
                "schema_version": "fxpm.bounded-raw-winner-canary-capture-tag.v1",
                "capture_identity": id(value),
                "generation_context_sha256": value.context.context_sha256,
                "request_sha256": value.request.receipt_sha256,
                "source_session_receipt_sha256": value.source_session.receipt_sha256,
                "plan_permit_receipt_sha256": value.plan_permit.receipt_sha256,
                "capture_permit_receipt_sha256": value.capture_permit.receipt_sha256,
                "cell_plan_receipt_sha256": value.cell_plan.receipt_sha256,
                "horizon_plan_receipt_sha256": (
                    value.cell_plan.horizon_plan.receipt_sha256
                ),
                "fitted_frame_sha256": value.fitted_timeframe.frame_sha256,
                "pipeline_config_identity": id(value.pipeline_config),
                "effective_pipeline_config_sha256": (
                    value.frozen_pipeline_config.effective_config_sha256
                ),
                "source_group_decision_sha256": (
                    value.fitted_timeframe.source_group_decision.receipt_sha256
                ),
                "regime_fit_decision_sha256": (
                    value.fitted_timeframe.regime_fit_decision.receipt_sha256
                ),
                "train_regime_support_receipt_sha256": (
                    value.fitted_timeframe.train_regime_support["receipt_sha256"]
                ),
            }
            return _canonical_bytes(body)
        except HistoricalProductionAdapterError:
            raise
        except Exception as exc:
            raise HistoricalProductionAdapterError(
                "CANARY_CAPTURE_BINDING_INVALID"
            ) from exc

    def capture_tag(value: Any) -> str:
        return hmac.new(capture_key, binding_payload(value), hashlib.sha256).hexdigest()

    def verify(value: Any) -> bool:
        if type(value) is not BoundedRawWinnerCanaryCapture:
            _fail("CANARY_CAPTURE_OBJECT_INVALID")
        try:
            import pm_authoritative_search_execution as search_execution
            import pm_production_search_planner as search_planner

            expected_tag = capture_tag(value)
            if (
                type(value._capture_tag) is not str
                or not hmac.compare_digest(value._capture_tag, expected_tag)
                or value.request_body
                != orchestration.verify_bounded_raw_winner_canary_request(
                    value.context, value.request
                )
                or value.cell_plan._bounded_canary_request is not value.request
                or value.cell_plan._bounded_canary_source_session
                is not value.source_session
                or value.cell_plan._bounded_canary_plan_permit is not value.plan_permit
                or value.symbol != value.request_body["symbol"]
                or value.regime != value.cell_plan.regime
                or value.raw_artifact_namespace
                != value.request_body["artifact_namespace"]
            ):
                _fail("CANARY_CAPTURE_AUTHENTICATION_INVALID")
            search_execution._verify_bounded_raw_winner_canary_capture_bindings(
                value.context,
                request=value.request,
                process=value.process,
                source_session=value.source_session,
                plan_permit=value.plan_permit,
                plan=value.cell_plan,
                capture_permit=value.capture_permit,
            )
            search_planner.verify_production_cell_search_plan(
                value.context,
                process=value.process,
                sink=value.sink,
                plan=value.cell_plan,
            )
        except HistoricalProductionAdapterError:
            raise
        except Exception as exc:
            raise HistoricalProductionAdapterError(
                "CANARY_CAPTURE_REVERIFY_FAILED"
            ) from exc
        return True

    def open_capture(
        context: authority_spool.GenerationSpoolContext,
        *,
        request: orchestration.BoundedRawWinnerCanaryRequest,
        process: Any,
        plan: Any,
        source_session: Any,
        plan_permit: Any,
        capture_permit: Any,
    ) -> BoundedRawWinnerCanaryCapture:
        (
            request_body,
            sink,
            verified_source_session,
            pipeline_config,
            frozen_pipeline_config,
        ) = _validated_bounded_raw_winner_canary_capture_inputs(
            context,
            request=request,
            process=process,
            plan=plan,
            source_session=source_session,
            plan_permit=plan_permit,
            capture_permit=capture_permit,
        )
        fitted = verified_source_session._fitted
        value = object.__new__(BoundedRawWinnerCanaryCapture)
        value.context = context
        value.request = request
        value.request_body = dict(request_body)
        value.anchor = dict(request_body["anchor"])
        value.symbol = str(request_body["symbol"])
        value.sink = sink
        value.raw_artifact_namespace = str(request_body["artifact_namespace"])
        value.process = process
        value.cell_plan = plan
        value.source_session = verified_source_session
        value.plan_permit = plan_permit
        value.capture_permit = capture_permit
        value.fitted_timeframe = fitted
        value.pipeline_config = pipeline_config
        value.frozen_pipeline_config = frozen_pipeline_config
        value.regime = str(plan.regime)
        key = (str(plan.timeframe), str(plan.regime))
        binding = orchestration.historical_horizon_plan_binding(plan.horizon_plan)
        plan_body = _released_horizon_plan_body(
            plan.horizon_plan, "CANARY_HORIZON_PLAN_NOT_GENUINE"
        )
        value._bindings = {key: binding}
        value._horizon_plan_bodies = {key: plan_body}
        value._horizon_plans = {key: plan.horizon_plan}
        value._source = {str(plan.timeframe): fitted.source_group_decision}
        value._fit = {str(plan.timeframe): fitted.regime_fit_decision}
        value._evidence = {}
        value._cell_records = {}
        value._active = None
        value._finished = False
        value._canary_receipt = None
        value._capture_tag = capture_tag(value)
        verify(value)
        return value

    return open_capture, verify


(
    open_bounded_raw_winner_canary_capture,
    _verify_bounded_raw_winner_canary_capture,
) = _make_bounded_raw_winner_canary_capture_authority()


def verify_bounded_raw_winner_canary_receipt(
    context: authority_spool.GenerationSpoolContext,
    *,
    request: orchestration.BoundedRawWinnerCanaryRequest,
    receipt: BoundedRawWinnerCanaryReceipt,
) -> dict[str, Any]:
    """Reopen the sealed symbol roster and independently replay raw Winner."""

    if (
        type(context) is not authority_spool.GenerationSpoolContext
        or type(receipt) is not BoundedRawWinnerCanaryReceipt
        or receipt._seal is not _CANARY_RECEIPT_SEAL
    ):
        _fail("CANARY_RECEIPT_OBJECT_INVALID")
    request_body = orchestration.verify_bounded_raw_winner_canary_request(
        context, request
    )
    body = receipt.to_dict()
    if (
        set(body) != _CANARY_RECEIPT_FIELDS
        or _canonical_bytes(body) != receipt._body_json
        or body.get("schema_version") != BOUNDED_RAW_WINNER_CANARY_RECEIPT_SCHEMA
        or body.get("authority_scope") != orchestration.BOUNDED_RAW_WINNER_CANARY_SCOPE
    ):
        _fail("CANARY_RECEIPT_BYTES_OR_FIELDS_INVALID")
    expected_bindings = {
        "request_sha256": request.receipt_sha256,
        "request_nonce": request_body["request_nonce"],
        "generation_id": context.generation_id,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "generation_context_sha256": context.context_sha256,
        "anchor_plan_receipt_sha256": request_body["anchor_plan_receipt_sha256"],
        "activation_id": request_body["activation_id"],
        "activation_manifest_sha256": request_body["activation_manifest_sha256"],
        "anchor_plan_sha256": request_body["anchor_plan_sha256"],
        "anchor": request_body["anchor"],
        "search_process_receipt_sha256": request_body["search_process_receipt_sha256"],
        "causal_precommit_receipt_sha256": request_body[
            "causal_precommit_receipt_sha256"
        ],
        "native_authority_verification": request_body["native_authority_verification"],
        "symbol": request_body["symbol"],
        "timeframe": request_body["timeframe"],
        "regime_selection_rule": request_body["regime_selection_rule"],
        "raw_artifact_namespace": request_body["artifact_namespace"],
    }
    if any(body.get(key) != expected for key, expected in expected_bindings.items()):
        _fail("CANARY_RECEIPT_REQUEST_BINDING_INVALID")
    if (
        body.get("regime") not in book.REGIME_ORDER
        or type(body.get("attempted_trials_exact")) is not int
        or not 0
        <= body["attempted_trials_exact"]
        <= request_body["attempt_count_exact"]
        or any(
            body.get(field) is not False
            for field in (
                "book_projection_performed",
                "book_multiplicity_authorized",
                "causal_authorized",
                "current_refit_authorized",
                "capital_authorized",
                "publication_authorized",
                "active_pointer_mutation_authorized",
                "configuration_mutation_authorized",
                "order_authorized",
                "profitability_claimed",
            )
        )
    ):
        _fail("CANARY_RECEIPT_NONAUTHORITY_INVALID")
    for field in (
        "source_session_receipt_sha256",
        "plan_permit_receipt_sha256",
        "capture_permit_receipt_sha256",
        "effective_pipeline_config_sha256",
        "cell_plan_receipt_sha256",
        "horizon_plan_receipt_sha256",
    ):
        _hex64(body.get(field), f"CANARY_{field.upper()}_INVALID")

    entry = _artifact_entry(receipt.artifact_entry, "CANARY_RECEIPT_ENTRY_INVALID")
    if (
        entry["artifact_type"] != BOUNDED_RAW_WINNER_CANARY_RECEIPT_ARTIFACT_TYPE
        or entry["descriptor"]["sha256"] != receipt.receipt_sha256
        or entry["descriptor"]["size_bytes"] != len(receipt._body_json)
        or entry["descriptor"]["format"] != authority_spool.CANONICAL_JSON_FORMAT
    ):
        _fail("CANARY_RECEIPT_ENTRY_INVALID")
    try:
        manifest = authority_spool.verify_symbol_receipt(
            context.root,
            receipt.symbol_receipt,
            expected_generation_id=context.generation_id,
            expected_symbol=request_body["symbol"],
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=context.config_sha256,
        )
    except Exception as exc:
        raise HistoricalProductionAdapterError(
            "CANARY_SYMBOL_RECEIPT_REVERIFY_FAILED"
        ) from exc
    artifacts = manifest.get("artifacts")
    if type(artifacts) is not list or entry not in artifacts:
        _fail("CANARY_RECEIPT_NOT_IN_SEALED_SYMBOL_ROSTER")
    source_session_entry = _artifact_entry(
        body["source_session_entry"],
        "CANARY_SOURCE_SESSION_ENTRY_INVALID",
    )
    if (
        source_session_entry not in artifacts
        or source_session_entry["artifact_type"]
        != "bounded_raw_winner_canary_source_session"
        or source_session_entry["descriptor"]["sha256"]
        != body["source_session_receipt_sha256"]
    ):
        _fail("CANARY_SOURCE_SESSION_ENTRY_BINDING_INVALID")
    source_session = _reopen_ascii_json_entry(
        context,
        source_session_entry,
        expected_type="bounded_raw_winner_canary_source_session",
        code="CANARY_SOURCE_SESSION_REOPEN_FAILED",
    )
    if (
        source_session.get("schema_version")
        != "fxpm.bounded-raw-winner-canary-source-session.v1"
        or source_session.get("request_sha256") != request.receipt_sha256
        or source_session.get("request_nonce") != request_body["request_nonce"]
        or source_session.get("generation_context_sha256") != context.context_sha256
        or source_session.get("config_sha256") != context.config_sha256
        or source_session.get("effective_pipeline_config_sha256")
        != body["effective_pipeline_config_sha256"]
        or source_session.get("selection_cutoff_utc")
        != request_body["anchor"]["selection_cutoff_utc"]
        or source_session.get("caller_supplied_product_or_fit_accepted") is not False
        or source_session.get("confers_downstream_or_trading_authority") is not False
    ):
        _fail("CANARY_SOURCE_SESSION_RECEIPT_BINDING_INVALID")

    cell_plan_artifact_id = (
        f"{body['raw_artifact_namespace']}/search-cell-plan/"
        f"{body['timeframe']}/{body['regime']}"
    )
    cell_plan_entries = [
        item
        for item in artifacts
        if type(item) is dict and item.get("artifact_id") == cell_plan_artifact_id
    ]
    if len(cell_plan_entries) != 1:
        _fail("CANARY_CELL_PLAN_ENTRY_NOT_UNIQUE")
    cell_plan_entry = _artifact_entry(
        cell_plan_entries[0],
        "CANARY_CELL_PLAN_ENTRY_INVALID",
    )
    cell_plan_receipt = _reopen_ascii_json_entry(
        context,
        cell_plan_entry,
        expected_type="production_search_cell_plan_receipt",
        code="CANARY_CELL_PLAN_REOPEN_FAILED",
    )
    cell_plan_sha256 = cell_plan_receipt.get("receipt_sha256")
    cell_plan_body = {
        key: value
        for key, value in cell_plan_receipt.items()
        if key != "receipt_sha256"
    }
    cell_plan_horizon_binding = cell_plan_receipt.get("horizon_plan_binding")
    if (
        cell_plan_receipt.get("schema_version")
        != "fxpm.production-search-cell-plan-receipt.v2"
        or cell_plan_receipt.get("mode") != "BOUNDED_RAW_WINNER_CANARY"
        or cell_plan_receipt.get("artifact_namespace") != body["raw_artifact_namespace"]
        or cell_plan_receipt.get("generation_id") != context.generation_id
        or cell_plan_receipt.get("generation_context_sha256") != context.context_sha256
        or cell_plan_receipt.get("search_process_receipt_sha256")
        != body["search_process_receipt_sha256"]
        or cell_plan_receipt.get("cell")
        != {
            "symbol": body["symbol"],
            "timeframe": body["timeframe"],
            "regime": body["regime"],
        }
        or cell_plan_receipt.get("bounded_canary_request_sha256")
        != request.receipt_sha256
        or cell_plan_receipt.get("bounded_canary_source_session_receipt_sha256")
        != body["source_session_receipt_sha256"]
        or cell_plan_receipt.get("bounded_canary_plan_permit_receipt_sha256")
        != body["plan_permit_receipt_sha256"]
        or cell_plan_sha256 != _sha256(cell_plan_body)
        or cell_plan_sha256 != body["cell_plan_receipt_sha256"]
        or type(cell_plan_horizon_binding) is not dict
        or cell_plan_horizon_binding.get("plan_receipt_sha256")
        != body["horizon_plan_receipt_sha256"]
        or cell_plan_receipt.get("attempt_count")
        != request_body["attempt_count_exact"]
    ):
        _fail("CANARY_CELL_PLAN_RECEIPT_BINDING_INVALID")
    state_context_admission_summary = _verified_canary_state_context_admission_summary(
        body.get("state_context_admission_summary"),
        cell_receipt=cell_plan_receipt,
    )
    if (
        state_context_admission_summary["search_process_receipt_sha256"]
        != body["search_process_receipt_sha256"]
        or state_context_admission_summary["cell_plan_receipt_sha256"]
        != body["cell_plan_receipt_sha256"]
        or state_context_admission_summary["horizon_plan_receipt_sha256"]
        != body["horizon_plan_receipt_sha256"]
    ):
        _fail("CANARY_STATE_CONTEXT_ADMISSION_TOP_LEVEL_BINDING_INVALID")

    raw_completed = body["raw_winner_completed"]
    if raw_completed is True:
        if (
            body["failure_code"] is not None
            or body["standalone_winner_verifier_accepted"] is not True
            or body["attempted_trials_exact"]
            != request_body["attempt_count_exact"]
            or body["technical_failure_entry"] is not None
            or any(
                body[field] is None
                for field in (
                    "frozen_plan_receipt",
                    "frozen_descriptor_graph",
                    "horizon_winner_cross_binding",
                    "winner_request_entry",
                    "winner_decision_entry",
                    "raw_production_receipt",
                    "production_descriptor_graph",
                    "winner_request_sha256",
                    "winner_decision_sha256",
                    "winner_decision_status",
                )
            )
        ):
            _fail("CANARY_SUCCESS_RECEIPT_INCOMPLETE")
        request_entry = _artifact_entry(
            body["winner_request_entry"],
            "CANARY_WINNER_REQUEST_ENTRY_INVALID",
        )
        decision_entry = _artifact_entry(
            body["winner_decision_entry"],
            "CANARY_WINNER_DECISION_ENTRY_INVALID",
        )
        if request_entry not in artifacts or decision_entry not in artifacts:
            _fail("CANARY_WINNER_ARTIFACTS_NOT_IN_SEALED_ROSTER")
        request_payload = _reopen_ascii_json_entry(
            context,
            request_entry,
            expected_type="winner_v11_request",
            code="CANARY_WINNER_REQUEST_REOPEN_FAILED",
        )
        decision_payload = _reopen_ascii_json_entry(
            context,
            decision_entry,
            expected_type="winner_v11_decision",
            code="CANARY_WINNER_DECISION_REOPEN_FAILED",
        )
        try:
            reconstructed_request = winner_selection.create_winner_selection_request(
                request_payload["precommitment"]["payload"],
                request_payload["attempt_ledger"]["payload"],
                request_payload["evidence_ledger"]["payload"],
            )
            if (
                reconstructed_request is None
                or reconstructed_request.to_dict() != request_payload
                or reconstructed_request.request_sha256 != body["winner_request_sha256"]
            ):
                _fail("CANARY_WINNER_REQUEST_RECONSTRUCTION_FAILED")
            decision_body = dict(decision_payload)
            decision_sha256 = decision_body.pop("receipt_sha256")
            reconstructed_decision = winner_selection.WinnerSelectionDecision(
                _canonical_bytes(decision_body), decision_sha256
            )
            if (
                reconstructed_decision.receipt_sha256 != body["winner_decision_sha256"]
                or reconstructed_decision.status != body["winner_decision_status"]
                or not winner_selection.verify_historical_cell_winner_decision(
                    reconstructed_decision, reconstructed_request
                )
            ):
                _fail("CANARY_STANDALONE_WINNER_REPLAY_FAILED")
        except HistoricalProductionAdapterError:
            raise
        except Exception as exc:
            raise HistoricalProductionAdapterError(
                "CANARY_STANDALONE_WINNER_REPLAY_FAILED"
            ) from exc
        production_bytes = _reopen_winner_raw_production_receipt(
            context, body["raw_production_receipt"]
        )
        production_body = json.loads(production_bytes.decode("ascii"))
        roster = production_body.get("artifact_roster")
        if (
            production_body.get("generation_context_sha256") != context.context_sha256
            or production_body.get("artifact_namespace")
            != request_body["artifact_namespace"]
            or production_body.get("symbol") != body["symbol"]
            or production_body.get("timeframe") != body["timeframe"]
            or production_body.get("regime") != body["regime"]
            or production_body.get("request_sha256") != body["winner_request_sha256"]
            or production_body.get("decision_receipt_sha256")
            != body["winner_decision_sha256"]
            or type(roster) is not dict
            or roster.get("winner-request") != request_entry["descriptor"]
            or roster.get("winner-decision") != decision_entry["descriptor"]
            or production_body.get("provenance_cross_binding_sha256")
            != body["horizon_winner_cross_binding"]["sha256"]
        ):
            _fail("CANARY_RAW_PRODUCTION_RECEIPT_BINDING_INVALID")
    elif raw_completed is False:
        if (
            type(body["failure_code"]) is not str
            or body["standalone_winner_verifier_accepted"] is not False
            or body["all_process_rejected"] is not False
            or any(
                body[field] is not None
                for field in (
                    "winner_request_entry",
                    "winner_decision_entry",
                    "raw_production_receipt",
                    "production_descriptor_graph",
                    "winner_request_sha256",
                    "winner_decision_sha256",
                    "winner_decision_status",
                )
            )
            or body["technical_failure_entry"] is None
        ):
            _fail("CANARY_FAILURE_RECEIPT_INVALID")
        failure_entry = _artifact_entry(
            body["technical_failure_entry"],
            "CANARY_TECHNICAL_FAILURE_ENTRY_INVALID",
        )
        failure_body = _reopen_ascii_json_entry(
            context,
            failure_entry,
            expected_type=BOUNDED_RAW_WINNER_CANARY_FAILURE_ARTIFACT_TYPE,
            code="CANARY_TECHNICAL_FAILURE_REOPEN_FAILED",
        )
        if failure_entry not in artifacts:
            _fail("CANARY_TECHNICAL_FAILURE_BINDING_INVALID")
        frozen_receipt_sha256 = None
        if body["frozen_plan_receipt"] is not None:
            frozen_receipt_sha256 = _descriptor(
                body["frozen_plan_receipt"],
                "CANARY_TECHNICAL_FAILURE_FROZEN_RECEIPT_INVALID",
            )["sha256"]
        failure_body = _verified_canary_failure_body(
            failure_body,
            request_sha256=request.receipt_sha256,
            anchor_id=body["anchor"]["anchor_id"],
            symbol=body["symbol"],
            timeframe=body["timeframe"],
            regime=body["regime"],
            failure_code=body["failure_code"],
            attempted_trials_exact=body["attempted_trials_exact"],
            frozen_receipt_sha256=frozen_receipt_sha256,
        )
        if (
            body["status"] == "RAW_WINNER_TECHNICAL_FAILURE"
            and failure_body["schema_version"]
            == BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA
            and failure_body["exception_type"] is None
        ):
            _fail("CANARY_TECHNICAL_FAILURE_EXCEPTION_TYPE_MISSING")
    else:
        _fail("CANARY_RAW_COMPLETION_FLAG_INVALID")
    return json.loads(receipt._body_json.decode("ascii"))


def open_historical_symbol_capture(
    context: authority_spool.GenerationSpoolContext,
    request: orchestration.AnchorProducerRequest,
    symbol: str,
) -> HistoricalSymbolSearchCapture:
    """Open one bounded symbol capture for an orchestration producer callback."""

    return HistoricalSymbolSearchCapture(context, request, symbol)


__all__ = [
    "BOUNDED_RAW_WINNER_CANARY_EXCEPTION_TYPE_BYTES",
    "BOUNDED_RAW_WINNER_CANARY_FAILURE_ARTIFACT_TYPE",
    "BOUNDED_RAW_WINNER_CANARY_FAILURE_DETAIL_BYTES",
    "BOUNDED_RAW_WINNER_CANARY_FAILURE_DETAIL_LIMIT",
    "BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA",
    "BOUNDED_RAW_WINNER_CANARY_RECEIPT_ARTIFACT_TYPE",
    "BOUNDED_RAW_WINNER_CANARY_RECEIPT_SCHEMA",
    "BOUNDED_RAW_WINNER_CANARY_STATE_CONTEXT_ADMISSION_SCHEMA",
    "BoundedRawWinnerCanaryReceipt",
    "CAPTURE_CONTRACT_VERSION",
    "FAILURE_ARTIFACT_SCHEMA",
    "FrozenHistoricalCell",
    "HISTORICAL_PRODUCER_RELEASE_SHA256",
    "HISTORICAL_SYMBOL_WORKER_BUNDLE_MAX_BYTES",
    "HISTORICAL_SYMBOL_WORKER_BUNDLE_SCHEMA",
    "HORIZON_WINNER_CROSS_BINDING_SCHEMA",
    "HistoricalProductionAdapterError",
    "HistoricalSymbolSearchCapture",
    "OPTIMIZER_PAYLOAD_SCHEMA",
    "RAW_ARTIFACT_NAMESPACE_PREFIX",
    "RAW_DESCRIPTOR_GRAPH_SCHEMA",
    "SYMBOL_INDEX_SCHEMA",
    "historical_production_adapter_contract",
    "open_historical_symbol_capture",
    "reopen_historical_symbol_worker_bundle",
    "spool_historical_symbol_worker_bundle",
    "verify_bounded_raw_winner_canary_receipt",
]
