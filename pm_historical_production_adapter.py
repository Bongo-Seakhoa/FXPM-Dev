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
BOUNDED_RAW_WINNER_CANARY_RECEIPT_SCHEMA = (
    "fxpm.bounded-raw-winner-canary-receipt.v1"
)
BOUNDED_RAW_WINNER_CANARY_RECEIPT_ARTIFACT_TYPE = (
    "bounded_raw_winner_canary_nonauthorizing_receipt"
)
BOUNDED_RAW_WINNER_CANARY_FAILURE_ARTIFACT_TYPE = (
    "bounded_raw_winner_canary_technical_failure"
)
BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA = (
    "fxpm.bounded-raw-winner-canary-failure.v1"
)

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

_DESCRIPTOR_FIELDS = frozenset(
    {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
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
    plan_attempt_by_pair = {
        (value.get("candidate_id"), value.get("trial_id")): value
        for value in planned_attempts
        if type(value) is dict
    }
    if (
        len(candidate_by_id) != len(candidates)
        or len(trial_by_id) != len(trials)
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

        candidate_matches = [
            candidate
            for candidate in candidates
            if type(candidate) is dict
            and candidate.get("lookback_kind") == horizon.get("lookback_kind")
            and candidate.get("lookback_value") == horizon.get("lookback_value")
            and _utc_seconds(
                candidate.get("realized_start_utc"),
                "ATTEMPT_SOURCE_CANDIDATE_START_INVALID",
            )
            == _utc_seconds(
                horizon.get("fit_start_utc"), "ATTEMPT_SOURCE_FIT_START_INVALID"
            )
        ]
        parameter_sha256 = _sha256(trial_definition.get("parameters"))
        trial_matches = [
            trial
            for trial in trials
            if type(trial) is dict
            and trial.get("strategy_id") == process.get("strategy_id")
            and trial.get("strategy_release_sha256")
            == process.get("strategy_code_sha256")
            and trial.get("parameter_set_sha256") == parameter_sha256
        ]
        if len(candidate_matches) != 1 or len(trial_matches) != 1:
            _fail("ATTEMPT_SOURCE_PLAN_MAPPING_INVALID")
        candidate = candidate_matches[0]
        trial = trial_matches[0]
        planned = plan_attempt_by_pair.get(
            (candidate.get("candidate_id"), trial.get("trial_id"))
        )
        if (
            type(planned) is not dict
            or planned.get("attempt_id") in mapped_attempt_ids
            or planned.get("strategy_id") != process.get("strategy_id")
        ):
            _fail("ATTEMPT_SOURCE_PLAN_MAPPING_INVALID")

        fit_start = _utc_seconds(
            candidate.get("realized_start_utc"),
            "ATTEMPT_SOURCE_FIT_START_INVALID",
        )
        fit_start_ts = pd.Timestamp(fit_start)
        fit_end_ts = pd.Timestamp(prefix_end)
        mask = (opens >= fit_start_ts) & (opens < fit_end_ts) & (closes <= fit_end_ts)
        selected = frame.loc[mask].copy(deep=True)
        source_bar_count = len(selected)
        if (
            not 2 <= source_bar_count <= 10_000_000
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
            or selected.empty
            or _utc_seconds(
                pd.Timestamp(selected["_BAR_CLOSE_TS"].iloc[-1]).isoformat(),
                "ATTEMPT_SOURCE_SELECTED_LATEST_INVALID",
            )
            != prefix_latest
        ):
            _fail("ATTEMPT_SOURCE_INTERVAL_BINDING_INVALID")
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
            "fit_frame_sha256": _frame_sha256(selected),
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
    if len(candidate_by_id) != len(candidates) or len(trial_by_id) != len(trials):
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
        candidate_matches = [
            candidate
            for candidate in candidates
            if type(candidate) is dict
            and candidate.get("lookback_kind") == horizon.get("lookback_kind")
            and candidate.get("lookback_value") == horizon.get("lookback_value")
            and _utc_seconds(
                candidate.get("realized_start_utc"),
                "HORIZON_CANDIDATE_START_INVALID",
            )
            == fit_start
        ]
        if len(candidate_matches) != 1:
            _fail("HORIZON_WINNER_CANDIDATE_MAPPING_INVALID")
        candidate = candidate_matches[0]
        parameter_sha = _sha256(trial_definition.get("parameters"))
        trial_matches = [
            trial
            for trial in trials
            if type(trial) is dict
            and trial.get("strategy_id") == process.get("strategy_id")
            and trial.get("strategy_release_sha256")
            == process.get("strategy_code_sha256")
            and trial.get("parameter_set_sha256") == parameter_sha
        ]
        if len(trial_matches) != 1:
            _fail("HORIZON_WINNER_TRIAL_MAPPING_INVALID")
        trial = trial_matches[0]
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
            or horizon.get("source_bar_count")
            != execution_contract.get("source_bar_count")
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
        "missing_cell_default": None,
        "technical_failure_is_economic_no_winner": False,
        "source_unavailable_requires_terminal_source_decision": True,
        "regime_unobserved_requires_zero_train_labels": True,
        "regime_unobserved_requires_genuine_horizon_zero_support_binding": True,
        "insufficient_evidence_is_derive_only_from_genuine_horizon_or_winner": True,
        "boundary_limited_zero_search_is_non_economic_insufficient_evidence": True,
        "all_process_rejected_is_non_economic_insufficient_evidence": True,
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
                regime_fit_decision_sha256=self._fit[
                    handle.timeframe
                ].receipt_sha256,
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
            or handle.cross_binding_descriptor["format"]
            != raw_winner.ASCII_JSON_FORMAT
        ):
            _fail("HORIZON_WINNER_CROSS_BINDING_REPLAY_FAILED")
        if not _raw_namespace_supported(
            raw_winner.produce_winner_v11_raw_evidence
        ):
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


class BoundedRawWinnerCanaryCapture(HistoricalSymbolSearchCapture):
    """Exact one-cell capture that terminates before every Book operation."""

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        _fail("CANARY_CAPTURE_USE_VERIFIED_OPENER")

    def freeze_observed_cell(self, *args: Any, **kwargs: Any) -> FrozenHistoricalCell:
        _verify_bounded_raw_winner_canary_capture(self)
        return super().freeze_observed_cell(*args, **kwargs)

    def _failure_entry(
        self,
        *,
        failure_code: str,
        attempted_trials_exact: int,
        handle: FrozenHistoricalCell | None = None,
        search_outcome: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        _verify_bounded_raw_winner_canary_capture(self)
        code = _reason(failure_code, "CANARY_FAILURE_CODE_INVALID")
        if (
            type(attempted_trials_exact) is not int
            or attempted_trials_exact < 0
        ):
            _fail("CANARY_FAILURE_ATTEMPT_COUNT_INVALID")
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
            "frozen_receipt_sha256": (
                handle.frozen_receipt_descriptor["sha256"]
                if handle is not None
                else None
            ),
            "search_outcome": (
                dict(search_outcome) if search_outcome is not None else None
            ),
            "economic_no_trade_or_no_winner": False,
            "downstream_authority": False,
        }
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
            "anchor_plan_receipt_sha256": request_body[
                "anchor_plan_receipt_sha256"
            ],
            "activation_id": request_body["activation_id"],
            "activation_manifest_sha256": request_body[
                "activation_manifest_sha256"
            ],
            "anchor_plan_sha256": request_body["anchor_plan_sha256"],
            "anchor": dict(self.anchor),
            "search_process_receipt_sha256": request_body[
                "search_process_receipt_sha256"
            ],
            "causal_precommit_receipt_sha256": request_body[
                "causal_precommit_receipt_sha256"
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
                completed.production_descriptor_graph
                if completed is not None
                else None
            ),
            "winner_request_sha256": (
                production.request.request_sha256
                if production is not None
                else None
            ),
            "winner_decision_sha256": (
                production.decision.receipt_sha256
                if production is not None
                else None
            ),
            "winner_decision_status": (
                production.decision.status if production is not None else None
            ),
            "all_process_rejected": (
                completed.all_process_rejected
                if completed is not None
                else False
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
    ) -> None:
        _verify_bounded_raw_winner_canary_capture(self)
        self._require_active(handle)
        if type(failure) is not raw_winner.WinnerRawEvidenceIncomplete:
            _fail("CANARY_RAW_EVIDENCE_FAILURE_TYPE_INVALID")
        entry = self._failure_entry(
            failure_code=failure.code,
            attempted_trials_exact=handle.attempted_trials_exact,
            handle=handle,
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
    binding = orchestration.historical_horizon_plan_binding(
        plan.horizon_plan
    )
    horizon_body = _released_horizon_plan_body(
        plan.horizon_plan, "CANARY_HORIZON_PLAN_NOT_GENUINE"
    )
    plan_receipt = plan.cell_receipt
    if (
        plan.mode != "BOUNDED_RAW_WINNER_CANARY"
        or plan.symbol != request_body["symbol"]
        or plan.timeframe != request_body["timeframe"]
        or plan.attempt_count
        != orchestration.BOUNDED_RAW_WINNER_CANARY_ATTEMPT_COUNT
        or plan.search_process_receipt_sha256
        != request_body["search_process_receipt_sha256"]
        or plan.causal_precommit_receipt_sha256
        != request_body["causal_precommit_receipt_sha256"]
        or plan_receipt.get("artifact_namespace")
        != request_body["artifact_namespace"]
        or plan_receipt.get("attempt_count")
        != request_body["attempt_count_exact"]
        or binding.get("cell")
        != {
            "symbol": request_body["symbol"],
            "timeframe": request_body["timeframe"],
            "regime": plan.regime,
        }
        or binding.get("anchor_id")
        != request_body["anchor"]["anchor_id"]
        or binding.get("selection_cutoff_utc")
        != request_body["anchor"]["selection_cutoff_utc"]
        or binding.get("process_precommit_receipt_sha256")
        != request_body["causal_precommit_receipt_sha256"]
        or binding.get("plan_receipt_sha256")
        != plan.horizon_plan.receipt_sha256
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
        return hmac.new(
            capture_key, binding_payload(value), hashlib.sha256
        ).hexdigest()

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
                or value.cell_plan._bounded_canary_plan_permit
                is not value.plan_permit
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
        or body.get("schema_version")
        != BOUNDED_RAW_WINNER_CANARY_RECEIPT_SCHEMA
        or body.get("authority_scope")
        != orchestration.BOUNDED_RAW_WINNER_CANARY_SCOPE
    ):
        _fail("CANARY_RECEIPT_BYTES_OR_FIELDS_INVALID")
    expected_bindings = {
        "request_sha256": request.receipt_sha256,
        "request_nonce": request_body["request_nonce"],
        "generation_id": context.generation_id,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "generation_context_sha256": context.context_sha256,
        "anchor_plan_receipt_sha256": request_body[
            "anchor_plan_receipt_sha256"
        ],
        "activation_id": request_body["activation_id"],
        "activation_manifest_sha256": request_body[
            "activation_manifest_sha256"
        ],
        "anchor_plan_sha256": request_body["anchor_plan_sha256"],
        "anchor": request_body["anchor"],
        "search_process_receipt_sha256": request_body[
            "search_process_receipt_sha256"
        ],
        "causal_precommit_receipt_sha256": request_body[
            "causal_precommit_receipt_sha256"
        ],
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
        <= orchestration.BOUNDED_RAW_WINNER_CANARY_ATTEMPT_COUNT
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

    entry = _artifact_entry(
        receipt.artifact_entry, "CANARY_RECEIPT_ENTRY_INVALID"
    )
    if (
        entry["artifact_type"]
        != BOUNDED_RAW_WINNER_CANARY_RECEIPT_ARTIFACT_TYPE
        or entry["descriptor"]["sha256"] != receipt.receipt_sha256
        or entry["descriptor"]["size_bytes"] != len(receipt._body_json)
        or entry["descriptor"]["format"]
        != authority_spool.CANONICAL_JSON_FORMAT
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
        or source_session.get("generation_context_sha256")
        != context.context_sha256
        or source_session.get("config_sha256") != context.config_sha256
        or source_session.get("effective_pipeline_config_sha256")
        != body["effective_pipeline_config_sha256"]
        or source_session.get("selection_cutoff_utc")
        != request_body["anchor"]["selection_cutoff_utc"]
        or source_session.get("caller_supplied_product_or_fit_accepted") is not False
        or source_session.get("confers_downstream_or_trading_authority") is not False
    ):
        _fail("CANARY_SOURCE_SESSION_RECEIPT_BINDING_INVALID")

    raw_completed = body["raw_winner_completed"]
    if raw_completed is True:
        if (
            body["failure_code"] is not None
            or body["standalone_winner_verifier_accepted"] is not True
            or body["attempted_trials_exact"]
            != orchestration.BOUNDED_RAW_WINNER_CANARY_ATTEMPT_COUNT
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
            reconstructed_request = (
                winner_selection.create_winner_selection_request(
                    request_payload["precommitment"]["payload"],
                    request_payload["attempt_ledger"]["payload"],
                    request_payload["evidence_ledger"]["payload"],
                )
            )
            if (
                reconstructed_request is None
                or reconstructed_request.to_dict() != request_payload
                or reconstructed_request.request_sha256
                != body["winner_request_sha256"]
            ):
                _fail("CANARY_WINNER_REQUEST_RECONSTRUCTION_FAILED")
            decision_body = dict(decision_payload)
            decision_sha256 = decision_body.pop("receipt_sha256")
            reconstructed_decision = winner_selection.WinnerSelectionDecision(
                _canonical_bytes(decision_body), decision_sha256
            )
            if (
                reconstructed_decision.receipt_sha256
                != body["winner_decision_sha256"]
                or reconstructed_decision.status
                != body["winner_decision_status"]
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
            production_body.get("generation_context_sha256")
            != context.context_sha256
            or production_body.get("artifact_namespace")
            != request_body["artifact_namespace"]
            or production_body.get("symbol") != body["symbol"]
            or production_body.get("timeframe") != body["timeframe"]
            or production_body.get("regime") != body["regime"]
            or production_body.get("request_sha256")
            != body["winner_request_sha256"]
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
        if (
            failure_entry not in artifacts
            or failure_body.get("failure_code") != body["failure_code"]
            or failure_body.get("request_sha256") != request.receipt_sha256
            or failure_body.get("economic_no_trade_or_no_winner") is not False
            or failure_body.get("downstream_authority") is not False
        ):
            _fail("CANARY_TECHNICAL_FAILURE_BINDING_INVALID")
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
    "BOUNDED_RAW_WINNER_CANARY_FAILURE_ARTIFACT_TYPE",
    "BOUNDED_RAW_WINNER_CANARY_FAILURE_SCHEMA",
    "BOUNDED_RAW_WINNER_CANARY_RECEIPT_ARTIFACT_TYPE",
    "BOUNDED_RAW_WINNER_CANARY_RECEIPT_SCHEMA",
    "BoundedRawWinnerCanaryReceipt",
    "CAPTURE_CONTRACT_VERSION",
    "FAILURE_ARTIFACT_SCHEMA",
    "FrozenHistoricalCell",
    "HISTORICAL_PRODUCER_RELEASE_SHA256",
    "HORIZON_WINNER_CROSS_BINDING_SCHEMA",
    "HistoricalProductionAdapterError",
    "HistoricalSymbolSearchCapture",
    "OPTIMIZER_PAYLOAD_SCHEMA",
    "RAW_ARTIFACT_NAMESPACE_PREFIX",
    "RAW_DESCRIPTOR_GRAPH_SCHEMA",
    "SYMBOL_INDEX_SCHEMA",
    "historical_production_adapter_contract",
    "open_historical_symbol_capture",
    "verify_bounded_raw_winner_canary_receipt",
]
