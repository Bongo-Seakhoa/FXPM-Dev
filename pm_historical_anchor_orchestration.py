"""Bounded orchestration for repeated historical optimizer anchors.

This module schedules already-precommitted anchor-local optimization work.  It
does not create returns, winner decisions, causal decisions, or current-refit
authority.  A complete run is only eligible for the separate Causal verifier.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import hmac
import json
from pathlib import Path
import re
import secrets
import threading
from typing import Any, Final, Sequence

import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_causal_process as causal
import pm_training_horizon as training_horizon


PRECOMMIT_SCHEMA: Final = "fxpm.historical-anchor-orchestration-precommit.v1"
PRODUCER_REQUEST_SCHEMA: Final = "fxpm.historical-anchor-producer-request.v1"
PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA: Final = (
    "fxpm.historical-anchor-pre-evidence-planning-request.v1"
)
BOUNDED_RAW_WINNER_CANARY_REQUEST_SCHEMA: Final = (
    "fxpm.bounded-raw-winner-canary-request.v1"
)
BOUNDED_RAW_WINNER_CANARY_SCOPE: Final = "BOUNDED_CANARY_NONAUTHORIZING"
BOUNDED_RAW_WINNER_CANARY_SELECTION_RULE: Final = (
    "FIRST_EMPIRICALLY_OBSERVED_IN_PINNED_ORDER"
)
BOUNDED_RAW_WINNER_CANARY_RUN_PREFIX: Final = "raw-winner-canary-nonauthorizing"
PRODUCTION_NATIVE_AUTHORITY_VERIFICATION_SCOPE_SCHEMA: Final = (
    "fxpm.production_native_authority_verification_scope.v1"
)
SYMBOL_OUTPUT_SCHEMA: Final = "fxpm.historical-anchor-symbol-output.v1"
RUN_RECEIPT_SCHEMA: Final = "fxpm.historical-anchor-orchestration-receipt.v1"
HORIZON_BINDING_SCHEMA: Final = "fxpm.historical-horizon-plan-binding.v1"
CURRENT_HORIZON_BINDING_SCHEMA: Final = "fxpm.historical-horizon-plan-binding.v2"
AUTHORITY_SCOPE: Final = "HISTORICAL_ANCHOR_ORCHESTRATION_ONLY"

_ADAPTER_PAYLOAD_SCHEMA: Final = "fxpm.historical-optimizer-payload.v1"
_ADAPTER_INDEX_SCHEMA: Final = "fxpm.historical-symbol-production-index.v1"
_ADAPTER_GRAPH_SCHEMA: Final = "fxpm.historical-raw-winner-descriptor-graph.v1"
_ADAPTER_CROSS_BINDING_SCHEMA: Final = "fxpm.historical-horizon-winner-cross-binding.v2"
_ADAPTER_ASCII_JSON_FORMAT: Final = "fxpm-ascii-json-v1"
_ADAPTER_CAPTURE_CONTRACT: Final = "fxpm.historical-search-capture.v3"
_BOOK_ARTIFACT_TYPE: Final = "historical_book_multiplicity_v14"
_TERMINAL_TECHNICAL_REASON_CODE: Final = (
    "AUTHENTICATED_NATIVE_PREFIX_TERMINAL_TECHNICAL"
)
_WORKER_REQUEST_REOPEN_SEAL: Final = object()

_HORIZON_PLAN_SCHEMA: Final = "fxpm.historical-horizon-plan.v2"
_CURRENT_HORIZON_PLAN_SCHEMA: Final = "fxpm.historical-horizon-plan.v3"
_HORIZON_SCOPE: Final = "HISTORICAL_ANCHOR_LOCAL_HORIZON_EVIDENCE_ONLY"
_MAX_PRECOMMIT_BYTES: Final = 16 * 1024 * 1024
_MAX_RECEIPT_BYTES: Final = 16 * 1024 * 1024
_HEX64 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_UTC = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
    re.ASCII,
)

_CELL_ROSTER: Final = tuple(causal.expected_cell_roster())
_CELL_ORDINAL: Final = {cell: index for index, cell in enumerate(_CELL_ROSTER)}
_CELLS_PER_SYMBOL: Final = len(book.TIMEFRAME_ORDER) * len(book.REGIME_ORDER)

_ANCHOR_GEOMETRY_FIELDS: Final = frozenset(
    {
        "anchor_id",
        "selection_cutoff_utc",
        "forward_start_date",
        "forward_end_date",
        "purge_calendar_days",
    }
)
_ANCHOR_FIELDS: Final = frozenset(
    {
        *_ANCHOR_GEOMETRY_FIELDS,
        "horizon_binding_count",
        "horizon_binding_roster_sha256",
    }
)
_BINDING_FIELDS: Final = frozenset(
    {
        "schema_version",
        "ordinal",
        "cell",
        "anchor_id",
        "selection_cutoff_utc",
        "process_precommit_receipt_sha256",
        "plan_receipt_sha256",
        "source_prefix_receipt_sha256",
        "candidate_registry_sha256",
        "trial_registry_sha256",
        "attempt_roster_sha256",
        "evidence_roster_sha256",
        "plan_status",
    }
)
_PRECOMMIT_FIELDS: Final = frozenset(
    {
        "schema_version",
        "authority_scope",
        "run_id",
        "generation_id",
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "process_precommit_receipt_sha256",
        "optimizer_producer_release_sha256",
        "book_contract_version",
        "winner_contract_version",
        "symbols",
        "timeframes",
        "regimes",
        "source_group_count_per_anchor",
        "accounting_slot_count_per_anchor",
        "anchor_count",
        "anchors",
        "requires_exact_per_anchor_producer_callbacks",
        "possible_regime_slots_are_forced_observations",
        "authenticated_regime_unobserved_is_complete",
        "technical_or_unsearched_blocks_anchor",
        "confers_causal_authority",
        "confers_current_refit_authority",
        "confers_live_or_capital_authority",
    }
)
_RUN_FIELDS: Final = frozenset(
    {
        "schema_version",
        "authority_scope",
        "run_id",
        "generation_id",
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "precommit_receipt_sha256",
        "anchor_count",
        "anchors",
        "symbol_count",
        "symbol_receipts",
        "complete_sparse_state_rosters",
        "possible_regime_slots_are_forced_observations",
        "eligible_for_downstream_causal_evaluation",
        "causal_process_validated",
        "current_refit_authorized",
        "live_authorized",
        "capital_authorized",
        "profitability_claimed",
    }
)
_ANCHOR_RESULT_FIELDS: Final = frozenset(
    {
        "anchor_id",
        "producer_request_sha256",
        "horizon_binding_roster_sha256",
        "symbol_output_roster_sha256",
        "book_decision_sha256",
        "book_generation_sha256",
        "book_status",
        "historical_state_counts",
        "source_group_count",
        "accounting_slot_count",
        "eligible_for_downstream_causal_evaluation",
        "causal_process_validated",
        "current_refit_authorized",
    }
)
_HORIZON_PLAN_STATUSES: Final = frozenset(
    {
        "READY_FOR_HISTORICAL_EVIDENCE",
        "HISTORICAL_SOURCE_UNAVAILABLE",
        "HISTORICAL_TERMINAL_TECHNICAL",
        "HISTORICAL_TECHNICAL_INCOMPLETE",
        "HISTORICAL_BOUNDARY_LIMITED",
        "HISTORICAL_REGIME_UNOBSERVED",
    }
)
_SYMBOL_OUTPUT_FIELDS: Final = frozenset(
    {
        "schema_version",
        "authority_scope",
        "producer_request_sha256",
        "anchor_id",
        "symbol",
        "horizon_binding_count",
        "horizon_binding_roster_sha256",
        "horizon_bindings",
        "optimizer_payload",
        "confers_causal_or_current_authority",
    }
)
_CANARY_REQUEST_FIELDS: Final = frozenset(
    {
        "schema_version",
        "authority_scope",
        "request_nonce",
        "issued_at_utc",
        "earliest_source_accessed_at_utc",
        "run_id",
        "generation_id",
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "anchor_plan_receipt_sha256",
        "activation_id",
        "activation_manifest_sha256",
        "native_run_manifest_sha256",
        "native_v5_common_request_sha256",
        "anchor_plan_sha256",
        "anchor_count",
        "anchor",
        "search_process_receipt_sha256",
        "causal_precommit_receipt_sha256",
        "native_authority_verification",
        "causal_evidence_access_deadline_utc",
        "production_symbols",
        "production_timeframes",
        "production_regimes",
        "symbol",
        "timeframe",
        "regime_selection_rule",
        "attempt_count_exact",
        "artifact_namespace",
        "one_symbol_timeframe_regime_only",
        "source_access_must_follow_request",
        "planning_request_schema_accepted",
        "book_projection_authorized",
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
_NATIVE_AUTHORITY_VERIFICATION_FIELDS: Final = frozenset(
    {
        "schema_version",
        "scope",
        "full_verification_count",
        "identity_revalidation_attempt_count",
        "identity_revalidation_count",
        "identity_revalidation_failure_count",
        "active_pointer_sha256",
        "activation_manifest_sha256",
        "pid",
        "thread_id",
        "poison_reason",
        "closed",
        "poisoned",
    }
)
_ADAPTER_PAYLOAD_FIELDS: Final = frozenset(
    {
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
)
_ADAPTER_INDEX_FIELDS: Final = frozenset(
    {
        "schema_version",
        "capture_contract_version",
        "producer_release_sha256",
        "generation_id",
        "generation_context_sha256",
        "producer_request_sha256",
        "anchor",
        "symbol",
        "source_groups",
        "regime_fits",
        "horizon_bindings",
        "cell_records",
        "state_counts",
        "cell_count",
        "raw_artifact_namespace",
        "raw_descriptor_graph",
        "raw_descriptor_graph_root_count",
        "raw_descriptor_graph_roster_sha256",
        "raw_vectors_spooled_before_compaction",
        "posthoc_evidence_reconstruction",
        "complete_sparse_state_roster",
        "confers_causal_current_live_or_capital_authority",
    }
)
_ADAPTER_GRAPH_ROOT_FIELDS: Final = frozenset(
    {
        "schema_version",
        "generation_id",
        "generation_context_sha256",
        "producer_request_sha256",
        "run_id",
        "anchor_id",
        "symbol",
        "phase",
        "artifact_namespace",
        "artifact_count",
        "artifact_total_bytes",
        "artifact_roster_sha256",
        "artifacts",
        "complete_for_phase",
        "confers_causal_current_live_or_capital_authority",
    }
)
_ADAPTER_PHASE_GRAPH_FIELDS: Final = _ADAPTER_GRAPH_ROOT_FIELDS | frozenset(
    {"timeframe", "regime"}
)
_ADAPTER_CROSS_BINDING_FIELDS: Final = frozenset(
    {
        "schema_version",
        "producer_request_sha256",
        "anchor_id",
        "selection_cutoff_utc",
        "process_precommit_receipt_sha256",
        "source_group_decision_sha256",
        "regime_fit_decision_sha256",
        "native_prefix_index_sha256",
        "symbol",
        "timeframe",
        "regime",
        "artifact_namespace",
        "horizon_roster",
        "winner_roster",
        "mapping_count",
        "mapping_sha256",
        "mapping",
        "bijective_coverage",
        "created_before_winner_evidence_access",
        "confers_causal_current_live_or_capital_authority",
    }
)


class HistoricalAnchorOrchestrationError(ValueError):
    """A fail-closed orchestration contract error."""


def _fail(code: str) -> None:
    raise HistoricalAnchorOrchestrationError(code)


def _canonical_bytes(value: Any, maximum: int) -> bytes:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise HistoricalAnchorOrchestrationError("CANONICAL_JSON_INVALID") from exc
    if len(encoded) > maximum:
        _fail("CANONICAL_JSON_BOUND_EXCEEDED")
    return encoded


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value, _MAX_PRECOMMIT_BYTES)).hexdigest()


def _hex(value: Any, code: str) -> str:
    if type(value) is not str or _HEX64.fullmatch(value) is None:
        _fail(code)
    return value


def _safe_id(value: Any, code: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        _fail(code)
    return value


def _utc(value: Any, code: str) -> datetime:
    if type(value) is not str or _UTC.fullmatch(value) is None:
        _fail(code)
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise HistoricalAnchorOrchestrationError(code) from exc


def _date(value: Any, code: str) -> date:
    if type(value) is not str or len(value) != 10:
        _fail(code)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise HistoricalAnchorOrchestrationError(code) from exc
    if parsed.isoformat() != value:
        _fail(code)
    return parsed


def _descriptor(value: Any) -> dict[str, Any]:
    if type(value) is not dict:
        _fail("ARTIFACT_DESCRIPTOR_INVALID")
    required = {
        "schema_version",
        "relative_path",
        "sha256",
        "size_bytes",
        "format",
    }
    if set(value) != required:
        _fail("ARTIFACT_DESCRIPTOR_INVALID")
    return json.loads(_canonical_bytes(value, 64 * 1024).decode("utf-8"))


@dataclass(frozen=True, slots=True)
class HistoricalAnchorPrecommit:
    _body_json: bytes
    _descriptor_json: bytes

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()

    @property
    def artifact_descriptor(self) -> dict[str, Any]:
        return json.loads(self._descriptor_json.decode("utf-8"))


@dataclass(frozen=True, slots=True)
class AnchorProducerRequest:
    _body_json: bytes
    _symbol_sinks: Mapping[str, authority_spool.SymbolAuthoritySink] | None = None
    _worker_symbol: str | None = None
    _worker_reopen_seal: object | None = None

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()

    def symbol_sink(self, symbol: str) -> authority_spool.SymbolAuthoritySink:
        """Return the exact orchestration-owned sink that this run will seal."""

        if type(symbol) is not str or symbol not in book.PINNED_SYMBOL_ORDER:
            _fail("PRODUCER_REQUEST_SYMBOL_INVALID")
        if self._worker_reopen_seal is _WORKER_REQUEST_REOPEN_SEAL:
            if (
                self._worker_symbol != symbol
                or type(self._symbol_sinks) is not dict
                or set(self._symbol_sinks) != {symbol}
            ):
                _fail("PRODUCER_WORKER_SYMBOL_SINK_UNAVAILABLE")
            sink = self._symbol_sinks.get(symbol)
            if type(sink) is not authority_spool.SymbolAuthoritySink:
                _fail("PRODUCER_REQUEST_SYMBOL_SINK_INVALID")
            return sink
        if type(self._symbol_sinks) is not dict or set(self._symbol_sinks) != set(
            book.PINNED_SYMBOL_ORDER
        ):
            _fail("PRODUCER_REQUEST_SYMBOL_SINKS_UNAVAILABLE")
        sink = self._symbol_sinks.get(symbol)
        if type(sink) is not authority_spool.SymbolAuthoritySink:
            _fail("PRODUCER_REQUEST_SYMBOL_SINK_INVALID")
        return sink


def reopen_anchor_producer_request_for_symbol(
    context: authority_spool.GenerationSpoolContext,
    *,
    request_body_bytes: bytes,
    expected_receipt_sha256: str,
    symbol: str,
    sink: authority_spool.SymbolAuthoritySink,
) -> AnchorProducerRequest:
    """Reopen exact issuer bytes with one worker-local symbol sink.

    The restricted sink capability is process-local routing state.  It neither
    changes the canonical request bytes nor permits a worker to access another
    symbol's sink.
    """

    if (
        type(context) is not authority_spool.GenerationSpoolContext
        or type(request_body_bytes) is not bytes
        or not request_body_bytes
        or len(request_body_bytes) > 256 * 1024
        or type(symbol) is not str
        or symbol not in book.PINNED_SYMBOL_ORDER
        or type(sink) is not authority_spool.SymbolAuthoritySink
        or sink.symbol != symbol
        or sink.context is not context
        or sink.context.context_sha256 != context.context_sha256
    ):
        _fail("PRODUCER_WORKER_REQUEST_INPUT_INVALID")
    expected_receipt = _hex(
        expected_receipt_sha256,
        "PRODUCER_WORKER_REQUEST_RECEIPT_INVALID",
    )
    if hashlib.sha256(request_body_bytes).hexdigest() != expected_receipt:
        _fail("PRODUCER_WORKER_REQUEST_RECEIPT_MISMATCH")
    try:
        body = json.loads(request_body_bytes.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise HistoricalAnchorOrchestrationError(
            "PRODUCER_WORKER_REQUEST_JSON_INVALID"
        ) from exc
    if (
        type(body) is not dict
        or _canonical_bytes(body, 256 * 1024) != request_body_bytes
        or body.get("schema_version")
        not in {
            PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA,
            PRODUCER_REQUEST_SCHEMA,
        }
        or body.get("generation_id") != context.generation_id
        or body.get("symbols") != list(book.PINNED_SYMBOL_ORDER)
        or body.get("timeframes") != list(book.TIMEFRAME_ORDER)
        or body.get("regimes") != list(book.REGIME_ORDER)
        or body.get("requires_exact_sparse_state_roster") is not True
        or body.get("possible_regime_slots_are_forced_observations") is not False
        or body.get("confers_causal_or_current_authority") is not False
    ):
        _fail("PRODUCER_WORKER_REQUEST_CONTRACT_INVALID")
    if body["schema_version"] == PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA:
        context_body = _context_body(context)
        if (
            any(body.get(name) != value for name, value in context_body.items())
            or body.get("historical_anchor_precommit_pending") is not True
            or body.get("planning_access_limited_to_native_prefix_and_process_design")
            is not True
            or body.get("return_or_winner_evidence_access_authorized") is not False
        ):
            _fail("PRODUCER_WORKER_PLANNING_REQUEST_INVALID")
    request = AnchorProducerRequest(
        request_body_bytes,
        {symbol: sink},
        symbol,
        _WORKER_REQUEST_REOPEN_SEAL,
    )
    if request.receipt_sha256 != expected_receipt or request.symbol_sink(symbol) is not sink:
        _fail("PRODUCER_WORKER_REQUEST_REOPEN_FAILED")
    return request


@dataclass(frozen=True, slots=True)
class BoundedRawWinnerCanaryRequest:
    """Issuer-authenticated authority for exactly one diagnostic raw-Winner cell.

    The closure-held issuance tag is deliberately not serialized.  A direct
    constructor call, ``dataclasses.replace`` operation, or copied tag cannot
    authenticate changed request bytes or a substituted authority sink.
    """

    _body_json: bytes
    _symbol_sink: authority_spool.SymbolAuthoritySink
    _issuance_tag: str

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()

    def symbol_sink(self) -> authority_spool.SymbolAuthoritySink:
        if not _verify_canary_request_issuance(self):
            _fail("CANARY_REQUEST_ISSUANCE_INVALID")
        return self._symbol_sink


@dataclass(frozen=True, slots=True)
class AnchorSymbolProduct:
    """One symbol's exact optimizer output for one historical anchor."""

    symbol: str
    horizon_plans: Iterable[Any]
    book_evidence: Iterable[book.HistoricalBookCellEvidence]
    optimizer_payload: Any


@dataclass(frozen=True, slots=True)
class HistoricalAnchorRunReceipt:
    _body_json: bytes
    _descriptor_json: bytes

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("utf-8"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()

    @property
    def artifact_descriptor(self) -> dict[str, Any]:
        return json.loads(self._descriptor_json.decode("utf-8"))


def _context_body(context: authority_spool.GenerationSpoolContext) -> dict[str, str]:
    try:
        observed = authority_spool.reopen_generation_context(
            context.root,
            expected_generation_id=context.generation_id,
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=context.config_sha256,
        )
    except Exception as exc:
        raise HistoricalAnchorOrchestrationError("SPOOL_CONTEXT_INVALID") from exc
    if observed.context_sha256 != context.context_sha256:
        _fail("SPOOL_CONTEXT_CHANGED")
    return {
        "generation_id": context.generation_id,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "generation_context_sha256": context.context_sha256,
    }


def _utc_seconds(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        _fail("TRUSTED_UTC_CLOCK_INVALID")
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def _verified_canary_causal_precommit(
    value: Any,
    *,
    expected_anchors: Sequence[Mapping[str, Any]],
    expected_search_process_receipt_sha256: str,
) -> tuple[dict[str, Any], bytes]:
    """Reverify the exact producer-owned causal precommit without evidence access."""

    try:
        import pm_causal_evidence_producer as causal_evidence

        if type(value) is not causal_evidence.FrozenCausalProcessPrecommit:
            _fail("CANARY_CAUSAL_PRECOMMIT_OBJECT_INVALID")
        body = value.to_dict()
        body_json = _canonical_bytes(body, _MAX_PRECOMMIT_BYTES)
        if body_json != value._body_json:
            _fail("CANARY_CAUSAL_PRECOMMIT_BYTES_NONCANONICAL")
        if hashlib.sha256(body_json).hexdigest() != value.receipt_sha256:
            _fail("CANARY_CAUSAL_PRECOMMIT_RECEIPT_MISMATCH")
        causal._validate_precommit_payload(body)
    except HistoricalAnchorOrchestrationError:
        raise
    except Exception as exc:
        raise HistoricalAnchorOrchestrationError(
            "CANARY_CAUSAL_PRECOMMIT_REVERIFY_FAILED"
        ) from exc
    definition = body.get("process_definition")
    if type(definition) is not dict:
        _fail("CANARY_CAUSAL_PROCESS_DEFINITION_INVALID")
    anchors = definition.get("anchor_plan")
    expected = [dict(anchor) for anchor in expected_anchors]
    if anchors != expected:
        _fail("CANARY_CAUSAL_ANCHOR_PLAN_MISMATCH")
    policies = definition.get("policy_identities")
    if (
        type(policies) is not dict
        or policies.get("trial_registry_policy_sha256")
        != expected_search_process_receipt_sha256
    ):
        _fail("CANARY_CAUSAL_SEARCH_PROCESS_MISMATCH")
    return body, body_json


def _normalized_native_authority_verification(
    value: Any,
    *,
    expected_activation_manifest_sha256: str,
    allow_none: bool,
) -> dict[str, Any] | None:
    """Validate the closed process-local authority-verification accounting."""

    if value is None:
        if allow_none:
            return None
        _fail("CANARY_NATIVE_AUTHORITY_VERIFICATION_MISSING")
    if type(value) is not dict or set(value) != _NATIVE_AUTHORITY_VERIFICATION_FIELDS:
        _fail("CANARY_NATIVE_AUTHORITY_VERIFICATION_FIELDS_INVALID")
    normalized = json.loads(_canonical_bytes(value, 64 * 1024).decode("utf-8"))
    if (
        normalized.get("schema_version")
        != PRODUCTION_NATIVE_AUTHORITY_VERIFICATION_SCOPE_SCHEMA
    ):
        _fail("CANARY_NATIVE_AUTHORITY_VERIFICATION_SCHEMA_INVALID")
    _hex(normalized.get("scope"), "CANARY_NATIVE_AUTHORITY_SCOPE_ID_INVALID")
    _hex(
        normalized.get("active_pointer_sha256"),
        "CANARY_NATIVE_AUTHORITY_POINTER_SHA256_INVALID",
    )
    activation_sha = _hex(
        normalized.get("activation_manifest_sha256"),
        "CANARY_NATIVE_AUTHORITY_MANIFEST_SHA256_INVALID",
    )
    if activation_sha != expected_activation_manifest_sha256:
        _fail("CANARY_NATIVE_AUTHORITY_MANIFEST_MISMATCH")
    expected_counts = {
        "full_verification_count": 1,
        "identity_revalidation_attempt_count": 5,
        "identity_revalidation_count": 5,
        "identity_revalidation_failure_count": 0,
    }
    if any(
        type(normalized.get(field)) is not int
        or normalized.get(field) != expected
        for field, expected in expected_counts.items()
    ):
        _fail("CANARY_NATIVE_AUTHORITY_VERIFICATION_COUNTS_INVALID")
    if (
        type(normalized.get("pid")) is not int
        or normalized.get("pid") <= 0
        or type(normalized.get("thread_id")) is not int
        or normalized.get("thread_id") <= 0
        or normalized.get("poison_reason") is not None
        or normalized.get("closed") is not True
        or normalized.get("poisoned") is not False
    ):
        _fail("CANARY_NATIVE_AUTHORITY_VERIFICATION_STATE_INVALID")
    return normalized


def _close_and_snapshot_native_authority_verification(
    value: Any,
    *,
    expected_activation_manifest_sha256: str,
) -> dict[str, Any] | None:
    """Close the genuine reuse scope after call six, then freeze its counters."""

    if value is None:
        # Retained solely for existing isolated issuer fixtures.  The CLI
        # always supplies a genuine scope and refuses mechanical success when
        # this field is absent.
        return None
    try:
        import pm_native_activation as native_activation

        if type(value) is not native_activation.ProductionNativeAuthorityVerificationScope:
            _fail("CANARY_NATIVE_AUTHORITY_VERIFICATION_SCOPE_INVALID")
        value.close()
        stats = value.stats()
    except HistoricalAnchorOrchestrationError:
        raise
    except Exception as exc:
        raise HistoricalAnchorOrchestrationError(
            "CANARY_NATIVE_AUTHORITY_VERIFICATION_SCOPE_INVALID"
        ) from exc
    return _normalized_native_authority_verification(
        stats,
        expected_activation_manifest_sha256=expected_activation_manifest_sha256,
        allow_none=False,
    )


def _bounded_canary_process_attempt_count(process_body: Mapping[str, Any]) -> int:
    """Read the canary roster size from its verified production search process."""

    try:
        count = process_body["semantics"]["attempt_policy"][
            "attempt_count_per_observed_cell"
        ]
    except (KeyError, TypeError):
        _fail("CANARY_SEARCH_PROCESS_ATTEMPT_COUNT_INVALID")
    if type(count) is not int or count <= 0:
        _fail("CANARY_SEARCH_PROCESS_ATTEMPT_COUNT_INVALID")
    return count


def _build_bounded_raw_winner_canary_request_components(
    context: authority_spool.GenerationSpoolContext,
    *,
    data_dir: Path,
    anchor_plan_receipt: Any,
    search_process: Any,
    causal_precommit: Any,
    symbol: str,
    timeframe: str,
    native_authority_verification_scope: Any | None = None,
) -> tuple[bytes, authority_spool.SymbolAuthoritySink]:
    """Issue one non-authorizing raw-Winner canary before source access.

    The active native authority and complete production anchor roster are
    reopened here.  The caller can choose only a pinned symbol and timeframe;
    the first empirically observed regime is derived later from the genuine
    fitted prefix and can never be supplied post hoc.
    """

    try:
        import pm_production_anchor_plan as production_anchor_plan
        import pm_production_search_planner as search_planner
        import pm_portfolio_replay as portfolio_replay

        context_body = _context_body(context)
        search_planner.verify_frozen_production_search_process(context, search_process)
        process_body = search_process.to_dict()
        attempt_count_exact = _bounded_canary_process_attempt_count(process_body)
        verified_plan = production_anchor_plan.verify_production_anchor_plan_receipt(
            Path(data_dir).resolve(strict=True), anchor_plan_receipt
        )
        native_contract, native_errors = (
            portfolio_replay.active_native_portfolio_contract(
                Path(data_dir).resolve(strict=True),
                book.PINNED_SYMBOL_ORDER,
                timeframes=book.TIMEFRAME_ORDER,
            )
        )
    except HistoricalAnchorOrchestrationError:
        raise
    except Exception as exc:
        raise HistoricalAnchorOrchestrationError(
            "CANARY_ISSUANCE_PREREQUISITE_REVERIFY_FAILED"
        ) from exc

    native_authority_verification = (
        _close_and_snapshot_native_authority_verification(
            native_authority_verification_scope,
            expected_activation_manifest_sha256=_hex(
                verified_plan.get("activation_manifest_sha256"),
                "CANARY_ACTIVATION_MANIFEST_INVALID",
            ),
        )
    )

    run_id = process_body.get("run_id")
    generation_id = context_body["generation_id"]
    if (
        type(run_id) is not str
        or not run_id.startswith(BOUNDED_RAW_WINNER_CANARY_RUN_PREFIX)
        or not generation_id.startswith(BOUNDED_RAW_WINNER_CANARY_RUN_PREFIX)
        or run_id != generation_id
    ):
        _fail("CANARY_RUN_IDENTITY_INVALID")
    if process_body.get("generation_context_sha256") != context.context_sha256:
        _fail("CANARY_SEARCH_PROCESS_CONTEXT_MISMATCH")
    if (
        native_errors
        or portfolio_replay.canonical_json_sha256(native_contract)
        != context.native_universe_sha256
        or native_contract.get("activation_id") != verified_plan.get("activation_id")
        or native_contract.get("activation_manifest_sha256")
        != verified_plan.get("activation_manifest_sha256")
        or native_contract.get("symbols") != list(book.PINNED_SYMBOL_ORDER)
        or native_contract.get("timeframes") != list(book.TIMEFRAME_ORDER)
    ):
        _fail("CANARY_ACTIVE_NATIVE_UNIVERSE_CONTEXT_MISMATCH")
    if verified_plan.get("symbol_roster") != list(
        book.PINNED_SYMBOL_ORDER
    ) or verified_plan.get("timeframe_roster") != list(book.TIMEFRAME_ORDER):
        _fail("CANARY_PRODUCTION_GRID_MISMATCH")
    anchors = verified_plan.get("anchor_plan")
    if (
        type(anchors) is not list
        or len(anchors) < 4
        or verified_plan.get("anchor_count") != len(anchors)
    ):
        _fail("CANARY_ANCHOR_PLAN_INVALID")
    normalized_anchors = [_normalize_anchor_geometry(anchor) for anchor in anchors]
    causal_body, _ = _verified_canary_causal_precommit(
        causal_precommit,
        expected_anchors=normalized_anchors,
        expected_search_process_receipt_sha256=search_process.receipt_sha256,
    )
    if symbol not in book.PINNED_SYMBOL_ORDER or type(symbol) is not str:
        _fail("CANARY_SYMBOL_INVALID")
    if timeframe not in book.TIMEFRAME_ORDER or type(timeframe) is not str:
        _fail("CANARY_TIMEFRAME_INVALID")

    now = datetime.now(timezone.utc)
    issued_at = _utc_seconds(now)
    earliest_source_access = issued_at
    deadline = causal_body.get("evidence_access_deadline_utc")
    if _utc(deadline, "CANARY_CAUSAL_DEADLINE_INVALID") < _utc(
        earliest_source_access, "CANARY_SOURCE_ACCESS_BOUNDARY_INVALID"
    ):
        _fail("CANARY_CAUSAL_EVIDENCE_WINDOW_EXPIRED")
    nonce = secrets.token_hex(16)
    anchor = normalized_anchors[-1]
    artifact_namespace = "/".join(
        (
            BOUNDED_RAW_WINNER_CANARY_RUN_PREFIX,
            anchor["anchor_id"],
            symbol.lower(),
            timeframe.lower(),
            nonce,
        )
    )
    sink = authority_spool.open_symbol_sink(context, symbol)
    body = {
        "schema_version": BOUNDED_RAW_WINNER_CANARY_REQUEST_SCHEMA,
        "authority_scope": BOUNDED_RAW_WINNER_CANARY_SCOPE,
        "request_nonce": nonce,
        "issued_at_utc": issued_at,
        "earliest_source_accessed_at_utc": earliest_source_access,
        "run_id": run_id,
        **context_body,
        "anchor_plan_receipt_sha256": _hex(
            verified_plan.get("receipt_sha256"),
            "CANARY_ANCHOR_PLAN_RECEIPT_INVALID",
        ),
        "activation_id": _safe_id(
            verified_plan.get("activation_id"), "CANARY_ACTIVATION_ID_INVALID"
        ),
        "activation_manifest_sha256": _hex(
            verified_plan.get("activation_manifest_sha256"),
            "CANARY_ACTIVATION_MANIFEST_INVALID",
        ),
        "native_run_manifest_sha256": _hex(
            verified_plan.get("native_run_manifest_sha256"),
            "CANARY_NATIVE_RUN_MANIFEST_INVALID",
        ),
        "native_v5_common_request_sha256": _hex(
            verified_plan.get("native_v5_common_request_sha256"),
            "CANARY_NATIVE_COMMON_REQUEST_INVALID",
        ),
        "anchor_plan_sha256": _hex(
            verified_plan.get("anchor_plan_sha256"),
            "CANARY_ANCHOR_PLAN_SHA256_INVALID",
        ),
        "anchor_count": len(normalized_anchors),
        "anchor": anchor,
        "search_process_receipt_sha256": _hex(
            search_process.receipt_sha256,
            "CANARY_SEARCH_PROCESS_RECEIPT_INVALID",
        ),
        "causal_precommit_receipt_sha256": _hex(
            causal_precommit.receipt_sha256,
            "CANARY_CAUSAL_PRECOMMIT_RECEIPT_INVALID",
        ),
        "native_authority_verification": native_authority_verification,
        "causal_evidence_access_deadline_utc": deadline,
        "production_symbols": list(book.PINNED_SYMBOL_ORDER),
        "production_timeframes": list(book.TIMEFRAME_ORDER),
        "production_regimes": list(book.REGIME_ORDER),
        "symbol": symbol,
        "timeframe": timeframe,
        "regime_selection_rule": BOUNDED_RAW_WINNER_CANARY_SELECTION_RULE,
        "attempt_count_exact": attempt_count_exact,
        "artifact_namespace": artifact_namespace,
        "one_symbol_timeframe_regime_only": True,
        "source_access_must_follow_request": True,
        "planning_request_schema_accepted": False,
        "book_projection_authorized": False,
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
    body_json = _canonical_bytes(body, _MAX_PRECOMMIT_BYTES)
    return body_json, sink


def verify_bounded_raw_winner_canary_request(
    context: authority_spool.GenerationSpoolContext,
    request: Any,
) -> dict[str, Any]:
    """Verify issuer provenance and every immutable non-authority invariant."""

    if type(request) is not BoundedRawWinnerCanaryRequest:
        _fail("CANARY_REQUEST_OBJECT_INVALID")
    if not _verify_canary_request_issuance(request):
        _fail("CANARY_REQUEST_ISSUANCE_INVALID")
    context_body = _context_body(context)
    try:
        body = request.to_dict()
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise HistoricalAnchorOrchestrationError(
            "CANARY_REQUEST_BYTES_INVALID"
        ) from exc
    if set(body) != _CANARY_REQUEST_FIELDS:
        _fail("CANARY_REQUEST_FIELDS_INVALID")
    if _canonical_bytes(body, _MAX_PRECOMMIT_BYTES) != request._body_json:
        _fail("CANARY_REQUEST_BYTES_NONCANONICAL")
    if (
        body["schema_version"] != BOUNDED_RAW_WINNER_CANARY_REQUEST_SCHEMA
        or body["authority_scope"] != BOUNDED_RAW_WINNER_CANARY_SCOPE
    ):
        _fail("CANARY_REQUEST_SCOPE_INVALID")
    for key, expected in context_body.items():
        if body.get(key) != expected:
            _fail("CANARY_REQUEST_CONTEXT_MISMATCH")
    if body["run_id"] != body["generation_id"] or not body["run_id"].startswith(
        BOUNDED_RAW_WINNER_CANARY_RUN_PREFIX
    ):
        _fail("CANARY_REQUEST_RUN_IDENTITY_INVALID")
    _safe_id(body["request_nonce"], "CANARY_REQUEST_NONCE_INVALID")
    issued = _utc(body["issued_at_utc"], "CANARY_REQUEST_ISSUED_AT_INVALID")
    earliest = _utc(
        body["earliest_source_accessed_at_utc"],
        "CANARY_REQUEST_SOURCE_BOUNDARY_INVALID",
    )
    deadline = _utc(
        body["causal_evidence_access_deadline_utc"],
        "CANARY_REQUEST_CAUSAL_DEADLINE_INVALID",
    )
    if earliest != issued or deadline < earliest:
        _fail("CANARY_REQUEST_TIME_ORDER_INVALID")
    for key in (
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "anchor_plan_receipt_sha256",
        "activation_manifest_sha256",
        "native_run_manifest_sha256",
        "native_v5_common_request_sha256",
        "anchor_plan_sha256",
        "search_process_receipt_sha256",
        "causal_precommit_receipt_sha256",
    ):
        _hex(body[key], f"CANARY_REQUEST_{key.upper()}_INVALID")
    _normalized_native_authority_verification(
        body["native_authority_verification"],
        expected_activation_manifest_sha256=body["activation_manifest_sha256"],
        allow_none=True,
    )
    _safe_id(body["activation_id"], "CANARY_REQUEST_ACTIVATION_ID_INVALID")
    if (
        body["production_symbols"] != list(book.PINNED_SYMBOL_ORDER)
        or body["production_timeframes"] != list(book.TIMEFRAME_ORDER)
        or body["production_regimes"] != list(book.REGIME_ORDER)
        or body["symbol"] not in book.PINNED_SYMBOL_ORDER
        or body["timeframe"] not in book.TIMEFRAME_ORDER
    ):
        _fail("CANARY_REQUEST_PRODUCTION_GRID_INVALID")
    if type(body["anchor_count"]) is not int or body["anchor_count"] < 4:
        _fail("CANARY_REQUEST_ANCHOR_COUNT_INVALID")
    _normalize_anchor_geometry(body["anchor"])
    if (
        body["regime_selection_rule"] != BOUNDED_RAW_WINNER_CANARY_SELECTION_RULE
        or type(body["attempt_count_exact"]) is not int
        or body["attempt_count_exact"] <= 0
        or body["one_symbol_timeframe_regime_only"] is not True
        or body["source_access_must_follow_request"] is not True
        or body["planning_request_schema_accepted"] is not False
    ):
        _fail("CANARY_REQUEST_CELL_CONTRACT_INVALID")
    authority_flags = (
        "book_projection_authorized",
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
    if any(body[key] is not False for key in authority_flags):
        _fail("CANARY_REQUEST_AUTHORITY_ESCALATION")
    expected_namespace_prefix = "/".join(
        (
            BOUNDED_RAW_WINNER_CANARY_RUN_PREFIX,
            body["anchor"]["anchor_id"],
            body["symbol"].lower(),
            body["timeframe"].lower(),
            body["request_nonce"],
        )
    )
    if body["artifact_namespace"] != expected_namespace_prefix:
        _fail("CANARY_REQUEST_NAMESPACE_INVALID")
    sink = request._symbol_sink
    if (
        type(sink) is not authority_spool.SymbolAuthoritySink
        or sink.symbol != body["symbol"]
        or sink.context is not context
        or sink.context.context_sha256 != context.context_sha256
    ):
        _fail("CANARY_REQUEST_SINK_INVALID")
    return json.loads(_canonical_bytes(body, _MAX_PRECOMMIT_BYTES).decode("utf-8"))


def _make_bounded_raw_winner_canary_issuer(issue_builder: Callable[..., Any]):
    """Return issuer and verifier while keeping the raw HMAC signer private.

    Search-source, planning, capture, and one-shot state belong to the single
    authoritative executor facade.  This module authenticates only the
    prerequisite-valid request created before that facade opens any evidence.
    """

    request_key = secrets.token_bytes(32)

    def request_payload(
        body_json: bytes,
        sink: authority_spool.SymbolAuthoritySink,
    ) -> bytes:
        try:
            sink_binding = {
                "object_identity": id(sink),
                "symbol": sink.symbol,
                "generation_id": sink.context.generation_id,
                "generation_context_sha256": sink.context.context_sha256,
                "native_universe_sha256": sink.context.native_universe_sha256,
                "config_sha256": sink.context.config_sha256,
            }
        except AttributeError as exc:
            raise HistoricalAnchorOrchestrationError(
                "CANARY_REQUEST_SINK_INVALID"
            ) from exc
        return _canonical_bytes(
            {
                "schema_version": "fxpm.bounded-raw-winner-canary-issuance.v2",
                "body_sha256": hashlib.sha256(body_json).hexdigest(),
                "body_size_bytes": len(body_json),
                "sink_binding": sink_binding,
            },
            64 * 1024,
        )

    def request_tag(body_json: bytes, sink: authority_spool.SymbolAuthoritySink) -> str:
        return hmac.new(
            request_key,
            request_payload(body_json, sink),
            hashlib.sha256,
        ).hexdigest()

    def verify_request(value: Any) -> bool:
        try:
            if type(value) is not BoundedRawWinnerCanaryRequest:
                return False
            expected = request_tag(bytes(value._body_json), value._symbol_sink)
            return hmac.compare_digest(value._issuance_tag, expected)
        except (AttributeError, TypeError, ValueError, OverflowError):
            return False

    def issue(
        context: authority_spool.GenerationSpoolContext,
        *,
        data_dir: Path,
        anchor_plan_receipt: Any,
        search_process: Any,
        causal_precommit: Any,
        symbol: str,
        timeframe: str,
        native_authority_verification_scope: Any | None = None,
    ) -> BoundedRawWinnerCanaryRequest:
        body_json, sink = issue_builder(
            context,
            data_dir=data_dir,
            anchor_plan_receipt=anchor_plan_receipt,
            search_process=search_process,
            causal_precommit=causal_precommit,
            symbol=symbol,
            timeframe=timeframe,
            native_authority_verification_scope=(
                native_authority_verification_scope
            ),
        )
        request = BoundedRawWinnerCanaryRequest(
            body_json,
            sink,
            request_tag(body_json, sink),
        )
        verify_bounded_raw_winner_canary_request(context, request)
        return request

    consumed_receipts: set[str] = set()
    consumption_lock = threading.Lock()

    def consume(
        context: authority_spool.GenerationSpoolContext,
        request: Any,
    ) -> tuple[dict[str, Any], authority_spool.SymbolAuthoritySink]:
        """One-shot consumption gate: a canary request authorizes one attempt.

        Consumption is keyed by the issuance-unique receipt (every issued body
        carries a fresh nonce), held in closure state so no caller can reset
        it. Re-consumption fails closed even for a byte-identical request.
        """

        body = verify_bounded_raw_winner_canary_request(context, request)
        receipt = request.receipt_sha256
        with consumption_lock:
            if receipt in consumed_receipts:
                _fail("CANARY_REQUEST_ALREADY_CONSUMED")
            # Burn the request before the executor opens any source. A failed
            # partial canary must never be replayed inside the same process.
            consumed_receipts.add(receipt)
        return body, request.symbol_sink()

    return issue, verify_request, consume


(
    issue_bounded_raw_winner_canary_request,
    _verify_canary_request_issuance,
    consume_bounded_raw_winner_canary_request,
) = _make_bounded_raw_winner_canary_issuer(
    _build_bounded_raw_winner_canary_request_components
)


def _normalize_anchor_geometry(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _ANCHOR_GEOMETRY_FIELDS:
        _fail("ANCHOR_FIELDS_INVALID")
    anchor = json.loads(_canonical_bytes(value, 64 * 1024).decode("utf-8"))
    _safe_id(anchor["anchor_id"], "ANCHOR_ID_INVALID")
    cutoff = _utc(anchor["selection_cutoff_utc"], "ANCHOR_CUTOFF_INVALID")
    start = _date(anchor["forward_start_date"], "ANCHOR_FORWARD_START_INVALID")
    end = _date(anchor["forward_end_date"], "ANCHOR_FORWARD_END_INVALID")
    purge = anchor["purge_calendar_days"]
    policy = causal.pinned_process_policy()
    if type(purge) is not int or type(purge) is bool:
        _fail("ANCHOR_PURGE_INVALID")
    if not policy["minimum_purge_calendar_days"] <= purge <= 366:
        _fail("ANCHOR_PURGE_OUTSIDE_CAUSAL_POLICY")
    if start != cutoff.date() + timedelta(days=purge):
        _fail("ANCHOR_PURGE_GEOMETRY_INVALID")
    days = (end - start).days + 1
    if (
        not policy["minimum_days_per_anchor"]
        <= days
        <= policy["maximum_days_per_anchor"]
    ):
        _fail("ANCHOR_FORWARD_LENGTH_INVALID")
    return anchor


def _normalize_anchor(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _ANCHOR_FIELDS:
        _fail("ANCHOR_FIELDS_INVALID")
    anchor = json.loads(_canonical_bytes(value, 64 * 1024).decode("utf-8"))
    _normalize_anchor_geometry({name: anchor[name] for name in _ANCHOR_GEOMETRY_FIELDS})
    if anchor["horizon_binding_count"] != book.FAMILY_SIZE:
        _fail("HORIZON_BINDING_COUNT_INCOMPLETE")
    _hex(
        anchor["horizon_binding_roster_sha256"],
        "HORIZON_BINDING_ROSTER_SHA256_INVALID",
    )
    return anchor


def issue_historical_pre_evidence_planning_request(
    context: authority_spool.GenerationSpoolContext,
    *,
    run_id: str,
    process_precommit_receipt_sha256: str,
    optimizer_producer_release_sha256: str,
    anchor: Mapping[str, Any],
    symbol_sinks: Mapping[str, authority_spool.SymbolAuthoritySink],
) -> AnchorProducerRequest:
    """Issue a plan-only request before the historical roster is committed.

    The causal process and anchor geometry are already frozen at this point.
    This request authorizes deterministic native-prefix, empirical-regime and
    Horizon-plan construction only.  It deliberately has no historical-run
    precommit receipt: the exact 2,400 binding roster produced by this phase is
    what the subsequent historical precommit commits.  Strategy-return and
    winner evidence remain forbidden until the canonical producer request is
    issued by :func:`execute_historical_anchor_run`.
    """

    context_body = _context_body(context)
    normalized_anchor = _normalize_anchor_geometry(dict(anchor))
    if type(symbol_sinks) is not dict or set(symbol_sinks) != set(
        book.PINNED_SYMBOL_ORDER
    ):
        _fail("PRE_EVIDENCE_PLANNING_SYMBOL_SINKS_INVALID")
    for symbol in book.PINNED_SYMBOL_ORDER:
        sink = symbol_sinks.get(symbol)
        if (
            type(sink) is not authority_spool.SymbolAuthoritySink
            or sink.symbol != symbol
            or sink.context is not context
            or sink.context.context_sha256 != context.context_sha256
        ):
            _fail("PRE_EVIDENCE_PLANNING_SYMBOL_SINK_INVALID")
    body = {
        "schema_version": PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA,
        "authority_scope": AUTHORITY_SCOPE,
        "run_id": _safe_id(run_id, "RUN_ID_INVALID"),
        **context_body,
        "historical_anchor_precommit_pending": True,
        "process_precommit_receipt_sha256": _hex(
            process_precommit_receipt_sha256,
            "PROCESS_PRECOMMIT_RECEIPT_INVALID",
        ),
        "optimizer_producer_release_sha256": _hex(
            optimizer_producer_release_sha256,
            "OPTIMIZER_PRODUCER_RELEASE_INVALID",
        ),
        "anchor": normalized_anchor,
        "symbols": list(book.PINNED_SYMBOL_ORDER),
        "timeframes": list(book.TIMEFRAME_ORDER),
        "regimes": list(book.REGIME_ORDER),
        "requires_exact_sparse_state_roster": True,
        "possible_regime_slots_are_forced_observations": False,
        "planning_access_limited_to_native_prefix_and_process_design": True,
        "return_or_winner_evidence_access_authorized": False,
        "confers_causal_or_current_authority": False,
    }
    return AnchorProducerRequest(
        _canonical_bytes(body, 256 * 1024),
        symbol_sinks,
    )


def _normalize_anchors(values: Any) -> list[dict[str, Any]]:
    if type(values) is not list:
        _fail("ANCHORS_NOT_LIST")
    policy = causal.pinned_process_policy()
    if (
        not policy["minimum_anchor_count"]
        <= len(values)
        <= policy["maximum_anchor_count"]
    ):
        _fail("ANCHOR_COUNT_OUTSIDE_CAUSAL_POLICY")
    result: list[dict[str, Any]] = []
    identifiers: set[str] = set()
    prior_cutoff: datetime | None = None
    prior_end: date | None = None
    for value in values:
        anchor = _normalize_anchor(value)
        identifier = anchor["anchor_id"]
        if identifier in identifiers:
            _fail("ANCHOR_ID_DUPLICATED")
        identifiers.add(identifier)
        cutoff = _utc(anchor["selection_cutoff_utc"], "ANCHOR_CUTOFF_INVALID")
        start = _date(anchor["forward_start_date"], "ANCHOR_FORWARD_START_INVALID")
        end = _date(anchor["forward_end_date"], "ANCHOR_FORWARD_END_INVALID")
        if prior_cutoff is not None and cutoff <= prior_cutoff:
            _fail("ANCHOR_CUTOFF_ORDER_INVALID")
        if prior_end is not None:
            prior_end_exclusive = datetime.combine(
                prior_end + timedelta(days=1), datetime.min.time(), timezone.utc
            )
            if start <= prior_end or cutoff <= prior_end_exclusive:
                _fail("ANCHOR_FORWARD_SEGMENTS_NOT_ROLLING_DISJOINT")
        result.append(anchor)
        prior_cutoff = cutoff
        prior_end = end
    return result


def precommit_historical_anchor_run(
    context: authority_spool.GenerationSpoolContext,
    *,
    run_id: str,
    process_precommit_receipt_sha256: str,
    optimizer_producer_release_sha256: str,
    anchors: list[dict[str, Any]],
) -> HistoricalAnchorPrecommit:
    """Freeze exact repeated anchors before any optimizer callback is invoked."""

    context_body = _context_body(context)
    normalized_anchors = _normalize_anchors(anchors)
    body = {
        "schema_version": PRECOMMIT_SCHEMA,
        "authority_scope": AUTHORITY_SCOPE,
        "run_id": _safe_id(run_id, "RUN_ID_INVALID"),
        **context_body,
        "process_precommit_receipt_sha256": _hex(
            process_precommit_receipt_sha256,
            "PROCESS_PRECOMMIT_RECEIPT_INVALID",
        ),
        "optimizer_producer_release_sha256": _hex(
            optimizer_producer_release_sha256,
            "OPTIMIZER_PRODUCER_RELEASE_INVALID",
        ),
        "book_contract_version": book.BOOK_MULTIPLICITY_CONTRACT_VERSION,
        "winner_contract_version": book.PINNED_WINNER_SELECTION_CONTRACT_VERSION,
        "symbols": list(book.PINNED_SYMBOL_ORDER),
        "timeframes": list(book.TIMEFRAME_ORDER),
        "regimes": list(book.REGIME_ORDER),
        "source_group_count_per_anchor": book.SOURCE_GROUP_SIZE,
        "accounting_slot_count_per_anchor": book.FAMILY_SIZE,
        "anchor_count": len(normalized_anchors),
        "anchors": normalized_anchors,
        "requires_exact_per_anchor_producer_callbacks": True,
        "possible_regime_slots_are_forced_observations": False,
        "authenticated_regime_unobserved_is_complete": True,
        "technical_or_unsearched_blocks_anchor": True,
        "confers_causal_authority": False,
        "confers_current_refit_authority": False,
        "confers_live_or_capital_authority": False,
    }
    encoded = _canonical_bytes(body, _MAX_PRECOMMIT_BYTES)
    descriptor = authority_spool.write_canonical_json_artifact(context, body)
    if descriptor["sha256"] != hashlib.sha256(encoded).hexdigest():
        _fail("PRECOMMIT_SPOOL_RECEIPT_MISMATCH")
    return HistoricalAnchorPrecommit(
        encoded,
        _canonical_bytes(descriptor, 64 * 1024),
    )


def historical_horizon_plan_binding(plan: Any) -> dict[str, Any]:
    """Bind a sealed plan without upgrading the issued anchor generation."""

    try:
        valid = training_horizon._valid_plan(plan)
        body = plan.to_dict()
        receipt = plan.receipt_sha256
    except Exception as exc:
        raise HistoricalAnchorOrchestrationError("HORIZON_PLAN_OBJECT_INVALID") from exc
    if valid is not True or type(body) is not dict:
        _fail("HORIZON_PLAN_OBJECT_INVALID")
    if type(
        getattr(plan, "_body_json", None)
    ) is not bytes or plan._body_json != _canonical_bytes(body, 96 * 1024 * 1024):
        _fail("HORIZON_PLAN_BODY_INVALID")
    encoded = plan._body_json
    if (
        _hex(receipt, "HORIZON_PLAN_RECEIPT_INVALID")
        != hashlib.sha256(encoded).hexdigest()
    ):
        _fail("HORIZON_PLAN_RECEIPT_MISMATCH")
    required = {
        "schema_version",
        "authority_scope",
        "confers_trading_or_capital_authority",
        "confers_final_oos_authority",
        "anchor",
        "cell",
        "source_prefix_receipt_sha256",
        "candidate_registry_sha256",
        "trial_registry_sha256",
        "attempt_roster_sha256",
        "evidence_roster_sha256",
        "plan_status",
    }
    if not required.issubset(body):
        _fail("HORIZON_PLAN_FIELDS_INCOMPLETE")
    if (
        body["schema_version"]
        not in (_HORIZON_PLAN_SCHEMA, _CURRENT_HORIZON_PLAN_SCHEMA)
        or body["authority_scope"] != _HORIZON_SCOPE
        or body["confers_trading_or_capital_authority"] is not False
        or body["confers_final_oos_authority"] is not False
    ):
        _fail("HORIZON_PLAN_AUTHORITY_INVALID")
    cell = body["cell"]
    anchor = body["anchor"]
    if type(cell) is not dict or set(cell) != {"symbol", "timeframe", "regime"}:
        _fail("HORIZON_PLAN_CELL_INVALID")
    cell_tuple = (cell["symbol"], cell["timeframe"], cell["regime"])
    ordinal = _CELL_ORDINAL.get(cell_tuple)
    if ordinal is None:
        _fail("HORIZON_PLAN_CELL_OUTSIDE_PINNED_ROSTER")
    if type(anchor) is not dict:
        _fail("HORIZON_PLAN_ANCHOR_INVALID")
    for name in (
        "anchor_id",
        "selection_cutoff_utc",
        "process_precommit_receipt_sha256",
    ):
        if name not in anchor:
            _fail("HORIZON_PLAN_ANCHOR_INVALID")
    source_prefix = body["source_prefix_receipt_sha256"]
    if source_prefix is not None:
        _hex(source_prefix, "HORIZON_SOURCE_PREFIX_RECEIPT_INVALID")
    for name in (
        "candidate_registry_sha256",
        "trial_registry_sha256",
        "attempt_roster_sha256",
        "evidence_roster_sha256",
    ):
        _hex(body[name], f"HORIZON_{name.upper()}_INVALID")
    if body["plan_status"] not in _HORIZON_PLAN_STATUSES:
        _fail("HORIZON_PLAN_STATUS_INVALID")
    return {
        "schema_version": (
            HORIZON_BINDING_SCHEMA
            if body["schema_version"] == _HORIZON_PLAN_SCHEMA
            else CURRENT_HORIZON_BINDING_SCHEMA
        ),
        "ordinal": ordinal,
        "cell": dict(cell),
        "anchor_id": _safe_id(anchor["anchor_id"], "HORIZON_ANCHOR_ID_INVALID"),
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "process_precommit_receipt_sha256": _hex(
            anchor["process_precommit_receipt_sha256"],
            "HORIZON_PROCESS_PRECOMMIT_INVALID",
        ),
        "plan_receipt_sha256": receipt,
        "source_prefix_receipt_sha256": source_prefix,
        "candidate_registry_sha256": body["candidate_registry_sha256"],
        "trial_registry_sha256": body["trial_registry_sha256"],
        "attempt_roster_sha256": body["attempt_roster_sha256"],
        "evidence_roster_sha256": body["evidence_roster_sha256"],
        "plan_status": body["plan_status"],
    }


def _normalize_binding(
    value: Any,
    *,
    expected_cell: tuple[str, str, str],
    anchor: Mapping[str, Any],
    process_precommit_receipt_sha256: str,
) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _BINDING_FIELDS:
        _fail("HORIZON_BINDING_FIELDS_INVALID")
    binding = json.loads(_canonical_bytes(value, 64 * 1024).decode("utf-8"))
    if binding["schema_version"] != HORIZON_BINDING_SCHEMA:
        _fail("HORIZON_BINDING_SCHEMA_INVALID")
    expected_ordinal = _CELL_ORDINAL[expected_cell]
    if binding["ordinal"] != expected_ordinal or binding["cell"] != {
        "symbol": expected_cell[0],
        "timeframe": expected_cell[1],
        "regime": expected_cell[2],
    }:
        _fail("HORIZON_BINDING_ROSTER_MISMATCH")
    if (
        binding["anchor_id"] != anchor["anchor_id"]
        or binding["selection_cutoff_utc"] != anchor["selection_cutoff_utc"]
        or binding["process_precommit_receipt_sha256"]
        != process_precommit_receipt_sha256
    ):
        _fail("HORIZON_BINDING_ANCHOR_MISMATCH")
    for name in (
        "process_precommit_receipt_sha256",
        "plan_receipt_sha256",
        "candidate_registry_sha256",
        "trial_registry_sha256",
        "attempt_roster_sha256",
        "evidence_roster_sha256",
    ):
        _hex(binding[name], f"HORIZON_BINDING_{name.upper()}_INVALID")
    source = binding["source_prefix_receipt_sha256"]
    if source is not None:
        _hex(source, "HORIZON_BINDING_SOURCE_PREFIX_INVALID")
    if binding["plan_status"] not in _HORIZON_PLAN_STATUSES:
        _fail("HORIZON_BINDING_PLAN_STATUS_INVALID")
    return binding


class _CanonicalListHasher:
    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b"[")
        self._count = 0

    def add(self, value: Any) -> None:
        if self._count:
            self._digest.update(b",")
        self._digest.update(_canonical_bytes(value, 64 * 1024))
        self._count += 1

    def finish(self) -> tuple[int, str]:
        self._digest.update(b"]")
        return self._count, self._digest.hexdigest()


def historical_horizon_binding_roster_sha256(
    bindings: Iterable[Mapping[str, Any]],
) -> str:
    """Hash the exact 2,400-slot Horizon roster without retaining it."""

    if isinstance(bindings, (str, bytes, bytearray, dict)):
        _fail("HORIZON_BINDING_ROSTER_ITERABLE_INVALID")
    iterator = iter(bindings)
    hasher = _CanonicalListHasher()
    roster_schema = None
    for expected_cell in _CELL_ROSTER:
        try:
            binding = next(iterator)
        except StopIteration as exc:
            raise HistoricalAnchorOrchestrationError(
                "HORIZON_BINDING_ROSTER_INCOMPLETE"
            ) from exc
        if type(binding) is not dict or set(binding) != _BINDING_FIELDS:
            _fail("HORIZON_BINDING_FIELDS_INVALID")
        schema = binding["schema_version"]
        if schema not in (HORIZON_BINDING_SCHEMA, CURRENT_HORIZON_BINDING_SCHEMA):
            _fail("HORIZON_BINDING_SCHEMA_INVALID")
        if roster_schema is None:
            roster_schema = schema
        elif schema != roster_schema:
            _fail("HORIZON_BINDING_MIXED_GENERATIONS")
        if binding.get("ordinal") != _CELL_ORDINAL[expected_cell] or binding.get(
            "cell"
        ) != {
            "symbol": expected_cell[0],
            "timeframe": expected_cell[1],
            "regime": expected_cell[2],
        }:
            _fail("HORIZON_BINDING_ROSTER_MISMATCH")
        hasher.add(binding)
    try:
        next(iterator)
    except StopIteration:
        return hasher.finish()[1]
    _fail("HORIZON_BINDING_ROSTER_OVERSIZED")


def _valid_precommit(
    context: authority_spool.GenerationSpoolContext,
    precommit: HistoricalAnchorPrecommit,
) -> dict[str, Any]:
    if type(precommit) is not HistoricalAnchorPrecommit:
        _fail("PRECOMMIT_OBJECT_INVALID")
    body = precommit.to_dict()
    if _canonical_bytes(body, _MAX_PRECOMMIT_BYTES) != precommit._body_json:
        _fail("PRECOMMIT_CANONICAL_MISMATCH")
    context_body = _context_body(context)
    if (
        set(body) != _PRECOMMIT_FIELDS
        or body.get("schema_version") != PRECOMMIT_SCHEMA
        or body.get("authority_scope") != AUTHORITY_SCOPE
        or any(body.get(name) != value for name, value in context_body.items())
        or body.get("book_contract_version") != book.BOOK_MULTIPLICITY_CONTRACT_VERSION
        or body.get("winner_contract_version")
        != book.PINNED_WINNER_SELECTION_CONTRACT_VERSION
        or body.get("symbols") != list(book.PINNED_SYMBOL_ORDER)
        or body.get("timeframes") != list(book.TIMEFRAME_ORDER)
        or body.get("regimes") != list(book.REGIME_ORDER)
        or body.get("source_group_count_per_anchor") != book.SOURCE_GROUP_SIZE
        or body.get("accounting_slot_count_per_anchor") != book.FAMILY_SIZE
        or body.get("requires_exact_per_anchor_producer_callbacks") is not True
        or body.get("possible_regime_slots_are_forced_observations") is not False
        or body.get("authenticated_regime_unobserved_is_complete") is not True
        or body.get("technical_or_unsearched_blocks_anchor") is not True
        or body.get("confers_causal_authority") is not False
        or body.get("confers_current_refit_authority") is not False
        or body.get("confers_live_or_capital_authority") is not False
    ):
        _fail("PRECOMMIT_BODY_INVALID")
    _safe_id(body.get("run_id"), "PRECOMMIT_RUN_ID_INVALID")
    _hex(
        body.get("process_precommit_receipt_sha256"),
        "PRECOMMIT_PROCESS_RECEIPT_INVALID",
    )
    _hex(
        body.get("optimizer_producer_release_sha256"),
        "PRECOMMIT_PRODUCER_RELEASE_INVALID",
    )
    anchors = _normalize_anchors(body.get("anchors"))
    if body.get("anchor_count") != len(anchors):
        _fail("PRECOMMIT_ANCHOR_COUNT_MISMATCH")
    descriptor = _descriptor(precommit.artifact_descriptor)
    if (
        descriptor["sha256"] != precommit.receipt_sha256
        or descriptor["size_bytes"] != len(precommit._body_json)
        or descriptor["format"] != authority_spool.CANONICAL_JSON_FORMAT
    ):
        _fail("PRECOMMIT_ARTIFACT_DESCRIPTOR_MISMATCH")
    return body


def _producer_request(
    precommit: HistoricalAnchorPrecommit,
    precommit_body: Mapping[str, Any],
    anchor: Mapping[str, Any],
    symbol_sinks: Mapping[str, authority_spool.SymbolAuthoritySink] | None = None,
) -> AnchorProducerRequest:
    body = {
        "schema_version": PRODUCER_REQUEST_SCHEMA,
        "authority_scope": AUTHORITY_SCOPE,
        "run_id": precommit_body["run_id"],
        "generation_id": precommit_body["generation_id"],
        "precommit_receipt_sha256": precommit.receipt_sha256,
        "process_precommit_receipt_sha256": precommit_body[
            "process_precommit_receipt_sha256"
        ],
        "optimizer_producer_release_sha256": precommit_body[
            "optimizer_producer_release_sha256"
        ],
        "anchor": dict(anchor),
        "symbols": list(book.PINNED_SYMBOL_ORDER),
        "timeframes": list(book.TIMEFRAME_ORDER),
        "regimes": list(book.REGIME_ORDER),
        "requires_exact_sparse_state_roster": True,
        "possible_regime_slots_are_forced_observations": False,
        "confers_causal_or_current_authority": False,
    }
    return AnchorProducerRequest(_canonical_bytes(body, 256 * 1024), symbol_sinks)


def _iterable(value: Any, code: str) -> Any:
    if isinstance(value, (str, bytes, bytearray, dict)) or value is None:
        _fail(code)
    try:
        return iter(value)
    except TypeError as exc:
        raise HistoricalAnchorOrchestrationError(code) from exc


def _historical_evidence_state(
    evidence: book.HistoricalBookCellEvidence,
) -> str:
    """Classify one exact Book input without trusting producer labels."""

    if type(evidence) is not book.HistoricalBookCellEvidence:
        _fail("SYMBOL_BOOK_EVIDENCE_TYPE_INVALID")
    if evidence.winner_decision is not None or evidence.winner_request is not None:
        if (
            evidence.winner_decision is None
            or evidence.winner_request is None
            or evidence.process_state_decision is not None
        ):
            _fail("SYMBOL_BOOK_EVIDENCE_KIND_INVALID")
        try:
            source = evidence.source_group_decision.to_dict()
        except Exception as exc:
            raise HistoricalAnchorOrchestrationError(
                "SYMBOL_BOOK_SOURCE_DECISION_INVALID"
            ) from exc
        if source.get("availability") != book.AVAILABLE:
            _fail("SYMBOL_BOOK_WINNER_SOURCE_NOT_AVAILABLE")
        return "WINNER_EVIDENCE"
    if type(evidence.process_state_decision) is not book.HistoricalProcessStateDecision:
        _fail("SYMBOL_BOOK_EVIDENCE_KIND_INVALID")
    state = evidence.process_state_decision.state_kind
    if state not in {
        book.TERMINAL_SOURCE_ABSENCE,
        book.TERMINAL_TECHNICAL,
        book.TECHNICAL_FAILURE,
        book.UNSEARCHED,
        book.REGIME_UNOBSERVED,
        book.INSUFFICIENT_EVIDENCE,
    }:
        _fail("SYMBOL_BOOK_PROCESS_STATE_INVALID")
    return state


def _assert_horizon_evidence_consistency(
    binding: Mapping[str, Any],
    evidence: book.HistoricalBookCellEvidence,
) -> None:
    plan_status = binding["plan_status"]
    state = _historical_evidence_state(evidence)
    if plan_status == "HISTORICAL_TECHNICAL_INCOMPLETE":
        _fail("HORIZON_TECHNICAL_INCOMPLETE_BLOCKS_ANCHOR")
    if plan_status == "HISTORICAL_SOURCE_UNAVAILABLE":
        if state != book.TERMINAL_SOURCE_ABSENCE:
            _fail("HORIZON_SOURCE_UNAVAILABLE_EVIDENCE_MISMATCH")
        return
    if plan_status == "HISTORICAL_TERMINAL_TECHNICAL":
        if state != book.TERMINAL_TECHNICAL:
            _fail("HORIZON_TERMINAL_TECHNICAL_EVIDENCE_MISMATCH")
        return
    if plan_status == "HISTORICAL_REGIME_UNOBSERVED":
        if state != book.REGIME_UNOBSERVED:
            _fail("HORIZON_REGIME_UNOBSERVED_EVIDENCE_MISMATCH")
        return
    if plan_status in {
        "READY_FOR_HISTORICAL_EVIDENCE",
        "HISTORICAL_BOUNDARY_LIMITED",
    }:
        if state not in {"WINNER_EVIDENCE", book.INSUFFICIENT_EVIDENCE}:
            _fail("HORIZON_SEARCHABLE_EVIDENCE_MISMATCH")
        if (
            plan_status == "HISTORICAL_BOUNDARY_LIMITED"
            and state != book.INSUFFICIENT_EVIDENCE
        ):
            _fail("HORIZON_BOUNDARY_EVIDENCE_MISMATCH")
        if state == book.INSUFFICIENT_EVIDENCE:
            state_body = evidence.process_state_decision.to_dict()
            insufficient = state_body.get("insufficient_evidence_binding")
            if type(insufficient) is not dict or any(
                insufficient.get(evidence_name) != binding.get(binding_name)
                for evidence_name, binding_name in (
                    ("selection_cutoff_utc", "selection_cutoff_utc"),
                    (
                        "source_prefix_receipt_sha256",
                        "source_prefix_receipt_sha256",
                    ),
                    ("horizon_plan_receipt_sha256", "plan_receipt_sha256"),
                    ("candidate_registry_sha256", "candidate_registry_sha256"),
                    ("trial_registry_sha256", "trial_registry_sha256"),
                    (
                        "horizon_attempt_roster_sha256",
                        "attempt_roster_sha256",
                    ),
                    ("evidence_roster_sha256", "evidence_roster_sha256"),
                )
            ):
                _fail("HORIZON_INSUFFICIENT_EVIDENCE_BINDING_MISMATCH")
            attempted = state_body.get("attempted_trials_exact")
            if plan_status == "HISTORICAL_BOUNDARY_LIMITED":
                if (
                    insufficient.get("terminal_stage")
                    != book.INSUFFICIENT_PRESEARCH_STAGE
                    or insufficient.get("reason_code")
                    != book.INSUFFICIENT_PRESEARCH_BOUNDARY_REASON
                    or attempted != 0
                    or insufficient.get("process_rejected_attempt_count") != 0
                ):
                    _fail("HORIZON_BOUNDARY_INSUFFICIENT_EVIDENCE_MISMATCH")
            elif (
                insufficient.get("terminal_stage") != book.INSUFFICIENT_POSTSEARCH_STAGE
                or insufficient.get("reason_code")
                != book.INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON
                or type(attempted) is not int
                or attempted <= 0
                or insufficient.get("planned_attempt_count") != attempted
                or insufficient.get("process_rejected_attempt_count") != attempted
            ):
                _fail("HORIZON_POSTSEARCH_INSUFFICIENT_EVIDENCE_MISMATCH")
        return
    _fail("HORIZON_BINDING_PLAN_STATUS_INVALID")


def _read_canonical_descriptor_json(
    context: authority_spool.GenerationSpoolContext,
    descriptor_value: Any,
    *,
    maximum_bytes: int,
) -> dict[str, Any]:
    """Reopen an already manifest-verified canonical artifact exactly."""

    descriptor = _descriptor(descriptor_value)
    if (
        descriptor["format"] != authority_spool.CANONICAL_JSON_FORMAT
        or descriptor["size_bytes"] > maximum_bytes
    ):
        _fail("VERIFIED_ARTIFACT_FORMAT_OR_BOUND_INVALID")
    root = Path(context.root).resolve(strict=True)
    path = root.joinpath(*Path(descriptor["relative_path"]).parts).resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise HistoricalAnchorOrchestrationError(
            "VERIFIED_ARTIFACT_PATH_ESCAPE"
        ) from exc
    raw = path.read_bytes()
    if (
        len(raw) != descriptor["size_bytes"]
        or len(raw) > maximum_bytes
        or hashlib.sha256(raw).hexdigest() != descriptor["sha256"]
    ):
        _fail("VERIFIED_ARTIFACT_INTEGRITY_MISMATCH")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise HistoricalAnchorOrchestrationError(
            "VERIFIED_ARTIFACT_JSON_INVALID"
        ) from exc
    if type(value) is not dict or _canonical_bytes(value, maximum_bytes) != raw:
        _fail("VERIFIED_ARTIFACT_CANONICAL_MISMATCH")
    return value


def _read_ascii_descriptor_json(
    context: authority_spool.GenerationSpoolContext,
    descriptor_value: Any,
    *,
    maximum_bytes: int,
) -> dict[str, Any]:
    """Reopen the adapter's intentionally ASCII-canonical Winner binding."""

    descriptor = _descriptor(descriptor_value)
    if (
        descriptor["format"] != _ADAPTER_ASCII_JSON_FORMAT
        or descriptor["size_bytes"] > maximum_bytes
    ):
        _fail("VERIFIED_ASCII_ARTIFACT_FORMAT_OR_BOUND_INVALID")
    root = Path(context.root).resolve(strict=True)
    path = root.joinpath(*Path(descriptor["relative_path"]).parts).resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise HistoricalAnchorOrchestrationError(
            "VERIFIED_ASCII_ARTIFACT_PATH_ESCAPE"
        ) from exc
    raw = path.read_bytes()
    if (
        len(raw) != descriptor["size_bytes"]
        or len(raw) > maximum_bytes
        or hashlib.sha256(raw).hexdigest() != descriptor["sha256"]
    ):
        _fail("VERIFIED_ASCII_ARTIFACT_INTEGRITY_MISMATCH")
    try:
        value = json.loads(raw.decode("ascii"))
        canonical = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise HistoricalAnchorOrchestrationError(
            "VERIFIED_ASCII_ARTIFACT_JSON_INVALID"
        ) from exc
    if type(value) is not dict or canonical != raw:
        _fail("VERIFIED_ASCII_ARTIFACT_CANONICAL_MISMATCH")
    return value


def _reopen_verified_book_decision(
    payload: Any,
) -> book.BookMultiplicityDecision:
    """Rehydrate persisted Book bytes and invoke Book-v14's exact authority."""

    if type(payload) is not dict or "receipt_sha256" not in payload:
        _fail("BOOK_DECISION_PAYLOAD_INVALID")
    receipt_sha256 = _hex(
        payload.get("receipt_sha256"), "BOOK_DECISION_RECEIPT_INVALID"
    )
    body = dict(payload)
    body.pop("receipt_sha256")
    try:
        encoded = json.dumps(
            body,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise HistoricalAnchorOrchestrationError(
            "BOOK_DECISION_CANONICAL_INVALID"
        ) from exc
    if hashlib.sha256(encoded).hexdigest() != receipt_sha256:
        _fail("BOOK_DECISION_RECEIPT_MISMATCH")
    decision = book.BookMultiplicityDecision(encoded, receipt_sha256)
    if (
        not book.verify_historical_book_multiplicity_decision(decision)
        or decision.to_dict() != payload
    ):
        _fail("BOOK_DECISION_SEMANTICS_INVALID")
    return decision


def _manifest_entry(value: Any, *, code: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != {
        "artifact_id",
        "artifact_type",
        "descriptor",
    }:
        _fail(code)
    artifact_id = value.get("artifact_id")
    artifact_type = value.get("artifact_type")
    if (
        type(artifact_id) is not str
        or not artifact_id
        or artifact_id != artifact_id.strip()
        or type(artifact_type) is not str
        or not artifact_type
        or artifact_type != artifact_type.strip()
    ):
        _fail(code)
    _descriptor(value.get("descriptor"))
    return value


def _verify_graph_roster(
    body: Mapping[str, Any],
    *,
    expected_fields: frozenset[str],
    code: str,
) -> list[dict[str, Any]]:
    if type(body) is not dict or set(body) != expected_fields:
        _fail(code)
    raw_entries = body.get("artifacts")
    if type(raw_entries) is not list:
        _fail(code)
    entries: list[dict[str, Any]] = []
    for value in raw_entries:
        entries.append(_manifest_entry(value, code=code))
    ids = [entry["artifact_id"] for entry in entries]
    if (
        entries != sorted(entries, key=lambda item: item["artifact_id"])
        or len(ids) != len(set(ids))
        or body.get("artifact_count") != len(entries)
        or body.get("artifact_total_bytes")
        != sum(entry["descriptor"]["size_bytes"] for entry in entries)
        or body.get("artifact_roster_sha256") != _canonical_sha256(entries)
        or body.get("complete_for_phase") is not True
        or body.get("confers_causal_current_live_or_capital_authority") is not False
    ):
        _fail(code)
    return entries


def _verify_adapter_manifest_graph(
    context: authority_spool.GenerationSpoolContext,
    artifacts: Mapping[str, Mapping[str, Any]],
    payload_value: Any,
    *,
    precommit_body: Mapping[str, Any],
    anchor: Mapping[str, Any],
    symbol: str,
    producer_request_sha256: str,
    horizon_bindings: Sequence[Mapping[str, Any]],
) -> set[str]:
    """Reopen every adapter graph edge and reject unreferenced raw artifacts."""

    if type(payload_value) is not dict:
        _fail("ADAPTER_OPTIMIZER_PAYLOAD_REQUIRED")
    if payload_value.get("schema_version") != _ADAPTER_PAYLOAD_SCHEMA:
        _fail("ADAPTER_OPTIMIZER_PAYLOAD_SCHEMA_INVALID")
    payload = payload_value
    if set(payload) != _ADAPTER_PAYLOAD_FIELDS:
        _fail("ADAPTER_OPTIMIZER_PAYLOAD_FIELDS_INVALID")
    anchor_id = anchor["anchor_id"]
    run_id = precommit_body["run_id"]
    namespace = f"historical-anchor/{run_id}/{anchor_id}"
    release_sha256 = precommit_body["optimizer_producer_release_sha256"]
    root_id = f"{anchor_id}/adapter/raw-winner-descriptor-graph"
    index_id = f"{anchor_id}/adapter/production-index"
    root_entry = _manifest_entry(
        artifacts.get(root_id), code="ADAPTER_GRAPH_ROOT_ENTRY_INVALID"
    )
    index_entry = _manifest_entry(
        artifacts.get(index_id), code="ADAPTER_PRODUCTION_INDEX_ENTRY_INVALID"
    )
    if (
        root_entry["artifact_type"] != "historical_raw_winner_descriptor_graph_root"
        or index_entry["artifact_type"] != "historical_symbol_production_index"
        or payload.get("capture_contract_version") != _ADAPTER_CAPTURE_CONTRACT
        or payload.get("producer_release_sha256") != release_sha256
        or payload.get("generation_context_sha256") != context.context_sha256
        or payload.get("producer_request_sha256") != producer_request_sha256
        or payload.get("anchor_id") != anchor_id
        or payload.get("symbol") != symbol
        or payload.get("cell_count") != _CELLS_PER_SYMBOL
        or payload.get("raw_artifact_namespace") != namespace
        or payload.get("raw_descriptor_graph") != root_entry["descriptor"]
        or payload.get("production_index") != index_entry["descriptor"]
        or payload.get("complete_sparse_state_roster") is not True
        or payload.get("posthoc_evidence_reconstruction") is not False
        or payload.get("confers_causal_current_live_or_capital_authority") is not False
    ):
        _fail("ADAPTER_OPTIMIZER_PAYLOAD_BINDING_INVALID")

    root = _read_canonical_descriptor_json(
        context,
        root_entry["descriptor"],
        maximum_bytes=context.limits.max_json_bytes,
    )
    root_entries = _verify_graph_roster(
        root,
        expected_fields=_ADAPTER_GRAPH_ROOT_FIELDS,
        code="ADAPTER_GRAPH_ROOT_INVALID",
    )
    if (
        root.get("schema_version") != _ADAPTER_GRAPH_SCHEMA
        or root.get("generation_id") != context.generation_id
        or root.get("generation_context_sha256") != context.context_sha256
        or root.get("producer_request_sha256") != producer_request_sha256
        or root.get("run_id") != run_id
        or root.get("anchor_id") != anchor_id
        or root.get("symbol") != symbol
        or root.get("phase") != "SYMBOL_ROOT"
        or root.get("artifact_namespace") != namespace
        or payload.get("raw_descriptor_graph_root_count") != len(root_entries)
        or payload.get("raw_descriptor_graph_roster_sha256")
        != _canonical_sha256(root_entries)
    ):
        _fail("ADAPTER_GRAPH_ROOT_BINDING_INVALID")

    binding_by_cell = {
        (
            binding.get("cell", {}).get("timeframe"),
            binding.get("cell", {}).get("regime"),
        ): binding
        for binding in horizon_bindings
        if type(binding) is dict and type(binding.get("cell")) is dict
    }
    if len(binding_by_cell) != _CELLS_PER_SYMBOL:
        _fail("ADAPTER_HORIZON_BINDING_ROSTER_INVALID")
    referenced = {root_id, index_id}
    cross_by_cell: dict[tuple[str, str], dict[str, Any]] = {}
    for graph_entry in root_entries:
        artifact_id = graph_entry["artifact_id"]
        if artifacts.get(artifact_id) != graph_entry or artifact_id in referenced:
            _fail("ADAPTER_GRAPH_MANIFEST_EDGE_INVALID")
        referenced.add(artifact_id)
        parts = artifact_id.split("/")
        if (
            len(parts) != 5
            or parts[:2] != [anchor_id, "adapter"]
            or parts[2] not in book.TIMEFRAME_ORDER
            or parts[3] not in book.REGIME_ORDER
        ):
            _fail("ADAPTER_GRAPH_ROOT_ID_INVALID")
        timeframe, regime, suffix = parts[2], parts[3], parts[4]
        if graph_entry["artifact_type"] == "historical_raw_winner_descriptor_graph":
            phase = {
                "frozen-descriptor-graph": "FROZEN",
                "production-descriptor-graph": "PRODUCTION",
            }.get(suffix)
            if phase is None:
                _fail("ADAPTER_PHASE_GRAPH_ID_INVALID")
            phase_body = _read_canonical_descriptor_json(
                context,
                graph_entry["descriptor"],
                maximum_bytes=context.limits.max_json_bytes,
            )
            leaves = _verify_graph_roster(
                phase_body,
                expected_fields=_ADAPTER_PHASE_GRAPH_FIELDS,
                code="ADAPTER_PHASE_GRAPH_INVALID",
            )
            prefix = f"{namespace}/winner-search/{timeframe}/{regime}/"
            if (
                phase_body.get("schema_version") != _ADAPTER_GRAPH_SCHEMA
                or phase_body.get("generation_id") != context.generation_id
                or phase_body.get("generation_context_sha256") != context.context_sha256
                or phase_body.get("producer_request_sha256") != producer_request_sha256
                or phase_body.get("run_id") != run_id
                or phase_body.get("anchor_id") != anchor_id
                or phase_body.get("symbol") != symbol
                or phase_body.get("timeframe") != timeframe
                or phase_body.get("regime") != regime
                or phase_body.get("phase") != phase
                or phase_body.get("artifact_namespace") != namespace
                or any(not leaf["artifact_id"].startswith(prefix) for leaf in leaves)
            ):
                _fail("ADAPTER_PHASE_GRAPH_BINDING_INVALID")
            for leaf in leaves:
                leaf_id = leaf["artifact_id"]
                if artifacts.get(leaf_id) != leaf or leaf_id in referenced:
                    _fail("ADAPTER_RAW_LEAF_MANIFEST_EDGE_INVALID")
                referenced.add(leaf_id)
        elif graph_entry["artifact_type"] == "historical_horizon_winner_cross_binding":
            if suffix != "horizon-winner-cross-binding":
                _fail("ADAPTER_CROSS_BINDING_ID_INVALID")
            cross = _read_ascii_descriptor_json(
                context,
                graph_entry["descriptor"],
                maximum_bytes=context.limits.max_json_bytes,
            )
            mapping = cross.get("mapping") if type(cross) is dict else None
            binding = binding_by_cell.get((timeframe, regime))
            expected_horizon_roster = (
                None
                if type(binding) is not dict
                else {
                    "plan_receipt_sha256": binding["plan_receipt_sha256"],
                    "source_prefix_receipt_sha256": binding[
                        "source_prefix_receipt_sha256"
                    ],
                    "candidate_registry_sha256": binding["candidate_registry_sha256"],
                    "trial_registry_sha256": binding["trial_registry_sha256"],
                    "attempt_roster_sha256": binding["attempt_roster_sha256"],
                    "evidence_roster_sha256": binding["evidence_roster_sha256"],
                }
            )
            if (
                type(cross) is not dict
                or set(cross) != _ADAPTER_CROSS_BINDING_FIELDS
                or cross.get("schema_version") != _ADAPTER_CROSS_BINDING_SCHEMA
                or cross.get("producer_request_sha256") != producer_request_sha256
                or cross.get("anchor_id") != anchor_id
                or cross.get("selection_cutoff_utc") != anchor["selection_cutoff_utc"]
                or cross.get("process_precommit_receipt_sha256")
                != precommit_body["process_precommit_receipt_sha256"]
                or cross.get("symbol") != symbol
                or cross.get("timeframe") != timeframe
                or cross.get("regime") != regime
                or cross.get("artifact_namespace") != namespace
                or cross.get("horizon_roster") != expected_horizon_roster
                or type(mapping) is not list
                or cross.get("mapping_count") != len(mapping)
                or cross.get("mapping_sha256") != _canonical_sha256(mapping)
                or cross.get("bijective_coverage") is not True
                or cross.get("created_before_winner_evidence_access") is not True
                or cross.get("confers_causal_current_live_or_capital_authority")
                is not False
            ):
                _fail("ADAPTER_CROSS_BINDING_INVALID")
            for name in (
                "source_group_decision_sha256",
                "regime_fit_decision_sha256",
                "native_prefix_index_sha256",
            ):
                _hex(cross.get(name), "ADAPTER_CROSS_BINDING_IDENTITY_INVALID")
            if (timeframe, regime) in cross_by_cell:
                _fail("ADAPTER_CROSS_BINDING_DUPLICATE")
            cross_by_cell[(timeframe, regime)] = {
                "body": cross,
                "descriptor": graph_entry["descriptor"],
            }
        else:
            _fail("ADAPTER_GRAPH_ROOT_TYPE_INVALID")

    index = _read_canonical_descriptor_json(
        context,
        index_entry["descriptor"],
        maximum_bytes=context.limits.max_json_bytes,
    )
    if (
        set(index) != _ADAPTER_INDEX_FIELDS
        or index.get("schema_version") != _ADAPTER_INDEX_SCHEMA
        or index.get("capture_contract_version") != _ADAPTER_CAPTURE_CONTRACT
        or index.get("producer_release_sha256") != release_sha256
        or index.get("generation_id") != context.generation_id
        or index.get("generation_context_sha256") != context.context_sha256
        or index.get("producer_request_sha256") != producer_request_sha256
        or index.get("anchor") != dict(anchor)
        or index.get("symbol") != symbol
        or index.get("horizon_bindings") != list(horizon_bindings)
        or type(index.get("source_groups")) is not list
        or len(index["source_groups"]) != len(book.TIMEFRAME_ORDER)
        or type(index.get("regime_fits")) is not list
        or len(index["regime_fits"]) != len(book.TIMEFRAME_ORDER)
        or type(index.get("cell_records")) is not list
        or len(index["cell_records"]) != _CELLS_PER_SYMBOL
        or index.get("cell_count") != _CELLS_PER_SYMBOL
        or index.get("state_counts") != payload.get("state_counts")
        or index.get("raw_artifact_namespace") != namespace
        or index.get("raw_descriptor_graph") != root_entry["descriptor"]
        or index.get("raw_descriptor_graph_root_count") != len(root_entries)
        or index.get("raw_descriptor_graph_roster_sha256")
        != _canonical_sha256(root_entries)
        or index.get("raw_vectors_spooled_before_compaction") is not True
        or index.get("posthoc_evidence_reconstruction") is not False
        or index.get("complete_sparse_state_roster") is not True
        or index.get("confers_causal_current_live_or_capital_authority") is not False
    ):
        _fail("ADAPTER_PRODUCTION_INDEX_INVALID")
    source_by_timeframe = {
        value.get("timeframe"): value
        for value in index["source_groups"]
        if type(value) is dict
    }
    fit_by_timeframe = {
        value.get("timeframe"): value
        for value in index["regime_fits"]
        if type(value) is dict
    }
    cell_records = {
        (value.get("timeframe"), value.get("regime")): value
        for value in index["cell_records"]
        if type(value) is dict
    }
    if (
        set(source_by_timeframe) != set(book.TIMEFRAME_ORDER)
        or set(fit_by_timeframe) != set(book.TIMEFRAME_ORDER)
        or len(cell_records) != _CELLS_PER_SYMBOL
    ):
        _fail("ADAPTER_PRODUCTION_INDEX_ROSTER_INVALID")
    empty_roster_sha256 = _canonical_sha256([])
    terminal_record_fields = {
        "symbol",
        "timeframe",
        "regime",
        "historical_state",
        "attempted_trials_exact",
        "process_state_receipt_sha256",
        "winner_request_sha256",
        "winner_decision_sha256",
        "insufficient_evidence_binding",
        "insufficient_horizon_plan_receipt_sha256",
        "process_rejection_request_sha256",
        "process_rejection_decision_sha256",
        "raw_artifact_namespace",
        "frozen_plan_receipt",
        "frozen_descriptor_graph",
        "horizon_winner_cross_binding",
        "raw_production_receipt",
        "production_descriptor_graph",
        "technical_failure_artifact",
    }
    terminal_null_record_fields = terminal_record_fields - {
        "symbol",
        "timeframe",
        "regime",
        "historical_state",
        "attempted_trials_exact",
        "process_state_receipt_sha256",
    }
    for timeframe in book.TIMEFRAME_ORDER:
        source = source_by_timeframe[timeframe]
        fit = fit_by_timeframe[timeframe]
        for regime in book.REGIME_ORDER:
            binding = binding_by_cell[(timeframe, regime)]
            record = cell_records[(timeframe, regime)]
            terminal_flags = (
                binding.get("plan_status") == "HISTORICAL_TERMINAL_TECHNICAL",
                source.get("availability") == book.TERMINAL_TECHNICAL,
                fit.get("fit_state") == book.REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL,
                record.get("historical_state") == book.TERMINAL_TECHNICAL,
            )
            if any(terminal_flags) and not all(terminal_flags):
                _fail("ADAPTER_TERMINAL_TECHNICAL_BIJECTION_INVALID")
            if not all(terminal_flags):
                continue
            if (
                binding.get("source_prefix_receipt_sha256") is not None
                or any(
                    binding.get(name) != empty_roster_sha256
                    for name in (
                        "candidate_registry_sha256",
                        "trial_registry_sha256",
                        "attempt_roster_sha256",
                        "evidence_roster_sha256",
                    )
                )
                or source.get("reason_code") != _TERMINAL_TECHNICAL_REASON_CODE
                or fit.get("reason_code") != _TERMINAL_TECHNICAL_REASON_CODE
                or source.get("native_source_sha256") is not None
                or source.get("acquisition_attempts_exact") != 1
                or type(source.get("activation_id")) is not str
                or not source["activation_id"]
                or record.get("symbol") != symbol
                or record.get("timeframe") != timeframe
                or record.get("regime") != regime
                or set(record) != terminal_record_fields
                or record.get("attempted_trials_exact") != 0
                or any(
                    record.get(name) is not None for name in terminal_null_record_fields
                )
            ):
                _fail("ADAPTER_TERMINAL_TECHNICAL_SEMANTICS_INVALID")
            for name in (
                "source_contract_sha256",
                "source_group_evidence_sha256",
                "activation_manifest_sha256",
                "activation_entry_sha256",
                "terminal_proof_sha256",
                "receipt_sha256",
            ):
                _hex(source.get(name), "ADAPTER_TERMINAL_TECHNICAL_SOURCE_INVALID")
            rebuilt_source = book.create_historical_source_group_decision(
                symbol=symbol,
                timeframe=timeframe,
                availability=book.TERMINAL_TECHNICAL,
                reason_code=_TERMINAL_TECHNICAL_REASON_CODE,
                source_contract_sha256=source["source_contract_sha256"],
                source_group_evidence_sha256=source["source_group_evidence_sha256"],
                acquisition_attempts_exact=1,
                activation_id=source["activation_id"],
                activation_manifest_sha256=source["activation_manifest_sha256"],
                activation_entry_sha256=source["activation_entry_sha256"],
                terminal_proof_sha256=source["terminal_proof_sha256"],
            )
            if rebuilt_source is None or rebuilt_source.to_dict() != source:
                _fail("ADAPTER_TERMINAL_TECHNICAL_SOURCE_REPLAY_INVALID")
            _hex(
                fit.get("regime_fit_policy_sha256"),
                "ADAPTER_TERMINAL_TECHNICAL_FIT_INVALID",
            )
            rebuilt_fit = book.create_regime_fit_decision(
                source_group_decision=rebuilt_source,
                fit_state=book.REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL,
                reason_code=_TERMINAL_TECHNICAL_REASON_CODE,
                regime_fit_policy_sha256=fit["regime_fit_policy_sha256"],
            )
            if rebuilt_fit is None or rebuilt_fit.to_dict() != fit:
                _fail("ADAPTER_TERMINAL_TECHNICAL_FIT_REPLAY_INVALID")
            rebuilt_process = book.create_historical_process_state_decision(
                symbol=symbol,
                timeframe=timeframe,
                regime=regime,
                state_kind=book.TERMINAL_TECHNICAL,
                reason_code=_TERMINAL_TECHNICAL_REASON_CODE,
                source_group_decision=rebuilt_source,
                regime_fit_decision=rebuilt_fit,
                attempted_trials_exact=0,
            )
            if rebuilt_process is None or rebuilt_process.receipt_sha256 != record.get(
                "process_state_receipt_sha256"
            ):
                _fail("ADAPTER_TERMINAL_TECHNICAL_PROCESS_REPLAY_INVALID")
    for (timeframe, regime), observed in cross_by_cell.items():
        cross = observed["body"]
        record = cell_records.get((timeframe, regime))
        if (
            cross["source_group_decision_sha256"]
            != source_by_timeframe[timeframe].get("receipt_sha256")
            or cross["regime_fit_decision_sha256"]
            != fit_by_timeframe[timeframe].get("receipt_sha256")
            or type(record) is not dict
            or record.get("horizon_winner_cross_binding") != observed["descriptor"]
        ):
            _fail("ADAPTER_CROSS_BINDING_INDEX_SPLICE")
    return referenced


def _anchor_book(
    *,
    sinks: Mapping[str, authority_spool.SymbolAuthoritySink],
    precommit_body: Mapping[str, Any],
    anchor: Mapping[str, Any],
    request: AnchorProducerRequest,
    producer: Callable[[AnchorProducerRequest], Iterable[AnchorSymbolProduct]],
) -> tuple[book.BookMultiplicityDecision, list[dict[str, Any]]]:
    try:
        products = _iterable(producer(request), "ANCHOR_PRODUCER_OUTPUT_INVALID")
    except HistoricalAnchorOrchestrationError:
        raise
    except Exception as exc:
        raise HistoricalAnchorOrchestrationError("ANCHOR_PRODUCER_FAILED") from exc

    horizon_hasher = _CanonicalListHasher()
    output_entries: list[dict[str, Any]] = []
    stream_failure: list[Exception] = []

    def evidence_stream() -> Iterable[book.HistoricalBookCellEvidence]:
        try:
            for symbol in book.PINNED_SYMBOL_ORDER:
                try:
                    product = next(products)
                except StopIteration as exc:
                    raise HistoricalAnchorOrchestrationError(
                        "ANCHOR_SYMBOL_ROSTER_INCOMPLETE"
                    ) from exc
                if type(product) is not AnchorSymbolProduct or product.symbol != symbol:
                    _fail("ANCHOR_SYMBOL_ROSTER_MISMATCH")
                plans_iterator = _iterable(
                    product.horizon_plans, "SYMBOL_HORIZON_PLANS_INVALID"
                )
                evidence_iterator = _iterable(
                    product.book_evidence, "SYMBOL_BOOK_EVIDENCE_INVALID"
                )
                symbol_bindings: list[dict[str, Any]] = []
                symbol_hasher = _CanonicalListHasher()
                expected_cells = tuple(
                    (symbol, timeframe, regime)
                    for timeframe in book.TIMEFRAME_ORDER
                    for regime in book.REGIME_ORDER
                )
                for expected_cell in expected_cells:
                    try:
                        horizon_plan = next(plans_iterator)
                    except StopIteration as exc:
                        raise HistoricalAnchorOrchestrationError(
                            "SYMBOL_HORIZON_PLANS_INCOMPLETE"
                        ) from exc
                    derived_binding = historical_horizon_plan_binding(horizon_plan)
                    binding = _normalize_binding(
                        derived_binding,
                        expected_cell=expected_cell,
                        anchor=anchor,
                        process_precommit_receipt_sha256=precommit_body[
                            "process_precommit_receipt_sha256"
                        ],
                    )
                    horizon_hasher.add(binding)
                    symbol_hasher.add(binding)
                    symbol_bindings.append(binding)
                try:
                    next(plans_iterator)
                except StopIteration:
                    pass
                else:
                    _fail("SYMBOL_HORIZON_PLANS_OVERSIZED")
                _, symbol_horizon_sha = symbol_hasher.finish()
                artifact = {
                    "schema_version": SYMBOL_OUTPUT_SCHEMA,
                    "authority_scope": AUTHORITY_SCOPE,
                    "producer_request_sha256": request.receipt_sha256,
                    "anchor_id": anchor["anchor_id"],
                    "symbol": symbol,
                    "horizon_binding_count": _CELLS_PER_SYMBOL,
                    "horizon_binding_roster_sha256": symbol_horizon_sha,
                    "horizon_bindings": symbol_bindings,
                    "optimizer_payload": product.optimizer_payload,
                    "confers_causal_or_current_authority": False,
                }
                entry = sinks[symbol].put_json(
                    f"{anchor['anchor_id']}/optimizer-output",
                    "historical_anchor_optimizer_output",
                    artifact,
                )
                output_entries.append(
                    {
                        "symbol": symbol,
                        "artifact_sha256": entry["descriptor"]["sha256"],
                        "artifact_size_bytes": entry["descriptor"]["size_bytes"],
                        "horizon_binding_roster_sha256": symbol_horizon_sha,
                    }
                )
                for cell_index, expected_cell in enumerate(expected_cells):
                    try:
                        evidence = next(evidence_iterator)
                    except StopIteration as exc:
                        raise HistoricalAnchorOrchestrationError(
                            "SYMBOL_BOOK_EVIDENCE_INCOMPLETE"
                        ) from exc
                    if (
                        type(evidence) is not book.HistoricalBookCellEvidence
                        or (
                            evidence.symbol,
                            evidence.timeframe,
                            evidence.regime,
                        )
                        != expected_cell
                    ):
                        _fail("SYMBOL_BOOK_EVIDENCE_ROSTER_MISMATCH")
                    _assert_horizon_evidence_consistency(
                        symbol_bindings[cell_index], evidence
                    )
                    yield evidence
                try:
                    next(evidence_iterator)
                except StopIteration:
                    pass
                else:
                    _fail("SYMBOL_BOOK_EVIDENCE_OVERSIZED")
            try:
                next(products)
            except StopIteration:
                return
            _fail("ANCHOR_SYMBOL_ROSTER_OVERSIZED")
        # GeneratorExit is control flow from an early-closing consumer, not an
        # evidence-production failure.  Recording it here previously replaced
        # the consumer's real fail-closed diagnosis with the generic
        # ANCHOR_EVIDENCE_STREAM_FAILED wrapper.
        except Exception as exc:
            stream_failure.append(exc)
            raise

    decision = book.evaluate_historical_book_multiplicity(evidence_stream())
    if stream_failure:
        failure = stream_failure[0]
        if isinstance(failure, HistoricalAnchorOrchestrationError):
            raise failure
        raise HistoricalAnchorOrchestrationError(
            "ANCHOR_EVIDENCE_STREAM_FAILED"
        ) from failure
    binding_count, binding_sha = horizon_hasher.finish()
    if binding_count != book.FAMILY_SIZE:
        _fail("ANCHOR_HORIZON_BINDING_ROSTER_INCOMPLETE")
    if binding_sha != anchor["horizon_binding_roster_sha256"]:
        _fail("ANCHOR_HORIZON_PRECOMMIT_MISMATCH")
    if not book.verify_historical_book_multiplicity_decision(decision):
        _fail("ANCHOR_BOOK_DECISION_INVALID")
    body = decision.to_dict()
    if (
        body.get("valid") is not True
        or body.get("optimization_run_complete") is not True
        or body.get("source_group_size") != book.SOURCE_GROUP_SIZE
        or body.get("family_size") != book.FAMILY_SIZE
    ):
        _fail("ANCHOR_BOOK_INCOMPLETE")
    return decision, output_entries


def execute_historical_anchor_run(
    context: authority_spool.GenerationSpoolContext,
    precommit: HistoricalAnchorPrecommit,
    producers: Mapping[
        str,
        Callable[[AnchorProducerRequest], Iterable[AnchorSymbolProduct]],
    ],
) -> HistoricalAnchorRunReceipt:
    """Run exact anchor callbacks and commit only complete historical Books."""

    precommit_body = _valid_precommit(context, precommit)
    anchors = precommit_body["anchors"]
    expected_ids = [anchor["anchor_id"] for anchor in anchors]
    if type(producers) is not dict or set(producers) != set(expected_ids):
        _fail("EXACT_PER_ANCHOR_PRODUCER_CALLBACKS_REQUIRED")
    if any(not callable(producers[identifier]) for identifier in expected_ids):
        _fail("ANCHOR_PRODUCER_NOT_CALLABLE")
    required_artifacts = 1 + 4 * len(anchors)
    if context.limits.max_artifacts_per_symbol < required_artifacts:
        _fail("SPOOL_ARTIFACT_COUNT_BOUND_TOO_SMALL")

    sinks = {
        symbol: authority_spool.open_symbol_sink(
            context,
            symbol,
            manifest_domain=authority_spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN,
        )
        for symbol in book.PINNED_SYMBOL_ORDER
    }
    precommit_descriptor = precommit.artifact_descriptor
    for sink in sinks.values():
        sink.register_artifact(
            "run/precommit",
            "historical_anchor_precommit",
            precommit_descriptor,
        )

    anchor_results: list[dict[str, Any]] = []
    book_receipts: set[str] = set()
    for anchor in anchors:
        request = _producer_request(precommit, precommit_body, anchor, sinks)
        decision, outputs = _anchor_book(
            sinks=sinks,
            precommit_body=precommit_body,
            anchor=anchor,
            request=request,
            producer=producers[anchor["anchor_id"]],
        )
        if decision.receipt_sha256 in book_receipts:
            _fail("ANCHOR_BOOK_DECISION_REUSED")
        book_receipts.add(decision.receipt_sha256)
        decision_body = decision.to_dict()
        decision_descriptor = authority_spool.write_canonical_json_artifact(
            context, decision_body
        )
        for sink in sinks.values():
            sink.register_artifact(
                f"{anchor['anchor_id']}/book-decision",
                _BOOK_ARTIFACT_TYPE,
                decision_descriptor,
            )
        anchor_results.append(
            {
                "anchor_id": anchor["anchor_id"],
                "producer_request_sha256": request.receipt_sha256,
                "horizon_binding_roster_sha256": anchor[
                    "horizon_binding_roster_sha256"
                ],
                "symbol_output_roster_sha256": _canonical_sha256(outputs),
                "book_decision_sha256": decision.receipt_sha256,
                "book_generation_sha256": decision.book_generation_sha256,
                "book_status": decision.status,
                "historical_state_counts": decision_body["counts"],
                "source_group_count": decision_body["source_group_size"],
                "accounting_slot_count": decision_body["family_size"],
                "eligible_for_downstream_causal_evaluation": True,
                "causal_process_validated": False,
                "current_refit_authorized": False,
            }
        )

    symbol_receipts: list[dict[str, Any]] = []
    for symbol in book.PINNED_SYMBOL_ORDER:
        symbol_receipts.append(sinks[symbol].seal())
    body = {
        "schema_version": RUN_RECEIPT_SCHEMA,
        "authority_scope": AUTHORITY_SCOPE,
        "run_id": precommit_body["run_id"],
        "generation_id": context.generation_id,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "generation_context_sha256": context.context_sha256,
        "precommit_receipt_sha256": precommit.receipt_sha256,
        "anchor_count": len(anchor_results),
        "anchors": anchor_results,
        "symbol_count": len(symbol_receipts),
        "symbol_receipts": symbol_receipts,
        "complete_sparse_state_rosters": True,
        "possible_regime_slots_are_forced_observations": False,
        "eligible_for_downstream_causal_evaluation": True,
        "causal_process_validated": False,
        "current_refit_authorized": False,
        "live_authorized": False,
        "capital_authorized": False,
        "profitability_claimed": False,
    }
    encoded = _canonical_bytes(body, _MAX_RECEIPT_BYTES)
    descriptor = authority_spool.write_canonical_json_artifact(context, body)
    if descriptor["sha256"] != hashlib.sha256(encoded).hexdigest():
        _fail("RUN_RECEIPT_SPOOL_MISMATCH")
    candidate = HistoricalAnchorRunReceipt(
        encoded,
        _canonical_bytes(descriptor, 64 * 1024),
    )
    if not verify_historical_anchor_run_receipt(context, precommit, candidate):
        _fail("RUN_RECEIPT_SELF_VERIFICATION_FAILED")
    return candidate


def verify_historical_anchor_run_receipt(
    context: authority_spool.GenerationSpoolContext,
    precommit: HistoricalAnchorPrecommit,
    receipt: HistoricalAnchorRunReceipt,
) -> bool:
    """Reopen every symbol manifest and verify a completed run receipt."""

    try:
        precommit_body = _valid_precommit(context, precommit)
        if type(receipt) is not HistoricalAnchorRunReceipt:
            return False
        body = receipt.to_dict()
        if _canonical_bytes(body, _MAX_RECEIPT_BYTES) != receipt._body_json:
            return False
        if (
            set(body) != _RUN_FIELDS
            or body.get("schema_version") != RUN_RECEIPT_SCHEMA
            or body.get("authority_scope") != AUTHORITY_SCOPE
            or body.get("run_id") != precommit_body["run_id"]
            or body.get("precommit_receipt_sha256") != precommit.receipt_sha256
            or body.get("generation_id") != context.generation_id
            or body.get("native_universe_sha256") != context.native_universe_sha256
            or body.get("config_sha256") != context.config_sha256
            or body.get("generation_context_sha256") != context.context_sha256
            or body.get("anchor_count") != len(precommit_body["anchors"])
            or body.get("symbol_count")
            != book.SOURCE_GROUP_SIZE // len(book.TIMEFRAME_ORDER)
            or body.get("complete_sparse_state_rosters") is not True
            or body.get("possible_regime_slots_are_forced_observations") is not False
            or body.get("eligible_for_downstream_causal_evaluation") is not True
            or body.get("causal_process_validated") is not False
            or body.get("current_refit_authorized") is not False
            or body.get("live_authorized") is not False
            or body.get("capital_authorized") is not False
            or body.get("profitability_claimed") is not False
        ):
            return False
        anchor_results = body.get("anchors")
        if type(anchor_results) is not list or len(anchor_results) != len(
            precommit_body["anchors"]
        ):
            return False
        results_by_anchor: dict[str, dict[str, Any]] = {}
        planned_by_anchor = {
            item["anchor_id"]: item for item in precommit_body["anchors"]
        }
        book_receipts: set[str] = set()
        for result, planned in zip(
            anchor_results, precommit_body["anchors"], strict=True
        ):
            if type(result) is not dict or set(result) != _ANCHOR_RESULT_FIELDS:
                return False
            counts = result.get("historical_state_counts")
            expected_request = _producer_request(precommit, precommit_body, planned)
            if (
                result.get("anchor_id") != planned["anchor_id"]
                or result.get("producer_request_sha256")
                != expected_request.receipt_sha256
                or result.get("horizon_binding_roster_sha256")
                != planned["horizon_binding_roster_sha256"]
                or result.get("book_status")
                not in {"HISTORICAL_LOCAL_CANDIDATES_AVAILABLE", "NO_LOCAL_WINNERS"}
                or result.get("source_group_count") != book.SOURCE_GROUP_SIZE
                or result.get("accounting_slot_count") != book.FAMILY_SIZE
                or result.get("eligible_for_downstream_causal_evaluation") is not True
                or result.get("causal_process_validated") is not False
                or result.get("current_refit_authorized") is not False
                or type(counts) is not dict
                or set(counts)
                != {
                    "WINNER",
                    "NO_WINNER",
                    "SOURCE_UNAVAILABLE",
                    book.TERMINAL_TECHNICAL,
                    book.TECHNICAL_FAILURE,
                    book.UNSEARCHED,
                    book.REGIME_UNOBSERVED,
                    book.INSUFFICIENT_EVIDENCE,
                }
                or any(type(value) is not int or value < 0 for value in counts.values())
                or sum(counts.values()) != book.FAMILY_SIZE
                or counts[book.TECHNICAL_FAILURE] != 0
                or counts[book.UNSEARCHED] != 0
            ):
                return False
            for name in (
                "producer_request_sha256",
                "horizon_binding_roster_sha256",
                "symbol_output_roster_sha256",
                "book_decision_sha256",
                "book_generation_sha256",
            ):
                _hex(result.get(name), f"RUN_ANCHOR_{name.upper()}_INVALID")
            if result["book_decision_sha256"] in book_receipts:
                return False
            book_receipts.add(result["book_decision_sha256"])
            results_by_anchor[planned["anchor_id"]] = result
        symbol_receipts = body.get("symbol_receipts")
        if type(symbol_receipts) is not list or len(symbol_receipts) != len(
            book.PINNED_SYMBOL_ORDER
        ):
            return False
        expected_ids = {"run/precommit"}
        for anchor in precommit_body["anchors"]:
            expected_ids.add(f"{anchor['anchor_id']}/optimizer-output")
            expected_ids.add(f"{anchor['anchor_id']}/book-decision")
        output_rosters: dict[str, list[dict[str, Any]]] = {
            identifier: [] for identifier in results_by_anchor
        }
        horizon_hashers = {
            identifier: _CanonicalListHasher() for identifier in results_by_anchor
        }
        terminal_binding_cells_by_anchor: dict[str, set[tuple[str, str, str]]] = {
            identifier: set() for identifier in results_by_anchor
        }
        book_artifacts: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        precommit_descriptor = _descriptor(precommit.artifact_descriptor)
        for symbol, symbol_receipt in zip(
            book.PINNED_SYMBOL_ORDER, symbol_receipts, strict=True
        ):
            manifest = authority_spool.verify_symbol_receipt(
                context.root,
                symbol_receipt,
                expected_generation_id=context.generation_id,
                expected_symbol=symbol,
                expected_native_universe_sha256=context.native_universe_sha256,
                expected_config_sha256=context.config_sha256,
                expected_manifest_domain=(
                    authority_spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN
                ),
            )
            artifacts = {item["artifact_id"]: item for item in manifest["artifacts"]}
            if not expected_ids.issubset(artifacts):
                return False
            if (
                artifacts["run/precommit"]["artifact_type"]
                != "historical_anchor_precommit"
                or artifacts["run/precommit"].get("descriptor") != precommit_descriptor
            ):
                return False
            adapter_artifact_ids: set[str] = set()
            adapter_output_count = 0
            for anchor in precommit_body["anchors"]:
                anchor_id = anchor["anchor_id"]
                output_entry = artifacts[f"{anchor_id}/optimizer-output"]
                book_entry = artifacts[f"{anchor_id}/book-decision"]
                if (
                    output_entry["artifact_type"]
                    != "historical_anchor_optimizer_output"
                    or book_entry["artifact_type"] != _BOOK_ARTIFACT_TYPE
                ):
                    return False
                output = _read_canonical_descriptor_json(
                    context,
                    output_entry.get("descriptor"),
                    maximum_bytes=context.limits.max_json_bytes,
                )
                if (
                    set(output) != _SYMBOL_OUTPUT_FIELDS
                    or output.get("schema_version") != SYMBOL_OUTPUT_SCHEMA
                    or output.get("authority_scope") != AUTHORITY_SCOPE
                    or output.get("producer_request_sha256")
                    != results_by_anchor[anchor_id]["producer_request_sha256"]
                    or output.get("anchor_id") != anchor_id
                    or output.get("symbol") != symbol
                    or output.get("horizon_binding_count") != _CELLS_PER_SYMBOL
                    or output.get("confers_causal_or_current_authority") is not False
                ):
                    return False
                raw_bindings = output.get("horizon_bindings")
                if (
                    type(raw_bindings) is not list
                    or len(raw_bindings) != _CELLS_PER_SYMBOL
                ):
                    return False
                symbol_hasher = _CanonicalListHasher()
                for cell_index, raw_binding in enumerate(raw_bindings):
                    timeframe_index, regime_index = divmod(
                        cell_index, len(book.REGIME_ORDER)
                    )
                    expected_cell = (
                        symbol,
                        book.TIMEFRAME_ORDER[timeframe_index],
                        book.REGIME_ORDER[regime_index],
                    )
                    binding = _normalize_binding(
                        raw_binding,
                        expected_cell=expected_cell,
                        anchor=anchor,
                        process_precommit_receipt_sha256=precommit_body[
                            "process_precommit_receipt_sha256"
                        ],
                    )
                    if binding["plan_status"] == "HISTORICAL_TERMINAL_TECHNICAL":
                        terminal_binding_cells_by_anchor[anchor_id].add(expected_cell)
                    symbol_hasher.add(binding)
                    horizon_hashers[anchor_id].add(binding)
                _, symbol_horizon_sha = symbol_hasher.finish()
                if output.get("horizon_binding_roster_sha256") != symbol_horizon_sha:
                    return False
                adapter_ids = _verify_adapter_manifest_graph(
                    context,
                    artifacts,
                    output.get("optimizer_payload"),
                    precommit_body=precommit_body,
                    anchor=anchor,
                    symbol=symbol,
                    producer_request_sha256=results_by_anchor[anchor_id][
                        "producer_request_sha256"
                    ],
                    horizon_bindings=raw_bindings,
                )
                if not adapter_ids or adapter_artifact_ids.intersection(adapter_ids):
                    return False
                adapter_artifact_ids.update(adapter_ids)
                adapter_output_count += 1
                output_descriptor = _descriptor(output_entry.get("descriptor"))
                output_rosters[anchor_id].append(
                    {
                        "symbol": symbol,
                        "artifact_sha256": output_descriptor["sha256"],
                        "artifact_size_bytes": output_descriptor["size_bytes"],
                        "horizon_binding_roster_sha256": symbol_horizon_sha,
                    }
                )
                decision_payload = _read_canonical_descriptor_json(
                    context,
                    book_entry.get("descriptor"),
                    maximum_bytes=context.limits.max_json_bytes,
                )
                observed_book = book_artifacts.get(anchor_id)
                if observed_book is None:
                    book_artifacts[anchor_id] = (
                        _descriptor(book_entry.get("descriptor")),
                        decision_payload,
                    )
                elif observed_book != (
                    _descriptor(book_entry.get("descriptor")),
                    decision_payload,
                ):
                    return False
            if adapter_output_count != len(precommit_body["anchors"]):
                return False
            if set(artifacts) != expected_ids | adapter_artifact_ids:
                return False
        for anchor_id, result in results_by_anchor.items():
            planned = planned_by_anchor[anchor_id]
            binding_count, binding_sha = horizon_hashers[anchor_id].finish()
            if (
                binding_count != book.FAMILY_SIZE
                or binding_sha != planned["horizon_binding_roster_sha256"]
                or _canonical_sha256(output_rosters[anchor_id])
                != result["symbol_output_roster_sha256"]
            ):
                return False
            _, decision_payload = book_artifacts[anchor_id]
            decision = _reopen_verified_book_decision(decision_payload)
            terminal_book_cells = {
                (cell.get("symbol"), cell.get("timeframe"), cell.get("regime"))
                for cell in decision_payload.get("cells", [])
                if type(cell) is dict
                and cell.get("historical_state") == book.TERMINAL_TECHNICAL
            }
            terminal_book_groups = {(cell[0], cell[1]) for cell in terminal_book_cells}
            terminal_source_groups = {
                (
                    item.get("payload", {}).get("symbol"),
                    item.get("payload", {}).get("timeframe"),
                )
                for item in decision_payload.get("source_groups", [])
                if type(item) is dict
                and type(item.get("payload")) is dict
                and item["payload"].get("availability") == book.TERMINAL_TECHNICAL
                and item["payload"].get("reason_code")
                == _TERMINAL_TECHNICAL_REASON_CODE
            }
            terminal_fit_groups = {
                (
                    item.get("payload", {}).get("symbol"),
                    item.get("payload", {}).get("timeframe"),
                )
                for item in decision_payload.get("regime_fits", [])
                if type(item) is dict
                and type(item.get("payload")) is dict
                and item["payload"].get("fit_state")
                == book.REGIME_NOT_APPLICABLE_TECHNICAL_TERMINAL
                and item["payload"].get("reason_code")
                == _TERMINAL_TECHNICAL_REASON_CODE
            }
            if (
                decision.receipt_sha256 != result["book_decision_sha256"]
                or decision_payload.get("contract_version")
                != book.BOOK_MULTIPLICITY_CONTRACT_VERSION
                or decision_payload.get("valid") is not True
                or decision_payload.get("optimization_run_complete") is not True
                or decision_payload.get("status") != result["book_status"]
                or decision_payload.get("book_generation_sha256")
                != result["book_generation_sha256"]
                or decision_payload.get("counts") != result["historical_state_counts"]
                or decision_payload.get("source_group_size")
                != result["source_group_count"]
                or decision_payload.get("family_size")
                != result["accounting_slot_count"]
                or terminal_book_cells != terminal_binding_cells_by_anchor[anchor_id]
                or terminal_source_groups != terminal_book_groups
                or terminal_fit_groups != terminal_book_groups
                or any(
                    cell.get("state_reason") != _TERMINAL_TECHNICAL_REASON_CODE
                    for cell in decision_payload.get("cells", [])
                    if type(cell) is dict
                    and cell.get("historical_state") == book.TERMINAL_TECHNICAL
                )
            ):
                return False
        descriptor = _descriptor(receipt.artifact_descriptor)
        return bool(
            descriptor["sha256"] == receipt.receipt_sha256
            and descriptor["size_bytes"] == len(receipt._body_json)
            and descriptor["format"] == authority_spool.CANONICAL_JSON_FORMAT
        )
    except Exception:
        return False


__all__ = [
    "AUTHORITY_SCOPE",
    "AnchorProducerRequest",
    "AnchorSymbolProduct",
    "BOUNDED_RAW_WINNER_CANARY_REQUEST_SCHEMA",
    "BOUNDED_RAW_WINNER_CANARY_RUN_PREFIX",
    "BOUNDED_RAW_WINNER_CANARY_SCOPE",
    "BOUNDED_RAW_WINNER_CANARY_SELECTION_RULE",
    "PRODUCTION_NATIVE_AUTHORITY_VERIFICATION_SCOPE_SCHEMA",
    "BoundedRawWinnerCanaryRequest",
    "HORIZON_BINDING_SCHEMA",
    "CURRENT_HORIZON_BINDING_SCHEMA",
    "HistoricalAnchorOrchestrationError",
    "HistoricalAnchorPrecommit",
    "HistoricalAnchorRunReceipt",
    "PRECOMMIT_SCHEMA",
    "PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA",
    "PRODUCER_REQUEST_SCHEMA",
    "RUN_RECEIPT_SCHEMA",
    "SYMBOL_OUTPUT_SCHEMA",
    "execute_historical_anchor_run",
    "historical_horizon_binding_roster_sha256",
    "historical_horizon_plan_binding",
    "issue_historical_pre_evidence_planning_request",
    "consume_bounded_raw_winner_canary_request",
    "issue_bounded_raw_winner_canary_request",
    "precommit_historical_anchor_run",
    "reopen_anchor_producer_request_for_symbol",
    "verify_bounded_raw_winner_canary_request",
    "verify_historical_anchor_run_receipt",
]


# Declared R&D slot authority is separate from the issued-v1 causal request.
DECLARED_SLOT_PRECOMMIT_SCHEMA = "fxpm.declared-slot-run-precommit.v1"
DECLARED_SLOT_REQUEST_SCHEMA = "fxpm.declared-slot-producer-request.v1"
DECLARED_SLOT_PRECOMMIT_SCHEMA_V2 = "fxpm.declared-slot-run-precommit.v2"
DECLARED_SLOT_REQUEST_SCHEMA_V2 = "fxpm.declared-slot-producer-request.v2"
DECLARED_WINNER_TRANSPORT_V15 = "fxpm.winner-transport.v15"
DECLARED_WINNER_TRANSPORT_V16 = "fxpm.winner-transport.v16"
_DECLARED_PROTECTED_END_EXCLUSIVE_UTC = "2024-07-14T08:05:00Z"
_DECLARED_SLOT_SEAL = object()
_DECLARED_SOURCE_FILES = (
    "pm_historical_anchor_orchestration.py", "pm_production_search_planner.py",
    "pm_authoritative_search_execution.py", "pm_historical_production_adapter.py",
    "pm_historical_search_producer.py",
)


def _declared_source_identity() -> dict[str, str]:
    return {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
            for name in _DECLARED_SOURCE_FILES}


def _declared_slot_native_snapshot_status(context, snapshot, errors):
    """Classify the actual portfolio contract, including unavailable baselines.

    A full observation is compared before its errors are considered: an error
    in another cell cannot hide a different observed activation.  The resolver's
    four-field unavailable baseline is not an observed replacement snapshot.
    This same check is used on saved bytes during cold completion verification.
    """
    baseline = {"pointer_schema", "manifest_schema", "symbols", "timeframes"}
    full = baseline | {"activation_id", "activation_manifest_sha256", "activation_entry_count",
        "eligible_symbols", "excluded_symbols", "universe_eligibility_explicit",
        "activation_roster_sha256", "bars_sha256_by_source", "sidecar_sha256_by_source"}
    if (type(snapshot) is not dict or type(errors) is not list
            or any(type(value) is not str or not value for value in errors)):
        _fail("DECLARED_SLOT_NATIVE_SNAPSHOT_INVALID")
    if set(snapshot) == baseline:
        return "UNAVAILABLE" if errors else "INVALID"
    if set(snapshot) != full:
        return "INVALID"
    import pm_portfolio_replay as portfolio_replay
    if portfolio_replay.canonical_json_sha256(snapshot) != context.native_universe_sha256:
        return "MISMATCH"
    if (errors or snapshot["symbols"] != list(book.PINNED_SYMBOL_ORDER)
            or snapshot["timeframes"] != list(book.TIMEFRAME_ORDER)
            or type(snapshot["activation_id"]) is not str or not snapshot["activation_id"]
            or type(snapshot["activation_manifest_sha256"]) is not str
            or _HEX64.fullmatch(snapshot["activation_manifest_sha256"]) is None):
        return "INVALID"
    return "MATCH"


def _declared_slot_consumed_native_snapshot_matches(snapshot, product):
    """Join a reverified consumed product to the exact observed activation."""
    activation = product.index.get("activation")
    return (type(activation) is dict
        and activation.get("activation_id") == snapshot.get("activation_id")
        and activation.get("manifest_sha256") == snapshot.get("activation_manifest_sha256"))


@dataclass(frozen=True)
class DeclaredSlotRunPrecommit:
    _body_json: bytes
    _seal: object

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()


@dataclass(frozen=True)
class DeclaredSlotProducerRequest:
    _body_json: bytes
    _precommit: DeclaredSlotRunPrecommit
    _sink: authority_spool.SymbolAuthoritySink
    _seal: object

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()

    def symbol_sink(self, symbol: str) -> authority_spool.SymbolAuthoritySink:
        if symbol != self.to_dict()["symbol"]:
            _fail("DECLARED_SLOT_CELL_INVALID")
        return self._sink


def _declared_json(value: Any) -> bytes:
    # ASCII is the existing raw-Winner canonical byte contract.
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True,
                      separators=(",", ":")).encode("ascii")


DECLARED_SLOT_SOURCE_ACCESS_RENEWAL_SCHEMA = "fxpm.declared-slot-source-access-renewal.v1"
_DECLARED_RENEWAL_MAX_BYTES = 16 * 1024
_DECLARED_RENEWAL_AUTHORITY_REF = "E-00003232"


@dataclass(frozen=True, repr=False)
class DeclaredSlotSourceAccessRenewal:
    _body_json: bytes
    _seal: object

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()


def _declared_renewal_directory(precommit):
    return f"declared-slots/{precommit.receipt_sha256}/source-access-renewal"


def _declared_renewal_precommit(context, precommit, receipt_sha256):
    if (precommit is None) == (receipt_sha256 is None):
        _fail("DECLARED_SLOT_RENEWAL_PRECOMMIT_INPUTS_INVALID")
    if receipt_sha256 is None:
        return precommit
    digest = _hex(receipt_sha256, "DECLARED_SLOT_RENEWAL_PRECOMMIT_INVALID")
    try:
        observed, _, encoded = authority_spool._stream_file(context.root,
            f"declared-slots/{digest}/precommit.json", maximum_bytes=_MAX_PRECOMMIT_BYTES, collect=True)
        body = authority_spool._strict_json_bytes(encoded, _MAX_PRECOMMIT_BYTES)
    except (OSError, authority_spool.AuthoritySpoolError) as exc:
        raise HistoricalAnchorOrchestrationError("DECLARED_SLOT_RENEWAL_PRECOMMIT_INVALID") from exc
    if observed != digest or _declared_json(body) != encoded:
        _fail("DECLARED_SLOT_RENEWAL_PRECOMMIT_INVALID")
    result = DeclaredSlotRunPrecommit(encoded, _DECLARED_SLOT_SEAL)
    _register_declared_precommit(result, context)
    return result


def _declared_renewal_parent(context, precommit, process=None):
    if type(precommit) is not DeclaredSlotRunPrecommit or precommit._seal is not _DECLARED_SLOT_SEAL:
        _fail("DECLARED_SLOT_RENEWAL_PRECOMMIT_INVALID")
    _verify_declared_precommit_identity(precommit, context)
    parent = precommit.to_dict()
    _validate_declared_precommit_body(context, parent)
    _verify_declared_issued_bytes(context,
        f"declared-slots/{precommit.receipt_sha256}/precommit.json", precommit._body_json)
    if (parent["schema_version"] != DECLARED_SLOT_PRECOMMIT_SCHEMA_V2
            or _declared_json(parent) != precommit._body_json
            or any(parent.get(key) != getattr(context, attr) for key, attr in (
                ("generation_id", "generation_id"),
                ("generation_context_sha256", "context_sha256"),
                ("native_universe_sha256", "native_universe_sha256"),
                ("config_sha256", "config_sha256")))
            or parent["slot_roster_sha256"] != hashlib.sha256(_declared_json(parent["slot_roster"])).hexdigest()
            or any(parent[key] is not False for key in
                ("qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("DECLARED_SLOT_RENEWAL_PRECOMMIT_INVALID")
    if process is not None:
        import pm_production_search_planner as planner
        planner.verify_frozen_production_search_process(context, process)
        planner._require_current_process(process)
        if parent["search_process_receipt_sha256"] != process.receipt_sha256:
            _fail("DECLARED_SLOT_PROCESS_BINDING_MISMATCH")
    return parent


def _declared_renewal_first_commit(context, precommit, parent):
    import pm_historical_search_producer as producer

    cell, slot = parent["cells"][0], parent["slot_roster"]["slots"][0]
    item = producer._read_declared_commit(context,
        producer._declared_commit_path(precommit, cell["cell_id"], 1))
    if item is None:
        _fail("DECLARED_SLOT_RENEWAL_PREFIX_INVALID")
    body, digest, _ = item
    fields = {"schema_version", "precommit_sha256", "generation_context_sha256",
        "cell_id", "slot_id", "ordinal", "previous_commit_sha256", "completion", "artifact_roster"}
    if (type(body) is not dict or set(body) != fields
            or body["schema_version"] != producer.DECLARED_SLOT_COMMIT_SCHEMA
            or body["precommit_sha256"] != precommit.receipt_sha256
            or body["generation_context_sha256"] != context.context_sha256
            or body["cell_id"] != cell["cell_id"] or body["slot_id"] != slot["slot_id"]
            or type(body["ordinal"]) is not int or body["ordinal"] != 1
            or body["previous_commit_sha256"] is not None
            or type(body["completion"]) is not dict or type(body["artifact_roster"]) is not list):
        _fail("DECLARED_SLOT_RENEWAL_PREFIX_INVALID")
    completion = body["completion"]
    request = completion.get("request_body")
    if (completion.get("status") not in ("OWNER", "NO_TRADE") or type(request) is not dict
            or completion.get("precommit_sha256") != precommit.receipt_sha256
            or completion.get("cell_id") != cell["cell_id"] or completion.get("slot_id") != slot["slot_id"]
            or completion.get("request_sha256") != hashlib.sha256(_declared_json(request)).hexdigest()
            or request.get("precommit_sha256") != precommit.receipt_sha256
            or request.get("generation_context_sha256") != context.context_sha256
            or request.get("search_process_receipt_sha256") != parent["search_process_receipt_sha256"]
            or request.get("config_sha256") != parent["config_sha256"]
            or request.get("effective_config_sha256") != parent["effective_config_sha256"]
            or request.get("cell_id") != cell["cell_id"] or request.get("slot_id") != slot["slot_id"]
            or request.get("source_access_window_utc") != parent["source_access_window_utc"]
            or request.get("spread_profile_identity") != parent["spread_profile_identity"]
            or "source_access_renewal_sha256" in request
            or any(completion.get(key) is not False for key in
                ("qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("DECLARED_SLOT_RENEWAL_PREFIX_INVALID")
    _verify_declared_issued_bytes(context, _declared_request_path(request), _declared_json(request))
    return digest


def _validate_declared_source_access_renewal(context, precommit, parent, body, live_source):
    fields = {"schema_version", "precommit_sha256", "generation_context_sha256", "cell_id",
        "commit01_body_sha256", "old_source_access_window_utc", "source_access_window_utc",
        "original_source_identity", "renewed_source_identity", "authority_ref", "issued_at_utc",
        "qualification_authority", "capital_authority", "live_authority", "order_authority"}
    if type(body) is not dict or set(body) != fields:
        _fail("DECLARED_SLOT_RENEWAL_INVALID")
    if (body["schema_version"] != DECLARED_SLOT_SOURCE_ACCESS_RENEWAL_SCHEMA
            or parent["schema_version"] != DECLARED_SLOT_PRECOMMIT_SCHEMA_V2
            or body["precommit_sha256"] != precommit.receipt_sha256
            or body["generation_context_sha256"] != context.context_sha256
            or body["cell_id"] != parent["cells"][0]["cell_id"]
            or body["authority_ref"] != _DECLARED_RENEWAL_AUTHORITY_REF
            or body["old_source_access_window_utc"] != parent["source_access_window_utc"]
            or body["original_source_identity"] != parent["source_identity"]
            or body["renewed_source_identity"] != live_source
            or any(body[key] is not False for key in
                ("qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("DECLARED_SLOT_RENEWAL_BINDING_MISMATCH")
    for identity in (body["original_source_identity"], body["renewed_source_identity"]):
        if type(identity) is not dict or set(identity) != set(_DECLARED_SOURCE_FILES):
            _fail("DECLARED_SLOT_RENEWAL_SOURCE_INVALID")
        for digest in identity.values():
            _hex(digest, "DECLARED_SLOT_RENEWAL_SOURCE_INVALID")
    changed = [name for name in _DECLARED_SOURCE_FILES
               if body["original_source_identity"][name] != body["renewed_source_identity"][name]]
    if changed != ["pm_historical_anchor_orchestration.py"]:
        _fail("DECLARED_SLOT_RENEWAL_SOURCE_INVALID")
    window = body["source_access_window_utc"]
    if type(window) is not dict or set(window) != {"start_utc", "end_exclusive_utc"}:
        _fail("DECLARED_SLOT_RENEWAL_WINDOW_INVALID")
    start = _utc(window["start_utc"], "DECLARED_SLOT_RENEWAL_WINDOW_INVALID")
    end = _utc(window["end_exclusive_utc"], "DECLARED_SLOT_RENEWAL_WINDOW_INVALID")
    issued = _utc(body["issued_at_utc"], "DECLARED_SLOT_RENEWAL_WINDOW_INVALID")
    old_start = _utc(parent["source_access_window_utc"]["start_utc"],
        "DECLARED_SLOT_RENEWAL_WINDOW_INVALID")
    old_end = _utc(parent["source_access_window_utc"]["end_exclusive_utc"],
        "DECLARED_SLOT_RENEWAL_WINDOW_INVALID")
    if not max(old_start, issued) <= start < end or end <= max(old_end, issued):
        _fail("DECLARED_SLOT_RENEWAL_WINDOW_INVALID")
    _hex(body["commit01_body_sha256"], "DECLARED_SLOT_RENEWAL_PREFIX_INVALID")
    if body["commit01_body_sha256"] != _declared_renewal_first_commit(context, precommit, parent):
        _fail("DECLARED_SLOT_RENEWAL_PREFIX_INVALID")


def _read_declared_slot_source_access_renewal(context, precommit, parent, live_source=None):
    relative_dir = _declared_renewal_directory(precommit)
    directory = authority_spool._local_path(context.root, relative_dir)
    try:
        authority_spool._reject_reparse_chain(directory)
        entries = tuple(directory.iterdir())
    except FileNotFoundError:
        return None
    except (OSError, authority_spool.AuthoritySpoolError) as exc:
        raise HistoricalAnchorOrchestrationError("DECLARED_SLOT_RENEWAL_INVALID") from exc
    if len(entries) != 1:
        _fail("DECLARED_SLOT_RENEWAL_MIXED_OR_PARTIAL")
    path = entries[0]
    if path.suffix != ".json" or _HEX64.fullmatch(path.stem) is None:
        _fail("DECLARED_SLOT_RENEWAL_INVALID")
    try:
        authority_spool._reject_reparse_chain(path)
        if not path.is_file():
            _fail("DECLARED_SLOT_RENEWAL_INVALID")
        digest, _, encoded = authority_spool._stream_file(context.root,
            relative_dir + "/" + path.name, maximum_bytes=_DECLARED_RENEWAL_MAX_BYTES, collect=True)
        body = authority_spool._strict_json_bytes(encoded, _DECLARED_RENEWAL_MAX_BYTES)
    except (OSError, authority_spool.AuthoritySpoolError) as exc:
        raise HistoricalAnchorOrchestrationError("DECLARED_SLOT_RENEWAL_INVALID") from exc
    if digest != path.stem or _declared_json(body) != encoded:
        _fail("DECLARED_SLOT_RENEWAL_INVALID")
    _validate_declared_source_access_renewal(context, precommit, parent, body,
        _declared_source_identity() if live_source is None else live_source)
    return DeclaredSlotSourceAccessRenewal(encoded, _DECLARED_SLOT_SEAL)


def verify_declared_slot_source_access_renewal(context, *, precommit, process=None):
    """Reopen the one immutable renewal without checking a current access clock."""
    parent = _declared_renewal_parent(context, precommit, process)
    renewal = _read_declared_slot_source_access_renewal(context, precommit, parent)
    if renewal is None:
        _fail("DECLARED_SLOT_RENEWAL_MISSING")
    return renewal


def issue_declared_slot_source_access_renewal(context, *, precommit=None,
        precommit_receipt_sha256=None, process, frozen_pipeline_config,
        source_access_window_utc, authority_ref=_DECLARED_RENEWAL_AUTHORITY_REF):
    """Bind one verified prefix to a new access window; never execute its suffix."""
    if type(context) is not authority_spool.GenerationSpoolContext or context.writable is not True:
        _fail("DECLARED_SLOT_RENEWAL_READ_ONLY")
    precommit = _declared_renewal_precommit(context, precommit, precommit_receipt_sha256)
    parent = _declared_renewal_parent(context, precommit, process)
    import pm_production_config_binding as config_binding
    import pm_historical_search_producer as producer
    import pm_spread_profile as spread_profile

    if type(frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig:
        _fail("DECLARED_SLOT_CONFIG_INVALID")
    frozen_pipeline_config.verify_current(context)
    config = frozen_pipeline_config.materialize(context)
    if (frozen_pipeline_config.config_sha256 != parent["config_sha256"]
            or frozen_pipeline_config.effective_config_sha256 != parent["effective_config_sha256"]
            or config.discovery_risk_per_trade_pct != 1.0):
        _fail("DECLARED_SLOT_CONFIG_BINDING_MISMATCH")
    profile = spread_profile.load_spread_profile(Path(config.data_dir) / "native" / "terms" / "spread_profile.json")
    if profile.sha256 != parent["spread_profile_identity"]["profile_sha256"]:
        _fail("DECLARED_SLOT_PROFILE_BINDING_MISMATCH")
    cell_id = parent["cells"][0]["cell_id"]
    for slot in parent["slot_roster"]["slots"][1:]:
        if authority_spool._local_path(context.root,
                producer._declared_commit_path(precommit, cell_id, slot["ordinal"])).exists():
            _fail("DECLARED_SLOT_RENEWAL_PREFIX_INVALID")
    for relative_path in (producer._declared_terminal_path(precommit, cell_id),
                          producer._declared_run_path(precommit, cell_id)):
        if authority_spool._local_path(context.root, relative_path).exists():
            _fail("DECLARED_SLOT_RENEWAL_PREFIX_INVALID")
    if (type(source_access_window_utc) is not dict
            or set(source_access_window_utc) != {"start_utc", "end_exclusive_utc"}):
        _fail("DECLARED_SLOT_RENEWAL_WINDOW_INVALID")
    body = {"schema_version": DECLARED_SLOT_SOURCE_ACCESS_RENEWAL_SCHEMA,
        "precommit_sha256": precommit.receipt_sha256,
        "generation_context_sha256": context.context_sha256, "cell_id": cell_id,
        "commit01_body_sha256": _declared_renewal_first_commit(context, precommit, parent),
        "old_source_access_window_utc": parent["source_access_window_utc"],
        "source_access_window_utc": dict(source_access_window_utc),
        "original_source_identity": parent["source_identity"],
        "renewed_source_identity": _declared_source_identity(),
        "authority_ref": authority_ref, "issued_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False}
    _validate_declared_source_access_renewal(context, precommit, parent, body, body["renewed_source_identity"])
    encoded = _declared_json(body)
    if len(encoded) > _DECLARED_RENEWAL_MAX_BYTES:
        _fail("DECLARED_SLOT_RENEWAL_INVALID")
    relative_dir = _declared_renewal_directory(precommit)
    directory = authority_spool._local_path(context.root, relative_dir)
    try:
        authority_spool._reject_reparse_chain(directory)
        directory.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise HistoricalAnchorOrchestrationError("DECLARED_SLOT_RENEWAL_ALREADY_ISSUED") from exc
    except (OSError, authority_spool.AuthoritySpoolError) as exc:
        raise HistoricalAnchorOrchestrationError("DECLARED_SLOT_RENEWAL_PUBLICATION_FAILED") from exc
    digest = hashlib.sha256(encoded).hexdigest()
    try:
        authority_spool._publish_exact_json(context, relative_dir + "/" + digest + ".json",
            body, _DECLARED_RENEWAL_MAX_BYTES)
    except (OSError, authority_spool.AuthoritySpoolError) as exc:
        try:
            authority_spool._reject_reparse_chain(directory)
            directory.rmdir()
        except (OSError, authority_spool.AuthoritySpoolError):
            pass
        raise HistoricalAnchorOrchestrationError("DECLARED_SLOT_RENEWAL_PUBLICATION_FAILED") from exc
    return verify_declared_slot_source_access_renewal(context, precommit=precommit, process=process)


def _declared_slot_effective_access(context, precommit, parent, slot):
    if slot["ordinal"] == 1:
        return parent["source_access_window_utc"], None
    renewal = _read_declared_slot_source_access_renewal(context, precommit, parent)
    if renewal is None:
        return parent["source_access_window_utc"], None
    return renewal.to_dict()["source_access_window_utc"], renewal.receipt_sha256


def declared_slot_transport_edition(body: Mapping[str, Any]) -> str:
    """Decode a declared edition; this does not authenticate the supplied body."""
    if type(body) is not dict:
        _fail("DECLARED_SLOT_TRANSPORT_EDITION_INVALID")
    schema = body.get("schema_version")
    if schema in (DECLARED_SLOT_PRECOMMIT_SCHEMA, DECLARED_SLOT_REQUEST_SCHEMA):
        if "winner_transport_edition" not in body:
            return DECLARED_WINNER_TRANSPORT_V15
    elif schema in (DECLARED_SLOT_PRECOMMIT_SCHEMA_V2, DECLARED_SLOT_REQUEST_SCHEMA_V2):
        if body.get("winner_transport_edition") == DECLARED_WINNER_TRANSPORT_V16:
            return DECLARED_WINNER_TRANSPORT_V16
    _fail("DECLARED_SLOT_TRANSPORT_EDITION_INVALID")


def declared_slot_owner_id(*, cell_id: str, slot_id: str,
                           winner_decision_sha256: str,
                           process_receipt_sha256: str) -> str:
    _hex(cell_id, "DECLARED_SLOT_OWNER_ID_INVALID")
    if type(slot_id) is not str or not slot_id or len(slot_id) > 128:
        _fail("DECLARED_SLOT_OWNER_ID_INVALID")
    body = {"cell_id": cell_id, "slot_id": slot_id,
            "winner_decision_sha256": _hex(winner_decision_sha256, "DECLARED_SLOT_OWNER_ID_INVALID"),
            "process_receipt_sha256": _hex(process_receipt_sha256, "DECLARED_SLOT_OWNER_ID_INVALID")}
    return hashlib.sha256(_declared_json(body)).hexdigest()


def _declared_roster(roster: Any, *, launch_activation_utc,
                     protected_end_exclusive_utc) -> dict[str, Any]:
    import pm_production_anchor_plan as anchor_plan
    if type(roster) is not dict:
        _fail("DECLARED_SLOT_ROSTER_INVALID")
    try:
        return anchor_plan.verify_declared_owner_slot_roster(roster,
            launch_activation_utc=launch_activation_utc,
            frozen_refit_clock=roster["frozen_refit_clock"],
            config_identity_refs=roster["config_identity_refs"],
            extend_frozen_clock_backward=roster["clock_extended_backward"],
            protected_end_exclusive_utc=protected_end_exclusive_utc)
    except Exception as exc:
        raise HistoricalAnchorOrchestrationError("DECLARED_SLOT_ROSTER_INVALID") from exc


def precommit_declared_slot_owner_run(context, *, run_id, process, slot_roster,
        cell_roster, frozen_pipeline_config, spread_profile_identity,
        source_access_window_utc, launch_activation_utc) -> DeclaredSlotRunPrecommit:
    return _precommit_declared_slot_owner_run(context, run_id=run_id, process=process,
        slot_roster=slot_roster, cell_roster=cell_roster,
        frozen_pipeline_config=frozen_pipeline_config,
        spread_profile_identity=spread_profile_identity,
        source_access_window_utc=source_access_window_utc,
        launch_activation_utc=launch_activation_utc,
        transport_edition=DECLARED_WINNER_TRANSPORT_V15)


def precommit_declared_slot_owner_run_v2(context, *, run_id, process, slot_roster,
        cell_roster, frozen_pipeline_config, spread_profile_identity,
        source_access_window_utc, launch_activation_utc) -> DeclaredSlotRunPrecommit:
    return _precommit_declared_slot_owner_run(context, run_id=run_id, process=process,
        slot_roster=slot_roster, cell_roster=cell_roster,
        frozen_pipeline_config=frozen_pipeline_config,
        spread_profile_identity=spread_profile_identity,
        source_access_window_utc=source_access_window_utc,
        launch_activation_utc=launch_activation_utc,
        transport_edition=DECLARED_WINNER_TRANSPORT_V16)


def _precommit_declared_slot_owner_run(context, *, run_id, process, slot_roster,
        cell_roster, frozen_pipeline_config, spread_profile_identity,
        source_access_window_utc, launch_activation_utc, transport_edition):
    if transport_edition not in (DECLARED_WINNER_TRANSPORT_V15, DECLARED_WINNER_TRANSPORT_V16):
        _fail("DECLARED_SLOT_TRANSPORT_EDITION_INVALID")
    import pm_production_search_planner as planner
    import pm_production_config_binding as config_binding
    planner.verify_frozen_production_search_process(context, process)
    planner._require_current_process(process)
    if type(frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig:
        _fail("DECLARED_SLOT_CONFIG_INVALID")
    frozen_pipeline_config.verify_current(context)
    if frozen_pipeline_config.materialize(context).discovery_risk_per_trade_pct != 1.0:
        _fail("DECLARED_SLOT_CONFIG_INVALID")
    _utc(launch_activation_utc, "DECLARED_SLOT_LAUNCH_INVALID")
    roster = _declared_roster(slot_roster,
        launch_activation_utc=launch_activation_utc,
        protected_end_exclusive_utc=_DECLARED_PROTECTED_END_EXCLUSIVE_UTC)
    if (type(cell_roster) not in (list, tuple) or len(cell_roster) != 1
            or type(cell_roster[0]) is not dict
            or set(cell_roster[0]) != {"cell_id", "symbol", "timeframe", "regime"}):
        _fail("DECLARED_SLOT_CELL_INVALID")
    cell = dict(cell_roster[0])
    import pm_winner_selection as selection
    if (cell["cell_id"] != selection.compute_cell_id(cell["symbol"], cell["timeframe"], cell["regime"])
            or cell["symbol"] not in book.PINNED_SYMBOL_ORDER
            or cell["timeframe"] not in book.TIMEFRAME_ORDER or cell["regime"] not in book.REGIME_ORDER):
        _fail("DECLARED_SLOT_CELL_INVALID")
    if roster["config_identity_refs"] != {"config_sha256": context.config_sha256,
            "effective_config_sha256": frozen_pipeline_config.effective_config_sha256}:
        _fail("DECLARED_SLOT_CONFIG_BINDING_MISMATCH")
    if (type(spread_profile_identity) is not dict
            or set(spread_profile_identity) != {"profile_sha256"}):
        _fail("DECLARED_SLOT_PROFILE_INVALID")
    _hex(spread_profile_identity["profile_sha256"], "DECLARED_SLOT_PROFILE_INVALID")
    if (type(source_access_window_utc) is not dict
            or set(source_access_window_utc) != {"start_utc", "end_exclusive_utc"}):
        _fail("DECLARED_SLOT_SOURCE_WINDOW_INVALID")
    start = _utc(source_access_window_utc["start_utc"], "DECLARED_SLOT_SOURCE_WINDOW_INVALID")
    end = _utc(source_access_window_utc["end_exclusive_utc"], "DECLARED_SLOT_SOURCE_WINDOW_INVALID")
    now = datetime.now(timezone.utc)
    if not start <= now < end:
        _fail("DECLARED_SLOT_SOURCE_WINDOW_INVALID")
    if process.to_dict().get("run_id") != run_id:
        _fail("DECLARED_SLOT_PROCESS_BINDING_MISMATCH")
    body = {"schema_version": (DECLARED_SLOT_PRECOMMIT_SCHEMA_V2
            if transport_edition == DECLARED_WINNER_TRANSPORT_V16 else DECLARED_SLOT_PRECOMMIT_SCHEMA),
        "run_id": _safe_id(run_id, "DECLARED_SLOT_PRECOMMIT_INVALID"),
        "generation_id": context.generation_id, "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256, "config_sha256": context.config_sha256,
        "effective_config_sha256": frozen_pipeline_config.effective_config_sha256,
        "search_process_receipt_sha256": process.receipt_sha256,
        "source_identity": _declared_source_identity(), "slot_roster": roster,
        "slot_roster_sha256": hashlib.sha256(_declared_json(roster)).hexdigest(),
        "launch_activation_utc": launch_activation_utc,
        "protected_end_exclusive_utc": _DECLARED_PROTECTED_END_EXCLUSIVE_UTC,
        "cells": [cell], "spread_profile_identity": dict(spread_profile_identity),
        "source_access_window_utc": dict(source_access_window_utc),
        "precommitted_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False}
    if transport_edition == DECLARED_WINNER_TRANSPORT_V16:
        body["winner_transport_edition"] = transport_edition
    declared_slot_transport_edition(body)
    result = DeclaredSlotRunPrecommit(_declared_json(body), _DECLARED_SLOT_SEAL)
    authority_spool._publish_exact_json(context,
        f"declared-slots/{result.receipt_sha256}/precommit.json", body, _MAX_PRECOMMIT_BYTES)
    _register_declared_precommit(result, context)
    return result


def verify_declared_slot_owner_run_precommit(context, precommit, *, process=None) -> dict[str, Any]:
    if type(precommit) is not DeclaredSlotRunPrecommit or precommit._seal is not _DECLARED_SLOT_SEAL:
        _fail("DECLARED_SLOT_PRECOMMIT_INVALID")
    _verify_declared_precommit_identity(precommit, context)
    body = precommit.to_dict()
    _validate_declared_precommit_body(context, body)
    _verify_declared_issued_bytes(context, f"declared-slots/{precommit.receipt_sha256}/precommit.json", precommit._body_json)
    source_identity = _declared_source_identity()
    original_source = False
    renewal = _read_declared_slot_source_access_renewal(context, precommit, body, source_identity)
    if renewal is not None:
        source_identity = renewal.to_dict()["original_source_identity"]
    elif body.get("source_identity") != source_identity:
        from pm_current_refit_parameters import load_retained_strategy_domain_v1
        retained_source = load_retained_strategy_domain_v1()["declared_source_identity"]
        if body.get("source_identity") == retained_source:
            source_identity, original_source = retained_source, True
    expected = {"generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256, "source_identity": source_identity}
    if (_declared_json(body) != precommit._body_json
            or (original_source and body.get("schema_version") != DECLARED_SLOT_PRECOMMIT_SCHEMA)
            or any(body.get(k) != v for k, v in expected.items())
            or _declared_roster(body.get("slot_roster"),
                launch_activation_utc=body["launch_activation_utc"],
                protected_end_exclusive_utc=_DECLARED_PROTECTED_END_EXCLUSIVE_UTC) != body["slot_roster"]
            or body.get("slot_roster_sha256") != hashlib.sha256(_declared_json(body["slot_roster"])).hexdigest()
            or any(body.get(k) is not False for k in ("qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("DECLARED_SLOT_PRECOMMIT_BINDING_MISMATCH")
    if process is not None:
        import pm_production_search_planner as planner
        planner.verify_frozen_production_search_process(context, process)
        original_process = process.to_dict().get("schema_version") == planner.ORIGINAL_PROCESS_SCHEMA
        if (body.get("search_process_receipt_sha256") != process.receipt_sha256
                or original_process != original_source):
            _fail("DECLARED_SLOT_PROCESS_BINDING_MISMATCH")
    return body


def declared_slot_producer_request(context, *, precommit, process, cell_id, slot_id,
                                   attempt_id, sink) -> DeclaredSlotProducerRequest:
    body = verify_declared_slot_owner_run_precommit(context, precommit, process=process)
    edition = declared_slot_transport_edition(body)
    import pm_production_search_planner as planner
    planner._require_current_process(process)
    matches = [x for x in body["cells"] if x["cell_id"] == cell_id]
    slots = [x for x in body["slot_roster"]["slots"] if x["slot_id"] == slot_id]
    if len(matches) != 1 or len(slots) != 1:
        _fail("DECLARED_SLOT_CELL_INVALID" if len(matches) != 1 else "DECLARED_SLOT_SLOT_INVALID")
    cell, slot = matches[0], slots[0]
    access_window, renewal_sha256 = _declared_slot_effective_access(context, precommit, body, slot)
    _safe_id(attempt_id, "DECLARED_SLOT_REQUEST_INVALID")
    if (type(sink) is not authority_spool.SymbolAuthoritySink or sink.context is not context
            or sink.symbol != cell["symbol"]):
        _fail("DECLARED_SLOT_REQUEST_INVALID")
    anchor = {"anchor_id": slot_id, "ordinal": slot["ordinal"],
        "selection_cutoff_utc": slot["native_cutoff_utc"],
        "forward_start_utc": slot["activation_utc"],
        "forward_end_exclusive_utc": slot["end_exclusive_utc"],
        "process_precommit_receipt_sha256": precommit.receipt_sha256}
    request_body = {"schema_version": (DECLARED_SLOT_REQUEST_SCHEMA_V2
            if edition == DECLARED_WINNER_TRANSPORT_V16 else DECLARED_SLOT_REQUEST_SCHEMA),
        "precommit_sha256": precommit.receipt_sha256, "run_id": body["run_id"],
        "generation_context_sha256": context.context_sha256,
        "config_sha256": context.config_sha256, "effective_config_sha256": body["effective_config_sha256"],
        "search_process_receipt_sha256": process.receipt_sha256,
        "process_precommit_receipt_sha256": precommit.receipt_sha256,
        "cell_id": cell_id, "slot_id": slot_id, "cell": cell, **{k:cell[k] for k in ("symbol", "timeframe", "regime")},
        "attempt_id": attempt_id, "anchor": anchor,
        "artifact_namespace": f"declared-slot/{body['run_id']}/{cell_id}/{slot_id}/{attempt_id}",
        "source_access_window_utc": access_window,
        "spread_profile_identity": body["spread_profile_identity"],
        "confers_causal_or_current_authority": False}
    if edition == DECLARED_WINNER_TRANSPORT_V16:
        request_body["winner_transport_edition"] = edition
    if renewal_sha256 is not None:
        request_body["source_access_renewal_sha256"] = renewal_sha256
    result = DeclaredSlotProducerRequest(_declared_json(request_body), precommit, sink, _DECLARED_SLOT_SEAL)
    authority_spool._publish_exact_json(context, _declared_request_path(request_body), request_body, _MAX_PRECOMMIT_BYTES)
    _register_declared_request(result, context, live=True)
    verify_declared_slot_producer_request(context, result, precommit=precommit, process=process)
    return result


def verify_declared_slot_producer_request(context, request, *, precommit=None, process=None) -> dict[str, Any]:
    if type(request) is not DeclaredSlotProducerRequest or request._seal is not _DECLARED_SLOT_SEAL:
        _fail("DECLARED_SLOT_REQUEST_INVALID")
    _verify_declared_request_identity(request, context)
    committed = request._precommit if precommit is None else precommit
    parent = verify_declared_slot_owner_run_precommit(context, committed, process=process)
    body = request.to_dict()
    edition = declared_slot_transport_edition(parent)
    fields = {"schema_version", "precommit_sha256", "run_id", "generation_context_sha256", "config_sha256", "effective_config_sha256", "search_process_receipt_sha256", "process_precommit_receipt_sha256", "cell_id", "slot_id", "cell", "symbol", "timeframe", "regime", "attempt_id", "anchor", "artifact_namespace", "source_access_window_utc", "spread_profile_identity", "confers_causal_or_current_authority"}
    if edition == DECLARED_WINNER_TRANSPORT_V16:
        fields.add("winner_transport_edition")
    if "source_access_renewal_sha256" in body:
        fields.add("source_access_renewal_sha256")
    if set(body) != fields:
        _fail("DECLARED_SLOT_REQUEST_INVALID")
    if declared_slot_transport_edition(body) != edition:
        _fail("DECLARED_SLOT_REQUEST_BINDING_MISMATCH")
    cell = next((x for x in parent["cells"] if x["cell_id"] == body.get("cell_id")), None)
    slot = next((x for x in parent["slot_roster"]["slots"] if x["slot_id"] == body.get("slot_id")), None)
    if cell is None or slot is None:
        _fail("DECLARED_SLOT_REQUEST_BINDING_MISMATCH")
    access_window, renewal_sha256 = _declared_slot_effective_access(context, committed, parent, slot)
    if (cell is None or slot is None or _declared_json(body) != request._body_json
            or body.get("schema_version") != (DECLARED_SLOT_REQUEST_SCHEMA_V2
                if edition == DECLARED_WINNER_TRANSPORT_V16 else DECLARED_SLOT_REQUEST_SCHEMA)
            or body.get("run_id") != parent["run_id"]
            or body.get("precommit_sha256") != committed.receipt_sha256
            or body.get("generation_context_sha256") != context.context_sha256
            or body.get("config_sha256") != context.config_sha256
            or body.get("effective_config_sha256") != parent["effective_config_sha256"]
            or body.get("search_process_receipt_sha256") != parent["search_process_receipt_sha256"]
            or body.get("process_precommit_receipt_sha256") != committed.receipt_sha256
            or body.get("cell") != cell or any(body.get(k) != cell[k] for k in ("symbol", "timeframe", "regime"))
            or body.get("anchor") != {"anchor_id": slot["slot_id"], "ordinal": slot["ordinal"],
                "selection_cutoff_utc": slot["native_cutoff_utc"], "forward_start_utc": slot["activation_utc"],
                "forward_end_exclusive_utc": slot["end_exclusive_utc"], "process_precommit_receipt_sha256": committed.receipt_sha256}
            or body.get("source_access_window_utc") != access_window
            or ("source_access_renewal_sha256" in body) != (renewal_sha256 is not None)
            or body.get("source_access_renewal_sha256") != renewal_sha256
            or body.get("spread_profile_identity") != parent["spread_profile_identity"]
            or body.get("artifact_namespace") != f"declared-slot/{parent['run_id']}/{cell['cell_id']}/{slot['slot_id']}/{body.get('attempt_id')}"
            or body.get("confers_causal_or_current_authority") is not False
            or type(request._sink) is not authority_spool.SymbolAuthoritySink
            or request._sink.context is not context or request._sink.symbol != cell["symbol"]):
        _fail("DECLARED_SLOT_REQUEST_BINDING_MISMATCH")
    _safe_id(body.get("attempt_id"), "DECLARED_SLOT_REQUEST_INVALID")
    _verify_declared_issued_bytes(context, _declared_request_path(body), request._body_json)
    return body


__all__ += ["DeclaredSlotRunPrecommit", "DeclaredSlotProducerRequest",
    "DECLARED_SLOT_PRECOMMIT_SCHEMA", "DECLARED_SLOT_REQUEST_SCHEMA",
    "DECLARED_SLOT_PRECOMMIT_SCHEMA_V2", "DECLARED_SLOT_REQUEST_SCHEMA_V2",
    "DECLARED_WINNER_TRANSPORT_V15", "DECLARED_WINNER_TRANSPORT_V16",
    "declared_slot_transport_edition", "precommit_declared_slot_owner_run_v2",
    "declared_slot_owner_id", "precommit_declared_slot_owner_run",
    "verify_declared_slot_owner_run_precommit", "declared_slot_producer_request",
    "verify_declared_slot_producer_request", "DeclaredSlotSourceAccessRenewal",
    "DECLARED_SLOT_SOURCE_ACCESS_RENEWAL_SCHEMA", "issue_declared_slot_source_access_renewal",
    "verify_declared_slot_source_access_renewal"]


def _declared_request_path(body):
    return f"declared-slots/{body['precommit_sha256']}/requests/{body['cell_id']}/{body['slot_id']}/{body['attempt_id']}.json"


def _verify_declared_issued_bytes(context, relative_path, expected):
    try:
        digest, size, observed = authority_spool._stream_file(context.root, relative_path,
            maximum_bytes=_MAX_PRECOMMIT_BYTES, collect=True)
        if observed != expected or size != len(expected) or digest != hashlib.sha256(expected).hexdigest():
            _fail("DECLARED_SLOT_ISSUED_BYTES_CHANGED")
    except HistoricalAnchorOrchestrationError:
        raise
    except Exception as exc:
        raise HistoricalAnchorOrchestrationError("DECLARED_SLOT_ISSUED_BYTES_MISSING") from exc


def _make_declared_request_authority():
    issued = {}

    def register(request, context, *, live):
        issued[id(request)] = (request, request._body_json, id(context), id(request._sink), live)

    def verify(request, context, *, require_live=False):
        row = issued.get(id(request))
        if (row is None or row[0] is not request or row[1] != request._body_json
                or row[2] != id(context) or row[3] != id(request._sink)
                or (require_live and row[4] is not True)):
            _fail("DECLARED_SLOT_REQUEST_CAPABILITY_INVALID")
        return True

    def release(request, context):
        if id(request) not in issued:
            return
        verify(request, context)
        del issued[id(request)]

    return register, verify, release


(_register_declared_request, _verify_declared_request_identity,
 _release_declared_slot_producer_request) = _make_declared_request_authority()


def _validate_declared_precommit_body(context, body):
    if (type(body) is not dict or body.get("schema_version") not in
            (DECLARED_SLOT_PRECOMMIT_SCHEMA, DECLARED_SLOT_PRECOMMIT_SCHEMA_V2)):
        _fail("DECLARED_SLOT_PRECOMMIT_INVALID")
    fields = {"schema_version", "run_id", "generation_id",
            "generation_context_sha256", "native_universe_sha256", "config_sha256",
            "effective_config_sha256", "search_process_receipt_sha256", "source_identity",
            "slot_roster", "slot_roster_sha256", "cells", "spread_profile_identity",
            "launch_activation_utc", "protected_end_exclusive_utc",
            "source_access_window_utc", "precommitted_at_utc", "qualification_authority",
            "capital_authority", "live_authority", "order_authority"}
    if body.get("schema_version") == DECLARED_SLOT_PRECOMMIT_SCHEMA_V2:
        fields.add("winner_transport_edition")
    if set(body) != fields:
        _fail("DECLARED_SLOT_PRECOMMIT_INVALID")
    declared_slot_transport_edition(body)
    _safe_id(body["run_id"], "DECLARED_SLOT_PRECOMMIT_INVALID")
    _hex(body["effective_config_sha256"], "DECLARED_SLOT_CONFIG_INVALID")
    _hex(body["search_process_receipt_sha256"], "DECLARED_SLOT_PROCESS_INVALID")
    _utc(body["launch_activation_utc"], "DECLARED_SLOT_LAUNCH_INVALID")
    if body["protected_end_exclusive_utc"] != _DECLARED_PROTECTED_END_EXCLUSIVE_UTC:
        _fail("DECLARED_SLOT_PROTECTED_END_INVALID")
    _declared_roster(body["slot_roster"],
        launch_activation_utc=body["launch_activation_utc"],
        protected_end_exclusive_utc=_DECLARED_PROTECTED_END_EXCLUSIVE_UTC)
    if body["slot_roster"]["config_identity_refs"] != {"config_sha256": context.config_sha256,
            "effective_config_sha256": body["effective_config_sha256"]}:
        _fail("DECLARED_SLOT_CONFIG_BINDING_MISMATCH")
    cells = body["cells"]
    if (type(cells) is not list or len(cells) != 1 or type(cells[0]) is not dict
            or set(cells[0]) != {"cell_id", "symbol", "timeframe", "regime"}):
        _fail("DECLARED_SLOT_CELL_INVALID")
    import pm_winner_selection as selection
    cell = cells[0]
    if (cell["symbol"] not in book.PINNED_SYMBOL_ORDER or cell["timeframe"] not in book.TIMEFRAME_ORDER
            or cell["regime"] not in book.REGIME_ORDER
            or cell["cell_id"] != selection.compute_cell_id(cell["symbol"], cell["timeframe"], cell["regime"])):
        _fail("DECLARED_SLOT_CELL_INVALID")
    profile, window = body["spread_profile_identity"], body["source_access_window_utc"]
    if type(profile) is not dict or set(profile) != {"profile_sha256"}:
        _fail("DECLARED_SLOT_PROFILE_INVALID")
    _hex(profile["profile_sha256"], "DECLARED_SLOT_PROFILE_INVALID")
    if type(window) is not dict or set(window) != {"start_utc", "end_exclusive_utc"}:
        _fail("DECLARED_SLOT_SOURCE_WINDOW_INVALID")
    start = _utc(window["start_utc"], "DECLARED_SLOT_SOURCE_WINDOW_INVALID")
    end = _utc(window["end_exclusive_utc"], "DECLARED_SLOT_SOURCE_WINDOW_INVALID")
    committed = _utc(body["precommitted_at_utc"], "DECLARED_SLOT_SOURCE_WINDOW_INVALID")
    if not start <= committed < end:
        _fail("DECLARED_SLOT_SOURCE_WINDOW_INVALID")


def _make_declared_precommit_authority():
    issued = {}
    def register(value, context):
        issued[id(value)] = (value, value._body_json, id(context))
    def verify(value, context):
        row = issued.get(id(value))
        if row is None or row[0] is not value or row[1] != value._body_json or row[2] != id(context):
            _fail("DECLARED_SLOT_PRECOMMIT_CAPABILITY_INVALID")
    return register, verify


_register_declared_precommit, _verify_declared_precommit_identity = _make_declared_precommit_authority()


def reopen_declared_slot_owner_run_precommit(context, *, receipt_sha256, process, frozen_pipeline_config):
    digest = _hex(receipt_sha256, "DECLARED_SLOT_PRECOMMIT_INVALID")
    observed_hash, _, encoded = authority_spool._stream_file(context.root,
        f"declared-slots/{digest}/precommit.json", maximum_bytes=_MAX_PRECOMMIT_BYTES, collect=True)
    if observed_hash != digest or encoded is None:
        _fail("DECLARED_SLOT_PRECOMMIT_INVALID")
    body = authority_spool._strict_json_bytes(encoded, _MAX_PRECOMMIT_BYTES)
    if _declared_json(body) != encoded:
        _fail("DECLARED_SLOT_PRECOMMIT_INVALID")
    _validate_declared_precommit_body(context, body)
    import pm_production_config_binding as config_binding
    import pm_spread_profile as spread_profile
    if type(frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig:
        _fail("DECLARED_SLOT_CONFIG_INVALID")
    frozen_pipeline_config.verify_current(context)
    config = frozen_pipeline_config.materialize(context)
    if (frozen_pipeline_config.effective_config_sha256 != body["effective_config_sha256"]
            or config.discovery_risk_per_trade_pct != 1.0):
        _fail("DECLARED_SLOT_CONFIG_BINDING_MISMATCH")
    profile = spread_profile.load_spread_profile(Path(config.data_dir) / "native" / "terms" / "spread_profile.json")
    if profile.sha256 != body["spread_profile_identity"]["profile_sha256"]:
        _fail("DECLARED_SLOT_PROFILE_BINDING_MISMATCH")
    result = DeclaredSlotRunPrecommit(encoded, _DECLARED_SLOT_SEAL)
    _register_declared_precommit(result, context)
    verify_declared_slot_owner_run_precommit(context, result, process=process)
    return result


__all__ += ["reopen_declared_slot_owner_run_precommit"]
