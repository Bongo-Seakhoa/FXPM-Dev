"""Genuine latest-cutoff current-refit producer for production coordination.

The callable in this module is entered only after the coordinator has closed
the historical run.  It independently replays the causal PASS from the raw
payloads carried by that request, freezes a current-search authorization,
runs the identical frozen process over the complete 2,400-cell Book, creates
the strict raw-current-refit graph, performs a synchronized management replay,
and delegates the final 13-field projection to the released production
adapter.  No caller decision flag, selected outcome, candidate roster, or
publication authority is accepted.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import gc
import hashlib
import json
from pathlib import Path
import threading
from typing import Any, Final, Iterable, Mapping, Sequence

import pm_authoritative_search_execution as search_execution
import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_causal_process as causal
import pm_current_refit_artifacts as current_artifacts
import pm_current_refit_execution as current_execution
import pm_current_refit_production as current_production
import pm_historical_production_adapter as historical_adapter
import pm_memory_governor as memory_governor
import pm_model_lifecycle as lifecycle
import pm_native_prefix_production as native_prefix
import pm_production_config_binding as config_binding
import pm_production_search_planner as planner
import pm_training_horizon as training_horizon
import pm_winner_raw_evidence as raw_winner
import pm_winner_selection as winner
import pm_core
from pm_core import PipelineConfig


UTC: Final = timezone.utc
PRODUCER_SCHEMA: Final = "fxpm.current-refit-search-producer.v2"
COORDINATOR_REQUEST_SCHEMA: Final = "fxpm.production-authority-coordinator.v1"
GENERATION_SYMBOL: Final = "__CURRENT_REFIT_GENERATION__"
SOURCE_BUNDLE_SCHEMA: Final = "fxpm.current-refit-source-bundle.v1"
HORIZON_BUNDLE_SCHEMA: Final = "fxpm.current-refit-horizon-bundle.v1"
BOOK_ENTRY_SCHEMA: Final = "fxpm.book-multiplicity-cell.v9"
MAX_REQUEST_BYTES: Final = 64 * 1024 * 1024
SYMBOL_SCHEDULER_SCHEMA: Final = "fxpm.current-refit-symbol-scheduler.v1"
SYMBOL_BUNDLE_SCHEMA: Final = "fxpm.current-refit-symbol-bundle.v1"
TIMEFRAME_BUNDLE_SCHEMA: Final = "fxpm.current-refit-timeframe-bundle.v1"
SYMBOL_SCHEDULER_TERMINATION_SECONDS: Final = 5.0
MAX_TIMEFRAME_BUNDLE_BYTES: Final = 256 * 1024**2
MAX_SYMBOL_BUNDLE_BYTES: Final = 8 * 1024**2
_HEX: Final = frozenset("0123456789abcdef")


class CurrentRefitSearchProducerError(RuntimeError):
    """Typed fail-closed current-generation error; never a market verdict."""

    def __init__(self, code: str, details: Sequence[Any] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value) for value in details)
        suffix = ":" + ",".join(self.details[:8]) if self.details else ""
        super().__init__(self.code + suffix)


class CurrentRefitSearchNotPromotable(RuntimeError):
    """Evidence-complete current Book outcome with no promotable local winner."""

    def __init__(self, code: str, details: Sequence[Any] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value) for value in details)
        suffix = ":" + ",".join(self.details[:8]) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise CurrentRefitSearchProducerError(code, details)


def _canonical_bytes(value: Any, maximum: int = MAX_REQUEST_BYTES) -> bytes:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError, OverflowError) as exc:
        raise CurrentRefitSearchProducerError(
            "VALUE_NOT_CANONICAL_JSON", (type(exc).__name__, str(exc))
        ) from exc
    if not 0 < len(encoded) <= maximum:
        _fail("CANONICAL_JSON_SIZE_INVALID", len(encoded))
    return encoded


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _hex64(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in _HEX for character in value)
    ):
        _fail(code)
    return value


def _utc(value: Any, code: str) -> datetime:
    if type(value) is not str or not value:
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CurrentRefitSearchProducerError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    parsed = parsed.astimezone(UTC)
    if parsed.microsecond:
        _fail(code)
    return parsed


def _utc_plus(value: Any, code: str) -> str:
    return _utc(value, code).isoformat()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _required_valid_until_utc(
    config: PipelineConfig, refit_payload: Mapping[str, Any]
) -> datetime:
    execution = refit_payload.get("execution")
    if not isinstance(execution, Mapping):
        _fail("CURRENT_REFIT_EXECUTION_BODY_INVALID")
    completed_at = _utc(
        execution.get("completed_at_utc"),
        "CURRENT_REFIT_COMPLETED_AT_UTC_INVALID",
    )
    try:
        deadline = config.get_next_retrain_at_utc(completed_at)
    except (TypeError, ValueError) as exc:
        raise CurrentRefitSearchProducerError(
            "CURRENT_REFIT_VALID_UNTIL_DERIVATION_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if deadline.tzinfo is None or deadline.utcoffset() != timedelta(0):
        _fail("CURRENT_REFIT_VALID_UNTIL_NOT_AWARE_UTC")
    return deadline.astimezone(UTC)


def verify_causal_process_pass(
    *,
    causal_precommit_payload: Mapping[str, Any],
    causal_evidence_payload: Mapping[str, Any],
    causal_reconciliation_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Re-evaluate raw causal evidence and require the released PASS literal."""

    values: list[dict[str, Any]] = []
    for name, value in (
        ("PRECOMMIT", causal_precommit_payload),
        ("EVIDENCE", causal_evidence_payload),
        ("RECONCILIATION", causal_reconciliation_payload),
    ):
        if not isinstance(value, Mapping):
            _fail(f"CAUSAL_{name}_NOT_OBJECT")
        values.append(json.loads(_canonical_bytes(dict(value)).decode("ascii")))
    precommit, evidence, reconciliation = values
    try:
        decision = causal.evaluate_historical_process(
            precommit, evidence, reconciliation
        )
        if (
            causal.verify_historical_process_decision(
                decision, precommit, evidence, reconciliation
            )
            is not True
        ):
            raise ValueError("decision did not reverify")
        body = decision.body()
    except Exception as exc:
        raise CurrentRefitSearchProducerError(
            "CAUSAL_PROCESS_RAW_REVERIFY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if type(body) is not dict:
        _fail("CAUSAL_DECISION_BODY_INVALID")
    if body.get("decision") != "OFFLINE_PROCESS_VALIDATION_PASS":
        _fail(
            "CAUSAL_PROCESS_NOT_PASS",
            *tuple(body.get("failure_reasons") or ()),
            *tuple(body.get("insufficiency_reasons") or ()),
        )
    return {
        "decision_body": json.loads(_canonical_bytes(body).decode("ascii")),
        "decision_sha256": causal.canonical_sha256(body),
    }


@dataclass(frozen=True, slots=True)
class CurrentRefitSymbolSchedulerInputs:
    """Reconstructable production worker inputs; contains no sealed authority."""

    active_config_path: Path
    data_dir: Path
    cutoff_utc: str
    source_accessed_at_utc: str
    requested_workers: int


@dataclass(frozen=True, slots=True)
class CurrentRefitSearchProductionInputs:
    """Sealed, outcome-free inputs available before current search starts."""

    context: authority_spool.GenerationSpoolContext
    search_process: planner.FrozenProductionSearchProcess
    active_config_path: Path
    config_payload: Mapping[str, Any]
    pipeline_config: PipelineConfig
    native_products: Mapping[str, native_prefix.NativePrefixProduct]
    current_selection_window: Mapping[str, str]
    output_root: Path
    symbol_scheduler: CurrentRefitSymbolSchedulerInputs | None = None


@dataclass(frozen=True, slots=True)
class _ExecutableCurrentRefitInputs:
    """Private owned inputs materialized from the exact active config bytes."""

    context: authority_spool.GenerationSpoolContext
    search_process: planner.FrozenProductionSearchProcess
    pipeline_config: PipelineConfig
    native_products: Mapping[str, native_prefix.NativePrefixProduct]
    current_selection_window: Mapping[str, str]
    output_root: Path
    symbol_scheduler: CurrentRefitSymbolSchedulerInputs | None = None


@dataclass(frozen=True, slots=True)
class _SelectedSearch:
    symbol: str
    timeframe: str
    regime: str
    artifacts: Mapping[str, Any]
    source_capture: Mapping[str, Any]
    winner_decision_sha256: str


@dataclass(frozen=True, slots=True)
class _SelectedMaterial:
    book_cell: Mapping[str, Any]
    search: _SelectedSearch
    winner_process: Mapping[str, Any]
    horizon_process: Mapping[str, Any]
    trial_definition: Mapping[str, Any]
    refit_recipe: Mapping[str, Any]
    descriptors: Mapping[str, Mapping[str, Any]]
    detector: Mapping[str, str]


class _CaptureRequest:
    def __init__(self, body: Mapping[str, Any]) -> None:
        self._body = json.loads(_canonical_bytes(dict(body)).decode("ascii"))

    def to_dict(self) -> dict[str, Any]:
        return json.loads(_canonical_bytes(self._body).decode("ascii"))


def _request_body(request: Any) -> dict[str, Any]:
    try:
        body = request.to_dict()
    except Exception as exc:
        raise CurrentRefitSearchProducerError("CURRENT_REFIT_REQUEST_INVALID") from exc
    fields = {
        "schema_version",
        "historical_run_receipt_sha256",
        "historical_process_decision_sha256",
        "historical_process_decision",
        "historical_book_receipt_sha256",
        "causal_precommit_payload",
        "causal_evidence_payload",
        "causal_reconciliation_payload",
        "authority_claimed",
    }
    if (
        type(body) is not dict
        or set(body) != fields
        or body.get("schema_version") != COORDINATOR_REQUEST_SCHEMA
        or body.get("authority_claimed") is not False
    ):
        _fail("CURRENT_REFIT_REQUEST_INVALID")
    _canonical_bytes(body)
    for name in (
        "historical_run_receipt_sha256",
        "historical_process_decision_sha256",
        "historical_book_receipt_sha256",
    ):
        _hex64(body.get(name), f"CURRENT_REQUEST_{name.upper()}_INVALID")
    for name in (
        "historical_process_decision",
        "causal_precommit_payload",
        "causal_evidence_payload",
        "causal_reconciliation_payload",
    ):
        if type(body.get(name)) is not dict:
            _fail("CURRENT_REFIT_REQUEST_INVALID", name)
    return json.loads(_canonical_bytes(body).decode("ascii"))


def _native_key(symbol: str, timeframe: str) -> str:
    return f"{symbol}|{timeframe}"


def _current_cutoff(inputs: _ExecutableCurrentRefitInputs) -> str:
    scheduler = getattr(inputs, "symbol_scheduler", None)
    if scheduler is not None:
        if type(scheduler) is not CurrentRefitSymbolSchedulerInputs:
            _fail("CURRENT_SYMBOL_SCHEDULER_INPUT_INVALID")
        if inputs.native_products:
            _fail("CURRENT_SYMBOL_SCHEDULER_PRODUCT_SPLICE")
        return _utc_plus(scheduler.cutoff_utc, "CURRENT_CUTOFF_INVALID")
    expected = [
        _native_key(symbol, timeframe)
        for symbol in book.PINNED_SYMBOL_ORDER
        for timeframe in book.TIMEFRAME_ORDER
    ]
    products = inputs.native_products
    if type(products) is not dict or list(products) != expected:
        _fail("CURRENT_NATIVE_PRODUCT_ROSTER_INVALID")
    cutoffs: set[str] = set()
    for key in expected:
        product = products[key]
        if (
            type(product) is not native_prefix.NativePrefixProduct
            or getattr(product, "_seal", None) is not native_prefix._PRODUCT_SEAL
            or type(product.index) is not dict
        ):
            _fail("CURRENT_NATIVE_PRODUCT_NOT_SEALED", key)
        symbol, timeframe = key.split("|", 1)
        if (
            str(product.index.get("symbol") or "") != symbol
            or str(product.index.get("timeframe") or "").upper() != timeframe
        ):
            _fail("CURRENT_NATIVE_PRODUCT_IDENTITY_MISMATCH", key)
        cutoffs.add(
            _utc_plus(product.index.get("cutoff_utc"), "CURRENT_CUTOFF_INVALID")
        )
    if len(cutoffs) != 1:
        _fail("CURRENT_NATIVE_COMMON_CUTOFF_MISSING", *sorted(cutoffs))
    return next(iter(cutoffs))


def _state_evidence(
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    source: Any,
    fit: Any,
    state_kind: str,
    reason_code: str,
    attempted: int,
    horizon_plan: Any = None,
    production: Any = None,
    cross_binding: bytes | None = None,
    context: authority_spool.GenerationSpoolContext | None = None,
) -> book.HistoricalBookCellEvidence:
    kwargs: dict[str, Any] = {}
    if state_kind == book.INSUFFICIENT_EVIDENCE:
        kwargs["horizon_plan"] = horizon_plan
        if production is not None:
            entry = production.entries.get("production-receipt")
            descriptor = entry.get("descriptor") if type(entry) is dict else None
            if type(descriptor) is not dict or context is None:
                _fail("CURRENT_WINNER_PRODUCTION_RECEIPT_MISSING")
            path = context.root.joinpath(*Path(descriptor["relative_path"]).parts)
            raw = path.read_bytes()
            if len(raw) != descriptor.get("size_bytes") or hashlib.sha256(
                raw
            ).hexdigest() != descriptor.get("sha256"):
                _fail("CURRENT_WINNER_PRODUCTION_RECEIPT_CHANGED")
            kwargs.update(
                process_rejection_request=production.request,
                process_rejection_decision=production.decision,
                horizon_winner_cross_binding_bytes=cross_binding,
                winner_raw_production_receipt_bytes=raw,
            )
    decision = book.create_historical_process_state_decision(
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
        state_kind=state_kind,
        reason_code=reason_code,
        source_group_decision=source,
        regime_fit_decision=fit,
        attempted_trials_exact=attempted,
        **kwargs,
    )
    if decision is None:
        _fail("CURRENT_PROCESS_STATE_DECISION_REJECTED", symbol, timeframe, regime)
    evidence = book.process_state_cell_evidence(
        process_state_decision=decision,
        source_group_decision=source,
        regime_fit_decision=fit,
        **kwargs,
    )
    if evidence is None:
        _fail("CURRENT_PROCESS_STATE_EVIDENCE_REJECTED", symbol, timeframe, regime)
    return evidence


def _all_process_rejected(
    execution: search_execution.CurrentObservedCellExecution,
) -> bool:
    production = execution.production
    if production is None:
        return False
    try:
        return bool(
            historical_adapter._all_precommitted_attempts_process_rejected(  # noqa: SLF001
                request=production.request,
                decision=production.decision,
                attempted_trials_exact=execution.attempted_trials_exact,
            )
        )
    except Exception as exc:
        raise CurrentRefitSearchProducerError(
            "CURRENT_PROCESS_REJECTION_REPLAY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc


def _selected_attempt(
    plan: planner.ProductionCellSearchPlan, candidate_id: str
) -> dict[str, Any]:
    matches = [
        artifact
        for artifact in plan.attempt_artifacts()
        if artifact.get("winner_process", {}).get("process_candidate_id")
        == candidate_id
    ]
    if len(matches) != 1:
        _fail(
            "CURRENT_SELECTED_ATTEMPT_NOT_UNIQUE",
            plan.symbol,
            plan.timeframe,
            plan.regime,
        )
    return json.loads(_canonical_bytes(matches[0]).decode("ascii"))


def _release_timeframe_state() -> None:
    """Release finalized feature frames before the next native cell is opened."""

    try:
        pm_core.FeatureComputer.clear_cache()
        gc.collect()
    except Exception as exc:
        raise CurrentRefitSearchProducerError(
            "CURRENT_TIMEFRAME_RESOURCE_RELEASE_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc


def _evaluate_current_timeframe(
    *,
    context: authority_spool.GenerationSpoolContext,
    process: planner.FrozenProductionSearchProcess,
    authorization: planner.CurrentSearchAuthorization,
    sink: authority_spool.SymbolAuthoritySink,
    config: PipelineConfig,
    product: native_prefix.NativePrefixProduct,
    symbol: str,
    timeframe: str,
) -> tuple[
    list[book.HistoricalBookCellEvidence],
    dict[str, _SelectedSearch],
    dict[str, dict[str, Any]],
]:
    """Evaluate one native timeframe without retaining its feature frames."""

    evidence_batch: list[book.HistoricalBookCellEvidence] = []
    selected: dict[str, _SelectedSearch] = {}
    raw_artifacts: dict[str, dict[str, Any]] = {}
    if product.source_state == native_prefix.AVAILABLE:
        fitted = search_execution.fit_native_timeframe_for_search(
            product=product,
            config=config,
        )
        support = fitted.train_regime_support["counts"]
        for regime in book.REGIME_ORDER:
            if support[regime] == 0:
                plan = planner.build_current_unobserved_cell_plan(
                    context,
                    process=process,
                    authorization=authorization,
                    sink=sink,
                    fitted=fitted,
                    regime=regime,
                )
                evidence_batch.append(
                    _state_evidence(
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        source=fitted.source_group_decision,
                        fit=fitted.regime_fit_decision,
                        state_kind=book.REGIME_UNOBSERVED,
                        reason_code=book.REGIME_UNOBSERVED_REASON,
                        attempted=0,
                    )
                )
                del plan
                continue
            plan = planner.build_current_observed_cell_plan(
                context,
                process=process,
                authorization=authorization,
                sink=sink,
                fitted=fitted,
                regime=regime,
            )
            execution = search_execution.execute_current_observed_cell(
                process=process,
                sink=sink,
                cell_plan=plan,
                fitted=fitted,
                config=config,
            )
            if execution.production is None:
                evidence_batch.append(
                    _state_evidence(
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        source=fitted.source_group_decision,
                        fit=fitted.regime_fit_decision,
                        state_kind=book.INSUFFICIENT_EVIDENCE,
                        reason_code=book.INSUFFICIENT_PRESEARCH_BOUNDARY_REASON,
                        attempted=0,
                        horizon_plan=plan.horizon_plan,
                    )
                )
                continue
            production = execution.production
            try:
                raw_artifacts[regime] = {
                    "winner_request": _artifact_binding(
                        production.entries["winner-request"]
                    ),
                    "winner_decision": _artifact_binding(
                        production.entries["winner-decision"]
                    ),
                    "production_receipt": _artifact_binding(
                        production.entries["production-receipt"]
                    ),
                    "cross_binding": (
                        None
                        if execution.cross_binding_entry is None
                        else _artifact_binding(execution.cross_binding_entry)
                    ),
                }
            except (KeyError, TypeError) as exc:
                raise CurrentRefitSearchProducerError(
                    "CURRENT_WORKER_RAW_ARTIFACT_ROSTER_INVALID",
                    (symbol, timeframe, regime, type(exc).__name__),
                ) from exc
            if _all_process_rejected(execution):
                evidence_batch.append(
                    _state_evidence(
                        symbol=symbol,
                        timeframe=timeframe,
                        regime=regime,
                        source=fitted.source_group_decision,
                        fit=fitted.regime_fit_decision,
                        state_kind=book.INSUFFICIENT_EVIDENCE,
                        reason_code=book.INSUFFICIENT_ALL_ATTEMPTS_REJECTED_REASON,
                        attempted=execution.attempted_trials_exact,
                        horizon_plan=plan.horizon_plan,
                        production=production,
                        cross_binding=execution.cross_binding_bytes,
                        context=context,
                    )
                )
                continue
            evidence = book.winner_cell_evidence(
                production.decision,
                production.request,
                fitted.source_group_decision,
                fitted.regime_fit_decision,
            )
            if evidence is None:
                _fail(
                    "CURRENT_WINNER_BOOK_EVIDENCE_REJECTED",
                    symbol,
                    timeframe,
                    regime,
                )
            candidate_id = production.decision.winner_process_candidate_id
            if candidate_id is not None:
                key = _native_key(symbol, timeframe) + f"|{regime}"
                selected[key] = _SelectedSearch(
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    artifacts=_selected_attempt(plan, candidate_id),
                    source_capture=plan.current_refit_source_capture() or {},
                    winner_decision_sha256=production.decision.receipt_sha256,
                )
            evidence_batch.append(evidence)
        del fitted
    elif product.source_state in {
        native_prefix.SOURCE_UNAVAILABLE,
        native_prefix.TECHNICAL_TERMINAL,
    }:
        terminal = search_execution.fit_terminal_native_timeframe(
            product=product,
            config=config,
        )
        reason = terminal.regime_fit_decision.to_dict()["reason_code"]
        state_kind = (
            book.TERMINAL_SOURCE_ABSENCE
            if product.source_state == native_prefix.SOURCE_UNAVAILABLE
            else book.TERMINAL_TECHNICAL
        )
        for regime in book.REGIME_ORDER:
            plan = planner.build_current_terminal_cell_plan(
                context,
                process=process,
                authorization=authorization,
                sink=sink,
                product=product,
                regime=regime,
            )
            evidence_batch.append(
                _state_evidence(
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    source=terminal.source_group_decision,
                    fit=terminal.regime_fit_decision,
                    state_kind=state_kind,
                    reason_code=reason,
                    attempted=0,
                )
            )
            del plan
        del terminal
    else:
        _fail("CURRENT_NATIVE_TECHNICAL_FAILURE", symbol, timeframe)
    return evidence_batch, selected, raw_artifacts


def _book_evidence_stream(
    inputs: _ExecutableCurrentRefitInputs,
    *,
    authorization: planner.CurrentSearchAuthorization,
    selected: dict[str, _SelectedSearch],
    compact_receipts: dict[str, Any],
) -> Iterable[book.HistoricalBookCellEvidence]:
    context = inputs.context
    process = inputs.search_process
    config = inputs.pipeline_config
    for symbol in book.PINNED_SYMBOL_ORDER:
        sink = authority_spool.open_symbol_sink(
            context,
            symbol,
            manifest_domain=authority_spool.CURRENT_REFIT_MANIFEST_DOMAIN,
        )
        try:
            for timeframe in book.TIMEFRAME_ORDER:
                evidence_batch: list[book.HistoricalBookCellEvidence] = []
                selected_batch: dict[str, _SelectedSearch] = {}
                try:
                    product = inputs.native_products[_native_key(symbol, timeframe)]
                    evidence_batch, selected_batch, _raw_artifacts = (
                        _evaluate_current_timeframe(
                        context=context,
                        process=process,
                        authorization=authorization,
                        sink=sink,
                        config=config,
                        product=product,
                        symbol=symbol,
                        timeframe=timeframe,
                    )
                    )
                finally:
                    _release_timeframe_state()
                for key, value in selected_batch.items():
                    if key in selected:
                        _fail("CURRENT_SELECTED_SEARCH_DUPLICATE", key)
                    selected[key] = value
                # Yield only after the finalized timeframe's full feature
                # cache is gone.  A downstream Book-verifier exception can no
                # longer strand frames inside this generator.
                yield from evidence_batch
        finally:
            compact_receipts[symbol] = sink.seal()


def _serialize_cell_evidence(
    evidence: book.HistoricalBookCellEvidence,
    *,
    selected: _SelectedSearch | None,
    raw_artifacts: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if type(evidence) is not book.HistoricalBookCellEvidence:
        _fail("CURRENT_WORKER_EVIDENCE_TYPE_INVALID")
    raw = (
        None
        if raw_artifacts is None
        else json.loads(
            _canonical_bytes(dict(raw_artifacts), maximum=MAX_SYMBOL_BUNDLE_BYTES).decode(
                "ascii"
            )
        )
    )
    if evidence.winner_decision is not None:
        if (
            evidence.winner_request is None
            or evidence.process_state_decision is not None
            or type(raw) is not dict
            or any(
                type(raw.get(name)) is not dict
                for name in (
                    "winner_request",
                    "winner_decision",
                    "production_receipt",
                    "cross_binding",
                )
            )
            or raw.get("winner_request", {}).get("sha256")
            != _sha(evidence.winner_request.to_dict())
            or raw.get("winner_decision", {}).get("sha256")
            != _sha(evidence.winner_decision.to_dict())
        ):
            _fail("CURRENT_WORKER_WINNER_EVIDENCE_INVALID")
        cell = {
            "kind": "WINNER",
            "regime": evidence.regime,
            "raw_artifacts": raw,
            "winner_decision_sha256": evidence.winner_decision.receipt_sha256,
            "process_state": None,
        }
    else:
        if (
            evidence.process_state_decision is None
            or evidence.winner_request is not None
        ):
            _fail("CURRENT_WORKER_PROCESS_EVIDENCE_INVALID")
        horizon = evidence.insufficient_horizon_plan
        horizon_payload = None
        if horizon is not None:
            horizon_payload = {
                "payload": horizon.to_dict(),
                "receipt_sha256": horizon.receipt_sha256,
            }
        rejection_request = evidence.insufficient_process_rejection_request
        rejection_decision = evidence.insufficient_process_rejection_decision
        raw_receipt = evidence.insufficient_winner_raw_production_receipt_bytes
        cross_binding = evidence.insufficient_horizon_winner_cross_binding_bytes
        if rejection_request is not None:
            if (
                rejection_decision is None
                or type(raw) is not dict
                or raw_receipt is None
                or cross_binding is None
                or raw.get("winner_request", {}).get("sha256")
                != _sha(rejection_request.to_dict())
                or raw.get("winner_decision", {}).get("sha256")
                != _sha(rejection_decision.to_dict())
                or raw.get("production_receipt", {}).get("sha256")
                != hashlib.sha256(raw_receipt).hexdigest()
                or (raw.get("cross_binding") or {}).get("sha256")
                != hashlib.sha256(cross_binding).hexdigest()
            ):
                _fail("CURRENT_WORKER_REJECTION_ARTIFACT_BINDING_INVALID")
        elif any(
            value is not None
            for value in (
                raw,
                rejection_decision,
                raw_receipt,
                cross_binding,
            )
        ):
            _fail("CURRENT_WORKER_PROCESS_ARTIFACT_ORPHANED")
        cell = {
            "kind": "PROCESS_STATE",
            "regime": evidence.regime,
            "raw_artifacts": raw,
            "winner_decision_sha256": None,
            "process_state": {
                "decision": _owned_envelope(evidence.process_state_decision),
                "horizon_plan": horizon_payload,
                "process_rejection_decision_sha256": (
                    None
                    if rejection_decision is None
                    else rejection_decision.receipt_sha256
                ),
            },
        }
    cell["selected"] = (
        None
        if selected is None
        else {
            "artifacts": json.loads(
                _canonical_bytes(dict(selected.artifacts)).decode("ascii")
            ),
            "source_capture": json.loads(
                _canonical_bytes(dict(selected.source_capture)).decode("ascii")
            ),
            "winner_decision_sha256": selected.winner_decision_sha256,
        }
    )
    return cell


def _artifact_binding(entry: Mapping[str, Any]) -> dict[str, Any]:
    descriptor = entry.get("descriptor")
    if (
        type(entry) is not dict
        or type(descriptor) is not dict
        or type(entry.get("artifact_id")) is not str
        or type(entry.get("artifact_type")) is not str
    ):
        _fail("CURRENT_WORKER_ARTIFACT_ENTRY_INVALID")
    return {
        "artifact_id": entry["artifact_id"],
        "artifact_type": entry["artifact_type"],
        "sha256": descriptor["sha256"],
        "size_bytes": descriptor["size_bytes"],
    }


def _spool_timeframe_bundle(
    *,
    sink: authority_spool.SymbolAuthoritySink,
    context: authority_spool.GenerationSpoolContext,
    process: planner.FrozenProductionSearchProcess,
    authorization: planner.CurrentSearchAuthorization,
    cutoff_utc: str,
    symbol: str,
    timeframe: str,
    evidence_batch: Sequence[book.HistoricalBookCellEvidence],
    selected_batch: Mapping[str, _SelectedSearch],
    raw_artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if (
        len(evidence_batch) != len(book.REGIME_ORDER)
        or [value.regime for value in evidence_batch] != list(book.REGIME_ORDER)
    ):
        _fail("CURRENT_WORKER_REGIME_ROSTER_INVALID", symbol, timeframe)
    first = evidence_batch[0]
    source_envelope = _owned_envelope(first.source_group_decision)
    fit_envelope = _owned_envelope(first.regime_fit_decision)
    cell_bindings: list[dict[str, Any]] = []
    for evidence in evidence_batch:
        if (
            evidence.symbol != symbol
            or evidence.timeframe != timeframe
            or _owned_envelope(evidence.source_group_decision) != source_envelope
            or _owned_envelope(evidence.regime_fit_decision) != fit_envelope
        ):
            _fail("CURRENT_WORKER_TIMEFRAME_EVIDENCE_SPLICE", symbol, timeframe)
        key = f"{symbol}|{timeframe}|{evidence.regime}"
        cell = {
            "schema_version": TIMEFRAME_BUNDLE_SCHEMA,
            "generation_context_sha256": context.context_sha256,
            "search_process_receipt_sha256": process.receipt_sha256,
            "authorization_receipt_sha256": authorization.receipt_sha256,
            "current_cutoff_utc": cutoff_utc,
            "symbol": symbol,
            "timeframe": timeframe,
            **_serialize_cell_evidence(
                evidence,
                selected=selected_batch.get(key),
                raw_artifacts=raw_artifacts.get(evidence.regime),
            ),
        }
        entry = sink.put_json(
            f"current-refit/worker-evidence/{timeframe}/{evidence.regime}",
            "current_refit_worker_cell_evidence",
            cell,
        )
        cell_bindings.append(_artifact_binding(entry))
    if set(selected_batch) - {
        f"{symbol}|{timeframe}|{regime}" for regime in book.REGIME_ORDER
    }:
        _fail("CURRENT_WORKER_SELECTED_ROSTER_INVALID", symbol, timeframe)
    index = {
        "schema_version": TIMEFRAME_BUNDLE_SCHEMA,
        "generation_context_sha256": context.context_sha256,
        "search_process_receipt_sha256": process.receipt_sha256,
        "authorization_receipt_sha256": authorization.receipt_sha256,
        "current_cutoff_utc": cutoff_utc,
        "symbol": symbol,
        "timeframe": timeframe,
        "source_group": source_envelope,
        "regime_fit": fit_envelope,
        "regime_order": list(book.REGIME_ORDER),
        "cell_artifacts": cell_bindings,
    }
    return _artifact_binding(
        sink.put_json(
            f"current-refit/worker-evidence/{timeframe}/index",
            "current_refit_worker_timeframe_index",
            index,
        )
    )


def _current_refit_symbol_worker(
    contract: Mapping[str, Any],
    symbol: str,
) -> dict[str, Any]:
    """Spawn-safe complete-symbol worker; reconstruct every authority locally."""

    owned = json.loads(
        _canonical_bytes(dict(contract), maximum=MAX_TIMEFRAME_BUNDLE_BYTES).decode(
            "ascii"
        )
    )
    expected_fields = {
        "schema_version",
        "spool_root",
        "generation_id",
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "run_id",
        "search_process_receipt_sha256",
        "authorization_receipt_sha256",
        "active_config_path",
        "effective_config_sha256",
        "data_dir",
        "current_cutoff_utc",
        "source_accessed_at_utc",
        "causal_precommit_payload",
        "causal_evidence_payload",
        "causal_reconciliation_payload",
        "symbol_roster_sha256",
        "timeframe_roster_sha256",
    }
    if type(owned) is not dict or set(owned) != expected_fields:
        _fail("CURRENT_WORKER_CONTRACT_FIELDS_INVALID")
    if (
        owned["schema_version"] != SYMBOL_SCHEDULER_SCHEMA
        or _sha(list(book.PINNED_SYMBOL_ORDER))
        != owned["symbol_roster_sha256"]
        or _sha(list(book.TIMEFRAME_ORDER))
        != owned["timeframe_roster_sha256"]
        or symbol not in book.PINNED_SYMBOL_ORDER
    ):
        _fail("CURRENT_WORKER_CONTRACT_ROSTER_INVALID", symbol)
    context = authority_spool.reopen_generation_context(
        Path(owned["spool_root"]),
        expected_generation_id=owned["generation_id"],
        expected_native_universe_sha256=owned["native_universe_sha256"],
        expected_config_sha256=owned["config_sha256"],
    )
    if context.context_sha256 != owned["generation_context_sha256"]:
        _fail("CURRENT_WORKER_CONTEXT_CHANGED", symbol)
    process = planner.freeze_production_search_process(
        context,
        run_id=owned["run_id"],
    )
    if process.receipt_sha256 != owned["search_process_receipt_sha256"]:
        _fail("CURRENT_WORKER_SEARCH_PROCESS_CHANGED", symbol)
    authorization = planner.freeze_current_search_authorization(
        context,
        process=process,
        causal_precommit_payload=owned["causal_precommit_payload"],
        causal_evidence_payload=owned["causal_evidence_payload"],
        causal_reconciliation_payload=owned["causal_reconciliation_payload"],
        current_cutoff_utc=owned["current_cutoff_utc"],
    )
    if authorization.receipt_sha256 != owned["authorization_receipt_sha256"]:
        _fail("CURRENT_WORKER_AUTHORIZATION_CHANGED", symbol)
    config_payload, supplied_config = config_binding.load_effective_pipeline_config(
        Path(owned["active_config_path"])
    )
    frozen_config = config_binding.freeze_production_pipeline_config(
        context,
        active_config_path=Path(owned["active_config_path"]),
        config_payload=config_payload,
        supplied_config=supplied_config,
    )
    if frozen_config.effective_config_sha256 != owned["effective_config_sha256"]:
        _fail("CURRENT_WORKER_EFFECTIVE_CONFIG_CHANGED", symbol)
    config = frozen_config.materialize(context)
    try:
        configured_data_dir = Path(config.data_dir).resolve(strict=True)
        expected_data_dir = Path(owned["data_dir"]).resolve(strict=True)
    except Exception as exc:
        raise CurrentRefitSearchProducerError(
            "CURRENT_WORKER_DATA_ROOT_INVALID",
            (symbol, type(exc).__name__, str(exc)),
        ) from exc
    if configured_data_dir != expected_data_dir or not expected_data_dir.is_dir():
        _fail("CURRENT_WORKER_DATA_ROOT_CHANGED", symbol)
    sink = authority_spool.open_symbol_sink(
        context,
        symbol,
        manifest_domain=authority_spool.CURRENT_REFIT_MANIFEST_DOMAIN,
    )
    timeframe_bindings: list[dict[str, Any]] = []
    for timeframe in book.TIMEFRAME_ORDER:
        try:
            product = native_prefix.produce_activated_native_prefix(
                data_dir=expected_data_dir,
                context=context,
                symbol=symbol,
                timeframe=timeframe,
                cutoff_utc=owned["current_cutoff_utc"],
                source_accessed_at_utc=owned["source_accessed_at_utc"],
                production_contract_required=True,
            )
            if product.source_state not in {
                native_prefix.AVAILABLE,
                native_prefix.SOURCE_UNAVAILABLE,
                native_prefix.TECHNICAL_TERMINAL,
            }:
                _fail(
                    "CURRENT_NATIVE_PREFIX_TECHNICAL_FAILURE",
                    symbol,
                    timeframe,
                    product.native_prefix.get("source_reason"),
                )
            evidence_batch, selected_batch, raw_artifacts = (
                _evaluate_current_timeframe(
                context=context,
                process=process,
                authorization=authorization,
                sink=sink,
                config=config,
                product=product,
                symbol=symbol,
                timeframe=timeframe,
            )
            )
            timeframe_bindings.append(
                _spool_timeframe_bundle(
                    sink=sink,
                    context=context,
                    process=process,
                    authorization=authorization,
                    cutoff_utc=_utc_plus(
                        owned["current_cutoff_utc"], "CURRENT_CUTOFF_INVALID"
                    ),
                    symbol=symbol,
                    timeframe=timeframe,
                    evidence_batch=evidence_batch,
                    selected_batch=selected_batch,
                    raw_artifacts=raw_artifacts,
                )
            )
        finally:
            _release_timeframe_state()
    frozen_config.verify_current(context)
    symbol_index = {
        "schema_version": SYMBOL_BUNDLE_SCHEMA,
        "generation_context_sha256": context.context_sha256,
        "search_process_receipt_sha256": process.receipt_sha256,
        "authorization_receipt_sha256": authorization.receipt_sha256,
        "current_cutoff_utc": _utc_plus(
            owned["current_cutoff_utc"], "CURRENT_CUTOFF_INVALID"
        ),
        "symbol": symbol,
        "timeframe_order": list(book.TIMEFRAME_ORDER),
        "timeframe_artifacts": timeframe_bindings,
    }
    index_entry = sink.put_json(
        "current-refit/worker-evidence/index",
        "current_refit_worker_symbol_index",
        symbol_index,
    )
    compact_receipt = sink.seal()
    return {
        "schema_version": SYMBOL_SCHEDULER_SCHEMA,
        "symbol": symbol,
        "index": _artifact_binding(index_entry),
        "compact_receipt": compact_receipt,
    }


def _manifest_artifact(
    manifest: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    artifact_type: str,
) -> dict[str, Any]:
    if (
        type(binding) is not dict
        or set(binding) != {"artifact_id", "artifact_type", "sha256", "size_bytes"}
        or binding.get("artifact_type") != artifact_type
    ):
        _fail("CURRENT_WORKER_ARTIFACT_BINDING_INVALID")
    matches = [
        entry
        for entry in manifest.get("artifacts", ())
        if type(entry) is dict and entry.get("artifact_id") == binding["artifact_id"]
    ]
    if len(matches) != 1:
        _fail("CURRENT_WORKER_ARTIFACT_ROSTER_MISSING", binding.get("artifact_id"))
    entry = matches[0]
    descriptor = entry.get("descriptor")
    if (
        entry.get("artifact_type") != artifact_type
        or binding["artifact_type"] != artifact_type
        or type(descriptor) is not dict
        or descriptor.get("sha256") != binding.get("sha256")
        or descriptor.get("size_bytes") != binding.get("size_bytes")
    ):
        _fail("CURRENT_WORKER_ARTIFACT_ROSTER_MISMATCH", binding.get("artifact_id"))
    return entry


def _require_source_capture_manifest_entries(
    manifest: Mapping[str, Any],
    capture: Mapping[str, Any],
    *,
    code: str,
) -> None:
    if type(capture) is not dict:
        _fail(code)
    source = capture.get("source_snapshot")
    source_entry = capture.get("source_spool_entry")
    bars_entry = capture.get("bars_spool_entry")
    if any(type(value) is not dict for value in (source, source_entry, bars_entry)):
        _fail(code)
    artifacts = manifest.get("artifacts", ())
    if any(
        sum(
            1
            for candidate in artifacts
            if type(candidate) is dict and candidate == entry
        )
        != 1
        for entry in (source_entry, bars_entry)
    ):
        _fail(code)
    source_spool_descriptor = source_entry.get("descriptor")
    bars_spool_descriptor = bars_entry.get("descriptor")
    if any(
        type(value) is not dict
        for value in (source_spool_descriptor, bars_spool_descriptor)
    ):
        _fail(code)
    try:
        expected_source_raw = current_execution.build_raw_artifact_descriptor(
            relative_path=str(source_spool_descriptor["relative_path"]),
            size_bytes=int(source_spool_descriptor["size_bytes"]),
            content_sha256=_hex64(
                source_spool_descriptor["sha256"],
                code,
            ),
            semantic_sha256=current_artifacts.canonical_artifact_sha256(source),
            format_version=planner.CURRENT_SOURCE_SNAPSHOT_SCHEMA,
        )
        expected_bars_raw = current_execution.build_raw_artifact_descriptor(
            relative_path=str(bars_spool_descriptor["relative_path"]),
            size_bytes=int(bars_spool_descriptor["size_bytes"]),
            content_sha256=_hex64(
                bars_spool_descriptor["sha256"],
                code,
            ),
            semantic_sha256=_hex64(source.get("bars_content_sha256"), code),
            format_version=planner.CURRENT_BAR_FILE_SCHEMA,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CurrentRefitSearchProducerError(
            code,
            (type(exc).__name__, str(exc)),
        ) from exc
    if (
        capture.get("source_raw_descriptor") != expected_source_raw
        or capture.get("bars_raw_descriptor") != expected_bars_raw
        or source.get("bars_artifact_id") != expected_bars_raw["artifact_id"]
    ):
        _fail(code)


def _read_spooled_json(
    context: authority_spool.GenerationSpoolContext,
    entry: Mapping[str, Any],
    *,
    maximum: int,
) -> dict[str, Any]:
    descriptor = entry.get("descriptor")
    if (
        type(descriptor) is not dict
        or descriptor.get("format") != authority_spool.CANONICAL_JSON_FORMAT
    ):
        _fail("CURRENT_WORKER_ARTIFACT_DESCRIPTOR_INVALID")
    raw = _read_spooled_bytes(context, entry, maximum=maximum)
    try:
        value = json.loads(raw.decode("ascii"))
    except Exception as exc:
        raise CurrentRefitSearchProducerError(
            "CURRENT_WORKER_ARTIFACT_JSON_INVALID",
            (type(exc).__name__, str(exc)),
        ) from exc
    if type(value) is not dict or _canonical_bytes(value, maximum=maximum) != raw:
        _fail("CURRENT_WORKER_ARTIFACT_NONCANONICAL")
    return value


def _read_spooled_bytes(
    context: authority_spool.GenerationSpoolContext,
    entry: Mapping[str, Any],
    *,
    maximum: int,
) -> bytes:
    descriptor = entry.get("descriptor")
    if (
        type(descriptor) is not dict
        or type(descriptor.get("size_bytes")) is not int
        or not 0 < descriptor["size_bytes"] <= maximum
    ):
        _fail("CURRENT_WORKER_ARTIFACT_DESCRIPTOR_INVALID")
    try:
        path = context.root.joinpath(*Path(descriptor["relative_path"]).parts)
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
    except Exception as exc:
        raise CurrentRefitSearchProducerError(
            "CURRENT_WORKER_ARTIFACT_READ_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if (
        len(raw) != descriptor["size_bytes"]
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or hashlib.sha256(raw).hexdigest() != descriptor["sha256"]
    ):
        _fail("CURRENT_WORKER_ARTIFACT_CHANGED")
    return raw


def _read_spooled_ascii_object(
    context: authority_spool.GenerationSpoolContext,
    entry: Mapping[str, Any],
    *,
    maximum: int,
) -> tuple[dict[str, Any], bytes]:
    raw = _read_spooled_bytes(context, entry, maximum=maximum)
    try:
        value = json.loads(raw.decode("ascii"))
    except Exception as exc:
        raise CurrentRefitSearchProducerError(
            "CURRENT_WORKER_ARTIFACT_JSON_INVALID",
            (type(exc).__name__, str(exc)),
        ) from exc
    if type(value) is not dict or _canonical_bytes(value, maximum=maximum) != raw:
        _fail("CURRENT_WORKER_ARTIFACT_NONCANONICAL")
    return value, raw


def _envelope_payload(value: Any, code: str) -> dict[str, Any]:
    if (
        type(value) is not dict
        or set(value) != {"payload", "receipt_sha256"}
        or type(value.get("payload")) is not dict
    ):
        _fail(code)
    encoded = _canonical_bytes(value["payload"])
    receipt = _hex64(value.get("receipt_sha256"), code)
    if hashlib.sha256(encoded).hexdigest() != receipt:
        _fail(code)
    return value["payload"]


def _rebuild_source_decision(value: Any) -> book.HistoricalSourceGroupDecision:
    payload = _envelope_payload(value, "CURRENT_WORKER_SOURCE_DECISION_INVALID")
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
    if decision is None or _owned_envelope(decision) != value:
        _fail("CURRENT_WORKER_SOURCE_DECISION_CHANGED")
    return decision


def _rebuild_regime_fit_decision(
    value: Any,
    source: book.HistoricalSourceGroupDecision,
) -> book.RegimeFitDecision:
    payload = _envelope_payload(value, "CURRENT_WORKER_REGIME_FIT_INVALID")
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
    if decision is None or _owned_envelope(decision) != value:
        _fail("CURRENT_WORKER_REGIME_FIT_CHANGED")
    return decision


def _rebuild_winner_request(value: Any, code: str) -> winner.WinnerSelectionRequest:
    if (
        type(value) is not dict
        or set(value)
        != {"contract_version", "precommitment", "attempt_ledger", "evidence_ledger"}
    ):
        _fail(code)
    wrappers = []
    for name in ("precommitment", "attempt_ledger", "evidence_ledger"):
        wrapper = value.get(name)
        if (
            type(wrapper) is not dict
            or set(wrapper) != {"payload", "payload_sha256"}
            or type(wrapper.get("payload")) is not dict
            or _sha(wrapper["payload"]) != wrapper.get("payload_sha256")
        ):
            _fail(code)
        wrappers.append(wrapper["payload"])
    try:
        request = winner.create_winner_selection_request(*wrappers)
    except ValueError as exc:
        raise CurrentRefitSearchProducerError(code, (type(exc).__name__, str(exc))) from exc
    if request is None or request.to_dict() != value:
        _fail(code)
    return request


def _rebuild_horizon_plan(value: Any) -> Any:
    if (
        type(value) is not dict
        or set(value) != {"payload", "receipt_sha256"}
        or type(value.get("payload")) is not dict
    ):
        _fail("CURRENT_WORKER_HORIZON_PLAN_INVALID")
    body = value["payload"]
    unobserved = "regime_support_binding" in body
    if body.get("schema_version") == training_horizon._PLAN_SCHEMA:  # noqa: SLF001
        request_schema = training_horizon._REQUEST_SCHEMA  # noqa: SLF001
        unobserved_schema = training_horizon._UNOBSERVED_REQUEST_SCHEMA  # noqa: SLF001
    elif body.get("schema_version") == training_horizon._CURRENT_PLAN_SCHEMA:  # noqa: SLF001
        request_schema = training_horizon._CURRENT_REQUEST_SCHEMA  # noqa: SLF001
        unobserved_schema = training_horizon._CURRENT_UNOBSERVED_REQUEST_SCHEMA  # noqa: SLF001
    else:
        _fail("CURRENT_WORKER_HORIZON_PLAN_INVALID")
    request = {
        "schema_version": unobserved_schema if unobserved else request_schema,
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
        raise CurrentRefitSearchProducerError(
            "CURRENT_WORKER_HORIZON_PLAN_REBUILD_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if (
        plan.to_dict() != body
        or plan.receipt_sha256
        != _hex64(
            value.get("receipt_sha256"),
            "CURRENT_WORKER_HORIZON_PLAN_RECEIPT_INVALID",
        )
    ):
        _fail("CURRENT_WORKER_HORIZON_PLAN_CHANGED")
    return plan


def _resolve_raw_artifacts(
    manifest: Mapping[str, Any],
    value: Any,
    *,
    required: bool,
) -> dict[str, dict[str, Any]] | None:
    if value is None:
        if required:
            _fail("CURRENT_WORKER_RAW_ARTIFACT_BINDING_MISSING")
        return None
    if (
        type(value) is not dict
        or set(value)
        != {
            "winner_request",
            "winner_decision",
            "production_receipt",
            "cross_binding",
        }
    ):
        _fail("CURRENT_WORKER_RAW_ARTIFACT_BINDING_INVALID")
    request_type = value.get("winner_request", {}).get("artifact_type") if type(value.get("winner_request")) is dict else None
    edition_types = {
        "winner_v14_request": "winner_v14_decision",
        "winner_v16_request": "winner_v16_decision",
    }
    if request_type not in edition_types:
        _fail("CURRENT_WORKER_RAW_ARTIFACT_BINDING_INVALID")
    specifications = {
        "winner_request": (request_type, True),
        "winner_decision": (edition_types[request_type], True),
        "production_receipt": (
            "winner_raw_evidence_production_receipt",
            True,
        ),
        "cross_binding": ("current_horizon_winner_cross_binding", True),
    }
    entries: dict[str, dict[str, Any]] = {}
    for name, (artifact_type, required_entry) in specifications.items():
        binding = value.get(name)
        if binding is None:
            if required_entry:
                _fail("CURRENT_WORKER_RAW_ARTIFACT_BINDING_MISSING", name)
            continue
        entries[name] = _manifest_artifact(
            manifest,
            binding,
            artifact_type=artifact_type,
        )
    return entries


def _rebuild_raw_winner_artifacts(
    *,
    context: authority_spool.GenerationSpoolContext,
    manifest: Mapping[str, Any],
    bindings: Any,
    symbol: str,
    timeframe: str,
    regime: str,
    expected_decision_sha256: str,
) -> tuple[
    winner.WinnerSelectionRequest,
    winner.WinnerSelectionDecision,
    bytes,
    bytes,
]:
    entries = _resolve_raw_artifacts(manifest, bindings, required=True)
    assert entries is not None
    request_body, request_bytes = _read_spooled_ascii_object(
        context,
        entries["winner_request"],
        maximum=(winner.MAX_SUPPORTED_WINNER_REQUEST_BYTES
                 if entries["winner_request"]["artifact_type"] == "winner_v16_request"
                 else winner.MAX_REQUEST_BYTES),
    )
    if (request_body.get("contract_version") != winner.V16_GENERATION.request_version
            and len(request_bytes) > winner.MAX_REQUEST_BYTES):
        _fail("CURRENT_WORKER_ARTIFACT_DESCRIPTOR_INVALID")
    request = _rebuild_winner_request(
        request_body,
        "CURRENT_WORKER_WINNER_REQUEST_INVALID",
    )
    generation = winner.winner_generation_for_precommitment(request.precommitment.to_dict())
    expected_label = "v16" if generation is winner.V16_GENERATION else "v14"
    if (entries["winner_request"]["artifact_type"] != f"winner_{expected_label}_request"
            or entries["winner_decision"]["artifact_type"] != f"winner_{expected_label}_decision"):
        _fail("CURRENT_WORKER_RAW_ARTIFACT_BINDING_INVALID")
    decision = winner.evaluate_historical_cell_winner(request)
    decision_body, _ = _read_spooled_ascii_object(
        context,
        entries["winner_decision"],
        maximum=winner.MAX_DECISION_BYTES,
    )
    expected_decision = _hex64(
        expected_decision_sha256,
        "CURRENT_WORKER_WINNER_DECISION_INVALID",
    )
    if (
        not winner.verify_historical_cell_winner_decision(decision, request)
        or decision.to_dict() != decision_body
        or decision.receipt_sha256 != expected_decision
    ):
        _fail("CURRENT_WORKER_WINNER_DECISION_CHANGED", symbol, timeframe, regime)
    production_body, production_raw = _read_spooled_ascii_object(
        context,
        entries["production_receipt"],
        maximum=MAX_TIMEFRAME_BUNDLE_BYTES,
    )
    cross_binding = _read_spooled_bytes(
        context,
        entries["cross_binding"],
        maximum=MAX_TIMEFRAME_BUNDLE_BYTES,
    )
    if generation is winner.V16_GENERATION:
        try:
            cross_body = json.loads(cross_binding.decode("ascii"))
        except (UnicodeError, ValueError) as exc:
            raise CurrentRefitSearchProducerError("CURRENT_WORKER_CROSS_BINDING_INVALID") from exc
        if (type(cross_body) is not dict
                or cross_body.get("schema_version") != raw_winner.CURRENT_HORIZON_WINNER_CROSS_BINDING_SCHEMA_V5
                or cross_body.get("mode") != raw_winner.CURRENT_CROSS_BINDING_MODE
                or _canonical_bytes(cross_body) != cross_binding):
            _fail("CURRENT_WORKER_CROSS_BINDING_INVALID")
    roster = production_body.get("artifact_roster")
    if (
        production_body.get("schema_version") != (raw_winner.V16_PRODUCTION_SCHEMA
            if generation is winner.V16_GENERATION else raw_winner.PRODUCTION_SCHEMA)
        or production_body.get("generation_context_sha256")
        != context.context_sha256
        or production_body.get("symbol") != symbol
        or production_body.get("timeframe") != timeframe
        or production_body.get("regime") != regime
        or production_body.get("request_sha256") != request.request_sha256
        or production_body.get("decision_receipt_sha256")
        != decision.receipt_sha256
        or production_body.get("provenance_cross_binding_sha256")
        != hashlib.sha256(cross_binding).hexdigest()
        or type(roster) is not dict
        or roster.get("winner-request")
        != entries["winner_request"].get("descriptor")
        or roster.get("winner-decision")
        != entries["winner_decision"].get("descriptor")
    ):
        _fail("CURRENT_WORKER_PRODUCTION_RECEIPT_SPLICE", symbol, timeframe, regime)
    return request, decision, cross_binding, production_raw


def _rebuild_timeframe_evidence(
    *,
    context: authority_spool.GenerationSpoolContext,
    manifest: Mapping[str, Any],
    index: Mapping[str, Any],
    process_receipt_sha256: str,
    authorization_receipt_sha256: str,
    cutoff_utc: str,
    symbol: str,
    timeframe: str,
) -> tuple[list[book.HistoricalBookCellEvidence], list[dict[str, Any]]]:
    expected_index_fields = {
        "schema_version",
        "generation_context_sha256",
        "search_process_receipt_sha256",
        "authorization_receipt_sha256",
        "current_cutoff_utc",
        "symbol",
        "timeframe",
        "source_group",
        "regime_fit",
        "regime_order",
        "cell_artifacts",
    }
    if (
        type(index) is not dict
        or set(index) != expected_index_fields
        or index["schema_version"] != TIMEFRAME_BUNDLE_SCHEMA
        or index["generation_context_sha256"] != context.context_sha256
        or index["search_process_receipt_sha256"] != process_receipt_sha256
        or index["authorization_receipt_sha256"]
        != authorization_receipt_sha256
        or _utc_plus(index["current_cutoff_utc"], "CURRENT_CUTOFF_INVALID")
        != cutoff_utc
        or index["symbol"] != symbol
        or index["timeframe"] != timeframe
        or index["regime_order"] != list(book.REGIME_ORDER)
        or type(index["cell_artifacts"]) is not list
        or len(index["cell_artifacts"]) != len(book.REGIME_ORDER)
    ):
        _fail("CURRENT_WORKER_TIMEFRAME_INDEX_INVALID", symbol, timeframe)
    source = _rebuild_source_decision(index["source_group"])
    fit = _rebuild_regime_fit_decision(index["regime_fit"], source)
    evidence_batch: list[book.HistoricalBookCellEvidence] = []
    selected_rows: list[dict[str, Any]] = []
    expected_cell_fields = {
        "schema_version",
        "generation_context_sha256",
        "search_process_receipt_sha256",
        "authorization_receipt_sha256",
        "current_cutoff_utc",
        "symbol",
        "timeframe",
        "kind",
        "regime",
        "raw_artifacts",
        "winner_decision_sha256",
        "process_state",
        "selected",
    }
    for regime, binding in zip(book.REGIME_ORDER, index["cell_artifacts"]):
        entry = _manifest_artifact(
            manifest,
            binding,
            artifact_type="current_refit_worker_cell_evidence",
        )
        cell = _read_spooled_json(
            context,
            entry,
            maximum=MAX_TIMEFRAME_BUNDLE_BYTES,
        )
        if (
            set(cell) != expected_cell_fields
            or cell["schema_version"] != TIMEFRAME_BUNDLE_SCHEMA
            or cell["generation_context_sha256"] != context.context_sha256
            or cell["search_process_receipt_sha256"] != process_receipt_sha256
            or cell["authorization_receipt_sha256"]
            != authorization_receipt_sha256
            or _utc_plus(cell["current_cutoff_utc"], "CURRENT_CUTOFF_INVALID")
            != cutoff_utc
            or cell["symbol"] != symbol
            or cell["timeframe"] != timeframe
            or cell["regime"] != regime
        ):
            _fail("CURRENT_WORKER_CELL_BINDING_INVALID", symbol, timeframe, regime)
        if cell["kind"] == "WINNER":
            if cell["process_state"] is not None:
                _fail("CURRENT_WORKER_WINNER_CELL_INVALID", symbol, timeframe, regime)
            request, decision, _cross_binding, _production_raw = (
                _rebuild_raw_winner_artifacts(
                    context=context,
                    manifest=manifest,
                    bindings=cell["raw_artifacts"],
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    expected_decision_sha256=cell["winner_decision_sha256"],
                )
            )
            evidence = book.winner_cell_evidence(decision, request, source, fit)
        elif cell["kind"] == "PROCESS_STATE":
            state = cell["process_state"]
            if (
                type(state) is not dict
                or set(state)
                != {
                    "decision",
                    "horizon_plan",
                    "process_rejection_decision_sha256",
                }
                or cell["winner_decision_sha256"] is not None
            ):
                _fail("CURRENT_WORKER_PROCESS_CELL_INVALID", symbol, timeframe, regime)
            process_payload = _envelope_payload(
                state["decision"],
                "CURRENT_WORKER_PROCESS_DECISION_INVALID",
            )
            horizon_plan = (
                None
                if state["horizon_plan"] is None
                else _rebuild_horizon_plan(state["horizon_plan"])
            )
            rejection_request = None
            rejection_decision = None
            cross_binding = None
            raw_receipt = None
            if state["process_rejection_decision_sha256"] is not None:
                (
                    rejection_request,
                    rejection_decision,
                    cross_binding,
                    raw_receipt,
                ) = _rebuild_raw_winner_artifacts(
                    context=context,
                    manifest=manifest,
                    bindings=cell["raw_artifacts"],
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                    expected_decision_sha256=state[
                        "process_rejection_decision_sha256"
                    ],
                )
            elif cell["raw_artifacts"] is not None:
                _fail(
                    "CURRENT_WORKER_PROCESS_ARTIFACT_ORPHANED",
                    symbol,
                    timeframe,
                    regime,
                )
            process_cell = process_payload.get("cell")
            if type(process_cell) is not dict:
                _fail(
                    "CURRENT_WORKER_PROCESS_DECISION_INVALID",
                    symbol,
                    timeframe,
                    regime,
                )
            binding_schema = (process_payload.get("insufficient_evidence_binding") or {}).get("schema_version")
            state_factory = (book.create_transport_historical_process_state_decision
                if binding_schema == book.V16_GENERATION.insufficient_schema else book.create_historical_process_state_decision)
            process_decision = state_factory(
                symbol=process_cell.get("symbol"),
                timeframe=process_cell.get("timeframe"),
                regime=process_cell.get("regime"),
                state_kind=process_payload.get("state_kind"),
                reason_code=process_payload.get("reason_code"),
                source_group_decision=source,
                regime_fit_decision=fit,
                attempted_trials_exact=process_payload.get(
                    "attempted_trials_exact"
                ),
                horizon_plan=horizon_plan,
                process_rejection_request=rejection_request,
                process_rejection_decision=rejection_decision,
                horizon_winner_cross_binding_bytes=cross_binding,
                winner_raw_production_receipt_bytes=raw_receipt,
            )
            if (
                process_decision is None
                or _owned_envelope(process_decision) != state["decision"]
            ):
                _fail(
                    "CURRENT_WORKER_PROCESS_DECISION_CHANGED",
                    symbol,
                    timeframe,
                    regime,
                )
            evidence = book.process_state_cell_evidence(
                process_state_decision=process_decision,
                source_group_decision=source,
                regime_fit_decision=fit,
                horizon_plan=horizon_plan,
                process_rejection_request=rejection_request,
                process_rejection_decision=rejection_decision,
                horizon_winner_cross_binding_bytes=cross_binding,
                winner_raw_production_receipt_bytes=raw_receipt,
            )
        else:
            _fail("CURRENT_WORKER_CELL_KIND_INVALID", symbol, timeframe, regime)
        if (
            evidence is None
            or evidence.symbol != symbol
            or evidence.timeframe != timeframe
            or evidence.regime != regime
        ):
            _fail("CURRENT_WORKER_EVIDENCE_REVERIFY_FAILED", symbol, timeframe, regime)
        selected_row = cell["selected"]
        if selected_row is not None:
            if (
                cell["kind"] != "WINNER"
                or type(selected_row) is not dict
                or set(selected_row)
                != {"artifacts", "source_capture", "winner_decision_sha256"}
                or selected_row["winner_decision_sha256"]
                != cell["winner_decision_sha256"]
            ):
                _fail("CURRENT_WORKER_SELECTED_CAPTURE_INVALID", symbol, timeframe, regime)
            selected_rows.append(
                {
                    "regime": regime,
                    "artifacts": selected_row["artifacts"],
                    "source_capture": selected_row["source_capture"],
                    "winner_decision_sha256": selected_row[
                        "winner_decision_sha256"
                    ],
                }
            )
        evidence_batch.append(evidence)
    return evidence_batch, selected_rows


def _run_symbol_scheduler(
    inputs: _ExecutableCurrentRefitInputs,
    *,
    request: Mapping[str, Any],
    authorization: planner.CurrentSearchAuthorization,
    current_cutoff: str,
) -> dict[str, Any]:
    scheduler = inputs.symbol_scheduler
    if type(scheduler) is not CurrentRefitSymbolSchedulerInputs:
        _fail("CURRENT_SYMBOL_SCHEDULER_INPUT_INVALID")
    requested_workers = min(
        max(1, int(scheduler.requested_workers)),
        len(book.PINNED_SYMBOL_ORDER),
    )
    contract = {
        "schema_version": SYMBOL_SCHEDULER_SCHEMA,
        "spool_root": str(inputs.context.root),
        "generation_id": inputs.context.generation_id,
        "native_universe_sha256": inputs.context.native_universe_sha256,
        "config_sha256": inputs.context.config_sha256,
        "generation_context_sha256": inputs.context.context_sha256,
        "run_id": inputs.search_process.to_dict()["run_id"],
        "search_process_receipt_sha256": inputs.search_process.receipt_sha256,
        "authorization_receipt_sha256": authorization.receipt_sha256,
        "active_config_path": str(scheduler.active_config_path),
        "effective_config_sha256": config_binding.freeze_production_pipeline_config(
            inputs.context,
            active_config_path=scheduler.active_config_path,
            config_payload=config_binding.load_effective_pipeline_config(
                scheduler.active_config_path
            )[0],
            supplied_config=inputs.pipeline_config,
        ).effective_config_sha256,
        "data_dir": str(scheduler.data_dir),
        "current_cutoff_utc": current_cutoff,
        "source_accessed_at_utc": scheduler.source_accessed_at_utc,
        "causal_precommit_payload": request["causal_precommit_payload"],
        "causal_evidence_payload": request["causal_evidence_payload"],
        "causal_reconciliation_payload": request["causal_reconciliation_payload"],
        "symbol_roster_sha256": _sha(list(book.PINNED_SYMBOL_ORDER)),
        "timeframe_roster_sha256": _sha(list(book.TIMEFRAME_ORDER)),
    }
    results: dict[str, Any] = {}
    symbols = list(book.PINNED_SYMBOL_ORDER)
    governor = memory_governor.MemoryGovernor(
        requested_workers=requested_workers,
        termination_grace_seconds=SYMBOL_SCHEDULER_TERMINATION_SECONDS,
    )
    tasks = (
        memory_governor.MemoryTask(
            key=symbol,
            args=(contract, symbol),
        )
        for symbol in symbols
    )
    outcomes = governor.iter_completed(_current_refit_symbol_worker, tasks)
    try:
        for outcome in outcomes:
            if (
                outcome.status != "COMPLETED"
                or outcome.technical_failure
                or type(outcome.value) is not dict
                or outcome.task_key not in symbols
                or outcome.task_key in results
                or outcome.value.get("symbol") != outcome.task_key
            ):
                _fail(
                    "CURRENT_SYMBOL_WORKER_FAILED",
                    outcome.task_key,
                    outcome.status,
                    outcome.error_type,
                    outcome.error_message,
                )
            results[outcome.task_key] = outcome.value
    finally:
        outcomes.close()
    if set(results) != set(book.PINNED_SYMBOL_ORDER):
        missing = [
            symbol for symbol in book.PINNED_SYMBOL_ORDER if symbol not in results
        ]
        _fail("CURRENT_SYMBOL_WORKER_ROSTER_INCOMPLETE", *missing[:8])
    # Completion order is deliberately irrelevant, but no extra key may exist.
    return {symbol: results[symbol] for symbol in book.PINNED_SYMBOL_ORDER}


def _scheduled_book_evidence_stream(
    inputs: _ExecutableCurrentRefitInputs,
    *,
    request: Mapping[str, Any],
    authorization: planner.CurrentSearchAuthorization,
    current_cutoff: str,
    selected: dict[str, _SelectedSearch],
    compact_receipts: dict[str, Any],
) -> Iterable[book.HistoricalBookCellEvidence]:
    scheduler = inputs.symbol_scheduler
    if type(scheduler) is not CurrentRefitSymbolSchedulerInputs:
        _fail("CURRENT_SYMBOL_SCHEDULER_INPUT_INVALID")
    results = _run_symbol_scheduler(
        inputs,
        request=request,
        authorization=authorization,
        current_cutoff=current_cutoff,
    )
    for symbol in book.PINNED_SYMBOL_ORDER:
        result = results.get(symbol)
        if (
            type(result) is not dict
            or set(result) != {
                "schema_version",
                "symbol",
                "index",
                "compact_receipt",
            }
            or result["schema_version"] != SYMBOL_SCHEDULER_SCHEMA
            or result["symbol"] != symbol
        ):
            _fail("CURRENT_SYMBOL_WORKER_RESULT_INVALID", symbol)
        manifest = authority_spool.verify_symbol_receipt(
            inputs.context.root,
            result["compact_receipt"],
            expected_generation_id=inputs.context.generation_id,
            expected_symbol=symbol,
            expected_native_universe_sha256=inputs.context.native_universe_sha256,
            expected_config_sha256=inputs.context.config_sha256,
            expected_manifest_domain=authority_spool.CURRENT_REFIT_MANIFEST_DOMAIN,
        )
        compact_receipts[symbol] = result["compact_receipt"]
        symbol_entry = _manifest_artifact(
            manifest,
            result["index"],
            artifact_type="current_refit_worker_symbol_index",
        )
        symbol_index = _read_spooled_json(
            inputs.context,
            symbol_entry,
            maximum=MAX_SYMBOL_BUNDLE_BYTES,
        )
        expected_symbol_fields = {
            "schema_version",
            "generation_context_sha256",
            "search_process_receipt_sha256",
            "authorization_receipt_sha256",
            "current_cutoff_utc",
            "symbol",
            "timeframe_order",
            "timeframe_artifacts",
        }
        if (
            set(symbol_index) != expected_symbol_fields
            or symbol_index["schema_version"] != SYMBOL_BUNDLE_SCHEMA
            or symbol_index["generation_context_sha256"]
            != inputs.context.context_sha256
            or symbol_index["search_process_receipt_sha256"]
            != inputs.search_process.receipt_sha256
            or symbol_index["authorization_receipt_sha256"]
            != authorization.receipt_sha256
            or _utc_plus(
                symbol_index["current_cutoff_utc"], "CURRENT_CUTOFF_INVALID"
            )
            != current_cutoff
            or symbol_index["symbol"] != symbol
            or symbol_index["timeframe_order"] != list(book.TIMEFRAME_ORDER)
            or type(symbol_index["timeframe_artifacts"]) is not list
            or len(symbol_index["timeframe_artifacts"]) != len(book.TIMEFRAME_ORDER)
        ):
            _fail("CURRENT_WORKER_SYMBOL_INDEX_INVALID", symbol)
        for timeframe, binding in zip(
            book.TIMEFRAME_ORDER,
            symbol_index["timeframe_artifacts"],
        ):
            timeframe_entry = _manifest_artifact(
                manifest,
                binding,
                artifact_type="current_refit_worker_timeframe_index",
            )
            timeframe_index = _read_spooled_json(
                inputs.context,
                timeframe_entry,
                maximum=MAX_SYMBOL_BUNDLE_BYTES,
            )
            evidence_batch, selected_rows = _rebuild_timeframe_evidence(
                context=inputs.context,
                manifest=manifest,
                index=timeframe_index,
                process_receipt_sha256=inputs.search_process.receipt_sha256,
                authorization_receipt_sha256=authorization.receipt_sha256,
                cutoff_utc=current_cutoff,
                symbol=symbol,
                timeframe=timeframe,
            )
            for row in selected_rows:
                key = f"{symbol}|{timeframe}|{row['regime']}"
                if key in selected:
                    _fail("CURRENT_SELECTED_SEARCH_DUPLICATE", key)
                capture = row["source_capture"]
                if type(capture) is not dict:
                    _fail("CURRENT_SELECTED_SOURCE_CAPTURE_INVALID", key)
                _require_source_capture_manifest_entries(
                    manifest,
                    capture,
                    code="CURRENT_SELECTED_SOURCE_DESCRIPTOR_UNBOUND",
                )
                selected[key] = _SelectedSearch(
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=row["regime"],
                    artifacts=row["artifacts"],
                    source_capture=capture,
                    winner_decision_sha256=row["winner_decision_sha256"],
                )
            yield from evidence_batch


def _raw_descriptor(
    entry: Mapping[str, Any],
    *,
    semantic_sha256: str,
    format_version: str,
) -> dict[str, Any]:
    descriptor = entry.get("descriptor")
    if type(descriptor) is not dict:
        _fail("CURRENT_CAPTURE_DESCRIPTOR_INVALID")
    try:
        return current_execution.build_raw_artifact_descriptor(
            relative_path=descriptor["relative_path"],
            size_bytes=descriptor["size_bytes"],
            content_sha256=descriptor["sha256"],
            semantic_sha256=semantic_sha256,
            format_version=format_version,
        )
    except Exception as exc:
        raise CurrentRefitSearchProducerError(
            "CURRENT_CAPTURE_RAW_DESCRIPTOR_INVALID",
            (type(exc).__name__, str(exc)),
        ) from exc


def _owned_envelope(decision: Any) -> dict[str, Any]:
    value = decision.to_dict()
    receipt = value.pop("receipt_sha256", None)
    if receipt != _sha(value):
        _fail("CURRENT_OWNED_DECISION_RECEIPT_INVALID")
    return {"payload": value, "receipt_sha256": receipt}


def _selected_book_cells(
    current_book: Mapping[str, Any],
    selected_search: Mapping[str, _SelectedSearch],
) -> list[Mapping[str, Any]]:
    cells = current_book.get("cells")
    if type(cells) is not list:
        _fail("CURRENT_BOOK_CELL_ROSTER_INVALID")
    selected_cells = [
        cell
        for cell in cells
        if type(cell) is dict
        and cell.get("historical_local_candidate_pending_causal") is True
    ]
    if selected_cells:
        return selected_cells
    if (
        current_book.get("status") != "NO_LOCAL_WINNERS"
        or current_book.get("reason_codes")
        != ["COMPLETE_FAMILY_HAS_NO_LOCAL_WINNERS"]
        or type(current_book.get("historical_local_candidate_count")) is not int
        or current_book.get("historical_local_candidate_count") != 0
        or current_book.get("historical_local_candidate_cell_ids") != []
        or bool(selected_search)
    ):
        _fail("CURRENT_BOOK_EMPTY_SELECTION_CONTRACT_INVALID")
    raise CurrentRefitSearchNotPromotable(
        "CURRENT_BOOK_HAS_NO_ELIGIBLE_WINNERS",
        ("NO_LOCAL_WINNERS", "COMPLETE_FAMILY_HAS_NO_LOCAL_WINNERS"),
    )


def _materialize_selected(
    *,
    sink: authority_spool.SymbolAuthoritySink,
    current_book: Mapping[str, Any],
    selected_cells: Sequence[Mapping[str, Any]],
    selected_search: Mapping[str, _SelectedSearch],
    current_cutoff: str,
) -> tuple[list[_SelectedMaterial], dict[str, Any], dict[str, Any], dict[str, Any]]:
    materials: list[_SelectedMaterial] = []
    source_by_group: dict[tuple[str, str], str] = {}
    source_entries: list[dict[str, Any]] = []
    horizon_entries: list[dict[str, Any]] = []
    regime_groups: dict[tuple[str, str], dict[str, Any]] = {}
    for cell in selected_cells:
        key = f"{cell['symbol']}|{cell['timeframe']}|{cell['regime']}"
        search = selected_search.get(key)
        if search is None:
            _fail("CURRENT_BOOK_WINNER_SEARCH_CAPTURE_MISSING", key)
        artifacts = search.artifacts
        process_body = artifacts["winner_process"]
        horizon_body = artifacts["horizon_process"]
        trial_body = artifacts["trial_definition"]
        recipe_body = artifacts["refit_recipe"]
        process_sha = current_artifacts.canonical_artifact_sha256(process_body)
        if (
            process_body["process_candidate_id"] != cell["local_process_candidate_id"]
            or process_sha != cell["local_process_sha256"]
            or search.winner_decision_sha256 != cell["winner_decision_sha256"]
        ):
            _fail("CURRENT_SELECTED_PROCESS_BOOK_SPLICE", key)
        prefix = f"current-refit/selected/{cell['symbol']}/{cell['timeframe']}/{cell['regime']}"
        role_specs = {
            "winner_process": (
                current_production.ART_WINNER_PROCESS,
                process_body,
                "fxpm.current-refit-winner-process.v1",
            ),
            "horizon_process": (
                current_production.ART_HORIZON_PROCESS,
                horizon_body,
                "fxpm.current-refit-horizon-process.v2",
            ),
            "trial_definition": (
                current_production.ART_TRIAL,
                trial_body,
                "fxpm.current-refit-trial-definition.v1",
            ),
            "refit_recipe": (
                current_production.ART_RECIPE,
                recipe_body,
                "fxpm.current-refit-recipe.v1",
            ),
            "winner_book_entry": (
                current_production.ART_BOOK_ENTRY,
                cell,
                BOOK_ENTRY_SCHEMA,
            ),
        }
        descriptors: dict[str, dict[str, Any]] = {}
        for role, (artifact_type, body, version) in role_specs.items():
            semantic = (
                _hex64(
                    body.get("historical_book_entry_sha256"),
                    "CURRENT_BOOK_ENTRY_SEMANTIC_SHA256_INVALID",
                )
                if role == "winner_book_entry"
                else current_artifacts.canonical_artifact_sha256(body)
            )
            entry = sink.put_json(f"{prefix}/{role}", artifact_type, body)
            descriptors[role] = _raw_descriptor(
                entry,
                semantic_sha256=semantic,
                format_version=version,
            )
        capture = search.source_capture
        source = capture.get("source_snapshot")
        source_descriptor = capture.get("source_raw_descriptor")
        bars_descriptor = capture.get("bars_raw_descriptor")
        if not all(
            type(value) is dict
            for value in (source, source_descriptor, bars_descriptor)
        ):
            _fail("CURRENT_SELECTED_SOURCE_CAPTURE_INVALID", key)
        descriptors["source_snapshot"] = dict(source_descriptor)
        descriptors["bars"] = dict(bars_descriptor)
        source_sha = current_artifacts.canonical_artifact_sha256(source)
        group = (cell["symbol"], cell["timeframe"])
        previous = source_by_group.setdefault(group, source_sha)
        if previous != source_sha:
            _fail("CURRENT_SOURCE_DIFFERS_ACROSS_REGIMES", *group)
        if previous == source_sha and not any(
            row["symbol"] == group[0] and row["timeframe"] == group[1]
            for row in source_entries
        ):
            source_entries.append(
                {
                    "symbol": group[0],
                    "timeframe": group[1],
                    "source_snapshot_sha256": source_sha,
                }
            )
        horizon_sha = current_artifacts.canonical_artifact_sha256(horizon_body)
        horizon_entries.append(
            {
                "symbol": cell["symbol"],
                "timeframe": cell["timeframe"],
                "regime": cell["regime"],
                "horizon_process_sha256": horizon_sha,
            }
        )
        fit_envelope = next(
            value
            for value in current_book["regime_fits"]
            if value["payload"]["symbol"] == group[0]
            and value["payload"]["timeframe"] == group[1]
        )
        regime_groups[group] = fit_envelope
        record = fit_envelope["payload"]["regime_detector_record"]
        detector = {
            "regime_fit_receipt_sha256": fit_envelope["receipt_sha256"],
            "regime_detector_record_sha256": record["record_sha256"],
            "regime_detector_parameters_sha256": record["parameters_sha256"],
            "regime_detector_code_sha256": record["detector_code_sha256"],
        }
        materials.append(
            _SelectedMaterial(
                book_cell=cell,
                search=search,
                winner_process=process_body,
                horizon_process=horizon_body,
                trial_definition=trial_body,
                refit_recipe=recipe_body,
                descriptors=descriptors,
                detector=detector,
            )
        )
    tf_order = {name: index for index, name in enumerate(book.TIMEFRAME_ORDER)}
    source_entries.sort(key=lambda row: (row["symbol"], tf_order[row["timeframe"]]))
    horizon_entries.sort(
        key=lambda row: (row["symbol"], tf_order[row["timeframe"]], row["regime"])
    )
    source_bundle = {
        "schema_version": SOURCE_BUNDLE_SCHEMA,
        "current_cutoff_utc": current_cutoff,
        "entries": source_entries,
    }
    horizon_bundle = {
        "schema_version": HORIZON_BUNDLE_SCHEMA,
        "current_cutoff_utc": current_cutoff,
        "entries": horizon_entries,
    }
    regime_bundle = current_execution.build_current_regime_params_bundle(
        current_cutoff_utc=current_cutoff,
        regime_fits=[
            regime_groups[key]
            for key in sorted(regime_groups, key=lambda x: (x[0], tf_order[x[1]]))
        ],
    )
    return materials, source_bundle, horizon_bundle, regime_bundle


def _winner_roster(materials: Sequence[_SelectedMaterial]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for material in materials:
        cell = material.book_cell
        rows.append(
            {
                "symbol": cell["symbol"],
                "timeframe": cell["timeframe"],
                "regime": cell["regime"],
                "strategy_id": material.winner_process["strategy_id"],
                "winner_process_candidate_id": material.winner_process[
                    "process_candidate_id"
                ],
                "winner_process_sha256": current_artifacts.canonical_artifact_sha256(
                    material.winner_process
                ),
                "winner_horizon_process_sha256": current_artifacts.canonical_artifact_sha256(
                    material.horizon_process
                ),
                "winner_trial_definition_sha256": current_artifacts.canonical_artifact_sha256(
                    material.trial_definition
                ),
                "winner_refit_recipe_sha256": current_artifacts.canonical_artifact_sha256(
                    material.refit_recipe
                ),
                "winner_decision_sha256": cell["winner_decision_sha256"],
                "winner_book_entry_sha256": cell["historical_book_entry_sha256"],
                **material.detector,
            }
        )
    return rows


def _accepted_book_receipt(
    *,
    inputs: _ExecutableCurrentRefitInputs,
    request: Mapping[str, Any],
    current_book: Mapping[str, Any],
    materials: Sequence[_SelectedMaterial],
    source_bundle: Mapping[str, Any],
    horizon_bundle: Mapping[str, Any],
    regime_bundle: Mapping[str, Any],
    current_cutoff: str,
    started_at: str,
    completed_at: str,
) -> dict[str, Any]:
    process = request["historical_process_decision"]
    roster = _winner_roster(materials)
    maximum_input = max(
        material.search.source_capture["source_snapshot"]["latest_closed_bar_utc"]
        for material in materials
    )
    payload = {
        "schema_version": lifecycle.ACCEPTED_BOOK_GENERATION_RECEIPT_VERSION,
        "generation_id": inputs.context.generation_id,
        "historical_process_decision_sha256": request[
            "historical_process_decision_sha256"
        ],
        "process_id": process["process_id"],
        "process_definition_sha256": process["process_definition_sha256"],
        "generation_sha256": "0" * 64,
        "current_cutoff_utc": current_cutoff,
        "maximum_input_timestamp_utc": maximum_input,
        "source_snapshot_bundle_sha256": lifecycle.canonical_receipt_sha256(
            source_bundle
        ),
        "regime_params_sha256": lifecycle.canonical_receipt_sha256(regime_bundle),
        "horizon_decisions_sha256": lifecycle.canonical_receipt_sha256(horizon_bundle),
        "full_cell_outcome_roster_sha256": current_book["book_generation_sha256"],
        "selected_winner_roster_sha256": lifecycle.compute_current_winner_roster_sha256(
            roster
        ),
        "trial_ledger_bundle_sha256": _sha(
            [
                {
                    "cell": f"{value.book_cell['symbol']}|{value.book_cell['timeframe']}|{value.book_cell['regime']}",
                    "trial_definition_sha256": current_artifacts.canonical_artifact_sha256(
                        value.trial_definition
                    ),
                }
                for value in materials
            ]
        ),
        "winner_bundle_sha256": _sha(roster),
        "output_book_sha256": lifecycle.canonical_receipt_sha256(current_book),
        "book_multiplicity_receipt_sha256": current_book["receipt_sha256"],
        "full_process_execution_trace_sha256": _sha(
            {
                "search_process_receipt_sha256": inputs.search_process.receipt_sha256,
                "current_book_generation_sha256": current_book[
                    "book_generation_sha256"
                ],
                "selected_winner_roster_sha256": lifecycle.compute_current_winner_roster_sha256(
                    roster
                ),
            }
        ),
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": "COMPLETE",
        "selection_scope": "FULL_CURRENT_PROCESS_ALL_PRECOMMITTED_CELLS",
        "precommitted_cell_count": book.FAMILY_SIZE,
        "selected_winner_count": len(materials),
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
    identity = {
        name: value for name, value in payload.items() if name != "generation_sha256"
    }
    payload["generation_sha256"] = lifecycle.canonical_receipt_sha256(identity)
    return lifecycle.build_typed_receipt(
        payload,
        receipt_type=lifecycle.CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
    )


def _derive_refit_cells(
    *,
    materials: Sequence[_SelectedMaterial],
    accepted_receipt_sha256: str,
    historical_process_decision_sha256: str,
) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for material in materials:
        cell = material.book_cell
        source = material.search.source_capture["source_snapshot"]
        source_sha = current_artifacts.canonical_artifact_sha256(source)
        process_sha = current_artifacts.canonical_artifact_sha256(
            material.winner_process
        )
        horizon_sha = current_artifacts.canonical_artifact_sha256(
            material.horizon_process
        )
        trial_sha = current_artifacts.canonical_artifact_sha256(
            material.trial_definition
        )
        recipe_sha = current_artifacts.canonical_artifact_sha256(material.refit_recipe)
        book_entry_sha = _hex64(
            cell.get("historical_book_entry_sha256"),
            "CURRENT_BOOK_ENTRY_SEMANTIC_SHA256_INVALID",
        )
        parameters = material.trial_definition["parameters"]
        parameter_payload = {
            "schema_version": "fxpm.current-refit-parameters.v1",
            "strategy_id": material.winner_process["strategy_id"],
            "parameters": parameters,
        }
        parameter_sha = current_artifacts.canonical_artifact_sha256(parameter_payload)
        output_payload = {
            "schema_version": "fxpm.current-refit-specialist.v1",
            "symbol": cell["symbol"],
            "timeframe": cell["timeframe"],
            "regime": cell["regime"],
            "strategy_id": material.winner_process["strategy_id"],
            "parameters": parameters,
            "parameter_sha256": parameter_sha,
            "source_snapshot_sha256": source_sha,
            "winner_process_sha256": process_sha,
            "winner_horizon_process_sha256": horizon_sha,
            "winner_trial_definition_sha256": trial_sha,
            "winner_refit_recipe_sha256": recipe_sha,
            "winner_book_entry_sha256": book_entry_sha,
            **material.detector,
            "fit_start_utc": source["fit_start_utc"],
            "fit_end_utc": source["fit_end_utc"],
            "latest_closed_bar_utc": source["latest_closed_bar_utc"],
        }
        output_sha = current_artifacts.canonical_artifact_sha256(output_payload)
        identity = {
            "historical_process_decision_sha256": historical_process_decision_sha256,
            "accepted_book_generation_receipt_sha256": accepted_receipt_sha256,
            "symbol": cell["symbol"],
            "timeframe": cell["timeframe"],
            "regime": cell["regime"],
            "strategy_id": material.winner_process["strategy_id"],
            "winner_process_candidate_id": material.winner_process[
                "process_candidate_id"
            ],
            "winner_process_sha256": process_sha,
            "winner_horizon_process_sha256": horizon_sha,
            "winner_trial_definition_sha256": trial_sha,
            "winner_refit_recipe_sha256": recipe_sha,
            "winner_decision_sha256": cell["winner_decision_sha256"],
            "winner_book_entry_sha256": book_entry_sha,
            **material.detector,
            "fit_start_utc": source["fit_start_utc"],
            "fit_end_utc": source["fit_end_utc"],
            "latest_closed_bar_utc": source["latest_closed_bar_utc"],
            "source_snapshot_sha256": source_sha,
            "parameter_sha256": parameter_sha,
            "output_artifact_sha256": output_sha,
        }
        current_cell_id = lifecycle.compute_current_refit_cell_id(identity)
        input_hashes = {
            role: material.descriptors[role]["content_sha256"]
            for role in (
                "source_snapshot",
                "winner_process",
                "horizon_process",
                "trial_definition",
                "refit_recipe",
                "winner_book_entry",
            )
        }
        trace_payload = {
            "schema_version": "fxpm.current-refit-cell-trace.v1",
            "current_refit_cell_id": current_cell_id,
            "input_content_sha256": {
                key: input_hashes[key] for key in sorted(input_hashes)
            },
            "parameter_sha256": parameter_sha,
            "output_artifact_sha256": output_sha,
            "execution_mode": "STATELESS_RULE_MATERIALIZATION",
        }
        cells.append(
            {
                "current_refit_cell_id": current_cell_id,
                **identity,
                "execution_trace_sha256": current_artifacts.canonical_artifact_sha256(
                    trace_payload
                ),
                "attempts": 1,
                "status": "COMPLETE",
                "refit_process_frozen": True,
                "parameter_level_oos_evidence_claimed": False,
            }
        )
    return cells


def _refit_receipt(
    *,
    request: Mapping[str, Any],
    accepted_receipt: Mapping[str, Any],
    materials: Sequence[_SelectedMaterial],
    source_bundle: Mapping[str, Any],
    horizon_bundle: Mapping[str, Any],
    regime_bundle: Mapping[str, Any],
    current_cutoff: str,
) -> dict[str, Any]:
    accepted_payload, accepted_sha = lifecycle.open_typed_receipt(
        accepted_receipt,
        expected_receipt_type=lifecycle.CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
    )
    cells = _derive_refit_cells(
        materials=materials,
        accepted_receipt_sha256=accepted_sha,
        historical_process_decision_sha256=request[
            "historical_process_decision_sha256"
        ],
    )
    source_sha = lifecycle.canonical_receipt_sha256(source_bundle)
    regime_sha = lifecycle.canonical_receipt_sha256(regime_bundle)
    horizon_sha = lifecycle.canonical_receipt_sha256(horizon_bundle)
    roster_sha = lifecycle.compute_current_winner_roster_sha256(cells)
    if any(
        accepted_payload[name] != value
        for name, value in (
            ("source_snapshot_bundle_sha256", source_sha),
            ("regime_params_sha256", regime_sha),
            ("horizon_decisions_sha256", horizon_sha),
            ("selected_winner_roster_sha256", roster_sha),
        )
    ):
        _fail("CURRENT_ACCEPTED_BOOK_REFIT_BINDING_MISMATCH")
    issued = _now()
    authorization = {
        "authorization_id": f"current-refit-{accepted_sha[:20]}",
        "historical_process_decision_sha256": request[
            "historical_process_decision_sha256"
        ],
        "process_id": request["historical_process_decision"]["process_id"],
        "process_definition_sha256": request["historical_process_decision"][
            "process_definition_sha256"
        ],
        "accepted_book_generation_receipt_sha256": accepted_sha,
        "accepted_book_generation_sha256": accepted_payload["generation_sha256"],
        "current_cutoff_utc": current_cutoff,
        "issued_at_utc": issued,
        "source_snapshot_bundle_sha256": source_sha,
        "regime_params_sha256": regime_sha,
        "horizon_decisions_sha256": horizon_sha,
        "selected_winner_roster_sha256": roster_sha,
        "one_touch_nonce_sha256": _sha(
            {"accepted_receipt_sha256": accepted_sha, "issued_at_utc": issued}
        ),
    }
    authorization_sha = lifecycle.canonical_receipt_sha256(authorization)
    started = _now()
    specialists = [
        {
            "current_refit_cell_id": cell["current_refit_cell_id"],
            "parameter_sha256": cell["parameter_sha256"],
            "output_artifact_sha256": cell["output_artifact_sha256"],
        }
        for cell in cells
    ]
    trace_refs = [
        {
            "current_refit_cell_id": cell["current_refit_cell_id"],
            "execution_trace_sha256": cell["execution_trace_sha256"],
        }
        for cell in cells
    ]
    execution = {
        "authorization_sha256": authorization_sha,
        "process_definition_sha256": authorization["process_definition_sha256"],
        "accepted_book_generation_sha256": accepted_payload["generation_sha256"],
        "started_at_utc": started,
        "completed_at_utc": _now(),
        "current_cutoff_utc": current_cutoff,
        "output_book_sha256": current_artifacts.canonical_artifact_sha256(
            {
                "schema_version": "fxpm.current-refit-output-book.v1",
                "current_cutoff_utc": current_cutoff,
                "accepted_book_generation_receipt_sha256": accepted_sha,
                "specialists": specialists,
            }
        ),
        "cell_execution_bundle_sha256": lifecycle.canonical_receipt_sha256(
            {"cell_executions": cells}
        ),
        "execution_trace_sha256": current_artifacts.canonical_artifact_sha256(
            {
                "schema_version": "fxpm.current-refit-execution-trace.v1",
                "current_cutoff_utc": current_cutoff,
                "accepted_book_generation_receipt_sha256": accepted_sha,
                "source_snapshot_bundle_sha256": source_sha,
                "regime_params_sha256": regime_sha,
                "horizon_decisions_sha256": horizon_sha,
                "cell_execution_traces": trace_refs,
            }
        ),
        "cell_executions": cells,
        "attempts": 1,
        "status": "COMPLETE",
        "refit_process_frozen": True,
        "post_authorization_policy_change": False,
        "parameter_level_oos_evidence_claimed": False,
    }
    payload = {
        "schema_version": lifecycle.CURRENT_REFIT_RECEIPT_VERSION,
        "authorization": authorization,
        "authorization_sha256": authorization_sha,
        "execution": execution,
        "execution_sha256": lifecycle.canonical_receipt_sha256(execution),
    }
    receipt = lifecycle.build_typed_receipt(
        payload,
        receipt_type=lifecycle.CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    )
    lineage = lifecycle.build_process_validated_current_refit_lineage(
        historical_process_decision=request["historical_process_decision"],
        accepted_book_generation_receipt=accepted_receipt,
        current_refit_receipt=receipt,
        as_of_utc=current_cutoff,
    )
    if lineage.get("contract_status") != "PROBATION_ONLY":
        _fail("CURRENT_REFIT_LIFECYCLE_REVERIFY_FAILED")
    return receipt


def _generation_artifact(
    sink: authority_spool.SymbolAuthoritySink,
    artifact_id: str,
    artifact_type: str,
    body: Mapping[str, Any],
    format_version: str,
    semantic_sha256: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    entry = sink.put_json(artifact_id, artifact_type, body)
    raw = _raw_descriptor(
        entry,
        semantic_sha256=semantic_sha256 or lifecycle.canonical_receipt_sha256(body),
        format_version=format_version,
    )
    return entry, raw


def _raw_manifest(
    *,
    output_root: Path,
    current_refit_receipt: Mapping[str, Any],
    bundle_descriptors: Mapping[str, Mapping[str, Any]],
    materials: Sequence[_SelectedMaterial],
    refit_cells: Sequence[Mapping[str, Any]],
) -> Path:
    descriptors: dict[str, dict[str, Any]] = {
        value["artifact_id"]: dict(value) for value in bundle_descriptors.values()
    }
    requests: list[dict[str, Any]] = []
    cell_by_slot = {
        (cell["symbol"], cell["timeframe"], cell["regime"]): cell
        for cell in refit_cells
    }
    for material in materials:
        slot = (
            material.book_cell["symbol"],
            material.book_cell["timeframe"],
            material.book_cell["regime"],
        )
        cell = cell_by_slot[slot]
        role_descriptors = {
            role: material.descriptors[role]
            for role in (
                "source_snapshot",
                "winner_process",
                "horizon_process",
                "trial_definition",
                "refit_recipe",
                "winner_book_entry",
            )
        }
        bars = material.descriptors["bars"]
        for descriptor in (*role_descriptors.values(), bars):
            descriptors[descriptor["artifact_id"]] = dict(descriptor)
        request_fields = (
            "current_refit_cell_id",
            "symbol",
            "timeframe",
            "regime",
            "strategy_id",
            "fit_start_utc",
            "fit_end_utc",
            "latest_closed_bar_utc",
            "source_snapshot_sha256",
            "winner_process_sha256",
            "winner_horizon_process_sha256",
            "winner_trial_definition_sha256",
            "winner_refit_recipe_sha256",
            "winner_book_entry_sha256",
            "regime_fit_receipt_sha256",
            "regime_detector_record_sha256",
            "regime_detector_parameters_sha256",
            "regime_detector_code_sha256",
        )
        request = {name: cell[name] for name in request_fields}
        request["artifact_ids"] = {
            role: descriptor["artifact_id"]
            for role, descriptor in role_descriptors.items()
        }
        requests.append(request)
    manifest = current_execution.build_raw_current_refit_manifest(
        current_refit_receipt=current_refit_receipt,
        bundle_artifact_ids={
            role: descriptor["artifact_id"]
            for role, descriptor in bundle_descriptors.items()
        },
        artifact_descriptors=[descriptors[key] for key in sorted(descriptors)],
        cell_requests=requests,
    )
    raw = _canonical_bytes(manifest)
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"raw_current_refit_manifest_{hashlib.sha256(raw).hexdigest()}.json"
    try:
        with path.open("xb") as handle:
            handle.write(raw)
    except FileExistsError:
        if path.read_bytes() != raw:
            _fail("CURRENT_RAW_MANIFEST_IMMUTABLE_CONFLICT")
    return path


def _current_replay_windows(
    value: Mapping[str, Any], *, current_cutoff: str
) -> tuple[dict[str, str], dict[str, str]]:
    """Bind execution and management views to one precommitted current window.

    The synchronized replay engine consumes the anchor-plan ``start_date`` /
    ``end_date`` shape.  The standalone funded-evidence verifier consumes an
    explicitly typed management-window shape.  Both are derived here from the
    same frozen anchor-plan object; neither window is inferred from produced
    trades or accepted from the later evidence callback.
    """

    if not isinstance(value, Mapping) or set(value) != {"start_date", "end_date"}:
        _fail("CURRENT_REPLAY_WINDOW_SCHEMA_INVALID")
    start_text = value.get("start_date")
    end_text = value.get("end_date")
    if type(start_text) is not str or type(end_text) is not str:
        _fail("CURRENT_REPLAY_WINDOW_SCHEMA_INVALID")
    start = _utc(f"{start_text}T00:00:00Z", "CURRENT_REPLAY_WINDOW_START_INVALID")
    end = _utc(f"{end_text}T00:00:00Z", "CURRENT_REPLAY_WINDOW_END_INVALID")
    cutoff = _utc(current_cutoff, "CURRENT_REPLAY_CUTOFF_INVALID")
    if cutoff.time() != datetime.min.time():
        _fail("CURRENT_REPLAY_CUTOFF_NOT_UTC_MIDNIGHT")
    if start >= end or end != cutoff:
        _fail("CURRENT_REPLAY_WINDOW_CUTOFF_MISMATCH")
    execution = {
        "start_date": start.date().isoformat(),
        "end_date": end.date().isoformat(),
    }
    management = {
        "mode": "common_intersection",
        "partition": "current_refit_management_replay",
        "evaluation_start": start.isoformat(),
        "evaluation_end": end.isoformat(),
    }
    return execution, management


def _management_native_source_contract(identity: Mapping[str, Any]) -> dict[str, Any]:
    """Format a legacy native identity for archived replay fixtures.

    This formatter does not reopen sources or authenticate its input. Its
    former production caller performed those checks before projection; that
    caller is retired. The retained provenance fields are legacy schema
    values, not an authentication result for new interval evidence.
    """

    if not isinstance(identity, Mapping):
        _fail("CURRENT_REPLAY_NATIVE_IDENTITY_INVALID")
    symbol = str(identity.get("symbol") or "").strip()
    timeframe = str(identity.get("timeframe") or "").strip().upper()
    activation_id = str(identity.get("activation_id") or "").strip()
    if not symbol or not timeframe or not activation_id:
        _fail("CURRENT_REPLAY_NATIVE_IDENTITY_INVALID")
    manifest_sha = _hex64(
        identity.get("activation_manifest_sha256"),
        "CURRENT_REPLAY_NATIVE_MANIFEST_SHA_INVALID",
    )
    bars_sha = _hex64(
        identity.get("source_bars_sha256"),
        "CURRENT_REPLAY_NATIVE_BARS_SHA_INVALID",
    )
    sidecar_sha = _hex64(
        identity.get("source_audit_sidecar_sha256"),
        "CURRENT_REPLAY_NATIVE_SIDECAR_SHA_INVALID",
    )
    return {
        "source_kind": "broker_native",
        "provenance_valid": True,
        "timestamp_semantics": "bar_open_utc",
        "symbol": symbol,
        "timeframe": timeframe,
        "activation_id": activation_id,
        "activation_manifest_sha256": manifest_sha,
        "source_sha256": bars_sha,
        "sidecar_sha256": sidecar_sha,
        "native_prefix_index_sha256": _hex64(
            identity.get("index_sha256"),
            "CURRENT_REPLAY_NATIVE_INDEX_SHA_INVALID",
        ),
        "native_source_identity_sha256": _hex64(
            identity.get("source_identity_sha256"),
            "CURRENT_REPLAY_NATIVE_SOURCE_IDENTITY_SHA_INVALID",
        ),
    }


def _produce(
    inputs: _ExecutableCurrentRefitInputs,
    request: Mapping[str, Any],
) -> Any:
    _fail("RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED")


class CurrentRefitSearchProducer:
    """Coordinator-compatible one-shot current producer."""

    __slots__ = (
        "_context",
        "_current_selection_window",
        "_frozen_config",
        "_lock",
        "_native_products",
        "_output_root",
        "_search_process",
        "_symbol_scheduler",
        "_used",
    )

    def __init__(self, inputs: CurrentRefitSearchProductionInputs) -> None:
        if type(inputs) is not CurrentRefitSearchProductionInputs:
            _fail("CURRENT_PRODUCTION_INPUTS_INVALID")
        if (
            type(inputs.context) is not authority_spool.GenerationSpoolContext
            or type(inputs.search_process) is not planner.FrozenProductionSearchProcess
            or type(inputs.pipeline_config) is not PipelineConfig
            or not isinstance(inputs.native_products, Mapping)
        ):
            _fail("CURRENT_PRODUCTION_INPUTS_INVALID")
        try:
            frozen_config = config_binding.freeze_production_pipeline_config(
                inputs.context,
                active_config_path=Path(inputs.active_config_path),
                config_payload=inputs.config_payload,
                supplied_config=inputs.pipeline_config,
            )
            native_products = dict(inputs.native_products)
            symbol_scheduler = inputs.symbol_scheduler
            if symbol_scheduler is not None:
                if (
                    type(symbol_scheduler) is not CurrentRefitSymbolSchedulerInputs
                    or native_products
                    or type(symbol_scheduler.requested_workers) is not int
                    or symbol_scheduler.requested_workers <= 0
                ):
                    _fail("CURRENT_SYMBOL_SCHEDULER_INPUT_INVALID")
                scheduler_config_path = Path(
                    symbol_scheduler.active_config_path
                ).resolve(strict=True)
                scheduler_data_dir = Path(symbol_scheduler.data_dir).resolve(strict=True)
                if (
                    scheduler_config_path
                    != Path(inputs.active_config_path).resolve(strict=True)
                    or not scheduler_data_dir.is_dir()
                ):
                    _fail("CURRENT_SYMBOL_SCHEDULER_PATH_INVALID")
                symbol_scheduler = CurrentRefitSymbolSchedulerInputs(
                    active_config_path=scheduler_config_path,
                    data_dir=scheduler_data_dir,
                    cutoff_utc=_utc_plus(
                        symbol_scheduler.cutoff_utc,
                        "CURRENT_SYMBOL_SCHEDULER_CUTOFF_INVALID",
                    ),
                    source_accessed_at_utc=_utc_plus(
                        symbol_scheduler.source_accessed_at_utc,
                        "CURRENT_SYMBOL_SCHEDULER_ACCESS_TIME_INVALID",
                    ),
                    requested_workers=symbol_scheduler.requested_workers,
                )
            current_selection_window = json.loads(
                _canonical_bytes(dict(inputs.current_selection_window)).decode("ascii")
            )
        except config_binding.ProductionConfigBindingError as exc:
            raise CurrentRefitSearchProducerError(
                "CURRENT_PRODUCTION_CONFIG_BINDING_FAILED",
                (exc.code, *exc.details),
            ) from exc
        except Exception as exc:
            raise CurrentRefitSearchProducerError(
                "CURRENT_PRODUCTION_INPUT_SNAPSHOT_FAILED",
                (type(exc).__name__, str(exc)),
            ) from exc
        self._context = inputs.context
        self._search_process = inputs.search_process
        self._frozen_config = frozen_config
        self._native_products = native_products
        self._symbol_scheduler = symbol_scheduler
        self._current_selection_window = current_selection_window
        self._output_root = Path(inputs.output_root)
        self._lock = threading.Lock()
        self._used = False

    def __call__(self, request: Any) -> Any:
        _fail("RETIRED_COST_POLICY_INTERVAL_QUALIFICATION_REPLACEMENT_REQUIRED")


def build_current_refit_search_producer(
    inputs: CurrentRefitSearchProductionInputs,
) -> CurrentRefitSearchProducer:
    return CurrentRefitSearchProducer(inputs)


__all__ = [
    "COORDINATOR_REQUEST_SCHEMA",
    "PRODUCER_SCHEMA",
    "CurrentRefitSearchNotPromotable",
    "CurrentRefitSearchProducer",
    "CurrentRefitSearchProducerError",
    "CurrentRefitSearchProductionInputs",
    "CurrentRefitSymbolSchedulerInputs",
    "build_current_refit_search_producer",
    "verify_causal_process_pass",
]
