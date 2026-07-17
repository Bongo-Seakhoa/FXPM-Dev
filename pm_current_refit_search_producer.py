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

from dataclasses import dataclass, replace
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
import pm_forward_oos_replay_producer as forward_replay
import pm_historical_production_adapter as historical_adapter
import pm_model_lifecycle as lifecycle
import pm_native_prefix_production as native_prefix
import pm_portfolio_replay as portfolio
import pm_production_config_binding as config_binding
import pm_production_search_planner as planner
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
_HEX: Final = frozenset("0123456789abcdef")


class CurrentRefitSearchProducerError(RuntimeError):
    """Typed fail-closed current-generation error; never a market verdict."""

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


@dataclass(frozen=True, slots=True)
class _ExecutableCurrentRefitInputs:
    """Private owned inputs materialized from the exact active config bytes."""

    context: authority_spool.GenerationSpoolContext
    search_process: planner.FrozenProductionSearchProcess
    pipeline_config: PipelineConfig
    native_products: Mapping[str, native_prefix.NativePrefixProduct]
    current_selection_window: Mapping[str, str]
    output_root: Path


@dataclass(frozen=True, slots=True)
class _SelectedSearch:
    symbol: str
    timeframe: str
    regime: str
    product: native_prefix.NativePrefixProduct
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
            str(product.index.get("symbol") or "").upper() != symbol
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
        sink = authority_spool.open_symbol_sink(context, symbol)
        try:
            for timeframe in book.TIMEFRAME_ORDER:
                evidence_batch: list[book.HistoricalBookCellEvidence] = []
                try:
                    product = inputs.native_products[_native_key(symbol, timeframe)]
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
                            candidate_id = (
                                production.decision.winner_process_candidate_id
                            )
                            if candidate_id is not None:
                                key = _native_key(symbol, timeframe) + f"|{regime}"
                                selected[key] = _SelectedSearch(
                                    symbol=symbol,
                                    timeframe=timeframe,
                                    regime=regime,
                                    product=product,
                                    artifacts=_selected_attempt(plan, candidate_id),
                                    source_capture=(
                                        plan.current_refit_source_capture() or {}
                                    ),
                                    winner_decision_sha256=(
                                        production.decision.receipt_sha256
                                    ),
                                )
                            evidence_batch.append(evidence)
                        del fitted
                    elif product.source_state == native_prefix.SOURCE_UNAVAILABLE:
                        terminal = search_execution.fit_terminal_native_timeframe(
                            product=product,
                            config=config,
                        )
                        reason = terminal.regime_fit_decision.to_dict()["reason_code"]
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
                                    state_kind=book.TERMINAL_SOURCE_ABSENCE,
                                    reason_code=reason,
                                    attempted=0,
                                )
                            )
                            del plan
                        del terminal
                    else:
                        _fail("CURRENT_NATIVE_TECHNICAL_FAILURE", symbol, timeframe)
                finally:
                    _release_timeframe_state()
                # Yield only after the finalized timeframe's full feature
                # cache is gone.  A downstream Book-verifier exception can no
                # longer strand frames inside this generator.
                yield from evidence_batch
        finally:
            compact_receipts[symbol] = sink.seal()


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


def _materialize_selected(
    *,
    sink: authority_spool.SymbolAuthoritySink,
    current_book: Mapping[str, Any],
    selected_search: Mapping[str, _SelectedSearch],
    current_cutoff: str,
) -> tuple[list[_SelectedMaterial], dict[str, Any], dict[str, Any], dict[str, Any]]:
    selected_cells = [
        cell
        for cell in current_book["cells"]
        if cell["historical_local_candidate_pending_causal"] is True
    ]
    if not selected_cells:
        _fail("CURRENT_BOOK_HAS_NO_ELIGIBLE_WINNERS")
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
    """Project a verifier-sealed native-prefix identity into replay evidence.

    ``forward_replay._native_product_identity`` has already reopened the
    activated product and reverified broker-native provenance, UTC bar-open
    timestamps, the activation, bars and audit sidecar.  This projection only
    renames those derived facts into the standalone portfolio verifier's
    schema; it does not let a caller assert provenance.
    """

    if not isinstance(identity, Mapping):
        _fail("CURRENT_REPLAY_NATIVE_IDENTITY_INVALID")
    symbol = str(identity.get("symbol") or "").strip().upper()
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


def _replay_candidates(
    *,
    inputs: _ExecutableCurrentRefitInputs,
    current_book: Mapping[str, Any],
    materials: Sequence[_SelectedMaterial],
    current_cutoff: str,
    current_refit_receipt: Mapping[str, Any],
) -> tuple[
    tuple[portfolio.CandidateTrade, ...],
    list[str],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    cutoff = _utc(current_cutoff, "CURRENT_REPLAY_CUTOFF_INVALID")
    execution_window, management_window = _current_replay_windows(
        inputs.current_selection_window,
        current_cutoff=current_cutoff,
    )
    anchor_id = f"CURRENT-{cutoff.strftime('%Y%m%dT%H%M%SZ')}"
    specialists: list[forward_replay._VerifiedSpecialist] = []  # noqa: SLF001
    for material in materials:
        cell = material.book_cell
        group_fit = next(
            value
            for value in current_book["regime_fits"]
            if value["payload"]["symbol"] == cell["symbol"]
            and value["payload"]["timeframe"] == cell["timeframe"]
        )
        record = group_fit["payload"]["regime_detector_record"]
        parameters = json.loads(record["parameters_canonical_json"])
        specialists.append(
            forward_replay._VerifiedSpecialist(  # noqa: SLF001
                anchor_id=anchor_id,
                cell_id=cell["cell_id"],
                symbol=cell["symbol"],
                timeframe=cell["timeframe"],
                regime=cell["regime"],
                strategy_id=material.winner_process["strategy_id"],
                strategy_class=material.winner_process["strategy_class"],
                strategy_module=material.winner_process["strategy_module"],
                parameters=material.trial_definition["parameters"],
                process_candidate_id=material.winner_process["process_candidate_id"],
                process_sha256=current_artifacts.canonical_artifact_sha256(
                    material.winner_process
                ),
                priority_score=1.0 - float(cell["adjusted_q_value"]),
                source_count_receipt=material.search.source_capture[
                    "selection_prefix_receipt"
                ],
                detector_parameters=parameters,
                detector_parameters_sha256=record["parameters_sha256"],
                detector_record=record,
            )
        )
    execution_products = {
        _native_key(value.symbol, value.timeframe): inputs.native_products[
            _native_key(value.symbol, value.timeframe)
        ]
        for value in specialists
    }
    valuation_products = {
        _native_key(value.symbol, "M5"): inputs.native_products[
            _native_key(value.symbol, "M5")
        ]
        for value in specialists
    }
    execution_identities = [
        forward_replay._native_product_identity(  # noqa: SLF001
            product,
            expected_symbol=key.split("|", 1)[0],
            expected_timeframe=key.split("|", 1)[1],
            expected_cutoff_utc=current_cutoff,
            role=f"CURRENT_EXECUTION:{key}",
        )
        for key, product in sorted(execution_products.items())
    ]
    valuation_identities = [
        forward_replay._native_product_identity(  # noqa: SLF001
            product,
            expected_symbol=key.split("|", 1)[0],
            expected_timeframe="M5",
            expected_cutoff_utc=current_cutoff,
            role=f"CURRENT_VALUATION:{key}",
        )
        for key, product in sorted(valuation_products.items())
    ]
    precommit_anchor = {
        "anchor_id": anchor_id,
        "forward_end_exclusive_utc": current_cutoff,
        "capital_phase_windows": {
            forward_replay.CAPITAL_INNER_SELECTION: execution_window
        },
        "execution_products": execution_identities,
        "valuation_m5_products": valuation_identities,
        "execution_product_roster_sha256": _sha(execution_identities),
        "valuation_m5_product_roster_sha256": _sha(valuation_identities),
        "valuation_products_supplied_separately": True,
    }
    anchor = {
        "anchor_id": anchor_id,
        "forward_end_date": (cutoff.date() - timedelta(days=1)).isoformat(),
    }
    deployment = forward_replay._deployment_config(inputs.pipeline_config)  # noqa: SLF001
    replayed = forward_replay._execute_anchor_replay(  # noqa: SLF001
        context=inputs.context,
        anchor=anchor,
        specialists=specialists,
        native_products=execution_products,
        valuation_products=valuation_products,
        precommit_anchor=precommit_anchor,
        capital_phase=forward_replay.CAPITAL_INNER_SELECTION,
        capital_window=execution_window,
        config=inputs.pipeline_config,
        deployment=deployment,
    )
    receipt_sha = lifecycle.canonical_receipt_sha256(current_refit_receipt)
    candidates = tuple(
        replace(
            position.candidate,
            candidate_id="current:"
            + hashlib.sha256(
                f"{receipt_sha}|{position.candidate.candidate_id}".encode("ascii")
            ).hexdigest(),
            promotion_status="PROCESS_VALIDATED_CURRENT_REFIT",
        ).normalized()
        for position in replayed.positions
    )
    if not candidates:
        _fail("CURRENT_SYNCHRONIZED_REPLAY_HAS_NO_POSITIONS")
    replayed_cell_ids = sorted(material.book_cell["cell_id"] for material in materials)
    execution_by_source = {
        f"{value['symbol']}|{value['timeframe']}": value
        for value in execution_identities
    }
    valuation_by_symbol = {value["symbol"]: value for value in valuation_identities}
    native_sources: dict[str, dict[str, Any]] = {}
    for material in materials:
        cell = material.book_cell
        cell_id = str(cell.get("cell_id") or "").strip()
        source_id = f"{cell.get('symbol')}|{cell.get('timeframe')}"
        identity = execution_by_source.get(source_id)
        if not cell_id or identity is None or cell_id in native_sources:
            _fail("CURRENT_REPLAY_NATIVE_SOURCE_ROSTER_INVALID", cell_id or source_id)
        native_sources[cell_id] = _management_native_source_contract(identity)
    valuation_sources: dict[str, dict[str, Any]] = {}
    for symbol, identity in sorted(valuation_by_symbol.items()):
        canonical_symbol = str(symbol).upper()
        if canonical_symbol in valuation_sources:
            _fail("CURRENT_REPLAY_VALUATION_SOURCE_ROSTER_INVALID", canonical_symbol)
        valuation_sources[canonical_symbol] = _management_native_source_contract(
            identity
        )
    activation_bindings = {
        (
            value["activation_id"],
            value["activation_manifest_sha256"],
        )
        for value in (*native_sources.values(), *valuation_sources.values())
    }
    if len(activation_bindings) != 1:
        _fail("CURRENT_REPLAY_NATIVE_ACTIVATION_MIXED")
    return (
        candidates,
        replayed_cell_ids,
        management_window,
        native_sources,
        valuation_sources,
    )


def _produce(
    inputs: _ExecutableCurrentRefitInputs,
    request: Mapping[str, Any],
) -> Any:
    context = inputs.context
    process = inputs.search_process
    if (
        type(context) is not authority_spool.GenerationSpoolContext
        or type(process) is not planner.FrozenProductionSearchProcess
        or type(inputs.pipeline_config) is not PipelineConfig
    ):
        _fail("CURRENT_PRODUCTION_INPUTS_INVALID")
    planner.verify_frozen_production_search_process(context, process)
    pass_result = verify_causal_process_pass(
        causal_precommit_payload=request["causal_precommit_payload"],
        causal_evidence_payload=request["causal_evidence_payload"],
        causal_reconciliation_payload=request["causal_reconciliation_payload"],
    )
    if (
        pass_result["decision_body"] != request["historical_process_decision"]
        or pass_result["decision_sha256"]
        != request["historical_process_decision_sha256"]
        or causal.canonical_sha256(request["historical_process_decision"])
        != request["historical_process_decision_sha256"]
    ):
        _fail("CURRENT_REQUEST_CAUSAL_DECISION_MISMATCH")
    current_cutoff = _current_cutoff(inputs)
    authorization = planner.freeze_current_search_authorization(
        context,
        process=process,
        causal_precommit_payload=request["causal_precommit_payload"],
        causal_evidence_payload=request["causal_evidence_payload"],
        causal_reconciliation_payload=request["causal_reconciliation_payload"],
        current_cutoff_utc=current_cutoff,
    )
    started_at = _now()
    selected_search: dict[str, _SelectedSearch] = {}
    compact_receipts: dict[str, Any] = {}
    current_decision = book.evaluate_historical_book_multiplicity(
        _book_evidence_stream(
            inputs,
            authorization=authorization,
            selected=selected_search,
            compact_receipts=compact_receipts,
        )
    )
    if not book.verify_historical_book_multiplicity_decision(current_decision):
        _fail("CURRENT_BOOK_MULTIPLICITY_REJECTED")
    current_book = current_decision.to_dict()
    if (
        current_book.get("optimization_run_complete") is not True
        or current_book.get("valid") is not True
    ):
        _fail("CURRENT_BOOK_INCOMPLETE")
    generation_sink = authority_spool.open_symbol_sink(context, GENERATION_SYMBOL)
    materials, source_bundle, horizon_bundle, regime_bundle = _materialize_selected(
        sink=generation_sink,
        current_book=current_book,
        selected_search=selected_search,
        current_cutoff=current_cutoff,
    )
    accepted = _accepted_book_receipt(
        inputs=inputs,
        request=request,
        current_book=current_book,
        materials=materials,
        source_bundle=source_bundle,
        horizon_bundle=horizon_bundle,
        regime_bundle=regime_bundle,
        current_cutoff=current_cutoff,
        started_at=started_at,
        completed_at=_now(),
    )
    refit = _refit_receipt(
        request=request,
        accepted_receipt=accepted,
        materials=materials,
        source_bundle=source_bundle,
        horizon_bundle=horizon_bundle,
        regime_bundle=regime_bundle,
        current_cutoff=current_cutoff,
    )
    accepted_sha = lifecycle.canonical_receipt_sha256(accepted)
    refit_payload, refit_sha = lifecycle.open_typed_receipt(
        refit,
        expected_receipt_type=lifecycle.CURRENT_REFIT_EXECUTION_RECEIPT_TYPE,
    )
    _, accepted_raw = _generation_artifact(
        generation_sink,
        "current-refit/generation/accepted-book",
        current_production.ART_ACCEPTED_BOOK,
        accepted,
        "fxpm.current-process-book-receipt.v1",
        semantic_sha256=accepted_sha,
    )
    _, source_raw = _generation_artifact(
        generation_sink,
        "current-refit/generation/source-bundle",
        current_production.ART_SOURCE_BUNDLE,
        source_bundle,
        SOURCE_BUNDLE_SCHEMA,
    )
    _, regime_raw = _generation_artifact(
        generation_sink,
        "current-refit/generation/regime-bundle",
        current_production.ART_REGIME_BUNDLE,
        regime_bundle,
        current_execution.CURRENT_REGIME_PARAMS_BUNDLE_VERSION,
    )
    _, horizon_raw = _generation_artifact(
        generation_sink,
        "current-refit/generation/horizon-bundle",
        current_production.ART_HORIZON_BUNDLE,
        horizon_bundle,
        HORIZON_BUNDLE_SCHEMA,
    )
    generation_sink.put_json(
        "current-refit/generation/current-book",
        current_production.ART_CURRENT_BOOK,
        current_book,
    )
    generation_sink.put_json(
        "current-refit/generation/refit-receipt",
        current_production.ART_REFIT_RECEIPT,
        refit,
    )
    refit_cells = refit_payload["execution"]["cell_executions"]
    manifest_path = _raw_manifest(
        output_root=inputs.output_root,
        current_refit_receipt=refit,
        bundle_descriptors={
            "accepted_book_generation": accepted_raw,
            "source_snapshot_bundle": source_raw,
            "regime_params_bundle": regime_raw,
            "horizon_decisions_bundle": horizon_raw,
        },
        materials=materials,
        refit_cells=refit_cells,
    )
    verification = current_execution.verify_raw_current_refit_execution(
        refit,
        manifest_path,
        artifact_root=context.root,
    )
    if verification.get("verified") is not True or verification.get("errors") != []:
        _fail("CURRENT_REFIT_RAW_REVERIFY_FAILED", *verification.get("errors", ()))
    derived = current_production.derive_physical_current_refit_ledger(
        current_book_decision=current_book,
        accepted_book_generation_receipt=accepted,
        current_refit_receipt=refit,
        raw_manifest_path=manifest_path,
        artifact_root=context.root,
        output_root=inputs.output_root,
    )
    candidates, replayed_ids, window, native_sources, valuation_sources = (
        _replay_candidates(
            inputs=inputs,
            current_book=current_book,
            materials=materials,
            current_cutoff=current_cutoff,
            current_refit_receipt=refit,
        )
    )
    current_production.spool_current_refit_replay_capture(
        generation_sink,
        staged_ledger_content_sha256=derived.content_sha256,
        physical_ledger_sha256=derived.physical_ledger_sha256,
        accepted_book_generation_receipt_sha256=accepted_sha,
        current_refit_receipt_sha256=refit_sha,
        current_refit_execution_sha256=refit_payload["execution_sha256"],
        current_cutoff_utc=current_cutoff,
        candidates=candidates,
        replayed_cell_ids=replayed_ids,
        current_refit_execution_access_id=(
            "current-refit-execution:" + refit_payload["execution_sha256"]
        ),
        window=window,
        native_sources=native_sources,
        valuation_sources=valuation_sources,
    )
    compact_receipts[GENERATION_SYMBOL] = generation_sink.seal()
    capture_inputs = current_production.ProductionCaptureInputs(
        spool_root=context.root,
        compact_symbol_receipts=compact_receipts,
        output_root=Path(inputs.output_root),
        generation_id=context.generation_id,
        native_universe_sha256=context.native_universe_sha256,
        config_sha256=context.config_sha256,
    )
    adapter = current_production.CurrentRefitProductionAdapter(capture_inputs)
    capture_request = _CaptureRequest(
        {
            "schema_version": request["schema_version"],
            "historical_run_receipt_sha256": request["historical_run_receipt_sha256"],
            "historical_process_decision_sha256": request[
                "historical_process_decision_sha256"
            ],
            "historical_book_receipt_sha256": request["historical_book_receipt_sha256"],
            "authority_claimed": False,
        }
    )
    return adapter(capture_request)


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
        self._current_selection_window = current_selection_window
        self._output_root = Path(inputs.output_root)
        self._lock = threading.Lock()
        self._used = False

    def __call__(self, request: Any) -> Any:
        with self._lock:
            if self._used:
                _fail("CURRENT_REFIT_PRODUCER_ALREADY_CONSUMED")
            # Consume before re-opening the active configuration or touching
            # latest-cutoff evidence.  A partially observed current run is
            # never retried inside the same authority generation.
            self._used = True
        try:
            pipeline_config = self._frozen_config.materialize(self._context)
        except config_binding.ProductionConfigBindingError as exc:
            raise CurrentRefitSearchProducerError(
                "CURRENT_PRODUCTION_CONFIG_REVERIFY_FAILED",
                (exc.code, *exc.details),
            ) from exc
        owned = _ExecutableCurrentRefitInputs(
            context=self._context,
            search_process=self._search_process,
            pipeline_config=pipeline_config,
            native_products=self._native_products,
            current_selection_window=self._current_selection_window,
            output_root=self._output_root,
        )
        return _produce(owned, _request_body(request))


def build_current_refit_search_producer(
    inputs: CurrentRefitSearchProductionInputs,
) -> CurrentRefitSearchProducer:
    return CurrentRefitSearchProducer(inputs)


__all__ = [
    "COORDINATOR_REQUEST_SCHEMA",
    "PRODUCER_SCHEMA",
    "CurrentRefitSearchProducer",
    "CurrentRefitSearchProducerError",
    "CurrentRefitSearchProductionInputs",
    "build_current_refit_search_producer",
    "verify_causal_process_pass",
]
