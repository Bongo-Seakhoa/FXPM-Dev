"""Bounded production driver for genuine repeated historical searches.

The historical orchestration contract must commit the exact 2,400 Horizon
bindings for an anchor before it can issue the request that permits strategy
return evidence.  This module closes that otherwise circular seam in two
strict phases:

* :func:`preplan_historical_search` opens each independently activated native
  symbol/timeframe prefix at the frozen anchor cutoff, fits only the causal
  regime surface, and builds the released Horizon plans through the
  pre-evidence request.  It never executes a strategy or opens forward-OOS
  rows.  Only the sealed prefix products and compact plan-binding identities
  are retained.
* The returned per-anchor callbacks accept only the canonical post-precommit
  request, refit the retained prefix one timeframe at a time, regenerate and
  compare every Horizon binding, then execute observed cells through
  :mod:`pm_authoritative_search_execution`.  Sparse regimes and authenticated
  source absence remain explicit; every technical failure aborts the anchor.

Forward execution/valuation prefixes are intentionally produced only after a
verified historical run reveals the genuine winner roster.  No caller can
preselect those products or gain causal/current/capital/trading authority here.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import gc
import hashlib
import json
from pathlib import Path
import threading
from typing import Any, Callable, Final, Iterable, Mapping, Sequence

import pm_authoritative_search_execution as search_execution
import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_causal_evidence_producer as causal_evidence
import pm_causal_process as causal
import pm_core
import pm_historical_anchor_orchestration as historical
import pm_historical_production_adapter as historical_adapter
import pm_native_prefix_production as native_prefix
import pm_production_search_planner as search_planner
from pm_core import PipelineConfig


PRODUCER_SCHEMA: Final = "fxpm.production-historical-search-producer.v1"
AUTHORITY_SCOPE: Final = (
    "HISTORICAL_SEARCH_AND_FORWARD_NATIVE_PRODUCTS_ONLY_NO_DEPLOYMENT_AUTHORITY"
)

_UTC: Final = timezone.utc
_HEX: Final = frozenset("0123456789abcdef")
_SYMBOLS: Final = tuple(book.PINNED_SYMBOL_ORDER)
_TIMEFRAMES: Final = tuple(book.TIMEFRAME_ORDER)
_REGIMES: Final = tuple(book.REGIME_ORDER)
_CELLS_PER_TIMEFRAME: Final = len(_REGIMES)
_CELLS_PER_SYMBOL: Final = len(_TIMEFRAMES) * _CELLS_PER_TIMEFRAME
_GROUPS_PER_ANCHOR: Final = len(_SYMBOLS) * len(_TIMEFRAMES)
_CELLS_PER_ANCHOR: Final = _GROUPS_PER_ANCHOR * len(_REGIMES)


class HistoricalSearchProducerError(RuntimeError):
    """Typed non-economic failure; callers must abort the generation."""

    def __init__(self, code: str, details: Sequence[Any] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value)[:512] for value in tuple(details)[:32])
        suffix = ":" + "|".join(self.details) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise HistoricalSearchProducerError(code, details)


def _canonical_bytes(value: Any, maximum: int = 32 * 1024 * 1024) -> bytes:
    try:
        raw = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise HistoricalSearchProducerError("CANONICAL_JSON_INVALID") from exc
    if not raw or len(raw) > maximum:
        _fail("CANONICAL_JSON_SIZE_INVALID", len(raw), maximum)
    return raw


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


def _now_utc_seconds() -> str:
    return datetime.now(_UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc(value: Any, code: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise HistoricalSearchProducerError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    normalized = parsed.astimezone(_UTC)
    expected = normalized.isoformat(timespec="seconds").replace("+00:00", "Z")
    if normalized.microsecond or expected != value:
        _fail(code)
    return normalized


def _owned_mapping(value: Mapping[str, Any], code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        _fail(code)
    try:
        owned = json.loads(_canonical_bytes(dict(value)).decode("ascii"))
    except HistoricalSearchProducerError:
        raise
    if type(owned) is not dict:
        _fail(code)
    return owned


def _release_timeframe_state() -> None:
    """Bound the global feature cache after a complete native timeframe."""

    try:
        pm_core.FeatureComputer.clear_cache()
        gc.collect()
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "TIMEFRAME_RESOURCE_RELEASE_FAILED", (type(exc).__name__, str(exc))
        ) from exc


def _geometry(value: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        "anchor_id",
        "selection_cutoff_utc",
        "forward_start_date",
        "forward_end_date",
        "purge_calendar_days",
    }
    owned = _owned_mapping(value, "ANCHOR_GEOMETRY_INVALID")
    if set(owned) != expected:
        _fail("ANCHOR_GEOMETRY_FIELDS_INVALID")
    return owned


def _binding_digest(plan: Any) -> tuple[dict[str, Any], str]:
    try:
        binding = historical.historical_horizon_plan_binding(plan.horizon_plan)
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "HORIZON_BINDING_DERIVATION_FAILED", (type(exc).__name__, str(exc))
        ) from exc
    if type(binding) is not dict:
        _fail("HORIZON_BINDING_INVALID")
    return binding, _sha(binding)


def _plan_status(plan: Any) -> str:
    try:
        body = plan.horizon_plan.to_dict()
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "HORIZON_PLAN_BODY_UNAVAILABLE", (type(exc).__name__, str(exc))
        ) from exc
    status = body.get("plan_status") if type(body) is dict else None
    if type(status) is not str or not status:
        _fail("HORIZON_PLAN_STATUS_INVALID")
    return status


def _verified_support_counts(fitted: Any) -> dict[str, int]:
    support = getattr(fitted, "train_regime_support", None)
    counts = support.get("counts") if isinstance(support, Mapping) else None
    if type(counts) is not dict or set(counts) != set(_REGIMES):
        _fail("TRAIN_REGIME_SUPPORT_INVALID")
    result: dict[str, int] = {}
    for regime in _REGIMES:
        count = counts.get(regime)
        if type(count) is not int or type(count) is bool or count < 0:
            _fail("TRAIN_REGIME_SUPPORT_INVALID", regime)
        result[regime] = count
    return result


def _timeframe_plans(
    *,
    context: authority_spool.GenerationSpoolContext,
    process: search_planner.FrozenProductionSearchProcess,
    request: historical.AnchorProducerRequest,
    causal_precommit_payload: Mapping[str, Any],
    anchor_ordinal: int,
    product: native_prefix.NativePrefixProduct,
    config: PipelineConfig,
) -> tuple[Any, tuple[Any, ...]]:
    """Reverify one prefix and build exactly four released Horizon plans."""

    state = getattr(product, "source_state", None)
    if state == native_prefix.AVAILABLE:
        fitted = search_execution.fit_native_timeframe_for_search(
            product=product,
            config=config,
        )
        counts = _verified_support_counts(fitted)
        plans: list[Any] = []
        for regime in _REGIMES:
            if counts[regime] == 0:
                plan = search_planner.build_historical_unobserved_cell_plan(
                    context,
                    process=process,
                    request=request,
                    causal_precommit_payload=causal_precommit_payload,
                    anchor_ordinal=anchor_ordinal,
                    fitted=fitted,
                    regime=regime,
                )
                if _plan_status(plan) != "HISTORICAL_REGIME_UNOBSERVED":
                    _fail("UNOBSERVED_PLAN_STATUS_INVALID", regime)
            else:
                plan = search_planner.build_historical_observed_cell_plan(
                    context,
                    process=process,
                    request=request,
                    causal_precommit_payload=causal_precommit_payload,
                    anchor_ordinal=anchor_ordinal,
                    fitted=fitted,
                    regime=regime,
                )
                if _plan_status(plan) not in {
                    "READY_FOR_HISTORICAL_EVIDENCE",
                    "HISTORICAL_BOUNDARY_LIMITED",
                }:
                    _fail("OBSERVED_PLAN_STATUS_INVALID", regime)
            plans.append(plan)
        return fitted, tuple(plans)
    if state not in {native_prefix.SOURCE_UNAVAILABLE, native_prefix.TECHNICAL_FAILURE}:
        _fail("NATIVE_PREFIX_SOURCE_STATE_INVALID", state)
    # fit_terminal_native_timeframe raises for TECHNICAL_FAILURE by contract;
    # no Book/no-trade state can be manufactured from infrastructure failure.
    terminal = search_execution.fit_terminal_native_timeframe(
        product=product,
        config=config,
    )
    plans = search_planner.build_historical_terminal_timeframe_plans(
        context,
        process=process,
        request=request,
        causal_precommit_payload=causal_precommit_payload,
        anchor_ordinal=anchor_ordinal,
        product=product,
    )
    if len(plans) != len(_REGIMES) or any(
        _plan_status(plan) != "HISTORICAL_SOURCE_UNAVAILABLE" for plan in plans
    ):
        _fail("TERMINAL_PLAN_ROSTER_INVALID")
    return terminal, tuple(plans)


@dataclass(frozen=True, slots=True)
class _PreplannedAnchor:
    ordinal: int
    geometry_json: bytes
    augmented_json: bytes
    selection_products: tuple[native_prefix.NativePrefixProduct, ...]
    binding_digests: tuple[str, ...]

    @property
    def geometry(self) -> dict[str, Any]:
        return json.loads(self.geometry_json.decode("ascii"))

    @property
    def augmented(self) -> dict[str, Any]:
        return json.loads(self.augmented_json.decode("ascii"))


class PreplannedHistoricalSearch:
    """One-shot plan roster and its canonical post-precommit callbacks."""

    __slots__ = (
        "context",
        "_anchors",
        "_causal_json",
        "_causal_sha256",
        "_completed_anchors",
        "_config",
        "_data_dir",
        "_forward_resolver_used",
        "_lock",
        "_process",
        "_run_id",
        "_planning_started_at_utc",
        "_used_anchors",
    )

    def __init__(
        self,
        *,
        context: authority_spool.GenerationSpoolContext,
        data_dir: Path,
        config: PipelineConfig,
        process: search_planner.FrozenProductionSearchProcess,
        causal_precommit_payload: Mapping[str, Any],
        run_id: str,
        planning_started_at_utc: str,
        anchors: Sequence[_PreplannedAnchor],
    ) -> None:
        self.context = context
        self._data_dir = Path(data_dir)
        self._config = config
        self._process = process
        self._causal_json = _canonical_bytes(dict(causal_precommit_payload))
        self._causal_sha256 = causal.canonical_sha256(causal_precommit_payload)
        self._run_id = run_id
        self._planning_started_at_utc = planning_started_at_utc
        self._anchors = tuple(anchors)
        self._lock = threading.Lock()
        self._used_anchors: set[str] = set()
        self._completed_anchors: set[str] = set()
        self._forward_resolver_used = False

    @property
    def causal_precommit_payload_sha256(self) -> str:
        return self._causal_sha256

    @property
    def planning_started_at_utc(self) -> str:
        return self._planning_started_at_utc

    @property
    def anchors(self) -> tuple[dict[str, Any], ...]:
        return tuple(anchor.augmented for anchor in self._anchors)

    def _anchor(self, anchor_id: str) -> _PreplannedAnchor:
        matches = [
            anchor
            for anchor in self._anchors
            if anchor.augmented.get("anchor_id") == anchor_id
        ]
        if len(matches) != 1:
            _fail("PREPLANNED_ANCHOR_ID_INVALID", anchor_id)
        return matches[0]

    def _validate_producer_request(
        self,
        request: historical.AnchorProducerRequest,
        anchor: _PreplannedAnchor,
    ) -> None:
        if type(request) is not historical.AnchorProducerRequest:
            _fail("ANCHOR_PRODUCER_REQUEST_INVALID")
        body = request.to_dict()
        if (
            body.get("schema_version") != historical.PRODUCER_REQUEST_SCHEMA
            or body.get("run_id") != self._run_id
            or body.get("generation_id") != self.context.generation_id
            or body.get("process_precommit_receipt_sha256")
            != self._causal_sha256
            or body.get("optimizer_producer_release_sha256")
            != historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
            or body.get("anchor") != anchor.augmented
            or body.get("symbols") != list(_SYMBOLS)
            or body.get("timeframes") != list(_TIMEFRAMES)
            or body.get("regimes") != list(_REGIMES)
            or body.get("requires_exact_sparse_state_roster") is not True
            or body.get("possible_regime_slots_are_forced_observations") is not False
            or body.get("confers_causal_or_current_authority") is not False
        ):
            _fail("ANCHOR_PRODUCER_REQUEST_BINDING_MISMATCH")

    def _consume_anchor(self, anchor_id: str) -> None:
        with self._lock:
            if anchor_id in self._used_anchors:
                _fail("ANCHOR_PRODUCER_ALREADY_CONSUMED", anchor_id)
            # Consumption precedes any strategy evidence access.  A partial
            # failed anchor is never retried inside the frozen generation.
            self._used_anchors.add(anchor_id)

    def _iter_anchor_products(
        self,
        request: historical.AnchorProducerRequest,
        anchor: _PreplannedAnchor,
    ) -> Iterable[historical.AnchorSymbolProduct]:
        causal_payload = json.loads(self._causal_json.decode("ascii"))
        product_index = 0
        cell_index = 0
        for symbol in _SYMBOLS:
            capture = historical_adapter.open_historical_symbol_capture(
                self.context,
                request,
                symbol,
            )
            for timeframe in _TIMEFRAMES:
                if product_index >= len(anchor.selection_products):
                    _fail("SELECTION_PREFIX_ROSTER_INCOMPLETE")
                product = anchor.selection_products[product_index]
                product_index += 1
                authority: Any = None
                plans: tuple[Any, ...] = ()
                try:
                    authority, plans = _timeframe_plans(
                        context=self.context,
                        process=self._process,
                        request=request,
                        causal_precommit_payload=causal_payload,
                        anchor_ordinal=anchor.ordinal,
                        product=product,
                        config=self._config,
                    )
                    if len(plans) != len(_REGIMES):
                        _fail("TIMEFRAME_PLAN_ROSTER_INCOMPLETE", symbol, timeframe)
                    for regime, plan in zip(_REGIMES, plans, strict=True):
                        if cell_index >= len(anchor.binding_digests):
                            _fail("PREPLANNED_BINDING_ROSTER_INCOMPLETE")
                        binding, digest = _binding_digest(plan)
                        if digest != anchor.binding_digests[cell_index] or binding.get(
                            "cell"
                        ) != {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "regime": regime,
                        }:
                            _fail(
                                "CANONICAL_HORIZON_PLAN_CHANGED_AFTER_PREPLANNING",
                                symbol,
                                timeframe,
                                regime,
                            )
                        cell_index += 1
                    capture.bind_timeframe(
                        timeframe,
                        horizon_plans=tuple(plan.horizon_plan for plan in plans),
                        source_group_decision=authority.source_group_decision,
                        regime_fit_decision=authority.regime_fit_decision,
                    )
                    if getattr(product, "source_state", None) == native_prefix.AVAILABLE:
                        counts = _verified_support_counts(authority)
                        for regime, plan in zip(_REGIMES, plans, strict=True):
                            status = _plan_status(plan)
                            if counts[regime] == 0 or status == (
                                "HISTORICAL_BOUNDARY_LIMITED"
                            ):
                                continue
                            if status != "READY_FOR_HISTORICAL_EVIDENCE":
                                _fail(
                                    "OBSERVED_PLAN_NOT_EXECUTABLE",
                                    symbol,
                                    timeframe,
                                    regime,
                                    status,
                                )
                            result = search_execution.execute_historical_observed_cell(
                                capture=capture,
                                fitted=authority,
                                regime=regime,
                                horizon_plan=plan.horizon_plan,
                                attempt_artifacts=plan.attempt_artifacts(),
                                config=self._config,
                            )
                            if result.status != "WINNER_RAW_EVIDENCE_COMPLETE":
                                _fail(
                                    "HISTORICAL_OBSERVED_CELL_EXECUTION_FAILED",
                                    symbol,
                                    timeframe,
                                    regime,
                                    result.status,
                                    result.failure_code,
                                )
                except HistoricalSearchProducerError:
                    raise
                except Exception as exc:
                    raise HistoricalSearchProducerError(
                        "HISTORICAL_EXECUTION_TIMEFRAME_FAILED",
                        (
                            anchor.augmented["anchor_id"],
                            symbol,
                            timeframe,
                            type(exc).__name__,
                            str(exc),
                        ),
                    ) from exc
                finally:
                    plans = ()
                    authority = None
                    _release_timeframe_state()
            yield capture.finish()
        if (
            product_index != len(anchor.selection_products)
            or product_index != _GROUPS_PER_ANCHOR
            or cell_index != len(anchor.binding_digests)
            or cell_index != _CELLS_PER_ANCHOR
        ):
            _fail("ANCHOR_REPLAY_ROSTER_INCOMPLETE")
        with self._lock:
            anchor_id = str(anchor.augmented["anchor_id"])
            if anchor_id in self._completed_anchors:
                _fail("ANCHOR_PRODUCER_COMPLETION_DUPLICATED", anchor_id)
            self._completed_anchors.add(anchor_id)

    def anchor_producers(
        self,
    ) -> dict[
        str,
        Callable[
            [historical.AnchorProducerRequest],
            Iterable[historical.AnchorSymbolProduct],
        ],
    ]:
        """Return exact one-shot callbacks in causal anchor order."""

        result: dict[str, Callable[[historical.AnchorProducerRequest], Any]] = {}
        for anchor in self._anchors:
            anchor_id = str(anchor.augmented["anchor_id"])

            def producer(
                request: historical.AnchorProducerRequest,
                *,
                frozen_anchor: _PreplannedAnchor = anchor,
                frozen_id: str = anchor_id,
            ) -> Iterable[historical.AnchorSymbolProduct]:
                self._validate_producer_request(request, frozen_anchor)
                self._consume_anchor(frozen_id)
                return self._iter_anchor_products(request, frozen_anchor)

            result[anchor_id] = producer
        return result

    def resolve_forward_native_products(
        self,
        request: causal_evidence.ForwardNativeProductRequest,
    ) -> causal_evidence.ForwardNativeProductResolution:
        """Materialize only the factory's exact post-Book winner roster.

        The canonical causal factory derives the product keys from verified
        historical Book bytes.  This resolver can neither add nor choose a
        key: it validates the complete issuer request against its frozen
        generation, causal process, historical precommit and anchor geometry,
        consumes itself once, then creates sealed native products without
        opening a forward frame.
        """

        if type(request) is not causal_evidence.ForwardNativeProductRequest:
            _fail("FORWARD_NATIVE_PRODUCT_REQUEST_INVALID")
        body = request.to_dict()
        expected_fields = {
            "schema_version",
            "generation_id",
            "generation_context_sha256",
            "native_universe_sha256",
            "config_sha256",
            "historical_precommit_receipt_sha256",
            "historical_run_receipt_sha256",
            "causal_precommit_payload_sha256",
            "anchor_count",
            "anchors",
            "required_roster_sha256",
            "native_products_must_be_sealed_before_forward_frame_access",
            "resolver_may_choose_winners_or_product_keys",
            "confers_causal_current_refit_capital_publication_or_trading_authority",
        }
        if (
            type(body) is not dict
            or set(body) != expected_fields
            or _canonical_bytes(body, 4 * 1024 * 1024)
            != request._body_json  # noqa: SLF001 - exact issuer bytes are the API.
            or hashlib.sha256(request._body_json).hexdigest()  # noqa: SLF001
            != request.request_sha256
            or body.get("schema_version")
            != causal_evidence.FORWARD_NATIVE_PRODUCT_REQUEST_SCHEMA
            or body.get("generation_id") != self.context.generation_id
            or body.get("generation_context_sha256") != self.context.context_sha256
            or body.get("native_universe_sha256")
            != self.context.native_universe_sha256
            or body.get("config_sha256") != self.context.config_sha256
            or body.get("causal_precommit_payload_sha256") != self._causal_sha256
            or body.get("native_products_must_be_sealed_before_forward_frame_access")
            is not True
            or body.get("resolver_may_choose_winners_or_product_keys") is not False
            or body.get(
                "confers_causal_current_refit_capital_publication_or_trading_authority"
            )
            is not False
        ):
            _fail("FORWARD_NATIVE_PRODUCT_REQUEST_BINDING_MISMATCH")
        _hex64(
            body.get("historical_precommit_receipt_sha256"),
            "FORWARD_HISTORICAL_PRECOMMIT_RECEIPT_INVALID",
        )
        _hex64(
            body.get("historical_run_receipt_sha256"),
            "FORWARD_HISTORICAL_RUN_RECEIPT_INVALID",
        )
        rows = body.get("anchors")
        if (
            type(rows) is not list
            or body.get("anchor_count") != len(self._anchors)
            or len(rows) != len(self._anchors)
            or body.get("required_roster_sha256") != _sha(rows)
        ):
            _fail("FORWARD_NATIVE_PRODUCT_ANCHOR_ROSTER_INVALID")
        expected_precommit = historical.precommit_historical_anchor_run(
            self.context,
            run_id=self._run_id,
            process_precommit_receipt_sha256=self._causal_sha256,
            optimizer_producer_release_sha256=(
                historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
            ),
            anchors=list(self.anchors),
        )
        if (
            body["historical_precommit_receipt_sha256"]
            != expected_precommit.receipt_sha256
        ):
            _fail("FORWARD_HISTORICAL_PRECOMMIT_RECEIPT_MISMATCH")

        normalized_rows: list[dict[str, Any]] = []
        for row, anchor in zip(rows, self._anchors, strict=True):
            geometry = anchor.geometry
            expected_cutoff = (
                date.fromisoformat(str(geometry["forward_end_date"]))
                + timedelta(days=1)
            ).isoformat() + "T00:00:00Z"
            if type(row) is not dict or set(row) != {
                "anchor_id",
                "forward_end_exclusive_utc",
                "execution_product_keys",
                "valuation_m5_product_keys",
            }:
                _fail("FORWARD_NATIVE_PRODUCT_ANCHOR_ROW_INVALID")
            execution_keys = row.get("execution_product_keys")
            valuation_keys = row.get("valuation_m5_product_keys")
            if (
                row.get("anchor_id") != geometry["anchor_id"]
                or row.get("forward_end_exclusive_utc") != expected_cutoff
                or type(execution_keys) is not list
                or execution_keys != sorted(set(execution_keys))
                or type(valuation_keys) is not list
                or valuation_keys != sorted(set(valuation_keys))
            ):
                _fail("FORWARD_NATIVE_PRODUCT_ANCHOR_ROW_MISMATCH")
            for key in sorted(set(execution_keys) | set(valuation_keys)):
                if type(key) is not str or key.count("|") != 1:
                    _fail("FORWARD_PRODUCT_KEY_INVALID", row["anchor_id"], key)
                symbol, timeframe = key.split("|", 1)
                if symbol not in _SYMBOLS or timeframe not in _TIMEFRAMES:
                    _fail("FORWARD_PRODUCT_KEY_OUTSIDE_PINNED_ROSTER", key)
                if key in valuation_keys and timeframe != "M5":
                    _fail("FORWARD_VALUATION_TIMEFRAME_INVALID", key)
            normalized_rows.append(
                {
                    "anchor_id": row["anchor_id"],
                    "forward_end_exclusive_utc": expected_cutoff,
                    "execution_product_keys": list(execution_keys),
                    "valuation_m5_product_keys": list(valuation_keys),
                }
            )
        if normalized_rows != rows or _sha(normalized_rows) != body[
            "required_roster_sha256"
        ]:
            _fail("FORWARD_NATIVE_PRODUCT_REQUIRED_ROSTER_CHANGED")
        with self._lock:
            if self._forward_resolver_used:
                _fail("FORWARD_NATIVE_PRODUCT_RESOLVER_ALREADY_CONSUMED")
            expected_completed = {
                str(anchor.augmented["anchor_id"]) for anchor in self._anchors
            }
            if self._completed_anchors != expected_completed:
                _fail("FORWARD_NATIVE_PRODUCT_HISTORICAL_RUN_NOT_COMPLETE")
            # Consume before source access; partial resolution is never retried
            # within the same frozen generation.
            self._forward_resolver_used = True

        deadline = _utc(
            json.loads(self._causal_json.decode("ascii"))[
                "evidence_access_deadline_utc"
            ],
            "CAUSAL_EVIDENCE_DEADLINE_INVALID",
        )
        execution_by_anchor: dict[str, dict[str, native_prefix.NativePrefixProduct]] = {}
        valuation_by_anchor: dict[str, dict[str, native_prefix.NativePrefixProduct]] = {}
        for row in normalized_rows:
            anchor_id = str(row["anchor_id"])
            execution_keys = row["execution_product_keys"]
            valuation_keys = row["valuation_m5_product_keys"]
            products: dict[str, native_prefix.NativePrefixProduct] = {}
            for key in sorted(set(execution_keys) | set(valuation_keys)):
                symbol, timeframe = key.split("|", 1)
                accessed = _now_utc_seconds()
                if _utc(accessed, "FORWARD_SOURCE_ACCESS_TIME_INVALID") > deadline:
                    _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED")
                try:
                    product = native_prefix.produce_activated_native_prefix(
                        data_dir=self._data_dir,
                        context=self.context,
                        symbol=symbol,
                        timeframe=timeframe,
                        cutoff_utc=row["forward_end_exclusive_utc"],
                        source_accessed_at_utc=accessed,
                        production_contract_required=True,
                    )
                except Exception as exc:
                    raise HistoricalSearchProducerError(
                        "FORWARD_NATIVE_PREFIX_PRODUCTION_FAILED",
                        (anchor_id, key, type(exc).__name__, str(exc)),
                    ) from exc
                if product.source_state != native_prefix.AVAILABLE:
                    _fail(
                        "FORWARD_NATIVE_PREFIX_NOT_AVAILABLE",
                        anchor_id,
                        key,
                        product.source_state,
                        product.native_prefix.get("source_reason"),
                    )
                products[key] = product
            execution_by_anchor[anchor_id] = {
                key: products[key] for key in execution_keys
            }
            valuation_by_anchor[anchor_id] = {
                key: products[key] for key in valuation_keys
            }
        if datetime.now(_UTC) > deadline:
            _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED_DURING_FORWARD_PREFIX")
        return causal_evidence.ForwardNativeProductResolution(
            execution_products_by_anchor=execution_by_anchor,
            valuation_products_by_anchor=valuation_by_anchor,
        )

    def __call__(
        self,
        request: causal_evidence.ForwardNativeProductRequest,
    ) -> causal_evidence.ForwardNativeProductResolution:
        return self.resolve_forward_native_products(request)


def preplan_historical_search(
    *,
    data_dir: Path,
    context: authority_spool.GenerationSpoolContext,
    config: PipelineConfig,
    process: search_planner.FrozenProductionSearchProcess,
    causal_precommit_payload: Mapping[str, Any],
    run_id: str,
    anchors: Sequence[Mapping[str, Any]],
) -> PreplannedHistoricalSearch:
    """Create the real pre-evidence roster without strategy-return access."""

    if type(context) is not authority_spool.GenerationSpoolContext:
        _fail("GENERATION_CONTEXT_INVALID")
    if not isinstance(config, PipelineConfig):
        _fail("PIPELINE_CONFIG_INVALID")
    if type(process) is not search_planner.FrozenProductionSearchProcess:
        _fail("FROZEN_SEARCH_PROCESS_INVALID")
    try:
        search_planner.verify_frozen_production_search_process(context, process)
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "FROZEN_SEARCH_PROCESS_REVERIFY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    process_body = process.to_dict()
    if type(run_id) is not str or process_body.get("run_id") != run_id:
        _fail("RUN_ID_SEARCH_PROCESS_MISMATCH")
    payload = _owned_mapping(
        causal_precommit_payload,
        "CAUSAL_PRECOMMIT_PAYLOAD_INVALID",
    )
    try:
        causal._validate_precommit_payload(payload)  # noqa: SLF001
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "CAUSAL_PRECOMMIT_RAW_REVERIFY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    definition = payload.get("process_definition")
    causal_anchors = definition.get("anchor_plan") if type(definition) is dict else None
    if (
        type(anchors) not in {list, tuple}
        or type(causal_anchors) is not list
        or len(anchors) != len(causal_anchors)
    ):
        _fail("CAUSAL_ANCHOR_ROSTER_MISMATCH")
    geometries = [_geometry(value) for value in anchors]
    if geometries != [_geometry(value) for value in causal_anchors]:
        _fail("CAUSAL_ANCHOR_GEOMETRY_MISMATCH")
    try:
        root = Path(data_dir).resolve(strict=True)
    except OSError as exc:
        raise HistoricalSearchProducerError("NATIVE_DATA_ROOT_INVALID") from exc
    if not root.is_dir():
        _fail("NATIVE_DATA_ROOT_NOT_DIRECTORY")
    causal_sha = causal.canonical_sha256(payload)
    planning_started = _now_utc_seconds()
    deadline = _utc(
        payload.get("evidence_access_deadline_utc"),
        "CAUSAL_EVIDENCE_DEADLINE_INVALID",
    )
    if _utc(planning_started, "SOURCE_ACCESS_TIME_INVALID") > deadline:
        _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED")
    # Deep-copy once so no caller mutation can alter plan/execution semantics
    # after the pre-evidence roster is committed.
    frozen_config = copy.deepcopy(config)

    preplanned: list[_PreplannedAnchor] = []
    for anchor_ordinal, geometry in enumerate(geometries):
        planning_sinks = {
            symbol: authority_spool.open_symbol_sink(context, symbol)
            for symbol in _SYMBOLS
        }
        try:
            request = historical.issue_historical_pre_evidence_planning_request(
                context,
                run_id=run_id,
                process_precommit_receipt_sha256=causal_sha,
                optimizer_producer_release_sha256=(
                    historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
                ),
                anchor=geometry,
                symbol_sinks=planning_sinks,
            )
        except Exception as exc:
            raise HistoricalSearchProducerError(
                "PRE_EVIDENCE_PLANNING_REQUEST_FAILED",
                (geometry["anchor_id"], type(exc).__name__, str(exc)),
            ) from exc
        products: list[native_prefix.NativePrefixProduct] = []
        binding_digests: list[str] = []

        def bindings() -> Iterable[dict[str, Any]]:
            for symbol in _SYMBOLS:
                for timeframe in _TIMEFRAMES:
                    _authority: Any = None
                    plans: tuple[Any, ...] = ()
                    try:
                        accessed = _now_utc_seconds()
                        if _utc(accessed, "SOURCE_ACCESS_TIME_INVALID") > deadline:
                            _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED")
                        product = native_prefix.produce_activated_native_prefix(
                            data_dir=root,
                            context=context,
                            symbol=symbol,
                            timeframe=timeframe,
                            cutoff_utc=geometry["selection_cutoff_utc"],
                            source_accessed_at_utc=accessed,
                            production_contract_required=True,
                        )
                        products.append(product)
                        _authority, plans = _timeframe_plans(
                            context=context,
                            process=process,
                            request=request,
                            causal_precommit_payload=payload,
                            anchor_ordinal=anchor_ordinal,
                            product=product,
                            config=frozen_config,
                        )
                        if len(plans) != len(_REGIMES):
                            _fail("TIMEFRAME_PLAN_ROSTER_INCOMPLETE")
                        for regime, plan in zip(_REGIMES, plans, strict=True):
                            binding, digest = _binding_digest(plan)
                            if binding.get("cell") != {
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "regime": regime,
                            }:
                                _fail("HORIZON_BINDING_CELL_MISMATCH")
                            binding_digests.append(digest)
                            yield binding
                    except HistoricalSearchProducerError:
                        raise
                    except Exception as exc:
                        raise HistoricalSearchProducerError(
                            "HISTORICAL_PREPLANNING_TIMEFRAME_FAILED",
                            (
                                geometry["anchor_id"],
                                symbol,
                                timeframe,
                                type(exc).__name__,
                                str(exc),
                            ),
                        ) from exc
                    finally:
                        plans = ()
                        _authority = None
                        _release_timeframe_state()

        try:
            roster_sha = historical.historical_horizon_binding_roster_sha256(
                bindings()
            )
        except HistoricalSearchProducerError:
            raise
        except Exception as exc:
            raise HistoricalSearchProducerError(
                "HISTORICAL_PREPLANNING_ROSTER_FAILED",
                (geometry["anchor_id"], type(exc).__name__, str(exc)),
            ) from exc
        if (
            len(products) != _GROUPS_PER_ANCHOR
            or len(binding_digests) != _CELLS_PER_ANCHOR
            or len(set(binding_digests)) != len(binding_digests)
        ):
            _fail("HISTORICAL_PREPLANNING_ROSTER_INCOMPLETE", geometry["anchor_id"])
        augmented = {
            **geometry,
            "horizon_binding_count": _CELLS_PER_ANCHOR,
            "horizon_binding_roster_sha256": roster_sha,
        }
        preplanned.append(
            _PreplannedAnchor(
                ordinal=anchor_ordinal,
                geometry_json=_canonical_bytes(geometry, 64 * 1024),
                augmented_json=_canonical_bytes(augmented, 64 * 1024),
                selection_products=tuple(products),
                binding_digests=tuple(binding_digests),
            )
        )
        planning_sinks.clear()
    return PreplannedHistoricalSearch(
        context=context,
        data_dir=root,
        config=frozen_config,
        process=process,
        causal_precommit_payload=payload,
        run_id=run_id,
        planning_started_at_utc=planning_started,
        anchors=preplanned,
    )


def historical_search_producer_contract() -> dict[str, Any]:
    return {
        "schema_version": PRODUCER_SCHEMA,
        "authority_scope": AUTHORITY_SCOPE,
        "preplanning_return_or_winner_evidence_access": False,
        "canonical_post_precommit_plan_regeneration_required": True,
        "same_sealed_selection_prefix_reused": True,
        "genuine_observed_cell_executor": (
            "pm_authoritative_search_execution.execute_historical_observed_cell"
        ),
        "source_unavailable_is_economic_no_trade": False,
        "technical_failure_aborts_generation": True,
        "unobserved_regimes_are_explicit": True,
        "forward_product_roster_source": "VERIFIED_HISTORICAL_BOOK_WINNERS",
        "forward_frames_opened_before_forward_precommit": False,
        "bounded_unit": "ONE_SYMBOL_TIMEFRAME_FRAME_AT_A_TIME",
        "confers_causal_current_capital_publication_or_trading_authority": False,
    }


HISTORICAL_SEARCH_PRODUCER_RELEASE_SHA256: Final = _sha(
    historical_search_producer_contract()
)


__all__ = [
    "AUTHORITY_SCOPE",
    "HISTORICAL_SEARCH_PRODUCER_RELEASE_SHA256",
    "HistoricalSearchProducerError",
    "PRODUCER_SCHEMA",
    "PreplannedHistoricalSearch",
    "historical_search_producer_contract",
    "preplan_historical_search",
]
