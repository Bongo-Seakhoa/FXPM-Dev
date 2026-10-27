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

from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
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
import pm_historical_scheduler as historical_scheduler
import pm_native_prefix_production as native_prefix
import pm_production_config_binding as config_binding
import pm_production_search_planner as search_planner
from pm_core import PipelineConfig


PRODUCER_SCHEMA: Final = "fxpm.production-historical-search-producer.v1"
WORKER_CONTRACT_SCHEMA: Final = "fxpm.historical-symbol-worker-contract.v1"
PREPLAN_BUNDLE_SCHEMA: Final = "fxpm.historical-preplan-symbol-bundle.v2"
FORWARD_STAGE_BUNDLE_SCHEMA: Final = "fxpm.historical-forward-stage-bundle.v1"
WORKER_RESULT_SCHEMA: Final = "fxpm.historical-symbol-worker-result.v1"
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
_MAX_WORKER_BUNDLE_BYTES: Final = (
    historical_adapter.HISTORICAL_SYMBOL_WORKER_BUNDLE_MAX_BYTES
)


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


@dataclass(frozen=True, slots=True)
class HistoricalSymbolSchedulerInputs:
    """Outcome-free routing inputs for governed historical workers."""

    active_config_path: Path
    requested_workers: int


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
    if state == native_prefix.TECHNICAL_FAILURE:
        # An in-flight or unauthenticated infrastructure failure is not a
        # completed abstention.  Preserve the authoritative executor's typed
        # fail-closed diagnosis and never build Book cells from it.
        search_execution.fit_terminal_native_timeframe(
            product=product,
            config=config,
        )
        _fail("TERMINAL_NATIVE_TECHNICAL_FAILURE_REQUIRES_ABORT")
    if state not in {
        native_prefix.SOURCE_UNAVAILABLE,
        native_prefix.TECHNICAL_TERMINAL,
    }:
        _fail("NATIVE_PREFIX_SOURCE_STATE_INVALID", state)
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
    expected_status = (
        "HISTORICAL_SOURCE_UNAVAILABLE"
        if state == native_prefix.SOURCE_UNAVAILABLE
        else "HISTORICAL_TERMINAL_TECHNICAL"
    )
    if len(plans) != len(_REGIMES) or any(
        _plan_status(plan) != expected_status for plan in plans
    ):
        _fail("TERMINAL_PLAN_ROSTER_INVALID")
    return terminal, tuple(plans)


def _request_json(request: historical.AnchorProducerRequest) -> tuple[str, str]:
    if type(request) is not historical.AnchorProducerRequest:
        _fail("ANCHOR_REQUEST_INVALID")
    try:
        raw = request._body_json  # noqa: SLF001 - exact issuer bytes are the API.
        text = raw.decode("ascii")
    except (AttributeError, UnicodeError) as exc:
        raise HistoricalSearchProducerError("ANCHOR_REQUEST_BYTES_INVALID") from exc
    if (
        not raw
        or len(raw) > 256 * 1024
        or _canonical_bytes(request.to_dict(), 256 * 1024) != raw
        or hashlib.sha256(raw).hexdigest() != request.receipt_sha256
    ):
        _fail("ANCHOR_REQUEST_BYTES_INVALID")
    return text, request.receipt_sha256


def _prefix_route(product: native_prefix.NativePrefixProduct) -> dict[str, Any]:
    if type(product) is not native_prefix.NativePrefixProduct:
        _fail("NATIVE_PREFIX_PRODUCT_INVALID")
    index = dict(product.index)
    activation = index.get("activation")
    descriptor = dict(product.index_descriptor)
    if (
        type(activation) is not dict
        or product.source_state != index.get("source_state")
        or descriptor.get("sha256") != product.index_sha256
    ):
        _fail("NATIVE_PREFIX_ROUTE_INVALID")
    route = {
        "symbol": index.get("symbol"),
        "timeframe": index.get("timeframe"),
        "cutoff_utc": index.get("cutoff_utc"),
        "source_state": product.source_state,
        "index_sha256": product.index_sha256,
        "index_descriptor": descriptor,
        "activation_manifest_sha256": activation.get("manifest_sha256"),
    }
    _hex64(route["index_sha256"], "NATIVE_PREFIX_ROUTE_INDEX_INVALID")
    _hex64(
        route["activation_manifest_sha256"],
        "NATIVE_PREFIX_ROUTE_ACTIVATION_INVALID",
    )
    return json.loads(_canonical_bytes(route, 64 * 1024).decode("ascii"))


def _reopen_prefix_route(
    *,
    route: Mapping[str, Any],
    data_dir: Path,
    context: authority_spool.GenerationSpoolContext,
    symbol: str,
    timeframe: str,
    include_technical_failure: bool = False,
    _pin_consumed_activation: bool = False,
) -> native_prefix.NativePrefixProduct:
    owned = _owned_mapping(route, "NATIVE_PREFIX_ROUTE_INVALID")
    if (
        set(owned)
        != {
            "symbol",
            "timeframe",
            "cutoff_utc",
            "source_state",
            "index_sha256",
            "index_descriptor",
            "activation_manifest_sha256",
        }
        or owned["symbol"] != symbol
        or owned["timeframe"] != timeframe
        or owned["source_state"] not in ({
            native_prefix.AVAILABLE,
            native_prefix.SOURCE_UNAVAILABLE,
            native_prefix.TECHNICAL_TERMINAL,
        } | ({native_prefix.TECHNICAL_FAILURE} if include_technical_failure else set()))
    ):
        _fail("NATIVE_PREFIX_ROUTE_BINDING_INVALID", symbol, timeframe)
    pinned_arguments = ({"pinned_activation_manifest_sha256": owned["activation_manifest_sha256"]}
                        if _pin_consumed_activation else {})
    try:
        product = native_prefix.verify_activated_native_prefix(
            data_dir=data_dir,
            context=context,
            index_descriptor=owned["index_descriptor"],
            expected_index_sha256=_hex64(
                owned["index_sha256"],
                "NATIVE_PREFIX_ROUTE_INDEX_INVALID",
            ),
            expected_symbol=symbol,
            expected_timeframe=timeframe,
            expected_cutoff_utc=owned["cutoff_utc"],
            expected_activation_manifest_sha256=_hex64(
                owned["activation_manifest_sha256"],
                "NATIVE_PREFIX_ROUTE_ACTIVATION_INVALID",
            ),
            production_contract_required=True,
            **pinned_arguments,
        )
    except HistoricalSearchProducerError:
        raise
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "NATIVE_PREFIX_ROUTE_REOPEN_FAILED",
            (symbol, timeframe, type(exc).__name__, str(exc)),
        ) from exc
    if product.source_state != owned["source_state"]:
        _fail("NATIVE_PREFIX_ROUTE_STATE_CHANGED", symbol, timeframe)
    return product


def _read_worker_bundle(
    context: authority_spool.GenerationSpoolContext,
    descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    value = _owned_mapping(descriptor, "WORKER_BUNDLE_DESCRIPTOR_INVALID")
    if (
        set(value)
        != {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
        or value.get("schema_version") != authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA
        or value.get("format") != authority_spool.CANONICAL_JSON_FORMAT
        or type(value.get("size_bytes")) is not int
        or not 0 < value["size_bytes"] <= _MAX_WORKER_BUNDLE_BYTES
    ):
        _fail("WORKER_BUNDLE_DESCRIPTOR_INVALID")
    digest = _hex64(value.get("sha256"), "WORKER_BUNDLE_DESCRIPTOR_INVALID")
    expected_relative = (
        f"objects/sha256/{digest[:2]}/{digest}."
        f"{authority_spool.CANONICAL_JSON_FORMAT}"
    )
    if value.get("relative_path") != expected_relative:
        _fail("WORKER_BUNDLE_DESCRIPTOR_PATH_INVALID")
    try:
        path = context.root.joinpath(*Path(expected_relative).parts)
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
    except OSError as exc:
        raise HistoricalSearchProducerError(
            "WORKER_BUNDLE_READ_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if (
        before.st_size != value["size_bytes"]
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or len(raw) != value["size_bytes"]
        or hashlib.sha256(raw).hexdigest() != digest
    ):
        _fail("WORKER_BUNDLE_CHANGED")
    try:
        body = json.loads(raw.decode("ascii"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise HistoricalSearchProducerError("WORKER_BUNDLE_JSON_INVALID") from exc
    if type(body) is not dict or _canonical_bytes(body, _MAX_WORKER_BUNDLE_BYTES) != raw:
        _fail("WORKER_BUNDLE_NONCANONICAL")
    return body


def _worker_authority(
    contract: Mapping[str, Any],
    symbol: str,
    *,
    phase: str,
) -> tuple[
    dict[str, Any],
    authority_spool.GenerationSpoolContext,
    search_planner.FrozenProductionSearchProcess,
    historical.AnchorProducerRequest,
    PipelineConfig,
    Path,
    dict[str, Any],
]:
    owned = _owned_mapping(contract, "HISTORICAL_WORKER_CONTRACT_INVALID")
    common_fields = {
        "schema_version",
        "phase",
        "spool_root",
        "generation_id",
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "run_id",
        "search_process_receipt_sha256",
        "active_config_path",
        "effective_config_sha256",
        "data_dir",
        "causal_precommit_payload",
        "anchor_ordinal",
        "request_json",
        "request_sha256",
        "symbol_roster_sha256",
        "timeframe_roster_sha256",
    }
    phase_fields = (
        {"source_accessed_at_utc"}
        if phase == "PREPLAN"
        else {"selection_routes_by_symbol", "binding_digests_by_symbol"}
    )
    if (
        set(owned) != common_fields | phase_fields
        or owned.get("schema_version") != WORKER_CONTRACT_SCHEMA
        or owned.get("phase") != phase
        or symbol not in _SYMBOLS
        or owned.get("symbol_roster_sha256") != _sha(list(_SYMBOLS))
        or owned.get("timeframe_roster_sha256") != _sha(list(_TIMEFRAMES))
        or type(owned.get("anchor_ordinal")) is not int
        or owned["anchor_ordinal"] < 0
    ):
        _fail("HISTORICAL_WORKER_CONTRACT_BINDING_INVALID", phase, symbol)
    try:
        context = authority_spool.reopen_generation_context(
            Path(owned["spool_root"]),
            expected_generation_id=owned["generation_id"],
            expected_native_universe_sha256=owned["native_universe_sha256"],
            expected_config_sha256=owned["config_sha256"],
        )
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "HISTORICAL_WORKER_CONTEXT_REOPEN_FAILED",
            (phase, symbol, type(exc).__name__, str(exc)),
        ) from exc
    if context.context_sha256 != owned["generation_context_sha256"]:
        _fail("HISTORICAL_WORKER_CONTEXT_CHANGED", phase, symbol)
    try:
        process = search_planner.freeze_production_search_process(
            context,
            run_id=owned["run_id"],
        )
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "HISTORICAL_WORKER_PROCESS_REOPEN_FAILED",
            (phase, symbol, type(exc).__name__, str(exc)),
        ) from exc
    if process.receipt_sha256 != owned["search_process_receipt_sha256"]:
        _fail("HISTORICAL_WORKER_PROCESS_CHANGED", phase, symbol)
    try:
        active_config_path = Path(owned["active_config_path"])
        config_payload, supplied = config_binding.load_effective_pipeline_config(
            active_config_path
        )
        frozen = config_binding.freeze_production_pipeline_config(
            context,
            active_config_path=active_config_path,
            config_payload=config_payload,
            supplied_config=supplied,
        )
        if frozen.effective_config_sha256 != owned["effective_config_sha256"]:
            _fail("HISTORICAL_WORKER_EFFECTIVE_CONFIG_CHANGED", phase, symbol)
        config = frozen.materialize(context)
        data_dir = Path(config.data_dir).resolve(strict=True)
        expected_data_dir = Path(owned["data_dir"]).resolve(strict=True)
    except HistoricalSearchProducerError:
        raise
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "HISTORICAL_WORKER_CONFIG_REOPEN_FAILED",
            (phase, symbol, type(exc).__name__, str(exc)),
        ) from exc
    if data_dir != expected_data_dir or not data_dir.is_dir():
        _fail("HISTORICAL_WORKER_DATA_ROOT_CHANGED", phase, symbol)
    request_text = owned.get("request_json")
    if type(request_text) is not str:
        _fail("HISTORICAL_WORKER_REQUEST_INVALID", phase, symbol)
    try:
        request_raw = request_text.encode("ascii")
    except UnicodeError as exc:
        raise HistoricalSearchProducerError(
            "HISTORICAL_WORKER_REQUEST_INVALID"
        ) from exc
    sink = authority_spool.open_symbol_sink(
        context,
        symbol,
        manifest_domain=authority_spool.HISTORICAL_ANCHOR_MANIFEST_DOMAIN,
    )
    try:
        request = historical.reopen_anchor_producer_request_for_symbol(
            context,
            request_body_bytes=request_raw,
            expected_receipt_sha256=owned["request_sha256"],
            symbol=symbol,
            sink=sink,
        )
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "HISTORICAL_WORKER_REQUEST_REOPEN_FAILED",
            (phase, symbol, type(exc).__name__, str(exc)),
        ) from exc
    payload = _owned_mapping(
        owned["causal_precommit_payload"],
        "HISTORICAL_WORKER_CAUSAL_PAYLOAD_INVALID",
    )
    return owned, context, process, request, config, data_dir, payload


def _preplan_worker_authorities(
    contract: Mapping[str, Any],
    symbol: str,
) -> tuple[tuple[Any, ...], ...]:
    owned = _owned_mapping(contract, "HISTORICAL_PREPLAN_BATCH_CONTRACT_INVALID")
    records = owned.pop("preplan_anchors", None)
    if type(records) is not list or not records:
        _fail("HISTORICAL_PREPLAN_BATCH_ANCHORS_INVALID", symbol)
    expected_ordinals = list(range(len(records)))
    authorities: list[tuple[Any, ...]] = []
    for ordinal, record in enumerate(records):
        if type(record) is not dict or set(record) != {
            "anchor_ordinal",
            "request_json",
            "request_sha256",
            "source_accessed_at_utc",
        }:
            _fail("HISTORICAL_PREPLAN_BATCH_ANCHOR_INVALID", symbol, ordinal)
        single = {
            **owned,
            **record,
            "phase": "PREPLAN",
        }
        authorities.append(_worker_authority(single, symbol, phase="PREPLAN"))
    if [value[0]["anchor_ordinal"] for value in authorities] != expected_ordinals:
        _fail("HISTORICAL_PREPLAN_BATCH_ORDER_INVALID", symbol)
    return tuple(authorities)


def _historical_preplan_symbol_worker(
    contract: Mapping[str, Any],
    symbol: str,
) -> dict[str, Any]:
    """Spawn-safe pre-evidence worker for one symbol and every frozen anchor."""

    authorities = _preplan_worker_authorities(contract, symbol)
    first_owned, context, _process, _request, _config, data_dir, causal_payload = (
        authorities[0]
    )
    deadline = _utc(
        causal_payload.get("evidence_access_deadline_utc"),
        "CAUSAL_EVIDENCE_DEADLINE_INVALID",
    )
    routes: list[list[dict[str, Any]]] = [[] for _ in authorities]
    bindings: list[list[dict[str, Any]]] = [[] for _ in authorities]
    for timeframe in _TIMEFRAMES:
        if _utc(_now_utc_seconds(), "SOURCE_ACCESS_TIME_INVALID") > deadline:
            _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED")
        products = native_prefix.produce_activated_native_prefixes(
            data_dir=data_dir,
            context=context,
            symbol=symbol,
            timeframe=timeframe,
            requests=tuple(
                {
                    "cutoff_utc": authority[3].to_dict()["anchor"][
                        "selection_cutoff_utc"
                    ],
                    "source_accessed_at_utc": authority[0][
                        "source_accessed_at_utc"
                    ],
                }
                for authority in authorities
            ),
            production_contract_required=True,
        )
        if len(products) != len(authorities):
            _fail("HISTORICAL_PREPLAN_PREFIX_BATCH_INCOMPLETE", symbol, timeframe)
        for anchor_index, (authority, product) in enumerate(
            zip(authorities, products, strict=True)
        ):
            (
                owned,
                anchor_context,
                process,
                request,
                config,
                anchor_data_dir,
                anchor_payload,
            ) = authority
            _authority: Any = None
            plans: tuple[Any, ...] = ()
            plan: Any = None
            try:
                if (
                    anchor_context.context_sha256 != context.context_sha256
                    or anchor_data_dir != data_dir
                    or anchor_payload != causal_payload
                ):
                    _fail("HISTORICAL_PREPLAN_BATCH_AUTHORITY_DRIFT", symbol)
                routes[anchor_index].append(_prefix_route(product))
                _authority, plans = _timeframe_plans(
                    context=anchor_context,
                    process=process,
                    request=request,
                    causal_precommit_payload=anchor_payload,
                    anchor_ordinal=owned["anchor_ordinal"],
                    product=product,
                    config=config,
                )
                if len(plans) != len(_REGIMES):
                    _fail("TIMEFRAME_PLAN_ROSTER_INCOMPLETE", symbol, timeframe)
                for regime, plan in zip(_REGIMES, plans, strict=True):
                    binding, _digest = _binding_digest(plan)
                    if binding.get("cell") != {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "regime": regime,
                    }:
                        _fail("HORIZON_BINDING_CELL_MISMATCH")
                    bindings[anchor_index].append(binding)
            finally:
                plan = None
                plans = ()
                _authority = None
                _release_timeframe_state()
    anchor_rows: list[dict[str, Any]] = []
    for authority, anchor_routes, anchor_bindings in zip(
        authorities,
        routes,
        bindings,
        strict=True,
    ):
        _owned, _context, _process, request, _config, _data, _payload = authority
        if (
            len(anchor_routes) != len(_TIMEFRAMES)
            or len(anchor_bindings) != len(_TIMEFRAMES) * len(_REGIMES)
        ):
            _fail("HISTORICAL_PREPLAN_SYMBOL_ROSTER_INCOMPLETE", symbol)
        anchor_rows.append(
            {
                "request_sha256": request.receipt_sha256,
                "anchor_id": request.to_dict()["anchor"]["anchor_id"],
                "selection_routes": anchor_routes,
                "horizon_bindings": anchor_bindings,
            }
        )
    body = {
        "schema_version": PREPLAN_BUNDLE_SCHEMA,
        "generation_context_sha256": context.context_sha256,
        "symbol": symbol,
        "timeframe_order": list(_TIMEFRAMES),
        "regime_order": list(_REGIMES),
        "anchors": anchor_rows,
    }
    descriptor = authority_spool.write_canonical_json_artifact(context, body)
    return {
        "schema_version": WORKER_RESULT_SCHEMA,
        "symbol": symbol,
        "bundle_descriptor": descriptor,
    }


def _historical_forward_stage_symbol_worker(
    contract: Mapping[str, Any],
    symbol: str,
) -> dict[str, Any]:
    """Spawn-safe, non-authoritative forward prefix staging for one symbol."""

    owned = _owned_mapping(contract, "FORWARD_STAGE_WORKER_CONTRACT_INVALID")
    expected = {
        "schema_version",
        "phase",
        "spool_root",
        "generation_id",
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "data_dir",
        "evidence_access_deadline_utc",
        "symbol_roster_sha256",
        "timeframe_roster_sha256",
        "requests_by_symbol",
    }
    requests_by_symbol = owned.get("requests_by_symbol")
    requests = (
        requests_by_symbol.get(symbol)
        if type(requests_by_symbol) is dict
        else None
    )
    if (
        set(owned) != expected
        or owned.get("schema_version") != WORKER_CONTRACT_SCHEMA
        or owned.get("phase") != "FORWARD_STAGE"
        or symbol not in _SYMBOLS
        or owned.get("symbol_roster_sha256") != _sha(list(_SYMBOLS))
        or owned.get("timeframe_roster_sha256") != _sha(list(_TIMEFRAMES))
        or type(requests) is not dict
        or not requests
        or any(timeframe not in _TIMEFRAMES for timeframe in requests)
    ):
        _fail("FORWARD_STAGE_WORKER_CONTRACT_INVALID", symbol)
    evidence_deadline = _utc(
        owned["evidence_access_deadline_utc"],
        "CAUSAL_EVIDENCE_DEADLINE_INVALID",
    )
    try:
        context = authority_spool.reopen_generation_context(
            Path(owned["spool_root"]),
            expected_generation_id=owned["generation_id"],
            expected_native_universe_sha256=owned["native_universe_sha256"],
            expected_config_sha256=owned["config_sha256"],
        )
        data_dir = Path(owned["data_dir"]).resolve(strict=True)
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "FORWARD_STAGE_WORKER_AUTHORITY_REOPEN_FAILED",
            (symbol, type(exc).__name__, str(exc)),
        ) from exc
    if (
        context.context_sha256 != owned["generation_context_sha256"]
        or not data_dir.is_dir()
    ):
        _fail("FORWARD_STAGE_WORKER_AUTHORITY_CHANGED", symbol)
    rows: list[dict[str, Any]] = []
    for timeframe in _TIMEFRAMES:
        cutoffs = requests.get(timeframe)
        if cutoffs is None:
            continue
        if (
            type(cutoffs) is not list
            or not cutoffs
            or any(type(cutoff) is not str for cutoff in cutoffs)
            or cutoffs != sorted(set(cutoffs))
        ):
            _fail("FORWARD_STAGE_WORKER_CUTOFFS_INVALID", symbol, timeframe)
        for cutoff in cutoffs:
            _utc(cutoff, "FORWARD_STAGE_WORKER_CUTOFF_INVALID")
        accessed = _now_utc_seconds()
        if _utc(accessed, "FORWARD_SOURCE_ACCESS_TIME_INVALID") > evidence_deadline:
            _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED")
        candidates = native_prefix.stage_activated_native_prefixes(
            data_dir=data_dir,
            context=context,
            symbol=symbol,
            timeframe=timeframe,
            requests=tuple(
                {
                    "cutoff_utc": cutoff,
                    "source_accessed_at_utc": accessed,
                }
                for cutoff in cutoffs
            ),
            production_contract_required=True,
        )
        rows.append(
            {
                "timeframe": timeframe,
                "cutoffs": list(cutoffs),
                "candidates": list(candidates),
            }
        )
    body = {
        "schema_version": FORWARD_STAGE_BUNDLE_SCHEMA,
        "generation_context_sha256": context.context_sha256,
        "symbol": symbol,
        "rows": rows,
    }
    descriptor = authority_spool.write_canonical_json_artifact(context, body)
    return {
        "schema_version": WORKER_RESULT_SCHEMA,
        "symbol": symbol,
        "bundle_descriptor": descriptor,
    }


def _historical_execute_symbol_worker(
    contract: Mapping[str, Any],
    symbol: str,
) -> dict[str, Any]:
    """Spawn-safe complete historical search worker for one anchor/symbol."""

    (
        owned,
        context,
        process,
        request,
        config,
        data_dir,
        causal_payload,
    ) = _worker_authority(contract, symbol, phase="EXECUTE")
    routes_by_symbol = owned["selection_routes_by_symbol"]
    digests_by_symbol = owned["binding_digests_by_symbol"]
    routes = routes_by_symbol.get(symbol) if type(routes_by_symbol) is dict else None
    expected_digests = (
        digests_by_symbol.get(symbol) if type(digests_by_symbol) is dict else None
    )
    if (
        type(routes) is not list
        or len(routes) != len(_TIMEFRAMES)
        or type(expected_digests) is not list
        or len(expected_digests) != len(_TIMEFRAMES) * len(_REGIMES)
    ):
        _fail("HISTORICAL_EXECUTION_WORKER_ROSTER_INVALID", symbol)
    capture = historical_adapter.open_historical_symbol_capture(
        context,
        request,
        symbol,
    )
    cell_index = 0
    for timeframe, route in zip(_TIMEFRAMES, routes, strict=True):
        product: native_prefix.NativePrefixProduct | None = None
        authority: Any = None
        plans: tuple[Any, ...] = ()
        plan: Any = None
        result: search_execution.HistoricalObservedCellExecution | None = None
        try:
            product = _reopen_prefix_route(
                route=route,
                data_dir=data_dir,
                context=context,
                symbol=symbol,
                timeframe=timeframe,
            )
            authority, plans = _timeframe_plans(
                context=context,
                process=process,
                request=request,
                causal_precommit_payload=causal_payload,
                anchor_ordinal=owned["anchor_ordinal"],
                product=product,
                config=config,
            )
            if len(plans) != len(_REGIMES):
                _fail("TIMEFRAME_PLAN_ROSTER_INCOMPLETE", symbol, timeframe)
            for regime, plan in zip(_REGIMES, plans, strict=True):
                binding, digest = _binding_digest(plan)
                if (
                    digest != expected_digests[cell_index]
                    or binding.get("cell")
                    != {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "regime": regime,
                    }
                ):
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
                terminal_native_timeframe=(
                    authority
                    if product.source_state == native_prefix.TECHNICAL_TERMINAL
                    else None
                ),
            )
            if product.source_state == native_prefix.AVAILABLE:
                counts = _verified_support_counts(authority)
                for regime, plan in zip(_REGIMES, plans, strict=True):
                    status = _plan_status(plan)
                    if counts[regime] == 0 or status == "HISTORICAL_BOUNDARY_LIMITED":
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
                        search_process=process,
                        cell_plan=plan,
                        config=config,
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
                    result = None
        finally:
            result = None
            plan = None
            plans = ()
            authority = None
            product = None
            _release_timeframe_state()
    if cell_index != len(expected_digests):
        _fail("HISTORICAL_EXECUTION_WORKER_ROSTER_INCOMPLETE", symbol)
    descriptor = historical_adapter.spool_historical_symbol_worker_bundle(capture)
    return {
        "schema_version": WORKER_RESULT_SCHEMA,
        "symbol": symbol,
        "bundle_descriptor": descriptor,
    }


@dataclass(frozen=True, slots=True)
class _PreplannedAnchor:
    ordinal: int
    geometry_json: bytes
    augmented_json: bytes
    selection_routes_json: tuple[bytes, ...]
    binding_digests: tuple[str, ...]

    @property
    def geometry(self) -> dict[str, Any]:
        return json.loads(self.geometry_json.decode("ascii"))

    @property
    def augmented(self) -> dict[str, Any]:
        return json.loads(self.augmented_json.decode("ascii"))

    @property
    def selection_routes(self) -> tuple[dict[str, Any], ...]:
        return tuple(json.loads(value.decode("ascii")) for value in self.selection_routes_json)


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
        "_effective_config_sha256",
        "_forward_resolver_used",
        "_lock",
        "_process",
        "_run_id",
        "_planning_started_at_utc",
        "_symbol_scheduler",
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
        symbol_scheduler: HistoricalSymbolSchedulerInputs,
        effective_config_sha256: str,
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
        self._symbol_scheduler = symbol_scheduler
        self._effective_config_sha256 = _hex64(
            effective_config_sha256,
            "EFFECTIVE_CONFIG_SHA256_INVALID",
        )
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
            or body.get("process_precommit_receipt_sha256") != self._causal_sha256
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
        routes = anchor.selection_routes
        if (
            len(routes) != _GROUPS_PER_ANCHOR
            or len(anchor.binding_digests) != _CELLS_PER_ANCHOR
        ):
            _fail("ANCHOR_REPLAY_ROSTER_INCOMPLETE")
        routes_by_symbol: dict[str, list[dict[str, Any]]] = {}
        digests_by_symbol: dict[str, list[str]] = {}
        route_index = 0
        cell_index = 0
        for symbol in _SYMBOLS:
            symbol_routes = list(routes[route_index : route_index + len(_TIMEFRAMES)])
            symbol_digests = list(
                anchor.binding_digests[
                    cell_index : cell_index + len(_TIMEFRAMES) * len(_REGIMES)
                ]
            )
            if (
                len(symbol_routes) != len(_TIMEFRAMES)
                or len(symbol_digests) != len(_TIMEFRAMES) * len(_REGIMES)
            ):
                _fail("ANCHOR_REPLAY_ROSTER_INCOMPLETE")
            routes_by_symbol[symbol] = symbol_routes
            digests_by_symbol[symbol] = symbol_digests
            route_index += len(_TIMEFRAMES)
            cell_index += len(_TIMEFRAMES) * len(_REGIMES)
        request_json, request_sha = _request_json(request)
        contract = {
            "schema_version": WORKER_CONTRACT_SCHEMA,
            "phase": "EXECUTE",
            "spool_root": str(self.context.root),
            "generation_id": self.context.generation_id,
            "native_universe_sha256": self.context.native_universe_sha256,
            "config_sha256": self.context.config_sha256,
            "generation_context_sha256": self.context.context_sha256,
            "run_id": self._run_id,
            "search_process_receipt_sha256": self._process.receipt_sha256,
            "active_config_path": str(self._symbol_scheduler.active_config_path),
            "effective_config_sha256": self._effective_config_sha256,
            "data_dir": str(self._data_dir),
            "causal_precommit_payload": json.loads(
                self._causal_json.decode("ascii")
            ),
            "anchor_ordinal": anchor.ordinal,
            "request_json": request_json,
            "request_sha256": request_sha,
            "symbol_roster_sha256": _sha(list(_SYMBOLS)),
            "timeframe_roster_sha256": _sha(list(_TIMEFRAMES)),
            "selection_routes_by_symbol": routes_by_symbol,
            "binding_digests_by_symbol": digests_by_symbol,
        }
        try:
            results = historical_scheduler.run_symbol_scheduler(
                _historical_execute_symbol_worker,
                contract=contract,
                symbols=_SYMBOLS,
                requested_workers=self._symbol_scheduler.requested_workers,
                phase="historical_execute",
            )
        except historical_scheduler.HistoricalSchedulerError as exc:
            raise HistoricalSearchProducerError(exc.code, exc.details) from exc
        for symbol in _SYMBOLS:
            result = results.get(symbol)
            if (
                type(result) is not dict
                or set(result)
                != {"schema_version", "symbol", "bundle_descriptor"}
                or result.get("schema_version") != WORKER_RESULT_SCHEMA
                or result.get("symbol") != symbol
            ):
                _fail("HISTORICAL_EXECUTION_WORKER_RESULT_INVALID", symbol)
            try:
                product = historical_adapter.reopen_historical_symbol_worker_bundle(
                    self.context,
                    request,
                    result["bundle_descriptor"],
                )
            except Exception as exc:
                raise HistoricalSearchProducerError(
                    "HISTORICAL_EXECUTION_WORKER_BUNDLE_REOPEN_FAILED",
                    (anchor.augmented["anchor_id"], symbol, type(exc).__name__, str(exc)),
                ) from exc
            if (
                type(product) is not historical.AnchorSymbolProduct
                or product.symbol != symbol
            ):
                _fail("HISTORICAL_EXECUTION_WORKER_PRODUCT_INVALID", symbol)
            yield product
            del product
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
            or _canonical_bytes(body, 4 * 1024 * 1024) != request._body_json  # noqa: SLF001 - exact issuer bytes are the API.
            or hashlib.sha256(request._body_json).hexdigest()  # noqa: SLF001
            != request.request_sha256
            or body.get("schema_version")
            != causal_evidence.FORWARD_NATIVE_PRODUCT_REQUEST_SCHEMA
            or body.get("generation_id") != self.context.generation_id
            or body.get("generation_context_sha256") != self.context.context_sha256
            or body.get("native_universe_sha256") != self.context.native_universe_sha256
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
        if (
            normalized_rows != rows
            or _sha(normalized_rows) != body["required_roster_sha256"]
        ):
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
        requests_by_symbol: dict[str, dict[str, list[str]]] = {}
        for row in normalized_rows:
            cutoff = row["forward_end_exclusive_utc"]
            for key in sorted(
                set(row["execution_product_keys"])
                | set(row["valuation_m5_product_keys"])
            ):
                symbol, timeframe = key.split("|", 1)
                values = requests_by_symbol.setdefault(symbol, {}).setdefault(
                    timeframe,
                    [],
                )
                if cutoff not in values:
                    values.append(cutoff)
        for timeframes in requests_by_symbol.values():
            for cutoffs in timeframes.values():
                cutoffs.sort()
        if not requests_by_symbol:
            return causal_evidence.ForwardNativeProductResolution(
                execution_products_by_anchor={
                    str(row["anchor_id"]): {} for row in normalized_rows
                },
                valuation_products_by_anchor={
                    str(row["anchor_id"]): {} for row in normalized_rows
                },
            )
        contract = {
            "schema_version": WORKER_CONTRACT_SCHEMA,
            "phase": "FORWARD_STAGE",
            "spool_root": str(self.context.root),
            "generation_id": self.context.generation_id,
            "native_universe_sha256": self.context.native_universe_sha256,
            "config_sha256": self.context.config_sha256,
            "generation_context_sha256": self.context.context_sha256,
            "data_dir": str(self._data_dir),
            "evidence_access_deadline_utc": deadline.isoformat(
                timespec="seconds"
            ).replace("+00:00", "Z"),
            "symbol_roster_sha256": _sha(list(_SYMBOLS)),
            "timeframe_roster_sha256": _sha(list(_TIMEFRAMES)),
            "requests_by_symbol": requests_by_symbol,
        }
        try:
            staged = historical_scheduler.run_symbol_scheduler(
                _historical_forward_stage_symbol_worker,
                contract=contract,
                symbols=tuple(requests_by_symbol),
                requested_workers=self._symbol_scheduler.requested_workers,
                phase="historical_forward_stage",
            )
        except historical_scheduler.HistoricalSchedulerError as exc:
            raise HistoricalSearchProducerError(exc.code, exc.details) from exc
        candidates_by_cell: dict[
            tuple[str, str], tuple[tuple[str, dict[str, Any]], ...]
        ] = {}
        for symbol in requests_by_symbol:
            result = staged.get(symbol)
            if (
                type(result) is not dict
                or set(result)
                != {"schema_version", "symbol", "bundle_descriptor"}
                or result.get("schema_version") != WORKER_RESULT_SCHEMA
                or result.get("symbol") != symbol
            ):
                _fail("FORWARD_STAGE_WORKER_RESULT_INVALID", symbol)
            body = _read_worker_bundle(self.context, result["bundle_descriptor"])
            if (
                set(body)
                != {
                    "schema_version",
                    "generation_context_sha256",
                    "symbol",
                    "rows",
                }
                or body.get("schema_version") != FORWARD_STAGE_BUNDLE_SCHEMA
                or body.get("generation_context_sha256")
                != self.context.context_sha256
                or body.get("symbol") != symbol
                or type(body.get("rows")) is not list
            ):
                _fail("FORWARD_STAGE_WORKER_BUNDLE_INVALID", symbol)
            observed_timeframes: set[str] = set()
            for stage_row in body["rows"]:
                if (
                    type(stage_row) is not dict
                    or set(stage_row) != {"timeframe", "cutoffs", "candidates"}
                ):
                    _fail("FORWARD_STAGE_WORKER_ROW_INVALID", symbol)
                timeframe = stage_row["timeframe"]
                cutoffs = stage_row["cutoffs"]
                candidates = stage_row["candidates"]
                if (
                    timeframe in observed_timeframes
                    or cutoffs != requests_by_symbol[symbol].get(timeframe)
                    or type(candidates) is not list
                    or len(candidates) != len(cutoffs)
                ):
                    _fail("FORWARD_STAGE_WORKER_ROW_MISMATCH", symbol, timeframe)
                observed_timeframes.add(timeframe)
                candidates_by_cell[(symbol, timeframe)] = tuple(
                    zip(cutoffs, candidates, strict=True)
                )
            if observed_timeframes != set(requests_by_symbol[symbol]):
                _fail("FORWARD_STAGE_WORKER_ROSTER_INCOMPLETE", symbol)

        verified_by_cell: dict[
            tuple[str, str], tuple[native_prefix.NativePrefixProduct, ...]
        ] = {}
        verification_cancelled = threading.Event()

        def verify_cell(
            cell: tuple[str, str],
        ) -> tuple[
            tuple[str, str],
            tuple[native_prefix.NativePrefixProduct, ...],
        ]:
            symbol, timeframe = cell
            values = candidates_by_cell[cell]
            products = native_prefix.verify_activated_native_prefix_candidates(
                data_dir=self._data_dir,
                context=self.context,
                symbol=symbol,
                timeframe=timeframe,
                candidates=tuple(candidate for _cutoff, candidate in values),
                production_contract_required=True,
                require_current_activation=True,
                cancellation_event=verification_cancelled,
            )
            return cell, products

        worker_count = min(
            self._symbol_scheduler.requested_workers,
            len(candidates_by_cell),
        )
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(verify_cell, cell): cell
                for cell in candidates_by_cell
            }
            try:
                for future in as_completed(futures):
                    cell, products = future.result()
                    verified_by_cell[cell] = products
            except Exception:
                verification_cancelled.set()
                for future in futures:
                    future.cancel()
                raise
        product_by_cutoff: dict[
            tuple[str, str, str], native_prefix.NativePrefixProduct
        ] = {}
        for cell, values in candidates_by_cell.items():
            products = verified_by_cell.get(cell)
            if products is None or len(products) != len(values):
                _fail("FORWARD_NATIVE_PREFIX_VERIFY_ROSTER_INCOMPLETE", *cell)
            for (cutoff, _candidate), product in zip(
                values,
                products,
                strict=True,
            ):
                if product.source_state != native_prefix.AVAILABLE:
                    _fail(
                        "FORWARD_NATIVE_PREFIX_NOT_AVAILABLE",
                        cell[0],
                        cell[1],
                        cutoff,
                        product.source_state,
                        product.native_prefix.get("source_reason"),
                    )
                product_by_cutoff[(cell[0], cell[1], cutoff)] = product
        execution_by_anchor: dict[
            str, dict[str, native_prefix.NativePrefixProduct]
        ] = {}
        valuation_by_anchor: dict[
            str, dict[str, native_prefix.NativePrefixProduct]
        ] = {}
        for row in normalized_rows:
            anchor_id = str(row["anchor_id"])
            cutoff = row["forward_end_exclusive_utc"]
            execution_by_anchor[anchor_id] = {
                key: product_by_cutoff[(*key.split("|", 1), cutoff)]
                for key in row["execution_product_keys"]
            }
            valuation_by_anchor[anchor_id] = {
                key: product_by_cutoff[(*key.split("|", 1), cutoff)]
                for key in row["valuation_m5_product_keys"]
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
    symbol_scheduler: HistoricalSymbolSchedulerInputs,
) -> PreplannedHistoricalSearch:
    """Create the real pre-evidence roster without strategy-return access."""

    if type(context) is not authority_spool.GenerationSpoolContext:
        _fail("GENERATION_CONTEXT_INVALID")
    if not isinstance(config, PipelineConfig):
        _fail("PIPELINE_CONFIG_INVALID")
    if type(process) is not search_planner.FrozenProductionSearchProcess:
        _fail("FROZEN_SEARCH_PROCESS_INVALID")
    if (
        type(symbol_scheduler) is not HistoricalSymbolSchedulerInputs
        or type(symbol_scheduler.requested_workers) is not int
        or type(symbol_scheduler.requested_workers) is bool
        or symbol_scheduler.requested_workers <= 0
    ):
        _fail("HISTORICAL_SYMBOL_SCHEDULER_INPUT_INVALID")
    try:
        active_config_path = Path(symbol_scheduler.active_config_path)
        active_payload, _active_supplied = (
            config_binding.load_effective_pipeline_config(active_config_path)
        )
        frozen_config_binding = config_binding.freeze_production_pipeline_config(
            context,
            active_config_path=active_config_path,
            config_payload=active_payload,
            supplied_config=config,
        )
        frozen_config = frozen_config_binding.materialize(context)
    except Exception as exc:
        raise HistoricalSearchProducerError(
            "HISTORICAL_SYMBOL_SCHEDULER_INPUT_INVALID",
            (type(exc).__name__, str(exc)),
        ) from exc
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
        frozen_data_root = Path(frozen_config.data_dir).resolve(strict=True)
    except OSError as exc:
        raise HistoricalSearchProducerError("NATIVE_DATA_ROOT_INVALID") from exc
    if not root.is_dir():
        _fail("NATIVE_DATA_ROOT_NOT_DIRECTORY")
    if root != frozen_data_root:
        _fail("NATIVE_DATA_ROOT_CONFIG_MISMATCH")
    causal_sha = causal.canonical_sha256(payload)
    planning_started = _now_utc_seconds()
    deadline = _utc(
        payload.get("evidence_access_deadline_utc"),
        "CAUSAL_EVIDENCE_DEADLINE_INVALID",
    )
    if _utc(planning_started, "SOURCE_ACCESS_TIME_INVALID") > deadline:
        _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED")
    planning_sinks = {
        symbol: authority_spool.open_symbol_sink(context, symbol)
        for symbol in _SYMBOLS
    }
    planning_records: list[dict[str, Any]] = []
    for anchor_ordinal, geometry in enumerate(geometries):
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
        source_accessed = _now_utc_seconds()
        if _utc(source_accessed, "SOURCE_ACCESS_TIME_INVALID") > deadline:
            _fail("CAUSAL_EVIDENCE_ACCESS_WINDOW_EXPIRED")
        request_json, request_sha = _request_json(request)
        planning_records.append(
            {
                "geometry": geometry,
                "request": request,
                "worker": {
                    "anchor_ordinal": anchor_ordinal,
                    "request_json": request_json,
                    "request_sha256": request_sha,
                    "source_accessed_at_utc": source_accessed,
                },
            }
        )
    contract = {
        "schema_version": WORKER_CONTRACT_SCHEMA,
        "phase": "PREPLAN_BATCH",
        "spool_root": str(context.root),
        "generation_id": context.generation_id,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "generation_context_sha256": context.context_sha256,
        "run_id": run_id,
        "search_process_receipt_sha256": process.receipt_sha256,
        "active_config_path": str(frozen_config_binding.active_config_path),
        "effective_config_sha256": (
            frozen_config_binding.effective_config_sha256
        ),
        "data_dir": str(root),
        "causal_precommit_payload": payload,
        "symbol_roster_sha256": _sha(list(_SYMBOLS)),
        "timeframe_roster_sha256": _sha(list(_TIMEFRAMES)),
        "preplan_anchors": [record["worker"] for record in planning_records],
    }
    try:
        results = historical_scheduler.run_symbol_scheduler(
            _historical_preplan_symbol_worker,
            contract=contract,
            symbols=_SYMBOLS,
            requested_workers=symbol_scheduler.requested_workers,
            phase="historical_preplan",
        )
    except historical_scheduler.HistoricalSchedulerError as exc:
        raise HistoricalSearchProducerError(exc.code, exc.details) from exc
    bodies_by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in _SYMBOLS:
        result = results.get(symbol)
        if (
            type(result) is not dict
            or set(result) != {"schema_version", "symbol", "bundle_descriptor"}
            or result.get("schema_version") != WORKER_RESULT_SCHEMA
            or result.get("symbol") != symbol
        ):
            _fail("HISTORICAL_PREPLAN_WORKER_RESULT_INVALID", symbol)
        body = _read_worker_bundle(context, result["bundle_descriptor"])
        expected_fields = {
            "schema_version",
            "generation_context_sha256",
            "symbol",
            "timeframe_order",
            "regime_order",
            "anchors",
        }
        if (
            set(body) != expected_fields
            or body.get("schema_version") != PREPLAN_BUNDLE_SCHEMA
            or body.get("generation_context_sha256") != context.context_sha256
            or body.get("symbol") != symbol
            or body.get("timeframe_order") != list(_TIMEFRAMES)
            or body.get("regime_order") != list(_REGIMES)
            or type(body.get("anchors")) is not list
            or len(body["anchors"]) != len(planning_records)
        ):
            _fail("HISTORICAL_PREPLAN_WORKER_BUNDLE_INVALID", symbol)
        bodies_by_symbol[symbol] = body
    preplanned: list[_PreplannedAnchor] = []
    for anchor_ordinal, record in enumerate(planning_records):
        geometry = record["geometry"]
        request = record["request"]
        routes: list[dict[str, Any]] = []
        bindings: list[dict[str, Any]] = []
        binding_digests: list[str] = []
        for symbol in _SYMBOLS:
            row = bodies_by_symbol[symbol]["anchors"][anchor_ordinal]
            if (
                type(row) is not dict
                or set(row)
                != {
                    "request_sha256",
                    "anchor_id",
                    "selection_routes",
                    "horizon_bindings",
                }
                or row.get("request_sha256") != request.receipt_sha256
                or row.get("anchor_id") != geometry["anchor_id"]
                or type(row.get("selection_routes")) is not list
                or len(row["selection_routes"]) != len(_TIMEFRAMES)
                or type(row.get("horizon_bindings")) is not list
                or len(row["horizon_bindings"])
                != len(_TIMEFRAMES) * len(_REGIMES)
            ):
                _fail("HISTORICAL_PREPLAN_WORKER_ANCHOR_INVALID", symbol)
            for timeframe, route in zip(
                _TIMEFRAMES,
                row["selection_routes"],
                strict=True,
            ):
                if (
                    type(route) is not dict
                    or route.get("symbol") != symbol
                    or route.get("timeframe") != timeframe
                    or route.get("cutoff_utc") != geometry["selection_cutoff_utc"]
                ):
                    _fail(
                        "HISTORICAL_PREPLAN_ROUTE_ROSTER_INVALID",
                        symbol,
                        timeframe,
                    )
                routes.append(route)
            expected_cells = (
                (symbol, timeframe, regime)
                for timeframe in _TIMEFRAMES
                for regime in _REGIMES
            )
            for cell, binding in zip(
                expected_cells,
                row["horizon_bindings"],
                strict=True,
            ):
                if type(binding) is not dict or binding.get("cell") != {
                    "symbol": cell[0],
                    "timeframe": cell[1],
                    "regime": cell[2],
                }:
                    _fail("HISTORICAL_PREPLAN_BINDING_ROSTER_INVALID", *cell)
                bindings.append(binding)
                binding_digests.append(_sha(binding))
        try:
            roster_sha = historical.historical_horizon_binding_roster_sha256(
                bindings
            )
        except HistoricalSearchProducerError:
            raise
        except Exception as exc:
            raise HistoricalSearchProducerError(
                "HISTORICAL_PREPLANNING_ROSTER_FAILED",
                (geometry["anchor_id"], type(exc).__name__, str(exc)),
            ) from exc
        if (
            len(routes) != _GROUPS_PER_ANCHOR
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
                selection_routes_json=tuple(
                    _canonical_bytes(route, 64 * 1024) for route in routes
                ),
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
        symbol_scheduler=HistoricalSymbolSchedulerInputs(
            active_config_path=frozen_config_binding.active_config_path,
            requested_workers=symbol_scheduler.requested_workers,
        ),
        effective_config_sha256=frozen_config_binding.effective_config_sha256,
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
        "selection_cutoffs_share_one_source_pass_per_symbol_timeframe": True,
        "forward_cutoffs_share_one_source_pass_per_requested_symbol_timeframe": True,
        "forward_staging_uses_process_isolated_symbol_scheduler": True,
        "forward_independent_verification_worker_source": (
            "PIPELINE_OPTIMIZATION_WORKERS"
        ),
        "bounded_unit": "ONE_SYMBOL_TIMEFRAME_FRAME_AT_A_TIME",
        "symbol_scheduler_worker_source": "PIPELINE_OPTIMIZATION_WORKERS",
        "ram_admission_gate": False,
        "worker_results_are_compact_disk_receipts": True,
        "completion_order_can_change_book_input_order": False,
        "confers_causal_current_capital_publication_or_trading_authority": False,
    }


HISTORICAL_SEARCH_PRODUCER_RELEASE_SHA256: Final = _sha(
    historical_search_producer_contract()
)


__all__ = [
    "AUTHORITY_SCOPE",
    "HISTORICAL_SEARCH_PRODUCER_RELEASE_SHA256",
    "HistoricalSymbolSchedulerInputs",
    "HistoricalSearchProducerError",
    "PRODUCER_SCHEMA",
    "PreplannedHistoricalSearch",
    "historical_search_producer_contract",
    "preplan_historical_search",
]


# One declared cell's search receipts; this is not a trading/account supervisor.
DECLARED_CELL_RUN_SCHEMA = "fxpm.declared-cell-owner-run.v1"
DECLARED_SLOT_COMMIT_SCHEMA = "fxpm.declared-slot-commit.v1"
_DECLARED_SLOT_POST_COMPLETION_COMMIT_SCHEMA = "fxpm.declared-slot-commit.v2"
_DECLARED_SLOT_POINTER_SCHEMA = "fxpm.declared-slot-commit-pointer.v1"
_DECLARED_CELL_TERMINAL_SCHEMA = "fxpm.declared-cell-owner-terminal.v1"
_DECLARED_CELL_POST_COMPLETION_TERMINAL_SCHEMA = "fxpm.declared-cell-owner-terminal.v2"


@dataclass(frozen=True)
class DeclaredCellOwnerRunReceipt:
    _body_json: bytes
    def to_dict(self):
        return json.loads(self._body_json)
    @property
    def receipt_sha256(self):
        return hashlib.sha256(self._body_json).hexdigest()


@dataclass(frozen=True)
class DeclaredCellOwnerProgress:
    slot_summaries: tuple[dict[str, Any], ...]
    slot_commit_artifacts: tuple[dict[str, Any], ...]
    first_uncommitted_slot: dict[str, Any] | None
    terminal_stop: bool
    last_commit_sha256: str | None
    terminal_failure: dict[str, Any] | None = None
    symbol_receipt: dict[str, Any] | None = None
    run_receipt: dict[str, Any] | None = None


class _DeclaredSlotPostCompletionFailure(HistoricalSearchProducerError):
    def __init__(self, completion, failure):
        super().__init__("DECLARED_SLOT_POST_COMPLETION_FAILURE")
        self.completion = json.loads(_canonical_bytes(completion))
        self.failure = json.loads(_canonical_bytes(failure))


def _declared_failure_details(exc):
    return {"exception_type": type(exc).__name__[:128],
        "failure_code": str(getattr(exc, "code", "DECLARED_SLOT_SOURCE_FAILURE"))[:128],
        "failure_observed_at_utc": _now_utc_seconds()}


@dataclass(frozen=True)
class _DeclaredSlotSourceSession:
    _body_json: bytes
    _fitted: Any
    _config: Any
    _frozen_config: Any
    _entry: Mapping[str, Any]
    def to_dict(self):
        return json.loads(self._body_json)
    @property
    def receipt_sha256(self):
        return hashlib.sha256(self._body_json).hexdigest()


def _declared_base_completion(request, *, status, reason_code, source_entry):
    body = request.to_dict()
    return {"schema_version": historical_adapter.DECLARED_SLOT_COMPLETION_SCHEMA,
        "request_body": body, "request_sha256": request.receipt_sha256,
        "precommit_sha256": body["precommit_sha256"], "cell_id": body["cell_id"], "slot_id": body["slot_id"],
        "status": status, "reason_code": reason_code, "owner": None,
        "source_session_entry": source_entry, "source_group": None, "regime_fit": None,
        "horizon_plan": None, "plan_transport": None, "frozen_plan_receipt": None,
        "frozen_descriptor_graph": None, "horizon_winner_cross_binding": None,
        "winner_request_entry": None, "winner_decision_entry": None,
        "raw_production_receipt": None, "production_descriptor_graph": None,
        "completed_at_utc": _now_utc_seconds(), "qualification_authority": False,
        "capital_authority": False, "live_authority": False, "order_authority": False}


def _verify_declared_completion_transport(context, *, completion, expected_edition):
    code = "DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"
    if type(completion) is not dict:
        _fail(code)
    try:
        actual = historical.declared_slot_transport_edition(completion.get("request_body"))
    except historical.HistoricalAnchorOrchestrationError as exc:
        raise HistoricalSearchProducerError(code) from exc
    if (expected_edition not in (historical.DECLARED_WINNER_TRANSPORT_V15,
            historical.DECLARED_WINNER_TRANSPORT_V16) or actual != expected_edition):
        _fail(code)
    descriptor = completion.get("frozen_plan_receipt")
    if descriptor is None:
        if completion.get("status") == "OWNER":
            _fail(code)
        return
    try:
        receipt, _ = historical_adapter._reopen_worker_ascii_json_descriptor(context,
            descriptor, maximum_bytes=historical_adapter.winner_selection.MAX_REQUEST_BYTES,
            code=code)
    except historical_adapter.HistoricalProductionAdapterError as exc:
        raise HistoricalSearchProducerError(code) from exc
    expected_schema = (historical_adapter.raw_winner.V16_FROZEN_PLAN_SCHEMA
        if expected_edition == historical.DECLARED_WINNER_TRANSPORT_V16
        else historical_adapter.raw_winner.CURRENT_FROZEN_PLAN_SCHEMA)
    if receipt.get("schema_version") != expected_schema:
        _fail(code)


def _make_declared_slot_source_authority():
    sessions = {}

    def verify(context, *, request, process, source_session):
        historical.verify_declared_slot_producer_request(context, request, process=process)
        historical._verify_declared_request_identity(request, context, require_live=True)
        row = sessions.get(request.receipt_sha256)
        if (type(source_session) is not _DeclaredSlotSourceSession or row is None
                or row[0] is not source_session or row[1] != source_session._body_json
                or row[2] != id(context) or row[3] is not request
                or row[4] is not source_session._fitted or row[5] is not source_session._config
                or row[6] != _canonical_bytes(dict(source_session._entry))):
            _fail("DECLARED_SLOT_SOURCE_CAPABILITY_INVALID")
        return source_session

    def by_fitted(context, *, request, process, fitted):
        row = sessions.get(request.receipt_sha256)
        if row is None or row[4] is not fitted:
            _fail("DECLARED_SLOT_SOURCE_CAPABILITY_INVALID")
        return verify(context, request=request, process=process, source_session=row[0])

    def execute(context, *, request, process, frozen_pipeline_config):
        search_planner._require_current_process(process)
        body = historical.verify_declared_slot_producer_request(context, request, process=process)
        edition = historical.declared_slot_transport_edition(body)
        historical._verify_declared_request_identity(request, context, require_live=True)

        def finish(completion):
            _verify_declared_completion_transport(context, completion=completion,
                expected_edition=edition)
            return completion

        if request.receipt_sha256 in sessions:
            _fail("DECLARED_SLOT_SOURCE_ALREADY_OPENED")
        if type(frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig:
            _fail("DECLARED_SLOT_CONFIG_INVALID")
        frozen_pipeline_config.verify_current(context)
        config = frozen_pipeline_config.materialize(context)
        if (frozen_pipeline_config.effective_config_sha256 != body["effective_config_sha256"]
                or config.discovery_risk_per_trade_pct != 1.0):
            _fail("DECLARED_SLOT_CONFIG_BINDING_MISMATCH")
        window = body["source_access_window_utc"]
        in_window = window["start_utc"] <= _now_utc_seconds() < window["end_exclusive_utc"]
        if in_window:
            profile = search_execution.spread_profile.load_spread_profile(Path(config.data_dir) / "native" / "terms" / "spread_profile.json")
            if profile.sha256 != body["spread_profile_identity"]["profile_sha256"]:
                _fail("DECLARED_SLOT_PROFILE_BINDING_MISMATCH")
        sink = request.symbol_sink(body["symbol"])
        source_body = {"schema_version": "fxpm.declared-slot-source-session.v1",
            "request_sha256": request.receipt_sha256, "generation_context_sha256": context.context_sha256,
            "config_sha256": context.config_sha256,
            "effective_config_sha256": body["effective_config_sha256"],
            "spread_profile_identity": body["spread_profile_identity"],
            "source_accessed_at_utc": None, "selection_cutoff_utc": body["anchor"]["selection_cutoff_utc"],
            "source_state": "FAILED", "native_source_route": None, "native_prefix_index_descriptor": None,
            "native_prefix_index_sha256": None, "source_group_decision_sha256": None,
            "native_snapshot_entry": None, "native_snapshot_errors": None,
            "regime_fit_decision_sha256": None, "caller_supplied_product_or_fit_accepted": False,
            "qualification_authority": False, "live_authority": False, "order_authority": False}
        capture = None
        try:
            if not in_window:
                _fail("DECLARED_SLOT_SOURCE_WINDOW_INVALID")
            accessed = _now_utc_seconds()
            if not window["start_utc"] <= accessed < window["end_exclusive_utc"]:
                _fail("DECLARED_SLOT_SOURCE_WINDOW_INVALID")
            source_body["source_accessed_at_utc"] = accessed
            import pm_portfolio_replay as portfolio_replay
            snapshot, snapshot_errors = portfolio_replay.active_native_portfolio_contract(
                Path(config.data_dir), book.PINNED_SYMBOL_ORDER, timeframes=book.TIMEFRAME_ORDER)
            # Exact contract bytes deduplicate in the content-addressed spool;
            # every slot keeps its own roster reference to that bound evidence.
            source_body["native_snapshot_entry"] = sink.put_json(
                f"{body['artifact_namespace']}/native-snapshot", "declared_slot_native_snapshot", snapshot)
            source_body["native_snapshot_errors"] = snapshot_errors
            snapshot_status = historical._declared_slot_native_snapshot_status(context, snapshot, snapshot_errors)
            if snapshot_status == "MISMATCH":
                _fail("DECLARED_SLOT_NATIVE_SNAPSHOT_MISMATCH")
            product = native_prefix.produce_activated_native_prefix(data_dir=Path(config.data_dir), context=context,
                symbol=body["symbol"], timeframe=body["timeframe"],
                cutoff_utc=body["anchor"]["selection_cutoff_utc"], source_accessed_at_utc=accessed,
                production_contract_required=True)
            product = native_prefix.reverify_activated_native_prefix_product(product, context, require_current_activation=True)
            source_body.update(source_state=product.source_state, native_source_route=_prefix_route(product),
                native_prefix_index_descriptor=dict(product.index_descriptor), native_prefix_index_sha256=product.index_sha256)
            # Unavailable ACTIVE still follows the genuine technical-failure
            # route. Every usable or authenticated-absence product must join
            # the frozen snapshot, including an A/B/A pointer race.
            if product.source_state != native_prefix.TECHNICAL_FAILURE:
                if snapshot_status != "MATCH":
                    _fail("DECLARED_SLOT_NATIVE_SNAPSHOT_" + snapshot_status)
                if not historical._declared_slot_consumed_native_snapshot_matches(snapshot, product):
                    _fail("DECLARED_SLOT_CONSUMED_NATIVE_SNAPSHOT_MISMATCH")
            if product.source_state != native_prefix.AVAILABLE:
                entry = sink.put_json(f"{body['artifact_namespace']}/source-session", "declared_slot_source_session", source_body)
                return finish(_declared_base_completion(request,
                    status="MISSING" if product.source_state in {native_prefix.SOURCE_UNAVAILABLE, native_prefix.TECHNICAL_TERMINAL} else "STOP",
                    reason_code="TERMINAL_TECHNICAL" if product.source_state == native_prefix.TECHNICAL_TERMINAL else product.source_state,
                    source_entry=entry))
            fitted = search_execution.fit_native_timeframe_for_search(product=product, config=config, acquisition_attempts_exact=1)
            if any(t.descriptor()["profile_sha256"] != body["spread_profile_identity"]["profile_sha256"] for t in fitted._spread_tiers.values()):
                _fail("DECLARED_SLOT_PROFILE_BINDING_MISMATCH")
            source_body.update(source_group_decision_sha256=fitted.source_group_decision.receipt_sha256,
                regime_fit_decision_sha256=fitted.regime_fit_decision.receipt_sha256)
            entry = sink.put_json(f"{body['artifact_namespace']}/source-session", "declared_slot_source_session", source_body)
            session = _DeclaredSlotSourceSession(_canonical_bytes(source_body), fitted, config, frozen_pipeline_config, entry)
            sessions[request.receipt_sha256] = (session, session._body_json, id(context), request, fitted, config, _canonical_bytes(entry))
            if (fitted.source_group_decision.to_dict().get("availability") != book.AVAILABLE
                    or fitted.regime_fit_decision.to_dict().get("fit_state") != book.REGIME_FITTED):
                _fail("DECLARED_SLOT_FIT_NOT_READY")
            counts = _verified_support_counts(fitted)
            if counts[body["regime"]] == 0:
                completion = _declared_base_completion(request, status="NO_TRADE", reason_code="REGIME_UNOBSERVED", source_entry=entry)
                completion["source_group"] = historical_adapter._owned_decision_envelope(fitted.source_group_decision, "DECLARED_SLOT_SOURCE_INVALID")
                completion["regime_fit"] = historical_adapter._owned_decision_envelope(fitted.regime_fit_decision, "DECLARED_SLOT_FIT_INVALID")
                return finish(completion)
            plan = search_planner.build_declared_slot_observed_cell_plan(context, process=process, request=request, fitted=fitted)
            open_capture = (historical_adapter.open_transport_declared_slot_capture
                if edition == historical.DECLARED_WINNER_TRANSPORT_V16
                else historical_adapter.open_declared_slot_capture)
            execute_cell = (search_execution.execute_transport_historical_observed_cell
                if edition == historical.DECLARED_WINNER_TRANSPORT_V16
                else search_execution.execute_historical_observed_cell)
            capture = open_capture(context, request=request, process=process, plan=plan, source_session=session)
            execute_cell(capture=capture, fitted=fitted, regime=body["regime"],
                search_process=process, cell_plan=plan, config=config)
            return finish(capture.completion)
        except Exception as exc:
            if authority_spool.is_resource_capacity_error(exc):
                raise
            if capture is not None and capture._completion is not None:
                raise _DeclaredSlotPostCompletionFailure(capture.completion, _declared_failure_details(exc)) from exc
            # A handled technical stop, never an economic abstention. Hard process
            # interruptions (BaseException/termination) leave no final commit.
            source_body.update(_declared_failure_details(exc))
            source_body["failure_phase"] = "PRE_SOURCE" if source_body["source_accessed_at_utc"] is None else "SOURCE"
            # Keep the earlier immutable source session if it has already issued.
            failure_id = f"{body['artifact_namespace']}/technical-source-session"
            entry = sink.put_json(failure_id, "declared_slot_source_session", source_body)
            code = source_body["failure_code"]
            reason = code if code in {"DECLARED_SLOT_NATIVE_SNAPSHOT_MISMATCH",
                "DECLARED_SLOT_NATIVE_SNAPSHOT_UNAVAILABLE", "DECLARED_SLOT_NATIVE_SNAPSHOT_INVALID",
                "DECLARED_SLOT_CONSUMED_NATIVE_SNAPSHOT_MISMATCH"} else "DECLARED_SLOT_SOURCE_FAILURE"
            return finish(_declared_base_completion(request, status="STOP", reason_code=reason, source_entry=entry))
        finally:
            # No fitted frame or live permit survives the portable completion.
            sessions.pop(request.receipt_sha256, None)

    return execute, verify, by_fitted


(execute_declared_slot_owner_search, _verified_declared_slot_source_session,
 _verified_declared_slot_fitted) = _make_declared_slot_source_authority()


def _declared_commit_path(precommit, cell_id, ordinal):
    return f"declared-slots/{precommit.receipt_sha256}/{cell_id}/commits/{ordinal:02d}.json"


def _declared_terminal_path(precommit, cell_id):
    return f"declared-slots/{precommit.receipt_sha256}/{cell_id}/terminal.json"


def _declared_failure_time_order_anomalies(failure, *, completed_at_utc, recorded_at_utc):
    if (type(failure) is not dict or set(failure) != {"exception_type", "failure_code", "failure_observed_at_utc"}
            or any(type(failure[key]) is not str or not 1 <= len(failure[key]) <= 128
                for key in ("exception_type", "failure_code"))):
        _fail("DECLARED_SLOT_TERMINAL_INVALID")
    completed = _utc(completed_at_utc, "DECLARED_SLOT_TERMINAL_INVALID")
    observed = _utc(failure["failure_observed_at_utc"], "DECLARED_SLOT_TERMINAL_INVALID")
    recorded = _utc(recorded_at_utc, "DECLARED_SLOT_TERMINAL_INVALID")
    anomalies = []
    if observed < completed:
        anomalies.append("FAILURE_OBSERVED_BEFORE_COMPLETION")
    if recorded < observed:
        anomalies.append("FAILURE_RECORDED_BEFORE_OBSERVED")
    return anomalies


def _verify_declared_terminal_failure(body, *, context, precommit, cell_id, completed, last_commit_sha256):
    expected = {"schema_version", "precommit_sha256", "generation_context_sha256", "cell_id", "slot_id",
        "ordinal", "request_sha256", "last_commit_sha256", "reason_code", "failure_phase", "failure",
        "recorded_at_utc", "terminal_stop", "qualification_authority", "capital_authority", "live_authority", "order_authority"}
    if type(body) is dict and body.get("schema_version") == _DECLARED_CELL_POST_COMPLETION_TERMINAL_SCHEMA:
        expected.add("time_order_anomalies")
    if type(body) is not dict or set(body) != expected or not completed or last_commit_sha256 is None:
        _fail("DECLARED_SLOT_TERMINAL_INVALID")
    last = completed[-1]
    if (body["schema_version"] not in (_DECLARED_CELL_TERMINAL_SCHEMA, _DECLARED_CELL_POST_COMPLETION_TERMINAL_SCHEMA)
            or body["precommit_sha256"] != precommit.receipt_sha256
            or body["generation_context_sha256"] != context.context_sha256 or body["cell_id"] != cell_id
            or body["slot_id"] != last["slot_id"] or body["ordinal"] != last["ordinal"]
            or type(body["ordinal"]) is not int
            or body["request_sha256"] != last["request_sha256"]
            or body["last_commit_sha256"] != last_commit_sha256
            or body["reason_code"] != "DECLARED_SLOT_POST_COMPLETION_FAILURE"
            or body["failure_phase"] != "POST_COMPLETION" or body["terminal_stop"] is not True
            or any(body[key] is not False for key in ("qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("DECLARED_SLOT_TERMINAL_BINDING_MISMATCH")
    anomalies = _declared_failure_time_order_anomalies(body["failure"],
        completed_at_utc=last["completed_at_utc"], recorded_at_utc=body["recorded_at_utc"])
    if body["schema_version"] == _DECLARED_CELL_TERMINAL_SCHEMA:
        if anomalies:
            _fail("DECLARED_SLOT_TERMINAL_INVALID")
    elif type(body["time_order_anomalies"]) is not list or body["time_order_anomalies"] != anomalies:
        _fail("DECLARED_SLOT_TERMINAL_INVALID")
    return body


def _declared_terminal_failure_body(context, *, precommit, cell_id, completed, last_commit_sha256,
                                    failure, recorded_at_utc, time_order_anomalies=None):
    last = completed[-1]
    body = {"schema_version": _DECLARED_CELL_TERMINAL_SCHEMA, "precommit_sha256": precommit.receipt_sha256,
        "generation_context_sha256": context.context_sha256, "cell_id": cell_id,
        "slot_id": last["slot_id"], "ordinal": last["ordinal"], "request_sha256": last["request_sha256"],
        "last_commit_sha256": last_commit_sha256, "reason_code": "DECLARED_SLOT_POST_COMPLETION_FAILURE",
        "failure_phase": "POST_COMPLETION", "failure": failure, "recorded_at_utc": recorded_at_utc,
        "terminal_stop": True, "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False}
    if time_order_anomalies is not None:
        body["schema_version"] = _DECLARED_CELL_POST_COMPLETION_TERMINAL_SCHEMA
        body["time_order_anomalies"] = time_order_anomalies
    _verify_declared_terminal_failure(body, context=context, precommit=precommit, cell_id=cell_id,
        completed=completed, last_commit_sha256=last_commit_sha256)
    return body


def _commit_declared_terminal_failure(context, *, precommit, process, cell_id, completed, last_commit_sha256, failure):
    historical.verify_declared_slot_owner_run_precommit(context, precommit, process=process)
    search_planner._require_current_process(process)
    body = _declared_terminal_failure_body(context, precommit=precommit, cell_id=cell_id,
        completed=completed, last_commit_sha256=last_commit_sha256, failure=failure,
        recorded_at_utc=_now_utc_seconds())
    authority_spool._publish_exact_json(context, _declared_terminal_path(precommit, cell_id), body, context.limits.max_json_bytes)
    return body


def _declared_terminal_failure_from_commit(body, *, context, precommit, cell_id, completed, last_commit_sha256):
    envelope = body.get("post_completion_failure")
    if (body.get("schema_version") != _DECLARED_SLOT_POST_COMPLETION_COMMIT_SCHEMA
            or type(envelope) is not dict
            or set(envelope) != {"failure", "recorded_at_utc", "time_order_anomalies"}
            or type(envelope["time_order_anomalies"]) is not list):
        _fail("DECLARED_SLOT_TERMINAL_INVALID")
    return _declared_terminal_failure_body(context, precommit=precommit, cell_id=cell_id,
        completed=completed, last_commit_sha256=last_commit_sha256, failure=envelope["failure"],
        recorded_at_utc=envelope["recorded_at_utc"], time_order_anomalies=envelope["time_order_anomalies"])


def _read_declared_record(context, path, *, maximum_bytes=None):
    target = authority_spool._local_path(context.root, path)
    if not target.exists():
        return None
    maximum = context.limits.max_json_bytes if maximum_bytes is None else maximum_bytes
    digest, size, encoded = authority_spool._stream_file(context.root, path,
        maximum_bytes=maximum, collect=True)
    try:
        body = authority_spool._strict_json_bytes(encoded, maximum)
    except Exception as exc:
        raise HistoricalSearchProducerError("DECLARED_SLOT_COMMIT_INVALID") from exc
    descriptor = {"schema_version": authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA,
        "relative_path": path, "sha256": digest, "size_bytes": size,
        "format": authority_spool.CANONICAL_JSON_FORMAT}
    return body, digest, descriptor


def _read_declared_commit(context, path):
    item = _read_declared_record(context, path)
    if item is None:
        return None
    pointer = item[0]
    if (type(pointer) is not dict or set(pointer) != {"schema_version", "commit_descriptor"}
            or pointer["schema_version"] != _DECLARED_SLOT_POINTER_SCHEMA):
        _fail("DECLARED_SLOT_COMMIT_POINTER_INVALID")
    descriptor = authority_spool._validate_descriptor(pointer["commit_descriptor"], content_addressed=True)
    if descriptor["format"] != authority_spool.CANONICAL_JSON_FORMAT:
        _fail("DECLARED_SLOT_COMMIT_POINTER_INVALID")
    item = _read_declared_record(context, descriptor["relative_path"])
    if item is None or item[2] != descriptor:
        _fail("DECLARED_SLOT_COMMIT_INTEGRITY_MISMATCH")
    return item


def _declared_commit_artifact(slot, descriptor):
    return {"artifact_id": f"declared-slot-commit/{slot['ordinal']:02d}",
        "artifact_type": "declared_slot_commit", "descriptor": descriptor}


def _declared_slot_summary(body, digest):
    completion = body["completion"]
    return {"slot_id": body["slot_id"], "ordinal": body["ordinal"], "status": completion["status"],
        "request_sha256": completion["request_sha256"], "completed_at_utc": completion["completed_at_utc"],
        "commit_sha256": digest}


def _declared_run_path(precommit, cell_id):
    return f"declared-slots/{precommit.receipt_sha256}/{cell_id}/run-receipt.json"


def _declared_run_body(context, *, precommit, cell_id, summaries, declared_slots,
                       terminal_stop, previous, symbol_receipt, terminal_failure):
    body = {"schema_version": DECLARED_CELL_RUN_SCHEMA, "precommit_sha256": precommit.receipt_sha256,
        "generation_context_sha256": context.context_sha256, "cell_id": cell_id,
        "completed_slots": len(summaries), "declared_slots": declared_slots, "terminal_stop": terminal_stop,
        "slot_commits": [{"slot_id": item["slot_id"], "status": item["status"]} for item in summaries],
        "last_commit_sha256": previous, "symbol_receipt": symbol_receipt,
        "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False}
    if terminal_failure is not None:
        body["terminal_failure"] = terminal_failure
    return body


def _declared_symbol_receipt(context, symbol, descriptor):
    return {"schema_version": authority_spool.COMPACT_RECEIPT_SCHEMA, "generation_id": context.generation_id,
        "symbol": symbol, "manifest_domain": authority_spool.GENERAL_MANIFEST_DOMAIN,
        "native_universe_sha256": context.native_universe_sha256, "config_sha256": context.config_sha256,
        "generation_context_sha256": context.context_sha256, "symbol_manifest": descriptor}


def reopen_declared_cell_owner_progress(context, *, precommit, process, cell_id, frozen_pipeline_config):
    parent = historical.verify_declared_slot_owner_run_precommit(context, precommit, process=process)
    if type(frozen_pipeline_config) is not config_binding.FrozenProductionPipelineConfig:
        _fail("DECLARED_SLOT_CONFIG_INVALID")
    frozen_pipeline_config.verify_current(context)
    if (frozen_pipeline_config.effective_config_sha256 != parent["effective_config_sha256"]
            or frozen_pipeline_config.materialize(context).discovery_risk_per_trade_pct != 1.0):
        _fail("DECLARED_SLOT_CONFIG_BINDING_MISMATCH")
    if len(parent["cells"]) != 1 or parent["cells"][0]["cell_id"] != cell_id:
        _fail("DECLARED_SLOT_CELL_INVALID")
    symbol = parent["cells"][0]["symbol"]
    manifest_path = authority_spool._manifest_relative_path(symbol, authority_spool.GENERAL_MANIFEST_DOMAIN)
    manifest = _read_declared_record(context, manifest_path, maximum_bytes=context.limits.max_manifest_bytes)
    symbol_receipt, run_receipt, verified_manifest = None, None, None
    if manifest is not None:
        symbol_receipt = _declared_symbol_receipt(context, symbol, manifest[2])
        verified_manifest = authority_spool.verify_symbol_receipt(context.root, symbol_receipt,
            expected_generation_id=context.generation_id, expected_symbol=symbol,
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_config_sha256=context.config_sha256, expected_manifest_domain=authority_spool.GENERAL_MANIFEST_DOMAIN)
    completed, commit_artifacts, previous = [], [], None
    namespaces = set()
    first, terminal, terminal_failure = None, False, None
    for slot in parent["slot_roster"]["slots"]:
        item = _read_declared_commit(context, _declared_commit_path(precommit, cell_id, slot["ordinal"]))
        if item is None:
            if first is None:
                first = dict(slot)
            continue
        body, digest, descriptor = item
        if terminal:
            _fail("DECLARED_SLOT_AFTER_TERMINAL_STOP")
        if first is not None:
            _fail("DECLARED_SLOT_PREFIX_GAP")
        expected = {"schema_version", "precommit_sha256", "generation_context_sha256",
            "cell_id", "slot_id", "ordinal", "previous_commit_sha256", "completion", "artifact_roster"}
        if type(body) is dict and body.get("schema_version") == _DECLARED_SLOT_POST_COMPLETION_COMMIT_SCHEMA:
            expected.add("post_completion_failure")
        if (type(body) is not dict or set(body) != expected
                or body["schema_version"] not in (DECLARED_SLOT_COMMIT_SCHEMA, _DECLARED_SLOT_POST_COMPLETION_COMMIT_SCHEMA)
                or body["precommit_sha256"] != precommit.receipt_sha256
                or body["generation_context_sha256"] != context.context_sha256
                or body["cell_id"] != cell_id or body["slot_id"] != slot["slot_id"]
                or body["ordinal"] != slot["ordinal"] or body["previous_commit_sha256"] != previous):
            _fail("DECLARED_SLOT_PREFIX_BINDING_MISMATCH")
        _verify_declared_completion_transport(context, completion=body["completion"],
            expected_edition=historical.declared_slot_transport_edition(parent))
        verified = historical_adapter.verify_declared_slot_completion(context, precommit=precommit, process=process,
            completion=body["completion"], artifact_roster=body["artifact_roster"], frozen_pipeline_config=frozen_pipeline_config)
        if verified["slot_id"] != slot["slot_id"] or verified["cell_id"] != cell_id:
            _fail("DECLARED_SLOT_PREFIX_BINDING_MISMATCH")
        namespace = verified["request_body"]["artifact_namespace"] + "/"
        if any(namespace.startswith(prior) or prior.startswith(namespace) for prior in namespaces):
            _fail("DECLARED_SLOT_PREFIX_BINDING_MISMATCH")
        namespaces.add(namespace)
        for entry in body["artifact_roster"]:
            if not entry["artifact_id"].startswith(namespace):
                _fail("DECLARED_SLOT_PREFIX_BINDING_MISMATCH")
        completed.append(_declared_slot_summary(body, digest))
        commit_artifacts.append(_declared_commit_artifact(slot, descriptor))
        previous, terminal = digest, verified["status"] == "STOP"
        if body["schema_version"] == _DECLARED_SLOT_POST_COMPLETION_COMMIT_SCHEMA:
            terminal_failure = _declared_terminal_failure_from_commit(body, context=context,
                precommit=precommit, cell_id=cell_id, completed=completed, last_commit_sha256=previous)
            terminal = True
        del body, verified, item
    sibling_terminal = _read_declared_record(context, _declared_terminal_path(precommit, cell_id))
    if sibling_terminal is not None:
        if (terminal_failure is None and type(sibling_terminal[0]) is dict
                and sibling_terminal[0].get("schema_version") == _DECLARED_CELL_POST_COMPLETION_TERMINAL_SCHEMA):
            _fail("DECLARED_SLOT_TERMINAL_INVALID")
        sibling_terminal = _verify_declared_terminal_failure(sibling_terminal[0], context=context,
            precommit=precommit, cell_id=cell_id, completed=completed, last_commit_sha256=previous)
        if terminal_failure is not None and sibling_terminal != terminal_failure:
            _fail("DECLARED_SLOT_TERMINAL_BINDING_MISMATCH")
        terminal_failure = sibling_terminal
        terminal = True
    if terminal:
        first = None
    if manifest is not None:
        if first is not None:
            _fail("DECLARED_SLOT_FINALIZED_INCOMPLETE_PREFIX")
        if verified_manifest["artifacts"] != commit_artifacts:
            _fail("DECLARED_SLOT_FINAL_MANIFEST_BINDING_MISMATCH")
    run = _read_declared_record(context, _declared_run_path(precommit, cell_id))
    if run is not None:
        if symbol_receipt is None:
            _fail("DECLARED_SLOT_FINAL_MANIFEST_MISSING")
        expected = _declared_run_body(context, precommit=precommit, cell_id=cell_id, summaries=completed,
            declared_slots=len(parent["slot_roster"]["slots"]), terminal_stop=terminal, previous=previous,
            symbol_receipt=symbol_receipt, terminal_failure=terminal_failure)
        if run[0] != expected:
            _fail("DECLARED_SLOT_RUN_RECEIPT_BINDING_MISMATCH")
        run_receipt = run[0]
    return DeclaredCellOwnerProgress(tuple(completed), tuple(commit_artifacts), first, terminal, previous,
        terminal_failure, symbol_receipt, run_receipt)


def _commit_declared_slot_completion(context, *, precommit, process, cell_id, slot, completion,
                                      artifact_roster, previous_commit_sha256, frozen_pipeline_config,
                                      post_completion_failure=None):
    parent = historical.verify_declared_slot_owner_run_precommit(context, precommit, process=process)
    search_planner._require_current_process(process)
    # Independently reopen the COMPLETE result before publishing an immutable
    # completion marker. The marker, not process memory, commits the slot.
    try:
        if len(artifact_roster) > context.limits.max_artifacts_per_symbol:
            _fail("DECLARED_SLOT_COMMIT_ROSTER_BOUND_EXCEEDED")
        authority_spool._canonical_json_bytes(artifact_roster, min(context.limits.max_json_bytes, context.limits.max_manifest_bytes))
    except authority_spool.AuthoritySpoolError as exc:
        raise HistoricalSearchProducerError("DECLARED_SLOT_COMMIT_ROSTER_BOUND_EXCEEDED", (str(exc)[:128],)) from exc
    _verify_declared_completion_transport(context, completion=completion,
        expected_edition=historical.declared_slot_transport_edition(parent))
    verified = historical_adapter.verify_declared_slot_completion(context, precommit=precommit, process=process,
        completion=completion, artifact_roster=artifact_roster, frozen_pipeline_config=frozen_pipeline_config)
    if verified["slot_id"] != slot["slot_id"] or verified["cell_id"] != cell_id:
        _fail("DECLARED_SLOT_COMMIT_INVALID")
    namespace = verified["request_body"]["artifact_namespace"] + "/"
    if any(not entry["artifact_id"].startswith(namespace) for entry in artifact_roster):
        _fail("DECLARED_SLOT_COMMIT_INVALID")
    if authority_spool._local_path(context.root, _declared_terminal_path(precommit, cell_id)).exists():
        _fail("DECLARED_SLOT_AFTER_TERMINAL_STOP")
    path = _declared_commit_path(precommit, cell_id, slot["ordinal"])
    if slot["ordinal"] == 1:
        if previous_commit_sha256 is not None:
            _fail("DECLARED_SLOT_PREFIX_BINDING_MISMATCH")
    else:
        prior = _read_declared_commit(context, _declared_commit_path(precommit, cell_id, slot["ordinal"] - 1))
        if prior is None or prior[1] != previous_commit_sha256:
            _fail("DECLARED_SLOT_PREFIX_BINDING_MISMATCH")
        if (prior[0]["completion"]["status"] == "STOP"
                or prior[0]["schema_version"] == _DECLARED_SLOT_POST_COMPLETION_COMMIT_SCHEMA):
            _fail("DECLARED_SLOT_AFTER_TERMINAL_STOP")
    if authority_spool._local_path(context.root, path).exists():
        _fail("DECLARED_SLOT_COMMITTED_SLOT_REEXECUTION")
    body = {"schema_version": DECLARED_SLOT_COMMIT_SCHEMA, "precommit_sha256": precommit.receipt_sha256,
        "generation_context_sha256": context.context_sha256, "cell_id": cell_id,
        "slot_id": slot["slot_id"], "ordinal": slot["ordinal"], "previous_commit_sha256": previous_commit_sha256,
        "completion": verified, "artifact_roster": list(artifact_roster)}
    if post_completion_failure is not None:
        # Completion and the already-known stop share one immutable pointer.
        recorded_at_utc = _now_utc_seconds()
        anomalies = _declared_failure_time_order_anomalies(post_completion_failure,
            completed_at_utc=verified["completed_at_utc"], recorded_at_utc=recorded_at_utc)
        body["schema_version"] = _DECLARED_SLOT_POST_COMPLETION_COMMIT_SCHEMA
        body["post_completion_failure"] = {"failure": dict(post_completion_failure),
            "recorded_at_utc": recorded_at_utc, "time_order_anomalies": anomalies}
    try:
        authority_spool._canonical_json_bytes(body, context.limits.max_json_bytes)
    except authority_spool.AuthoritySpoolError as exc:
        raise HistoricalSearchProducerError("DECLARED_SLOT_COMMIT_BOUND_EXCEEDED", (str(exc)[:128],)) from exc
    descriptor = authority_spool.write_canonical_json_artifact(context, body)
    pointer = {"schema_version": _DECLARED_SLOT_POINTER_SCHEMA, "commit_descriptor": descriptor}
    authority_spool._publish_exact_json(context, path, pointer, context.limits.max_json_bytes)
    return _declared_slot_summary(body, descriptor["sha256"]), descriptor


def _execute_declared_cell_owner_slots_unlocked(*, context, precommit, process, frozen_pipeline_config, cell_id):
    parent = historical.verify_declared_slot_owner_run_precommit(context, precommit, process=process)
    search_planner._require_current_process(process)
    frozen_pipeline_config.verify_current(context)
    if frozen_pipeline_config.effective_config_sha256 != parent["effective_config_sha256"]:
        _fail("DECLARED_SLOT_CONFIG_BINDING_MISMATCH")
    progress = reopen_declared_cell_owner_progress(context, precommit=precommit, process=process, cell_id=cell_id, frozen_pipeline_config=frozen_pipeline_config)
    symbol = parent["cells"][0]["symbol"]
    if progress.run_receipt is not None:
        return DeclaredCellOwnerRunReceipt(_canonical_bytes(progress.run_receipt))
    previous = progress.last_commit_sha256
    completed = list(progress.slot_summaries)
    commit_artifacts = list(progress.slot_commit_artifacts)
    terminal_failure = progress.terminal_failure
    if not progress.terminal_stop and progress.first_uncommitted_slot is not None:
        start = progress.first_uncommitted_slot["ordinal"]
        for slot in parent["slot_roster"]["slots"][start - 1:]:
            import secrets
            import os
            sink = authority_spool.open_symbol_sink(context, symbol)
            attempt_id = "attempt-" + secrets.token_hex(16)
            request = historical.declared_slot_producer_request(context, precommit=precommit, process=process,
                cell_id=cell_id, slot_id=slot["slot_id"], attempt_id=attempt_id, sink=sink)
            try:
                attempt_body = {"schema_version": "fxpm.declared-slot-attempt-start.v1", "request_sha256": request.receipt_sha256,
                    "slot_id": slot["slot_id"], "started_at_utc": _now_utc_seconds(), "pid": os.getpid(),
                    "meaning": "START_ONLY_NOT_A_COMPLETION_OR_FUTURE_OUTCOME",
                    "prior_uncommitted_attempts": _declared_prior_attempt_starts(context, precommit, cell_id, slot)}
                authority_spool._publish_exact_json(context,
                    f"declared-slots/{precommit.receipt_sha256}/{cell_id}/attempts/{slot['slot_id']}/{attempt_id}.json",
                    attempt_body, context.limits.max_json_bytes)
                post_completion_failure = None
                try:
                    completion = execute_declared_slot_owner_search(context, request=request, process=process,
                        frozen_pipeline_config=frozen_pipeline_config)
                except _DeclaredSlotPostCompletionFailure as exc:
                    completion, post_completion_failure = exc.completion, exc.failure
                namespace = request.to_dict()["artifact_namespace"] + "/"
                roster = [dict(sink._artifacts[k]) for k in sorted(sink._artifacts) if k.startswith(namespace)]
                summary, descriptor = _commit_declared_slot_completion(context, precommit=precommit, process=process,
                    cell_id=cell_id, slot=slot, completion=completion, artifact_roster=roster,
                    previous_commit_sha256=previous, frozen_pipeline_config=frozen_pipeline_config,
                    post_completion_failure=post_completion_failure)
                previous = descriptor["sha256"]
                completed.append(summary)
                commit_artifacts.append(_declared_commit_artifact(slot, descriptor))
            finally:
                historical._release_declared_slot_producer_request(request, context)
            del sink, roster, request
            if post_completion_failure is not None:
                committed = _read_declared_commit(context, _declared_commit_path(precommit, cell_id, slot["ordinal"]))
                if committed is None or committed[1] != previous:
                    _fail("DECLARED_SLOT_COMMIT_INTEGRITY_MISMATCH")
                terminal_failure = _declared_terminal_failure_from_commit(committed[0], context=context,
                    precommit=precommit, cell_id=cell_id, completed=completed, last_commit_sha256=previous)
                break
            if completion["status"] == "STOP":
                break
    # This first surface precommits exactly one cell, so outer finalization is
    # reached only after all13 slots or an authenticated terminal technical stop.
    terminal_stop = bool(terminal_failure is not None or (completed and completed[-1]["status"] == "STOP"))
    if not terminal_stop and len(completed) != len(parent["slot_roster"]["slots"]):
        _fail("DECLARED_SLOT_FINALIZED_INCOMPLETE_PREFIX")
    sink = authority_spool.open_symbol_sink(context, symbol)
    for entry in commit_artifacts:
        sink.register_artifact(entry["artifact_id"], entry["artifact_type"], entry["descriptor"])
    symbol_receipt = sink.seal()
    body = _declared_run_body(context, precommit=precommit, cell_id=cell_id, summaries=completed,
        declared_slots=len(parent["slot_roster"]["slots"]), terminal_stop=terminal_stop, previous=previous,
        symbol_receipt=symbol_receipt, terminal_failure=terminal_failure)
    authority_spool._publish_exact_json(context, _declared_run_path(precommit, cell_id), body, context.limits.max_json_bytes)
    return DeclaredCellOwnerRunReceipt(_canonical_bytes(body))


__all__ += ["DECLARED_CELL_RUN_SCHEMA", "DECLARED_SLOT_COMMIT_SCHEMA", "DeclaredCellOwnerRunReceipt",
    "DeclaredCellOwnerProgress", "execute_declared_slot_owner_search", "execute_declared_cell_owner_slots",
    "reopen_declared_cell_owner_progress"]


def execute_declared_cell_owner_slots(*, context, precommit, process, frozen_pipeline_config, cell_id):
    """Hold the existing nonblocking OS primitive through restore and commit."""
    import os
    import pm_native_storage as storage
    if (type(context) is not authority_spool.GenerationSpoolContext
            or type(precommit) is not historical.DeclaredSlotRunPrecommit):
        _fail("DECLARED_SLOT_PRECOMMIT_INVALID")
    _hex64(cell_id, "DECLARED_SLOT_CELL_INVALID")
    historical.verify_declared_slot_owner_run_precommit(context, precommit, process=process)
    search_planner._require_current_process(process)
    relative = f"declared-slots/{precommit.receipt_sha256}/{cell_id}"
    authority_spool._ensure_directory(context.root, relative)
    path = authority_spool._local_path(context.root, relative + "/run.lock")
    authority_spool._reject_reparse_chain(path)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "r+b", buffering=0) as handle:
        authority_spool._reject_reparse_chain(path)
        current, opened = os.stat(path, follow_symlinks=False), os.fstat(handle.fileno())
        if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
            _fail("DECLARED_SLOT_RUN_LOCK_INVALID")
        if opened.st_size == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        if not storage._try_kernel_lock(handle):
            _fail("DECLARED_SLOT_RUN_ACTIVE")
        try:
            current, pinned = os.stat(path, follow_symlinks=False), os.fstat(handle.fileno())
            if ((current.st_dev, current.st_ino) != (pinned.st_dev, pinned.st_ino)
                    or (opened.st_dev, opened.st_ino) != (pinned.st_dev, pinned.st_ino)
                    or pinned.st_size != 1):
                _fail("DECLARED_SLOT_RUN_LOCK_INVALID")
            return _execute_declared_cell_owner_slots_unlocked(context=context, precommit=precommit,
                process=process, frozen_pipeline_config=frozen_pipeline_config, cell_id=cell_id)
        finally:
            storage._release_kernel_lock(handle)


def _declared_prior_attempt_starts(context, precommit, cell_id, slot):
    """Bounded diagnostics at the first uncommitted slot, never an outcome."""
    relative = f"declared-slots/{precommit.receipt_sha256}/{cell_id}/attempts/{slot['slot_id']}"
    directory = authority_spool._local_path(context.root, relative)
    if not directory.exists():
        return []
    authority_spool._reject_reparse_chain(directory)
    paths = list(directory.iterdir())
    if len(paths) > context.limits.max_artifacts_per_symbol:
        _fail("DECLARED_SLOT_ATTEMPT_ROSTER_BOUND_EXCEEDED")
    result = []
    for path in sorted(paths):
        if path.suffix != ".json":
            _fail("DECLARED_SLOT_ATTEMPT_RECORD_INVALID")
        name = relative + "/" + path.name
        digest, size, encoded = authority_spool._stream_file(context.root, name,
            maximum_bytes=context.limits.max_json_bytes, collect=True)
        body = authority_spool._strict_json_bytes(encoded, context.limits.max_json_bytes)
        if (body.get("schema_version") != "fxpm.declared-slot-attempt-start.v1"
                or body.get("slot_id") != slot["slot_id"]):
            _fail("DECLARED_SLOT_ATTEMPT_RECORD_INVALID")
        result.append({"relative_path": name, "sha256": digest, "size_bytes": size,
            "state": "NO_COMMIT_OBSERVED_AT_RESUME", "cause": "UNKNOWN"})
    return result
