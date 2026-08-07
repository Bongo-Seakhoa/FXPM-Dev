"""Frozen production search planning for historical anchors and current refit.

This module closes the mechanical seam between the generation-wide process
precommitment and :mod:`pm_training_horizon`.  It deliberately does not run a
backtest or choose a winner.  For every empirically observed cell it freezes:

* the exact 100-strategy registry, defaults, parameter grids, code and feature
  contracts;
* one timeframe-specific rolling calendar depth (the current Horizon v1 API
  forms a full depth x trial product, so more than one depth would make it
  impossible to cover all 100 strategies inside Winner V11's 128-attempt
  ceiling);
* all 100 full default parameter maps plus 28 deterministic, outcome-blind
  non-default grid points; and
* four genuine, non-overlapping, whole-UTC-day causal validation folds.

The same cell design is reproduced at every historical anchor and at the
latest-cutoff current rerun.  The emitted plan still confers no winner,
causal, capital, publication, demo, or live authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from enum import Enum
import csv
import hashlib
import io
import inspect
import json
import math
from pathlib import Path
from typing import Any, Final, Mapping, Sequence

import pandas as pd

import pm_authoritative_search_execution as search_execution
import pm_authority_spool as authority_spool
import pm_book_multiplicity as book
import pm_causal_process as causal
import pm_current_refit_artifacts as current_refit
import pm_current_refit_execution as current_refit_execution
import pm_historical_anchor_orchestration as historical
import pm_historical_production_adapter as historical_adapter
import pm_native_prefix_production as native_prefix
import pm_state_contract as state_contract
import pm_training_horizon as training_horizon
from pm_current_refit_parameters import (
    canonicalize_parameter_mapping,
    canonicalize_parameter_value,
    decode_parameter_mapping,
    parameter_values_equal,
)
from pm_strategies import StrategyRegistry


UTC: Final = timezone.utc
PROCESS_SCHEMA: Final = "fxpm.production-search-process-precommit.v1"
PROCESS_SEMANTICS_SCHEMA: Final = "fxpm.production-search-process-semantics.v1"
DOMAIN_SCHEMA: Final = "fxpm.production-strategy-domain.v1"
ATTEMPT_POLICY_SCHEMA: Final = "fxpm.production-attempt-design-policy.v1"
PROCESS_SPOOL_SYMBOL: Final = "__PRODUCTION_SEARCH_PROCESS__"
CURRENT_AUTH_SPOOL_SYMBOL: Final = "__CURRENT_SEARCH_AUTHORIZATION__"
PROCESS_ARTIFACT_ID: Final = "production-search-process-precommit"
PROCESS_ARTIFACT_TYPE: Final = "production_search_process_precommit"
CELL_PLAN_SCHEMA: Final = "fxpm.production-search-cell-plan-receipt.v1"
CELL_PLAN_ARTIFACT_TYPE: Final = "production_search_cell_plan_receipt"
STATE_CONTEXT_DISPOSITION_SCHEMA: Final = (
    "fxpm.production-trial-state-context-disposition.v1"
)
STATE_CONTEXT_SELECTION_SCHEMA: Final = (
    "fxpm.production-cell-state-context-selection.v1"
)
STATE_CONTEXT_BOUNDARY_REASON: Final = "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
CURRENT_SOURCE_CAPTURE_SCHEMA: Final = "fxpm.production-current-source-capture.v1"
CURRENT_SOURCE_SNAPSHOT_SCHEMA: Final = "fxpm.current-refit-source-snapshot.v1"
CURRENT_BAR_FILE_SCHEMA: Final = "fxpm.current-refit-bars.csv.v1"
CURRENT_SOURCE_ARTIFACT_TYPE: Final = "current_refit_source_snapshot"
CURRENT_BAR_ARTIFACT_TYPE: Final = "current_refit_native_bars_csv"
CURRENT_BAR_COLUMNS: Final = (
    "timestamp_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

TIMEFRAMES: Final = tuple(book.TIMEFRAME_ORDER)
REGIMES: Final = tuple(book.REGIME_ORDER)
TIMEFRAME_SECONDS: Final = {
    "M5": 300,
    "M15": 900,
    "M30": 1_800,
    "H1": 3_600,
    "H4": 14_400,
    "D1": 86_400,
}

# Faster surfaces receive materially shorter histories so obsolete edge is not
# allowed to dominate.  The four unseen 63-day folds sit outside each rolling
# fit window, so the total prefix required is larger than this fit depth.
FIT_CALENDAR_DAYS: Final = {
    "M5": 365,
    "M15": 548,
    "M30": 730,
    "H1": 1_095,
    "H4": 1_460,
    "D1": 1_825,
}
CAUSAL_FOLD_COUNT: Final = 4
CAUSAL_FOLD_MIN_DAYS: Final = 63
BASELINE_ATTEMPTS: Final = 100
VARIANT_ATTEMPTS: Final = 28
ATTEMPT_COUNT: Final = BASELINE_ATTEMPTS + VARIANT_ATTEMPTS
PARAMETER_DESIGN_SEED: Final = "fxpm-production-space-fill-v1"
MAX_PROCESS_BYTES: Final = 16 * 1024 * 1024
STATE_INITIAL_ERROR_FLOOR: Final = 100.0

# The largest feasible entry is selected from this frozen ladder using source
# geometry only.  Exactly one depth reaches Horizon, so the 100 registry
# defaults still fit beside 28 non-default designs under Winner V11's 128
# attempt ceiling.  Shorter entries prevent ordinary broker-history boundaries
# from turning an otherwise genuine sparse timeframe into a technical veto.
FIT_CALENDAR_DAY_LADDERS: Final = {
    "M5": (90, 180, 270, 365),
    "M15": (120, 270, 365, 548),
    "M30": (180, 365, 548, 730),
    "H1": (270, 548, 730, 1_095),
    "H4": (365, 730, 1_095, 1_460),
    "D1": (365, 730, 1_095, 1_460, 1_825),
}

_PROCESS_SEAL = object()
_CURRENT_AUTH_SEAL = object()
_CELL_PLAN_SEAL = object()
_SHA_CHARS: Final = frozenset("0123456789abcdef")
_REGIME_CATEGORY_AFFINITY: Final = {
    "TREND": "trend",
    "RANGE": "mean_reversion",
    "BREAKOUT": "breakout",
    "CHOP": "mean_reversion",
}


class ProductionSearchPlannerError(ValueError):
    """Typed fail-closed planner error; never an economic market verdict."""

    def __init__(self, code: str, details: Sequence[str] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value) for value in details)
        suffix = ":" + ",".join(self.details[:8]) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise ProductionSearchPlannerError(code, tuple(str(value) for value in details))


def _canonical_bytes(value: Any, *, maximum: int = MAX_PROCESS_BYTES) -> bytes:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError, OverflowError) as exc:
        raise ProductionSearchPlannerError("CANONICAL_JSON_INVALID") from exc
    if not 0 < len(encoded) <= maximum:
        _fail("CANONICAL_JSON_SIZE_INVALID")
    return encoded


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _hex64(value: Any, code: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in _SHA_CHARS for character in value)
    ):
        _fail(code)
    return value


def _identity(value: Any, code: str, *, maximum: int = 128) -> str:
    if type(value) is not str or not value or value != value.strip():
        _fail(code)
    try:
        encoded = value.encode("ascii")
    except UnicodeError as exc:
        raise ProductionSearchPlannerError(code) from exc
    if len(encoded) > maximum or any(byte < 32 or byte == 127 for byte in encoded):
        _fail(code)
    return value


def _utc(value: Any, code: str) -> datetime:
    if type(value) is not str or not 1 <= len(value) <= 64:
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProductionSearchPlannerError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        _fail(code)
    parsed = parsed.astimezone(UTC)
    if parsed.microsecond:
        _fail(code)
    return parsed


def _utc_text(value: datetime | pd.Timestamp) -> str:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        _fail("TIMESTAMP_NAIVE")
    parsed = parsed.tz_convert("UTC")
    if parsed.microsecond or parsed.nanosecond:
        _fail("TIMESTAMP_SUBSECOND")
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.resolve(strict=True).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise ProductionSearchPlannerError("SOURCE_FILE_UNAVAILABLE") from exc
    return digest.hexdigest()


def _source_sha256(value: Any) -> str:
    try:
        source = Path(inspect.getsourcefile(value) or "")
    except (OSError, TypeError) as exc:
        raise ProductionSearchPlannerError("SOURCE_FILE_UNRESOLVED") from exc
    return _file_sha256(source)


def _callable_sha256(value: Any) -> str:
    try:
        source = inspect.getsource(value).replace("\r\n", "\n").encode("utf-8")
    except (OSError, TypeError) as exc:
        raise ProductionSearchPlannerError("CALLABLE_SOURCE_UNRESOLVED") from exc
    return hashlib.sha256(source).hexdigest()


def _frame_sha256(frame: pd.DataFrame) -> str:
    try:
        hashed = pd.util.hash_pandas_object(frame, index=True).to_numpy(dtype="<u8")
    except Exception as exc:
        raise ProductionSearchPlannerError("FRAME_HASH_FAILED") from exc
    digest = hashlib.sha256()
    digest.update(b"fxpm-authoritative-search-frame-v1\x00")
    digest.update(_canonical_bytes([str(value) for value in frame.columns]))
    digest.update(hashed.tobytes(order="C"))
    return digest.hexdigest()


def _parameter_value_key(value: Any) -> bytes:
    return _canonical_bytes(canonicalize_parameter_value(value), maximum=256 * 1024)


def _strategy_domain(name: str, ordinal: int) -> dict[str, Any]:
    strategy = StrategyRegistry.get(name)
    defaults = canonicalize_parameter_mapping(strategy.get_default_params())
    normalized_defaults = canonicalize_parameter_mapping(strategy.get_params())
    if defaults != normalized_defaults:
        _fail("STRATEGY_DEFAULT_NORMALIZATION_DRIFT", name)
    raw_grid = strategy.get_param_grid()
    if type(raw_grid) is not dict or not raw_grid:
        _fail("STRATEGY_GRID_INVALID", name)
    axes: list[dict[str, Any]] = []
    grid_size = 1
    for axis_name, raw_choices in raw_grid.items():
        if (
            type(axis_name) is not str
            or not axis_name
            or type(raw_choices) not in {list, tuple}
            or not raw_choices
        ):
            _fail("STRATEGY_GRID_AXIS_INVALID", name, axis_name)
        choices = [canonicalize_parameter_value(value) for value in raw_choices]
        encoded = [_parameter_value_key(value) for value in choices]
        if len(encoded) != len(set(encoded)):
            _fail("STRATEGY_GRID_CHOICE_DUPLICATED", name, axis_name)
        if axis_name not in defaults:
            _fail("STRATEGY_GRID_AXIS_OUTSIDE_DEFAULTS", name, axis_name)
        if not any(
            parameter_values_equal(defaults[axis_name], choice) for choice in choices
        ):
            _fail("STRATEGY_DEFAULT_OUTSIDE_GRID", name, axis_name)
        grid_size *= len(choices)
        axes.append({"name": axis_name, "choices": choices})
    body = {
        "schema_version": DOMAIN_SCHEMA,
        "ordinal": ordinal,
        "strategy_id": name,
        "strategy_family": strategy.category.value,
        "strategy_module": type(strategy).__module__,
        "strategy_class": type(strategy).__name__,
        "strategy_source_sha256": _source_sha256(type(strategy)),
        "entry_rule_identity_sha256": _callable_sha256(type(strategy).generate_signals),
        "exit_rule_identity_sha256": _callable_sha256(type(strategy).calculate_stops),
        "minimum_bars": int(strategy.MIN_BARS),
        "required_features": sorted(strategy.get_feature_request()),
        "defaults": defaults,
        "grid_axes": axes,
        "grid_size": grid_size,
    }
    return {**body, "domain_sha256": _sha(body)}


def _normalize_candidate_domain_value(value: Any) -> Any:
    """Mirror the accepted state-contract candidate-domain normalization."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {
            str(key): _normalize_candidate_domain_value(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple)):
        return [_normalize_candidate_domain_value(item) for item in value]
    if isinstance(value, set):
        return sorted(_normalize_candidate_domain_value(item) for item in value)
    return value


def _live_candidate_domain_sha256() -> str:
    documents: list[dict[str, Any]] = []
    for name in sorted(StrategyRegistry.list_all()):
        strategy = StrategyRegistry.get(name)
        documents.append(
            {
                "name": name,
                "defaults": _normalize_candidate_domain_value(
                    strategy.get_default_params()
                ),
                "grid": _normalize_candidate_domain_value(strategy.get_param_grid()),
            }
        )
    try:
        return state_contract.strategy_candidate_domain_sha256(documents)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "STRATEGY_CANDIDATE_DOMAIN_INVALID", (type(exc).__name__,)
        ) from exc


def _strategy_domains() -> list[dict[str, Any]]:
    names = list(StrategyRegistry.list_all())
    if (
        len(names) != BASELINE_ATTEMPTS
        or len(names) != len(set(names))
        or tuple(names) != state_contract.ACTUAL_STRATEGY_NAMES
    ):
        _fail("STRATEGY_REGISTRY_MUST_CONTAIN_EXACTLY_100")
    live_domain_sha256 = _live_candidate_domain_sha256()
    if live_domain_sha256 != state_contract.STRATEGY_CANDIDATE_DOMAIN_SHA256:
        _fail("STRATEGY_CANDIDATE_DOMAIN_AUTHORITY_MISMATCH")
    domains = [_strategy_domain(name, ordinal) for ordinal, name in enumerate(names)]
    if [item["strategy_id"] for item in domains] != names:
        _fail("STRATEGY_REGISTRY_ORDER_CHANGED")
    return domains


def _dependency_payload(
    timeframe: str,
    *,
    feature_lookback_bars: int,
    regime_warmup_bars: int,
) -> dict[str, Any]:
    seconds = TIMEFRAME_SECONDS[timeframe]
    process_policy = causal.pinned_process_policy()
    holding_days = process_policy.get(
        "maximum_holding_calendar_days_by_timeframe", {}
    ).get(timeframe)
    if type(holding_days) is not int or not 1 <= holding_days <= 365:
        _fail("CAUSAL_HOLDING_POLICY_INVALID", timeframe)
    holding_seconds = holding_days * 86_400
    holding_bars = int(math.ceil(holding_seconds / seconds))
    if (
        type(feature_lookback_bars) is not int
        or type(regime_warmup_bars) is not int
        or feature_lookback_bars < 1
        or regime_warmup_bars < 1
    ):
        _fail("DEPENDENCY_STATE_GEOMETRY_INVALID")
    return {
        "schema_version": "fxpm.historical-dependency-policy-payload.v1",
        "feature_lookback_bars": feature_lookback_bars,
        "label_lookahead_bars": 1,
        "max_holding_bars": holding_bars,
        "execution_delay_bars": 1,
        "regime_warmup_bars": regime_warmup_bars,
        "cost_lookback_bars": 1,
        "max_holding_seconds": holding_seconds,
    }


def _derived_dependency_geometry(
    timeframe: str, payload: Mapping[str, Any]
) -> dict[str, int]:
    seconds = TIMEFRAME_SECONDS[timeframe]
    warmup_bars = max(
        int(payload["feature_lookback_bars"]),
        int(payload["regime_warmup_bars"]),
        int(payload["cost_lookback_bars"]),
    )
    left_purge_bars = max(
        int(payload["label_lookahead_bars"]),
        int(payload["execution_delay_bars"]) + int(payload["max_holding_bars"]),
    )
    delayed_holding_seconds = int(payload["execution_delay_bars"]) * seconds + max(
        int(payload["max_holding_bars"]) * seconds,
        int(payload["max_holding_seconds"]),
    )
    left_purge_seconds = max(
        int(payload["label_lookahead_bars"]) * seconds,
        delayed_holding_seconds,
    )
    right_purge_bars = max(
        int(payload["feature_lookback_bars"]),
        int(payload["label_lookahead_bars"]),
        int(payload["execution_delay_bars"]) + int(payload["max_holding_bars"]),
        int(payload["regime_warmup_bars"]),
        int(payload["cost_lookback_bars"]),
    )
    return {
        "timeframe_seconds": seconds,
        "warmup_bars": warmup_bars,
        "warmup_seconds": warmup_bars * seconds,
        "left_purge_bars": left_purge_bars,
        "left_purge_seconds": left_purge_seconds,
        "right_purge_bars": right_purge_bars,
        "right_purge_seconds": max(right_purge_bars * seconds, delayed_holding_seconds),
    }


def _process_semantics(domains: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    domain_roster = [dict(value) for value in domains]
    horizon_policy = {
        timeframe: {
            "lookback_kind": "CALENDAR_INTERVAL",
            "precommitted_depth_ladder_days": list(FIT_CALENDAR_DAY_LADDERS[timeframe]),
            "selection_rule": (
                "LARGEST_FEASIBLE_BY_AUTHENTICATED_SOURCE_GEOMETRY_ONLY"
            ),
            "dependency_payload_rule": (
                "PM_STATE_CONTRACT_MAX_ATTEMPT_BOUND_PLUS_EXACT_REGIME_WARMUP_"
                "AND_CAUSAL_HOLDING_POLICY"
            ),
        }
        for timeframe in TIMEFRAMES
    }
    return {
        "schema_version": PROCESS_SEMANTICS_SCHEMA,
        "registry_order": [item["strategy_id"] for item in domain_roster],
        "registry_order_sha256": _sha([item["strategy_id"] for item in domain_roster]),
        "strategy_domain_count": len(domain_roster),
        "strategy_domains": domain_roster,
        "strategy_domain_roster_sha256": _sha(domain_roster),
        "accepted_candidate_domain_sha256": (
            state_contract.STRATEGY_CANDIDATE_DOMAIN_SHA256
        ),
        "state_contract_registry_sha256": state_contract.DEFAULT_AUTHORITY.registry_hash,
        "attempt_policy": {
            "schema_version": ATTEMPT_POLICY_SCHEMA,
            "winner_contract_version": book.PINNED_WINNER_SELECTION_CONTRACT_VERSION,
            "winner_max_attempts": search_execution.MAX_ATTEMPTS,
            "attempt_count_per_observed_cell": ATTEMPT_COUNT,
            "one_full_default_per_strategy": True,
            "default_attempt_count": BASELINE_ATTEMPTS,
            "additional_nondefault_attempt_count": VARIANT_ATTEMPTS,
            "variant_strategy_allocation": (
                "REGIME_CATEGORY_AFFINITY_THEN_FROZEN_CELL_HASH_PERMUTATION"
            ),
            "variant_parameter_sampler": (
                "SHA256_AXIS_MODULO_WITH_FORCED_NONDEFAULT_V1"
            ),
            "parameter_design_seed": PARAMETER_DESIGN_SEED,
            "outcome_or_evidence_dependent_sampling": False,
            "adaptive_or_posthoc_attempts": False,
        },
        "horizon_policy_by_timeframe": horizon_policy,
        "fold_policy": {
            "role": "CAUSAL_ROLLING_ORIGIN_INNER",
            "fold_count": CAUSAL_FOLD_COUNT,
            "minimum_calendar_days_per_fold": CAUSAL_FOLD_MIN_DAYS,
            "whole_utc_days": True,
            "validation_intervals_nonoverlapping": True,
            "rolling_calendar_fit": True,
            "final_oos_authority": False,
        },
        "one_horizon_candidate_reason": (
            "HORIZON_V1_CARTESIAN_PRODUCT_AND_WINNER_V11_128_ATTEMPT_BOUND"
        ),
        "source_boundary_policy": (
            "ONE_LARGEST_FEASIBLE_PRECOMMITTED_DEPTH_OR_TYPED_INSUFFICIENT_EVIDENCE"
        ),
        "full_prefix_state_policy": (
            "AUTHENTICATED_FULL_NATIVE_PREFIX_REPLAY_THEN_SCORE_ONLY_FROZEN_MASKS"
        ),
        "all_100_strategies_represented": True,
        "parameter_grid_exhaustive_search_claimed": False,
    }


def _expected_process_body(
    context: authority_spool.GenerationSpoolContext,
    *,
    run_id: str,
) -> dict[str, Any]:
    observed = authority_spool.reopen_generation_context(
        context.root,
        expected_generation_id=context.generation_id,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    if observed.context_sha256 != context.context_sha256:
        _fail("GENERATION_CONTEXT_CHANGED")
    domains = _strategy_domains()
    semantics = _process_semantics(domains)
    return {
        "schema_version": PROCESS_SCHEMA,
        "authority_scope": "FROZEN_SEARCH_PROCESS_ONLY",
        "run_id": _identity(run_id, "RUN_ID_INVALID"),
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "native_universe_sha256": context.native_universe_sha256,
        "config_sha256": context.config_sha256,
        "planner_release_sha256": PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256,
        "authoritative_executor_release_sha256": (
            search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
        ),
        "historical_adapter_release_sha256": (
            historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
        ),
        "candidate_domain_sha256": state_contract.STRATEGY_CANDIDATE_DOMAIN_SHA256,
        "state_contract_registry_sha256": state_contract.DEFAULT_AUTHORITY.registry_hash,
        "causal_process_policy_sha256": _sha(causal.pinned_process_policy()),
        "semantics": semantics,
        "process_semantics_sha256": _sha(semantics),
        "frozen_before_any_search_evaluation": True,
        "technical_failure_is_market_no_trade": False,
        "confers_winner_causal_current_capital_publication_or_live_authority": False,
    }


@dataclass(frozen=True, slots=True)
class FrozenProductionSearchProcess:
    _body_json: bytes
    _spool_receipt_json: bytes
    _seal: object

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()

    @property
    def spool_receipt(self) -> dict[str, Any]:
        return json.loads(self._spool_receipt_json.decode("ascii"))


@dataclass(frozen=True, slots=True)
class CurrentSearchAuthorization:
    _body_json: bytes
    _spool_receipt_json: bytes
    _seal: object

    def to_dict(self) -> dict[str, Any]:
        return json.loads(self._body_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(self._body_json).hexdigest()

    @property
    def spool_receipt(self) -> dict[str, Any]:
        return json.loads(self._spool_receipt_json.decode("ascii"))


@dataclass(frozen=True, slots=True)
class ProductionCellSearchPlan:
    mode: str
    symbol: str
    timeframe: str
    regime: str
    causal_precommit_receipt_sha256: str
    search_process_receipt_sha256: str
    horizon_plan: Any
    _attempt_artifacts_json: tuple[bytes, ...]
    _state_requirements_json: tuple[bytes, ...]
    _cell_receipt_json: bytes
    _spool_entry_json: bytes
    boundary_limited: bool
    _seal: object
    native_prefix_product: native_prefix.NativePrefixProduct | None = None
    fitted_timeframe: search_execution.FittedNativeSearchTimeframe | None = None
    _current_refit_source_capture_json: bytes | None = None
    _bounded_canary_request: Any = None
    _bounded_canary_source_session: Any = None
    _bounded_canary_plan_permit: Any = None

    def attempt_artifacts(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            json.loads(value.decode("ascii")) for value in self._attempt_artifacts_json
        )

    @property
    def attempt_count(self) -> int:
        return len(self._attempt_artifacts_json)

    def state_requirements(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            json.loads(value.decode("ascii")) for value in self._state_requirements_json
        )

    def current_refit_source_capture(self) -> dict[str, Any] | None:
        """Return the immutable pre-selection raw source capture, if current."""

        if self._current_refit_source_capture_json is None:
            return None
        return json.loads(self._current_refit_source_capture_json.decode("ascii"))

    @property
    def cell_receipt(self) -> dict[str, Any]:
        return json.loads(self._cell_receipt_json.decode("ascii"))

    @property
    def spool_entry(self) -> dict[str, Any]:
        return json.loads(self._spool_entry_json.decode("ascii"))

    @property
    def receipt_sha256(self) -> str:
        return str(self.cell_receipt["receipt_sha256"])


def freeze_production_search_process(
    context: authority_spool.GenerationSpoolContext,
    *,
    run_id: str,
) -> FrozenProductionSearchProcess:
    """Freeze the complete search domain before any anchor callback runs."""

    if type(context) is not authority_spool.GenerationSpoolContext:
        _fail("GENERATION_CONTEXT_INVALID")
    body = _expected_process_body(context, run_id=run_id)
    encoded = _canonical_bytes(body)
    sink = authority_spool.open_symbol_sink(context, PROCESS_SPOOL_SYMBOL)
    entry = sink.put_json(PROCESS_ARTIFACT_ID, PROCESS_ARTIFACT_TYPE, body)
    receipt = sink.seal()
    descriptor = entry.get("descriptor")
    if (
        type(descriptor) is not dict
        or descriptor.get("sha256") != hashlib.sha256(encoded).hexdigest()
        or descriptor.get("size_bytes") != len(encoded)
    ):
        _fail("PROCESS_SPOOL_DESCRIPTOR_MISMATCH")
    result = FrozenProductionSearchProcess(
        encoded,
        _canonical_bytes(receipt, maximum=256 * 1024),
        _PROCESS_SEAL,
    )
    verify_frozen_production_search_process(context, result)
    return result


def verify_frozen_production_search_process(
    context: authority_spool.GenerationSpoolContext,
    process: FrozenProductionSearchProcess,
) -> bool:
    """Recompute registry/domain truth and reopen the immutable spool receipt."""

    if (
        type(context) is not authority_spool.GenerationSpoolContext
        or type(process) is not FrozenProductionSearchProcess
        or process._seal is not _PROCESS_SEAL
    ):
        _fail("PROCESS_OBJECT_INVALID")
    body = process.to_dict()
    if _canonical_bytes(body) != process._body_json:
        _fail("PROCESS_BYTES_NONCANONICAL")
    expected = _expected_process_body(context, run_id=body.get("run_id"))
    if body != expected:
        _fail("PROCESS_SEMANTICS_CHANGED")
    manifest = authority_spool.verify_symbol_receipt(
        context.root,
        process.spool_receipt,
        expected_generation_id=context.generation_id,
        expected_symbol=PROCESS_SPOOL_SYMBOL,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    artifacts = manifest.get("artifacts")
    if type(artifacts) is not list or len(artifacts) != 1:
        _fail("PROCESS_SPOOL_ROSTER_INVALID")
    entry = artifacts[0]
    if (
        entry.get("artifact_id") != PROCESS_ARTIFACT_ID
        or entry.get("artifact_type") != PROCESS_ARTIFACT_TYPE
        or entry.get("descriptor", {}).get("sha256") != process.receipt_sha256
        or entry.get("descriptor", {}).get("size_bytes") != len(process._body_json)
    ):
        _fail("PROCESS_SPOOL_ROSTER_MISMATCH")
    return True


def freeze_current_search_authorization(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    causal_precommit_payload: Mapping[str, Any],
    causal_evidence_payload: Mapping[str, Any],
    causal_reconciliation_payload: Mapping[str, Any],
    current_cutoff_utc: str,
) -> CurrentSearchAuthorization:
    """Freeze one current-refit authorization only after raw causal PASS."""

    verify_frozen_production_search_process(context, process)
    values: list[dict[str, Any]] = []
    for value, code in (
        (causal_precommit_payload, "CAUSAL_PRECOMMIT_NOT_OBJECT"),
        (causal_evidence_payload, "CAUSAL_EVIDENCE_NOT_OBJECT"),
        (causal_reconciliation_payload, "CAUSAL_RECONCILIATION_NOT_OBJECT"),
    ):
        if not isinstance(value, Mapping):
            _fail(code)
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
        decision_body = decision.body()
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "CAUSAL_PROCESS_RAW_REVERIFY_FAILED", (type(exc).__name__, str(exc))
        ) from exc
    if decision_body.get("decision") != "OFFLINE_PROCESS_VALIDATION_PASS":
        _fail("CAUSAL_PROCESS_NOT_PASS")
    definition = precommit.get("process_definition")
    policies = definition.get("policy_identities") if type(definition) is dict else None
    if (
        type(policies) is not dict
        or policies.get("trial_registry_policy_sha256") != process.receipt_sha256
    ):
        _fail("CAUSAL_SEARCH_PROCESS_BINDING_INVALID")
    cutoff = _utc(current_cutoff_utc, "CURRENT_CUTOFF_INVALID")
    anchors = definition.get("anchor_plan") if type(definition) is dict else None
    if type(anchors) is not list or not anchors:
        _fail("CAUSAL_ANCHOR_PLAN_INVALID")
    last_forward_end = date.fromisoformat(anchors[-1]["forward_end_date"])
    last_end_exclusive = datetime.combine(
        last_forward_end + timedelta(days=1), datetime_time.min, UTC
    )
    if cutoff <= last_end_exclusive:
        _fail("CURRENT_CUTOFF_NOT_AFTER_CAUSAL_EVIDENCE")
    body = {
        "schema_version": "fxpm.production-current-search-authorization.v1",
        "authority_scope": "CURRENT_REFIT_SEARCH_ONLY_AFTER_RAW_CAUSAL_PASS",
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "run_id": process.to_dict()["run_id"],
        "search_process_receipt_sha256": process.receipt_sha256,
        "causal_precommit_receipt_sha256": causal.canonical_sha256(precommit),
        "causal_evidence_sha256": causal.canonical_sha256(evidence),
        "causal_reconciliation_sha256": causal.canonical_sha256(reconciliation),
        "causal_decision_sha256": causal.canonical_sha256(decision_body),
        "current_cutoff_utc": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "requires_identical_frozen_process": True,
        "confers_capital_publication_demo_or_live_authority": False,
    }
    encoded = _canonical_bytes(body)
    sink = authority_spool.open_symbol_sink(context, CURRENT_AUTH_SPOOL_SYMBOL)
    entry = sink.put_json(
        "current-search-authorization",
        "production_current_search_authorization",
        body,
    )
    receipt = sink.seal()
    if entry.get("descriptor", {}).get("sha256") != hashlib.sha256(encoded).hexdigest():
        _fail("CURRENT_AUTHORIZATION_SPOOL_MISMATCH")
    result = CurrentSearchAuthorization(
        encoded,
        _canonical_bytes(receipt, maximum=256 * 1024),
        _CURRENT_AUTH_SEAL,
    )
    verify_current_search_authorization(context, process=process, authorization=result)
    return result


def verify_current_search_authorization(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    authorization: CurrentSearchAuthorization,
) -> bool:
    verify_frozen_production_search_process(context, process)
    if (
        type(authorization) is not CurrentSearchAuthorization
        or authorization._seal is not _CURRENT_AUTH_SEAL
    ):
        _fail("CURRENT_AUTHORIZATION_OBJECT_INVALID")
    body = authorization.to_dict()
    if (
        _canonical_bytes(body) != authorization._body_json
        or body.get("schema_version")
        != "fxpm.production-current-search-authorization.v1"
        or body.get("generation_id") != context.generation_id
        or body.get("generation_context_sha256") != context.context_sha256
        or body.get("run_id") != process.to_dict()["run_id"]
        or body.get("search_process_receipt_sha256") != process.receipt_sha256
    ):
        _fail("CURRENT_AUTHORIZATION_BINDING_INVALID")
    manifest = authority_spool.verify_symbol_receipt(
        context.root,
        authorization.spool_receipt,
        expected_generation_id=context.generation_id,
        expected_symbol=CURRENT_AUTH_SPOOL_SYMBOL,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    artifacts = manifest.get("artifacts")
    if (
        type(artifacts) is not list
        or len(artifacts) != 1
        or artifacts[0].get("artifact_id") != "current-search-authorization"
        or artifacts[0].get("descriptor", {}).get("sha256")
        != authorization.receipt_sha256
    ):
        _fail("CURRENT_AUTHORIZATION_SPOOL_ROSTER_INVALID")
    return True


def _artifact_namespace(value: str) -> str:
    raw = _identity(value, "ARTIFACT_NAMESPACE_INVALID", maximum=512)
    parts = raw.split("/")
    if not 1 <= len(parts) <= 12 or any(
        not part
        or len(part) > 96
        or not part[0].isalnum()
        or any(not (character.isalnum() or character in "._:-") for character in part)
        for part in parts
    ):
        _fail("ARTIFACT_NAMESPACE_INVALID")
    return "/".join(parts)


def _reverify_terminal_product(
    context: authority_spool.GenerationSpoolContext,
    *,
    product: native_prefix.NativePrefixProduct,
    anchor: Mapping[str, Any],
) -> native_prefix.NativePrefixProduct:
    if (
        type(product) is not native_prefix.NativePrefixProduct
        or product.source_state
        not in {
            native_prefix.SOURCE_UNAVAILABLE,
            native_prefix.TECHNICAL_TERMINAL,
        }
        or type(product.index) is not dict
        or type(product.native_prefix) is not dict
        or type(product.index_descriptor) is not dict
    ):
        _fail("TERMINAL_NATIVE_PRODUCT_INVALID")
    index = dict(product.index)
    native = dict(product.native_prefix)
    if (
        index.get("source_state") != product.source_state
        or native.get("source_state") != product.source_state
        or index.get("cutoff_utc") != anchor.get("selection_cutoff_utc")
        or native.get("source_terminal_receipt_sha256") is None
        or native.get("prefix_content_sha256") is not None
        or native.get("prefix_rows") is not None
        or _utc(native.get("source_accessed_at_utc"), "SOURCE_ACCESS_TIME_INVALID")
        > _utc(anchor.get("evidence_accessed_at_utc"), "EVIDENCE_ACCESS_TIME_INVALID")
    ):
        _fail("TERMINAL_NATIVE_PRODUCT_BINDING_INVALID")
    try:
        reopened = native_prefix.reverify_activated_native_prefix_product(
            product,
            context,
            require_current_activation=False,
        )
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "TERMINAL_NATIVE_PRODUCT_REVERIFY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    if (
        reopened.source_state != product.source_state
        or reopened.index_sha256 != product.index_sha256
        or dict(reopened.index) != index
        or dict(reopened.index_descriptor) != dict(product.index_descriptor)
        or dict(reopened.native_prefix) != native
    ):
        _fail("TERMINAL_NATIVE_PRODUCT_REOPEN_MISMATCH")
    return reopened


def _terminal_dependency_and_policy(
    *,
    process: FrozenProductionSearchProcess,
    causal_precommit_receipt_sha256: str,
    product: native_prefix.NativePrefixProduct,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    native = dict(product.native_prefix)
    timeframe = str(native["timeframe"])
    payload = _dependency_payload(
        timeframe,
        feature_lookback_bars=1,
        regime_warmup_bars=1,
    )
    payload_sha = _sha(payload)
    terminal_contract = {
        "schema_version": "fxpm.terminal-search-non-evaluation-contract.v1",
        "source_state": product.source_state,
        "native_prefix_index_sha256": product.index_sha256,
        "source_terminal_receipt_sha256": native["source_terminal_receipt_sha256"],
        "economic_evaluation": "NOT_APPLICABLE",
    }
    dependency_receipt = _sha(
        {
            "schema_version": "fxpm.production-dependency-policy-binding.v1",
            "causal_process_precommit_receipt_sha256": (
                causal_precommit_receipt_sha256
            ),
            "search_process_receipt_sha256": process.receipt_sha256,
            "timeframe": timeframe,
            "payload_sha256": payload_sha,
            "terminal_contract_sha256": _sha(terminal_contract),
        }
    )
    dependency_policy = {
        "schema_version": "fxpm.historical-dependency-policy.v1",
        "authority_key_id": "fxpm-production-search-process-v1",
        "signed_policy_receipt_sha256": dependency_receipt,
        "payload_sha256": payload_sha,
        "payload": payload,
    }
    horizon_policy = {
        "schema_version": "fxpm.historical-horizon-policy.v1",
        "policy_id": f"fxpm-production-terminal-{timeframe}-v1",
        "policy_release_sha256": PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256,
        "cost_contract_sha256": _sha({**terminal_contract, "role": "BASE"}),
        "cost_stress_contract_sha256": _sha({**terminal_contract, "role": "STRESS"}),
        "evaluator_release_sha256": (
            search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
        ),
    }
    return payload, dependency_policy, horizon_policy


def _seal_terminal_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    sink: authority_spool.SymbolAuthoritySink,
    namespace: str,
    process: FrozenProductionSearchProcess,
    causal_precommit_receipt_sha256: str,
    product: native_prefix.NativePrefixProduct,
    regime: str,
    horizon_plan: Any,
    dependency_payload: Mapping[str, Any],
) -> tuple[bytes, bytes]:
    if type(sink) is not authority_spool.SymbolAuthoritySink:
        _fail("CELL_PLAN_SINK_INVALID")
    normalized_namespace = _artifact_namespace(namespace)
    native = dict(product.native_prefix)
    symbol = str(native["symbol"])
    timeframe = str(native["timeframe"])
    plan_binding = historical.historical_horizon_plan_binding(horizon_plan)
    native_binding = {
        "source_state": product.source_state,
        "index_sha256": product.index_sha256,
        "index_descriptor": dict(product.index_descriptor),
        "activation_manifest_sha256": native["activation_manifest_sha256"],
        "activation_entry_sha256": native["activation_entry_sha256"],
        "source_terminal_receipt_sha256": native["source_terminal_receipt_sha256"],
        "source_reason": native["source_reason"],
        "source_accessed_at_utc": native["source_accessed_at_utc"],
    }
    body = {
        "schema_version": CELL_PLAN_SCHEMA,
        "authority_scope": "FROZEN_CELL_SEARCH_PLAN_ONLY",
        "mode": "TERMINAL",
        "artifact_namespace": normalized_namespace,
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "run_id": process.to_dict()["run_id"],
        "causal_process_precommit_receipt_sha256": _hex64(
            causal_precommit_receipt_sha256,
            "CAUSAL_PRECOMMIT_RECEIPT_INVALID",
        ),
        "search_process_receipt_sha256": process.receipt_sha256,
        "cell": {"symbol": symbol, "timeframe": timeframe, "regime": regime},
        "native_prefix": native_binding,
        "source_group_decision_sha256": None,
        "regime_fit_decision_sha256": None,
        "train_regime_support_sha256": None,
        "candidate": None,
        "boundary_limited": False,
        "dependency_payload": dict(dependency_payload),
        "dependency_geometry": _derived_dependency_geometry(
            timeframe, dependency_payload
        ),
        "horizon_plan_binding": plan_binding,
        "attempt_count": 0,
        "attempt_artifact_sha256": [],
        "attempt_roster_sha256": _sha([]),
        "state_requirement_sha256": [],
        "state_requirement_roster_sha256": _sha([]),
        "attempts_frozen_before_search": True,
        "state_replay_required_before_scored_partition": False,
        "terminal_source_is_market_no_trade_verdict": False,
        "technical_failure_requires_generation_abort": (
            product.source_state == native_prefix.TECHNICAL_FAILURE
        ),
        "confers_winner_causal_current_capital_publication_or_live_authority": False,
    }
    receipt_body = {**body, "receipt_sha256": _sha(body)}
    artifact_id = f"{normalized_namespace}/search-cell-plan/{timeframe}/{regime}"
    entry = sink.put_json(artifact_id, CELL_PLAN_ARTIFACT_TYPE, receipt_body)
    encoded = _canonical_bytes(receipt_body)
    descriptor = entry.get("descriptor")
    if (
        type(descriptor) is not dict
        or descriptor.get("sha256") != hashlib.sha256(encoded).hexdigest()
        or descriptor.get("size_bytes") != len(encoded)
    ):
        _fail("CELL_PLAN_SPOOL_DESCRIPTOR_MISMATCH")
    return encoded, _canonical_bytes(entry, maximum=256 * 1024)


def _regime_support_binding(
    fitted: search_execution.FittedNativeSearchTimeframe,
    *,
    regime: str,
) -> dict[str, Any]:
    source_body = fitted.source_group_decision.to_dict()
    fit_body = fitted.regime_fit_decision.to_dict()
    support = dict(fitted.train_regime_support)
    counts = support.get("counts")
    if (
        fit_body.get("fit_state") != book.REGIME_FITTED
        or fit_body.get("source_group_decision_sha256")
        != fitted.source_group_decision.receipt_sha256
        or fit_body.get("regime_detector_record_sha256") is None
        or fit_body.get("train_regime_support") != support
        or type(counts) is not dict
        or set(counts) != set(REGIMES)
        or counts.get(regime) != 0
        or source_body.get("availability") != book.AVAILABLE
    ):
        _fail("UNOBSERVED_REGIME_SUPPORT_BINDING_INVALID")
    body = {
        "schema_version": "fxpm.historical-horizon-regime-support-binding.v1",
        "source_group_decision_sha256": (fitted.source_group_decision.receipt_sha256),
        "regime_fit_decision_sha256": fitted.regime_fit_decision.receipt_sha256,
        "regime_detector_record_sha256": fit_body["regime_detector_record_sha256"],
        "train_regime_support_receipt_sha256": support["receipt_sha256"],
        "target_regime": regime,
        "target_regime_count": 0,
        "train_regime_support": support,
    }
    return {**body, "binding_sha256": _sha(body)}


def _seal_unobserved_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    sink: authority_spool.SymbolAuthoritySink,
    mode: str,
    namespace: str,
    process: FrozenProductionSearchProcess,
    causal_precommit_receipt_sha256: str,
    fitted: search_execution.FittedNativeSearchTimeframe,
    regime: str,
    horizon_plan: Any,
    support_binding: Mapping[str, Any],
    dependency_payload: Mapping[str, Any],
    dependency_geometry: Mapping[str, Any],
) -> tuple[bytes, bytes]:
    if type(sink) is not authority_spool.SymbolAuthoritySink:
        _fail("CELL_PLAN_SINK_INVALID")
    normalized_mode = _identity(mode, "CELL_PLAN_MODE_INVALID").upper()
    if normalized_mode not in {
        "HISTORICAL_REGIME_UNOBSERVED",
        "CURRENT_REGIME_UNOBSERVED",
    }:
        _fail("CELL_PLAN_MODE_INVALID")
    normalized_namespace = _artifact_namespace(namespace)
    native = dict(fitted.product.native_prefix)
    symbol = str(native["symbol"])
    timeframe = str(native["timeframe"])
    plan_binding = historical.historical_horizon_plan_binding(horizon_plan)
    body = {
        "schema_version": CELL_PLAN_SCHEMA,
        "authority_scope": "FROZEN_CELL_SEARCH_PLAN_ONLY",
        "mode": normalized_mode,
        "artifact_namespace": normalized_namespace,
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "run_id": process.to_dict()["run_id"],
        "causal_process_precommit_receipt_sha256": _hex64(
            causal_precommit_receipt_sha256,
            "CAUSAL_PRECOMMIT_RECEIPT_INVALID",
        ),
        "search_process_receipt_sha256": process.receipt_sha256,
        "cell": {"symbol": symbol, "timeframe": timeframe, "regime": regime},
        "native_prefix": {
            "index_sha256": fitted.product.index_sha256,
            "content_sha256": native["prefix_content_sha256"],
            "start_utc": native["prefix_start_utc"],
            "end_exclusive_utc": native["prefix_end_exclusive_utc"],
            "last_close_utc": native["prefix_last_close_utc"],
            "rows": native["prefix_rows"],
        },
        "source_group_decision_sha256": (fitted.source_group_decision.receipt_sha256),
        "regime_fit_decision_sha256": fitted.regime_fit_decision.receipt_sha256,
        "train_regime_support_sha256": fitted.train_regime_support["receipt_sha256"],
        "regime_support_binding": dict(support_binding),
        "candidate": None,
        "boundary_limited": False,
        "dependency_payload": dict(dependency_payload),
        "dependency_geometry": dict(dependency_geometry),
        "horizon_plan_binding": plan_binding,
        "attempt_count": 0,
        "attempt_artifact_sha256": [],
        "attempt_roster_sha256": _sha([]),
        "state_requirement_sha256": [],
        "state_requirement_roster_sha256": _sha([]),
        "attempts_frozen_before_search": True,
        "state_replay_required_before_scored_partition": False,
        "empirically_unobserved_is_no_trade_verdict": False,
        "confers_winner_causal_current_capital_publication_or_live_authority": False,
    }
    receipt_body = {**body, "receipt_sha256": _sha(body)}
    artifact_id = f"{normalized_namespace}/search-cell-plan/{timeframe}/{regime}"
    entry = sink.put_json(artifact_id, CELL_PLAN_ARTIFACT_TYPE, receipt_body)
    encoded = _canonical_bytes(receipt_body)
    descriptor = entry.get("descriptor")
    if (
        type(descriptor) is not dict
        or descriptor.get("sha256") != hashlib.sha256(encoded).hexdigest()
        or descriptor.get("size_bytes") != len(encoded)
    ):
        _fail("CELL_PLAN_SPOOL_DESCRIPTOR_MISMATCH")
    return encoded, _canonical_bytes(entry, maximum=256 * 1024)


def _seal_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    sink: authority_spool.SymbolAuthoritySink,
    mode: str,
    namespace: str,
    process: FrozenProductionSearchProcess,
    causal_precommit_receipt_sha256: str,
    fitted: search_execution.FittedNativeSearchTimeframe,
    regime: str,
    horizon_plan: Any,
    candidate: Mapping[str, Any],
    dependency_payload: Mapping[str, Any],
    dependency_geometry: Mapping[str, Any],
    attempt_artifacts: Sequence[Mapping[str, Any]],
    state_requirements: Sequence[Mapping[str, Any]],
    state_context_selection: Mapping[str, Any],
    boundary_limited: bool,
    current_refit_source_capture_sha256: str | None = None,
    bounded_canary_request: Any = None,
    bounded_canary_source_session: Any = None,
    bounded_canary_plan_permit: Any = None,
) -> tuple[bytes, bytes]:
    if type(sink) is not authority_spool.SymbolAuthoritySink:
        _fail("CELL_PLAN_SINK_INVALID")
    normalized_mode = _identity(mode, "CELL_PLAN_MODE_INVALID").upper()
    if normalized_mode not in {
        "HISTORICAL_ANCHOR",
        "CURRENT_REFIT",
        "BOUNDED_RAW_WINNER_CANARY",
    }:
        _fail("CELL_PLAN_MODE_INVALID")
    if normalized_mode == "CURRENT_REFIT":
        source_capture_sha256 = _hex64(
            current_refit_source_capture_sha256,
            "CURRENT_SOURCE_CAPTURE_SHA256_INVALID",
        )
    elif current_refit_source_capture_sha256 is not None:
        _fail("HISTORICAL_SOURCE_CAPTURE_FORBIDDEN")
    else:
        source_capture_sha256 = None
    if normalized_mode == "BOUNDED_RAW_WINNER_CANARY":
        if (
            type(bounded_canary_request) is not historical.BoundedRawWinnerCanaryRequest
            or type(bounded_canary_source_session)
            is not search_execution.BoundedRawWinnerCanarySourceSession
            or bounded_canary_plan_permit is None
        ):
            _fail("CANARY_CELL_PLAN_CAPABILITY_REQUIRED")
    elif any(
        value is not None
        for value in (
            bounded_canary_request,
            bounded_canary_source_session,
            bounded_canary_plan_permit,
        )
    ):
        _fail("NONCANARY_CELL_PLAN_CAPABILITY_FORBIDDEN")
    normalized_namespace = _artifact_namespace(namespace)
    native = dict(fitted.product.native_prefix)
    symbol = str(native["symbol"])
    timeframe = str(native["timeframe"])
    plan_binding = historical.historical_horizon_plan_binding(horizon_plan)
    attempt_hashes = [_sha(dict(value)) for value in attempt_artifacts]
    state_hashes = [
        _hex64(value.get("requirement_sha256"), "STATE_REQUIREMENT_HASH_INVALID")
        for value in state_requirements
    ]
    if (
        len(attempt_hashes) != ATTEMPT_COUNT
        or len(state_hashes) != ATTEMPT_COUNT
        or len(set(attempt_hashes)) != len(attempt_hashes)
        or len(state_hashes) != len(set(state_hashes))
    ):
        _fail("CELL_PLAN_ROSTER_INVALID")
    body = {
        "schema_version": CELL_PLAN_SCHEMA,
        "authority_scope": "FROZEN_CELL_SEARCH_PLAN_ONLY",
        "mode": normalized_mode,
        "artifact_namespace": normalized_namespace,
        "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "run_id": process.to_dict()["run_id"],
        "causal_process_precommit_receipt_sha256": _hex64(
            causal_precommit_receipt_sha256,
            "CAUSAL_PRECOMMIT_RECEIPT_INVALID",
        ),
        "search_process_receipt_sha256": process.receipt_sha256,
        "cell": {
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": regime,
        },
        "native_prefix": {
            "index_sha256": fitted.product.index_sha256,
            "content_sha256": native["prefix_content_sha256"],
            "start_utc": native["prefix_start_utc"],
            "end_exclusive_utc": native["prefix_end_exclusive_utc"],
            "last_close_utc": native["prefix_last_close_utc"],
            "rows": native["prefix_rows"],
        },
        "source_group_decision_sha256": fitted.source_group_decision.receipt_sha256,
        "regime_fit_decision_sha256": fitted.regime_fit_decision.receipt_sha256,
        "train_regime_support_sha256": fitted.train_regime_support["receipt_sha256"],
        "candidate": dict(candidate),
        "boundary_limited": bool(boundary_limited),
        "dependency_payload": dict(dependency_payload),
        "dependency_geometry": dict(dependency_geometry),
        "horizon_plan_binding": plan_binding,
        "attempt_count": len(attempt_hashes),
        "attempt_artifact_sha256": attempt_hashes,
        "attempt_roster_sha256": _sha(attempt_hashes),
        "state_requirement_sha256": state_hashes,
        "state_requirement_roster_sha256": _sha(state_hashes),
        "state_context_selection": dict(state_context_selection),
        "current_refit_source_capture_sha256": source_capture_sha256,
        "attempts_frozen_before_search": True,
        "state_replay_required_before_scored_partition": True,
        "confers_winner_causal_current_capital_publication_or_live_authority": False,
    }
    if normalized_mode == "BOUNDED_RAW_WINNER_CANARY":
        body.update(
            {
                "bounded_canary_request_sha256": (
                    bounded_canary_request.receipt_sha256
                ),
                "bounded_canary_source_session_receipt_sha256": (
                    bounded_canary_source_session.receipt_sha256
                ),
                "bounded_canary_plan_permit_receipt_sha256": (
                    bounded_canary_plan_permit.receipt_sha256
                ),
            }
        )
    receipt_body = {**body, "receipt_sha256": _sha(body)}
    artifact_id = f"{normalized_namespace}/search-cell-plan/{timeframe}/{regime}"
    entry = sink.put_json(artifact_id, CELL_PLAN_ARTIFACT_TYPE, receipt_body)
    encoded = _canonical_bytes(receipt_body)
    descriptor = entry.get("descriptor")
    if (
        type(descriptor) is not dict
        or descriptor.get("sha256") != hashlib.sha256(encoded).hexdigest()
        or descriptor.get("size_bytes") != len(encoded)
    ):
        _fail("CELL_PLAN_SPOOL_DESCRIPTOR_MISMATCH")
    return encoded, _canonical_bytes(entry, maximum=256 * 1024)


def _verify_current_refit_source_capture(
    context: authority_spool.GenerationSpoolContext,
    *,
    sink: authority_spool.SymbolAuthoritySink,
    capture: Any,
) -> tuple[dict[str, Any], str]:
    if type(capture) is not dict or set(capture) != {
        "schema_version",
        "source_snapshot",
        "source_raw_descriptor",
        "bars_raw_descriptor",
        "source_spool_entry",
        "bars_spool_entry",
        "selection_prefix_receipt",
        "frozen_before_search_evaluation",
        "post_selection_rewrite_allowed",
    }:
        _fail("CURRENT_SOURCE_CAPTURE_FIELDS_INVALID")
    if (
        capture["schema_version"] != CURRENT_SOURCE_CAPTURE_SCHEMA
        or capture["frozen_before_search_evaluation"] is not True
        or capture["post_selection_rewrite_allowed"] is not False
    ):
        _fail("CURRENT_SOURCE_CAPTURE_SEMANTICS_INVALID")
    source = capture["source_snapshot"]
    selection_prefix = capture["selection_prefix_receipt"]
    if (
        type(source) is not dict
        or type(selection_prefix) is not dict
        or set(selection_prefix)
        != {
            "schema_version",
            "fit_start_utc",
            "fit_end_exclusive_utc",
            "latest_closed_bar_utc",
            "source_bar_count",
            "fit_frame_sha256",
            "native_prefix_content_sha256",
        }
    ):
        _fail("CURRENT_SELECTION_PREFIX_RECEIPT_INVALID")
    if (
        selection_prefix["schema_version"] != "fxpm.current-selection-prefix-receipt.v1"
        or selection_prefix["source_bar_count"] != source.get("bar_count")
        or selection_prefix["latest_closed_bar_utc"]
        != source.get("latest_closed_bar_utc")
        or selection_prefix["fit_end_exclusive_utc"] != source.get("fit_end_utc")
        or source.get("fit_start_utc") != source.get("first_bar_utc")
        or source.get("latest_closed_bar_utc") != source.get("last_bar_utc")
        or any(
            not isinstance(selection_prefix.get(name), str)
            or not selection_prefix[name]
            for name in ("fit_start_utc", "fit_end_exclusive_utc")
        )
        or any(
            not isinstance(selection_prefix.get(name), str)
            or len(selection_prefix[name]) != 64
            for name in ("fit_frame_sha256", "native_prefix_content_sha256")
        )
    ):
        _fail("CURRENT_SELECTION_PREFIX_RECEIPT_INVALID")
    if not (
        _utc(
            selection_prefix["fit_start_utc"],
            "CURRENT_SELECTION_FIT_START_INVALID",
        )
        < _utc(source["fit_start_utc"], "CURRENT_SOURCE_FIT_START_INVALID")
        <= _utc(
            selection_prefix["latest_closed_bar_utc"],
            "CURRENT_SELECTION_LATEST_BAR_INVALID",
        )
        <= _utc(
            selection_prefix["fit_end_exclusive_utc"],
            "CURRENT_SELECTION_FIT_END_INVALID",
        )
    ):
        _fail("CURRENT_SELECTION_PREFIX_GEOMETRY_INVALID")
    if type(source) is not dict or set(source) != {
        "schema_version",
        "symbol",
        "timeframe",
        "fit_start_utc",
        "fit_end_utc",
        "latest_closed_bar_utc",
        "first_bar_utc",
        "last_bar_utc",
        "bar_count",
        "bars_artifact_id",
        "bars_content_sha256",
        "activation_id",
        "activation_manifest_sha256",
        "native_source_sha256",
        "source_contract_sha256",
        "continuity_receipt_sha256",
        "columns",
        "timestamp_encoding",
    }:
        _fail("CURRENT_SOURCE_SNAPSHOT_FIELDS_INVALID")
    source_sha256 = current_refit.canonical_artifact_sha256(source)
    source_raw = capture["source_raw_descriptor"]
    bars_raw = capture["bars_raw_descriptor"]
    source_entry = capture["source_spool_entry"]
    bars_entry = capture["bars_spool_entry"]
    if any(
        type(value) is not dict
        for value in (source_raw, bars_raw, source_entry, bars_entry)
    ):
        _fail("CURRENT_SOURCE_CAPTURE_DESCRIPTOR_INVALID")
    expected_source_raw = _raw_descriptor_from_spool(
        source_entry.get("descriptor", {}),
        semantic_sha256=source_sha256,
        format_version=CURRENT_SOURCE_SNAPSHOT_SCHEMA,
    )
    expected_bars_raw = _raw_descriptor_from_spool(
        bars_entry.get("descriptor", {}),
        semantic_sha256=_hex64(
            source.get("bars_content_sha256"),
            "CURRENT_SOURCE_BAR_CONTENT_SHA256_INVALID",
        ),
        format_version=CURRENT_BAR_FILE_SCHEMA,
    )
    if (
        source_raw != expected_source_raw
        or bars_raw != expected_bars_raw
        or source.get("bars_artifact_id") != bars_raw.get("artifact_id")
        or source.get("columns") != list(CURRENT_BAR_COLUMNS)
        or source.get("timestamp_encoding") != "UTC_ISO8601_PLUS00"
        or source.get("schema_version") != CURRENT_SOURCE_SNAPSHOT_SCHEMA
    ):
        _fail("CURRENT_SOURCE_CAPTURE_DESCRIPTOR_SPLICE")
    for entry, artifact_type in (
        (source_entry, CURRENT_SOURCE_ARTIFACT_TYPE),
        (bars_entry, CURRENT_BAR_ARTIFACT_TYPE),
    ):
        if entry.get("artifact_type") != artifact_type:
            _fail("CURRENT_SOURCE_CAPTURE_ARTIFACT_TYPE_INVALID")
        observed = sink.register_artifact(
            str(entry.get("artifact_id") or ""),
            artifact_type,
            entry.get("descriptor", {}),
        )
        if observed != entry:
            _fail("CURRENT_SOURCE_CAPTURE_SPOOL_ENTRY_CHANGED")
    root = Path(context.root).resolve(strict=True)
    for raw, expected_content in (
        (source_raw, source_sha256),
        (bars_raw, source["bars_content_sha256"]),
    ):
        relative = Path(str(raw.get("relative_path") or ""))
        if relative.is_absolute() or ".." in relative.parts:
            _fail("CURRENT_SOURCE_CAPTURE_PATH_INVALID")
        try:
            path = (root / relative).resolve(strict=True)
            path.relative_to(root)
            content = path.read_bytes()
        except (OSError, ValueError) as exc:
            raise ProductionSearchPlannerError(
                "CURRENT_SOURCE_CAPTURE_FILE_UNAVAILABLE",
                (type(exc).__name__, str(exc)),
            ) from exc
        if (
            len(content) != raw.get("size_bytes")
            or hashlib.sha256(content).hexdigest() != expected_content
        ):
            _fail("CURRENT_SOURCE_CAPTURE_FILE_CHANGED")
    source_path = root / Path(source_raw["relative_path"])
    if source_path.read_bytes() != current_refit.canonical_artifact_bytes(source):
        _fail("CURRENT_SOURCE_CAPTURE_JSON_CHANGED")
    return dict(source), source_sha256


def _verify_state_context_plan_binding(
    *,
    receipt: Mapping[str, Any],
    attempts: Sequence[Mapping[str, Any]],
    states: Sequence[Mapping[str, Any]],
    horizon_plan: Any,
    boundary_limited: bool,
    timeframe: str,
) -> None:
    """Recompute the full state-requirement -> Horizon disposition chain."""

    try:
        plan_body = horizon_plan.to_dict()
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "STATE_CONTEXT_HORIZON_PLAN_INVALID",
            (type(exc).__name__, str(exc)),
        ) from exc
    payload = receipt.get("dependency_payload")
    geometry = receipt.get("dependency_geometry")
    candidates = plan_body.get("candidates")
    trials = plan_body.get("trials")
    plan_attempts = plan_body.get("attempts")
    dependency_policy = plan_body.get("dependency_policy")
    if (
        type(payload) is not dict
        or type(geometry) is not dict
        or type(candidates) is not list
        or len(candidates) != 1
        or type(trials) is not list
        or type(plan_attempts) is not list
        or type(dependency_policy) is not dict
        or len(attempts) != len(states)
        or not states
        or len(trials) != len(states)
        or len(plan_attempts) != len(states)
    ):
        _fail("STATE_CONTEXT_PLAN_ROSTER_INVALID")
    candidate = candidates[0]
    selected = payload.get("feature_lookback_bars")
    if (
        type(candidate) is not dict
        or candidate != receipt.get("candidate")
        or type(selected) is not int
        or selected < 1
        or geometry != _derived_dependency_geometry(timeframe, payload)
        or geometry.get("warmup_bars", -1) < selected
        or plan_body.get("derived_dependency_geometry") != geometry
        or dependency_policy.get("payload") != payload
        or (not candidate.get("folds")) is not boundary_limited
    ):
        _fail("STATE_CONTEXT_GEOMETRY_BINDING_INVALID")

    for state in states:
        if type(state) is not dict:
            _fail("STATE_REQUIREMENT_BODY_INVALID")
        requirement_sha256 = _hex64(
            state.get("requirement_sha256"),
            "STATE_REQUIREMENT_HASH_INVALID",
        )
        requirement_body = {
            key: value for key, value in state.items() if key != "requirement_sha256"
        }
        state_evidence = state.get("state_evidence")
        if (
            requirement_sha256 != _sha(requirement_body)
            or state.get("execution_rule")
            != "COMPUTE_FEATURES_AND_RAW_SIGNALS_ONCE_OVER_FULL_PREFIX_THEN_SLICE"
            or type(state_evidence) is not list
            or any(
                type(value) is not dict
                or value.get("schema_version")
                != "fxpm.production-full-prefix-state-replay.v1"
                or value.get(
                    "requires_executor_full_prefix_replay_before_any_scored_slice"
                )
                is not True
                for value in state_evidence
            )
        ):
            _fail("STATE_REQUIREMENT_REPLAY_INVALID")

    expected_dispositions = _state_context_dispositions(
        states,
        candidate=candidate,
        selected_common_context_bars=selected,
    )
    expected_selection = _state_context_selection_receipt(
        states,
        expected_dispositions,
        selected_common_context_bars=selected,
        boundary_limited=boundary_limited,
    )
    if receipt.get("state_context_selection") != expected_selection:
        _fail("STATE_CONTEXT_SELECTION_RECEIPT_INVALID")

    for ordinal, (artifact, state, trial, disposition, plan_attempt) in enumerate(
        zip(
            attempts,
            states,
            trials,
            expected_dispositions,
            plan_attempts,
            strict=True,
        )
    ):
        process = artifact.get("winner_process") if type(artifact) is dict else None
        trial_definition = (
            artifact.get("trial_definition") if type(artifact) is dict else None
        )
        parameters = (
            trial_definition.get("parameters")
            if type(trial_definition) is dict
            else None
        )
        if (
            type(process) is not dict
            or type(parameters) is not dict
            or type(trial) is not dict
            or type(plan_attempt) is not dict
            or state.get("strategy_id") != process.get("strategy_id")
            or state.get("parameters_sha256") != _sha(parameters)
            or trial.get("strategy_id") != process.get("strategy_id")
            or trial.get("parameter_set_sha256") != _sha(parameters)
            or trial.get("state_context_disposition") != disposition
            or trial.get("trial_definition_sha256")
            != _sha(
                {
                    key: value
                    for key, value in trial.items()
                    if key != "trial_definition_sha256"
                }
            )
            or plan_attempt.get("trial_id") != trial.get("trial_id")
            or plan_attempt.get("candidate_id") != candidate.get("candidate_id")
            or plan_attempt.get("state_context_disposition_sha256")
            != disposition["disposition_sha256"]
        ):
            _fail("STATE_CONTEXT_ATTEMPT_BINDING_INVALID", ordinal)
        expected_status = (
            "PROCESS_REJECTED" if boundary_limited else disposition["status"]
        )
        expected_reason = (
            "SOURCE_BOUNDARY_SHORTFALL"
            if boundary_limited
            else disposition["reason_code"]
        )
        if (
            plan_attempt.get("status") != expected_status
            or plan_attempt.get("reason_code") != expected_reason
        ):
            _fail("STATE_CONTEXT_ATTEMPT_STATUS_INVALID", ordinal)


def verify_production_cell_search_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    sink: authority_spool.SymbolAuthoritySink,
    plan: ProductionCellSearchPlan,
) -> bool:
    """Reopen one immutable cell plan and rederive every carried roster hash."""

    verify_frozen_production_search_process(context, process)
    if (
        type(sink) is not authority_spool.SymbolAuthoritySink
        or type(plan) is not ProductionCellSearchPlan
        or plan._seal is not _CELL_PLAN_SEAL
    ):
        _fail("CELL_PLAN_OBJECT_INVALID")
    receipt = plan.cell_receipt
    receipt_sha256 = receipt.get("receipt_sha256")
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if (
        _canonical_bytes(receipt) != plan._cell_receipt_json
        or receipt_sha256 != _sha(body)
        or receipt.get("schema_version") != CELL_PLAN_SCHEMA
        or receipt.get("generation_id") != context.generation_id
        or receipt.get("generation_context_sha256") != context.context_sha256
        or receipt.get("run_id") != process.to_dict()["run_id"]
        or receipt.get("search_process_receipt_sha256") != process.receipt_sha256
        or receipt.get("mode") != plan.mode
        or receipt.get("boundary_limited") is not plan.boundary_limited
        or receipt.get("causal_process_precommit_receipt_sha256")
        != plan.causal_precommit_receipt_sha256
        or receipt.get("cell")
        != {
            "symbol": plan.symbol,
            "timeframe": plan.timeframe,
            "regime": plan.regime,
        }
    ):
        _fail("CELL_PLAN_RECEIPT_BINDING_INVALID")
    attempts = plan.attempt_artifacts()
    states = plan.state_requirements()
    source_capture = plan.current_refit_source_capture()
    if plan.mode == "BOUNDED_RAW_WINNER_CANARY":
        if (
            type(plan._bounded_canary_request)
            is not historical.BoundedRawWinnerCanaryRequest
            or type(plan._bounded_canary_source_session)
            is not search_execution.BoundedRawWinnerCanarySourceSession
            or plan._bounded_canary_plan_permit is None
            or receipt.get("bounded_canary_request_sha256")
            != plan._bounded_canary_request.receipt_sha256
        ):
            _fail("CANARY_CELL_PLAN_CAPABILITY_MISSING")
        search_execution._verify_bounded_raw_winner_canary_plan_bindings(
            context,
            request=plan._bounded_canary_request,
            process=process,
            source_session=plan._bounded_canary_source_session,
            plan_permit=plan._bounded_canary_plan_permit,
            plan=plan,
        )
    elif any(
        value is not None
        for value in (
            plan._bounded_canary_request,
            plan._bounded_canary_source_session,
            plan._bounded_canary_plan_permit,
        )
    ):
        _fail("NONCANARY_CELL_PLAN_CAPABILITY_FORBIDDEN")
    if plan.mode == "CURRENT_REFIT":
        if source_capture is None:
            _fail("CURRENT_SOURCE_CAPTURE_MISSING")
        source_snapshot, source_snapshot_sha256 = _verify_current_refit_source_capture(
            context,
            sink=sink,
            capture=source_capture,
        )
        if receipt.get("current_refit_source_capture_sha256") != _sha(
            source_capture
        ) or any(
            artifact.get("horizon_process", {}).get("source_snapshot_sha256")
            != source_snapshot_sha256
            or artifact.get("horizon_process", {}).get("source_bar_count")
            != source_snapshot["bar_count"]
            or artifact.get("horizon_process", {}).get("fit_start_utc")
            != source_snapshot["fit_start_utc"]
            or artifact.get("horizon_process", {}).get("fit_end_utc")
            != source_snapshot["fit_end_utc"]
            or artifact.get("horizon_process", {}).get("latest_closed_bar_utc")
            != source_snapshot["latest_closed_bar_utc"]
            for artifact in attempts
        ):
            _fail("CURRENT_SOURCE_CAPTURE_ATTEMPT_BINDING_INVALID")
    elif (
        source_capture is not None
        or receipt.get("current_refit_source_capture_sha256") is not None
    ):
        _fail("NONCURRENT_SOURCE_CAPTURE_FORBIDDEN")
    if any(
        _canonical_bytes(value) != encoded
        for value, encoded in zip(attempts, plan._attempt_artifacts_json)
    ) or any(
        _canonical_bytes(value) != encoded
        for value, encoded in zip(states, plan._state_requirements_json)
    ):
        _fail("CELL_PLAN_CARRIED_BYTES_NONCANONICAL")
    attempt_hashes = [_sha(value) for value in attempts]
    state_hashes = [
        _hex64(value.get("requirement_sha256"), "STATE_REQUIREMENT_HASH_INVALID")
        for value in states
    ]
    terminal = plan.mode == "TERMINAL"
    unobserved = plan.mode in {
        "HISTORICAL_REGIME_UNOBSERVED",
        "CURRENT_REGIME_UNOBSERVED",
    }
    expected_attempt_count = 0 if terminal or unobserved else ATTEMPT_COUNT
    if (
        len(attempt_hashes) != expected_attempt_count
        or len(state_hashes) != expected_attempt_count
        or receipt.get("attempt_count") != expected_attempt_count
        or receipt.get("attempt_artifact_sha256") != attempt_hashes
        or receipt.get("attempt_roster_sha256") != _sha(attempt_hashes)
        or receipt.get("state_requirement_sha256") != state_hashes
        or receipt.get("state_requirement_roster_sha256") != _sha(state_hashes)
        or receipt.get("horizon_plan_binding")
        != historical.historical_horizon_plan_binding(plan.horizon_plan)
    ):
        _fail("CELL_PLAN_ROSTER_REPLAY_INVALID")
    if not terminal and not unobserved:
        _verify_state_context_plan_binding(
            receipt=receipt,
            attempts=attempts,
            states=states,
            horizon_plan=plan.horizon_plan,
            boundary_limited=plan.boundary_limited,
            timeframe=plan.timeframe,
        )
    if terminal:
        product = plan.native_prefix_product
        try:
            horizon_body = plan.horizon_plan.to_dict()
        except Exception as exc:
            raise ProductionSearchPlannerError(
                "TERMINAL_HORIZON_PLAN_INVALID",
                (type(exc).__name__, str(exc)),
            ) from exc
        anchor = horizon_body.get("anchor") if type(horizon_body) is dict else None
        reopened = _reverify_terminal_product(
            context,
            product=product,
            anchor=anchor,
        )
        native = dict(reopened.native_prefix)
        expected_native = {
            "source_state": reopened.source_state,
            "index_sha256": reopened.index_sha256,
            "index_descriptor": dict(reopened.index_descriptor),
            "activation_manifest_sha256": native["activation_manifest_sha256"],
            "activation_entry_sha256": native["activation_entry_sha256"],
            "source_terminal_receipt_sha256": native["source_terminal_receipt_sha256"],
            "source_reason": native["source_reason"],
            "source_accessed_at_utc": native["source_accessed_at_utc"],
        }
        expected_status = (
            "HISTORICAL_SOURCE_UNAVAILABLE"
            if reopened.source_state == native_prefix.SOURCE_UNAVAILABLE
            else "HISTORICAL_TERMINAL_TECHNICAL"
        )
        if (
            receipt.get("native_prefix") != expected_native
            or horizon_body.get("native_prefix") != native
            or horizon_body.get("plan_status") != expected_status
            or receipt.get("source_group_decision_sha256") is not None
            or receipt.get("regime_fit_decision_sha256") is not None
            or receipt.get("train_regime_support_sha256") is not None
            or receipt.get("candidate") is not None
            or receipt.get("state_replay_required_before_scored_partition") is not False
            or receipt.get("terminal_source_is_market_no_trade_verdict") is not False
            or receipt.get("technical_failure_requires_generation_abort")
            is not (reopened.source_state == native_prefix.TECHNICAL_FAILURE)
        ):
            _fail("TERMINAL_CELL_PLAN_BINDING_INVALID")
        if plan.fitted_timeframe is not None:
            _fail("TERMINAL_CELL_PLAN_FITTED_STATE_FORBIDDEN")
    elif unobserved:
        fitted = plan.fitted_timeframe
        if (
            type(fitted) is not search_execution.FittedNativeSearchTimeframe
            or plan.native_prefix_product is not fitted.product
        ):
            _fail("UNOBSERVED_CELL_PLAN_FITTED_STATE_INVALID")
        _, native, _, _ = _validated_frame(fitted)
        support_binding = _regime_support_binding(fitted, regime=plan.regime)
        try:
            horizon_body = plan.horizon_plan.to_dict()
        except Exception as exc:
            raise ProductionSearchPlannerError(
                "UNOBSERVED_HORIZON_PLAN_INVALID",
                (type(exc).__name__, str(exc)),
            ) from exc
        expected_native = {
            "index_sha256": fitted.product.index_sha256,
            "content_sha256": native["prefix_content_sha256"],
            "start_utc": native["prefix_start_utc"],
            "end_exclusive_utc": native["prefix_end_exclusive_utc"],
            "last_close_utc": native["prefix_last_close_utc"],
            "rows": native["prefix_rows"],
        }
        if (
            receipt.get("native_prefix") != expected_native
            or receipt.get("source_group_decision_sha256")
            != fitted.source_group_decision.receipt_sha256
            or receipt.get("regime_fit_decision_sha256")
            != fitted.regime_fit_decision.receipt_sha256
            or receipt.get("train_regime_support_sha256")
            != fitted.train_regime_support["receipt_sha256"]
            or receipt.get("regime_support_binding") != support_binding
            or receipt.get("candidate") is not None
            or receipt.get("state_replay_required_before_scored_partition") is not False
            or receipt.get("empirically_unobserved_is_no_trade_verdict") is not False
            or horizon_body.get("native_prefix") != native
            or horizon_body.get("regime_support_binding") != support_binding
            or horizon_body.get("plan_status") != "HISTORICAL_REGIME_UNOBSERVED"
            or any(
                horizon_body.get(name) != []
                for name in (
                    "candidates",
                    "trials",
                    "attempts",
                    "expected_evaluations",
                )
            )
        ):
            _fail("UNOBSERVED_CELL_PLAN_BINDING_INVALID")
    elif (
        plan.mode
        not in {
            "HISTORICAL_ANCHOR",
            "CURRENT_REFIT",
            "BOUNDED_RAW_WINNER_CANARY",
        }
        or plan.native_prefix_product is not None
        or plan.fitted_timeframe is not None
    ):
        _fail("CELL_PLAN_MODE_INVALID")
    namespace = _artifact_namespace(receipt.get("artifact_namespace"))
    artifact_id = f"{namespace}/search-cell-plan/{plan.timeframe}/{plan.regime}"
    expected_entry = sink.put_json(artifact_id, CELL_PLAN_ARTIFACT_TYPE, receipt)
    observed_entry = plan.spool_entry
    if (
        _canonical_bytes(observed_entry, maximum=256 * 1024) != plan._spool_entry_json
        or expected_entry != observed_entry
        or observed_entry.get("artifact_id") != artifact_id
        or observed_entry.get("artifact_type") != CELL_PLAN_ARTIFACT_TYPE
        or observed_entry.get("descriptor", {}).get("sha256")
        != hashlib.sha256(plan._cell_receipt_json).hexdigest()
        or observed_entry.get("descriptor", {}).get("size_bytes")
        != len(plan._cell_receipt_json)
    ):
        _fail("CELL_PLAN_SPOOL_REOPEN_INVALID")
    return True


def _cell_hash(symbol: str, timeframe: str, regime: str, *parts: str) -> str:
    return hashlib.sha256(
        "\x00".join((PARAMETER_DESIGN_SEED, symbol, timeframe, regime, *parts)).encode(
            "ascii"
        )
    ).hexdigest()


def _domain_by_name(process_body: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    semantics = process_body.get("semantics")
    domains = semantics.get("strategy_domains") if type(semantics) is dict else None
    if type(domains) is not list or len(domains) != BASELINE_ATTEMPTS:
        _fail("PROCESS_DOMAIN_ROSTER_INVALID")
    result = {
        value.get("strategy_id"): dict(value)
        for value in domains
        if type(value) is dict
    }
    if len(result) != len(domains):
        _fail("PROCESS_DOMAIN_IDENTITY_INVALID")
    return result


def _variant_strategy_names(
    domains: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
    timeframe: str,
    regime: str,
) -> list[str]:
    affinity = _REGIME_CATEGORY_AFFINITY[regime]

    def order(item: Mapping[str, Any]) -> tuple[int, str]:
        family_match = 1 if item.get("strategy_family") == affinity else 0
        tie = _cell_hash(
            symbol,
            timeframe,
            regime,
            str(item.get("strategy_id") or ""),
            "variant-strategy-permutation",
        )
        return (-family_match, tie)

    ordered = sorted(domains, key=order)
    selected = [str(item["strategy_id"]) for item in ordered[:VARIANT_ATTEMPTS]]
    if len(selected) != VARIANT_ATTEMPTS or len(selected) != len(set(selected)):
        _fail("VARIANT_STRATEGY_ALLOCATION_INVALID")
    return selected


def _nondefault_parameters(
    strategy_name: str,
    *,
    symbol: str,
    timeframe: str,
    regime: str,
) -> dict[str, Any]:
    strategy = StrategyRegistry.get(strategy_name)
    defaults = strategy.get_default_params()
    grid = strategy.get_param_grid()
    runtime = dict(defaults)
    changed = False
    for axis_ordinal, (axis_name, choices) in enumerate(grid.items()):
        digest = _cell_hash(
            symbol,
            timeframe,
            regime,
            strategy_name,
            axis_name,
            str(axis_ordinal),
        )
        selected = choices[int(digest[:16], 16) % len(choices)]
        runtime[axis_name] = selected
        changed = changed or not parameter_values_equal(selected, defaults[axis_name])
    if not changed:
        for axis_name, choices in grid.items():
            alternatives = [
                value
                for value in choices
                if not parameter_values_equal(value, defaults[axis_name])
            ]
            if alternatives:
                runtime[axis_name] = alternatives[0]
                changed = True
                break
    if not changed:
        _fail("NONDEFAULT_PARAMETER_POINT_UNAVAILABLE", strategy_name)
    try:
        fitted = StrategyRegistry.get(strategy_name, **runtime)
        normalized = canonicalize_parameter_mapping(fitted.get_params())
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "NONDEFAULT_PARAMETER_MATERIALIZATION_FAILED", (strategy_name,)
        ) from exc
    baseline = canonicalize_parameter_mapping(strategy.get_params())
    if normalized == baseline:
        _fail("NONDEFAULT_PARAMETER_POINT_COLLAPSED", strategy_name)
    return normalized


def _attempt_designs(
    process_body: Mapping[str, Any],
    *,
    symbol: str,
    timeframe: str,
    regime: str,
) -> list[dict[str, Any]]:
    domains = list(_domain_by_name(process_body).values())
    domains.sort(key=lambda value: int(value["ordinal"]))
    designs: list[dict[str, Any]] = []
    for item in domains:
        name = str(item["strategy_id"])
        parameters = canonicalize_parameter_mapping(
            StrategyRegistry.get(name).get_params()
        )
        designs.append(
            {
                "design_role": "REGISTRY_DEFAULT",
                "strategy_id": name,
                "parameters": parameters,
            }
        )
    by_name = {str(item["strategy_id"]): item for item in domains}
    for name in _variant_strategy_names(
        domains, symbol=symbol, timeframe=timeframe, regime=regime
    ):
        if name not in by_name:
            _fail("VARIANT_STRATEGY_OUTSIDE_DOMAIN", name)
        designs.append(
            {
                "design_role": "DETERMINISTIC_NONDEFAULT_GRID_POINT",
                "strategy_id": name,
                "parameters": _nondefault_parameters(
                    name,
                    symbol=symbol,
                    timeframe=timeframe,
                    regime=regime,
                ),
            }
        )
    if len(designs) != ATTEMPT_COUNT or ATTEMPT_COUNT != search_execution.MAX_ATTEMPTS:
        _fail("ATTEMPT_COUNT_NOT_EXACT_WINNER_BOUND")
    identities = [(item["strategy_id"], _sha(item["parameters"])) for item in designs]
    if len(identities) != len(set(identities)):
        _fail("ATTEMPT_PARAMETER_IDENTITY_DUPLICATED")
    return designs


def _validated_frame(
    fitted: search_execution.FittedNativeSearchTimeframe,
) -> tuple[pd.DataFrame, dict[str, Any], pd.DatetimeIndex, pd.DatetimeIndex]:
    if type(fitted) is not search_execution.FittedNativeSearchTimeframe:
        _fail("FITTED_TIMEFRAME_INVALID")
    if type(fitted.product) is not native_prefix.NativePrefixProduct:
        _fail("NATIVE_PREFIX_PRODUCT_INVALID")
    native = dict(fitted.product.native_prefix)
    if fitted.product.source_state != native_prefix.AVAILABLE:
        _fail("NATIVE_PREFIX_NOT_AVAILABLE")
    frame = fitted.frame_copy()
    try:
        reopened = native_prefix.open_verified_prefix_frame(fitted.product)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "NATIVE_PREFIX_FRAME_REOPEN_FAILED", (type(exc).__name__, str(exc))
        ) from exc
    if (
        not isinstance(frame, pd.DataFrame)
        or frame.empty
        or "_BAR_CLOSE_TS" not in frame.columns
        or _frame_sha256(frame) != fitted.frame_sha256
        or not set(reopened.columns).issubset(frame.columns)
        or not reopened.index.equals(frame.index)
        or not reopened.equals(frame.loc[:, reopened.columns])
    ):
        _fail("FITTED_FRAME_BINDING_INVALID")
    opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(
        pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
    )
    if (
        opens.hasnans
        or closes.hasnans
        or not opens.is_monotonic_increasing
        or not opens.is_unique
        or len(opens) != len(closes)
        or bool((closes <= opens).any())
        or native.get("prefix_rows") != len(frame)
        or _utc_text(opens[0]) != native.get("prefix_first_open_utc")
        or _utc_text(opens[-1]) != native.get("prefix_last_open_utc")
        or _utc_text(closes[-1]) != native.get("prefix_last_close_utc")
        or _utc(native.get("prefix_last_close_utc"), "PREFIX_LAST_CLOSE_INVALID")
        > _utc(native.get("prefix_end_exclusive_utc"), "PREFIX_END_INVALID")
    ):
        _fail("FITTED_FRAME_GEOMETRY_INVALID")
    support = fitted.train_regime_support
    if type(support) is not dict:
        _fail("REGIME_SUPPORT_INVALID")
    receipt = support.get("receipt_sha256")
    body = {name: value for name, value in support.items() if name != "receipt_sha256"}
    if receipt != _sha(body):
        _fail("REGIME_SUPPORT_RECEIPT_INVALID")
    return frame, native, opens, closes


def _available_prefix_requested_cutoff(prefix_index: Mapping[str, Any]) -> Any:
    if type(prefix_index) is not dict:
        _fail("NATIVE_PREFIX_INDEX_INVALID")
    return prefix_index.get("cutoff_utc")


def _validate_available_prefix_anchor_binding(
    *,
    native: Mapping[str, Any],
    prefix_index: Mapping[str, Any],
    anchor: Any,
    canary_request_body: Mapping[str, Any] | None,
) -> None:
    """Bind the requested cutoff without requiring a bar at that instant."""

    if type(anchor) is not dict:
        _fail("CELL_PLAN_ANCHOR_INVALID")
    requested_cutoff = _available_prefix_requested_cutoff(prefix_index)
    if canary_request_body is not None:
        request_anchor = canary_request_body.get("anchor")
        if (
            type(request_anchor) is not dict
            or native.get("symbol") != canary_request_body.get("symbol")
            or native.get("timeframe") != canary_request_body.get("timeframe")
            or native.get("activation_id") != canary_request_body.get("activation_id")
            or native.get("activation_manifest_sha256")
            != canary_request_body.get("activation_manifest_sha256")
            or requested_cutoff != request_anchor.get("selection_cutoff_utc")
        ):
            _fail("CANARY_NATIVE_PREFIX_BINDING_INVALID")
    if requested_cutoff != anchor.get("selection_cutoff_utc") or _utc(
        native.get("source_accessed_at_utc"), "SOURCE_ACCESS_TIME_INVALID"
    ) > _utc(anchor.get("evidence_accessed_at_utc"), "EVIDENCE_ACCESS_TIME_INVALID"):
        _fail("NATIVE_PREFIX_NOT_EXACT_ANCHOR_CUTOFF")


def _state_error_and_tolerance(
    frame: pd.DataFrame,
    fitted: search_execution.FittedNativeSearchTimeframe,
) -> tuple[float, float]:
    try:
        values = pd.to_numeric(
            frame.loc[:, ["Open", "High", "Low", "Close"]].stack(),
            errors="coerce",
        )
        if values.empty or values.isna().any():
            _fail("STATE_PRICE_SURFACE_INVALID")
        maximum = float(values.abs().max())
        spread = float(values.max() - values.min())
        initial_error = max(STATE_INITIAL_ERROR_FLOOR, 2.0 * maximum, spread)
        instrument = dict(fitted.product.instrument_snapshot or {})
        parity = float(instrument.get("parity_tick_tolerance"))
        tick_size = float(fitted.instrument_spec.tick_size)
        tolerance = min(parity, tick_size)
    except ProductionSearchPlannerError:
        raise
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "STATE_ERROR_TOLERANCE_DERIVATION_FAILED", (type(exc).__name__,)
        ) from exc
    if (
        not math.isfinite(initial_error)
        or not math.isfinite(tolerance)
        or initial_error <= 0.0
        or tolerance <= 0.0
        or tolerance > tick_size
    ):
        _fail("STATE_ERROR_TOLERANCE_INVALID")
    return initial_error, tolerance


def _state_replay_requirement(
    *,
    fitted: search_execution.FittedNativeSearchTimeframe,
    native: Mapping[str, Any],
    strategy_name: str,
    wire_parameters: Mapping[str, Any],
    initial_error_bound: float,
    tolerance: float,
) -> dict[str, Any]:
    base = StrategyRegistry.get(strategy_name)
    try:
        parameters = decode_parameter_mapping(
            dict(wire_parameters), base.get_default_params()
        )
        strategy = StrategyRegistry.get(strategy_name, **parameters)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "STATE_STRATEGY_PARAMETERS_INVALID",
            (strategy_name, type(exc).__name__, str(exc)),
        ) from exc
    requested = sorted(strategy.get_feature_request())
    unresolved = state_contract.DEFAULT_AUTHORITY.resolve_cell(
        strategy_name,
        parameters=parameters,
        requested_features=requested,
        timeframe=str(native["timeframe"]),
        include_regime_controller=False,
        initial_error_bound=initial_error_bound,
        tolerance=tolerance,
    )
    evidence: dict[str, state_contract.StateEvidence] = {}
    evidence_bodies: list[dict[str, Any]] = []
    for node in unresolved.required_path_state_nodes:
        contract = state_contract.DEFAULT_AUTHORITY._contracts.get(node)  # noqa: SLF001
        if (
            contract is None
            or not contract.checkpoint_schema
            or not contract.transition_schema
        ):
            _fail("STATE_REPLAY_SCHEMA_UNAVAILABLE", strategy_name, node)
        identity_body = {
            "schema_version": "fxpm.production-full-prefix-state-replay.v1",
            "node": node,
            "mode": state_contract.EvidenceMode.AUTHENTICATED_REPLAY.value,
            "state_contract_registry_sha256": (
                state_contract.DEFAULT_AUTHORITY.registry_hash
            ),
            "native_prefix_index_sha256": fitted.product.index_sha256,
            "native_prefix_content_sha256": native["prefix_content_sha256"],
            "prefix_start_utc": native["prefix_start_utc"],
            "prefix_end_exclusive_utc": native["prefix_end_exclusive_utc"],
            "prefix_rows": native["prefix_rows"],
            "strategy_id": strategy_name,
            "parameters_sha256": _sha(dict(wire_parameters)),
            "feature_request": requested,
            "feature_computer_release_sha256": _source_sha256(
                search_execution.FeatureComputer
            ),
            "state_schema": contract.checkpoint_schema,
            "transition_schema": contract.transition_schema,
            "requires_executor_full_prefix_replay_before_any_scored_slice": True,
        }
        identity = _sha(identity_body)
        item = state_contract.StateEvidence(
            node=node,
            mode=state_contract.EvidenceMode.AUTHENTICATED_REPLAY,
            identity_sha256=identity,
            registry_hash=state_contract.DEFAULT_AUTHORITY.registry_hash,
            state_schema=str(contract.checkpoint_schema),
            transition_schema=str(contract.transition_schema),
        )
        evidence[node] = item
        evidence_bodies.append({**identity_body, "identity_sha256": identity})
    resolved = state_contract.DEFAULT_AUTHORITY.resolve_cell(
        strategy_name,
        parameters=parameters,
        requested_features=requested,
        timeframe=str(native["timeframe"]),
        include_regime_controller=False,
        initial_error_bound=initial_error_bound,
        tolerance=tolerance,
        state_evidence=evidence,
    )
    if (
        not resolved.admissible
        or type(resolved.bounded_context_bars) is not int
        or resolved.bounded_context_bars < 0
    ):
        _fail(
            "STATE_CONTRACT_PROCESS_REJECTED",
            strategy_name,
            *resolved.reason_codes,
        )
    body = {
        "schema_version": "fxpm.production-attempt-state-requirement.v1",
        "strategy_id": strategy_name,
        "parameters_sha256": _sha(dict(wire_parameters)),
        "requested_features": requested,
        "include_regime_controller": False,
        "regime_surface_source": "FITTED_FULL_NATIVE_PREFIX_REGIME_LIVE",
        "regime_fit_decision_sha256": fitted.regime_fit_decision.receipt_sha256,
        "initial_error_bound_hex": float(initial_error_bound).hex(),
        "tolerance_hex": float(tolerance).hex(),
        "state_evidence": evidence_bodies,
        "resolution": asdict(resolved),
        "resolution_decision_sha256": resolved.decision_hash,
        "bounded_context_bars": resolved.bounded_context_bars,
        "execution_rule": (
            "COMPUTE_FEATURES_AND_RAW_SIGNALS_ONCE_OVER_FULL_PREFIX_THEN_SLICE"
        ),
    }
    return {**body, "requirement_sha256": _sha(body)}


def _state_requirements(
    *,
    fitted: search_execution.FittedNativeSearchTimeframe,
    frame: pd.DataFrame,
    native: Mapping[str, Any],
    designs: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    initial_error, tolerance = _state_error_and_tolerance(frame, fitted)
    requirements = [
        _state_replay_requirement(
            fitted=fitted,
            native=native,
            strategy_name=str(design["strategy_id"]),
            wire_parameters=dict(design["parameters"]),
            initial_error_bound=initial_error,
            tolerance=tolerance,
        )
        for design in designs
    ]
    warmup_surface = frame.get("REGIME_WARMUP")
    if not isinstance(warmup_surface, pd.Series) or warmup_surface.empty:
        _fail("REGIME_WARMUP_SURFACE_MISSING")
    if warmup_surface.isna().any() or not warmup_surface.isin([True, False]).all():
        _fail("REGIME_WARMUP_SURFACE_INVALID")
    flags = warmup_surface.astype(bool).to_numpy()
    false_positions = [ordinal for ordinal, value in enumerate(flags) if not value]
    if not false_positions:
        _fail("REGIME_WARMUP_NEVER_COMPLETES")
    leading = false_positions[0]
    if any(flags[leading:]):
        _fail("REGIME_WARMUP_SURFACE_NONPREFIX")
    return requirements, max(1, leading)


def _midnight(value: pd.Timestamp) -> bool:
    return value.time() == datetime_time.min


def _latest(
    values: pd.DatetimeIndex, *, at_or_before: pd.Timestamp
) -> pd.Timestamp | None:
    eligible = values[values <= at_or_before]
    return None if eligible.empty else pd.Timestamp(eligible[-1])


def _earliest(
    values: pd.DatetimeIndex, *, at_or_after: pd.Timestamp
) -> pd.Timestamp | None:
    eligible = values[values >= at_or_after]
    return None if eligible.empty else pd.Timestamp(eligible[0])


def _cutoff_anchored_refit_window(
    *,
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    fit_end: pd.Timestamp,
    depth_days: int,
    state_context_bars: int,
    boundary_limited: bool,
) -> tuple[pd.Timestamp, int]:
    """Realize the tested rolling lookback once at the deployment cutoff.

    A boundary-limited candidate has no folds and Horizon deterministically
    records every attempt as ``PROCESS_REJECTED/SOURCE_BOUNDARY_SHORTFALL``.
    Its authenticated source metadata is still frozen, but no strategy state
    is evaluated, so unavailable evaluation warmup cannot turn that terminal
    statistical outcome into a technical planner failure.
    """

    try:
        return search_execution._realize_cutoff_refit_window(
            opens=opens,
            closes=closes,
            fit_end=fit_end,
            lookback_kind="CALENDAR_INTERVAL",
            lookback_value=depth_days,
            state_context_bars=state_context_bars,
            boundary_limited=boundary_limited,
        )
    except search_execution.AuthoritativeSearchExecutionError as exc:
        _fail(exc.code)


def _current_refit_utc_text(value: Any, code: str) -> str:
    """Render the strict raw-executor timestamp spelling (never ``Z``)."""

    try:
        parsed = pd.Timestamp(value)
    except Exception as exc:
        raise ProductionSearchPlannerError(code) from exc
    if parsed.tzinfo is None:
        _fail(code)
    parsed = parsed.tz_convert("UTC")
    if parsed.microsecond or parsed.nanosecond:
        _fail(code)
    return parsed.isoformat()


def _current_refit_csv_number(value: Any) -> str:
    try:
        number = float(value)
    except Exception as exc:
        raise ProductionSearchPlannerError("CURRENT_SOURCE_BAR_VALUE_INVALID") from exc
    if not math.isfinite(number):
        _fail("CURRENT_SOURCE_BAR_VALUE_INVALID")
    if number == 0.0:
        return "0"
    return format(number, ".17g")


def _raw_descriptor_from_spool(
    descriptor: Mapping[str, Any],
    *,
    semantic_sha256: str,
    format_version: str,
) -> dict[str, Any]:
    if type(descriptor) is not dict:
        _fail("CURRENT_SOURCE_SPOOL_DESCRIPTOR_INVALID")
    try:
        return current_refit_execution.build_raw_artifact_descriptor(
            relative_path=str(descriptor["relative_path"]),
            size_bytes=int(descriptor["size_bytes"]),
            content_sha256=_hex64(
                descriptor["sha256"], "CURRENT_SOURCE_CONTENT_SHA256_INVALID"
            ),
            semantic_sha256=_hex64(
                semantic_sha256, "CURRENT_SOURCE_SEMANTIC_SHA256_INVALID"
            ),
            format_version=format_version,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ProductionSearchPlannerError(
            "CURRENT_SOURCE_RAW_DESCRIPTOR_INVALID",
            (type(exc).__name__, str(exc)),
        ) from exc


def _write_current_refit_source_capture(
    context: authority_spool.GenerationSpoolContext,
    *,
    sink: authority_spool.SymbolAuthoritySink,
    namespace: str,
    fitted: search_execution.FittedNativeSearchTimeframe,
    frame: pd.DataFrame,
    native: Mapping[str, Any],
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    current_fit_start: pd.Timestamp,
    fit_end: pd.Timestamp,
    expected_source_count: int,
) -> dict[str, Any]:
    """Spool the exact current fit source before any candidate identity exists.

    Raw current-refit timestamps denote completed observations.  Consequently
    the CSV axis uses the authenticated ``_BAR_CLOSE_TS`` values while the
    cutoff-window membership remains defined by native bar opens and closes.
    No price, volume, row, or timestamp is inferred or post-selected.
    """

    if type(context) is not authority_spool.GenerationSpoolContext:
        _fail("CURRENT_SOURCE_GENERATION_CONTEXT_INVALID")
    if type(sink) is not authority_spool.SymbolAuthoritySink:
        _fail("CURRENT_SOURCE_SINK_INVALID")
    normalized_namespace = _artifact_namespace(namespace)
    mask = (opens >= current_fit_start) & (opens < fit_end) & (closes <= fit_end)
    positions = mask.nonzero()[0]
    if (
        len(positions) != expected_source_count
        or len(positions) < 2
        or positions[-1] >= len(frame)
    ):
        _fail("CURRENT_SOURCE_WINDOW_COUNT_MISMATCH")
    selected = frame.iloc[positions]
    selected_closes = closes[positions]
    required = {"Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(selected.columns):
        _fail("CURRENT_SOURCE_COLUMNS_MISSING")
    try:
        native_frame = native_prefix.open_verified_prefix_frame(fitted.product)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "CURRENT_SOURCE_NATIVE_FRAME_REOPEN_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    native_selected = native_frame.iloc[positions].copy(deep=True)
    if len(native_selected) != len(selected):
        _fail("CURRENT_SOURCE_NATIVE_WINDOW_MISMATCH")
    selection_prefix_receipt = {
        "schema_version": "fxpm.current-selection-prefix-receipt.v1",
        "fit_start_utc": _current_refit_utc_text(
            opens[positions[0]], "CURRENT_SELECTION_FIT_START_INVALID"
        ),
        "fit_end_exclusive_utc": _current_refit_utc_text(
            fit_end, "CURRENT_SELECTION_FIT_END_INVALID"
        ),
        "latest_closed_bar_utc": _current_refit_utc_text(
            selected_closes[-1], "CURRENT_SELECTION_LATEST_BAR_INVALID"
        ),
        "source_bar_count": int(len(native_selected)),
        "fit_frame_sha256": _frame_sha256(native_selected),
        "native_prefix_content_sha256": _hex64(
            native["prefix_content_sha256"],
            "CURRENT_SELECTION_PREFIX_SHA256_INVALID",
        ),
    }
    del native_selected, native_frame

    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(CURRENT_BAR_COLUMNS)
    for close_timestamp, values in zip(
        selected_closes,
        selected.loc[:, ["Open", "High", "Low", "Close", "Volume"]].itertuples(
            index=False, name=None
        ),
    ):
        writer.writerow(
            (
                _current_refit_utc_text(
                    close_timestamp, "CURRENT_SOURCE_BAR_TIMESTAMP_INVALID"
                ),
                *(_current_refit_csv_number(value) for value in values),
            )
        )
    try:
        bar_bytes = output.getvalue().encode("ascii")
    except UnicodeError as exc:
        raise ProductionSearchPlannerError(
            "CURRENT_SOURCE_BAR_ENCODING_INVALID"
        ) from exc
    bar_content_sha256 = hashlib.sha256(bar_bytes).hexdigest()
    symbol = str(native["symbol"])
    timeframe = str(native["timeframe"])
    bars_entry = sink.put_opaque(
        f"{normalized_namespace}/raw-current-input/{symbol}/{timeframe}/bars",
        CURRENT_BAR_ARTIFACT_TYPE,
        (bar_bytes,),
        format="csv",
    )
    bars_raw_descriptor = _raw_descriptor_from_spool(
        bars_entry.get("descriptor", {}),
        semantic_sha256=bar_content_sha256,
        format_version=CURRENT_BAR_FILE_SCHEMA,
    )
    if bars_raw_descriptor["content_sha256"] != bar_content_sha256:
        _fail("CURRENT_SOURCE_BAR_SPOOL_MISMATCH")

    source_decision = fitted.source_group_decision.to_dict()
    source = {
        "schema_version": CURRENT_SOURCE_SNAPSHOT_SCHEMA,
        "symbol": symbol,
        "timeframe": timeframe,
        "fit_start_utc": _current_refit_utc_text(
            selected_closes[0], "CURRENT_SOURCE_FIT_START_INVALID"
        ),
        "fit_end_utc": _current_refit_utc_text(
            fit_end, "CURRENT_SOURCE_FIT_END_INVALID"
        ),
        "latest_closed_bar_utc": _current_refit_utc_text(
            selected_closes[-1], "CURRENT_SOURCE_LATEST_BAR_INVALID"
        ),
        "first_bar_utc": _current_refit_utc_text(
            selected_closes[0], "CURRENT_SOURCE_FIRST_BAR_INVALID"
        ),
        "last_bar_utc": _current_refit_utc_text(
            selected_closes[-1], "CURRENT_SOURCE_LAST_BAR_INVALID"
        ),
        "bar_count": int(len(selected)),
        "bars_artifact_id": bars_raw_descriptor["artifact_id"],
        "bars_content_sha256": bar_content_sha256,
        "activation_id": str(native["activation_id"]),
        "activation_manifest_sha256": _hex64(
            native["activation_manifest_sha256"],
            "CURRENT_SOURCE_ACTIVATION_MANIFEST_INVALID",
        ),
        "native_source_sha256": _hex64(
            native["artifact_sha256"], "CURRENT_SOURCE_NATIVE_SHA256_INVALID"
        ),
        "source_contract_sha256": _hex64(
            source_decision.get("source_contract_sha256"),
            "CURRENT_SOURCE_CONTRACT_SHA256_INVALID",
        ),
        "continuity_receipt_sha256": _hex64(
            native["continuity_receipt_sha256"],
            "CURRENT_SOURCE_CONTINUITY_SHA256_INVALID",
        ),
        "columns": list(CURRENT_BAR_COLUMNS),
        "timestamp_encoding": "UTC_ISO8601_PLUS00",
    }
    source_sha256 = current_refit.canonical_artifact_sha256(source)
    source_entry = sink.put_json(
        f"{normalized_namespace}/raw-current-input/{symbol}/{timeframe}/source",
        CURRENT_SOURCE_ARTIFACT_TYPE,
        source,
    )
    source_raw_descriptor = _raw_descriptor_from_spool(
        source_entry.get("descriptor", {}),
        semantic_sha256=source_sha256,
        format_version=CURRENT_SOURCE_SNAPSHOT_SCHEMA,
    )
    if source_raw_descriptor["content_sha256"] != source_sha256:
        _fail("CURRENT_SOURCE_SNAPSHOT_SPOOL_MISMATCH")
    capture = {
        "schema_version": CURRENT_SOURCE_CAPTURE_SCHEMA,
        "source_snapshot": source,
        "source_raw_descriptor": source_raw_descriptor,
        "bars_raw_descriptor": bars_raw_descriptor,
        "source_spool_entry": source_entry,
        "bars_spool_entry": bars_entry,
        "selection_prefix_receipt": selection_prefix_receipt,
        "frozen_before_search_evaluation": True,
        "post_selection_rewrite_allowed": False,
    }
    # Round-trip now, while no outcome or selected attempt exists.
    return json.loads(_canonical_bytes(capture).decode("ascii"))


def _try_causal_folds(
    *,
    frame: pd.DataFrame,
    native: Mapping[str, Any],
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    timeframe: str,
    depth_days: int,
    geometry: Mapping[str, int],
    state_context_bars: int,
) -> tuple[pd.Timestamp, list[dict[str, Any]]] | None:
    prefix_end = pd.Timestamp(
        _utc(native["prefix_end_exclusive_utc"], "PREFIX_END_INVALID")
    )
    midnight_opens = pd.DatetimeIndex([value for value in opens if _midnight(value)])
    midnight_closes = pd.DatetimeIndex([value for value in closes if _midnight(value)])
    current_end = _latest(midnight_closes, at_or_before=prefix_end)
    raw: list[dict[str, Any]] = []
    failure = current_end is None
    for _ in range(CAUSAL_FOLD_COUNT):
        if failure or current_end is None:
            failure = True
            break
        target_start = current_end - pd.Timedelta(days=CAUSAL_FOLD_MIN_DAYS)
        validation_start = _latest(midnight_opens, at_or_before=target_start)
        if validation_start is None or validation_start >= current_end:
            failure = True
            break
        validation_index = int(opens.get_indexer([validation_start])[0])
        warmup_bars = int(geometry["warmup_bars"])
        if validation_index < warmup_bars:
            failure = True
            break
        warmup_start = pd.Timestamp(opens[validation_index - warmup_bars])
        fit_end_limit = validation_start - pd.Timedelta(
            seconds=int(geometry["left_purge_seconds"])
        )
        fit_end = _latest(closes, at_or_before=fit_end_limit)
        if fit_end is None:
            failure = True
            break
        fit_start_target = fit_end - pd.Timedelta(days=depth_days)
        fit_start = _earliest(opens, at_or_after=fit_start_target)
        if fit_start is None or fit_start >= fit_end or warmup_start < fit_start:
            failure = True
            break
        fit_start_index = int(opens.get_indexer([fit_start])[0])
        if fit_start_index < state_context_bars:
            failure = True
            break
        fit_mask = (opens >= fit_start) & (opens < fit_end) & (closes <= fit_end)
        validation_mask = (
            (opens >= validation_start)
            & (opens < current_end)
            & (closes <= current_end)
        )
        fit_rows = int(fit_mask.sum())
        validation_rows = int(validation_mask.sum())
        minimum_fit_rows = search_execution.minimum_fold_fit_rows(
            recipe_warmup_bars=max(
                int(strategy.MIN_BARS)
                for strategy in StrategyRegistry.get_all_instances()
            ),
            dependency_warmup_bars=warmup_bars,
        )
        if fit_rows < minimum_fit_rows or validation_rows < 2:
            failure = True
            break
        source_slice = {
            "prefix_content_sha256": native["prefix_content_sha256"],
            "fit_start_utc": _utc_text(fit_start),
            "fit_end_exclusive_utc": _utc_text(fit_end),
            "validation_warmup_start_utc": _utc_text(warmup_start),
            "validation_start_utc": _utc_text(validation_start),
            "validation_end_exclusive_utc": _utc_text(current_end),
            "fit_rows": fit_rows,
            "validation_rows": validation_rows,
        }
        raw.append(
            {
                **source_slice,
                "source_slice_sha256": _sha(source_slice),
                "right_fit_start_utc": None,
                "right_fit_end_exclusive_utc": None,
            }
        )
        current_end = _latest(midnight_closes, at_or_before=validation_start)

    if failure or len(raw) != CAUSAL_FOLD_COUNT:
        return None

    raw.reverse()
    folds: list[dict[str, Any]] = []
    for ordinal, item in enumerate(raw):
        fold_body = {
            "fold_id": f"CAUSAL-{ordinal + 1:02d}",
            "role": "CAUSAL_ROLLING_ORIGIN_INNER",
            "fit_start_utc": item["fit_start_utc"],
            "fit_end_exclusive_utc": item["fit_end_exclusive_utc"],
            "validation_warmup_start_utc": item["validation_warmup_start_utc"],
            "validation_start_utc": item["validation_start_utc"],
            "validation_end_exclusive_utc": item["validation_end_exclusive_utc"],
            "right_fit_start_utc": None,
            "right_fit_end_exclusive_utc": None,
            "fit_rows": item["fit_rows"],
            "validation_rows": item["validation_rows"],
            "source_slice_sha256": item["source_slice_sha256"],
            "final_oos_authority": False,
        }
        folds.append({**fold_body, "fold_definition_sha256": _sha(fold_body)})
    candidate_start = min(
        pd.Timestamp(_utc(value["fit_start_utc"], "FIT_START_INVALID"))
        for value in folds
    )
    if any(
        pd.Timestamp(_utc(value["validation_warmup_start_utc"], "WARMUP_START_INVALID"))
        < candidate_start
        for value in folds
    ):
        return None
    return candidate_start, folds


def _build_causal_folds(
    *,
    frame: pd.DataFrame,
    native: Mapping[str, Any],
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    timeframe: str,
    geometry: Mapping[str, int],
    state_context_bars: int,
) -> tuple[int, pd.Timestamp, list[dict[str, Any]], bool]:
    ladder = FIT_CALENDAR_DAY_LADDERS[timeframe]
    for depth_days in reversed(ladder):
        result = _try_causal_folds(
            frame=frame,
            native=native,
            opens=opens,
            closes=closes,
            timeframe=timeframe,
            depth_days=depth_days,
            geometry=geometry,
            state_context_bars=state_context_bars,
        )
        if result is not None:
            candidate_start, folds = result
            return depth_days, candidate_start, folds, False
    prefix_start = pd.Timestamp(
        _utc(native["prefix_start_utc"], "PREFIX_START_INVALID")
    )
    # Horizon's boundary-limited state is intentionally triggered by the
    # missing bar immediately before the authenticated prefix.  No fold or
    # economic result is invented; downstream records typed statistical
    # insufficiency rather than SOURCE_UNAVAILABLE or NO_LOCAL_WINNER.
    unavailable_start = prefix_start - pd.Timedelta(
        seconds=TIMEFRAME_SECONDS[timeframe]
    )
    return ladder[0], unavailable_start, [], True


def _select_feasible_common_state_context(
    *,
    frame: pd.DataFrame,
    native: Mapping[str, Any],
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    timeframe: str,
    state_requirements: Sequence[Mapping[str, Any]],
    regime_warmup_bars: int,
) -> dict[str, Any]:
    """Select the largest common state context backed by genuine fold geometry.

    Winner comparisons require one paired fold axis.  A single extreme strategy
    must not veto lower-context siblings, so source geometry is tested at each
    exact frozen requirement from largest to smallest.  Requirements above the
    selected value remain in the multiplicity roster and are typed separately
    before evaluation.
    """

    if (
        not state_requirements
        or type(regime_warmup_bars) is not int
        or regime_warmup_bars < 1
    ):
        _fail("STATE_CONTEXT_SELECTION_INPUT_INVALID")
    contexts: list[int] = []
    for requirement in state_requirements:
        bounded = requirement.get("bounded_context_bars")
        if type(bounded) is not int or bounded < 0:
            _fail("STATE_CONTEXT_REQUIREMENT_INVALID")
        contexts.append(max(1, bounded))
    unique_descending = sorted(set(contexts), reverse=True)
    selected: dict[str, Any] | None = None
    for state_context_bars in unique_descending:
        payload = _dependency_payload(
            timeframe,
            feature_lookback_bars=state_context_bars,
            regime_warmup_bars=regime_warmup_bars,
        )
        geometry = _derived_dependency_geometry(timeframe, payload)
        depth_days, candidate_start, folds, boundary_limited = _build_causal_folds(
            frame=frame,
            native=native,
            opens=opens,
            closes=closes,
            timeframe=timeframe,
            geometry=geometry,
            state_context_bars=state_context_bars,
        )
        selected = {
            "selected_common_context_bars": state_context_bars,
            "dependency_payload": payload,
            "dependency_geometry": geometry,
            "depth_days": depth_days,
            "candidate_start": candidate_start,
            "folds": folds,
            "boundary_limited": boundary_limited,
        }
        if not boundary_limited:
            break
    if selected is None:
        _fail("STATE_CONTEXT_SELECTION_EMPTY")
    return selected


def _state_context_dispositions(
    state_requirements: Sequence[Mapping[str, Any]],
    *,
    candidate: Mapping[str, Any],
    selected_common_context_bars: int,
) -> list[dict[str, Any]]:
    if (
        not state_requirements
        or type(selected_common_context_bars) is not int
        or selected_common_context_bars < 1
    ):
        _fail("STATE_CONTEXT_DISPOSITION_INPUT_INVALID")
    candidate_id = _identity(
        candidate.get("candidate_id"),
        "STATE_CONTEXT_CANDIDATE_ID_INVALID",
    )
    candidate_sha256 = _hex64(
        candidate.get("candidate_definition_sha256"),
        "STATE_CONTEXT_CANDIDATE_SHA256_INVALID",
    )
    dispositions: list[dict[str, Any]] = []
    for requirement in state_requirements:
        requirement_sha256 = _hex64(
            requirement.get("requirement_sha256"),
            "STATE_REQUIREMENT_HASH_INVALID",
        )
        required = requirement.get("bounded_context_bars")
        if type(required) is not int or required < 0:
            _fail("STATE_CONTEXT_REQUIREMENT_INVALID")
        within = required <= selected_common_context_bars
        body = {
            "schema_version": STATE_CONTEXT_DISPOSITION_SCHEMA,
            "candidate_id": candidate_id,
            "candidate_definition_sha256": candidate_sha256,
            "state_requirement_sha256": requirement_sha256,
            "required_context_bars": required,
            "selected_context_bars": selected_common_context_bars,
            "status": "PLANNED" if within else "PROCESS_REJECTED",
            "reason_code": None if within else STATE_CONTEXT_BOUNDARY_REASON,
        }
        dispositions.append({**body, "disposition_sha256": _sha(body)})
    return dispositions


def _state_context_selection_receipt(
    state_requirements: Sequence[Mapping[str, Any]],
    dispositions: Sequence[Mapping[str, Any]],
    *,
    selected_common_context_bars: int,
    boundary_limited: bool,
) -> dict[str, Any]:
    if (
        len(state_requirements) != len(dispositions)
        or type(boundary_limited) is not bool
    ):
        _fail("STATE_CONTEXT_SELECTION_ROSTER_INVALID")
    disposition_hashes = [
        _hex64(
            value.get("disposition_sha256"),
            "STATE_CONTEXT_DISPOSITION_HASH_INVALID",
        )
        for value in dispositions
    ]
    within_count = sum(value.get("status") == "PLANNED" for value in dispositions)
    exceeds_count = sum(
        value.get("status") == "PROCESS_REJECTED" for value in dispositions
    )
    if within_count + exceeds_count != len(dispositions):
        _fail("STATE_CONTEXT_SELECTION_RELATION_INVALID")
    body = {
        "schema_version": STATE_CONTEXT_SELECTION_SCHEMA,
        "selection_rule": (
            "LARGEST_SOURCE_GEOMETRY_FEASIBLE_COMMON_CONTEXT_DESCENDING"
        ),
        "selected_common_context_bars": selected_common_context_bars,
        "candidate_boundary_limited": boundary_limited,
        "required_context_roster_sha256": _sha(
            [value.get("bounded_context_bars") for value in state_requirements]
        ),
        "disposition_sha256": disposition_hashes,
        "disposition_roster_sha256": _sha(disposition_hashes),
        "within_selected_context_count": within_count,
        "exceeds_selected_context_count": exceeds_count,
    }
    return {**body, "receipt_sha256": _sha(body)}


def _candidate(
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    depth_days: int,
    candidate_start: pd.Timestamp,
    folds: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    identity = _cell_hash(
        symbol,
        timeframe,
        regime,
        "calendar-depth",
        str(depth_days),
    )
    body = {
        "candidate_id": f"DEPTH-{timeframe}-{depth_days}-{identity[:12]}",
        "lookback_kind": "CALENDAR_INTERVAL",
        "lookback_value": depth_days,
        "realized_start_utc": _utc_text(candidate_start),
    }
    return {
        **body,
        "candidate_definition_sha256": _sha(body),
        "folds": [dict(value) for value in folds],
    }


def _anchor_body(
    request_body: Mapping[str, Any],
    *,
    ordinal: int,
    evidence_accessed_at_utc: str,
) -> dict[str, Any]:
    anchor = request_body.get("anchor")
    if type(anchor) is not dict:
        _fail("ANCHOR_REQUEST_ANCHOR_INVALID")
    if type(ordinal) is not int or not 0 <= ordinal <= 1_000_000:
        _fail("ANCHOR_ORDINAL_INVALID")
    start = date.fromisoformat(anchor["forward_start_date"])
    end = date.fromisoformat(anchor["forward_end_date"]) + timedelta(days=1)
    accessed = _utc(evidence_accessed_at_utc, "EVIDENCE_ACCESS_TIME_INVALID")
    return {
        "anchor_id": anchor["anchor_id"],
        "ordinal": ordinal,
        "selection_cutoff_utc": anchor["selection_cutoff_utc"],
        "forward_start_utc": datetime.combine(start, datetime_time.min, UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "forward_end_exclusive_utc": datetime.combine(
            end, datetime_time.min, UTC
        ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "evidence_accessed_at_utc": accessed.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "process_precommit_receipt_sha256": request_body[
            "process_precommit_receipt_sha256"
        ],
    }


def _validate_request(
    request: historical.AnchorProducerRequest,
    *,
    process: FrozenProductionSearchProcess,
    context: authority_spool.GenerationSpoolContext,
    causal_precommit_payload: Mapping[str, Any],
) -> dict[str, Any]:
    if type(request) is not historical.AnchorProducerRequest:
        _fail("ANCHOR_REQUEST_INVALID")
    body = request.to_dict()
    if not isinstance(causal_precommit_payload, Mapping):
        _fail("CAUSAL_PRECOMMIT_NOT_OBJECT")
    precommit = json.loads(
        _canonical_bytes(dict(causal_precommit_payload)).decode("ascii")
    )
    try:
        causal._validate_precommit_payload(precommit)  # noqa: SLF001
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "CAUSAL_PRECOMMIT_RAW_REVERIFY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    definition = precommit.get("process_definition")
    policies = definition.get("policy_identities") if type(definition) is dict else None
    causal_sha256 = causal.canonical_sha256(precommit)
    request_schema = body.get("schema_version")
    planning_only = request_schema == historical.PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA
    if request_schema not in {
        historical.PRODUCER_REQUEST_SCHEMA,
        historical.PRE_EVIDENCE_PLANNING_REQUEST_SCHEMA,
    }:
        _fail("ANCHOR_REQUEST_PROCESS_BINDING_INVALID")
    if planning_only:
        expected_fields = {
            "schema_version",
            "authority_scope",
            "run_id",
            "generation_id",
            "native_universe_sha256",
            "config_sha256",
            "generation_context_sha256",
            "historical_anchor_precommit_pending",
            "process_precommit_receipt_sha256",
            "optimizer_producer_release_sha256",
            "anchor",
            "symbols",
            "timeframes",
            "regimes",
            "requires_exact_sparse_state_roster",
            "possible_regime_slots_are_forced_observations",
            "planning_access_limited_to_native_prefix_and_process_design",
            "return_or_winner_evidence_access_authorized",
            "confers_causal_or_current_authority",
        }
        if (
            set(body) != expected_fields
            or body.get("native_universe_sha256") != context.native_universe_sha256
            or body.get("config_sha256") != context.config_sha256
            or body.get("generation_context_sha256") != context.context_sha256
            or body.get("historical_anchor_precommit_pending") is not True
            or body.get("planning_access_limited_to_native_prefix_and_process_design")
            is not True
            or body.get("return_or_winner_evidence_access_authorized") is not False
            or "precommit_receipt_sha256" in body
        ):
            _fail("ANCHOR_PRE_EVIDENCE_PLANNING_BINDING_INVALID")
    else:
        _hex64(
            body.get("precommit_receipt_sha256"),
            "ANCHOR_PRECOMMIT_RECEIPT_INVALID",
        )
    if (
        body.get("authority_scope") != historical.AUTHORITY_SCOPE
        or body.get("generation_id") != context.generation_id
        or body.get("run_id") != process.to_dict()["run_id"]
        or body.get("process_precommit_receipt_sha256") != causal_sha256
        or type(policies) is not dict
        or policies.get("trial_registry_policy_sha256") != process.receipt_sha256
        or body.get("optimizer_producer_release_sha256")
        != historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256
        or body.get("symbols") != list(book.PINNED_SYMBOL_ORDER)
        or body.get("timeframes") != list(TIMEFRAMES)
        or body.get("regimes") != list(REGIMES)
        or body.get("requires_exact_sparse_state_roster") is not True
        or body.get("possible_regime_slots_are_forced_observations") is not False
        or body.get("confers_causal_or_current_authority") is not False
    ):
        _fail("ANCHOR_REQUEST_PROCESS_BINDING_INVALID")
    anchor = body.get("anchor")
    causal_anchors = definition.get("anchor_plan") if type(definition) is dict else None
    if (
        type(anchor) is not dict
        or type(causal_anchors) is not list
        or len(
            [
                value
                for value in causal_anchors
                if type(value) is dict
                and value.get("anchor_id") == anchor.get("anchor_id")
                and value.get("selection_cutoff_utc")
                == anchor.get("selection_cutoff_utc")
                and value.get("forward_start_date") == anchor.get("forward_start_date")
                and value.get("forward_end_date") == anchor.get("forward_end_date")
            ]
        )
        != 1
    ):
        _fail("ANCHOR_REQUEST_CAUSAL_GEOMETRY_MISMATCH")
    return body


def _build_terminal_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    product: native_prefix.NativePrefixProduct,
    regime: str,
    anchor: Mapping[str, Any],
    run_id: str,
    causal_precommit_receipt_sha256: str,
    namespace: str,
    sink: authority_spool.SymbolAuthoritySink,
) -> ProductionCellSearchPlan:
    verify_frozen_production_search_process(context, process)
    normalized_regime = _identity(regime, "REGIME_INVALID").upper()
    if normalized_regime not in REGIMES:
        _fail("REGIME_INVALID")
    reopened = _reverify_terminal_product(context, product=product, anchor=anchor)
    native = dict(reopened.native_prefix)
    symbol = str(native.get("symbol") or "")
    timeframe = str(native.get("timeframe") or "")
    if symbol not in book.PINNED_SYMBOL_ORDER or timeframe not in TIMEFRAMES:
        _fail("CELL_OUTSIDE_PINNED_ROSTER")
    payload, dependency_policy, horizon_policy = _terminal_dependency_and_policy(
        process=process,
        causal_precommit_receipt_sha256=causal_precommit_receipt_sha256,
        product=reopened,
    )
    request_value = {
        "schema_version": "fxpm.historical-horizon-request.v1",
        "run_id": run_id,
        "anchor": dict(anchor),
        "cell": {
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": normalized_regime,
        },
        "native_prefix": native,
        "dependency_policy": dependency_policy,
        "horizon_policy": horizon_policy,
        "candidates": [],
        "trials": [],
    }
    try:
        horizon_plan = training_horizon.build_historical_horizon_plan(request_value)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "TERMINAL_HORIZON_PLAN_BUILD_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    plan_body = horizon_plan.to_dict()
    expected_status = (
        "HISTORICAL_SOURCE_UNAVAILABLE"
        if reopened.source_state == native_prefix.SOURCE_UNAVAILABLE
        else "HISTORICAL_TERMINAL_TECHNICAL"
    )
    if (
        plan_body.get("plan_status") != expected_status
        or plan_body.get("native_prefix") != native
        or plan_body.get("candidates") != []
        or plan_body.get("trials") != []
        or plan_body.get("attempts") != []
        or plan_body.get("expected_evaluations") != []
        or plan_body.get("anchor", {}).get("process_precommit_receipt_sha256")
        != causal_precommit_receipt_sha256
    ):
        _fail("TERMINAL_HORIZON_PLAN_OUTPUT_BINDING_INVALID")
    receipt_json, entry_json = _seal_terminal_cell_plan(
        context,
        sink=sink,
        namespace=namespace,
        process=process,
        causal_precommit_receipt_sha256=causal_precommit_receipt_sha256,
        product=reopened,
        regime=normalized_regime,
        horizon_plan=horizon_plan,
        dependency_payload=payload,
    )
    result = ProductionCellSearchPlan(
        mode="TERMINAL",
        symbol=symbol,
        timeframe=timeframe,
        regime=normalized_regime,
        causal_precommit_receipt_sha256=causal_precommit_receipt_sha256,
        search_process_receipt_sha256=process.receipt_sha256,
        horizon_plan=horizon_plan,
        _attempt_artifacts_json=(),
        _state_requirements_json=(),
        _cell_receipt_json=receipt_json,
        _spool_entry_json=entry_json,
        boundary_limited=False,
        _seal=_CELL_PLAN_SEAL,
        native_prefix_product=reopened,
    )
    verify_production_cell_search_plan(
        context,
        process=process,
        sink=sink,
        plan=result,
    )
    return result


def build_historical_terminal_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    request: historical.AnchorProducerRequest,
    causal_precommit_payload: Mapping[str, Any],
    anchor_ordinal: int,
    product: native_prefix.NativePrefixProduct,
    regime: str,
) -> ProductionCellSearchPlan:
    """Freeze one authentic source-terminal historical cell with zero attempts."""

    verify_frozen_production_search_process(context, process)
    request_body = _validate_request(
        request,
        process=process,
        context=context,
        causal_precommit_payload=causal_precommit_payload,
    )
    anchor = _anchor_body(
        request_body,
        ordinal=anchor_ordinal,
        evidence_accessed_at_utc=str(
            causal_precommit_payload["evidence_access_deadline_utc"]
        ),
    )
    native = dict(product.native_prefix)
    symbol = str(native.get("symbol") or "")
    namespace = (
        f"historical-anchor/{request_body['run_id']}/"
        f"{request_body['anchor']['anchor_id']}"
    )
    return _build_terminal_cell_plan(
        context,
        process=process,
        product=product,
        regime=regime,
        anchor=anchor,
        run_id=request_body["run_id"],
        causal_precommit_receipt_sha256=request_body[
            "process_precommit_receipt_sha256"
        ],
        namespace=namespace,
        sink=request.symbol_sink(symbol),
    )


def build_historical_terminal_timeframe_plans(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    request: historical.AnchorProducerRequest,
    causal_precommit_payload: Mapping[str, Any],
    anchor_ordinal: int,
    product: native_prefix.NativePrefixProduct,
) -> tuple[ProductionCellSearchPlan, ...]:
    """Freeze the exact four-regime terminal roster required by capture."""

    return tuple(
        build_historical_terminal_cell_plan(
            context,
            process=process,
            request=request,
            causal_precommit_payload=causal_precommit_payload,
            anchor_ordinal=anchor_ordinal,
            product=product,
            regime=regime,
        )
        for regime in REGIMES
    )


def _build_unobserved_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    fitted: search_execution.FittedNativeSearchTimeframe,
    regime: str,
    anchor: Mapping[str, Any],
    run_id: str,
    causal_precommit_receipt_sha256: str,
    mode: str,
    namespace: str,
    sink: authority_spool.SymbolAuthoritySink,
) -> ProductionCellSearchPlan:
    verify_frozen_production_search_process(context, process)
    normalized_regime = _identity(regime, "REGIME_INVALID").upper()
    if normalized_regime not in REGIMES:
        _fail("REGIME_INVALID")
    _, native, _, _ = _validated_frame(fitted)
    symbol = str(native.get("symbol") or "")
    timeframe = str(native.get("timeframe") or "")
    if symbol not in book.PINNED_SYMBOL_ORDER or timeframe not in TIMEFRAMES:
        _fail("CELL_OUTSIDE_PINNED_ROSTER")
    _validate_available_prefix_anchor_binding(
        native=native,
        prefix_index=fitted.product.index,
        anchor=anchor,
        canary_request_body=None,
    )
    support_binding = _regime_support_binding(fitted, regime=normalized_regime)
    payload = _dependency_payload(
        timeframe,
        feature_lookback_bars=1,
        regime_warmup_bars=1,
    )
    payload_sha = _sha(payload)
    dependency_receipt = _sha(
        {
            "schema_version": "fxpm.production-dependency-policy-binding.v1",
            "causal_process_precommit_receipt_sha256": (
                causal_precommit_receipt_sha256
            ),
            "search_process_receipt_sha256": process.receipt_sha256,
            "timeframe": timeframe,
            "payload_sha256": payload_sha,
            "regime_support_binding_sha256": support_binding["binding_sha256"],
            "economic_evaluation": False,
        }
    )
    dependency_policy = {
        "schema_version": "fxpm.historical-dependency-policy.v1",
        "authority_key_id": "fxpm-production-search-process-v1",
        "signed_policy_receipt_sha256": dependency_receipt,
        "payload_sha256": payload_sha,
        "payload": payload,
    }
    horizon_policy = {
        "schema_version": "fxpm.historical-horizon-policy.v1",
        "policy_id": f"fxpm-production-unobserved-{timeframe}-v1",
        "policy_release_sha256": PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256,
        "cost_contract_sha256": _sha(dict(fitted.base_cost_policy)),
        "cost_stress_contract_sha256": _sha(dict(fitted.stress_cost_policy)),
        "evaluator_release_sha256": (
            search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
        ),
    }
    request_value = {
        "schema_version": "fxpm.historical-horizon-unobserved-request.v1",
        "run_id": run_id,
        "anchor": dict(anchor),
        "cell": {
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": normalized_regime,
        },
        "native_prefix": native,
        "dependency_policy": dependency_policy,
        "horizon_policy": horizon_policy,
        "regime_support_binding": support_binding,
        "candidates": [],
        "trials": [],
    }
    try:
        horizon_plan = training_horizon.build_historical_horizon_plan(request_value)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "UNOBSERVED_HORIZON_PLAN_BUILD_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    plan_body = horizon_plan.to_dict()
    if (
        plan_body.get("plan_status") != "HISTORICAL_REGIME_UNOBSERVED"
        or plan_body.get("native_prefix") != native
        or plan_body.get("regime_support_binding") != support_binding
        or any(
            plan_body.get(name) != []
            for name in ("candidates", "trials", "attempts", "expected_evaluations")
        )
    ):
        _fail("UNOBSERVED_HORIZON_PLAN_OUTPUT_BINDING_INVALID")
    dependency_geometry = _derived_dependency_geometry(timeframe, payload)
    receipt_json, entry_json = _seal_unobserved_cell_plan(
        context,
        sink=sink,
        mode=mode,
        namespace=namespace,
        process=process,
        causal_precommit_receipt_sha256=causal_precommit_receipt_sha256,
        fitted=fitted,
        regime=normalized_regime,
        horizon_plan=horizon_plan,
        support_binding=support_binding,
        dependency_payload=payload,
        dependency_geometry=dependency_geometry,
    )
    result = ProductionCellSearchPlan(
        mode=mode,
        symbol=symbol,
        timeframe=timeframe,
        regime=normalized_regime,
        causal_precommit_receipt_sha256=causal_precommit_receipt_sha256,
        search_process_receipt_sha256=process.receipt_sha256,
        horizon_plan=horizon_plan,
        _attempt_artifacts_json=(),
        _state_requirements_json=(),
        _cell_receipt_json=receipt_json,
        _spool_entry_json=entry_json,
        boundary_limited=False,
        _seal=_CELL_PLAN_SEAL,
        native_prefix_product=fitted.product,
        fitted_timeframe=fitted,
    )
    verify_production_cell_search_plan(context, process=process, sink=sink, plan=result)
    return result


def build_historical_unobserved_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    request: historical.AnchorProducerRequest,
    causal_precommit_payload: Mapping[str, Any],
    anchor_ordinal: int,
    fitted: search_execution.FittedNativeSearchTimeframe,
    regime: str,
) -> ProductionCellSearchPlan:
    """Freeze a zero-attempt plan from the authenticated train support."""

    verify_frozen_production_search_process(context, process)
    request_body = _validate_request(
        request,
        process=process,
        context=context,
        causal_precommit_payload=causal_precommit_payload,
    )
    anchor = _anchor_body(
        request_body,
        ordinal=anchor_ordinal,
        evidence_accessed_at_utc=str(
            causal_precommit_payload["evidence_access_deadline_utc"]
        ),
    )
    native = dict(fitted.product.native_prefix)
    namespace = (
        f"historical-anchor/{request_body['run_id']}/"
        f"{request_body['anchor']['anchor_id']}"
    )
    return _build_unobserved_cell_plan(
        context,
        process=process,
        fitted=fitted,
        regime=regime,
        anchor=anchor,
        run_id=request_body["run_id"],
        causal_precommit_receipt_sha256=request_body[
            "process_precommit_receipt_sha256"
        ],
        mode="HISTORICAL_REGIME_UNOBSERVED",
        namespace=namespace,
        sink=request.symbol_sink(str(native.get("symbol") or "")),
    )


def build_historical_observed_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    request: historical.AnchorProducerRequest,
    causal_precommit_payload: Mapping[str, Any],
    anchor_ordinal: int,
    fitted: search_execution.FittedNativeSearchTimeframe,
    regime: str,
) -> ProductionCellSearchPlan:
    """Build and freeze one genuine historical observed-cell plan."""

    verify_frozen_production_search_process(context, process)
    request_body = _validate_request(
        request,
        process=process,
        context=context,
        causal_precommit_payload=causal_precommit_payload,
    )
    anchor = _anchor_body(
        request_body,
        ordinal=anchor_ordinal,
        evidence_accessed_at_utc=str(
            causal_precommit_payload["evidence_access_deadline_utc"]
        ),
    )
    namespace = (
        f"historical-anchor/{request_body['run_id']}/"
        f"{request_body['anchor']['anchor_id']}"
    )
    native = dict(fitted.product.native_prefix)
    symbol = str(native.get("symbol") or "")
    return _build_observed_cell_plan(
        context,
        process=process,
        fitted=fitted,
        regime=regime,
        anchor=anchor,
        run_id=request_body["run_id"],
        causal_precommit_receipt_sha256=request_body[
            "process_precommit_receipt_sha256"
        ],
        mode="HISTORICAL_ANCHOR",
        namespace=namespace,
        sink=request.symbol_sink(symbol),
    )


def build_bounded_raw_winner_canary_observed_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    request: historical.BoundedRawWinnerCanaryRequest,
    causal_precommit_payload: Mapping[str, Any],
    source_session: Any,
    plan_permit: Any,
) -> ProductionCellSearchPlan:
    """Freeze the one raw-only cell from executor-authenticated source state."""

    verify_frozen_production_search_process(context, process)
    request_body = historical.verify_bounded_raw_winner_canary_request(context, request)
    if not isinstance(causal_precommit_payload, Mapping):
        _fail("CANARY_CAUSAL_PRECOMMIT_NOT_OBJECT")
    precommit = json.loads(
        _canonical_bytes(dict(causal_precommit_payload)).decode("ascii")
    )
    try:
        causal._validate_precommit_payload(precommit)  # noqa: SLF001
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "CANARY_CAUSAL_PRECOMMIT_RAW_REVERIFY_FAILED",
            (type(exc).__name__, str(exc)),
        ) from exc
    definition = precommit.get("process_definition")
    policies = definition.get("policy_identities") if type(definition) is dict else None
    anchors = definition.get("anchor_plan") if type(definition) is dict else None
    if (
        causal.canonical_sha256(precommit)
        != request_body["causal_precommit_receipt_sha256"]
        or request_body["search_process_receipt_sha256"] != process.receipt_sha256
        or type(policies) is not dict
        or policies.get("trial_registry_policy_sha256") != process.receipt_sha256
        or type(anchors) is not list
        or len(anchors) != request_body["anchor_count"]
        or not anchors
        or anchors[-1] != request_body["anchor"]
        or precommit.get("evidence_access_deadline_utc")
        != request_body["causal_evidence_access_deadline_utc"]
    ):
        _fail("CANARY_CAUSAL_PROCESS_BINDING_INVALID")

    if request_body["regime_selection_rule"] != (
        historical.BOUNDED_RAW_WINNER_CANARY_SELECTION_RULE
    ):
        _fail("CANARY_REGIME_SELECTION_RULE_INVALID")

    anchor_request = {
        "anchor": request_body["anchor"],
        "process_precommit_receipt_sha256": request_body[
            "causal_precommit_receipt_sha256"
        ],
    }
    anchor = _anchor_body(
        anchor_request,
        ordinal=request_body["anchor_count"] - 1,
        evidence_accessed_at_utc=request_body["causal_evidence_access_deadline_utc"],
    )
    result = _build_observed_cell_plan(
        context,
        process=process,
        fitted=None,
        regime=None,
        anchor=anchor,
        run_id=request_body["run_id"],
        causal_precommit_receipt_sha256=request_body["causal_precommit_receipt_sha256"],
        mode="BOUNDED_RAW_WINNER_CANARY",
        namespace=request_body["artifact_namespace"],
        sink=request.symbol_sink(),
        bounded_canary_request=request,
        bounded_canary_source_session=source_session,
        bounded_canary_plan_permit=plan_permit,
    )
    if (
        result.symbol != request_body["symbol"]
        or result.timeframe != request_body["timeframe"]
        or result.attempt_count != request_body["attempt_count_exact"]
        or result.mode != "BOUNDED_RAW_WINNER_CANARY"
    ):
        _fail("CANARY_CELL_PLAN_OUTPUT_BINDING_INVALID")
    return result


def build_current_observed_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    authorization: CurrentSearchAuthorization,
    sink: authority_spool.SymbolAuthoritySink,
    fitted: search_execution.FittedNativeSearchTimeframe,
    regime: str,
) -> ProductionCellSearchPlan:
    """Build the same frozen process at the latest authorized closed cutoff."""

    verify_current_search_authorization(
        context, process=process, authorization=authorization
    )
    if type(sink) is not authority_spool.SymbolAuthoritySink:
        _fail("CURRENT_CELL_SINK_INVALID")
    auth = authorization.to_dict()
    native = dict(fitted.product.native_prefix)
    if (
        _available_prefix_requested_cutoff(fitted.product.index)
        != auth["current_cutoff_utc"]
    ):
        _fail("CURRENT_PREFIX_CUTOFF_MISMATCH")
    cutoff = _utc(auth["current_cutoff_utc"], "CURRENT_CUTOFF_INVALID")
    first_forward_date = cutoff.date() + timedelta(days=1)
    forward_start = datetime.combine(first_forward_date, datetime_time.min, UTC)
    if forward_start <= cutoff:
        forward_start += timedelta(days=1)
    forward_end = forward_start + timedelta(days=1)
    compact_cutoff = cutoff.strftime("%Y%m%dT%H%M%SZ")
    anchor = {
        "anchor_id": f"CURRENT-{compact_cutoff}",
        "ordinal": 1_000_000,
        "selection_cutoff_utc": auth["current_cutoff_utc"],
        "forward_start_utc": forward_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forward_end_exclusive_utc": forward_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "evidence_accessed_at_utc": native["source_accessed_at_utc"],
        "process_precommit_receipt_sha256": auth["causal_precommit_receipt_sha256"],
    }
    namespace = f"current-refit/{auth['run_id']}/{compact_cutoff}"
    return _build_observed_cell_plan(
        context,
        process=process,
        fitted=fitted,
        regime=regime,
        anchor=anchor,
        run_id=auth["run_id"],
        causal_precommit_receipt_sha256=auth["causal_precommit_receipt_sha256"],
        mode="CURRENT_REFIT",
        namespace=namespace,
        sink=sink,
    )


def build_current_terminal_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    authorization: CurrentSearchAuthorization,
    sink: authority_spool.SymbolAuthoritySink,
    product: native_prefix.NativePrefixProduct,
    regime: str,
) -> ProductionCellSearchPlan:
    """Freeze one latest-cutoff terminal cell without creating search work."""

    verify_current_search_authorization(
        context, process=process, authorization=authorization
    )
    if type(sink) is not authority_spool.SymbolAuthoritySink:
        _fail("CURRENT_CELL_SINK_INVALID")
    auth = authorization.to_dict()
    if (
        type(product) is not native_prefix.NativePrefixProduct
        or type(product.index) is not dict
        or product.index.get("cutoff_utc") != auth["current_cutoff_utc"]
    ):
        _fail("CURRENT_TERMINAL_PREFIX_CUTOFF_MISMATCH")
    native = dict(product.native_prefix)
    cutoff = _utc(auth["current_cutoff_utc"], "CURRENT_CUTOFF_INVALID")
    first_forward_date = cutoff.date() + timedelta(days=1)
    forward_start = datetime.combine(first_forward_date, datetime_time.min, UTC)
    if forward_start <= cutoff:
        forward_start += timedelta(days=1)
    forward_end = forward_start + timedelta(days=1)
    compact_cutoff = cutoff.strftime("%Y%m%dT%H%M%SZ")
    anchor = {
        "anchor_id": f"CURRENT-{compact_cutoff}",
        "ordinal": 1_000_000,
        "selection_cutoff_utc": auth["current_cutoff_utc"],
        "forward_start_utc": forward_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forward_end_exclusive_utc": forward_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "evidence_accessed_at_utc": native["source_accessed_at_utc"],
        "process_precommit_receipt_sha256": auth["causal_precommit_receipt_sha256"],
    }
    namespace = f"current-refit/{auth['run_id']}/{compact_cutoff}"
    return _build_terminal_cell_plan(
        context,
        process=process,
        product=product,
        regime=regime,
        anchor=anchor,
        run_id=auth["run_id"],
        causal_precommit_receipt_sha256=auth["causal_precommit_receipt_sha256"],
        namespace=namespace,
        sink=sink,
    )


def build_current_unobserved_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    authorization: CurrentSearchAuthorization,
    sink: authority_spool.SymbolAuthoritySink,
    fitted: search_execution.FittedNativeSearchTimeframe,
    regime: str,
) -> ProductionCellSearchPlan:
    """Freeze one latest-cutoff zero-support cell with no search attempts."""

    verify_current_search_authorization(
        context, process=process, authorization=authorization
    )
    if type(sink) is not authority_spool.SymbolAuthoritySink:
        _fail("CURRENT_CELL_SINK_INVALID")
    auth = authorization.to_dict()
    native = dict(fitted.product.native_prefix)
    if (
        _available_prefix_requested_cutoff(fitted.product.index)
        != auth["current_cutoff_utc"]
    ):
        _fail("CURRENT_PREFIX_CUTOFF_MISMATCH")
    cutoff = _utc(auth["current_cutoff_utc"], "CURRENT_CUTOFF_INVALID")
    first_forward_date = cutoff.date() + timedelta(days=1)
    forward_start = datetime.combine(first_forward_date, datetime_time.min, UTC)
    if forward_start <= cutoff:
        forward_start += timedelta(days=1)
    forward_end = forward_start + timedelta(days=1)
    compact_cutoff = cutoff.strftime("%Y%m%dT%H%M%SZ")
    anchor = {
        "anchor_id": f"CURRENT-{compact_cutoff}",
        "ordinal": 1_000_000,
        "selection_cutoff_utc": auth["current_cutoff_utc"],
        "forward_start_utc": forward_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forward_end_exclusive_utc": forward_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "evidence_accessed_at_utc": native["source_accessed_at_utc"],
        "process_precommit_receipt_sha256": auth["causal_precommit_receipt_sha256"],
    }
    namespace = f"current-refit/{auth['run_id']}/{compact_cutoff}"
    return _build_unobserved_cell_plan(
        context,
        process=process,
        fitted=fitted,
        regime=regime,
        anchor=anchor,
        run_id=auth["run_id"],
        causal_precommit_receipt_sha256=auth["causal_precommit_receipt_sha256"],
        mode="CURRENT_REGIME_UNOBSERVED",
        namespace=namespace,
        sink=sink,
    )


def _build_observed_cell_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    fitted: search_execution.FittedNativeSearchTimeframe | None,
    regime: str | None,
    anchor: Mapping[str, Any],
    run_id: str,
    causal_precommit_receipt_sha256: str,
    mode: str,
    namespace: str,
    sink: authority_spool.SymbolAuthoritySink,
    bounded_canary_request: Any = None,
    bounded_canary_source_session: Any = None,
    bounded_canary_plan_permit: Any = None,
) -> ProductionCellSearchPlan:
    """Build one sealed cell plan after its outer authority is verified."""

    verify_frozen_production_search_process(context, process)
    normalized_mode = _identity(mode, "CELL_PLAN_MODE_INVALID").upper()
    canary_request_body: dict[str, Any] | None = None
    if normalized_mode == "BOUNDED_RAW_WINNER_CANARY":
        if fitted is not None or regime is not None:
            _fail("CANARY_CALLER_FITTED_OR_REGIME_FORBIDDEN")
        (
            canary_request_body,
            verified_source_session,
            _verified_pipeline_config,
            _verified_frozen_pipeline_config,
        ) = search_execution._consume_bounded_raw_winner_canary_plan_permit(
            context,
            request=bounded_canary_request,
            process=process,
            source_session=bounded_canary_source_session,
            plan_permit=bounded_canary_plan_permit,
        )
        fitted = verified_source_session._fitted
        support = fitted.train_regime_support
        counts = support.get("counts") if type(support) is dict else None
        if (
            type(counts) is not dict
            or set(counts) != set(REGIMES)
            or any(
                type(counts[label]) is not int
                or type(counts[label]) is bool
                or counts[label] < 0
                for label in REGIMES
            )
        ):
            _fail("CANARY_REGIME_SUPPORT_COUNTS_INVALID")
        observed = [label for label in REGIMES if counts[label] > 0]
        if (
            not observed
            or support.get("label_order") != list(REGIMES)
            or support.get("observed_regimes") != observed
            or support.get("unobserved_regimes")
            != [label for label in REGIMES if counts[label] == 0]
        ):
            _fail("CANARY_REGIME_SUPPORT_ORDER_INVALID")
        normalized_regime = observed[0]
        anchor = dict(anchor)
        anchor["evidence_accessed_at_utc"] = verified_source_session.to_dict()[
            "source_accessed_at_utc"
        ]
    else:
        if any(
            value is not None
            for value in (
                bounded_canary_request,
                bounded_canary_source_session,
                bounded_canary_plan_permit,
            )
        ):
            _fail("NONCANARY_CELL_PLAN_CAPABILITY_FORBIDDEN")
        normalized_regime = _identity(regime, "REGIME_INVALID").upper()
    if normalized_regime not in REGIMES:
        _fail("REGIME_INVALID")
    if type(fitted) is not search_execution.FittedNativeSearchTimeframe:
        _fail("FITTED_TIMEFRAME_INVALID")
    frame, native, opens, closes = _validated_frame(fitted)
    symbol = str(native.get("symbol") or "")
    timeframe = str(native.get("timeframe") or "")
    if symbol not in book.PINNED_SYMBOL_ORDER or timeframe not in TIMEFRAMES:
        _fail("CELL_OUTSIDE_PINNED_ROSTER")
    _validate_available_prefix_anchor_binding(
        native=native,
        prefix_index=fitted.product.index,
        anchor=anchor,
        canary_request_body=canary_request_body,
    )
    support = fitted.train_regime_support
    if normalized_regime not in support.get("observed_regimes", []):
        _fail("REGIME_NOT_EMPIRICALLY_OBSERVED_USE_TERMINAL_PATH")

    process_body = process.to_dict()
    designs = _attempt_designs(
        process_body,
        symbol=symbol,
        timeframe=timeframe,
        regime=normalized_regime,
    )
    state_requirements, regime_warmup_bars = _state_requirements(
        fitted=fitted,
        frame=frame,
        native=native,
        designs=designs,
    )
    state_selection = _select_feasible_common_state_context(
        frame=frame,
        native=native,
        opens=opens,
        closes=closes,
        timeframe=timeframe,
        state_requirements=state_requirements,
        regime_warmup_bars=regime_warmup_bars,
    )
    state_context_bars = state_selection["selected_common_context_bars"]
    payload = state_selection["dependency_payload"]
    dependency_geometry = state_selection["dependency_geometry"]
    depth_days = state_selection["depth_days"]
    candidate_start = state_selection["candidate_start"]
    folds = state_selection["folds"]
    boundary_limited = state_selection["boundary_limited"]
    candidate = _candidate(
        symbol=symbol,
        timeframe=timeframe,
        regime=normalized_regime,
        depth_days=depth_days,
        candidate_start=candidate_start,
        folds=folds,
    )
    state_dispositions = _state_context_dispositions(
        state_requirements,
        candidate=candidate,
        selected_common_context_bars=state_context_bars,
    )
    state_context_selection = _state_context_selection_receipt(
        state_requirements,
        state_dispositions,
        selected_common_context_bars=state_context_bars,
        boundary_limited=boundary_limited,
    )
    fit_end = pd.Timestamp(
        _utc(native["prefix_end_exclusive_utc"], "PREFIX_END_INVALID")
    )
    current_fit_start, source_count = _cutoff_anchored_refit_window(
        opens=opens,
        closes=closes,
        fit_end=fit_end,
        depth_days=depth_days,
        state_context_bars=state_context_bars,
        boundary_limited=boundary_limited,
    )
    if source_count < max(
        2,
        max(
            StrategyRegistry.get(name).MIN_BARS for name in StrategyRegistry.list_all()
        ),
    ):
        _fail("ATTEMPT_SOURCE_PREFIX_TOO_SHORT")

    current_source_capture: dict[str, Any] | None = None
    artifact_source_sha256 = str(native["prefix_content_sha256"])
    artifact_fit_start_utc = _utc_text(current_fit_start)
    artifact_fit_end_utc = str(native["prefix_end_exclusive_utc"])
    artifact_latest_closed_bar_utc = str(native["prefix_last_close_utc"])
    if mode == "CURRENT_REFIT":
        current_source_capture = _write_current_refit_source_capture(
            context,
            sink=sink,
            namespace=namespace,
            fitted=fitted,
            frame=frame,
            native=native,
            opens=opens,
            closes=closes,
            current_fit_start=current_fit_start,
            fit_end=fit_end,
            expected_source_count=source_count,
        )
        current_source = current_source_capture["source_snapshot"]
        artifact_source_sha256 = current_refit.canonical_artifact_sha256(current_source)
        artifact_fit_start_utc = current_source["fit_start_utc"]
        artifact_fit_end_utc = current_source["fit_end_utc"]
        artifact_latest_closed_bar_utc = current_source["latest_closed_bar_utc"]

    artifacts: list[dict[str, Any]] = []
    trials: list[dict[str, Any]] = []
    for ordinal, (design, state_disposition) in enumerate(
        zip(designs, state_dispositions, strict=True)
    ):
        seed_digest = _cell_hash(
            symbol,
            timeframe,
            normalized_regime,
            design["strategy_id"],
            _sha(design["parameters"]),
            "refit-seed",
        )
        seed = int(seed_digest[:8], 16) & 0x7FFFFFFF
        built = current_refit.build_current_refit_search_artifacts_v2(
            symbol=symbol,
            timeframe=timeframe,
            regime=normalized_regime,
            strategy_id=design["strategy_id"],
            parameters=design["parameters"],
            lookback_kind=candidate["lookback_kind"],
            lookback_value=candidate["lookback_value"],
            fit_start_utc=artifact_fit_start_utc,
            fit_end_utc=artifact_fit_end_utc,
            latest_closed_bar_utc=artifact_latest_closed_bar_utc,
            source_snapshot_sha256=artifact_source_sha256,
            source_bar_count=source_count,
            seed=seed,
        )
        raw = {
            "winner_process": built.winner_process,
            "horizon_process": built.horizon_process,
            "trial_definition": built.trial_definition,
            "refit_recipe": built.refit_recipe,
        }
        artifacts.append(raw)
        parameter_sha = _sha(built.trial_definition["parameters"])
        trial_identity = {
            "trial_id": f"TRIAL-{ordinal:03d}-{parameter_sha[:16]}",
            "strategy_id": design["strategy_id"],
            "strategy_release_sha256": built.winner_process["strategy_code_sha256"],
            "parameter_set_sha256": parameter_sha,
            "state_context_disposition": dict(state_disposition),
        }
        trials.append(
            {
                **trial_identity,
                "trial_definition_sha256": _sha(trial_identity),
            }
        )

    payload_sha = _sha(payload)
    dependency_receipt = _sha(
        {
            "schema_version": "fxpm.production-dependency-policy-binding.v1",
            "causal_process_precommit_receipt_sha256": (
                causal_precommit_receipt_sha256
            ),
            "search_process_receipt_sha256": process.receipt_sha256,
            "timeframe": timeframe,
            "payload_sha256": payload_sha,
        }
    )
    request_value = {
        "schema_version": "fxpm.historical-horizon-request.v1",
        "run_id": run_id,
        "anchor": anchor,
        "cell": {
            "symbol": symbol,
            "timeframe": timeframe,
            "regime": normalized_regime,
        },
        "native_prefix": native,
        "dependency_policy": {
            "schema_version": "fxpm.historical-dependency-policy.v1",
            "authority_key_id": "fxpm-production-search-process-v1",
            "signed_policy_receipt_sha256": dependency_receipt,
            "payload_sha256": payload_sha,
            "payload": payload,
        },
        "horizon_policy": {
            "schema_version": "fxpm.historical-horizon-policy.v1",
            "policy_id": f"fxpm-production-horizon-{timeframe}-v1",
            "policy_release_sha256": PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256,
            "cost_contract_sha256": _sha(dict(fitted.base_cost_policy)),
            "cost_stress_contract_sha256": _sha(dict(fitted.stress_cost_policy)),
            "evaluator_release_sha256": (
                search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
            ),
        },
        "candidates": [candidate],
        "trials": trials,
    }
    try:
        horizon_plan = training_horizon.build_historical_horizon_plan(request_value)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "HORIZON_PLAN_BUILD_FAILED", (type(exc).__name__, str(exc))
        ) from exc
    plan_body = horizon_plan.to_dict()
    expected_status = (
        "HISTORICAL_BOUNDARY_LIMITED"
        if boundary_limited
        else "READY_FOR_HISTORICAL_EVIDENCE"
    )
    if (
        plan_body.get("plan_status") != expected_status
        or len(plan_body.get("attempts", [])) != ATTEMPT_COUNT
        or plan_body.get("anchor", {}).get("process_precommit_receipt_sha256")
        != causal_precommit_receipt_sha256
    ):
        _fail("HORIZON_PLAN_OUTPUT_BINDING_INVALID")
    encoded_artifacts = tuple(_canonical_bytes(value) for value in artifacts)
    receipt_json, entry_json = _seal_cell_plan(
        context,
        sink=sink,
        mode=mode,
        namespace=namespace,
        process=process,
        causal_precommit_receipt_sha256=causal_precommit_receipt_sha256,
        fitted=fitted,
        regime=normalized_regime,
        horizon_plan=horizon_plan,
        candidate=candidate,
        dependency_payload=payload,
        dependency_geometry=dependency_geometry,
        attempt_artifacts=artifacts,
        state_requirements=state_requirements,
        state_context_selection=state_context_selection,
        boundary_limited=boundary_limited,
        current_refit_source_capture_sha256=(
            _sha(current_source_capture) if current_source_capture is not None else None
        ),
        bounded_canary_request=bounded_canary_request,
        bounded_canary_source_session=bounded_canary_source_session,
        bounded_canary_plan_permit=bounded_canary_plan_permit,
    )
    result = ProductionCellSearchPlan(
        mode=mode,
        symbol=symbol,
        timeframe=timeframe,
        regime=normalized_regime,
        causal_precommit_receipt_sha256=causal_precommit_receipt_sha256,
        search_process_receipt_sha256=process.receipt_sha256,
        horizon_plan=horizon_plan,
        _attempt_artifacts_json=encoded_artifacts,
        _state_requirements_json=tuple(
            _canonical_bytes(value) for value in state_requirements
        ),
        _cell_receipt_json=receipt_json,
        _spool_entry_json=entry_json,
        boundary_limited=boundary_limited,
        _seal=_CELL_PLAN_SEAL,
        _current_refit_source_capture_json=(
            _canonical_bytes(current_source_capture)
            if current_source_capture is not None
            else None
        ),
        _bounded_canary_request=bounded_canary_request,
        _bounded_canary_source_session=bounded_canary_source_session,
        _bounded_canary_plan_permit=bounded_canary_plan_permit,
    )
    if result.attempt_count != ATTEMPT_COUNT:
        _fail("CELL_PLAN_ATTEMPT_ROSTER_INCOMPLETE")
    verify_production_cell_search_plan(
        context,
        process=process,
        sink=sink,
        plan=result,
    )
    return result


def production_search_planner_contract() -> dict[str, Any]:
    dependencies = {
        "pm_authoritative_search_execution": _source_sha256(search_execution),
        "pm_current_refit_artifacts": _source_sha256(current_refit),
        "pm_causal_process": _source_sha256(causal),
        "pm_historical_production_adapter": _source_sha256(historical_adapter),
        "pm_state_contract": _source_sha256(state_contract),
        "pm_strategies": _source_sha256(StrategyRegistry),
        "pm_training_horizon": _source_sha256(training_horizon),
    }
    return {
        "schema_version": "fxpm.production-search-planner-contract.v1",
        "source_sha256": _file_sha256(Path(__file__)),
        "dependency_source_sha256": dependencies,
        "strategy_count": BASELINE_ATTEMPTS,
        "attempt_count": ATTEMPT_COUNT,
        "all_registry_defaults_included": True,
        "deterministic_nondefault_variants": VARIANT_ATTEMPTS,
        "multiple_horizon_depths_claimed": False,
        "source_geometry_only_depth_ladder": True,
        "authenticated_full_prefix_state_replay_required": True,
        "current_refit_reuses_historical_horizon_oracle": True,
        "full_grid_exhaustive_search_claimed": False,
        "technical_failure_is_market_verdict": False,
        "confers_trading_or_capital_authority": False,
    }


PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256: Final = _sha(
    production_search_planner_contract()
)


__all__ = [
    "ATTEMPT_COUNT",
    "FIT_CALENDAR_DAYS",
    "FIT_CALENDAR_DAY_LADDERS",
    "CELL_PLAN_SCHEMA",
    "CurrentSearchAuthorization",
    "FrozenProductionSearchProcess",
    "ProductionCellSearchPlan",
    "ProductionSearchPlannerError",
    "PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256",
    "build_current_observed_cell_plan",
    "build_current_terminal_cell_plan",
    "build_current_unobserved_cell_plan",
    "build_historical_observed_cell_plan",
    "build_historical_terminal_cell_plan",
    "build_historical_terminal_timeframe_plans",
    "build_historical_unobserved_cell_plan",
    "freeze_current_search_authorization",
    "freeze_production_search_process",
    "production_search_planner_contract",
    "verify_frozen_production_search_process",
    "verify_current_search_authorization",
    "verify_production_cell_search_plan",
]
