"""Frozen production search planning for historical anchors and current refit.

This module closes the mechanical seam between the generation-wide process
precommitment and :mod:`pm_training_horizon`.  It deliberately does not run a
backtest or choose a winner.  For every empirically observed cell it freezes:

* the exact 100-strategy registry, defaults, parameter grids, code and feature
  contracts;
* one timeframe-specific rolling calendar depth (the current Horizon v1 API
  forms a full depth x trial product, so one source-geometry-selected depth
  preserves the complete precommitted attempt roster);
* all 100 full default parameter maps plus two deterministic, outcome-blind
  non-default grid points per strategy; and
* four genuine, non-overlapping, whole-UTC-day causal validation folds.

The same cell design is reproduced at every historical anchor and at the
latest-cutoff current rerun.  The emitted plan still confers no winner,
causal, capital, publication, demo, or live authority.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from enum import Enum
import csv
import hashlib
import io
import inspect
import json
import math
from pathlib import Path
from typing import Any, Final, Mapping, Optional, Sequence, Set
from types import MappingProxyType

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
import pm_regime
import pm_state_contract as state_contract
import pm_training_horizon as training_horizon
from pm_current_refit_parameters import (
    build_parameter_domain,
    canonicalize_parameter_mapping,
    canonicalize_parameter_value,
    decode_parameter_mapping,
    parameter_domain_default_ordinal,
    parameter_domain_point,
    parameter_values_equal,
    validate_parameter_domain_member,
)
from pm_strategies import StrategyRegistry, resolve_strategy_parameters


UTC: Final = timezone.utc
PROCESS_SCHEMA: Final = "fxpm.production-search-process-precommit.v2"
ROUTE_A_FULL_PANEL_PROCESS_SCHEMA: Final = "fxpm.production-search-process-precommit.v3"
ROUTE_A_FULL_PANEL_EXECUTION_EDITION: Final = "fxpm.route-a-full-planned-owner-execution.v1"
PROCESS_SEMANTICS_SCHEMA: Final = "fxpm.production-search-process-semantics.v2"
INDEPENDENT_R_SOURCE_EDITION: Final = "fxpm.independent-signal-modeled-r-source.v1"
INDEPENDENT_R_PROCESS_SCHEMA: Final = "fxpm.production-search-process-precommit.v5"
INDEPENDENT_R_EXECUTION_EDITION: Final = "fxpm.independent-signal-modeled-r-execution.v1"
INDEPENDENT_R_PROCESS_SEMANTICS_SCHEMA: Final = "fxpm.production-search-process-semantics.v4"
DOMAIN_SCHEMA: Final = "fxpm.production-strategy-domain.v3"
ATTEMPT_POLICY_SCHEMA: Final = "fxpm.production-attempt-design-policy.v2"
PROCESS_SPOOL_SYMBOL: Final = "__PRODUCTION_SEARCH_PROCESS__"
CURRENT_AUTH_SPOOL_SYMBOL: Final = "__CURRENT_SEARCH_AUTHORIZATION__"
PROCESS_ARTIFACT_ID: Final = "production-search-process-precommit"
PROCESS_ARTIFACT_TYPE: Final = "production_search_process_precommit"
CELL_PLAN_SCHEMA: Final = "fxpm.production-search-cell-plan-receipt.v2"
CELL_PLAN_ARTIFACT_TYPE: Final = "production_search_cell_plan_receipt"
STATE_CONTEXT_DISPOSITION_SCHEMA: Final = (
    "fxpm.production-trial-state-context-disposition.v1"
)
STATE_CONTEXT_SELECTION_SCHEMA: Final = (
    "fxpm.production-cell-state-context-selection.v2"
)
STATE_REQUIREMENT_SCHEMA: Final = "fxpm.production-attempt-state-requirement.v2"
STATE_CONTEXT_BOUNDARY_REASON: Final = "STATE_CONTEXT_SOURCE_BOUNDARY_SHORTFALL"
STATE_ERROR_ENVELOPE_SCHEMA: Final = (
    "fxpm.production-preprotected-state-error-envelope.v1"
)
EFFECTIVE_STATE_DOMAIN_SCHEMA: Final = (
    "fxpm.production-effective-state-domain.v2"
)
STATE_DOMAIN_CEILING_RECORD_SCHEMA: Final = (
    "fxpm.production-state-domain-ceiling-record.v1"
)
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
# Every registered strategy receives this many deterministic, outcome-blind
# non-default grid points, in addition to its registry default.
#
# Legal grids of the 100 strategies (cartesian_grid_size): the smallest
# admits 165 distinct parameter combinations, the median 4,455, the
# largest 27,500, so these points are well inside every grid.
#
# Deflation in pm_statistics scales with the effective independent trial count,
# so more attempts raise the bar a winner must clear. MAX_ATTEMPTS in
# pm_winner_selection is a payload bound, not a statistical guard.
VARIANTS_PER_STRATEGY: Final = 2
VARIANT_ATTEMPTS: Final = BASELINE_ATTEMPTS * VARIANTS_PER_STRATEGY
ATTEMPT_COUNT: Final = BASELINE_ATTEMPTS + VARIANT_ATTEMPTS
PARAMETER_DESIGN_SEED: Final = "fxpm-production-space-fill-v1"
GROUPED_PARAMETER_DESIGN_SEED: Final = "fxpm-production-grouped-mixed-radix-v2"
MAX_PROCESS_BYTES: Final = 16 * 1024 * 1024
STATE_INITIAL_ERROR_FLOOR: Final = 100.0

# The largest feasible entry is selected from this frozen ladder using source
# geometry only. Exactly one depth reaches Horizon, so redundant depth
# candidates never multiply or truncate the complete precommitted attempt
# roster. Shorter entries prevent ordinary broker-history boundaries from
# turning an otherwise genuine sparse timeframe into a technical veto.
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


def _frozen_tier_horizon_hashes(pair_contract: Mapping[str, Any]) -> tuple[str, str]:
    """Bind the Horizon to both frozen policies, never an ordered PnL ladder."""
    try:
        pair = search_execution.pm_core.validate_backtest_frozen_tier_pair_contract(pair_contract)
    except (TypeError, ValueError, KeyError, OverflowError) as exc:
        raise ProductionSearchPlannerError("FROZEN_TIER_PAIR_CONTRACT_INVALID") from exc
    policies = {level["tier"]: _sha(level["numerical_execution_cost_policy"]) for level in pair["tiers"]}
    return _sha(policies), _sha(pair)


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


def _required_feature_union(
    name: str,
    *,
    defaults: Mapping[str, Any],
    parameter_domain: Mapping[str, Any],
) -> list[str]:
    """Derive the compact union whose exhaustive coverage is release-tested.

    The runtime derivation uses defaults plus every legal atomic row,
    which is only thousands of materializations rather than millions.  The
    state-contract suite independently visits the complete legal registry
    and proves that every member's request is contained in this union.  A
    future interaction-dependent request therefore fails the release test
    before this compact producer can understate a cell domain.
    """

    parameter_points = [dict(defaults)]
    for axis in parameter_domain["axes"]:
        for row in axis["choices"]:
            wire = {**parameter_domain["defaults"], **dict(zip(axis["names"], row))}
            parameter_points.append(decode_parameter_mapping(wire, defaults))
    requested: set[str] = set()
    try:
        for parameters in parameter_points:
            strategy = StrategyRegistry.get(name, **parameters)
            requested.update(str(value) for value in strategy.get_feature_request())
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "STRATEGY_FEATURE_UNION_MATERIALIZATION_FAILED",
            (name, type(exc).__name__, str(exc)),
        ) from exc
    if not requested or any(not value for value in requested):
        _fail("STRATEGY_FEATURE_UNION_INVALID", name)
    return sorted(requested)


def _strategy_domain(name: str, ordinal: int, timeframe: str = None) -> dict[str, Any]:
    strategy = StrategyRegistry.get(name)
    raw_defaults, raw_grid, groups = resolve_strategy_parameters(strategy, timeframe)
    if set(raw_defaults) - set(strategy.get_params()):
        strategy = StrategyRegistry.get(name, **raw_defaults)
    defaults = canonicalize_parameter_mapping(raw_defaults)
    normalized_defaults = canonicalize_parameter_mapping(strategy.get_params())
    if defaults != normalized_defaults:
        _fail("STRATEGY_DEFAULT_NORMALIZATION_DRIFT", name)
    if type(raw_grid) is not dict or not raw_grid:
        _fail("STRATEGY_GRID_INVALID", name)
    parameter_domain = build_parameter_domain(
        raw_defaults, raw_grid, coupled_groups=groups,
    )
    axes: list[dict[str, Any]] = []
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
        axes.append({"name": axis_name, "choices": choices})
    required_feature_union = _required_feature_union(
        name,
        defaults=raw_defaults,
        parameter_domain=parameter_domain,
    )
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
        "required_feature_union": required_feature_union,
        "required_feature_union_derivation": (
            "DEFAULT_PLUS_EVERY_LEGAL_ATOMIC_ROW_WITH_EXHAUSTIVE_LEGAL_RELEASE_PROOF"
        ),
        "defaults": defaults,
        "grid_axes": axes,
        "parameter_domain": parameter_domain,
        "grid_size": parameter_domain["point_count"],
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
    for name in sorted(state_contract.ACTUAL_STRATEGY_NAMES):
        strategy = StrategyRegistry.get(name)
        documents.append(
            {
                "name": name,
                "defaults": _normalize_candidate_domain_value(
                    strategy.get_default_params()
                ),
                "grid": _normalize_candidate_domain_value(strategy.get_param_grid()),
                "coupled_groups": _normalize_candidate_domain_value(strategy.get_param_couplings()),
            }
        )
    try:
        return state_contract.strategy_candidate_domain_sha256(documents)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "STRATEGY_CANDIDATE_DOMAIN_INVALID", (type(exc).__name__,)
        ) from exc


def _strategy_domains() -> list[dict[str, Any]]:
    actual_names = tuple(StrategyRegistry.list_all())
    names = list(state_contract.ACTUAL_STRATEGY_NAMES)
    if (
        actual_names not in (tuple(names), (*names, NOISE101_STRATEGY_ID))
        or len(names) != BASELINE_ATTEMPTS
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
                "PM_STATE_CONTRACT_EXHAUSTIVE_EFFECTIVE_DOMAIN_CEILING_PLUS_"
                "EXACT_REGIME_WARMUP_AND_CAUSAL_HOLDING_POLICY"
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
            "winner_contract_version": book.CURRENT_PINNED_WINNER_SELECTION_CONTRACT_VERSION,
            "winner_max_attempts": search_execution.MAX_ATTEMPTS,
            "attempt_count_per_observed_cell": ATTEMPT_COUNT,
            "one_full_default_per_strategy": True,
            "default_attempt_count": BASELINE_ATTEMPTS,
            "additional_nondefault_attempt_count": VARIANT_ATTEMPTS,
            "variant_strategy_allocation": (
                "REGIME_CATEGORY_AFFINITY_THEN_FROZEN_CELL_HASH_PERMUTATION"
            ),
            "variant_parameter_sampler": (
                "SHA256_COPRIME_MIXED_RADIX_LEGAL_POINTS_V2"
            ),
            "parameter_design_seed": GROUPED_PARAMETER_DESIGN_SEED,
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
            "HORIZON_V1_SINGLE_DEPTH_PRESERVES_COMPLETE_PRECOMMITTED_ATTEMPT_ROSTER"
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


_PROCESS_VERIFICATION_STATE = ContextVar("fxpm_owned_process_verification", default=None)


@contextmanager
def _process_verification_scope():
    if _PROCESS_VERIFICATION_STATE.get() is not None:
        yield
        return
    token = _PROCESS_VERIFICATION_STATE.set({"verified": {}, "expected": {}})
    try:
        yield
    finally:
        _PROCESS_VERIFICATION_STATE.reset(token)


def _owned_process_verification_call(function):
    @wraps(function)
    def owned(*args, **kwargs):
        with _process_verification_scope():
            return function(*args, **kwargs)
    return owned


def _process_context_binding(context):
    if type(context) is not authority_spool.GenerationSpoolContext:
        _fail("GENERATION_CONTEXT_INVALID")
    return (str(context.root), context.generation_id, context.context_sha256,
        context.native_universe_sha256, context.config_sha256)


def _process_source_generation():
    import pm_winner_selection as selection
    return (PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256,
        search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
        historical_adapter.HISTORICAL_PRODUCER_RELEASE_SHA256,
        state_contract.STRATEGY_CANDIDATE_DOMAIN_SHA256,
        state_contract.DEFAULT_AUTHORITY.registry_hash,
        tuple(state_contract.ACTUAL_STRATEGY_NAMES),
        selection.V17_GENERATION.selection_version, selection.V17_GENERATION.request_version,
        selection.V17_GENERATION.policy_sha256,
        selection.V17_SELECTION_ADDENDUM_CONTRACT_SHA256,
        tuple(StrategyRegistry.list_all()),
        _canonical_bytes(causal.pinned_process_policy()),
        PROCESS_SCHEMA, ROUTE_A_FULL_PANEL_PROCESS_SCHEMA,
        ROUTE_A_FULL_PANEL_EXECUTION_EDITION, PROCESS_SEMANTICS_SCHEMA,
        INDEPENDENT_R_PROCESS_SCHEMA, INDEPENDENT_R_EXECUTION_EDITION,
        INDEPENDENT_R_PROCESS_SEMANTICS_SCHEMA, selection.V19_GENERATION.policy_sha256,
        ATTEMPT_POLICY_SCHEMA, book.CURRENT_PINNED_WINNER_SELECTION_CONTRACT_VERSION,
        search_execution.MAX_ATTEMPTS, ATTEMPT_COUNT, BASELINE_ATTEMPTS,
        VARIANT_ATTEMPTS, GROUPED_PARAMETER_DESIGN_SEED,
        tuple((timeframe, tuple(FIT_CALENDAR_DAY_LADDERS[timeframe])) for timeframe in TIMEFRAMES),
        CAUSAL_FOLD_COUNT, CAUSAL_FOLD_MIN_DAYS,
        NOISE101_PROCESS_SCHEMA, NOISE101_EXECUTION_EDITION, NOISE101_CATALOGUE_SCHEMA,
        selection.V18_GENERATION.selection_version, selection.V18_GENERATION.request_version,
        selection.V18_GENERATION.policy_sha256, selection.V18_GENERATION.max_attempts)


def _expected_process_body(
    context: authority_spool.GenerationSpoolContext,
    *,
    run_id: str,
    schema_version: str = PROCESS_SCHEMA,
) -> dict[str, Any]:
    run_id = _identity(run_id, "RUN_ID_INVALID")
    if schema_version not in (PROCESS_SCHEMA, ROUTE_A_FULL_PANEL_PROCESS_SCHEMA, NOISE101_PROCESS_SCHEMA, INDEPENDENT_R_PROCESS_SCHEMA):
        _fail("PROCESS_SOURCE_EDITION_UNKNOWN")
    state = _PROCESS_VERIFICATION_STATE.get()
    if state is None:
        return _derive_expected_process_body(context, run_id=run_id, schema_version=schema_version)
    key = (_process_context_binding(context), run_id, schema_version, _process_source_generation())
    cached = state["expected"].get(key)
    if cached is None:
        body = _derive_expected_process_body(context, run_id=run_id, schema_version=schema_version)
        cached = _canonical_bytes(body)
        state["expected"][key] = cached
    return json.loads(cached.decode("ascii"))


def _derive_expected_process_body(
    context: authority_spool.GenerationSpoolContext,
    *,
    run_id: str,
    schema_version: str = PROCESS_SCHEMA,
) -> dict[str, Any]:
    observed = authority_spool.reopen_generation_context(
        context.root,
        expected_generation_id=context.generation_id,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    if observed.context_sha256 != context.context_sha256:
        _fail("GENERATION_CONTEXT_CHANGED")
    if schema_version not in {PROCESS_SCHEMA, ROUTE_A_FULL_PANEL_PROCESS_SCHEMA, NOISE101_PROCESS_SCHEMA, INDEPENDENT_R_PROCESS_SCHEMA}:
        _fail("PROCESS_SOURCE_EDITION_UNKNOWN")
    noise_metadata = noise101_process_metadata() if schema_version == NOISE101_PROCESS_SCHEMA else None
    domains = _strategy_domains() if noise_metadata is None else None
    semantics = _process_semantics(domains) if noise_metadata is None else noise_metadata["semantics"]
    if schema_version == ROUTE_A_FULL_PANEL_PROCESS_SCHEMA:
        import pm_winner_selection as selection
        semantics["attempt_policy"]["winner_contract_version"] = selection.V17_GENERATION.selection_version
    if schema_version == INDEPENDENT_R_PROCESS_SCHEMA:
        import pm_winner_selection as selection
        semantics["schema_version"] = INDEPENDENT_R_PROCESS_SEMANTICS_SCHEMA
        semantics["attempt_policy"]["winner_contract_version"] = selection.V19_GENERATION.selection_version
        semantics["trade_method"] = {
            "method_edition": "fxpm.independent-signal-modeled-r.v1",
            "entry_regime": "CONFIRMED_PREVIOUS_BAR_AT_ENTRY_OPEN",
            "daily_attribution": "RECORDED_EXIT_BAR_OPEN_UTC_DAY",
            "open_at_window_end": "EXCLUDED",
            "unresolved_at_window_end": "RIGHT_CENSORED_NO_MARK_TO_MARKET",
            "account_replay": "SEPARATE_EXISTING_PORTFOLIO_AND_MARK_OPEN",
            "cost_model": "SPREAD_PLUS_SWAP", "commission": "NOT_MODELED"}
    body = {
        "schema_version": schema_version,
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

    if noise_metadata is not None:
        body.update(noise_metadata)
        body["process_semantics_sha256"] = _sha(semantics)
        body["authority_scope"] = "SOFTWARE_SEARCH_DESCRIPTION_ONLY_UNQUALIFIED"
    if schema_version in (ROUTE_A_FULL_PANEL_PROCESS_SCHEMA, NOISE101_PROCESS_SCHEMA, INDEPENDENT_R_PROCESS_SCHEMA):
        body["execution_edition"] = {ROUTE_A_FULL_PANEL_PROCESS_SCHEMA: ROUTE_A_FULL_PANEL_EXECUTION_EDITION,
            NOISE101_PROCESS_SCHEMA: NOISE101_EXECUTION_EDITION,
            INDEPENDENT_R_PROCESS_SCHEMA: INDEPENDENT_R_EXECUTION_EDITION}[schema_version]
        import pm_winner_selection as selection
        generation = {ROUTE_A_FULL_PANEL_PROCESS_SCHEMA: selection.V17_GENERATION,
            NOISE101_PROCESS_SCHEMA: selection.V18_GENERATION,
            INDEPENDENT_R_PROCESS_SCHEMA: selection.V19_GENERATION}[schema_version]
        body["winner_selection_binding"] = {
            "selection_version": generation.selection_version,
            "request_version": generation.request_version,
            "selection_policy_sha256": generation.policy_sha256,
            "selection_addendum_contract_sha256": generation.policy["selection_addendum_contract_sha256"],
        }
    return body

@dataclass(frozen=True, slots=True)
class FrozenProductionSearchProcess:
    _body_json: bytes
    _spool_receipt_json: bytes
    _seal: object
    _semantic_admission: tuple | None = field(default=None, init=False, repr=False, compare=False)

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
    _declared_slot_request: Any = None

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


@_owned_process_verification_call
def _freeze_production_search_process(
    context: authority_spool.GenerationSpoolContext,
    *,
    run_id: str,
    schema_version: str,
) -> FrozenProductionSearchProcess:
    """Freeze the complete search domain before any anchor callback runs."""

    if type(context) is not authority_spool.GenerationSpoolContext:
        _fail("GENERATION_CONTEXT_INVALID")
    body = _expected_process_body(context, run_id=run_id, schema_version=schema_version)
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


def freeze_production_search_process(context: authority_spool.GenerationSpoolContext, *, run_id: str) -> FrozenProductionSearchProcess:
    """Freeze the existing default edition without selecting Route A execution."""
    return _freeze_production_search_process(context, run_id=run_id, schema_version=PROCESS_SCHEMA)


def freeze_route_a_full_panel_production_search_process(context: authority_spool.GenerationSpoolContext, *, run_id: str) -> FrozenProductionSearchProcess:
    """Freeze the explicit full-panel edition before creating declared requests."""
    return _freeze_production_search_process(context, run_id=run_id,
        schema_version=ROUTE_A_FULL_PANEL_PROCESS_SCHEMA)


def freeze_independent_signal_r_production_search_process(context: authority_spool.GenerationSpoolContext, *, run_id: str) -> FrozenProductionSearchProcess:
    """Freeze the explicit ordinary 100/300 independent-trade edition."""
    return _freeze_production_search_process(context, run_id=run_id,
        schema_version=INDEPENDENT_R_PROCESS_SCHEMA)


def frozen_search_execution_edition(context: authority_spool.GenerationSpoolContext,
        process: FrozenProductionSearchProcess) -> str | None:
    """Authenticate the process before selecting its frozen execution path."""
    verify_frozen_production_search_process(context, process)
    body = process.to_dict()
    return body.get("execution_edition") if is_route_a_full_panel_process(body) else None


@_owned_process_verification_call
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
    if type(process._body_json) is not bytes or type(process._spool_receipt_json) is not bytes:
        _fail("PROCESS_OBJECT_INVALID")
    state = _PROCESS_VERIFICATION_STATE.get()
    context_binding, source_generation = _process_context_binding(context), _process_source_generation()
    key = (context_binding, process._body_json, process._spool_receipt_json, source_generation)
    admission = (context_binding, source_generation, process._body_json)
    if state is not None and key in state["verified"]:
        object.__setattr__(process, "_semantic_admission", admission)
        return True
    body = process.to_dict()
    if _canonical_bytes(body) != process._body_json:
        _fail("PROCESS_BYTES_NONCANONICAL")
    if process._semantic_admission != admission:
        expected = _expected_process_body(
            context, run_id=body.get("run_id"), schema_version=body.get("schema_version"),
        )
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
    object.__setattr__(process, "_semantic_admission", admission)
    if state is not None:
        state["verified"][key] = True
    return True


def reopen_production_search_process(
    context: authority_spool.GenerationSpoolContext,
    *,
    spool_receipt: Mapping[str, Any],
    expected_receipt_sha256: str,
) -> FrozenProductionSearchProcess:
    """Read an existing process seal without issuing or changing its bytes."""

    if type(context) is not authority_spool.GenerationSpoolContext:
        _fail("GENERATION_CONTEXT_INVALID")
    expected_sha = _hex64(expected_receipt_sha256, "PROCESS_RECEIPT_SHA256_INVALID")
    manifest = authority_spool.verify_symbol_receipt(
        context.root, spool_receipt,
        expected_generation_id=context.generation_id,
        expected_symbol=PROCESS_SPOOL_SYMBOL,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    entries = manifest.get("artifacts")
    if (type(entries) is not list or len(entries) != 1
            or entries[0].get("artifact_id") != PROCESS_ARTIFACT_ID
            or entries[0].get("artifact_type") != PROCESS_ARTIFACT_TYPE):
        _fail("PROCESS_SPOOL_ROSTER_INVALID")
    descriptor = entries[0].get("descriptor")
    if (type(descriptor) is not dict or descriptor.get("sha256") != expected_sha
            or descriptor.get("format") != authority_spool.CANONICAL_JSON_FORMAT
            or type(descriptor.get("size_bytes")) is not int
            or not 0 < descriptor["size_bytes"] <= MAX_PROCESS_BYTES):
        _fail("PROCESS_SPOOL_DESCRIPTOR_MISMATCH")
    digest, _, encoded = authority_spool._stream_file(
        context.root, descriptor["relative_path"], maximum_bytes=MAX_PROCESS_BYTES,
        expected_size=descriptor["size_bytes"], collect=True,
        chunk_bytes=context.limits.stream_chunk_bytes,
    )
    if digest != expected_sha or encoded is None:
        _fail("PROCESS_SPOOL_DESCRIPTOR_MISMATCH")
    result = FrozenProductionSearchProcess(
        encoded, _canonical_bytes(dict(spool_receipt), maximum=256 * 1024), _PROCESS_SEAL,
    )
    verify_frozen_production_search_process(context, result)
    return result


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
    _require_current_process(process)
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
        "schema_version": "fxpm.historical-horizon-policy.v3",
        "policy_id": f"fxpm-production-terminal-{timeframe}-v1",
        "policy_release_sha256": PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256,
        "cost_contract_sha256": _sha({
            **terminal_contract,
            "binding_domain": "fxpm.terminal-cost-not-applicable.v1",
        }),
        "frozen_tier_pair_contract_sha256": _sha({
            **terminal_contract,
            "binding_domain": "fxpm.terminal-frozen-tier-evaluation-not-applicable.v1",
        }),
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
    state_domain_records: Sequence[Mapping[str, Any]],
    state_context_selection: Mapping[str, Any],
    state_error_envelope: Mapping[str, Any],
    boundary_limited: bool,
    current_refit_source_capture_sha256: str | None = None,
    bounded_canary_request: Any = None,
    bounded_canary_source_session: Any = None,
    bounded_canary_plan_permit: Any = None,
    declared_slot_request: Any = None,
    m5_view_window: Mapping[str, Any] | None = None,
) -> tuple[bytes, bytes]:
    if type(sink) is not authority_spool.SymbolAuthoritySink:
        _fail("CELL_PLAN_SINK_INVALID")
    normalized_mode = _identity(mode, "CELL_PLAN_MODE_INVALID").upper()
    if normalized_mode not in {
        "HISTORICAL_ANCHOR",
        "CURRENT_REFIT",
        "BOUNDED_RAW_WINNER_CANARY",
        "DECLARED_SLOT_UNISSUED",
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
    edition = source_edition_info(process)
    normalized_namespace = _artifact_namespace(namespace)
    native = dict(fitted.product.native_prefix)
    symbol = str(native["symbol"])
    timeframe = str(_planning_source_value(native, m5_view_window, "timeframe"))
    plan_binding = historical.historical_horizon_plan_binding(horizon_plan)
    attempt_hashes = [_sha(dict(value)) for value in attempt_artifacts]
    state_hashes = [
        _hex64(value.get("requirement_sha256"), "STATE_REQUIREMENT_HASH_INVALID")
        for value in state_requirements
    ]
    state_domain_record_hashes = [
        _hex64(
            value.get("record_sha256"),
            "STATE_DOMAIN_CEILING_RECORD_SHA256_INVALID",
        )
        for value in state_domain_records
    ]
    _verify_state_error_envelope_fitted_binding(state_error_envelope, fitted=fitted, native=native, m5_view_window=m5_view_window)
    if (
        len(attempt_hashes) != edition.attempt_count
        or len(state_hashes) != edition.attempt_count
        or len(state_domain_record_hashes) != edition.strategy_count
        or len(set(attempt_hashes)) != len(attempt_hashes)
        or len(state_hashes) != len(set(state_hashes))
        or len(state_domain_record_hashes)
        != len(set(state_domain_record_hashes))
    ):
        _fail("CELL_PLAN_ROSTER_INVALID")
    body = {
        "schema_version": M5_DERIVED_CELL_PLAN_SCHEMA if m5_view_window is not None else CELL_PLAN_SCHEMA,
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
        "state_domain_ceiling_records": [
            dict(value) for value in state_domain_records
        ],
        "state_domain_ceiling_record_sha256": state_domain_record_hashes,
        "state_domain_ceiling_record_roster_sha256": _sha(
            state_domain_record_hashes
        ),
        "state_context_selection": dict(state_context_selection),
        "state_error_envelope": dict(state_error_envelope),
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
    if normalized_mode == "DECLARED_SLOT_UNISSUED":
        declared = historical.verify_declared_slot_producer_request(context, declared_slot_request, process=process)
        if declared["artifact_namespace"] != normalized_namespace:
            _fail("DECLARED_SLOT_REQUEST_BINDING_MISMATCH")
        body["declared_slot_request_sha256"] = declared_slot_request.receipt_sha256
    elif declared_slot_request is not None:
        _fail("DECLARED_SLOT_REQUEST_INVALID")
    if m5_view_window is not None:
        if normalized_mode != "DECLARED_SLOT_UNISSUED":
            _fail("M5_CELL_PLAN_MODE_INVALID")
        body.update(source_kind="m5_derived", m5_view_window=dict(m5_view_window),
            development_scope=historical.declared_slot_development_scope(declared),
            development_scope_sha256=declared.get("development_scope_sha256"))
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
    process_body: Mapping[str, Any],
    receipt: Mapping[str, Any],
    attempts: Sequence[Mapping[str, Any]],
    states: Sequence[Mapping[str, Any]],
    horizon_plan: Any,
    boundary_limited: bool,
    timeframe: str,
    m5_view_window: Mapping[str, Any] | None = None,
) -> None:
    """Recompute the full state-requirement -> Horizon disposition chain."""

    edition = source_edition_info(process_body)
    is_noise = edition.edition_id == NOISE101_SOURCE_EDITION_ID
    try:
        plan_body = horizon_plan.to_dict()
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "STATE_CONTEXT_HORIZON_PLAN_INVALID",
            (type(exc).__name__, str(exc)),
        ) from exc
    payload = receipt.get("dependency_payload")
    geometry = receipt.get("dependency_geometry")
    native_prefix = receipt.get("native_prefix")
    envelope = receipt.get("state_error_envelope")
    initial_error, tolerance, regime_warmup_bars = (
        _validated_state_error_envelope(envelope, native_prefix=_planning_source_bounds(native_prefix, m5_view_window))
    )
    carried_domain_records = receipt.get("state_domain_ceiling_records")
    if type(carried_domain_records) is not list:
        _fail("STATE_DOMAIN_CEILING_ROSTER_INVALID")
    expected_domain_records, domain_regime_warmup_bars = (
        _state_domain_ceiling_records(
            process_body,
            native_prefix=native_prefix,
            cell=receipt.get("cell", {}),
            state_error_envelope=envelope,
            m5_view_window=m5_view_window,
        )
    )
    record_hashes = [value["record_sha256"] for value in expected_domain_records]
    if (
        carried_domain_records != expected_domain_records
        or receipt.get("state_domain_ceiling_record_sha256") != record_hashes
        or receipt.get("state_domain_ceiling_record_roster_sha256")
        != _sha(record_hashes)
        or domain_regime_warmup_bars != regime_warmup_bars
    ):
        _fail("STATE_DOMAIN_CEILING_REPLAY_INVALID")
    domains_by_strategy = {
        value["strategy_id"]: value for value in expected_domain_records
    }
    candidates = plan_body.get("candidates")
    trials = plan_body.get("trials")
    plan_attempts = plan_body.get("attempts")
    dependency_policy = plan_body.get("dependency_policy")
    if (
        type(payload) is not dict
        or type(geometry) is not dict
        or type(native_prefix) is not dict
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
    folds = candidate.get("folds") if type(candidate) is dict else None
    folds_are_objects = type(folds) is list and all(
        type(value) is dict for value in folds
    )
    envelope_cutoff = _utc(
        envelope.get("preprotected_cutoff_utc"),
        "STATE_ERROR_ENVELOPE_CUTOFF_INVALID",
    )
    earliest_fit_start = (
        min(
            _utc(value.get("fit_start_utc"), "FIT_START_INVALID")
            for value in folds
        )
        if folds_are_objects and folds
        else None
    )
    if (
        type(candidate) is not dict
        or candidate != receipt.get("candidate")
        or not folds_are_objects
        or (
            earliest_fit_start is not None
            and (
                envelope.get("four_fold_protected_axis_available") is not True
                or envelope_cutoff > earliest_fit_start
            )
        )
        or type(selected) is not int
        or selected < 1
        or geometry != _derived_dependency_geometry(timeframe, payload)
        or geometry.get("warmup_bars", -1) < selected
        or payload.get("regime_warmup_bars") != regime_warmup_bars
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
        domain_record = domains_by_strategy.get(state.get("strategy_id"))
        effective_domain = (
            domain_record.get("effective_domain")
            if type(domain_record) is dict
            else None
        )
        ceiling = (
            domain_record.get("ceiling")
            if type(domain_record) is dict
            else None
        )
        if (
            requirement_sha256 != _sha(requirement_body)
            or state.get("schema_version") != (M5_DERIVED_STATE_REQUIREMENT_SCHEMA if m5_view_window is not None else (NOISE101_STATE_REQUIREMENT_SCHEMA if is_noise else STATE_REQUIREMENT_SCHEMA))
            or state.get("execution_rule")
            != "COMPUTE_FEATURES_AND_RAW_SIGNALS_ONCE_OVER_FULL_PREFIX_THEN_SLICE"
            or state.get("initial_error_bound_hex") != initial_error.hex()
            or state.get("tolerance_hex") != tolerance.hex()
            or type(effective_domain) is not dict
            or type(ceiling) is not dict
            or state.get("effective_domain_sha256")
            != effective_domain.get("effective_domain_sha256")
            or state.get("state_domain_ceiling_decision_sha256")
            != ceiling.get("decision_hash")
            or state.get("state_domain_ceiling_bars")
            != ceiling.get("bounded_context_bars")
            or type(state.get("bounded_context_bars")) is not int
            or state["bounded_context_bars"] > ceiling.get(
                "bounded_context_bars", -1
            )
            or type(state_evidence) is not list
            or any(
                type(value) is not dict
                or value.get("schema_version")
                != ("fxpm.production-m5-derived-full-prefix-state-replay.v1" if m5_view_window is not None else (NOISE101_FULL_PREFIX_STATE_REPLAY_SCHEMA if is_noise else "fxpm.production-full-prefix-state-replay.v1"))
                or value.get(
                    "requires_executor_full_prefix_replay_before_any_scored_slice"
                )
                is not True
                for value in state_evidence
            )
        ):
            _fail("STATE_REQUIREMENT_REPLAY_INVALID")
        if is_noise:
            _verify_noise101_state_requirement(edition, timeframe, state,
                effective_domain=effective_domain, native_prefix=native_prefix,
                state_error_envelope=envelope)

    if m5_view_window is not None:
        for state in states:
            if (state.get("source_kind") != "m5_derived"
                    or state.get("m5_view_window") != m5_view_window
                    or state.get("m5_view_window_sha256") != _sha(dict(m5_view_window))
                    or state.get("regime_surface_source") != "FITTED_FULL_M5_DERIVED_PREFIX_REGIME_LIVE"):
                _fail("M5_STATE_REQUIREMENT_BINDING_INVALID")
            for replay in state["state_evidence"]:
                if (replay.get("source_kind") != "m5_derived"
                        or replay.get("m5_view_window") != m5_view_window
                        or replay.get("m5_view_window_sha256") != _sha(dict(m5_view_window))
                        or replay.get("native_prefix_index_sha256") != native_prefix["index_sha256"]
                        or replay.get("native_prefix_content_sha256") != native_prefix["content_sha256"]
                        or replay.get("prefix_start_utc") != native_prefix["start_utc"]
                        or replay.get("prefix_end_exclusive_utc") != native_prefix["end_exclusive_utc"]
                        or replay.get("prefix_rows") != native_prefix["rows"]):
                    _fail("M5_STATE_REPLAY_BINDING_INVALID")
    expected_dispositions = _state_context_dispositions(
        states,
        candidate=candidate,
        selected_common_context_bars=selected,
    )
    expected_selection = _state_context_selection_receipt(
        expected_domain_records,
        expected_dispositions,
        selected_common_context_bars=selected,
        boundary_limited=boundary_limited,
        source_edition=edition,
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
    return _verify_production_cell_search_plan_source(context, process=process, sink=sink, plan=plan, m5_derived=False)


def verify_m5_derived_production_cell_search_plan(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    sink: authority_spool.SymbolAuthoritySink,
    plan: ProductionCellSearchPlan,
) -> bool:
    return _verify_production_cell_search_plan_source(context, process=process, sink=sink, plan=plan, m5_derived=True)


def _verify_production_cell_search_plan_source(
    context: authority_spool.GenerationSpoolContext,
    *,
    process: FrozenProductionSearchProcess,
    sink: authority_spool.SymbolAuthoritySink,
    plan: ProductionCellSearchPlan,
    m5_derived: bool,
    read_only: bool = False,
) -> bool:
    """Reopen one immutable cell plan and rederive every carried roster hash."""

    verify_frozen_production_search_process(context, process)
    if (
        type(sink) is not authority_spool.SymbolAuthoritySink
        or type(plan) is not ProductionCellSearchPlan
        or plan._seal is not _CELL_PLAN_SEAL
    ):
        _fail("CELL_PLAN_OBJECT_INVALID")
    edition = source_edition_info(process)
    receipt = plan.cell_receipt
    receipt_sha256 = receipt.get("receipt_sha256")
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if (
        _canonical_bytes(receipt) != plan._cell_receipt_json
        or receipt_sha256 != _sha(body)
        or receipt.get("schema_version") != (M5_DERIVED_CELL_PLAN_SCHEMA if m5_derived else CELL_PLAN_SCHEMA)
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
    m5_view_window = None
    if m5_derived:
        if plan.mode != "DECLARED_SLOT_UNISSUED" or plan.fitted_timeframe is None:
            _fail("M5_CELL_PLAN_MODE_INVALID")
        _, m5_native, _, _, m5_view_window = _validated_m5_frame(plan.fitted_timeframe)
        expected_base = {"index_sha256": plan.fitted_timeframe.product.index_sha256,
            "content_sha256": m5_native["prefix_content_sha256"], "start_utc": m5_native["prefix_start_utc"],
            "end_exclusive_utc": m5_native["prefix_end_exclusive_utc"],
            "last_close_utc": m5_native["prefix_last_close_utc"], "rows": m5_native["prefix_rows"]}
        if receipt.get("native_prefix") != expected_base:
            _fail("M5_CELL_PLAN_NATIVE_BINDING_INVALID")
        horizon_body = plan.horizon_plan.to_dict()
        if (set(receipt) != _M5_CELL_PLAN_FIELDS
                or receipt.get("source_kind") != "m5_derived"
                or receipt.get("m5_view_window") != m5_view_window
                or horizon_body.get("source_kind") != "m5_derived"
                or horizon_body.get("m5_view_window") != m5_view_window
                or horizon_body.get("native_prefix") != m5_native
                or plan.native_prefix_product is not plan.fitted_timeframe.product
                or plan.timeframe != m5_view_window["timeframe"]):
            _fail("M5_CELL_PLAN_VIEW_BINDING_INVALID")
    if plan.mode == "DECLARED_SLOT_UNISSUED":
        declared = historical.verify_declared_slot_producer_request(context, plan._declared_slot_request, process=process)
        if (receipt.get("declared_slot_request_sha256") != plan._declared_slot_request.receipt_sha256
                or declared["artifact_namespace"] != receipt.get("artifact_namespace")
                or declared["cell"] != {"cell_id": declared["cell_id"], "symbol": plan.symbol, "timeframe": plan.timeframe, "regime": plan.regime}
                or declared["process_precommit_receipt_sha256"] != plan.causal_precommit_receipt_sha256):
            _fail("DECLARED_SLOT_REQUEST_BINDING_MISMATCH")
        if m5_derived and (receipt.get("development_scope") != historical.declared_slot_development_scope(declared)
                or receipt.get("development_scope_sha256") != declared.get("development_scope_sha256")):
            _fail("M5_CELL_PLAN_DEVELOPMENT_SCOPE_MISMATCH")
    elif plan._declared_slot_request is not None:
        _fail("DECLARED_SLOT_REQUEST_INVALID")
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
    expected_attempt_count = 0 if terminal or unobserved else edition.attempt_count
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
            process_body=process.to_dict(),
            receipt=receipt,
            attempts=attempts,
            states=states,
            horizon_plan=plan.horizon_plan,
            boundary_limited=plan.boundary_limited,
            timeframe=plan.timeframe,
            m5_view_window=m5_view_window,
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
    elif not m5_derived and (
        plan.mode
        not in {
            "HISTORICAL_ANCHOR",
            "CURRENT_REFIT",
            "BOUNDED_RAW_WINNER_CANARY",
            "DECLARED_SLOT_UNISSUED",
        }
        or plan.native_prefix_product is not None
        or plan.fitted_timeframe is not None
    ):
        _fail("CELL_PLAN_MODE_INVALID")
    namespace = _artifact_namespace(receipt.get("artifact_namespace"))
    artifact_id = f"{namespace}/search-cell-plan/{plan.timeframe}/{plan.regime}"
    if read_only:
        expected_entry = sink._artifacts.get(artifact_id)
        if expected_entry is None:
            _fail("M5_SAVED_PLAN_ORIGINAL_MISSING")
        stored, _ = _read_existing_m5_plan_json(context, expected_entry.get("descriptor"))
        if stored != plan._cell_receipt_json:
            _fail("M5_SAVED_PLAN_ORIGINAL_CHANGED")
    else:
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


def _cell_hash(
    symbol: str, timeframe: str, regime: str, *parts: str,
    seed: str = PARAMETER_DESIGN_SEED,
) -> str:
    return hashlib.sha256(
        "\x00".join((seed, symbol, timeframe, regime, *parts)).encode(
            "ascii"
        )
    ).hexdigest()


def _domain_by_name(process_body: Mapping[str, Any], *, timeframe: str | None = None) -> dict[str, dict[str, Any]]:
    if process_body.get("schema_version") == NOISE101_PROCESS_SCHEMA:
        edition = source_edition_info(process_body)
        if timeframe not in edition.ordered_timeframes:
            _fail("NOISE101_AUTHENTICATED_TIMEFRAME_REQUIRED")
        catalogue = process_body.get("semantics", {}).get("candidate_catalogue")
        if type(catalogue) is not dict:
            _fail("NOISE101_CATALOGUE_FIELDS_INVALID")
        roster = catalogue["rosters_by_timeframe"][timeframe]
        if roster["domain_roster_sha256"] != dict(edition.domain_roster_sha256_by_timeframe)[timeframe]:
            _fail("NOISE101_CATALOGUE_ROSTER_HASH_INVALID")
        refs = roster["domain_sha256_by_strategy_ordinal"]
        if len(refs) != edition.strategy_count:
            _fail("PROCESS_DOMAIN_ROSTER_INVALID")
        rows = [catalogue["domain_pool"][digest] for digest in refs]
        if tuple(row["strategy_id"] for row in rows) != edition.ordered_strategy_ids:
            _fail("PROCESS_DOMAIN_IDENTITY_INVALID")
        return {row["strategy_id"]: dict(row) for row in rows}
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


def _state_authority_for_candidate(candidate_domain_sha256: str):
    if (
        state_contract.STRATEGY_CANDIDATE_DOMAIN_SHA256 is not None
        and candidate_domain_sha256 == state_contract.STRATEGY_CANDIDATE_DOMAIN_SHA256
    ):
        return state_contract.DEFAULT_AUTHORITY
    catalogue = build_noise101_candidate_catalogue()
    if candidate_domain_sha256 == _sha(catalogue):
        scope = _PROCESS_VERIFICATION_STATE.get()
        if scope is not None and "noise101_authority" in scope:
            return scope["noise101_authority"]
        authority = state_contract.build_noise101_state_authority(candidate_domain_sha256)
        if scope is not None:
            scope["noise101_authority"] = authority
        return authority
    _fail("PROCESS_SOURCE_EDITION_UNKNOWN")


def _require_current_process(process: FrozenProductionSearchProcess) -> None:
    if type(process) is not FrozenProductionSearchProcess or process._seal is not _PROCESS_SEAL:
        _fail("PROCESS_OBJECT_INVALID")
    if process.to_dict().get("schema_version") not in {PROCESS_SCHEMA, ROUTE_A_FULL_PANEL_PROCESS_SCHEMA, NOISE101_PROCESS_SCHEMA, INDEPENDENT_R_PROCESS_SCHEMA}:
        _fail("RETAINED_PROCESS_IS_READ_ONLY")
    body = process.to_dict()
    if (body["schema_version"] == NOISE101_PROCESS_SCHEMA
            and body.get("authority_scope") == "SOFTWARE_SEARCH_DESCRIPTION_ONLY_UNQUALIFIED"):
        _fail("NOISE101_SOFTWARE_DESCRIPTION_NOT_EXECUTABLE")


def _variant_strategy_names(
    domains: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    source_edition: SourceEditionDescriptor | None = None,
) -> list[tuple[str, int]]:
    if any(item.get("schema_version") != DOMAIN_SCHEMA for item in domains):
        _fail("RETAINED_PROCESS_IS_READ_ONLY")
    strategy_count = source_edition.strategy_count if source_edition is not None else BASELINE_ATTEMPTS
    variant_count = strategy_count * VARIANTS_PER_STRATEGY
    affinity = _REGIME_CATEGORY_AFFINITY[regime]

    def order(item: Mapping[str, Any]) -> tuple[int, str]:
        family_match = 1 if item.get("strategy_family") == affinity else 0
        tie = _cell_hash(
            symbol,
            timeframe,
            regime,
            str(item.get("strategy_id") or ""),
            "variant-strategy-permutation",
            seed=GROUPED_PARAMETER_DESIGN_SEED,
        )
        return (-family_match, tie)

    ordered = sorted(domains, key=order)
    names = [str(item["strategy_id"]) for item in ordered]
    if len(names) != strategy_count or len(names) != len(set(names)):
        _fail("VARIANT_STRATEGY_ALLOCATION_INVALID")
    # Every strategy is tuned, VARIANTS_PER_STRATEGY times. Regime-family
    # affinity still drives the ordering, so the sweep remains deterministic and
    # reproducible, but affinity no longer decides *whether* a strategy is tuned
    # at all - only the order in which its variants appear in the plan.
    selected = [
        (name, ordinal)
        for ordinal in range(VARIANTS_PER_STRATEGY)
        for name in names
    ]
    if len(selected) != variant_count or len(selected) != len(set(selected)):
        _fail("VARIANT_STRATEGY_ALLOCATION_INVALID")
    return selected


def _nondefault_parameters(
    strategy_name: str,
    *,
    symbol: str,
    timeframe: str,
    regime: str,
    variant_ordinal: int = 0,
    already_emitted: Optional[Set[str]] = None,
    domain: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """One deterministic, outcome-blind non-default grid point for a strategy.

    ``variant_ordinal`` selects which point, so a strategy can contribute several
    distinct designs to a cell plan without any of them depending on an outcome.
    The current edition traverses a seeded coprime permutation of legal points.
    """
    if domain is None:
        domain = _strategy_domain(strategy_name, 0, timeframe)
    if domain.get("strategy_id") != strategy_name:
        _fail("VARIANT_STRATEGY_OUTSIDE_DOMAIN", strategy_name)
    if domain.get("schema_version") != DOMAIN_SCHEMA:
        _fail("RETAINED_PROCESS_IS_READ_ONLY")
    seen = already_emitted if already_emitted is not None else set()
    if domain.get("schema_version") == DOMAIN_SCHEMA:
        structured = domain.get("parameter_domain")
        baseline = validate_parameter_domain_member(structured, domain.get("defaults"))
        count = structured["point_count"]
        if type(variant_ordinal) is not int or not 0 <= variant_ordinal < count - 1:
            _fail("NONDEFAULT_PARAMETER_POINT_UNAVAILABLE", strategy_name)
        digest = _cell_hash(
            symbol, timeframe, regime, strategy_name, "legal-domain-permutation",
            seed=GROUPED_PARAMETER_DESIGN_SEED,
        )
        start = int(digest[:32], 16) % count
        stride = 1 + int(digest[32:], 16) % (count - 1)
        while math.gcd(stride, count) != 1:
            stride = 1 if stride == count - 1 else stride + 1
        default_ordinal = parameter_domain_default_ordinal(structured)
        selected = -1
        for offset in range(variant_ordinal + 2):
            ordinal = (start + offset * stride) % count
            if ordinal == default_ordinal:
                continue
            selected += 1
            if selected != variant_ordinal:
                continue
            wire = parameter_domain_point(structured, ordinal)
            base = StrategyRegistry.get(strategy_name)
            defaults, _, _ = resolve_strategy_parameters(base, timeframe, defaults_only=True)
            runtime = decode_parameter_mapping(wire, defaults)
            normalized = canonicalize_parameter_mapping(
                StrategyRegistry.get(strategy_name, **runtime).get_params()
            )
            if normalized != wire or normalized == baseline or _sha(normalized) in seen:
                _fail("NONDEFAULT_PARAMETER_MATERIALIZATION_DRIFT", strategy_name)
            validate_parameter_domain_member(structured, normalized)
            seen.add(_sha(normalized))
            return normalized
        _fail("NONDEFAULT_PARAMETER_POINT_UNAVAILABLE", strategy_name)


def _attempt_designs(
    process_body: Mapping[str, Any],
    *,
    symbol: str,
    timeframe: str,
    regime: str,
) -> list[dict[str, Any]]:
    edition = source_edition_info(process_body)
    domains = list(_domain_by_name(process_body, timeframe=timeframe).values())
    domains.sort(key=lambda value: int(value["ordinal"]))
    if any(item.get("schema_version") != DOMAIN_SCHEMA for item in domains):
        _fail("RETAINED_PROCESS_IS_READ_ONLY")
    designs: list[dict[str, Any]] = []
    for item in domains:
        name = str(item["strategy_id"])
        parameters = dict(item["defaults"])
        designs.append(
            {
                "design_role": "REGISTRY_DEFAULT",
                "strategy_id": name,
                "parameters": parameters,
            }
        )
    by_name = {str(item["strategy_id"]): item for item in domains}
    emitted_by_strategy: dict[str, Set[str]] = {}
    for name, variant_ordinal in _variant_strategy_names(
        domains, symbol=symbol, timeframe=timeframe, regime=regime, source_edition=edition
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
                    variant_ordinal=variant_ordinal,
                    already_emitted=emitted_by_strategy.setdefault(name, set()),
                    domain=by_name[name],
                ),
            }
        )
    generation = full_panel_generation(process_body)
    maximum = generation.max_attempts if generation is not None else search_execution.MAX_ATTEMPTS
    if len(designs) != edition.attempt_count or edition.attempt_count != maximum:
        _fail("ATTEMPT_COUNT_NOT_EXACT_WINNER_BOUND")
    identities = [(item["strategy_id"], _sha(item["parameters"])) for item in designs]
    if len(identities) != len(set(identities)):
        _fail("ATTEMPT_PARAMETER_IDENTITY_DUPLICATED")
    return designs


M5_DERIVED_CELL_PLAN_SCHEMA = "fxpm.production-m5-derived-cell-search-plan.v1"
M5_DERIVED_STATE_REQUIREMENT_SCHEMA = "fxpm.production-m5-derived-state-requirement.v1"
_M5_CELL_PLAN_FIELDS = frozenset(('artifact_namespace', 'attempt_artifact_sha256', 'attempt_count', 'attempt_roster_sha256', 'attempts_frozen_before_search', 'authority_scope', 'boundary_limited', 'candidate', 'causal_process_precommit_receipt_sha256', 'cell', 'confers_winner_causal_current_capital_publication_or_live_authority', 'current_refit_source_capture_sha256', 'declared_slot_request_sha256', 'dependency_geometry', 'dependency_payload', 'development_scope', 'development_scope_sha256', 'generation_context_sha256', 'generation_id', 'horizon_plan_binding', 'm5_view_window', 'mode', 'native_prefix', 'receipt_sha256', 'regime_fit_decision_sha256', 'run_id', 'schema_version', 'search_process_receipt_sha256', 'source_group_decision_sha256', 'source_kind', 'state_context_selection', 'state_domain_ceiling_record_roster_sha256', 'state_domain_ceiling_record_sha256', 'state_domain_ceiling_records', 'state_error_envelope', 'state_replay_required_before_scored_partition', 'state_requirement_roster_sha256', 'state_requirement_sha256', 'train_regime_support_sha256'))


def _planning_source_value(native, window, key):
    if window is None:
        return native[key]
    fields = {"timeframe": "timeframe", "prefix_content_sha256": "view_payload_sha256",
        "prefix_start_utc": "view_start_utc", "prefix_end_exclusive_utc": "view_end_exclusive_utc",
        "prefix_last_close_utc": "view_last_close_utc", "prefix_rows": "view_rows"}
    return window[fields[key]]


def _planning_source_get(native, window, key):
    if window is None:
        return native.get(key)
    return _planning_source_value(native, window, key)


def _planning_source_bounds(native, window):
    if window is None:
        return native
    return {"rows": window["view_rows"], "start_utc": window["view_start_utc"],
        "end_exclusive_utc": window["view_end_exclusive_utc"],
        "last_close_utc": window["view_last_close_utc"]}


def _m5_state_binding(body, window, schema):
    if window is None:
        return body
    return {**body, "schema_version": schema, "source_kind": "m5_derived",
        "m5_view_window": dict(window), "m5_view_window_sha256": _sha(dict(window))}


def _validated_m5_frame(fitted):
    frame, window = search_execution.m5_derived_fitted_frame(fitted)
    native = dict(fitted.product.native_prefix)
    if (type(window) is not dict or window.get("source_kind") != "m5_derived"
            or native.get("timeframe") != "M5" or fitted.product.source_state != native_prefix.AVAILABLE
            or type(frame) is not pd.DataFrame or frame.empty or "_BAR_CLOSE_TS" not in frame):
        _fail("M5_CELL_PLAN_SOURCE_INVALID")
    opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce"))
    closes = pd.DatetimeIndex(pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce"))
    if (opens.hasnans or closes.hasnans or not opens.is_monotonic_increasing
            or not opens.is_unique or len(opens) != len(closes) or bool((closes <= opens).any())
            or window["symbol"] != native.get("symbol") or window["timeframe"] not in ("M15", "M30", "H1")
            or window["view_rows"] != len(frame)
            or _utc_text(opens[0]) != _utc_text(window["view_start_utc"])
            or _utc_text(closes[-1]) != _utc_text(window["view_last_close_utc"])
            or window["base_native_prefix_index_sha256"] != fitted.product.index_sha256
            or window["base_native_prefix_content_sha256"] != native.get("prefix_content_sha256")):
        _fail("M5_CELL_PLAN_VIEW_GEOMETRY_INVALID")
    support = fitted.train_regime_support
    if type(support) is not dict or support.get("receipt_sha256") != _sha(
            {key: value for key, value in support.items() if key != "receipt_sha256"}):
        _fail("REGIME_SUPPORT_RECEIPT_INVALID")
    return frame, native, opens, closes, window


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


def _state_price_error_bound(frame: pd.DataFrame) -> float:
    """Return the conservative price-state envelope for one admitted prefix."""

    try:
        if frame.empty:
            _fail("STATE_PRICE_SURFACE_INVALID")
        minimum = math.inf
        maximum = -math.inf
        for name in ("Open", "High", "Low", "Close"):
            values = pd.to_numeric(frame[name], errors="coerce")
            if values.empty or values.isna().any():
                _fail("STATE_PRICE_SURFACE_INVALID")
            minimum = min(minimum, float(values.min()))
            maximum = max(maximum, float(values.max()))
    except ProductionSearchPlannerError:
        raise
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "STATE_PRICE_SURFACE_INVALID", (type(exc).__name__,)
        ) from exc
    maximum_absolute = max(abs(minimum), abs(maximum))
    spread = maximum - minimum
    bound = max(STATE_INITIAL_ERROR_FLOOR, 2.0 * maximum_absolute, spread)
    if not math.isfinite(bound) or bound <= 0.0:
        _fail("STATE_ERROR_ENVELOPE_INVALID")
    return bound


def _state_tolerance(
    fitted: search_execution.FittedNativeSearchTimeframe,
) -> float:
    try:
        instrument = dict(fitted.product.instrument_snapshot or {})
        parity = float(instrument.get("parity_tick_tolerance"))
        tick_size = float(fitted.instrument_spec.tick_size)
        tolerance = min(parity, tick_size)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "STATE_TOLERANCE_DERIVATION_FAILED", (type(exc).__name__,)
        ) from exc
    if (
        not math.isfinite(tolerance)
        or tolerance <= 0.0
        or not math.isfinite(tick_size)
        or tick_size <= 0.0
        or tolerance > tick_size
    ):
        _fail("STATE_TOLERANCE_INVALID")
    return tolerance


def _preprotected_state_envelope_cutoff(
    *,
    native: Mapping[str, Any],
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    timeframe: str,
    m5_view_window: Mapping[str, Any] | None = None,
) -> tuple[pd.Timestamp, bool]:
    """Choose an index-only cutoff earlier than every possible tuning split.

    Use the first fit start of the largest depth that is feasible under the
    minimum state context.  Any realized state context can only remove that
    depth or select a later fit start.  The envelope therefore ends before
    every possible optimizer, tuning, or final-lockbox observation without
    defeating the existing shorter-depth source-boundary recovery.
    """

    prefix_end = pd.Timestamp(
        _utc(_planning_source_get(native, m5_view_window, "prefix_end_exclusive_utc"), "PREFIX_END_INVALID")
    )
    minimal_payload = _dependency_payload(
        timeframe,
        feature_lookback_bars=1,
        regime_warmup_bars=1,
    )
    minimal_geometry = _derived_dependency_geometry(timeframe, minimal_payload)
    for depth_days in reversed(FIT_CALENDAR_DAY_LADDERS[timeframe]):
        result = _try_causal_folds(native=native, opens=opens, closes=closes, timeframe=timeframe, depth_days=depth_days, geometry=minimal_geometry, state_context_bars=1, m5_view_window=m5_view_window)
        if result is not None:
            candidate_start, _folds = result
            return candidate_start, True
    return prefix_end, False


def _regime_warmup_contract(
    fitted: search_execution.FittedNativeSearchTimeframe,
) -> tuple[int, str]:
    """Derive regime warmup from the frozen detector parameters, not a surface."""

    try:
        parameters = pm_regime.RegimeParams.from_dict(
            dict(fitted.regime_parameters)
        )
        parameters_sha256 = pm_regime.regime_params_sha256(parameters.to_dict())
        decision = fitted.regime_fit_decision.to_dict()
        expected_sha256 = decision.get("regime_params_sha256")
        warmup_bars = int(pm_regime.MarketRegimeDetector(parameters).warmup_bars)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "REGIME_WARMUP_CONTRACT_DERIVATION_FAILED", (type(exc).__name__,)
        ) from exc
    if (
        expected_sha256 != parameters_sha256
        or warmup_bars < 1
        or warmup_bars > 1_000_000
    ):
        _fail("REGIME_WARMUP_CONTRACT_INVALID")
    return warmup_bars, parameters_sha256


def _freeze_state_error_envelope(
    *,
    frame: pd.DataFrame,
    fitted: search_execution.FittedNativeSearchTimeframe,
    native: Mapping[str, Any],
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    m5_view_window: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Seal a pre-protected envelope; later values may only fail it closed."""

    timeframe = str(_planning_source_get(native, m5_view_window, "timeframe") or "").upper()
    if (
        timeframe not in TIMEFRAMES
        or len(frame) != len(opens)
        or len(frame) != len(closes)
    ):
        _fail("STATE_ERROR_ENVELOPE_INPUT_INVALID")
    cutoff, four_fold_axis_available = _preprotected_state_envelope_cutoff(native=native, opens=opens, closes=closes, timeframe=timeframe, m5_view_window=m5_view_window)
    # When no four-fold axis exists, ``cutoff`` is the prefix end and the whole
    # frame is non-protected planning input.  Otherwise this same mask ends at
    # the largest feasible first-fold fit start, before every tuning observation.
    source_mask = (opens < cutoff) & (closes <= cutoff)
    source = frame.loc[source_mask, ["Open", "High", "Low", "Close"]]
    if source.empty:
        _fail("STATE_ERROR_ENVELOPE_SOURCE_EMPTY")
    initial_error = _state_price_error_bound(source)
    # This reads later values only as a one-bit verifier.  The observed bound is
    # deliberately neither returned nor sealed, so it cannot tune a ceiling.
    if _state_price_error_bound(frame) > initial_error:
        _fail("STATE_ERROR_ENVELOPE_BREACHED")
    tolerance = _state_tolerance(fitted)
    regime_warmup_bars, regime_parameters_sha256 = _regime_warmup_contract(fitted)
    source_closes = closes[source_mask]
    body = {
        "schema_version": STATE_ERROR_ENVELOPE_SCHEMA,
        "derivation": "MAX_100_DOUBLE_MAX_ABS_AND_RANGE_ON_PREPROTECTED_PREFIX",
        "preprotected_cutoff_rule": (
            "LARGEST_FEASIBLE_FIRST_FOLD_FIT_START_BEFORE_ANY_TUNING_OR_FINAL_OBSERVATION"
            if four_fold_axis_available
            else "FULL_PREFIX_ONLY_WHEN_FOUR_FOLD_PROTECTED_AXIS_UNAVAILABLE"
        ),
        "preprotected_cutoff_utc": _utc_text(cutoff),
        "four_fold_protected_axis_available": four_fold_axis_available,
        "source_rows": int(source_mask.sum()),
        "post_cutoff_rows": int(len(frame) - int(source_mask.sum())),
        "source_last_close_utc": _utc_text(source_closes[-1]),
        "source_price_surface_sha256": _frame_sha256(source),
        "initial_error_bound_hex": initial_error.hex(),
        "tolerance_hex": tolerance.hex(),
        "full_prefix_verification": "WITHIN_FROZEN_PREPROTECTED_ENVELOPE",
        "protected_market_values_can_only_fail_closed": True,
        "regime_parameters_sha256": regime_parameters_sha256,
        "regime_warmup_bars": regime_warmup_bars,
    }
    receipt = {**body, "receipt_sha256": _sha(body)}
    _validated_state_error_envelope(receipt, native_prefix=_planning_source_bounds(native, m5_view_window))
    return receipt


def _validated_state_error_envelope(
    value: Mapping[str, Any],
    *,
    native_prefix: Mapping[str, Any] | None = None,
) -> tuple[float, float, int]:
    if type(value) is not dict:
        _fail("STATE_ERROR_ENVELOPE_INVALID")
    expected_fields = {
        "schema_version",
        "derivation",
        "preprotected_cutoff_rule",
        "preprotected_cutoff_utc",
        "four_fold_protected_axis_available",
        "source_rows",
        "post_cutoff_rows",
        "source_last_close_utc",
        "source_price_surface_sha256",
        "initial_error_bound_hex",
        "tolerance_hex",
        "full_prefix_verification",
        "protected_market_values_can_only_fail_closed",
        "regime_parameters_sha256",
        "regime_warmup_bars",
        "receipt_sha256",
    }
    try:
        initial_error = float.fromhex(str(value.get("initial_error_bound_hex")))
        tolerance = float.fromhex(str(value.get("tolerance_hex")))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ProductionSearchPlannerError("STATE_ERROR_ENVELOPE_INVALID") from exc
    body = {key: item for key, item in value.items() if key != "receipt_sha256"}
    cutoff = _utc(
        value.get("preprotected_cutoff_utc"),
        "STATE_ERROR_ENVELOPE_CUTOFF_INVALID",
    )
    source_end = _utc(
        value.get("source_last_close_utc"),
        "STATE_ERROR_ENVELOPE_SOURCE_END_INVALID",
    )
    four_fold_axis_available = value.get("four_fold_protected_axis_available")
    expected_rule = (
        "LARGEST_FEASIBLE_FIRST_FOLD_FIT_START_BEFORE_ANY_TUNING_OR_FINAL_OBSERVATION"
        if four_fold_axis_available is True
        else "FULL_PREFIX_ONLY_WHEN_FOUR_FOLD_PROTECTED_AXIS_UNAVAILABLE"
    )
    native_rows: Any = None
    native_end: datetime | None = None
    native_last_close: datetime | None = None
    if native_prefix is not None:
        if type(native_prefix) is not dict:
            _fail("STATE_ERROR_ENVELOPE_NATIVE_BINDING_INVALID")
        native_rows = native_prefix.get("prefix_rows", native_prefix.get("rows"))
        native_end = _utc(
            native_prefix.get(
                "prefix_end_exclusive_utc",
                native_prefix.get("end_exclusive_utc"),
            ),
            "STATE_ERROR_ENVELOPE_NATIVE_END_INVALID",
        )
        native_last_close = _utc(
            native_prefix.get(
                "prefix_last_close_utc",
                native_prefix.get("last_close_utc"),
            ),
            "STATE_ERROR_ENVELOPE_NATIVE_LAST_CLOSE_INVALID",
        )
    if (
        set(value) != expected_fields
        or value.get("schema_version") != STATE_ERROR_ENVELOPE_SCHEMA
        or value.get("derivation")
        != "MAX_100_DOUBLE_MAX_ABS_AND_RANGE_ON_PREPROTECTED_PREFIX"
        or value.get("preprotected_cutoff_rule") != expected_rule
        or value.get("receipt_sha256") != _sha(body)
        or type(four_fold_axis_available) is not bool
        or type(value.get("source_rows")) is not int
        or value["source_rows"] < 1
        or type(value.get("post_cutoff_rows")) is not int
        or value["post_cutoff_rows"] < 0
        or value.get("full_prefix_verification")
        != "WITHIN_FROZEN_PREPROTECTED_ENVELOPE"
        or value.get("protected_market_values_can_only_fail_closed") is not True
        or type(value.get("regime_warmup_bars")) is not int
        or not 1 <= value["regime_warmup_bars"] <= 1_000_000
        or not math.isfinite(initial_error)
        or initial_error < STATE_INITIAL_ERROR_FLOOR
        or not math.isfinite(tolerance)
        or tolerance <= 0.0
        or source_end > cutoff
        or (four_fold_axis_available and value["post_cutoff_rows"] < 1)
        or (
            native_prefix is not None
            and (
                type(native_rows) is not int
                or native_rows
                != value["source_rows"] + value["post_cutoff_rows"]
                or cutoff > native_end
                or source_end > native_last_close
                or (
                    four_fold_axis_available
                    and cutoff >= native_end
                )
                or (
                    not four_fold_axis_available
                    and (
                        value["post_cutoff_rows"] != 0
                        or value["source_rows"] != native_rows
                        or cutoff != native_end
                        or source_end != native_last_close
                    )
                )
            )
        )
    ):
        _fail("STATE_ERROR_ENVELOPE_INVALID")
    _hex64(
        value.get("source_price_surface_sha256"),
        "STATE_ERROR_ENVELOPE_SOURCE_HASH_INVALID",
    )
    _hex64(
        value.get("regime_parameters_sha256"),
        "STATE_ERROR_ENVELOPE_REGIME_HASH_INVALID",
    )
    return initial_error, tolerance, int(value["regime_warmup_bars"])


def _verify_state_error_envelope_fitted_binding(
    value: Mapping[str, Any],
    *,
    fitted: search_execution.FittedNativeSearchTimeframe,
    native: Mapping[str, Any],
    m5_view_window: Mapping[str, Any] | None = None,
) -> tuple[float, float, int]:
    """Bind the envelope's non-price terms to the authenticated fit."""

    initial_error, tolerance, regime_warmup_bars = (
        _validated_state_error_envelope(value, native_prefix=_planning_source_bounds(native, m5_view_window))
    )
    expected_tolerance = _state_tolerance(fitted)
    expected_warmup_bars, expected_parameters_sha256 = _regime_warmup_contract(
        fitted
    )
    if (
        tolerance != expected_tolerance
        or regime_warmup_bars != expected_warmup_bars
        or value.get("regime_parameters_sha256") != expected_parameters_sha256
    ):
        _fail("STATE_ERROR_ENVELOPE_FITTED_BINDING_INVALID")
    return initial_error, tolerance, regime_warmup_bars


def verify_preprotected_state_error_envelope(
    fitted: search_execution.FittedNativeSearchTimeframe,
    value: Mapping[str, Any],
    *,
    m5_view_window: Mapping[str, Any] | None = None,
) -> bool:
    """Independently rederive one envelope from its authenticated full frame."""

    if m5_view_window is not None:
        frame, native, opens, closes, actual_window = _validated_m5_frame(fitted)
        if (type(m5_view_window) is not dict
                or _canonical_bytes(m5_view_window) != _canonical_bytes(actual_window)):
            _fail("STATE_ERROR_ENVELOPE_M5_WINDOW_INVALID")
        expected = _freeze_state_error_envelope(
            frame=frame, fitted=fitted, native=native, opens=opens, closes=closes,
            m5_view_window=actual_window,
        )
        if type(value) is not dict or dict(value) != expected:
            _fail("STATE_ERROR_ENVELOPE_SOURCE_REPLAY_INVALID")
        return True
    frame, native, opens, closes = _validated_frame(fitted)
    expected = _freeze_state_error_envelope(
        frame=frame,
        fitted=fitted,
        native=native,
        opens=opens,
        closes=closes,
    )
    if type(value) is not dict or dict(value) != expected:
        _fail("STATE_ERROR_ENVELOPE_SOURCE_REPLAY_INVALID")
    return True


def _effective_state_domains(
    process_body: Mapping[str, Any],
    *,
    symbol: str,
    timeframe: str,
    regime: str,
) -> list[dict[str, Any]]:
    """Bind the frozen registry domain to one explicit production cell."""

    edition = source_edition_info(process_body)
    is_noise = edition.edition_id == NOISE101_SOURCE_EDITION_ID
    domains = list(_domain_by_name(process_body, timeframe=timeframe).values())
    domains.sort(key=lambda value: int(value.get("ordinal", -1)))
    candidate_domain_sha256 = _hex64(
        process_body.get("candidate_domain_sha256"),
        "PROCESS_CANDIDATE_DOMAIN_SHA256_INVALID",
    )
    _state_authority_for_candidate(candidate_domain_sha256)
    effective: list[dict[str, Any]] = []
    for ordinal, domain in enumerate(domains):
        defaults = domain.get("defaults")
        axes = domain.get("grid_axes")
        features = domain.get("required_feature_union")
        if (
            domain.get("schema_version") != DOMAIN_SCHEMA
            or domain.get("ordinal") != ordinal
            or type(defaults) is not dict
            or not defaults
            or type(axes) is not list
            or not axes
            or type(features) is not list
            or any(type(value) is not str or not value for value in features)
            or features != sorted(set(features))
            or domain.get("required_feature_union_derivation")
            != "DEFAULT_PLUS_EVERY_LEGAL_ATOMIC_ROW_WITH_EXHAUSTIVE_LEGAL_RELEASE_PROOF"
            or domain.get("domain_sha256")
            != _sha(
                {
                    key: value
                    for key, value in domain.items()
                    if key != "domain_sha256"
                }
            )
        ):
            _fail("EFFECTIVE_STATE_SOURCE_DOMAIN_INVALID", ordinal)
        parameter_domains = {
            key: [value] for key, value in sorted(defaults.items())
        }
        for axis in axes:
            if (
                type(axis) is not dict
                or set(axis) != {"name", "choices"}
                or type(axis.get("name")) is not str
                or axis["name"] not in parameter_domains
                or type(axis.get("choices")) is not list
                or not axis["choices"]
            ):
                _fail("EFFECTIVE_STATE_DOMAIN_AXIS_INVALID", ordinal)
            parameter_domains[axis["name"]] = list(axis["choices"])
        parameter_domain = domain.get("parameter_domain")
        validate_parameter_domain_member(parameter_domain, defaults)
        full_count = parameter_domain["point_count"]
        if full_count != domain.get("grid_size"):
            _fail("EFFECTIVE_STATE_DOMAIN_COUNT_INVALID", ordinal)
        body = {
            "schema_version": NOISE101_EFFECTIVE_STATE_DOMAIN_SCHEMA if is_noise else EFFECTIVE_STATE_DOMAIN_SCHEMA,
            "cell": {
                "symbol": symbol,
                "timeframe": timeframe,
                "regime": regime,
            },
            "strategy_ordinal": ordinal,
            "strategy_id": domain["strategy_id"],
            "candidate_domain_sha256": candidate_domain_sha256,
            "source_strategy_domain_sha256": domain["domain_sha256"],
            "parameter_domains": parameter_domains,
            "full_domain_point_count": full_count,
            "requested_feature_union": list(features),
            "feature_union_derivation": domain[
                "required_feature_union_derivation"
            ],
            "outcome_or_protected_value_dependent": False,
        }
        if is_noise:
            body["source_edition_context"] = source_edition_context_for_cell(edition, timeframe)
        body["parameter_domain"] = parameter_domain
        effective.append({**body, "effective_domain_sha256": _sha(body)})
    if (
        len(effective) != edition.strategy_count
        or [value["strategy_id"] for value in effective]
        != list(edition.ordered_strategy_ids)
    ):
        _fail("EFFECTIVE_STATE_DOMAIN_ROSTER_INVALID")
    return effective


def _sealed_state_replay_evidence(
    *,
    node: str,
    identity_body: Mapping[str, Any],
    schema_unavailable_code: str,
    error_context: Sequence[Any] = (),
    authority: Optional[state_contract.StateContractAuthority] = None,
) -> tuple[state_contract.StateEvidence, dict[str, Any]]:
    """Bind one authenticated replay identity to the authority's path schema."""

    authority = authority if authority is not None else state_contract.DEFAULT_AUTHORITY
    contract = authority._contracts.get(node)  # noqa: SLF001
    if (
        contract is None
        or contract.kind != state_contract.StateKind.PATH_STATE
        or not contract.checkpoint_schema
        or not contract.transition_schema
    ):
        _fail(schema_unavailable_code, *error_context, node)
    body = {
        **dict(identity_body),
        "state_schema": contract.checkpoint_schema,
        "transition_schema": contract.transition_schema,
    }
    identity = _sha(body)
    return (
        state_contract.StateEvidence(
            node=node,
            mode=state_contract.EvidenceMode.AUTHENTICATED_REPLAY,
            identity_sha256=identity,
            registry_hash=authority.registry_hash,
            state_schema=str(contract.checkpoint_schema),
            transition_schema=str(contract.transition_schema),
        ),
        {**body, "identity_sha256": identity},
    )


def _domain_state_replay_evidence(
    *,
    native_prefix: Mapping[str, Any],
    effective_domain: Mapping[str, Any],
    initial_error_bound: float,
    tolerance: float,
    authority: Optional[state_contract.StateContractAuthority] = None,
    m5_view_window: Mapping[str, Any] | None = None,
) -> tuple[dict[str, state_contract.StateEvidence], list[dict[str, Any]]]:
    parameter_domains = effective_domain.get("parameter_domains")
    requested = effective_domain.get("requested_feature_union")
    if type(parameter_domains) is not dict or type(requested) is not list:
        _fail("STATE_DOMAIN_EVIDENCE_INPUT_INVALID")
    authority = authority if authority is not None else _state_authority_for_candidate(
        effective_domain.get("candidate_domain_sha256")
    )
    parameters = {
        key: values[0]
        for key, values in parameter_domains.items()
        if type(values) is list and values
    }
    if len(parameters) != len(parameter_domains):
        _fail("STATE_DOMAIN_EVIDENCE_PARAMETER_INVALID")
    if effective_domain.get("schema_version") in (EFFECTIVE_STATE_DOMAIN_SCHEMA, NOISE101_EFFECTIVE_STATE_DOMAIN_SCHEMA):
        parameters = dict(effective_domain["parameter_domain"]["defaults"])
    unresolved = authority.resolve_cell(
        str(effective_domain.get("strategy_id") or ""),
        parameters=parameters,
        requested_features=requested,
        timeframe=str(effective_domain.get("cell", {}).get("timeframe") or ""),
        include_regime_controller=False,
        initial_error_bound=initial_error_bound,
        tolerance=tolerance,
    )
    evidence: dict[str, state_contract.StateEvidence] = {}
    bodies: list[dict[str, Any]] = []
    for node in unresolved.required_path_state_nodes:
        identity_body = {
            "schema_version": "fxpm.production-domain-state-replay.v1",
            "node": node,
            "mode": state_contract.EvidenceMode.AUTHENTICATED_REPLAY.value,
            "state_contract_registry_sha256": (
                authority.registry_hash
            ),
            "native_prefix_index_sha256": _hex64(
                native_prefix.get("index_sha256"),
                "STATE_DOMAIN_NATIVE_INDEX_SHA256_INVALID",
            ),
            "native_prefix_content_sha256": _hex64(
                native_prefix.get("content_sha256"),
                "STATE_DOMAIN_NATIVE_CONTENT_SHA256_INVALID",
            ),
            "prefix_start_utc": native_prefix.get("start_utc"),
            "prefix_end_exclusive_utc": native_prefix.get("end_exclusive_utc"),
            "prefix_rows": native_prefix.get("rows"),
            "strategy_id": effective_domain["strategy_id"],
            "effective_domain_sha256": effective_domain[
                "effective_domain_sha256"
            ],
            "requested_feature_union": list(requested),
            "feature_computer_release_sha256": _source_sha256(
                search_execution.FeatureComputer
            ),
            "requires_executor_full_prefix_replay_before_any_scored_slice": True,
        }
        if effective_domain.get("schema_version") == NOISE101_EFFECTIVE_STATE_DOMAIN_SCHEMA:
            identity_body["schema_version"] = NOISE101_DOMAIN_STATE_REPLAY_SCHEMA
            identity_body["source_edition_context"] = dict(effective_domain["source_edition_context"])
        identity_body = _m5_state_binding(identity_body, m5_view_window, "fxpm.production-m5-derived-domain-full-prefix-state-replay.v1")
        item, receipt = _sealed_state_replay_evidence(
            node=node,
            identity_body=identity_body,
            schema_unavailable_code="STATE_DOMAIN_REPLAY_SCHEMA_UNAVAILABLE",
            authority=authority,
        )
        evidence[node] = item
        bodies.append(receipt)
    return evidence, bodies


def _state_domain_ceiling_records(
    process_body: Mapping[str, Any],
    *,
    native_prefix: Mapping[str, Any],
    cell: Mapping[str, Any],
    state_error_envelope: Mapping[str, Any],
    m5_view_window: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Rebuild all 100 cell ceilings from sealed non-market inputs."""

    initial_error, tolerance, regime_warmup_bars = _validated_state_error_envelope(
        state_error_envelope,
        native_prefix=_planning_source_bounds(native_prefix, m5_view_window),
    )
    symbol = _identity(cell.get("symbol"), "STATE_DOMAIN_SYMBOL_INVALID")
    timeframe = _identity(
        cell.get("timeframe"), "STATE_DOMAIN_TIMEFRAME_INVALID"
    ).upper()
    regime = _identity(cell.get("regime"), "STATE_DOMAIN_REGIME_INVALID").upper()
    effective_domains = _effective_state_domains(
        process_body,
        symbol=symbol,
        timeframe=timeframe,
        regime=regime,
    )
    authority = _state_authority_for_candidate(process_body.get("candidate_domain_sha256"))
    edition = source_edition_info(process_body)
    records: list[dict[str, Any]] = []
    for ordinal, effective_domain in enumerate(effective_domains):
        evidence, evidence_bodies = _domain_state_replay_evidence(native_prefix=native_prefix, effective_domain=effective_domain, initial_error_bound=initial_error, tolerance=tolerance, authority=authority, m5_view_window=m5_view_window)
        try:
            ceiling = authority.resolve_cell_domain_ceiling(
                effective_domain["strategy_id"],
                parameter_domains=effective_domain["parameter_domains"],
                requested_features=effective_domain["requested_feature_union"],
                timeframe=timeframe,
                effective_domain_hash=effective_domain[
                    "effective_domain_sha256"
                ],
                include_regime_controller=False,
                initial_error_bound=initial_error,
                tolerance=tolerance,
                state_evidence=evidence,
                parameter_domain=effective_domain.get("parameter_domain"),
            )
        except Exception as exc:
            raise ProductionSearchPlannerError(
                "STATE_DOMAIN_CEILING_RESOLUTION_FAILED",
                (effective_domain["strategy_id"], type(exc).__name__, str(exc)),
            ) from exc
        if (
            not ceiling.proved
            or type(ceiling.bounded_context_bars) is not int
            or ceiling.bounded_context_bars < 0
            or ceiling.full_domain_point_count
            != effective_domain["full_domain_point_count"]
        ):
            _fail(
                "STATE_DOMAIN_CEILING_PROCESS_REJECTED",
                effective_domain["strategy_id"],
                *ceiling.reason_codes,
            )
        body = {
            "schema_version": (NOISE101_STATE_DOMAIN_CEILING_RECORD_SCHEMA if edition.edition_id == NOISE101_SOURCE_EDITION_ID else STATE_DOMAIN_CEILING_RECORD_SCHEMA),
            "strategy_ordinal": ordinal,
            "strategy_id": effective_domain["strategy_id"],
            "effective_domain": effective_domain,
            "state_replay_evidence": evidence_bodies,
            "ceiling": ceiling.to_dict(),
        }
        records.append({**body, "record_sha256": _sha(body)})
    if (
        len(records) != edition.strategy_count
        or len({value["record_sha256"] for value in records}) != len(records)
    ):
        _fail("STATE_DOMAIN_CEILING_ROSTER_INVALID")
    return records, regime_warmup_bars


def _state_replay_requirement(
    *,
    fitted: search_execution.FittedNativeSearchTimeframe,
    native: Mapping[str, Any],
    strategy_name: str,
    wire_parameters: Mapping[str, Any],
    initial_error_bound: float,
    tolerance: float,
    state_domain_record: Mapping[str, Any],
    m5_view_window: Mapping[str, Any] | None = None,
    state_error_envelope: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    record_body = {
        key: value
        for key, value in state_domain_record.items()
        if key != "record_sha256"
    }
    effective_domain = state_domain_record.get("effective_domain")
    ceiling = state_domain_record.get("ceiling")
    if (
        state_domain_record.get("schema_version")
        not in (STATE_DOMAIN_CEILING_RECORD_SCHEMA, NOISE101_STATE_DOMAIN_CEILING_RECORD_SCHEMA)
        or state_domain_record.get("record_sha256") != _sha(record_body)
        or state_domain_record.get("strategy_id") != strategy_name
        or type(effective_domain) is not dict
        or type(ceiling) is not dict
        or effective_domain.get("strategy_id") != strategy_name
        or ceiling.get("strategy_name") != strategy_name
        or ceiling.get("status") != "PROVED"
        or ceiling.get("effective_domain_sha256")
        != effective_domain.get("effective_domain_sha256")
    ):
        _fail("STATE_DOMAIN_ATTEMPT_RECORD_INVALID", strategy_name)
    parameter_domains = effective_domain.get("parameter_domains")
    if (
        type(parameter_domains) is not dict
        or set(wire_parameters) != set(parameter_domains)
        or any(
            type(choices) is not list
            or not choices
            or _parameter_value_key(wire_parameters[key])
            not in {_parameter_value_key(value) for value in choices}
            for key, choices in parameter_domains.items()
        )
    ):
        _fail("STATE_ATTEMPT_OUTSIDE_EFFECTIVE_DOMAIN", strategy_name)
    authority = _state_authority_for_candidate(effective_domain.get("candidate_domain_sha256"))
    if effective_domain.get("schema_version") in (EFFECTIVE_STATE_DOMAIN_SCHEMA, NOISE101_EFFECTIVE_STATE_DOMAIN_SCHEMA):
        try:
            validate_parameter_domain_member(effective_domain["parameter_domain"], wire_parameters)
        except Exception as exc:
            raise ProductionSearchPlannerError(
                "STATE_ATTEMPT_OUTSIDE_EFFECTIVE_DOMAIN", (strategy_name,)
            ) from exc
    base = StrategyRegistry.get(strategy_name)
    try:
        timeframe = str(_planning_source_value(native, m5_view_window, "timeframe"))
        defaults, _, _ = resolve_strategy_parameters(base, timeframe, persisted_parameters=wire_parameters, defaults_only=True)
        parameters = decode_parameter_mapping(dict(wire_parameters), defaults)
        strategy = StrategyRegistry.get(strategy_name, **parameters)
    except Exception as exc:
        raise ProductionSearchPlannerError(
            "STATE_STRATEGY_PARAMETERS_INVALID",
            (strategy_name, type(exc).__name__, str(exc)),
        ) from exc
    requested = sorted(strategy.get_feature_request())
    feature_union = effective_domain.get("requested_feature_union")
    if (
        type(feature_union) is not list
        or feature_union != sorted(set(feature_union))
        or not set(requested).issubset(feature_union)
    ):
        _fail("STATE_ATTEMPT_FEATURE_OUTSIDE_EFFECTIVE_DOMAIN", strategy_name)
    unresolved = authority.resolve_cell(
        strategy_name,
        parameters=parameters,
        requested_features=requested,
        timeframe=str(_planning_source_value(native, m5_view_window, "timeframe")),
        include_regime_controller=False,
        initial_error_bound=initial_error_bound,
        tolerance=tolerance,
    )
    source_context = effective_domain.get("source_edition_context")
    noise_bundle = None
    if source_context is not None and strategy_name == NOISE101_STRATEGY_ID:
        if state_error_envelope is None:
            _fail("NOISE101_STATE_BOUNDARY_REQUIRED")
        noise_bundle = _noise101_preprotected_state_bundle(fitted=fitted, native=native,
            parameters=parameters, source_context=source_context, state_error_envelope=state_error_envelope)
        source_context = noise_bundle["source_edition_context"]
    evidence: dict[str, state_contract.StateEvidence] = {}
    evidence_bodies: list[dict[str, Any]] = []
    for node in unresolved.required_path_state_nodes:
        identity_body = {
            "schema_version": "fxpm.production-full-prefix-state-replay.v1",
            "node": node,
            "mode": state_contract.EvidenceMode.AUTHENTICATED_REPLAY.value,
            "state_contract_registry_sha256": (
                authority.registry_hash
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
            "requires_executor_full_prefix_replay_before_any_scored_slice": True,
        }
        if source_context is not None:
            identity_body["schema_version"] = NOISE101_FULL_PREFIX_STATE_REPLAY_SCHEMA
            identity_body["source_edition_context"] = dict(source_context)
        identity_body = _m5_state_binding(identity_body, m5_view_window, "fxpm.production-m5-derived-full-prefix-state-replay.v1")
        item, receipt = _sealed_state_replay_evidence(
            node=node,
            identity_body=identity_body,
            schema_unavailable_code="STATE_REPLAY_SCHEMA_UNAVAILABLE",
            error_context=(strategy_name,),
            authority=authority,
        )
        evidence[node] = item
        evidence_bodies.append(receipt)
    resolved = authority.resolve_cell(
        strategy_name,
        parameters=parameters,
        requested_features=requested,
        timeframe=str(_planning_source_value(native, m5_view_window, "timeframe")),
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
    ceiling_bars = ceiling.get("bounded_context_bars")
    if type(ceiling_bars) is not int or resolved.bounded_context_bars > ceiling_bars:
        _fail("STATE_DOMAIN_CEILING_BREACH", strategy_name)
    body = {
        "schema_version": NOISE101_STATE_REQUIREMENT_SCHEMA if source_context is not None else STATE_REQUIREMENT_SCHEMA,
        "strategy_id": strategy_name,
        "parameters_sha256": _sha(dict(wire_parameters)),
        "effective_domain_sha256": effective_domain[
            "effective_domain_sha256"
        ],
        "state_domain_ceiling_decision_sha256": ceiling["decision_hash"],
        "state_domain_ceiling_bars": ceiling_bars,
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
    if source_context is not None:
        body["source_edition_context"] = dict(source_context)
        if noise_bundle is not None:
            body["recorded_session_state"] = noise_bundle
    if m5_view_window is not None:
        body = _m5_state_binding(body, m5_view_window, M5_DERIVED_STATE_REQUIREMENT_SCHEMA)
        body["regime_surface_source"] = "FITTED_FULL_M5_DERIVED_PREFIX_REGIME_LIVE"
    return {**body, "requirement_sha256": _sha(body)}


def _state_requirements(
    *,
    fitted: search_execution.FittedNativeSearchTimeframe,
    native: Mapping[str, Any],
    designs: Sequence[Mapping[str, Any]],
    state_error_envelope: Mapping[str, Any],
    state_domain_records: Sequence[Mapping[str, Any]],
    m5_view_window: Mapping[str, Any] | None = None,
    source_edition: SourceEditionDescriptor | None = None,
) -> tuple[list[dict[str, Any]], int]:
    initial_error, tolerance, regime_warmup_bars = (
        _verify_state_error_envelope_fitted_binding(state_error_envelope, fitted=fitted, native=native, m5_view_window=m5_view_window)
    )
    by_strategy = {
        str(record.get("strategy_id") or ""): record
        for record in state_domain_records
        if type(record) is dict
    }
    if (
        len(state_domain_records) != (source_edition.strategy_count if source_edition is not None else BASELINE_ATTEMPTS)
        or len(by_strategy) != (source_edition.strategy_count if source_edition is not None else BASELINE_ATTEMPTS)
    ):
        _fail("STATE_DOMAIN_CEILING_ROSTER_INVALID")
    requirements = []
    for design in designs:
        strategy_name = str(design["strategy_id"])
        record = by_strategy.get(strategy_name)
        if record is None:
            _fail("STATE_DOMAIN_CEILING_MISSING", strategy_name)
        requirements.append(
            _state_replay_requirement(fitted=fitted, native=native, strategy_name=strategy_name, wire_parameters=dict(design['parameters']), initial_error_bound=initial_error, tolerance=tolerance, state_domain_record=record, m5_view_window=m5_view_window, state_error_envelope=state_error_envelope)
        )
    return requirements, regime_warmup_bars


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
    native: Mapping[str, Any],
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    timeframe: str,
    depth_days: int,
    geometry: Mapping[str, int],
    state_context_bars: int,
    m5_view_window: Mapping[str, Any] | None = None,
) -> tuple[pd.Timestamp, list[dict[str, Any]]] | None:
    prefix_end = pd.Timestamp(
        _utc(_planning_source_value(native, m5_view_window, "prefix_end_exclusive_utc"), "PREFIX_END_INVALID")
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
            "prefix_content_sha256": _planning_source_value(native, m5_view_window, "prefix_content_sha256"),
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
    native: Mapping[str, Any],
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    timeframe: str,
    geometry: Mapping[str, int],
    state_context_bars: int,
    m5_view_window: Mapping[str, Any] | None = None,
) -> tuple[int, pd.Timestamp, list[dict[str, Any]], bool]:
    ladder = FIT_CALENDAR_DAY_LADDERS[timeframe]
    for depth_days in reversed(ladder):
        result = _try_causal_folds(native=native, opens=opens, closes=closes, timeframe=timeframe, depth_days=depth_days, geometry=geometry, state_context_bars=state_context_bars, m5_view_window=m5_view_window)
        if result is not None:
            candidate_start, folds = result
            return depth_days, candidate_start, folds, False
    prefix_start = pd.Timestamp(
        _utc(_planning_source_value(native, m5_view_window, "prefix_start_utc"), "PREFIX_START_INVALID")
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
    native: Mapping[str, Any],
    opens: pd.DatetimeIndex,
    closes: pd.DatetimeIndex,
    timeframe: str,
    state_domain_records: Sequence[Mapping[str, Any]],
    regime_warmup_bars: int,
    m5_view_window: Mapping[str, Any] | None = None,
    source_edition: SourceEditionDescriptor | None = None,
) -> dict[str, Any]:
    """Select geometry from exhaustive domain ceilings, never sampled attempts.

    Winner comparisons require one paired fold axis.  A single extreme strategy
    must not veto lower-context siblings, so source geometry is tested at each
    exhaustive per-strategy ceiling from largest to smallest.  Exact attempts
    are admitted against their strategy ceiling and typed against the selected
    value later; changing a sampler therefore cannot change fold geometry.
    """

    if (
        len(state_domain_records) != (source_edition.strategy_count if source_edition is not None else BASELINE_ATTEMPTS)
        or type(regime_warmup_bars) is not int
        or regime_warmup_bars < 1
    ):
        _fail("STATE_CONTEXT_SELECTION_INPUT_INVALID")
    contexts: list[int] = []
    for record in state_domain_records:
        ceiling = record.get("ceiling") if type(record) is dict else None
        bounded = ceiling.get("bounded_context_bars") if type(ceiling) is dict else None
        if type(bounded) is not int or bounded < 0:
            _fail("STATE_DOMAIN_CEILING_INVALID")
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
        depth_days, candidate_start, folds, boundary_limited = _build_causal_folds(native=native, opens=opens, closes=closes, timeframe=timeframe, geometry=geometry, state_context_bars=state_context_bars, m5_view_window=m5_view_window)
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
    state_domain_records: Sequence[Mapping[str, Any]],
    dispositions: Sequence[Mapping[str, Any]],
    *,
    selected_common_context_bars: int,
    boundary_limited: bool,
    source_edition: SourceEditionDescriptor | None = None,
) -> dict[str, Any]:
    if (
        len(state_domain_records) != (source_edition.strategy_count if source_edition is not None else BASELINE_ATTEMPTS)
        or not dispositions
        or type(boundary_limited) is not bool
    ):
        _fail("STATE_CONTEXT_SELECTION_ROSTER_INVALID")
    record_hashes = [
        _hex64(
            value.get("record_sha256"),
            "STATE_DOMAIN_CEILING_RECORD_SHA256_INVALID",
        )
        for value in state_domain_records
    ]
    ceiling_hashes = [
        _hex64(
            value.get("ceiling", {}).get("decision_hash"),
            "STATE_DOMAIN_CEILING_DECISION_SHA256_INVALID",
        )
        for value in state_domain_records
    ]
    ceiling_contexts = [
        value.get("ceiling", {}).get("bounded_context_bars")
        for value in state_domain_records
    ]
    if any(type(value) is not int or value < 0 for value in ceiling_contexts):
        _fail("STATE_DOMAIN_CEILING_CONTEXT_INVALID")
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
        "schema_version": (NOISE101_STATE_CONTEXT_SELECTION_SCHEMA if source_edition is not None and source_edition.edition_id == NOISE101_SOURCE_EDITION_ID else STATE_CONTEXT_SELECTION_SCHEMA),
        "selection_rule": (
            "LARGEST_SOURCE_GEOMETRY_FEASIBLE_EXHAUSTIVE_DOMAIN_CEILING_"
            "DESCENDING"
        ),
        "selected_common_context_bars": selected_common_context_bars,
        "candidate_boundary_limited": boundary_limited,
        "state_domain_ceiling_record_sha256": record_hashes,
        "state_domain_ceiling_record_roster_sha256": _sha(record_hashes),
        "state_domain_ceiling_decision_sha256": ceiling_hashes,
        "state_domain_ceiling_context_roster_sha256": _sha(ceiling_contexts),
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
        != process.to_dict()["historical_adapter_release_sha256"]
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
    _require_current_process(process)
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
        "schema_version": "fxpm.historical-horizon-request.v3",
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
    _require_current_process(process)
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
    pair_cost_policy_sha256, frozen_tier_pair_contract_sha256 = (
        _frozen_tier_horizon_hashes(fitted.frozen_tier_pair_contract)
    )
    horizon_policy = {
        "schema_version": "fxpm.historical-horizon-policy.v3",
        "policy_id": f"fxpm-production-unobserved-{timeframe}-v1",
        "policy_release_sha256": PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256,
        "cost_contract_sha256": pair_cost_policy_sha256,
        "frozen_tier_pair_contract_sha256": frozen_tier_pair_contract_sha256,
        "evaluator_release_sha256": (
            search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
        ),
    }
    request_value = {
        "schema_version": "fxpm.historical-horizon-unobserved-request.v3",
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
    declared_slot_request: Any = None,
) -> ProductionCellSearchPlan:
    return _build_observed_cell_plan_source(context, process=process, fitted=fitted, regime=regime, anchor=anchor, run_id=run_id, causal_precommit_receipt_sha256=causal_precommit_receipt_sha256, mode=mode, namespace=namespace, sink=sink, bounded_canary_request=bounded_canary_request, bounded_canary_source_session=bounded_canary_source_session, bounded_canary_plan_permit=bounded_canary_plan_permit, declared_slot_request=declared_slot_request, m5_derived=False)


def _build_observed_cell_plan_source(
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
    m5_derived: bool,
    bounded_canary_request: Any = None,
    bounded_canary_source_session: Any = None,
    bounded_canary_plan_permit: Any = None,
    declared_slot_request: Any = None,
) -> ProductionCellSearchPlan:
    """Build one sealed cell plan after its outer authority is verified."""

    verify_frozen_production_search_process(context, process)
    _require_current_process(process)
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
    m5_view_window = None
    if m5_derived:
        if normalized_mode != "DECLARED_SLOT_UNISSUED":
            _fail("M5_CELL_PLAN_MODE_INVALID")
        frame, native, opens, closes, m5_view_window = _validated_m5_frame(fitted)
    else:
        frame, native, opens, closes = _validated_frame(fitted)
    symbol = str(native.get("symbol") or "")
    timeframe = str(_planning_source_get(native, m5_view_window, "timeframe") or "")
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
    edition = source_edition_info(process_body)
    state_error_envelope = _freeze_state_error_envelope(frame=frame, fitted=fitted, native=native, opens=opens, closes=closes, m5_view_window=m5_view_window)
    cell_identity = {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": normalized_regime,
    }
    native_binding = {
        "index_sha256": fitted.product.index_sha256,
        "content_sha256": native["prefix_content_sha256"],
        "start_utc": native["prefix_start_utc"],
        "end_exclusive_utc": native["prefix_end_exclusive_utc"],
        "last_close_utc": native["prefix_last_close_utc"],
        "rows": native["prefix_rows"],
    }
    state_domain_records, regime_warmup_bars = _state_domain_ceiling_records(process_body, native_prefix=native_binding, cell=cell_identity, state_error_envelope=state_error_envelope, m5_view_window=m5_view_window)
    state_selection = _select_feasible_common_state_context(native=native, opens=opens, closes=closes, timeframe=timeframe, state_domain_records=state_domain_records, regime_warmup_bars=regime_warmup_bars, m5_view_window=m5_view_window, source_edition=edition)
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
    designs = _attempt_designs(
        process_body,
        symbol=symbol,
        timeframe=timeframe,
        regime=normalized_regime,
    )
    state_requirements, exact_regime_warmup_bars = _state_requirements(fitted=fitted, native=native, designs=designs, state_error_envelope=state_error_envelope, state_domain_records=state_domain_records, m5_view_window=m5_view_window, source_edition=edition)
    if exact_regime_warmup_bars != regime_warmup_bars:
        _fail("STATE_DOMAIN_REGIME_WARMUP_MISMATCH")
    state_dispositions = _state_context_dispositions(
        state_requirements,
        candidate=candidate,
        selected_common_context_bars=state_context_bars,
    )
    state_context_selection = _state_context_selection_receipt(
        state_domain_records,
        state_dispositions,
        selected_common_context_bars=state_context_bars,
        boundary_limited=boundary_limited,
        source_edition=edition,
    )
    fit_end = pd.Timestamp(
        _utc(_planning_source_value(native, m5_view_window, "prefix_end_exclusive_utc"), "PREFIX_END_INVALID")
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
    artifact_source_sha256 = str(_planning_source_value(native, m5_view_window, "prefix_content_sha256"))
    artifact_fit_start_utc = _utc_text(current_fit_start)
    artifact_fit_end_utc = (str(native["prefix_end_exclusive_utc"]) if m5_view_window is None
        else _utc_text(_planning_source_value(native, m5_view_window, "prefix_end_exclusive_utc")))
    artifact_latest_closed_bar_utc = (str(native["prefix_last_close_utc"]) if m5_view_window is None
        else _utc_text(_planning_source_value(native, m5_view_window, "prefix_last_close_utc")))
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
    pair_cost_policy_sha256, frozen_tier_pair_contract_sha256 = (
        _frozen_tier_horizon_hashes(fitted.frozen_tier_pair_contract)
    )
    request_value = {
        "schema_version": "fxpm.historical-horizon-request.v3",
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
            "schema_version": "fxpm.historical-horizon-policy.v3",
            "policy_id": f"fxpm-production-horizon-{timeframe}-v1",
            "policy_release_sha256": PRODUCTION_SEARCH_PLANNER_RELEASE_SHA256,
            "cost_contract_sha256": pair_cost_policy_sha256,
            "frozen_tier_pair_contract_sha256": frozen_tier_pair_contract_sha256,
            "evaluator_release_sha256": (
                search_execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256
            ),
        },
        "candidates": [candidate],
        "trials": trials,
    }
    if m5_view_window is not None:
        request_value.update(schema_version="fxpm.m5-derived-historical-horizon-request.v1",
            source_kind="m5_derived", target_view_window=dict(m5_view_window))
    try:
        horizon_plan = (training_horizon.build_m5_derived_historical_horizon_plan(request_value)
            if m5_view_window is not None else training_horizon.build_historical_horizon_plan(request_value))
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
        or len(plan_body.get("attempts", [])) != edition.attempt_count
        or plan_body.get("anchor", {}).get("process_precommit_receipt_sha256")
        != causal_precommit_receipt_sha256
    ):
        _fail("HORIZON_PLAN_OUTPUT_BINDING_INVALID")
    encoded_artifacts = tuple(_canonical_bytes(value) for value in artifacts)
    receipt_json, entry_json = _seal_cell_plan(context, sink=sink, mode=mode, namespace=namespace, process=process, causal_precommit_receipt_sha256=causal_precommit_receipt_sha256, fitted=fitted, regime=normalized_regime, horizon_plan=horizon_plan, candidate=candidate, dependency_payload=payload, dependency_geometry=dependency_geometry, attempt_artifacts=artifacts, state_requirements=state_requirements, state_domain_records=state_domain_records, state_context_selection=state_context_selection, state_error_envelope=state_error_envelope, boundary_limited=boundary_limited, current_refit_source_capture_sha256=_sha(current_source_capture) if current_source_capture is not None else None, bounded_canary_request=bounded_canary_request, bounded_canary_source_session=bounded_canary_source_session, bounded_canary_plan_permit=bounded_canary_plan_permit, declared_slot_request=declared_slot_request, m5_view_window=m5_view_window)
    result = ProductionCellSearchPlan(
        mode=mode,
        native_prefix_product=fitted.product if m5_view_window is not None else None,
        fitted_timeframe=fitted if m5_view_window is not None else None,
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
        _declared_slot_request=declared_slot_request,
    )
    if result.attempt_count != edition.attempt_count:
        _fail("CELL_PLAN_ATTEMPT_ROSTER_INCOMPLETE")
    (verify_m5_derived_production_cell_search_plan if m5_view_window is not None else verify_production_cell_search_plan)(
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
        "pm_regime": _source_sha256(pm_regime),
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
        "state_error_envelope_frozen_before_tuning_and_protected_values": True,
        "protected_state_envelope_breach_fails_closed": True,
        "regime_warmup_derived_from_frozen_parameters": True,
        "dataframe_free_exhaustive_state_domain_ceiling_required": True,
        "state_geometry_independent_of_sampled_attempts": True,
        "effective_per_cell_state_domain_hash_required": True,
        "complete_legal_domain_feature_union_release_proof_required": True,
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
    "reopen_production_search_process",
    "production_search_planner_contract",
    "verify_frozen_production_search_process",
    "verify_current_search_authorization",
    "verify_preprotected_state_error_envelope",
    "verify_production_cell_search_plan",
]


def build_declared_slot_observed_cell_plan(context, *, process, request, fitted):
    """Use the existing observed-cell planner under an explicit unissued slot."""
    import pm_historical_search_producer as producer
    body = historical.verify_declared_slot_producer_request(context, request, process=process)
    session = producer._verified_declared_slot_fitted(context, request=request, process=process, fitted=fitted)
    anchor = {**body["anchor"], "evidence_accessed_at_utc": session.to_dict()["source_accessed_at_utc"]}
    return _build_observed_cell_plan(context, process=process, fitted=fitted,
        regime=body["regime"], anchor=anchor, run_id=body["run_id"],
        causal_precommit_receipt_sha256=body["precommit_sha256"],
        mode="DECLARED_SLOT_UNISSUED", namespace=body["artifact_namespace"],
        sink=request.symbol_sink(body["symbol"]), declared_slot_request=request)


__all__ += ["build_declared_slot_observed_cell_plan"]

def build_declared_slot_m5_derived_observed_cell_plan(context, *, process, request, fitted):
    """Build only from the original declared request and live registered M5 view."""
    import pm_historical_search_producer as producer
    body = historical.verify_declared_slot_producer_request(context, request, process=process)
    session = producer._verified_declared_slot_fitted(context, request=request, process=process, fitted=fitted)
    search_execution.m5_derived_fitted_frame(fitted)
    anchor = {**body["anchor"], "evidence_accessed_at_utc": session.to_dict()["source_accessed_at_utc"]}
    return _build_observed_cell_plan_source(context, process=process, fitted=fitted,
        regime=body["regime"], anchor=anchor, run_id=body["run_id"],
        causal_precommit_receipt_sha256=body["precommit_sha256"],
        mode="DECLARED_SLOT_UNISSUED", namespace=body["artifact_namespace"],
        sink=request.symbol_sink(body["symbol"]), declared_slot_request=request, m5_derived=True)

def _read_existing_m5_plan_json(context, descriptor):
    keys = {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
    if (type(descriptor) is not dict or set(descriptor) != keys
            or descriptor["schema_version"] != authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA
            or descriptor["format"] != authority_spool.CANONICAL_JSON_FORMAT
            or type(descriptor["size_bytes"]) is not int
            or not 0 < descriptor["size_bytes"] <= context.limits.max_json_bytes):
        _fail("M5_SAVED_PLAN_DESCRIPTOR_INVALID")
    expected = _hex64(descriptor["sha256"], "M5_SAVED_PLAN_DESCRIPTOR_INVALID")
    try:
        digest, size, encoded = authority_spool._stream_file(context.root, descriptor["relative_path"],
            maximum_bytes=context.limits.max_json_bytes, expected_size=descriptor["size_bytes"],
            collect=True, chunk_bytes=context.limits.stream_chunk_bytes)
    except Exception as error:
        raise ProductionSearchPlannerError("M5_SAVED_PLAN_ORIGINAL_MISSING_OR_CHANGED",
            (type(error).__name__,)) from error
    if encoded is None or digest != expected or size != descriptor["size_bytes"]:
        _fail("M5_SAVED_PLAN_ORIGINAL_CHANGED")
    try:
        value = authority_spool._strict_json_bytes(encoded, context.limits.max_json_bytes)
    except Exception as error:
        raise ProductionSearchPlannerError("M5_SAVED_PLAN_JSON_INVALID", (type(error).__name__,)) from error
    if type(value) is not dict or _canonical_bytes(value, maximum=context.limits.max_json_bytes) != encoded:
        _fail("M5_SAVED_PLAN_JSON_INVALID")
    return encoded, value


def reopen_declared_slot_m5_derived_observed_cell_plan(
    context, *, process, request, fitted, sink, plan_transport, horizon_plan,
):
    """Restore existing bytes only; neither missing headers nor artifacts are created."""
    if (type(context) is not authority_spool.GenerationSpoolContext
            or type(sink) is not authority_spool.SymbolAuthoritySink
            or type(plan_transport) is not dict
            or set(plan_transport) != {"cell_receipt", "spool_entry", "attempt_artifacts", "state_requirements"}
            or type(horizon_plan) is not dict or set(horizon_plan) != {"descriptor", "receipt_sha256"}):
        _fail("M5_SAVED_PLAN_TRANSPORT_INVALID")
    verify_frozen_production_search_process(context, process)
    _require_current_process(process)
    declared = historical.verify_declared_slot_producer_request(context, request, process=process)
    if sink is not request.symbol_sink(declared["symbol"]):
        _fail("M5_SAVED_PLAN_SINK_MISMATCH")
    original = plan_transport["spool_entry"]
    artifact_id = (f"{_artifact_namespace(declared['artifact_namespace'])}/search-cell-plan/"
        f"{declared['timeframe']}/{declared['regime']}")
    if (type(original) is not dict or original.get("artifact_id") != artifact_id
            or original.get("artifact_type") != CELL_PLAN_ARTIFACT_TYPE
            or sink._artifacts.get(artifact_id) != original):
        _fail("M5_SAVED_PLAN_ORIGINAL_MISSING")
    encoded_cell, receipt = _read_existing_m5_plan_json(context, original.get("descriptor"))
    if (receipt != plan_transport["cell_receipt"] or set(receipt) != _M5_CELL_PLAN_FIELDS
            or receipt.get("schema_version") != M5_DERIVED_CELL_PLAN_SCHEMA
            or receipt.get("source_kind") != "m5_derived"
            or receipt.get("receipt_sha256") != _sha({key: value for key, value in receipt.items()
                if key != "receipt_sha256"})
            or receipt.get("generation_id") != context.generation_id
            or receipt.get("generation_context_sha256") != context.context_sha256
            or receipt.get("search_process_receipt_sha256") != process.receipt_sha256
            or receipt.get("declared_slot_request_sha256") != request.receipt_sha256
            or receipt.get("cell") != {key: declared[key] for key in ("symbol", "timeframe", "regime")}):
        _fail("M5_SAVED_PLAN_BINDING_MISMATCH")
    encoded_horizon, horizon_body = _read_existing_m5_plan_json(context, horizon_plan["descriptor"])
    if horizon_plan["descriptor"]["sha256"] != horizon_plan["receipt_sha256"]:
        _fail("M5_SAVED_HORIZON_BINDING_INVALID")
    restored_horizon = training_horizon.reopen_m5_derived_historical_horizon_plan(
        encoded_horizon, horizon_plan["receipt_sha256"])
    frame, native, opens, closes, window = _validated_m5_frame(fitted)
    fit_context = getattr(getattr(fitted, "_derived_binding", None), "context", None)
    if (type(fit_context) is not authority_spool.GenerationSpoolContext
            or fit_context.root != context.root or fit_context.context_sha256 != context.context_sha256
            or receipt.get("m5_view_window") != window or horizon_body.get("m5_view_window") != window
            or horizon_body.get("native_prefix") != native
            or receipt.get("horizon_plan_binding") != historical.historical_horizon_plan_binding(restored_horizon)
            or receipt.get("source_group_decision_sha256") != fitted.source_group_decision.receipt_sha256
            or receipt.get("regime_fit_decision_sha256") != fitted.regime_fit_decision.receipt_sha256
            or receipt.get("train_regime_support_sha256") != fitted.train_regime_support["receipt_sha256"]):
        _fail("M5_SAVED_PLAN_LIVE_BINDING_MISMATCH")
    anchor = horizon_body.get("anchor")
    if type(anchor) is not dict or any(anchor.get(key) != value for key, value in declared["anchor"].items()
            if key != "evidence_accessed_at_utc"):
        _fail("M5_SAVED_PLAN_ANCHOR_MISMATCH")
    _validate_available_prefix_anchor_binding(native=native, prefix_index=fitted.product.index,
        anchor=anchor, canary_request_body=None)
    attempts = plan_transport["attempt_artifacts"]
    states = plan_transport["state_requirements"]
    if (type(attempts) is not list or type(states) is not list
            or any(type(value) is not dict for value in (*attempts, *states))
            or receipt.get("attempt_artifact_sha256") != [_sha(value) for value in attempts]
            or receipt.get("state_requirement_sha256") != [value.get("requirement_sha256") for value in states]):
        _fail("M5_SAVED_PLAN_ARTIFACTS_CHANGED")
    result = ProductionCellSearchPlan(mode="DECLARED_SLOT_UNISSUED", symbol=declared["symbol"],
        timeframe=declared["timeframe"], regime=declared["regime"],
        causal_precommit_receipt_sha256=declared["precommit_sha256"],
        search_process_receipt_sha256=process.receipt_sha256, horizon_plan=restored_horizon,
        _attempt_artifacts_json=tuple(_canonical_bytes(value) for value in attempts),
        _state_requirements_json=tuple(_canonical_bytes(value) for value in states),
        _cell_receipt_json=encoded_cell, _spool_entry_json=_canonical_bytes(original, maximum=256*1024),
        boundary_limited=receipt["boundary_limited"], _seal=_CELL_PLAN_SEAL,
        native_prefix_product=fitted.product, fitted_timeframe=fitted, _declared_slot_request=request)
    _verify_production_cell_search_plan_source(context, process=process, sink=sink, plan=result,
        m5_derived=True, read_only=True)
    return result


def describe_route_a_evaluation_design(*, frozen_domains: Any,
                                     configurations: Any) -> dict[str, Any]:
    """Describe legal evaluation identities without issuing or executing a process.

    Supplied process, source and fitted-state bindings remain caller assertions.
    Equal keys describe potential reuse, never an actual evaluation or decision.
    Use per-fold or per-tier design calls if the unchanged result codec limit is reached.
    """
    prefix = "ROUTE_A_DESIGN_"

    def encode(value, suffix, *, final_result=False):
        try:
            return _canonical_bytes(value)
        except ProductionSearchPlannerError as exc:
            if final_result and exc.code == "CANONICAL_JSON_SIZE_INVALID":
                raise ProductionSearchPlannerError(prefix + "RESULT_TOO_LARGE") from exc
            raise ProductionSearchPlannerError(prefix + suffix) from exc
        except (ValueError, TypeError, OverflowError, RecursionError) as exc:
            raise ProductionSearchPlannerError(prefix + suffix) from exc

    def snapshot(value, suffix, *, final_result=False):
        encoded = encode(value, suffix, final_result=final_result)
        try:
            return json.loads(encoded)
        except (ValueError, TypeError, OverflowError, RecursionError) as exc:
            raise ProductionSearchPlannerError(prefix + suffix) from exc

    def canonical_hash(value, suffix):
        return hashlib.sha256(encode(value, suffix)).hexdigest()

    def timestamp(value):
        if (type(value) is not str or len(value) not in {20, 25}
                or value[4] != "-" or value[7] != "-" or value[10] != "T"
                or value[13] != ":" or value[16] != ":"
                or value[19:] not in {"Z", "+00:00"}
                or not all(value[index].isascii() and value[index].isdigit()
                           for index in (0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18))):
            _fail(prefix + "BINDING_INVALID")
        return _utc(value, prefix + "BINDING_INVALID")

    unit_fields = {"generation_context_sha256", "producer_request_sha256",
        "executor_release_sha256", "producer_source_sha256", "horizon_plan_receipt_sha256",
        "cell_plan_receipt_sha256", "source_group_sha256", "regime_fit_sha256", "frame_sha256",
        "fitted_state_reference_sha256", "execution_spec_sha256", "frozen_receipt_sha256",
        "cost_precommitments_sha256", "fold_plan_sha256", "attempt_roster_sha256"}
    hash_fields = {"search_process_receipt_sha256", "config_sha256", "effective_config_sha256",
        "source_contract_sha256", "partition_source_contract_sha256", "spread_model_sha256"}
    binding_fields = hash_fields | {"declared_unit_binding", "symbol", "timeframe", "regime",
        "anchor_id", "selection_cutoff_utc", "fold_id", "fold_ordinal", "partition_geometry",
        "spread_tier"}
    geometry_fields = {"schema_version", "role", "frame_start_utc", "scored_start_utc",
        "end_exclusive_utc", "warmup_bars", "scored_bars", "frame_rows", "frame_sha256",
        "receipt_sha256"}
    protected = datetime(2024, 7, 14, 8, 5, tzinfo=UTC)

    def evaluation_binding(value):
        if type(value) is not dict or set(value) != binding_fields:
            _fail(prefix + "BINDING_INVALID")
        geometry = value["partition_geometry"]
        if type(geometry) is not dict or set(geometry) != geometry_fields:
            _fail(prefix + "BINDING_INVALID")
        if type(geometry["role"]) is not str:
            _fail(prefix + "BINDING_INVALID")
        if geometry["role"] == "final_lockbox":
            _fail(prefix + "LOCKBOX_REFUSED")
        if geometry["role"] not in {"optimizer_train", "tuning_validation"}:
            _fail(prefix + "BINDING_INVALID")
        cutoff = timestamp(value["selection_cutoff_utc"])
        start = timestamp(geometry["frame_start_utc"])
        scored = timestamp(geometry["scored_start_utc"])
        end = timestamp(geometry["end_exclusive_utc"])
        if max(cutoff, start, scored, end) > protected:
            _fail(prefix + "PROTECTED_CUTOFF_EXCEEDED")
        if not start <= scored < end <= cutoff:
            _fail(prefix + "BINDING_INVALID")
        if (type(geometry["schema_version"]) is not str
                or geometry["schema_version"] != "fxpm.winner-partition-geometry.v1"
                or type(geometry["warmup_bars"]) is not int or geometry["warmup_bars"] < 0
                or type(geometry["scored_bars"]) is not int or geometry["scored_bars"] <= 0
                or type(geometry["frame_rows"]) is not int
                or geometry["frame_rows"] != geometry["warmup_bars"] + geometry["scored_bars"]):
            _fail(prefix + "BINDING_INVALID")
        for field in ("frame_sha256", "receipt_sha256"):
            _hex64(geometry[field], prefix + "BINDING_INVALID")
        if geometry["receipt_sha256"] != canonical_hash(
                {key: item for key, item in geometry.items() if key != "receipt_sha256"},
                "BINDING_INVALID"):
            _fail(prefix + "BINDING_INVALID")
        unit = value["declared_unit_binding"]
        if type(unit) is not dict or set(unit) != unit_fields:
            _fail(prefix + "BINDING_INVALID")
        for field in unit_fields:
            _hex64(unit[field], prefix + "BINDING_INVALID")
        for field in hash_fields:
            _hex64(value[field], prefix + "BINDING_INVALID")
        for field in ("symbol", "timeframe", "anchor_id", "fold_id"):
            _identity(value[field], prefix + "BINDING_INVALID")
        if (value["timeframe"] not in TIMEFRAMES
                or (value["regime"] is not None
                    and (type(value["regime"]) is not str or value["regime"] not in REGIMES))
                or type(value["spread_tier"]) is not str or value["spread_tier"] not in {"P75", "P95"}
                or type(value["fold_ordinal"]) is not int
                or not 0 <= value["fold_ordinal"] < CAUSAL_FOLD_COUNT):
            _fail(prefix + "BINDING_INVALID")
        result = snapshot(value, "BINDING_INVALID")
        result["selection_cutoff_utc"] = _utc_text(cutoff)
        return result

    if type(frozen_domains) not in {list, tuple} or not frozen_domains:
        _fail(prefix + "INPUT_INVALID")
    if type(configurations) not in {list, tuple}:
        _fail(prefix + "INPUT_INVALID")
    domains = {}
    for supplied in frozen_domains:
        if (type(supplied) is not dict
                or not {"strategy_id", "strategy_family", "parameter_domain", "domain_sha256"} <= set(supplied)):
            _fail(prefix + "DOMAIN_INVALID")
        strategy = _identity(supplied["strategy_id"], prefix + "DOMAIN_INVALID")
        _identity(supplied["strategy_family"], prefix + "DOMAIN_INVALID")
        _hex64(supplied["domain_sha256"], prefix + "DOMAIN_INVALID")
        if strategy in domains or type(supplied["parameter_domain"]) is not dict:
            _fail(prefix + "DOMAIN_INVALID")
        domain = snapshot(supplied, "DOMAIN_INVALID")
        if domain["domain_sha256"] != canonical_hash(
                {key: value for key, value in domain.items() if key != "domain_sha256"},
                "DOMAIN_INVALID"):
            _fail(prefix + "DOMAIN_INVALID")
        parameter_domain = domain["parameter_domain"]
        try:
            validate_parameter_domain_member(parameter_domain, parameter_domain.get("defaults"))
        except ValueError as exc:
            raise ProductionSearchPlannerError(prefix + "DOMAIN_INVALID") from exc
        domains[strategy] = domain

    rows = []
    first_requests = {}
    for ordinal, supplied in enumerate(configurations):
        required = {"strategy_id", "parameters", "evaluation_binding"}
        if (type(supplied) is not dict or not required <= set(supplied)
                or not set(supplied) <= required | {"bookkeeping"}):
            _fail(prefix + "INPUT_INVALID")
        strategy = _identity(supplied["strategy_id"], prefix + "DOMAIN_INVALID")
        if strategy not in domains:
            _fail(prefix + "DOMAIN_INVALID")
        bookkeeping = supplied.get("bookkeeping", {})
        if (type(bookkeeping) is not dict
                or not set(bookkeeping) <= {"request_ordinal", "attempt_ordinal", "horizon_attempt_id"}):
            _fail(prefix + "INPUT_INVALID")
        for field, value in bookkeeping.items():
            if field == "horizon_attempt_id":
                _identity(value, prefix + "INPUT_INVALID")
            elif type(value) is not int or value < 0:
                _fail(prefix + "INPUT_INVALID")
        binding = evaluation_binding(supplied["evaluation_binding"])
        domain = domains[strategy]
        try:
            parameters = validate_parameter_domain_member(domain["parameter_domain"], supplied["parameters"])
        except ValueError as exc:
            raise ProductionSearchPlannerError(prefix + "PARAMETERS_INVALID") from exc
        key = {"strategy_id": strategy, "strategy_family": domain["strategy_family"],
            "domain_sha256": domain["domain_sha256"], "parameters": parameters,
            "evaluation_binding": binding}
        encoded_key = encode(key, "INPUT_INVALID")
        previous = first_requests.get(encoded_key)
        if previous is None:
            first_requests[encoded_key] = ordinal
        rows.append({"request_ordinal": ordinal, "strategy_id": strategy,
            "strategy_family": domain["strategy_family"],
            "domain_cardinality": domain["parameter_domain"]["point_count"],
            "parameters": parameters, "shared_evaluation_key": key,
            "same_identity_as_request_ordinal": previous,
            "bookkeeping": snapshot(bookkeeping, "INPUT_INVALID")})
    return snapshot({"schema_version": "fxpm.route-a-evaluation-design.v1",
        "status": "DESIGN_ONLY", "execution_ready": False,
        "provenance_scope": "CALLER_BOUND_METADATA_ONLY", "requests": rows,
        "policy_reference": "E-00004535",
        "shortlist": {"target_size": 5, "status": "UNAVAILABLE", "reason_code": "SELECTION_NOT_IMPLEMENTED"},
        "family_quota": {"status": "UNAVAILABLE", "reason_code": "FAMILY_QUOTA_AUTHORITY_UNAVAILABLE"},
        "stress_on_base_cost": {"status": "UNAVAILABLE", "reason_code": "STRESS_ON_BASE_COST_EVIDENCE_UNAVAILABLE"},
        "sharing_status": "IDENTITY_ONLY_NO_SHARING_DECISION",
        "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False}, "INPUT_INVALID", final_result=True)


__all__.append("describe_route_a_evaluation_design")


def _route_a_full_panel_preparation_contract(*, owner_snapshot,
                                            partition_cost_precommitments):
    """Describe the complete frozen panel; this metadata issues no authority."""
    prefix = "ROUTE_A_FULL_PANEL_"
    def encode(value):
        # Frozen evidence already has its own bounds; it is not a process body.
        return json.dumps(value, allow_nan=False, ensure_ascii=True,
            separators=(",", ":"), sort_keys=True).encode("ascii")
    try:
        if (type(owner_snapshot) is not dict
                or owner_snapshot.get("schema_version") != "fxpm.route-a-owner-membership-snapshot.v2"
                or owner_snapshot.get("provenance_scope") != "CURRENT_OWNER_METADATA_PROJECTION_ONLY"
                or type(owner_snapshot.get("attempt_roster")) is not list
                or not owner_snapshot["attempt_roster"]
                or type(partition_cost_precommitments) is not dict):
            _fail(prefix + "SOURCE_INVALID")
        binding = owner_snapshot["declared_unit_binding"]
        encoded_precommitments = encode(partition_cost_precommitments)
        if (type(binding) is not dict
                or binding["cost_precommitments_sha256"]
                    != hashlib.sha256(encoded_precommitments).hexdigest()
                or type(owner_snapshot["original_trial_count"]) is not int
                or owner_snapshot["original_trial_count"] != len(owner_snapshot["attempt_roster"])):
            _fail(prefix + "PRECOMMITMENT_BINDING_INVALID")
        precommitted_rows = {(row["attempt_ordinal"], row["fold_ordinal"], row["partition"]): row
            for row in partition_cost_precommitments["rows"]}
        if len(precommitted_rows) != len(partition_cost_precommitments["rows"]):
            _fail(prefix + "PRECOMMITMENT_BINDING_INVALID")
        rows, rejected = [], []
        for ordinal, attempt in enumerate(owner_snapshot["attempt_roster"]):
            if (type(attempt) is not dict or attempt["ordinal"] != ordinal
                    or type(attempt["raw_attempt_id"]) is not str
                    or not attempt["raw_attempt_id"]
                    or len(attempt["source_folds"]) != 4
                    or len(attempt["folds"]) != 4):
                _fail(prefix + "ROSTER_INVALID")
            if attempt["status"] == "PROCESS_REJECTED":
                if (type(attempt["rejection"]) is not dict
                        or any(fold["partitions"] != {"status": "UNSCHEDULED"}
                               for fold in attempt["folds"])):
                    _fail(prefix + "REJECTION_INVALID")
                rejected.append({"attempt_ordinal": ordinal,
                    "raw_attempt_id": attempt["raw_attempt_id"],
                    "horizon_attempt_id": attempt["horizon_attempt_id"],
                    "rejection": attempt["rejection"]})
                continue
            if attempt["status"] != "PLANNED" or attempt["rejection"] is not None:
                _fail(prefix + "ROSTER_INVALID")
            for fold_ordinal, (fold, source) in enumerate(zip(
                    attempt["folds"], attempt["source_folds"], strict=True)):
                if (fold["fold_id"] != source["fold_id"]
                        or fold["role"] != "CAUSAL_ROLLING_ORIGIN_INNER"
                        or source["role"] != fold["role"]
                        or set(fold["partitions"]) != {
                            "optimizer_train", "tuning_validation", "final_lockbox"}):
                    _fail(prefix + "FOLD_INVALID")
                for role in ("optimizer_train", "tuning_validation", "final_lockbox"):
                    identity = fold["partitions"][role]
                    geometry_sha = identity["partition_geometry_receipt_sha256"]
                    record = partition_cost_precommitments["records"][geometry_sha]
                    if (identity["precommitment_row"] != precommitted_rows[ordinal, fold_ordinal, role]
                            or identity["partition_frame_sha256"] != record["partition_frame_sha256"]
                            or set(identity["tier_refs"]) != {"P75", "P95"}):
                        _fail(prefix + "PARTITION_INVALID")
                    for tier in ("P75", "P95"):
                        expected_ref = "|".join((partition_cost_precommitments["schema_version"],
                            partition_cost_precommitments["receipt_sha256"], geometry_sha, tier))
                        if identity["tier_refs"][tier] != expected_ref:
                            _fail(prefix + "PARTITION_INVALID")
                        rows.append({"attempt_ordinal": ordinal,
                            "raw_attempt_id": attempt["raw_attempt_id"],
                            "horizon_attempt_id": attempt["horizon_attempt_id"],
                            "fold_ordinal": fold_ordinal, "fold_id": fold["fold_id"],
                            "partition_role": role, "spread_tier": tier,
                            "partition_geometry_receipt_sha256": identity["partition_geometry_receipt_sha256"],
                            "partition_frame_sha256": identity["partition_frame_sha256"]})
        if not rows or len(rows) != (len(owner_snapshot["attempt_roster"]) - len(rejected)) * 24:
            _fail(prefix + "PANEL_INCOMPLETE")
        return json.loads(encode({
            "schema_version": "fxpm.route-a-full-planned-owner-preparation.v1",
            "declared_unit_binding": binding,
            "original_trial_count": owner_snapshot["original_trial_count"],
            "requests": rows, "process_rejections": rejected}).decode("ascii"))
    except ProductionSearchPlannerError:
        raise
    except (KeyError, TypeError, ValueError, IndexError, OverflowError) as exc:
        raise ProductionSearchPlannerError(prefix + "SOURCE_INVALID") from exc


__all__.extend(["ROUTE_A_FULL_PANEL_PROCESS_SCHEMA", "ROUTE_A_FULL_PANEL_EXECUTION_EDITION",
    "freeze_route_a_full_panel_production_search_process", "frozen_search_execution_edition"])


NOISE101_SOURCE_EDITION_ID: Final = "fxpm.noise101-per-timeframe-source-edition.v1"
NOISE101_PROCESS_SCHEMA: Final = "fxpm.production-search-process-precommit.v4"
NOISE101_EXECUTION_EDITION: Final = "fxpm.route-a-noise101-per-timeframe-owner-execution.v1"
NOISE101_PROCESS_SEMANTICS_SCHEMA: Final = "fxpm.production-search-process-semantics.v3"
NOISE101_CATALOGUE_SCHEMA: Final = "fxpm.strategy-candidate-catalogue-by-timeframe.v1"
NOISE101_SOURCE_RELEASE_SCHEMA: Final = "fxpm.source-release-roster.v1"
NOISE101_STRATEGY_ID: Final = "NoiseBandBreakoutStrategy"
NOISE101_STRATEGY_COUNT: Final = 101
NOISE101_ATTEMPT_COUNT: Final = 303
NOISE101_SOFTWARE_QUALIFICATION: Final = "SOFTWARE_DRAFT_UNQUALIFIED"


@dataclass(frozen=True)
class SourceEditionDescriptor:
    edition_id: str
    process_schema: str
    execution_edition: str | None
    catalogue_schema: str | None
    winner_generation_id: str
    ordered_strategy_ids: tuple[str, ...]
    ordered_timeframes: tuple[str, ...]
    strategy_count: int
    nondefault_variants_per_strategy: int
    attempt_count: int
    candidate_catalogue_sha256: str
    domain_roster_sha256_by_timeframe: Mapping[str, str]
    state_registry_sha256: str
    source_release_sha256: str

    @property
    def independent_signal_r_enabled(self) -> bool:
        return self.edition_id == INDEPENDENT_R_SOURCE_EDITION

    @property
    def winner_generation(self):
        return search_execution.winner.winner_generation_for_selection_version(self.winner_generation_id)

    def to_dict(self) -> dict[str, Any]:
        return {"edition_id": self.edition_id, "process_schema": self.process_schema,
            "execution_edition": self.execution_edition, "catalogue_schema": self.catalogue_schema,
            "winner_generation_id": self.winner_generation_id,
            "ordered_strategy_ids": list(self.ordered_strategy_ids),
            "ordered_timeframes": list(self.ordered_timeframes),
            "strategy_count": self.strategy_count,
            "nondefault_variants_per_strategy": self.nondefault_variants_per_strategy,
            "attempt_count": self.attempt_count,
            "candidate_catalogue_sha256": self.candidate_catalogue_sha256,
            "domain_roster_sha256_by_timeframe": dict(self.domain_roster_sha256_by_timeframe),
            "state_registry_sha256": self.state_registry_sha256,
            "source_release_sha256": self.source_release_sha256}


def _edition_process_body(process_or_body):
    if type(process_or_body) is FrozenProductionSearchProcess:
        return process_or_body.to_dict()
    if isinstance(process_or_body, Mapping):
        return process_or_body
    _fail("PROCESS_OBJECT_INVALID")


def is_route_a_full_panel_process(process_or_body) -> bool:
    """Select a verified caller's route without repeating source/context checks."""
    body = _edition_process_body(process_or_body)
    return ((body.get("schema_version") == ROUTE_A_FULL_PANEL_PROCESS_SCHEMA
             and body.get("execution_edition") == ROUTE_A_FULL_PANEL_EXECUTION_EDITION)
        or (body.get("schema_version") == NOISE101_PROCESS_SCHEMA
             and body.get("execution_edition") == NOISE101_EXECUTION_EDITION)
        or (body.get("schema_version") == INDEPENDENT_R_PROCESS_SCHEMA
             and body.get("execution_edition") == INDEPENDENT_R_EXECUTION_EDITION))


def full_panel_generation(process_or_body):
    body = _edition_process_body(process_or_body)
    if not is_route_a_full_panel_process(body):
        return None
    return {ROUTE_A_FULL_PANEL_PROCESS_SCHEMA: search_execution.winner.V17_GENERATION,
        NOISE101_PROCESS_SCHEMA: search_execution.winner.V18_GENERATION,
        INDEPENDENT_R_PROCESS_SCHEMA: search_execution.winner.V19_GENERATION}[body["schema_version"]]


def source_edition_info(process_or_body) -> SourceEditionDescriptor:
    """Read immutable identities from already-authenticated process metadata."""
    if type(process_or_body) is SourceEditionDescriptor:
        return process_or_body
    body = _edition_process_body(process_or_body)
    schema = body.get("schema_version")
    if schema == NOISE101_PROCESS_SCHEMA:
        value = body.get("source_edition")
        keys = set(SourceEditionDescriptor.__dataclass_fields__)
        if type(value) is not dict or set(value) != keys:
            _fail("SOURCE_EDITION_FIELDS_INVALID")
        if (value["edition_id"] != NOISE101_SOURCE_EDITION_ID
                or value["process_schema"] != NOISE101_PROCESS_SCHEMA
                or value["execution_edition"] != NOISE101_EXECUTION_EDITION
                or value["catalogue_schema"] != NOISE101_CATALOGUE_SCHEMA
                or value["winner_generation_id"] != search_execution.winner.V18_GENERATION.selection_version
                or value["ordered_strategy_ids"] != [*state_contract.ACTUAL_STRATEGY_NAMES, NOISE101_STRATEGY_ID]
                or value["ordered_timeframes"] != list(TIMEFRAMES)
                or value["strategy_count"] != NOISE101_STRATEGY_COUNT
                or value["nondefault_variants_per_strategy"] != VARIANTS_PER_STRATEGY
                or value["attempt_count"] != NOISE101_ATTEMPT_COUNT
                or body.get("execution_edition") != value["execution_edition"]
                or body.get("candidate_domain_sha256") != value["candidate_catalogue_sha256"]
                or body.get("state_contract_registry_sha256") != value["state_registry_sha256"]):
            _fail("SOURCE_EDITION_BINDING_INVALID")
        for name in ("candidate_catalogue_sha256", "state_registry_sha256", "source_release_sha256"):
            _hex64(value[name], "SOURCE_EDITION_HASH_INVALID")
        rosters = value["domain_roster_sha256_by_timeframe"]
        if type(rosters) is not dict or set(rosters) != set(TIMEFRAMES):
            _fail("SOURCE_EDITION_TIMEFRAME_ROSTER_INVALID")
        for digest in rosters.values():
            _hex64(digest, "SOURCE_EDITION_HASH_INVALID")
        return SourceEditionDescriptor(**{**value,
            "ordered_strategy_ids": tuple(value["ordered_strategy_ids"]),
            "ordered_timeframes": tuple(value["ordered_timeframes"]),
            "domain_roster_sha256_by_timeframe": MappingProxyType({tf: rosters[tf] for tf in TIMEFRAMES})})
    if schema not in (PROCESS_SCHEMA, ROUTE_A_FULL_PANEL_PROCESS_SCHEMA, INDEPENDENT_R_PROCESS_SCHEMA):
        _fail("PROCESS_SOURCE_EDITION_UNKNOWN")
    sem = body.get("semantics", {})
    names = tuple(sem.get("registry_order", state_contract.ACTUAL_STRATEGY_NAMES))
    if names != state_contract.ACTUAL_STRATEGY_NAMES:
        _fail("PROCESS_DOMAIN_ROSTER_INVALID")
    generation = {PROCESS_SCHEMA: search_execution.winner.CURRENT_GENERATION,
        ROUTE_A_FULL_PANEL_PROCESS_SCHEMA: search_execution.winner.V17_GENERATION,
        INDEPENDENT_R_PROCESS_SCHEMA: search_execution.winner.V19_GENERATION}[schema]
    roster = sem.get("strategy_domain_roster_sha256", body.get("candidate_domain_sha256", ""))
    return SourceEditionDescriptor(INDEPENDENT_R_SOURCE_EDITION if schema == INDEPENDENT_R_PROCESS_SCHEMA else schema,
        schema, body.get("execution_edition"), None,
        generation.selection_version, names, TIMEFRAMES, BASELINE_ATTEMPTS,
        VARIANTS_PER_STRATEGY, ATTEMPT_COUNT, body.get("candidate_domain_sha256", ""),
        MappingProxyType({tf: roster for tf in TIMEFRAMES}), body.get("state_contract_registry_sha256", ""),
        _sha({name: body.get(name) for name in ("planner_release_sha256",
            "authoritative_executor_release_sha256", "historical_adapter_release_sha256")}))


def process_attempt_count(process_or_body) -> int:
    return source_edition_info(process_or_body).attempt_count


def process_strategy_count(process_or_body) -> int:
    return source_edition_info(process_or_body).strategy_count


def _noise101_source_release_roster() -> dict[str, Any]:
    from pm_causal_evidence_producer import _COMPONENT_SOURCE_FILES
    # Reuse the authoritative component closure, adding the actual search callers.
    files = {name for group in _COMPONENT_SOURCE_FILES.values() for name in group}
    files.update({"pm_production_search_planner.py", "pm_state_contract.py", "pm_pipeline.py",
        "pm_authoritative_search_execution.py", "pm_historical_production_adapter.py",
        "pm_historical_anchor_orchestration.py", "pm_historical_search_producer.py",
        "pm_strategies.py", "pm_current_refit_parameters.py", "pm_current_refit_artifacts.py",
        "pm_current_refit_execution.py", "pm_current_refit_production.py", "pm_optuna.py",
        "pm_main.py", "pm_authority_spool.py", "pm_authority_spool_quota.py",
        "pm_signal_opportunities.py", "pm_trade_power.py", "pm_current_refit_search_producer.py"})
    base = Path(__file__).resolve().parent
    return {"schema_version": NOISE101_SOURCE_RELEASE_SCHEMA,
        "modules": {Path(name).stem: hashlib.sha256((base / name).read_bytes()).hexdigest()
            for name in sorted(files)}}


def _noise101_legacy_domains() -> list[dict[str, Any]]:
    scope = _PROCESS_VERIFICATION_STATE.get()
    if scope is not None and "noise101_legacy_domains" in scope:
        return json.loads(scope["noise101_legacy_domains"].decode("ascii"))
    names = state_contract.ACTUAL_STRATEGY_NAMES
    documents = []
    for name in sorted(names):
        strategy = StrategyRegistry.get(name)
        documents.append({"name": name,
            "defaults": _normalize_candidate_domain_value(strategy.get_default_params()),
            "grid": _normalize_candidate_domain_value(strategy.get_param_grid()),
            "coupled_groups": _normalize_candidate_domain_value(strategy.get_param_couplings())})
    if state_contract.strategy_candidate_domain_sha256(documents) != state_contract.STRATEGY_CANDIDATE_DOMAIN_SHA256:
        _fail("LEGACY100_CANDIDATE_DOMAIN_DRIFT")
    domains = [_strategy_domain(name, ordinal) for ordinal, name in enumerate(names)]
    if scope is not None:
        scope["noise101_legacy_domains"] = _canonical_bytes(domains)
    return domains


def build_noise101_candidate_catalogue() -> dict[str, Any]:
    """Resolve complete metadata once; no market inputs or process issuance."""
    scope = _PROCESS_VERIFICATION_STATE.get()
    if scope is not None and "noise101_catalogue" in scope:
        return json.loads(scope["noise101_catalogue"].decode("ascii"))
    names = [*state_contract.ACTUAL_STRATEGY_NAMES, NOISE101_STRATEGY_ID]
    if set(StrategyRegistry.list_all()) != set(names) or len(names) != NOISE101_STRATEGY_COUNT:
        _fail("NOISE101_REGISTRY_ROSTER_INVALID")
    legacy = _noise101_legacy_domains()
    pool = {row["domain_sha256"]: row for row in legacy}
    rosters = {}
    for timeframe in TIMEFRAMES:
        references = []
        for old in legacy:
            strategy = StrategyRegistry.get(old["strategy_id"])
            defaults, grid, groups = resolve_strategy_parameters(strategy, timeframe)
            actual_axes = [{"name": name, "choices": [canonicalize_parameter_value(value) for value in choices]}
                for name, choices in grid.items()]
            if (_canonical_bytes(canonicalize_parameter_mapping(defaults)) != _canonical_bytes(old["defaults"])
                    or _canonical_bytes(actual_axes) != _canonical_bytes(old["grid_axes"])
                    or _canonical_bytes(_normalize_candidate_domain_value(groups))
                        != _canonical_bytes(_normalize_candidate_domain_value(strategy.get_param_couplings()))):
                _fail("LEGACY100_TIMEFRAME_PARAMETER_DRIFT", old["strategy_id"], timeframe)
            references.append(old["domain_sha256"])
        noise = _strategy_domain(NOISE101_STRATEGY_ID, BASELINE_ATTEMPTS, timeframe)
        pool[noise["domain_sha256"]] = noise
        references.append(noise["domain_sha256"])
        identity = {"timeframe": timeframe, "ordered_strategy_ids": names,
            "domain_sha256_by_strategy_ordinal": references}
        rosters[timeframe] = {"domain_sha256_by_strategy_ordinal": references,
            "domain_roster_sha256": _sha(identity)}
    body = {"schema_version": NOISE101_CATALOGUE_SCHEMA,
        "source_edition_id": NOISE101_SOURCE_EDITION_ID,
        "ordered_strategy_ids": names, "ordered_timeframes": list(TIMEFRAMES),
        "source_release_sha256": _sha(_noise101_source_release_roster()),
        "domain_pool": {key: pool[key] for key in sorted(pool)},
        "rosters_by_timeframe": rosters}
    validate_noise101_candidate_catalogue(body)
    if scope is not None:
        scope["noise101_catalogue"] = _canonical_bytes(body)
    return body


def validate_noise101_candidate_catalogue(body: Mapping[str, Any]) -> dict[str, Any]:
    if (type(body) is not dict or set(body) != {"schema_version", "source_edition_id",
            "ordered_strategy_ids", "ordered_timeframes", "source_release_sha256", "domain_pool", "rosters_by_timeframe"}
            or body["schema_version"] != NOISE101_CATALOGUE_SCHEMA
            or body["source_edition_id"] != NOISE101_SOURCE_EDITION_ID
            or body["ordered_strategy_ids"] != [*state_contract.ACTUAL_STRATEGY_NAMES, NOISE101_STRATEGY_ID]
            or body["ordered_timeframes"] != list(TIMEFRAMES)
            or type(body["domain_pool"]) is not dict
            or type(body["rosters_by_timeframe"]) is not dict
            or set(body["rosters_by_timeframe"]) != set(TIMEFRAMES)):
        _fail("NOISE101_CATALOGUE_FIELDS_INVALID")
    _hex64(body["source_release_sha256"], "NOISE101_SOURCE_RELEASE_INVALID")
    for digest, concrete in body["domain_pool"].items():
        if type(concrete) is not dict or concrete.get("domain_sha256") != digest or _sha(
                {k: v for k, v in concrete.items() if k != "domain_sha256"}) != digest:
            _fail("NOISE101_CONCRETE_DOMAIN_HASH_INVALID")
    used = set()
    for timeframe in TIMEFRAMES:
        roster = body["rosters_by_timeframe"][timeframe]
        if type(roster) is not dict or set(roster) != {"domain_sha256_by_strategy_ordinal", "domain_roster_sha256"}:
            _fail("NOISE101_CATALOGUE_ROSTER_INVALID")
        refs = roster["domain_sha256_by_strategy_ordinal"]
        if type(refs) is not list or len(refs) != NOISE101_STRATEGY_COUNT:
            _fail("NOISE101_CATALOGUE_ROSTER_INVALID")
        for ordinal, (name, digest) in enumerate(zip(body["ordered_strategy_ids"], refs, strict=True)):
            concrete = body["domain_pool"].get(digest)
            if concrete is None or concrete.get("strategy_id") != name or concrete.get("ordinal") != ordinal:
                _fail("NOISE101_CATALOGUE_MEMBER_CROSSED")
            used.add(digest)
        identity = {"timeframe": timeframe, "ordered_strategy_ids": body["ordered_strategy_ids"],
            "domain_sha256_by_strategy_ordinal": refs}
        if _sha(identity) != roster["domain_roster_sha256"]:
            _fail("NOISE101_CATALOGUE_ROSTER_HASH_INVALID")
        defaults = body["domain_pool"][refs[-1]]["defaults"]
        expected, _, _ = resolve_strategy_parameters(StrategyRegistry.get(NOISE101_STRATEGY_ID),
            timeframe, defaults_only=True, persisted_parameters=defaults)
        if defaults != canonicalize_parameter_mapping(expected):
            _fail("NOISE101_CATALOGUE_TIMEFRAME_CROSSED")
    if used != set(body["domain_pool"]):
        _fail("NOISE101_CATALOGUE_UNUSED_DOMAIN")
    return dict(body)


def noise101_process_metadata() -> dict[str, Any]:
    catalogue = build_noise101_candidate_catalogue()
    digest = _sha(catalogue)
    authority = state_contract.build_noise101_state_authority(digest)
    descriptor = SourceEditionDescriptor(NOISE101_SOURCE_EDITION_ID, NOISE101_PROCESS_SCHEMA,
        NOISE101_EXECUTION_EDITION, NOISE101_CATALOGUE_SCHEMA,
        search_execution.winner.V18_GENERATION.selection_version,
        tuple(catalogue["ordered_strategy_ids"]), TIMEFRAMES, NOISE101_STRATEGY_COUNT,
        VARIANTS_PER_STRATEGY, NOISE101_ATTEMPT_COUNT, digest,
        MappingProxyType({tf: catalogue["rosters_by_timeframe"][tf]["domain_roster_sha256"] for tf in TIMEFRAMES}),
        authority.registry_hash, catalogue["source_release_sha256"])
    semantics = _process_semantics(_noise101_legacy_domains())
    semantics.pop("strategy_domains")
    semantics.pop("strategy_domain_roster_sha256")
    semantics.pop("all_100_strategies_represented")
    semantics.update(schema_version=NOISE101_PROCESS_SEMANTICS_SCHEMA,
        registry_order=list(descriptor.ordered_strategy_ids),
        registry_order_sha256=_sha(list(descriptor.ordered_strategy_ids)),
        strategy_domain_count=NOISE101_STRATEGY_COUNT, candidate_catalogue=catalogue,
        accepted_candidate_domain_sha256=digest, state_contract_registry_sha256=authority.registry_hash,
        all_declared_strategies_represented=True)
    semantics["attempt_policy"].update(winner_contract_version=descriptor.winner_generation_id,
        winner_max_attempts=search_execution.winner.V18_GENERATION.max_attempts,
        attempt_count_per_observed_cell=NOISE101_ATTEMPT_COUNT,
        default_attempt_count=NOISE101_STRATEGY_COUNT,
        additional_nondefault_attempt_count=NOISE101_STRATEGY_COUNT * VARIANTS_PER_STRATEGY)
    return {"schema_version": NOISE101_PROCESS_SCHEMA, "execution_edition": NOISE101_EXECUTION_EDITION,
        "candidate_domain_sha256": digest, "state_contract_registry_sha256": authority.registry_hash,
        "source_edition": descriptor.to_dict(), "semantics": semantics}


def freeze_noise101_full_panel_production_search_process(context: authority_spool.GenerationSpoolContext,
        *, run_id: str) -> FrozenProductionSearchProcess:
    """Production requires actual recorded-clock/DST and cost qualification authority."""
    _fail("NOISE101_PRODUCTION_QUALIFICATION_NOT_ESTABLISHED")


def freeze_noise101_software_search_description(context: authority_spool.GenerationSpoolContext,
        *, run_id: str) -> FrozenProductionSearchProcess:
    """Freeze/reopen honest software metadata; real execution refuses this scope."""
    return _freeze_production_search_process(context, run_id=run_id, schema_version=NOISE101_PROCESS_SCHEMA)


def write_noise101_candidate_catalogue(path: Path, catalogue: Mapping[str, Any] | None = None):
    body = build_noise101_candidate_catalogue() if catalogue is None else validate_noise101_candidate_catalogue(dict(catalogue))
    encoded = _canonical_bytes(body)
    with Path(path).open("xb") as handle:
        handle.write(encoded)
    return {"path": str(path), "sha256": hashlib.sha256(encoded).hexdigest(),
        "bytes": len(encoded), "qualification_status": NOISE101_SOFTWARE_QUALIFICATION}


def noise101_structural_disposition(timeframe: str, symbol: str, parameters: Mapping[str, Any]):
    strategy = StrategyRegistry.get(NOISE101_STRATEGY_ID, **dict(parameters))
    resolve_strategy_parameters(strategy, timeframe, defaults_only=True, persisted_parameters=dict(parameters))
    length = strategy.session_spec(symbol)[1]
    checks = length // (parameters["bar_minutes"] * parameters["check_bars"])
    return {"nominal_session_checks": checks, "existing_minimum_session_checks": 6,
        "structurally_zero_signal": checks < 6, "removed_from_declared_roster": False}


__all__.extend(["NOISE101_SOURCE_EDITION_ID", "NOISE101_PROCESS_SCHEMA", "NOISE101_EXECUTION_EDITION",
    "NOISE101_CATALOGUE_SCHEMA", "SourceEditionDescriptor", "source_edition_info", "process_attempt_count",
    "process_strategy_count", "is_route_a_full_panel_process", "full_panel_generation",
    "build_noise101_candidate_catalogue", "validate_noise101_candidate_catalogue",
    "noise101_process_metadata", "freeze_noise101_full_panel_production_search_process",
    "write_noise101_candidate_catalogue", "noise101_structural_disposition"])


NOISE101_EFFECTIVE_STATE_DOMAIN_SCHEMA = "fxpm.production-effective-state-domain.v3"
NOISE101_STATE_REQUIREMENT_SCHEMA = "fxpm.production-attempt-state-requirement.v3"
NOISE101_STATE_DOMAIN_CEILING_RECORD_SCHEMA = "fxpm.production-state-domain-ceiling-record.v2"
NOISE101_STATE_CONTEXT_SELECTION_SCHEMA = "fxpm.production-cell-state-context-selection.v3"
NOISE101_DOMAIN_STATE_REPLAY_SCHEMA = "fxpm.production-domain-state-replay.v2"
NOISE101_FULL_PREFIX_STATE_REPLAY_SCHEMA = "fxpm.production-full-prefix-state-replay.v2"


def source_edition_context_for_cell(process_or_edition, timeframe: str, *,
        noise_state_sha256=None, noise_transition_sha256=None,
        state_boundary_source_sha256=None) -> dict[str, Any]:
    edition = source_edition_info(process_or_edition)
    if edition.edition_id != NOISE101_SOURCE_EDITION_ID or timeframe not in edition.ordered_timeframes:
        _fail("NOISE101_SOURCE_CONTEXT_EDITION_INVALID")
    return state_contract.validate_noise101_source_context({
        "source_edition_id": edition.edition_id,
        "candidate_catalogue_sha256": edition.candidate_catalogue_sha256,
        "timeframe": timeframe,
        "domain_roster_sha256": edition.domain_roster_sha256_by_timeframe[timeframe],
        "state_registry_sha256": edition.state_registry_sha256,
        "noise_state_sha256": noise_state_sha256,
        "noise_transition_sha256": noise_transition_sha256,
        "state_boundary_source_sha256": state_boundary_source_sha256}, allow_unproved=True)


def verify_source_edition_context(process_or_edition, timeframe: str, context, *,
        strategy_id: str | None = None, allow_unproved: bool = False):
    """Bind an already-verified requirement; no source reads or replay."""
    edition = source_edition_info(process_or_edition)
    if edition.edition_id != NOISE101_SOURCE_EDITION_ID:
        if context is not None:
            _fail("LEGACY_SOURCE_CONTEXT_REJECTED")
        return None
    if timeframe not in edition.ordered_timeframes:
        _fail("NOISE101_AUTHENTICATED_TIMEFRAME_REQUIRED")
    complete_noise = strategy_id == NOISE101_STRATEGY_ID and not allow_unproved
    actual = state_contract.validate_noise101_source_context(context,
        allow_unproved=not complete_noise)
    expected = source_edition_context_for_cell(edition, timeframe)
    for field in ("source_edition_id", "candidate_catalogue_sha256", "timeframe",
            "domain_roster_sha256", "state_registry_sha256"):
        if actual[field] != expected[field]:
            _fail("NOISE101_SOURCE_CONTEXT_CROSSED", field)
    if strategy_id is not None and strategy_id not in edition.ordered_strategy_ids:
        _fail("NOISE101_SOURCE_CONTEXT_STRATEGY_INVALID")
    return actual


def _noise101_preprotected_state_bundle(*, fitted, native, parameters,
        source_context, state_error_envelope):
    frame = fitted._frame
    cutoff = pd.Timestamp(_utc(state_error_envelope["preprotected_cutoff_utc"],
        "NOISE101_STATE_CUTOFF_INVALID"))
    opens = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True))
    closes = pd.DatetimeIndex(pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True))
    source = frame.loc[(opens < cutoff) & (closes <= cutoff)]
    if len(source) != state_error_envelope["source_rows"]:
        _fail("NOISE101_STATE_BOUNDARY_SOURCE_CHANGED")
    # The existing envelope proves this prefix precedes every protected observation.
    boundary = _noise101_boundary_identity({"index_sha256": fitted.product.index_sha256,
        "content_sha256": native["prefix_content_sha256"],
        "start_utc": native["prefix_start_utc"]}, state_error_envelope)
    context = {**source_context, "state_boundary_source_sha256": _sha(boundary)}
    strategy = StrategyRegistry.get(NOISE101_STRATEGY_ID, **dict(parameters))
    symbol = str(native["symbol"])
    spec = strategy.session_spec(symbol)
    inside, keys = search_execution.pm_core._recorded_session_arrays(source.index, spec)
    relative = (source.index.hour * 60 + source.index.minute - spec[0]) % 1440
    bundle = state_contract.encode_noise101_replay_state(source_context=context,
        implementation_sha256=_callable_sha256(type(strategy).generate_signals),
        symbol=symbol, parameters=parameters, session_spec=spec,
        session_keys=[int(value) for value in keys],
        in_session=[bool(value) for value in inside],
        slot_minutes=[int(value) + parameters["bar_minutes"] for value in relative],
        open_prices=source["Open"].tolist(), close_prices=source["Close"].tolist(),
        recorded_opens=[value.isoformat() for value in source.index],
        recorded_timezone=str(source.index.tz) if source.index.tz is not None else None)
    state_contract.decode_noise101_replay_state(bundle)
    return bundle


__all__.extend(["source_edition_context_for_cell", "verify_source_edition_context"])


__all__.append("freeze_noise101_software_search_description")


def _noise101_boundary_identity(native_prefix, state_error_envelope):
    return {"native_prefix_index_sha256": native_prefix["index_sha256"],
        "native_prefix_content_sha256": native_prefix["content_sha256"],
        "prefix_start_utc": native_prefix["start_utc"],
        "preprotected_cutoff_utc": state_error_envelope["preprotected_cutoff_utc"],
        "source_price_surface_sha256": state_error_envelope["source_price_surface_sha256"],
        "source_rows": state_error_envelope["source_rows"],
        "replay_from_authenticated_prefix_start": True}


def _verify_noise101_state_requirement(edition, timeframe, state, *,
        effective_domain, native_prefix, state_error_envelope):
    strategy_id = state.get("strategy_id")
    context = verify_source_edition_context(edition, timeframe,
        state.get("source_edition_context"), strategy_id=strategy_id)
    expected = dict(effective_domain["source_edition_context"])
    if any(context[key] != expected[key] for key in ("source_edition_id", "candidate_catalogue_sha256", "timeframe", "domain_roster_sha256", "state_registry_sha256")):
        _fail("NOISE101_STATE_DOMAIN_CONTEXT_CROSSED")
    for replay in state["state_evidence"]:
        if replay.get("source_edition_context") != context:
            _fail("NOISE101_STATE_REPLAY_CONTEXT_CROSSED")
    if strategy_id == NOISE101_STRATEGY_ID:
        bundle = state.get("recorded_session_state")
        try:
            decoded = state_contract.decode_noise101_replay_state(bundle)
        except (TypeError, KeyError, ValueError) as exc:
            raise ProductionSearchPlannerError("NOISE101_STATE_BUNDLE_INVALID", (str(exc),)) from exc
        if (bundle["source_edition_context"] != context
                or context["state_boundary_source_sha256"] != _sha(_noise101_boundary_identity(native_prefix, state_error_envelope))
                or bundle["transition"]["implementation_sha256"] != _callable_sha256(type(StrategyRegistry.get(NOISE101_STRATEGY_ID)).generate_signals)
                or decoded["timeframe"] != timeframe
                or decoded["parameters_sha256"] != state["parameters_sha256"]
                or decoded["symbol"] != effective_domain["cell"]["symbol"]):
            _fail("NOISE101_STATE_BOUNDARY_BINDING_INVALID")
    elif "recorded_session_state" in state or context != expected:
        _fail("NOISE101_STATE_BUNDLE_STRATEGY_CROSSED")


def resume_noise101_recorded_session_signals(bundle, suffix_frame, *, strategy, symbol):
    """Replay the bounded recorded history through the same strategy implementation."""
    state = state_contract.decode_noise101_replay_state(bundle)
    if (_sha(dict(strategy.params)) != state["parameters_sha256"]
            or type(strategy).__name__ != NOISE101_STRATEGY_ID or symbol != state["symbol"]
            or _callable_sha256(type(strategy).generate_signals) != bundle["transition"]["implementation_sha256"]
            or strategy.session_spec(symbol) != (state["session_spec"]["anchor_minutes"], state["session_spec"]["length_minutes"])
            or any(strategy.params[key] != state[key] for key in ("bar_minutes", "check_bars", "lookback"))):
        _fail("NOISE101_RESUME_STRATEGY_CROSSED")
    timezone = state["recorded_timezone"]
    labels = [row["recorded_open"] for row in state["replay_observations"]]
    if timezone is None:
        index = pd.DatetimeIndex(labels)
    else:
        index = pd.DatetimeIndex(pd.to_datetime(labels, utc=True)).tz_convert(timezone)
    suffix_index = pd.DatetimeIndex(suffix_frame.index)
    if (str(suffix_index.tz) if suffix_index.tz is not None else None) != timezone:
        _fail("NOISE101_RESUME_RECORDED_TIMEZONE_CROSSED")
    if len(suffix_index) and state["last_recorded_open"] is not None and suffix_index[0] <= pd.Timestamp(state["last_recorded_open"]):
        _fail("NOISE101_RESUME_SOURCE_OVERLAP")
    history = pd.DataFrame({"Open": [row["open"] for row in state["replay_observations"]],
        "Close": [row["close"] for row in state["replay_observations"]]}, index=index)
    combined = pd.concat([history, suffix_frame])
    signals = strategy.generate_signals(combined, symbol)
    return signals.iloc[len(history):].copy() if hasattr(signals, "iloc") else signals[len(history):].copy()


__all__.extend(["SourceEditionDescriptor", "NOISE101_SOURCE_EDITION_ID", "NOISE101_PROCESS_SCHEMA",
    "NOISE101_EXECUTION_EDITION", "source_edition_info", "is_route_a_full_panel_process",
    "full_panel_generation", "process_attempt_count", "process_strategy_count",
    "build_noise101_candidate_catalogue", "validate_noise101_candidate_catalogue",
    "write_noise101_candidate_catalogue", "noise101_process_metadata",
    "freeze_noise101_software_search_description", "freeze_noise101_full_panel_production_search_process",
    "source_edition_context_for_cell", "verify_source_edition_context",
    "noise101_structural_disposition", "resume_noise101_recorded_session_signals"])

__all__.extend(["INDEPENDENT_R_SOURCE_EDITION", "INDEPENDENT_R_PROCESS_SCHEMA",
    "INDEPENDENT_R_EXECUTION_EDITION", "INDEPENDENT_R_PROCESS_SEMANTICS_SCHEMA",
    "freeze_independent_signal_r_production_search_process"])
