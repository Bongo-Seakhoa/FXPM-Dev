"""Authenticated, deterministic history/warm-up dependency contract.

This module inventories the strategy registry without importing the optimizer or
pipeline modules.  It deliberately separates two different acquisition needs:

* a native-bar warm-up count for rolling/recursive calculations; and
* calendar context that cannot be represented faithfully by a number of bars
  (for example, the previous complete month or the start of a broker session).

The resulting document has no wall-clock timestamp.  Given the same registry,
source tree, live minimum, and policy constants it serializes byte-for-byte
identically, which makes it suitable for binding into acquisition request maps.
"""

from __future__ import annotations

import ast
import hashlib
import inspect
import json
import math
import numbers
import re
import textwrap
from dataclasses import MISSING
from pathlib import Path
from typing import Any, Mapping, Sequence

from pm_core import FeatureComputer, PipelineConfig
from pm_regime import DEFAULT_PARAMS_BY_TIMEFRAME, MarketRegimeDetector, RegimeParams
from pm_strategies import StrategyRegistry


HISTORY_DEPENDENCY_CONTRACT_VERSION = "fxpm-history-dependencies-research-v2"
HISTORY_DEPENDENCY_SERIALIZATION = "canonical-json-sort-keys-v1"
HISTORY_DEPENDENCY_PRODUCTION_STATUS = "RESEARCH_ONLY_VETOED"
CANONICAL_TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")

PRODUCTION_BLOCKERS = (
    "PATH_DEPENDENT_STATE_HAS_NO_PROVEN_PREFIX_BOUND",
    "RECURSIVE_NUMERICAL_CONVERGENCE_NOT_DERIVED",
    "CANDIDATE_COMPOSITION_RULE_NOT_PROVEN",
    "NUMERICAL_ERROR_NOT_BOUND_TO_SIGNAL_STABILITY",
    "LIVE_INITIALIZATION_CONTEXT_NOT_AUTHENTICATED",
    "CONTRACT_CHECKSUM_IS_NOT_TRUSTED_SOURCE_ATTESTATION",
)

# These constants reproduce the original inventory heuristic for research and
# comparison only.  A scalar multiplier and buffer cannot prove convergence of
# recursive indicators or prefix invariance of path-dependent state.
WARMUP_SAFETY_MULTIPLIER = 1.50
WARMUP_SAFETY_BUFFER_BARS = 32

# Hard bounds keep a corrupt/dynamic registry from producing an unbounded
# attestation document or consuming unbounded memory during request generation.
MAX_REGISTERED_STRATEGIES = 512
MAX_KEYS_PER_STRATEGY = 256
MAX_VALUES_PER_GRID_AXIS = 4_096
MAX_SOURCE_BYTES = 8 * 1024 * 1024


class HistoryDependencyContractError(RuntimeError):
    """Base error for an incomplete or unauthenticated dependency inventory."""


class UnknownDependencyAxisError(HistoryDependencyContractError):
    """A dependency-looking strategy axis has no reviewed interpretation."""


# These axes represent a number of native bars.  The inventory takes the maximum
# value available in the grid or defaults and sums maxima within each strategy.
# Summing is deliberately conservative: axes may be sequentially stacked, and a
# future code review may tighten a proven-parallel family without weakening the
# default acquisition contract.
BAR_DEPENDENCY_AXES = frozenset(
    {
        "adl_slope_bars",
        "adx_period",
        "atr_period",
        "bb_period",
        "breakout_lookback",
        "cci_period",
        "channel_period",
        "ci_period",
        "cmf_period",
        "cmo_period",
        "confirm_bars",
        "confirm_window",
        "corr_period",
        "d_period",
        "ema_fast",
        "ema_filter_period",
        "ema_mid",
        "ema_period",
        "ema_slope_bars",
        "ema_slow",
        "ema_trend_period",
        "er_period",
        "fade_window_bars",
        "failure_window_bars",
        "fast_period",
        "impulse_bars",
        "k_period",
        "kc_ema",
        "kc_period",
        "kijun_period",
        "lag",
        "lookback",
        "lookback_level",
        "macd_fast",
        "macd_signal",
        "macd_slow",
        "mfi_period",
        "min_inside_bars",
        "mom_lookback",
        "momentum_period",
        "nr_lookback",
        "obv_smooth",
        "pct_lookback",
        "period",
        "pattern_max_age_bars",
        "pullback_bars",
        "pivot_max_age_bars",
        "range_bars",
        "reclaim_window",
        "retest_window",
        "roc_period",
        "rsi_period",
        "senkou_b_period",
        "signal_period",
        "slope_bars",
        "slow_period",
        "smooth_d",
        "smooth_k",
        "setup_max_age_bars",
        "stoch_period",
        "structural_sl_lookback",
        "swing_bars",
        "swing_order",
        "tenkan_period",
        "transition_window",
        "trend_ema",
        "trend_ema_period",
        "trix_period",
        "vidya_period",
        "vol_lookback",
        "vr_period",
        "vwap_window",
        "wing_bars",
        "z_period",
        "z_window",
    }
)

# These are dependencies, but their unit is a calendar/session boundary rather
# than a bar.  In particular, adr_period means complete trading days and must
# never be interpreted as merely N intraday bars.
CALENDAR_DEPENDENCY_AXES = frozenset(
    {
        "adr_period",
        "end_hour",
        "entry_hour",
        "late_start_hour",
        "period_type",
        "quiet_end_hour",
        "quiet_start_hour",
        "session_end_hour",
        "session_start_hour",
        "start_hour",
        "tom_end_day",
        "tom_start_day",
        "weekday",
    }
)

# Boolean switches can still imply a fixed lookback.  They are inventoried
# separately from numeric axes so ``True`` is never misread as one bar.
IMPLIED_BAR_DEPENDENCY_AXES = {"use_ema200_trend_filter": 200}
DEPENDENCY_LIKE_NON_HISTORY_AXES: frozenset[str] = frozenset()

_DEPENDENCY_NAME_PATTERN = re.compile(
    r"(?:^|_)(?:period|lookback|window|bars?|lag|slow|fast|smooth|swing|wing|"
    r"ema|vwap|session|hours?|weekday|days?|weeks?|months?)(?:$|_)",
    re.IGNORECASE,
)

# Audited FeatureComputer dependencies.  The AST literal scan below can raise
# this floor automatically if the two production feature entry points acquire a
# larger integral period.  Stacked components describe sequential operations;
# parallel indicators use their longest branch.
FEATURE_COMPUTER_COMPONENTS = {
    "moving_average_longest_branch": 200,
    "donchian_longest_branch": 50,
    "macd_slow_then_signal": 35,  # max(12, 26) + 9
    "stochastic_k_then_d": 17,  # 14 + 3
    "adx_directional_then_adx": 28,  # 14 + 14
    "hull_longest_branch_then_smoothing": 24,  # 20 + sqrt(20)
    "bollinger_longest_branch": 20,
    "keltner_longest_branch": 20,
}

_REGIME_BAR_FIELDS = (
    "adx_period",
    "atr_period",
    "bb_period",
    "efficiency_period",
    "bb_squeeze_lookback",
    "atr_lookback",
    "structure_period",
)

_PREVIOUS_SESSION_STRATEGIES = frozenset(
    {
        "GapContinuationStrategy",
        "GapFadeStrategy",
        "PreviousSessionLevelRejectionStrategy",
    }
)
_CURRENT_DAY_STRATEGIES = frozenset({"SessionVWAPReversionStrategy"})
_PREVIOUS_DAY_STRATEGIES = frozenset({"PriorDayRangeReclaimStrategy"})
_PREVIOUS_WEEK_STRATEGIES = frozenset({"PreviousWeekRangeBreakoutStrategy"})
_WEEK_MONTH_STRATEGIES = frozenset({"WeeklyMonthlyLevelRejectionStrategy"})

_DERIVATION_RULE = (
    "Research-only legacy heuristic: for each strategy, take the non-negative maximum "
    "of every reviewed native-bar dependency axis across its optimization grid and "
    "defaults, then sum those maxima. For each timeframe, take max(strategy heuristic, "
    "FeatureComputer heuristic, regime heuristic, live_min_bars), multiply by the "
    "diagnostic multiplier, round up, and add the diagnostic buffer. This is not a "
    "dependency DAG: it does not prove serial/parallel composition, recurrence "
    "convergence, path-state reset, or signal stability. Calendar/session requirements "
    "remain separate source-start constraints."
)


def _json_safe(value: Any) -> Any:
    """Convert supported values to deterministic, strict-JSON primitives."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise HistoryDependencyContractError("non-finite value in dependency snapshot")
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (TypeError, ValueError):
            pass
    raise HistoryDependencyContractError(
        f"unsupported value type in dependency snapshot: {type(value).__name__}"
    )


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_document(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def serialize_history_dependency_contract(contract: Mapping[str, Any]) -> str:
    """Return the canonical on-disk representation (with one trailing newline)."""
    return _canonical_bytes(contract).decode("utf-8") + "\n"


def contract_content_sha256(contract: Mapping[str, Any]) -> str:
    """Recompute the embedded content hash, excluding the hash field itself."""
    payload = dict(contract)
    payload.pop("contract_sha256", None)
    return _sha256_document(payload)


def _hash_file(path: Path) -> str:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise HistoryDependencyContractError(f"cannot stat dependency source {path}: {exc}") from exc
    if size > MAX_SOURCE_BYTES:
        raise HistoryDependencyContractError(
            f"dependency source exceeds {MAX_SOURCE_BYTES} bytes: {path}"
        )
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise HistoryDependencyContractError(f"cannot read dependency source {path}: {exc}") from exc


def _source_hashes(source_paths: Mapping[str, Path] | None = None) -> dict[str, str]:
    root = Path(__file__).resolve().parent
    paths = source_paths or {
        "pm_core.py": root / "pm_core.py",
        "pm_regime.py": root / "pm_regime.py",
        "pm_regime_tuner.py": root / "pm_regime_tuner.py",
        "pm_strategies.py": root / "pm_strategies.py",
    }
    if not paths:
        raise HistoryDependencyContractError("dependency source set is empty")
    return {str(label): _hash_file(Path(path)) for label, path in sorted(paths.items())}


def _positive_integral_literals(callable_obj: Any) -> tuple[int, ...]:
    """Conservatively inventory integral literals in a feature entry point."""
    try:
        source = textwrap.dedent(inspect.getsource(callable_obj))
        tree = ast.parse(source)
    except (OSError, TypeError, IndentationError, SyntaxError) as exc:
        raise HistoryDependencyContractError(
            f"cannot inspect feature dependency source {callable_obj}: {exc}"
        ) from exc
    values: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or isinstance(node.value, bool):
            continue
        value = node.value
        if isinstance(value, numbers.Real):
            numeric = float(value)
            if numeric > 0 and numeric.is_integer():
                values.add(int(numeric))
    return tuple(sorted(values))


def _feature_inventory() -> dict[str, Any]:
    literals = set(_positive_integral_literals(FeatureComputer.compute_required))
    literals.update(_positive_integral_literals(FeatureComputer.compute_all))
    literal_max = max(literals, default=0)
    audited_max = max(FEATURE_COMPUTER_COMPONENTS.values())
    return {
        "components_bars": dict(sorted(FEATURE_COMPUTER_COMPONENTS.items())),
        "entry_point_integral_literal_max": int(literal_max),
        "entry_point_integral_literals": sorted(literals),
        "required_bars": int(max(audited_max, literal_max)),
        "derivation": (
            "maximum of audited stacked FeatureComputer components and positive integral "
            "literals in compute_required/compute_all"
        ),
    }


def _regime_parameter_max(params: RegimeParams) -> int:
    maxima = [int(max(0, getattr(params, field_name))) for field_name in _REGIME_BAR_FIELDS]
    # ADX applies directional smoothing and then ADX smoothing sequentially.
    maxima.append(int(max(0, params.adx_period)) * 2)
    return max(maxima, default=0)


def _regime_inventory(timeframes: Sequence[str]) -> dict[str, Any]:
    base = RegimeParams()
    base_required = max(
        _regime_parameter_max(base),
        int(MarketRegimeDetector(base).warmup_bars),
    )
    by_timeframe: dict[str, Any] = {}
    for timeframe in timeframes:
        configured = DEFAULT_PARAMS_BY_TIMEFRAME.get(timeframe, base)
        configured_required = max(
            _regime_parameter_max(configured),
            int(MarketRegimeDetector(configured).warmup_bars),
        )
        # The tuner constructs RegimeParams with only hysteresis overrides, so
        # base lookbacks remain an admissible production state for every TF.
        by_timeframe[timeframe] = {
            "timeframe_default_bars": int(configured_required),
            "base_or_tuned_bars": int(base_required),
            "required_bars": int(max(configured_required, base_required)),
        }
    return {
        "dependency_fields": list(_REGIME_BAR_FIELDS),
        "base_required_bars": int(base_required),
        "by_timeframe": by_timeframe,
        "derivation": (
            "maximum of detector warmup, reviewed regime lookback fields, and the "
            "two-stage ADX period; includes RegimeParams base used by the tuner"
        ),
    }


def _is_dependency_like(key: str) -> bool:
    return bool(_DEPENDENCY_NAME_PATTERN.search(str(key)))


def classify_dependency_axis(key: str) -> str:
    """Classify a grid/default key, failing closed for unknown lookback-like names."""
    key = str(key)
    if key in BAR_DEPENDENCY_AXES:
        return "native_bar_dependency"
    if key in CALENDAR_DEPENDENCY_AXES:
        return "calendar_or_session_dependency"
    if key in IMPLIED_BAR_DEPENDENCY_AXES:
        return "implied_native_bar_dependency"
    if key in DEPENDENCY_LIKE_NON_HISTORY_AXES:
        return "reviewed_non_history_parameter"
    if _is_dependency_like(key):
        raise UnknownDependencyAxisError(
            f"unrecognized dependency-like strategy parameter {key!r}; "
            "classify it before generating an acquisition request"
        )
    return "non_history_parameter"


def find_unrecognized_dependency_like_keys(registry: Any = StrategyRegistry) -> tuple[str, ...]:
    """Return dependency-looking keys that are not covered by the reviewed policy."""
    unknown: set[str] = set()
    names = sorted(str(name) for name in registry.list_all())
    for name in names:
        strategy = registry.get(name)
        grid = strategy.get_param_grid() or {}
        defaults = strategy.get_default_params() or {}
        for key in set(grid) | set(defaults):
            key_text = str(key)
            if (
                _is_dependency_like(key_text)
                and key_text not in BAR_DEPENDENCY_AXES
                and key_text not in CALENDAR_DEPENDENCY_AXES
                and key_text not in IMPLIED_BAR_DEPENDENCY_AXES
                and key_text not in DEPENDENCY_LIKE_NON_HISTORY_AXES
            ):
                unknown.add(key_text)
    return tuple(sorted(unknown))


def _bounded_mapping(value: Any, *, strategy_name: str, kind: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise HistoryDependencyContractError(
            f"{strategy_name} {kind} must be a mapping, got {type(value).__name__}"
        )
    if len(value) > MAX_KEYS_PER_STRATEGY:
        raise HistoryDependencyContractError(
            f"{strategy_name} {kind} exceeds {MAX_KEYS_PER_STRATEGY} keys"
        )
    result: dict[str, Any] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key)
        if kind == "grid":
            if not isinstance(raw_value, (list, tuple)):
                raise HistoryDependencyContractError(
                    f"{strategy_name} grid axis {key!r} is not a bounded sequence"
                )
            if len(raw_value) > MAX_VALUES_PER_GRID_AXIS:
                raise HistoryDependencyContractError(
                    f"{strategy_name} grid axis {key!r} exceeds "
                    f"{MAX_VALUES_PER_GRID_AXIS} values"
                )
        result[key] = _json_safe(raw_value)
    return dict(sorted(result.items()))


def _axis_maximum(
    key: str,
    grid: Mapping[str, Any],
    defaults: Mapping[str, Any],
    *,
    strategy_name: str,
) -> int:
    candidates: list[Any] = []
    if key in grid:
        candidates.extend(grid[key])
    if key in defaults:
        candidates.append(defaults[key])
    numeric: list[int] = []
    for value in candidates:
        if isinstance(value, bool) or not isinstance(value, numbers.Real):
            continue
        parsed = float(value)
        if not math.isfinite(parsed) or not parsed.is_integer():
            raise HistoryDependencyContractError(
                f"{strategy_name} dependency axis {key!r} contains non-integral {value!r}"
            )
        numeric.append(max(0, int(parsed)))
    if not numeric:
        raise HistoryDependencyContractError(
            f"{strategy_name} dependency axis {key!r} has no numeric values"
        )
    return max(numeric)


def _strategy_calendar_context(
    strategy_name: str,
    grid: Mapping[str, Any],
    defaults: Mapping[str, Any],
) -> list[dict[str, Any]]:
    keys = set(grid) | set(defaults)
    context: list[dict[str, Any]] = []
    if keys & CALENDAR_DEPENDENCY_AXES:
        context.append({"kind": "authenticated_utc_calendar_clock"})
    if "session_start_hour" in keys:
        context.append({"kind": "current_session_from_configured_boundary"})
    if strategy_name in _CURRENT_DAY_STRATEGIES:
        context.append({"kind": "current_calendar_day_from_boundary"})
    if strategy_name in _PREVIOUS_SESSION_STRATEGIES:
        context.append({"kind": "previous_complete_sessions", "count": 1})
    if "adr_period" in keys:
        count = _axis_maximum(
            "adr_period", grid, defaults, strategy_name=strategy_name
        )
        context.append({"kind": "previous_complete_trading_days", "count": count})
    elif strategy_name in _PREVIOUS_DAY_STRATEGIES:
        context.append({"kind": "previous_complete_trading_days", "count": 1})
    if strategy_name in _PREVIOUS_WEEK_STRATEGIES:
        context.append({"kind": "previous_complete_weeks", "count": 1})
    if strategy_name in _WEEK_MONTH_STRATEGIES:
        context.extend(
            [
                {"kind": "previous_complete_weeks", "count": 1},
                {"kind": "previous_complete_months", "count": 1},
            ]
        )
    return sorted(context, key=lambda item: (item["kind"], item.get("count", 0)))


def _strategy_inventory(registry: Any) -> dict[str, Any]:
    names = [str(name) for name in registry.list_all()]
    if not names:
        raise HistoryDependencyContractError("strategy registry is empty")
    if len(names) != len(set(names)):
        raise HistoryDependencyContractError("strategy registry contains duplicate names")
    if len(names) > MAX_REGISTERED_STRATEGIES:
        raise HistoryDependencyContractError(
            f"strategy registry exceeds {MAX_REGISTERED_STRATEGIES} entries"
        )

    grid_snapshot: dict[str, Any] = {}
    defaults_snapshot: dict[str, Any] = {}
    strategies: list[dict[str, Any]] = []
    all_grid_keys: set[str] = set()
    all_default_keys: set[str] = set()
    classifications: dict[str, set[str]] = {
        "native_bar_dependency": set(),
        "calendar_or_session_dependency": set(),
        "implied_native_bar_dependency": set(),
        "reviewed_non_history_parameter": set(),
        "non_history_parameter": set(),
    }

    for name in sorted(names):
        strategy = registry.get(name)
        grid = _bounded_mapping(
            strategy.get_param_grid() or {}, strategy_name=name, kind="grid"
        )
        defaults = _bounded_mapping(
            strategy.get_default_params() or {}, strategy_name=name, kind="defaults"
        )
        grid_snapshot[name] = grid
        defaults_snapshot[name] = defaults
        all_grid_keys.update(grid)
        all_default_keys.update(defaults)

        bar_axes: dict[str, int] = {}
        for key in sorted(set(grid) | set(defaults)):
            classification = classify_dependency_axis(key)
            classifications[classification].add(key)
            if classification == "native_bar_dependency":
                bar_axes[key] = _axis_maximum(
                    key, grid, defaults, strategy_name=name
                )
            elif classification == "implied_native_bar_dependency":
                enabled_values = []
                if key in grid:
                    enabled_values.extend(grid[key])
                if key in defaults:
                    enabled_values.append(defaults[key])
                if any(value is True for value in enabled_values):
                    bar_axes[key] = int(IMPLIED_BAR_DEPENDENCY_AXES[key])

        strategies.append(
            {
                "strategy": name,
                "grid_keys": sorted(grid),
                "default_only_keys": sorted(set(defaults) - set(grid)),
                "bar_axis_maxima": dict(sorted(bar_axes.items())),
                "stacked_dependency_bars": int(sum(bar_axes.values())),
                "calendar_context": _strategy_calendar_context(name, grid, defaults),
            }
        )

    maximum = max(item["stacked_dependency_bars"] for item in strategies)
    maximum_strategies = sorted(
        item["strategy"]
        for item in strategies
        if item["stacked_dependency_bars"] == maximum
    )
    single_axis_max = max(
        (
            value
            for item in strategies
            for value in item["bar_axis_maxima"].values()
        ),
        default=0,
    )
    return {
        "registry_count": len(strategies),
        "strategy_grid_sha256": _sha256_document(grid_snapshot),
        "strategy_defaults_sha256": _sha256_document(defaults_snapshot),
        "grid_key_inventory": {
            "all_grid_keys": sorted(all_grid_keys),
            "all_default_keys": sorted(all_default_keys),
            **{
                classification: sorted(keys)
                for classification, keys in sorted(classifications.items())
            },
        },
        "strategy_maxima": {
            "maximum_stacked_dependency_bars": int(maximum),
            "strategies_at_maximum": maximum_strategies,
            "maximum_single_axis_bars": int(single_axis_max),
        },
        "strategies": strategies,
    }


def _live_minimum_default() -> int:
    field = PipelineConfig.__dataclass_fields__.get("live_min_bars")
    if field is None or field.default is MISSING:
        raise HistoryDependencyContractError(
            "PipelineConfig.live_min_bars has no deterministic class default"
        )
    value = int(field.default)
    if value <= 0:
        raise HistoryDependencyContractError("live_min_bars must be positive")
    return value


def _calendar_summary(strategies: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    maximum_counts = {
        "previous_complete_sessions": 0,
        "previous_complete_trading_days": 0,
        "previous_complete_weeks": 0,
        "previous_complete_months": 0,
    }
    context_sources: dict[str, list[str]] = {}
    for strategy in strategies:
        name = str(strategy["strategy"])
        for context in strategy["calendar_context"]:
            kind = str(context["kind"])
            context_sources.setdefault(kind, []).append(name)
            if kind in maximum_counts:
                maximum_counts[kind] = max(
                    maximum_counts[kind], int(context.get("count", 0))
                )
    return {
        "time_basis": (
            "authenticated native-bar timestamps normalized to UTC; session and period "
            "boundaries must use the same alignment as strategy implementation"
        ),
        "current_session_from_boundary_required": bool(
            context_sources.get("current_session_from_configured_boundary")
        ),
        "current_calendar_day_from_boundary_required": bool(
            context_sources.get("current_calendar_day_from_boundary")
        ),
        **maximum_counts,
        "context_strategy_sources": {
            kind: sorted(set(names)) for kind, names in sorted(context_sources.items())
        },
        "source_start_rule": (
            "Use the earliest of the native-bar predecessor required by the timeframe "
            "warmup and every applicable complete calendar/session boundary. Calendar "
            "requirements are conjunctive and may not be replaced by an estimated bar count."
        ),
    }


def _dependency_spec() -> dict[str, Any]:
    return {
        "bar_dependency_axes": sorted(BAR_DEPENDENCY_AXES),
        "calendar_dependency_axes": sorted(CALENDAR_DEPENDENCY_AXES),
        "dependency_like_non_history_axes": sorted(DEPENDENCY_LIKE_NON_HISTORY_AXES),
        "implied_bar_dependency_axes": dict(sorted(IMPLIED_BAR_DEPENDENCY_AXES.items())),
        "feature_components": dict(sorted(FEATURE_COMPUTER_COMPONENTS.items())),
        "previous_session_strategies": sorted(_PREVIOUS_SESSION_STRATEGIES),
        "current_day_strategies": sorted(_CURRENT_DAY_STRATEGIES),
        "previous_day_strategies": sorted(_PREVIOUS_DAY_STRATEGIES),
        "previous_week_strategies": sorted(_PREVIOUS_WEEK_STRATEGIES),
        "week_month_strategies": sorted(_WEEK_MONTH_STRATEGIES),
        "safety_multiplier": WARMUP_SAFETY_MULTIPLIER,
        "safety_buffer_bars": WARMUP_SAFETY_BUFFER_BARS,
        "derivation_rule": _DERIVATION_RULE,
    }


def build_history_dependency_contract(
    *,
    registry: Any = StrategyRegistry,
    timeframes: Sequence[str] = CANONICAL_TIMEFRAMES,
    live_min_bars: int | None = None,
    source_paths: Mapping[str, Path] | None = None,
) -> dict[str, Any]:
    """Build a deterministic dependency document for native-history acquisition.

    ``live_min_bars`` may be supplied from an authenticated runtime config.  When
    omitted, the dataclass default is read directly from ``PipelineConfig``.
    The function imports no optimizer/pipeline implementation modules.
    """
    normalized_timeframes = tuple(str(item).upper() for item in timeframes)
    if normalized_timeframes != CANONICAL_TIMEFRAMES:
        raise HistoryDependencyContractError(
            f"production history contract requires {CANONICAL_TIMEFRAMES}, "
            f"got {normalized_timeframes}"
        )

    effective_live_min = _live_minimum_default() if live_min_bars is None else int(live_min_bars)
    if effective_live_min <= 0:
        raise HistoryDependencyContractError("live_min_bars must be positive")

    strategy = _strategy_inventory(registry)
    feature = _feature_inventory()
    regime = _regime_inventory(normalized_timeframes)
    calendar = _calendar_summary(strategy["strategies"])
    sources = _source_hashes(source_paths)
    contract_code_sha256 = _hash_file(Path(__file__).resolve())
    dependency_spec = _dependency_spec()

    per_timeframe: dict[str, Any] = {}
    strategy_max = int(strategy["strategy_maxima"]["maximum_stacked_dependency_bars"])
    feature_max = int(feature["required_bars"])
    for timeframe in normalized_timeframes:
        regime_max = int(regime["by_timeframe"][timeframe]["required_bars"])
        raw_required = max(strategy_max, feature_max, regime_max, effective_live_min)
        required = math.ceil(raw_required * WARMUP_SAFETY_MULTIPLIER)
        required += WARMUP_SAFETY_BUFFER_BARS
        per_timeframe[timeframe] = {
            "component_bars": {
                "strategy_stacked_maximum": strategy_max,
                "feature_computer": feature_max,
                "regime_detector": regime_max,
                "live_minimum": effective_live_min,
            },
            "raw_required_bars": int(raw_required),
            # This is retained as an auditable output of the original
            # inventory formula, not as a production acquisition requirement.
            # Adversarial prefix tests disproved that interpretation for
            # unbounded discrete state and slowly converging recurrences.
            "candidate_pretrain_bars": int(required),
            "calendar_context_requirement": "calendar_context_requirements",
        }

    payload: dict[str, Any] = {
        "contract_version": HISTORY_DEPENDENCY_CONTRACT_VERSION,
        "serialization": HISTORY_DEPENDENCY_SERIALIZATION,
        "production_status": HISTORY_DEPENDENCY_PRODUCTION_STATUS,
        "production_eligible": False,
        "production_blockers": list(PRODUCTION_BLOCKERS),
        "timeframes": list(normalized_timeframes),
        "derivation_policy": {
            "rule": _DERIVATION_RULE,
            "safety_multiplier": WARMUP_SAFETY_MULTIPLIER,
            "safety_buffer_bars": WARMUP_SAFETY_BUFFER_BARS,
        },
        "hashes": {
            "contract_code_sha256": contract_code_sha256,
            "dependency_spec_sha256": _sha256_document(dependency_spec),
            "source_files_sha256": sources,
            "source_bundle_sha256": _sha256_document(sources),
            "strategy_grid_sha256": strategy["strategy_grid_sha256"],
            "strategy_defaults_sha256": strategy["strategy_defaults_sha256"],
        },
        "components": {
            "strategy_registry": strategy,
            "feature_computer": feature,
            "regime_detector": regime,
            "live_minimum": {
                "live_min_bars": effective_live_min,
                "source": (
                    "PipelineConfig.live_min_bars class default"
                    if live_min_bars is None
                    else "authenticated caller override"
                ),
            },
        },
        "calendar_context_requirements": calendar,
        "per_timeframe": per_timeframe,
        "runtime_bounds": {
            "maximum_registered_strategies": MAX_REGISTERED_STRATEGIES,
            "maximum_keys_per_strategy": MAX_KEYS_PER_STRATEGY,
            "maximum_values_per_grid_axis": MAX_VALUES_PER_GRID_AXIS,
            "maximum_source_bytes_per_file": MAX_SOURCE_BYTES,
            "retained_market_rows": 0,
        },
    }
    payload["contract_sha256"] = _sha256_document(payload)
    return payload


def validate_history_dependency_contract_hash(contract: Mapping[str, Any]) -> None:
    """Verify document integrity only; this is not production authentication."""
    expected = str(contract.get("contract_sha256", ""))
    actual = contract_content_sha256(contract)
    if not expected or expected != actual:
        raise HistoryDependencyContractError(
            f"history dependency contract hash mismatch: expected={expected or '<missing>'} "
            f"actual={actual}"
        )


def assert_history_dependency_contract_production_ready(
    contract: Mapping[str, Any],
) -> None:
    """Fail closed until state, convergence, live, and trust blockers are closed."""

    validate_history_dependency_contract_hash(contract)
    # A caller may recompute a checksum over arbitrary JSON.  Production
    # authorization therefore cannot be granted by document-controlled fields
    # while the trusted code authority itself remains vetoed.
    if HISTORY_DEPENDENCY_PRODUCTION_STATUS != "PRODUCTION_READY":
        raise HistoryDependencyContractError(
            "history dependency contract is not production ready: code authority vetoed"
        )
    if (
        contract.get("production_eligible") is not True
        or str(contract.get("production_status") or "") != "PRODUCTION_READY"
        or contract.get("production_blockers")
    ):
        blockers = [str(value) for value in contract.get("production_blockers") or []]
        raise HistoryDependencyContractError(
            "history dependency contract is not production ready: "
            + (", ".join(blockers) if blockers else "UNSPECIFIED_BLOCKER")
        )


__all__ = [
    "BAR_DEPENDENCY_AXES",
    "CALENDAR_DEPENDENCY_AXES",
    "CANONICAL_TIMEFRAMES",
    "DEPENDENCY_LIKE_NON_HISTORY_AXES",
    "HISTORY_DEPENDENCY_CONTRACT_VERSION",
    "HISTORY_DEPENDENCY_PRODUCTION_STATUS",
    "HISTORY_DEPENDENCY_SERIALIZATION",
    "HistoryDependencyContractError",
    "IMPLIED_BAR_DEPENDENCY_AXES",
    "UnknownDependencyAxisError",
    "WARMUP_SAFETY_BUFFER_BARS",
    "WARMUP_SAFETY_MULTIPLIER",
    "assert_history_dependency_contract_production_ready",
    "build_history_dependency_contract",
    "classify_dependency_axis",
    "contract_content_sha256",
    "find_unrecognized_dependency_like_keys",
    "serialize_history_dependency_contract",
    "validate_history_dependency_contract_hash",
]
