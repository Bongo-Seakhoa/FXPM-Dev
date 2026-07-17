"""Authenticated production anchor geometry derived from native-history v5.

This module creates geometry only.  It resolves and reopens the active native
activation, independently revalidates the exact v5 run calendar, and derives
the repeated historical anchors without accepting dates, rosters, or market
verdicts from a caller.  The resulting receipt grants no search, deployment,
publication, or trading authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Final, Mapping

import pm_causal_process as causal
from pm_evidence import TIMEFRAME_EVIDENCE_CONTRACT_VERSION
from pm_native_activation import (
    ACTIVE_POINTER_SCHEMA,
    ACTIVATION_MANIFEST_SCHEMA,
    NATIVE_AUTHENTICITY_DECISION_BASIS,
    NATIVE_EVIDENCE_CANDIDATE,
    NATIVE_PRODUCTION_SYMBOLS,
    NATIVE_PRODUCTION_TIMEFRAMES,
    NATIVE_RUN_MANIFEST_SCHEMA,
    NATIVE_SOURCE_UNAVAILABLE,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    UNIVERSE_AUTHENTICITY_SCHEMA,
    ActiveNativeUniverse,
    resolve_active_native_universe,
)
from pm_native_request_contract import (
    COMMON_REQUEST_KEYS,
    PRODUCTION_SYMBOL_ORDER,
    PRODUCTION_SYMBOL_ORDER_SHA256,
    PRODUCTION_TIMEFRAME_ORDER,
    PRODUCTION_TIMEFRAME_ORDER_SHA256,
    NativeRequestContractError,
    validate_v5_common_request,
)


PLAN_SCHEMA: Final = "fxpm.production-anchor-plan.v1"
PLAN_SCOPE: Final = "AUTHENTICATED_GEOMETRY_INPUT_ONLY"
DEFAULT_ANCHOR_COUNT: Final = 4
DEFAULT_FORWARD_OOS_CALENDAR_DAYS: Final = 365
CAPITAL_INNER_SELECTION_CALENDAR_DAYS: Final = 365
CAPITAL_CURRENT_SELECTION_CALENDAR_DAYS: Final = 365
PINNED_MINIMUM_PURGE_CALENDAR_DAYS: Final = 35
STRICT_CUTOFF_SEPARATION_CALENDAR_DAYS: Final = 1
INTER_ANCHOR_OOS_GAP_CALENDAR_DAYS: Final = (
    PINNED_MINIMUM_PURGE_CALENDAR_DAYS + STRICT_CUTOFF_SEPARATION_CALENDAR_DAYS
)
MAX_AUTHORITY_FILE_BYTES: Final = 64 * 1024**2
MAX_RECEIPT_BYTES: Final = 1024**2

_UTC = timezone.utc
_HEX = frozenset("0123456789abcdef")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_AUTHORITY_FLAG = (
    "confers_search_process_current_refit_capital_readiness_publication_"
    "or_trading_authority"
)
_INTERVAL_FIELDS = frozenset({"start_utc", "end_exclusive_utc"})
_CAPITAL_WINDOW_FIELDS = frozenset({"start_date", "end_date"})
_CALENDAR_FIELDS = frozenset(
    {
        "window_contract_version",
        "timeframe_request_map_schema_version",
        "common_as_of_utc",
        "timeframe_window_years",
        "timeframe_calendar_boundaries_sha256",
        "timeframe_request_map_sha256",
        "timeframe_source_request_intervals",
        "timeframe_strategy_evidence_intervals",
    }
)
_RESOLVER_CALENDAR_FIELDS = (
    "common_as_of_utc",
    "common_requested_start_utc",
    "common_calendar_boundaries",
    "window_contract_version",
    "timeframe_calendar_boundaries",
    "timeframe_calendar_boundaries_sha256",
    "timeframe_request_map_sha256",
    "timeframe_request_map_schema_version",
    "timeframe_strategy_evidence_intervals",
    "timeframe_audit_reference_map",
    "timeframe_audit_consumer_map",
    "timeframe_audit_windows",
    "timeframe_audit_reference_intervals",
    "timeframe_audit_only_intervals",
    "timeframe_consensus_policy",
)
_ANCHOR_FIELDS = frozenset(
    {
        "anchor_id",
        "selection_cutoff_utc",
        "forward_start_date",
        "forward_end_date",
        "purge_calendar_days",
    }
)
_PROOF_FIELDS = frozenset(
    {
        "optimizer_train_years",
        "earliest_selection_cutoff_utc",
        "required_optimizer_train_start_utc",
        "strategy_evidence_start_utc",
        "strategy_evidence_end_exclusive_utc",
        "source_request_start_utc",
        "source_request_end_exclusive_utc",
        "authenticated_optimizer_train_depth_retained",
    }
)
_BODY_FIELDS = frozenset(
    {
        "schema_version",
        "activation_id",
        "activation_manifest_sha256",
        "native_run_manifest_sha256",
        "native_v5_common_request",
        "native_v5_common_request_sha256",
        "native_calendar",
        "native_calendar_sha256",
        "symbol_roster",
        "symbol_roster_sha256",
        "timeframe_roster",
        "timeframe_roster_sha256",
        "current_refit_cutoff_utc",
        "latest_fully_closed_common_date",
        "latest_historical_outer_end_date",
        "anchor_count",
        "minimum_purge_calendar_days",
        "forward_oos_calendar_days_per_anchor",
        "inter_anchor_oos_gap_calendar_days",
        "anchor_plan",
        "anchor_plan_sha256",
        "capital_inner_selection_calendar_days",
        "capital_current_selection_calendar_days",
        "inner_selection_windows_by_anchor",
        "inner_selection_windows_by_anchor_sha256",
        "current_selection_window",
        "current_selection_window_sha256",
        "optimizer_train_history_proof_by_timeframe",
        "minimum_additional_history_calendar_days_by_timeframe",
        "causal_process_policy_sha256",
        "scope",
        _AUTHORITY_FLAG,
    }
)


class ProductionAnchorPlanError(ValueError):
    """Typed fail-closed anchor-plan construction or verification error."""

    def __init__(self, code: str, details: tuple[str, ...] = ()) -> None:
        bounded = tuple(str(value)[:256] for value in tuple(details)[:64])
        self.code = str(code)
        self.details = bounded
        suffix = f":{'|'.join(bounded)}" if bounded else ""
        super().__init__(f"{self.code}{suffix}")


class ProductionAnchorPlanInsufficientHistory(ProductionAnchorPlanError):
    """Authenticated history cannot retain the configured training depth."""

    def __init__(self, additional_days_by_timeframe: Mapping[str, int]) -> None:
        self.minimum_additional_history_calendar_days_by_timeframe = {
            timeframe: int(additional_days_by_timeframe[timeframe])
            for timeframe in PRODUCTION_TIMEFRAME_ORDER
        }
        super().__init__(
            "INSUFFICIENT_AUTHENTICATED_OPTIMIZER_TRAIN_HISTORY",
            tuple(
                f"{timeframe}={self.minimum_additional_history_calendar_days_by_timeframe[timeframe]}"
                for timeframe in PRODUCTION_TIMEFRAME_ORDER
            ),
        )


def _fail(code: str, *details: Any) -> None:
    raise ProductionAnchorPlanError(code, tuple(str(value) for value in details))


def _canonical_bytes(value: Any, *, maximum: int = MAX_RECEIPT_BYTES) -> bytes:
    try:
        raw = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ProductionAnchorPlanError("CANONICAL_JSON_INVALID") from exc
    if not raw or len(raw) > maximum:
        _fail("CANONICAL_JSON_SIZE_INVALID", len(raw), maximum)
    try:
        if json.loads(raw.decode("ascii")) != value:
            _fail("CANONICAL_JSON_ROUND_TRIP_MISMATCH")
    except json.JSONDecodeError as exc:  # pragma: no cover - json emitted it.
        raise ProductionAnchorPlanError("CANONICAL_JSON_INVALID") from exc
    return raw


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in _HEX for character in value)
    )


def _require_sha256(value: Any, code: str) -> str:
    if not _is_sha256(value):
        _fail(code)
    return str(value)


def _safe_id(value: Any, code: str) -> str:
    if type(value) is not str or _SAFE_ID.fullmatch(value) is None:
        _fail(code)
    return value


def _utc(value: Any, code: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ProductionAnchorPlanError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    parsed = parsed.astimezone(_UTC)
    if _iso(parsed) != value:
        _fail(code)
    return parsed


def _iso(value: datetime) -> str:
    normalized = value.astimezone(_UTC)
    if normalized.microsecond:
        return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


def _calendar_date(value: Any, code: str) -> date:
    if type(value) is not str or len(value) != 10:
        _fail(code)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ProductionAnchorPlanError(code) from exc
    if parsed.isoformat() != value:
        _fail(code)
    return parsed


def _read_json_bounded(path: Path, code: str) -> tuple[dict[str, Any], bytes]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ProductionAnchorPlanError(f"{code}_MISSING") from exc
    if not 0 < size <= MAX_AUTHORITY_FILE_BYTES:
        _fail(f"{code}_SIZE_INVALID", size)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ProductionAnchorPlanError(f"{code}_READ_FAILED") from exc
    if len(raw) != size:
        _fail(f"{code}_READ_SIZE_MISMATCH")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProductionAnchorPlanError(f"{code}_JSON_INVALID") from exc
    if type(value) is not dict:
        _fail(f"{code}_NOT_OBJECT")
    return value, raw


def _validate_universe(universe: Any) -> ActiveNativeUniverse:
    if type(universe) is not ActiveNativeUniverse:
        _fail("NATIVE_UNIVERSE_EXACT_TYPE_REQUIRED")
    if universe.errors or not universe.valid:
        raise ProductionAnchorPlanError(
            "NATIVE_UNIVERSE_RESOLUTION_FAILED",
            tuple(str(value) for value in universe.errors),
        )
    if tuple(universe.requested_symbols) != tuple(NATIVE_PRODUCTION_SYMBOLS):
        _fail("NATIVE_UNIVERSE_SYMBOL_ROSTER_MISMATCH")
    if tuple(universe.required_timeframes) != tuple(NATIVE_PRODUCTION_TIMEFRAMES):
        _fail("NATIVE_UNIVERSE_TIMEFRAME_ROSTER_MISMATCH")
    if tuple(NATIVE_PRODUCTION_SYMBOLS) != tuple(PRODUCTION_SYMBOL_ORDER) or tuple(
        NATIVE_PRODUCTION_TIMEFRAMES
    ) != tuple(PRODUCTION_TIMEFRAME_ORDER):
        _fail("PINNED_NATIVE_ROSTER_MODULE_MISMATCH")
    if (
        universe.explicit_contract is not True
        or universe.exact_symbol_identity is not True
        or universe.eligibility_schema != UNIVERSE_AUTHENTICITY_SCHEMA
        or universe.decision_basis != NATIVE_AUTHENTICITY_DECISION_BASIS
    ):
        _fail("NATIVE_UNIVERSE_CURRENT_AUTHENTICITY_REQUIRED")
    if universe.excluded_symbols or universe.fully_excluded_symbols:
        _fail("NATIVE_UNIVERSE_EXCLUDED_NO_TRADE_AUTHORITY_FORBIDDEN")
    if set(universe.cell_statuses) != set(NATIVE_PRODUCTION_SYMBOLS):
        _fail("NATIVE_UNIVERSE_CELL_SYMBOL_GRID_MISMATCH")
    allowed = {NATIVE_EVIDENCE_CANDIDATE, NATIVE_SOURCE_UNAVAILABLE}
    for symbol in NATIVE_PRODUCTION_SYMBOLS:
        grid = universe.cell_statuses.get(symbol)
        if not isinstance(grid, Mapping) or set(grid) != set(
            NATIVE_PRODUCTION_TIMEFRAMES
        ):
            _fail("NATIVE_UNIVERSE_CELL_TIMEFRAME_GRID_MISMATCH", symbol)
        for timeframe in NATIVE_PRODUCTION_TIMEFRAMES:
            status = grid.get(timeframe)
            if status == "excluded_no_trade":
                _fail(
                    "NATIVE_UNIVERSE_EXCLUDED_NO_TRADE_AUTHORITY_FORBIDDEN",
                    symbol,
                    timeframe,
                )
            if status not in allowed:
                _fail(
                    "NATIVE_UNIVERSE_NONTERMINAL_CELL_FORBIDDEN",
                    symbol,
                    timeframe,
                    status,
                )
    return universe


def _resolver_calendar_projection(
    common: Mapping[str, Any], request: Mapping[str, Any]
) -> dict[str, Any]:
    value = {
        "common_as_of_utc": common["common_as_of_utc"],
        "common_requested_start_utc": common["common_requested_start_utc"],
        "common_calendar_boundaries": request.get("common_calendar_boundaries"),
        **{
            field: common[field]
            for field in _RESOLVER_CALENDAR_FIELDS
            if field
            not in {
                "common_as_of_utc",
                "common_requested_start_utc",
                "common_calendar_boundaries",
            }
        },
    }
    return json.loads(_canonical_bytes(value).decode("ascii"))


def _load_verified_v5_common(
    data_dir: Path, universe: ActiveNativeUniverse
) -> tuple[dict[str, Any], str, str, str]:
    native_root = (Path(data_dir).resolve() / "native").resolve()
    activation_id = _safe_id(universe.activation_id, "ACTIVATION_ID_INVALID")
    expected_activation_sha = _require_sha256(
        universe.activation_manifest_sha256,
        "ACTIVATION_MANIFEST_SHA256_INVALID",
    )
    pointer_path = native_root / "ACTIVE.json"
    pointer, pointer_before = _read_json_bounded(pointer_path, "ACTIVE_POINTER")
    if (
        set(pointer)
        != {
            "schema_version",
            "activation_id",
            "manifest_relative_path",
            "manifest_sha256",
            "activated_at_utc",
            "production_authoritative",
        }
        or pointer.get("schema_version") != ACTIVE_POINTER_SCHEMA
        or pointer.get("activation_id") != activation_id
        or pointer.get("manifest_relative_path") != f"activations/{activation_id}.json"
        or pointer.get("manifest_sha256") != expected_activation_sha
        or pointer.get("production_authoritative") is not True
    ):
        _fail("ACTIVE_POINTER_AUTHORITY_MISMATCH")
    expected_activation = (
        native_root / "activations" / f"{activation_id}.json"
    ).resolve()
    try:
        expected_activation.relative_to(native_root)
    except ValueError as exc:
        raise ProductionAnchorPlanError("ACTIVATION_MANIFEST_PATH_UNSAFE") from exc
    observed_activation = universe.activation_manifest_path
    if (
        not isinstance(observed_activation, Path)
        or observed_activation.resolve() != expected_activation
    ):
        _fail("ACTIVATION_MANIFEST_PATH_MISMATCH")
    activation, activation_raw = _read_json_bounded(
        expected_activation, "ACTIVATION_MANIFEST"
    )
    if hashlib.sha256(activation_raw).hexdigest() != expected_activation_sha:
        _fail("ACTIVATION_MANIFEST_SHA256_MISMATCH")
    if (
        activation.get("schema_version") != ACTIVATION_MANIFEST_SCHEMA
        or activation.get("activation_id") != activation_id
        or str(activation.get("status") or "").lower() != "active"
        or activation.get("production_authoritative") is not True
    ):
        _fail("ACTIVATION_MANIFEST_AUTHORITY_INVALID")

    run_id = _safe_id(activation.get("run_id"), "NATIVE_RUN_ID_INVALID")
    run_relative = f"runs/{run_id}/run_manifest.json"
    if (
        str(activation.get("run_manifest_relative_path") or "").replace("\\", "/")
        != run_relative
    ):
        _fail("NATIVE_RUN_MANIFEST_PATH_IDENTITY_MISMATCH")
    run_sha = _require_sha256(
        activation.get("run_manifest_sha256"),
        "NATIVE_RUN_MANIFEST_SHA256_INVALID",
    )
    run_path = (native_root / Path(run_relative)).resolve()
    try:
        run_path.relative_to(native_root)
    except ValueError as exc:  # pragma: no cover - exact relative path is fixed.
        raise ProductionAnchorPlanError("NATIVE_RUN_MANIFEST_PATH_UNSAFE") from exc
    run, run_raw = _read_json_bounded(run_path, "NATIVE_RUN_MANIFEST")
    if hashlib.sha256(run_raw).hexdigest() != run_sha:
        _fail("NATIVE_RUN_MANIFEST_SHA256_MISMATCH")
    if (
        run.get("schema_version") != NATIVE_RUN_MANIFEST_SCHEMA
        or run.get("run_id") != run_id
        or run.get("immutable_record") is not True
        or run.get("complete") is not True
    ):
        _fail("NATIVE_RUN_MANIFEST_AUTHORITY_INVALID")
    request = run.get("request")
    if not isinstance(request, Mapping):
        _fail("NATIVE_RUN_REQUEST_INVALID")
    expected_request_fields = {
        *COMMON_REQUEST_KEYS,
        "symbols",
        "timeframes",
        "retries",
        "window_years",
        "common_calendar_boundaries",
    }
    if set(request) != expected_request_fields:
        _fail("NATIVE_RUN_REQUEST_FIELDS_INVALID")
    if request.get("symbols") != list(PRODUCTION_SYMBOL_ORDER):
        _fail("NATIVE_RUN_REQUEST_SYMBOL_ROSTER_MISMATCH")
    if request.get("timeframes") != list(PRODUCTION_TIMEFRAME_ORDER):
        _fail("NATIVE_RUN_REQUEST_TIMEFRAME_ROSTER_MISMATCH")
    if request.get("retries") != 1:
        _fail("NATIVE_RUN_REQUEST_RETRY_POLICY_MISMATCH")
    if (
        request.get("timeframe_request_map_schema_version")
        != TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
    ):
        _fail("NATIVE_CALENDAR_V5_REQUIRED")
    try:
        common = validate_v5_common_request(
            {field: request.get(field) for field in COMMON_REQUEST_KEYS}
        )
    except NativeRequestContractError as exc:
        raise ProductionAnchorPlanError(
            "NATIVE_CALENDAR_V5_VALIDATION_FAILED",
            tuple(str(value) for value in exc.errors),
        ) from exc
    if (
        request.get("window_years") != common["timeframe_window_years"]["M5"]
        or request.get("common_calendar_boundaries")
        != common["timeframe_calendar_boundaries"]["M5"]
    ):
        _fail("NATIVE_RUN_REQUEST_COMPATIBILITY_PROJECTION_MISMATCH")
    expected_resolver_calendar = _resolver_calendar_projection(common, request)
    if type(universe.calendar_contract) is not dict or _canonical_bytes(
        universe.calendar_contract
    ) != _canonical_bytes(expected_resolver_calendar):
        _fail("NATIVE_RESOLVER_CALENDAR_BINDING_MISMATCH")
    _pointer_after, pointer_after = _read_json_bounded(pointer_path, "ACTIVE_POINTER")
    if pointer_after != pointer_before:
        _fail("ACTIVE_POINTER_CHANGED_DURING_DERIVATION")
    return common, run_sha, activation_id, expected_activation_sha


def _resolve_verified_native_authority(
    data_dir: Path,
) -> tuple[dict[str, Any], str, str, str]:
    universe = resolve_active_native_universe(
        Path(data_dir),
        NATIVE_PRODUCTION_SYMBOLS,
        required_timeframes=NATIVE_PRODUCTION_TIMEFRAMES,
        verify_exclusion_evidence=True,
        production_contract_required=True,
    )
    universe = _validate_universe(universe)
    return _load_verified_v5_common(Path(data_dir), universe)


def _calendar_projection(common: Mapping[str, Any]) -> dict[str, Any]:
    value = {
        "window_contract_version": common["window_contract_version"],
        "timeframe_request_map_schema_version": common[
            "timeframe_request_map_schema_version"
        ],
        "common_as_of_utc": common["common_as_of_utc"],
        "timeframe_window_years": common["timeframe_window_years"],
        "timeframe_calendar_boundaries_sha256": common[
            "timeframe_calendar_boundaries_sha256"
        ],
        "timeframe_request_map_sha256": common["timeframe_request_map_sha256"],
        "timeframe_source_request_intervals": common[
            "timeframe_source_request_intervals"
        ],
        "timeframe_strategy_evidence_intervals": common[
            "timeframe_strategy_evidence_intervals"
        ],
    }
    return json.loads(_canonical_bytes(value).decode("ascii"))


def _validate_v5_common_preimage(value: Any) -> dict[str, Any]:
    """Rebuild the complete v5 request contract carried by the receipt."""

    if type(value) is not dict or set(value) != set(COMMON_REQUEST_KEYS):
        _fail("NATIVE_V5_COMMON_REQUEST_FIELDS_INVALID")
    try:
        common = validate_v5_common_request(value)
    except NativeRequestContractError as exc:
        raise ProductionAnchorPlanError(
            "NATIVE_V5_COMMON_REQUEST_VALIDATION_FAILED",
            tuple(str(item) for item in exc.errors),
        ) from exc
    if _canonical_bytes(common) != _canonical_bytes(value):
        _fail("NATIVE_V5_COMMON_REQUEST_CANONICAL_DERIVATION_MISMATCH")
    return json.loads(_canonical_bytes(common).decode("ascii"))


def _validate_calendar_projection(value: Any) -> tuple[dict[str, Any], datetime]:
    if type(value) is not dict or set(value) != _CALENDAR_FIELDS:
        _fail("NATIVE_CALENDAR_FIELDS_INVALID")
    if (
        value.get("window_contract_version") != TIMEFRAME_EVIDENCE_CONTRACT_VERSION
        or value.get("timeframe_request_map_schema_version")
        != TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
    ):
        _fail("NATIVE_CALENDAR_V5_REQUIRED")
    common_as_of = _utc(value.get("common_as_of_utc"), "COMMON_AS_OF_UTC_INVALID")
    _require_sha256(
        value.get("timeframe_calendar_boundaries_sha256"),
        "NATIVE_CALENDAR_BOUNDARIES_SHA256_INVALID",
    )
    _require_sha256(
        value.get("timeframe_request_map_sha256"),
        "NATIVE_CALENDAR_REQUEST_MAP_SHA256_INVALID",
    )
    years_map = value.get("timeframe_window_years")
    sources = value.get("timeframe_source_request_intervals")
    strategies = value.get("timeframe_strategy_evidence_intervals")
    for name, mapping in (
        ("TIMEFRAME_WINDOW_YEARS", years_map),
        ("TIMEFRAME_SOURCE_INTERVALS", sources),
        ("TIMEFRAME_STRATEGY_INTERVALS", strategies),
    ):
        if not isinstance(mapping, Mapping) or set(mapping) != set(
            PRODUCTION_TIMEFRAME_ORDER
        ):
            _fail(f"NATIVE_CALENDAR_{name}_ROSTER_INVALID")
    strategy_ends: list[datetime] = []
    for timeframe in PRODUCTION_TIMEFRAME_ORDER:
        years = years_map[timeframe]
        if not isinstance(years, Mapping) or set(years) != {
            "optimizer_train",
            "tuning_validation",
            "final_lockbox",
        }:
            _fail("NATIVE_CALENDAR_YEAR_PARTITIONS_INVALID", timeframe)
        for partition, count in years.items():
            if type(count) is not int or type(count) is bool or count <= 0:
                _fail("NATIVE_CALENDAR_YEAR_VALUE_INVALID", timeframe, partition)
        source = sources[timeframe]
        strategy = strategies[timeframe]
        if (
            not isinstance(source, Mapping)
            or set(source) != _INTERVAL_FIELDS
            or not isinstance(strategy, Mapping)
            or set(strategy) != _INTERVAL_FIELDS
        ):
            _fail("NATIVE_CALENDAR_INTERVAL_FIELDS_INVALID", timeframe)
        source_start = _utc(
            source["start_utc"], f"NATIVE_SOURCE_START_INVALID:{timeframe}"
        )
        source_end = _utc(
            source["end_exclusive_utc"],
            f"NATIVE_SOURCE_END_INVALID:{timeframe}",
        )
        strategy_start = _utc(
            strategy["start_utc"],
            f"NATIVE_STRATEGY_START_INVALID:{timeframe}",
        )
        strategy_end = _utc(
            strategy["end_exclusive_utc"],
            f"NATIVE_STRATEGY_END_INVALID:{timeframe}",
        )
        if (
            source_start >= source_end
            or strategy_start >= strategy_end
            or source_start > strategy_start
            or source_end != strategy_end
        ):
            _fail("NATIVE_CALENDAR_SOURCE_STRATEGY_INTERVAL_MISMATCH", timeframe)
        strategy_ends.append(strategy_end)
    current_cutoff = min(
        datetime(end.year, end.month, end.day, tzinfo=_UTC) for end in strategy_ends
    )
    common_midnight = datetime(
        common_as_of.year, common_as_of.month, common_as_of.day, tzinfo=_UTC
    )
    if current_cutoff != common_midnight:
        _fail("NATIVE_CALENDAR_COMMON_CLOSED_MIDNIGHT_MISMATCH")
    return json.loads(_canonical_bytes(value).decode("ascii")), current_cutoff


def _derive_current_selection_window(current_cutoff: datetime) -> dict[str, str]:
    if current_cutoff.tzinfo != _UTC or current_cutoff.time() != datetime.min.time():
        _fail("CURRENT_REFIT_CUTOFF_NOT_UTC_MIDNIGHT")
    end = current_cutoff.date()
    start = end - timedelta(days=CAPITAL_CURRENT_SELECTION_CALENDAR_DAYS)
    return {"start_date": start.isoformat(), "end_date": end.isoformat()}


def _derive_anchor_plan(current_cutoff: datetime) -> list[dict[str, Any]]:
    current_window = _derive_current_selection_window(current_cutoff)
    latest_end = date.fromisoformat(current_window["start_date"]) - timedelta(days=1)
    reverse_geometry: list[tuple[date, date, date]] = []
    next_cutoff: date | None = None
    for _ in range(DEFAULT_ANCHOR_COUNT):
        forward_end = (
            latest_end
            if next_cutoff is None
            else next_cutoff
            - timedelta(days=STRICT_CUTOFF_SEPARATION_CALENDAR_DAYS + 1)
        )
        forward_start = forward_end - timedelta(
            days=DEFAULT_FORWARD_OOS_CALENDAR_DAYS - 1
        )
        selection_cutoff = forward_start - timedelta(
            days=PINNED_MINIMUM_PURGE_CALENDAR_DAYS
        )
        reverse_geometry.append((selection_cutoff, forward_start, forward_end))
        next_cutoff = selection_cutoff
    anchors: list[dict[str, Any]] = []
    for ordinal, (selection, start, end) in enumerate(
        reversed(reverse_geometry), start=1
    ):
        anchors.append(
            {
                "anchor_id": f"causal-anchor-{ordinal:02d}-{selection:%Y%m%d}",
                "selection_cutoff_utc": _iso(
                    datetime(
                        selection.year, selection.month, selection.day, tzinfo=_UTC
                    )
                ),
                "forward_start_date": start.isoformat(),
                "forward_end_date": end.isoformat(),
                "purge_calendar_days": PINNED_MINIMUM_PURGE_CALENDAR_DAYS,
            }
        )
    try:
        causal._validate_anchor_plan(  # noqa: SLF001 - released causal verifier.
            json.loads(_canonical_bytes(anchors).decode("ascii"))
        )
    except causal.ProcessVerificationError as exc:
        raise ProductionAnchorPlanError("CAUSAL_ANCHOR_PLAN_REJECTED") from exc
    return anchors


def _derive_inner_selection_windows(
    anchors: list[dict[str, Any]],
) -> dict[str, dict[str, str]]:
    windows: dict[str, dict[str, str]] = {}
    for anchor in anchors:
        cutoff = _utc(
            anchor["selection_cutoff_utc"], "CAPITAL_INNER_SELECTION_CUTOFF_INVALID"
        ).date()
        windows[anchor["anchor_id"]] = {
            "start_date": (
                cutoff - timedelta(days=CAPITAL_INNER_SELECTION_CALENDAR_DAYS)
            ).isoformat(),
            "end_date": cutoff.isoformat(),
        }
    return windows


def _capital_window(value: Any, code: str) -> tuple[dict[str, str], date, date]:
    if type(value) is not dict or set(value) != _CAPITAL_WINDOW_FIELDS:
        _fail(code)
    start = _calendar_date(value["start_date"], code)
    end = _calendar_date(value["end_date"], code)
    if end <= start:
        _fail(code)
    return dict(value), start, end


def _subtract_calendar_years(value: datetime, years: int) -> datetime:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _history_proofs(
    calendar: Mapping[str, Any], anchors: list[dict[str, Any]]
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    earliest = _utc(
        anchors[0]["selection_cutoff_utc"], "EARLIEST_ANCHOR_CUTOFF_INVALID"
    )
    proofs: dict[str, dict[str, Any]] = {}
    additional_days: dict[str, int] = {}
    years_map = calendar["timeframe_window_years"]
    strategies = calendar["timeframe_strategy_evidence_intervals"]
    sources = calendar["timeframe_source_request_intervals"]
    for timeframe in PRODUCTION_TIMEFRAME_ORDER:
        years = years_map[timeframe]["optimizer_train"]
        required_start = _subtract_calendar_years(earliest, years)
        strategy_start = _utc(
            strategies[timeframe]["start_utc"],
            f"NATIVE_STRATEGY_START_INVALID:{timeframe}",
        )
        strategy_end = _utc(
            strategies[timeframe]["end_exclusive_utc"],
            f"NATIVE_STRATEGY_END_INVALID:{timeframe}",
        )
        source_start = _utc(
            sources[timeframe]["start_utc"],
            f"NATIVE_SOURCE_START_INVALID:{timeframe}",
        )
        source_end = _utc(
            sources[timeframe]["end_exclusive_utc"],
            f"NATIVE_SOURCE_END_INVALID:{timeframe}",
        )
        if earliest > strategy_end:
            _fail("AUTHENTICATED_STRATEGY_INTERVAL_END_TOO_EARLY", timeframe)
        shortfall = max(timedelta(0), strategy_start - required_start)
        minimum_days = shortfall.days + int(
            bool(shortfall.seconds or shortfall.microseconds)
        )
        additional_days[timeframe] = minimum_days
        proofs[timeframe] = {
            "optimizer_train_years": years,
            "earliest_selection_cutoff_utc": _iso(earliest),
            "required_optimizer_train_start_utc": _iso(required_start),
            "strategy_evidence_start_utc": _iso(strategy_start),
            "strategy_evidence_end_exclusive_utc": _iso(strategy_end),
            "source_request_start_utc": _iso(source_start),
            "source_request_end_exclusive_utc": _iso(source_end),
            "authenticated_optimizer_train_depth_retained": minimum_days == 0,
        }
    if any(additional_days.values()):
        raise ProductionAnchorPlanInsufficientHistory(additional_days)
    return proofs, additional_days


def _causal_policy_sha256() -> str:
    policy = causal.pinned_process_policy()
    if (
        policy.get("minimum_anchor_count") != DEFAULT_ANCHOR_COUNT
        or policy.get("minimum_days_per_anchor") != DEFAULT_FORWARD_OOS_CALENDAR_DAYS
        or policy.get("minimum_purge_calendar_days")
        != PINNED_MINIMUM_PURGE_CALENDAR_DAYS
    ):
        _fail("CAUSAL_PROCESS_POLICY_PIN_MISMATCH")
    return _sha(policy)


@dataclass(frozen=True, slots=True)
class ProductionAnchorPlanReceipt:
    """Portable canonical plan bytes bound to a content hash."""

    _body_json: bytes
    _receipt_sha256: str

    @property
    def receipt_sha256(self) -> str:
        return self._receipt_sha256

    @property
    def canonical_body_bytes(self) -> bytes:
        return bytes(self._body_json)

    def to_dict(self) -> dict[str, Any]:
        body = json.loads(self._body_json.decode("ascii"))
        body["receipt_sha256"] = self._receipt_sha256
        return body


def _verify_receipt_against_authority(
    receipt: Any,
    *,
    authority_common: Mapping[str, Any],
    authority_run_sha256: str,
    authority_activation_id: str,
    authority_activation_sha256: str,
) -> dict[str, Any]:
    """Verify one receipt against independently reopened authority values."""

    if type(receipt) is not ProductionAnchorPlanReceipt:
        _fail("ANCHOR_PLAN_RECEIPT_EXACT_TYPE_REQUIRED")
    if (
        type(receipt._body_json) is not bytes
        or not receipt._body_json
        or len(receipt._body_json) > MAX_RECEIPT_BYTES
    ):
        _fail("ANCHOR_PLAN_RECEIPT_BYTES_INVALID")
    try:
        body = json.loads(receipt._body_json.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProductionAnchorPlanError("ANCHOR_PLAN_RECEIPT_JSON_INVALID") from exc
    if type(body) is not dict or set(body) != _BODY_FIELDS:
        _fail("ANCHOR_PLAN_RECEIPT_FIELDS_INVALID")
    if _canonical_bytes(body) != receipt._body_json:
        _fail("ANCHOR_PLAN_RECEIPT_NOT_CANONICAL")
    observed_sha = hashlib.sha256(receipt._body_json).hexdigest()
    if receipt._receipt_sha256 != observed_sha:
        _fail("ANCHOR_PLAN_RECEIPT_SHA256_MISMATCH")
    if body["schema_version"] != PLAN_SCHEMA:
        _fail("ANCHOR_PLAN_SCHEMA_INVALID")
    activation_id = _safe_id(body["activation_id"], "ACTIVATION_ID_INVALID")
    activation_sha = _require_sha256(
        body["activation_manifest_sha256"], "ACTIVATION_MANIFEST_SHA256_INVALID"
    )
    run_sha = _require_sha256(
        body["native_run_manifest_sha256"], "NATIVE_RUN_MANIFEST_SHA256_INVALID"
    )
    if (
        activation_id != authority_activation_id
        or activation_sha != authority_activation_sha256
        or run_sha != authority_run_sha256
        or _canonical_bytes(body["native_v5_common_request"])
        != _canonical_bytes(authority_common)
    ):
        _fail("ANCHOR_PLAN_CURRENT_NATIVE_AUTHORITY_MISMATCH")
    if body["symbol_roster"] != list(PRODUCTION_SYMBOL_ORDER):
        _fail("ANCHOR_PLAN_SYMBOL_ROSTER_MISMATCH")
    if body["symbol_roster_sha256"] != PRODUCTION_SYMBOL_ORDER_SHA256:
        _fail("ANCHOR_PLAN_SYMBOL_ROSTER_SHA256_MISMATCH")
    if body["timeframe_roster"] != list(PRODUCTION_TIMEFRAME_ORDER):
        _fail("ANCHOR_PLAN_TIMEFRAME_ROSTER_MISMATCH")
    if body["timeframe_roster_sha256"] != PRODUCTION_TIMEFRAME_ORDER_SHA256:
        _fail("ANCHOR_PLAN_TIMEFRAME_ROSTER_SHA256_MISMATCH")
    common = _validate_v5_common_preimage(body["native_v5_common_request"])
    if body["native_v5_common_request_sha256"] != _sha(common):
        _fail("NATIVE_V5_COMMON_REQUEST_SHA256_MISMATCH")
    calendar, expected_cutoff = _validate_calendar_projection(body["native_calendar"])
    if calendar != _calendar_projection(common):
        _fail("NATIVE_CALENDAR_COMMON_REQUEST_PROJECTION_MISMATCH")
    if body["native_calendar_sha256"] != _sha(calendar):
        _fail("NATIVE_CALENDAR_SHA256_MISMATCH")
    if body["current_refit_cutoff_utc"] != _iso(expected_cutoff):
        _fail("CURRENT_REFIT_CUTOFF_DERIVATION_MISMATCH")
    if (
        body["latest_fully_closed_common_date"]
        != (expected_cutoff.date() - timedelta(days=1)).isoformat()
    ):
        _fail("LATEST_FULLY_CLOSED_COMMON_DATE_MISMATCH")
    if (
        body["anchor_count"] != DEFAULT_ANCHOR_COUNT
        or body["minimum_purge_calendar_days"] != PINNED_MINIMUM_PURGE_CALENDAR_DAYS
        or body["forward_oos_calendar_days_per_anchor"]
        != DEFAULT_FORWARD_OOS_CALENDAR_DAYS
        or body["inter_anchor_oos_gap_calendar_days"]
        != INTER_ANCHOR_OOS_GAP_CALENDAR_DAYS
        or body["capital_inner_selection_calendar_days"]
        != CAPITAL_INNER_SELECTION_CALENDAR_DAYS
        or body["capital_current_selection_calendar_days"]
        != CAPITAL_CURRENT_SELECTION_CALENDAR_DAYS
    ):
        _fail("ANCHOR_PLAN_GEOMETRY_POLICY_MISMATCH")
    expected_anchors = _derive_anchor_plan(expected_cutoff)
    if body["anchor_plan"] != expected_anchors:
        _fail("ANCHOR_PLAN_GEOMETRY_DERIVATION_MISMATCH")
    if body["anchor_plan_sha256"] != _sha(expected_anchors):
        _fail("ANCHOR_PLAN_SHA256_MISMATCH")
    latest_outer_end = _calendar_date(
        expected_anchors[-1]["forward_end_date"],
        "LATEST_HISTORICAL_OUTER_END_INVALID",
    )
    if body["latest_historical_outer_end_date"] != latest_outer_end.isoformat():
        _fail("LATEST_HISTORICAL_OUTER_END_MISMATCH")

    current_window, current_start, current_end = _capital_window(
        body["current_selection_window"], "CAPITAL_CURRENT_SELECTION_WINDOW_INVALID"
    )
    if body["current_selection_window_sha256"] != _sha(current_window):
        _fail("CAPITAL_CURRENT_SELECTION_WINDOW_SHA256_MISMATCH")
    if current_start != latest_outer_end + timedelta(days=1):
        _fail("CAPITAL_CURRENT_SELECTION_REUSES_OR_GAPS_OUTER_EVIDENCE")
    if (current_end - current_start).days != CAPITAL_CURRENT_SELECTION_CALENDAR_DAYS:
        _fail("CAPITAL_CURRENT_SELECTION_WINDOW_LENGTH_MISMATCH")
    if current_end != expected_cutoff.date():
        _fail("CAPITAL_CURRENT_SELECTION_WINDOW_CUTOFF_MISMATCH")
    expected_current_window = _derive_current_selection_window(expected_cutoff)
    if current_window != expected_current_window:
        _fail("CAPITAL_CURRENT_SELECTION_WINDOW_DERIVATION_MISMATCH")

    inner_windows = body["inner_selection_windows_by_anchor"]
    anchor_ids = [anchor["anchor_id"] for anchor in expected_anchors]
    if type(inner_windows) is not dict or set(inner_windows) != set(anchor_ids):
        _fail("CAPITAL_INNER_SELECTION_WINDOW_ROSTER_INVALID")
    if body["inner_selection_windows_by_anchor_sha256"] != _sha(inner_windows):
        _fail("CAPITAL_INNER_SELECTION_WINDOWS_SHA256_MISMATCH")
    for anchor in expected_anchors:
        anchor_id = anchor["anchor_id"]
        _window_body, inner_start, inner_end = _capital_window(
            inner_windows[anchor_id], "CAPITAL_INNER_SELECTION_WINDOW_INVALID"
        )
        if (inner_end - inner_start).days != CAPITAL_INNER_SELECTION_CALENDAR_DAYS:
            _fail("CAPITAL_INNER_SELECTION_WINDOW_LENGTH_MISMATCH", anchor_id)
        cutoff_date = _utc(
            anchor["selection_cutoff_utc"], "CAPITAL_INNER_SELECTION_CUTOFF_INVALID"
        ).date()
        outer_start = _calendar_date(
            anchor["forward_start_date"], "CAPITAL_OUTER_START_INVALID"
        )
        if inner_end != cutoff_date or inner_end > outer_start:
            _fail("CAPITAL_INNER_SELECTION_WINDOW_LEAKAGE", anchor_id)
    expected_inner_windows = _derive_inner_selection_windows(expected_anchors)
    if inner_windows != expected_inner_windows:
        _fail("CAPITAL_INNER_SELECTION_WINDOW_DERIVATION_MISMATCH")

    proofs = body["optimizer_train_history_proof_by_timeframe"]
    if not isinstance(proofs, Mapping) or set(proofs) != set(
        PRODUCTION_TIMEFRAME_ORDER
    ):
        _fail("OPTIMIZER_TRAIN_HISTORY_PROOF_ROSTER_INVALID")
    for timeframe, proof in proofs.items():
        if type(proof) is not dict or set(proof) != _PROOF_FIELDS:
            _fail("OPTIMIZER_TRAIN_HISTORY_PROOF_FIELDS_INVALID", timeframe)
    expected_proofs, expected_additional_days = _history_proofs(
        calendar, expected_anchors
    )
    if proofs != expected_proofs:
        _fail("OPTIMIZER_TRAIN_HISTORY_PROOF_MISMATCH")
    additional_days = body["minimum_additional_history_calendar_days_by_timeframe"]
    if (
        type(additional_days) is not dict
        or set(additional_days) != set(PRODUCTION_TIMEFRAME_ORDER)
        or any(
            type(value) is not int or value < 0 for value in additional_days.values()
        )
        or additional_days != expected_additional_days
    ):
        _fail("MINIMUM_ADDITIONAL_HISTORY_DAYS_MISMATCH")
    if body["causal_process_policy_sha256"] != _causal_policy_sha256():
        _fail("CAUSAL_PROCESS_POLICY_SHA256_MISMATCH")
    if body["scope"] != PLAN_SCOPE or body[_AUTHORITY_FLAG] is not False:
        _fail("ANCHOR_PLAN_AUTHORITY_SCOPE_INVALID")
    result = json.loads(receipt._body_json.decode("ascii"))
    result["receipt_sha256"] = receipt._receipt_sha256
    return result


def verify_production_anchor_plan_receipt(
    data_dir: Path, receipt: Any, /
) -> dict[str, Any]:
    """Reopen current native authority, then verify every receipt identity.

    ``data_dir`` is an explicit trusted input.  The receipt cannot authenticate
    the mutable ACTIVE pointer by self-hash, so verification never accepts a
    caller-supplied activation/run root and is portable across process restarts.
    """

    common, run_sha, activation_id, activation_sha = _resolve_verified_native_authority(
        Path(data_dir)
    )
    return _verify_receipt_against_authority(
        receipt,
        authority_common=common,
        authority_run_sha256=run_sha,
        authority_activation_id=activation_id,
        authority_activation_sha256=activation_sha,
    )


def derive_production_anchor_plan(data_dir: Path, /) -> ProductionAnchorPlanReceipt:
    """Resolve native authority and derive the only production plan.

    The positional-only API intentionally accepts no date, symbol, timeframe,
    anchor, issuer, or authority override.
    """

    common, run_sha, activation_id, activation_sha = _resolve_verified_native_authority(
        Path(data_dir)
    )
    calendar, current_cutoff = _validate_calendar_projection(
        _calendar_projection(common)
    )
    current_selection_window = _derive_current_selection_window(current_cutoff)
    anchors = _derive_anchor_plan(current_cutoff)
    inner_selection_windows = _derive_inner_selection_windows(anchors)
    proofs, additional_history_days = _history_proofs(calendar, anchors)
    body = {
        "schema_version": PLAN_SCHEMA,
        "activation_id": activation_id,
        "activation_manifest_sha256": activation_sha,
        "native_run_manifest_sha256": run_sha,
        "native_v5_common_request": common,
        "native_v5_common_request_sha256": _sha(common),
        "native_calendar": calendar,
        "native_calendar_sha256": _sha(calendar),
        "symbol_roster": list(PRODUCTION_SYMBOL_ORDER),
        "symbol_roster_sha256": PRODUCTION_SYMBOL_ORDER_SHA256,
        "timeframe_roster": list(PRODUCTION_TIMEFRAME_ORDER),
        "timeframe_roster_sha256": PRODUCTION_TIMEFRAME_ORDER_SHA256,
        "current_refit_cutoff_utc": _iso(current_cutoff),
        "latest_fully_closed_common_date": (
            current_cutoff.date() - timedelta(days=1)
        ).isoformat(),
        "latest_historical_outer_end_date": anchors[-1]["forward_end_date"],
        "anchor_count": DEFAULT_ANCHOR_COUNT,
        "minimum_purge_calendar_days": PINNED_MINIMUM_PURGE_CALENDAR_DAYS,
        "forward_oos_calendar_days_per_anchor": (DEFAULT_FORWARD_OOS_CALENDAR_DAYS),
        "inter_anchor_oos_gap_calendar_days": (INTER_ANCHOR_OOS_GAP_CALENDAR_DAYS),
        "anchor_plan": anchors,
        "anchor_plan_sha256": _sha(anchors),
        "capital_inner_selection_calendar_days": (
            CAPITAL_INNER_SELECTION_CALENDAR_DAYS
        ),
        "capital_current_selection_calendar_days": (
            CAPITAL_CURRENT_SELECTION_CALENDAR_DAYS
        ),
        "inner_selection_windows_by_anchor": inner_selection_windows,
        "inner_selection_windows_by_anchor_sha256": _sha(inner_selection_windows),
        "current_selection_window": current_selection_window,
        "current_selection_window_sha256": _sha(current_selection_window),
        "optimizer_train_history_proof_by_timeframe": proofs,
        "minimum_additional_history_calendar_days_by_timeframe": (
            additional_history_days
        ),
        "causal_process_policy_sha256": _causal_policy_sha256(),
        "scope": PLAN_SCOPE,
        _AUTHORITY_FLAG: False,
    }
    body_json = _canonical_bytes(body)
    receipt = ProductionAnchorPlanReceipt(
        _body_json=body_json,
        _receipt_sha256=hashlib.sha256(body_json).hexdigest(),
    )
    _verify_receipt_against_authority(
        receipt,
        authority_common=common,
        authority_run_sha256=run_sha,
        authority_activation_id=activation_id,
        authority_activation_sha256=activation_sha,
    )
    return receipt


__all__ = [
    "CAPITAL_CURRENT_SELECTION_CALENDAR_DAYS",
    "CAPITAL_INNER_SELECTION_CALENDAR_DAYS",
    "DEFAULT_ANCHOR_COUNT",
    "DEFAULT_FORWARD_OOS_CALENDAR_DAYS",
    "INTER_ANCHOR_OOS_GAP_CALENDAR_DAYS",
    "PINNED_MINIMUM_PURGE_CALENDAR_DAYS",
    "PLAN_SCHEMA",
    "PLAN_SCOPE",
    "ProductionAnchorPlanError",
    "ProductionAnchorPlanInsufficientHistory",
    "ProductionAnchorPlanReceipt",
    "derive_production_anchor_plan",
    "verify_production_anchor_plan_receipt",
]
