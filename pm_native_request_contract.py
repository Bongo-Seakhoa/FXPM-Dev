"""Shared deterministic authority for FxPM's production V5 native request map.

The acquisition writer, activation auditor, composite reader, and horizon
authority must not each decide what a V5 request means.  This module rebuilds
the complete six-timeframe contract from its small set of declared inputs and
then compares every persisted projection with that independently derived
value.  Re-hashing a forged map therefore never makes it valid.

There is deliberately no file, MT5, optimizer, or execution access here.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache, wraps
from types import MappingProxyType
from typing import Any, Final, Mapping, Sequence

import pandas as pd

from pm_evidence import (
    DEFAULT_TIMEFRAME_WINDOW_YEARS,
    PARTITIONS,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    build_timeframe_calendar_boundaries,
    normalize_timeframe_window_years,
)
from pm_native_consensus import (
    CONSENSUS_POLICY,
    PRODUCTION_TIMEFRAMES,
    REFERENCE_GRAPH,
    REQUIRED_AUDIT_WINDOWS,
)
from pm_native_intervals import TIMEFRAME_SECONDS, native_source_interval


TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION: Final = (
    "fxpm.native_history.timeframe_request_map.v5"
)
PRODUCTION_TIMEFRAME_ORDER: Final[tuple[str, ...]] = (
    "M5",
    "M15",
    "M30",
    "H1",
    "H4",
    "D1",
)
PRODUCTION_TIMEFRAME_SET: Final[frozenset[str]] = frozenset(PRODUCTION_TIMEFRAME_ORDER)
PRODUCTION_PARTITIONS: Final[tuple[str, ...]] = tuple(PARTITIONS)
PRODUCTION_AUDIT_WINDOWS: Final[tuple[str, ...]] = tuple(REQUIRED_AUDIT_WINDOWS)

# This is the production acquisition universe, not a caller-selected subset.
# Casing is part of the broker identity (notably ``Platinum`` and
# ``Palladium``), so it is intentionally not normalized to upper case.
PRODUCTION_SYMBOL_ORDER: Final[tuple[str, ...]] = (
    "AUDUSD",
    "EURUSD",
    "USDCAD",
    "USDJPY",
    "AU200",
    "AUDCAD",
    "AUDCHF",
    "AUDJPY",
    "AUDNZD",
    "BCHUSD",
    "BTCETH",
    "BTCUSD",
    "CADCHF",
    "CADJPY",
    "CHFJPY",
    "DE30",
    "DOGUSD",
    "ES35",
    "ETHUSD",
    "EU50",
    "EURAUD",
    "EURCAD",
    "EURCHF",
    "EURGBP",
    "EURJPY",
    "EURNZD",
    "EURZAR",
    "FR40",
    "GBPAUD",
    "GBPCAD",
    "GBPCHF",
    "GBPJPY",
    "GBPNZD",
    "GBPUSD",
    "GBPZAR",
    "HK50",
    "JP225",
    "LTCUSD",
    "NZDCAD",
    "NZDCHF",
    "NZDJPY",
    "NZDUSD",
    "SOLUSD",
    "TONUSD",
    "TRXUSD",
    "UK100",
    "US100",
    "US30",
    "US500",
    "USDCHF",
    "USDMXN",
    "USDNOK",
    "USDPLN",
    "USDSEK",
    "USDSGD",
    "USDZAR",
    "XAGUSD",
    "XAUUSD",
    "XBRUSD",
    "XNGUSD",
    "XRPUSD",
    "XTIUSD",
    "Platinum",
    "USDTHB",
    "Palladium",
    "XAGEUR",
    "XAUAUD",
    "XAUEUR",
    "XAUGBP",
    "AIG",
    "ALCOA",
    "ALIBABA",
    "AMAZON",
    "APPLE",
    "ATT",
    "BAC",
    "CISCO",
    "CITIGROUP",
    "COCA-COLA",
    "DISNEY",
    "EBAY",
    "FORD",
    "GE",
    "GM",
    "GOOGLE",
    "HP",
    "IBM",
    "INTEL",
    "JPM",
    "MASTERCARD",
    "MCDONALDS",
    "MICROSOFT",
    "NETFLIX",
    "NIKE",
    "NVIDIA",
    "ORACLE",
    "PEPSICO",
    "PFIZER",
    "TESLA",
    "VISA",
)
PRODUCTION_SYMBOL_SET: Final[frozenset[str]] = frozenset(PRODUCTION_SYMBOL_ORDER)
PRODUCTION_SYMBOL_ORDER_SHA256: Final = hashlib.sha256(
    json.dumps(
        list(PRODUCTION_SYMBOL_ORDER),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
).hexdigest()
PRODUCTION_TIMEFRAME_ORDER_SHA256: Final = hashlib.sha256(
    json.dumps(
        list(PRODUCTION_TIMEFRAME_ORDER),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
).hexdigest()

BROKER_SESSION_ALIGNMENT_MODE: Final = "broker_observed_successor_or_nominal_close_v1"
FIXED_ALIGNMENT_MODE: Final = "fixed_utc_native_boundary_v1"

CELL_SUCCESS: Final = "SUCCESS"
CELL_BOUNDARY_SHORTFALL: Final = "BOUNDARY_HISTORY_SHORTFALL"
CELL_TECHNICAL_GAP: Final = "TECHNICAL_HISTORY_GAP"
CELL_TERMINAL_PREFIX_UNRESOLVED: Final = "TERMINAL_PREFIX_UNRESOLVED"

PROVENANCE_SCHEMA_VERSION: Final = "fxpm-native-bars-v1"
ACQUISITION_CONTRACT_VERSION: Final = "2026-07-14-native-calendar-acquisition-v5"
PAGINATION_CONTRACT_VERSION: Final = "fxpm.native_history.pagination.v3"
SOURCE_QUALITY_OMISSION_SCHEMA_VERSION: Final = (
    "fxpm.native_history.source_quality_omission.v1"
)
SOURCE_QUALITY_OMISSION_POLICY_ID: Final = (
    "fbs-fr40-2019-11-25-nonpositive-ohlc-holiday-v1"
)
MAX_SOURCE_QUALITY_OMISSIONS_PER_CELL: Final = 1
SOURCE_QUALITY_OMISSION_OPEN_UTC: Final = "2019-11-25T00:00:00Z"
_SOURCE_QUALITY_OMISSION_ROW: Final[dict[str, float | int]] = {
    "Open": 5917.25,
    "High": 5935.3,
    "Low": 0.0,
    "Close": 5929.2,
    "Volume": 5778,
    "Spread": 0,
    "RealVolume": 0,
}
_SOURCE_QUALITY_OMISSION_ACCOUNTS: Final = frozenset(
    {
        (
            "FBS Markets Inc.",
            "USD",
            "a615b62e2a01f5ac872eb6396e3584b98f9e0d4c78849bed8531e7ad1c24b732",
        ),
        (
            "FBS Markets Inc.",
            "USD",
            "b26594318e54e02993836852cff39b75b1f88aae47e3116ce20e386257e1a2b3",
        ),
    }
)
AVAILABLE_HISTORY_BOUNDARY_SCHEMA_VERSION: Final = (
    "fxpm.native_history.available_history_boundary.v1"
)
AVAILABLE_HISTORY_BOUNDARY_CONFIRMATION_METHOD: Final = (
    "mt5_v4_three_call_single_bar_boundary_match_v1"
)
MANIFEST_SCHEMA_VERSION: Final = "fxpm.native_history_run_manifest.v1"
TRUSTED_POLICY_SCHEMA_VERSION: Final = "fxpm.native_history.trusted_policy.v1"
SOURCE_IDENTITY_SCHEMA_VERSION: Final = "fxpm.native_history.source_identity.v2"
_SOURCE_IDENTITY_DOMAIN: Final = b"fxpm.native-history.source-identity.v2\x00"
_SOURCE_ACCOUNT_DOMAIN: Final = b"fxpm.native-history.source-account.v2\x00"
_SOURCE_CONNECTION_DOMAIN: Final = b"fxpm.native-history.source-connection.v2\x00"
_SOURCE_INSTALLATION_DOMAIN: Final = b"fxpm.native-history.source-installation.v2\x00"
PRODUCTION_ARTIFACT_COLUMNS: Final[tuple[str, ...]] = (
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Spread",
    "RealVolume",
    "_SOURCE_FIRST_TS",
    "_SOURCE_LAST_TS",
    "_BAR_CLOSE_TS",
)

# These caps are validation/resource authorities, not statistical sufficiency
# thresholds.  They only prevent attacker-controlled JSON from causing
# unbounded allocation, traversal, or date arithmetic.
MAX_CANONICAL_DEPTH: Final = 32
MAX_CANONICAL_NODES: Final = 100_000
MAX_CANONICAL_CHARS: Final = 4_000_000
MAX_CANONICAL_INTEGER_BITS: Final = 256
MAX_CONTAINER_ITEMS: Final = 10_000
MAX_CANONICAL_STRING_CHARS: Final = 32_768
MAX_TRUSTED_COMMON_BYTES: Final = 4_000_000
MAX_TRUSTED_SOURCE_BYTES: Final = 1_000_000
MIN_CONTRACT_YEAR: Final = 1970
MAX_CONTRACT_YEAR: Final = 2200
MAX_HORIZON_YEARS: Final = 50
MAX_PARTITION_YEARS: Final = 50
MAX_POLICY_DAYS: Final = 3_650.0
MAX_PRODUCTION_SYMBOLS: Final = len(PRODUCTION_SYMBOL_ORDER)
MAX_PRODUCTION_CELLS: Final = len(PRODUCTION_SYMBOL_ORDER) * len(
    PRODUCTION_TIMEFRAME_ORDER
)
MAX_TERMINAL_BARS: Final = 10_000_000_000
MAX_PAGES_PER_CELL: Final = 10_000
MAX_PAGE_SPAN_SECONDS: Final = 20 * 366 * 86_400
MAX_PAGE_ATTEMPTS: Final = 1_000_000
MAX_ARTIFACT_ROWS: Final = 50_000_000
MAX_ARTIFACT_BYTES: Final = 64 * 1024 * 1024 * 1024
# One production run contains exactly 600 native cells.  This aggregate is a
# shared writer/reader safety authority: acquisition must refuse publication
# above it and reseal must apply the identical ceiling.  It is not a data-
# sufficiency threshold and must never be used to truncate source history.
MAX_TOTAL_ARTIFACT_BYTES: Final = 2 * 1024 * 1024 * 1024 * 1024
TERMINAL_HCC_MAX_FILE_BYTES: Final = 2 * 1024**3
TERMINAL_MIN_FREE_BYTES: Final = 2 * TERMINAL_HCC_MAX_FILE_BYTES
MAX_TERMINAL_HCC_SCAN_ENTRIES: Final = 1_000_000
OUTPUT_MIN_FREE_BYTES: Final = 4 * 1024**3
OUTPUT_MAX_CELL_BYTES: Final = MAX_ARTIFACT_BYTES
_DEFAULT_PAGE_DAYS: Final[dict[str, int]] = {
    "M5": 30,
    "M15": 90,
    "M30": 180,
    "H1": 365,
    "H4": 1_460,
    "D1": 3_650,
}

COMMON_REQUEST_KEYS: Final[tuple[str, ...]] = (
    "window_contract_version",
    "timeframe_request_map_schema_version",
    "horizon_years",
    "requested_information_cutoff_utc",
    "requested_horizon_start_utc",
    "common_requested_start_utc",
    "required_evidence_lead_start_utc",
    "warmup_days",
    "safety_days",
    "closed_bar_slack_seconds",
    "common_as_of_utc",
    "boundary_tolerance_days",
    "embargo_days",
    "timeframe_window_years",
    "timeframe_calendar_boundaries",
    "timeframe_calendar_boundaries_sha256",
    "timeframe_required_evidence_lead_start_utc",
    "timeframe_source_request_intervals",
    "timeframe_request_map_sha256",
    "timeframe_strategy_evidence_intervals",
    "timeframe_audit_reference_map",
    "timeframe_audit_consumer_map",
    "timeframe_audit_windows",
    "timeframe_audit_reference_intervals",
    "timeframe_audit_only_intervals",
    "timeframe_consensus_policy",
)

LEAF_PROJECTION_KEYS: Final[tuple[str, ...]] = (
    "cell_timeframe",
    "cell_timeframe_window_years",
    "cell_calendar_boundaries",
    "cell_required_evidence_lead_start_utc",
    "source_request_interval",
    "strategy_evidence_interval",
    "audit_reference_timeframes",
    "audit_consumer_targets",
    "audit_windows",
    "audit_reference_intervals",
    "audit_only_intervals",
)

_SYMBOL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.+@-]{0,127}$")
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_IDENTITY_KEYS: Final[dict[str, tuple[str, ...]]] = {
    "account": ("login", "server", "company", "currency"),
    "configured_connection": (
        "server",
        "terminal_path",
        "expected_login",
        "expected_server",
        "expected_currency",
    ),
    "terminal": (
        "name",
        "company",
        "path",
        "data_path",
        "commondata_path",
        "build",
        "version",
        "max_bars",
    ),
}
_PROHIBITED_AUTHORITY_CLAIMS: Final[tuple[str, ...]] = (
    "tradeable",
    "production_eligible",
    "funded_live_eligible",
    "statistical_authority",
    "statistical_sufficiency_receipt",
    "promotion_authorization",
    "no_trade",
    "economic_verdict",
    "profitability_authority",
    "profitability_claim",
    "grants_profitability_authority",
    "guaranteed_profitable",
    "funded_live_authorized",
    "live_trading_authorization",
    "winner_authorization",
)
_FAILED_ACQUISITION_STATES: Final[frozenset[str]] = frozenset(
    {
        "write_failed",
        "fetch_failed",
        "terminal_history_empty_unresolved",
        "no_fully_closed_history",
        "data_quality_failed",
        "symbol_resolution_failed",
        "terminal_prefix_unresolved",
        "terminal_cache_resource_limit",
        "output_storage_resource_limit",
        "unexpected_acquisition_failure",
        "connector_preflight_failed",
    }
)
_FAILED_PROVENANCE_BASE_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "schema_version",
        "acquisition_contract_version",
        "immutable_record",
        "run_id",
        "status",
        "failure_status",
        "eligible_for_optimization",
        "eligible_for_downstream_validation",
        "statistical_sufficiency_assessed",
        "canonical_symbol",
        "symbol",
        "broker_symbol",
        "timeframe",
        "source_kind",
        "timezone",
        "source_environment",
        "instrument",
        "acquisition_started_utc",
        "acquisition_completed_utc",
        "error",
        "timestamp_semantics",
        "bar_interval_semantics",
        "bar_close_column",
        "output_storage_resource_snapshot",
        "artifact",
        "sha256",
        "rows",
        "start_ts",
        "end_ts",
        "complete",
    }
)
_FAILED_PROVENANCE_FETCH_FIELDS: Final[frozenset[str]] = frozenset(
    {"timeframe_seconds", "request", "pagination"}
)
_FAILED_PROVENANCE_REQUEST_FIELDS: Final[frozenset[str]] = frozenset(
    {"timeframe_seconds", "request"}
)
_FAILED_PROVENANCE_TERMINAL_RESOURCE_FIELDS: Final[frozenset[str]] = frozenset(
    {"terminal_cache_resource_snapshot"}
)


class NativeRequestContractError(ValueError):
    """A deterministic, machine-readable V5 validation failure."""

    def __init__(self, errors: Sequence[str]):
        ordered = tuple(dict.fromkeys(str(value) for value in errors if str(value)))
        if not ordered:
            ordered = ("V5_CONTRACT_INVALID",)
        self.errors = ordered
        super().__init__(";".join(ordered))


def _authority_claim_errors(
    value: Any,
    *,
    code_prefix: str,
    _prohibited: frozenset[str] = frozenset(_PROHIBITED_AUTHORITY_CLAIMS),
) -> list[str]:
    """Reject decision authority anywhere inside acquisition evidence.

    Acquisition proves source mechanics only.  A nested field must not be able
    to smuggle an economic, promotion, winner, or live-trading verdict through
    a record that later code treats as trusted provenance.
    """

    errors: list[str] = []
    stack: list[tuple[Any, int]] = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        if depth > MAX_CANONICAL_DEPTH:
            return [f"{code_prefix}:DEPTH"]
        if isinstance(current, Mapping):
            for key, item in current.items():
                if isinstance(key, str) and key in _prohibited:
                    errors.append(f"{code_prefix}:{key}")
                stack.append((item, depth + 1))
        elif isinstance(current, (list, tuple)):
            stack.extend((item, depth + 1) for item in current)
    return errors


def _freeze_authority(value: Any, *, depth: int = 0) -> Any:
    """Create a small immutable import-time snapshot of trusted code policy."""

    if depth > 16:
        raise RuntimeError("native request authority policy is unexpectedly deep")
    if isinstance(value, Mapping):
        if len(value) > 1_024:
            raise RuntimeError("native request authority policy is unexpectedly wide")
        return tuple(
            (key, _freeze_authority(item, depth=depth + 1))
            for key, item in sorted(value.items())
        )
    if isinstance(value, (list, tuple)):
        if len(value) > 1_024:
            raise RuntimeError("native request authority policy is unexpectedly wide")
        return tuple(_freeze_authority(item, depth=depth + 1) for item in value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise RuntimeError(
        f"unsupported native request authority value: {type(value).__name__}"
    )


_AUTHORITY_SYMBOL_ORDER = tuple(PRODUCTION_SYMBOL_ORDER)
_AUTHORITY_TIMEFRAME_ORDER = tuple(PRODUCTION_TIMEFRAME_ORDER)
_AUTHORITY_IMPORTED_TIMEFRAMES = tuple(PRODUCTION_TIMEFRAMES)
_AUTHORITY_PARTITIONS = tuple(PRODUCTION_PARTITIONS)
_AUTHORITY_AUDIT_WINDOWS = tuple(PRODUCTION_AUDIT_WINDOWS)
_AUTHORITY_TIMEFRAME_SECONDS = _freeze_authority(TIMEFRAME_SECONDS)
_AUTHORITY_DEFAULT_WINDOWS = _freeze_authority(DEFAULT_TIMEFRAME_WINDOW_YEARS)
_AUTHORITY_REFERENCE_GRAPH = _freeze_authority(REFERENCE_GRAPH)
_AUTHORITY_CONSENSUS_POLICY = _freeze_authority(CONSENSUS_POLICY)
_AUTHORITY_SOURCE_KEYS = _freeze_authority(_SOURCE_IDENTITY_KEYS)
_AUTHORITY_PAGE_DAYS = _freeze_authority(_DEFAULT_PAGE_DAYS)
_AUTHORITY_ARTIFACT_COLUMNS = tuple(PRODUCTION_ARTIFACT_COLUMNS)
_AUTHORITY_SOURCE_DOMAINS = (
    _SOURCE_IDENTITY_DOMAIN,
    _SOURCE_ACCOUNT_DOMAIN,
    _SOURCE_CONNECTION_DOMAIN,
    _SOURCE_INSTALLATION_DOMAIN,
)
_AUTHORITY_COMMON_KEYS = tuple(COMMON_REQUEST_KEYS)
_AUTHORITY_LEAF_KEYS = tuple(LEAF_PROJECTION_KEYS)
_AUTHORITY_PROHIBITED_CLAIMS = tuple(_PROHIBITED_AUTHORITY_CLAIMS)
_AUTHORITY_FAILED_STATES = frozenset(_FAILED_ACQUISITION_STATES)
_AUTHORITY_SOURCE_QUALITY_OMISSION_ROW = _freeze_authority(_SOURCE_QUALITY_OMISSION_ROW)
_AUTHORITY_SOURCE_QUALITY_OMISSION_ACCOUNTS = frozenset(
    _SOURCE_QUALITY_OMISSION_ACCOUNTS
)
_AUTHORITY_REGEX_PATTERNS = (
    _SYMBOL_RE.pattern,
    _RUN_ID_RE.pattern,
    _SHA256_RE.pattern,
)
_AUTHORITY_IMPORTED_FUNCTIONS = (
    build_timeframe_calendar_boundaries,
    normalize_timeframe_window_years,
    native_source_interval,
)
_AUTHORITY_VERSIONS = (
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    PROVENANCE_SCHEMA_VERSION,
    ACQUISITION_CONTRACT_VERSION,
    PAGINATION_CONTRACT_VERSION,
    SOURCE_QUALITY_OMISSION_SCHEMA_VERSION,
    SOURCE_QUALITY_OMISSION_POLICY_ID,
    SOURCE_QUALITY_OMISSION_OPEN_UTC,
    AVAILABLE_HISTORY_BOUNDARY_SCHEMA_VERSION,
    AVAILABLE_HISTORY_BOUNDARY_CONFIRMATION_METHOD,
    MANIFEST_SCHEMA_VERSION,
    TRUSTED_POLICY_SCHEMA_VERSION,
    SOURCE_IDENTITY_SCHEMA_VERSION,
    PRODUCTION_SYMBOL_ORDER_SHA256,
    PRODUCTION_TIMEFRAME_ORDER_SHA256,
)
_AUTHORITY_CAPS = (
    MAX_CANONICAL_DEPTH,
    MAX_CANONICAL_NODES,
    MAX_CANONICAL_CHARS,
    MAX_CANONICAL_INTEGER_BITS,
    MAX_CONTAINER_ITEMS,
    MAX_CANONICAL_STRING_CHARS,
    MAX_PRODUCTION_SYMBOLS,
    MAX_PRODUCTION_CELLS,
    MAX_PAGES_PER_CELL,
    MAX_PAGE_ATTEMPTS,
    MAX_SOURCE_QUALITY_OMISSIONS_PER_CELL,
    MAX_ARTIFACT_ROWS,
    MAX_ARTIFACT_BYTES,
    MAX_TOTAL_ARTIFACT_BYTES,
    MAX_TRUSTED_COMMON_BYTES,
    MAX_TRUSTED_SOURCE_BYTES,
    MIN_CONTRACT_YEAR,
    MAX_CONTRACT_YEAR,
    MAX_HORIZON_YEARS,
    MAX_PARTITION_YEARS,
    MAX_TERMINAL_BARS,
    MAX_PAGE_SPAN_SECONDS,
    TERMINAL_HCC_MAX_FILE_BYTES,
    TERMINAL_MIN_FREE_BYTES,
    MAX_TERMINAL_HCC_SCAN_ENTRIES,
    OUTPUT_MIN_FREE_BYTES,
    OUTPUT_MAX_CELL_BYTES,
)


def _runtime_function_guard_factory():
    expected: tuple[tuple[str, Any], ...] | None = None

    def finalize(functions: Mapping[str, Any]) -> None:
        nonlocal expected
        if expected is not None:
            raise RuntimeError("native request function authority already finalized")
        expected = tuple(sorted(functions.items()))

    def assert_intact(namespace: Mapping[str, Any]) -> None:
        if expected is None:
            raise NativeRequestContractError(("V5_RUNTIME_FUNCTIONS_NOT_FINALIZED",))
        for name, function in expected:
            if namespace.get(name) is not function:
                raise NativeRequestContractError(
                    (f"V5_RUNTIME_FUNCTION_REBOUND:{name}",)
                )

    return finalize, assert_intact


_finalize_runtime_functions, _assert_runtime_functions_intact = (
    _runtime_function_guard_factory()
)


def _assert_runtime_authority_intact(
    *,
    _freeze: Any = _freeze_authority,
    _symbols: tuple[str, ...] = _AUTHORITY_SYMBOL_ORDER,
    _timeframes: tuple[str, ...] = _AUTHORITY_TIMEFRAME_ORDER,
    _imported_timeframes: tuple[str, ...] = _AUTHORITY_IMPORTED_TIMEFRAMES,
    _partitions: tuple[str, ...] = _AUTHORITY_PARTITIONS,
    _audit_windows: tuple[str, ...] = _AUTHORITY_AUDIT_WINDOWS,
    _seconds: Any = _AUTHORITY_TIMEFRAME_SECONDS,
    _windows: Any = _AUTHORITY_DEFAULT_WINDOWS,
    _references: Any = _AUTHORITY_REFERENCE_GRAPH,
    _consensus: Any = _AUTHORITY_CONSENSUS_POLICY,
    _source_keys: Any = _AUTHORITY_SOURCE_KEYS,
    _page_days: Any = _AUTHORITY_PAGE_DAYS,
    _artifact_columns: tuple[str, ...] = _AUTHORITY_ARTIFACT_COLUMNS,
    _source_domains: tuple[bytes, ...] = _AUTHORITY_SOURCE_DOMAINS,
    _common_keys: tuple[str, ...] = _AUTHORITY_COMMON_KEYS,
    _leaf_keys: tuple[str, ...] = _AUTHORITY_LEAF_KEYS,
    _prohibited: tuple[str, ...] = _AUTHORITY_PROHIBITED_CLAIMS,
    _failed_states: frozenset[str] = _AUTHORITY_FAILED_STATES,
    _omission_row: Any = _AUTHORITY_SOURCE_QUALITY_OMISSION_ROW,
    _omission_accounts: frozenset[tuple[str, str, str]] = (
        _AUTHORITY_SOURCE_QUALITY_OMISSION_ACCOUNTS
    ),
    _patterns: tuple[str, ...] = _AUTHORITY_REGEX_PATTERNS,
    _functions: tuple[Any, ...] = _AUTHORITY_IMPORTED_FUNCTIONS,
    _versions: tuple[str, ...] = _AUTHORITY_VERSIONS,
    _caps: tuple[int, ...] = _AUTHORITY_CAPS,
    _function_guard: Any = _assert_runtime_functions_intact,
) -> None:
    """Fail closed if runtime globals no longer match the import-time policy.

    This is tamper detection for accidental/hostile runtime rebinding, not a
    cryptographic signature and not an authorization oracle.
    """

    try:
        _function_guard(globals())
        intact = (
            tuple(PRODUCTION_SYMBOL_ORDER) == _symbols
            and tuple(_AUTHORITY_SYMBOL_ORDER) == _symbols
            and frozenset(PRODUCTION_SYMBOL_SET) == frozenset(_symbols)
            and tuple(PRODUCTION_TIMEFRAME_ORDER) == _timeframes
            and tuple(_AUTHORITY_TIMEFRAME_ORDER) == _timeframes
            and frozenset(PRODUCTION_TIMEFRAME_SET) == frozenset(_timeframes)
            and tuple(PRODUCTION_TIMEFRAMES) == _imported_timeframes
            and tuple(_AUTHORITY_IMPORTED_TIMEFRAMES) == _imported_timeframes
            and tuple(PRODUCTION_PARTITIONS) == _partitions
            and tuple(_AUTHORITY_PARTITIONS) == _partitions
            and tuple(PRODUCTION_AUDIT_WINDOWS) == _audit_windows
            and tuple(_AUTHORITY_AUDIT_WINDOWS) == _audit_windows
            and _freeze(TIMEFRAME_SECONDS) == _seconds
            and _freeze(DEFAULT_TIMEFRAME_WINDOW_YEARS) == _windows
            and _freeze(REFERENCE_GRAPH) == _references
            and _freeze(CONSENSUS_POLICY) == _consensus
            and _freeze(_SOURCE_IDENTITY_KEYS) == _source_keys
            and _freeze(_DEFAULT_PAGE_DAYS) == _page_days
            and tuple(PRODUCTION_ARTIFACT_COLUMNS) == _artifact_columns
            and (
                _SOURCE_IDENTITY_DOMAIN,
                _SOURCE_ACCOUNT_DOMAIN,
                _SOURCE_CONNECTION_DOMAIN,
                _SOURCE_INSTALLATION_DOMAIN,
            )
            == _source_domains
            and tuple(COMMON_REQUEST_KEYS) == _common_keys
            and tuple(LEAF_PROJECTION_KEYS) == _leaf_keys
            and tuple(_PROHIBITED_AUTHORITY_CLAIMS) == _prohibited
            and frozenset(_FAILED_ACQUISITION_STATES) == _failed_states
            and _freeze(_SOURCE_QUALITY_OMISSION_ROW) == _omission_row
            and frozenset(_SOURCE_QUALITY_OMISSION_ACCOUNTS) == _omission_accounts
            and (_SYMBOL_RE.pattern, _RUN_ID_RE.pattern, _SHA256_RE.pattern)
            == _patterns
            and (
                build_timeframe_calendar_boundaries,
                normalize_timeframe_window_years,
                native_source_interval,
            )
            == _functions
            and _AUTHORITY_TIMEFRAME_SECONDS == _seconds
            and _AUTHORITY_DEFAULT_WINDOWS == _windows
            and _AUTHORITY_REFERENCE_GRAPH == _references
            and _AUTHORITY_CONSENSUS_POLICY == _consensus
            and _AUTHORITY_SOURCE_KEYS == _source_keys
            and _AUTHORITY_PAGE_DAYS == _page_days
            and _AUTHORITY_ARTIFACT_COLUMNS == _artifact_columns
            and _AUTHORITY_SOURCE_DOMAINS == _source_domains
            and _AUTHORITY_COMMON_KEYS == _common_keys
            and _AUTHORITY_LEAF_KEYS == _leaf_keys
            and _AUTHORITY_PROHIBITED_CLAIMS == _prohibited
            and _AUTHORITY_FAILED_STATES == _failed_states
            and _AUTHORITY_SOURCE_QUALITY_OMISSION_ROW == _omission_row
            and _AUTHORITY_SOURCE_QUALITY_OMISSION_ACCOUNTS == _omission_accounts
            and _AUTHORITY_REGEX_PATTERNS == _patterns
            and _AUTHORITY_IMPORTED_FUNCTIONS == _functions
            and _AUTHORITY_VERSIONS == _versions
            and _AUTHORITY_CAPS == _caps
            and (
                TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
                TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
                PROVENANCE_SCHEMA_VERSION,
                ACQUISITION_CONTRACT_VERSION,
                PAGINATION_CONTRACT_VERSION,
                SOURCE_QUALITY_OMISSION_SCHEMA_VERSION,
                SOURCE_QUALITY_OMISSION_POLICY_ID,
                SOURCE_QUALITY_OMISSION_OPEN_UTC,
                AVAILABLE_HISTORY_BOUNDARY_SCHEMA_VERSION,
                AVAILABLE_HISTORY_BOUNDARY_CONFIRMATION_METHOD,
                MANIFEST_SCHEMA_VERSION,
                TRUSTED_POLICY_SCHEMA_VERSION,
                SOURCE_IDENTITY_SCHEMA_VERSION,
                PRODUCTION_SYMBOL_ORDER_SHA256,
                PRODUCTION_TIMEFRAME_ORDER_SHA256,
            )
            == _versions
            and (
                MAX_CANONICAL_DEPTH,
                MAX_CANONICAL_NODES,
                MAX_CANONICAL_CHARS,
                MAX_CANONICAL_INTEGER_BITS,
                MAX_CONTAINER_ITEMS,
                MAX_CANONICAL_STRING_CHARS,
                MAX_PRODUCTION_SYMBOLS,
                MAX_PRODUCTION_CELLS,
                MAX_PAGES_PER_CELL,
                MAX_PAGE_ATTEMPTS,
                MAX_SOURCE_QUALITY_OMISSIONS_PER_CELL,
                MAX_ARTIFACT_ROWS,
                MAX_ARTIFACT_BYTES,
                MAX_TOTAL_ARTIFACT_BYTES,
                MAX_TRUSTED_COMMON_BYTES,
                MAX_TRUSTED_SOURCE_BYTES,
                MIN_CONTRACT_YEAR,
                MAX_CONTRACT_YEAR,
                MAX_HORIZON_YEARS,
                MAX_PARTITION_YEARS,
                MAX_TERMINAL_BARS,
                MAX_PAGE_SPAN_SECONDS,
                TERMINAL_HCC_MAX_FILE_BYTES,
                TERMINAL_MIN_FREE_BYTES,
                MAX_TERMINAL_HCC_SCAN_ENTRIES,
                OUTPUT_MIN_FREE_BYTES,
                OUTPUT_MAX_CELL_BYTES,
            )
            == _caps
        )
    except Exception as exc:
        raise NativeRequestContractError(
            (f"V5_RUNTIME_AUTHORITY_REBOUND:{type(exc).__name__}",)
        ) from None
    if not intact:
        raise NativeRequestContractError(("V5_RUNTIME_AUTHORITY_REBOUND",))


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and _SHA256_RE.fullmatch(value) is not None
    )


def _total_function(
    code: str,
    _authority_guard: Any = _assert_runtime_authority_intact,
):
    """Make a public validation/builder API total over JSON-shaped input."""

    def decorate(function):
        @wraps(function)
        def guarded(*args: Any, **kwargs: Any):
            try:
                _authority_guard()
                return function(*args, **kwargs)
            except NativeRequestContractError:
                raise
            except Exception as exc:
                raise NativeRequestContractError(
                    (f"{code}:{type(exc).__name__}",)
                ) from None

        return guarded

    return decorate


@dataclass
class _CanonicalBudget:
    nodes: int = 0
    chars: int = 0

    def consume(self, amount: int = 1) -> None:
        self.nodes += int(amount)
        if self.nodes > MAX_CANONICAL_NODES:
            raise NativeRequestContractError(("CANONICAL_JSON_NODE_CAP_EXCEEDED",))

    def consume_chars(self, amount: int) -> None:
        self.chars += int(amount)
        if self.chars > MAX_CANONICAL_CHARS:
            raise NativeRequestContractError(("CANONICAL_JSON_CHAR_CAP_EXCEEDED",))


def _canonical_value(
    value: Any,
    *,
    depth: int = 0,
    budget: _CanonicalBudget | None = None,
) -> Any:
    if depth > MAX_CANONICAL_DEPTH:
        raise NativeRequestContractError(("CANONICAL_JSON_DEPTH_EXCEEDED",))
    if budget is None:
        budget = _CanonicalBudget()
    budget.consume()
    if value is None or isinstance(value, (str, bool, int)):
        if isinstance(value, str) and len(value) > MAX_CANONICAL_STRING_CHARS:
            raise NativeRequestContractError(("CANONICAL_JSON_STRING_CAP_EXCEEDED",))
        if isinstance(value, str):
            budget.consume_chars(len(value))
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and value.bit_length() > MAX_CANONICAL_INTEGER_BITS
        ):
            raise NativeRequestContractError(("CANONICAL_JSON_INTEGER_CAP_EXCEEDED",))
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise NativeRequestContractError(("CANONICAL_JSON_NONFINITE",))
        return value
    if isinstance(value, Mapping):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise NativeRequestContractError(("CANONICAL_JSON_MAP_CAP_EXCEEDED",))
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise NativeRequestContractError(("CANONICAL_JSON_KEY_INVALID",))
            if len(key) > MAX_CANONICAL_STRING_CHARS:
                raise NativeRequestContractError(("CANONICAL_JSON_KEY_CAP_EXCEEDED",))
            budget.consume_chars(len(key))
            result[key] = _canonical_value(
                item,
                depth=depth + 1,
                budget=budget,
            )
        return result
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise NativeRequestContractError(("CANONICAL_JSON_LIST_CAP_EXCEEDED",))
        return [
            _canonical_value(item, depth=depth + 1, budget=budget) for item in value
        ]
    raise NativeRequestContractError(
        (f"CANONICAL_JSON_TYPE_INVALID:{type(value).__name__}",)
    )


@_total_function("V5_CANONICAL_JSON_ERROR")
def canonical_json_bytes(value: Any) -> bytes:
    """Return the exact JSON encoding used by V5 acquisition artifacts."""

    return json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


@_total_function("V5_CANONICAL_HASH_ERROR")
def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _same(left: Any, right: Any) -> bool:
    try:
        return canonical_json_bytes(left) == canonical_json_bytes(right)
    except NativeRequestContractError:
        return False


def _aware_utc(value: Any, error_code: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        if not isinstance(value, str) or len(value) > 128:
            raise NativeRequestContractError((error_code,))
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            result = datetime.fromisoformat(text)
        except (TypeError, ValueError) as exc:
            raise NativeRequestContractError((error_code,)) from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise NativeRequestContractError((error_code,))
    try:
        normalized = result.astimezone(timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise NativeRequestContractError((error_code,)) from exc
    if not MIN_CONTRACT_YEAR <= normalized.year <= MAX_CONTRACT_YEAR:
        raise NativeRequestContractError((error_code,))
    return normalized


def _iso(value: datetime) -> str:
    return _aware_utc(value, "TIMESTAMP_INVALID").isoformat().replace("+00:00", "Z")


def _source_quality_omission_row(value: Any) -> dict[str, float | int]:
    if not isinstance(value, Mapping) or set(value) != set(
        _SOURCE_QUALITY_OMISSION_ROW
    ):
        raise NativeRequestContractError(("V5_SOURCE_QUALITY_OMISSION_ROW_INVALID",))
    normalized: dict[str, float | int] = {}
    for field in ("Open", "High", "Low", "Close"):
        raw = value.get(field)
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise NativeRequestContractError(
                ("V5_SOURCE_QUALITY_OMISSION_ROW_INVALID",)
            )
        number = float(raw)
        if not math.isfinite(number):
            raise NativeRequestContractError(
                ("V5_SOURCE_QUALITY_OMISSION_ROW_INVALID",)
            )
        normalized[field] = number
    for field in ("Volume", "Spread", "RealVolume"):
        raw = value.get(field)
        if type(raw) is not int or raw < 0:
            raise NativeRequestContractError(
                ("V5_SOURCE_QUALITY_OMISSION_ROW_INVALID",)
            )
        normalized[field] = raw
    if normalized != _SOURCE_QUALITY_OMISSION_ROW:
        raise NativeRequestContractError(("V5_SOURCE_QUALITY_OMISSION_ROW_MISMATCH",))
    return normalized


@_total_function("V5_SOURCE_QUALITY_OMISSION_BUILD_ERROR")
def build_v5_source_quality_omission(
    *,
    source_environment: Any,
    symbol: Any,
    broker_symbol: Any,
    timeframe: Any,
    bar_open_utc: Any,
    observed_row: Any,
) -> dict[str, Any]:
    """Build the sole approved broker-row omission receipt.

    The receipt never supplies a replacement price. It records the exact FBS
    FR40 row observed on both authorised accounts and represents that timestamp
    as a market-data gap (equivalent to a holiday).
    """

    source = canonical_source_identity(source_environment)
    account = source.get("account")
    if not isinstance(account, Mapping):
        raise NativeRequestContractError(("V5_SOURCE_QUALITY_OMISSION_SOURCE_INVALID",))
    identity = (
        account.get("company"),
        account.get("currency"),
        account.get("account_identity_sha256"),
    )
    if identity not in _SOURCE_QUALITY_OMISSION_ACCOUNTS:
        raise NativeRequestContractError(("V5_SOURCE_QUALITY_OMISSION_SOURCE_INVALID",))
    canonical = canonical_symbol(symbol)
    if canonical != "FR40" or broker_symbol != "FR40":
        raise NativeRequestContractError(("V5_SOURCE_QUALITY_OMISSION_SYMBOL_INVALID",))
    tf = _canonical_timeframe(timeframe)
    opened = _aware_utc(bar_open_utc, "V5_SOURCE_QUALITY_OMISSION_TIME_INVALID")
    if _iso(opened) != SOURCE_QUALITY_OMISSION_OPEN_UTC:
        raise NativeRequestContractError(("V5_SOURCE_QUALITY_OMISSION_TIME_INVALID",))
    row = _source_quality_omission_row(observed_row)
    row_binding = {
        "bar_open_utc": SOURCE_QUALITY_OMISSION_OPEN_UTC,
        "observed_row": row,
    }
    body = {
        "schema_version": SOURCE_QUALITY_OMISSION_SCHEMA_VERSION,
        "policy_id": SOURCE_QUALITY_OMISSION_POLICY_ID,
        "symbol": canonical,
        "broker_symbol": "FR40",
        "timeframe": tf,
        "bar_open_utc": SOURCE_QUALITY_OMISSION_OPEN_UTC,
        "reason": "NONPOSITIVE_OHLC_SOURCE_DEFECT_OMITTED_AS_GAP",
        "observed_row": row,
        "source_identity_sha256": canonical_sha256(source),
        "row_sha256": canonical_sha256(row_binding),
        "replacement_supplied": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


@_total_function("V5_SOURCE_QUALITY_OMISSION_VALIDATION_ERROR")
def validate_v5_source_quality_omission(
    value: Any,
    *,
    source_environment: Any,
    symbol: Any,
    broker_symbol: Any,
    timeframe: Any,
) -> dict[str, Any]:
    """Rebuild one omission receipt from trusted cell/source context."""

    if not isinstance(value, Mapping):
        raise NativeRequestContractError(("V5_SOURCE_QUALITY_OMISSION_INVALID",))
    canonical_json_bytes(value)
    expected = build_v5_source_quality_omission(
        source_environment=source_environment,
        symbol=symbol,
        broker_symbol=broker_symbol,
        timeframe=timeframe,
        bar_open_utc=value.get("bar_open_utc"),
        observed_row=value.get("observed_row"),
    )
    if not _same(value, expected):
        raise NativeRequestContractError(("V5_SOURCE_QUALITY_OMISSION_INVALID",))
    return deepcopy(expected)


def _floor_boundary(value: datetime, seconds: int) -> datetime:
    current = _aware_utc(value, "REQUESTED_INFORMATION_CUTOFF_INVALID")
    try:
        epoch = math.floor(current.timestamp())
        return datetime.fromtimestamp(epoch - (epoch % seconds), tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise NativeRequestContractError(
            ("REQUESTED_INFORMATION_CUTOFF_INVALID",)
        ) from exc


def _subtract_calendar_years(value: datetime, years: int) -> datetime:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _strict_positive_int(
    value: Any,
    code: str,
    *,
    maximum: int | None = None,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
        or (maximum is not None and value > maximum)
    ):
        raise NativeRequestContractError((code,))
    return value


def _strict_nonnegative_int(
    value: Any,
    code: str,
    *,
    maximum: int | None = None,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or (maximum is not None and value > maximum)
    ):
        raise NativeRequestContractError((code,))
    return value


def _strict_nonnegative_number(
    value: Any,
    code: str,
    *,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise NativeRequestContractError((code,))
    if isinstance(value, int) and value.bit_length() > MAX_CANONICAL_INTEGER_BITS:
        raise NativeRequestContractError((code,))
    result = float(value)
    if (
        not math.isfinite(result)
        or result < 0.0
        or (maximum is not None and result > maximum)
    ):
        raise NativeRequestContractError((code,))
    return result


def _strict_year_matrix(value: Any) -> dict[str, dict[str, int]]:
    if not isinstance(value, Mapping) or set(value) != PRODUCTION_TIMEFRAME_SET:
        raise NativeRequestContractError(("V5_TIMEFRAME_YEAR_ROSTER_INVALID",))
    parsed: dict[str, dict[str, int]] = {}
    for timeframe in PRODUCTION_TIMEFRAME_ORDER:
        cell = value.get(timeframe)
        if not isinstance(cell, Mapping) or set(cell) != set(PRODUCTION_PARTITIONS):
            raise NativeRequestContractError(
                (f"V5_TIMEFRAME_YEAR_PARTITIONS_INVALID:{timeframe}",)
            )
        parsed[timeframe] = {
            partition: _strict_positive_int(
                cell[partition],
                f"V5_TIMEFRAME_YEAR_INVALID:{timeframe}:{partition}",
                maximum=MAX_PARTITION_YEARS,
            )
            for partition in PRODUCTION_PARTITIONS
        }
    normalized = normalize_timeframe_window_years(parsed)
    if not _same(parsed, normalized):
        raise NativeRequestContractError(("V5_TIMEFRAME_YEAR_POLICY_MISMATCH",))
    return parsed


@_total_function("V5_SYMBOL_VALIDATION_ERROR")
def canonical_symbol(value: Any) -> str:
    """Return one safe broker symbol, preserving its case-sensitive identity."""

    if not isinstance(value, str) or len(value) > 256:
        raise NativeRequestContractError(("V5_SYMBOL_INVALID",))
    symbol = value.strip()
    if not _SYMBOL_RE.fullmatch(symbol) or symbol in {".", ".."}:
        raise NativeRequestContractError(("V5_SYMBOL_INVALID",))
    return symbol


@_total_function("V5_SYMBOL_ROSTER_VALIDATION_ERROR")
def canonical_symbols(values: Any) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)) or not values:
        raise NativeRequestContractError(("V5_SYMBOL_ROSTER_INVALID",))
    if len(values) > MAX_PRODUCTION_SYMBOLS:
        raise NativeRequestContractError(("V5_SYMBOL_ROSTER_CAP_EXCEEDED",))
    result = tuple(canonical_symbol(value) for value in values)
    if len(result) != len(set(result)):
        raise NativeRequestContractError(("V5_SYMBOL_ROSTER_DUPLICATE",))
    if any(raw != normalized for raw, normalized in zip(values, result)):
        raise NativeRequestContractError(("V5_SYMBOL_ROSTER_NONCANONICAL",))
    return result


def _strict_production_symbol_order(values: Any) -> tuple[str, ...]:
    result = canonical_symbols(values)
    if result != _AUTHORITY_SYMBOL_ORDER:
        raise NativeRequestContractError(("V5_PRODUCTION_SYMBOL_ROSTER_MISMATCH",))
    return result


def _source_identity_digest(domain: bytes, payload: Any) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(payload)).hexdigest()


def _canonical_redacted_source_identity(value: Mapping[str, Any]) -> dict[str, Any]:
    expected_top = {
        "schema_version",
        "metadata_status",
        "identity_sha256",
        "account",
        "configured_connection",
        "terminal",
    }
    if set(value) != expected_top:
        raise NativeRequestContractError(("V5_REDACTED_SOURCE_SCHEMA_INVALID",))
    if (
        value.get("schema_version") != SOURCE_IDENTITY_SCHEMA_VERSION
        or value.get("metadata_status") != "available"
        or not _is_sha256(value.get("identity_sha256"))
    ):
        raise NativeRequestContractError(("V5_REDACTED_SOURCE_IDENTITY_INVALID",))
    account = value.get("account")
    configured = value.get("configured_connection")
    terminal = value.get("terminal")
    if not isinstance(account, Mapping) or set(account) != {
        "company",
        "currency",
        "account_identity_sha256",
    }:
        raise NativeRequestContractError(("V5_REDACTED_SOURCE_ACCOUNT_INVALID",))
    if not isinstance(configured, Mapping) or set(configured) != {
        "connection_identity_sha256",
        "server_configured",
        "terminal_path_configured",
    }:
        raise NativeRequestContractError(("V5_REDACTED_SOURCE_CONNECTION_INVALID",))
    if not isinstance(terminal, Mapping) or set(terminal) != {
        "name",
        "company",
        "build",
        "version",
        "max_bars",
        "installation_identity_sha256",
    }:
        raise NativeRequestContractError(("V5_REDACTED_SOURCE_TERMINAL_INVALID",))
    if (
        not isinstance(account.get("company"), str)
        or not account["company"].strip()
        or len(account["company"]) > 256
        or not isinstance(account.get("currency"), str)
        or not account["currency"].strip()
        or len(account["currency"]) > 16
        or not _is_sha256(account.get("account_identity_sha256"))
    ):
        raise NativeRequestContractError(("V5_REDACTED_SOURCE_ACCOUNT_INVALID",))
    if (
        not _is_sha256(configured.get("connection_identity_sha256"))
        or type(configured.get("server_configured")) is not bool
        or type(configured.get("terminal_path_configured")) is not bool
    ):
        raise NativeRequestContractError(("V5_REDACTED_SOURCE_CONNECTION_INVALID",))
    for field in ("name", "company"):
        if (
            not isinstance(terminal.get(field), str)
            or not terminal[field].strip()
            or len(terminal[field]) > 256
        ):
            raise NativeRequestContractError(
                (f"V5_REDACTED_SOURCE_TERMINAL_INVALID:{field}",)
            )
    for field in ("build", "max_bars"):
        if (
            isinstance(terminal.get(field), bool)
            or not isinstance(terminal.get(field), int)
            or terminal[field] <= 0
            or terminal[field] > MAX_TERMINAL_BARS
        ):
            raise NativeRequestContractError(
                (f"V5_REDACTED_SOURCE_TERMINAL_INVALID:{field}",)
            )
    version = terminal.get("version")
    if (
        not isinstance(version, (list, tuple))
        or len(version) != 3
        or isinstance(version[0], bool)
        or isinstance(version[1], bool)
        or not isinstance(version[0], int)
        or not isinstance(version[1], int)
        or version[0] <= 0
        or version[1] != terminal["build"]
        or not isinstance(version[2], str)
        or not version[2].strip()
        or len(version[2]) > 128
        or not _is_sha256(terminal.get("installation_identity_sha256"))
    ):
        raise NativeRequestContractError(("V5_REDACTED_SOURCE_TERMINAL_INVALID",))
    return {
        "schema_version": SOURCE_IDENTITY_SCHEMA_VERSION,
        "metadata_status": "available",
        "identity_sha256": value["identity_sha256"],
        "account": {
            "company": account["company"],
            "currency": account["currency"],
            "account_identity_sha256": account["account_identity_sha256"],
        },
        "configured_connection": {
            "connection_identity_sha256": configured["connection_identity_sha256"],
            "server_configured": configured["server_configured"],
            "terminal_path_configured": configured["terminal_path_configured"],
        },
        "terminal": {
            "name": terminal["name"],
            "company": terminal["company"],
            "build": terminal["build"],
            "version": list(version),
            "max_bars": terminal["max_bars"],
            "installation_identity_sha256": terminal["installation_identity_sha256"],
        },
    }


@_total_function("V5_SOURCE_IDENTITY_VALIDATION_ERROR")
def canonical_source_identity(source_environment: Any) -> dict[str, Any]:
    """Project runtime metadata to a privacy-preserving immutable identity.

    Account login, server, installation paths, and configured connection
    values are domain-separated digests. Volatile flags and any unrelated
    fields (including credentials) are excluded. A previously redacted V2
    projection is also accepted so public provenance need not retain plaintext
    account metadata.
    """

    if not isinstance(source_environment, Mapping):
        raise NativeRequestContractError(("V5_SOURCE_ENVIRONMENT_INVALID",))
    if source_environment.get("schema_version") == SOURCE_IDENTITY_SCHEMA_VERSION:
        return _canonical_redacted_source_identity(source_environment)
    if source_environment.get("metadata_status") != "available":
        raise NativeRequestContractError(("V5_SOURCE_METADATA_UNAVAILABLE",))
    identity: dict[str, Any] = {"metadata_status": "available"}
    for section, keys in _SOURCE_IDENTITY_KEYS.items():
        raw = source_environment.get(section)
        if not isinstance(raw, Mapping):
            raise NativeRequestContractError(
                (f"V5_SOURCE_IDENTITY_SECTION_INVALID:{section}",)
            )
        missing = tuple(key for key in keys if key not in raw)
        if missing:
            raise NativeRequestContractError(
                tuple(
                    f"V5_SOURCE_IDENTITY_FIELD_MISSING:{section}:{key}"
                    for key in missing
                )
            )
        identity[section] = {key: _canonical_value(raw[key]) for key in keys}
    account = identity.get("account")
    configured = identity.get("configured_connection")
    terminal = identity.get("terminal")
    if (
        not isinstance(account, Mapping)
        or not isinstance(configured, Mapping)
        or not isinstance(terminal, Mapping)
    ):
        raise NativeRequestContractError(("V5_SOURCE_IDENTITY_INCOMPLETE",))
    for field in ("login", "server", "company", "currency"):
        value = account.get(field)
        if field not in account or value is None or value == "":
            raise NativeRequestContractError(
                (f"V5_SOURCE_ACCOUNT_FIELD_MISSING:{field}",)
            )
    login = account["login"]
    if isinstance(login, bool) or not isinstance(login, (int, str)):
        raise NativeRequestContractError(("V5_SOURCE_ACCOUNT_LOGIN_INVALID",))
    for field in ("server", "company", "currency"):
        if not isinstance(account[field], str) or not account[field].strip():
            raise NativeRequestContractError(
                (f"V5_SOURCE_ACCOUNT_FIELD_INVALID:{field}",)
            )
    if len(account["server"]) > 256 or len(account["company"]) > 256:
        raise NativeRequestContractError(("V5_SOURCE_ACCOUNT_FIELD_INVALID",))
    if len(account["currency"]) > 16:
        raise NativeRequestContractError(("V5_SOURCE_ACCOUNT_FIELD_INVALID:currency",))

    for field, account_field in (
        ("expected_login", "login"),
        ("expected_server", "server"),
        ("expected_currency", "currency"),
    ):
        if configured[field] != account[account_field]:
            raise NativeRequestContractError(
                (f"V5_SOURCE_CONFIGURED_IDENTITY_MISMATCH:{field}",)
            )
    configured_server = configured["server"]
    if configured_server is not None and configured_server != account["server"]:
        raise NativeRequestContractError(
            ("V5_SOURCE_CONFIGURED_IDENTITY_MISMATCH:server",)
        )
    terminal_path = configured["terminal_path"]
    if terminal_path is not None and (
        not isinstance(terminal_path, str)
        or not terminal_path.strip()
        or len(terminal_path) > 4_096
    ):
        raise NativeRequestContractError(
            ("V5_SOURCE_CONFIGURED_TERMINAL_PATH_INVALID",)
        )

    for field in ("name", "company", "path", "data_path", "commondata_path"):
        value = terminal[field]
        if not isinstance(value, str) or not value.strip() or len(value) > 4_096:
            raise NativeRequestContractError(
                (f"V5_SOURCE_TERMINAL_FIELD_INVALID:{field}",)
            )
    for field in ("build", "max_bars"):
        if (
            isinstance(terminal[field], bool)
            or not isinstance(terminal[field], int)
            or terminal[field] <= 0
            or terminal[field] > MAX_TERMINAL_BARS
        ):
            raise NativeRequestContractError(
                (f"V5_SOURCE_TERMINAL_FIELD_INVALID:{field}",)
            )
    version = terminal["version"]
    if (
        not isinstance(version, (list, tuple))
        or len(version) != 3
        or any(isinstance(item, (list, tuple, Mapping)) for item in version)
        or not all(isinstance(item, (str, int)) for item in version)
        or isinstance(version[0], bool)
        or isinstance(version[1], bool)
        or not isinstance(version[0], int)
        or not isinstance(version[1], int)
        or version[0] <= 0
        or version[1] != terminal["build"]
        or not isinstance(version[2], str)
        or not version[2].strip()
        or len(version[2]) > 128
    ):
        raise NativeRequestContractError(("V5_SOURCE_TERMINAL_VERSION_INVALID",))
    raw_identity = {
        "metadata_status": "available",
        "account": dict(account),
        "configured_connection": dict(configured),
        "terminal": dict(terminal),
    }
    return {
        "schema_version": SOURCE_IDENTITY_SCHEMA_VERSION,
        "metadata_status": "available",
        "identity_sha256": _source_identity_digest(
            _SOURCE_IDENTITY_DOMAIN, raw_identity
        ),
        "account": {
            "company": account["company"],
            "currency": account["currency"],
            "account_identity_sha256": _source_identity_digest(
                _SOURCE_ACCOUNT_DOMAIN, account
            ),
        },
        "configured_connection": {
            "connection_identity_sha256": _source_identity_digest(
                _SOURCE_CONNECTION_DOMAIN, configured
            ),
            "server_configured": configured["server"] is not None,
            "terminal_path_configured": configured["terminal_path"] is not None,
        },
        "terminal": {
            "name": terminal["name"],
            "company": terminal["company"],
            "build": terminal["build"],
            "version": list(version),
            "max_bars": terminal["max_bars"],
            "installation_identity_sha256": _source_identity_digest(
                _SOURCE_INSTALLATION_DOMAIN,
                {
                    "path": terminal["path"],
                    "data_path": terminal["data_path"],
                    "commondata_path": terminal["commondata_path"],
                    "configured_terminal_path": configured["terminal_path"],
                },
            ),
        },
    }


@_total_function("V5_SOURCE_IDENTITY_MATCH_ERROR")
def validate_source_identity_match(
    observed_environment: Any,
    expected_environment: Any,
) -> dict[str, Any]:
    observed = canonical_source_identity(observed_environment)
    expected = canonical_source_identity(expected_environment)
    if not _same(observed, expected):
        raise NativeRequestContractError(("V5_SOURCE_IDENTITY_MISMATCH",))
    return observed


def _validate_source_identity_against_canonical_bytes(
    observed_environment: Any,
    expected_source_identity_json: bytes,
) -> dict[str, Any]:
    """Validate one observed source against context bytes built once per run."""

    observed = canonical_source_identity(observed_environment)
    if canonical_json_bytes(observed) != expected_source_identity_json:
        raise NativeRequestContractError(("V5_SOURCE_IDENTITY_MISMATCH",))
    return observed


@dataclass(frozen=True)
class TrustedV5Policy:
    """Immutable out-of-band expectations for one acquisition run.

    This object must be created from operator/configuration inputs before an
    artifact is read.  Artifact fields are never allowed to construct their
    own production authority. ``policy_sha256`` is only a corruption digest;
    it is deliberately neither a signature nor a locally mintable trust
    credential. Trust in the supplied source/cutoff remains out of band.
    """

    common_request_json: bytes
    symbols: tuple[str, ...]
    timeframes: tuple[str, ...]
    retries: int
    source_identity_json: bytes
    policy_sha256: str


_V5_CELL_CONTEXT_TOKEN: Final = object()


@dataclass(frozen=True, init=False)
class V5CellValidationContext:
    """Opaque immutable run facts validated once for a bounded cell batch."""

    common_request_json: bytes
    source_identity_json: bytes
    retries: int
    _common_request_object_id: int
    _source_environment_object_id: int
    _token: object


def _freeze_v5_cell_context_value(value: Any) -> Any:
    """Deep-freeze already bounded canonical JSON for safe batch reuse."""

    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _freeze_v5_cell_context_value(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_v5_cell_context_value(item) for item in value)
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise NativeRequestContractError(("V5_CELL_VALIDATION_CONTEXT_INVALID",))


def _thaw_v5_cell_context_value(value: Any) -> Any:
    """Return the public JSON-shaped copy of a frozen context projection."""

    if isinstance(value, Mapping):
        return {key: _thaw_v5_cell_context_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_v5_cell_context_value(item) for item in value]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise NativeRequestContractError(("V5_CELL_VALIDATION_CONTEXT_INVALID",))


@lru_cache(maxsize=8)
def _decode_v5_cell_context(
    common_request_json: bytes,
    source_identity_json: bytes,
    retries: int,
) -> tuple[Mapping[str, Any], Mapping[str, Any], int]:
    """Decode each immutable context byte pair once in a strictly bounded cache."""

    common = json.loads(common_request_json.decode("utf-8"))
    source = json.loads(source_identity_json.decode("utf-8"))
    if not isinstance(common, dict) or not isinstance(source, dict):
        raise NativeRequestContractError(("V5_CELL_VALIDATION_CONTEXT_INVALID",))
    frozen_common = _freeze_v5_cell_context_value(common)
    frozen_source = _freeze_v5_cell_context_value(source)
    if not isinstance(frozen_common, Mapping) or not isinstance(frozen_source, Mapping):
        raise NativeRequestContractError(("V5_CELL_VALIDATION_CONTEXT_INVALID",))
    return frozen_common, frozen_source, retries


def _strict_timeframe_order(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or len(value) != len(
        PRODUCTION_TIMEFRAME_ORDER
    ):
        raise NativeRequestContractError(("V5_MANIFEST_TIMEFRAME_ROSTER_INVALID",))
    if any(not isinstance(item, str) or len(item) > 8 for item in value):
        raise NativeRequestContractError(("V5_MANIFEST_TIMEFRAME_ROSTER_INVALID",))
    normalized = tuple(item.strip().upper() for item in value)
    if tuple(value) != _AUTHORITY_TIMEFRAME_ORDER or normalized != (
        _AUTHORITY_TIMEFRAME_ORDER
    ):
        raise NativeRequestContractError(("V5_MANIFEST_TIMEFRAME_ROSTER_INVALID",))
    return normalized


def _canonical_timeframe(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 8:
        raise NativeRequestContractError(("V5_LEAF_TIMEFRAME_INVALID",))
    timeframe = value.strip().upper()
    if timeframe not in PRODUCTION_TIMEFRAME_SET:
        raise NativeRequestContractError(("V5_LEAF_TIMEFRAME_INVALID",))
    return timeframe


def _policy_payload(policy: TrustedV5Policy) -> dict[str, Any]:
    return {
        "schema_version": TRUSTED_POLICY_SCHEMA_VERSION,
        "common_request_sha256": hashlib.sha256(policy.common_request_json).hexdigest(),
        "symbols": list(policy.symbols),
        "timeframes": list(policy.timeframes),
        "retries": policy.retries,
        "source_identity_sha256": hashlib.sha256(
            policy.source_identity_json
        ).hexdigest(),
    }


def _validate_trusted_policy(policy: Any) -> TrustedV5Policy:
    if type(policy) is not TrustedV5Policy:
        raise NativeRequestContractError(("V5_TRUSTED_POLICY_REQUIRED",))
    if (
        not isinstance(policy.common_request_json, bytes)
        or not 0 < len(policy.common_request_json) <= MAX_TRUSTED_COMMON_BYTES
        or not isinstance(policy.source_identity_json, bytes)
        or not 0 < len(policy.source_identity_json) <= MAX_TRUSTED_SOURCE_BYTES
    ):
        raise NativeRequestContractError(("V5_TRUSTED_POLICY_PAYLOAD_INVALID",))
    if not _is_sha256(policy.policy_sha256):
        raise NativeRequestContractError(("V5_TRUSTED_POLICY_DIGEST_INVALID",))
    if type(policy.symbols) is not tuple or type(policy.timeframes) is not tuple:
        raise NativeRequestContractError(("V5_TRUSTED_POLICY_ROSTER_INVALID",))
    _strict_production_symbol_order(policy.symbols)
    _strict_timeframe_order(policy.timeframes)
    if type(policy.retries) is not int or policy.retries != 1:
        raise NativeRequestContractError(("V5_ACQUISITION_RETRY_OWNER_INVALID",))
    if canonical_sha256(_policy_payload(policy)) != policy.policy_sha256:
        raise NativeRequestContractError(("V5_TRUSTED_POLICY_DIGEST_MISMATCH",))
    try:
        common = json.loads(policy.common_request_json.decode("utf-8"))
        source = json.loads(policy.source_identity_json.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NativeRequestContractError(
            ("V5_TRUSTED_POLICY_PAYLOAD_INVALID",)
        ) from exc
    canonical_common = validate_v5_common_request(common)
    canonical_source = canonical_source_identity(source)
    if (
        canonical_json_bytes(canonical_common) != policy.common_request_json
        or canonical_json_bytes(canonical_source) != policy.source_identity_json
    ):
        raise NativeRequestContractError(("V5_TRUSTED_POLICY_PAYLOAD_NONCANONICAL",))
    return policy


@_total_function("V5_TRUSTED_POLICY_BUILD_ERROR")
def build_trusted_v5_policy(
    *,
    expected_symbols: Any,
    expected_source_environment: Any,
    requested_information_cutoff_utc: Any,
    timeframe_order: Any = PRODUCTION_TIMEFRAME_ORDER,
    retries: int = 1,
    timeframe_window_years: Any = None,
    horizon_years: int = 10,
    embargo_days: float = 7.0,
    warmup_days: float = 400.0,
    safety_days: float = 14.0,
    boundary_tolerance_days: float = 3.0,
) -> TrustedV5Policy:
    """Build an immutable authority exclusively from trusted run inputs."""

    symbols = _strict_production_symbol_order(expected_symbols)
    timeframes = _strict_timeframe_order(timeframe_order)
    if type(retries) is not int or retries != 1:
        raise NativeRequestContractError(("V5_ACQUISITION_RETRY_OWNER_INVALID",))
    retry_count = retries
    common = build_v5_request_contract(
        requested_information_cutoff_utc=requested_information_cutoff_utc,
        timeframe_window_years=timeframe_window_years,
        horizon_years=horizon_years,
        embargo_days=embargo_days,
        warmup_days=warmup_days,
        safety_days=safety_days,
        boundary_tolerance_days=boundary_tolerance_days,
    )
    source_identity = canonical_source_identity(expected_source_environment)
    provisional = TrustedV5Policy(
        common_request_json=canonical_json_bytes(common),
        symbols=symbols,
        timeframes=timeframes,
        retries=retry_count,
        source_identity_json=canonical_json_bytes(source_identity),
        policy_sha256="",
    )
    return TrustedV5Policy(
        common_request_json=provisional.common_request_json,
        symbols=provisional.symbols,
        timeframes=provisional.timeframes,
        retries=provisional.retries,
        source_identity_json=provisional.source_identity_json,
        policy_sha256=canonical_sha256(_policy_payload(provisional)),
    )


def expected_reference_map() -> dict[str, list[str]]:
    return {
        timeframe: list(REFERENCE_GRAPH[timeframe])
        for timeframe in PRODUCTION_TIMEFRAME_ORDER
    }


def expected_consumer_map() -> dict[str, list[str]]:
    return {
        source: [
            target
            for target in PRODUCTION_TIMEFRAME_ORDER
            if source in REFERENCE_GRAPH[target]
        ]
        for source in PRODUCTION_TIMEFRAME_ORDER
    }


def _interval(start: datetime, end: datetime) -> dict[str, str]:
    if start >= end:
        raise NativeRequestContractError(("V5_DERIVED_INTERVAL_EMPTY",))
    return {"start_utc": _iso(start), "end_exclusive_utc": _iso(end)}


@_total_function("V5_REQUEST_HASH_PAYLOAD_ERROR")
def timeframe_request_hash_payload(request: Any) -> dict[str, Any]:
    """Return exactly the authenticated V5 map payload."""

    if not isinstance(request, Mapping):
        raise NativeRequestContractError(("V5_REQUEST_OBJECT_INVALID",))
    return {
        "schema_version": request.get("timeframe_request_map_schema_version"),
        "window_contract_version": request.get("window_contract_version"),
        "common_as_of_utc": request.get("common_as_of_utc"),
        "timeframe_window_years": request.get("timeframe_window_years"),
        "timeframe_calendar_boundaries": request.get("timeframe_calendar_boundaries"),
        "timeframe_required_evidence_lead_start_utc": request.get(
            "timeframe_required_evidence_lead_start_utc"
        ),
        "timeframe_source_request_intervals": request.get(
            "timeframe_source_request_intervals"
        ),
        "timeframe_strategy_evidence_intervals": request.get(
            "timeframe_strategy_evidence_intervals"
        ),
        "timeframe_audit_reference_map": request.get("timeframe_audit_reference_map"),
        "timeframe_audit_consumer_map": request.get("timeframe_audit_consumer_map"),
        "timeframe_audit_windows": request.get("timeframe_audit_windows"),
        "timeframe_audit_reference_intervals": request.get(
            "timeframe_audit_reference_intervals"
        ),
        "timeframe_audit_only_intervals": request.get("timeframe_audit_only_intervals"),
        "timeframe_consensus_policy": request.get("timeframe_consensus_policy"),
    }


@_total_function("V5_REQUEST_BUILD_ERROR")
def build_v5_request_contract(
    *,
    requested_information_cutoff_utc: Any,
    timeframe_window_years: Any = None,
    horizon_years: int = 10,
    embargo_days: float = 7.0,
    warmup_days: float = 400.0,
    safety_days: float = 14.0,
    boundary_tolerance_days: float = 3.0,
) -> dict[str, Any]:
    """Build the complete current V5 common request deterministically."""

    cutoff = _aware_utc(
        requested_information_cutoff_utc,
        "REQUESTED_INFORMATION_CUTOFF_INVALID",
    )
    horizon = _strict_positive_int(
        horizon_years,
        "V5_HORIZON_YEARS_INVALID",
        maximum=MAX_HORIZON_YEARS,
    )
    embargo = _strict_nonnegative_number(
        embargo_days,
        "V5_EMBARGO_DAYS_INVALID",
        maximum=MAX_POLICY_DAYS,
    )
    warmup = _strict_nonnegative_number(
        warmup_days,
        "V5_WARMUP_DAYS_INVALID",
        maximum=MAX_POLICY_DAYS,
    )
    safety = _strict_nonnegative_number(
        safety_days,
        "V5_SAFETY_DAYS_INVALID",
        maximum=MAX_POLICY_DAYS,
    )
    tolerance = _strict_nonnegative_number(
        boundary_tolerance_days,
        "V5_BOUNDARY_TOLERANCE_INVALID",
        maximum=MAX_POLICY_DAYS,
    )
    years = _strict_year_matrix(
        timeframe_window_years
        if timeframe_window_years is not None
        else DEFAULT_TIMEFRAME_WINDOW_YEARS
    )
    as_of = _floor_boundary(cutoff, TIMEFRAME_SECONDS["M5"])

    semantic_windows: dict[str, dict[str, tuple[datetime, datetime]]] = {}
    leads: dict[str, datetime] = {}
    for timeframe in PRODUCTION_TIMEFRAME_ORDER:
        boundaries = build_timeframe_calendar_boundaries(
            as_of,
            timeframe,
            timeframe_window_years=years,
            embargo=pd.Timedelta(days=embargo),
        )
        semantic_windows[timeframe] = {
            partition: (start.to_pydatetime(), end.to_pydatetime())
            for partition, (start, end) in boundaries.windows.items()
        }
        leads[timeframe] = semantic_windows[timeframe]["optimizer_train"][
            0
        ] - timedelta(
            days=warmup + safety,
            seconds=TIMEFRAME_SECONDS[timeframe],
        )

    boundaries_serialized = {
        timeframe: {
            partition: {"start_ts": _iso(start), "end_ts": _iso(end)}
            for partition, (start, end) in semantic_windows[timeframe].items()
        }
        for timeframe in PRODUCTION_TIMEFRAME_ORDER
    }
    leads_serialized = {
        timeframe: _iso(leads[timeframe]) for timeframe in PRODUCTION_TIMEFRAME_ORDER
    }

    strategy_ranges = {
        timeframe: native_source_interval(timeframe, leads[timeframe], as_of)
        for timeframe in PRODUCTION_TIMEFRAME_ORDER
    }
    strategy_intervals = {
        timeframe: _interval(*strategy_ranges[timeframe])
        for timeframe in PRODUCTION_TIMEFRAME_ORDER
    }

    references = expected_reference_map()
    consumers = expected_consumer_map()
    audit_window_ranges: dict[str, dict[str, tuple[datetime, datetime]]] = {}
    for target in PRODUCTION_TIMEFRAME_ORDER:
        audit_window_ranges[target] = {
            "strategy_full_context": (leads[target], as_of),
            **semantic_windows[target],
        }
    audit_windows = {
        target: {
            window: _interval(*interval)
            for window, interval in audit_window_ranges[target].items()
        }
        for target in PRODUCTION_TIMEFRAME_ORDER
    }

    reference_ranges: dict[str, dict[str, dict[str, tuple[datetime, datetime]]]] = {}
    for target in PRODUCTION_TIMEFRAME_ORDER:
        reference_ranges[target] = {}
        for reference in references[target]:
            reference_ranges[target][reference] = {
                window: native_source_interval(reference, start, end)
                for window, (start, end) in audit_window_ranges[target].items()
            }
    reference_intervals = {
        target: {
            reference: {
                window: _interval(*interval) for window, interval in windows.items()
            }
            for reference, windows in reference_ranges[target].items()
        }
        for target in PRODUCTION_TIMEFRAME_ORDER
    }

    source_parts = {
        timeframe: [strategy_ranges[timeframe]]
        for timeframe in PRODUCTION_TIMEFRAME_ORDER
    }
    for target in PRODUCTION_TIMEFRAME_ORDER:
        for reference in references[target]:
            source_parts[reference].extend(reference_ranges[target][reference].values())
    source_ranges = {
        timeframe: (
            min(interval[0] for interval in source_parts[timeframe]),
            max(interval[1] for interval in source_parts[timeframe]),
        )
        for timeframe in PRODUCTION_TIMEFRAME_ORDER
    }
    source_intervals = {
        timeframe: _interval(*source_ranges[timeframe])
        for timeframe in PRODUCTION_TIMEFRAME_ORDER
    }

    audit_only: dict[str, list[dict[str, Any]]] = {
        timeframe: [] for timeframe in PRODUCTION_TIMEFRAME_ORDER
    }
    for source in PRODUCTION_TIMEFRAME_ORDER:
        own_start, own_end = strategy_ranges[source]
        for target in consumers[source]:
            edge_start, edge_end = reference_ranges[target][source][
                "strategy_full_context"
            ]
            if edge_start < own_start:
                audit_only[source].append(
                    {
                        "consumer_target_timeframe": target,
                        "audit_window": "strategy_full_context",
                        "start_utc": _iso(edge_start),
                        "end_exclusive_utc": _iso(min(edge_end, own_start)),
                        "role": "audit_only_reference",
                    }
                )
            if edge_end > own_end:
                audit_only[source].append(
                    {
                        "consumer_target_timeframe": target,
                        "audit_window": "strategy_full_context",
                        "start_utc": _iso(max(edge_start, own_end)),
                        "end_exclusive_utc": _iso(edge_end),
                        "role": "audit_only_reference",
                    }
                )

    earliest_lead = min(leads.values())
    common: dict[str, Any] = {
        "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        "timeframe_request_map_schema_version": (TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION),
        "horizon_years": horizon,
        "requested_information_cutoff_utc": _iso(cutoff),
        "requested_horizon_start_utc": _iso(_subtract_calendar_years(as_of, horizon)),
        "common_requested_start_utc": _iso(earliest_lead),
        "required_evidence_lead_start_utc": _iso(earliest_lead),
        "warmup_days": warmup,
        "safety_days": safety,
        "closed_bar_slack_seconds": max(TIMEFRAME_SECONDS.values()),
        "common_as_of_utc": _iso(as_of),
        "boundary_tolerance_days": tolerance,
        "embargo_days": embargo,
        "timeframe_window_years": deepcopy(years),
        "timeframe_calendar_boundaries": boundaries_serialized,
        "timeframe_calendar_boundaries_sha256": canonical_sha256(boundaries_serialized),
        "timeframe_required_evidence_lead_start_utc": leads_serialized,
        "timeframe_source_request_intervals": source_intervals,
        "timeframe_strategy_evidence_intervals": strategy_intervals,
        "timeframe_audit_reference_map": references,
        "timeframe_audit_consumer_map": consumers,
        "timeframe_audit_windows": audit_windows,
        "timeframe_audit_reference_intervals": reference_intervals,
        "timeframe_audit_only_intervals": audit_only,
        "timeframe_consensus_policy": deepcopy(CONSENSUS_POLICY),
    }
    common["timeframe_request_map_sha256"] = canonical_sha256(
        timeframe_request_hash_payload(common)
    )
    return common


@_total_function("V5_LEAF_BUILD_ERROR")
def build_v5_leaf_request(
    common_request: Mapping[str, Any],
    timeframe: str,
    *,
    last_fully_closed_bar_open_utc: Any = None,
) -> dict[str, Any]:
    """Build one exact leaf projection, including native-fetch equality."""

    common = validate_v5_common_request(common_request)
    tf = _canonical_timeframe(timeframe)
    source = common["timeframe_source_request_intervals"][tf]
    if last_fully_closed_bar_open_utc is None:
        raise NativeRequestContractError(("V5_LAST_CLOSED_BAR_REQUIRED",))
    last_closed = _aware_utc(
        last_fully_closed_bar_open_utc,
        "V5_LAST_CLOSED_BAR_INVALID",
    )
    source_start, source_end = _interval_tuple(source, "V5_SOURCE_INTERVAL_INVALID")
    common_as_of = _aware_utc(common["common_as_of_utc"], "V5_AS_OF_INVALID")
    close_bound_valid = (
        last_closed < common_as_of
        if tf in {"H4", "D1"}
        else last_closed + timedelta(seconds=TIMEFRAME_SECONDS[tf]) <= common_as_of
    )
    if not (source_start <= last_closed < source_end and close_bound_valid):
        raise NativeRequestContractError(("V5_LAST_CLOSED_BAR_OUTSIDE_REQUEST",))
    leaf = deepcopy(common)
    leaf.update(
        {
            "window_years": common["timeframe_window_years"][tf],
            "common_calendar_boundaries": common["timeframe_calendar_boundaries"][tf],
            "cell_timeframe": tf,
            "cell_timeframe_window_years": common["timeframe_window_years"][tf],
            "cell_calendar_boundaries": common["timeframe_calendar_boundaries"][tf],
            "cell_required_evidence_lead_start_utc": common[
                "timeframe_required_evidence_lead_start_utc"
            ][tf],
            "source_request_interval": source,
            "strategy_evidence_interval": common[
                "timeframe_strategy_evidence_intervals"
            ][tf],
            "audit_reference_timeframes": common["timeframe_audit_reference_map"][tf],
            "audit_consumer_targets": common["timeframe_audit_consumer_map"][tf],
            "audit_windows": common["timeframe_audit_windows"][tf],
            "audit_reference_intervals": common["timeframe_audit_reference_intervals"][
                tf
            ],
            "audit_only_intervals": common["timeframe_audit_only_intervals"][tf],
            "native_fetch_start_utc": source["start_utc"],
            "native_fetch_end_exclusive_utc": source["end_exclusive_utc"],
            "alignment_mode": (
                BROKER_SESSION_ALIGNMENT_MODE
                if tf in {"H4", "D1"}
                else FIXED_ALIGNMENT_MODE
            ),
            "successor_sentinel_fetch_slack_seconds": (1 if tf in {"H4", "D1"} else 0),
            "last_fully_closed_bar_open_utc": _iso(last_closed),
        }
    )
    return leaf


def _map_roster_errors(request: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in (
        "timeframe_window_years",
        "timeframe_calendar_boundaries",
        "timeframe_required_evidence_lead_start_utc",
        "timeframe_source_request_intervals",
        "timeframe_strategy_evidence_intervals",
        "timeframe_audit_reference_map",
        "timeframe_audit_consumer_map",
        "timeframe_audit_windows",
        "timeframe_audit_reference_intervals",
        "timeframe_audit_only_intervals",
    ):
        value = request.get(field)
        if not isinstance(value, Mapping) or set(value) != PRODUCTION_TIMEFRAME_SET:
            errors.append(f"V5_MAP_ROSTER_INVALID:{field}")
    return errors


def _partition_chain_errors(request: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        as_of = _aware_utc(request.get("common_as_of_utc"), "V5_AS_OF_INVALID")
        embargo = timedelta(
            days=_strict_nonnegative_number(
                request.get("embargo_days"),
                "V5_EMBARGO_DAYS_INVALID",
                maximum=MAX_POLICY_DAYS,
            )
        )
    except NativeRequestContractError as exc:
        return list(exc.errors)
    boundaries = request.get("timeframe_calendar_boundaries")
    if not isinstance(boundaries, Mapping):
        return ["V5_MAP_ROSTER_INVALID:timeframe_calendar_boundaries"]
    for timeframe in PRODUCTION_TIMEFRAME_ORDER:
        cell = boundaries.get(timeframe)
        if not isinstance(cell, Mapping) or set(cell) != set(PRODUCTION_PARTITIONS):
            errors.append(f"V5_PARTITION_ROSTER_INVALID:{timeframe}")
            continue
        parsed: dict[str, tuple[datetime, datetime]] = {}
        for partition in PRODUCTION_PARTITIONS:
            value = cell.get(partition)
            if not isinstance(value, Mapping):
                errors.append(f"V5_PARTITION_INTERVAL_INVALID:{timeframe}:{partition}")
                continue
            try:
                start = _aware_utc(
                    value.get("start_ts"),
                    f"V5_PARTITION_INTERVAL_INVALID:{timeframe}:{partition}",
                )
                end = _aware_utc(
                    value.get("end_ts"),
                    f"V5_PARTITION_INTERVAL_INVALID:{timeframe}:{partition}",
                )
                if start >= end:
                    raise NativeRequestContractError(
                        (f"V5_PARTITION_INTERVAL_INVALID:{timeframe}:{partition}",)
                    )
                parsed[partition] = (start, end)
            except NativeRequestContractError as exc:
                errors.extend(exc.errors)
        if len(parsed) != len(PRODUCTION_PARTITIONS):
            continue
        train = parsed["optimizer_train"]
        tuning = parsed["tuning_validation"]
        lockbox = parsed["final_lockbox"]
        if not (
            train[1] + embargo == tuning[0]
            and tuning[1] + embargo == lockbox[0]
            and lockbox[1] == as_of
            and train[0] < train[1] <= tuning[0] < tuning[1] <= lockbox[0] < lockbox[1]
        ):
            errors.append(f"V5_PARTITION_CHAIN_INVALID:{timeframe}")
    return errors


@_total_function("V5_COMMON_REQUEST_VALIDATION_ERROR")
def validate_v5_common_request(request: Any) -> dict[str, Any]:
    """Validate and return the canonical V5 common projection.

    Validation is semantic, not merely cryptographic: all derived maps are
    independently rebuilt from the declared cutoff, years, embargo, warmup,
    and safety values.
    """

    if not isinstance(request, Mapping):
        raise NativeRequestContractError(("V5_REQUEST_OBJECT_INVALID",))
    # Traverse once under a global node/depth/container budget before any
    # field-specific work.  This makes every later comparison bounded.
    canonical_json_bytes(request)
    errors: list[str] = []
    missing = [key for key in COMMON_REQUEST_KEYS if key not in request]
    if missing:
        errors.extend(f"V5_COMMON_FIELD_MISSING:{key}" for key in missing)
    unexpected = sorted(set(request) - set(COMMON_REQUEST_KEYS))
    if unexpected:
        errors.extend(f"V5_COMMON_FIELD_UNEXPECTED:{key}" for key in unexpected)
    if request.get("window_contract_version") != TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
        errors.append("V5_WINDOW_CONTRACT_VERSION_INVALID")
    if (
        request.get("timeframe_request_map_schema_version")
        != TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
    ):
        errors.append("V5_REQUEST_MAP_SCHEMA_VERSION_INVALID")
    errors.extend(_map_roster_errors(request))
    errors.extend(_partition_chain_errors(request))
    audit_only = request.get("timeframe_audit_only_intervals")
    if isinstance(audit_only, Mapping):
        for timeframe in PRODUCTION_TIMEFRAME_ORDER:
            values = audit_only.get(timeframe)
            if (
                not isinstance(values, list)
                or len(values) > len(PRODUCTION_TIMEFRAME_ORDER) * 2
            ):
                errors.append(f"V5_AUDIT_ONLY_INTERVAL_CAP_INVALID:{timeframe}")
    if errors:
        raise NativeRequestContractError(errors)

    try:
        expected = build_v5_request_contract(
            requested_information_cutoff_utc=request[
                "requested_information_cutoff_utc"
            ],
            timeframe_window_years=request["timeframe_window_years"],
            horizon_years=request["horizon_years"],
            embargo_days=request["embargo_days"],
            warmup_days=request["warmup_days"],
            safety_days=request["safety_days"],
            boundary_tolerance_days=request["boundary_tolerance_days"],
        )
    except NativeRequestContractError:
        raise

    for key in COMMON_REQUEST_KEYS:
        if not _same(request.get(key), expected[key]):
            errors.append(f"V5_COMMON_FIELD_MISMATCH:{key}")
    boundaries = request.get("timeframe_calendar_boundaries")
    if request.get("timeframe_calendar_boundaries_sha256") != canonical_sha256(
        boundaries
    ):
        errors.append("V5_BOUNDARY_MAP_HASH_MISMATCH")
    if request.get("timeframe_request_map_sha256") != canonical_sha256(
        timeframe_request_hash_payload(request)
    ):
        errors.append("V5_REQUEST_MAP_HASH_MISMATCH")
    if errors:
        raise NativeRequestContractError(errors)
    return {key: deepcopy(expected[key]) for key in COMMON_REQUEST_KEYS}


@_total_function("V5_MANIFEST_REQUEST_BUILD_ERROR")
def build_v5_manifest_request(
    common_request: Any,
    *,
    symbols: Any,
    retries: int = 1,
    timeframes: Any = PRODUCTION_TIMEFRAME_ORDER,
) -> dict[str, Any]:
    """Build the production writer's run-level compatibility projection."""

    common = validate_v5_common_request(common_request)
    canonical_roster = _strict_production_symbol_order(symbols)
    timeframe_roster = _strict_timeframe_order(timeframes)
    if type(retries) is not int or retries != 1:
        raise NativeRequestContractError(("V5_ACQUISITION_RETRY_OWNER_INVALID",))
    retry_count = retries
    return {
        "symbols": list(canonical_roster),
        "timeframes": list(timeframe_roster),
        "retries": retry_count,
        "window_years": deepcopy(common["timeframe_window_years"]["M5"]),
        "common_calendar_boundaries": deepcopy(
            common["timeframe_calendar_boundaries"]["M5"]
        ),
        **deepcopy(common),
    }


@_total_function("V5_MANIFEST_REQUEST_VALIDATION_ERROR")
def validate_v5_manifest_request(
    request: Any,
    trusted_policy: Any,
) -> dict[str, Any]:
    """Validate a manifest request against an out-of-band trusted policy."""

    policy = _validate_trusted_policy(trusted_policy)
    if not isinstance(request, Mapping):
        raise NativeRequestContractError(("V5_MANIFEST_REQUEST_INVALID",))
    canonical_json_bytes(request)
    required = {
        *COMMON_REQUEST_KEYS,
        "symbols",
        "timeframes",
        "retries",
        "window_years",
        "common_calendar_boundaries",
    }
    missing = sorted(required - set(request))
    if missing:
        raise NativeRequestContractError(
            tuple(f"V5_MANIFEST_FIELD_MISSING:{field}" for field in missing)
        )
    extra = sorted(set(request) - required)
    if extra:
        raise NativeRequestContractError(
            tuple(f"V5_MANIFEST_FIELD_UNEXPECTED:{field}" for field in extra)
        )
    common = validate_v5_common_request(
        {key: request.get(key) for key in COMMON_REQUEST_KEYS}
    )
    expected_common = json.loads(policy.common_request_json.decode("utf-8"))
    errors: list[str] = []
    if not _same(common, expected_common):
        errors.append("V5_TRUSTED_COMMON_REQUEST_MISMATCH")
    try:
        observed_symbols = _strict_production_symbol_order(request.get("symbols"))
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
        observed_symbols = ()
    if observed_symbols != policy.symbols or request.get("symbols") != list(
        policy.symbols
    ):
        errors.append("V5_TRUSTED_SYMBOL_ROSTER_MISMATCH")
    try:
        observed_timeframes = _strict_timeframe_order(request.get("timeframes"))
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
        observed_timeframes = ()
    if observed_timeframes != policy.timeframes or request.get("timeframes") != list(
        policy.timeframes
    ):
        errors.append("V5_TRUSTED_TIMEFRAME_ORDER_MISMATCH")
    if request.get("retries") != policy.retries:
        errors.append("V5_TRUSTED_RETRY_COUNT_MISMATCH")
    if not _same(request.get("window_years"), common["timeframe_window_years"]["M5"]):
        errors.append("V5_MANIFEST_WINDOW_YEARS_MISMATCH")
    if not _same(
        request.get("common_calendar_boundaries"),
        common["timeframe_calendar_boundaries"]["M5"],
    ):
        errors.append("V5_MANIFEST_COMPATIBILITY_BOUNDARIES_MISMATCH")
    if errors:
        raise NativeRequestContractError(errors)
    return common


def _validate_v5_leaf_request_against_common(
    request: Mapping[str, Any],
    common: Mapping[str, Any],
    timeframe: str,
) -> dict[str, Any]:
    """Validate one bounded leaf against already validated immutable run facts."""

    tf = _canonical_timeframe(timeframe)
    errors: list[str] = []
    expected_keys = {
        *COMMON_REQUEST_KEYS,
        "window_years",
        "common_calendar_boundaries",
        *LEAF_PROJECTION_KEYS,
        "native_fetch_start_utc",
        "native_fetch_end_exclusive_utc",
        "alignment_mode",
        "successor_sentinel_fetch_slack_seconds",
        "last_fully_closed_bar_open_utc",
    }
    extra = sorted(set(request) - expected_keys)
    if extra:
        errors.extend(f"V5_LEAF_FIELD_UNEXPECTED:{field}" for field in extra)
    for key in COMMON_REQUEST_KEYS:
        if key not in request or not _same(request.get(key), common[key]):
            errors.append(f"V5_LEAF_COMMON_MISMATCH:{key}")
    expected_projection = {
        "window_years": common["timeframe_window_years"][tf],
        "common_calendar_boundaries": common["timeframe_calendar_boundaries"][tf],
        "cell_timeframe": tf,
        "cell_timeframe_window_years": common["timeframe_window_years"][tf],
        "cell_calendar_boundaries": common["timeframe_calendar_boundaries"][tf],
        "cell_required_evidence_lead_start_utc": common[
            "timeframe_required_evidence_lead_start_utc"
        ][tf],
        "source_request_interval": common["timeframe_source_request_intervals"][tf],
        "strategy_evidence_interval": common["timeframe_strategy_evidence_intervals"][
            tf
        ],
        "audit_reference_timeframes": common["timeframe_audit_reference_map"][tf],
        "audit_consumer_targets": common["timeframe_audit_consumer_map"][tf],
        "audit_windows": common["timeframe_audit_windows"][tf],
        "audit_reference_intervals": common["timeframe_audit_reference_intervals"][tf],
        "audit_only_intervals": common["timeframe_audit_only_intervals"][tf],
    }
    for key, expected in expected_projection.items():
        if key not in request or not _same(request.get(key), expected):
            errors.append(f"V5_LEAF_PROJECTION_MISMATCH:{key}")
    source = expected_projection["source_request_interval"]
    if request.get("native_fetch_start_utc") != source["start_utc"]:
        errors.append("V5_NATIVE_FETCH_START_MISMATCH")
    if request.get("native_fetch_end_exclusive_utc") != source["end_exclusive_utc"]:
        errors.append("V5_NATIVE_FETCH_END_MISMATCH")
    expected_alignment = (
        BROKER_SESSION_ALIGNMENT_MODE if tf in {"H4", "D1"} else FIXED_ALIGNMENT_MODE
    )
    if request.get("alignment_mode") != expected_alignment:
        errors.append("V5_NATIVE_ALIGNMENT_MODE_MISMATCH")
    expected_slack = 1 if tf in {"H4", "D1"} else 0
    if request.get("successor_sentinel_fetch_slack_seconds") != expected_slack:
        errors.append("V5_NATIVE_SUCCESSOR_SLACK_MISMATCH")
    try:
        last_closed = _aware_utc(
            request.get("last_fully_closed_bar_open_utc"),
            "V5_LAST_CLOSED_BAR_INVALID",
        )
        source_start, source_end = _interval_tuple(source, "V5_SOURCE_INTERVAL_INVALID")
        common_as_of = _aware_utc(common["common_as_of_utc"], "V5_AS_OF_INVALID")
        close_bound_valid = (
            last_closed < common_as_of
            if tf in {"H4", "D1"}
            else last_closed + timedelta(seconds=TIMEFRAME_SECONDS[tf]) <= common_as_of
        )
        if not (source_start <= last_closed < source_end and close_bound_valid):
            errors.append("V5_LAST_CLOSED_BAR_OUTSIDE_REQUEST")
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
    if errors:
        raise NativeRequestContractError(errors)
    return _thaw_v5_cell_context_value(expected_projection)


@_total_function("V5_LEAF_REQUEST_VALIDATION_ERROR")
def validate_v5_leaf_request(
    request: Any,
    common_request: Mapping[str, Any],
    timeframe: str,
) -> dict[str, Any]:
    """Validate one provenance leaf against the exact run-level map."""

    if not isinstance(request, Mapping):
        raise NativeRequestContractError(("V5_LEAF_REQUEST_INVALID",))
    canonical_json_bytes(request)
    common = validate_v5_common_request(common_request)
    tf = _canonical_timeframe(timeframe)
    return _validate_v5_leaf_request_against_common(request, common, tf)


@_total_function("V5_FAILED_LEAF_REQUEST_VALIDATION_ERROR")
def validate_v5_failed_leaf_request(
    request: Any,
    common_request: Mapping[str, Any],
    timeframe: str,
) -> dict[str, Any]:
    """Validate the static request projection of a provenance-only failure.

    With no immutable bars artifact there is no authenticated final closed-bar
    observation.  Every static field must still match the run contract exactly,
    while that one observation is required to remain null.
    """

    if not isinstance(request, Mapping):
        raise NativeRequestContractError(("V5_FAILED_LEAF_REQUEST_INVALID",))
    canonical_json_bytes(request)
    if request.get("last_fully_closed_bar_open_utc") is not None:
        raise NativeRequestContractError(("V5_FAILED_LAST_CLOSED_BAR_MUST_BE_NULL",))
    tf = _canonical_timeframe(timeframe)
    common = validate_v5_common_request(common_request)
    proxy = deepcopy(dict(request))
    source_start, _ = _interval_tuple(
        common["timeframe_source_request_intervals"][tf],
        "V5_SOURCE_INTERVAL_INVALID",
    )
    proxy["last_fully_closed_bar_open_utc"] = _iso(source_start)
    projection = validate_v5_leaf_request(proxy, common, tf)
    return projection


def _interval_tuple(value: Any, code: str) -> tuple[datetime, datetime]:
    if not isinstance(value, Mapping):
        raise NativeRequestContractError((code,))
    start = _aware_utc(value.get("start_utc"), code)
    end = _aware_utc(value.get("end_exclusive_utc"), code)
    if start >= end:
        raise NativeRequestContractError((code,))
    return start, end


def _resource_snapshot_errors(value: Any, *, code: str) -> list[str]:
    """Validate the exact non-authoritative resource guard observation.

    The snapshot does not authenticate market data. It proves that the writer
    actually evaluated the fixed HCC and free-space ceilings it claims to have
    enforced; omission or internally inconsistent arithmetic is therefore a
    contract error rather than an optional diagnostic.
    """

    if not isinstance(value, Mapping):
        return [code]
    status = value.get("status")
    violations = value.get("violations")
    if not isinstance(violations, list) or violations:
        return [code]
    expected_fields = {
        "status",
        "data_path",
        "disk_total_bytes",
        "disk_free_bytes",
        "minimum_free_bytes",
        "hcc_files_inspected",
        "hcc_entries_inspected",
        "largest_hcc_bytes",
        "largest_hcc_relative_path",
        "maximum_hcc_file_bytes",
        "scan_scope",
        "scan_error",
        "violations",
    }
    if status != "within_bounds" or set(value) != expected_fields:
        return [code]
    try:
        disk_total = _strict_positive_int(
            value.get("disk_total_bytes"), code, maximum=2**63 - 1
        )
        disk_free = _strict_nonnegative_int(
            value.get("disk_free_bytes"), code, maximum=2**63 - 1
        )
        minimum_free = _strict_nonnegative_int(
            value.get("minimum_free_bytes"), code, maximum=2**63 - 1
        )
        inspected = _strict_nonnegative_int(
            value.get("hcc_files_inspected"), code, maximum=10_000_000
        )
        entries_inspected = _strict_nonnegative_int(
            value.get("hcc_entries_inspected"),
            code,
            maximum=MAX_TERMINAL_HCC_SCAN_ENTRIES,
        )
        largest = _strict_nonnegative_int(
            value.get("largest_hcc_bytes"), code, maximum=2**63 - 1
        )
        maximum_hcc = _strict_positive_int(
            value.get("maximum_hcc_file_bytes"), code, maximum=2**63 - 1
        )
    except NativeRequestContractError:
        return [code]
    largest_path = value.get("largest_hcc_relative_path")
    if largest_path is not None and (
        not isinstance(largest_path, str)
        or not largest_path
        or len(largest_path) > 4096
    ):
        return [code]
    if not isinstance(value.get("data_path"), str) or not value.get("data_path"):
        return [code]
    if (
        minimum_free != TERMINAL_MIN_FREE_BYTES
        or maximum_hcc != TERMINAL_HCC_MAX_FILE_BYTES
        or disk_free < minimum_free
        or disk_free > disk_total
        or largest > maximum_hcc
        or entries_inspected < inspected
        or (inspected == 0 and (largest != 0 or largest_path is not None))
        # MT5 can create zero-length yearly HCC placeholders before the first
        # history download fills them.  The bounded writer counts those files
        # but, by construction, retains no largest-file identity until a file
        # has positive size.  ``inspected > 0, largest == 0, path is None`` is
        # therefore an exact all-zero observation, not missing resource
        # evidence.  A positive largest size must still carry its identity.
        or (inspected > 0 and largest > 0 and largest_path is None)
        # A first request for a broker symbol can legitimately precede MT5's
        # creation of ``history/<symbol>``.  In that case the writer performs
        # the same bounded scan at the containing server-history scope.  Both
        # scopes are exact writer states; no unbounded traversal is admitted.
        or value.get("scan_scope") not in {"symbol", "server"}
        or value.get("scan_error") is not None
    ):
        return [code]
    return []


def _output_resource_snapshot_errors(value: Any) -> list[str]:
    code = "V5_OUTPUT_RESOURCE_BOUND_INVALID"
    expected_fields = {
        "status",
        "output_path",
        "disk_total_bytes",
        "disk_free_bytes",
        "minimum_free_bytes",
        "maximum_cell_bytes",
        "estimated_cell_bytes",
        "sample_rows",
        "violations",
    }
    if (
        not isinstance(value, Mapping)
        or set(value) != expected_fields
        or value.get("status") != "within_bounds"
        or not isinstance(value.get("violations"), list)
        or value.get("violations")
        or not isinstance(value.get("output_path"), str)
        or not value.get("output_path")
    ):
        return [code]
    try:
        total = _strict_positive_int(
            value.get("disk_total_bytes"), code, maximum=2**63 - 1
        )
        free = _strict_nonnegative_int(
            value.get("disk_free_bytes"), code, maximum=2**63 - 1
        )
        minimum = _strict_nonnegative_int(
            value.get("minimum_free_bytes"), code, maximum=2**63 - 1
        )
        maximum = _strict_positive_int(
            value.get("maximum_cell_bytes"), code, maximum=2**63 - 1
        )
        estimated = _strict_nonnegative_int(
            value.get("estimated_cell_bytes"), code, maximum=2**63 - 1
        )
        sample_rows = _strict_nonnegative_int(
            value.get("sample_rows"), code, maximum=2048
        )
    except NativeRequestContractError:
        return [code]
    if (
        minimum != OUTPUT_MIN_FREE_BYTES
        or maximum != OUTPUT_MAX_CELL_BYTES
        or free > total
        or estimated > maximum
        or free - estimated < minimum
        or (estimated == 0 and sample_rows != 0)
        or (estimated > 0 and sample_rows == 0)
    ):
        return [code]
    return []


def _error_diagnostic_valid(value: Any) -> bool:
    return bool(
        isinstance(value, Mapping)
        and set(value) == {"type", "message"}
        and isinstance(value.get("type"), str)
        and bool(value.get("type"))
        and len(value.get("type")) <= 256
        and isinstance(value.get("message"), str)
        and len(value.get("message")) <= 4096
    )


def _output_resource_limit_snapshot_errors(value: Any) -> list[str]:
    code = "V5_OUTPUT_RESOURCE_LIMIT_EVIDENCE_INVALID"
    if not isinstance(value, Mapping):
        return [code]
    if value.get("status") == "unavailable":
        return (
            []
            if set(value)
            == {
                "status",
                "output_path",
                "minimum_free_bytes",
                "maximum_cell_bytes",
                "violations",
                "error",
            }
            and isinstance(value.get("output_path"), str)
            and bool(value.get("output_path"))
            and value.get("minimum_free_bytes") == OUTPUT_MIN_FREE_BYTES
            and value.get("maximum_cell_bytes") == OUTPUT_MAX_CELL_BYTES
            and value.get("violations") == ["output_disk_usage_unavailable"]
            and _error_diagnostic_valid(value.get("error"))
            else [code]
        )
    expected_fields = {
        "status",
        "output_path",
        "disk_total_bytes",
        "disk_free_bytes",
        "minimum_free_bytes",
        "maximum_cell_bytes",
        "estimated_cell_bytes",
        "sample_rows",
        "violations",
    }
    if (
        set(value) != expected_fields
        or value.get("status") != "limit_exceeded"
        or not isinstance(value.get("output_path"), str)
        or not value.get("output_path")
    ):
        return [code]
    try:
        total = _strict_positive_int(
            value.get("disk_total_bytes"), code, maximum=2**63 - 1
        )
        free = _strict_nonnegative_int(
            value.get("disk_free_bytes"), code, maximum=2**63 - 1
        )
        minimum = _strict_nonnegative_int(
            value.get("minimum_free_bytes"), code, maximum=2**63 - 1
        )
        maximum = _strict_positive_int(
            value.get("maximum_cell_bytes"), code, maximum=2**63 - 1
        )
        estimated = _strict_nonnegative_int(
            value.get("estimated_cell_bytes"), code, maximum=2**63 - 1
        )
        sample_rows = _strict_nonnegative_int(
            value.get("sample_rows"), code, maximum=2048
        )
    except NativeRequestContractError:
        return [code]
    expected_violations: list[str] = []
    if estimated > OUTPUT_MAX_CELL_BYTES:
        expected_violations.append("output_cell_byte_cap_exceeded")
    if free - estimated < OUTPUT_MIN_FREE_BYTES:
        expected_violations.append("output_disk_free_reserve_breached")
    if (
        minimum != OUTPUT_MIN_FREE_BYTES
        or maximum != OUTPUT_MAX_CELL_BYTES
        or free > total
        or (estimated == 0 and sample_rows != 0)
        or (estimated > 0 and sample_rows == 0)
        or not expected_violations
        or value.get("violations") != expected_violations
    ):
        return [code]
    return []


def _terminal_resource_limit_snapshot_errors(value: Any) -> list[str]:
    code = "V5_TERMINAL_RESOURCE_LIMIT_EVIDENCE_INVALID"
    if not isinstance(value, Mapping):
        return [code]
    status = value.get("status")
    if status == "unavailable":
        return (
            []
            if set(value) == {"status", "violations", "reason"}
            and value.get("violations") == []
            and value.get("reason") == "terminal_data_path_not_reported"
            else [code]
        )
    if status == "invalid":
        violation = value.get("violations")
        if violation == ["terminal_data_path_invalid"]:
            valid = set(value) == {"status", "data_path", "violations"}
        elif violation == ["terminal_disk_usage_unavailable"]:
            valid = bool(
                set(value) == {"status", "data_path", "violations", "error"}
                and _error_diagnostic_valid(value.get("error"))
            )
        else:
            valid = False
        return (
            []
            if valid
            and isinstance(value.get("data_path"), str)
            and bool(value.get("data_path"))
            else [code]
        )
    expected_fields = {
        "status",
        "data_path",
        "disk_total_bytes",
        "disk_free_bytes",
        "minimum_free_bytes",
        "hcc_files_inspected",
        "hcc_entries_inspected",
        "largest_hcc_bytes",
        "largest_hcc_relative_path",
        "maximum_hcc_file_bytes",
        "scan_scope",
        "scan_error",
        "violations",
    }
    if status != "limit_exceeded" or set(value) != expected_fields:
        return [code]
    try:
        total = _strict_positive_int(
            value.get("disk_total_bytes"), code, maximum=2**63 - 1
        )
        free = _strict_nonnegative_int(
            value.get("disk_free_bytes"), code, maximum=2**63 - 1
        )
        minimum = _strict_nonnegative_int(
            value.get("minimum_free_bytes"), code, maximum=2**63 - 1
        )
        inspected = _strict_nonnegative_int(
            value.get("hcc_files_inspected"), code, maximum=10_000_000
        )
        entries_inspected = _strict_nonnegative_int(
            value.get("hcc_entries_inspected"),
            code,
            maximum=MAX_TERMINAL_HCC_SCAN_ENTRIES,
        )
        largest = _strict_nonnegative_int(
            value.get("largest_hcc_bytes"), code, maximum=2**63 - 1
        )
        maximum_hcc = _strict_positive_int(
            value.get("maximum_hcc_file_bytes"), code, maximum=2**63 - 1
        )
    except NativeRequestContractError:
        return [code]
    largest_path = value.get("largest_hcc_relative_path")
    scan_error = value.get("scan_error")
    violations = value.get("violations")
    if not isinstance(violations, list):
        return [code]
    expected_required: list[str] = []
    if free < TERMINAL_MIN_FREE_BYTES:
        expected_required.append("terminal_disk_free_reserve_breached")
    if scan_error is not None:
        expected_required.append("terminal_hcc_scan_failed")
    if largest > TERMINAL_HCC_MAX_FILE_BYTES:
        expected_required.append("terminal_hcc_file_bound_breached")
    permitted = {
        "terminal_disk_free_reserve_breached",
        "terminal_server_path_escape",
        "terminal_symbol_path_escape",
        "terminal_hcc_scan_failed",
        "terminal_hcc_file_bound_breached",
    }
    if (
        not isinstance(value.get("data_path"), str)
        or not value.get("data_path")
        or minimum != TERMINAL_MIN_FREE_BYTES
        or maximum_hcc != TERMINAL_HCC_MAX_FILE_BYTES
        or free > total
        or entries_inspected < inspected
        or value.get("scan_scope") not in {"symbol", "server"}
        or (scan_error is not None and not _error_diagnostic_valid(scan_error))
        or (inspected == 0 and (largest != 0 or largest_path is not None))
        or (
            inspected > 0
            and (
                largest <= 0
                or not isinstance(largest_path, str)
                or not largest_path
                or len(largest_path) > 4096
            )
        )
        or not violations
        or len(set(violations)) != len(violations)
        or any(item not in permitted for item in violations)
        or any(item not in violations for item in expected_required)
        or (
            "terminal_disk_free_reserve_breached" in violations
            and free >= TERMINAL_MIN_FREE_BYTES
        )
        or (
            "terminal_hcc_file_bound_breached" in violations
            and largest <= TERMINAL_HCC_MAX_FILE_BYTES
        )
        or ("terminal_hcc_scan_failed" in violations) is not (scan_error is not None)
    ):
        return [code]
    return []


_MT5_V4_DIAGNOSTIC_FIELDS: Final = frozenset(
    {
        "range_contract_version",
        "symbol",
        "broker_symbol",
        "timeframe",
        "sync_retry",
        "sync_status",
        "request_start_epoch_second",
        "request_end_epoch_second_exclusive",
        "copy_rates_range_attempts",
        "copy_rates_range_max_attempts",
        "history_download_trigger_attempts",
        "history_download_trigger_nonempty",
        "history_download_trigger_max_bars",
        "history_download_trigger_errors",
        "terminal_confirmation_consumed",
        "call_ledger_schema",
        "call_ledger",
        "hard_failure",
        "hard_failure_call",
        "last_error",
        "initial_raw_relation",
        "confirmation_raw_relation",
        "raw_relation",
        "raw_count",
        "raw_extent",
        "admitted_count",
        "admitted_extent",
        "boundary_span_seconds",
        "boundary_slack_seconds",
        "boundary_complete",
        "best_extent",
    }
)
_MT5_V4_LEDGER_FIELDS: Final = frozenset(
    {
        "sequence",
        "call_type",
        "elapsed_ms",
        "last_error",
        "exception",
        "hard_failure",
        "raw_relation",
        "raw_count",
        "raw_extent",
        "admitted_count",
        "admitted_extent",
        "parse_error",
    }
)
_MT5_RAW_RELATIONS: Final = frozenset(
    {None, "IN_RANGE", "LEFT_DISJOINT_SUFFIX", "RIGHT_DISJOINT_PREFIX", "MIXED_INVALID"}
)


def _mt5_extent_valid(value: Any, *, count: int) -> bool:
    if count == 0:
        return value is None
    if not isinstance(value, Mapping) or set(value) != {
        "rows",
        "first_epoch_second",
        "last_epoch_second",
    }:
        return False
    return bool(
        type(value.get("rows")) is int
        and value.get("rows") == count
        and type(value.get("first_epoch_second")) is int
        and type(value.get("last_epoch_second")) is int
        and value["first_epoch_second"] <= value["last_epoch_second"]
    )


def _mt5_last_error_valid(value: Any) -> bool:
    capture_error = value.get("capture_error") if isinstance(value, Mapping) else None
    capture_error_valid = bool(
        capture_error is None
        or isinstance(capture_error, str)
        or (
            isinstance(capture_error, Mapping)
            and set(capture_error) == {"type", "message"}
            and isinstance(capture_error.get("type"), str)
            and bool(capture_error.get("type"))
            and isinstance(capture_error.get("message"), str)
        )
    )
    return bool(
        isinstance(value, Mapping)
        and set(value) == {"code", "message", "capture_error"}
        and (value.get("code") is None or type(value.get("code")) is int)
        and isinstance(value.get("message"), str)
        and capture_error_valid
    )


def _mt5_boundary_geometry(
    *, start_epoch: int, end_epoch: int, timeframe_seconds: int
) -> tuple[int, int]:
    span = max(0, end_epoch - start_epoch)
    if span <= 0:
        return span, 0
    base_slack = max(4 * 86_400, 4 * timeframe_seconds)
    nonoverlap_budget = span - timeframe_seconds
    if nonoverlap_budget <= 0:
        return span, 0
    return span, min(base_slack, max(0, (nonoverlap_budget - 1) // 2))


def _mt5_boundary_complete_from_extent(
    extent: Any,
    *,
    start_epoch: int,
    end_epoch: int,
    timeframe_seconds: int,
) -> bool:
    if not isinstance(extent, Mapping):
        return False
    span, slack = _mt5_boundary_geometry(
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        timeframe_seconds=timeframe_seconds,
    )
    if span <= 0:
        return False
    return bool(
        extent["first_epoch_second"] <= start_epoch + slack
        and extent["last_epoch_second"] >= end_epoch - timeframe_seconds - slack
    )


def _mt5_analysis_geometry_valid(
    entry: Mapping[str, Any], *, start_epoch: int, end_epoch: int
) -> bool:
    relation = entry.get("raw_relation")
    raw_count = entry.get("raw_count")
    raw_extent = entry.get("raw_extent")
    admitted_count = entry.get("admitted_count")
    admitted_extent = entry.get("admitted_extent")
    parse_error = entry.get("parse_error")
    if raw_count == 0:
        return bool(
            relation is None
            and raw_extent is None
            and admitted_count == 0
            and admitted_extent is None
            and parse_error is None
        )
    if parse_error is not None:
        return bool(
            relation == "MIXED_INVALID"
            and admitted_count == 0
            and admitted_extent is None
        )
    if not isinstance(raw_extent, Mapping):
        return False
    raw_first = raw_extent["first_epoch_second"]
    raw_last = raw_extent["last_epoch_second"]
    if raw_first >= start_epoch and raw_last < end_epoch:
        expected_relation = "IN_RANGE"
    elif raw_first >= end_epoch:
        expected_relation = "RIGHT_DISJOINT_PREFIX"
    elif raw_last < start_epoch:
        expected_relation = "LEFT_DISJOINT_SUFFIX"
    else:
        expected_relation = "MIXED_INVALID"
    if relation != expected_relation:
        return False
    if relation == "IN_RANGE":
        return bool(admitted_count == raw_count and _same(admitted_extent, raw_extent))
    if relation in {"RIGHT_DISJOINT_PREFIX", "LEFT_DISJOINT_SUFFIX"}:
        return bool(admitted_count == 0 and admitted_extent is None)
    if admitted_count == 0:
        return admitted_extent is None
    return bool(
        isinstance(admitted_extent, Mapping)
        and admitted_extent["first_epoch_second"] >= start_epoch
        and admitted_extent["last_epoch_second"] < end_epoch
    )


def _mt5_v4_diagnostics_errors(
    value: Any,
    *,
    interval: tuple[datetime, datetime],
    expected_symbol: str,
    expected_broker_symbol: str,
    timeframe: str,
    copy_attempts: int,
    category: str,
) -> list[str]:
    """Validate one exact released MT5 V4 call receipt.

    The page ledger is not allowed to assert data/emptiness independently of
    the connector that observed it.  This binds the half-open request, bounded
    call sequence, selected raw/admitted extent, and typed page outcome.
    """

    code = "V5_PAGE_MT5_V4_DIAGNOSTICS_INVALID"
    if not isinstance(value, Mapping) or set(value) != _MT5_V4_DIAGNOSTIC_FIELDS:
        return [code]
    start, end = interval
    start_epoch = int(start.timestamp())
    end_epoch = int(end.timestamp())
    timeframe_seconds = TIMEFRAME_SECONDS[timeframe]
    expected_span, expected_slack = _mt5_boundary_geometry(
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        timeframe_seconds=timeframe_seconds,
    )
    if (
        value.get("range_contract_version") != "mt5-half-open-range-v4"
        or value.get("call_ledger_schema") != "mt5.range-call-ledger.v1"
        or value.get("symbol") != expected_symbol
        or value.get("broker_symbol") != expected_broker_symbol
        or value.get("timeframe") != timeframe
        or value.get("sync_retry") is not True
        or value.get("request_start_epoch_second") != start_epoch
        or value.get("request_end_epoch_second_exclusive") != end_epoch
        or value.get("copy_rates_range_attempts") != copy_attempts
        or value.get("copy_rates_range_max_attempts") != 2
        or type(value.get("terminal_confirmation_consumed")) is not bool
        or type(value.get("hard_failure")) is not bool
        or type(value.get("boundary_complete")) is not bool
    ):
        return [code]
    integer_fields = (
        "history_download_trigger_attempts",
        "history_download_trigger_nonempty",
        "history_download_trigger_max_bars",
        "raw_count",
        "admitted_count",
        "boundary_span_seconds",
        "boundary_slack_seconds",
    )
    if any(
        type(value.get(field)) is not int or value.get(field) < 0
        for field in integer_fields
    ):
        return [code]
    trigger_attempts = value["history_download_trigger_attempts"]
    expected_trigger_bars = min(
        50_000,
        max(1, int(math.ceil(expected_span / timeframe_seconds)) + 2),
    )
    if trigger_attempts not in {0, 1}:
        return [code]
    if (
        value["history_download_trigger_nonempty"] > trigger_attempts
        or (trigger_attempts == 0 and value["history_download_trigger_max_bars"] != 0)
        or (
            trigger_attempts == 1
            and value["history_download_trigger_max_bars"] != expected_trigger_bars
        )
        or value["boundary_span_seconds"] != expected_span
        or value["boundary_slack_seconds"] != expected_slack
        or value.get("initial_raw_relation") not in _MT5_RAW_RELATIONS
        or value.get("confirmation_raw_relation") not in _MT5_RAW_RELATIONS
        or value.get("raw_relation") not in _MT5_RAW_RELATIONS
        or value["admitted_count"] > value["raw_count"]
        or not _mt5_extent_valid(value.get("raw_extent"), count=value["raw_count"])
        or not _mt5_extent_valid(
            value.get("admitted_extent"), count=value["admitted_count"]
        )
    ):
        return [code]
    admitted_extent = value.get("admitted_extent")
    if admitted_extent is not None and (
        admitted_extent["first_epoch_second"] < start_epoch
        or admitted_extent["last_epoch_second"] >= end_epoch
    ):
        return [code]
    if value.get("boundary_complete") is not _mt5_boundary_complete_from_extent(
        admitted_extent,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        timeframe_seconds=timeframe_seconds,
    ):
        return [code]

    ledger = value.get("call_ledger")
    if not isinstance(ledger, list):
        return [code]
    expected_calls: tuple[str, ...]
    if copy_attempts == 1 and trigger_attempts == 0:
        expected_calls = ("copy_rates_range_initial",)
    elif copy_attempts == 1 and trigger_attempts == 1:
        expected_calls = ("copy_rates_range_initial", "copy_rates_from_warm")
    elif copy_attempts == 2 and trigger_attempts == 1:
        expected_calls = (
            "copy_rates_range_initial",
            "copy_rates_from_warm",
            "copy_rates_range_retry",
        )
    else:
        return [code]
    if len(ledger) != len(expected_calls):
        return [code]
    for sequence, (entry, call_type) in enumerate(zip(ledger, expected_calls), start=1):
        if (
            not isinstance(entry, Mapping)
            or set(entry) != _MT5_V4_LEDGER_FIELDS
            or entry.get("sequence") != sequence
            or entry.get("call_type") != call_type
            or type(entry.get("elapsed_ms")) not in {int, float}
            or isinstance(entry.get("elapsed_ms"), bool)
            or not math.isfinite(float(entry.get("elapsed_ms")))
            or float(entry.get("elapsed_ms")) < 0.0
            or not _mt5_last_error_valid(entry.get("last_error"))
            or type(entry.get("hard_failure")) is not bool
            or entry.get("raw_relation") not in _MT5_RAW_RELATIONS
            or type(entry.get("raw_count")) is not int
            or entry.get("raw_count") < 0
            or type(entry.get("admitted_count")) is not int
            or not 0 <= entry.get("admitted_count") <= entry.get("raw_count")
            or not _mt5_extent_valid(
                entry.get("raw_extent"), count=entry.get("raw_count")
            )
            or not _mt5_extent_valid(
                entry.get("admitted_extent"), count=entry.get("admitted_count")
            )
            or (
                entry.get("exception") is not None
                and not isinstance(entry.get("exception"), Mapping)
            )
            or (
                entry.get("parse_error") is not None
                and not isinstance(entry.get("parse_error"), str)
            )
            or not _mt5_analysis_geometry_valid(
                entry,
                start_epoch=start_epoch,
                end_epoch=end_epoch,
            )
        ):
            return [code]
        last_error = entry["last_error"]
        expected_entry_hard = bool(
            entry.get("exception") is not None
            or last_error.get("capture_error") is not None
            or (
                type(last_error.get("code")) is int
                and last_error.get("code") < 0
                and last_error.get("code") != -4
            )
        )
        if entry.get("hard_failure") is not expected_entry_hard:
            return [code]
    exact_entries = [
        entry for entry in ledger if entry["call_type"].startswith("copy_rates_range")
    ]
    best_entry: Mapping[str, Any] | None = None
    for entry in exact_entries:
        if entry["admitted_count"] <= 0:
            continue
        if best_entry is None:
            best_entry = entry
            continue
        best_extent = best_entry["admitted_extent"]
        candidate_extent = entry["admitted_extent"]
        best_score = (
            best_extent["last_epoch_second"] - best_extent["first_epoch_second"],
            best_entry["admitted_count"],
        )
        candidate_score = (
            candidate_extent["last_epoch_second"]
            - candidate_extent["first_epoch_second"],
            entry["admitted_count"],
        )
        if candidate_score > best_score:
            best_entry = entry
    if best_entry is not None:
        selected_entry = best_entry
    else:
        selected_entry = next(
            (
                entry
                for entry in reversed(exact_entries)
                if entry.get("raw_relation") is not None
            ),
            exact_entries[-1],
        )
    selected_projection = {
        field: value.get(field)
        for field in (
            "raw_relation",
            "raw_count",
            "raw_extent",
            "admitted_count",
            "admitted_extent",
        )
    }
    postprocess_call = value.get("hard_failure_call")
    postprocess_failure = bool(
        best_entry is not None
        and value.get("hard_failure") is True
        and value.get("sync_status") == "invalid_filtered_response"
        and postprocess_call in {"dataframe_conversion", "dataframe_half_open_guard"}
    )
    expected_selected_projection = {
        field: selected_entry.get(field) for field in selected_projection
    }
    if postprocess_call == "dataframe_half_open_guard":
        expected_selected_projection["admitted_count"] = 0
        expected_selected_projection["admitted_extent"] = None
    if not _same(
        selected_projection,
        expected_selected_projection,
    ):
        return [code]
    retry_entry = exact_entries[-1] if len(exact_entries) == 2 else None
    if (
        value.get("initial_raw_relation") != exact_entries[0].get("raw_relation")
        or value.get("confirmation_raw_relation")
        != (None if retry_entry is None else retry_entry.get("raw_relation"))
        or value.get("last_error") != ledger[-1].get("last_error")
        or not _same(
            value.get("best_extent"),
            (
                None
                if best_entry is None or postprocess_call == "dataframe_half_open_guard"
                else best_entry.get("admitted_extent")
            ),
        )
    ):
        return [code]
    warm_entry = next(
        (entry for entry in ledger if entry["call_type"] == "copy_rates_from_warm"),
        None,
    )
    expected_trigger_errors: list[dict[str, Any]] = []
    if warm_entry is not None:
        if warm_entry.get("exception") is not None:
            expected_trigger_errors = [dict(warm_entry["exception"])]
        elif warm_entry.get("hard_failure") is True:
            last_error = warm_entry["last_error"]
            expected_trigger_errors = [
                {
                    "type": "MT5LastError",
                    "message": (
                        f"{last_error.get('code')}: {last_error.get('message', '')}"
                    )[:512],
                }
            ]
    if (
        value.get("history_download_trigger_nonempty")
        != int(warm_entry is not None and warm_entry.get("raw_count", 0) > 0)
        or not _same(
            value.get("history_download_trigger_errors"), expected_trigger_errors
        )
        or value.get("terminal_confirmation_consumed")
        is not (
            value.get("initial_raw_relation") == "RIGHT_DISJOINT_PREFIX"
            and trigger_attempts == 1
        )
    ):
        return [code]
    initial_entry = exact_entries[0]
    expected_should_retry = bool(
        initial_entry.get("hard_failure") is False
        and initial_entry.get("parse_error") is None
        and not _mt5_boundary_complete_from_extent(
            initial_entry.get("admitted_extent"),
            start_epoch=start_epoch,
            end_epoch=end_epoch,
            timeframe_seconds=timeframe_seconds,
        )
    )
    if (trigger_attempts == 1) is not expected_should_retry:
        return [code]
    if warm_entry is not None:
        warm_stops = bool(
            warm_entry.get("hard_failure") is True
            or warm_entry.get("parse_error") is not None
        )
        if (retry_entry is None) is not warm_stops:
            return [code]
    hard_call_entry = next(
        (
            entry
            for entry in ledger
            if entry.get("hard_failure") is True
            or (
                entry.get("call_type") != "copy_rates_range_initial"
                and entry.get("parse_error") is not None
            )
        ),
        None,
    )
    expected_hard_call = (
        None if hard_call_entry is None else hard_call_entry["call_type"]
    )
    if hard_call_entry is None and postprocess_failure:
        expected_hard_call = str(postprocess_call)
    if (
        value.get("hard_failure")
        is not (hard_call_entry is not None or postprocess_failure)
        or value.get("hard_failure_call") != expected_hard_call
    ):
        return [code]

    exact_relations = [entry.get("raw_relation") for entry in exact_entries]
    initial_right = initial_entry.get("raw_relation") == "RIGHT_DISJOINT_PREFIX"
    if postprocess_failure:
        expected_sync_status = "invalid_filtered_response"
    elif hard_call_entry is not None:
        expected_sync_status = "hard_mt5_failure"
    elif best_entry is None and initial_right:
        expected_sync_status = "terminal_prefix_unsynchronized"
    elif best_entry is None and "LEFT_DISJOINT_SUFFIX" in exact_relations:
        expected_sync_status = "terminal_suffix_unsynchronized"
    elif best_entry is None and "RIGHT_DISJOINT_PREFIX" in exact_relations:
        expected_sync_status = "terminal_prefix_unsynchronized"
    elif best_entry is None and "MIXED_INVALID" in exact_relations:
        expected_sync_status = "mixed_invalid_no_admissible_data"
    elif best_entry is None:
        expected_sync_status = "empty_unsynchronized"
    elif _mt5_boundary_complete_from_extent(
        selected_entry.get("admitted_extent"),
        start_epoch=start_epoch,
        end_epoch=end_epoch,
        timeframe_seconds=timeframe_seconds,
    ):
        expected_sync_status = "boundary_complete"
    elif selected_entry.get("raw_relation") == "MIXED_INVALID":
        expected_sync_status = "mixed_invalid_filtered"
    else:
        expected_sync_status = "boundary_incomplete"
    sync_status = value.get("sync_status")
    if sync_status != expected_sync_status:
        return [code]
    if category == "data":
        if (
            value["admitted_count"] <= 0
            or value.get("hard_failure") is not False
            or sync_status
            not in {
                "boundary_complete",
                "boundary_incomplete",
                "mixed_invalid_filtered",
            }
        ):
            return [code]
    elif category == "empty":
        if (
            value["admitted_count"] != 0
            or value.get("hard_failure") is not False
            or sync_status not in {"empty", "empty_unsynchronized"}
        ):
            return [code]
    elif category == "terminal_prefix":
        if sync_status != "terminal_prefix_unsynchronized":
            return [code]
    elif category == "failed":
        if value.get("hard_failure") is not True or sync_status not in {
            "hard_mt5_failure",
            "invalid_filtered_response",
        }:
            return [code]
    else:
        return [code]
    return []


def _terminal_prefix_diagnostics_errors(value: Any, *, copy_attempts: int) -> list[str]:
    """Bind a terminal-prefix stop to the released V4 confirmation sequence."""

    code = "V5_TERMINAL_PREFIX_DIAGNOSTICS_INVALID"
    if not isinstance(value, Mapping) or copy_attempts != 2:
        return [code]
    initial_relation = value.get("initial_raw_relation")
    ledger = value.get("call_ledger")
    if (
        value.get("range_contract_version") != "mt5-half-open-range-v4"
        or value.get("call_ledger_schema") != "mt5.range-call-ledger.v1"
        or value.get("sync_retry") is not True
        or value.get("sync_status") != "terminal_prefix_unsynchronized"
        or value.get("raw_relation") != "RIGHT_DISJOINT_PREFIX"
        or value.get("confirmation_raw_relation") != "RIGHT_DISJOINT_PREFIX"
        or value.get("copy_rates_range_attempts") != 2
        or value.get("copy_rates_range_max_attempts") != 2
        or value.get("history_download_trigger_attempts") != 1
        or value.get("admitted_count") != 0
        or value.get("admitted_extent") is not None
        or value.get("hard_failure") is not False
        or value.get("hard_failure_call") is not None
        or initial_relation not in {None, "RIGHT_DISJOINT_PREFIX"}
        or value.get("terminal_confirmation_consumed")
        is not (initial_relation == "RIGHT_DISJOINT_PREFIX")
        or not isinstance(ledger, list)
        or len(ledger) != 3
    ):
        return [code]
    expected_calls = (
        "copy_rates_range_initial",
        "copy_rates_from_warm",
        "copy_rates_range_retry",
    )
    for sequence, (entry, call_type) in enumerate(zip(ledger, expected_calls), start=1):
        if not isinstance(entry, Mapping) or (
            entry.get("sequence") != sequence
            or entry.get("call_type") != call_type
            or entry.get("hard_failure") is not False
            or entry.get("exception") is not None
            or entry.get("parse_error") is not None
        ):
            return [code]
    retry = ledger[-1]
    if (
        ledger[0].get("raw_relation") != initial_relation
        or retry.get("raw_relation") != value.get("confirmation_raw_relation")
        or retry.get("raw_relation") != "RIGHT_DISJOINT_PREFIX"
        or retry.get("admitted_count") != 0
        or retry.get("admitted_extent") is not None
    ):
        return [code]
    return []


def _authenticated_available_history_boundary(
    value: Any,
    *,
    terminal_page: Mapping[str, Any],
    first_data_page: Mapping[str, Any],
) -> tuple[datetime | None, list[str]]:
    """Re-derive an available-suffix boundary from MT5 and page evidence."""

    code = "V5_AVAILABLE_HISTORY_BOUNDARY_INVALID"
    proof_keys = {
        "schema_version",
        "source_state",
        "boundary_kind",
        "boundary_open_utc",
        "terminal_page_start_utc",
        "terminal_page_end_exclusive_utc",
        "first_data_page_start_utc",
        "first_data_page_end_exclusive_utc",
        "confirmation_method",
        "market_listing_date_asserted",
        "statistical_sufficiency_assessed",
    }
    if not isinstance(value, Mapping) or set(value) != proof_keys:
        return None, [code]
    diagnostics = terminal_page.get("connector_diagnostics")
    next_diagnostics = first_data_page.get("connector_diagnostics")
    if not isinstance(diagnostics, Mapping) or not isinstance(
        next_diagnostics, Mapping
    ):
        return None, [code]
    ledger = diagnostics.get("call_ledger")
    if not isinstance(ledger, list) or len(ledger) != 3:
        return None, [code]
    boundary_epoch: int | None = None
    for entry in ledger:
        if not isinstance(entry, Mapping):
            return None, [code]
        extent = entry.get("raw_extent")
        if (
            entry.get("raw_relation") != "RIGHT_DISJOINT_PREFIX"
            or entry.get("raw_count") != 1
            or entry.get("admitted_count") != 0
            or entry.get("admitted_extent") is not None
            or not isinstance(extent, Mapping)
            or set(extent) != {"rows", "first_epoch_second", "last_epoch_second"}
            or extent.get("rows") != 1
            or type(extent.get("first_epoch_second")) is not int
            or extent.get("last_epoch_second") != extent.get("first_epoch_second")
        ):
            return None, [code]
        observed_epoch = int(extent["first_epoch_second"])
        if boundary_epoch is None:
            boundary_epoch = observed_epoch
        elif observed_epoch != boundary_epoch:
            return None, [code]
    top_extent = diagnostics.get("raw_extent")
    next_extent = next_diagnostics.get("admitted_extent")
    if (
        boundary_epoch is None
        or diagnostics.get("initial_raw_relation") != "RIGHT_DISJOINT_PREFIX"
        or diagnostics.get("confirmation_raw_relation") != "RIGHT_DISJOINT_PREFIX"
        or diagnostics.get("raw_relation") != "RIGHT_DISJOINT_PREFIX"
        or diagnostics.get("raw_count") != 1
        or not isinstance(top_extent, Mapping)
        or top_extent.get("rows") != 1
        or top_extent.get("first_epoch_second") != boundary_epoch
        or top_extent.get("last_epoch_second") != boundary_epoch
        or type(next_diagnostics.get("admitted_count")) is not int
        or next_diagnostics.get("admitted_count", 0) <= 0
        or not isinstance(next_extent, Mapping)
        or next_extent.get("first_epoch_second") != boundary_epoch
    ):
        return None, [code]
    try:
        terminal_start, terminal_end = _interval_tuple(
            terminal_page, "V5_AVAILABLE_HISTORY_BOUNDARY_INVALID"
        )
        first_data_start, first_data_end = _interval_tuple(
            first_data_page, "V5_AVAILABLE_HISTORY_BOUNDARY_INVALID"
        )
        boundary = datetime.fromtimestamp(boundary_epoch, tz=timezone.utc)
    except (NativeRequestContractError, OSError, OverflowError, ValueError):
        return None, [code]
    if (
        terminal_end != first_data_start
        or not terminal_start < terminal_end <= boundary < first_data_end
    ):
        return None, [code]
    expected = {
        "schema_version": AVAILABLE_HISTORY_BOUNDARY_SCHEMA_VERSION,
        "source_state": "broker_native_available_suffix",
        "boundary_kind": "first_authenticated_available_bar_open",
        "boundary_open_utc": _iso(boundary),
        "terminal_page_start_utc": _iso(terminal_start),
        "terminal_page_end_exclusive_utc": _iso(terminal_end),
        "first_data_page_start_utc": _iso(first_data_start),
        "first_data_page_end_exclusive_utc": _iso(first_data_end),
        "confirmation_method": AVAILABLE_HISTORY_BOUNDARY_CONFIRMATION_METHOD,
        "market_listing_date_asserted": False,
        "statistical_sufficiency_assessed": False,
    }
    if not _same(value, expected):
        return None, [code]
    return boundary, []


def _mt5_v4_preflight_diagnostics_errors(
    value: Any,
    *,
    interval: tuple[datetime, datetime],
    expected_symbol: str,
    timeframe: str,
) -> list[str]:
    """Validate the released connector's exact zero-copy source preflight receipt."""

    code = "V5_PAGE_MT5_V4_PREFLIGHT_DIAGNOSTICS_INVALID"
    if not isinstance(value, Mapping) or set(value) != _MT5_V4_DIAGNOSTIC_FIELDS:
        return [code]
    start, end = interval
    expected = {
        "range_contract_version": "mt5-half-open-range-v4",
        "symbol": expected_symbol,
        "broker_symbol": None,
        "timeframe": timeframe,
        "sync_retry": True,
        "request_start_epoch_second": int(start.timestamp()),
        "request_end_epoch_second_exclusive": int(end.timestamp()),
        "copy_rates_range_attempts": 0,
        "copy_rates_range_max_attempts": 0,
        "history_download_trigger_attempts": 0,
        "history_download_trigger_nonempty": 0,
        "history_download_trigger_max_bars": 0,
        "history_download_trigger_errors": [],
        "terminal_confirmation_consumed": False,
        "call_ledger_schema": "mt5.range-call-ledger.v1",
        "call_ledger": [],
        "hard_failure": False,
        "hard_failure_call": None,
        "last_error": None,
        "initial_raw_relation": None,
        "confirmation_raw_relation": None,
        "raw_relation": None,
        "raw_count": 0,
        "raw_extent": None,
        "admitted_count": 0,
        "admitted_extent": None,
        "boundary_span_seconds": 0,
        "boundary_slack_seconds": 0,
        "boundary_complete": False,
        "best_extent": None,
    }
    if value.get("sync_status") not in {
        "connection_unavailable",
        "symbol_unavailable",
    }:
        return [code]
    for field, expected_value in expected.items():
        observed = value.get(field)
        if not _same(observed, expected_value) or type(observed) is not type(
            expected_value
        ):
            return [code]
    return []


@_total_function("V5_MT5_PREFLIGHT_DIAGNOSTICS_VALIDATION_ERROR")
def validate_mt5_v4_preflight_diagnostics(
    diagnostics: Any,
    *,
    start_utc: Any,
    end_exclusive_utc: Any,
    symbol: Any,
    timeframe: str,
) -> dict[str, Any]:
    """Public exact gate for a released zero-copy MT5 preflight failure."""

    canonical_json_bytes(diagnostics)
    start = _aware_utc(start_utc, "V5_PAGE_INTERVAL_INVALID")
    end = _aware_utc(end_exclusive_utc, "V5_PAGE_INTERVAL_INVALID")
    if start >= end:
        raise NativeRequestContractError(("V5_PAGE_INTERVAL_INVALID",))
    canonical = canonical_symbol(symbol)
    tf = _canonical_timeframe(timeframe)
    errors = _mt5_v4_preflight_diagnostics_errors(
        diagnostics,
        interval=(start, end),
        expected_symbol=canonical,
        timeframe=tf,
    )
    if errors:
        raise NativeRequestContractError(tuple(dict.fromkeys(errors)))
    return deepcopy(dict(diagnostics))


@_total_function("V5_MT5_PAGE_DIAGNOSTICS_VALIDATION_ERROR")
def validate_mt5_v4_page_diagnostics(
    diagnostics: Any,
    *,
    start_utc: Any,
    end_exclusive_utc: Any,
    symbol: Any,
    broker_symbol: Any,
    timeframe: str,
    copy_attempts: int,
    category: str,
) -> dict[str, Any]:
    """Public writer/reader parity gate for one MT5 V4 page receipt."""

    canonical_json_bytes(diagnostics)
    start = _aware_utc(start_utc, "V5_PAGE_INTERVAL_INVALID")
    end = _aware_utc(end_exclusive_utc, "V5_PAGE_INTERVAL_INVALID")
    if start >= end:
        raise NativeRequestContractError(("V5_PAGE_INTERVAL_INVALID",))
    canonical = canonical_symbol(symbol)
    if not isinstance(broker_symbol, str) or not broker_symbol:
        raise NativeRequestContractError(("V5_BROKER_SYMBOL_INVALID",))
    tf = _canonical_timeframe(timeframe)
    attempts = _strict_positive_int(
        copy_attempts, "V5_PAGE_COPY_ATTEMPTS_INVALID", maximum=2
    )
    errors = _mt5_v4_diagnostics_errors(
        diagnostics,
        interval=(start, end),
        expected_symbol=canonical,
        expected_broker_symbol=broker_symbol,
        timeframe=tf,
        copy_attempts=attempts,
        category=category,
    )
    if category == "terminal_prefix":
        errors.extend(
            _terminal_prefix_diagnostics_errors(
                diagnostics,
                copy_attempts=attempts,
            )
        )
    if errors:
        raise NativeRequestContractError(tuple(dict.fromkeys(errors)))
    return deepcopy(dict(diagnostics))


@_total_function("V5_AVAILABLE_HISTORY_BOUNDARY_VALIDATION_ERROR")
def validate_authenticated_available_history_boundary(
    pagination: Any,
    artifact_observation: Any,
    *,
    symbol: Any,
    broker_symbol: Any,
    timeframe: str,
    leaf_request: Mapping[str, Any],
    common_request: Mapping[str, Any],
    leaf_source_environment: Any,
    manifest_source_environment: Any,
    expected_retries: Any,
) -> dict[str, Any]:
    """Return the canonical derived available-suffix boundary proof.

    This is a source-integrity verifier only. It neither asserts a market
    listing date nor grants statistical, optimization, or trading authority.
    The externally bound acquisition geometry is required so this public gate
    cannot authenticate a proof from only the two pages that carry it while
    ignoring a malformed remainder of the page ledger.
    """

    if not isinstance(pagination, Mapping):
        raise NativeRequestContractError(("V5_PAGINATION_INVALID",))
    pagination_json = canonical_json_bytes(pagination)
    try:
        owned_pagination = json.loads(pagination_json.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NativeRequestContractError(("V5_PAGINATION_INVALID",)) from exc
    if type(owned_pagination) is not dict:
        raise NativeRequestContractError(("V5_PAGINATION_INVALID",))
    pagination = owned_pagination
    observation = _validate_artifact_observation(artifact_observation)
    expected_symbol = canonical_symbol(symbol)
    tf = _canonical_timeframe(timeframe)
    if not isinstance(broker_symbol, str) or not broker_symbol:
        raise NativeRequestContractError(("V5_BROKER_SYMBOL_INVALID",))
    retries = _strict_positive_int(
        expected_retries,
        "V5_RETRIES_INVALID",
        maximum=MAX_PAGE_ATTEMPTS,
    )
    common = validate_v5_common_request(common_request)
    leaf = validate_v5_leaf_request(leaf_request, common, tf)
    source_identity = validate_source_identity_match(
        leaf_source_environment,
        manifest_source_environment,
    )
    terminal_max_bars = source_identity["terminal"]["max_bars"]
    timeframe_seconds = TIMEFRAME_SECONDS[tf]
    configured_slots = max(
        1,
        int(_DEFAULT_PAGE_DAYS[tf] * 86_400 // timeframe_seconds),
    )
    safe_slots = max(1, int(terminal_max_bars * 0.90))
    page_span = min(configured_slots, safe_slots) * timeframe_seconds
    if tf in {"H4", "D1"} and page_span < 14 * 86_400:
        raise NativeRequestContractError(("V5_BROKER_SESSION_PAGE_SPAN_TOO_SHORT",))
    tolerance = float(common["boundary_tolerance_days"] * 86_400)
    source_interval = leaf["source_request_interval"]
    strategy_interval = leaf["strategy_evidence_interval"]
    evidence = _pagination_evidence(
        pagination,
        source_interval,
        strategy_interval,
        tf,
        expected_symbol=expected_symbol,
        expected_broker_symbol=broker_symbol,
        expected_source_environment=source_identity,
        expected_retries=retries,
        expected_page_span_seconds=page_span,
        boundary_tolerance_seconds=tolerance,
    )
    errors = [
        *evidence["errors"],
        *_artifact_page_ledger_errors(
            observation,
            pagination,
            tf,
            boundary_tolerance_seconds=tolerance,
            source_quality_omission_counts=evidence["source_quality_omission_counts"],
        ),
    ]
    if errors:
        raise NativeRequestContractError(tuple(dict.fromkeys(errors)))
    boundary = evidence["authenticated_available_history_boundary_utc"]
    if not isinstance(boundary, datetime):
        raise NativeRequestContractError(
            ("V5_AVAILABLE_HISTORY_BOUNDARY_EVIDENCE_INVALID",)
        )
    proof = pagination.get("available_history_boundary")
    if not isinstance(proof, Mapping):
        raise NativeRequestContractError(("V5_AVAILABLE_HISTORY_BOUNDARY_INVALID",))
    if proof.get("boundary_open_utc") != _iso(boundary):
        raise NativeRequestContractError(("V5_AVAILABLE_HISTORY_BOUNDARY_INVALID",))
    if observation.first_open_utc != _iso(boundary):
        raise NativeRequestContractError(
            ("V5_AVAILABLE_HISTORY_BOUNDARY_ARTIFACT_MISMATCH",)
        )
    return deepcopy(dict(proof))


def _pagination_evidence(
    pagination: Any,
    source_interval: Mapping[str, Any],
    strategy_interval: Mapping[str, Any],
    timeframe: str,
    *,
    expected_symbol: str,
    expected_broker_symbol: str,
    expected_source_environment: Mapping[str, Any],
    expected_retries: int,
    expected_page_span_seconds: int,
    boundary_tolerance_seconds: float,
    evidence_mode: str = "publishable",
) -> dict[str, Any]:
    """Recompute V3 newest-first bounded geometry and typed stop evidence."""

    empty_result: dict[str, Any] = {
        "boundary_missing": False,
        "technical_gap": False,
        "terminal_prefix_unresolved": False,
        "technical_prefix_unresolved": False,
        "authenticated_available_history_boundary_utc": None,
        "internal_empty_page_count": 0,
        "truncated_page_count": 0,
        "internal_truncated_page_count": 0,
        "boundary_truncated_page_count": 0,
        "failed_page_count": 0,
        "terminal_prefix_unresolved_page_count": 0,
        "source_quality_omission_counts": (),
        "errors": [],
    }
    if not isinstance(pagination, Mapping):
        return {**empty_result, "errors": ["V5_PAGINATION_INVALID"]}
    errors: list[str] = []
    if evidence_mode not in {"publishable", "failed"}:
        return {**empty_result, "errors": ["V5_PAGINATION_MODE_INVALID"]}
    if pagination.get("contract_version") != PAGINATION_CONTRACT_VERSION:
        errors.append("V5_PAGINATION_CONTRACT_VERSION_INVALID")
    if pagination.get("request_order") != "newest_to_oldest":
        errors.append("V5_PAGINATION_REQUEST_ORDER_INVALID")
    try:
        retries = _strict_positive_int(
            expected_retries, "V5_RETRIES_INVALID", maximum=MAX_PAGE_ATTEMPTS
        )
        if retries != 1:
            errors.append("V5_ACQUISITION_RETRY_OWNER_INVALID")
        if pagination.get("retries_per_page") != 1:
            errors.append("V5_PAGINATION_RETRY_BUDGET_MISMATCH")
        page_span = _strict_positive_int(
            pagination.get("page_span_seconds"),
            "V5_PAGE_SPAN_INVALID",
            maximum=MAX_PAGE_SPAN_SECONDS,
        )
        if page_span != expected_page_span_seconds:
            errors.append("V5_PAGINATION_PAGE_SPAN_MISMATCH")
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
        page_span = 1
    try:
        source_start, source_end = _interval_tuple(
            source_interval, "V5_SOURCE_INTERVAL_INVALID"
        )
    except NativeRequestContractError as exc:
        return {**empty_result, "errors": list(exc.errors)}
    try:
        strategy_start, _strategy_end = _interval_tuple(
            strategy_interval, "V5_STRATEGY_INTERVAL_INVALID"
        )
    except NativeRequestContractError as exc:
        return {**empty_result, "errors": list(exc.errors)}

    all_intervals: list[tuple[datetime, datetime]] = []
    cursor_end = source_end
    while cursor_end > source_start and len(all_intervals) <= MAX_PAGES_PER_CELL:
        page_start = max(source_start, cursor_end - timedelta(seconds=page_span))
        all_intervals.append((page_start, cursor_end))
        cursor_end = page_start
    all_intervals.reverse()
    planned = len(all_intervals)
    if cursor_end != source_start or planned > MAX_PAGES_PER_CELL:
        errors.append("V5_PAGE_LEDGER_CARDINALITY_CAP_EXCEEDED")
    if pagination.get("pages_planned") != planned:
        errors.append("V5_PAGE_LEDGER_PLANNED_COUNT_MISMATCH")

    attempts = pagination.get("page_attempts")
    if not isinstance(attempts, list) or not attempts or len(attempts) > planned:
        return {
            **empty_result,
            "errors": [*errors, "V5_PAGE_ATTEMPT_LEDGER_INVALID"],
        }
    stop_reason = pagination.get("stop_reason")
    unrequested_prefix = pagination.get("unrequested_prefix")
    terminal_prefix_stopped = stop_reason == "terminal_prefix_unresolved"
    technical_prefix_stopped = stop_reason == "technical_prefix_unresolved"
    prefix_stopped = terminal_prefix_stopped or technical_prefix_stopped
    if stop_reason not in {
        None,
        "terminal_prefix_unresolved",
        "technical_prefix_unresolved",
    }:
        errors.append("V5_PAGINATION_STOP_REASON_INVALID")
    expected_intervals = all_intervals[-len(attempts) :]
    if not prefix_stopped and len(attempts) != planned:
        errors.append("V5_PAGE_LEDGER_CARDINALITY_MISMATCH")
    if prefix_stopped:
        expected_prefix_end = expected_intervals[0][0]
        if expected_prefix_end == source_start:
            if unrequested_prefix is not None:
                errors.append("V5_UNREQUESTED_PREFIX_INVALID")
        else:
            try:
                declared_prefix = _interval_tuple(
                    unrequested_prefix, "V5_UNREQUESTED_PREFIX_INVALID"
                )
                if declared_prefix != (source_start, expected_prefix_end):
                    errors.append("V5_UNREQUESTED_PREFIX_MISMATCH")
                if not isinstance(unrequested_prefix, Mapping) or (
                    unrequested_prefix.get("reason") != stop_reason
                ):
                    errors.append("V5_UNREQUESTED_PREFIX_REASON_INVALID")
            except NativeRequestContractError as exc:
                errors.extend(exc.errors)
    elif unrequested_prefix is not None:
        errors.append("V5_UNREQUESTED_PREFIX_WITHOUT_STOP")

    page_records: dict[
        tuple[datetime, datetime], tuple[int, int, int, Mapping[str, Any]]
    ] = {}
    attempts_total = 0
    copy_attempts_total = 0
    for index, (page, expected_interval) in enumerate(
        zip(attempts, expected_intervals)
    ):
        if not isinstance(page, Mapping):
            errors.append(f"V5_PAGE_INTERVAL_INVALID:{index}")
            continue
        try:
            interval = _interval_tuple(page, f"V5_PAGE_INTERVAL_INVALID:{index}")
            outer_attempts = _strict_positive_int(
                page.get("attempts"),
                f"V5_PAGE_ATTEMPTS_INVALID:{index}",
                maximum=1,
            )
            copy_attempts = _strict_positive_int(
                page.get("copy_rates_range_attempts"),
                f"V5_PAGE_COPY_ATTEMPTS_INVALID:{index}",
                maximum=2,
            )
        except NativeRequestContractError as exc:
            errors.extend(exc.errors)
            continue
        if interval != expected_interval:
            errors.append(f"V5_PAGE_LEDGER_GEOMETRY_MISMATCH:{index}")
        if page.get("request_sequence") != len(attempts) - index:
            errors.append(f"V5_PAGE_REQUEST_SEQUENCE_INVALID:{index}")
        if interval in page_records:
            errors.append("V5_PAGE_LEDGER_DUPLICATE")
        page_records[interval] = (index, outer_attempts, copy_attempts, page)
        attempts_total += outer_attempts
        copy_attempts_total += copy_attempts
        errors.extend(
            _resource_snapshot_errors(
                page.get("terminal_cache_resource_snapshot"),
                code=f"V5_PAGE_RESOURCE_BOUND_INVALID:{index}",
            )
        )
    if pagination.get("pages_requested") != len(attempts):
        errors.append("V5_PAGINATION_PAGE_COUNT_MISMATCH")
    if pagination.get("attempts_total") != attempts_total:
        errors.append("V5_PAGINATION_ATTEMPTS_TOTAL_MISMATCH")
    if pagination.get("copy_rates_range_attempts_total") != copy_attempts_total:
        errors.append("V5_PAGINATION_COPY_ATTEMPTS_TOTAL_MISMATCH")
    errors.extend(
        _resource_snapshot_errors(
            pagination.get("initial_terminal_cache_resource_snapshot"),
            code="V5_INITIAL_RESOURCE_BOUND_INVALID",
        )
    )

    diagnostic_names = (
        "empty_pages",
        "failed_pages",
        "truncated_pages",
        "terminal_prefix_unresolved_pages",
    )
    diagnostic_sets: dict[str, set[tuple[datetime, datetime]]] = {}
    diagnostic_values: dict[str, list[Mapping[str, Any]]] = {}
    for name in diagnostic_names:
        values = pagination.get(name)
        if not isinstance(values, list) or len(values) > len(attempts):
            errors.append(f"V5_PAGINATION_DIAGNOSTIC_INVALID:{name}")
            values = []
        seen: set[tuple[datetime, datetime]] = set()
        typed: list[Mapping[str, Any]] = []
        for position, value in enumerate(values):
            if not isinstance(value, Mapping):
                errors.append(f"V5_PAGINATION_DIAGNOSTIC_INVALID:{name}:{position}")
                continue
            try:
                interval = _interval_tuple(
                    value, f"V5_PAGINATION_DIAGNOSTIC_INVALID:{name}:{position}"
                )
            except NativeRequestContractError as exc:
                errors.extend(exc.errors)
                continue
            record = page_records.get(interval)
            if record is None or interval in seen:
                errors.append(f"V5_PAGINATION_DIAGNOSTIC_UNBOUND:{name}:{position}")
                continue
            _index, outer_attempts, copy_attempts, bound_page = record
            if (
                value.get("attempts") != outer_attempts
                or value.get("copy_rates_range_attempts") != copy_attempts
            ):
                errors.append(
                    f"V5_PAGINATION_DIAGNOSTIC_ATTEMPT_MISMATCH:{name}:{position}"
                )
                continue
            if any(
                key not in value or not _same(value.get(key), bound_value)
                for key, bound_value in bound_page.items()
            ):
                errors.append(
                    f"V5_PAGINATION_DIAGNOSTIC_BASE_MISMATCH:{name}:{position}"
                )
                continue
            allowed_extra = (
                {
                    "actual_first_open_utc",
                    "actual_last_open_utc",
                    "start_delay_seconds",
                    "end_lag_seconds",
                }
                if name == "truncated_pages"
                else set()
            )
            if set(value) - set(bound_page) != allowed_extra:
                errors.append(
                    f"V5_PAGINATION_DIAGNOSTIC_SCHEMA_MISMATCH:{name}:{position}"
                )
                continue
            seen.add(interval)
            typed.append(value)
        diagnostic_sets[name] = seen
        diagnostic_values[name] = typed
    categories = (
        diagnostic_sets["empty_pages"],
        diagnostic_sets["failed_pages"],
        diagnostic_sets["terminal_prefix_unresolved_pages"],
    )
    if any(categories[i] & categories[j] for i in range(3) for j in range(i + 1, 3)):
        errors.append("V5_PAGINATION_DIAGNOSTIC_CONFLICT")
    if diagnostic_sets["truncated_pages"] & set().union(*categories):
        errors.append("V5_PAGINATION_DIAGNOSTIC_CONFLICT")
    for interval, (_index, _outer, copy_attempts, page) in page_records.items():
        if interval in diagnostic_sets["terminal_prefix_unresolved_pages"]:
            category = "terminal_prefix"
        elif interval in diagnostic_sets["failed_pages"]:
            category = "failed"
        elif interval in diagnostic_sets["empty_pages"]:
            category = "empty"
        else:
            category = "data"
        errors.extend(
            _mt5_v4_diagnostics_errors(
                page.get("connector_diagnostics"),
                interval=interval,
                expected_symbol=expected_symbol,
                expected_broker_symbol=expected_broker_symbol,
                timeframe=timeframe,
                copy_attempts=copy_attempts,
                category=category,
            )
        )
    source_quality_omission_counts = [0] * len(attempts)
    seen_omission_receipts: set[str] = set()
    seen_omission_times: set[str] = set()
    total_omissions = 0
    for interval, (index, _outer, _copy, page) in page_records.items():
        raw_omissions = page.get("source_quality_omissions")
        if raw_omissions is None:
            continue
        if (
            not isinstance(raw_omissions, list)
            or not raw_omissions
            or len(raw_omissions) > MAX_SOURCE_QUALITY_OMISSIONS_PER_CELL
        ):
            errors.append(f"V5_SOURCE_QUALITY_OMISSION_LEDGER_INVALID:{index}")
            continue
        connector_diagnostics = page.get("connector_diagnostics")
        admitted_extent = (
            connector_diagnostics.get("admitted_extent")
            if isinstance(connector_diagnostics, Mapping)
            else None
        )
        admitted_first = (
            datetime.fromtimestamp(
                admitted_extent.get("first_epoch_second", 0), tz=timezone.utc
            )
            if isinstance(admitted_extent, Mapping)
            and type(admitted_extent.get("first_epoch_second")) is int
            else None
        )
        admitted_last = (
            datetime.fromtimestamp(
                admitted_extent.get("last_epoch_second", 0), tz=timezone.utc
            )
            if isinstance(admitted_extent, Mapping)
            and type(admitted_extent.get("last_epoch_second")) is int
            else None
        )
        for position, omission in enumerate(raw_omissions):
            try:
                verified = validate_v5_source_quality_omission(
                    omission,
                    source_environment=expected_source_environment,
                    symbol=expected_symbol,
                    broker_symbol=expected_broker_symbol,
                    timeframe=timeframe,
                )
                opened = _aware_utc(
                    verified["bar_open_utc"],
                    "V5_SOURCE_QUALITY_OMISSION_TIME_INVALID",
                )
                receipt = verified["receipt_sha256"]
                opened_text = verified["bar_open_utc"]
                if not interval[0] <= opened < interval[1]:
                    raise NativeRequestContractError(
                        ("V5_SOURCE_QUALITY_OMISSION_PAGE_MISMATCH",)
                    )
                # Connector extent endpoints are independently bound to the
                # artifact. The one approved omission is internal, so allowing
                # an endpoint omission would make the surviving extent
                # caller-asserted rather than mechanically provable.
                if (
                    admitted_first is None
                    or admitted_last is None
                    or not admitted_first < opened < admitted_last
                ):
                    raise NativeRequestContractError(
                        ("V5_SOURCE_QUALITY_OMISSION_ENDPOINT_INVALID",)
                    )
                if (
                    receipt in seen_omission_receipts
                    or opened_text in seen_omission_times
                ):
                    raise NativeRequestContractError(
                        ("V5_SOURCE_QUALITY_OMISSION_DUPLICATE",)
                    )
                seen_omission_receipts.add(receipt)
                seen_omission_times.add(opened_text)
                source_quality_omission_counts[index] += 1
                total_omissions += 1
            except NativeRequestContractError as exc:
                errors.extend(f"{code}:{index}:{position}" for code in exc.errors)
    if total_omissions > MAX_SOURCE_QUALITY_OMISSIONS_PER_CELL:
        errors.append("V5_SOURCE_QUALITY_OMISSION_MULTIPLICITY_INVALID")
    prefix_pages = diagnostic_values["terminal_prefix_unresolved_pages"]
    authenticated_boundary: datetime | None = None
    boundary_proof = pagination.get("available_history_boundary")
    if terminal_prefix_stopped:
        if len(prefix_pages) != 1 or not expected_intervals:
            errors.append("V5_TERMINAL_PREFIX_EVIDENCE_INVALID")
        else:
            prefix_interval = _interval_tuple(
                prefix_pages[0], "V5_TERMINAL_PREFIX_EVIDENCE_INVALID"
            )
            if prefix_interval != expected_intervals[0]:
                errors.append("V5_TERMINAL_PREFIX_PAGE_NOT_OLDEST")
            prefix_record = page_records.get(prefix_interval)
            diagnostics = prefix_pages[0].get("connector_diagnostics")
            errors.extend(
                _terminal_prefix_diagnostics_errors(
                    diagnostics,
                    copy_attempts=(0 if prefix_record is None else prefix_record[2]),
                )
            )
            if boundary_proof is not None:
                if len(attempts) < 2:
                    errors.append("V5_AVAILABLE_HISTORY_BOUNDARY_INVALID")
                else:
                    authenticated_boundary, boundary_errors = (
                        _authenticated_available_history_boundary(
                            boundary_proof,
                            terminal_page=prefix_pages[0],
                            first_data_page=attempts[1],
                        )
                    )
                    errors.extend(boundary_errors)
            if expected_intervals[0][1] > strategy_start:
                if authenticated_boundary is None:
                    errors.append("V5_TERMINAL_PREFIX_INTERSECTS_STRATEGY_INTERVAL")
    elif prefix_pages:
        errors.append("V5_TERMINAL_PREFIX_EVIDENCE_WITHOUT_STOP")
    elif boundary_proof is not None:
        errors.append("V5_AVAILABLE_HISTORY_BOUNDARY_WITHOUT_TERMINAL_PREFIX")

    technical_prefix_intervals: set[tuple[datetime, datetime]] = set()
    if technical_prefix_stopped:
        candidates = diagnostic_sets["empty_pages"] | diagnostic_sets["failed_pages"]
        expected_oldest = expected_intervals[0] if expected_intervals else None
        if (
            expected_oldest is None
            or candidates != {expected_oldest}
            or expected_oldest[1] > strategy_start
            or diagnostic_sets["terminal_prefix_unresolved_pages"]
            or expected_oldest in diagnostic_sets["truncated_pages"]
        ):
            errors.append("V5_TECHNICAL_PREFIX_EVIDENCE_INVALID")
        else:
            technical_prefix_intervals.add(expected_oldest)
    if (
        diagnostic_sets["failed_pages"] - technical_prefix_intervals
        and evidence_mode == "publishable"
    ):
        errors.append("V5_PUBLISHABLE_FAILED_PAGE_PRESENT")

    non_data = set().union(*categories)
    expected_data = len(attempts) - len(non_data)
    if pagination.get("pages_with_data") != expected_data:
        errors.append("V5_PAGINATION_DATA_COUNT_MISMATCH")
    data_indices = sorted(
        record[0]
        for interval, record in page_records.items()
        if interval not in non_data
    )
    if not data_indices and evidence_mode == "publishable":
        errors.append("V5_PUBLISHABLE_PAGE_DATA_MISSING")
    first_data = data_indices[0] if data_indices else None
    last_data = data_indices[-1] if data_indices else None
    terminal_prefix = bool(
        terminal_prefix_stopped and prefix_pages and authenticated_boundary is None
    )
    technical_prefix = bool(technical_prefix_stopped and technical_prefix_intervals)
    boundary_missing = bool(
        (terminal_prefix_stopped and prefix_pages) or technical_prefix
    )
    technical_gap = bool(
        (diagnostic_sets["failed_pages"] - technical_prefix_intervals)
        or (diagnostic_sets["empty_pages"] - technical_prefix_intervals)
    )
    internal_empty_count = len(
        diagnostic_sets["empty_pages"] - technical_prefix_intervals
    )

    seconds = TIMEFRAME_SECONDS[timeframe]
    internal_truncated_count = 0
    boundary_truncated_count = 0
    for item in diagnostic_values["truncated_pages"]:
        try:
            interval = _interval_tuple(item, "V5_TRUNCATED_PAGE_INVALID")
            record = page_records.get(interval)
            if record is None:
                raise NativeRequestContractError(("V5_TRUNCATED_PAGE_INVALID",))
            index = record[0]
            actual_first = _aware_utc(
                item.get("actual_first_open_utc"), "V5_TRUNCATED_PAGE_INVALID"
            )
            actual_last = _aware_utc(
                item.get("actual_last_open_utc"), "V5_TRUNCATED_PAGE_INVALID"
            )
            if not interval[0] <= actual_first <= actual_last < interval[1]:
                raise NativeRequestContractError(("V5_TRUNCATED_PAGE_INVALID",))
            expected_start_delay = max(
                0.0, (actual_first - interval[0]).total_seconds()
            )
            expected_last = interval[1] - timedelta(seconds=seconds)
            expected_end_lag = max(0.0, (expected_last - actual_last).total_seconds())
            declared_start_delay = _strict_nonnegative_number(
                item.get("start_delay_seconds"),
                "V5_TRUNCATED_PAGE_INVALID",
                maximum=float(page_span),
            )
            declared_end_lag = _strict_nonnegative_number(
                item.get("end_lag_seconds"),
                "V5_TRUNCATED_PAGE_INVALID",
                maximum=float(page_span),
            )
            if declared_start_delay != expected_start_delay:
                errors.append("V5_TRUNCATED_START_DELAY_MISMATCH")
            if declared_end_lag != expected_end_lag:
                errors.append("V5_TRUNCATED_END_LAG_MISMATCH")
            if (
                expected_start_delay <= boundary_tolerance_seconds
                and expected_end_lag <= boundary_tolerance_seconds
            ):
                errors.append("V5_TRUNCATED_PAGE_BELOW_TOLERANCE")
            page_internal = False
            page_boundary = False
            if expected_start_delay > boundary_tolerance_seconds:
                if index == first_data:
                    boundary_missing = True
                    page_boundary = True
                else:
                    technical_gap = True
                    page_internal = True
            if expected_end_lag > boundary_tolerance_seconds:
                if index == last_data:
                    boundary_missing = True
                    page_boundary = True
                else:
                    technical_gap = True
                    page_internal = True
            if page_internal:
                internal_truncated_count += 1
            elif page_boundary:
                boundary_truncated_count += 1
        except (NativeRequestContractError, TypeError, ValueError) as exc:
            if isinstance(exc, NativeRequestContractError):
                errors.extend(exc.errors)
            else:
                errors.append("V5_TRUNCATED_PAGE_INVALID")
    return {
        "boundary_missing": boundary_missing,
        "technical_gap": technical_gap,
        "terminal_prefix_unresolved": terminal_prefix,
        "technical_prefix_unresolved": technical_prefix,
        "authenticated_available_history_boundary_utc": authenticated_boundary,
        "internal_empty_page_count": internal_empty_count,
        "truncated_page_count": len(diagnostic_sets["truncated_pages"]),
        "internal_truncated_page_count": internal_truncated_count,
        "boundary_truncated_page_count": boundary_truncated_count,
        "failed_page_count": len(diagnostic_sets["failed_pages"]),
        "terminal_prefix_unresolved_page_count": len(prefix_pages),
        "source_quality_omission_counts": tuple(source_quality_omission_counts),
        "errors": errors,
    }


@dataclass(frozen=True)
class V5ArtifactObservation:
    """Read-only facts recomputed from the actual pinned bars file.

    The caller that streams the file must populate the quality counters.  The
    contract binds those recomputed facts to provenance; it does not provide a
    local signature or pretend that caller-supplied metadata authenticates
    bytes that were never read.
    """

    sha256: str
    bytes: int
    rows: int
    first_open_utc: str
    last_open_utc: str
    last_close_utc: str
    columns: tuple[str, ...]
    # (requested_start, requested_end_exclusive, rows, actual_first, actual_last)
    # for every page, in exact request order. Empty pages use ``None`` for
    # both actual endpoints.
    page_observations: tuple[tuple[str, str, int, str | None, str | None], ...] = ()
    duplicate_open_count: int = 0
    non_monotonic_open_count: int = 0
    invalid_bar_close_count: int = 0
    nonfinite_ohlc_count: int = 0
    invalid_ohlc_count: int = 0
    missing_required_value_count: int = 0
    out_of_request_count: int = 0
    page_assignment_error_count: int = 0
    nonpositive_price_count: int = 0
    negative_volume_count: int = 0
    negative_spread_count: int = 0
    source_quality_omission_target_open_count: int = 0


@_total_function("V5_ARTIFACT_OBSERVATION_BUILD_ERROR")
def build_v5_artifact_observation(
    *,
    sha256: Any,
    bytes: Any,
    rows: Any,
    first_open_utc: Any,
    last_open_utc: Any,
    last_close_utc: Any,
    columns: Any,
    page_observations: Any = (),
    duplicate_open_count: Any = 0,
    non_monotonic_open_count: Any = 0,
    invalid_bar_close_count: Any = 0,
    nonfinite_ohlc_count: Any = 0,
    invalid_ohlc_count: Any = 0,
    missing_required_value_count: Any = 0,
    out_of_request_count: Any = 0,
    page_assignment_error_count: Any = 0,
    nonpositive_price_count: Any = 0,
    negative_volume_count: Any = 0,
    negative_spread_count: Any = 0,
    source_quality_omission_target_open_count: Any = 0,
) -> V5ArtifactObservation:
    if not _is_sha256(sha256):
        raise NativeRequestContractError(("V5_ARTIFACT_SHA256_INVALID",))
    digest = sha256
    byte_count = _strict_positive_int(
        bytes,
        "V5_ARTIFACT_BYTES_INVALID",
        maximum=MAX_ARTIFACT_BYTES,
    )
    row_count = _strict_positive_int(
        rows,
        "V5_ARTIFACT_ROWS_INVALID",
        maximum=MAX_ARTIFACT_ROWS,
    )
    first = _aware_utc(first_open_utc, "V5_ARTIFACT_FIRST_OPEN_INVALID")
    last = _aware_utc(last_open_utc, "V5_ARTIFACT_LAST_OPEN_INVALID")
    close = _aware_utc(last_close_utc, "V5_ARTIFACT_LAST_CLOSE_INVALID")
    if first > last or last >= close:
        raise NativeRequestContractError(("V5_ARTIFACT_TIMESTAMP_ORDER_INVALID",))
    if not isinstance(columns, (list, tuple)) or not columns or len(columns) > 32:
        raise NativeRequestContractError(("V5_ARTIFACT_COLUMNS_INVALID",))
    if any(
        not isinstance(column, str) or not column or len(column) > 128
        for column in columns
    ):
        raise NativeRequestContractError(("V5_ARTIFACT_COLUMNS_INVALID",))
    column_tuple = tuple(columns)
    if column_tuple != _AUTHORITY_ARTIFACT_COLUMNS:
        raise NativeRequestContractError(("V5_ARTIFACT_COLUMNS_INVALID",))
    if (
        not isinstance(page_observations, (list, tuple))
        or not page_observations
        or len(page_observations) > MAX_PAGES_PER_CELL
    ):
        raise NativeRequestContractError(("V5_ARTIFACT_PAGE_OBSERVATIONS_INVALID",))
    normalized_pages: list[tuple[str, str, int, str | None, str | None]] = []
    page_rows_total = 0
    for index, page in enumerate(page_observations):
        if not isinstance(page, Mapping) or set(page) != {
            "start_utc",
            "end_exclusive_utc",
            "rows",
            "actual_first_open_utc",
            "actual_last_open_utc",
        }:
            raise NativeRequestContractError(
                (f"V5_ARTIFACT_PAGE_OBSERVATION_INVALID:{index}",)
            )
        page_start, page_end = _interval_tuple(
            page, f"V5_ARTIFACT_PAGE_OBSERVATION_INVALID:{index}"
        )
        page_rows = _strict_nonnegative_int(
            page.get("rows"),
            f"V5_ARTIFACT_PAGE_ROWS_INVALID:{index}",
            maximum=row_count,
        )
        raw_first = page.get("actual_first_open_utc")
        raw_last = page.get("actual_last_open_utc")
        if page_rows == 0:
            if raw_first is not None or raw_last is not None:
                raise NativeRequestContractError(
                    (f"V5_ARTIFACT_EMPTY_PAGE_ENDPOINT_INVALID:{index}",)
                )
            first_value = last_value = None
        else:
            first_open = _aware_utc(
                raw_first, f"V5_ARTIFACT_PAGE_FIRST_OPEN_INVALID:{index}"
            )
            last_open = _aware_utc(
                raw_last, f"V5_ARTIFACT_PAGE_LAST_OPEN_INVALID:{index}"
            )
            if not page_start <= first_open <= last_open < page_end:
                raise NativeRequestContractError(
                    (f"V5_ARTIFACT_PAGE_TIMESTAMP_ORDER_INVALID:{index}",)
                )
            first_value = _iso(first_open)
            last_value = _iso(last_open)
        page_rows_total += page_rows
        if page_rows_total > row_count:
            raise NativeRequestContractError(("V5_ARTIFACT_PAGE_ROWS_TOTAL_INVALID",))
        normalized_pages.append(
            (
                _iso(page_start),
                _iso(page_end),
                page_rows,
                first_value,
                last_value,
            )
        )
    if page_rows_total != row_count:
        raise NativeRequestContractError(("V5_ARTIFACT_PAGE_ROWS_TOTAL_INVALID",))
    quality_values: dict[str, int] = {}
    for field, value in (
        ("duplicate_open_count", duplicate_open_count),
        ("non_monotonic_open_count", non_monotonic_open_count),
        ("invalid_bar_close_count", invalid_bar_close_count),
        ("nonfinite_ohlc_count", nonfinite_ohlc_count),
        ("invalid_ohlc_count", invalid_ohlc_count),
        ("missing_required_value_count", missing_required_value_count),
        ("out_of_request_count", out_of_request_count),
        ("page_assignment_error_count", page_assignment_error_count),
        ("nonpositive_price_count", nonpositive_price_count),
        ("negative_volume_count", negative_volume_count),
        ("negative_spread_count", negative_spread_count),
        (
            "source_quality_omission_target_open_count",
            source_quality_omission_target_open_count,
        ),
    ):
        quality_values[field] = _strict_nonnegative_int(
            value,
            f"V5_ARTIFACT_QUALITY_COUNTER_INVALID:{field}",
            maximum=row_count,
        )
    return V5ArtifactObservation(
        sha256=digest,
        bytes=byte_count,
        rows=row_count,
        first_open_utc=_iso(first),
        last_open_utc=_iso(last),
        last_close_utc=_iso(close),
        columns=column_tuple,
        page_observations=tuple(normalized_pages),
        **quality_values,
    )


def _validate_artifact_observation(value: Any) -> V5ArtifactObservation:
    if type(value) is not V5ArtifactObservation:
        raise NativeRequestContractError(("V5_ARTIFACT_OBSERVATION_REQUIRED",))
    rebuilt = build_v5_artifact_observation(
        sha256=value.sha256,
        bytes=value.bytes,
        rows=value.rows,
        first_open_utc=value.first_open_utc,
        last_open_utc=value.last_open_utc,
        last_close_utc=value.last_close_utc,
        columns=value.columns,
        page_observations=[
            {
                "start_utc": page[0],
                "end_exclusive_utc": page[1],
                "rows": page[2],
                "actual_first_open_utc": page[3],
                "actual_last_open_utc": page[4],
            }
            for page in value.page_observations
        ],
        duplicate_open_count=value.duplicate_open_count,
        non_monotonic_open_count=value.non_monotonic_open_count,
        invalid_bar_close_count=value.invalid_bar_close_count,
        nonfinite_ohlc_count=value.nonfinite_ohlc_count,
        invalid_ohlc_count=value.invalid_ohlc_count,
        missing_required_value_count=value.missing_required_value_count,
        out_of_request_count=value.out_of_request_count,
        page_assignment_error_count=value.page_assignment_error_count,
        nonpositive_price_count=value.nonpositive_price_count,
        negative_volume_count=value.negative_volume_count,
        negative_spread_count=value.negative_spread_count,
        source_quality_omission_target_open_count=(
            value.source_quality_omission_target_open_count
        ),
    )
    if rebuilt != value:
        raise NativeRequestContractError(("V5_ARTIFACT_OBSERVATION_NONCANONICAL",))
    for field in (
        "duplicate_open_count",
        "non_monotonic_open_count",
        "invalid_bar_close_count",
        "nonfinite_ohlc_count",
        "invalid_ohlc_count",
        "missing_required_value_count",
        "out_of_request_count",
        "page_assignment_error_count",
        "nonpositive_price_count",
        "negative_volume_count",
        "negative_spread_count",
    ):
        if getattr(value, field) != 0:
            raise NativeRequestContractError((f"V5_ARTIFACT_QUALITY_FAILED:{field}",))
    return value


def _artifact_page_ledger_errors(
    observation: V5ArtifactObservation,
    pagination: Any,
    timeframe: str,
    *,
    boundary_tolerance_seconds: float,
    source_quality_omission_counts: Sequence[int],
) -> list[str]:
    """Bind the writer's page ledger to facts recomputed from artifact rows."""

    if not isinstance(pagination, Mapping):
        return ["V5_PAGINATION_INVALID"]
    attempts = pagination.get("page_attempts")
    if not isinstance(attempts, list):
        return ["V5_PAGE_ATTEMPT_LEDGER_INVALID"]
    errors: list[str] = []
    if len(observation.page_observations) != len(attempts):
        errors.append("V5_ARTIFACT_PAGE_LEDGER_CARDINALITY_MISMATCH")
        return errors
    if len(source_quality_omission_counts) != len(attempts) or any(
        type(value) is not int
        or value < 0
        or value > MAX_SOURCE_QUALITY_OMISSIONS_PER_CELL
        for value in source_quality_omission_counts
    ):
        errors.append("V5_SOURCE_QUALITY_OMISSION_COUNT_BINDING_INVALID")
        source_quality_omission_counts = (0,) * len(attempts)
    if (
        sum(source_quality_omission_counts) > 0
        and observation.source_quality_omission_target_open_count != 0
    ):
        errors.append("V5_SOURCE_QUALITY_OMISSION_TARGET_STILL_PRESENT")

    diagnostics: dict[str, dict[tuple[datetime, datetime], Mapping[str, Any]]] = {}
    for name in (
        "empty_pages",
        "failed_pages",
        "truncated_pages",
        "terminal_prefix_unresolved_pages",
    ):
        raw_values = pagination.get(name)
        values = raw_values if isinstance(raw_values, list) else []
        mapped: dict[tuple[datetime, datetime], Mapping[str, Any]] = {}
        for index, value in enumerate(values[:MAX_PAGES_PER_CELL]):
            if not isinstance(value, Mapping):
                continue
            try:
                interval = _interval_tuple(
                    value, f"V5_PAGINATION_DIAGNOSTIC_INVALID:{name}:{index}"
                )
            except NativeRequestContractError:
                continue
            mapped[interval] = value
        diagnostics[name] = mapped

    observed_first: datetime | None = None
    observed_last: datetime | None = None
    seconds = TIMEFRAME_SECONDS[timeframe]
    for index, (attempt, page) in enumerate(
        zip(attempts, observation.page_observations)
    ):
        if not isinstance(attempt, Mapping):
            errors.append(f"V5_ARTIFACT_PAGE_LEDGER_BINDING_MISMATCH:{index}")
            continue
        try:
            attempt_interval = _interval_tuple(
                attempt, f"V5_PAGE_INTERVAL_INVALID:{index}"
            )
            observed_interval = (
                _aware_utc(page[0], f"V5_ARTIFACT_PAGE_OBSERVATION_INVALID:{index}"),
                _aware_utc(page[1], f"V5_ARTIFACT_PAGE_OBSERVATION_INVALID:{index}"),
            )
        except NativeRequestContractError as exc:
            errors.extend(exc.errors)
            continue
        if attempt_interval != observed_interval:
            errors.append(f"V5_ARTIFACT_PAGE_LEDGER_BINDING_MISMATCH:{index}")
        page_rows = page[2]
        maximum_slots = int(
            math.ceil(
                (observed_interval[1] - observed_interval[0]).total_seconds()
                / float(seconds)
            )
        )
        if page_rows > maximum_slots:
            errors.append(f"V5_ARTIFACT_PAGE_DENSITY_INVALID:{index}")
        in_empty = observed_interval in diagnostics["empty_pages"]
        in_failed = observed_interval in diagnostics["failed_pages"]
        in_truncated = observed_interval in diagnostics["truncated_pages"]
        in_terminal_prefix = (
            observed_interval in diagnostics["terminal_prefix_unresolved_pages"]
        )
        if page_rows == 0:
            if not (in_empty or in_failed or in_terminal_prefix):
                errors.append(f"V5_ARTIFACT_EMPTY_PAGE_UNDECLARED:{index}")
            continue
        if in_empty or in_failed or in_terminal_prefix:
            errors.append(f"V5_ARTIFACT_NONEMPTY_PAGE_MISCLASSIFIED:{index}")
        actual_first = _aware_utc(
            page[3], f"V5_ARTIFACT_PAGE_FIRST_OPEN_INVALID:{index}"
        )
        actual_last = _aware_utc(page[4], f"V5_ARTIFACT_PAGE_LAST_OPEN_INVALID:{index}")
        connector_diagnostics = attempt.get("connector_diagnostics")
        if not isinstance(connector_diagnostics, Mapping):
            errors.append(f"V5_ARTIFACT_CONNECTOR_BINDING_INVALID:{index}")
        else:
            admitted_count = connector_diagnostics.get("admitted_count")
            admitted_extent = connector_diagnostics.get("admitted_extent")
            if (
                type(admitted_count) is not int
                or not isinstance(admitted_extent, Mapping)
                or type(admitted_extent.get("first_epoch_second")) is not int
                or type(admitted_extent.get("last_epoch_second")) is not int
            ):
                errors.append(f"V5_ARTIFACT_CONNECTOR_BINDING_INVALID:{index}")
            else:
                admitted_first = datetime.fromtimestamp(
                    admitted_extent.get("first_epoch_second", 0), tz=timezone.utc
                )
                admitted_last = datetime.fromtimestamp(
                    admitted_extent.get("last_epoch_second", 0), tz=timezone.utc
                )
                removed = admitted_count - page_rows
                omission_count = source_quality_omission_counts[index]
                unexplained_removed = removed - omission_count
                if timeframe in {"H4", "D1"}:
                    successor_delta_seconds = (
                        admitted_last - actual_last
                    ).total_seconds()
                    information_cutoff = observed_interval[1] - timedelta(seconds=1)
                    expected_close = (
                        admitted_last
                        if seconds * 0.5 <= successor_delta_seconds <= seconds * 1.5
                        else actual_last + timedelta(seconds=seconds)
                    )
                    valid_successor_removal = bool(
                        unexplained_removed == 1
                        and index == len(attempts) - 1
                        and admitted_first == actual_first
                        and admitted_last <= information_cutoff
                        and admitted_last + timedelta(seconds=seconds)
                        > information_cutoff
                        and expected_close
                        == _aware_utc(
                            observation.last_close_utc,
                            "V5_ARTIFACT_LAST_CLOSE_INVALID",
                        )
                        and actual_last < admitted_last
                    )
                    if not (
                        (
                            unexplained_removed == 0
                            and admitted_first == actual_first
                            and admitted_last == actual_last
                        )
                        or valid_successor_removal
                    ):
                        errors.append(f"V5_ARTIFACT_CONNECTOR_BINDING_INVALID:{index}")
                elif (
                    unexplained_removed != 0
                    or admitted_first != actual_first
                    or admitted_last != actual_last
                ):
                    errors.append(f"V5_ARTIFACT_CONNECTOR_BINDING_INVALID:{index}")
        observed_first = (
            actual_first
            if observed_first is None
            else min(observed_first, actual_first)
        )
        observed_last = (
            actual_last if observed_last is None else max(observed_last, actual_last)
        )
        start_delay = max(0.0, (actual_first - observed_interval[0]).total_seconds())
        expected_last = observed_interval[1] - timedelta(seconds=seconds)
        end_lag = max(0.0, (expected_last - actual_last).total_seconds())
        should_be_truncated = (
            start_delay > boundary_tolerance_seconds
            or end_lag > boundary_tolerance_seconds
        )
        if in_truncated != should_be_truncated:
            errors.append(f"V5_ARTIFACT_TRUNCATION_LEDGER_MISMATCH:{index}")
        if in_truncated:
            declared = diagnostics["truncated_pages"][observed_interval]
            if (
                declared.get("actual_first_open_utc") != _iso(actual_first)
                or declared.get("actual_last_open_utc") != _iso(actual_last)
                or declared.get("start_delay_seconds") != start_delay
                or declared.get("end_lag_seconds") != end_lag
            ):
                errors.append(f"V5_ARTIFACT_TRUNCATION_FACT_MISMATCH:{index}")
    if observed_first is None or observed_last is None:
        errors.append("V5_ARTIFACT_PAGE_DATA_MISSING")
    else:
        if _iso(observed_first) != observation.first_open_utc:
            errors.append("V5_ARTIFACT_PAGE_FIRST_OPEN_MISMATCH")
        if _iso(observed_last) != observation.last_open_utc:
            errors.append("V5_ARTIFACT_PAGE_LAST_OPEN_MISMATCH")
    return errors


@_total_function("V5_CELL_CONTEXT_BUILD_ERROR")
def build_v5_cell_validation_context(
    common_request: Mapping[str, Any],
    manifest_source_environment: Any,
    expected_retries: int,
) -> V5CellValidationContext:
    """Validate run-wide facts once, then reuse their immutable canonical bytes."""

    common = validate_v5_common_request(common_request)
    source = canonical_source_identity(manifest_source_environment)
    retries = _strict_positive_int(
        expected_retries,
        "V5_RETRIES_INVALID",
        maximum=MAX_PAGE_ATTEMPTS,
    )
    if retries != 1:
        raise NativeRequestContractError(("V5_ACQUISITION_RETRY_OWNER_INVALID",))
    context = object.__new__(V5CellValidationContext)
    object.__setattr__(context, "common_request_json", canonical_json_bytes(common))
    object.__setattr__(context, "source_identity_json", canonical_json_bytes(source))
    object.__setattr__(context, "retries", retries)
    object.__setattr__(context, "_common_request_object_id", id(common_request))
    object.__setattr__(
        context,
        "_source_environment_object_id",
        id(manifest_source_environment),
    )
    object.__setattr__(context, "_token", _V5_CELL_CONTEXT_TOKEN)
    return context


def _validated_v5_cell_context(
    value: Any,
    *,
    common_request: Any,
    manifest_source_environment: Any,
    expected_retries: Any,
) -> tuple[Mapping[str, Any], Mapping[str, Any], int]:
    if (
        type(value) is not V5CellValidationContext
        or getattr(value, "_token", None) is not _V5_CELL_CONTEXT_TOKEN
        or not isinstance(getattr(value, "common_request_json", None), bytes)
        or not 0
        < len(getattr(value, "common_request_json", b""))
        <= MAX_TRUSTED_COMMON_BYTES
        or not isinstance(getattr(value, "source_identity_json", None), bytes)
        or not 0
        < len(getattr(value, "source_identity_json", b""))
        <= MAX_TRUSTED_SOURCE_BYTES
        or type(getattr(value, "retries", None)) is not int
        or getattr(value, "retries", None) != 1
        or type(getattr(value, "_common_request_object_id", None)) is not int
        or getattr(value, "_common_request_object_id", None) != id(common_request)
        or type(getattr(value, "_source_environment_object_id", None)) is not int
        or getattr(value, "_source_environment_object_id", None)
        != id(manifest_source_environment)
        or type(expected_retries) is not int
        or expected_retries != value.retries
    ):
        raise NativeRequestContractError(("V5_CELL_VALIDATION_CONTEXT_INVALID",))
    return _decode_v5_cell_context(
        value.common_request_json,
        value.source_identity_json,
        value.retries,
    )


@_total_function("V5_CELL_STATUS_VALIDATION_ERROR")
def validate_v5_cell_status(
    provenance: Any,
    manifest_result: Any,
    common_request: Mapping[str, Any],
    *,
    symbol: Any,
    timeframe: str,
    manifest_source_environment: Any,
    artifact_observation: Any,
    expected_retries: Any,
    validation_context: Any = None,
) -> str:
    """Validate the published V5 success/partial classification.

    Returns a publishable source state. Internal gaps are nonpublishable. A
    verified contiguous suffix may be a boundary shortfall even when its older
    source prefix remains unresolved.
    """

    if not isinstance(provenance, Mapping) or not isinstance(manifest_result, Mapping):
        raise NativeRequestContractError(("V5_CELL_RECORD_INVALID",))
    canonical_json_bytes(provenance)
    canonical_json_bytes(manifest_result)
    tf = _canonical_timeframe(timeframe)
    expected_symbol = canonical_symbol(symbol)
    observation = _validate_artifact_observation(artifact_observation)
    context_source_identity_json: bytes | None = None
    if validation_context is None:
        retries = _strict_positive_int(
            expected_retries,
            "V5_RETRIES_INVALID",
            maximum=MAX_PAGE_ATTEMPTS,
        )
        common = validate_v5_common_request(common_request)
        source_identity = canonical_source_identity(manifest_source_environment)
    else:
        common, source_identity, retries = _validated_v5_cell_context(
            validation_context,
            common_request=common_request,
            manifest_source_environment=manifest_source_environment,
            expected_retries=expected_retries,
        )
        context_source_identity_json = validation_context.source_identity_json
    terminal_max_bars = source_identity["terminal"]["max_bars"]
    timeframe_seconds = TIMEFRAME_SECONDS[tf]
    configured_slots = max(
        1,
        int(_DEFAULT_PAGE_DAYS[tf] * 86_400 // timeframe_seconds),
    )
    safe_slots = max(1, int(terminal_max_bars * 0.90))
    expected_page_span_seconds = min(configured_slots, safe_slots) * timeframe_seconds
    boundary_tolerance_seconds = float(common["boundary_tolerance_days"] * 86_400)
    errors: list[str] = []
    if tf in {"H4", "D1"} and expected_page_span_seconds < 14 * 86_400:
        errors.append("V5_BROKER_SESSION_PAGE_SPAN_TOO_SHORT")
    for label, value in (
        ("PROVENANCE_SYMBOL", provenance.get("symbol")),
        ("PROVENANCE_CANONICAL_SYMBOL", provenance.get("canonical_symbol")),
        ("MANIFEST_SYMBOL", manifest_result.get("symbol")),
    ):
        if value != expected_symbol or not isinstance(value, str):
            errors.append(f"V5_{label}_MISMATCH")
    if provenance.get("timeframe") != tf or manifest_result.get("timeframe") != tf:
        errors.append("V5_CELL_TIMEFRAME_MISMATCH")
    if not isinstance(provenance.get("broker_symbol"), str) or not provenance.get(
        "broker_symbol"
    ):
        errors.append("V5_BROKER_SYMBOL_INVALID")
    try:
        if validation_context is None:
            validate_v5_leaf_request(provenance.get("request"), common, tf)
        else:
            leaf_request = provenance.get("request")
            if not isinstance(leaf_request, Mapping):
                raise NativeRequestContractError(("V5_LEAF_REQUEST_INVALID",))
            canonical_json_bytes(leaf_request)
            _validate_v5_leaf_request_against_common(leaf_request, common, tf)
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
    try:
        if context_source_identity_json is None:
            validate_source_identity_match(
                provenance.get("source_environment"), manifest_source_environment
            )
        else:
            _validate_source_identity_against_canonical_bytes(
                provenance.get("source_environment"),
                context_source_identity_json,
            )
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
    errors.extend(
        _output_resource_snapshot_errors(
            provenance.get("output_storage_resource_snapshot")
        )
    )

    strict_fields = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "source_kind": "broker_native",
        "timezone": "UTC",
        "timestamp_semantics": "bar_open_utc",
        "bar_interval_semantics": "half_open_[bar_open,bar_close)",
        "bar_close_column": "_BAR_CLOSE_TS",
        "timeframe_seconds": TIMEFRAME_SECONDS[tf],
        "statistical_sufficiency_assessed": False,
    }
    for field, expected in strict_fields.items():
        if provenance.get(field) != expected or type(provenance.get(field)) is not type(
            expected
        ):
            errors.append(f"V5_PROVENANCE_FIELD_INVALID:{field}")
    run_id = provenance.get("run_id")
    if not isinstance(run_id, str) or not _RUN_ID_RE.fullmatch(run_id):
        errors.append("V5_PROVENANCE_RUN_ID_INVALID")
    for record_name, record in (
        ("PROVENANCE", provenance),
        ("MANIFEST", manifest_result),
    ):
        errors.extend(
            _authority_claim_errors(
                record,
                code_prefix=f"V5_{record_name}_AUTHORITY_CLAIM_PRESENT",
            )
        )
        for field in _PROHIBITED_AUTHORITY_CLAIMS:
            value = record.get(field)
            if (
                field in record
                and value is not None
                and value is not False
                and value != ""
            ):
                errors.append(f"V5_{record_name}_AUTHORITY_CLAIM_PRESENT:{field}")

    status = provenance.get("status")
    manifest_status = manifest_result.get("status")
    failure = provenance.get("failure_status")
    manifest_failure = manifest_result.get("failure_status")
    if status != manifest_status:
        errors.append("V5_STATUS_MANIFEST_MISMATCH")
    if failure != manifest_failure:
        errors.append("V5_FAILURE_STATUS_MANIFEST_MISMATCH")
    if not isinstance(status, str) or status not in {"success", "partial"}:
        errors.append("V5_PUBLISHABLE_STATUS_INVALID")
    if manifest_status not in {"success", "partial"} or not isinstance(
        manifest_status, str
    ):
        errors.append("V5_MANIFEST_STATUS_INVALID")
    if failure is not None and not isinstance(failure, str):
        errors.append("V5_FAILURE_STATUS_TYPE_INVALID")
    if provenance.get("complete") is not (status == "success"):
        errors.append("V5_COMPLETE_FLAG_CONTRADICTION")

    request = provenance.get("request")
    source_interval = (
        request.get("source_request_interval") if isinstance(request, Mapping) else None
    )
    pagination_evidence = _pagination_evidence(
        provenance.get("pagination"),
        source_interval,
        common["timeframe_strategy_evidence_intervals"][tf],
        tf,
        expected_symbol=expected_symbol,
        expected_broker_symbol=str(provenance.get("broker_symbol") or ""),
        expected_source_environment=source_identity,
        expected_retries=retries,
        expected_page_span_seconds=expected_page_span_seconds,
        boundary_tolerance_seconds=boundary_tolerance_seconds,
    )
    errors.extend(pagination_evidence["errors"])
    errors.extend(
        _artifact_page_ledger_errors(
            observation,
            provenance.get("pagination"),
            tf,
            boundary_tolerance_seconds=boundary_tolerance_seconds,
            source_quality_omission_counts=pagination_evidence[
                "source_quality_omission_counts"
            ],
        )
    )
    boundary_missing = bool(pagination_evidence["boundary_missing"])
    technical_gap = bool(pagination_evidence["technical_gap"])
    terminal_prefix_unresolved = bool(pagination_evidence["terminal_prefix_unresolved"])
    coverage = provenance.get("coverage")
    if not isinstance(coverage, Mapping):
        errors.append("V5_COVERAGE_RECORD_INVALID")
        coverage = {}
    for metric in (
        "internal_empty_page_count",
        "truncated_page_count",
        "internal_truncated_page_count",
        "boundary_truncated_page_count",
        "failed_page_count",
        "terminal_prefix_unresolved_page_count",
    ):
        try:
            declared_metric = _strict_nonnegative_int(
                coverage.get(metric),
                f"V5_COVERAGE_DIAGNOSTIC_INVALID:{metric}",
                maximum=MAX_PAGES_PER_CELL,
            )
        except NativeRequestContractError as exc:
            errors.extend(exc.errors)
            declared_metric = None
        if declared_metric != pagination_evidence[metric]:
            errors.append(f"V5_COVERAGE_DIAGNOSTIC_MISMATCH:{metric}")
    expected_integrity = (
        "internal_gap_detected" if technical_gap else "complete_page_ledger"
    )
    if coverage.get("technical_integrity_status") != expected_integrity:
        errors.append("V5_COVERAGE_INTEGRITY_STATUS_MISMATCH")

    evidence_first_open = _aware_utc(
        observation.first_open_utc,
        "V5_ARTIFACT_FIRST_OPEN_INVALID",
    )
    authenticated_boundary = pagination_evidence[
        "authenticated_available_history_boundary_utc"
    ]
    if (
        authenticated_boundary is not None
        and authenticated_boundary != evidence_first_open
    ):
        errors.append("V5_AVAILABLE_HISTORY_BOUNDARY_ARTIFACT_MISMATCH")
    evidence_last_close = _aware_utc(
        observation.last_close_utc,
        "V5_ARTIFACT_LAST_CLOSE_INVALID",
    )
    evidence_source_start, evidence_source_end = _interval_tuple(
        common["timeframe_source_request_intervals"][tf],
        "V5_SOURCE_INTERVAL_INVALID",
    )
    evidence_coverage_start = evidence_source_start
    evidence_coverage_end = evidence_source_end
    if tf in {"H4", "D1"}:
        # The requested H4/D1 evidence domain ends at the common cutoff; the
        # extra second is only a successor sentinel.
        evidence_coverage_end = _aware_utc(
            common["common_as_of_utc"],
            "V5_AS_OF_INVALID",
        )
    coverage_boundary_shortfall = (
        evidence_first_open - evidence_coverage_start
    ).total_seconds() > boundary_tolerance_seconds or (
        evidence_coverage_end - evidence_last_close
    ).total_seconds() > boundary_tolerance_seconds
    boundary_missing = boundary_missing or coverage_boundary_shortfall
    strategy_start, strategy_end = _interval_tuple(
        common["timeframe_strategy_evidence_intervals"][tf],
        "V5_STRATEGY_INTERVAL_INVALID",
    )
    if tf in {"H4", "D1"}:
        strategy_end = _aware_utc(
            common["common_as_of_utc"],
            "V5_AS_OF_INVALID",
        )
    strategy_missing = (
        evidence_first_open - strategy_start
    ).total_seconds() > boundary_tolerance_seconds or (
        strategy_end - evidence_last_close
    ).total_seconds() > boundary_tolerance_seconds
    if strategy_missing and (
        pagination_evidence["terminal_prefix_unresolved"]
        or pagination_evidence["technical_prefix_unresolved"]
    ):
        errors.append("V5_AUTHENTICATED_SUFFIX_STRATEGY_COVERAGE_MISSING")
        technical_gap = True

    cell_state = ""
    if status == "success":
        cell_state = CELL_SUCCESS
        if failure is not None:
            errors.append("V5_SUCCESS_FAILURE_STATUS_PRESENT")
        if boundary_missing or technical_gap:
            errors.append("V5_SUCCESS_PARTIAL_DIAGNOSTICS_PRESENT")
        if coverage.get("status") != "full_requested_span":
            errors.append("V5_SUCCESS_COVERAGE_STATUS_INVALID")
        pagination = provenance.get("pagination")
        if isinstance(pagination, Mapping) and any(
            pagination.get(name) not in ([], ())
            for name in (
                "empty_pages",
                "failed_pages",
                "truncated_pages",
                "terminal_prefix_unresolved_pages",
            )
        ):
            errors.append("V5_SUCCESS_PARTIAL_DIAGNOSTICS_PRESENT")
    elif status == "partial" and failure == "boundary_history_shortfall":
        cell_state = CELL_BOUNDARY_SHORTFALL
        if not boundary_missing:
            errors.append("V5_BOUNDARY_SHORTFALL_EVIDENCE_MISSING")
        if technical_gap:
            errors.append("V5_BOUNDARY_SHORTFALL_HAS_TECHNICAL_GAP")
        if coverage.get("status") != "partial_requested_span":
            errors.append("V5_BOUNDARY_SHORTFALL_COVERAGE_STATUS_INVALID")
        if coverage.get("technical_integrity_status") != "complete_page_ledger":
            errors.append("V5_BOUNDARY_SHORTFALL_INTEGRITY_INVALID")
    elif status == "partial" and failure == "technical_history_gap":
        cell_state = CELL_TECHNICAL_GAP
        if not technical_gap:
            errors.append("V5_TECHNICAL_GAP_EVIDENCE_MISSING")
        if coverage.get("status") != "partial_requested_span":
            errors.append("V5_TECHNICAL_GAP_COVERAGE_STATUS_INVALID")
        if coverage.get("technical_integrity_status") != "internal_gap_detected":
            errors.append("V5_TECHNICAL_GAP_INTEGRITY_INVALID")
        errors.append("V5_TECHNICAL_ARTIFACT_NOT_PUBLISHABLE")
    elif status == "partial" and failure == "terminal_prefix_unresolved":
        cell_state = CELL_TERMINAL_PREFIX_UNRESOLVED
        if not terminal_prefix_unresolved:
            errors.append("V5_TERMINAL_PREFIX_EVIDENCE_MISSING")
        if coverage.get("status") != "partial_requested_span":
            errors.append("V5_TERMINAL_PREFIX_COVERAGE_STATUS_INVALID")
        if coverage.get("technical_integrity_status") != ("terminal_prefix_unresolved"):
            errors.append("V5_TERMINAL_PREFIX_INTEGRITY_INVALID")
        errors.append("V5_TERMINAL_PREFIX_ARTIFACT_NOT_PUBLISHABLE")
    elif status == "partial":
        errors.append("V5_PARTIAL_FAILURE_STATUS_INVALID")

    expected_downstream = status == "success"
    if (
        provenance.get("eligible_for_optimization") is not False
        or manifest_result.get("eligible_for_optimization") is not False
    ):
        errors.append("V5_ACQUISITION_OPTIMIZATION_ELIGIBILITY_INVALID")
    if (
        provenance.get("eligible_for_downstream_validation") is not expected_downstream
        or manifest_result.get("eligible_for_downstream_validation")
        is not expected_downstream
    ):
        errors.append("V5_DOWNSTREAM_VALIDATION_FLAG_INVALID")

    artifact = provenance.get("artifact")
    if not isinstance(artifact, Mapping):
        errors.append("V5_ARTIFACT_BINDING_INVALID")
        artifact = {}
    expected_artifact = {
        "relative_path": "bars.csv",
        "sha256": observation.sha256,
        "bytes": observation.bytes,
        "rows": observation.rows,
    }
    if not _same(artifact, expected_artifact):
        errors.append("V5_ARTIFACT_BINDING_MISMATCH")
    if (
        provenance.get("sha256") != observation.sha256
        or provenance.get("rows") != observation.rows
        or manifest_result.get("sha256") != observation.sha256
        or manifest_result.get("rows") != observation.rows
    ):
        errors.append("V5_ARTIFACT_SUMMARY_MISMATCH")
    if provenance.get("columns") != list(observation.columns):
        errors.append("V5_ARTIFACT_COLUMNS_MISMATCH")
    first_open = _aware_utc(
        observation.first_open_utc, "V5_ARTIFACT_FIRST_OPEN_INVALID"
    )
    last_open = _aware_utc(observation.last_open_utc, "V5_ARTIFACT_LAST_OPEN_INVALID")
    last_close = _aware_utc(
        observation.last_close_utc, "V5_ARTIFACT_LAST_CLOSE_INVALID"
    )
    try:
        provenance_start = _aware_utc(
            provenance.get("start_ts"), "V5_PROVENANCE_START_INVALID"
        )
        provenance_end = _aware_utc(
            provenance.get("end_ts"), "V5_PROVENANCE_END_INVALID"
        )
        if provenance_start != first_open or provenance_end != last_open:
            errors.append("V5_ARTIFACT_TIMESTAMP_BINDING_MISMATCH")
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
    request = provenance.get("request")
    if not isinstance(request, Mapping):
        request = {}
    try:
        declared_last_open = _aware_utc(
            request.get("last_fully_closed_bar_open_utc"),
            "V5_LAST_CLOSED_BAR_INVALID",
        )
        if declared_last_open != last_open:
            errors.append("V5_LAST_CLOSED_BAR_BINDING_MISMATCH")
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
    source_start, source_end = _interval_tuple(
        common["timeframe_source_request_intervals"][tf],
        "V5_SOURCE_INTERVAL_INVALID",
    )
    as_of = _aware_utc(common["common_as_of_utc"], "V5_AS_OF_INVALID")
    if not (
        source_start <= first_open <= last_open < source_end
        and last_open < last_close <= as_of
    ):
        errors.append("V5_ARTIFACT_CLOSURE_OUTSIDE_REQUEST")
    if tf not in {"H4", "D1"} and last_close != last_open + timedelta(
        seconds=TIMEFRAME_SECONDS[tf]
    ):
        errors.append("V5_FIXED_TIMEFRAME_CLOSE_INVALID")
    coverage_start = source_start
    coverage_end = source_end
    if tf in {"H4", "D1"}:
        coverage_end = as_of
    expected_start_delay = max(
        0.0,
        (first_open - coverage_start).total_seconds(),
    )
    expected_end_lag = max(
        0.0,
        (coverage_end - last_close).total_seconds(),
    )
    span_seconds = max(1.0, (coverage_end - coverage_start).total_seconds())
    observed_span_seconds = max(
        0.0,
        (
            min(coverage_end, last_close) - max(coverage_start, first_open)
        ).total_seconds(),
    )
    expected_ratio = float(min(1.0, observed_span_seconds / span_seconds))
    for field, expected in (
        ("actual_first_open_utc", observation.first_open_utc),
        ("actual_last_open_utc", observation.last_open_utc),
        ("actual_last_close_utc", observation.last_close_utc),
    ):
        if coverage.get(field) != expected:
            errors.append(f"V5_COVERAGE_ARTIFACT_BINDING_MISMATCH:{field}")
    for field, expected in (
        ("start_delay_seconds", expected_start_delay),
        ("end_lag_seconds", expected_end_lag),
        ("calendar_span_coverage_ratio", expected_ratio),
    ):
        raw = coverage.get(field)
        try:
            observed = _strict_nonnegative_number(
                raw,
                f"V5_COVERAGE_METRIC_INVALID:{field}",
                maximum=(1.0 if field == "calendar_span_coverage_ratio" else None),
            )
            if not isinstance(raw, float) or observed != expected:
                errors.append(f"V5_COVERAGE_METRIC_MISMATCH:{field}")
        except NativeRequestContractError as exc:
            errors.extend(exc.errors)
    if coverage.get("market_calendar_completeness_assessed") is not False:
        errors.append("V5_MARKET_CALENDAR_AUTHORITY_INVALID")
    if errors:
        raise NativeRequestContractError(errors)
    return cell_state


@_total_function("V5_FAILED_CELL_ENVELOPE_VALIDATION_ERROR")
def validate_v5_failed_cell_envelope(
    provenance: Any,
    manifest_result: Any,
    common_request: Mapping[str, Any],
    *,
    symbol: Any,
    timeframe: str,
    run_id: Any,
    manifest_source_environment: Any,
    expected_retries: int,
    manifest_acquisition_started_utc: Any,
    manifest_acquisition_completed_utc: Any,
    _base_fields: frozenset[str] = _FAILED_PROVENANCE_BASE_FIELDS,
    _fetch_fields: frozenset[str] = _FAILED_PROVENANCE_FETCH_FIELDS,
    _request_fields: frozenset[str] = _FAILED_PROVENANCE_REQUEST_FIELDS,
    _terminal_fields: frozenset[str] = _FAILED_PROVENANCE_TERMINAL_RESOURCE_FIELDS,
) -> str:
    """Authenticate one provenance-only acquisition failure.

    This validates a typed source/infrastructure outcome only.  It cannot emit
    an economic verdict, promotion authority, statistical sufficiency, or a
    substitute bar artifact.
    """

    if not isinstance(provenance, Mapping) or not isinstance(manifest_result, Mapping):
        raise NativeRequestContractError(("V5_FAILED_CELL_RECORD_INVALID",))
    canonical_json_bytes(provenance)
    canonical_json_bytes(manifest_result)
    common = validate_v5_common_request(common_request)
    expected_symbol = canonical_symbol(symbol)
    tf = _canonical_timeframe(timeframe)
    retries = _strict_positive_int(
        expected_retries,
        "V5_RETRIES_INVALID",
        maximum=MAX_PAGE_ATTEMPTS,
    )
    if retries != 1:
        raise NativeRequestContractError(("V5_ACQUISITION_RETRY_OWNER_INVALID",))
    if not isinstance(run_id, str) or not _RUN_ID_RE.fullmatch(run_id):
        raise NativeRequestContractError(("V5_PRODUCTION_RUN_ID_INVALID",))
    manifest_started = _aware_utc(
        manifest_acquisition_started_utc,
        "V5_MANIFEST_ACQUISITION_STARTED_INVALID",
    )
    manifest_completed = _aware_utc(
        manifest_acquisition_completed_utc,
        "V5_MANIFEST_ACQUISITION_COMPLETED_INVALID",
    )
    if manifest_completed < manifest_started:
        raise NativeRequestContractError(("V5_MANIFEST_ACQUISITION_ORDER_INVALID",))
    source_identity = canonical_source_identity(manifest_source_environment)
    errors: list[str] = []
    errors.extend(
        _manifest_result_errors(
            manifest_result,
            symbol=expected_symbol,
            timeframe=tf,
            publishable_only=False,
        )
    )
    failure = provenance.get("failure_status")
    if (
        provenance.get("status") != "failed"
        or manifest_result.get("status") != "failed"
        or not isinstance(failure, str)
        or failure not in _FAILED_ACQUISITION_STATES
        or manifest_result.get("failure_status") != failure
    ):
        errors.append("V5_FAILED_STATUS_BINDING_INVALID")

    observed_fields = frozenset(provenance)
    request_schema = _base_fields | _request_fields
    fetch_schema = _base_fields | _fetch_fields
    terminal_schema = request_schema | _terminal_fields
    allowed_schemas: set[frozenset[str]]
    if failure in {
        "fetch_failed",
        "terminal_history_empty_unresolved",
        "terminal_prefix_unresolved",
        "no_fully_closed_history",
        "data_quality_failed",
    }:
        allowed_schemas = {fetch_schema}
    elif failure == "terminal_cache_resource_limit":
        allowed_schemas = {terminal_schema}
    elif failure in {"write_failed", "output_storage_resource_limit"}:
        allowed_schemas = {request_schema, fetch_schema, terminal_schema}
    else:
        allowed_schemas = {request_schema}
    if observed_fields not in allowed_schemas:
        errors.append("V5_FAILED_PROVENANCE_SCHEMA_INVALID")

    strict_fields = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "run_id": run_id,
        "status": "failed",
        "eligible_for_optimization": False,
        "eligible_for_downstream_validation": False,
        "statistical_sufficiency_assessed": False,
        "canonical_symbol": expected_symbol,
        "symbol": expected_symbol,
        "timeframe": tf,
        "source_kind": "broker_native",
        "timezone": "UTC",
        "timestamp_semantics": "bar_open_utc",
        "bar_interval_semantics": "half_open_[bar_open,bar_close)",
        "bar_close_column": "_BAR_CLOSE_TS",
        "artifact": None,
        "sha256": None,
        "rows": 0,
        "start_ts": None,
        "end_ts": None,
        "complete": False,
    }
    for field, expected in strict_fields.items():
        if provenance.get(field) != expected or type(provenance.get(field)) is not type(
            expected
        ):
            errors.append(f"V5_FAILED_PROVENANCE_FIELD_INVALID:{field}")
    if (
        manifest_result.get("rows") != 0
        or type(manifest_result.get("rows")) is not int
        or manifest_result.get("sha256") is not None
        or manifest_result.get("eligible_for_optimization") is not False
        or manifest_result.get("eligible_for_downstream_validation") is not False
    ):
        errors.append("V5_FAILED_MANIFEST_ARTIFACT_INVALID")
    broker_symbol = provenance.get("broker_symbol")
    if failure == "symbol_resolution_failed":
        if broker_symbol is not None:
            errors.append("V5_FAILED_BROKER_RESOLUTION_CONTRADICTION")
    elif not isinstance(broker_symbol, str) or not broker_symbol:
        errors.append("V5_FAILED_BROKER_SYMBOL_INVALID")
    if not isinstance(provenance.get("instrument"), Mapping):
        errors.append("V5_FAILED_INSTRUMENT_DIAGNOSTIC_INVALID")
    if not _error_diagnostic_valid(provenance.get("error")):
        errors.append("V5_FAILED_ERROR_DIAGNOSTIC_INVALID")
    expected_error_type = {
        "fetch_failed": "PageFetchError",
        "terminal_history_empty_unresolved": "TerminalHistoryEmptyUnresolved",
        "terminal_prefix_unresolved": "TerminalPrefixUnresolved",
        "no_fully_closed_history": "NoFullyClosedHistory",
        "data_quality_failed": "DataQualityError",
        "terminal_cache_resource_limit": "TerminalCacheResourceLimitError",
        "output_storage_resource_limit": "OutputStorageResourceLimitError",
    }.get(str(failure))
    if expected_error_type is not None and (
        not isinstance(provenance.get("error"), Mapping)
        or provenance["error"].get("type") != expected_error_type
    ):
        errors.append("V5_FAILED_ERROR_TYPE_MISMATCH")
    try:
        leaf_started = _aware_utc(
            provenance.get("acquisition_started_utc"),
            "V5_FAILED_ACQUISITION_STARTED_INVALID",
        )
        leaf_completed = _aware_utc(
            provenance.get("acquisition_completed_utc"),
            "V5_FAILED_ACQUISITION_COMPLETED_INVALID",
        )
        information_cutoff = _aware_utc(
            common["requested_information_cutoff_utc"],
            "V5_REQUESTED_CUTOFF_INVALID",
        )
        if (
            not (
                manifest_started <= leaf_started <= leaf_completed <= manifest_completed
            )
            or leaf_started < information_cutoff
        ):
            errors.append("V5_FAILED_ACQUISITION_INTERVAL_OUTSIDE_MANIFEST")
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
    try:
        validate_source_identity_match(
            provenance.get("source_environment"), source_identity
        )
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
    errors.extend(
        _authority_claim_errors(
            provenance,
            code_prefix="V5_FAILED_AUTHORITY_CLAIM_PRESENT",
        )
    )
    errors.extend(
        _authority_claim_errors(
            manifest_result,
            code_prefix="V5_FAILED_MANIFEST_AUTHORITY_CLAIM_PRESENT",
        )
    )

    output_snapshot = provenance.get("output_storage_resource_snapshot")
    if failure == "output_storage_resource_limit":
        errors.extend(_output_resource_limit_snapshot_errors(output_snapshot))
    else:
        errors.extend(_output_resource_snapshot_errors(output_snapshot))
    if failure == "terminal_cache_resource_limit":
        errors.extend(
            _terminal_resource_limit_snapshot_errors(
                provenance.get("terminal_cache_resource_snapshot")
            )
        )

    if observed_fields in {request_schema, fetch_schema, terminal_schema}:
        if provenance.get("timeframe_seconds") != TIMEFRAME_SECONDS[tf]:
            errors.append("V5_FAILED_TIMEFRAME_SECONDS_INVALID")
        try:
            validate_v5_failed_leaf_request(provenance.get("request"), common, tf)
        except NativeRequestContractError as exc:
            errors.extend(exc.errors)
    if observed_fields == fetch_schema:
        source_interval = common["timeframe_source_request_intervals"][tf]
        terminal_max_bars = source_identity["terminal"]["max_bars"]
        configured_slots = max(
            1,
            int(_DEFAULT_PAGE_DAYS[tf] * 86_400 // TIMEFRAME_SECONDS[tf]),
        )
        safe_slots = max(1, int(terminal_max_bars * 0.90))
        expected_page_span_seconds = (
            min(configured_slots, safe_slots) * TIMEFRAME_SECONDS[tf]
        )
        if tf in {"H4", "D1"} and expected_page_span_seconds < 14 * 86_400:
            errors.append("V5_BROKER_SESSION_PAGE_SPAN_TOO_SHORT")
        pagination = provenance.get("pagination")
        evidence = _pagination_evidence(
            pagination,
            source_interval,
            common["timeframe_strategy_evidence_intervals"][tf],
            tf,
            expected_symbol=expected_symbol,
            expected_broker_symbol=str(broker_symbol or ""),
            expected_source_environment=source_identity,
            expected_retries=retries,
            expected_page_span_seconds=expected_page_span_seconds,
            boundary_tolerance_seconds=float(
                common["boundary_tolerance_days"] * 86_400
            ),
            evidence_mode="failed",
        )
        errors.extend(evidence["errors"])
        if isinstance(pagination, Mapping):
            pages_requested = pagination.get("pages_requested")
            pages_with_data = pagination.get("pages_with_data")
            empty_pages = pagination.get("empty_pages")
            failed_pages = pagination.get("failed_pages")
            prefix_pages = pagination.get("terminal_prefix_unresolved_pages")
            if failure == "fetch_failed" and not evidence["failed_page_count"]:
                errors.append("V5_FAILED_PAGE_EVIDENCE_MISSING")
            elif failure == "terminal_history_empty_unresolved":
                if not (
                    type(pages_requested) is int
                    and pages_requested > 0
                    and pages_with_data == 0
                    and isinstance(empty_pages, list)
                    and len(empty_pages) == pages_requested
                    and failed_pages == []
                    and prefix_pages == []
                    and pagination.get("stop_reason") is None
                ):
                    errors.append("V5_TERMINAL_EMPTY_EVIDENCE_INVALID")
            elif failure == "terminal_prefix_unresolved":
                if not (
                    pages_with_data == 0
                    and evidence["terminal_prefix_unresolved_page_count"] == 1
                    and pagination.get("stop_reason") == "terminal_prefix_unresolved"
                ):
                    errors.append("V5_TERMINAL_PREFIX_FAILURE_EVIDENCE_INVALID")
            elif failure in {"no_fully_closed_history", "data_quality_failed"}:
                if not (
                    type(pages_with_data) is int
                    and pages_with_data > 0
                    and failed_pages == []
                    and prefix_pages == []
                ):
                    errors.append("V5_FAILED_DATA_PAGE_EVIDENCE_INVALID")
    if errors:
        raise NativeRequestContractError(errors)
    return str(failure)


def _manifest_result_errors(
    result: Any,
    *,
    symbol: str,
    timeframe: str,
    publishable_only: bool = True,
) -> list[str]:
    if not isinstance(result, Mapping):
        return [f"V5_MANIFEST_RESULT_INVALID:{symbol}:{timeframe}"]
    errors: list[str] = []
    expected_fields = {
        "symbol",
        "timeframe",
        "status",
        "failure_status",
        "rows",
        "sha256",
        "eligible_for_optimization",
        "eligible_for_downstream_validation",
        "relative_leaf",
        "resume_disposition",
    }
    if set(result) != expected_fields:
        errors.append(f"V5_MANIFEST_RESULT_SCHEMA_INVALID:{symbol}:{timeframe}")
    if result.get("symbol") != symbol or result.get("timeframe") != timeframe:
        errors.append(f"V5_MANIFEST_RESULT_IDENTITY_MISMATCH:{symbol}:{timeframe}")
    if result.get("relative_leaf") != f"{symbol}/{timeframe}":
        errors.append(f"V5_MANIFEST_RESULT_PATH_INVALID:{symbol}:{timeframe}")
    if result.get("resume_disposition") not in {"newly_acquired", "reused"}:
        errors.append(f"V5_MANIFEST_RESULT_DISPOSITION_INVALID:{symbol}:{timeframe}")
    status = result.get("status")
    failure = result.get("failure_status")
    allowed_statuses = (
        {"success", "partial"} if publishable_only else {"success", "partial", "failed"}
    )
    if not isinstance(status, str) or status not in allowed_statuses:
        errors.append(f"V5_MANIFEST_RESULT_STATUS_INVALID:{symbol}:{timeframe}")
        return errors
    expected_downstream = status == "success"
    if result.get("eligible_for_optimization") is not False:
        errors.append(
            f"V5_MANIFEST_RESULT_OPTIMIZATION_FLAG_INVALID:{symbol}:{timeframe}"
        )
    if result.get("eligible_for_downstream_validation") is not expected_downstream:
        errors.append(
            f"V5_MANIFEST_RESULT_DOWNSTREAM_FLAG_INVALID:{symbol}:{timeframe}"
        )
    for field in _PROHIBITED_AUTHORITY_CLAIMS:
        if field in result:
            value = result.get(field)
            if value is not None and value is not False and value != "":
                errors.append(
                    f"V5_MANIFEST_RESULT_AUTHORITY_CLAIM_PRESENT:{symbol}:{timeframe}:{field}"
                )
    if status == "success":
        if failure is not None:
            errors.append(f"V5_MANIFEST_RESULT_FAILURE_INVALID:{symbol}:{timeframe}")
    elif status == "partial":
        allowed_partial_failures = (
            {"boundary_history_shortfall"}
            if publishable_only
            else {
                "boundary_history_shortfall",
                "technical_history_gap",
                "terminal_prefix_unresolved",
            }
        )
        if failure not in allowed_partial_failures:
            errors.append(f"V5_MANIFEST_RESULT_FAILURE_INVALID:{symbol}:{timeframe}")
    elif not isinstance(failure, str) or failure not in _FAILED_ACQUISITION_STATES:
        errors.append(f"V5_MANIFEST_RESULT_FAILURE_INVALID:{symbol}:{timeframe}")
    if status in {"success", "partial"}:
        try:
            _strict_positive_int(
                result.get("rows"),
                f"V5_MANIFEST_RESULT_ROWS_INVALID:{symbol}:{timeframe}",
                maximum=MAX_ARTIFACT_ROWS,
            )
        except NativeRequestContractError as exc:
            errors.extend(exc.errors)
        if not _is_sha256(result.get("sha256")):
            errors.append(f"V5_MANIFEST_RESULT_SHA_INVALID:{symbol}:{timeframe}")
    elif (
        type(result.get("rows")) is not int
        or result.get("rows") != 0
        or result.get("sha256") is not None
    ):
        errors.append(
            f"V5_MANIFEST_RESULT_FAILED_ARTIFACT_INVALID:{symbol}:{timeframe}"
        )
    return errors


@_total_function("V5_PRODUCTION_MANIFEST_VALIDATION_ERROR")
def validate_v5_production_manifest(
    manifest: Any,
    trusted_policy: Any,
) -> dict[str, Any]:
    """Validate the complete run roster against out-of-band authority."""

    policy = _validate_trusted_policy(trusted_policy)
    if not isinstance(manifest, Mapping):
        raise NativeRequestContractError(("V5_PRODUCTION_MANIFEST_INVALID",))
    canonical_json_bytes(manifest)
    errors: list[str] = []
    expected_manifest_fields = {
        "schema_version",
        "immutable_record",
        "complete",
        "run_id",
        "acquisition_started_utc",
        "acquisition_completed_utc",
        "request",
        "source_environment",
        "results",
        "counts",
        "resume",
    }
    if set(manifest) != expected_manifest_fields:
        errors.append("V5_MANIFEST_SCHEMA_FIELDS_INVALID")
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        errors.append("V5_MANIFEST_SCHEMA_VERSION_INVALID")
    if manifest.get("immutable_record") is not True:
        errors.append("V5_MANIFEST_IMMUTABILITY_INVALID")
    if manifest.get("complete") is not True:
        errors.append("V5_MANIFEST_COMPLETION_INVALID")
    run_id = manifest.get("run_id")
    if not isinstance(run_id, str) or not _RUN_ID_RE.fullmatch(run_id):
        errors.append("V5_MANIFEST_RUN_ID_INVALID")
    try:
        acquisition_started = _aware_utc(
            manifest.get("acquisition_started_utc"),
            "V5_MANIFEST_ACQUISITION_STARTED_INVALID",
        )
        acquisition_completed = _aware_utc(
            manifest.get("acquisition_completed_utc"),
            "V5_MANIFEST_ACQUISITION_COMPLETED_INVALID",
        )
        if acquisition_completed < acquisition_started:
            errors.append("V5_MANIFEST_ACQUISITION_ORDER_INVALID")
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
    try:
        common = validate_v5_manifest_request(manifest.get("request"), policy)
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)
        common = json.loads(policy.common_request_json.decode("utf-8"))
    expected_source = json.loads(policy.source_identity_json.decode("utf-8"))
    observed_source = expected_source
    try:
        observed_source = canonical_source_identity(manifest.get("source_environment"))
        if not _same(observed_source, expected_source):
            errors.append("V5_TRUSTED_SOURCE_IDENTITY_MISMATCH")
    except NativeRequestContractError as exc:
        errors.extend(exc.errors)

    expected_cells = [
        (symbol, timeframe)
        for symbol in policy.symbols
        for timeframe in policy.timeframes
    ]
    if len(expected_cells) > MAX_PRODUCTION_CELLS:
        errors.append("V5_PRODUCTION_CELL_CAP_EXCEEDED")
    expected_cell_set = frozenset(expected_cells)
    results = manifest.get("results")
    if not isinstance(results, list) or len(results) != len(expected_cells):
        errors.append("V5_MANIFEST_RESULT_ROSTER_SIZE_INVALID")
        results = []
    result_map: dict[tuple[str, str], Mapping[str, Any]] = {}
    observed_cell_order: list[tuple[str, str]] = []
    for index, result in enumerate(results):
        if not isinstance(result, Mapping):
            errors.append(f"V5_MANIFEST_RESULT_INVALID:{index}")
            continue
        symbol = result.get("symbol")
        timeframe = result.get("timeframe")
        if not isinstance(symbol, str) or not isinstance(timeframe, str):
            errors.append(f"V5_MANIFEST_RESULT_IDENTITY_INVALID:{index}")
            continue
        key = (symbol, timeframe)
        if key in result_map or key not in expected_cell_set:
            errors.append(f"V5_MANIFEST_RESULT_ROSTER_INVALID:{index}")
            continue
        result_map[key] = result
        observed_cell_order.append(key)
        errors.extend(
            _manifest_result_errors(result, symbol=symbol, timeframe=timeframe)
        )
    if set(result_map) != expected_cell_set:
        errors.append("V5_MANIFEST_RESULT_ROSTER_INCOMPLETE")
    if observed_cell_order != expected_cells:
        errors.append("V5_MANIFEST_RESULT_ORDER_MISMATCH")

    observed_counts = {
        status: sum(result.get("status") == status for result in result_map.values())
        for status in ("success", "partial", "failed")
    }
    expected_counts = {**observed_counts, "total": len(expected_cells)}
    counts = manifest.get("counts")
    if not isinstance(counts, Mapping) or set(counts) != set(expected_counts):
        errors.append("V5_MANIFEST_COUNTS_INVALID")
    else:
        for field, expected in expected_counts.items():
            try:
                observed = _strict_nonnegative_int(
                    counts.get(field),
                    f"V5_MANIFEST_COUNT_INVALID:{field}",
                    maximum=MAX_PRODUCTION_CELLS,
                )
                if observed != expected:
                    errors.append(f"V5_MANIFEST_COUNT_MISMATCH:{field}")
            except NativeRequestContractError as exc:
                errors.extend(exc.errors)
    resume = manifest.get("resume")
    expected_resume_fields = {
        "enabled",
        "newly_acquired_cells",
        "reused_cells",
        "stale_staging_removed",
    }
    if not isinstance(resume, Mapping) or set(resume) != expected_resume_fields:
        errors.append("V5_MANIFEST_RESUME_INVALID")
    else:
        if type(resume.get("enabled")) is not bool:
            errors.append("V5_MANIFEST_RESUME_ENABLED_INVALID")
        disposition_counts = {
            "newly_acquired_cells": sum(
                result.get("resume_disposition") == "newly_acquired"
                for result in result_map.values()
            ),
            "reused_cells": sum(
                result.get("resume_disposition") == "reused"
                for result in result_map.values()
            ),
        }
        for field, expected in disposition_counts.items():
            try:
                observed = _strict_nonnegative_int(
                    resume.get(field),
                    f"V5_MANIFEST_RESUME_COUNT_INVALID:{field}",
                    maximum=MAX_PRODUCTION_CELLS,
                )
                if observed != expected:
                    errors.append(f"V5_MANIFEST_RESUME_COUNT_MISMATCH:{field}")
            except NativeRequestContractError as exc:
                errors.extend(exc.errors)
        try:
            _strict_nonnegative_int(
                resume.get("stale_staging_removed"),
                "V5_MANIFEST_STALE_STAGING_COUNT_INVALID",
                maximum=MAX_PRODUCTION_CELLS,
            )
        except NativeRequestContractError as exc:
            errors.extend(exc.errors)
        if resume.get("enabled") is False and disposition_counts["reused_cells"]:
            errors.append("V5_MANIFEST_RESUME_CONTRADICTION")
    if errors:
        raise NativeRequestContractError(errors)
    return {
        "run_id": run_id,
        "common_request": common,
        "source_environment": deepcopy(observed_source),
        "results_by_cell": dict(result_map),
    }


@_total_function("V5_PRODUCTION_CELL_VALIDATION_ERROR")
def validate_v5_production_cell(
    *,
    manifest: Any,
    provenance: Any,
    manifest_result: Any,
    artifact_observation: Any,
    trusted_policy: Any,
    symbol: Any,
    timeframe: str,
) -> str:
    """Production entrypoint binding policy, manifest, leaf, and actual bytes."""

    policy = _validate_trusted_policy(trusted_policy)
    bound = validate_v5_production_manifest(manifest, policy)
    canonical = canonical_symbol(symbol)
    tf = _canonical_timeframe(timeframe)
    expected_result = bound["results_by_cell"].get((canonical, tf))
    if expected_result is None or not _same(expected_result, manifest_result):
        raise NativeRequestContractError(("V5_PRODUCTION_RESULT_BINDING_MISMATCH",))
    if (
        not isinstance(provenance, Mapping)
        or provenance.get("run_id") != bound["run_id"]
    ):
        raise NativeRequestContractError(("V5_PRODUCTION_RUN_BINDING_MISMATCH",))
    return validate_v5_cell_status(
        provenance,
        manifest_result,
        bound["common_request"],
        symbol=canonical,
        timeframe=tf,
        manifest_source_environment=bound["source_environment"],
        artifact_observation=artifact_observation,
        expected_retries=policy.retries,
    )


@_total_function("V5_FIXTURE_BUILD_ERROR")
def _fixture_mt5_v4_page_diagnostics(
    *,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    first_open: datetime,
    last_open: datetime,
    rows: int,
) -> dict[str, Any]:
    """Build deterministic connector evidence for this module's test fixture."""

    seconds = TIMEFRAME_SECONDS[timeframe]
    span = int((end - start).total_seconds())
    nonoverlap_budget = span - seconds
    slack = 0
    if nonoverlap_budget > 0:
        slack = min(
            max(4 * 86_400, 4 * seconds),
            max(0, (nonoverlap_budget - 1) // 2),
        )
    extent = {
        "rows": rows,
        "first_epoch_second": int(first_open.timestamp()),
        "last_epoch_second": int(last_open.timestamp()),
    }
    last_error = {"code": 1, "message": "Success", "capture_error": None}
    ledger_entry = {
        "sequence": 1,
        "call_type": "copy_rates_range_initial",
        "elapsed_ms": 0.0,
        "last_error": deepcopy(last_error),
        "exception": None,
        "hard_failure": False,
        "raw_relation": "IN_RANGE",
        "raw_count": rows,
        "raw_extent": deepcopy(extent),
        "admitted_count": rows,
        "admitted_extent": deepcopy(extent),
        "parse_error": None,
    }
    boundary_complete = bool(
        extent["first_epoch_second"] <= int(start.timestamp()) + slack
        and extent["last_epoch_second"] >= int(end.timestamp()) - seconds - slack
    )
    return {
        "range_contract_version": "mt5-half-open-range-v4",
        "symbol": symbol,
        "broker_symbol": f"{symbol}.a",
        "timeframe": timeframe,
        "sync_retry": True,
        "sync_status": (
            "boundary_complete" if boundary_complete else "boundary_incomplete"
        ),
        "request_start_epoch_second": int(start.timestamp()),
        "request_end_epoch_second_exclusive": int(end.timestamp()),
        "copy_rates_range_attempts": 1,
        "copy_rates_range_max_attempts": 2,
        "history_download_trigger_attempts": 0,
        "history_download_trigger_nonempty": 0,
        "history_download_trigger_max_bars": 0,
        "history_download_trigger_errors": [],
        "terminal_confirmation_consumed": False,
        "call_ledger_schema": "mt5.range-call-ledger.v1",
        "call_ledger": [ledger_entry],
        "hard_failure": False,
        "hard_failure_call": None,
        "last_error": deepcopy(last_error),
        "initial_raw_relation": "IN_RANGE",
        "confirmation_raw_relation": None,
        "raw_relation": "IN_RANGE",
        "raw_count": rows,
        "raw_extent": deepcopy(extent),
        "admitted_count": rows,
        "admitted_extent": deepcopy(extent),
        "boundary_span_seconds": span,
        "boundary_slack_seconds": slack,
        "boundary_complete": boundary_complete,
        "best_extent": deepcopy(extent),
    }


def _fixture_mt5_v4_empty_diagnostics(
    *, symbol: str, timeframe: str, start: datetime, end: datetime
) -> dict[str, Any]:
    """Build a genuine empty->warm->empty V4 receipt for test fixtures."""

    seconds = TIMEFRAME_SECONDS[timeframe]
    span = int((end - start).total_seconds())
    nonoverlap_budget = span - seconds
    slack = 0
    if nonoverlap_budget > 0:
        slack = min(
            max(4 * 86_400, 4 * seconds),
            max(0, (nonoverlap_budget - 1) // 2),
        )
    last_error = {"code": 1, "message": "Success", "capture_error": None}

    def entry(sequence: int, call_type: str) -> dict[str, Any]:
        return {
            "sequence": sequence,
            "call_type": call_type,
            "elapsed_ms": 0.0,
            "last_error": deepcopy(last_error),
            "exception": None,
            "hard_failure": False,
            "raw_relation": None,
            "raw_count": 0,
            "raw_extent": None,
            "admitted_count": 0,
            "admitted_extent": None,
            "parse_error": None,
        }

    trigger_bars = min(50_000, max(1, int(math.ceil(span / seconds)) + 2))
    return {
        "range_contract_version": "mt5-half-open-range-v4",
        "symbol": symbol,
        "broker_symbol": f"{symbol}.a",
        "timeframe": timeframe,
        "sync_retry": True,
        "sync_status": "empty_unsynchronized",
        "request_start_epoch_second": int(start.timestamp()),
        "request_end_epoch_second_exclusive": int(end.timestamp()),
        "copy_rates_range_attempts": 2,
        "copy_rates_range_max_attempts": 2,
        "history_download_trigger_attempts": 1,
        "history_download_trigger_nonempty": 0,
        "history_download_trigger_max_bars": trigger_bars,
        "history_download_trigger_errors": [],
        "terminal_confirmation_consumed": False,
        "call_ledger_schema": "mt5.range-call-ledger.v1",
        "call_ledger": [
            entry(1, "copy_rates_range_initial"),
            entry(2, "copy_rates_from_warm"),
            entry(3, "copy_rates_range_retry"),
        ],
        "hard_failure": False,
        "hard_failure_call": None,
        "last_error": deepcopy(last_error),
        "initial_raw_relation": None,
        "confirmation_raw_relation": None,
        "raw_relation": None,
        "raw_count": 0,
        "raw_extent": None,
        "admitted_count": 0,
        "admitted_extent": None,
        "boundary_span_seconds": span,
        "boundary_slack_seconds": slack,
        "boundary_complete": False,
        "best_extent": None,
    }


def build_valid_v5_contract_fixture(
    *,
    symbol: str = "EURUSD",
    timeframe: str = "M5",
    requested_information_cutoff_utc: Any = datetime(
        2026, 7, 13, 9, 0, tzinfo=timezone.utc
    ),
    source_environment: Any = None,
) -> dict[str, Any]:
    """Return a 600-cell manifest with six detailed leaves for one symbol."""

    canonical = canonical_symbol(symbol)
    if canonical not in _AUTHORITY_SYMBOL_ORDER:
        raise NativeRequestContractError(("V5_FIXTURE_SYMBOL_NOT_PRODUCTION",))
    tf = _canonical_timeframe(timeframe)
    common = build_v5_request_contract(
        requested_information_cutoff_utc=requested_information_cutoff_utc
    )
    environment = deepcopy(
        source_environment
        if source_environment is not None
        else {
            "metadata_status": "available",
            "account": {
                "login": 42,
                "server": "TestBroker-Demo",
                "company": "TestBroker",
                "currency": "USD",
            },
            "configured_connection": {
                "server": "TestBroker-Demo",
                "terminal_path": "C:\\Program Files\\MetaTrader 5\\terminal64.exe",
                "expected_login": 42,
                "expected_server": "TestBroker-Demo",
                "expected_currency": "USD",
            },
            "terminal": {
                "name": "MetaTrader 5",
                "company": "TestBroker",
                "path": "C:\\Program Files\\MetaTrader 5",
                "data_path": "C:\\FxPM\\TerminalData",
                "commondata_path": "C:\\FxPM\\TerminalCommon",
                "build": 5432,
                "version": [500, 5432, "13 Jul 2026"],
                "max_bars": 100000000,
            },
        }
    )
    trusted_policy = build_trusted_v5_policy(
        expected_symbols=PRODUCTION_SYMBOL_ORDER,
        expected_source_environment=environment,
        requested_information_cutoff_utc=requested_information_cutoff_utc,
        retries=1,
    )
    fixture_source_identity = canonical_source_identity(environment)
    fixture_terminal_max_bars = fixture_source_identity["terminal"]["max_bars"]
    columns = (
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Spread",
        "RealVolume",
        "_SOURCE_FIRST_TS",
        "_SOURCE_LAST_TS",
        "_BAR_CLOSE_TS",
    )
    leaves: dict[str, dict[str, Any]] = {}
    provenances: dict[str, dict[str, Any]] = {}
    observations: dict[str, V5ArtifactObservation] = {}
    results: list[dict[str, Any]] = []
    as_of = _aware_utc(common["common_as_of_utc"], "V5_AS_OF_INVALID")
    for cell_timeframe in PRODUCTION_TIMEFRAME_ORDER:
        seconds = TIMEFRAME_SECONDS[cell_timeframe]
        last_open = as_of - timedelta(seconds=seconds)
        leaf = build_v5_leaf_request(
            common,
            cell_timeframe,
            last_fully_closed_bar_open_utc=last_open,
        )
        source_start, source_end = _interval_tuple(
            leaf["source_request_interval"], "V5_SOURCE_INTERVAL_INVALID"
        )
        configured_slots = max(
            1,
            int(_DEFAULT_PAGE_DAYS[cell_timeframe] * 86_400 // seconds),
        )
        safe_slots = max(1, int(fixture_terminal_max_bars * 0.90))
        page_span = min(configured_slots, safe_slots) * seconds
        expected_pages = int(
            math.ceil((source_end - source_start).total_seconds() / page_span)
        )
        if expected_pages > MAX_PAGES_PER_CELL:
            raise NativeRequestContractError(
                ("V5_PAGE_LEDGER_CARDINALITY_CAP_EXCEEDED",)
            )
        pages: list[dict[str, Any]] = []
        valid_resource = {
            "status": "within_bounds",
            "data_path": r"C:\FxPM\TerminalData",
            "disk_total_bytes": 100 * 1024**3,
            "disk_free_bytes": 50 * 1024**3,
            "minimum_free_bytes": TERMINAL_MIN_FREE_BYTES,
            "hcc_files_inspected": 1,
            "hcc_entries_inspected": 1,
            "largest_hcc_bytes": 1_000_000,
            "largest_hcc_relative_path": r"bases\TestBroker-Demo\history\EURUSD\2026.hcc",
            "maximum_hcc_file_bytes": TERMINAL_HCC_MAX_FILE_BYTES,
            "scan_scope": "symbol",
            "scan_error": None,
            "violations": [],
        }
        cursor_end = source_end
        request_sequence = 0
        while cursor_end > source_start:
            page_start = max(source_start, cursor_end - timedelta(seconds=page_span))
            request_sequence += 1
            pages.append(
                {
                    "start_utc": _iso(page_start),
                    "end_exclusive_utc": _iso(cursor_end),
                    "attempts": 1,
                    "copy_rates_range_attempts": 1,
                    "request_sequence": request_sequence,
                    "terminal_cache_resource_snapshot": deepcopy(valid_resource),
                }
            )
            cursor_end = page_start
        pages.sort(key=lambda page: page["start_utc"])
        digest = hashlib.sha256(
            f"{canonical}:{cell_timeframe}:fixture".encode("ascii")
        ).hexdigest()
        first_open = source_start
        page_observations: list[dict[str, Any]] = []
        row_count = 0
        for page in pages:
            page_start = _aware_utc(
                page["start_utc"], "V5_ARTIFACT_PAGE_FIRST_OPEN_INVALID"
            )
            page_end = _aware_utc(
                page["end_exclusive_utc"], "V5_ARTIFACT_PAGE_LAST_OPEN_INVALID"
            )
            page_last = min(page_end - timedelta(seconds=seconds), last_open)
            page_rows = int((page_last - page_start).total_seconds() // seconds) + 1
            if page_rows <= 0:
                raise NativeRequestContractError(("V5_FIXTURE_PAGE_GEOMETRY_INVALID",))
            row_count += page_rows
            page["connector_diagnostics"] = _fixture_mt5_v4_page_diagnostics(
                symbol=canonical,
                timeframe=cell_timeframe,
                start=page_start,
                end=page_end,
                first_open=page_start,
                last_open=page_last,
                rows=page_rows,
            )
            page_observations.append(
                {
                    "start_utc": page["start_utc"],
                    "end_exclusive_utc": page["end_exclusive_utc"],
                    "rows": page_rows,
                    "actual_first_open_utc": _iso(page_start),
                    "actual_last_open_utc": _iso(page_last),
                }
            )
        observation = build_v5_artifact_observation(
            sha256=digest,
            bytes=max(100_000, row_count * 100),
            rows=row_count,
            first_open_utc=first_open,
            last_open_utc=last_open,
            last_close_utc=as_of,
            columns=columns,
            page_observations=page_observations,
        )
        pagination = {
            "contract_version": PAGINATION_CONTRACT_VERSION,
            "request_order": "newest_to_oldest",
            "page_span_seconds": page_span,
            "pages_planned": len(pages),
            "pages_requested": len(pages),
            "pages_with_data": len(pages),
            "empty_pages": [],
            "failed_pages": [],
            "truncated_pages": [],
            "terminal_prefix_unresolved_pages": [],
            "stop_reason": None,
            "unrequested_prefix": None,
            "initial_terminal_cache_resource_snapshot": deepcopy(valid_resource),
            "attempts_total": len(pages),
            "copy_rates_range_attempts_total": len(pages),
            "page_attempts": pages,
            "retries_per_page": 1,
        }
        provenance = {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
            "immutable_record": True,
            "run_id": "fixture-v5-run",
            "symbol": canonical,
            "canonical_symbol": canonical,
            "broker_symbol": f"{canonical}.a",
            "timeframe": cell_timeframe,
            "timeframe_seconds": seconds,
            "source_kind": "broker_native",
            "timezone": "UTC",
            "timestamp_semantics": "bar_open_utc",
            "bar_interval_semantics": "half_open_[bar_open,bar_close)",
            "bar_close_column": "_BAR_CLOSE_TS",
            "status": "success",
            "failure_status": None,
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": True,
            "statistical_sufficiency_assessed": False,
            "complete": True,
            "artifact": {
                "relative_path": "bars.csv",
                "sha256": observation.sha256,
                "bytes": observation.bytes,
                "rows": observation.rows,
            },
            "sha256": observation.sha256,
            "rows": observation.rows,
            "start_ts": observation.first_open_utc,
            "end_ts": observation.last_open_utc,
            "columns": list(observation.columns),
            "request": leaf,
            "source_environment": deepcopy(fixture_source_identity),
            "output_storage_resource_snapshot": {
                "status": "within_bounds",
                "output_path": r"C:\FxPM\data\native\runs\fixture-v5-run",
                "disk_total_bytes": 100 * 1024**3,
                "disk_free_bytes": 50 * 1024**3,
                "minimum_free_bytes": OUTPUT_MIN_FREE_BYTES,
                "maximum_cell_bytes": OUTPUT_MAX_CELL_BYTES,
                "estimated_cell_bytes": observation.bytes,
                "sample_rows": 2048,
                "violations": [],
            },
            "pagination": pagination,
            "coverage": {
                "status": "full_requested_span",
                "actual_first_open_utc": observation.first_open_utc,
                "actual_last_open_utc": observation.last_open_utc,
                "actual_last_close_utc": observation.last_close_utc,
                "start_delay_seconds": 0.0,
                "end_lag_seconds": 0.0,
                "calendar_span_coverage_ratio": 1.0,
                "internal_empty_page_count": 0,
                "truncated_page_count": 0,
                "internal_truncated_page_count": 0,
                "boundary_truncated_page_count": 0,
                "failed_page_count": 0,
                "terminal_prefix_unresolved_page_count": 0,
                "technical_integrity_status": "complete_page_ledger",
                "market_calendar_completeness_assessed": False,
            },
        }
        result = {
            "symbol": canonical,
            "timeframe": cell_timeframe,
            "status": "success",
            "failure_status": None,
            "rows": observation.rows,
            "sha256": observation.sha256,
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": True,
            "relative_leaf": f"{canonical}/{cell_timeframe}",
            "resume_disposition": "newly_acquired",
        }
        leaves[cell_timeframe] = leaf
        provenances[cell_timeframe] = provenance
        observations[cell_timeframe] = observation
        results.append(result)
    selected_results_by_timeframe = {item["timeframe"]: item for item in results}
    results = []
    for production_symbol in PRODUCTION_SYMBOL_ORDER:
        for cell_timeframe in PRODUCTION_TIMEFRAME_ORDER:
            if production_symbol == canonical:
                result = selected_results_by_timeframe[cell_timeframe]
            else:
                digest = hashlib.sha256(
                    f"{production_symbol}:{cell_timeframe}:fixture".encode("ascii")
                ).hexdigest()
                result = {
                    "symbol": production_symbol,
                    "timeframe": cell_timeframe,
                    "status": "success",
                    "failure_status": None,
                    "rows": 1_000,
                    "sha256": digest,
                    "eligible_for_optimization": False,
                    "eligible_for_downstream_validation": True,
                    "relative_leaf": f"{production_symbol}/{cell_timeframe}",
                    "resume_disposition": "newly_acquired",
                }
            results.append(result)
    manifest_request = build_v5_manifest_request(
        common,
        symbols=PRODUCTION_SYMBOL_ORDER,
        retries=1,
    )
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "immutable_record": True,
        "complete": True,
        "run_id": "fixture-v5-run",
        "acquisition_started_utc": _iso(
            _aware_utc(
                common["requested_information_cutoff_utc"],
                "REQUESTED_INFORMATION_CUTOFF_INVALID",
            )
            + timedelta(seconds=1)
        ),
        "acquisition_completed_utc": _iso(
            _aware_utc(
                common["requested_information_cutoff_utc"],
                "REQUESTED_INFORMATION_CUTOFF_INVALID",
            )
            + timedelta(seconds=2)
        ),
        "request": manifest_request,
        "source_environment": deepcopy(fixture_source_identity),
        "results": results,
        "counts": {
            "success": MAX_PRODUCTION_CELLS,
            "partial": 0,
            "failed": 0,
            "total": MAX_PRODUCTION_CELLS,
        },
        "resume": {
            "enabled": False,
            "newly_acquired_cells": MAX_PRODUCTION_CELLS,
            "reused_cells": 0,
            "stale_staging_removed": 0,
        },
    }
    selected_result = next(
        item
        for item in results
        if item["symbol"] == canonical and item["timeframe"] == tf
    )
    return {
        "common_request": common,
        "manifest_request": manifest_request,
        "leaf_request": leaves[tf],
        "provenance": provenances[tf],
        "manifest_result": selected_result,
        "source_environment": environment,
        "artifact_observation": observations[tf],
        "manifest": manifest,
        "trusted_policy": trusted_policy,
        "leaf_requests": leaves,
        "provenances": provenances,
        "artifact_observations": observations,
    }


_finalize_runtime_functions(
    {
        name: globals()[name]
        for name in (
            "_aware_utc",
            "_authenticated_available_history_boundary",
            "_authority_claim_errors",
            "_artifact_page_ledger_errors",
            "_canonical_value",
            "_canonical_timeframe",
            "_canonical_redacted_source_identity",
            "_floor_boundary",
            "_interval",
            "_interval_tuple",
            "_is_sha256",
            "_error_diagnostic_valid",
            "_freeze_v5_cell_context_value",
            "_thaw_v5_cell_context_value",
            "_map_roster_errors",
            "_manifest_result_errors",
            "_mt5_analysis_geometry_valid",
            "_mt5_boundary_complete_from_extent",
            "_mt5_boundary_geometry",
            "_mt5_extent_valid",
            "_mt5_last_error_valid",
            "_mt5_v4_preflight_diagnostics_errors",
            "_mt5_v4_diagnostics_errors",
            "_pagination_evidence",
            "_partition_chain_errors",
            "_policy_payload",
            "_output_resource_snapshot_errors",
            "_output_resource_limit_snapshot_errors",
            "_resource_snapshot_errors",
            "_same",
            "_source_identity_digest",
            "_source_quality_omission_row",
            "_strict_nonnegative_int",
            "_strict_nonnegative_number",
            "_strict_positive_int",
            "_strict_production_symbol_order",
            "_strict_timeframe_order",
            "_strict_year_matrix",
            "_terminal_prefix_diagnostics_errors",
            "_terminal_resource_limit_snapshot_errors",
            "_validate_artifact_observation",
            "_validate_source_identity_against_canonical_bytes",
            "_validate_trusted_policy",
            "_validate_v5_leaf_request_against_common",
            "_validated_v5_cell_context",
            "_decode_v5_cell_context",
            "build_trusted_v5_policy",
            "build_v5_source_quality_omission",
            "build_v5_cell_validation_context",
            "build_v5_artifact_observation",
            "build_v5_leaf_request",
            "build_v5_manifest_request",
            "build_v5_request_contract",
            "build_valid_v5_contract_fixture",
            "canonical_json_bytes",
            "canonical_sha256",
            "canonical_source_identity",
            "canonical_symbol",
            "canonical_symbols",
            "expected_consumer_map",
            "expected_reference_map",
            "timeframe_request_hash_payload",
            "validate_source_identity_match",
            "validate_v5_source_quality_omission",
            "validate_authenticated_available_history_boundary",
            "validate_mt5_v4_preflight_diagnostics",
            "validate_mt5_v4_page_diagnostics",
            "validate_v5_cell_status",
            "validate_v5_common_request",
            "validate_v5_failed_cell_envelope",
            "validate_v5_failed_leaf_request",
            "validate_v5_leaf_request",
            "validate_v5_manifest_request",
            "validate_v5_production_cell",
            "validate_v5_production_manifest",
        )
    }
)


__all__ = [
    "ACQUISITION_CONTRACT_VERSION",
    "AVAILABLE_HISTORY_BOUNDARY_CONFIRMATION_METHOD",
    "AVAILABLE_HISTORY_BOUNDARY_SCHEMA_VERSION",
    "BROKER_SESSION_ALIGNMENT_MODE",
    "CELL_BOUNDARY_SHORTFALL",
    "CELL_SUCCESS",
    "CELL_TECHNICAL_GAP",
    "COMMON_REQUEST_KEYS",
    "FIXED_ALIGNMENT_MODE",
    "LEAF_PROJECTION_KEYS",
    "MANIFEST_SCHEMA_VERSION",
    "MAX_CANONICAL_DEPTH",
    "MAX_CANONICAL_INTEGER_BITS",
    "MAX_CANONICAL_STRING_CHARS",
    "MAX_CONTAINER_ITEMS",
    "MAX_HORIZON_YEARS",
    "MAX_ARTIFACT_BYTES",
    "MAX_TOTAL_ARTIFACT_BYTES",
    "MAX_PAGES_PER_CELL",
    "MAX_PARTITION_YEARS",
    "MAX_SOURCE_QUALITY_OMISSIONS_PER_CELL",
    "NativeRequestContractError",
    "OUTPUT_MAX_CELL_BYTES",
    "OUTPUT_MIN_FREE_BYTES",
    "PAGINATION_CONTRACT_VERSION",
    "SOURCE_QUALITY_OMISSION_POLICY_ID",
    "SOURCE_QUALITY_OMISSION_SCHEMA_VERSION",
    "SOURCE_QUALITY_OMISSION_OPEN_UTC",
    "PRODUCTION_ARTIFACT_COLUMNS",
    "PRODUCTION_AUDIT_WINDOWS",
    "PRODUCTION_PARTITIONS",
    "PRODUCTION_SYMBOL_ORDER",
    "PRODUCTION_SYMBOL_ORDER_SHA256",
    "PRODUCTION_TIMEFRAME_ORDER",
    "PRODUCTION_TIMEFRAME_ORDER_SHA256",
    "PROVENANCE_SCHEMA_VERSION",
    "SOURCE_IDENTITY_SCHEMA_VERSION",
    "TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION",
    "TRUSTED_POLICY_SCHEMA_VERSION",
    "TrustedV5Policy",
    "V5ArtifactObservation",
    "V5CellValidationContext",
    "build_trusted_v5_policy",
    "build_v5_source_quality_omission",
    "build_v5_cell_validation_context",
    "build_v5_artifact_observation",
    "build_v5_leaf_request",
    "build_v5_manifest_request",
    "build_v5_request_contract",
    "build_valid_v5_contract_fixture",
    "canonical_json_bytes",
    "canonical_sha256",
    "canonical_source_identity",
    "canonical_symbol",
    "canonical_symbols",
    "expected_consumer_map",
    "expected_reference_map",
    "timeframe_request_hash_payload",
    "validate_source_identity_match",
    "validate_v5_source_quality_omission",
    "validate_authenticated_available_history_boundary",
    "validate_mt5_v4_preflight_diagnostics",
    "validate_mt5_v4_page_diagnostics",
    "validate_v5_cell_status",
    "validate_v5_common_request",
    "validate_v5_failed_cell_envelope",
    "validate_v5_failed_leaf_request",
    "validate_v5_leaf_request",
    "validate_v5_manifest_request",
    "validate_v5_production_cell",
    "validate_v5_production_manifest",
]
