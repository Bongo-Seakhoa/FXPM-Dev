"""Acquire independently sourced native MT5 bars into an immutable shadow tree.

This module is deliberately isolated from FxPM's canonical ``data/*_M5.csv``
files and from every execution API.  Importing it does not initialize MT5.  A
real terminal connection is opened only by :func:`main` when ``--execute`` is
explicitly supplied; tests and callers can inject a read-only connector.

The storage contract is::

    data/native/runs/<run-id>/<SYMBOL>/<TIMEFRAME>/bars.csv
    data/native/runs/<run-id>/<SYMBOL>/<TIMEFRAME>/provenance.json
    data/native/runs/<run-id>/run_manifest.json

Every run id and symbol/timeframe leaf is create-only.  Each leaf is assembled
in a sibling staging directory and renamed into place, so a failed write cannot
expose a half-written CSV.  A write failure produces a provenance-only leaf
with an explicit ``write_failed`` status.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import math
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

import numpy as np
import pandas as pd
from pandas.core.dtypes.cast import find_common_type as _pandas_find_common_type


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pm_evidence import (  # noqa: E402
    TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    build_timeframe_calendar_boundaries,
    normalize_timeframe_window_years,
)
from pm_native_intervals import (  # noqa: E402
    TIMEFRAME_SECONDS,
    native_source_interval,
)
from pm_native_consensus import (  # noqa: E402
    CONSENSUS_POLICY as NATIVE_CONSENSUS_POLICY,
    PRODUCTION_TIMEFRAMES as NATIVE_PRODUCTION_TIMEFRAMES,
    REFERENCE_GRAPH as NATIVE_REFERENCE_GRAPH,
)
from pm_native_request_contract import (  # noqa: E402
    MAX_ARTIFACT_BYTES,
    MAX_HORIZON_YEARS,
    MAX_PAGES_PER_CELL,
    MAX_PARTITION_YEARS,
    MAX_TOTAL_ARTIFACT_BYTES,
    NativeRequestContractError,
    PRODUCTION_ARTIFACT_COLUMNS,
    PRODUCTION_SYMBOL_ORDER,
    build_v5_source_quality_omission,
    build_v5_cell_validation_context,
    build_trusted_v5_policy,
    canonical_json_bytes,
    canonical_sha256,
    canonical_source_identity,
    canonical_symbols as canonical_v5_symbols,
    validate_mt5_v4_preflight_diagnostics,
    validate_mt5_v4_page_diagnostics,
    validate_v5_cell_status,
    validate_v5_production_manifest,
)
from pm_native_csv_validation import (  # noqa: E402
    MAX_CSV_BYTES,
    MAX_CSV_LINE_BYTES,
    MAX_CSV_ROWS,
    NativeCsvScanResult,
    NativeCsvValidationError,
    scan_native_csv_stream,
)


LOGGER = logging.getLogger("fxpm.native_history")
SCHEMA_VERSION = "fxpm-native-bars-v1"
ACQUISITION_CONTRACT_VERSION = "2026-07-14-native-calendar-acquisition-v5"
PAGINATION_CONTRACT_VERSION = "fxpm.native_history.pagination.v3"
AVAILABLE_HISTORY_BOUNDARY_SCHEMA_VERSION = (
    "fxpm.native_history.available_history_boundary.v1"
)
AVAILABLE_HISTORY_BOUNDARY_CONFIRMATION_METHOD = (
    "mt5_v4_three_call_single_bar_boundary_match_v1"
)
MANIFEST_SCHEMA_VERSION = "fxpm.native_history_run_manifest.v1"
RESUME_CONTRACT_VERSION = "fxpm.native_history_resume.v1"
RESUME_SEED_SCHEMA_VERSION = "fxpm.native_history.resume_seed.v2"
ATTEMPT_RECEIPT_SCHEMA_VERSION = "fxpm.native_history.retryable_attempt.v1"
ATTEMPT_RECEIPTS_DIR = "_attempts"
RESUME_SEED_FILENAME = "resume_contract.json"
MAX_RETAINED_ATTEMPT_RECEIPTS_PER_CELL = 2
MAX_ATTEMPT_RECEIPT_BYTES = 256 * 1024
MAX_RETRYABLE_PROVENANCE_BYTES = 4 * 1024 * 1024
MAX_OWNER_RECORD_BYTES = 64 * 1024
MAX_CONNECTOR_DIAGNOSTIC_BYTES = 64 * 1024
MAX_RESUME_SEED_BYTES = 4 * 1024 * 1024
MAX_STAGING_ENTRIES_PER_DIRECTORY = 8
MAX_TERMINAL_INSTANCES = 128
MAX_TERMINAL_DIRECTORY_ENTRIES = 10_000
MAX_TERMINAL_HCC_SCAN_ENTRIES = 1_000_000
LEGACY_FLAT_WINDOW_CONTRACT_VERSION = "fxpm.native_history.flat_windows.v1"
TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V3 = "fxpm.native_history.timeframe_request_map.v3"
TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4 = "fxpm.native_history.timeframe_request_map.v4"
# Retain the old public name because immutable V3 artifacts and downstream
# forensic readers import it.  It is never the schema emitted by a new run.
LEGACY_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION = TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V3
FORENSIC_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION = TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4
TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION = "fxpm.native_history.timeframe_request_map.v5"
PARTITIONS: Tuple[str, ...] = (
    "optimizer_train",
    "tuning_validation",
    "final_lockbox",
)
DEFAULT_WINDOW_YEARS: Dict[str, int] = {
    "optimizer_train": 5,
    "tuning_validation": 2,
    "final_lockbox": 2,
}
DEFAULT_TIMEFRAMES: Tuple[str, ...] = tuple(NATIVE_PRODUCTION_TIMEFRAMES)
if set(DEFAULT_TIMEFRAMES) != set(TIMEFRAME_SECONDS):
    raise RuntimeError("native acquisition/consensus timeframe authorities diverged")
SEGMENTED_SESSION_COVERAGE_METHOD_V2 = "timestamp_segmented_weekly_session_rate_v2"

TWO_REFERENCE_AUDIT_GRAPH: Dict[str, Tuple[str, str]] = {
    timeframe: tuple(NATIVE_REFERENCE_GRAPH[timeframe])
    for timeframe in DEFAULT_TIMEFRAMES
}
TWO_REFERENCE_CONSENSUS_POLICY: Dict[str, Any] = json.loads(
    json.dumps(NATIVE_CONSENSUS_POLICY, sort_keys=True)
)

# The coverage estimator deliberately reasons in complete UTC calendar weeks.
# Twenty-six weeks is long enough to distinguish a sustained schedule from a
# transient outage/holiday cluster without imposing an instrument calendar.
_SESSION_V2_MIN_EVIDENCE_WEEKS = 26
_SESSION_V2_MIN_SEGMENT_WEEKS = 26
_SESSION_V2_CHANGE_WINDOW_WEEKS = 26
_SESSION_V2_MIN_CHANGE_FRACTION = 0.10


def cross_resolution_reference_map(
    timeframes: Sequence[str],
) -> Dict[str, List[str]]:
    """Return the exact ordered two-reference graph for the six native cells.

    V5 deliberately refuses to invent a weaker graph for a diagnostic subset.
    All six broker-native sources are required so every target has two
    independently acquired references and a single bad intermediate series
    cannot veto its otherwise valid neighbours.
    """

    normalized = tuple(str(value or "").strip().upper() for value in timeframes)
    if any(value not in TIMEFRAME_SECONDS for value in normalized):
        unknown = sorted(
            value for value in normalized if value not in TIMEFRAME_SECONDS
        )
        raise ValueError(
            f"unsupported timeframe(s) for cross-resolution audit: {unknown}"
        )
    if set(normalized) != set(DEFAULT_TIMEFRAMES) or len(normalized) != len(
        DEFAULT_TIMEFRAMES
    ):
        raise ValueError(
            "the V5 cross-resolution contract requires the exact six-timeframe "
            f"roster {list(DEFAULT_TIMEFRAMES)!r}"
        )
    return {
        timeframe: list(TWO_REFERENCE_AUDIT_GRAPH[timeframe])
        for timeframe in DEFAULT_TIMEFRAMES
    }


# At a 10,000-bar terminal cap these spans remain below the cap even for a
# continuously traded market.  The span is reduced dynamically when MT5
# reports a lower max-bars setting.
DEFAULT_PAGE_DAYS: Dict[str, int] = {
    "M5": 30,
    "M15": 90,
    "M30": 180,
    "H1": 365,
    "H4": 1460,
    "D1": 3650,
}

_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SUCCESS_ERROR_CODES = {0, 1}
TERMINAL_INGESTION_LOCK_ID = "terminal-ingestion-global"
HARD_TIMEOUT_EXIT_CODE = 124
RESOURCE_LIMIT_EXIT_CODE = 125
SUPERVISOR_POLL_SECONDS = 0.5
DEFAULT_EXECUTION_RUNTIME_SECONDS = 3600.0
# MT5 documents one HCC file as one year of M1 bars.  Two GiB permits more
# than 4 KiB for every possible minute in a leap year, so exceeding it is a
# container-corruption signal, not a legitimate history-depth requirement.
TERMINAL_HCC_MAX_FILE_BYTES = 2 * 1024**3
TERMINAL_MIN_FREE_BYTES = 2 * TERMINAL_HCC_MAX_FILE_BYTES
OUTPUT_MIN_FREE_BYTES = 4 * 1024**3
OUTPUT_MAX_CELL_BYTES = MAX_ARTIFACT_BYTES
MAX_REUSABLE_PROVENANCE_BYTES = 4 * 1024 * 1024
NATIVE_VALIDATION_IO_BUFFER_BYTES = 1024 * 1024
# The configured native page geometry tops out at 8,760 nominal slots (H1/H4).
# Keep a fixed margin for inclusive broker endpoints and malformed duplicates,
# but never retain more than one bounded page after the connector returns it.
MAX_NATIVE_PAGE_IN_PROCESS_ROWS = 20_000
MAX_NATIVE_PAGE_IN_PROCESS_BYTES = 64 * 1024 * 1024
MAX_NATIVE_PAGE_SPILL_BYTES = 128 * 1024 * 1024
MAX_NATIVE_ASSEMBLY_SPILL_BYTES = OUTPUT_MAX_CELL_BYTES
_FILE_ATTRIBUTE_REPARSE_POINT = int(
    getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
)

# Exact schema produced by ``NativeHistoryStore.commit_leaf`` for a publishable
# V5 cell.  Acquisition/reuse is deliberately not a permissive JSON reader:
# unknown fields can otherwise become an unaudited authority side-channel.
V5_PUBLISHABLE_PROVENANCE_KEYS = frozenset(
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
        "timeframe_seconds",
        "request",
        "source_environment",
        "instrument",
        "acquisition_started_utc",
        "acquisition_completed_utc",
        "pagination",
        "coverage",
        "expected_session_coverage_by_partition",
        "observed_session_bars_by_partition",
        "expected_session_bars_by_partition",
        "expected_session_coverage_details_by_partition",
        "quality",
        "columns",
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

# Retry receipts are non-authoritative operational evidence.  Project only the
# fields this writer knows how to interpret before walking any caller-supplied
# object.  In particular, candidate-frame quality/coverage never belongs in a
# failure receipt because there is no immutable bars artifact to bind it to.
RETRYABLE_ATTEMPT_PROVENANCE_KEYS = frozenset(
    (
        V5_PUBLISHABLE_PROVENANCE_KEYS
        - {
            "columns",
            "quality",
            "coverage",
            "expected_session_coverage_by_partition",
            "expected_session_coverage_details_by_partition",
            "observed_session_bars_by_partition",
            "expected_session_bars_by_partition",
        }
    )
    | {
        "connector_preflight_failure",
        "error",
        "native_page_assembly_resource_snapshot",
        "source_unavailable_proof",
        "terminal_cache_resource_snapshot",
    }
)


class ResumeValidationError(RuntimeError):
    """Raised when an immutable run cannot be resumed without ambiguity."""


class ConcurrentWriterError(ResumeValidationError):
    """Raised when another process owns the run's operating-system lock."""


class TerminalCacheResourceLimitError(RuntimeError):
    """Raised when the MT5 cache violates a hard disk/container bound."""

    def __init__(self, snapshot: Mapping[str, Any]) -> None:
        self.snapshot = dict(_json_safe(snapshot))
        violations = self.snapshot.get("violations") or ["unknown"]
        super().__init__(f"terminal cache resource limit: {violations}")


class OutputStorageResourceLimitError(RuntimeError):
    """Raised before serialization can exhaust the immutable output volume."""

    def __init__(self, snapshot: Mapping[str, Any]) -> None:
        self.snapshot = dict(_json_safe(snapshot))
        violations = self.snapshot.get("violations") or ["unknown"]
        super().__init__(f"native output storage limit: {violations}")


class NativePageAssemblyResourceLimitError(RuntimeError):
    """Raised before an admitted page can amplify into unbounded process memory."""

    def __init__(self, snapshot: Mapping[str, Any]) -> None:
        self.snapshot = dict(_json_safe(snapshot))
        violations = self.snapshot.get("violations") or ["unknown"]
        super().__init__(f"native page assembly resource limit: {violations}")


class NativeNoFullyClosedHistoryError(RuntimeError):
    """Raised when every exact source row is still forming/outside the horizon."""

    def __init__(self, out_of_range_rows: int) -> None:
        self.out_of_range_rows = int(out_of_range_rows)
        super().__init__("all spilled rows were outside the horizon or still forming")


def _utc(value: datetime) -> datetime:
    """Return an aware UTC datetime, rejecting ambiguous naive inputs."""

    if not isinstance(value, datetime):
        raise TypeError("datetime value required")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(
            "naive datetimes are not accepted; supply an explicit UTC offset"
        )
    return value.astimezone(timezone.utc)


def parse_utc_datetime(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and require an explicit timezone."""

    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return _utc(datetime.fromisoformat(text))


def _iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return _utc(value).isoformat().replace("+00:00", "Z")


def subtract_calendar_years(value: datetime, years: int) -> datetime:
    """Subtract calendar years, mapping leap day to 28 February when needed."""

    value = _utc(value)
    years = int(years)
    if years <= 0:
        raise ValueError("horizon years must be positive")
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        # 29 February -> 28 February in a non-leap target year.
        return value.replace(year=value.year - years, day=28)


def floor_utc_boundary(value: datetime, timeframe_seconds: int) -> datetime:
    """Floor an aware UTC timestamp to a fixed native-timeframe boundary."""

    value = _utc(value)
    seconds = int(timeframe_seconds)
    if seconds <= 0:
        raise ValueError("timeframe_seconds must be positive")
    epoch_seconds = math.floor(value.timestamp())
    return datetime.fromtimestamp(
        epoch_seconds - (epoch_seconds % seconds), tz=timezone.utc
    )


def ceil_utc_boundary(value: datetime, timeframe_seconds: int) -> datetime:
    floored = floor_utc_boundary(value, timeframe_seconds)
    return (
        floored
        if floored == _utc(value)
        else floored + timedelta(seconds=timeframe_seconds)
    )


def build_common_partition_windows(
    as_of_utc: datetime,
    window_years: Mapping[str, int],
    embargo_days: float,
) -> Dict[str, Tuple[datetime, datetime]]:
    """Match FxPM's common 5y/2y/2y half-open calendar evidence windows."""

    as_of = pd.Timestamp(_utc(as_of_utc))
    embargo = pd.Timedelta(days=max(0.0, float(embargo_days)))
    lockbox_end = as_of
    lockbox_start = lockbox_end - pd.DateOffset(
        years=int(window_years["final_lockbox"])
    )
    tuning_end = lockbox_start - embargo
    tuning_start = tuning_end - pd.DateOffset(
        years=int(window_years["tuning_validation"])
    )
    train_end = tuning_start - embargo
    train_start = train_end - pd.DateOffset(years=int(window_years["optimizer_train"]))
    return {
        "optimizer_train": (train_start.to_pydatetime(), train_end.to_pydatetime()),
        "tuning_validation": (tuning_start.to_pydatetime(), tuning_end.to_pydatetime()),
        "final_lockbox": (lockbox_start.to_pydatetime(), lockbox_end.to_pydatetime()),
    }


def expected_session_coverage_by_partition(
    frame: pd.DataFrame,
    partition_windows: Mapping[str, Tuple[datetime, datetime]],
) -> Tuple[Dict[str, Optional[float]], Dict[str, Any]]:
    """Estimate partition coverage from a train-only robust weekly session rate.

    The recurring bar rate is learned only from complete optimizer-train weeks.
    Coverage is then calculated independently inside each configured partition;
    no whole-file average is produced or accepted.  This works across FX,
    exchange-session CFDs, and 24/7 instruments without pretending weekends are
    missing data.  It remains a necessary data-quality statistic, never a claim
    that the broker's official holiday calendar has been reconstructed.
    """

    if frame is None or frame.empty:
        return {partition: None for partition in PARTITIONS}, {
            "method": "train_only_robust_weekly_session_rate_v1",
            "status": "insufficient_data",
        }
    index = pd.DatetimeIndex(
        pd.to_datetime(frame.index, utc=True, errors="coerce")
    ).dropna()
    index = index[~index.duplicated()].sort_values()
    train_start, train_end = partition_windows["optimizer_train"]
    train_start_ts = pd.Timestamp(train_start)
    train_end_ts = pd.Timestamp(train_end)
    train_index = index[(index >= train_start_ts) & (index < train_end_ts)]
    if train_index.empty:
        return {partition: None for partition in PARTITIONS}, {
            "method": "train_only_robust_weekly_session_rate_v1",
            "status": "optimizer_train_empty",
        }

    # Monday 00:00 UTC week labels.  Only complete calendar weeks strictly
    # inside optimizer-train inform the expected recurring session rate.
    train_days = train_index.normalize()
    week_starts = train_days - pd.to_timedelta(train_index.weekday, unit="D")
    counts = pd.Series(1, index=train_index).groupby(week_starts).sum().sort_index()
    first_full_week = train_start_ts.normalize() + pd.offsets.Week(weekday=0)
    if first_full_week < train_start_ts:
        first_full_week += pd.Timedelta(days=7)
    complete_counts = counts[
        (counts.index >= first_full_week)
        & ((counts.index + pd.Timedelta(days=7)) <= train_end_ts)
    ]
    if len(complete_counts) < 26:
        return {partition: None for partition in PARTITIONS}, {
            "method": "train_only_robust_weekly_session_rate_v1",
            "status": "fewer_than_26_complete_train_weeks",
            "complete_train_weeks": int(len(complete_counts)),
        }
    nominal_per_week = float(complete_counts.quantile(0.75, interpolation="higher"))
    if not math.isfinite(nominal_per_week) or nominal_per_week <= 0.0:
        return {partition: None for partition in PARTITIONS}, {
            "method": "train_only_robust_weekly_session_rate_v1",
            "status": "invalid_nominal_week",
        }

    coverage: Dict[str, Optional[float]] = {}
    details: Dict[str, Any] = {
        "method": "train_only_robust_weekly_session_rate_v1",
        "status": "estimated",
        "schedule_source_partition": "optimizer_train",
        "complete_train_weeks": int(len(complete_counts)),
        "nominal_bars_per_week_q75": nominal_per_week,
        "partitions": {},
        "caveat": (
            "Expected sessions are inferred from recurring train-period broker bars; "
            "this is not an exchange-calendar certification."
        ),
    }
    for partition in PARTITIONS:
        start, end = partition_windows[partition]
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        observed = int(((index >= start_ts) & (index < end_ts)).sum())
        duration_weeks = max(0.0, (end_ts - start_ts).total_seconds() / (7.0 * 86400.0))
        expected = nominal_per_week * duration_weeks
        value = min(1.0, observed / expected) if expected > 0.0 else None
        coverage[partition] = float(value) if value is not None else None
        details["partitions"][partition] = {
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "observed_rows": observed,
            "expected_rows_from_train_session_rate": float(expected),
            "coverage": coverage[partition],
        }
    return coverage, details


def _monday_floor_utc(value: pd.Timestamp) -> pd.Timestamp:
    """Return the Monday 00:00 UTC boundary containing ``value``."""

    value = pd.Timestamp(value)
    if value.tzinfo is None:
        value = value.tz_localize("UTC")
    else:
        value = value.tz_convert("UTC")
    return value.normalize() - pd.Timedelta(days=int(value.weekday()))


def _higher_quantile(values: np.ndarray, quantile: float) -> float:
    """Deterministic discrete quantile used for count/envelope estimates."""

    if values.size == 0:
        return float("nan")
    position = int(math.ceil(float(quantile) * (int(values.size) - 1)))
    return float(np.partition(np.asarray(values, dtype="float64"), position)[position])


def _segmented_session_coverage_by_partition(
    frame: pd.DataFrame,
    partition_windows: Mapping[str, Tuple[datetime, datetime]],
    *,
    method: str,
) -> Tuple[Dict[str, Optional[float]], Dict[str, Any]]:
    """Estimate timestamp completeness under sustained session-schedule changes.

    Version 2 is intentionally timestamp-only: OHLCV, spread, returns, strategy
    output, and performance labels are never read.  It constructs an explicit
    zero-filled UTC-week grid, detects only sustained level changes with at
    least 26 weeks of evidence on each side, and estimates each resulting
    schedule segment independently.  Consequently, absent calendar weeks stay
    in the denominator while a durable broker-session change does not make all
    later partitions look incomplete under an obsolete train-period rate.

    This is a broker-history completeness diagnostic, not an exchange-calendar
    certification.  A stable, systematic data omission is observationally
    indistinguishable from a real schedule reduction when timestamps are the
    only permitted input; the returned diagnostics make that limitation and
    every accepted change point explicit.
    """

    empty_coverage = {partition: None for partition in PARTITIONS}

    def _failure(status: str, **extra: Any):
        return dict(empty_coverage), {
            "method": method,
            "status": status,
            "performance_inputs_used": False,
            "timestamp_only": True,
            **extra,
        }

    if frame is None or frame.empty:
        return _failure("insufficient_data")
    if set(partition_windows) != set(PARTITIONS):
        return _failure("invalid_partition_windows")

    try:
        windows = {
            partition: (
                pd.Timestamp(_utc(start)),
                pd.Timestamp(_utc(end)),
            )
            for partition, (start, end) in partition_windows.items()
        }
    except (TypeError, ValueError, KeyError):
        return _failure("invalid_partition_windows")
    if any(start >= end for start, end in windows.values()):
        return _failure("invalid_partition_windows")

    raw_index = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce"))
    has_authenticated_closes = "_BAR_CLOSE_TS" in frame.columns
    raw_closes = (
        pd.DatetimeIndex(
            pd.to_datetime(
                frame["_BAR_CLOSE_TS"],
                utc=True,
                errors="coerce",
            )
        )
        if has_authenticated_closes
        else pd.DatetimeIndex([pd.NaT] * len(raw_index), tz="UTC")
    )
    normalized_bars = pd.DataFrame({"open": raw_index, "close": raw_closes}).dropna(
        subset=["open"]
    )
    normalized_bars = (
        normalized_bars.drop_duplicates(subset=["open"], keep="last")
        .sort_values("open")
        .reset_index(drop=True)
    )
    if has_authenticated_closes:
        normalized_bars["close"] = normalized_bars["close"].fillna(
            normalized_bars["open"] + pd.Timedelta(nanoseconds=1)
        )
    else:
        open_values = pd.DatetimeIndex(normalized_bars["open"])
        positive_deltas = np.diff(open_values.asi8)
        positive_deltas = positive_deltas[positive_deltas > 0]
        inferred_ns = int(np.median(positive_deltas)) if positive_deltas.size else 1
        normalized_bars["close"] = normalized_bars["open"] + pd.Timedelta(
            nanoseconds=max(1, inferred_ns)
        )
    index = pd.DatetimeIndex(normalized_bars["open"])
    close_index = pd.DatetimeIndex(normalized_bars["close"])
    envelope_start = min(start for start, _ in windows.values())
    envelope_end = max(end for _, end in windows.values())
    envelope_mask = (index >= envelope_start) & (index < envelope_end)
    index = index[envelope_mask]
    close_index = close_index[envelope_mask]
    if index.empty:
        return _failure("requested_calendar_empty")

    first_full_week = _monday_floor_utc(envelope_start)
    if first_full_week < envelope_start:
        first_full_week += pd.Timedelta(days=7)
    last_full_week = _monday_floor_utc(envelope_end)
    if last_full_week + pd.Timedelta(days=7) > envelope_end:
        last_full_week -= pd.Timedelta(days=7)
    if last_full_week < first_full_week:
        return _failure(
            "fewer_than_26_complete_calendar_weeks",
            complete_calendar_weeks=0,
        )
    week_starts = pd.DatetimeIndex(
        pd.date_range(first_full_week, last_full_week, freq="7D", tz="UTC")
    )
    if len(week_starts) < _SESSION_V2_MIN_EVIDENCE_WEEKS:
        return _failure(
            "fewer_than_26_complete_calendar_weeks",
            complete_calendar_weeks=int(len(week_starts)),
        )

    index_ns = index.asi8
    week_ns = week_starts.asi8
    week_end_ns = (week_starts + pd.Timedelta(days=7)).asi8
    weekly_counts = (
        np.searchsorted(index_ns, week_end_ns, side="left")
        - np.searchsorted(index_ns, week_ns, side="left")
    ).astype("int64")
    positive_weeks = int((weekly_counts > 0).sum())
    if positive_weeks < _SESSION_V2_MIN_EVIDENCE_WEEKS:
        return _failure(
            "fewer_than_26_nonempty_calendar_weeks",
            complete_calendar_weeks=int(len(week_starts)),
            nonempty_calendar_weeks=positive_weeks,
        )

    def _window_nominal(values: np.ndarray) -> Optional[Tuple[float, float]]:
        positive = values[values > 0]
        minimum_support = max(13, int(math.ceil(0.60 * len(values))))
        if positive.size < minimum_support:
            return None
        nominal = _higher_quantile(positive, 0.75)
        if not math.isfinite(nominal) or nominal <= 0.0:
            return None
        consistent = float(
            np.mean((positive >= 0.80 * nominal) & (positive <= 1.20 * nominal))
        )
        if consistent < 0.65:
            return None
        return nominal, consistent

    # Deterministic binary segmentation.  Zeros remain in the week grid but do
    # not define a new schedule.  Each accepted child retains >=26 weeks, and
    # the local windows on both sides must independently show a stable level.
    accepted_changes: Dict[int, Dict[str, Any]] = {}

    def _split_segment(start_pos: int, end_pos: int) -> List[Tuple[int, int]]:
        if end_pos - start_pos < 2 * _SESSION_V2_MIN_SEGMENT_WEEKS:
            return [(start_pos, end_pos)]
        best: Optional[Tuple[float, int, float, float, float, float]] = None
        for split_pos in range(
            start_pos + _SESSION_V2_MIN_SEGMENT_WEEKS,
            end_pos - _SESSION_V2_MIN_SEGMENT_WEEKS + 1,
        ):
            left = weekly_counts[
                max(start_pos, split_pos - _SESSION_V2_CHANGE_WINDOW_WEEKS) : split_pos
            ]
            right = weekly_counts[
                split_pos : min(end_pos, split_pos + _SESSION_V2_CHANGE_WINDOW_WEEKS)
            ]
            left_estimate = _window_nominal(left)
            right_estimate = _window_nominal(right)
            if left_estimate is None or right_estimate is None:
                continue
            left_nominal, left_consistency = left_estimate
            right_nominal, right_consistency = right_estimate
            relative_change = abs(left_nominal - right_nominal) / max(
                left_nominal,
                right_nominal,
            )
            if (
                abs(left_nominal - right_nominal) < 1.0
                or relative_change < _SESSION_V2_MIN_CHANGE_FRACTION
            ):
                continue
            support = min(
                float(np.mean(left > 0)),
                float(np.mean(right > 0)),
                left_consistency,
                right_consistency,
            )
            score = relative_change * support
            candidate = (
                score,
                split_pos,
                left_nominal,
                right_nominal,
                relative_change,
                support,
            )
            if (
                best is None
                or score > best[0] + 1e-12
                or (
                    math.isclose(score, best[0], rel_tol=0.0, abs_tol=1e-12)
                    and split_pos < best[1]
                )
            ):
                best = candidate
        if best is None:
            return [(start_pos, end_pos)]
        _, split_pos, left_nominal, right_nominal, relative_change, support = best
        accepted_changes[split_pos] = {
            "effective_week_start_utc": week_starts[split_pos].isoformat(),
            "left_nominal_bars_per_week": float(left_nominal),
            "right_nominal_bars_per_week": float(right_nominal),
            "relative_change": float(relative_change),
            "minimum_local_support": float(support),
            "local_window_weeks": _SESSION_V2_CHANGE_WINDOW_WEEKS,
        }
        return _split_segment(start_pos, split_pos) + _split_segment(split_pos, end_pos)

    segment_bounds = _split_segment(0, len(week_starts))

    # Gap baselines use each healthy week's maximum timestamp gap, assigning a
    # weekend/closure gap to the week containing its right-hand bar.
    all_gap_seconds = np.diff(index.asi8).astype("float64") / 1_000_000_000.0
    all_gap_right_weeks = index[1:].normalize() - pd.to_timedelta(
        index[1:].weekday,
        unit="D",
    )
    weekly_gap_max_by_key: Dict[int, float] = {}
    for right_week, gap_seconds in zip(all_gap_right_weeks.asi8, all_gap_seconds):
        week_key = int(right_week)
        weekly_gap_max_by_key[week_key] = max(
            weekly_gap_max_by_key.get(week_key, 0.0),
            float(gap_seconds),
        )
    segment_records: List[Dict[str, Any]] = []
    segment_models: List[Dict[str, Any]] = []
    lower_bound_available = True
    for segment_id, (start_pos, end_pos) in enumerate(segment_bounds):
        counts = weekly_counts[start_pos:end_pos]
        positive = counts[counts > 0]
        nominal = _higher_quantile(positive, 0.75)
        empirical_max = float(np.max(positive))
        nonempty_fraction = float(positive.size / max(1, counts.size))
        healthy_fraction = float(
            np.mean((counts >= 0.80 * nominal) & (counts <= 1.20 * nominal))
        )
        upper_schedule = empirical_max
        bound_defensible = bool(
            counts.size >= _SESSION_V2_MIN_EVIDENCE_WEEKS
            and positive.size >= 20
            and nonempty_fraction >= 0.75
            and empirical_max <= 1.15 * nominal
        )
        lower_bound_available = lower_bound_available and bound_defensible

        healthy_week_set = set(
            week_starts[start_pos:end_pos][counts >= 0.80 * nominal].asi8.tolist()
        )
        weekly_gap_maxima = [
            weekly_gap_max_by_key[week_key]
            for week_key in healthy_week_set
            if week_key in weekly_gap_max_by_key
        ]
        recurring_max_gap = (
            _higher_quantile(np.asarray(weekly_gap_maxima, dtype="float64"), 0.90)
            if len(weekly_gap_maxima) >= 13
            else None
        )
        segment_start = week_starts[start_pos]
        segment_end = week_starts[end_pos - 1] + pd.Timedelta(days=7)
        model = {
            "segment_id": segment_id,
            "start": segment_start,
            "end": segment_end,
            "nominal": float(nominal),
            "upper": float(upper_schedule),
            "bound_defensible": bound_defensible,
            "recurring_max_gap": (
                float(recurring_max_gap) if recurring_max_gap is not None else None
            ),
        }
        segment_models.append(model)
        segment_records.append(
            {
                "segment_id": segment_id,
                "start_week_utc": segment_start.isoformat(),
                "end_week_exclusive_utc": segment_end.isoformat(),
                "calendar_weeks": int(counts.size),
                "nonempty_weeks": int(positive.size),
                "zero_count_weeks": int((counts == 0).sum()),
                "nominal_bars_per_week_q75": float(nominal),
                "empirical_schedule_envelope_bars_per_week": float(upper_schedule),
                "empirical_max_bars_per_week": empirical_max,
                "upper_mean_bound_method": "segment_max_v2",
                "upper_mean_bound_family_alpha": None,
                "upper_mean_bound_radius": None,
                "nonempty_week_fraction": nonempty_fraction,
                "healthy_week_fraction": healthy_fraction,
                "conservative_lower_bound_defensible": bound_defensible,
                "recurring_max_gap_seconds_q90": (
                    float(recurring_max_gap) if recurring_max_gap is not None else None
                ),
            }
        )

    def _segment_for_week(week_start: pd.Timestamp) -> Dict[str, Any]:
        for model in segment_models:
            if model["start"] <= week_start < model["end"]:
                return model
        return (
            segment_models[0]
            if week_start < segment_models[0]["start"]
            else segment_models[-1]
        )

    def _partition_gap_diagnostic(
        partition_index: pd.DatetimeIndex,
    ) -> Optional[Dict[str, Any]]:
        if len(partition_index) < 2:
            return None
        gaps = np.diff(partition_index.asi8).astype("float64") / 1_000_000_000.0
        best: Optional[Tuple[float, int, float, float]] = None
        for gap_pos, gap_seconds in enumerate(gaps):
            right_week = _monday_floor_utc(partition_index[gap_pos + 1])
            baseline = _segment_for_week(right_week).get("recurring_max_gap")
            if baseline is None:
                continue
            unexplained = max(0.0, float(gap_seconds) - float(baseline))
            if (
                best is None
                or unexplained > best[0]
                or (
                    math.isclose(unexplained, best[0], rel_tol=0.0, abs_tol=1e-9)
                    and float(gap_seconds) > best[3]
                )
            ):
                best = (
                    unexplained,
                    gap_pos,
                    float(baseline),
                    float(gap_seconds),
                )
        if best is None:
            return None
        unexplained, gap_pos, baseline, observed_gap = best
        return {
            "seconds": float(unexplained),
            "gap_start_utc": partition_index[gap_pos].isoformat(),
            "gap_end_utc": partition_index[gap_pos + 1].isoformat(),
            "observed_gap_seconds": observed_gap,
            "recurring_schedule_gap_seconds": baseline,
        }

    coverage: Dict[str, Optional[float]] = {}
    partition_details: Dict[str, Any] = {}
    global_gap: Optional[Dict[str, Any]] = None
    week_duration_seconds = 7.0 * 86400.0
    for partition in PARTITIONS:
        start, end = windows[partition]
        observed_mask = (
            (index >= start)
            & (index < end)
            & (close_index > index)
            & (close_index <= end)
        )
        observed_index = index[observed_mask]
        observed = int(len(observed_index))
        expected_point = 0.0
        expected_upper = 0.0
        bound_is_available = True
        week_start = _monday_floor_utc(start)
        while week_start < end:
            week_end = week_start + pd.Timedelta(days=7)
            overlap_start = max(start, week_start)
            overlap_end = min(end, week_end)
            overlap_fraction = max(
                0.0,
                (overlap_end - overlap_start).total_seconds() / week_duration_seconds,
            )
            model = _segment_for_week(week_start)
            expected_point += float(model["nominal"]) * overlap_fraction
            if model["bound_defensible"]:
                expected_upper += float(model["upper"]) * overlap_fraction
            else:
                bound_is_available = False
            week_start = week_end
        point = min(1.0, observed / expected_point) if expected_point > 0.0 else None
        lower_bound = (
            min(1.0, observed / expected_upper)
            if bound_is_available and expected_upper > 0.0
            else None
        )
        coverage[partition] = float(point) if point is not None else None

        first_partition_full_week = _monday_floor_utc(start)
        if first_partition_full_week < start:
            first_partition_full_week += pd.Timedelta(days=7)
        full_partition_weeks: List[pd.Timestamp] = []
        cursor = first_partition_full_week
        while cursor + pd.Timedelta(days=7) <= end:
            full_partition_weeks.append(cursor)
            cursor += pd.Timedelta(days=7)
        missing_weeks = 0
        low_coverage_weeks = 0
        for full_week in full_partition_weeks:
            start_pos = int(np.searchsorted(index_ns, full_week.value, side="left"))
            end_pos = int(
                np.searchsorted(
                    index_ns,
                    (full_week + pd.Timedelta(days=7)).value,
                    side="left",
                )
            )
            count = end_pos - start_pos
            nominal = float(_segment_for_week(full_week)["nominal"])
            if count == 0:
                missing_weeks += 1
            if count < 0.50 * nominal:
                low_coverage_weeks += 1
        gap_diagnostic = _partition_gap_diagnostic(observed_index)
        if gap_diagnostic is not None and (
            global_gap is None
            or float(gap_diagnostic["seconds"]) > float(global_gap["seconds"])
        ):
            global_gap = {"partition": partition, **gap_diagnostic}
        partition_details[partition] = {
            "start_ts": start.isoformat(),
            "end_ts": end.isoformat(),
            "observed_rows": observed,
            "expected_rows_from_segmented_session_schedule": float(expected_point),
            # Compatibility alias consumed by v1 provenance readers.  The v2
            # method field prevents this from being mistaken for a train-only
            # estimate when artifacts are authenticated.
            "expected_rows_from_train_session_rate": float(expected_point),
            "coverage": coverage[partition],
            "conservative_coverage_lower_bound": (
                float(lower_bound) if lower_bound is not None else None
            ),
            "complete_calendar_weeks": int(len(full_partition_weeks)),
            "missing_calendar_weeks": int(missing_weeks),
            "weeks_below_half_expected_schedule": int(low_coverage_weeks),
            "largest_unexplained_gap": gap_diagnostic,
        }

    train_start, train_end = windows["optimizer_train"]
    complete_train_weeks = int(
        sum(
            1
            for week in week_starts
            if week >= train_start and week + pd.Timedelta(days=7) <= train_end
        )
    )
    details: Dict[str, Any] = {
        "method": method,
        "status": "estimated",
        "performance_inputs_used": False,
        "timestamp_only": True,
        "schedule_source": "all_requested_calendar_weeks_timestamp_only",
        "conservative_bound_method": "segment_empirical_max_v2",
        "conservative_bound_family_alpha": None,
        "complete_calendar_weeks": int(len(week_starts)),
        "complete_train_weeks": complete_train_weeks,
        "schedule_segments": segment_records,
        "change_points": [accepted_changes[key] for key in sorted(accepted_changes)],
        "conservative_lower_bound_available": bool(lower_bound_available),
        "largest_unexplained_gap": global_gap,
        "partitions": partition_details,
        "caveat": (
            "Expected sessions are inferred only from recurring broker timestamps. "
            "Missing weeks remain expected. A stable systematic omission cannot be "
            "distinguished from a genuine schedule reduction without an authenticated "
            "exchange or broker calendar."
        ),
    }
    return coverage, details


def segmented_session_coverage_by_partition_v2(
    frame: pd.DataFrame,
    partition_windows: Mapping[str, Tuple[datetime, datetime]],
) -> Tuple[Dict[str, Optional[float]], Dict[str, Any]]:
    """Reproduce the immutable v2 maximum-week coverage contract."""
    return _segmented_session_coverage_by_partition(
        frame,
        partition_windows,
        method=SEGMENTED_SESSION_COVERAGE_METHOD_V2,
    )


def _safe_name(value: str, label: str) -> str:
    value = str(value).strip()
    if not _SAFE_COMPONENT.fullmatch(value) or value in {".", ".."}:
        raise ValueError(f"unsafe {label}: {value!r}")
    return value


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.generic):
        return _json_safe(value.item())
    return str(value)


def _bounded_error_diagnostic(
    error: BaseException | None = None,
    *,
    error_type: Optional[str] = None,
    message: Optional[str] = None,
) -> Dict[str, str]:
    """Serialize one bounded diagnostic without granting it trading authority."""

    observed_type = error_type or (
        type(error).__name__ if error is not None else "Error"
    )
    if isinstance(message, str):
        observed_message = message
    elif error is None:
        observed_message = ""
    else:
        arguments = getattr(error, "args", ())
        observed_message = (
            arguments[0]
            if isinstance(arguments, tuple)
            and arguments
            and isinstance(arguments[0], str)
            else "exception message unavailable"
        )
    return {
        "type": observed_type[:256] if isinstance(observed_type, str) else "Error",
        "message": observed_message[:4096],
    }


def _authenticated_available_history_boundary(
    *,
    page_attempts: Sequence[Mapping[str, Any]],
    terminal_prefix_pages: Sequence[Mapping[str, Any]],
    empty_pages: Sequence[Mapping[str, Any]],
    failed_pages: Sequence[Mapping[str, Any]],
    actual_first_open_utc: datetime,
) -> Optional[Dict[str, Any]]:
    """Derive, never accept, a broker-native available-suffix boundary.

    MT5 sometimes returns the first available bar as a one-row, wholly newer
    sentinel for a range that predates broker history. That response alone is
    ambiguous with an incomplete terminal cache. The boundary becomes usable
    only when all three bounded V4 calls return the same one-row sentinel and
    the immediately following exact page plus the finalized CSV begin at that
    identical open. This authenticates the available suffix; it does not
    assert an exchange listing date or statistical sufficiency. The proof is
    a source fact independent of whether that boundary precedes or intersects
    the cell's own strategy-evidence interval.
    """

    if (
        len(terminal_prefix_pages) != 1
        or len(page_attempts) < 2
        or empty_pages
        or failed_pages
    ):
        return None
    terminal_page = terminal_prefix_pages[0]
    if dict(terminal_page) != dict(page_attempts[0]):
        return None
    try:
        terminal_start = parse_utc_datetime(str(terminal_page["start_utc"]))
        terminal_end = parse_utc_datetime(str(terminal_page["end_exclusive_utc"]))
        first_data_page = page_attempts[1]
        first_data_start = parse_utc_datetime(str(first_data_page["start_utc"]))
        first_data_end = parse_utc_datetime(str(first_data_page["end_exclusive_utc"]))
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        return None
    if (
        terminal_end != first_data_start
        or not terminal_start < terminal_end < first_data_end
    ):
        return None

    diagnostics = terminal_page.get("connector_diagnostics")
    if not isinstance(diagnostics, Mapping):
        return None
    ledger = diagnostics.get("call_ledger")
    if not isinstance(ledger, list) or len(ledger) != 3:
        return None
    boundary_epoch: Optional[int] = None
    for entry in ledger:
        if not isinstance(entry, Mapping):
            return None
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
            return None
        observed_epoch = int(extent["first_epoch_second"])
        if boundary_epoch is None:
            boundary_epoch = observed_epoch
        elif observed_epoch != boundary_epoch:
            return None
    if boundary_epoch is None:
        return None
    top_extent = diagnostics.get("raw_extent")
    if (
        diagnostics.get("initial_raw_relation") != "RIGHT_DISJOINT_PREFIX"
        or diagnostics.get("confirmation_raw_relation") != "RIGHT_DISJOINT_PREFIX"
        or diagnostics.get("raw_relation") != "RIGHT_DISJOINT_PREFIX"
        or diagnostics.get("raw_count") != 1
        or not isinstance(top_extent, Mapping)
        or top_extent.get("rows") != 1
        or top_extent.get("first_epoch_second") != boundary_epoch
        or top_extent.get("last_epoch_second") != boundary_epoch
    ):
        return None

    next_diagnostics = first_data_page.get("connector_diagnostics")
    next_extent = (
        next_diagnostics.get("admitted_extent")
        if isinstance(next_diagnostics, Mapping)
        else None
    )
    if (
        not isinstance(next_extent, Mapping)
        or type(next_diagnostics.get("admitted_count")) is not int
        or next_diagnostics.get("admitted_count", 0) <= 0
        or next_extent.get("first_epoch_second") != boundary_epoch
    ):
        return None
    boundary = datetime.fromtimestamp(boundary_epoch, tz=timezone.utc)
    if (
        boundary != _utc(actual_first_open_utc)
        or not terminal_end <= boundary < first_data_end
    ):
        return None
    return {
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


def _canonical_json_sha256(value: Any) -> str:
    """Hash one JSON-safe contract without platform/formatting ambiguity."""

    payload = json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _count_csv_rows(path: Path) -> int:
    """Count this tool's one-record-per-line CSV without loading it in memory."""

    newline_count = 0
    last_byte = b""
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            size += len(chunk)
            newline_count += chunk.count(b"\n")
            last_byte = chunk[-1:]
    if size == 0:
        raise ResumeValidationError(f"CSV is empty: {path}")
    line_count = newline_count + (0 if last_byte == b"\n" else 1)
    if line_count < 1:
        raise ResumeValidationError(f"CSV has no header: {path}")
    return line_count - 1


class _SegmentedSessionCoverageAccumulator:
    """O(calendar-weeks) sufficient statistics for timestamp-only V2 coverage."""

    _DAY_NS = 86_400 * 1_000_000_000
    _WEEK_NS = 7 * _DAY_NS

    def __init__(
        self,
        partition_windows: Mapping[str, Tuple[datetime, datetime]],
    ) -> None:
        self.valid = set(partition_windows) == set(PARTITIONS)
        try:
            self.windows = {
                partition: (pd.Timestamp(_utc(start)), pd.Timestamp(_utc(end)))
                for partition, (start, end) in partition_windows.items()
            }
        except (TypeError, ValueError, KeyError):
            self.windows = {}
            self.valid = False
        if self.valid and any(start >= end for start, end in self.windows.values()):
            self.valid = False
        if self.valid:
            self.envelope_start = min(start for start, _ in self.windows.values())
            self.envelope_end = max(end for _, end in self.windows.values())
            first = _monday_floor_utc(self.envelope_start)
            if first < self.envelope_start:
                first += pd.Timedelta(days=7)
            last = _monday_floor_utc(self.envelope_end)
            if last + pd.Timedelta(days=7) > self.envelope_end:
                last -= pd.Timedelta(days=7)
            self.week_starts = (
                pd.DatetimeIndex([], tz="UTC")
                if last < first
                else pd.DatetimeIndex(pd.date_range(first, last, freq="7D"))
            )
        else:
            self.envelope_start = pd.Timestamp(0, tz="UTC")
            self.envelope_end = pd.Timestamp(0, tz="UTC")
            self.week_starts = pd.DatetimeIndex([], tz="UTC")
        self.weekly_counts = np.zeros(len(self.week_starts), dtype="int64")
        self.weekly_gap_max: Dict[int, Tuple[float, int, int]] = {}
        self.partition_observed = {partition: 0 for partition in PARTITIONS}
        self.partition_gap_max: Dict[str, Dict[int, Tuple[float, int, int]]] = {
            partition: {} for partition in PARTITIONS
        }
        self.previous_envelope_ns: Optional[int] = None
        self.previous_partition_ns: Dict[str, Optional[int]] = {
            partition: None for partition in PARTITIONS
        }
        self.rows_seen = 0

    @classmethod
    def _monday_ns(cls, values: np.ndarray) -> np.ndarray:
        days = np.floor_divide(values, cls._DAY_NS)
        weekdays = np.mod(days + 3, 7)  # 1970-01-01 was Thursday.
        return (days - weekdays) * cls._DAY_NS

    @classmethod
    def _update_gap_map(
        cls,
        target: Dict[int, Tuple[float, int, int]],
        opens_ns: np.ndarray,
        previous_ns: Optional[int],
    ) -> Optional[int]:
        if opens_ns.size == 0:
            return previous_ns
        if previous_ns is None:
            left = opens_ns[:-1]
            right = opens_ns[1:]
        else:
            left = np.concatenate(
                (np.asarray([previous_ns], dtype="int64"), opens_ns[:-1])
            )
            right = opens_ns
        if right.size:
            gaps = (right - left).astype("float64") / 1_000_000_000.0
            right_weeks = cls._monday_ns(right)
            for week_key in np.unique(right_weeks):
                positions = np.flatnonzero(right_weeks == week_key)
                local_gaps = gaps[positions]
                local_pos = int(positions[int(np.argmax(local_gaps))])
                candidate = (
                    float(gaps[local_pos]),
                    int(left[local_pos]),
                    int(right[local_pos]),
                )
                observed = target.get(int(week_key))
                if observed is None or candidate[0] > observed[0]:
                    target[int(week_key)] = candidate
        return int(opens_ns[-1])

    def feed(self, frame: pd.DataFrame) -> None:
        if not self.valid or frame.empty:
            return
        opens = pd.DatetimeIndex(frame.index)
        closes = pd.DatetimeIndex(
            pd.to_datetime(frame["_BAR_CLOSE_TS"], utc=True, errors="coerce")
        )
        open_ns = opens.asi8
        close_ns = closes.asi8
        envelope_mask = (opens >= self.envelope_start) & (opens < self.envelope_end)
        envelope_ns = open_ns[np.asarray(envelope_mask)]
        self.rows_seen += int(envelope_ns.size)
        if envelope_ns.size and len(self.week_starts):
            positions = np.floor_divide(
                envelope_ns - int(self.week_starts[0].value), self._WEEK_NS
            )
            valid = (positions >= 0) & (positions < len(self.week_starts))
            if np.any(valid):
                self.weekly_counts += np.bincount(
                    positions[valid].astype("int64"), minlength=len(self.week_starts)
                )
        self.previous_envelope_ns = self._update_gap_map(
            self.weekly_gap_max, envelope_ns, self.previous_envelope_ns
        )
        nat_ns = np.iinfo("int64").min
        for partition in PARTITIONS:
            start, end = self.windows[partition]
            mask = (
                (opens >= start)
                & (opens < end)
                & (close_ns != nat_ns)
                & (close_ns > open_ns)
                & (close_ns <= int(end.value))
            )
            admitted = open_ns[np.asarray(mask)]
            self.partition_observed[partition] += int(admitted.size)
            self.previous_partition_ns[partition] = self._update_gap_map(
                self.partition_gap_max[partition],
                admitted,
                self.previous_partition_ns[partition],
            )

    def finish(
        self,
        *,
        method: str = SEGMENTED_SESSION_COVERAGE_METHOD_V2,
    ) -> Tuple[Dict[str, Optional[float]], Dict[str, Any]]:
        return _finish_segmented_session_accumulator(self, method=method)


def _finish_segmented_session_accumulator(
    state: _SegmentedSessionCoverageAccumulator,
    *,
    method: str,
) -> Tuple[Dict[str, Optional[float]], Dict[str, Any]]:
    """Reproduce V2 from bounded weekly/gap aggregates, without bar arrays."""

    empty_coverage = {partition: None for partition in PARTITIONS}

    def failure(status: str, **extra: Any):
        return dict(empty_coverage), {
            "method": method,
            "status": status,
            "performance_inputs_used": False,
            "timestamp_only": True,
            **extra,
        }

    if not state.valid:
        return failure("invalid_partition_windows")
    if state.rows_seen <= 0:
        return failure("requested_calendar_empty")
    week_starts = state.week_starts
    weekly_counts = state.weekly_counts
    if len(week_starts) < _SESSION_V2_MIN_EVIDENCE_WEEKS:
        return failure(
            "fewer_than_26_complete_calendar_weeks",
            complete_calendar_weeks=int(len(week_starts)),
        )
    positive_weeks = int((weekly_counts > 0).sum())
    if positive_weeks < _SESSION_V2_MIN_EVIDENCE_WEEKS:
        return failure(
            "fewer_than_26_nonempty_calendar_weeks",
            complete_calendar_weeks=int(len(week_starts)),
            nonempty_calendar_weeks=positive_weeks,
        )

    def window_nominal(values: np.ndarray) -> Optional[Tuple[float, float]]:
        positive = values[values > 0]
        minimum_support = max(13, int(math.ceil(0.60 * len(values))))
        if positive.size < minimum_support:
            return None
        nominal = _higher_quantile(positive, 0.75)
        if not math.isfinite(nominal) or nominal <= 0.0:
            return None
        consistent = float(
            np.mean((positive >= 0.80 * nominal) & (positive <= 1.20 * nominal))
        )
        return (nominal, consistent) if consistent >= 0.65 else None

    accepted_changes: Dict[int, Dict[str, Any]] = {}

    def split_segment(start_pos: int, end_pos: int) -> List[Tuple[int, int]]:
        if end_pos - start_pos < 2 * _SESSION_V2_MIN_SEGMENT_WEEKS:
            return [(start_pos, end_pos)]
        best: Optional[Tuple[float, int, float, float, float, float]] = None
        for split_pos in range(
            start_pos + _SESSION_V2_MIN_SEGMENT_WEEKS,
            end_pos - _SESSION_V2_MIN_SEGMENT_WEEKS + 1,
        ):
            left = weekly_counts[
                max(start_pos, split_pos - _SESSION_V2_CHANGE_WINDOW_WEEKS) : split_pos
            ]
            right = weekly_counts[
                split_pos : min(end_pos, split_pos + _SESSION_V2_CHANGE_WINDOW_WEEKS)
            ]
            left_estimate = window_nominal(left)
            right_estimate = window_nominal(right)
            if left_estimate is None or right_estimate is None:
                continue
            left_nominal, left_consistency = left_estimate
            right_nominal, right_consistency = right_estimate
            relative_change = abs(left_nominal - right_nominal) / max(
                left_nominal, right_nominal
            )
            if (
                abs(left_nominal - right_nominal) < 1.0
                or relative_change < _SESSION_V2_MIN_CHANGE_FRACTION
            ):
                continue
            support = min(
                float(np.mean(left > 0)),
                float(np.mean(right > 0)),
                left_consistency,
                right_consistency,
            )
            score = relative_change * support
            candidate = (
                score,
                split_pos,
                left_nominal,
                right_nominal,
                relative_change,
                support,
            )
            if (
                best is None
                or score > best[0] + 1e-12
                or (
                    math.isclose(score, best[0], rel_tol=0.0, abs_tol=1e-12)
                    and split_pos < best[1]
                )
            ):
                best = candidate
        if best is None:
            return [(start_pos, end_pos)]
        _, split_pos, left_nominal, right_nominal, relative_change, support = best
        accepted_changes[split_pos] = {
            "effective_week_start_utc": week_starts[split_pos].isoformat(),
            "left_nominal_bars_per_week": float(left_nominal),
            "right_nominal_bars_per_week": float(right_nominal),
            "relative_change": float(relative_change),
            "minimum_local_support": float(support),
            "local_window_weeks": _SESSION_V2_CHANGE_WINDOW_WEEKS,
        }
        return split_segment(start_pos, split_pos) + split_segment(split_pos, end_pos)

    segment_bounds = split_segment(0, len(week_starts))
    segment_records: List[Dict[str, Any]] = []
    segment_models: List[Dict[str, Any]] = []
    lower_bound_available = True
    for segment_id, (start_pos, end_pos) in enumerate(segment_bounds):
        counts = weekly_counts[start_pos:end_pos]
        positive = counts[counts > 0]
        nominal = _higher_quantile(positive, 0.75)
        empirical_max = float(np.max(positive))
        nonempty_fraction = float(positive.size / max(1, counts.size))
        healthy_fraction = float(
            np.mean((counts >= 0.80 * nominal) & (counts <= 1.20 * nominal))
        )
        bound_defensible = bool(
            counts.size >= _SESSION_V2_MIN_EVIDENCE_WEEKS
            and positive.size >= 20
            and nonempty_fraction >= 0.75
            and empirical_max <= 1.15 * nominal
        )
        lower_bound_available = lower_bound_available and bound_defensible
        healthy_week_set = set(
            week_starts[start_pos:end_pos][counts >= 0.80 * nominal].asi8.tolist()
        )
        weekly_gap_maxima = [
            state.weekly_gap_max[key][0]
            for key in healthy_week_set
            if key in state.weekly_gap_max
        ]
        recurring_max_gap = (
            _higher_quantile(np.asarray(weekly_gap_maxima, dtype="float64"), 0.90)
            if len(weekly_gap_maxima) >= 13
            else None
        )
        segment_start = week_starts[start_pos]
        segment_end = week_starts[end_pos - 1] + pd.Timedelta(days=7)
        model = {
            "segment_id": segment_id,
            "start": segment_start,
            "end": segment_end,
            "nominal": float(nominal),
            "upper": empirical_max,
            "bound_defensible": bound_defensible,
            "recurring_max_gap": (
                float(recurring_max_gap) if recurring_max_gap is not None else None
            ),
        }
        segment_models.append(model)
        segment_records.append(
            {
                "segment_id": segment_id,
                "start_week_utc": segment_start.isoformat(),
                "end_week_exclusive_utc": segment_end.isoformat(),
                "calendar_weeks": int(counts.size),
                "nonempty_weeks": int(positive.size),
                "zero_count_weeks": int((counts == 0).sum()),
                "nominal_bars_per_week_q75": float(nominal),
                "empirical_schedule_envelope_bars_per_week": empirical_max,
                "empirical_max_bars_per_week": empirical_max,
                "upper_mean_bound_method": "segment_max_v2",
                "upper_mean_bound_family_alpha": None,
                "upper_mean_bound_radius": None,
                "nonempty_week_fraction": nonempty_fraction,
                "healthy_week_fraction": healthy_fraction,
                "conservative_lower_bound_defensible": bound_defensible,
                "recurring_max_gap_seconds_q90": (
                    float(recurring_max_gap) if recurring_max_gap is not None else None
                ),
            }
        )

    def segment_for_week(week_start: pd.Timestamp) -> Dict[str, Any]:
        for model in segment_models:
            if model["start"] <= week_start < model["end"]:
                return model
        return (
            segment_models[0]
            if week_start < segment_models[0]["start"]
            else segment_models[-1]
        )

    def gap_diagnostic(partition: str) -> Optional[Dict[str, Any]]:
        best: Optional[Tuple[float, int, float, float, int]] = None
        for observed_gap, left_ns, right_ns in sorted(
            state.partition_gap_max[partition].values(), key=lambda value: value[2]
        ):
            week_ns = int(state._monday_ns(np.asarray([right_ns], dtype="int64"))[0])
            baseline = segment_for_week(pd.Timestamp(week_ns, tz="UTC")).get(
                "recurring_max_gap"
            )
            if baseline is None:
                continue
            unexplained = max(0.0, observed_gap - float(baseline))
            if (
                best is None
                or unexplained > best[0]
                or (
                    math.isclose(unexplained, best[0], rel_tol=0.0, abs_tol=1e-9)
                    and observed_gap > best[3]
                )
            ):
                best = (unexplained, left_ns, float(baseline), observed_gap, right_ns)
        if best is None:
            return None
        unexplained, left_ns, baseline, observed_gap, right_ns = best
        return {
            "seconds": float(unexplained),
            "gap_start_utc": pd.Timestamp(left_ns, tz="UTC").isoformat(),
            "gap_end_utc": pd.Timestamp(right_ns, tz="UTC").isoformat(),
            "observed_gap_seconds": float(observed_gap),
            "recurring_schedule_gap_seconds": float(baseline),
        }

    coverage: Dict[str, Optional[float]] = {}
    partition_details: Dict[str, Any] = {}
    global_gap: Optional[Dict[str, Any]] = None
    week_count_by_key = {
        int(week): int(count) for week, count in zip(week_starts.asi8, weekly_counts)
    }
    for partition in PARTITIONS:
        start, end = state.windows[partition]
        observed = int(state.partition_observed[partition])
        expected_point = 0.0
        expected_upper = 0.0
        bound_is_available = True
        week_start = _monday_floor_utc(start)
        while week_start < end:
            week_end = week_start + pd.Timedelta(days=7)
            overlap = max(
                0.0,
                (min(end, week_end) - max(start, week_start)).total_seconds()
                / (7.0 * 86_400.0),
            )
            model = segment_for_week(week_start)
            expected_point += float(model["nominal"]) * overlap
            if model["bound_defensible"]:
                expected_upper += float(model["upper"]) * overlap
            else:
                bound_is_available = False
            week_start = week_end
        point = min(1.0, observed / expected_point) if expected_point > 0 else None
        lower_bound = (
            min(1.0, observed / expected_upper)
            if bound_is_available and expected_upper > 0
            else None
        )
        coverage[partition] = float(point) if point is not None else None
        first_full = _monday_floor_utc(start)
        if first_full < start:
            first_full += pd.Timedelta(days=7)
        full_weeks: List[pd.Timestamp] = []
        cursor = first_full
        while cursor + pd.Timedelta(days=7) <= end:
            full_weeks.append(cursor)
            cursor += pd.Timedelta(days=7)
        missing_weeks = 0
        low_weeks = 0
        for week in full_weeks:
            count = week_count_by_key.get(int(week.value), 0)
            nominal = float(segment_for_week(week)["nominal"])
            missing_weeks += int(count == 0)
            low_weeks += int(count < 0.50 * nominal)
        diagnostic = gap_diagnostic(partition)
        if diagnostic is not None and (
            global_gap is None
            or float(diagnostic["seconds"]) > float(global_gap["seconds"])
        ):
            global_gap = {"partition": partition, **diagnostic}
        partition_details[partition] = {
            "start_ts": start.isoformat(),
            "end_ts": end.isoformat(),
            "observed_rows": observed,
            "expected_rows_from_segmented_session_schedule": float(expected_point),
            "expected_rows_from_train_session_rate": float(expected_point),
            "coverage": coverage[partition],
            "conservative_coverage_lower_bound": (
                float(lower_bound) if lower_bound is not None else None
            ),
            "complete_calendar_weeks": int(len(full_weeks)),
            "missing_calendar_weeks": int(missing_weeks),
            "weeks_below_half_expected_schedule": int(low_weeks),
            "largest_unexplained_gap": diagnostic,
        }
    train_start, train_end = state.windows["optimizer_train"]
    complete_train_weeks = int(
        sum(
            1
            for week in week_starts
            if week >= train_start and week + pd.Timedelta(days=7) <= train_end
        )
    )
    return coverage, {
        "method": method,
        "status": "estimated",
        "performance_inputs_used": False,
        "timestamp_only": True,
        "schedule_source": "all_requested_calendar_weeks_timestamp_only",
        "conservative_bound_method": "segment_empirical_max_v2",
        "conservative_bound_family_alpha": None,
        "complete_calendar_weeks": int(len(week_starts)),
        "complete_train_weeks": complete_train_weeks,
        "schedule_segments": segment_records,
        "change_points": [accepted_changes[key] for key in sorted(accepted_changes)],
        "conservative_lower_bound_available": bool(lower_bound_available),
        "largest_unexplained_gap": global_gap,
        "partitions": partition_details,
        "caveat": (
            "Expected sessions are inferred only from recurring broker timestamps. "
            "Missing weeks remain expected. A stable systematic omission cannot be "
            "distinguished from a genuine schedule reduction without an authenticated "
            "exchange or broker calendar."
        ),
    }


@dataclass(frozen=True)
class _InternalSpillIdentity:
    device: int
    inode: int
    size: int
    mtime_ns: int
    links: int
    mode: int


def _internal_spill_identity(path: Path) -> _InternalSpillIdentity:
    observed = path.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or int(getattr(observed, "st_file_attributes", 0) or 0)
        & _FILE_ATTRIBUTE_REPARSE_POINT
        or int(observed.st_nlink) != 1
    ):
        raise NativePageAssemblyResourceLimitError(
            {
                "status": "limit_exceeded",
                "violations": ["assembly_spill_identity_unsafe"],
                "path": str(path),
            }
        )
    return _InternalSpillIdentity(
        device=int(observed.st_dev),
        inode=int(observed.st_ino),
        size=int(observed.st_size),
        mtime_ns=int(observed.st_mtime_ns),
        links=int(observed.st_nlink),
        mode=int(observed.st_mode),
    )


def _same_internal_spill_identity(
    expected: _InternalSpillIdentity, observed: os.stat_result
) -> bool:
    return (
        expected.device,
        expected.inode,
        expected.size,
        expected.mtime_ns,
        expected.links,
        expected.mode,
    ) == (
        int(observed.st_dev),
        int(observed.st_ino),
        int(observed.st_size),
        int(observed.st_mtime_ns),
        int(observed.st_nlink),
        int(observed.st_mode),
    )


def _best_effort_bounded_flat_cleanup(path: Path, *, maximum_entries: int) -> None:
    """Remove only a finite, flat, regular-file staging directory."""

    try:
        observed_root = path.lstat()
    except OSError:
        return
    if _unsafe_native_file_attributes(observed_root) or not stat.S_ISDIR(
        observed_root.st_mode
    ):
        return
    try:
        with os.scandir(path) as scanner:
            for count, entry in enumerate(scanner, start=1):
                if count > int(maximum_entries):
                    return
                observed = entry.stat(follow_symlinks=False)
                if (
                    entry.is_symlink()
                    or _unsafe_native_file_attributes(observed)
                    or not stat.S_ISREG(observed.st_mode)
                ):
                    return
                (path / entry.name).unlink(missing_ok=True)
        path.rmdir()
    except OSError:
        return


@dataclass
class _NativeCellCsvArtifact:
    workspace: Path
    csv_path: Path
    identity: _InternalSpillIdentity
    scan: NativeCsvScanResult
    session_coverage: Dict[str, Optional[float]]
    session_details: Dict[str, Any]
    out_of_range_rows: int
    peak_in_process_rows: int
    peak_in_process_bytes: int
    peak_spill_bytes: int
    consumed: bool = False

    @property
    def rows(self) -> int:
        return int(self.scan.artifact.rows)

    @property
    def bytes(self) -> int:
        return int(self.scan.artifact.bytes)

    @property
    def start_ts(self) -> pd.Timestamp:
        return pd.Timestamp(self.scan.artifact.first_open_utc)

    @property
    def end_ts(self) -> pd.Timestamp:
        return pd.Timestamp(self.scan.artifact.last_open_utc)

    @property
    def columns(self) -> Tuple[str, ...]:
        return tuple(self.scan.artifact.columns)

    @property
    def empty(self) -> bool:
        return self.rows <= 0

    def verify(self) -> None:
        if self.consumed:
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_artifact_already_consumed"],
                }
            )
        observed = self.csv_path.lstat()
        if not _same_internal_spill_identity(self.identity, observed):
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_artifact_identity_changed"],
                    "path": str(self.csv_path),
                }
            )
        if _sha256(self.csv_path) != self.scan.artifact.sha256:
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_artifact_digest_changed"],
                    "path": str(self.csv_path),
                }
            )

    def cleanup(self) -> None:
        _best_effort_bounded_flat_cleanup(
            self.workspace,
            maximum_entries=MAX_PAGES_PER_CELL + 4,
        )


@dataclass(frozen=True)
class _NativeSpillPage:
    path: Path
    identity: _InternalSpillIdentity
    sha256: str
    start: datetime
    end: datetime
    first_ns: int
    last_ns: int
    rows: int
    dtypes: Tuple[Any, ...]


class _NativePageSpillAssembly:
    """Create-only internal page spill and one-page-at-a-time CSV finalizer."""

    RAW_COLUMNS = tuple(PRODUCTION_ARTIFACT_COLUMNS[:-3])

    def __init__(self, run_root: Path, symbol: str, timeframe: str) -> None:
        prefix = (
            f".{_safe_name(symbol, 'symbol')}.{_safe_name(timeframe, 'timeframe')}."
        )
        self.workspace = Path(
            tempfile.mkdtemp(prefix=prefix, suffix=".assembly.staging", dir=run_root)
        )
        try:
            os.chmod(self.workspace, 0o700)
        except OSError:
            pass
        root_stat = self.workspace.lstat()
        if self.workspace.is_symlink() or not stat.S_ISDIR(root_stat.st_mode):
            self.cleanup()
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_workspace_unsafe"],
                }
            )
        self.workspace_identity = (int(root_stat.st_dev), int(root_stat.st_ino))
        self.pages: List[_NativeSpillPage] = []
        self.pre_dedup_rows = 0
        self.duplicate_rows = 0
        self.peak_in_process_rows = 0
        self.peak_in_process_bytes = 0
        self.current_spill_bytes = 0
        self.peak_spill_bytes = 0
        self._dtype_inputs: Tuple[List[Any], ...] = tuple([] for _ in self.RAW_COLUMNS)

    def cleanup(self) -> None:
        workspace = getattr(self, "workspace", None)
        if isinstance(workspace, Path):
            _best_effort_bounded_flat_cleanup(
                workspace,
                maximum_entries=MAX_PAGES_PER_CELL + 4,
            )

    def _verify_workspace(self) -> None:
        observed = self.workspace.lstat()
        if (
            self.workspace.is_symlink()
            or not stat.S_ISDIR(observed.st_mode)
            or (int(observed.st_dev), int(observed.st_ino)) != self.workspace_identity
        ):
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_workspace_identity_changed"],
                }
            )

    def _enforce_allocated_storage(self, *, page_bytes: int = 0) -> None:
        violations: List[str] = []
        if page_bytes > MAX_NATIVE_PAGE_SPILL_BYTES:
            violations.append("assembly_page_spill_byte_cap_exceeded")
        if self.current_spill_bytes > MAX_NATIVE_ASSEMBLY_SPILL_BYTES:
            violations.append("assembly_cumulative_spill_byte_cap_exceeded")
        try:
            usage = shutil.disk_usage(self.workspace)
        except OSError as exc:
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_disk_usage_unavailable"],
                    "error": _bounded_error_diagnostic(exc),
                }
            ) from exc
        if int(usage.free) < OUTPUT_MIN_FREE_BYTES:
            violations.append("assembly_output_disk_free_reserve_breached")
        if violations:
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": violations,
                    "maximum_page_spill_bytes": MAX_NATIVE_PAGE_SPILL_BYTES,
                    "maximum_cumulative_spill_bytes": (MAX_NATIVE_ASSEMBLY_SPILL_BYTES),
                    "current_cumulative_spill_bytes": self.current_spill_bytes,
                    "page_spill_bytes": int(page_bytes),
                    "disk_free_bytes": int(usage.free),
                    "minimum_free_bytes": OUTPUT_MIN_FREE_BYTES,
                }
            )

    def add_page(
        self,
        frame: pd.DataFrame,
        page_start: datetime,
        page_end: datetime,
    ) -> None:
        self._verify_workspace()
        if len(self.pages) >= MAX_PAGES_PER_CELL:
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_page_count_cap_exceeded"],
                    "maximum_pages_per_cell": MAX_PAGES_PER_CELL,
                    "observed_page_count": len(self.pages) + 1,
                }
            )
        rows = int(len(frame))
        memory_bytes = int(frame.memory_usage(index=True, deep=True).sum())
        self.peak_in_process_rows = max(self.peak_in_process_rows, rows)
        self.peak_in_process_bytes = max(self.peak_in_process_bytes, memory_bytes)
        violations: List[str] = []
        if rows > MAX_NATIVE_PAGE_IN_PROCESS_ROWS:
            violations.append("assembly_page_in_process_row_cap_exceeded")
        if memory_bytes > MAX_NATIVE_PAGE_IN_PROCESS_BYTES:
            violations.append("assembly_page_in_process_byte_cap_exceeded")
        if violations:
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": violations,
                    "page_rows": rows,
                    "page_memory_bytes": memory_bytes,
                    "maximum_page_rows": MAX_NATIVE_PAGE_IN_PROCESS_ROWS,
                    "maximum_page_memory_bytes": MAX_NATIVE_PAGE_IN_PROCESS_BYTES,
                }
            )
        if tuple(str(column) for column in frame.columns) != self.RAW_COLUMNS:
            raise ValueError(
                "native page columns differ from the exact production artifact schema"
            )
        observed_dtypes = tuple(frame.dtypes)
        for inputs, observed in zip(self._dtype_inputs, observed_dtypes):
            inputs.append(observed)
        ordered = frame.sort_index(kind="stable")
        self.pre_dedup_rows += int(len(ordered))
        duplicates = int(ordered.index.duplicated(keep="last").sum())
        self.duplicate_rows += duplicates
        ordered = ordered[~ordered.index.duplicated(keep="last")]
        start_ts = pd.Timestamp(page_start)
        end_ts = pd.Timestamp(page_end)
        if (
            ordered.empty
            or bool((ordered.index < start_ts).any())
            or bool((ordered.index >= end_ts).any())
        ):
            raise ValueError("normalized page escaped its half-open admitted interval")
        token = uuid.uuid4().hex
        path = self.workspace / f"page-{token}.csv"
        try:
            with path.open("x", encoding="utf-8", newline="") as handle:
                ordered.to_csv(
                    handle,
                    index_label="time",
                    lineterminator="\n",
                )
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass
            identity = _internal_spill_identity(path)
            digest = _sha256(path)
            self.current_spill_bytes += identity.size
            self.peak_spill_bytes = max(self.peak_spill_bytes, self.current_spill_bytes)
            self._enforce_allocated_storage(page_bytes=identity.size)
            self.pages.append(
                _NativeSpillPage(
                    path=path,
                    identity=identity,
                    sha256=digest,
                    start=_utc(page_start),
                    end=_utc(page_end),
                    first_ns=int(ordered.index[0].value),
                    last_ns=int(ordered.index[-1].value),
                    rows=int(len(ordered)),
                    dtypes=observed_dtypes,
                )
            )
        except Exception:
            path.unlink(missing_ok=True)
            raise

    def _read_page(self, page: _NativeSpillPage) -> pd.DataFrame:
        self._verify_workspace()
        observed = page.path.lstat()
        if not _same_internal_spill_identity(page.identity, observed):
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_spill_identity_changed"],
                }
            )
        with page.path.open(
            "rb", buffering=NATIVE_VALIDATION_IO_BUFFER_BYTES
        ) as handle:
            if not _same_internal_spill_identity(
                page.identity, os.fstat(handle.fileno())
            ):
                raise NativePageAssemblyResourceLimitError(
                    {
                        "status": "limit_exceeded",
                        "violations": ["assembly_spill_open_identity_changed"],
                    }
                )
            digest = hashlib.sha256()
            observed_bytes = 0
            for chunk in iter(
                lambda: handle.read(NATIVE_VALIDATION_IO_BUFFER_BYTES), b""
            ):
                observed_bytes += len(chunk)
                if observed_bytes > MAX_NATIVE_PAGE_SPILL_BYTES:
                    raise NativePageAssemblyResourceLimitError(
                        {
                            "status": "limit_exceeded",
                            "violations": ["assembly_spill_byte_cap_exceeded"],
                        }
                    )
                digest.update(chunk)
            if (
                observed_bytes != page.identity.size
                or digest.hexdigest() != page.sha256
            ):
                raise NativePageAssemblyResourceLimitError(
                    {
                        "status": "limit_exceeded",
                        "violations": ["assembly_spill_digest_changed"],
                    }
                )
            handle.seek(0)
            # The spill is an internal lossless transport, not a fresh market
            # data parse.  Pandas' default fast float parser may move a valid
            # float64 by one ULP, which would make the final immutable artifact
            # differ from the broker-normalized page.  ``round_trip`` preserves
            # the exact binary values emitted by ``to_csv``.  The cap+1 row
            # bound also prevents a malformed authenticated spill from making
            # the parser materialize an unbounded row count before validation.
            frame = pd.read_csv(
                handle,
                index_col="time",
                parse_dates=["time"],
                float_precision="round_trip",
                nrows=page.rows + 1,
            )
            if not _same_internal_spill_identity(
                page.identity, os.fstat(handle.fileno())
            ):
                raise NativePageAssemblyResourceLimitError(
                    {
                        "status": "limit_exceeded",
                        "violations": ["assembly_spill_identity_changed_during_read"],
                    }
                )
            handle.seek(0)
            after_digest = hashlib.sha256()
            for chunk in iter(
                lambda: handle.read(NATIVE_VALIDATION_IO_BUFFER_BYTES), b""
            ):
                after_digest.update(chunk)
            if after_digest.hexdigest() != page.sha256:
                raise NativePageAssemblyResourceLimitError(
                    {
                        "status": "limit_exceeded",
                        "violations": ["assembly_spill_digest_changed_during_read"],
                    }
                )
        if tuple(str(column) for column in frame.columns) != self.RAW_COLUMNS:
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_spill_schema_changed"],
                }
            )
        try:
            for column, original_dtype in zip(self.RAW_COLUMNS, page.dtypes):
                frame[column] = frame[column].astype(original_dtype)
        except (TypeError, ValueError, OverflowError) as exc:
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_spill_dtype_binding_changed"],
                }
            ) from exc
        frame.index = pd.DatetimeIndex(
            pd.to_datetime(frame.index, utc=True, errors="raise")
        )
        memory_bytes = int(frame.memory_usage(index=True, deep=True).sum())
        if memory_bytes > MAX_NATIVE_PAGE_IN_PROCESS_BYTES:
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_spill_decoded_memory_cap_exceeded"],
                    "page_memory_bytes": memory_bytes,
                    "maximum_page_memory_bytes": MAX_NATIVE_PAGE_IN_PROCESS_BYTES,
                }
            )
        if int(len(frame)) != page.rows:
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_spill_row_binding_changed"],
                }
            )
        if (
            frame.empty
            or not frame.index.is_monotonic_increasing
            or not frame.index.is_unique
            or int(frame.index[0].value) != page.first_ns
            or int(frame.index[-1].value) != page.last_ns
            or bool((frame.index < pd.Timestamp(page.start)).any())
            or bool((frame.index >= pd.Timestamp(page.end)).any())
        ):
            raise NativePageAssemblyResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["assembly_spill_index_binding_changed"],
                }
            )
        return frame

    def finalize(
        self,
        *,
        timeframe: str,
        timeframe_seconds: int,
        broker_session_aligned: bool,
        native_start: datetime,
        fetch_end_exclusive: datetime,
        as_of_utc: datetime,
        partition_windows: Mapping[str, Tuple[datetime, datetime]],
        common_request: Mapping[str, Any],
        pagination: Mapping[str, Any],
    ) -> _NativeCellCsvArtifact:
        self._verify_workspace()
        pages = sorted(self.pages, key=lambda page: page.start)
        for older, newer in zip(pages, pages[1:]):
            if older.end > newer.start or older.last_ns >= newer.first_ns:
                raise ValueError(
                    "half-open page geometry cannot prove cross-page duplicate isolation"
                )
        csv_path = self.workspace / "assembled-bars.csv"
        accumulator = _SegmentedSessionCoverageAccumulator(partition_windows)
        target_dtypes = tuple(
            _pandas_find_common_type(inputs) for inputs in self._dtype_inputs
        )
        out_of_range_rows = 0
        rows_written = 0
        try:
            with csv_path.open("x", encoding="utf-8", newline="") as output:
                try:
                    os.chmod(csv_path, 0o600)
                except OSError:
                    pass
                for position, page in enumerate(pages):
                    frame = self._read_page(page)
                    frame = frame.astype(
                        {
                            column: dtype
                            for column, dtype in zip(self.RAW_COLUMNS, target_dtypes)
                        },
                        copy=False,
                    )
                    next_ns = (
                        pages[position + 1].first_ns
                        if position + 1 < len(pages)
                        else None
                    )
                    index_ns = frame.index.asi8
                    nominal_ns = int(timeframe_seconds * 1_000_000_000)
                    if broker_session_aligned:
                        close_ns = np.empty_like(index_ns)
                        if len(index_ns) > 1:
                            successors = index_ns[1:]
                            deltas = successors - index_ns[:-1]
                            regular = (deltas >= nominal_ns * 0.5) & (
                                deltas <= nominal_ns * 1.5
                            )
                            close_ns[:-1] = np.where(
                                regular, successors, index_ns[:-1] + nominal_ns
                            )
                        last_successor = (
                            int(next_ns)
                            if next_ns is not None
                            else int(index_ns[-1] + nominal_ns)
                        )
                        last_delta = last_successor - int(index_ns[-1])
                        close_ns[-1] = (
                            last_successor
                            if nominal_ns * 0.5 <= last_delta <= nominal_ns * 1.5
                            else int(index_ns[-1] + nominal_ns)
                        )
                        in_range = (frame.index >= pd.Timestamp(native_start)) & (
                            close_ns <= pd.Timestamp(as_of_utc).value
                        )
                        admitted = frame.loc[in_range].copy()
                        admitted_close_ns = close_ns[np.asarray(in_range)]
                    else:
                        in_range = (frame.index >= pd.Timestamp(native_start)) & (
                            frame.index < pd.Timestamp(fetch_end_exclusive)
                        )
                        admitted = frame.loc[in_range].copy()
                        admitted_close_ns = admitted.index.asi8 + nominal_ns
                    out_of_range_rows += int((~np.asarray(in_range)).sum())
                    if not admitted.empty:
                        projected_rows = rows_written + int(len(admitted))
                        if projected_rows > MAX_CSV_ROWS:
                            raise NativePageAssemblyResourceLimitError(
                                {
                                    "status": "limit_exceeded",
                                    "violations": ["assembly_output_row_cap_exceeded"],
                                    "observed_rows": projected_rows,
                                    "maximum_rows": MAX_CSV_ROWS,
                                }
                            )
                        admitted["_SOURCE_FIRST_TS"] = admitted.index
                        admitted["_SOURCE_LAST_TS"] = pd.to_datetime(
                            admitted_close_ns - 1, utc=True
                        )
                        admitted["_BAR_CLOSE_TS"] = pd.to_datetime(
                            admitted_close_ns, utc=True
                        )
                        admitted = admitted.loc[:, PRODUCTION_ARTIFACT_COLUMNS]
                        accumulator.feed(admitted)
                        admitted.to_csv(
                            output,
                            index_label="time",
                            lineterminator="\n",
                            header=(rows_written == 0),
                        )
                        rows_written += int(len(admitted))
                        output.flush()
                        if output.tell() > OUTPUT_MAX_CELL_BYTES:
                            raise OutputStorageResourceLimitError(
                                {
                                    "status": "limit_exceeded",
                                    "violations": ["output_cell_byte_cap_exceeded"],
                                    "actual_cell_bytes": int(output.tell()),
                                    "maximum_cell_bytes": OUTPUT_MAX_CELL_BYTES,
                                }
                            )
                    self.current_spill_bytes -= page.identity.size
                    page.path.unlink()
                    self._enforce_allocated_storage()
                    del admitted
                    del frame
                output.flush()
                os.fsync(output.fileno())
            if rows_written <= 0:
                raise NativeNoFullyClosedHistoryError(out_of_range_rows)
            identity = _internal_spill_identity(csv_path)
            if identity.size > OUTPUT_MAX_CELL_BYTES:
                raise OutputStorageResourceLimitError(
                    {
                        "status": "limit_exceeded",
                        "violations": ["output_cell_byte_cap_exceeded"],
                        "actual_cell_bytes": identity.size,
                        "maximum_cell_bytes": OUTPUT_MAX_CELL_BYTES,
                    }
                )
            with csv_path.open("rb") as handle:
                scan = scan_native_csv_stream(
                    handle,
                    expected_bytes=identity.size,
                    timeframe=timeframe,
                    common_request=common_request,
                    pagination=pagination,
                    maximum_bytes=MAX_CSV_BYTES,
                    maximum_rows=MAX_CSV_ROWS,
                    maximum_line_bytes=MAX_CSV_LINE_BYTES,
                )
            if scan.artifact.rows != rows_written:
                raise NativeCsvValidationError("ASSEMBLY_ROW_BINDING_MISMATCH")
            session_coverage, session_details = accumulator.finish()
            return _NativeCellCsvArtifact(
                workspace=self.workspace,
                csv_path=csv_path,
                identity=identity,
                scan=scan,
                session_coverage=session_coverage,
                session_details=session_details,
                out_of_range_rows=out_of_range_rows,
                peak_in_process_rows=self.peak_in_process_rows,
                peak_in_process_bytes=self.peak_in_process_bytes,
                peak_spill_bytes=self.peak_spill_bytes,
            )
        except Exception:
            self.cleanup()
            raise


def _fsync_file(path: Path) -> None:
    # Windows requires a writable descriptor for fsync/FlushFileBuffers.
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    """Durably flush one directory entry update on Windows or POSIX."""

    observed = path.lstat()
    if _unsafe_native_file_attributes(observed) or not stat.S_ISDIR(observed.st_mode):
        raise ResumeValidationError(f"directory durability target is unsafe: {path}")
    if os.name == "nt":
        import ctypes

        generic_write = 0x40000000
        share_all = 0x00000001 | 0x00000002 | 0x00000004
        open_existing = 3
        backup_semantics = 0x02000000
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateFileW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        kernel32.CreateFileW.restype = ctypes.c_void_p
        kernel32.FlushFileBuffers.argtypes = [ctypes.c_void_p]
        kernel32.FlushFileBuffers.restype = ctypes.c_int
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_int
        handle = kernel32.CreateFileW(
            str(path),
            generic_write,
            share_all,
            None,
            open_existing,
            backup_semantics,
            None,
        )
        invalid = ctypes.c_void_p(-1).value
        if handle in (None, invalid):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            if not kernel32.FlushFileBuffers(handle):
                raise ctypes.WinError(ctypes.get_last_error())
        finally:
            kernel32.CloseHandle(handle)
        return

    flags = os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0))
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class NativeHistoryRequest:
    symbols: Tuple[str, ...]
    timeframes: Tuple[str, ...] = DEFAULT_TIMEFRAMES
    horizon_years: int = 10
    as_of_utc: Optional[datetime] = None
    retries: int = 1
    retry_sleep_seconds: float = 0.0
    boundary_tolerance_days: float = 3.0
    window_years: Optional[Mapping[str, int]] = None
    timeframe_window_years: Optional[Mapping[str, Mapping[str, int]]] = None
    window_contract_version: Optional[str] = None
    embargo_days: float = 7.0
    warmup_days: float = 400.0
    safety_days: float = 14.0
    enforce_evidence_horizon: bool = True

    def __post_init__(self) -> None:
        requested_contract = str(self.window_contract_version or "").strip()
        has_timeframe_matrix = isinstance(self.timeframe_window_years, Mapping)
        if not requested_contract:
            requested_contract = (
                TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                if has_timeframe_matrix
                else LEGACY_FLAT_WINDOW_CONTRACT_VERSION
            )
        if requested_contract not in {
            LEGACY_FLAT_WINDOW_CONTRACT_VERSION,
            TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        }:
            raise ValueError(
                f"unsupported native window contract: {requested_contract!r}"
            )
        if (
            requested_contract == LEGACY_FLAT_WINDOW_CONTRACT_VERSION
            and has_timeframe_matrix
        ):
            raise ValueError(
                "legacy flat-window requests cannot include timeframe_window_years"
            )

        if requested_contract == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            normalized_symbols = canonical_v5_symbols(self.symbols)
            if len({symbol.casefold() for symbol in normalized_symbols}) != len(
                normalized_symbols
            ):
                raise ValueError(
                    "V5 symbols cannot alias on a case-insensitive filesystem"
                )
        else:
            normalized_symbols = tuple(
                _safe_name(str(item).upper(), "symbol") for item in self.symbols
            )
        normalized_timeframes = tuple(str(item).upper() for item in self.timeframes)
        if not normalized_symbols:
            raise ValueError("at least one symbol is required")
        if len(set(normalized_symbols)) != len(normalized_symbols):
            raise ValueError("duplicate symbols are not allowed")
        if not normalized_timeframes:
            raise ValueError("at least one timeframe is required")
        unknown = [
            item for item in normalized_timeframes if item not in TIMEFRAME_SECONDS
        ]
        if unknown:
            raise ValueError(f"unsupported native timeframe(s): {', '.join(unknown)}")
        if len(set(normalized_timeframes)) != len(normalized_timeframes):
            raise ValueError("duplicate timeframes are not allowed")
        if (
            type(self.horizon_years) is not int
            or not 1 <= self.horizon_years <= MAX_HORIZON_YEARS
        ):
            raise ValueError(
                f"horizon_years must be an integer in [1, {MAX_HORIZON_YEARS}]"
            )
        if type(self.retries) is not int or self.retries != 1:
            raise ValueError(
                "acquisition retries must equal 1; the MT5 connector owns its "
                "single bounded synchronization sequence"
            )
        if (
            isinstance(self.retry_sleep_seconds, bool)
            or type(self.retry_sleep_seconds) not in {int, float}
            or not math.isfinite(float(self.retry_sleep_seconds))
            or float(self.retry_sleep_seconds) != 0.0
        ):
            raise ValueError(
                "retry_sleep_seconds must equal 0; acquisition has no outer retry loop"
            )
        bounded_day_fields = (
            ("boundary_tolerance_days", self.boundary_tolerance_days, 365.0),
            ("embargo_days", self.embargo_days, 365.0),
            ("warmup_days", self.warmup_days, 3650.0),
            ("safety_days", self.safety_days, 3650.0),
        )
        for field_name, raw_value, maximum in bounded_day_fields:
            if (
                isinstance(raw_value, bool)
                or type(raw_value) not in {int, float}
                or not math.isfinite(float(raw_value))
                or not 0.0 <= float(raw_value) <= maximum
            ):
                raise ValueError(
                    f"{field_name} must be a finite number in [0, {maximum:g}]"
                )
        if self.window_years is not None and not isinstance(self.window_years, Mapping):
            raise ValueError("window_years must be a partition mapping")
        source_years = (
            self.window_years if isinstance(self.window_years, Mapping) else {}
        )
        if set(source_years) - set(PARTITIONS):
            raise ValueError("window_years contains unsupported partitions")
        normalized_years: Dict[str, int] = {}
        for partition in PARTITIONS:
            raw = source_years.get(partition, DEFAULT_WINDOW_YEARS[partition])
            if type(raw) is not int or not 1 <= raw <= MAX_PARTITION_YEARS:
                raise ValueError(
                    f"window_years[{partition!r}] must be an integer in "
                    f"[1, {MAX_PARTITION_YEARS}]"
                )
            normalized_years[partition] = raw
        normalized_timeframe_years: Optional[Dict[str, Dict[str, int]]] = None
        if requested_contract == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            if set(normalized_timeframes) != set(DEFAULT_TIMEFRAMES) or len(
                normalized_timeframes
            ) != len(DEFAULT_TIMEFRAMES):
                raise ValueError(
                    "the V5 timeframe evidence contract requires the exact "
                    f"six-timeframe roster {list(DEFAULT_TIMEFRAMES)!r}"
                )
            if self.timeframe_window_years:
                if not isinstance(self.timeframe_window_years, Mapping):
                    raise ValueError("timeframe_window_years must be a mapping")
                if set(self.timeframe_window_years) - set(DEFAULT_TIMEFRAMES):
                    raise ValueError(
                        "timeframe_window_years contains an unsupported timeframe"
                    )
                for timeframe, raw_windows in self.timeframe_window_years.items():
                    if not isinstance(raw_windows, Mapping):
                        raise ValueError(
                            f"timeframe_window_years[{timeframe!r}] must be a mapping"
                        )
                    if set(raw_windows) - set(PARTITIONS):
                        raise ValueError(
                            f"timeframe_window_years[{timeframe!r}] has unsupported partitions"
                        )
                    for partition, raw_years in raw_windows.items():
                        if (
                            type(raw_years) is not int
                            or not 1 <= raw_years <= MAX_PARTITION_YEARS
                        ):
                            raise ValueError(
                                "timeframe_window_years"
                                f"[{timeframe!r}][{partition!r}] must be an integer "
                                f"in [1, {MAX_PARTITION_YEARS}]"
                            )
            normalized_all = normalize_timeframe_window_years(
                self.timeframe_window_years
            )
            normalized_timeframe_years = {
                timeframe: dict(normalized_all[timeframe])
                for timeframe in normalized_timeframes
            }
            # Active tuning and final evaluation periods are deliberately
            # concurrent. Different training depth must not move the live
            # comparison windows for one timeframe relative to another.
            for partition in ("tuning_validation", "final_lockbox"):
                values = {
                    normalized_timeframe_years[timeframe][partition]
                    for timeframe in normalized_timeframes
                }
                if len(values) != 1:
                    raise ValueError(
                        f"timeframe {partition} years must be identical across "
                        "all requested timeframes"
                    )
        object.__setattr__(self, "symbols", normalized_symbols)
        object.__setattr__(self, "timeframes", normalized_timeframes)
        object.__setattr__(self, "window_years", normalized_years)
        object.__setattr__(self, "timeframe_window_years", normalized_timeframe_years)
        object.__setattr__(self, "window_contract_version", requested_contract)
        if self.as_of_utc is not None:
            object.__setattr__(self, "as_of_utc", _utc(self.as_of_utc))


_LEGACY_RESUME_REQUEST_KEYS: Tuple[str, ...] = (
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
    "window_years",
    "embargo_days",
    "common_calendar_boundaries",
)

_V3_RESUME_REQUEST_KEYS: Tuple[str, ...] = (
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
)

_V4_RESUME_REQUEST_KEYS: Tuple[str, ...] = (
    *_V3_RESUME_REQUEST_KEYS,
    "timeframe_strategy_evidence_intervals",
    "timeframe_audit_reference_map",
    "timeframe_audit_only_prefix_intervals",
)

_V5_RESUME_REQUEST_KEYS: Tuple[str, ...] = (
    *_V3_RESUME_REQUEST_KEYS,
    "timeframe_strategy_evidence_intervals",
    "timeframe_audit_reference_map",
    "timeframe_audit_consumer_map",
    "timeframe_audit_windows",
    "timeframe_audit_reference_intervals",
    "timeframe_audit_only_intervals",
    "timeframe_consensus_policy",
)

_SOURCE_IDENTITY_KEYS: Dict[str, Tuple[str, ...]] = {
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


@dataclass(frozen=True)
class NativeHistoryResumeContract:
    """The immutable common request and broker identity recovered from a run."""

    run_id: str
    request: Mapping[str, Any]
    source_identity: Mapping[str, Any]
    existing_cells: Tuple[Tuple[str, str], ...]
    symbols: Tuple[str, ...] = ()
    timeframes: Tuple[str, ...] = ()
    production_authoritative: bool = False
    acquisition_started_utc: Optional[str] = None
    contract_version: str = RESUME_CONTRACT_VERSION


def _normalized_native_root(native_root: Path) -> Path:
    root = Path(native_root)
    if tuple(part.lower() for part in root.parts[-2:]) != ("data", "native"):
        raise ValueError("native_root must end with data/native")
    resolved = root.resolve()
    if tuple(part.lower() for part in resolved.parts[-2:]) != ("data", "native"):
        raise ValueError("resolved native_root must remain inside a data/native tree")
    return resolved


def _pid_is_alive(pid: int) -> bool:
    """Conservatively determine whether a recorded lock-owner PID exists."""

    try:
        pid = int(pid)
    except (TypeError, ValueError, OverflowError):
        return True
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    if os.name == "nt":
        import ctypes

        process_query_limited_information = 0x1000
        still_active = 259
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
        if handle:
            try:
                exit_code = ctypes.c_ulong()
                if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    return True
                return int(exit_code.value) == still_active
            finally:
                kernel32.CloseHandle(handle)
        # ERROR_INVALID_PARAMETER means the PID does not exist.  Access denied
        # and every unknown failure are treated as live, preserving fail-closed
        # single-writer semantics.
        return ctypes.get_last_error() != 87
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True


class NativeHistoryRunLock:
    """Cross-platform non-blocking OS lock with durable owner metadata.

    The one-byte lock file is intentionally persistent.  OS locks disappear
    when a process dies; the adjacent owner record remains as evidence.  Such a
    record is recovered only after its PID is proven absent.  A live or
    indeterminate owner always fails closed.
    """

    SCHEMA_VERSION = "fxpm.native_history_writer_lock.v1"

    def __init__(self, native_root: Path, run_id: str) -> None:
        self.native_root = _normalized_native_root(native_root)
        self.run_id = _safe_name(run_id, "run id")
        self.runs_root = self.native_root / "runs"
        self.lock_path = self.runs_root / f".{self.run_id}.writer.lock"
        self.owner_path = self.runs_root / f".{self.run_id}.writer.owner.json"
        self.token = uuid.uuid4().hex
        self._handle: Optional[Any] = None
        self._acquired = False
        self.recovered_stale_owner: Optional[Dict[str, Any]] = None

    @property
    def acquired(self) -> bool:
        return bool(self._acquired and self._handle is not None)

    def _lock_handle(self) -> None:
        assert self._handle is not None
        self._handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock_handle(self) -> None:
        if self._handle is None:
            return
        self._handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)

    def _read_owner(self, *, tolerate_missing: bool = True) -> Optional[Dict[str, Any]]:
        if not self.owner_path.exists():
            if tolerate_missing:
                return None
            raise ConcurrentWriterError(
                f"writer owner record is missing: {self.owner_path}"
            )
        try:
            value = _load_json_object(
                self.owner_path,
                maximum_bytes=MAX_OWNER_RECORD_BYTES,
            )
        except Exception as exc:
            raise ConcurrentWriterError(
                f"writer owner record is unreadable; refusing stale recovery: {self.owner_path}"
            ) from exc
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != self.SCHEMA_VERSION
        ):
            raise ConcurrentWriterError(
                f"writer owner record has an unknown schema: {self.owner_path}"
            )
        return value

    def _write_owner(self, value: Mapping[str, Any]) -> None:
        tmp_path = (
            self.runs_root / f".{self.owner_path.name}.{uuid.uuid4().hex}.staging"
        )
        try:
            with tmp_path.open("x", encoding="utf-8", newline="\n") as handle:
                json.dump(_json_safe(value), handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            last_error: Optional[BaseException] = None
            for delay in (0.0, 0.05, 0.15, 0.35):
                if delay:
                    time.sleep(delay)
                try:
                    os.replace(tmp_path, self.owner_path)
                    return
                except PermissionError as exc:
                    last_error = exc
            assert last_error is not None
            raise last_error
        finally:
            tmp_path.unlink(missing_ok=True)

    def acquire(self) -> "NativeHistoryRunLock":
        if self.acquired:
            raise ConcurrentWriterError(
                f"writer lock already acquired: {self.lock_path}"
            )
        self.runs_root.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+b", buffering=0)
        self._handle = handle
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                os.fsync(handle.fileno())
            try:
                self._lock_handle()
            except OSError as exc:
                try:
                    owner = self._read_owner()
                except ConcurrentWriterError:
                    owner = None
                detail = f" owner={owner}" if owner else ""
                raise ConcurrentWriterError(
                    f"another process holds the native-history run lock {self.lock_path}.{detail}"
                ) from exc
            self._acquired = True
            owner = self._read_owner()
            if owner and bool(owner.get("active")):
                raw_pid = owner.get("pid")
                if not isinstance(raw_pid, int) or isinstance(raw_pid, bool):
                    raise ConcurrentWriterError(
                        "active writer metadata has no trustworthy PID; refusing stale recovery"
                    )
                if _pid_is_alive(raw_pid):
                    raise ConcurrentWriterError(
                        f"writer metadata identifies live PID {raw_pid}; refusing concurrent writer"
                    )
                self.recovered_stale_owner = dict(owner)
            acquired_record: Dict[str, Any] = {
                "schema_version": self.SCHEMA_VERSION,
                "active": True,
                "pid": os.getpid(),
                "token": self.token,
                "hostname": socket.gethostname(),
                "run_id": self.run_id,
                "acquired_utc": _iso(datetime.now(timezone.utc)),
                "argv": list(sys.argv),
            }
            if self.recovered_stale_owner is not None:
                acquired_record["recovered_stale_owner"] = self.recovered_stale_owner
            self._write_owner(acquired_record)
            return self
        except Exception:
            if self._acquired:
                try:
                    self._unlock_handle()
                except Exception:
                    pass
            handle.close()
            self._handle = None
            self._acquired = False
            raise

    def release(self) -> None:
        if not self.acquired:
            return
        assert self._handle is not None
        try:
            owner = self._read_owner()
            if owner and owner.get("token") == self.token:
                released = dict(owner)
                released.update(
                    {
                        "active": False,
                        "released_utc": _iso(datetime.now(timezone.utc)),
                    }
                )
                self._write_owner(released)
        finally:
            try:
                self._unlock_handle()
            finally:
                self._handle.close()
                self._handle = None
                self._acquired = False

    def __enter__(self) -> "NativeHistoryRunLock":
        return self.acquire()


class NativeHistoryTerminalLock(NativeHistoryRunLock):
    """Serialize MT5 history ingestion across every run in one native store.

    MT5's on-disk history cache is shared by run ids.  Per-run writer locks
    therefore protect immutable leaves but cannot prevent two acquisitions
    from concurrently mutating the same terminal cache.  This reserved lock
    is acquired before any run lock, MT5 import, or terminal connection.
    """

    SCHEMA_VERSION = "fxpm.native_history_terminal_lock.v1"

    def __init__(self, native_root: Path) -> None:
        super().__init__(native_root, TERMINAL_INGESTION_LOCK_ID)

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release()


def _reject_duplicate_native_json_pairs(
    pairs: Sequence[Tuple[str, Any]],
) -> Dict[str, Any]:
    value: Dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object key: {key}")
        value[key] = item
    return value


def _load_json_object(
    path: Path,
    *,
    maximum_bytes: int = MAX_REUSABLE_PROVENANCE_BYTES,
) -> Dict[str, Any]:
    """Read one small JSON authority under a hard byte and object budget.

    This helper is intentionally usable for the writer-owner record, which is
    outside a run root and therefore cannot use the stronger directory-chain
    pin below.  Run artifacts use ``_load_pinned_native_json_object``.
    """

    descriptor = -1
    try:
        before = path.lstat()
        unsafe_attributes = bool(
            stat.S_ISLNK(before.st_mode)
            or int(getattr(before, "st_file_attributes", 0) or 0)
            & _FILE_ATTRIBUTE_REPARSE_POINT
        )
        if (
            unsafe_attributes
            or not stat.S_ISREG(before.st_mode)
            or int(before.st_nlink) != 1
            or not 0 < int(before.st_size) <= int(maximum_bytes)
        ):
            raise ValueError("JSON authority is unsafe or exceeds its byte cap")
        flags = os.O_RDONLY | int(getattr(os, "O_BINARY", 0))
        flags |= int(getattr(os, "O_NOFOLLOW", 0))
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        identity = (
            int(before.st_dev),
            int(before.st_ino),
            int(before.st_size),
            int(before.st_mtime_ns),
            int(before.st_nlink),
            int(before.st_mode),
        )
        opened_identity = (
            int(opened.st_dev),
            int(opened.st_ino),
            int(opened.st_size),
            int(opened.st_mtime_ns),
            int(opened.st_nlink),
            int(opened.st_mode),
        )
        if opened_identity != identity:
            raise ValueError("JSON authority identity changed before read")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(int(maximum_bytes) + 1)
            if len(raw) != int(before.st_size) or handle.read(1):
                raise ValueError("JSON authority size changed during read")
            after_open = os.fstat(handle.fileno())
            if (
                int(after_open.st_dev),
                int(after_open.st_ino),
                int(after_open.st_size),
                int(after_open.st_mtime_ns),
                int(after_open.st_nlink),
                int(after_open.st_mode),
            ) != identity:
                raise ValueError("JSON authority identity changed during read")
        after = path.lstat()
        if (
            int(after.st_dev),
            int(after.st_ino),
            int(after.st_size),
            int(after.st_mtime_ns),
            int(after.st_nlink),
            int(after.st_mode),
        ) != identity:
            raise ValueError("JSON authority identity changed after read")
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_native_json_pairs,
        )
        if not isinstance(value, dict):
            raise ValueError("JSON authority is not an object")
        # Reuse the released V5 canonical depth/node/character/integer budget.
        canonical_json_bytes(value)
        return value
    except Exception as exc:
        raise ResumeValidationError(
            f"cannot read bounded JSON authority {path}: {exc}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


@dataclass(frozen=True)
class _PinnedNativeFileIdentity:
    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int
    links: int
    mode: int


@dataclass(frozen=True)
class _PinnedNativeDirectoryIdentity:
    path: Path
    device: int
    inode: int
    mtime_ns: int
    ctime_ns: int
    links: int
    mode: int


@dataclass(frozen=True)
class _PinnedNativeFileReceipt:
    path: Path
    identity: _PinnedNativeFileIdentity
    directories: Tuple[_PinnedNativeDirectoryIdentity, ...]


def _unsafe_native_file_attributes(value: os.stat_result) -> bool:
    return bool(
        stat.S_ISLNK(value.st_mode)
        or int(getattr(value, "st_file_attributes", 0) or 0)
        & _FILE_ATTRIBUTE_REPARSE_POINT
    )


def _pinned_native_file_identity(
    value: os.stat_result,
) -> _PinnedNativeFileIdentity:
    return _PinnedNativeFileIdentity(
        device=int(value.st_dev),
        inode=int(value.st_ino),
        size=int(value.st_size),
        mtime_ns=int(value.st_mtime_ns),
        ctime_ns=int(value.st_ctime_ns),
        links=int(value.st_nlink),
        mode=int(value.st_mode),
    )


def _same_open_native_file_identity(
    left: _PinnedNativeFileIdentity,
    right: _PinnedNativeFileIdentity,
) -> bool:
    # Windows can expose ctime at different precision through lstat/fstat.
    # Device+inode pins the object; size/mtime/link/mode pins its byte state.
    return (
        left.device,
        left.inode,
        left.size,
        left.mtime_ns,
        left.links,
        left.mode,
    ) == (
        right.device,
        right.inode,
        right.size,
        right.mtime_ns,
        right.links,
        right.mode,
    )


def _snapshot_native_directory_chain(
    run_root: Path,
    target_parent: Path,
) -> Tuple[_PinnedNativeDirectoryIdentity, ...]:
    root = Path(os.path.abspath(os.fspath(run_root)))
    parent = Path(os.path.abspath(os.fspath(target_parent)))
    try:
        relative = parent.relative_to(root)
    except ValueError as exc:
        raise ResumeValidationError(
            f"native artifact path escapes its immutable run: {target_parent}"
        ) from exc
    paths = [root]
    current = root
    for component in relative.parts:
        current = current / component
        paths.append(current)
    identities: List[_PinnedNativeDirectoryIdentity] = []
    for path in paths:
        try:
            observed = path.lstat()
        except OSError as exc:
            raise ResumeValidationError(
                f"native artifact directory is unavailable: {path}"
            ) from exc
        if _unsafe_native_file_attributes(observed) or not stat.S_ISDIR(
            observed.st_mode
        ):
            raise ResumeValidationError(f"native artifact directory is unsafe: {path}")
        identities.append(
            _PinnedNativeDirectoryIdentity(
                path=path,
                device=int(observed.st_dev),
                inode=int(observed.st_ino),
                mtime_ns=int(observed.st_mtime_ns),
                ctime_ns=int(observed.st_ctime_ns),
                links=int(observed.st_nlink),
                mode=int(observed.st_mode),
            )
        )
    return tuple(identities)


def _verify_pinned_native_file_receipt(
    receipt: _PinnedNativeFileReceipt,
) -> None:
    for expected in receipt.directories:
        try:
            observed = expected.path.lstat()
        except OSError as exc:
            raise ResumeValidationError(
                f"native artifact directory identity changed: {expected.path}"
            ) from exc
        if (
            _unsafe_native_file_attributes(observed)
            or not stat.S_ISDIR(observed.st_mode)
            or int(observed.st_dev) != expected.device
            or int(observed.st_ino) != expected.inode
            or int(observed.st_mtime_ns) != expected.mtime_ns
            or int(observed.st_ctime_ns) != expected.ctime_ns
            or int(observed.st_nlink) != expected.links
            or int(observed.st_mode) != expected.mode
        ):
            raise ResumeValidationError(
                f"native artifact directory identity changed: {expected.path}"
            )
    try:
        observed_file = receipt.path.lstat()
    except OSError as exc:
        raise ResumeValidationError(
            f"native artifact identity changed: {receipt.path}"
        ) from exc
    if (
        _unsafe_native_file_attributes(observed_file)
        or not stat.S_ISREG(observed_file.st_mode)
        or _pinned_native_file_identity(observed_file) != receipt.identity
    ):
        raise ResumeValidationError(f"native artifact identity changed: {receipt.path}")


@contextmanager
def _open_pinned_native_file(
    path: Path,
    *,
    run_root: Path,
    maximum_bytes: int,
) -> Iterator[Tuple[BinaryIO, _PinnedNativeFileReceipt]]:
    directories = _snapshot_native_directory_chain(run_root, path.parent)
    try:
        observed = path.lstat()
    except OSError as exc:
        raise ResumeValidationError(f"native artifact is unavailable: {path}") from exc
    identity = _pinned_native_file_identity(observed)
    if (
        _unsafe_native_file_attributes(observed)
        or not stat.S_ISREG(observed.st_mode)
        or identity.links != 1
        or not 0 < identity.size <= int(maximum_bytes)
    ):
        raise ResumeValidationError(f"native artifact is unsafe or unbounded: {path}")
    receipt = _PinnedNativeFileReceipt(
        path=path,
        identity=identity,
        directories=directories,
    )
    flags = os.O_RDONLY | int(getattr(os, "O_BINARY", 0))
    flags |= int(getattr(os, "O_NOFOLLOW", 0))
    descriptor = -1
    try:
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise ResumeValidationError(
                f"cannot securely open native artifact: {path}"
            ) from exc
        opened = _pinned_native_file_identity(os.fstat(descriptor))
        if not _same_open_native_file_identity(opened, identity):
            raise ResumeValidationError(
                f"native artifact identity changed before read: {path}"
            )
        with os.fdopen(
            descriptor,
            "rb",
            buffering=NATIVE_VALIDATION_IO_BUFFER_BYTES,
        ) as handle:
            descriptor = -1
            try:
                yield handle, receipt
            finally:
                after = _pinned_native_file_identity(os.fstat(handle.fileno()))
                if not _same_open_native_file_identity(after, identity):
                    raise ResumeValidationError(
                        f"native artifact identity changed during read: {path}"
                    )
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        _verify_pinned_native_file_receipt(receipt)


def _load_pinned_native_json_object(
    path: Path,
    *,
    run_root: Path,
    maximum_bytes: int = MAX_REUSABLE_PROVENANCE_BYTES,
) -> Tuple[Dict[str, Any], _PinnedNativeFileReceipt]:
    with _open_pinned_native_file(
        path,
        run_root=run_root,
        maximum_bytes=maximum_bytes,
    ) as (handle, receipt):
        raw = handle.read(maximum_bytes + 1)
        if len(raw) != receipt.identity.size or handle.read(1):
            raise ResumeValidationError(
                f"native JSON artifact size changed during read: {path}"
            )
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_native_json_pairs,
        )
        if not isinstance(value, dict):
            raise ValueError("native JSON artifact is not an object")
        canonical_json_bytes(value)
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        NativeRequestContractError,
        MemoryError,
        OverflowError,
        RecursionError,
        ValueError,
    ) as exc:
        detail = str(exc)[:512]
        raise ResumeValidationError(
            f"native JSON artifact is invalid: {path}: {detail}"
        ) from exc
    return value, receipt


def _reconcile_authenticated_hardlink_twin(
    *,
    authority_path: Path,
    staging_path: Path,
    run_root: Path,
    durable_parent: Path,
    expected_schema: str,
    expected_run_id: str,
    maximum_bytes: int,
    require_complete: bool = False,
) -> bool:
    """Remove only an authenticated post-link/pre-unlink crash twin.

    Create-only hardlink publication has one unavoidable process-crash point:
    both names can survive after the link succeeds.  This reconciler accepts
    exactly that topology—two safe regular names for the same two-link inode—
    validates the JSON authority while both names remain pinned, removes only
    the staging name, durably flushes its parent, and then re-authenticates the
    now single-link authority.  Every ambiguous topology fails closed.
    """

    try:
        staging_before = staging_path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ResumeValidationError(
            f"cannot inspect authority staging twin: {staging_path}"
        ) from exc
    try:
        authority_before = authority_path.lstat()
    except OSError as exc:
        raise ResumeValidationError(
            f"authority is absent while its staging name remains: {authority_path}"
        ) from exc

    for directory in (durable_parent, run_root):
        try:
            observed_directory = directory.lstat()
        except OSError as exc:
            raise ResumeValidationError(
                f"hardlink reconciliation directory is unavailable: {directory}"
            ) from exc
        if _unsafe_native_file_attributes(observed_directory) or not stat.S_ISDIR(
            observed_directory.st_mode
        ):
            raise ResumeValidationError(
                f"hardlink reconciliation directory is unsafe: {directory}"
            )

    authority_identity = _pinned_native_file_identity(authority_before)
    staging_identity = _pinned_native_file_identity(staging_before)
    if (
        _unsafe_native_file_attributes(authority_before)
        or _unsafe_native_file_attributes(staging_before)
        or not stat.S_ISREG(authority_before.st_mode)
        or not stat.S_ISREG(staging_before.st_mode)
        or authority_identity.links != 2
        or staging_identity != authority_identity
        or not 0 < authority_identity.size <= int(maximum_bytes)
    ):
        raise ResumeValidationError(
            "authority/staging crash residue is not one exact two-link inode"
        )

    flags = os.O_RDONLY | int(getattr(os, "O_BINARY", 0))
    flags |= int(getattr(os, "O_NOFOLLOW", 0))
    descriptor = -1
    try:
        descriptor = os.open(authority_path, flags)
        opened = _pinned_native_file_identity(os.fstat(descriptor))
        if not _same_open_native_file_identity(opened, authority_identity):
            raise ResumeValidationError(
                "authority identity changed before hardlink reconciliation"
            )
        with os.fdopen(
            descriptor,
            "rb",
            buffering=NATIVE_VALIDATION_IO_BUFFER_BYTES,
        ) as handle:
            descriptor = -1
            raw = handle.read(int(maximum_bytes) + 1)
            if len(raw) != authority_identity.size or handle.read(1):
                raise ResumeValidationError(
                    "authority size changed during hardlink reconciliation"
                )
            after_open = _pinned_native_file_identity(os.fstat(handle.fileno()))
            if not _same_open_native_file_identity(after_open, authority_identity):
                raise ResumeValidationError(
                    "authority identity changed during hardlink reconciliation"
                )
    finally:
        if descriptor >= 0:
            os.close(descriptor)

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_native_json_pairs,
        )
        if not isinstance(value, dict):
            raise ValueError("authority is not a JSON object")
        canonical_json_bytes(value)
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        NativeRequestContractError,
        MemoryError,
        OverflowError,
        RecursionError,
        ValueError,
    ) as exc:
        raise ResumeValidationError(
            "authority staging twin does not contain bounded canonical JSON"
        ) from exc
    if (
        value.get("schema_version") != expected_schema
        or value.get("run_id") != expected_run_id
        or value.get("immutable_record") is not True
        or (require_complete and value.get("complete") is not True)
    ):
        raise ResumeValidationError(
            "authority staging twin does not bind the expected immutable run"
        )

    # Re-pin both names immediately before the only destructive operation.
    try:
        authority_recheck = _pinned_native_file_identity(authority_path.lstat())
        staging_recheck = _pinned_native_file_identity(staging_path.lstat())
    except OSError as exc:
        raise ResumeValidationError(
            "authority/staging identity changed before crash reconciliation"
        ) from exc
    if authority_recheck != authority_identity or staging_recheck != authority_identity:
        raise ResumeValidationError(
            "authority/staging identity changed before crash reconciliation"
        )

    staging_path.unlink()
    _fsync_directory(durable_parent)
    try:
        final_after = authority_path.lstat()
    except OSError as exc:
        raise ResumeValidationError(
            "authority disappeared during crash reconciliation"
        ) from exc
    final_identity = _pinned_native_file_identity(final_after)
    if (
        _unsafe_native_file_attributes(final_after)
        or not stat.S_ISREG(final_after.st_mode)
        or final_identity.device != authority_identity.device
        or final_identity.inode != authority_identity.inode
        or final_identity.size != authority_identity.size
        or final_identity.mtime_ns != authority_identity.mtime_ns
        or final_identity.links != 1
        or final_identity.mode != authority_identity.mode
    ):
        raise ResumeValidationError(
            "authority did not become the expected single-link inode"
        )
    recovered, _receipt = _load_pinned_native_json_object(
        authority_path,
        run_root=run_root,
        maximum_bytes=maximum_bytes,
    )
    if recovered != value:
        raise ResumeValidationError(
            "authority content changed after crash reconciliation"
        )
    return True


def _scan_pinned_native_csv(
    path: Path,
    *,
    run_root: Path,
    timeframe: str,
    common_request: Mapping[str, Any],
    pagination: Mapping[str, Any],
) -> Tuple[NativeCsvScanResult, _PinnedNativeFileReceipt]:
    with _open_pinned_native_file(
        path,
        run_root=run_root,
        maximum_bytes=MAX_CSV_BYTES,
    ) as (handle, receipt):
        try:
            result = scan_native_csv_stream(
                handle,
                expected_bytes=receipt.identity.size,
                timeframe=timeframe,
                common_request=common_request,
                pagination=pagination,
                maximum_bytes=MAX_CSV_BYTES,
                maximum_rows=MAX_CSV_ROWS,
                maximum_line_bytes=MAX_CSV_LINE_BYTES,
            )
        except NativeCsvValidationError as exc:
            raise ResumeValidationError(
                f"native CSV deep validation failed for {path}: {exc.code}"
            ) from exc
    return result, receipt


def _timeframe_request_hash_payload(request: Mapping[str, Any]) -> Dict[str, Any]:
    payload = {
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
    }
    request_map_version = str(request.get("timeframe_request_map_schema_version") or "")
    if request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4:
        payload.update(
            {
                "timeframe_strategy_evidence_intervals": request.get(
                    "timeframe_strategy_evidence_intervals"
                ),
                "timeframe_audit_reference_map": request.get(
                    "timeframe_audit_reference_map"
                ),
                "timeframe_audit_only_prefix_intervals": request.get(
                    "timeframe_audit_only_prefix_intervals"
                ),
            }
        )
    elif request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
        payload.update(
            {
                "timeframe_strategy_evidence_intervals": request.get(
                    "timeframe_strategy_evidence_intervals"
                ),
                "timeframe_audit_reference_map": request.get(
                    "timeframe_audit_reference_map"
                ),
                "timeframe_audit_consumer_map": request.get(
                    "timeframe_audit_consumer_map"
                ),
                "timeframe_audit_windows": request.get("timeframe_audit_windows"),
                "timeframe_audit_reference_intervals": request.get(
                    "timeframe_audit_reference_intervals"
                ),
                "timeframe_audit_only_intervals": request.get(
                    "timeframe_audit_only_intervals"
                ),
                "timeframe_consensus_policy": request.get("timeframe_consensus_policy"),
            }
        )
    return payload


def _resume_request_contract(record: Mapping[str, Any], path: Path) -> Dict[str, Any]:
    request = record.get("request")
    if not isinstance(request, Mapping):
        raise ResumeValidationError(f"resume provenance has no request object: {path}")
    version = str(
        request.get("window_contract_version") or LEGACY_FLAT_WINDOW_CONTRACT_VERSION
    )
    if version == LEGACY_FLAT_WINDOW_CONTRACT_VERSION:
        keys = _LEGACY_RESUME_REQUEST_KEYS
    elif version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
        request_map_version = str(
            request.get("timeframe_request_map_schema_version") or ""
        )
        if request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            keys = _V5_RESUME_REQUEST_KEYS
        elif request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4:
            keys = _V4_RESUME_REQUEST_KEYS
        elif request_map_version == LEGACY_TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
            keys = _V3_RESUME_REQUEST_KEYS
        else:
            raise ResumeValidationError(
                f"unsupported timeframe request map {request_map_version!r}: {path}"
            )
    else:
        raise ResumeValidationError(
            f"unsupported native window contract {version!r}: {path}"
        )
    missing = [key for key in keys if key not in request]
    if missing:
        raise ResumeValidationError(
            f"resume provenance lacks immutable request fields {missing}: {path}"
        )
    contract = {key: _json_safe(request[key]) for key in keys}
    if version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
        boundaries = contract.get("timeframe_calendar_boundaries")
        if not isinstance(boundaries, Mapping) or not boundaries:
            raise ResumeValidationError(
                f"timeframe calendar boundary map is invalid: {path}"
            )
        declared_boundary_hash = str(
            contract.get("timeframe_calendar_boundaries_sha256") or ""
        )
        if declared_boundary_hash != _canonical_json_sha256(boundaries):
            raise ResumeValidationError(
                f"timeframe calendar boundary hash mismatch: {path}"
            )
        declared_request_hash = str(contract.get("timeframe_request_map_sha256") or "")
        if declared_request_hash != _canonical_json_sha256(
            _timeframe_request_hash_payload(contract)
        ):
            raise ResumeValidationError(f"timeframe request map hash mismatch: {path}")
    return contract


def _validate_timeframe_cell_request(
    record: Mapping[str, Any],
    timeframe: str,
    path: Path,
) -> None:
    """Authenticate a leaf projection against its hash-bound timeframe map."""

    request = record.get("request")
    if not isinstance(request, Mapping):
        raise ResumeValidationError(f"resume provenance has no request object: {path}")
    if (
        str(request.get("window_contract_version") or "")
        != TIMEFRAME_EVIDENCE_CONTRACT_VERSION
    ):
        return
    common = _resume_request_contract(record, path)
    tf = str(timeframe).upper()
    expected_years = (common.get("timeframe_window_years") or {}).get(tf)
    expected_boundaries = (common.get("timeframe_calendar_boundaries") or {}).get(tf)
    expected_interval = (common.get("timeframe_source_request_intervals") or {}).get(tf)
    expected_lead = (
        common.get("timeframe_required_evidence_lead_start_utc") or {}
    ).get(tf)
    checks = {
        "cell_timeframe": tf,
        "cell_timeframe_window_years": expected_years,
        "cell_calendar_boundaries": expected_boundaries,
        "source_request_interval": expected_interval,
        "cell_required_evidence_lead_start_utc": expected_lead,
    }
    request_map_version = str(common.get("timeframe_request_map_schema_version") or "")
    if request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION_V4:
        checks.update(
            {
                "strategy_evidence_interval": (
                    common.get("timeframe_strategy_evidence_intervals") or {}
                ).get(tf),
                "audit_only_prefix_interval": (
                    common.get("timeframe_audit_only_prefix_intervals") or {}
                ).get(tf),
                "audit_reference_timeframe": (
                    common.get("timeframe_audit_reference_map") or {}
                ).get(tf),
            }
        )
    elif request_map_version == TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION:
        checks.update(
            {
                "strategy_evidence_interval": (
                    common.get("timeframe_strategy_evidence_intervals") or {}
                ).get(tf),
                "audit_reference_timeframes": (
                    common.get("timeframe_audit_reference_map") or {}
                ).get(tf),
                "audit_consumer_targets": (
                    common.get("timeframe_audit_consumer_map") or {}
                ).get(tf),
                "audit_windows": (common.get("timeframe_audit_windows") or {}).get(tf),
                "audit_reference_intervals": (
                    common.get("timeframe_audit_reference_intervals") or {}
                ).get(tf),
                "audit_only_intervals": (
                    common.get("timeframe_audit_only_intervals") or {}
                ).get(tf),
            }
        )
    for field_name, expected in checks.items():
        if field_name not in request or _json_safe(
            request.get(field_name)
        ) != _json_safe(expected):
            raise ResumeValidationError(
                f"timeframe cell request field {field_name!r} does not match the "
                f"hash-bound timeframe map: {path}"
            )


def _source_identity(source_environment: Any) -> Dict[str, Any]:
    if not isinstance(source_environment, Mapping):
        return {}
    identity: Dict[str, Any] = {}
    for section, keys in _SOURCE_IDENTITY_KEYS.items():
        value = source_environment.get(section)
        if not isinstance(value, Mapping):
            continue
        selected = {key: _json_safe(value[key]) for key in keys if key in value}
        if selected:
            identity[section] = selected
    return identity


def _source_identity_for_request(
    source_environment: Any,
    request: Mapping[str, Any],
    *,
    context: str,
) -> Dict[str, Any]:
    """Use the canonical V5 identity; retain reduced legacy identity only."""

    if request.get("window_contract_version") == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
        try:
            return dict(canonical_source_identity(source_environment))
        except NativeRequestContractError as exc:
            raise ResumeValidationError(
                f"canonical V5 source identity is invalid in {context}: "
                f"{';'.join(exc.errors)}"
            ) from exc
    return _source_identity(source_environment)


def _is_staging_name(path: Path) -> bool:
    return path.name.startswith(".") and path.name.endswith(".staging")


def _bounded_directory_entries(
    path: Path,
    *,
    maximum_entries: int,
    context: str,
) -> Dict[str, Path]:
    """Enumerate at most ``maximum_entries`` without ``Path.iterdir``/listdir."""

    if int(maximum_entries) < 0:
        raise ValueError("maximum_entries cannot be negative")
    entries: Dict[str, Path] = {}
    try:
        with os.scandir(path) as scanner:
            for observed_count, entry in enumerate(scanner, start=1):
                if observed_count > int(maximum_entries):
                    raise ResumeValidationError(
                        f"{context} entry cap exceeded at {observed_count}"
                    )
                if entry.name in entries:
                    raise ResumeValidationError(
                        f"{context} contains a duplicate directory entry"
                    )
                entries[entry.name] = path / entry.name
    except ResumeValidationError:
        raise
    except OSError as exc:
        raise ResumeValidationError(f"cannot enumerate {context}: {path}") from exc
    return entries


class _TerminalHccScanBound(RuntimeError):
    """Internal stop signal for a fail-closed, constant-memory HCC scan."""


def _bounded_terminal_hcc_scan(
    scan_root: Path,
    *,
    relative_root: Path,
    maximum_directory_depth: int,
    budget: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Inspect HCC sizes with finite depth/cardinality and no materialized tree."""

    shared_budget = budget if budget is not None else {"entries": 0}
    hcc_count = 0
    largest_bytes = 0
    largest_path: Optional[str] = None
    violations: List[str] = []

    def walk(directory: Path, depth: int) -> None:
        nonlocal hcc_count, largest_bytes, largest_path
        with os.scandir(directory) as scanner:
            for directory_count, entry in enumerate(scanner, start=1):
                shared_budget["entries"] = int(shared_budget.get("entries", 0)) + 1
                if directory_count > MAX_TERMINAL_DIRECTORY_ENTRIES:
                    raise _TerminalHccScanBound(
                        "terminal_hcc_directory_entry_cap_exceeded"
                    )
                if shared_budget["entries"] > MAX_TERMINAL_HCC_SCAN_ENTRIES:
                    raise _TerminalHccScanBound(
                        "terminal_hcc_global_entry_cap_exceeded"
                    )
                path = directory / entry.name
                observed = entry.stat(follow_symlinks=False)
                if entry.is_symlink() or _unsafe_native_file_attributes(observed):
                    raise _TerminalHccScanBound(
                        "terminal_hcc_scan_unsafe_entry_detected"
                    )
                if stat.S_ISDIR(observed.st_mode):
                    if depth >= maximum_directory_depth:
                        # MT5 legitimately keeps non-authoritative cache
                        # subdirectories beside the yearly ``*.hcc`` files
                        # (for example ``history/<symbol>/cache``).  HCC year
                        # files themselves live at the bounded symbol level,
                        # so deeper trees are neither required nor relevant to
                        # this resource check.  Count the directory entry, but
                        # do not descend into it or reject the entire terminal.
                        continue
                    walk(path, depth + 1)
                elif stat.S_ISREG(observed.st_mode) and path.suffix.lower() == ".hcc":
                    size = int(observed.st_size)
                    hcc_count += 1
                    if size > largest_bytes:
                        largest_bytes = size
                        try:
                            largest_path = str(path.relative_to(relative_root))
                        except ValueError:
                            largest_path = path.name

    scan_error: Optional[Dict[str, str]] = None
    try:
        root_stat = scan_root.lstat()
        if _unsafe_native_file_attributes(root_stat) or not stat.S_ISDIR(
            root_stat.st_mode
        ):
            raise _TerminalHccScanBound("terminal_hcc_scan_unsafe_root_detected")
        walk(scan_root, 0)
    except _TerminalHccScanBound as exc:
        violations.append(str(exc))
    except OSError as exc:
        violations.append("terminal_hcc_scan_failed")
        scan_error = {
            "type": type(exc).__name__,
            "message": str(exc)[:4096],
        }
    return {
        "hcc_files_inspected": hcc_count,
        "largest_hcc_bytes": largest_bytes,
        "largest_hcc_relative_path": largest_path,
        "entries_inspected": int(shared_budget.get("entries", 0)),
        "scan_error": scan_error,
        "violations": violations,
    }


def _load_native_resume_seed(
    run_root: Path,
    safe_run_id: str,
) -> Tuple[
    Dict[str, Any],
    Dict[str, Any],
    Dict[str, Any],
    Tuple[str, ...],
    Tuple[str, ...],
    str,
]:
    """Authenticate the immutable target before any run-tree traversal."""

    seed_path = run_root / RESUME_SEED_FILENAME
    seed, _receipt = _load_pinned_native_json_object(
        seed_path,
        run_root=run_root,
        maximum_bytes=MAX_RESUME_SEED_BYTES,
    )
    if set(seed) != {
        "schema_version",
        "immutable_record",
        "run_id",
        "acquisition_started_utc",
        "request",
        "source_identity",
        "symbols",
        "timeframes",
        "production_authoritative",
    }:
        raise ResumeValidationError(
            f"resume contract has an unknown exact schema: {seed_path}"
        )
    raw_symbols = seed.get("symbols")
    raw_timeframes = seed.get("timeframes")
    if (
        seed.get("schema_version") != RESUME_SEED_SCHEMA_VERSION
        or seed.get("immutable_record") is not True
        or seed.get("run_id") != safe_run_id
        or not isinstance(seed.get("request"), Mapping)
        or not isinstance(raw_symbols, list)
        or not isinstance(raw_timeframes, list)
        or type(seed.get("production_authoritative")) is not bool
        or any(not isinstance(value, str) for value in raw_symbols)
        or any(not isinstance(value, str) for value in raw_timeframes)
    ):
        raise ResumeValidationError(f"resume contract is invalid: {seed_path}")
    try:
        seed_symbols = tuple(_safe_name(value, "symbol") for value in raw_symbols)
        seed_timeframes = tuple(
            _safe_name(value, "timeframe").upper() for value in raw_timeframes
        )
    except ValueError as exc:
        raise ResumeValidationError(
            f"resume contract target roster is invalid: {seed_path}"
        ) from exc
    if (
        not seed_symbols
        or len(seed_symbols) > len(PRODUCTION_SYMBOL_ORDER)
        or len(set(seed_symbols)) != len(seed_symbols)
        or not seed_timeframes
        or len(seed_timeframes) > len(DEFAULT_TIMEFRAMES)
        or len(set(seed_timeframes)) != len(seed_timeframes)
        or any(value not in TIMEFRAME_SECONDS for value in seed_timeframes)
    ):
        raise ResumeValidationError(
            f"resume contract target roster is invalid: {seed_path}"
        )
    if seed.get("production_authoritative") is True and (
        seed_symbols != tuple(PRODUCTION_SYMBOL_ORDER)
        or seed_timeframes != tuple(DEFAULT_TIMEFRAMES)
    ):
        raise ResumeValidationError(
            f"production resume contract target is not exact: {seed_path}"
        )
    declared_seed_request = dict(seed["request"])
    common_request = _resume_request_contract(
        {"request": declared_seed_request},
        seed_path,
    )
    if common_request != declared_seed_request:
        raise ResumeValidationError(
            f"resume contract request has an unknown exact schema: {seed_path}"
        )
    common_identity = _source_identity_for_request(
        seed.get("source_identity"),
        common_request,
        context=str(seed_path),
    )
    if not common_identity:
        raise ResumeValidationError(
            f"resume contract source identity is absent: {seed_path}"
        )
    try:
        acquisition_started_utc = _iso(
            parse_utc_datetime(str(seed.get("acquisition_started_utc")))
        )
    except (TypeError, ValueError) as exc:
        raise ResumeValidationError(
            f"resume contract acquisition start is invalid: {seed_path}"
        ) from exc
    return (
        seed,
        common_request,
        common_identity,
        seed_symbols,
        seed_timeframes,
        acquisition_started_utc,
    )


def _discover_native_leaves(
    run_root: Path,
    *,
    symbols: Sequence[str],
    timeframes: Sequence[str],
) -> Dict[Tuple[str, str], Path]:
    """Discover only seed-authorized final leaves under bounded topology."""

    symbol_set = frozenset(symbols)
    timeframe_set = frozenset(timeframes)
    root_entries = _bounded_directory_entries(
        run_root,
        maximum_entries=len(symbol_set) + 2,
        context="resumable run root",
    )
    leaves: Dict[Tuple[str, str], Path] = {}
    for name, symbol_entry in root_entries.items():
        if name == RESUME_SEED_FILENAME:
            continue
        if name == "run_manifest.json":
            raise ResumeValidationError(
                f"run already has an immutable final manifest: {run_root}"
            )
        if name == ATTEMPT_RECEIPTS_DIR:
            observed = symbol_entry.lstat()
            if _unsafe_native_file_attributes(observed) or not stat.S_ISDIR(
                observed.st_mode
            ):
                raise ResumeValidationError(
                    f"retryable attempt root is unsafe: {symbol_entry}"
                )
            continue
        if _is_staging_name(symbol_entry):
            raise ResumeValidationError(
                f"staging residue remains after authenticated cleanup: {symbol_entry}"
            )
        observed = symbol_entry.lstat()
        if (
            _unsafe_native_file_attributes(observed)
            or not stat.S_ISDIR(observed.st_mode)
            or name not in symbol_set
        ):
            raise ResumeValidationError(
                f"unexpected entry in resumable run: {symbol_entry}"
            )
        timeframe_entries = _bounded_directory_entries(
            symbol_entry,
            maximum_entries=len(timeframe_set),
            context=f"resumable symbol directory {name}",
        )
        for timeframe_name, timeframe_entry in timeframe_entries.items():
            observed_timeframe = timeframe_entry.lstat()
            if (
                _unsafe_native_file_attributes(observed_timeframe)
                or not stat.S_ISDIR(observed_timeframe.st_mode)
                or timeframe_name not in timeframe_set
            ):
                raise ResumeValidationError(
                    f"unexpected timeframe leaf in resumable run: {timeframe_entry}"
                )
            key = (name, timeframe_name)
            if key in leaves:
                raise ResumeValidationError(f"duplicate resumable cell: {key}")
            leaves[key] = timeframe_entry
    return leaves


def load_native_history_resume_contract(
    native_root: Path,
    run_id: str,
) -> NativeHistoryResumeContract:
    """Recover and cross-check the original request without using wall-clock now."""

    root = _normalized_native_root(native_root)
    safe_run_id = _safe_name(run_id, "run id")
    run_root = root / "runs" / safe_run_id
    if not run_root.is_dir() or run_root.is_symlink():
        raise ResumeValidationError(f"resumable run does not exist: {run_root}")
    if (run_root / "run_manifest.json").exists():
        raise ResumeValidationError(
            f"run already has an immutable final manifest and cannot be resumed: {run_root}"
        )
    (
        seed,
        common_request,
        common_identity,
        seed_symbols,
        seed_timeframes,
        acquisition_started_utc,
    ) = _load_native_resume_seed(run_root, safe_run_id)
    leaves = _discover_native_leaves(
        run_root,
        symbols=seed_symbols,
        timeframes=seed_timeframes,
    )

    for (symbol, timeframe), leaf in leaves.items():
        sidecar = leaf / "provenance.json"
        record, _receipt = _load_pinned_native_json_object(
            sidecar,
            run_root=run_root,
            maximum_bytes=MAX_REUSABLE_PROVENANCE_BYTES,
        )
        if record.get("schema_version") != SCHEMA_VERSION:
            raise ResumeValidationError(f"schema mismatch in {sidecar}")
        if record.get("acquisition_contract_version") != ACQUISITION_CONTRACT_VERSION:
            raise ResumeValidationError(f"acquisition contract mismatch in {sidecar}")
        if record.get("run_id") != safe_run_id:
            raise ResumeValidationError(f"run_id mismatch in {sidecar}")
        request = _resume_request_contract(record, sidecar)
        exact_v5_identity = (
            request.get("window_contract_version")
            == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
        )
        observed_symbol = str(record.get("canonical_symbol", record.get("symbol", "")))
        symbol_matches = (
            observed_symbol == symbol
            if exact_v5_identity
            else observed_symbol.upper() == symbol.upper()
        )
        if not symbol_matches:
            raise ResumeValidationError(f"symbol mismatch in {sidecar}")
        observed_timeframe = str(record.get("timeframe", ""))
        timeframe_matches = (
            observed_timeframe == timeframe
            if exact_v5_identity
            else observed_timeframe.upper() == timeframe
        )
        if not timeframe_matches:
            raise ResumeValidationError(f"timeframe mismatch in {sidecar}")
        _validate_timeframe_cell_request(record, timeframe, sidecar)
        identity = _source_identity_for_request(
            record.get("source_environment"),
            request,
            context=str(sidecar),
        )
        if not identity:
            raise ResumeValidationError(f"source identity is absent in {sidecar}")
        if request != common_request:
            raise ResumeValidationError(
                f"request contract differs across leaves: {sidecar}"
            )
        elif identity != common_identity:
            raise ResumeValidationError(
                f"source identity differs across leaves: {sidecar}"
            )

    return NativeHistoryResumeContract(
        run_id=safe_run_id,
        request=common_request,
        source_identity=common_identity,
        existing_cells=tuple(sorted(leaves)),
        symbols=seed_symbols,
        timeframes=seed_timeframes,
        production_authoritative=bool(seed["production_authoritative"]),
        acquisition_started_utc=acquisition_started_utc,
    )


def reconcile_completed_manifest_staging(
    native_root: Path,
    run_id: str,
    *,
    run_lock: NativeHistoryRunLock,
) -> bool:
    """Recover the exact completed-manifest hardlink crash state under lock."""

    root = _normalized_native_root(native_root)
    safe_run_id = _safe_name(run_id, "run id")
    if (
        not run_lock.acquired
        or run_lock.native_root != root
        or run_lock.run_id != safe_run_id
    ):
        raise ConcurrentWriterError(
            "completed-manifest reconciliation requires the exact acquired run lock"
        )
    runs_root = root / "runs"
    run_root = runs_root / safe_run_id
    staging = runs_root / f".{safe_run_id}.run_manifest.staging"
    if not staging.exists() and not staging.is_symlink():
        return False
    if not run_root.is_dir() or run_root.is_symlink():
        raise ResumeValidationError(
            "manifest staging exists without a safe exact run directory"
        )
    return _reconcile_authenticated_hardlink_twin(
        authority_path=run_root / "run_manifest.json",
        staging_path=staging,
        run_root=run_root,
        durable_parent=runs_root,
        expected_schema=MANIFEST_SCHEMA_VERSION,
        expected_run_id=safe_run_id,
        maximum_bytes=MAX_REUSABLE_PROVENANCE_BYTES,
        require_complete=True,
    )


def reconcile_resume_seed_staging(
    native_root: Path,
    run_id: str,
    *,
    run_lock: NativeHistoryRunLock,
) -> bool:
    """Recover one exact resume-seed hardlink crash twin under the run lock."""

    root = _normalized_native_root(native_root)
    safe_run_id = _safe_name(run_id, "run id")
    if (
        not run_lock.acquired
        or run_lock.native_root != root
        or run_lock.run_id != safe_run_id
    ):
        raise ConcurrentWriterError(
            "resume-seed reconciliation requires the exact acquired run lock"
        )
    run_root = root / "runs" / safe_run_id
    if not run_root.is_dir() or run_root.is_symlink():
        return False
    seed_path = run_root / RESUME_SEED_FILENAME
    try:
        seed_stat = seed_path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ResumeValidationError(
            f"cannot inspect resume seed before crash reconciliation: {seed_path}"
        ) from exc
    # The ordinary case must remain seed-first: a single-link seed is loaded
    # and byte-bounded before any run topology is enumerated.  A scan is needed
    # only for the exact two-link crash state it is intended to repair.
    if (
        _unsafe_native_file_attributes(seed_stat)
        or not stat.S_ISREG(seed_stat.st_mode)
        or int(seed_stat.st_nlink) == 1
    ):
        return False
    if int(seed_stat.st_nlink) != 2:
        raise ResumeValidationError("resume seed has an ambiguous hardlink count")
    entries = _bounded_directory_entries(
        run_root,
        maximum_entries=(
            len(PRODUCTION_SYMBOL_ORDER) + MAX_STAGING_ENTRIES_PER_DIRECTORY + 4
        ),
        context="resume-seed crash reconciliation root",
    )
    pattern = re.compile(
        rf"^\.{re.escape(RESUME_SEED_FILENAME)}\.[0-9a-f]{{32}}\.staging$"
    )
    candidates = [path for name, path in entries.items() if pattern.fullmatch(name)]
    if not candidates:
        return False
    if len(candidates) != 1:
        raise ResumeValidationError(
            "resume-seed crash reconciliation found multiple staging names"
        )
    return _reconcile_authenticated_hardlink_twin(
        authority_path=seed_path,
        staging_path=candidates[0],
        run_root=run_root,
        durable_parent=run_root,
        expected_schema=RESUME_SEED_SCHEMA_VERSION,
        expected_run_id=safe_run_id,
        maximum_bytes=MAX_RESUME_SEED_BYTES,
    )


class NativeHistoryStore:
    """Create-only leaves in either a new run or an explicitly resumed run."""

    def __init__(
        self,
        native_root: Path,
        run_id: str,
        *,
        resume: bool = False,
        run_lock: Optional[NativeHistoryRunLock] = None,
        csv_writer: Optional[Callable[[pd.DataFrame, Path], None]] = None,
        replace_fn: Callable[[Path, Path], None] = os.replace,
        manifest_link_fn: Callable[[Path, Path], None] = os.link,
        publish_retry_delays: Sequence[float] = (0.05, 0.15, 0.35, 0.75),
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.native_root = _normalized_native_root(native_root)
        self.run_id = _safe_name(run_id, "run id")
        self.runs_root = self.native_root / "runs"
        self.run_root = self.runs_root / self.run_id
        self.resume = bool(resume)
        self.stale_staging_removed = 0
        self.resume_contract: Optional[NativeHistoryResumeContract] = None
        self._artifact_bytes_by_cell: Dict[Tuple[str, str], int] = {}
        self._total_artifact_bytes = 0
        self._uses_default_csv_writer = csv_writer is None
        self._csv_writer = csv_writer or self._default_csv_writer
        self._replace_fn = replace_fn
        self._manifest_link_fn = manifest_link_fn
        self._publish_retry_delays = tuple(float(item) for item in publish_retry_delays)
        if any(item < 0.0 for item in self._publish_retry_delays):
            raise ValueError("publish retry delays cannot be negative")
        self._sleep_fn = sleep_fn
        if run_lock is not None:
            if (
                run_lock.native_root != self.native_root
                or run_lock.run_id != self.run_id
                or not run_lock.acquired
            ):
                raise ConcurrentWriterError(
                    "supplied run lock is not acquired for this exact native run"
                )
            self._run_lock = run_lock
        else:
            self._run_lock = NativeHistoryRunLock(
                self.native_root,
                self.run_id,
            ).acquire()
        try:
            reconcile_completed_manifest_staging(
                self.native_root,
                self.run_id,
                run_lock=self._run_lock,
            )
            if self.resume:
                if not self.run_root.is_dir() or self.run_root.is_symlink():
                    raise ResumeValidationError(
                        f"resumable run does not exist: {self.run_root}"
                    )
                if (self.run_root / "run_manifest.json").exists():
                    raise ResumeValidationError(
                        f"run already has an immutable final manifest: {self.run_root}"
                    )
                self._reconcile_resume_seed_staging()
                (
                    _seed,
                    _request,
                    _identity,
                    seed_symbols,
                    seed_timeframes,
                    _started,
                ) = _load_native_resume_seed(self.run_root, self.run_id)
                target = {
                    (symbol, timeframe)
                    for symbol in seed_symbols
                    for timeframe in seed_timeframes
                }
                self.stale_staging_removed = self._remove_stale_staging(
                    symbols=seed_symbols,
                    timeframes=seed_timeframes,
                )
                attempt_cells = self._validate_and_bound_attempt_receipts(
                    symbols=seed_symbols,
                    timeframes=seed_timeframes,
                )
                self.resume_contract = load_native_history_resume_contract(
                    self.native_root,
                    self.run_id,
                )
                if not attempt_cells.issubset(target):
                    raise ResumeValidationError(
                        "retryable attempt area contains cells outside the immutable "
                        "run target"
                    )
            else:
                self.runs_root.mkdir(parents=True, exist_ok=True)
                # Create-only is what makes all per-run provenance records immutable.
                self.run_root.mkdir(parents=False, exist_ok=False)
        except Exception:
            self.close()
            raise

    @property
    def writer_lock_acquired(self) -> bool:
        return self._run_lock.acquired

    def _assert_writer_lock(self) -> None:
        if not self.writer_lock_acquired:
            raise ConcurrentWriterError(
                f"native-history mutation attempted without run lock: {self.run_id}"
            )

    def _assert_run_unsealed(self) -> None:
        """Reject every mutation once any final-manifest entry exists."""

        self._assert_writer_lock()
        manifest_path = self.run_root / "run_manifest.json"
        try:
            observed = manifest_path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise ResumeValidationError(
                f"cannot inspect immutable run seal: {manifest_path}"
            ) from exc
        if _unsafe_native_file_attributes(observed) or not stat.S_ISREG(
            observed.st_mode
        ):
            raise ResumeValidationError(
                f"immutable run seal entry is unsafe: {manifest_path}"
            )
        raise ResumeValidationError(
            f"immutable native-history run is already sealed: {self.run_id}"
        )

    def _reconcile_resume_seed_staging(self) -> bool:
        """Recover one exact seed hardlink twin before loading the seed."""

        self._assert_writer_lock()
        return reconcile_resume_seed_staging(
            self.native_root,
            self.run_id,
            run_lock=self._run_lock,
        )

    @staticmethod
    def _bounded_json_publication_bytes(
        value: Mapping[str, Any],
        *,
        maximum_bytes: int,
        context: str,
        compact: bool = False,
    ) -> Tuple[Dict[str, Any], bytes]:
        """Detach one JSON object under canonical budgets and cap emitted bytes."""

        try:
            canonical = canonical_json_bytes(value)
            detached = json.loads(canonical)
            if not isinstance(detached, dict):
                raise ValueError("JSON publication is not an object")
            if compact:
                encoded = canonical + b"\n"
            else:
                encoded = (
                    json.dumps(
                        detached,
                        indent=2,
                        sort_keys=True,
                        ensure_ascii=False,
                    ).encode("utf-8")
                    + b"\n"
                )
        except (
            NativeRequestContractError,
            MemoryError,
            OverflowError,
            RecursionError,
            TypeError,
            ValueError,
        ) as exc:
            raise ResumeValidationError(
                f"{context} is not bounded canonical JSON"
            ) from exc
        if not 0 < len(encoded) <= int(maximum_bytes):
            raise OutputStorageResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": [f"{context}_byte_cap_exceeded"],
                    "observed_bytes": len(encoded),
                    "maximum_bytes": int(maximum_bytes),
                }
            )
        return detached, encoded

    @staticmethod
    def _publication_identity_matches(
        observed: _PinnedNativeFileIdentity,
        expected: _PinnedNativeFileIdentity,
        *,
        links: int,
    ) -> bool:
        return (
            observed.device,
            observed.inode,
            observed.size,
            observed.mtime_ns,
            observed.links,
            observed.mode,
        ) == (
            expected.device,
            expected.inode,
            expected.size,
            expected.mtime_ns,
            int(links),
            expected.mode,
        )

    @classmethod
    def _verify_publication_payload(
        cls,
        path: Path,
        *,
        expected: _PinnedNativeFileIdentity,
        expected_payload: bytes,
        links: int,
    ) -> None:
        try:
            before_stat = path.lstat()
        except OSError as exc:
            raise ResumeValidationError(
                f"publication authority is unavailable: {path}"
            ) from exc
        before = _pinned_native_file_identity(before_stat)
        if (
            _unsafe_native_file_attributes(before_stat)
            or not stat.S_ISREG(before_stat.st_mode)
            or not cls._publication_identity_matches(before, expected, links=links)
        ):
            raise ResumeValidationError(
                f"publication authority identity changed: {path}"
            )
        descriptor = -1
        flags = os.O_RDONLY | int(getattr(os, "O_BINARY", 0))
        flags |= int(getattr(os, "O_NOFOLLOW", 0))
        try:
            descriptor = os.open(path, flags)
            opened = _pinned_native_file_identity(os.fstat(descriptor))
            if not cls._publication_identity_matches(opened, expected, links=links):
                raise ResumeValidationError(
                    f"publication authority changed before read: {path}"
                )
            with os.fdopen(
                descriptor,
                "rb",
                buffering=NATIVE_VALIDATION_IO_BUFFER_BYTES,
            ) as handle:
                descriptor = -1
                raw = handle.read(len(expected_payload) + 1)
                after_open = _pinned_native_file_identity(os.fstat(handle.fileno()))
                if (
                    raw != expected_payload
                    or handle.read(1)
                    or not cls._publication_identity_matches(
                        after_open, expected, links=links
                    )
                ):
                    raise ResumeValidationError(
                        f"publication authority bytes changed: {path}"
                    )
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        after_stat = path.lstat()
        after = _pinned_native_file_identity(after_stat)
        if (
            _unsafe_native_file_attributes(after_stat)
            or not stat.S_ISREG(after_stat.st_mode)
            or not cls._publication_identity_matches(after, expected, links=links)
        ):
            raise ResumeValidationError(
                f"publication authority changed after read: {path}"
            )

    def _stage_publication_payload(
        self,
        path: Path,
        payload: bytes,
        *,
        maximum_bytes: int,
    ) -> _PinnedNativeFileIdentity:
        if not 0 < len(payload) <= int(maximum_bytes):
            raise OutputStorageResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["publication_payload_byte_cap_exceeded"],
                    "observed_bytes": len(payload),
                    "maximum_bytes": int(maximum_bytes),
                }
            )
        with path.open("xb", buffering=0) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        observed = path.lstat()
        identity = _pinned_native_file_identity(observed)
        if (
            _unsafe_native_file_attributes(observed)
            or not stat.S_ISREG(observed.st_mode)
            or identity.links != 1
            or identity.size != len(payload)
        ):
            raise ResumeValidationError(
                f"new publication staging file is unsafe: {path}"
            )
        self._verify_publication_payload(
            path,
            expected=identity,
            expected_payload=payload,
            links=1,
        )
        return identity

    @classmethod
    def _rollback_exact_publication_link(
        cls,
        final_path: Path,
        *,
        expected: _PinnedNativeFileIdentity,
    ) -> None:
        try:
            observed_stat = final_path.lstat()
        except OSError:
            return
        observed = _pinned_native_file_identity(observed_stat)
        if (
            _unsafe_native_file_attributes(observed_stat)
            or not stat.S_ISREG(observed_stat.st_mode)
            or observed.device != expected.device
            or observed.inode != expected.inode
        ):
            return
        final_path.unlink()
        _fsync_directory(final_path.parent)

    def _publish_pinned_payload(
        self,
        staging_path: Path,
        final_path: Path,
        *,
        expected: _PinnedNativeFileIdentity,
        expected_payload: bytes,
        prelink_validator: Optional[Callable[[], None]] = None,
        postlink_validator: Optional[Callable[[], None]] = None,
    ) -> None:
        """Create-only publish exact staged bytes and verify both link states."""

        if prelink_validator is not None:
            prelink_validator()
        self._verify_publication_payload(
            staging_path,
            expected=expected,
            expected_payload=expected_payload,
            links=1,
        )
        try:
            self._manifest_link_fn(staging_path, final_path)
        except Exception:
            self._rollback_exact_publication_link(final_path, expected=expected)
            raise
        published_single = False
        try:
            self._verify_publication_payload(
                staging_path,
                expected=expected,
                expected_payload=expected_payload,
                links=2,
            )
            self._verify_publication_payload(
                final_path,
                expected=expected,
                expected_payload=expected_payload,
                links=2,
            )
            _fsync_directory(final_path.parent)
            if postlink_validator is not None:
                postlink_validator()
            staging_path.unlink()
            _fsync_directory(staging_path.parent)
            published_single = True
            self._verify_publication_payload(
                final_path,
                expected=expected,
                expected_payload=expected_payload,
                links=1,
            )
        except Exception:
            if not published_single:
                self._rollback_exact_publication_link(final_path, expected=expected)
            raise

    def _enforce_artifact_byte_admission(
        self,
        symbol: str,
        timeframe: str,
        observed_bytes: int,
    ) -> Tuple[Tuple[str, str], int]:
        """Check one exact CSV size against the shared run-wide hard ceiling."""

        key = (
            _safe_name(symbol, "symbol"),
            _safe_name(timeframe, "timeframe").upper(),
        )
        if (
            type(observed_bytes) is not int
            or not 0 < observed_bytes <= MAX_ARTIFACT_BYTES
        ):
            raise OutputStorageResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["output_cell_exact_byte_cap_exceeded"],
                    "observed_cell_bytes": observed_bytes,
                    "maximum_cell_bytes": MAX_ARTIFACT_BYTES,
                }
            )
        previous = self._artifact_bytes_by_cell.get(key)
        if previous is not None and previous != observed_bytes:
            raise ResumeValidationError(
                "immutable cell artifact byte size changed within one writer session: "
                f"{key[0]}/{key[1]}"
            )
        projected = self._total_artifact_bytes - int(previous or 0) + observed_bytes
        if projected > MAX_TOTAL_ARTIFACT_BYTES:
            raise OutputStorageResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["output_run_total_artifact_byte_cap_exceeded"],
                    "observed_total_artifact_bytes": projected,
                    "maximum_total_artifact_bytes": MAX_TOTAL_ARTIFACT_BYTES,
                    "cell": {"symbol": key[0], "timeframe": key[1]},
                }
            )
        return key, projected

    def register_artifact_bytes(
        self,
        symbol: str,
        timeframe: str,
        observed_bytes: int,
    ) -> None:
        """Register a deeply validated or atomically published immutable CSV."""

        self._assert_writer_lock()
        key, projected = self._enforce_artifact_byte_admission(
            symbol,
            timeframe,
            observed_bytes,
        )
        self._artifact_bytes_by_cell[key] = observed_bytes
        self._total_artifact_bytes = projected

    def assert_exact_artifact_total(self, observed_total: int) -> None:
        """Bind a full deep scan to the writer's incrementally admitted total."""

        self._assert_writer_lock()
        if (
            type(observed_total) is not int
            or observed_total < 0
            or observed_total > MAX_TOTAL_ARTIFACT_BYTES
        ):
            raise OutputStorageResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["output_run_total_artifact_byte_cap_exceeded"],
                    "observed_total_artifact_bytes": observed_total,
                    "maximum_total_artifact_bytes": MAX_TOTAL_ARTIFACT_BYTES,
                }
            )
        if observed_total != self._total_artifact_bytes:
            raise ResumeValidationError(
                "deep-scanned artifact bytes differ from the writer admission ledger: "
                f"scanned={observed_total}, admitted={self._total_artifact_bytes}"
            )

    def close(self) -> None:
        lock = getattr(self, "_run_lock", None)
        if lock is not None:
            lock.release()

    def __enter__(self) -> "NativeHistoryStore":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _remove_stale_staging(
        self,
        *,
        symbols: Sequence[str],
        timeframes: Sequence[str],
        min_age_seconds: float = 0.0,
    ) -> int:
        """Remove abandoned staging children contained by this exact run.

        The non-blocking OS run lock has already proved that no other writer is
        live.  A hard-supervisor kill can leave a staging directory only seconds
        old, so an age grace period would leak storage and could let a completed
        run retain crash debris.  Containment is rechecked before every removal.
        """

        run_root = self.run_root.resolve(strict=True)
        symbol_set = frozenset(symbols)
        timeframe_set = frozenset(timeframes)
        now_epoch = time.time()
        removed = 0

        def require_safe_directory(path: Path, context: str) -> None:
            try:
                observed = path.lstat()
            except OSError as exc:
                raise ResumeValidationError(
                    f"{context} is unavailable before staging cleanup: {path}"
                ) from exc
            if _unsafe_native_file_attributes(observed) or not stat.S_ISDIR(
                observed.st_mode
            ):
                raise ResumeValidationError(
                    f"{context} is unsafe before staging cleanup: {path}"
                )

        def remove_candidate(path: Path) -> None:
            nonlocal removed
            try:
                path.resolve(strict=False).relative_to(run_root)
            except ValueError as exc:
                raise ResumeValidationError(
                    f"refusing to remove staging path outside exact run: {path}"
                ) from exc
            observed = path.lstat()
            if now_epoch - observed.st_mtime < float(min_age_seconds):
                return
            if _unsafe_native_file_attributes(observed):
                raise ResumeValidationError(f"staging path is unsafe: {path}")
            if stat.S_ISREG(observed.st_mode):
                path.unlink(missing_ok=True)
            elif stat.S_ISDIR(observed.st_mode):
                # Every staging directory created by this writer is flat.  A
                # page assembly can hold at most one file per bounded page plus
                # its final CSV; leaf staging holds exactly two files.
                with os.scandir(path) as scanner:
                    for count, entry in enumerate(scanner, start=1):
                        if count > MAX_PAGES_PER_CELL + 4:
                            raise ResumeValidationError(
                                f"staging directory entry cap exceeded: {path}"
                            )
                        child = path / entry.name
                        child_stat = entry.stat(follow_symlinks=False)
                        if (
                            entry.is_symlink()
                            or int(getattr(child_stat, "st_file_attributes", 0) or 0)
                            & _FILE_ATTRIBUTE_REPARSE_POINT
                            or not stat.S_ISREG(child_stat.st_mode)
                        ):
                            raise ResumeValidationError(
                                f"staging directory contains an unsafe child: {child}"
                            )
                        child.unlink()
                path.rmdir()
            else:
                raise ResumeValidationError(f"staging path type is unsafe: {path}")
            removed += 1

        root_entries = _bounded_directory_entries(
            run_root,
            maximum_entries=len(symbol_set) + 2 + MAX_STAGING_ENTRIES_PER_DIRECTORY,
            context="authenticated run cleanup root",
        )
        for name, path in root_entries.items():
            if _is_staging_name(path):
                remove_candidate(path)
                continue
            if name in {RESUME_SEED_FILENAME, ATTEMPT_RECEIPTS_DIR}:
                continue
            if name not in symbol_set:
                raise ResumeValidationError(
                    f"unexpected entry before authenticated staging cleanup: {path}"
                )
            require_safe_directory(path, "authenticated symbol directory")
            symbol_entries = _bounded_directory_entries(
                path,
                maximum_entries=len(timeframe_set) + MAX_STAGING_ENTRIES_PER_DIRECTORY,
                context=f"authenticated symbol cleanup {name}",
            )
            for timeframe_name, child in symbol_entries.items():
                if _is_staging_name(child):
                    remove_candidate(child)
                elif timeframe_name not in timeframe_set:
                    raise ResumeValidationError(
                        f"unexpected timeframe before staging cleanup: {child}"
                    )

        attempts_root = run_root / ATTEMPT_RECEIPTS_DIR
        if attempts_root.exists():
            require_safe_directory(attempts_root, "retryable attempt root")
            attempt_symbols = _bounded_directory_entries(
                attempts_root,
                maximum_entries=len(symbol_set),
                context="attempt cleanup symbols",
            )
            for symbol_name, symbol_path in attempt_symbols.items():
                if symbol_name not in symbol_set:
                    raise ResumeValidationError(
                        f"unexpected attempt symbol before cleanup: {symbol_path}"
                    )
                require_safe_directory(symbol_path, "retryable attempt symbol")
                attempt_timeframes = _bounded_directory_entries(
                    symbol_path,
                    maximum_entries=len(timeframe_set),
                    context=f"attempt cleanup timeframes {symbol_name}",
                )
                for timeframe_name, timeframe_path in attempt_timeframes.items():
                    if timeframe_name not in timeframe_set:
                        raise ResumeValidationError(
                            f"unexpected attempt timeframe before cleanup: {timeframe_path}"
                        )
                    require_safe_directory(
                        timeframe_path,
                        "retryable attempt timeframe",
                    )
                    receipt_entries = _bounded_directory_entries(
                        timeframe_path,
                        maximum_entries=MAX_RETAINED_ATTEMPT_RECEIPTS_PER_CELL
                        + MAX_STAGING_ENTRIES_PER_DIRECTORY,
                        context=f"attempt cleanup receipts {symbol_name}/{timeframe_name}",
                    )
                    for receipt_path in receipt_entries.values():
                        if _is_staging_name(receipt_path):
                            remove_candidate(receipt_path)
        return removed

    def _validate_and_bound_attempt_receipts(
        self,
        *,
        symbols: Sequence[str],
        timeframes: Sequence[str],
    ) -> set[Tuple[str, str]]:
        """Validate reserved attempt topology and cap crash residue per cell."""

        attempts_root = self.run_root / ATTEMPT_RECEIPTS_DIR
        if not attempts_root.exists():
            return set()
        attempts_stat = attempts_root.lstat()
        if _unsafe_native_file_attributes(attempts_stat) or not stat.S_ISDIR(
            attempts_stat.st_mode
        ):
            raise ResumeValidationError(
                f"retryable attempt root is unsafe: {attempts_root}"
            )
        symbol_set = frozenset(symbols)
        timeframe_set = frozenset(timeframes)
        cells: set[Tuple[str, str]] = set()
        symbol_entries = _bounded_directory_entries(
            attempts_root,
            maximum_entries=len(symbol_set),
            context="retryable attempt symbols",
        )
        for symbol, symbol_dir in symbol_entries.items():
            observed_symbol = symbol_dir.lstat()
            if (
                _unsafe_native_file_attributes(observed_symbol)
                or not stat.S_ISDIR(observed_symbol.st_mode)
                or symbol not in symbol_set
            ):
                raise ResumeValidationError(
                    f"retryable attempt symbol entry is unsafe: {symbol_dir}"
                )
            timeframe_entries = _bounded_directory_entries(
                symbol_dir,
                maximum_entries=len(timeframe_set),
                context=f"retryable attempt timeframes {symbol}",
            )
            for timeframe, timeframe_dir in timeframe_entries.items():
                observed_timeframe = timeframe_dir.lstat()
                if (
                    _unsafe_native_file_attributes(observed_timeframe)
                    or not stat.S_ISDIR(observed_timeframe.st_mode)
                    or timeframe not in timeframe_set
                ):
                    raise ResumeValidationError(
                        f"retryable attempt timeframe entry is unsafe: {timeframe_dir}"
                    )
                cells.add((symbol, timeframe))
                receipt_entries = _bounded_directory_entries(
                    timeframe_dir,
                    maximum_entries=MAX_RETAINED_ATTEMPT_RECEIPTS_PER_CELL,
                    context=f"retryable attempt receipts {symbol}/{timeframe}",
                )
                for entry in receipt_entries.values():
                    observed_entry = entry.lstat()
                    if (
                        _unsafe_native_file_attributes(observed_entry)
                        or not stat.S_ISREG(observed_entry.st_mode)
                        or re.fullmatch(r"attempt-[0-9a-f]{32}\.json", entry.name)
                        is None
                        or not 0
                        < int(observed_entry.st_size)
                        <= MAX_ATTEMPT_RECEIPT_BYTES
                    ):
                        raise ResumeValidationError(
                            f"retryable attempt receipt is unsafe or oversized: {entry}"
                        )
                    envelope, _receipt = _load_pinned_native_json_object(
                        entry,
                        run_root=self.run_root,
                        maximum_bytes=MAX_ATTEMPT_RECEIPT_BYTES,
                    )
                    expected_fields = {
                        "schema_version",
                        "immutable_record",
                        "nonfinal",
                        "retryable",
                        "run_id",
                        "symbol",
                        "timeframe",
                        "retention_status",
                        "full_record_sha256",
                        "full_record_bytes",
                        "record",
                    }
                    retention_status = envelope.get("retention_status")
                    full_record_sha256 = envelope.get("full_record_sha256")
                    full_record_bytes = envelope.get("full_record_bytes")
                    retained_record = envelope.get("record")
                    if (
                        set(envelope) != expected_fields
                        or envelope.get("schema_version")
                        != ATTEMPT_RECEIPT_SCHEMA_VERSION
                        or envelope.get("immutable_record") is not True
                        or envelope.get("nonfinal") is not True
                        or envelope.get("retryable") is not True
                        or envelope.get("run_id") != self.run_id
                        or envelope.get("symbol") != symbol
                        or envelope.get("timeframe") != timeframe
                        or retention_status not in {"full", "summary_only"}
                        or not isinstance(full_record_sha256, str)
                        or re.fullmatch(r"[0-9a-f]{64}", full_record_sha256) is None
                        or type(full_record_bytes) is not int
                        or not 0 < full_record_bytes <= MAX_ARTIFACT_BYTES
                        or not isinstance(retained_record, Mapping)
                    ):
                        raise ResumeValidationError(
                            f"retryable attempt receipt binding is invalid: {entry}"
                        )
                    if retention_status == "full":
                        retained_bytes = json.dumps(
                            retained_record,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                        ).encode("utf-8")
                        if (
                            len(retained_bytes) != full_record_bytes
                            or hashlib.sha256(retained_bytes).hexdigest()
                            != full_record_sha256
                        ):
                            raise ResumeValidationError(
                                f"retryable attempt full-record digest is invalid: {entry}"
                            )
                    elif set(retained_record) != {
                        "status",
                        "failure_status",
                        "canonical_symbol",
                        "timeframe",
                        "acquisition_started_utc",
                        "acquisition_completed_utc",
                        "error",
                        "request_sha256",
                        "source_identity_sha256",
                    }:
                        raise ResumeValidationError(
                            f"retryable attempt summary schema is invalid: {entry}"
                        )
        return cells

    def _safe_attempt_cell_dir(self, symbol: str, timeframe: str) -> Path:
        """Create bounded reserved directories without following path indirection."""

        current = self.run_root
        for component in (
            ATTEMPT_RECEIPTS_DIR,
            _safe_name(symbol, "symbol"),
            _safe_name(timeframe, "timeframe").upper(),
        ):
            candidate = current / component
            if candidate.exists():
                if not candidate.is_dir() or candidate.is_symlink():
                    raise ResumeValidationError(
                        f"retryable attempt path component is unsafe: {candidate}"
                    )
            else:
                candidate.mkdir(exist_ok=False)
            try:
                candidate.resolve(strict=True).relative_to(
                    self.run_root.resolve(strict=True)
                )
            except ValueError as exc:
                raise ResumeValidationError(
                    f"retryable attempt path escapes the exact run: {candidate}"
                ) from exc
            current = candidate
        return current

    @staticmethod
    def _default_csv_writer(frame: pd.DataFrame, path: Path) -> None:
        frame.to_csv(path, index_label="time", lineterminator="\n")

    def leaf_path(self, symbol: str, timeframe: str) -> Path:
        return (
            self.run_root
            / _safe_name(symbol, "symbol")
            / _safe_name(timeframe, "timeframe")
        )

    def ensure_resume_contract(
        self,
        request: Mapping[str, Any],
        source_environment: Mapping[str, Any],
        acquisition_started_utc: datetime,
        *,
        symbols: Sequence[str],
        timeframes: Sequence[str],
        production_authoritative: bool,
    ) -> datetime:
        """Create or authenticate the immutable run seed before any cell attempt."""

        self._assert_writer_lock()
        self._assert_run_unsealed()
        expected_request = dict(_json_safe(request))
        expected_identity = _source_identity_for_request(
            source_environment,
            expected_request,
            context=f"run seed {self.run_id}",
        )
        expected_symbols = tuple(_safe_name(value, "symbol") for value in symbols)
        expected_timeframes = tuple(
            _safe_name(value, "timeframe").upper() for value in timeframes
        )
        if not expected_identity:
            raise ResumeValidationError(
                "cannot seed a resumable run without a broker/terminal source identity"
            )
        seed_path = self.run_root / RESUME_SEED_FILENAME
        if seed_path.exists():
            recovered = load_native_history_resume_contract(
                self.native_root,
                self.run_id,
            )
            if (
                dict(_json_safe(recovered.request)) != expected_request
                or dict(_json_safe(recovered.source_identity)) != expected_identity
                or recovered.symbols != expected_symbols
                or recovered.timeframes != expected_timeframes
                or recovered.production_authoritative
                is not bool(production_authoritative)
                or recovered.acquisition_started_utc is None
            ):
                raise ResumeValidationError(
                    "immutable resume contract differs from this acquisition"
                )
            return parse_utc_datetime(recovered.acquisition_started_utc)

        started = _utc(acquisition_started_utc)
        seed = {
            "schema_version": RESUME_SEED_SCHEMA_VERSION,
            "immutable_record": True,
            "run_id": self.run_id,
            "acquisition_started_utc": _iso(started),
            "request": expected_request,
            "source_identity": expected_identity,
            "symbols": list(expected_symbols),
            "timeframes": list(expected_timeframes),
            "production_authoritative": bool(production_authoritative),
        }
        _detached_seed, seed_payload = self._bounded_json_publication_bytes(
            seed,
            maximum_bytes=MAX_RESUME_SEED_BYTES,
            context="resume_seed",
        )
        tmp_path = self.run_root / f".{RESUME_SEED_FILENAME}.{uuid.uuid4().hex}.staging"
        try:
            staged_identity = self._stage_publication_payload(
                tmp_path,
                seed_payload,
                maximum_bytes=MAX_RESUME_SEED_BYTES,
            )
            if seed_path.exists():
                raise FileExistsError(
                    f"immutable resume contract appeared concurrently: {seed_path}"
                )
            try:
                # The resume seed is authority-bearing and immutable.  Publish
                # it with the same atomic no-replace primitive as the manifest;
                # an exists-then-replace sequence can overwrite a concurrent
                # file in the race window.
                self._publish_pinned_payload(
                    tmp_path,
                    seed_path,
                    expected=staged_identity,
                    expected_payload=seed_payload,
                    prelink_validator=self._assert_run_unsealed,
                    postlink_validator=self._assert_run_unsealed,
                )
            except Exception as exc:
                if seed_path.exists():
                    raise FileExistsError(
                        f"immutable resume contract appeared concurrently: {seed_path}"
                    ) from exc
                raise
        finally:
            tmp_path.unlink(missing_ok=True)
        return started

    def commit_retryable_attempt(
        self,
        symbol: str,
        timeframe: str,
        provenance: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Retain a bounded nonfinal failure receipt without poisoning the cell."""

        self._assert_writer_lock()
        self._assert_run_unsealed()
        observed_status = provenance.get("status")
        if observed_status not in {"failed", "partial"}:
            raise ValueError(
                "retryable attempt receipts require failed or technical-partial status"
            )

        # Take references to a finite allowlist first, then ask the shared V5
        # canonicalizer to walk/copy them under its depth/node/character caps.
        # Calling ``_json_safe`` on the complete provenance object here used to
        # permit an arbitrarily large failure diagnostic to exhaust memory
        # before the 256 KiB on-disk receipt cap was ever considered.
        projected = {
            key: provenance.get(key)
            for key in sorted(RETRYABLE_ATTEMPT_PROVENANCE_KEYS)
            if key in provenance
        }
        raw_error = projected.get("error")
        if isinstance(raw_error, Mapping):
            projected["error"] = _bounded_error_diagnostic(
                error_type=(
                    raw_error.get("type")
                    if isinstance(raw_error.get("type"), str)
                    else "Error"
                ),
                message=(
                    raw_error.get("message")
                    if isinstance(raw_error.get("message"), str)
                    else ""
                ),
            )
        else:
            projected["error"] = _bounded_error_diagnostic(
                error_type="MissingErrorDiagnostic",
                message="failure record did not supply a structured diagnostic",
            )
        try:
            projected_bytes = canonical_json_bytes(projected)
            if len(projected_bytes) > MAX_RETRYABLE_PROVENANCE_BYTES:
                raise NativeRequestContractError(
                    ("RETRYABLE_PROVENANCE_BYTE_CAP_EXCEEDED",)
                )
            decoded = json.loads(projected_bytes)
            if not isinstance(decoded, dict):
                raise NativeRequestContractError(
                    ("RETRYABLE_PROVENANCE_OBJECT_REQUIRED",)
                )
            record: Dict[str, Any] = decoded
        except NativeRequestContractError as exc:
            # Oversized/hostile optional diagnostics do not get to suppress the
            # typed retry state.  Retain a deliberately small, non-authoritative
            # record whose error says exactly why the richer receipt was dropped.
            def bounded_scalar(key: str, default: Any = None) -> Any:
                value = provenance.get(key, default)
                if value is None or isinstance(value, (bool, int)):
                    return value
                if isinstance(value, float):
                    return value if math.isfinite(value) else default
                if isinstance(value, str):
                    return value[:32_768]
                return default

            record = {
                "schema_version": bounded_scalar("schema_version", SCHEMA_VERSION),
                "acquisition_contract_version": bounded_scalar(
                    "acquisition_contract_version", ACQUISITION_CONTRACT_VERSION
                ),
                "immutable_record": True,
                "run_id": self.run_id,
                "status": observed_status,
                "failure_status": bounded_scalar(
                    "failure_status", "retryable_provenance_rejected"
                ),
                "eligible_for_optimization": False,
                "eligible_for_downstream_validation": False,
                "statistical_sufficiency_assessed": False,
                "canonical_symbol": _safe_name(symbol, "symbol"),
                "symbol": _safe_name(symbol, "symbol"),
                "broker_symbol": bounded_scalar("broker_symbol"),
                "timeframe": _safe_name(timeframe, "timeframe").upper(),
                "source_kind": bounded_scalar("source_kind", "broker_native"),
                "timezone": bounded_scalar("timezone", "UTC"),
                "acquisition_started_utc": bounded_scalar("acquisition_started_utc"),
                "acquisition_completed_utc": bounded_scalar(
                    "acquisition_completed_utc"
                ),
                "error": _bounded_error_diagnostic(
                    error_type="RetryableProvenanceBounded",
                    message=";".join(exc.errors),
                ),
            }

        # The envelope and retained record are bound to the method arguments,
        # never to potentially inconsistent caller-provided identity fields.
        record["run_id"] = self.run_id
        record["canonical_symbol"] = _safe_name(symbol, "symbol")
        record["timeframe"] = _safe_name(timeframe, "timeframe").upper()
        record.setdefault("timestamp_semantics", "bar_open_utc")
        record.setdefault("bar_interval_semantics", "half_open_[bar_open,bar_close)")
        record.setdefault("bar_close_column", "_BAR_CLOSE_TS")
        if "output_storage_resource_snapshot" not in record:
            record["output_storage_resource_snapshot"] = self._output_storage_snapshot(
                None
            )
        request_record = record.get("request")
        if isinstance(request_record, Mapping):
            request_record = dict(request_record)
            request_record["last_fully_closed_bar_open_utc"] = None
            record["request"] = request_record
        record.update(
            {
                "artifact": None,
                "sha256": None,
                "rows": 0,
                "start_ts": None,
                "end_ts": None,
                "complete": False,
                "eligible_for_optimization": False,
                "eligible_for_downstream_validation": False,
            }
        )
        output_snapshot = record.get("output_storage_resource_snapshot")
        if (
            not isinstance(output_snapshot, Mapping)
            or output_snapshot.get("status") != "within_bounds"
            or bool(output_snapshot.get("violations"))
        ):
            LOGGER.error(
                "retryable attempt receipt was not persisted because the output "
                "storage reserve is unavailable or breached: %s/%s",
                symbol,
                timeframe,
            )
            return record

        full_bytes = json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        attempt_request = record.get("request")
        attempt_source_identity = (
            _source_identity_for_request(
                record.get("source_environment"),
                attempt_request,
                context=f"retryable attempt {symbol}/{timeframe}",
            )
            if isinstance(attempt_request, Mapping)
            else _source_identity(record.get("source_environment"))
        )
        summary_record = {
            "status": record.get("status"),
            "failure_status": record.get("failure_status"),
            "canonical_symbol": record.get("canonical_symbol"),
            "timeframe": record.get("timeframe"),
            "acquisition_started_utc": record.get("acquisition_started_utc"),
            "acquisition_completed_utc": record.get("acquisition_completed_utc"),
            "error": record.get("error"),
            "request_sha256": canonical_sha256(record.get("request")),
            "source_identity_sha256": canonical_sha256(attempt_source_identity),
        }

        def _envelope(
            retention_status: str, retained_record: Mapping[str, Any]
        ) -> Dict[str, Any]:
            return {
                "schema_version": ATTEMPT_RECEIPT_SCHEMA_VERSION,
                "immutable_record": True,
                "nonfinal": True,
                "retryable": True,
                "run_id": self.run_id,
                "symbol": _safe_name(symbol, "symbol"),
                "timeframe": _safe_name(timeframe, "timeframe").upper(),
                "retention_status": retention_status,
                "full_record_sha256": hashlib.sha256(full_bytes).hexdigest(),
                "full_record_bytes": len(full_bytes),
                "record": retained_record,
            }

        envelope = _envelope("full", record)
        payload = json.dumps(
            envelope,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        if len(payload) + 1 > MAX_ATTEMPT_RECEIPT_BYTES:
            envelope = _envelope("summary_only", summary_record)
            payload = json.dumps(
                envelope,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        if len(payload) + 1 > MAX_ATTEMPT_RECEIPT_BYTES:
            raise OutputStorageResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["attempt_receipt_byte_cap_exceeded"],
                    "maximum_attempt_receipt_bytes": MAX_ATTEMPT_RECEIPT_BYTES,
                    "observed_attempt_receipt_bytes": len(payload) + 1,
                }
            )

        attempt_dir = self._safe_attempt_cell_dir(symbol, timeframe)
        retained: List[Tuple[int, str, Path, os.stat_result]] = []
        existing_entries = _bounded_directory_entries(
            attempt_dir,
            maximum_entries=MAX_RETAINED_ATTEMPT_RECEIPTS_PER_CELL,
            context=f"retryable attempt publish {symbol}/{timeframe}",
        )
        for path in existing_entries.values():
            observed = path.lstat()
            if (
                _unsafe_native_file_attributes(observed)
                or not stat.S_ISREG(observed.st_mode)
                or re.fullmatch(r"attempt-[0-9a-f]{32}\.json", path.name) is None
            ):
                raise ResumeValidationError(
                    f"retryable attempt directory contains an unsafe entry: {path}"
                )
            retained.append((int(observed.st_mtime_ns), path.name, path, observed))
        retained.sort(key=lambda item: (item[0], item[1]))
        keep_before_create = MAX_RETAINED_ATTEMPT_RECEIPTS_PER_CELL - 1
        stale_entries = (
            retained[:-keep_before_create] if keep_before_create else retained
        )
        for _mtime, _name, stale, identity in stale_entries:
            self._assert_run_unsealed()
            try:
                current = stale.lstat()
            except FileNotFoundError:
                continue
            if (
                int(current.st_dev),
                int(current.st_ino),
                int(current.st_size),
                int(current.st_mtime_ns),
            ) != (
                int(identity.st_dev),
                int(identity.st_ino),
                int(identity.st_size),
                int(identity.st_mtime_ns),
            ):
                raise ResumeValidationError(
                    f"retryable attempt changed before bounded retirement: {stale}"
                )
            stale.unlink()
        if stale_entries:
            _fsync_directory(attempt_dir)

        token = uuid.uuid4().hex
        receipt_path = attempt_dir / f"attempt-{token}.json"
        staging_path = attempt_dir / f".attempt-{token}.staging"
        publication_payload = payload + b"\n"
        try:
            staged_identity = self._stage_publication_payload(
                staging_path,
                publication_payload,
                maximum_bytes=MAX_ATTEMPT_RECEIPT_BYTES,
            )
            self._publish_pinned_payload(
                staging_path,
                receipt_path,
                expected=staged_identity,
                expected_payload=publication_payload,
                prelink_validator=self._assert_run_unsealed,
                postlink_validator=self._assert_run_unsealed,
            )
        finally:
            staging_path.unlink(missing_ok=True)
        return record

    @staticmethod
    def _retryable_publish_error(exc: BaseException) -> bool:
        return isinstance(exc, PermissionError) or getattr(exc, "winerror", None) in {
            5,
            32,
            33,
        }

    def _output_storage_snapshot(
        self, frame: Optional[pd.DataFrame | _NativeCellCsvArtifact]
    ) -> Dict[str, Any]:
        """Bound output growth without dropping or downcasting market rows."""

        try:
            usage = shutil.disk_usage(self.run_root)
        except OSError as exc:
            return {
                "status": "unavailable",
                "output_path": str(self.run_root),
                "minimum_free_bytes": OUTPUT_MIN_FREE_BYTES,
                "maximum_cell_bytes": OUTPUT_MAX_CELL_BYTES,
                "violations": ["output_disk_usage_unavailable"],
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        estimated_bytes = 0
        sample_rows = 0
        already_allocated = isinstance(frame, _NativeCellCsvArtifact)
        if already_allocated:
            estimated_bytes = int(frame.bytes)
            sample_rows = min(int(frame.rows), 2048)
        elif frame is not None and len(frame):
            sample_rows = min(len(frame), 2048)
            sample = frame.iloc[:sample_rows]
            buffer = io.StringIO()
            sample.to_csv(buffer, index_label="time", lineterminator="\n")
            sample_bytes = len(buffer.getvalue().encode("utf-8"))
            # CSV width can drift with later numeric magnitudes. A 25% margin
            # plus one MiB of fixed overhead keeps this a conservative bounded
            # admission estimate without changing any serialized value.
            estimated_bytes = int(
                math.ceil(sample_bytes * len(frame) / sample_rows * 1.25) + 1024**2
            )
        violations: List[str] = []
        if estimated_bytes > OUTPUT_MAX_CELL_BYTES:
            violations.append("output_cell_byte_cap_exceeded")
        reported_free = (
            min(int(usage.total), int(usage.free) + estimated_bytes)
            if already_allocated
            else int(usage.free)
        )
        projected_free = reported_free - estimated_bytes
        if projected_free < OUTPUT_MIN_FREE_BYTES:
            violations.append("output_disk_free_reserve_breached")
        return {
            "status": "limit_exceeded" if violations else "within_bounds",
            "output_path": str(self.run_root),
            "disk_total_bytes": int(usage.total),
            "disk_free_bytes": reported_free,
            "minimum_free_bytes": OUTPUT_MIN_FREE_BYTES,
            "maximum_cell_bytes": OUTPUT_MAX_CELL_BYTES,
            "estimated_cell_bytes": estimated_bytes,
            "sample_rows": sample_rows,
            "violations": violations,
        }

    def _clear_attempt_receipts_for_cell(self, symbol: str, timeframe: str) -> None:
        """Remove only this successfully published cell's bounded nonfinal receipts."""

        self._assert_run_unsealed()
        attempt_dir = (
            self.run_root
            / ATTEMPT_RECEIPTS_DIR
            / _safe_name(symbol, "symbol")
            / _safe_name(timeframe, "timeframe").upper()
        )
        try:
            attempt_stat = attempt_dir.lstat()
        except FileNotFoundError:
            return
        if _unsafe_native_file_attributes(attempt_stat) or not stat.S_ISDIR(
            attempt_stat.st_mode
        ):
            raise ResumeValidationError(
                f"successful-cell attempt directory is unsafe: {attempt_dir}"
            )
        entries = _bounded_directory_entries(
            attempt_dir,
            maximum_entries=(
                MAX_RETAINED_ATTEMPT_RECEIPTS_PER_CELL
                + MAX_STAGING_ENTRIES_PER_DIRECTORY
            ),
            context=f"successful-cell attempt cleanup {symbol}/{timeframe}",
        )
        for entry in entries.values():
            observed = entry.lstat()
            if (
                _unsafe_native_file_attributes(observed)
                or not stat.S_ISREG(observed.st_mode)
                or not (
                    re.fullmatch(r"attempt-[0-9a-f]{32}\.json", entry.name)
                    or _is_staging_name(entry)
                )
            ):
                raise ResumeValidationError(
                    f"successful-cell attempt cleanup found unsafe residue: {entry}"
                )
            entry.unlink()
        for directory in (
            attempt_dir,
            attempt_dir.parent,
            attempt_dir.parent.parent,
        ):
            try:
                directory.rmdir()
            except OSError:
                break

    def _remove_empty_attempt_root(self) -> None:
        """Remove a harmless empty root; never erase unresolved retry evidence."""

        attempts_root = self.run_root / ATTEMPT_RECEIPTS_DIR
        try:
            observed = attempts_root.lstat()
        except FileNotFoundError:
            return
        if _unsafe_native_file_attributes(observed) or not stat.S_ISDIR(
            observed.st_mode
        ):
            raise ResumeValidationError(
                f"retryable attempt root is unsafe before manifest: {attempts_root}"
            )
        try:
            with os.scandir(attempts_root) as scanner:
                first = next(scanner, None)
        except OSError as exc:
            raise ResumeValidationError(
                f"cannot inspect retryable attempt root before manifest: {attempts_root}"
            ) from exc
        if first is not None:
            raise ResumeValidationError(
                "cannot publish an immutable manifest while retryable attempt "
                f"evidence remains: {attempts_root / first.name}"
            )
        attempts_root.rmdir()

    def _assert_attempt_root_absent(self) -> None:
        try:
            self.run_root.joinpath(ATTEMPT_RECEIPTS_DIR).lstat()
        except FileNotFoundError:
            return
        raise ResumeValidationError(
            "retryable attempt area appeared during immutable manifest publication"
        )

    def _validate_existing_immutable_leaf(
        self,
        symbol: str,
        timeframe: str,
        expected: Mapping[str, Any],
        *,
        allow_status_mismatch: bool = False,
    ) -> Dict[str, Any]:
        """Recognize a concurrently completed leaf without ever rewriting it."""

        leaf = self.leaf_path(symbol, timeframe)
        if not leaf.is_dir() or leaf.is_symlink():
            raise ResumeValidationError(
                f"concurrent final leaf is not a directory: {leaf}"
            )
        leaf_entries = _bounded_directory_entries(
            leaf,
            maximum_entries=2,
            context=f"concurrent final leaf {symbol}/{timeframe}",
        )
        entries = set(leaf_entries)
        if "provenance.json" not in entries or not entries.issubset(
            {"bars.csv", "provenance.json"}
        ):
            raise ResumeValidationError(
                f"concurrent final leaf has unexpected contents: {leaf}"
            )
        sidecar = leaf / "provenance.json"
        if sidecar.is_symlink():
            raise ResumeValidationError(
                f"concurrent provenance cannot be a symlink: {sidecar}"
            )
        record = _load_json_object(sidecar)
        required = {
            "schema_version": SCHEMA_VERSION,
            "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
            "run_id": self.run_id,
            "canonical_symbol": symbol,
            "timeframe": timeframe,
            "source_kind": "broker_native",
            "timezone": "UTC",
        }
        expected_request = expected.get("request")
        exact_v5_identity = (
            isinstance(expected_request, Mapping)
            and expected_request.get("window_contract_version")
            == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
        )
        for key, required_value in required.items():
            observed = record.get(key)
            if key == "canonical_symbol" and not exact_v5_identity:
                observed = str(observed or "").upper()
                required_value = str(required_value).upper()
            elif key == "timeframe" and not exact_v5_identity:
                observed = str(observed or "").upper()
                required_value = str(required_value).upper()
            if observed != required_value:
                raise ResumeValidationError(
                    f"concurrent final leaf {key} mismatch in {sidecar}"
                )
        if record.get("immutable_record") is not True:
            raise ResumeValidationError(
                f"concurrent final leaf is not immutable: {sidecar}"
            )
        expected_contract: Mapping[str, Any] = {}
        if isinstance(expected_request, Mapping):
            expected_contract = _resume_request_contract(
                {"request": expected_request},
                sidecar,
            )
            if _resume_request_contract(record, sidecar) != expected_contract:
                raise ResumeValidationError(
                    f"concurrent final leaf request differs from this writer: {sidecar}"
                )
        expected_source = _source_identity_for_request(
            expected.get("source_environment"),
            expected_contract,
            context=f"concurrent writer expectation {sidecar}",
        )
        if (
            expected_source
            and _source_identity_for_request(
                record.get("source_environment"),
                expected_contract,
                context=f"concurrent writer artifact {sidecar}",
            )
            != expected_source
        ):
            raise ResumeValidationError(
                f"concurrent final leaf source differs from this writer: {sidecar}"
            )
        if not allow_status_mismatch and expected.get("status") is not None:
            if record.get("status") != expected.get("status"):
                raise ResumeValidationError(
                    f"concurrent final leaf status differs from this writer: {sidecar}"
                )

        artifact = record.get("artifact")
        csv_path = leaf / "bars.csv"
        if artifact is None:
            raise ResumeValidationError(
                f"provenance-only failures are nonfinal and cannot occupy a leaf: {leaf}"
            )
        if (
            not isinstance(artifact, Mapping)
            or artifact.get("relative_path") != "bars.csv"
        ):
            raise ResumeValidationError(
                f"concurrent artifact metadata is invalid: {sidecar}"
            )
        if not csv_path.is_file() or csv_path.is_symlink():
            raise ResumeValidationError(
                f"concurrent CSV is absent or unsafe: {csv_path}"
            )
        digest = _sha256(csv_path)
        rows = _count_csv_rows(csv_path)
        size = int(csv_path.stat().st_size)
        if (
            artifact.get("sha256") != digest
            or record.get("sha256") != digest
            or int(artifact.get("rows", -1)) != rows
            or int(record.get("rows", -1)) != rows
            or int(artifact.get("bytes", -1)) != size
            or not (
                (
                    record.get("status") == "success"
                    and record.get("failure_status") is None
                )
                or (
                    record.get("status") == "partial"
                    and record.get("failure_status") == "boundary_history_shortfall"
                )
            )
        ):
            raise ResumeValidationError(
                f"concurrent final CSV fails hash/row/byte validation: {csv_path}"
            )
        return record

    def _publish_leaf_directory(
        self,
        staging: Path,
        final_dir: Path,
        symbol: str,
        timeframe: str,
        expected: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Publish once, retrying transient OneDrive denial only while absent."""

        if final_dir.exists():
            return self._validate_existing_immutable_leaf(
                symbol,
                timeframe,
                expected,
            )
        attempts = len(self._publish_retry_delays) + 1
        last_error: Optional[BaseException] = None
        for attempt in range(attempts):
            try:
                self._assert_run_unsealed()
                self._replace_fn(staging, final_dir)
                return self._validate_existing_immutable_leaf(
                    symbol,
                    timeframe,
                    expected,
                )
            except Exception as exc:
                last_error = exc
                if final_dir.exists():
                    try:
                        return self._validate_existing_immutable_leaf(
                            symbol,
                            timeframe,
                            expected,
                        )
                    except Exception as validation_exc:
                        raise ResumeValidationError(
                            f"final leaf appeared during publish but is not a valid "
                            f"immutable completion: {final_dir}"
                        ) from validation_exc
                if not self._retryable_publish_error(exc) or attempt >= attempts - 1:
                    raise
                self._sleep_fn(self._publish_retry_delays[attempt])
        assert last_error is not None
        raise last_error

    def commit_leaf(
        self,
        symbol: str,
        timeframe: str,
        provenance: Mapping[str, Any],
        frame: Optional[pd.DataFrame | _NativeCellCsvArtifact],
    ) -> Dict[str, Any]:
        """Atomically publish one symbol/timeframe leaf; never overwrite it."""

        self._assert_writer_lock()
        self._assert_run_unsealed()
        status = provenance.get("status")
        failure_status = provenance.get("failure_status")
        publishable = bool(
            (status == "success" and failure_status is None)
            or (status == "partial" and failure_status == "boundary_history_shortfall")
        )
        if not publishable or frame is None or frame.empty:
            raise ValueError(
                "final native leaves require a nonempty success artifact or an "
                "allowed boundary_history_shortfall partial artifact"
            )
        if isinstance(frame, pd.DataFrame):
            direct_rows = int(len(frame))
            direct_bytes = int(frame.memory_usage(index=True, deep=True).sum())
            if (
                direct_rows > MAX_CSV_ROWS
                or direct_bytes > MAX_NATIVE_PAGE_IN_PROCESS_BYTES
            ):
                raise OutputStorageResourceLimitError(
                    {
                        "status": "limit_exceeded",
                        "violations": [
                            *(
                                ["direct_frame_row_cap_exceeded"]
                                if direct_rows > MAX_CSV_ROWS
                                else []
                            ),
                            *(
                                ["direct_frame_memory_cap_exceeded"]
                                if direct_bytes > MAX_NATIVE_PAGE_IN_PROCESS_BYTES
                                else []
                            ),
                        ],
                        "observed_rows": direct_rows,
                        "observed_memory_bytes": direct_bytes,
                    }
                )
        final_dir = self.leaf_path(symbol, timeframe)
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        record, _input_provenance_bytes = self._bounded_json_publication_bytes(
            provenance,
            maximum_bytes=MAX_REUSABLE_PROVENANCE_BYTES,
            context="leaf_provenance_input",
            compact=True,
        )
        record.setdefault("timestamp_semantics", "bar_open_utc")
        record.setdefault("bar_interval_semantics", "half_open_[bar_open,bar_close)")
        record.setdefault("bar_close_column", "_BAR_CLOSE_TS")
        if final_dir.exists():
            try:
                existing = self._validate_existing_immutable_leaf(
                    symbol,
                    timeframe,
                    record,
                )
                existing_artifact = existing.get("artifact")
                if not isinstance(existing_artifact, Mapping):
                    raise ResumeValidationError(
                        f"existing immutable leaf has no artifact binding: {final_dir}"
                    )
                self.register_artifact_bytes(
                    symbol,
                    timeframe,
                    int(existing_artifact.get("bytes", -1)),
                )
                self._clear_attempt_receipts_for_cell(symbol, timeframe)
                return existing
            finally:
                if isinstance(frame, _NativeCellCsvArtifact):
                    frame.cleanup()
        if frame is not None or "output_storage_resource_snapshot" not in record:
            output_snapshot = self._output_storage_snapshot(frame)
            if output_snapshot.get("status") != "within_bounds" or output_snapshot.get(
                "violations"
            ):
                if isinstance(frame, _NativeCellCsvArtifact):
                    frame.cleanup()
                raise OutputStorageResourceLimitError(output_snapshot)
            record["output_storage_resource_snapshot"] = output_snapshot
        staging = final_dir.parent / f".{timeframe}.{uuid.uuid4().hex}.staging"
        try:
            staging.mkdir(parents=False, exist_ok=False)
        except Exception:
            if isinstance(frame, _NativeCellCsvArtifact):
                frame.cleanup()
            raise
        try:
            csv_path = staging / "bars.csv"
            if isinstance(frame, _NativeCellCsvArtifact):
                frame.verify()
                # The exact bytes are already allocated inside this run.  Recheck
                # the cap/reserve immediately before moving them into the leaf.
                output_snapshot = self._output_storage_snapshot(frame)
                if output_snapshot.get(
                    "status"
                ) != "within_bounds" or output_snapshot.get("violations"):
                    raise OutputStorageResourceLimitError(output_snapshot)
                if self._uses_default_csv_writer:
                    os.replace(frame.csv_path, csv_path)
                    frame.consumed = True
                else:
                    # Dependency-injected writers remain an explicit failure
                    # seam for atomic-write tests; they never run in the
                    # production streaming path.
                    self._csv_writer(frame, csv_path)
            else:
                self._csv_writer(frame, csv_path)
            _fsync_file(csv_path)
            artifact_identity = _internal_spill_identity(csv_path)
            digest = _sha256(csv_path)
            artifact_rows = (
                int(frame.rows)
                if isinstance(frame, _NativeCellCsvArtifact)
                else int(len(frame))
            )
            if artifact_identity.size > OUTPUT_MAX_CELL_BYTES or (
                isinstance(frame, _NativeCellCsvArtifact)
                and digest != frame.scan.artifact.sha256
            ):
                raise OutputStorageResourceLimitError(
                    {
                        "status": "limit_exceeded",
                        "violations": ["output_cell_exact_byte_binding_failed"],
                        "actual_cell_bytes": artifact_identity.size,
                        "maximum_cell_bytes": OUTPUT_MAX_CELL_BYTES,
                    }
                )
            # Admission is based on the exact staged bytes, not on the earlier
            # sample estimate.  Refuse cap+1 before a leaf or sidecar exists.
            self._enforce_artifact_byte_admission(
                symbol,
                timeframe,
                int(artifact_identity.size),
            )
            record["artifact"] = {
                "relative_path": "bars.csv",
                "sha256": digest,
                "bytes": int(artifact_identity.size),
                "rows": artifact_rows,
            }
            record.update(
                {
                    "sha256": digest,
                    "rows": artifact_rows,
                    "start_ts": (
                        frame.start_ts.isoformat()
                        if isinstance(frame, _NativeCellCsvArtifact)
                        else pd.Timestamp(frame.index[0]).isoformat()
                    ),
                    "end_ts": (
                        frame.end_ts.isoformat()
                        if isinstance(frame, _NativeCellCsvArtifact)
                        else pd.Timestamp(frame.index[-1]).isoformat()
                    ),
                    "complete": bool(record.get("status") == "success"),
                }
            )
            sidecar = staging / "provenance.json"
            record, sidecar_payload = self._bounded_json_publication_bytes(
                record,
                maximum_bytes=MAX_REUSABLE_PROVENANCE_BYTES,
                context="leaf_provenance",
            )
            sidecar_identity = self._stage_publication_payload(
                sidecar,
                sidecar_payload,
                maximum_bytes=MAX_REUSABLE_PROVENANCE_BYTES,
            )
            # Pin exact staged bytes again immediately before the atomic
            # directory publication.  A cap+1 growth, free-space fall, link
            # swap, or content mutation leaves no final leaf.
            current_identity = _internal_spill_identity(csv_path)
            current_snapshot = self._output_storage_snapshot(
                frame if isinstance(frame, _NativeCellCsvArtifact) else None
            )
            if (
                current_identity != artifact_identity
                or _sha256(csv_path) != digest
                or current_snapshot.get("status") != "within_bounds"
                or current_snapshot.get("violations")
            ):
                raise OutputStorageResourceLimitError(
                    {
                        **current_snapshot,
                        "status": "limit_exceeded",
                        "violations": list(current_snapshot.get("violations") or [])
                        + ["output_staged_identity_or_digest_changed"],
                    }
                )
            self._verify_publication_payload(
                sidecar,
                expected=sidecar_identity,
                expected_payload=sidecar_payload,
                links=1,
            )
            published = self._publish_leaf_directory(
                staging,
                final_dir,
                symbol,
                timeframe,
                record,
            )
            published_artifact = published.get("artifact")
            if not isinstance(published_artifact, Mapping):
                raise ResumeValidationError(
                    f"published immutable leaf has no artifact binding: {final_dir}"
                )
            self.register_artifact_bytes(
                symbol,
                timeframe,
                int(published_artifact.get("bytes", -1)),
            )
            self._clear_attempt_receipts_for_cell(symbol, timeframe)
            return published
        finally:
            _best_effort_bounded_flat_cleanup(staging, maximum_entries=4)
            if isinstance(frame, _NativeCellCsvArtifact):
                frame.cleanup()

    def commit_write_failure(
        self,
        symbol: str,
        timeframe: str,
        provenance: Mapping[str, Any],
        exc: BaseException,
    ) -> Dict[str, Any]:
        """Record a bounded retryable write failure outside the final leaf."""

        self._assert_writer_lock()
        failure = dict(provenance)
        failure.update(
            {
                "status": "failed",
                "failure_status": "write_failed",
                "eligible_for_optimization": False,
                "eligible_for_downstream_validation": False,
                "error": _bounded_error_diagnostic(exc),
            }
        )
        return self.commit_retryable_attempt(symbol, timeframe, failure)

    def commit_manifest(
        self,
        manifest: Mapping[str, Any],
        *,
        precommit_validator: Optional[Callable[[], None]] = None,
    ) -> Path:
        self._assert_writer_lock()
        self._assert_run_unsealed()
        _detached_manifest, manifest_payload = self._bounded_json_publication_bytes(
            manifest,
            maximum_bytes=MAX_REUSABLE_PROVENANCE_BYTES,
            context="run_manifest",
        )
        self._remove_empty_attempt_root()
        self._assert_run_unsealed()
        final_path = self.run_root / "run_manifest.json"
        if final_path.exists():
            raise FileExistsError(
                f"immutable run manifest already exists: {final_path}"
            )
        # Stage beside (not inside) the immutable run.  Production precommit
        # receipts pin the run-directory mutation identity, so creating the
        # writer's own temporary file inside that directory would invalidate
        # the very TOCTOU guard we need immediately before publication.
        tmp_path = self.runs_root / f".{self.run_id}.run_manifest.staging"
        try:
            try:
                stale = tmp_path.lstat()
            except FileNotFoundError:
                pass
            else:
                if (
                    _unsafe_native_file_attributes(stale)
                    or not stat.S_ISREG(stale.st_mode)
                    or int(stale.st_nlink) != 1
                ):
                    raise ResumeValidationError(
                        f"manifest staging authority is unsafe: {tmp_path}"
                    )
                tmp_path.unlink()
            staged_identity = self._stage_publication_payload(
                tmp_path,
                manifest_payload,
                maximum_bytes=MAX_REUSABLE_PROVENANCE_BYTES,
            )
            attempts = len(self._publish_retry_delays) + 1
            for attempt in range(attempts):
                try:

                    def validate_before_link() -> None:
                        if precommit_validator is not None:
                            precommit_validator()
                        self._assert_run_unsealed()
                        self._assert_attempt_root_absent()

                    self._publish_pinned_payload(
                        tmp_path,
                        final_path,
                        expected=staged_identity,
                        expected_payload=manifest_payload,
                        prelink_validator=validate_before_link,
                        postlink_validator=self._assert_attempt_root_absent,
                    )
                except Exception as exc:
                    if final_path.exists():
                        raise FileExistsError(
                            f"immutable run manifest appeared concurrently: {final_path}"
                        ) from exc
                    if (
                        not self._retryable_publish_error(exc)
                        or attempt >= attempts - 1
                    ):
                        raise
                    self._sleep_fn(self._publish_retry_delays[attempt])
                else:
                    return final_path
            raise RuntimeError("create-only manifest publication exhausted its retries")
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise


def snapshot_read_only_runtime_metadata(
    connector: Any,
    terminal_api: Any = None,
) -> Dict[str, Any]:
    """Capture broker/terminal provenance using information-only calls.

    ``terminal_api`` is deliberately injected by ``main`` (and by tests).  This
    function only invokes ``terminal_info``, ``version`` and account reads; it
    has no dependency on or reference to any execution operation.
    """

    metadata: Dict[str, Any] = {}
    errors: Dict[str, str] = {}

    public_reader = getattr(connector, "get_runtime_metadata", None)
    if callable(public_reader):
        try:
            value = public_reader()
            if isinstance(value, Mapping):
                return dict(_json_safe(value))
        except Exception as exc:
            errors["connector_runtime_metadata"] = f"{type(exc).__name__}: {exc}"

    account_reader = getattr(connector, "get_account_info", None)
    if callable(account_reader):
        try:
            account = account_reader()
            if account is not None:
                metadata["account"] = {
                    attr: _json_safe(getattr(account, attr))
                    for attr in (
                        "login",
                        "server",
                        "company",
                        "currency",
                        "leverage",
                        "trade_allowed",
                        "trade_expert",
                    )
                    if hasattr(account, attr)
                }
        except Exception as exc:
            errors["account_info"] = f"{type(exc).__name__}: {exc}"

    config = getattr(connector, "config", None)
    if config is not None:
        metadata["configured_connection"] = {
            "server": getattr(config, "server", "") or None,
            "terminal_path": getattr(config, "path", "") or None,
            "expected_login": getattr(config, "expected_login", 0) or None,
            "expected_server": getattr(config, "expected_server", "") or None,
            "expected_currency": getattr(config, "expected_currency", "") or None,
        }

    terminal: Dict[str, Any] = {}
    if terminal_api is not None:
        info_reader = getattr(terminal_api, "terminal_info", None)
        if callable(info_reader):
            try:
                info = info_reader()
                if info is not None:
                    for attr in (
                        "name",
                        "company",
                        "path",
                        "data_path",
                        "commondata_path",
                        "build",
                        "maxbars",
                        "connected",
                        "trade_allowed",
                        "tradeapi_disabled",
                        "dlls_allowed",
                    ):
                        if hasattr(info, attr):
                            key = "max_bars" if attr == "maxbars" else attr
                            terminal[key] = _json_safe(getattr(info, attr))
            except Exception as exc:
                errors["terminal_info"] = f"{type(exc).__name__}: {exc}"
        version_reader = getattr(terminal_api, "version", None)
        if callable(version_reader):
            try:
                version = version_reader()
                terminal["version"] = _json_safe(version)
            except Exception as exc:
                errors["terminal_version"] = f"{type(exc).__name__}: {exc}"

    if not terminal:
        maxbars_reader = getattr(connector, "_terminal_maxbars", None)
        if callable(maxbars_reader):
            try:
                terminal["max_bars"] = int(maxbars_reader() or 0)
            except Exception as exc:
                errors["terminal_max_bars"] = f"{type(exc).__name__}: {exc}"
    if terminal:
        metadata["terminal"] = terminal

    metadata["metadata_status"] = "available" if metadata and not errors else "partial"
    if errors:
        metadata["metadata_errors"] = errors
    return dict(_json_safe(metadata))


class NativeHistoryAcquirer:
    """Read-only MT5 history acquisition with per-timeframe fault isolation."""

    REQUIRED_COLUMNS = ("Open", "High", "Low", "Close")

    def __init__(
        self,
        connector: Any,
        store: NativeHistoryStore,
        request: NativeHistoryRequest,
        *,
        now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        sleep_fn: Callable[[float], None] = time.sleep,
        runtime_metadata: Optional[Mapping[str, Any]] = None,
        terminal_api: Any = None,
    ) -> None:
        self.connector = connector
        self.terminal_api = terminal_api
        self.store = store
        self.request = request
        connector_config = getattr(connector, "config", None)
        if connector_config is not None and hasattr(
            connector_config, "history_page_size"
        ):
            raw_trigger_cap = getattr(connector_config, "history_page_size")
            try:
                configured_trigger_cap = int(raw_trigger_cap or 50_000)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(
                    "V5 acquisition requires a valid MT5 history_page_size"
                ) from exc
            effective_trigger_cap = max(1, min(configured_trigger_cap, 50_000))
            if effective_trigger_cap != 50_000:
                raise ValueError(
                    "V5 acquisition requires an effective 50,000-bar MT5 history "
                    "download trigger cap so stored V4 receipts remain independently "
                    "recomputable"
                )
        self.now_fn = now_fn
        self.sleep_fn = sleep_fn
        self.requested_information_cutoff_utc = _utc(request.as_of_utc or now_fn())
        # Calendar v2 anchors every timeframe to the end of the same fully
        # closed native M5 bar. H4/D1 closure is still determined from their
        # broker-observed successors, never by UTC modulo.
        self.as_of_utc = floor_utc_boundary(
            self.requested_information_cutoff_utc,
            TIMEFRAME_SECONDS["M5"],
        )
        requested_start = subtract_calendar_years(self.as_of_utc, request.horizon_years)
        self.window_contract_version = str(request.window_contract_version)
        self.timeframe_partition_windows: Dict[
            str, Dict[str, Tuple[datetime, datetime]]
        ] = {}
        self.timeframe_required_evidence_lead_start_utc: Dict[
            str, Optional[datetime]
        ] = {}
        self.timeframe_requested_start_utc: Dict[str, datetime] = {}
        if self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            assert request.timeframe_window_years is not None
            for timeframe in request.timeframes:
                boundaries = build_timeframe_calendar_boundaries(
                    self.as_of_utc,
                    timeframe,
                    timeframe_window_years=request.timeframe_window_years,
                    embargo=pd.Timedelta(days=float(request.embargo_days)),
                )
                cell_windows = {
                    partition: (
                        start.to_pydatetime(),
                        end.to_pydatetime(),
                    )
                    for partition, (start, end) in boundaries.windows.items()
                }
                self.timeframe_partition_windows[timeframe] = cell_windows
                if request.enforce_evidence_horizon:
                    train_start = cell_windows["optimizer_train"][0]
                    evidence_lead_start = train_start - timedelta(
                        days=float(request.warmup_days) + float(request.safety_days),
                        seconds=TIMEFRAME_SECONDS[timeframe],
                    )
                    cell_start = evidence_lead_start
                else:
                    evidence_lead_start = None
                    cell_start = requested_start
                self.timeframe_required_evidence_lead_start_utc[timeframe] = (
                    evidence_lead_start
                )
                self.timeframe_requested_start_utc[timeframe] = cell_start
            compatibility_timeframe = (
                "M5" if "M5" in request.timeframes else request.timeframes[0]
            )
            self.partition_windows = self.timeframe_partition_windows[
                compatibility_timeframe
            ]
            self.common_start_utc = min(self.timeframe_requested_start_utc.values())
            evidence_starts = [
                value
                for value in self.timeframe_required_evidence_lead_start_utc.values()
                if value is not None
            ]
            evidence_lead_start = min(evidence_starts) if evidence_starts else None
        else:
            self.partition_windows = build_common_partition_windows(
                self.as_of_utc,
                request.window_years or DEFAULT_WINDOW_YEARS,
                request.embargo_days,
            )
            if request.enforce_evidence_horizon:
                train_start = self.partition_windows["optimizer_train"][0]
                closed_bar_slack = max(
                    TIMEFRAME_SECONDS[tf] for tf in request.timeframes
                )
                evidence_lead_start = train_start - timedelta(
                    days=float(request.warmup_days) + float(request.safety_days),
                    seconds=closed_bar_slack,
                )
                self.common_start_utc = min(requested_start, evidence_lead_start)
            else:
                evidence_lead_start = None
                self.common_start_utc = requested_start
            for timeframe in request.timeframes:
                self.timeframe_partition_windows[timeframe] = dict(
                    self.partition_windows
                )
                self.timeframe_required_evidence_lead_start_utc[timeframe] = (
                    evidence_lead_start
                )
                self.timeframe_requested_start_utc[timeframe] = self.common_start_utc
        self.requested_horizon_start_utc = requested_start
        self.required_evidence_lead_start_utc = evidence_lead_start
        self.timeframe_strategy_evidence_intervals = {
            timeframe: self._source_request_interval(
                timeframe,
                self.timeframe_requested_start_utc[timeframe],
                include_containing_start=(
                    self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                ),
            )
            for timeframe in request.timeframes
        }
        self.timeframe_audit_reference_map = (
            cross_resolution_reference_map(request.timeframes)
            if self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
            else {}
        )
        audit_roster = (
            DEFAULT_TIMEFRAMES
            if self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
            else request.timeframes
        )
        self.timeframe_audit_consumer_map: Dict[str, List[str]] = {
            timeframe: [] for timeframe in audit_roster
        }
        for target in audit_roster:
            for reference in self.timeframe_audit_reference_map.get(target, []):
                self.timeframe_audit_consumer_map[reference].append(target)

        # These are semantic target-clock windows.  A source-boundary transform
        # is applied exactly once per target/reference edge below.  In
        # particular, a broker-aligned H4/D1 strategy interval (which already
        # includes its own predecessor sentinel) must not become the starting
        # point for a second reference transform.
        self.timeframe_audit_windows: Dict[
            str, Dict[str, Tuple[datetime, datetime]]
        ] = {}
        for target in request.timeframes:
            target_windows: Dict[str, Tuple[datetime, datetime]] = {
                "strategy_full_context": (
                    self.timeframe_requested_start_utc[target],
                    self.as_of_utc,
                )
            }
            target_windows.update(self.timeframe_partition_windows[target])
            self.timeframe_audit_windows[target] = target_windows

        self.timeframe_audit_reference_intervals: Dict[
            str, Dict[str, Dict[str, Tuple[datetime, datetime]]]
        ] = {}
        for target in request.timeframes:
            edge_intervals: Dict[str, Dict[str, Tuple[datetime, datetime]]] = {}
            for reference in self.timeframe_audit_reference_map.get(target, []):
                edge_intervals[reference] = {
                    role: self._bounded_source_interval(reference, start, end)
                    for role, (start, end) in self.timeframe_audit_windows[
                        target
                    ].items()
                }
            self.timeframe_audit_reference_intervals[target] = edge_intervals

        # There remains exactly one immutable artifact per source timeframe.
        # Its fetch interval is the union of its own strategy evidence and every
        # edge for which that source is a comparator.  Metadata stays bounded by
        # the fixed six-node/twelve-edge graph rather than growing with bars.
        source_interval_parts: Dict[str, List[Tuple[datetime, datetime]]] = {
            timeframe: [self.timeframe_strategy_evidence_intervals[timeframe]]
            for timeframe in request.timeframes
        }
        for target, references in self.timeframe_audit_reference_intervals.items():
            for reference, audit_intervals in references.items():
                source_interval_parts[reference].extend(audit_intervals.values())
        self.timeframe_source_request_intervals = {
            timeframe: (
                min(interval[0] for interval in intervals),
                max(interval[1] for interval in intervals),
            )
            for timeframe, intervals in source_interval_parts.items()
        }

        self.timeframe_audit_only_intervals: Dict[str, List[Dict[str, Any]]] = {
            timeframe: [] for timeframe in request.timeframes
        }
        for source, consumers in self.timeframe_audit_consumer_map.items():
            own_start, own_end = self.timeframe_strategy_evidence_intervals[source]
            for target in consumers:
                edge_start, edge_end = self.timeframe_audit_reference_intervals[target][
                    source
                ]["strategy_full_context"]
                if edge_start < own_start:
                    self.timeframe_audit_only_intervals[source].append(
                        {
                            "consumer_target_timeframe": target,
                            "audit_window": "strategy_full_context",
                            "start_utc": edge_start,
                            "end_exclusive_utc": min(edge_end, own_start),
                            "role": "audit_only_reference",
                        }
                    )
                if edge_end > own_end:
                    self.timeframe_audit_only_intervals[source].append(
                        {
                            "consumer_target_timeframe": target,
                            "audit_window": "strategy_full_context",
                            "start_utc": max(edge_start, own_end),
                            "end_exclusive_utc": edge_end,
                            "role": "audit_only_reference",
                        }
                    )
        self.timeframe_consensus_policy = dict(TWO_REFERENCE_CONSENSUS_POLICY)
        self.runtime_metadata = dict(runtime_metadata or self._read_runtime_metadata())
        self._v5_cell_validation_context = None
        self._v5_common_request_contract: Optional[Dict[str, Any]] = None
        self._v5_expected_source_identity: Optional[Dict[str, Any]] = None
        if self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            try:
                canonical_source_identity(self.runtime_metadata)
                self._v5_common_request_contract = self._common_request_contract()
                self._v5_expected_source_identity = _source_identity_for_request(
                    self.runtime_metadata,
                    self._v5_common_request_contract,
                    context="initial connected V5 runtime",
                )
                self._v5_cell_validation_context = build_v5_cell_validation_context(
                    self._v5_common_request_contract,
                    self.runtime_metadata,
                    self.request.retries,
                )
            except NativeRequestContractError as exc:
                raise ResumeValidationError(
                    "V5 acquisition requires a complete trusted broker/terminal "
                    f"source identity before any cell is attempted: {';'.join(exc.errors)}"
                ) from exc
        self.terminal_max_bars = self._extract_terminal_max_bars(self.runtime_metadata)
        for broker_timeframe in {"H4", "D1"} & set(request.timeframes):
            configured_slots = max(
                1,
                int(
                    DEFAULT_PAGE_DAYS[broker_timeframe]
                    * 86_400
                    // TIMEFRAME_SECONDS[broker_timeframe]
                ),
            )
            safe_slots = max(1, int(self.terminal_max_bars * 0.90))
            page_span_seconds = (
                min(configured_slots, safe_slots) * TIMEFRAME_SECONDS[broker_timeframe]
            )
            if page_span_seconds < 14 * 86_400:
                raise ValueError(
                    "V5 H4/D1 acquisition requires at least a 14-day bounded page "
                    "span so a final forming-only broker page cannot be mistaken for "
                    "an empty historical interval"
                )
        self.resume_contract = store.resume_contract
        if store.resume:
            if self.resume_contract is None:
                raise ResumeValidationError(
                    "resume store has no recovered immutable contract"
                )
            expected_request = self._common_request_contract()
            if expected_request != dict(self.resume_contract.request):
                raise ResumeValidationError(
                    "resume request does not exactly match the original cutoff, calendar "
                    "boundaries, horizon, warmup, safety, and timeframe closure contract"
                )
            expected_production_authoritative = bool(
                self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                and tuple(self.request.symbols) == tuple(PRODUCTION_SYMBOL_ORDER)
                and tuple(self.request.timeframes) == tuple(DEFAULT_TIMEFRAMES)
            )
            if (
                tuple(self.request.symbols) != self.resume_contract.symbols
                or tuple(self.request.timeframes) != self.resume_contract.timeframes
                or expected_production_authoritative
                is not self.resume_contract.production_authoritative
            ):
                raise ResumeValidationError(
                    "resume target roster or production intent differs from the "
                    "immutable run contract"
                )
            current_identity = _source_identity_for_request(
                self.runtime_metadata,
                expected_request,
                context="connected V5 runtime",
            )
            if current_identity != dict(self.resume_contract.source_identity):
                raise ResumeValidationError(
                    "connected account/terminal source identity does not match the original run"
                )

    def _read_runtime_metadata(self) -> Dict[str, Any]:
        return snapshot_read_only_runtime_metadata(
            self.connector,
            self.terminal_api,
        )

    def _assert_runtime_source_identity(self, boundary: str) -> None:
        """Fail before publication if the connected broker source changes."""

        if self.window_contract_version != TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            return
        expected = self._v5_expected_source_identity
        request = self._v5_common_request_contract
        if expected is None or request is None:
            raise ResumeValidationError("V5 source identity was not initialized")
        try:
            observed = _source_identity_for_request(
                self._read_runtime_metadata(),
                request,
                context=f"connected V5 runtime at {boundary}",
            )
        except ResumeValidationError as exc:
            raise ResumeValidationError(
                f"connected source identity became unavailable at {boundary}"
            ) from exc
        if observed != expected:
            raise ResumeValidationError(
                f"connected account/terminal source identity changed at {boundary}"
            )

    def _source_request_interval(
        self,
        timeframe: str,
        requested_start: datetime,
        *,
        include_containing_start: bool = False,
    ) -> Tuple[datetime, datetime]:
        seconds = TIMEFRAME_SECONDS[timeframe]
        if include_containing_start:
            return self._bounded_source_interval(
                timeframe,
                requested_start,
                self.as_of_utc,
            )
        if timeframe in {"H4", "D1"}:
            return (
                requested_start - timedelta(seconds=seconds),
                self.as_of_utc + timedelta(seconds=1),
            )
        return (
            ceil_utc_boundary(requested_start, seconds),
            floor_utc_boundary(self.as_of_utc, seconds),
        )

    @staticmethod
    def _bounded_source_interval(
        timeframe: str,
        window_start: datetime,
        window_end: datetime,
    ) -> Tuple[datetime, datetime]:
        """Expand one semantic window to the source's native fetch boundaries."""

        return native_source_interval(timeframe, window_start, window_end)

    def _serialized_timeframe_boundaries(self) -> Dict[str, Dict[str, Dict[str, str]]]:
        return {
            timeframe: {
                partition: {"start_ts": _iso(start), "end_ts": _iso(end)}
                for partition, (start, end) in windows.items()
            }
            for timeframe, windows in self.timeframe_partition_windows.items()
        }

    def _serialized_timeframe_intervals(self) -> Dict[str, Dict[str, str]]:
        return {
            timeframe: {
                "start_utc": _iso(start),
                "end_exclusive_utc": _iso(end),
            }
            for timeframe, (
                start,
                end,
            ) in self.timeframe_source_request_intervals.items()
        }

    def _serialized_strategy_evidence_intervals(self) -> Dict[str, Dict[str, str]]:
        return {
            timeframe: {
                "start_utc": _iso(start),
                "end_exclusive_utc": _iso(end),
            }
            for timeframe, (start, end) in (
                self.timeframe_strategy_evidence_intervals.items()
            )
        }

    @staticmethod
    def _serialized_interval(
        interval: Tuple[datetime, datetime],
    ) -> Dict[str, str]:
        return {
            "start_utc": str(_iso(interval[0])),
            "end_exclusive_utc": str(_iso(interval[1])),
        }

    def _serialized_audit_windows(
        self,
    ) -> Dict[str, Dict[str, Dict[str, str]]]:
        return {
            target: {
                role: self._serialized_interval(interval)
                for role, interval in windows.items()
            }
            for target, windows in self.timeframe_audit_windows.items()
        }

    def _serialized_audit_reference_intervals(
        self,
    ) -> Dict[str, Dict[str, Dict[str, Dict[str, str]]]]:
        return {
            target: {
                reference: {
                    role: self._serialized_interval(interval)
                    for role, interval in windows.items()
                }
                for reference, windows in references.items()
            }
            for target, references in (self.timeframe_audit_reference_intervals.items())
        }

    def _serialized_audit_only_intervals(self) -> Dict[str, List[Dict[str, Any]]]:
        return {
            source: [
                {
                    **{
                        key: value
                        for key, value in interval.items()
                        if key not in {"start_utc", "end_exclusive_utc"}
                    },
                    "start_utc": _iso(interval["start_utc"]),
                    "end_exclusive_utc": _iso(interval["end_exclusive_utc"]),
                }
                for interval in intervals
            ]
            for source, intervals in self.timeframe_audit_only_intervals.items()
        }

    def _timeframe_request_fields(self) -> Dict[str, Any]:
        boundaries = self._serialized_timeframe_boundaries()
        intervals = self._serialized_timeframe_intervals()
        strategy_intervals = self._serialized_strategy_evidence_intervals()
        audit_windows = self._serialized_audit_windows()
        audit_reference_intervals = self._serialized_audit_reference_intervals()
        audit_only_intervals = self._serialized_audit_only_intervals()
        leads = {
            timeframe: _iso(value)
            for timeframe, value in self.timeframe_required_evidence_lead_start_utc.items()
        }
        years = {
            timeframe: dict((self.request.timeframe_window_years or {})[timeframe])
            for timeframe in self.request.timeframes
        }
        fields: Dict[str, Any] = {
            "window_contract_version": TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
            "timeframe_request_map_schema_version": (
                TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
            ),
            "timeframe_window_years": years,
            "timeframe_calendar_boundaries": boundaries,
            "timeframe_calendar_boundaries_sha256": _canonical_json_sha256(boundaries),
            "timeframe_required_evidence_lead_start_utc": leads,
            "timeframe_source_request_intervals": intervals,
            "timeframe_strategy_evidence_intervals": strategy_intervals,
            "timeframe_audit_reference_map": {
                target: list(references)
                for target, references in self.timeframe_audit_reference_map.items()
            },
            "timeframe_audit_consumer_map": {
                source: list(consumers)
                for source, consumers in self.timeframe_audit_consumer_map.items()
            },
            "timeframe_audit_windows": audit_windows,
            "timeframe_audit_reference_intervals": audit_reference_intervals,
            "timeframe_audit_only_intervals": audit_only_intervals,
            "timeframe_consensus_policy": dict(self.timeframe_consensus_policy),
        }
        fields["timeframe_request_map_sha256"] = _canonical_json_sha256(
            _timeframe_request_hash_payload(
                {
                    **fields,
                    "common_as_of_utc": _iso(self.as_of_utc),
                }
            )
        )
        return fields

    def _common_request_contract(self) -> Dict[str, Any]:
        request: Dict[str, Any] = {
            "horizon_years": int(self.request.horizon_years),
            "requested_information_cutoff_utc": _iso(
                self.requested_information_cutoff_utc
            ),
            "requested_horizon_start_utc": _iso(self.requested_horizon_start_utc),
            "common_requested_start_utc": _iso(self.common_start_utc),
            "required_evidence_lead_start_utc": _iso(
                self.required_evidence_lead_start_utc
            ),
            "warmup_days": float(self.request.warmup_days),
            "safety_days": float(self.request.safety_days),
            "closed_bar_slack_seconds": max(
                TIMEFRAME_SECONDS[tf] for tf in self.request.timeframes
            ),
            "common_as_of_utc": _iso(self.as_of_utc),
            "boundary_tolerance_days": float(self.request.boundary_tolerance_days),
            "window_years": dict(self.request.window_years or {}),
            "embargo_days": float(self.request.embargo_days),
            "common_calendar_boundaries": {
                partition: {"start_ts": _iso(start), "end_ts": _iso(end)}
                for partition, (start, end) in self.partition_windows.items()
            },
        }
        if self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            request.update(self._timeframe_request_fields())
            return {key: _json_safe(request[key]) for key in _V5_RESUME_REQUEST_KEYS}
        return {key: _json_safe(request[key]) for key in _LEGACY_RESUME_REQUEST_KEYS}

    def _validate_reusable_leaf(
        self,
        symbol: str,
        timeframe: str,
        leaf: Path,
        *,
        manifest_result: Optional[Mapping[str, Any]] = None,
        receipts: Optional[List[_PinnedNativeFileReceipt]] = None,
    ) -> Dict[str, Any]:
        """Fully validate an existing immutable leaf before any broker fetch."""

        leaf_entries = _bounded_directory_entries(
            leaf,
            maximum_entries=2,
            context=f"reusable leaf {symbol}/{timeframe}",
        )
        entries = set(leaf_entries)
        sidecar = leaf / "provenance.json"
        csv_path = leaf / "bars.csv"
        if sidecar.is_symlink() or (csv_path.exists() and csv_path.is_symlink()):
            raise ResumeValidationError(
                f"symlinks are not reusable acquisition artifacts: {leaf}"
            )
        record, provenance_receipt = _load_pinned_native_json_object(
            sidecar,
            run_root=self.store.run_root,
        )
        if receipts is not None:
            receipts.append(provenance_receipt)
        status = record.get("status")
        expected_entries = (
            {"provenance.json"}
            if status == "failed"
            else {"bars.csv", "provenance.json"}
        )
        if entries != expected_entries:
            raise ResumeValidationError(
                f"reusable leaf has the wrong exact file schema for {status!r}: {leaf}"
            )
        expected_scalars = {
            "schema_version": SCHEMA_VERSION,
            "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
            "run_id": self.store.run_id,
            "canonical_symbol": symbol,
            "timeframe": timeframe,
            "source_kind": "broker_native",
            "timezone": "UTC",
        }
        exact_v5_identity = (
            self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
        )
        for key, expected in expected_scalars.items():
            observed = record.get(key)
            if key == "canonical_symbol" and not exact_v5_identity:
                observed = str(observed or "").upper()
            elif key == "timeframe" and not exact_v5_identity:
                observed = str(observed or "").upper()
            if observed != expected:
                raise ResumeValidationError(
                    f"reusable provenance {key} mismatch in {sidecar}: "
                    f"expected {expected!r}, observed {observed!r}"
                )
        if record.get("immutable_record") is not True:
            raise ResumeValidationError(
                f"reusable provenance is not immutable: {sidecar}"
            )
        observed_request = _resume_request_contract(record, sidecar)
        expected_request = (
            self._v5_common_request_contract
            if exact_v5_identity and self._v5_common_request_contract is not None
            else self._common_request_contract()
        )
        if observed_request != expected_request:
            raise ResumeValidationError(f"reusable request mismatch in {sidecar}")
        _validate_timeframe_cell_request(record, timeframe, sidecar)
        if _source_identity_for_request(
            record.get("source_environment"),
            observed_request,
            context=f"reusable artifact {sidecar}",
        ) != _source_identity_for_request(
            self.runtime_metadata,
            expected_request,
            context="connected runtime",
        ):
            raise ResumeValidationError(
                f"reusable source identity mismatch in {sidecar}"
            )

        if status == "failed":
            raise ResumeValidationError(
                "failed acquisition leaves are nonfinal retryable states and cannot "
                f"be reused as completed cells: {sidecar}"
            )
        if status not in {"success", "partial"}:
            raise ResumeValidationError(f"reusable status is invalid: {sidecar}")

        artifact = record.get("artifact")
        if (
            not isinstance(artifact, Mapping)
            or artifact.get("relative_path") != "bars.csv"
        ):
            raise ResumeValidationError(
                f"reusable artifact metadata is invalid: {sidecar}"
            )
        if exact_v5_identity:
            result = (
                dict(manifest_result)
                if manifest_result is not None
                else self._manifest_result(
                    symbol,
                    timeframe,
                    record,
                    disposition="reused",
                )
            )
            csv_receipt = self._validate_v5_publishable_leaf(
                symbol,
                timeframe,
                csv_path,
                record,
                result,
            )
            if receipts is not None:
                receipts.append(csv_receipt)
        else:
            digest = _sha256(csv_path)
            rows = _count_csv_rows(csv_path)
            size = int(csv_path.stat().st_size)
            expected_digest = artifact.get("sha256")
            expected_rows = artifact.get("rows")
            expected_bytes = artifact.get("bytes")
            if (
                digest != expected_digest
                or record.get("sha256") != digest
                or int(expected_rows if expected_rows is not None else -1) != rows
                or int(record.get("rows", -1)) != rows
                or int(expected_bytes if expected_bytes is not None else -1) != size
            ):
                raise ResumeValidationError(
                    "reusable CSV hash/row/byte metadata does not match bytes on "
                    f"disk: {csv_path}"
                )
        return record

    def _validate_v5_publishable_leaf(
        self,
        symbol: str,
        timeframe: str,
        csv_path: Path,
        record: Mapping[str, Any],
        manifest_result: Mapping[str, Any],
    ) -> _PinnedNativeFileReceipt:
        """Recompute a V5 cell from pinned bytes and bind every declared fact."""

        if set(record) != V5_PUBLISHABLE_PROVENANCE_KEYS:
            raise ResumeValidationError(
                f"publishable V5 provenance has an unknown exact schema: {csv_path.parent}"
            )
        pagination = record.get("pagination")
        if not isinstance(pagination, Mapping):
            raise ResumeValidationError(
                f"publishable V5 provenance has no pagination ledger: {csv_path.parent}"
            )
        if self._v5_common_request_contract is None:
            raise ResumeValidationError("V5 common request contract is unavailable")
        common_request = self._v5_common_request_contract
        scan_request = common_request
        if pagination.get("stop_reason") in {
            "terminal_prefix_unresolved",
            "technical_prefix_unresolved",
        }:
            attempts = pagination.get("page_attempts")
            if isinstance(attempts, list) and attempts:
                intervals = dict(common_request["timeframe_source_request_intervals"])
                interval = dict(intervals[timeframe])
                interval["start_utc"] = attempts[0].get("start_utc")
                intervals[timeframe] = interval
                scan_request = {
                    **common_request,
                    "timeframe_source_request_intervals": intervals,
                }
        scanned, receipt = _scan_pinned_native_csv(
            csv_path,
            run_root=self.store.run_root,
            timeframe=timeframe,
            common_request=scan_request,
            pagination=pagination,
        )
        observation = scanned.artifact
        expected_artifact = {
            "relative_path": "bars.csv",
            "sha256": observation.sha256,
            "bytes": observation.bytes,
            "rows": observation.rows,
        }
        artifact = record.get("artifact")
        if not isinstance(artifact, Mapping) or dict(artifact) != expected_artifact:
            raise ResumeValidationError(
                f"publishable V5 artifact declaration differs from bytes: {csv_path}"
            )
        if (
            record.get("sha256") != observation.sha256
            or record.get("rows") != observation.rows
            or manifest_result.get("sha256") != observation.sha256
            or manifest_result.get("rows") != observation.rows
        ):
            raise ResumeValidationError(
                f"publishable V5 result is not bound to the artifact bytes: {csv_path}"
            )
        if record.get("columns") != list(PRODUCTION_ARTIFACT_COLUMNS):
            raise ResumeValidationError(
                f"publishable V5 columns differ from the production schema: {csv_path}"
            )
        if not isinstance(record.get("quality"), Mapping) or dict(
            record["quality"]
        ) != dict(scanned.quality):
            raise ResumeValidationError(
                f"publishable V5 quality facts differ from the artifact bytes: {csv_path}"
            )
        try:
            declared_start = _iso(parse_utc_datetime(str(record.get("start_ts"))))
            declared_end = _iso(parse_utc_datetime(str(record.get("end_ts"))))
        except (TypeError, ValueError, OverflowError, OSError) as exc:
            raise ResumeValidationError(
                f"publishable V5 artifact interval is invalid: {csv_path}"
            ) from exc
        if (
            declared_start != observation.first_open_utc
            or declared_end != observation.last_open_utc
        ):
            raise ResumeValidationError(
                f"publishable V5 artifact interval differs from the bytes: {csv_path}"
            )
        if self._v5_cell_validation_context is None:
            raise ResumeValidationError("V5 cell validation context is unavailable")
        try:
            validate_v5_cell_status(
                record,
                manifest_result,
                common_request,
                symbol=symbol,
                timeframe=timeframe,
                manifest_source_environment=self.runtime_metadata,
                artifact_observation=observation,
                expected_retries=self.request.retries,
                validation_context=self._v5_cell_validation_context,
            )
        except NativeRequestContractError as exc:
            raise ResumeValidationError(
                "publishable V5 cell fails the production reader contract for "
                f"{symbol}/{timeframe}: {';'.join(exc.errors)}"
            ) from exc
        return receipt

    def _validated_reusable_cells(self) -> Dict[Tuple[str, str], Dict[str, Any]]:
        if not self.store.resume:
            return {}
        target = {
            (symbol, timeframe)
            for symbol in self.request.symbols
            for timeframe in self.request.timeframes
        }
        discovered = _discover_native_leaves(
            self.store.run_root,
            symbols=self.request.symbols,
            timeframes=self.request.timeframes,
        )
        extras = sorted(set(discovered) - target)
        if extras:
            raise ResumeValidationError(
                f"existing run contains cells outside this resume target: {extras}"
            )
        # Validate every existing leaf before fetching one new cell, but never
        # retain all provenance documents in memory.  A 600-cell run permits a
        # 4 MiB sidecar per cell; keeping those objects would recreate the OOM
        # class this bounded reader is designed to remove.
        reusable: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for key, leaf in discovered.items():
            record = self._validate_reusable_leaf(key[0], key[1], leaf)
            artifact = record.get("artifact")
            if not isinstance(artifact, Mapping):
                raise ResumeValidationError(
                    f"reusable immutable leaf has no artifact binding: {leaf}"
                )
            self.store.register_artifact_bytes(
                key[0],
                key[1],
                int(artifact.get("bytes", -1)),
            )
            # Reconcile a crash that happened after the leaf's atomic publish
            # but before its nonfinal attempt receipt was removed.
            self.store._clear_attempt_receipts_for_cell(key[0], key[1])
            reusable[key] = {
                "status": record.get("status"),
                "failure_status": record.get("failure_status"),
                "eligible_for_optimization": bool(
                    record.get("eligible_for_optimization", False)
                ),
                "eligible_for_downstream_validation": bool(
                    record.get("eligible_for_downstream_validation", False)
                ),
                "acquisition_started_utc": record.get("acquisition_started_utc"),
                "artifact": {
                    "rows": int(artifact.get("rows", 0)),
                    "sha256": artifact.get("sha256"),
                },
            }
        return reusable

    @staticmethod
    def _extract_terminal_max_bars(metadata: Mapping[str, Any]) -> int:
        terminal = metadata.get("terminal") if isinstance(metadata, Mapping) else None
        if isinstance(terminal, Mapping):
            for key in ("max_bars", "maxbars"):
                try:
                    value = int(terminal.get(key, 0) or 0)
                except (TypeError, ValueError):
                    value = 0
                if value > 0:
                    return value
        return 0

    def _terminal_cache_resource_snapshot(
        self,
        broker_symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Inspect only MT5 HCC metadata and disk capacity; never mutate cache."""

        terminal = (
            self.runtime_metadata.get("terminal")
            if isinstance(self.runtime_metadata, Mapping)
            else None
        )
        raw_data_path = (
            terminal.get("data_path") if isinstance(terminal, Mapping) else None
        )
        if not isinstance(raw_data_path, str) or not raw_data_path.strip():
            return {
                "status": "unavailable",
                "violations": [],
                "reason": "terminal_data_path_not_reported",
            }
        data_path = Path(raw_data_path).expanduser()
        if not data_path.is_absolute() or not data_path.is_dir():
            return {
                "status": "invalid",
                "data_path": str(data_path),
                "violations": ["terminal_data_path_invalid"],
            }

        violations: List[str] = []
        try:
            usage = shutil.disk_usage(data_path)
            disk_total = int(usage.total)
            disk_free = int(usage.free)
        except OSError as exc:
            return {
                "status": "invalid",
                "data_path": str(data_path),
                "violations": ["terminal_disk_usage_unavailable"],
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        if disk_free < TERMINAL_MIN_FREE_BYTES:
            violations.append("terminal_disk_free_reserve_breached")

        account = (
            self.runtime_metadata.get("account")
            if isinstance(self.runtime_metadata, Mapping)
            else None
        )
        raw_server = account.get("server") if isinstance(account, Mapping) else None
        bases_root = data_path / "bases"
        history_root: Optional[Path] = None
        if isinstance(raw_server, str) and raw_server.strip():
            candidate = bases_root / raw_server.strip() / "history"
            try:
                candidate.resolve(strict=False).relative_to(
                    bases_root.resolve(strict=False)
                )
            except ValueError:
                violations.append("terminal_server_path_escape")
            else:
                if candidate.is_dir():
                    history_root = candidate
        if history_root is None and bases_root.is_dir():
            history_root = bases_root

        scan_root = history_root
        scan_depth = 3 if history_root == bases_root else 1
        scan_scope = "server" if history_root is not None else "none"
        if scan_root is not None and broker_symbol:
            candidate = history_root / str(broker_symbol)
            try:
                candidate.resolve(strict=False).relative_to(
                    history_root.resolve(strict=False)
                )
            except ValueError:
                violations.append("terminal_symbol_path_escape")
            else:
                if candidate.is_dir():
                    scan_root = candidate
                    scan_depth = 0
                    scan_scope = "symbol"

        hcc_scan = {
            "hcc_files_inspected": 0,
            "largest_hcc_bytes": 0,
            "largest_hcc_relative_path": None,
            "entries_inspected": 0,
            "scan_error": None,
            "violations": [],
        }
        if scan_root is not None:
            hcc_scan = _bounded_terminal_hcc_scan(
                scan_root,
                relative_root=data_path,
                maximum_directory_depth=scan_depth,
            )
            violations.extend(hcc_scan["violations"])
        hcc_count = int(hcc_scan["hcc_files_inspected"])
        largest_bytes = int(hcc_scan["largest_hcc_bytes"])
        if largest_bytes > TERMINAL_HCC_MAX_FILE_BYTES:
            violations.append("terminal_hcc_file_bound_breached")
        return {
            "status": "limit_exceeded" if violations else "within_bounds",
            "data_path": str(data_path),
            "disk_total_bytes": disk_total,
            "disk_free_bytes": disk_free,
            "minimum_free_bytes": TERMINAL_MIN_FREE_BYTES,
            "hcc_files_inspected": hcc_count,
            "largest_hcc_bytes": largest_bytes,
            "largest_hcc_relative_path": hcc_scan["largest_hcc_relative_path"],
            "hcc_entries_inspected": hcc_scan["entries_inspected"],
            "maximum_hcc_file_bytes": TERMINAL_HCC_MAX_FILE_BYTES,
            "scan_scope": scan_scope,
            "scan_error": hcc_scan["scan_error"],
            "violations": violations,
        }

    def _enforce_terminal_cache_resource_bounds(
        self,
        broker_symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        snapshot = self._terminal_cache_resource_snapshot(broker_symbol)
        if snapshot.get("status") != "within_bounds" or snapshot.get("violations"):
            raise TerminalCacheResourceLimitError(snapshot)
        return snapshot

    def _page_span(self, timeframe: str) -> timedelta:
        seconds = TIMEFRAME_SECONDS[timeframe]
        configured_slots = max(1, int(DEFAULT_PAGE_DAYS[timeframe] * 86400 // seconds))
        if self.terminal_max_bars > 0:
            safe_slots = max(1, int(self.terminal_max_bars * 0.90))
            configured_slots = min(configured_slots, safe_slots)
        return timedelta(seconds=configured_slots * seconds)

    def _read_instrument_metadata(self, symbol: str) -> Dict[str, Any]:
        reader = getattr(self.connector, "get_symbol_info", None)
        if not callable(reader):
            return {"status": "unavailable"}
        try:
            info = reader(symbol)
        except Exception as exc:
            return {
                "status": "unavailable",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        if info is None:
            return {"status": "unavailable"}
        value = {
            attr: _json_safe(getattr(info, attr))
            for attr in (
                "symbol",
                "digits",
                "point",
                "trade_tick_size",
                "trade_tick_value",
                "trade_contract_size",
                "path",
                "asset_class",
            )
            if hasattr(info, attr)
        }
        value["status"] = "available"
        tick_candidates: List[float] = []
        for key in ("trade_tick_size", "point"):
            try:
                candidate = float(value.get(key, 0.0) or 0.0)
            except (TypeError, ValueError, OverflowError):
                candidate = 0.0
            if math.isfinite(candidate) and candidate > 0.0:
                tick_candidates.append(candidate)
        value["parity_tick_tolerance"] = (
            max(tick_candidates) if tick_candidates else None
        )
        return value

    @staticmethod
    def _last_error(connector: Any) -> Optional[Tuple[Any, Any]]:
        reader = getattr(connector, "get_last_error", None)
        if not callable(reader):
            return None
        try:
            value = reader()
        except Exception:
            return None
        if isinstance(value, (tuple, list)) and value:
            return (value[0], value[1] if len(value) > 1 else None)
        return None

    @staticmethod
    def _is_hard_last_error(value: Optional[Tuple[Any, Any]]) -> bool:
        if value is None:
            return False
        try:
            code = int(value[0])
        except (TypeError, ValueError):
            return False
        return code not in _SUCCESS_ERROR_CODES

    def _fetch_page(
        self,
        symbol: str,
        timeframe: str,
        start_utc: datetime,
        end_utc: datetime,
        *,
        broker_symbol: Optional[str] = None,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """Fetch one page while leaving all MT5 synchronization to the connector."""

        errors: List[Dict[str, Any]] = []
        diagnostics: Mapping[str, Any] = {}
        try:
            frame = self.connector.get_bars_range(
                symbol,
                timeframe,
                start_utc,
                end_utc,
                sync_retry=True,
            )
        except Exception as exc:
            bounded = _bounded_error_diagnostic(exc)
            errors.append(
                {
                    "attempt": 1,
                    **bounded,
                }
            )
            last_error = self._last_error(self.connector)
            if self._is_hard_last_error(last_error):
                errors.append(
                    {
                        "attempt": 1,
                        "type": "MT5LastError",
                        "message": "MT5 connector reported a hard last-error code",
                    }
                )
            return None, {
                "outcome": "unexpected_error",
                "attempts": 1,
                "copy_rates_range_attempts": 0,
                "connector_diagnostics": {},
                "errors": errors,
            }

        diagnostic_candidate: Optional[Mapping[str, Any]] = None
        if isinstance(frame, pd.DataFrame):
            frame_attrs = getattr(frame, "attrs", None)
            if isinstance(frame_attrs, Mapping):
                candidate_diagnostics = frame_attrs.get("mt5_history_range_diagnostics")
                if isinstance(candidate_diagnostics, Mapping):
                    diagnostic_candidate = candidate_diagnostics
            # Pandas deep-copies attrs in DataFrame.copy().  The only approved
            # connector receipt is detached below, so no arbitrary/unbounded
            # attr may cross into page normalization.
            frame.attrs = {}
        if diagnostic_candidate is None:
            reader = getattr(
                self.connector,
                "get_last_history_range_diagnostics",
                None,
            )
            if callable(reader):
                try:
                    candidate_diagnostics = reader()
                except Exception:
                    candidate_diagnostics = None
                if isinstance(candidate_diagnostics, Mapping):
                    diagnostic_candidate = candidate_diagnostics
        try:
            diagnostic_bytes = canonical_json_bytes(
                diagnostic_candidate if diagnostic_candidate is not None else {}
            )
            if len(diagnostic_bytes) > MAX_CONNECTOR_DIAGNOSTIC_BYTES:
                raise NativeRequestContractError(
                    ("MT5_CONNECTOR_DIAGNOSTIC_BYTE_CAP_EXCEEDED",)
                )
            detached_diagnostics = json.loads(diagnostic_bytes)
            if not isinstance(detached_diagnostics, dict):
                raise NativeRequestContractError(
                    ("MT5_CONNECTOR_DIAGNOSTIC_OBJECT_REQUIRED",)
                )
            diagnostics = detached_diagnostics
        except (
            NativeRequestContractError,
            MemoryError,
            OverflowError,
            RecursionError,
            TypeError,
            ValueError,
        ) as exc:
            detail = (
                ";".join(exc.errors)
                if isinstance(exc, NativeRequestContractError)
                else type(exc).__name__
            )
            return None, {
                "outcome": "contract_error",
                "attempts": 1,
                "copy_rates_range_attempts": 0,
                "connector_diagnostics": {},
                "errors": [
                    {
                        "attempt": 1,
                        "type": "MT5RangeContractFailure",
                        "message": detail[:4096],
                    }
                ],
            }
        serialized_diagnostics = diagnostics
        expected_broker_symbol = broker_symbol
        if expected_broker_symbol is None:
            finder = getattr(self.connector, "find_broker_symbol", None)
            if callable(finder):
                try:
                    expected_broker_symbol = finder(symbol)
                except Exception:
                    expected_broker_symbol = None
        if not isinstance(expected_broker_symbol, str) or not expected_broker_symbol:
            expected_broker_symbol = str(diagnostics.get("broker_symbol") or "")
        try:
            # Zero is an authoritative pre-copy connector failure (for example,
            # connection_unavailable).  Never coerce it into a successful
            # one-attempt empty observation.
            connector_attempts = int(diagnostics.get("copy_rates_range_attempts", 0))
        except (TypeError, ValueError, OverflowError):
            connector_attempts = 0
        if connector_attempts == 0:
            try:
                validate_mt5_v4_preflight_diagnostics(
                    diagnostics,
                    start_utc=start_utc,
                    end_exclusive_utc=end_utc,
                    symbol=symbol,
                    timeframe=timeframe,
                )
            except NativeRequestContractError as exc:
                errors.append(
                    {
                        "attempt": 1,
                        "type": "MT5RangeContractFailure",
                        "message": ";".join(exc.errors),
                    }
                )
                return None, {
                    "outcome": "contract_error",
                    "attempts": 1,
                    "copy_rates_range_attempts": 0,
                    "connector_diagnostics": serialized_diagnostics,
                    "errors": errors,
                }
            sync_status = str(diagnostics.get("sync_status") or "")
            return None, {
                "outcome": "connector_preflight_failed",
                "attempts": 1,
                "copy_rates_range_attempts": 0,
                "connector_diagnostics": serialized_diagnostics,
                "errors": [
                    {
                        "attempt": 1,
                        "type": "MT5ConnectorPreflightFailure",
                        "message": f"MT5 connector preflight failed: {sync_status}",
                    }
                ],
            }
        if connector_attempts not in {1, 2}:
            errors.append(
                {
                    "attempt": 1,
                    "type": "MT5RangeContractFailure",
                    "message": "connector copy_rates_range attempt count is outside [1, 2]",
                }
            )
            return None, {
                "outcome": "contract_error",
                "attempts": 1,
                "copy_rates_range_attempts": max(0, connector_attempts),
                "connector_diagnostics": serialized_diagnostics,
                "errors": errors,
            }

        def _validate_connector_receipt(category: str) -> bool:
            try:
                validate_mt5_v4_page_diagnostics(
                    diagnostics,
                    start_utc=start_utc,
                    end_exclusive_utc=end_utc,
                    symbol=symbol,
                    broker_symbol=expected_broker_symbol,
                    timeframe=timeframe,
                    copy_attempts=connector_attempts,
                    category=category,
                )
            except NativeRequestContractError as exc:
                errors.append(
                    {
                        "attempt": 1,
                        "type": "MT5RangeContractFailure",
                        "message": ";".join(exc.errors),
                    }
                )
                return False
            return True

        hard_failure = bool(diagnostics.get("hard_failure"))
        if hard_failure:
            receipt_valid = _validate_connector_receipt("failed")
            errors.append(
                {
                    "attempt": 1,
                    "type": "MT5HardRangeFailure",
                    "message": str(
                        diagnostics.get("hard_failure_call")
                        or diagnostics.get("sync_status")
                        or "connector-declared hard failure"
                    ),
                    "last_error": _json_safe(diagnostics.get("last_error")),
                }
            )
            return None, {
                "outcome": "error" if receipt_valid else "contract_error",
                "attempts": 1,
                "copy_rates_range_attempts": connector_attempts,
                "connector_diagnostics": serialized_diagnostics,
                "errors": errors,
            }
        if frame is not None and len(frame) > 0:
            receipt_valid = _validate_connector_receipt("data")
            admitted_count = diagnostics.get("admitted_count")
            frame_index = getattr(frame, "index", None)
            frame_binding_valid = bool(
                type(admitted_count) is int
                and admitted_count == len(frame)
                and isinstance(frame_index, pd.DatetimeIndex)
                and len(frame_index) == len(frame)
            )
            if frame_binding_valid:
                admitted_extent = diagnostics.get("admitted_extent")
                frame_binding_valid = bool(
                    isinstance(admitted_extent, Mapping)
                    and admitted_extent.get("first_epoch_second")
                    == int(frame_index.min().timestamp())
                    and admitted_extent.get("last_epoch_second")
                    == int(frame_index.max().timestamp())
                )
            if not frame_binding_valid:
                errors.append(
                    {
                        "attempt": 1,
                        "type": "MT5RangeContractFailure",
                        "message": "connector admitted extent does not bind returned frame",
                    }
                )
            if not receipt_valid or not frame_binding_valid:
                return None, {
                    "outcome": "contract_error",
                    "attempts": 1,
                    "copy_rates_range_attempts": connector_attempts,
                    "connector_diagnostics": serialized_diagnostics,
                    "errors": errors,
                }
            return frame, {
                "outcome": "data",
                "attempts": 1,
                "copy_rates_range_attempts": connector_attempts,
                "connector_diagnostics": serialized_diagnostics,
                "errors": errors,
            }

        sync_status = str(diagnostics.get("sync_status") or "")
        raw_relation = str(diagnostics.get("raw_relation") or "")
        if sync_status == "terminal_prefix_unsynchronized":
            receipt_valid = _validate_connector_receipt("terminal_prefix")
            ledger = diagnostics.get("call_ledger")
            initial_relation = diagnostics.get("initial_raw_relation")
            expected_calls = (
                "copy_rates_range_initial",
                "copy_rates_from_warm",
                "copy_rates_range_retry",
            )
            ledger_valid = (
                isinstance(ledger, list)
                and len(ledger) == 3
                and all(
                    isinstance(entry, Mapping)
                    and entry.get("sequence") == sequence
                    and entry.get("call_type") == call_type
                    and entry.get("hard_failure") is False
                    and entry.get("exception") is None
                    and entry.get("parse_error") is None
                    for sequence, (entry, call_type) in enumerate(
                        zip(ledger, expected_calls), start=1
                    )
                )
                and initial_relation in {None, "RIGHT_DISJOINT_PREFIX"}
                and ledger[0].get("raw_relation") == initial_relation
                and ledger[-1].get("raw_relation") == "RIGHT_DISJOINT_PREFIX"
                and ledger[-1].get("admitted_count") == 0
                and ledger[-1].get("admitted_extent") is None
            )
            confirmed_prefix = (
                diagnostics.get("range_contract_version") == "mt5-half-open-range-v4"
                and diagnostics.get("call_ledger_schema") == "mt5.range-call-ledger.v1"
                and diagnostics.get("sync_retry") is True
                and connector_attempts == 2
                and diagnostics.get("copy_rates_range_max_attempts") == 2
                and diagnostics.get("history_download_trigger_attempts") == 1
                and raw_relation == "RIGHT_DISJOINT_PREFIX"
                and diagnostics.get("confirmation_raw_relation")
                == "RIGHT_DISJOINT_PREFIX"
                and diagnostics.get("admitted_count") == 0
                and diagnostics.get("admitted_extent") is None
                and diagnostics.get("terminal_confirmation_consumed")
                is (initial_relation == "RIGHT_DISJOINT_PREFIX")
                and diagnostics.get("hard_failure") is False
                and diagnostics.get("hard_failure_call") is None
                and ledger_valid
            )
            if confirmed_prefix and receipt_valid:
                return None, {
                    "outcome": "terminal_prefix_unresolved",
                    "attempts": 1,
                    "copy_rates_range_attempts": connector_attempts,
                    "connector_diagnostics": serialized_diagnostics,
                    "errors": errors,
                }
            errors.append(
                {
                    "attempt": 1,
                    "type": "MT5RangeContractFailure",
                    "message": "terminal-prefix state lacks the bounded V4 confirmation evidence",
                }
            )
            return None, {
                "outcome": "contract_error",
                "attempts": 1,
                "copy_rates_range_attempts": connector_attempts,
                "connector_diagnostics": serialized_diagnostics,
                "errors": errors,
            }
        if sync_status in {
            "terminal_suffix_unsynchronized",
            "mixed_invalid_no_admissible_data",
            "invalid_filtered_response",
        }:
            errors.append(
                {
                    "attempt": 1,
                    "type": "MT5RangeContractFailure",
                    "message": sync_status,
                }
            )
            return None, {
                "outcome": "contract_error",
                "attempts": 1,
                "copy_rates_range_attempts": connector_attempts,
                "connector_diagnostics": serialized_diagnostics,
                "errors": errors,
            }
        if sync_status not in {"", "empty", "empty_unsynchronized"}:
            errors.append(
                {
                    "attempt": 1,
                    "type": "MT5RangeContractFailure",
                    "message": (
                        "non-data connector state cannot be admitted as an "
                        f"empty history page: {sync_status}"
                    ),
                }
            )
            return None, {
                "outcome": "contract_error",
                "attempts": 1,
                "copy_rates_range_attempts": connector_attempts,
                "connector_diagnostics": serialized_diagnostics,
                "errors": errors,
            }
        if not _validate_connector_receipt("empty"):
            return None, {
                "outcome": "contract_error",
                "attempts": 1,
                "copy_rates_range_attempts": connector_attempts,
                "connector_diagnostics": serialized_diagnostics,
                "errors": errors,
            }
        return None, {
            "outcome": "empty",
            "attempts": 1,
            "copy_rates_range_attempts": connector_attempts,
            "connector_diagnostics": serialized_diagnostics,
            "errors": errors,
        }

    @staticmethod
    def _normalize_page(
        frame: pd.DataFrame,
        page_start: datetime,
        page_end: datetime,
    ) -> Tuple[pd.DataFrame, int]:
        local = frame.copy()
        raw_rows = len(local)
        if not isinstance(local.index, pd.DatetimeIndex):
            if "time" not in local.columns:
                raise ValueError(
                    "history frame has neither a DatetimeIndex nor a time column"
                )
            local.index = pd.to_datetime(local.pop("time"), utc=True, errors="coerce")
        else:
            local.index = pd.to_datetime(local.index, utc=True, errors="coerce")
        local = local[~local.index.isna()]
        start_ts = pd.Timestamp(page_start)
        end_ts = pd.Timestamp(page_end)
        # copy_rates_range can include both endpoints.  The acquisition contract
        # is half-open so a boundary bar belongs to exactly one page.
        local = local[(local.index >= start_ts) & (local.index < end_ts)]
        return local, raw_rows

    def _apply_source_quality_omission_policy(
        self,
        frame: pd.DataFrame,
        *,
        symbol: str,
        broker_symbol: str,
        timeframe: str,
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Omit only the one independently confirmed nonpositive FBS row."""

        if symbol != "FR40" or broker_symbol != "FR40" or frame.empty:
            return frame, []
        target = pd.Timestamp("2019-11-25T00:00:00Z")
        positions = np.flatnonzero(frame.index == target)
        if not len(positions):
            return frame, []
        receipts: List[Dict[str, Any]] = []
        removed_positions: List[int] = []
        for raw_position in positions:
            position = int(raw_position)
            row = frame.iloc[position]
            try:
                observed_row: Dict[str, float | int] = {
                    field: float(row[field])
                    for field in ("Open", "High", "Low", "Close")
                }
                for field in ("Volume", "Spread", "RealVolume"):
                    numeric = float(row[field])
                    if not math.isfinite(numeric) or not numeric.is_integer():
                        raise ValueError("nonintegral discrete source value")
                    observed_row[field] = int(numeric)
                receipt = build_v5_source_quality_omission(
                    source_environment=self.runtime_metadata,
                    symbol=symbol,
                    broker_symbol=broker_symbol,
                    timeframe=timeframe,
                    bar_open_utc=target.to_pydatetime(),
                    observed_row=observed_row,
                )
            except (NativeRequestContractError, KeyError, TypeError, ValueError):
                # A different row is not covered by this narrow policy. Leave
                # it in the artifact so the ordinary quality scanner can fail
                # closed instead of broadening the omission at runtime.
                continue
            receipts.append(receipt)
            removed_positions.append(position)
        if len(receipts) > 1:
            raise NativeCsvValidationError(
                "SOURCE_QUALITY_OMISSION_MULTIPLICITY_INVALID"
            )
        if not receipts:
            return frame, []
        keep = np.ones(len(frame), dtype=bool)
        keep[removed_positions[0]] = False
        return frame.iloc[keep].copy(), receipts

    def _leaf_request_record(
        self,
        timeframe: str,
        *,
        last_fully_closed_bar_open_utc: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        native_start, fetch_end_exclusive = self.timeframe_source_request_intervals[
            timeframe
        ]
        strategy_start, _strategy_end = self.timeframe_strategy_evidence_intervals[
            timeframe
        ]
        cell_partition_windows = self.timeframe_partition_windows[timeframe]
        request_record: Dict[str, Any] = {
            "horizon_years": int(self.request.horizon_years),
            "requested_information_cutoff_utc": _iso(
                self.requested_information_cutoff_utc
            ),
            "requested_horizon_start_utc": _iso(self.requested_horizon_start_utc),
            "common_requested_start_utc": _iso(self.common_start_utc),
            "required_evidence_lead_start_utc": _iso(
                self.required_evidence_lead_start_utc
            ),
            "warmup_days": float(self.request.warmup_days),
            "safety_days": float(self.request.safety_days),
            "closed_bar_slack_seconds": max(
                TIMEFRAME_SECONDS[tf] for tf in self.request.timeframes
            ),
            "common_as_of_utc": _iso(self.as_of_utc),
            "native_fetch_start_utc": _iso(native_start),
            "native_fetch_end_exclusive_utc": _iso(fetch_end_exclusive),
            "last_fully_closed_bar_open_utc": _iso(last_fully_closed_bar_open_utc),
            "alignment_mode": (
                "broker_observed_successor_or_nominal_close_v1"
                if timeframe in {"H4", "D1"}
                else "fixed_utc_native_boundary_v1"
            ),
            "successor_sentinel_fetch_slack_seconds": (
                1 if timeframe in {"H4", "D1"} else 0
            ),
            "boundary_tolerance_days": float(self.request.boundary_tolerance_days),
            "window_years": (
                dict((self.request.timeframe_window_years or {})[timeframe])
                if self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                else dict(self.request.window_years or {})
            ),
            "embargo_days": float(self.request.embargo_days),
            "common_calendar_boundaries": {
                partition: {"start_ts": _iso(start), "end_ts": _iso(end)}
                for partition, (start, end) in cell_partition_windows.items()
            },
        }
        if self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            request_record.update(self._timeframe_request_fields())
            request_record.update(
                {
                    "cell_timeframe": timeframe,
                    "cell_timeframe_window_years": dict(
                        (self.request.timeframe_window_years or {})[timeframe]
                    ),
                    "cell_calendar_boundaries": {
                        partition: {
                            "start_ts": _iso(start),
                            "end_ts": _iso(end),
                        }
                        for partition, (start, end) in cell_partition_windows.items()
                    },
                    "cell_required_evidence_lead_start_utc": _iso(
                        self.timeframe_required_evidence_lead_start_utc[timeframe]
                    ),
                    "source_request_interval": {
                        "start_utc": _iso(native_start),
                        "end_exclusive_utc": _iso(fetch_end_exclusive),
                    },
                    "strategy_evidence_interval": (
                        self._serialized_strategy_evidence_intervals()[timeframe]
                    ),
                    "audit_reference_timeframes": list(
                        self.timeframe_audit_reference_map.get(timeframe, [])
                    ),
                    "audit_consumer_targets": list(
                        self.timeframe_audit_consumer_map.get(timeframe, [])
                    ),
                    "audit_windows": self._serialized_audit_windows()[timeframe],
                    "audit_reference_intervals": (
                        self._serialized_audit_reference_intervals()[timeframe]
                    ),
                    "audit_only_intervals": (
                        self._serialized_audit_only_intervals()[timeframe]
                    ),
                }
            )
        return request_record

    def _fetch_timeframe(
        self,
        symbol: str,
        broker_symbol: Optional[str],
        timeframe: str,
        started_utc: datetime,
        instrument_metadata: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        seconds = TIMEFRAME_SECONDS[timeframe]
        broker_session_aligned = timeframe in {"H4", "D1"}
        cell_partition_windows = self.timeframe_partition_windows[timeframe]
        native_start, fetch_end_exclusive = self.timeframe_source_request_intervals[
            timeframe
        ]
        strategy_start, _strategy_end = self.timeframe_strategy_evidence_intervals[
            timeframe
        ]
        # Broker H4/D1 opens can shift with broker DST. Their source interval
        # includes a one-second sentinel beyond the common cutoff so an exact-
        # cutoff successor can prove the preceding bar closed; the successor is
        # removed by the close<=as_of filter below.
        if fetch_end_exclusive <= native_start:
            raise ValueError("calendar horizon contains no fully closed native bar")
        page_span = self._page_span(timeframe)
        cursor_end = fetch_end_exclusive
        assembly: Optional[_NativePageSpillAssembly] = None
        raw_rows = 0
        page_count = 0
        pages_with_data = 0
        attempts_total = 0
        copy_rates_range_attempts_total = 0
        page_attempts: List[Dict[str, Any]] = []
        empty_pages: List[Dict[str, Any]] = []
        failed_pages: List[Dict[str, Any]] = []
        truncated_pages: List[Dict[str, Any]] = []
        terminal_prefix_pages: List[Dict[str, Any]] = []
        pagination_stop_reason: Optional[str] = None
        unrequested_prefix: Optional[Dict[str, Any]] = None
        strategy_suffix_covered = False
        boundary_tolerance = timedelta(days=float(self.request.boundary_tolerance_days))
        planned_page_count = int(
            math.ceil(
                (fetch_end_exclusive - native_start).total_seconds()
                / page_span.total_seconds()
            )
        )
        if planned_page_count > MAX_PAGES_PER_CELL:
            raise TerminalCacheResourceLimitError(
                {
                    "status": "limit_exceeded",
                    "violations": ["page_ledger_cardinality_cap_exceeded"],
                    "pages_planned": planned_page_count,
                    "maximum_pages_per_cell": MAX_PAGES_PER_CELL,
                    "symbol": symbol,
                    "broker_symbol": broker_symbol,
                    "timeframe": timeframe,
                }
            )
        initial_resource_snapshot = self._enforce_terminal_cache_resource_bounds(
            broker_symbol or symbol
        )

        def _abort_assembly() -> None:
            if assembly is not None:
                assembly.cleanup()

        def _can_publish_newer_suffix(page_end: datetime) -> bool:
            return bool(
                self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                and assembly is not None
                and page_end <= strategy_start
                and len(failed_pages) + len(empty_pages) + len(terminal_prefix_pages)
                == 1
                and not truncated_pages
            )

        def _stop_at_older_prefix(reason: str, page_start: datetime) -> None:
            nonlocal pagination_stop_reason, unrequested_prefix
            pagination_stop_reason = reason
            if page_start > native_start:
                unrequested_prefix = {
                    "start_utc": _iso(native_start),
                    "end_exclusive_utc": _iso(page_start),
                    "reason": reason,
                }

        def _request_failure_record(
            failure_status: str,
            error: Mapping[str, Any],
            **extra: Any,
        ) -> Dict[str, Any]:
            return {
                "schema_version": SCHEMA_VERSION,
                "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
                "immutable_record": True,
                "run_id": self.store.run_id,
                "status": "failed",
                "failure_status": failure_status,
                "eligible_for_optimization": False,
                "eligible_for_downstream_validation": False,
                "statistical_sufficiency_assessed": False,
                "canonical_symbol": symbol,
                "symbol": symbol,
                "broker_symbol": broker_symbol,
                "timeframe": timeframe,
                "source_kind": "broker_native",
                "timezone": "UTC",
                "timeframe_seconds": seconds,
                "request": self._leaf_request_record(timeframe),
                "source_environment": _json_safe(self.runtime_metadata),
                "instrument": _json_safe(dict(instrument_metadata or {})),
                "acquisition_started_utc": _iso(started_utc),
                "acquisition_completed_utc": _iso(_utc(self.now_fn())),
                "error": _bounded_error_diagnostic(
                    error_type=str(error.get("type") or "Error"),
                    message=str(error.get("message") or ""),
                ),
                **extra,
            }

        while cursor_end > native_start:
            cursor = max(native_start, cursor_end - page_span)
            page_end = cursor_end
            try:
                frame, outcome = self._fetch_page(
                    symbol,
                    timeframe,
                    cursor,
                    page_end,
                    broker_symbol=broker_symbol,
                )
                outcome["terminal_cache_resource_snapshot"] = (
                    self._enforce_terminal_cache_resource_bounds(
                        broker_symbol or symbol
                    )
                )
            except Exception:
                _abort_assembly()
                raise
            page_count += 1
            attempts_total += int(outcome["attempts"])
            copy_rates_range_attempts_total += int(
                outcome.get("copy_rates_range_attempts", outcome["attempts"])
            )
            page_identity = {
                "start_utc": _iso(cursor),
                "end_exclusive_utc": _iso(page_end),
                "attempts": int(outcome["attempts"]),
                "copy_rates_range_attempts": int(
                    outcome.get("copy_rates_range_attempts", outcome["attempts"])
                ),
                "request_sequence": page_count,
            }
            connector_diagnostics = outcome.get("connector_diagnostics")
            if isinstance(connector_diagnostics, Mapping) and connector_diagnostics:
                page_identity["connector_diagnostics"] = dict(connector_diagnostics)
            resource_snapshot = outcome.get("terminal_cache_resource_snapshot")
            if isinstance(resource_snapshot, Mapping):
                page_identity["terminal_cache_resource_snapshot"] = _json_safe(
                    dict(resource_snapshot)
                )
            if outcome["outcome"] == "connector_preflight_failed":
                sync_status = str(
                    (outcome.get("connector_diagnostics") or {}).get("sync_status")
                    or ""
                )
                _abort_assembly()
                return None, _request_failure_record(
                    "connector_preflight_failed",
                    {
                        "type": "MT5ConnectorPreflightFailure",
                        "message": f"MT5 connector preflight failed: {sync_status}",
                    },
                    connector_preflight_failure=page_identity,
                )
            if outcome["outcome"] in {"contract_error", "unexpected_error"}:
                diagnostics = outcome.get("errors")
                messages = [
                    str(item.get("message") or "")
                    for item in diagnostics or []
                    if isinstance(item, Mapping)
                ]
                _abort_assembly()
                return None, _request_failure_record(
                    "unexpected_acquisition_failure",
                    {
                        "type": (
                            "MT5RangeContractFailure"
                            if outcome["outcome"] == "contract_error"
                            else "MT5ConnectorInvocationFailure"
                        ),
                        "message": ";".join(messages)
                        or "MT5 connector invocation failed without diagnostics",
                    },
                )
            page_attempts.append(page_identity)
            if outcome["outcome"] == "data" and frame is not None:
                incoming_rows = int(len(frame))
                incoming_bytes = int(frame.memory_usage(index=True, deep=True).sum())
                if (
                    incoming_rows > MAX_NATIVE_PAGE_IN_PROCESS_ROWS
                    or incoming_bytes > MAX_NATIVE_PAGE_IN_PROCESS_BYTES
                ):
                    _abort_assembly()
                    raise NativePageAssemblyResourceLimitError(
                        {
                            "status": "limit_exceeded",
                            "violations": [
                                *(
                                    ["assembly_page_in_process_row_cap_exceeded"]
                                    if incoming_rows > MAX_NATIVE_PAGE_IN_PROCESS_ROWS
                                    else []
                                ),
                                *(
                                    ["assembly_page_in_process_byte_cap_exceeded"]
                                    if incoming_bytes > MAX_NATIVE_PAGE_IN_PROCESS_BYTES
                                    else []
                                ),
                            ],
                            "page_rows": incoming_rows,
                            "page_memory_bytes": incoming_bytes,
                            "maximum_page_rows": MAX_NATIVE_PAGE_IN_PROCESS_ROWS,
                            "maximum_page_memory_bytes": (
                                MAX_NATIVE_PAGE_IN_PROCESS_BYTES
                            ),
                        }
                    )
                try:
                    normalized, page_raw_rows = self._normalize_page(
                        frame, cursor, page_end
                    )
                    normalized, source_quality_omissions = (
                        self._apply_source_quality_omission_policy(
                            normalized,
                            symbol=symbol,
                            broker_symbol=str(broker_symbol or ""),
                            timeframe=timeframe,
                        )
                    )
                except Exception:
                    _abort_assembly()
                    raise
                raw_rows += page_raw_rows
                if source_quality_omissions:
                    page_identity["source_quality_omissions"] = source_quality_omissions
                if not normalized.empty:
                    pages_with_data += 1
                    if assembly is None:
                        assembly = _NativePageSpillAssembly(
                            self.store.run_root, symbol, timeframe
                        )
                    try:
                        assembly.add_page(normalized, cursor, page_end)
                    except Exception:
                        _abort_assembly()
                        raise
                    page_first = normalized.index.min().to_pydatetime()
                    page_last = normalized.index.max().to_pydatetime()
                    expected_last = page_end - timedelta(seconds=seconds)
                    page_start_delay = max(timedelta(0), page_first - cursor)
                    page_end_lag = max(timedelta(0), expected_last - page_last)
                    if (
                        page_start_delay > boundary_tolerance
                        or page_end_lag > boundary_tolerance
                    ):
                        truncated_pages.append(
                            {
                                **page_identity,
                                "actual_first_open_utc": _iso(page_first),
                                "actual_last_open_utc": _iso(page_last),
                                "start_delay_seconds": float(
                                    page_start_delay.total_seconds()
                                ),
                                "end_lag_seconds": float(page_end_lag.total_seconds()),
                            }
                        )
                else:
                    empty_pages.append(page_identity)
                del normalized
                del frame
            elif outcome["outcome"] == "error":
                page_identity["errors"] = outcome.get("errors", [])
                failed_pages.append(page_identity)
                if _can_publish_newer_suffix(page_end):
                    strategy_suffix_covered = True
                    _stop_at_older_prefix("technical_prefix_unresolved", cursor)
                    break
            elif outcome["outcome"] == "terminal_prefix_unresolved":
                terminal_prefix_pages.append(page_identity)
                strategy_suffix_covered = _can_publish_newer_suffix(page_end)
                _stop_at_older_prefix("terminal_prefix_unresolved", cursor)
                break
            else:
                empty_pages.append(page_identity)
                if _can_publish_newer_suffix(page_end):
                    strategy_suffix_covered = True
                    _stop_at_older_prefix("technical_prefix_unresolved", cursor)
                    break
            cursor_end = cursor

        def _chronological_page_order(item: Mapping[str, Any]) -> str:
            return str(item.get("start_utc") or "")

        for ledger in (
            page_attempts,
            empty_pages,
            failed_pages,
            truncated_pages,
            terminal_prefix_pages,
        ):
            ledger.sort(key=_chronological_page_order)

        request_record = self._leaf_request_record(timeframe)

        base_record: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
            "immutable_record": True,
            "run_id": self.store.run_id,
            "status": "failed",
            "failure_status": None,
            "eligible_for_optimization": False,
            "eligible_for_downstream_validation": False,
            "statistical_sufficiency_assessed": False,
            "canonical_symbol": symbol,
            "symbol": symbol,
            "broker_symbol": broker_symbol,
            "timeframe": timeframe,
            "source_kind": "broker_native",
            "timezone": "UTC",
            "timeframe_seconds": seconds,
            "request": request_record,
            "source_environment": _json_safe(self.runtime_metadata),
            "instrument": _json_safe(dict(instrument_metadata or {})),
            "acquisition_started_utc": _iso(started_utc),
            "acquisition_completed_utc": _iso(_utc(self.now_fn())),
            "pagination": {
                "contract_version": PAGINATION_CONTRACT_VERSION,
                "request_order": "newest_to_oldest",
                "page_span_seconds": int(page_span.total_seconds()),
                "pages_planned": planned_page_count,
                "pages_requested": page_count,
                "pages_with_data": pages_with_data,
                "empty_pages": empty_pages,
                "failed_pages": failed_pages,
                "truncated_pages": truncated_pages,
                "terminal_prefix_unresolved_pages": terminal_prefix_pages,
                "stop_reason": pagination_stop_reason,
                "unrequested_prefix": unrequested_prefix,
                "initial_terminal_cache_resource_snapshot": (initial_resource_snapshot),
                "attempts_total": attempts_total,
                "copy_rates_range_attempts_total": (copy_rates_range_attempts_total),
                "page_attempts": page_attempts,
                "retries_per_page": int(self.request.retries),
            },
        }

        if failed_pages and not strategy_suffix_covered:
            base_record.update(
                {
                    "failure_status": "fetch_failed",
                    "error": {
                        "type": "PageFetchError",
                        "message": f"{len(failed_pages)} page(s) exhausted retry budget",
                    },
                    "rows": {"raw_received": raw_rows, "published": 0},
                }
            )
            _abort_assembly()
            return None, base_record
        if terminal_prefix_pages and assembly is None:
            base_record.update(
                {
                    "failure_status": "terminal_prefix_unresolved",
                    "error": {
                        "type": "TerminalPrefixUnresolved",
                        "message": (
                            "MT5 returned only bars newer than the requested page "
                            "after one bounded confirmation; broker absence was not inferred"
                        ),
                    },
                    "rows": {"raw_received": raw_rows, "published": 0},
                }
            )
            _abort_assembly()
            return None, base_record
        if assembly is None:
            base_record.update(
                {
                    "failure_status": "terminal_history_empty_unresolved",
                    "error": {
                        "type": "TerminalHistoryEmptyUnresolved",
                        "message": (
                            "MT5 returned no admitted bars in the requested calendar horizon; "
                            "this is terminal/source availability evidence, not proof of broker absence"
                        ),
                    },
                    "rows": {"raw_received": raw_rows, "published": 0},
                }
            )
            return None, base_record
        pre_dedup_rows = int(assembly.pre_dedup_rows)
        duplicate_rows = int(assembly.duplicate_rows)
        scanner_start = (
            parse_utc_datetime(page_attempts[0]["start_utc"])
            if pagination_stop_reason
            in {"terminal_prefix_unresolved", "technical_prefix_unresolved"}
            and page_attempts
            else native_start
        )
        scanner_request = {
            "common_as_of_utc": _iso(self.as_of_utc),
            "timeframe_source_request_intervals": {
                timeframe: {
                    "start_utc": _iso(scanner_start),
                    "end_exclusive_utc": _iso(fetch_end_exclusive),
                }
            },
        }
        try:
            artifact = assembly.finalize(
                timeframe=timeframe,
                timeframe_seconds=seconds,
                broker_session_aligned=broker_session_aligned,
                native_start=native_start,
                fetch_end_exclusive=fetch_end_exclusive,
                as_of_utc=self.as_of_utc,
                partition_windows=cell_partition_windows,
                common_request=scanner_request,
                pagination=base_record["pagination"],
            )
        except NativeNoFullyClosedHistoryError as exc:
            base_record.update(
                {
                    "failure_status": "no_fully_closed_history",
                    "error": {
                        "type": "NoFullyClosedHistory",
                        "message": str(exc),
                    },
                    "rows": {
                        "raw_received": raw_rows,
                        "after_page_filter": pre_dedup_rows,
                        "duplicates_removed": duplicate_rows,
                        "out_of_range_removed": exc.out_of_range_rows,
                        "published": 0,
                    },
                }
            )
            return None, base_record
        except NativeCsvValidationError as exc:
            base_record.update(
                {
                    "failure_status": "data_quality_failed",
                    "error": {
                        "type": "NativeCsvValidationError",
                        "message": str(exc),
                    },
                    "rows": {
                        "raw_received": raw_rows,
                        "after_page_filter": pre_dedup_rows,
                        "duplicates_removed": duplicate_rows,
                        "published": 0,
                    },
                }
            )
            return None, base_record
        out_of_range_rows = int(artifact.out_of_range_rows)
        quality = dict(artifact.scan.quality)
        actual_first = artifact.start_ts.to_pydatetime()
        actual_last = artifact.end_ts.to_pydatetime()
        actual_last_close = parse_utc_datetime(artifact.scan.artifact.last_close_utc)
        base_record["request"]["last_fully_closed_bar_open_utc"] = _iso(actual_last)
        tolerance = boundary_tolerance
        # Coverage compares the serialized artifact with its source request.
        coverage_start = native_start
        coverage_end = self.as_of_utc if broker_session_aligned else fetch_end_exclusive
        start_delay = max(timedelta(0), actual_first - coverage_start)
        end_lag = max(timedelta(0), coverage_end - actual_last_close)
        internal_truncated_pages: List[Dict[str, Any]] = []
        boundary_truncated_pages: List[Dict[str, Any]] = []
        tolerance_seconds = float(tolerance.total_seconds())
        for item in truncated_pages:
            try:
                page_start = parse_utc_datetime(item["start_utc"])
                page_end = parse_utc_datetime(item["end_exclusive_utc"])
                page_start_delay = float(item.get("start_delay_seconds", 0.0) or 0.0)
                page_end_lag = float(item.get("end_lag_seconds", 0.0) or 0.0)
            except (KeyError, TypeError, ValueError, OverflowError):
                internal_truncated_pages.append(item)
                continue
            contains_first = page_start <= actual_first < page_end
            contains_last = page_start <= actual_last < page_end
            internal_defect = (
                page_start_delay > tolerance_seconds and not contains_first
            ) or (page_end_lag > tolerance_seconds and not contains_last)
            if internal_defect:
                internal_truncated_pages.append(item)
            else:
                boundary_truncated_pages.append(item)
        available_history_boundary: Optional[Dict[str, Any]] = None
        if (
            self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
            and terminal_prefix_pages
        ):
            available_history_boundary = _authenticated_available_history_boundary(
                page_attempts=page_attempts,
                terminal_prefix_pages=terminal_prefix_pages,
                empty_pages=empty_pages,
                failed_pages=failed_pages,
                actual_first_open_utc=actual_first,
            )
            if available_history_boundary is not None:
                # The exact available suffix is now source-authenticated. Missing
                # earlier rows remain a boundary shortfall for downstream power
                # assessment, not an internal acquisition failure.
                strategy_suffix_covered = True
                base_record["pagination"]["available_history_boundary"] = (
                    available_history_boundary
                )
        # An empty MT5 range response is not evidence that the broker has no
        # history.  A cold/evicted terminal cache can produce the same response,
        # including at the leading boundary.  Therefore every empty page is a
        # technical/source-availability state until independently corroborated;
        # it can never be downgraded to an economic or listing verdict merely
        # because it precedes the first admitted bar.
        technical_internal_gap = bool(
            internal_truncated_pages
            or (
                (failed_pages or empty_pages or terminal_prefix_pages)
                and not strategy_suffix_covered
            )
        )
        terminal_prefix_unresolved = bool(terminal_prefix_pages)
        full_span = (
            start_delay <= tolerance
            and end_lag <= tolerance
            and not technical_internal_gap
            and not terminal_prefix_unresolved
            and not truncated_pages
        )
        span_seconds = max(1.0, (coverage_end - coverage_start).total_seconds())
        observed_span_seconds = max(
            0.0,
            (
                min(coverage_end, actual_last_close) - max(coverage_start, actual_first)
            ).total_seconds(),
        )
        prefix_empty_count = int(
            pagination_stop_reason == "technical_prefix_unresolved"
            and bool(empty_pages)
        )
        coverage = {
            "status": "full_requested_span" if full_span else "partial_requested_span",
            "actual_first_open_utc": _iso(actual_first),
            "actual_last_open_utc": _iso(actual_last),
            "actual_last_close_utc": _iso(actual_last_close),
            "start_delay_seconds": float(start_delay.total_seconds()),
            "end_lag_seconds": float(end_lag.total_seconds()),
            "calendar_span_coverage_ratio": float(
                min(1.0, observed_span_seconds / span_seconds)
            ),
            "internal_empty_page_count": len(empty_pages) - prefix_empty_count,
            "truncated_page_count": len(truncated_pages),
            "internal_truncated_page_count": len(internal_truncated_pages),
            "boundary_truncated_page_count": len(boundary_truncated_pages),
            "failed_page_count": len(failed_pages),
            "terminal_prefix_unresolved_page_count": len(terminal_prefix_pages),
            "technical_integrity_status": (
                "internal_gap_detected"
                if technical_internal_gap
                else "complete_page_ledger"
            ),
            "market_calendar_completeness_assessed": False,
            "caveat": (
                "Continuous-calendar slot density and cadence gaps include weekends, holidays, "
                "and exchange closures; they are diagnostics, not a trading-calendar completeness claim."
            ),
        }
        # Timestamp-only v2 remains an explicit diagnostic, not eligibility
        # authority. Production completeness is established later by the
        # independently acquired cross-resolution audit counterpart.
        session_coverage = artifact.session_coverage
        session_coverage_details = artifact.session_details
        coverage_partitions = (
            session_coverage_details.get("partitions", {})
            if isinstance(session_coverage_details, Mapping)
            else {}
        )
        observed_session_bars = {
            partition: (
                int((coverage_partitions.get(partition) or {}).get("observed_rows"))
                if isinstance(coverage_partitions.get(partition), Mapping)
                and (coverage_partitions.get(partition) or {}).get("observed_rows")
                is not None
                else None
            )
            for partition in PARTITIONS
        }
        expected_session_bars = {
            partition: (
                float(
                    (coverage_partitions.get(partition) or {}).get(
                        "expected_rows_from_train_session_rate"
                    )
                )
                if isinstance(coverage_partitions.get(partition), Mapping)
                and (coverage_partitions.get(partition) or {}).get(
                    "expected_rows_from_train_session_rate"
                )
                is not None
                else None
            )
            for partition in PARTITIONS
        }
        status = "success" if full_span else "partial"
        failure_status = None
        if not full_span:
            failure_status = (
                "technical_history_gap"
                if technical_internal_gap
                else "boundary_history_shortfall"
            )
        base_record.update(
            {
                "status": status,
                "failure_status": failure_status,
                # Acquisition proves source/data integrity, not statistical
                # sufficiency.  A separate validator must promote this artifact
                # before an optimizer is allowed to consume it.
                "eligible_for_optimization": False,
                "eligible_for_downstream_validation": bool(full_span),
                "rows": {
                    "raw_received": raw_rows,
                    "after_page_filter": pre_dedup_rows,
                    "duplicates_removed": duplicate_rows,
                    "out_of_range_removed": out_of_range_rows,
                    "published": int(artifact.rows),
                },
                "coverage": coverage,
                "expected_session_coverage_by_partition": session_coverage,
                "observed_session_bars_by_partition": observed_session_bars,
                "expected_session_bars_by_partition": expected_session_bars,
                "expected_session_coverage_details_by_partition": session_coverage_details,
                "quality": quality,
                "columns": [str(column) for column in artifact.columns],
            }
        )
        if not (
            (status == "success" and failure_status is None)
            or (status == "partial" and failure_status == "boundary_history_shortfall")
        ):
            artifact.cleanup()
            return None, base_record
        return artifact, base_record

    @classmethod
    def _quality_stats(
        cls,
        frame: pd.DataFrame,
        timeframe_seconds: int,
        requested_start: datetime,
        requested_end: datetime,
    ) -> Dict[str, Any]:
        expected_slots = max(
            0,
            int((requested_end - requested_start).total_seconds() // timeframe_seconds),
        )
        required_present = all(
            column in frame.columns for column in cls.REQUIRED_COLUMNS
        )
        invalid_ohlc_rows = len(frame)
        if required_present and len(frame):
            numeric = frame.loc[:, cls.REQUIRED_COLUMNS].apply(
                pd.to_numeric, errors="coerce"
            )
            finite_mask = np.isfinite(numeric.to_numpy(dtype=float)).all(axis=1)
            invariant_mask = (
                (numeric["Low"] <= numeric[["Open", "Close"]].min(axis=1))
                & (numeric["High"] >= numeric[["Open", "Close"]].max(axis=1))
                & (numeric["Low"] <= numeric["High"])
            ).to_numpy(dtype=bool)
            invalid_ohlc_rows = int((~(finite_mask & invariant_mask)).sum())

        if len(frame) >= 2:
            nanoseconds = frame.index.asi8
            diffs_seconds = np.diff(nanoseconds) / 1_000_000_000.0
            missing_slots = np.maximum(
                np.rint(diffs_seconds / float(timeframe_seconds)).astype(np.int64) - 1,
                0,
            )
            gap_mask = diffs_seconds > float(timeframe_seconds) * 1.5
            cadence_break_count = int(gap_mask.sum())
            nominal_missing_slots = int(missing_slots[gap_mask].sum())
            largest_gap_seconds = float(diffs_seconds.max())
        else:
            cadence_break_count = 0
            nominal_missing_slots = 0
            largest_gap_seconds = 0.0

        off_grid_rows = 0
        if len(frame):
            epoch_seconds = frame.index.asi8 // 1_000_000_000
            off_grid_rows = int(
                np.count_nonzero(epoch_seconds % int(timeframe_seconds))
            )
        return {
            "ohlc_invariants_valid": bool(required_present and invalid_ohlc_rows == 0),
            "invalid_ohlc_rows": int(invalid_ohlc_rows),
            "monotonic_increasing": bool(frame.index.is_monotonic_increasing),
            "unique_timestamps": bool(frame.index.is_unique),
            "off_native_grid_rows": off_grid_rows,
            "expected_continuous_calendar_slots": expected_slots,
            "continuous_calendar_slot_density": (
                float(len(frame) / expected_slots) if expected_slots > 0 else None
            ),
            "cadence_break_count": cadence_break_count,
            "nominal_missing_continuous_slots": nominal_missing_slots,
            "largest_interbar_gap_seconds": largest_gap_seconds,
        }

    def acquire(
        self,
        *,
        max_new_cells: Optional[int] = None,
        max_runtime_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        try:
            return self._acquire_impl(
                max_new_cells=max_new_cells,
                max_runtime_seconds=max_runtime_seconds,
            )
        finally:
            self.store.close()

    def _acquire_impl(
        self,
        *,
        max_new_cells: Optional[int] = None,
        max_runtime_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Acquire missing cells, stopping only between atomic leaves when bounded.

        A bounded incomplete invocation intentionally writes no final manifest.
        Its newly committed leaves are immutable and become reusable on the next
        explicit ``--resume`` invocation.
        """

        if max_new_cells is not None and int(max_new_cells) <= 0:
            raise ValueError("max_new_cells must be positive when supplied")
        if max_runtime_seconds is not None and float(max_runtime_seconds) <= 0.0:
            raise ValueError("max_runtime_seconds must be positive when supplied")
        started = _utc(self.now_fn())
        monotonic_started = time.monotonic()
        self._assert_runtime_source_identity("run-start")
        reusable = self._validated_reusable_cells()
        run_acquisition_started = started
        for reusable_record in reusable.values():
            try:
                reusable_started = parse_utc_datetime(
                    reusable_record["acquisition_started_utc"]
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ResumeValidationError(
                    "reusable provenance has no valid acquisition start timestamp"
                ) from exc
            run_acquisition_started = min(run_acquisition_started, reusable_started)
        run_acquisition_started = self.store.ensure_resume_contract(
            self._common_request_contract(),
            self.runtime_metadata,
            run_acquisition_started,
            symbols=self.request.symbols,
            timeframes=self.request.timeframes,
            production_authoritative=bool(
                self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                and tuple(self.request.symbols) == tuple(PRODUCTION_SYMBOL_ORDER)
                and tuple(self.request.timeframes) == tuple(DEFAULT_TIMEFRAMES)
            ),
        )
        results: List[Dict[str, Any]] = []
        newly_acquired = 0
        bounded_reason: Optional[str] = None
        stop = False

        for symbol in self.request.symbols:
            broker_symbol: Optional[str] = None
            resolution_error: Optional[BaseException] = None
            connection_preflight_failed = False
            connection_preflight_error: Optional[BaseException] = None
            instrument_metadata: Dict[str, Any] = {"status": "unavailable"}
            resolved = False
            for timeframe in self.request.timeframes:
                key = (symbol, timeframe)
                if key in reusable:
                    results.append(
                        self._manifest_result(
                            symbol,
                            timeframe,
                            reusable[key],
                            disposition="reused",
                        )
                    )
                    continue
                if max_new_cells is not None and newly_acquired >= int(max_new_cells):
                    bounded_reason = "max_new_cells"
                    stop = True
                    break
                if (
                    max_runtime_seconds is not None
                    and time.monotonic() - monotonic_started
                    >= float(max_runtime_seconds)
                ):
                    bounded_reason = "max_runtime_seconds"
                    stop = True
                    break
                self._assert_runtime_source_identity(
                    f"before-cell:{symbol}:{timeframe}"
                )
                if not resolved:
                    resolved = True
                    connection_reader = getattr(
                        self.connector, "_check_connection", None
                    )
                    if callable(connection_reader):
                        try:
                            connection_preflight_failed = not bool(connection_reader())
                        except Exception as exc:
                            connection_preflight_failed = True
                            connection_preflight_error = exc
                    if not connection_preflight_failed and resolution_error is None:
                        try:
                            broker_symbol = self.connector.find_broker_symbol(symbol)
                        except Exception as exc:
                            broker_symbol = None
                            resolution_error = exc
                    instrument_metadata = (
                        self._read_instrument_metadata(symbol)
                        if resolution_error is None and broker_symbol is not None
                        else {"status": "unavailable"}
                    )
                leaf_started = _utc(self.now_fn())
                self._assert_runtime_source_identity(
                    f"after-resolution:{symbol}:{timeframe}"
                )
                terminal_resource_limit_hit = False
                output_resource_limit_hit = False
                assembly_resource_limit_hit = False
                publishable_status = False
                if connection_preflight_failed:
                    # `_check_connection` is a terminal-wide preflight.  Once it
                    # returns false, invoking one range call per cell only fans
                    # the same outage into hundreds of receipts and can further
                    # destabilize MT5.  Persist one bounded non-authoritative
                    # receipt at the first missing cell and stop the run.
                    record = {
                        "schema_version": SCHEMA_VERSION,
                        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
                        "immutable_record": True,
                        "run_id": self.store.run_id,
                        "status": "failed",
                        "failure_status": "connector_preflight_failed",
                        "eligible_for_optimization": False,
                        "eligible_for_downstream_validation": False,
                        "statistical_sufficiency_assessed": False,
                        "canonical_symbol": symbol,
                        "symbol": symbol,
                        "broker_symbol": None,
                        "timeframe": timeframe,
                        "timeframe_seconds": TIMEFRAME_SECONDS[timeframe],
                        "request": self._leaf_request_record(timeframe),
                        "source_kind": "broker_native",
                        "timezone": "UTC",
                        "source_environment": _json_safe(self.runtime_metadata),
                        "instrument": _json_safe(instrument_metadata),
                        "acquisition_started_utc": _iso(leaf_started),
                        "acquisition_completed_utc": _iso(_utc(self.now_fn())),
                        "connector_preflight_failure": {
                            "schema_version": (
                                "fxpm.native_history.connection_preflight.v1"
                            ),
                            "scope": "terminal_connection",
                            "connected": False,
                            "check_exception": bool(
                                connection_preflight_error is not None
                            ),
                            "find_broker_symbol_calls": 0,
                            "history_range_calls": 0,
                            "copy_rates_range_calls": 0,
                        },
                        "error": _bounded_error_diagnostic(
                            connection_preflight_error,
                            error_type=(
                                type(connection_preflight_error).__name__
                                if connection_preflight_error is not None
                                else "MT5ConnectorPreflightFailure"
                            ),
                            message=(
                                str(connection_preflight_error)
                                if connection_preflight_error is not None
                                else (
                                    "MT5 terminal connection preflight returned false; "
                                    "the run stopped before symbol resolution or history "
                                    "calls"
                                )
                            ),
                        ),
                    }
                    committed = self.store.commit_retryable_attempt(
                        symbol, timeframe, record
                    )
                elif resolution_error is not None or broker_symbol is None:
                    error = resolution_error or RuntimeError(
                        "symbol unavailable in broker universe"
                    )
                    record: Dict[str, Any] = {
                        "schema_version": SCHEMA_VERSION,
                        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
                        "immutable_record": True,
                        "run_id": self.store.run_id,
                        "status": "failed",
                        "failure_status": "symbol_resolution_failed",
                        "eligible_for_optimization": False,
                        "eligible_for_downstream_validation": False,
                        "statistical_sufficiency_assessed": False,
                        "canonical_symbol": symbol,
                        "symbol": symbol,
                        "broker_symbol": None,
                        "timeframe": timeframe,
                        "timeframe_seconds": TIMEFRAME_SECONDS[timeframe],
                        "request": self._leaf_request_record(timeframe),
                        "source_kind": "broker_native",
                        "timezone": "UTC",
                        "source_environment": _json_safe(self.runtime_metadata),
                        "instrument": _json_safe(instrument_metadata),
                        "acquisition_started_utc": _iso(leaf_started),
                        "acquisition_completed_utc": _iso(_utc(self.now_fn())),
                        "error": _bounded_error_diagnostic(error),
                    }
                    committed = self.store.commit_retryable_attempt(
                        symbol, timeframe, record
                    )
                else:
                    try:
                        frame, record = self._fetch_timeframe(
                            symbol,
                            broker_symbol,
                            timeframe,
                            leaf_started,
                            instrument_metadata,
                        )
                    except NativePageAssemblyResourceLimitError as exc:
                        assembly_resource_limit_hit = True
                        frame = None
                        record = {
                            "schema_version": SCHEMA_VERSION,
                            "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
                            "immutable_record": True,
                            "run_id": self.store.run_id,
                            "status": "failed",
                            "failure_status": "native_page_assembly_resource_limit",
                            "eligible_for_optimization": False,
                            "eligible_for_downstream_validation": False,
                            "statistical_sufficiency_assessed": False,
                            "canonical_symbol": symbol,
                            "symbol": symbol,
                            "broker_symbol": broker_symbol,
                            "timeframe": timeframe,
                            "timeframe_seconds": TIMEFRAME_SECONDS[timeframe],
                            "request": self._leaf_request_record(timeframe),
                            "source_kind": "broker_native",
                            "timezone": "UTC",
                            "source_environment": _json_safe(self.runtime_metadata),
                            "instrument": _json_safe(instrument_metadata),
                            "acquisition_started_utc": _iso(leaf_started),
                            "acquisition_completed_utc": _iso(_utc(self.now_fn())),
                            "native_page_assembly_resource_snapshot": exc.snapshot,
                            "error": _bounded_error_diagnostic(exc),
                        }
                    except OutputStorageResourceLimitError as exc:
                        output_resource_limit_hit = True
                        frame = None
                        record = {
                            "schema_version": SCHEMA_VERSION,
                            "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
                            "immutable_record": True,
                            "run_id": self.store.run_id,
                            "status": "failed",
                            "failure_status": "output_storage_resource_limit",
                            "eligible_for_optimization": False,
                            "eligible_for_downstream_validation": False,
                            "statistical_sufficiency_assessed": False,
                            "canonical_symbol": symbol,
                            "symbol": symbol,
                            "broker_symbol": broker_symbol,
                            "timeframe": timeframe,
                            "timeframe_seconds": TIMEFRAME_SECONDS[timeframe],
                            "request": self._leaf_request_record(timeframe),
                            "source_kind": "broker_native",
                            "timezone": "UTC",
                            "source_environment": _json_safe(self.runtime_metadata),
                            "instrument": _json_safe(instrument_metadata),
                            "acquisition_started_utc": _iso(leaf_started),
                            "acquisition_completed_utc": _iso(_utc(self.now_fn())),
                            "output_storage_resource_snapshot": exc.snapshot,
                            "error": _bounded_error_diagnostic(exc),
                        }
                    except TerminalCacheResourceLimitError as exc:
                        terminal_resource_limit_hit = True
                        frame = None
                        record = {
                            "schema_version": SCHEMA_VERSION,
                            "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
                            "immutable_record": True,
                            "run_id": self.store.run_id,
                            "status": "failed",
                            "failure_status": "terminal_cache_resource_limit",
                            "eligible_for_optimization": False,
                            "eligible_for_downstream_validation": False,
                            "statistical_sufficiency_assessed": False,
                            "canonical_symbol": symbol,
                            "symbol": symbol,
                            "broker_symbol": broker_symbol,
                            "timeframe": timeframe,
                            "timeframe_seconds": TIMEFRAME_SECONDS[timeframe],
                            "request": self._leaf_request_record(timeframe),
                            "source_kind": "broker_native",
                            "timezone": "UTC",
                            "source_environment": _json_safe(self.runtime_metadata),
                            "instrument": _json_safe(instrument_metadata),
                            "acquisition_started_utc": _iso(leaf_started),
                            "acquisition_completed_utc": _iso(_utc(self.now_fn())),
                            "terminal_cache_resource_snapshot": exc.snapshot,
                            "error": _bounded_error_diagnostic(exc),
                        }
                    except Exception as exc:
                        frame = None
                        record = {
                            "schema_version": SCHEMA_VERSION,
                            "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
                            "immutable_record": True,
                            "run_id": self.store.run_id,
                            "status": "failed",
                            "failure_status": "unexpected_acquisition_failure",
                            "eligible_for_optimization": False,
                            "eligible_for_downstream_validation": False,
                            "statistical_sufficiency_assessed": False,
                            "canonical_symbol": symbol,
                            "symbol": symbol,
                            "broker_symbol": broker_symbol,
                            "timeframe": timeframe,
                            "timeframe_seconds": TIMEFRAME_SECONDS[timeframe],
                            "request": self._leaf_request_record(timeframe),
                            "source_kind": "broker_native",
                            "timezone": "UTC",
                            "source_environment": _json_safe(self.runtime_metadata),
                            "instrument": _json_safe(instrument_metadata),
                            "acquisition_started_utc": _iso(leaf_started),
                            "acquisition_completed_utc": _iso(_utc(self.now_fn())),
                            "error": _bounded_error_diagnostic(exc),
                        }
                    try:
                        self._assert_runtime_source_identity(
                            f"after-fetch:{symbol}:{timeframe}"
                        )
                    except Exception:
                        if isinstance(frame, _NativeCellCsvArtifact):
                            frame.cleanup()
                        raise
                    publishable_status = bool(
                        (
                            record.get("status") == "success"
                            and record.get("failure_status") is None
                        )
                        or (
                            record.get("status") == "partial"
                            and record.get("failure_status")
                            == "boundary_history_shortfall"
                        )
                    )
                    if not publishable_status and record.get("status") == "partial":
                        technical_failure = str(record.get("failure_status") or "")
                        record["error"] = _bounded_error_diagnostic(
                            error_type=(
                                "TerminalPrefixUnresolved"
                                if technical_failure == "terminal_prefix_unresolved"
                                else "TechnicalHistoryGap"
                            ),
                            message=(
                                "source artifact is nonfinal and must be reacquired: "
                                f"{technical_failure}"
                            ),
                        )
                    if not publishable_status or frame is None:
                        committed = self.store.commit_retryable_attempt(
                            symbol, timeframe, record
                        )
                    else:
                        try:
                            committed = self.store.commit_leaf(
                                symbol, timeframe, record, frame
                            )
                        except OutputStorageResourceLimitError as exc:
                            output_resource_limit_hit = True
                            record = {
                                **record,
                                "status": "failed",
                                "failure_status": "output_storage_resource_limit",
                                "eligible_for_optimization": False,
                                "eligible_for_downstream_validation": False,
                                "complete": False,
                                "output_storage_resource_snapshot": exc.snapshot,
                                "error": _bounded_error_diagnostic(exc),
                            }
                            committed = self.store.commit_retryable_attempt(
                                symbol, timeframe, record
                            )
                        except Exception as write_exc:
                            record = {
                                **record,
                                "status": "failed",
                                "failure_status": "write_failed",
                                "eligible_for_optimization": False,
                                "eligible_for_downstream_validation": False,
                                "complete": False,
                                "error": _bounded_error_diagnostic(write_exc),
                            }
                            committed = self.store.commit_retryable_attempt(
                                symbol, timeframe, record
                            )
                        finally:
                            if isinstance(frame, _NativeCellCsvArtifact):
                                frame.cleanup()
                newly_acquired += 1
                results.append(
                    self._manifest_result(
                        symbol,
                        timeframe,
                        committed,
                        disposition=(
                            "retryable_attempt"
                            if not publishable_status
                            or committed.get("status") == "failed"
                            else "newly_acquired"
                        ),
                    )
                )
                if (
                    terminal_resource_limit_hit
                    or output_resource_limit_hit
                    or assembly_resource_limit_hit
                ):
                    bounded_reason = (
                        "terminal_cache_resource_limit"
                        if terminal_resource_limit_hit
                        else (
                            "output_storage_resource_limit"
                            if output_resource_limit_hit
                            else "native_page_assembly_resource_limit"
                        )
                    )
                    stop = True
                    break
                if committed.get("failure_status") in {
                    "connector_preflight_failed",
                    "unexpected_acquisition_failure",
                    "write_failed",
                }:
                    bounded_reason = str(committed.get("failure_status"))
                    stop = True
                    break
                if committed.get("failure_status") == "symbol_resolution_failed":
                    # Broker-symbol resolution is symbol-scoped, not six
                    # independent timeframe failures.  Retain one bounded
                    # receipt, skip the rest of this symbol, and continue the
                    # universe so one alias cannot starve later symbols.
                    break
            if stop:
                break

        if stop:
            reported = {(item["symbol"], item["timeframe"]) for item in results}
            for symbol in self.request.symbols:
                for timeframe in self.request.timeframes:
                    key = (symbol, timeframe)
                    if key in reusable and key not in reported:
                        results.append(
                            self._manifest_result(
                                symbol,
                                timeframe,
                                reusable[key],
                                disposition="reused",
                            )
                        )

        status_counts: Dict[str, int] = {
            "success": sum(item["status"] == "success" for item in results),
            "partial": sum(item["status"] == "partial" for item in results),
            "failed": sum(item["status"] == "failed" for item in results),
            "total": len(results),
        }
        resume_summary = {
            "enabled": bool(self.store.resume),
            "reused_cells": sum(
                item.get("resume_disposition") == "reused" for item in results
            ),
            "newly_acquired_cells": newly_acquired,
            "retryable_attempts": sum(
                item.get("resume_disposition") == "retryable_attempt"
                for item in results
            ),
            "stale_staging_removed": int(self.store.stale_staging_removed),
        }
        expected_total = len(self.request.symbols) * len(self.request.timeframes)
        completed_cells = {
            (str(item["symbol"]), str(item["timeframe"]))
            for item in results
            if item.get("status") in {"success", "partial"}
            and item.get("resume_disposition") != "retryable_attempt"
        }
        unresolved_attempts = [
            item
            for item in results
            if item.get("resume_disposition") == "retryable_attempt"
        ]
        if stop or len(completed_cells) != expected_total or unresolved_attempts:
            return {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "run_id": self.store.run_id,
                "complete": False,
                "manifest_path": None,
                "bounded_stop_reason": bounded_reason
                or (
                    "retryable_acquisition_failures"
                    if unresolved_attempts
                    else "incomplete_target"
                ),
                "results": results,
                "counts": status_counts,
                "resume": resume_summary,
                "remaining_cells": expected_total - len(completed_cells),
            }

        completed = _utc(self.now_fn())
        compatibility_timeframe = (
            "M5" if "M5" in self.request.timeframes else self.request.timeframes[0]
        )
        manifest_request: Dict[str, Any] = {
            "symbols": list(self.request.symbols),
            "timeframes": list(self.request.timeframes),
            "horizon_years": int(self.request.horizon_years),
            "requested_information_cutoff_utc": _iso(
                self.requested_information_cutoff_utc
            ),
            "requested_horizon_start_utc": _iso(self.requested_horizon_start_utc),
            "common_requested_start_utc": _iso(self.common_start_utc),
            "required_evidence_lead_start_utc": _iso(
                self.required_evidence_lead_start_utc
            ),
            "warmup_days": float(self.request.warmup_days),
            "safety_days": float(self.request.safety_days),
            "closed_bar_slack_seconds": max(
                TIMEFRAME_SECONDS[tf] for tf in self.request.timeframes
            ),
            "common_as_of_utc": _iso(self.as_of_utc),
            "retries": int(self.request.retries),
            "boundary_tolerance_days": float(self.request.boundary_tolerance_days),
            "window_years": (
                dict(
                    (self.request.timeframe_window_years or {})[compatibility_timeframe]
                )
                if self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
                else dict(self.request.window_years or {})
            ),
            "embargo_days": float(self.request.embargo_days),
            "common_calendar_boundaries": {
                partition: {"start_ts": _iso(start), "end_ts": _iso(end)}
                for partition, (start, end) in self.timeframe_partition_windows[
                    compatibility_timeframe
                ].items()
            },
        }
        if self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            manifest_request.update(self._timeframe_request_fields())
        manifest: Dict[str, Any] = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "immutable_record": True,
            "complete": True,
            "run_id": self.store.run_id,
            "acquisition_started_utc": _iso(run_acquisition_started),
            "acquisition_completed_utc": _iso(completed),
            "request": manifest_request,
            "source_environment": _json_safe(self.runtime_metadata),
            "results": results,
            "counts": status_counts,
            "resume": {
                key: value
                for key, value in resume_summary.items()
                if key != "retryable_attempts"
            },
        }
        self._assert_runtime_source_identity("before-manifest-publication")
        self.store._remove_empty_attempt_root()
        precommit_validator = self._prepublish_v5_gate(manifest)
        manifest_path = self.store.commit_manifest(
            manifest,
            precommit_validator=precommit_validator,
        )
        manifest["manifest_path"] = str(manifest_path)
        return manifest

    def _prepublish_v5_gate(
        self,
        manifest: Mapping[str, Any],
    ) -> Optional[Callable[[], None]]:
        """Deeply authenticate all 600 V5 cells before manifest publication."""

        is_production_v5 = bool(
            self.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
            and tuple(self.request.symbols) == tuple(PRODUCTION_SYMBOL_ORDER)
            and tuple(self.request.timeframes) == tuple(DEFAULT_TIMEFRAMES)
        )
        if not is_production_v5:
            return None
        receipts: List[_PinnedNativeFileReceipt] = []
        try:
            policy = build_trusted_v5_policy(
                expected_symbols=self.request.symbols,
                expected_source_environment=self.runtime_metadata,
                requested_information_cutoff_utc=(
                    self.requested_information_cutoff_utc
                ),
                timeframe_order=self.request.timeframes,
                retries=self.request.retries,
                timeframe_window_years=self.request.timeframe_window_years,
                horizon_years=self.request.horizon_years,
                embargo_days=self.request.embargo_days,
                warmup_days=self.request.warmup_days,
                safety_days=self.request.safety_days,
                boundary_tolerance_days=self.request.boundary_tolerance_days,
            )
            bound = validate_v5_production_manifest(manifest, policy)
            seed_path = self.store.run_root / RESUME_SEED_FILENAME
            seed, seed_receipt = _load_pinned_native_json_object(
                seed_path,
                run_root=self.store.run_root,
            )
            receipts.append(seed_receipt)
            expected_seed_keys = {
                "schema_version",
                "immutable_record",
                "run_id",
                "acquisition_started_utc",
                "request",
                "source_identity",
                "symbols",
                "timeframes",
                "production_authoritative",
            }
            if set(seed) != expected_seed_keys:
                raise ResumeValidationError(
                    "production V5 resume seed has an unknown exact schema"
                )
            expected_seed = {
                "schema_version": RESUME_SEED_SCHEMA_VERSION,
                "immutable_record": True,
                "run_id": self.store.run_id,
                "acquisition_started_utc": manifest["acquisition_started_utc"],
                "request": self._common_request_contract(),
                "source_identity": dict(
                    canonical_source_identity(self.runtime_metadata)
                ),
                "symbols": list(PRODUCTION_SYMBOL_ORDER),
                "timeframes": list(DEFAULT_TIMEFRAMES),
                "production_authoritative": True,
            }
            if seed != expected_seed:
                raise ResumeValidationError(
                    "production V5 resume seed differs from the manifest authority"
                )
            expected_cells = {
                (symbol, timeframe)
                for symbol in PRODUCTION_SYMBOL_ORDER
                for timeframe in DEFAULT_TIMEFRAMES
            }
            discovered = _discover_native_leaves(
                self.store.run_root,
                symbols=PRODUCTION_SYMBOL_ORDER,
                timeframes=DEFAULT_TIMEFRAMES,
            )
            if set(discovered) != expected_cells:
                raise ResumeValidationError(
                    "production V5 run does not contain the exact 600-cell artifact roster"
                )
            results_by_cell = bound["results_by_cell"]
            total_artifact_bytes = 0
            run_started = parse_utc_datetime(str(manifest["acquisition_started_utc"]))
            run_completed = parse_utc_datetime(
                str(manifest["acquisition_completed_utc"])
            )
            for symbol in PRODUCTION_SYMBOL_ORDER:
                for timeframe in DEFAULT_TIMEFRAMES:
                    result = results_by_cell[(symbol, timeframe)]
                    record = self._validate_reusable_leaf(
                        symbol,
                        timeframe,
                        discovered[(symbol, timeframe)],
                        manifest_result=result,
                        receipts=receipts,
                    )
                    artifact = record.get("artifact")
                    if not isinstance(artifact, Mapping):
                        raise ResumeValidationError(
                            "production V5 cell has no exact artifact byte binding: "
                            f"{symbol}/{timeframe}"
                        )
                    artifact_bytes = artifact.get("bytes")
                    if type(artifact_bytes) is not int or artifact_bytes <= 0:
                        raise ResumeValidationError(
                            "production V5 cell artifact byte binding is invalid: "
                            f"{symbol}/{timeframe}"
                        )
                    total_artifact_bytes += artifact_bytes
                    if total_artifact_bytes > MAX_TOTAL_ARTIFACT_BYTES:
                        raise OutputStorageResourceLimitError(
                            {
                                "status": "limit_exceeded",
                                "violations": [
                                    "output_run_total_artifact_byte_cap_exceeded"
                                ],
                                "observed_total_artifact_bytes": total_artifact_bytes,
                                "maximum_total_artifact_bytes": (
                                    MAX_TOTAL_ARTIFACT_BYTES
                                ),
                            }
                        )
                    cell_started = parse_utc_datetime(
                        str(record["acquisition_started_utc"])
                    )
                    cell_completed = parse_utc_datetime(
                        str(record["acquisition_completed_utc"])
                    )
                    if not (
                        run_started <= cell_started <= cell_completed <= run_completed
                    ):
                        raise ResumeValidationError(
                            "production V5 cell acquisition time lies outside the run: "
                            f"{symbol}/{timeframe}"
                        )
            self.store.assert_exact_artifact_total(total_artifact_bytes)
        except NativeRequestContractError as exc:
            raise RuntimeError(
                "refusing to publish a production V5 run that fails its own "
                f"reader contract: {';'.join(exc.errors)}"
            ) from exc
        except (
            KeyError,
            OSError,
            TypeError,
            ValueError,
            ResumeValidationError,
            OutputStorageResourceLimitError,
        ) as exc:
            raise RuntimeError(
                "refusing to publish a production V5 run that fails deep artifact "
                f"authentication: {exc}"
            ) from exc

        frozen_receipts = tuple(receipts)

        def _verify_unchanged_inputs() -> None:
            for receipt in frozen_receipts:
                _verify_pinned_native_file_receipt(receipt)

        # Verify once after the complete multi-file scan and once again inside
        # the atomic manifest commit immediately before the create-only rename.
        _verify_unchanged_inputs()
        return _verify_unchanged_inputs

    @staticmethod
    def _manifest_result(
        symbol: str,
        timeframe: str,
        record: Mapping[str, Any],
        *,
        disposition: str = "newly_acquired",
    ) -> Dict[str, Any]:
        artifact = record.get("artifact") if isinstance(record, Mapping) else None
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "status": record.get("status"),
            "failure_status": record.get("failure_status"),
            "eligible_for_optimization": bool(
                record.get("eligible_for_optimization", False)
            ),
            "eligible_for_downstream_validation": bool(
                record.get("eligible_for_downstream_validation", False)
            ),
            "rows": int((artifact or {}).get("rows", 0))
            if isinstance(artifact, Mapping)
            else 0,
            "sha256": (artifact or {}).get("sha256")
            if isinstance(artifact, Mapping)
            else None,
            "relative_leaf": f"{symbol}/{timeframe}",
            "resume_disposition": disposition,
        }


def default_run_id(now: Optional[datetime] = None) -> str:
    now = _utc(now or datetime.now(timezone.utc))
    return now.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


def _load_config(path: Path) -> Dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("config root must be a JSON object")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Acquire independent native MT5 histories into data/native (read-only terminal use)."
    )
    parser.add_argument("--config", default="config.json")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Canonical symbols; defaults to config.json symbols",
    )
    parser.add_argument(
        "--timeframes", nargs="+", help="Defaults to all six native timeframes"
    )
    parser.add_argument("--years", type=int)
    parser.add_argument("--train-years", type=int)
    parser.add_argument("--tuning-years", type=int)
    parser.add_argument("--lockbox-years", type=int)
    parser.add_argument(
        "--timeframe-window-years-json",
        help=(
            "Nested timeframe/partition year map as inline JSON or a JSON file. "
            "Defaults new runs to the non-weakenable V5 timeframe matrix."
        ),
    )
    parser.add_argument(
        "--legacy-flat-windows",
        action="store_true",
        help=(
            "Explicitly reproduce the legacy flat window contract. Supplying "
            "--years/--train-years/--tuning-years/--lockbox-years also selects it."
        ),
    )
    parser.add_argument("--embargo-days", type=float)
    parser.add_argument("--warmup-days", type=float)
    parser.add_argument("--safety-days", type=float)
    parser.add_argument("--boundary-tolerance-days", type=float)
    parser.add_argument(
        "--as-of", help="Aware ISO-8601 UTC timestamp; defaults to current UTC"
    )
    parser.add_argument("--output-root", default="data/native")
    parser.add_argument("--run-id")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume an existing manifest-free run after strict provenance validation.",
    )
    parser.add_argument(
        "--max-new-cells",
        type=int,
        help="Stop between atomic cells after this many new leaves; no final manifest until complete.",
    )
    parser.add_argument(
        "--max-runtime-seconds",
        type=float,
        help=(
            "Hard wall-clock deadline for execute mode. A supervised worker is "
            "terminated at the deadline; create-only staging prevents a partial "
            "cell from becoming visible."
        ),
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=1,
        help="Compatibility field fixed at 1; MT5 connector owns bounded synchronization.",
    )
    parser.add_argument(
        "--retry-sleep-seconds",
        type=float,
        default=0.0,
        help="Compatibility field fixed at 0; no acquisition-level retry sleeps.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually open the configured MT5 terminal and acquire bars. Without this flag, print the plan only.",
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--_supervised-worker",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    return parser


def _filtered_dataclass_kwargs(
    dataclass_type: Any, values: Mapping[str, Any]
) -> Dict[str, Any]:
    allowed = {item.name for item in fields(dataclass_type)}
    return {key: value for key, value in dict(values or {}).items() if key in allowed}


def _load_nested_window_option(
    value: Optional[str],
    *,
    config_dir: Path,
) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        raise ValueError("--timeframe-window-years-json cannot be empty")
    if text.startswith("{"):
        parsed = json.loads(text)
    else:
        path = Path(text)
        if not path.is_absolute():
            path = config_dir / path
        parsed = _load_config(path.resolve())
    if not isinstance(parsed, dict):
        raise ValueError("timeframe window years JSON must be an object")
    return parsed


def _run_supervised_worker(
    argv: Optional[Sequence[str]],
    *,
    timeout_seconds: float,
) -> int:
    """Run execute mode in a killable, cache-monitored child process."""

    child_argv = list(sys.argv[1:] if argv is None else argv)
    child_argv.append("--_supervised-worker")
    command = [sys.executable, str(Path(__file__).resolve()), *child_argv]
    initial_snapshot = _supervisor_terminal_cache_snapshot()
    if initial_snapshot.get("violations"):
        print(
            json.dumps(
                {
                    "status": "terminal_cache_resource_limit",
                    "snapshot": initial_snapshot,
                    "published_partial_cell": False,
                    "resume_required": True,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return RESOURCE_LIMIT_EXIT_CODE

    process = subprocess.Popen(command)
    started = time.monotonic()
    while True:
        remaining = float(timeout_seconds) - (time.monotonic() - started)
        if remaining <= 0.0:
            _terminate_supervised_worker(process)
            print(
                json.dumps(
                    {
                        "status": "hard_runtime_timeout",
                        "timeout_seconds": float(timeout_seconds),
                        "published_partial_cell": False,
                        "resume_required": True,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return HARD_TIMEOUT_EXIT_CODE
        try:
            return int(process.wait(timeout=min(SUPERVISOR_POLL_SECONDS, remaining)))
        except subprocess.TimeoutExpired:
            snapshot = _supervisor_terminal_cache_snapshot()
            if not snapshot.get("violations"):
                continue
            _terminate_supervised_worker(process)
            print(
                json.dumps(
                    {
                        "status": "terminal_cache_resource_limit",
                        "snapshot": snapshot,
                        "published_partial_cell": False,
                        "resume_required": True,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return RESOURCE_LIMIT_EXIT_CODE


def _terminate_supervised_worker(process: Any) -> None:
    if process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=2.0)
    except (OSError, subprocess.TimeoutExpired):
        try:
            process.kill()
        finally:
            process.wait()


def _supervisor_terminal_cache_snapshot() -> Dict[str, Any]:
    """Monitor every standard MT5 data directory while a worker is blocked."""

    raw_appdata = os.environ.get("APPDATA")
    if not raw_appdata:
        return {
            "status": "unavailable",
            "violations": [],
            "reason": "APPDATA_not_reported",
        }
    terminal_root = Path(raw_appdata) / "MetaQuotes" / "Terminal"
    if not terminal_root.is_dir():
        return {
            "status": "unavailable",
            "violations": [],
            "reason": "standard_terminal_root_not_found",
        }
    violations: List[str] = []
    try:
        usage = shutil.disk_usage(terminal_root)
        disk_free = int(usage.free)
        disk_total = int(usage.total)
    except OSError as exc:
        return {
            "status": "invalid",
            "terminal_root": str(terminal_root),
            "violations": ["terminal_disk_usage_unavailable"],
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    if disk_free < TERMINAL_MIN_FREE_BYTES:
        violations.append("terminal_disk_free_reserve_breached")
    count = 0
    largest_bytes = 0
    largest_path: Optional[str] = None
    entries_inspected = 0
    scan_error: Optional[Dict[str, str]] = None
    budget = {"entries": 0}
    try:
        with os.scandir(terminal_root) as instances:
            for instance_count, entry in enumerate(instances, start=1):
                budget["entries"] += 1
                if instance_count > MAX_TERMINAL_INSTANCES:
                    violations.append("terminal_instance_entry_cap_exceeded")
                    break
                if budget["entries"] > MAX_TERMINAL_HCC_SCAN_ENTRIES:
                    violations.append("terminal_hcc_global_entry_cap_exceeded")
                    break
                observed_instance = entry.stat(follow_symlinks=False)
                if entry.is_symlink() or _unsafe_native_file_attributes(
                    observed_instance
                ):
                    violations.append("terminal_hcc_scan_unsafe_entry_detected")
                    break
                if not stat.S_ISDIR(observed_instance.st_mode):
                    continue
                instance = terminal_root / entry.name
                bases = instance / "bases"
                try:
                    bases_stat = bases.lstat()
                except FileNotFoundError:
                    continue
                if _unsafe_native_file_attributes(bases_stat) or not stat.S_ISDIR(
                    bases_stat.st_mode
                ):
                    violations.append("terminal_hcc_scan_unsafe_entry_detected")
                    break
                observed = _bounded_terminal_hcc_scan(
                    bases,
                    relative_root=terminal_root,
                    maximum_directory_depth=3,
                    budget=budget,
                )
                count += int(observed["hcc_files_inspected"])
                entries_inspected = int(observed["entries_inspected"])
                if int(observed["largest_hcc_bytes"]) > largest_bytes:
                    largest_bytes = int(observed["largest_hcc_bytes"])
                    largest_path = observed["largest_hcc_relative_path"]
                if observed["scan_error"] is not None:
                    scan_error = observed["scan_error"]
                if observed["violations"]:
                    violations.extend(observed["violations"])
                    break
    except OSError as exc:
        violations.append("terminal_hcc_scan_failed")
        scan_error = {
            "type": type(exc).__name__,
            "message": str(exc)[:4096],
        }
    entries_inspected = max(entries_inspected, int(budget["entries"]))
    if largest_bytes > TERMINAL_HCC_MAX_FILE_BYTES:
        violations.append("terminal_hcc_file_bound_breached")
    return {
        "status": "limit_exceeded" if violations else "within_bounds",
        "terminal_root": str(terminal_root),
        "disk_total_bytes": disk_total,
        "disk_free_bytes": disk_free,
        "minimum_free_bytes": TERMINAL_MIN_FREE_BYTES,
        "hcc_files_inspected": count,
        "hcc_entries_inspected": entries_inspected,
        "largest_hcc_bytes": largest_bytes,
        "largest_hcc_relative_path": largest_path,
        "maximum_hcc_file_bytes": TERMINAL_HCC_MAX_FILE_BYTES,
        "scan_error": scan_error,
        "violations": violations,
    }


def _resolved_cli_output_root(args: argparse.Namespace) -> Path:
    config_path = Path(args.config).resolve()
    output_root = Path(args.output_root)
    if not output_root.is_absolute():
        output_root = (config_path.parent / output_root).resolve()
    return _normalized_native_root(output_root)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.execute and args.max_runtime_seconds is None:
        args.max_runtime_seconds = DEFAULT_EXECUTION_RUNTIME_SECONDS
    if (
        args.execute
        and args.max_runtime_seconds is not None
        and not args._supervised_worker
    ):
        if args.max_runtime_seconds <= 0.0:
            raise ValueError("--max-runtime-seconds must be positive")
        return _run_supervised_worker(
            argv,
            timeout_seconds=float(args.max_runtime_seconds),
        )

    terminal_lock: Optional[NativeHistoryTerminalLock] = None
    preheld_lock: Optional[NativeHistoryRunLock] = None
    try:
        if args.execute:
            output_root = _resolved_cli_output_root(args)
            terminal_lock = NativeHistoryTerminalLock(output_root).acquire()
            if args.resume:
                if not args.run_id:
                    raise ValueError("--resume requires the exact existing --run-id")
                preheld_lock = NativeHistoryRunLock(output_root, args.run_id).acquire()
        return _main_with_args(args, preheld_lock=preheld_lock)
    finally:
        if preheld_lock is not None:
            preheld_lock.release()
        if terminal_lock is not None:
            terminal_lock.release()


def _main_with_args(
    args: argparse.Namespace,
    *,
    preheld_lock: Optional[NativeHistoryRunLock] = None,
) -> int:
    config_path = Path(args.config).resolve()
    config = _load_config(config_path)
    symbols = tuple(args.symbols or config.get("symbols") or ())
    timeframes = tuple(args.timeframes or DEFAULT_TIMEFRAMES)
    output_root = Path(args.output_root)
    if not output_root.is_absolute():
        output_root = (config_path.parent / output_root).resolve()
    output_root = _normalized_native_root(output_root)
    if args.resume and not args.run_id:
        raise ValueError("--resume requires the exact existing --run-id")
    if args.max_new_cells is not None and args.max_new_cells <= 0:
        raise ValueError("--max-new-cells must be positive")
    if args.max_runtime_seconds is not None and args.max_runtime_seconds <= 0.0:
        raise ValueError("--max-runtime-seconds must be positive")

    pipeline_config = (
        config.get("pipeline") if isinstance(config.get("pipeline"), Mapping) else {}
    )
    configured_windows = (
        pipeline_config.get("evidence_window_years")
        if isinstance(pipeline_config.get("evidence_window_years"), Mapping)
        else {}
    )
    configured_timeframe_windows = (
        pipeline_config.get("evidence_timeframe_window_years")
        if isinstance(pipeline_config.get("evidence_timeframe_window_years"), Mapping)
        else None
    )
    nested_cli_windows = _load_nested_window_option(
        args.timeframe_window_years_json,
        config_dir=config_path.parent,
    )
    flat_cli_supplied = any(
        value is not None
        for value in (
            args.years,
            args.train_years,
            args.tuning_years,
            args.lockbox_years,
        )
    )
    if nested_cli_windows is not None and (
        args.legacy_flat_windows or flat_cli_supplied
    ):
        raise ValueError(
            "nested timeframe windows cannot be combined with legacy flat-window options"
        )
    resume_contract: Optional[NativeHistoryResumeContract] = None
    if args.resume:
        if preheld_lock is not None:
            reconcile_completed_manifest_staging(
                output_root,
                args.run_id,
                run_lock=preheld_lock,
            )
            reconcile_resume_seed_staging(
                output_root,
                args.run_id,
                run_lock=preheld_lock,
            )
        resume_contract = load_native_history_resume_contract(output_root, args.run_id)
        locked = dict(resume_contract.request)
        window_contract_version = str(
            locked.get("window_contract_version") or LEGACY_FLAT_WINDOW_CONTRACT_VERSION
        )
        observed_symbols = (
            canonical_v5_symbols(symbols)
            if window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
            else tuple(str(symbol).upper() for symbol in symbols)
        )
        observed_timeframes = tuple(str(value).upper() for value in timeframes)
        if (
            observed_symbols != resume_contract.symbols
            or observed_timeframes != resume_contract.timeframes
        ):
            raise ResumeValidationError(
                "resume symbols/timeframes differ in membership or order from the "
                "immutable run contract"
            )
        expected_production_authoritative = bool(
            window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
            and observed_symbols == tuple(PRODUCTION_SYMBOL_ORDER)
            and observed_timeframes == tuple(DEFAULT_TIMEFRAMES)
        )
        if (
            resume_contract.production_authoritative
            is not expected_production_authoritative
        ):
            raise ResumeValidationError(
                "resume production intent differs from the immutable run contract"
            )
        symbols = resume_contract.symbols
        timeframes = resume_contract.timeframes
        if window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION and (
            args.legacy_flat_windows or flat_cli_supplied
        ):
            raise ResumeValidationError(
                "legacy flat-window options cannot resume a timeframe-window run"
            )
        if (
            window_contract_version == LEGACY_FLAT_WINDOW_CONTRACT_VERSION
            and nested_cli_windows is not None
        ):
            raise ResumeValidationError(
                "nested timeframe windows cannot resume a legacy flat-window run"
            )
        original_cutoff = parse_utc_datetime(
            str(locked["requested_information_cutoff_utc"])
        )
        if args.as_of and _iso(parse_utc_datetime(args.as_of)) != _iso(original_cutoff):
            raise ResumeValidationError(
                "--as-of must exactly equal the original requested information cutoff"
            )
        explicit_checks: Dict[str, Tuple[Any, Any]] = {
            "--years": (args.years, int(locked["horizon_years"])),
            "--embargo-days": (args.embargo_days, float(locked["embargo_days"])),
            "--warmup-days": (args.warmup_days, float(locked["warmup_days"])),
            "--safety-days": (args.safety_days, float(locked["safety_days"])),
            "--boundary-tolerance-days": (
                args.boundary_tolerance_days,
                float(locked["boundary_tolerance_days"]),
            ),
        }
        if window_contract_version == LEGACY_FLAT_WINDOW_CONTRACT_VERSION:
            explicit_checks.update(
                {
                    "--train-years": (
                        args.train_years,
                        int(locked["window_years"]["optimizer_train"]),
                    ),
                    "--tuning-years": (
                        args.tuning_years,
                        int(locked["window_years"]["tuning_validation"]),
                    ),
                    "--lockbox-years": (
                        args.lockbox_years,
                        int(locked["window_years"]["final_lockbox"]),
                    ),
                }
            )
        for option, (supplied, expected) in explicit_checks.items():
            if supplied is not None and supplied != expected:
                raise ResumeValidationError(
                    f"{option}={supplied!r} differs from original value {expected!r}"
                )
        if max(TIMEFRAME_SECONDS[item] for item in timeframes) != int(
            locked["closed_bar_slack_seconds"]
        ):
            raise ResumeValidationError(
                "resume timeframe set changes the original closed-bar slack contract"
            )
        target_symbols = (
            canonical_v5_symbols(symbols)
            if window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
            else tuple(str(symbol).upper() for symbol in symbols)
        )
        target = {
            (symbol, str(timeframe).upper())
            for symbol in target_symbols
            for timeframe in timeframes
        }
        extras = sorted(set(resume_contract.existing_cells) - target)
        if extras:
            raise ResumeValidationError(
                f"existing run contains cells outside this resume target: {extras}"
            )
        as_of = original_cutoff
        horizon_years = int(locked["horizon_years"])
        if window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION:
            locked_timeframe_years = locked.get("timeframe_window_years")
            if not isinstance(locked_timeframe_years, Mapping):
                raise ResumeValidationError(
                    "timeframe resume contract has no timeframe window matrix"
                )
            timeframe_window_years = {
                str(timeframe): dict(years)
                for timeframe, years in locked_timeframe_years.items()
                if isinstance(years, Mapping)
            }
            if nested_cli_windows is not None:
                normalized_supplied = normalize_timeframe_window_years(
                    nested_cli_windows
                )
                supplied_active = {
                    timeframe.upper(): dict(normalized_supplied[timeframe.upper()])
                    for timeframe in timeframes
                }
                if supplied_active != timeframe_window_years:
                    raise ResumeValidationError(
                        "--timeframe-window-years-json differs from the original map"
                    )
            window_years = dict(DEFAULT_WINDOW_YEARS)
        else:
            timeframe_window_years = None
            window_years = dict(locked["window_years"])
        embargo_days = float(locked["embargo_days"])
        warmup_days = float(locked["warmup_days"])
        safety_days = float(locked["safety_days"])
        boundary_tolerance_days = float(locked["boundary_tolerance_days"])
        enforce_evidence_horizon = (
            locked["required_evidence_lead_start_utc"] is not None
        )
    else:
        as_of = (
            parse_utc_datetime(args.as_of) if args.as_of else datetime.now(timezone.utc)
        )
        horizon_years = args.years if args.years is not None else 10
        configured_contract = str(
            pipeline_config.get("native_history_window_contract") or ""
        ).strip()
        legacy_flat = bool(
            args.legacy_flat_windows
            or flat_cli_supplied
            or configured_contract == LEGACY_FLAT_WINDOW_CONTRACT_VERSION
        )
        if configured_contract and configured_contract not in {
            LEGACY_FLAT_WINDOW_CONTRACT_VERSION,
            TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
        }:
            raise ValueError(
                "pipeline.native_history_window_contract must be "
                f"{LEGACY_FLAT_WINDOW_CONTRACT_VERSION!r} or "
                f"{TIMEFRAME_EVIDENCE_CONTRACT_VERSION!r}"
            )
        if legacy_flat:
            window_contract_version = LEGACY_FLAT_WINDOW_CONTRACT_VERSION
            timeframe_window_years = None
            window_years = {
                "optimizer_train": (
                    args.train_years
                    if args.train_years is not None
                    else configured_windows.get(
                        "optimizer_train", DEFAULT_WINDOW_YEARS["optimizer_train"]
                    )
                ),
                "tuning_validation": (
                    args.tuning_years
                    if args.tuning_years is not None
                    else configured_windows.get(
                        "tuning_validation", DEFAULT_WINDOW_YEARS["tuning_validation"]
                    )
                ),
                "final_lockbox": (
                    args.lockbox_years
                    if args.lockbox_years is not None
                    else configured_windows.get(
                        "final_lockbox", DEFAULT_WINDOW_YEARS["final_lockbox"]
                    )
                ),
            }
        else:
            window_contract_version = TIMEFRAME_EVIDENCE_CONTRACT_VERSION
            timeframe_window_years = (
                nested_cli_windows
                if nested_cli_windows is not None
                else configured_timeframe_windows
            )
            # Retained only as a compatibility projection in timeframe artifacts.
            window_years = dict(DEFAULT_WINDOW_YEARS)
        if args.embargo_days is not None:
            embargo_days = args.embargo_days
        else:
            try:
                configured_purge_days = max(
                    0.0,
                    float(pipeline_config.get("evidence_purge_days", 7.0)),
                )
            except (TypeError, ValueError, OverflowError):
                configured_purge_days = 7.0
            try:
                legacy_m5_purge_days = max(
                    0.0,
                    int(pipeline_config.get("split_embargo_bars", 0) or 0)
                    * 5.0
                    / 1440.0,
                )
            except (TypeError, ValueError, OverflowError):
                legacy_m5_purge_days = 0.0
            embargo_days = max(configured_purge_days, legacy_m5_purge_days)
        warmup_days = (
            args.warmup_days
            if args.warmup_days is not None
            else pipeline_config.get("evidence_warmup_days", 400.0)
        )
        safety_days = args.safety_days if args.safety_days is not None else 14.0
        boundary_tolerance_days = (
            args.boundary_tolerance_days
            if args.boundary_tolerance_days is not None
            else pipeline_config.get("evidence_boundary_tolerance_days", 7.0)
        )
        enforce_evidence_horizon = True

    request = NativeHistoryRequest(
        symbols=symbols,
        timeframes=timeframes,
        horizon_years=horizon_years,
        as_of_utc=as_of,
        retries=args.retries,
        retry_sleep_seconds=args.retry_sleep_seconds,
        window_years=window_years,
        timeframe_window_years=timeframe_window_years,
        window_contract_version=window_contract_version,
        embargo_days=embargo_days,
        warmup_days=warmup_days,
        safety_days=safety_days,
        boundary_tolerance_days=boundary_tolerance_days,
        enforce_evidence_horizon=enforce_evidence_horizon,
    )
    production_candidate = bool(
        request.window_contract_version == TIMEFRAME_EVIDENCE_CONTRACT_VERSION
        and args.symbols is None
        and request.timeframes == DEFAULT_TIMEFRAMES
        and len(request.symbols) == len(PRODUCTION_SYMBOL_ORDER)
    )
    if production_candidate and request.symbols != PRODUCTION_SYMBOL_ORDER:
        raise ValueError(
            "config-driven V5 production acquisition requires the exact ordered "
            "100-symbol authority roster, including case-sensitive broker identity"
        )
    production_authoritative = bool(
        production_candidate and request.symbols == PRODUCTION_SYMBOL_ORDER
    )
    run_id = args.run_id or default_run_id(as_of)
    if run_id == TERMINAL_INGESTION_LOCK_ID:
        raise ValueError(f"run id {TERMINAL_INGESTION_LOCK_ID!r} is reserved")
    plan = {
        "mode": "execute" if args.execute else "plan_only_no_connection",
        "symbols": list(request.symbols),
        "timeframes": list(request.timeframes),
        "horizon_years": request.horizon_years,
        "window_years": dict(request.window_years or {}),
        "window_contract_version": request.window_contract_version,
        "timeframe_window_years": (
            {
                timeframe: dict(years)
                for timeframe, years in (request.timeframe_window_years or {}).items()
            }
            if request.timeframe_window_years is not None
            else None
        ),
        "embargo_days": float(request.embargo_days),
        "warmup_days": float(request.warmup_days),
        "safety_days": float(request.safety_days),
        "boundary_tolerance_days": float(request.boundary_tolerance_days),
        "as_of_utc": _iso(as_of),
        "output_root": str(output_root),
        "run_id": run_id,
        "production_authoritative": production_authoritative,
        "resume": bool(args.resume),
        "reusable_cells_discovered": (
            len(resume_contract.existing_cells) if resume_contract is not None else 0
        ),
        "max_new_cells": args.max_new_cells,
        "max_runtime_seconds": args.max_runtime_seconds,
    }
    print(json.dumps(plan, indent=2, sort_keys=True))
    if not args.execute:
        return 0

    # The store adopts (resume) or acquires (new run) the OS writer lock before
    # the MT5 module is imported or a terminal connection is attempted.
    store = NativeHistoryStore(
        output_root,
        run_id,
        resume=args.resume,
        run_lock=preheld_lock,
    )
    connector: Any = None
    try:
        # Delayed import is intentional: plan mode never initializes or even
        # requires the MetaTrader5 package.  No execution method is referenced.
        import pm_mt5 as pm_mt5_module
        from pm_mt5 import MT5Config, MT5Connector

        mt5_values = dict(config.get("mt5") or {})
        if "terminal_path" in mt5_values and "path" not in mt5_values:
            mt5_values["path"] = mt5_values["terminal_path"]
        connector = MT5Connector(
            MT5Config(**_filtered_dataclass_kwargs(MT5Config, mt5_values))
        )
        if not connector.connect():
            error = connector.get_last_error()
            print(f"MT5 connection failed: {error}", file=sys.stderr)
            return 2
        runtime_metadata = snapshot_read_only_runtime_metadata(
            connector,
            getattr(pm_mt5_module, "mt5", None),
        )
        manifest = NativeHistoryAcquirer(
            connector,
            store,
            request,
            runtime_metadata=runtime_metadata,
            terminal_api=getattr(pm_mt5_module, "mt5", None),
        ).acquire(
            max_new_cells=args.max_new_cells,
            max_runtime_seconds=args.max_runtime_seconds,
        )
    finally:
        if connector is not None:
            connector.disconnect()
        store.close()
    print(json.dumps(_json_safe(manifest), indent=2, sort_keys=True))
    if not manifest.get("complete", False):
        return 4
    return 0 if int(manifest["counts"]["failed"]) == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
