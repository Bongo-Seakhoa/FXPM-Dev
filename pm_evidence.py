"""Fail-closed calendar and native-timeframe evidence contracts for FxPM.

This module is intentionally independent from :mod:`pm_core` and
:mod:`pm_pipeline`.  It contains only deterministic validation helpers so the
data-depth decision can be tested without constructing an optimizer or making
an MT5 connection.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

import pandas as pd


EVIDENCE_CONTRACT_VERSION = "2026-07-10-calendar-lockbox-v2"
TIMEFRAME_EVIDENCE_CONTRACT_VERSION = "fxpm-timeframe-evidence-v3"
CALENDAR_SPLIT_MODE = "calendar_lockbox_v2"
NATIVE_SOURCE_KIND = "broker_native"
PARTITIONS: Tuple[str, ...] = (
    "optimizer_train",
    "tuning_validation",
    "final_lockbox",
)
SUPPORTED_TIMEFRAMES: Tuple[str, ...] = ("M5", "M15", "M30", "H1", "H4", "D1")

DEFAULT_WINDOW_YEARS: Dict[str, int] = {
    "optimizer_train": 5,
    "tuning_validation": 2,
    "final_lockbox": 2,
}

# Acquisition/evidence planning priors.  These are necessary calendar-span
# floors, not sufficient profitability evidence; effective-sample and
# power-derived trade gates remain independent and binding.
DEFAULT_TIMEFRAME_WINDOW_YEARS: Dict[str, Dict[str, int]] = {
    "M5": {"optimizer_train": 5, "tuning_validation": 2, "final_lockbox": 2},
    "M15": {"optimizer_train": 5, "tuning_validation": 2, "final_lockbox": 2},
    "M30": {"optimizer_train": 6, "tuning_validation": 2, "final_lockbox": 2},
    "H1": {"optimizer_train": 7, "tuning_validation": 2, "final_lockbox": 2},
    "H4": {"optimizer_train": 8, "tuning_validation": 2, "final_lockbox": 2},
    "D1": {"optimizer_train": 12, "tuning_validation": 2, "final_lockbox": 2},
}

# Diagnostic planning baselines for instruments with comparatively short
# regular trading sessions.  Calendar duration and broker-session coverage are
# retained alongside them, but none of the three is a search-eligibility veto;
# authenticated horizon, dependence, trade-power, and multiplicity evidence is
# the statistical authority.
DEFAULT_MIN_NATIVE_BARS: Dict[str, Dict[str, int]] = {
    # A standard 6.5-hour equity session contributes about 98,280 M5 and
    # 32,760 M15 bars over five 252-session years (39,312/13,104 over two).
    # Floors retain a modest holiday/outage buffer without privileging 24x5 FX.
    "M5": {
        "optimizer_train": 90_000,
        "tuning_validation": 36_000,
        "final_lockbox": 36_000,
    },
    "M15": {
        "optimizer_train": 30_000,
        "tuning_validation": 12_000,
        "final_lockbox": 12_000,
    },
    "M30": {
        "optimizer_train": 15_000,
        "tuning_validation": 6_500,
        "final_lockbox": 6_500,
    },
    "H1": {
        "optimizer_train": 7_500,
        "tuning_validation": 3_500,
        "final_lockbox": 3_500,
    },
    "H4": {
        "optimizer_train": 2_500,
        "tuning_validation": 1_000,
        "final_lockbox": 1_000,
    },
    "D1": {"optimizer_train": 1_260, "tuning_validation": 500, "final_lockbox": 500},
}

TIMEFRAME_DURATION = {
    "M5": pd.Timedelta(minutes=5),
    "M15": pd.Timedelta(minutes=15),
    "M30": pd.Timedelta(minutes=30),
    "H1": pd.Timedelta(hours=1),
    "H4": pd.Timedelta(hours=4),
    "D1": pd.Timedelta(days=1),
}


def _utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def ensure_utc_index(frame: pd.DataFrame, *, copy: bool = True) -> pd.DataFrame:
    """Return a timestamp-sorted frame whose index is explicitly UTC.

    The default preserves the historical isolation contract.  Read-only hot
    paths may pass ``copy=False``; an already valid UTC/sorted frame is then
    borrowed without duplicating its column blocks.  Any repair that could
    mutate the caller's index still detaches the frame first.
    """
    if frame is None:
        return pd.DataFrame()
    result = frame.copy() if copy else frame
    index = pd.DatetimeIndex(pd.to_datetime(frame.index, errors="coerce"))
    valid = ~index.isna()
    if not bool(valid.all()):
        result = result.loc[valid].copy()
        index = index[valid]
    if index.tz is None:
        index = index.tz_localize("UTC")
    else:
        index = index.tz_convert("UTC")
    if not result.index.equals(index):
        if result is frame:
            # A shallow frame copy owns its index/manager while continuing to
            # share immutable column blocks with the read-only caller.
            result = result.copy(deep=False)
        result.index = index
    if not result.index.is_monotonic_increasing:
        result = result.sort_index()
    return result


def latest_closed_bar_end(
    frame: pd.DataFrame,
    timeframe: str,
    *,
    not_after_utc: Any,
) -> Optional[pd.Timestamp]:
    """Return the latest nominal bar close at or before an information cutoff."""
    tf = str(timeframe or "").strip().upper()
    if tf not in TIMEFRAME_DURATION or frame is None or frame.empty:
        return None
    cutoff = _utc_timestamp(not_after_utc)
    ordered = ensure_utc_index(frame, copy=False)
    index = ordered.index
    if "_BAR_CLOSE_TS" in ordered.columns:
        closes = pd.DatetimeIndex(
            pd.to_datetime(ordered["_BAR_CLOSE_TS"], utc=True, errors="coerce")
        )
        closes = closes.where(closes > index)
    else:
        closes = index + TIMEFRAME_DURATION[tf]
    eligible = closes[closes <= cutoff]
    return pd.Timestamp(eligible.max()) if len(eligible) else None


def trim_to_closed_bars(
    frame: pd.DataFrame,
    timeframe: str,
    *,
    as_of_utc: Any,
    copy: bool = True,
) -> pd.DataFrame:
    """Exclude every bar whose nominal close falls after the shared cutoff."""
    tf = str(timeframe or "").strip().upper()
    ordered = ensure_utc_index(frame, copy=False)
    if tf not in TIMEFRAME_DURATION or ordered.empty:
        empty = ordered.iloc[0:0]
        return empty.copy() if copy else empty
    cutoff = _utc_timestamp(as_of_utc)
    if "_BAR_CLOSE_TS" in ordered.columns:
        closes = pd.DatetimeIndex(
            pd.to_datetime(ordered["_BAR_CLOSE_TS"], utc=True, errors="coerce")
        )
        valid_close = (closes > ordered.index) & (closes <= cutoff)
    else:
        valid_close = (ordered.index + TIMEFRAME_DURATION[tf]) <= cutoff
    if bool(valid_close.all()):
        return ordered.copy() if copy else ordered
    valid_count = int(valid_close.sum())
    if valid_count and bool(valid_close[:valid_count].all()):
        selected = ordered.iloc[:valid_count]
    else:
        selected = ordered.loc[valid_close]
    return selected.copy() if copy else selected


def derive_purge_duration(
    *,
    configured_days: Any,
    split_embargo_bars: Any,
) -> pd.Timedelta:
    """Derive one common purge from calendar policy and the legacy M5 embargo."""
    try:
        days = max(0.0, float(configured_days))
    except (TypeError, ValueError, OverflowError):
        days = 7.0
    try:
        m5_bars = max(0, int(split_embargo_bars))
    except (TypeError, ValueError, OverflowError):
        m5_bars = 0
    return max(pd.Timedelta(days=days), pd.Timedelta(minutes=5 * m5_bars))


def normalize_window_years(value: Any) -> Dict[str, int]:
    """Return a complete, positive partition-year mapping.

    Missing, non-mapping, boolean, or non-positive values revert to the safe
    production default for that partition.
    """
    source = value if isinstance(value, Mapping) else {}
    normalized: Dict[str, int] = {}
    for partition in PARTITIONS:
        raw = source.get(partition, DEFAULT_WINDOW_YEARS[partition])
        try:
            years = int(raw)
        except (TypeError, ValueError, OverflowError):
            years = DEFAULT_WINDOW_YEARS[partition]
        if isinstance(raw, bool) or years <= 0:
            years = DEFAULT_WINDOW_YEARS[partition]
        normalized[partition] = years
    return normalized


def normalize_timeframe_window_years(value: Any) -> Dict[str, Dict[str, int]]:
    """Return complete per-timeframe acquisition-planning windows."""
    source = value if isinstance(value, Mapping) else {}
    normalized: Dict[str, Dict[str, int]] = {}
    for timeframe in SUPPORTED_TIMEFRAMES:
        tf_source = source.get(timeframe, {})
        if not isinstance(tf_source, Mapping):
            tf_source = {}
        normalized[timeframe] = {}
        for partition in PARTITIONS:
            safe = DEFAULT_TIMEFRAME_WINDOW_YEARS[timeframe][partition]
            if partition != "optimizer_train":
                # Tuning and final are the common active/comparison clock.
                # They cannot drift by timeframe; a future institution-wide
                # change requires a new contract, not six local overrides.
                normalized[timeframe][partition] = safe
                continue
            raw = tf_source.get(partition, safe)
            try:
                years = int(raw)
            except (TypeError, ValueError, OverflowError):
                years = safe
            if isinstance(raw, bool) or years <= 0:
                years = safe
            # A production config may acquire more history, but cannot silently
            # weaken the planning prior without a new contract version.
            normalized[timeframe][partition] = max(safe, years)
    return normalized


def normalize_min_native_bars(value: Any) -> Dict[str, Dict[str, int]]:
    """Return the complete timeframe/partition diagnostic baseline matrix."""
    source = value if isinstance(value, Mapping) else {}
    normalized: Dict[str, Dict[str, int]] = {}
    for timeframe in SUPPORTED_TIMEFRAMES:
        tf_source = source.get(timeframe, {})
        if not isinstance(tf_source, Mapping):
            tf_source = {}
        normalized[timeframe] = {}
        for partition in PARTITIONS:
            safe_default = DEFAULT_MIN_NATIVE_BARS[timeframe][partition]
            raw = tf_source.get(partition, safe_default)
            try:
                floor = int(raw)
            except (TypeError, ValueError, OverflowError):
                floor = safe_default
            if isinstance(raw, bool) or floor <= 0:
                floor = safe_default
            # A malformed config may tighten a floor, but may not silently
            # weaken the institutional default.
            normalized[timeframe][partition] = max(safe_default, floor)
    return normalized


@dataclass(frozen=True)
class CalendarBoundaries:
    """Common half-open calendar intervals shared by every timeframe."""

    as_of_utc: pd.Timestamp
    windows: Dict[str, Tuple[pd.Timestamp, pd.Timestamp]]
    embargo: pd.Timedelta
    contract_version: str = EVIDENCE_CONTRACT_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "as_of_utc": self.as_of_utc.isoformat(),
            "embargo_seconds": float(self.embargo.total_seconds()),
            "windows": {
                name: {"start_ts": start.isoformat(), "end_ts": end.isoformat()}
                for name, (start, end) in self.windows.items()
            },
        }


def build_calendar_boundaries(
    as_of_utc: Any,
    *,
    window_years: Optional[Mapping[str, Any]] = None,
    embargo: Any = pd.Timedelta(0),
) -> CalendarBoundaries:
    """Build train -> tuning -> lockbox windows from one UTC information cutoff."""
    as_of = _utc_timestamp(as_of_utc)
    years = normalize_window_years(window_years)
    try:
        purge = pd.Timedelta(embargo)
    except (TypeError, ValueError, OverflowError):
        purge = pd.Timedelta(0)
    if purge < pd.Timedelta(0):
        purge = pd.Timedelta(0)

    lockbox_end = as_of
    lockbox_start = lockbox_end - pd.DateOffset(years=years["final_lockbox"])
    tuning_end = lockbox_start - purge
    tuning_start = tuning_end - pd.DateOffset(years=years["tuning_validation"])
    train_end = tuning_start - purge
    train_start = train_end - pd.DateOffset(years=years["optimizer_train"])
    return CalendarBoundaries(
        as_of_utc=as_of,
        embargo=purge,
        windows={
            "optimizer_train": (train_start, train_end),
            "tuning_validation": (tuning_start, tuning_end),
            "final_lockbox": (lockbox_start, lockbox_end),
        },
    )


def build_timeframe_calendar_boundaries(
    as_of_utc: Any,
    timeframe: str,
    *,
    timeframe_window_years: Optional[Mapping[str, Mapping[str, Any]]] = None,
    embargo: Any = pd.Timedelta(0),
) -> CalendarBoundaries:
    """Build the v3 chronological windows for one independently sourced TF."""
    tf = str(timeframe or "").strip().upper()
    if tf not in SUPPORTED_TIMEFRAMES:
        raise ValueError(f"unsupported timeframe: {timeframe!r}")
    matrix = normalize_timeframe_window_years(timeframe_window_years)
    boundaries = build_calendar_boundaries(
        as_of_utc,
        window_years=matrix[tf],
        embargo=embargo,
    )
    return CalendarBoundaries(
        as_of_utc=boundaries.as_of_utc,
        windows=boundaries.windows,
        embargo=boundaries.embargo,
        contract_version=TIMEFRAME_EVIDENCE_CONTRACT_VERSION,
    )


def earliest_timeframe_evidence_start(
    as_of_utc: Any,
    *,
    timeframes: Sequence[str] = SUPPORTED_TIMEFRAMES,
    timeframe_window_years: Optional[Mapping[str, Mapping[str, Any]]] = None,
    embargo: Any = pd.Timedelta(0),
) -> pd.Timestamp:
    """Return the earliest train start required by a multi-timeframe run."""
    starts = [
        build_timeframe_calendar_boundaries(
            as_of_utc,
            timeframe,
            timeframe_window_years=timeframe_window_years,
            embargo=embargo,
        ).windows["optimizer_train"][0]
        for timeframe in timeframes
    ]
    if not starts:
        raise ValueError("at least one timeframe is required")
    return min(starts)


def calendar_window_meta(
    frame: pd.DataFrame,
    start_ts: Any,
    end_ts: Any,
) -> Dict[str, Any]:
    """Describe one common half-open calendar partition on ``frame``.

    ``start_ts`` and ``end_ts`` remain the configured calendar boundaries even
    when a market is closed at either endpoint.  The first/last observed bar
    timestamps are recorded separately.  This distinction is important: using
    the first observed Monday as a partition boundary would silently give a
    daily market a different split contract from a continuously traded market.
    """
    # This function only searches the index and reads the two boundary rows.
    # Borrow an already-normalized frame so callers can describe several
    # partitions of a large native history without duplicating every column
    # block once per partition.  ``ensure_utc_index`` still detaches when an
    # index repair or sort is actually required.
    ordered = ensure_utc_index(frame, copy=False)
    start = _utc_timestamp(start_ts)
    end = _utc_timestamp(end_ts)
    if end < start:
        raise ValueError("calendar partition end precedes its start")

    index = ordered.index
    start_index = int(index.searchsorted(start, side="left"))
    end_index = int(index.searchsorted(end, side="left"))
    partition = ordered.iloc[start_index:end_index]
    observed_start = partition.index[0] if len(partition) else None
    observed_end = partition.index[-1] if len(partition) else None
    return {
        "start_index": start_index,
        "end_index": end_index,
        "rows": int(max(0, end_index - start_index)),
        # Compatibility fields consumed by the pipeline slicer. They are
        # deliberately the shared calendar boundaries, not observed labels.
        "start_ts": start.isoformat(),
        "end_ts": end.isoformat(),
        "end_ts_exclusive": end.isoformat(),
        "end_ts_inclusive": observed_end.isoformat()
        if observed_end is not None
        else None,
        "data_start_ts": observed_start.isoformat()
        if observed_start is not None
        else None,
        "data_end_ts": observed_end.isoformat() if observed_end is not None else None,
    }


def _source_bounds(
    frame: Optional[pd.DataFrame],
) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    if frame is None or frame.empty:
        return None, None
    if "_SOURCE_FIRST_TS" in frame.columns:
        first = pd.to_datetime(frame["_SOURCE_FIRST_TS"], errors="coerce").dropna()
    else:
        first = pd.Series(pd.DatetimeIndex(frame.index), index=frame.index)
    if "_SOURCE_LAST_TS" in frame.columns:
        last = pd.to_datetime(frame["_SOURCE_LAST_TS"], errors="coerce").dropna()
    else:
        last = pd.Series(pd.DatetimeIndex(frame.index), index=frame.index)
    if first.empty or last.empty:
        return None, None
    return _utc_timestamp(first.min()), _utc_timestamp(last.max())


def _coverage_for_partition(
    source_meta: Mapping[str, Any], partition: str
) -> Optional[float]:
    values = source_meta.get("expected_session_coverage_by_partition")
    if isinstance(values, Mapping):
        raw = values.get(partition)
    else:
        # A whole-file average can conceal a hole isolated to the final
        # lockbox. Production evidence requires a partition-specific value.
        raw = None
    try:
        coverage = float(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if not 0.0 <= coverage <= 1.0:
        return None
    return coverage


@dataclass
class EvidenceVerdict:
    """Timeframe source-integrity verdict plus non-binding planning diagnostics.

    ``passed`` answers only whether the supplied native evidence is authentic
    and operationally self-consistent enough to enter a statistical search.
    It deliberately does *not* claim that a fixed row count, calendar span, or
    broker-session coverage percentage is enough to support a trading edge.
    Those quantities are retained in ``diagnostic_reason_codes`` so the
    authenticated horizon, dependence, trade-power, and multiplicity layers
    can make the actual search-sufficiency decision without losing context.
    """

    timeframe: str
    passed: bool
    reason_codes: list[str] = field(default_factory=list)
    diagnostic_reason_codes: list[str] = field(default_factory=list)
    source_kind: str = "unknown"
    source_sha256: Optional[str] = None
    partition_rows: Dict[str, int] = field(default_factory=dict)
    partition_span_days: Dict[str, float] = field(default_factory=dict)
    required_rows: Dict[str, int] = field(default_factory=dict)
    required_years: Dict[str, int] = field(default_factory=dict)
    expected_session_coverage: Dict[str, Optional[float]] = field(default_factory=dict)
    search_sufficiency_authority: str = (
        "DEFERRED_TO_AUTHENTICATED_HORIZON_ESS_TRADE_POWER_MULTIPLICITY"
    )
    contract_version: str = EVIDENCE_CONTRACT_VERSION

    def concise_reason(self) -> str:
        if self.reason_codes:
            joined = ";".join(self.reason_codes)
        elif self.diagnostic_reason_codes:
            joined = "PASS_WITH_DIAGNOSTICS:" + ";".join(self.diagnostic_reason_codes)
        else:
            joined = "PASS"
        return f"EVIDENCE:{self.timeframe}:{joined}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "timeframe": self.timeframe,
            "passed": bool(self.passed),
            "reason_codes": list(self.reason_codes),
            "diagnostic_reason_codes": list(self.diagnostic_reason_codes),
            "source_kind": self.source_kind,
            "source_sha256": self.source_sha256,
            "partition_rows": dict(self.partition_rows),
            "partition_span_days": dict(self.partition_span_days),
            "required_rows": dict(self.required_rows),
            "required_years": dict(self.required_years),
            "expected_session_coverage": dict(self.expected_session_coverage),
            "search_sufficiency_authority": self.search_sufficiency_authority,
        }


def validate_timeframe_evidence(
    timeframe: str,
    partition_frames: Mapping[str, Optional[pd.DataFrame]],
    *,
    source_meta: Optional[Mapping[str, Any]] = None,
    window_years: Optional[Mapping[str, Any]] = None,
    timeframe_window_years: Optional[Mapping[str, Mapping[str, Any]]] = None,
    min_native_bars: Optional[Mapping[str, Mapping[str, Any]]] = None,
    partition_bounds: Optional[Mapping[str, Tuple[Any, Any]]] = None,
    require_native: bool = True,
    require_provenance: bool = True,
    require_session_coverage: bool = True,
    min_expected_session_coverage: float = 0.95,
    boundary_tolerance_days: float = 7.0,
) -> EvidenceVerdict:
    """Validate native source integrity and report search-planning diagnostics.

    Fixed bar counts, calendar durations, boundary reach, and estimated broker
    session coverage are not interchangeable statistical experiments across
    symbols or native timeframes.  They are therefore diagnostics only.  The
    later authenticated horizon/ESS/trade-power/multiplicity contract decides
    whether an experiment is statistically supportable.  Source-kind,
    provenance, malformed boundaries, and declared/observed row mismatches
    remain blocking because they indicate technical or authenticity failure.
    """
    tf = str(timeframe or "").strip().upper()
    meta: Mapping[str, Any] = source_meta if isinstance(source_meta, Mapping) else {}
    source_kind = str(meta.get("source_kind", "unknown") or "unknown").strip().lower()
    years = (
        normalize_timeframe_window_years(timeframe_window_years).get(tf, {})
        if timeframe_window_years is not None
        else normalize_window_years(window_years)
    )
    floors = normalize_min_native_bars(min_native_bars)
    reasons: list[str] = []
    diagnostics: list[str] = []

    if tf not in SUPPORTED_TIMEFRAMES:
        return EvidenceVerdict(
            timeframe=tf or "UNKNOWN",
            passed=False,
            reason_codes=["UNSUPPORTED_TIMEFRAME"],
            source_kind=source_kind,
        )

    if require_native and source_kind != NATIVE_SOURCE_KIND:
        reasons.append(f"NATIVE_SOURCE_REQUIRED:{source_kind}")
    if require_provenance and not bool(meta.get("provenance_valid", False)):
        details = ",".join(
            str(value) for value in (meta.get("provenance_errors") or [])
        )
        reasons.append(f"NATIVE_PROVENANCE_INVALID:{details or 'missing'}")

    try:
        minimum_coverage = float(min_expected_session_coverage)
    except (TypeError, ValueError, OverflowError):
        minimum_coverage = 0.95
    if not 0.0 <= minimum_coverage <= 1.0:
        minimum_coverage = 0.95
    try:
        tolerance_days = max(0.0, float(boundary_tolerance_days))
    except (TypeError, ValueError, OverflowError):
        tolerance_days = 7.0

    partition_rows: Dict[str, int] = {}
    partition_span_days: Dict[str, float] = {}
    coverage_values: Dict[str, Optional[float]] = {}
    required_rows = dict(floors[tf])
    expected_counts = meta.get("expected_session_bars_by_partition")
    observed_counts = meta.get("observed_session_bars_by_partition")

    for partition in PARTITIONS:
        frame = partition_frames.get(partition)
        rows = 0 if frame is None else int(len(frame))
        partition_rows[partition] = rows
        expected_count: Optional[int] = None
        if isinstance(expected_counts, Mapping):
            try:
                candidate_expected = int(expected_counts.get(partition, 0) or 0)
                expected_count = candidate_expected if candidate_expected > 0 else None
            except (TypeError, ValueError, OverflowError):
                expected_count = None
        # Same-series schedule evidence may strengthen a requirement, but can
        # never weaken the cross-asset floor. Otherwise a stable broker outage
        # or schedule-estimator error can certify its own missing observations.
        if expected_count is not None:
            required = max(
                int(required_rows[partition]),
                int(math.ceil(expected_count * minimum_coverage)),
            )
            required_rows[partition] = required
        else:
            required = int(required_rows[partition])
        if rows <= 0:
            # An activation that advertised an evidence-candidate partition
            # but supplied no frame is internally inconsistent.  A genuine
            # broker boundary shortfall with observations is merely diagnostic.
            reasons.append(f"PARTITION_FRAME_EMPTY:{partition}")
        elif rows < required:
            diagnostics.append(f"INSUFFICIENT_BARS:{partition}:{rows}<{required}")
        if isinstance(observed_counts, Mapping):
            try:
                declared_observed = int(observed_counts.get(partition, -1))
            except (TypeError, ValueError, OverflowError):
                declared_observed = -1
            if declared_observed != rows:
                reasons.append(
                    f"SESSION_OBSERVED_COUNT_MISMATCH:{partition}:{declared_observed}!={rows}"
                )

        first, last = _source_bounds(frame)
        if first is None or last is None:
            span_days = 0.0
        else:
            span = max(pd.Timedelta(0), last - first + TIMEFRAME_DURATION[tf])
            span_days = float(span.total_seconds() / 86_400.0)
        partition_span_days[partition] = span_days

        anchor = last if last is not None else pd.Timestamp("2000-01-01", tz="UTC")
        required_start = anchor - pd.DateOffset(years=years[partition])
        required_days = float((anchor - required_start).total_seconds() / 86_400.0)
        if span_days + tolerance_days < required_days:
            diagnostics.append(
                f"INSUFFICIENT_CALENDAR_SPAN:{partition}:{span_days:.1f}d<{required_days:.1f}d"
            )

        if isinstance(partition_bounds, Mapping) and partition in partition_bounds:
            try:
                expected_start = _utc_timestamp(partition_bounds[partition][0])
                expected_end = _utc_timestamp(partition_bounds[partition][1])
            except Exception:
                reasons.append(f"PARTITION_BOUNDARY_INVALID:{partition}")
            else:
                if first is None or first > expected_start + pd.Timedelta(
                    days=tolerance_days
                ):
                    gap = (
                        float("inf")
                        if first is None
                        else float((first - expected_start).total_seconds() / 86_400.0)
                    )
                    diagnostics.append(f"PARTITION_START_GAP:{partition}:{gap:.1f}d")
                observed_end = None if last is None else last + TIMEFRAME_DURATION[tf]
                if observed_end is None or observed_end < expected_end - pd.Timedelta(
                    days=tolerance_days
                ):
                    gap = (
                        float("inf")
                        if observed_end is None
                        else float(
                            (expected_end - observed_end).total_seconds() / 86_400.0
                        )
                    )
                    diagnostics.append(f"PARTITION_END_GAP:{partition}:{gap:.1f}d")

        coverage = _coverage_for_partition(meta, partition)
        coverage_values[partition] = coverage
        if require_session_coverage:
            if coverage is None:
                diagnostics.append(f"SESSION_COVERAGE_UNKNOWN:{partition}")
            elif coverage < minimum_coverage:
                diagnostics.append(
                    f"SESSION_COVERAGE_LOW:{partition}:{coverage:.4f}<{minimum_coverage:.4f}"
                )

    return EvidenceVerdict(
        timeframe=tf,
        passed=not reasons,
        reason_codes=reasons,
        diagnostic_reason_codes=diagnostics,
        source_kind=source_kind,
        source_sha256=str(meta.get("sha256") or "") or None,
        partition_rows=partition_rows,
        partition_span_days=partition_span_days,
        required_rows=required_rows,
        required_years=years,
        expected_session_coverage=coverage_values,
    )


def validate_native_provenance(
    sidecar: Optional[Mapping[str, Any]],
    *,
    symbol: str,
    timeframe: str,
    sha256: str,
    rows: int,
    first_ts: Any,
    last_ts: Any,
) -> Sequence[str]:
    """Return reason codes for an immutable native-bar sidecar."""
    if not isinstance(sidecar, Mapping):
        return ["SIDECAR_MISSING"]
    reasons: list[str] = []
    if str(sidecar.get("schema_version", "")) != "fxpm-native-bars-v1":
        reasons.append("SIDECAR_SCHEMA_INVALID")
    if str(sidecar.get("symbol", "")).upper() != str(symbol).upper():
        reasons.append("SIDECAR_SYMBOL_MISMATCH")
    if str(sidecar.get("timeframe", "")).upper() != str(timeframe).upper():
        reasons.append("SIDECAR_TIMEFRAME_MISMATCH")
    if str(sidecar.get("source_kind", "")).lower() != NATIVE_SOURCE_KIND:
        reasons.append("SIDECAR_SOURCE_KIND_INVALID")
    if str(sidecar.get("timezone", "")).upper() not in {"UTC", "ETC/UTC", "+00:00"}:
        reasons.append("SIDECAR_TIMEZONE_NOT_UTC")
    if str(sidecar.get("timestamp_semantics", "")).strip().lower() != "bar_open_utc":
        reasons.append("SIDECAR_TIMESTAMP_SEMANTICS_INVALID")
    if str(sidecar.get("bar_close_column", "")) != "_BAR_CLOSE_TS":
        reasons.append("SIDECAR_BAR_CLOSE_SEMANTICS_INVALID")
    if str(sidecar.get("sha256", "")).lower() != str(sha256).lower():
        reasons.append("SIDECAR_SHA256_MISMATCH")
    try:
        sidecar_rows = int(sidecar.get("rows", -1))
    except (TypeError, ValueError, OverflowError):
        sidecar_rows = -1
    if sidecar_rows != int(rows):
        reasons.append("SIDECAR_ROW_COUNT_MISMATCH")
    if not bool(sidecar.get("complete", False)):
        reasons.append("SIDECAR_INCOMPLETE")

    for key, actual in (("start_ts", first_ts), ("end_ts", last_ts)):
        try:
            declared = _utc_timestamp(sidecar.get(key))
            observed = _utc_timestamp(actual)
        except Exception:
            reasons.append(f"SIDECAR_{key.upper()}_INVALID")
            continue
        if declared != observed:
            reasons.append(f"SIDECAR_{key.upper()}_MISMATCH")
    return reasons
