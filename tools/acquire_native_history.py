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
import json
import logging
import math
import os
import re
import shutil
import socket
import sys
import time
import uuid
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


LOGGER = logging.getLogger("fxpm.native_history")
SCHEMA_VERSION = "fxpm-native-bars-v1"
ACQUISITION_CONTRACT_VERSION = "2026-07-10-native-calendar-acquisition-v1"
MANIFEST_SCHEMA_VERSION = "fxpm.native_history_run_manifest.v1"
RESUME_CONTRACT_VERSION = "fxpm.native_history_resume.v1"
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
TIMEFRAME_SECONDS: Dict[str, int] = {
    "M5": 5 * 60,
    "M15": 15 * 60,
    "M30": 30 * 60,
    "H1": 60 * 60,
    "H4": 4 * 60 * 60,
    "D1": 24 * 60 * 60,
}
DEFAULT_TIMEFRAMES: Tuple[str, ...] = tuple(TIMEFRAME_SECONDS)
SEGMENTED_SESSION_COVERAGE_METHOD_V2 = "timestamp_segmented_weekly_session_rate_v2"

# The coverage estimator deliberately reasons in complete UTC calendar weeks.
# Twenty-six weeks is long enough to distinguish a sustained schedule from a
# transient outage/holiday cluster without imposing an instrument calendar.
_SESSION_V2_MIN_EVIDENCE_WEEKS = 26
_SESSION_V2_MIN_SEGMENT_WEEKS = 26
_SESSION_V2_CHANGE_WINDOW_WEEKS = 26
_SESSION_V2_MIN_CHANGE_FRACTION = 0.10

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


class ResumeValidationError(RuntimeError):
    """Raised when an immutable run cannot be resumed without ambiguity."""


class ConcurrentWriterError(ResumeValidationError):
    """Raised when another process owns the run's operating-system lock."""


def _utc(value: datetime) -> datetime:
    """Return an aware UTC datetime, rejecting ambiguous naive inputs."""

    if not isinstance(value, datetime):
        raise TypeError("datetime value required")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("naive datetimes are not accepted; supply an explicit UTC offset")
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
    return datetime.fromtimestamp(epoch_seconds - (epoch_seconds % seconds), tz=timezone.utc)


def ceil_utc_boundary(value: datetime, timeframe_seconds: int) -> datetime:
    floored = floor_utc_boundary(value, timeframe_seconds)
    return floored if floored == _utc(value) else floored + timedelta(seconds=timeframe_seconds)


def build_common_partition_windows(
    as_of_utc: datetime,
    window_years: Mapping[str, int],
    embargo_days: float,
) -> Dict[str, Tuple[datetime, datetime]]:
    """Match FxPM's common 5y/2y/2y half-open calendar evidence windows."""

    as_of = pd.Timestamp(_utc(as_of_utc))
    embargo = pd.Timedelta(days=max(0.0, float(embargo_days)))
    lockbox_end = as_of
    lockbox_start = lockbox_end - pd.DateOffset(years=int(window_years["final_lockbox"]))
    tuning_end = lockbox_start - embargo
    tuning_start = tuning_end - pd.DateOffset(years=int(window_years["tuning_validation"]))
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
    index = pd.DatetimeIndex(pd.to_datetime(frame.index, utc=True, errors="coerce")).dropna()
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


def segmented_session_coverage_by_partition_v2(
    frame: pd.DataFrame,
    partition_windows: Mapping[str, Tuple[datetime, datetime]],
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
            "method": SEGMENTED_SESSION_COVERAGE_METHOD_V2,
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

    index = pd.DatetimeIndex(
        pd.to_datetime(frame.index, utc=True, errors="coerce")
    ).dropna()
    index = index[~index.duplicated()].sort_values()
    envelope_start = min(start for start, _ in windows.values())
    envelope_end = max(end for _, end in windows.values())
    index = index[(index >= envelope_start) & (index < envelope_end)]
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
            if best is None or score > best[0] + 1e-12 or (
                math.isclose(score, best[0], rel_tol=0.0, abs_tol=1e-12)
                and split_pos < best[1]
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
        empirical_upper = float(np.max(positive))
        nonempty_fraction = float(positive.size / max(1, counts.size))
        healthy_fraction = float(
            np.mean((counts >= 0.80 * nominal) & (counts <= 1.20 * nominal))
        )
        bound_defensible = bool(
            counts.size >= _SESSION_V2_MIN_EVIDENCE_WEEKS
            and positive.size >= 20
            and nonempty_fraction >= 0.75
            and empirical_upper <= 1.15 * nominal
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
            "upper": empirical_upper,
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
                "empirical_schedule_envelope_bars_per_week": empirical_upper,
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
        return segment_models[0] if week_start < segment_models[0]["start"] else segment_models[-1]

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
            if best is None or unexplained > best[0] or (
                math.isclose(unexplained, best[0], rel_tol=0.0, abs_tol=1e-9)
                and float(gap_seconds) > best[3]
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
        observed_index = index[(index >= start) & (index < end)]
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
        "method": SEGMENTED_SESSION_COVERAGE_METHOD_V2,
        "status": "estimated",
        "performance_inputs_used": False,
        "timestamp_only": True,
        "schedule_source": "all_requested_calendar_weeks_timestamp_only",
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


def _fsync_file(path: Path) -> None:
    # Windows requires a writable descriptor for fsync/FlushFileBuffers.
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


@dataclass(frozen=True)
class NativeHistoryRequest:
    symbols: Tuple[str, ...]
    timeframes: Tuple[str, ...] = DEFAULT_TIMEFRAMES
    horizon_years: int = 10
    as_of_utc: Optional[datetime] = None
    retries: int = 3
    retry_sleep_seconds: float = 0.5
    boundary_tolerance_days: float = 3.0
    window_years: Optional[Mapping[str, int]] = None
    embargo_days: float = 7.0
    warmup_days: float = 400.0
    safety_days: float = 14.0
    enforce_evidence_horizon: bool = True

    def __post_init__(self) -> None:
        normalized_symbols = tuple(_safe_name(item.upper(), "symbol") for item in self.symbols)
        normalized_timeframes = tuple(str(item).upper() for item in self.timeframes)
        if not normalized_symbols:
            raise ValueError("at least one symbol is required")
        if len(set(normalized_symbols)) != len(normalized_symbols):
            raise ValueError("duplicate symbols are not allowed")
        if not normalized_timeframes:
            raise ValueError("at least one timeframe is required")
        unknown = [item for item in normalized_timeframes if item not in TIMEFRAME_SECONDS]
        if unknown:
            raise ValueError(f"unsupported native timeframe(s): {', '.join(unknown)}")
        if len(set(normalized_timeframes)) != len(normalized_timeframes):
            raise ValueError("duplicate timeframes are not allowed")
        if int(self.horizon_years) <= 0:
            raise ValueError("horizon_years must be positive")
        if int(self.retries) <= 0:
            raise ValueError("retries must be positive")
        if float(self.retry_sleep_seconds) < 0.0:
            raise ValueError("retry_sleep_seconds cannot be negative")
        if float(self.boundary_tolerance_days) < 0.0:
            raise ValueError("boundary_tolerance_days cannot be negative")
        if float(self.embargo_days) < 0.0:
            raise ValueError("embargo_days cannot be negative")
        if float(self.warmup_days) < 0.0 or float(self.safety_days) < 0.0:
            raise ValueError("warmup_days and safety_days cannot be negative")
        source_years = self.window_years if isinstance(self.window_years, Mapping) else {}
        normalized_years: Dict[str, int] = {}
        for partition in PARTITIONS:
            raw = source_years.get(partition, DEFAULT_WINDOW_YEARS[partition])
            try:
                years = int(raw)
            except (TypeError, ValueError, OverflowError):
                years = DEFAULT_WINDOW_YEARS[partition]
            if isinstance(raw, bool) or years <= 0:
                years = DEFAULT_WINDOW_YEARS[partition]
            normalized_years[partition] = years
        object.__setattr__(self, "symbols", normalized_symbols)
        object.__setattr__(self, "timeframes", normalized_timeframes)
        object.__setattr__(self, "window_years", normalized_years)
        if self.as_of_utc is not None:
            object.__setattr__(self, "as_of_utc", _utc(self.as_of_utc))


_RESUME_REQUEST_KEYS: Tuple[str, ...] = (
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
            raise ConcurrentWriterError(f"writer owner record is missing: {self.owner_path}")
        try:
            with self.owner_path.open("r", encoding="utf-8") as handle:
                value = json.load(handle)
        except Exception as exc:
            raise ConcurrentWriterError(
                f"writer owner record is unreadable; refusing stale recovery: {self.owner_path}"
            ) from exc
        if not isinstance(value, dict) or value.get("schema_version") != self.SCHEMA_VERSION:
            raise ConcurrentWriterError(
                f"writer owner record has an unknown schema: {self.owner_path}"
            )
        return value

    def _write_owner(self, value: Mapping[str, Any]) -> None:
        tmp_path = self.runs_root / f".{self.owner_path.name}.{uuid.uuid4().hex}.staging"
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
            raise ConcurrentWriterError(f"writer lock already acquired: {self.lock_path}")
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

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release()


def _load_json_object(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except Exception as exc:
        raise ResumeValidationError(f"cannot read resume provenance {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ResumeValidationError(f"resume provenance is not a JSON object: {path}")
    return value


def _resume_request_contract(record: Mapping[str, Any], path: Path) -> Dict[str, Any]:
    request = record.get("request")
    if not isinstance(request, Mapping):
        raise ResumeValidationError(f"resume provenance has no request object: {path}")
    missing = [key for key in _RESUME_REQUEST_KEYS if key not in request]
    if missing:
        raise ResumeValidationError(
            f"resume provenance lacks immutable request fields {missing}: {path}"
        )
    return {key: _json_safe(request[key]) for key in _RESUME_REQUEST_KEYS}


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


def _is_staging_name(path: Path) -> bool:
    return path.name.startswith(".") and path.name.endswith(".staging")


def _discover_native_leaves(run_root: Path) -> Dict[Tuple[str, str], Path]:
    """Discover only final two-level leaves, rejecting ambiguous run contents."""

    leaves: Dict[Tuple[str, str], Path] = {}
    for symbol_entry in sorted(run_root.iterdir(), key=lambda item: item.name):
        if _is_staging_name(symbol_entry):
            continue
        if symbol_entry.name == "run_manifest.json":
            continue
        if not symbol_entry.is_dir() or symbol_entry.is_symlink():
            raise ResumeValidationError(f"unexpected entry in resumable run: {symbol_entry}")
        symbol = _safe_name(symbol_entry.name, "symbol")
        for timeframe_entry in sorted(symbol_entry.iterdir(), key=lambda item: item.name):
            if _is_staging_name(timeframe_entry):
                continue
            if not timeframe_entry.is_dir() or timeframe_entry.is_symlink():
                raise ResumeValidationError(
                    f"unexpected entry in resumable symbol directory: {timeframe_entry}"
                )
            timeframe = _safe_name(timeframe_entry.name, "timeframe").upper()
            if timeframe not in TIMEFRAME_SECONDS:
                raise ResumeValidationError(
                    f"unsupported timeframe leaf in resumable run: {timeframe_entry}"
                )
            key = (symbol.upper(), timeframe)
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
    leaves = _discover_native_leaves(run_root)
    if not leaves:
        raise ResumeValidationError(
            "cannot recover the original cutoff from an empty run; start a new run "
            "or retain at least one provenance leaf"
        )

    common_request: Optional[Dict[str, Any]] = None
    common_identity: Optional[Dict[str, Any]] = None
    for (symbol, timeframe), leaf in leaves.items():
        sidecar = leaf / "provenance.json"
        record = _load_json_object(sidecar)
        if record.get("schema_version") != SCHEMA_VERSION:
            raise ResumeValidationError(f"schema mismatch in {sidecar}")
        if record.get("acquisition_contract_version") != ACQUISITION_CONTRACT_VERSION:
            raise ResumeValidationError(f"acquisition contract mismatch in {sidecar}")
        if record.get("run_id") != safe_run_id:
            raise ResumeValidationError(f"run_id mismatch in {sidecar}")
        if str(record.get("canonical_symbol", record.get("symbol", ""))).upper() != symbol:
            raise ResumeValidationError(f"symbol mismatch in {sidecar}")
        if str(record.get("timeframe", "")).upper() != timeframe:
            raise ResumeValidationError(f"timeframe mismatch in {sidecar}")
        request = _resume_request_contract(record, sidecar)
        identity = _source_identity(record.get("source_environment"))
        if not identity:
            raise ResumeValidationError(f"source identity is absent in {sidecar}")
        if common_request is None:
            common_request = request
            common_identity = identity
        elif request != common_request:
            raise ResumeValidationError(f"request contract differs across leaves: {sidecar}")
        elif identity != common_identity:
            raise ResumeValidationError(f"source identity differs across leaves: {sidecar}")

    assert common_request is not None and common_identity is not None
    return NativeHistoryResumeContract(
        run_id=safe_run_id,
        request=common_request,
        source_identity=common_identity,
        existing_cells=tuple(sorted(leaves)),
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
        self._csv_writer = csv_writer or self._default_csv_writer
        self._replace_fn = replace_fn
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
            if self.resume:
                if not self.run_root.is_dir() or self.run_root.is_symlink():
                    raise ResumeValidationError(
                        f"resumable run does not exist: {self.run_root}"
                    )
                if (self.run_root / "run_manifest.json").exists():
                    raise ResumeValidationError(
                        f"run already has an immutable final manifest: {self.run_root}"
                    )
                self.stale_staging_removed = self._remove_stale_staging()
                self.resume_contract = load_native_history_resume_contract(
                    self.native_root,
                    self.run_id,
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

    def _remove_stale_staging(self, min_age_seconds: float = 3600.0) -> int:
        """Remove only old dot-prefixed staging children contained by this run.

        Recent staging paths may belong to another live writer and are ignored.
        Data paging happens before staging is created, so a one-hour threshold is
        deliberately conservative for the small atomic publish operation.
        """

        run_root = self.run_root.resolve(strict=True)
        now_epoch = time.time()
        candidates = sorted(
            (
                path
                for path in run_root.rglob("*")
                if _is_staging_name(path)
                and now_epoch - path.lstat().st_mtime >= float(min_age_seconds)
            ),
            key=lambda path: len(path.parts),
            reverse=True,
        )
        removed = 0
        for path in candidates:
            try:
                path.resolve(strict=False).relative_to(run_root)
            except ValueError as exc:
                raise ResumeValidationError(
                    f"refusing to remove staging path outside exact run: {path}"
                ) from exc
            if path.is_symlink() or path.is_file():
                path.unlink(missing_ok=True)
            elif path.is_dir():
                shutil.rmtree(path)
            else:
                continue
            removed += 1
        return removed

    @staticmethod
    def _default_csv_writer(frame: pd.DataFrame, path: Path) -> None:
        frame.to_csv(path, index_label="time", lineterminator="\n")

    def leaf_path(self, symbol: str, timeframe: str) -> Path:
        return self.run_root / _safe_name(symbol, "symbol") / _safe_name(timeframe, "timeframe")

    @staticmethod
    def _retryable_publish_error(exc: BaseException) -> bool:
        return isinstance(exc, PermissionError) or getattr(exc, "winerror", None) in {
            5,
            32,
            33,
        }

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
            raise ResumeValidationError(f"concurrent final leaf is not a directory: {leaf}")
        entries = {item.name for item in leaf.iterdir() if not _is_staging_name(item)}
        if "provenance.json" not in entries or not entries.issubset(
            {"bars.csv", "provenance.json"}
        ):
            raise ResumeValidationError(f"concurrent final leaf has unexpected contents: {leaf}")
        sidecar = leaf / "provenance.json"
        if sidecar.is_symlink():
            raise ResumeValidationError(f"concurrent provenance cannot be a symlink: {sidecar}")
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
        for key, required_value in required.items():
            observed = record.get(key)
            if key in {"canonical_symbol", "timeframe"}:
                observed = str(observed or "").upper()
                required_value = str(required_value).upper()
            if observed != required_value:
                raise ResumeValidationError(
                    f"concurrent final leaf {key} mismatch in {sidecar}"
                )
        if record.get("immutable_record") is not True:
            raise ResumeValidationError(f"concurrent final leaf is not immutable: {sidecar}")
        expected_request = expected.get("request")
        if isinstance(expected_request, Mapping):
            if _resume_request_contract(record, sidecar) != {
                key: _json_safe(expected_request[key]) for key in _RESUME_REQUEST_KEYS
            }:
                raise ResumeValidationError(
                    f"concurrent final leaf request differs from this writer: {sidecar}"
                )
        expected_source = _source_identity(expected.get("source_environment"))
        if expected_source and _source_identity(record.get("source_environment")) != expected_source:
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
            if csv_path.exists() or record.get("status") != "failed":
                raise ResumeValidationError(
                    f"concurrent provenance-only leaf is internally inconsistent: {leaf}"
                )
            if int(record.get("rows", -1)) != 0 or record.get("sha256") is not None:
                raise ResumeValidationError(
                    f"concurrent provenance-only leaf has invalid artifact metadata: {leaf}"
                )
            return record
        if not isinstance(artifact, Mapping) or artifact.get("relative_path") != "bars.csv":
            raise ResumeValidationError(f"concurrent artifact metadata is invalid: {sidecar}")
        if not csv_path.is_file() or csv_path.is_symlink():
            raise ResumeValidationError(f"concurrent CSV is absent or unsafe: {csv_path}")
        digest = _sha256(csv_path)
        rows = _count_csv_rows(csv_path)
        size = int(csv_path.stat().st_size)
        if (
            artifact.get("sha256") != digest
            or record.get("sha256") != digest
            or int(artifact.get("rows", -1)) != rows
            or int(record.get("rows", -1)) != rows
            or int(artifact.get("bytes", -1)) != size
            or record.get("status") not in {"success", "partial"}
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
                self._replace_fn(staging, final_dir)
                return dict(expected)
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
        frame: Optional[pd.DataFrame],
    ) -> Dict[str, Any]:
        """Atomically publish one symbol/timeframe leaf; never overwrite it."""

        self._assert_writer_lock()
        final_dir = self.leaf_path(symbol, timeframe)
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        record = dict(_json_safe(provenance))
        record.setdefault("timestamp_semantics", "bar_open_utc")
        record.setdefault("bar_interval_semantics", "half_open_[bar_open,bar_close)")
        record.setdefault("bar_close_column", "_BAR_CLOSE_TS")
        if final_dir.exists():
            return self._validate_existing_immutable_leaf(
                symbol,
                timeframe,
                record,
            )
        staging = final_dir.parent / f".{timeframe}.{uuid.uuid4().hex}.staging"
        staging.mkdir(parents=False, exist_ok=False)
        try:
            if frame is not None:
                csv_path = staging / "bars.csv"
                self._csv_writer(frame, csv_path)
                _fsync_file(csv_path)
                stat = csv_path.stat()
                digest = _sha256(csv_path)
                record["artifact"] = {
                    "relative_path": "bars.csv",
                    "sha256": digest,
                    "bytes": int(stat.st_size),
                    "rows": int(len(frame)),
                }
                record.update(
                    {
                        "sha256": digest,
                        "rows": int(len(frame)),
                        "start_ts": pd.Timestamp(frame.index[0]).isoformat() if len(frame) else None,
                        "end_ts": pd.Timestamp(frame.index[-1]).isoformat() if len(frame) else None,
                        "complete": bool(record.get("status") == "success"),
                    }
                )
            else:
                record["artifact"] = None
                record.update(
                    {
                        "sha256": None,
                        "rows": 0,
                        "start_ts": None,
                        "end_ts": None,
                        "complete": False,
                    }
                )
            sidecar = staging / "provenance.json"
            with sidecar.open("x", encoding="utf-8", newline="\n") as handle:
                json.dump(record, handle, indent=2, sort_keys=True, ensure_ascii=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            return self._publish_leaf_directory(
                staging,
                final_dir,
                symbol,
                timeframe,
                record,
            )
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    def commit_write_failure(
        self,
        symbol: str,
        timeframe: str,
        provenance: Mapping[str, Any],
        exc: BaseException,
    ) -> Dict[str, Any]:
        """Publish a provenance-only failure after a data artifact write fails."""

        self._assert_writer_lock()
        final_dir = self.leaf_path(symbol, timeframe)
        if final_dir.exists():
            try:
                return self._validate_existing_immutable_leaf(
                    symbol,
                    timeframe,
                    provenance,
                    allow_status_mismatch=True,
                )
            except Exception as validation_exc:
                raise exc from validation_exc
        failure = dict(provenance)
        failure.update(
            {
                "status": "failed",
                "failure_status": "write_failed",
                "eligible_for_optimization": False,
                "eligible_for_downstream_validation": False,
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        )
        try:
            return self.commit_leaf(symbol, timeframe, failure, None)
        except ResumeValidationError as collision_exc:
            if final_dir.exists():
                try:
                    return self._validate_existing_immutable_leaf(
                        symbol,
                        timeframe,
                        provenance,
                        allow_status_mismatch=True,
                    )
                except Exception as validation_exc:
                    raise exc from validation_exc
            raise exc from collision_exc

    def commit_manifest(self, manifest: Mapping[str, Any]) -> Path:
        self._assert_writer_lock()
        final_path = self.run_root / "run_manifest.json"
        if final_path.exists():
            raise FileExistsError(f"immutable run manifest already exists: {final_path}")
        tmp_path = self.run_root / f".run_manifest.{uuid.uuid4().hex}.staging"
        try:
            with tmp_path.open("x", encoding="utf-8", newline="\n") as handle:
                json.dump(_json_safe(manifest), handle, indent=2, sort_keys=True, ensure_ascii=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            attempts = len(self._publish_retry_delays) + 1
            for attempt in range(attempts):
                try:
                    self._replace_fn(tmp_path, final_path)
                    break
                except Exception as exc:
                    if final_path.exists():
                        raise FileExistsError(
                            f"immutable run manifest appeared concurrently: {final_path}"
                        ) from exc
                    if not self._retryable_publish_error(exc) or attempt >= attempts - 1:
                        raise
                    self._sleep_fn(self._publish_retry_delays[attempt])
            return final_path
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
    ) -> None:
        self.connector = connector
        self.store = store
        self.request = request
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
        self.partition_windows = build_common_partition_windows(
            self.as_of_utc,
            request.window_years or DEFAULT_WINDOW_YEARS,
            request.embargo_days,
        )
        requested_start = subtract_calendar_years(self.as_of_utc, request.horizon_years)
        if request.enforce_evidence_horizon:
            train_start = self.partition_windows["optimizer_train"][0]
            closed_bar_slack = max(TIMEFRAME_SECONDS[tf] for tf in request.timeframes)
            evidence_lead_start = train_start - timedelta(
                days=float(request.warmup_days) + float(request.safety_days),
                seconds=closed_bar_slack,
            )
            self.common_start_utc = min(requested_start, evidence_lead_start)
        else:
            evidence_lead_start = None
            self.common_start_utc = requested_start
        self.requested_horizon_start_utc = requested_start
        self.required_evidence_lead_start_utc = evidence_lead_start
        self.runtime_metadata = dict(runtime_metadata or self._read_runtime_metadata())
        self.terminal_max_bars = self._extract_terminal_max_bars(self.runtime_metadata)
        self.resume_contract = store.resume_contract
        if store.resume:
            if self.resume_contract is None:
                raise ResumeValidationError("resume store has no recovered immutable contract")
            expected_request = self._common_request_contract()
            if expected_request != dict(self.resume_contract.request):
                raise ResumeValidationError(
                    "resume request does not exactly match the original cutoff, calendar "
                    "boundaries, horizon, warmup, safety, and timeframe closure contract"
                )
            current_identity = _source_identity(self.runtime_metadata)
            if current_identity != dict(self.resume_contract.source_identity):
                raise ResumeValidationError(
                    "connected account/terminal source identity does not match the original run"
                )

    def _read_runtime_metadata(self) -> Dict[str, Any]:
        return snapshot_read_only_runtime_metadata(self.connector)

    def _common_request_contract(self) -> Dict[str, Any]:
        request = {
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
        return {key: _json_safe(request[key]) for key in _RESUME_REQUEST_KEYS}

    def _validate_reusable_leaf(
        self,
        symbol: str,
        timeframe: str,
        leaf: Path,
    ) -> Dict[str, Any]:
        """Fully validate an existing immutable leaf before any broker fetch."""

        allowed = {"bars.csv", "provenance.json"}
        entries = {item.name for item in leaf.iterdir() if not _is_staging_name(item)}
        if entries != allowed:
            raise ResumeValidationError(
                f"reusable leaf must contain exactly bars.csv and provenance.json: {leaf}"
            )
        sidecar = leaf / "provenance.json"
        csv_path = leaf / "bars.csv"
        if csv_path.is_symlink() or sidecar.is_symlink():
            raise ResumeValidationError(f"symlinks are not reusable acquisition artifacts: {leaf}")
        record = _load_json_object(sidecar)
        expected_scalars = {
            "schema_version": SCHEMA_VERSION,
            "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
            "run_id": self.store.run_id,
            "canonical_symbol": symbol,
            "timeframe": timeframe,
            "source_kind": "broker_native",
            "timezone": "UTC",
        }
        for key, expected in expected_scalars.items():
            observed = record.get(key)
            if key in {"canonical_symbol", "timeframe"}:
                observed = str(observed or "").upper()
            if observed != expected:
                raise ResumeValidationError(
                    f"reusable provenance {key} mismatch in {sidecar}: "
                    f"expected {expected!r}, observed {observed!r}"
                )
        if record.get("immutable_record") is not True:
            raise ResumeValidationError(f"reusable provenance is not immutable: {sidecar}")
        if record.get("status") not in {"success", "partial"}:
            raise ResumeValidationError(
                f"failed/provenance-only leaves cannot be overwritten; start a new run: {sidecar}"
            )
        if _resume_request_contract(record, sidecar) != self._common_request_contract():
            raise ResumeValidationError(f"reusable request mismatch in {sidecar}")
        if _source_identity(record.get("source_environment")) != _source_identity(
            self.runtime_metadata
        ):
            raise ResumeValidationError(f"reusable source identity mismatch in {sidecar}")

        artifact = record.get("artifact")
        if not isinstance(artifact, Mapping) or artifact.get("relative_path") != "bars.csv":
            raise ResumeValidationError(f"reusable artifact metadata is invalid: {sidecar}")
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
                f"reusable CSV hash/row/byte metadata does not match bytes on disk: {csv_path}"
            )
        return record

    def _validated_reusable_cells(self) -> Dict[Tuple[str, str], Dict[str, Any]]:
        if not self.store.resume:
            return {}
        target = {
            (symbol, timeframe)
            for symbol in self.request.symbols
            for timeframe in self.request.timeframes
        }
        discovered = _discover_native_leaves(self.store.run_root)
        extras = sorted(set(discovered) - target)
        if extras:
            raise ResumeValidationError(
                f"existing run contains cells outside this resume target: {extras}"
            )
        # Validate every existing leaf before fetching or publishing one new cell.
        return {
            key: self._validate_reusable_leaf(key[0], key[1], leaf)
            for key, leaf in discovered.items()
        }

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
        value["parity_tick_tolerance"] = max(tick_candidates) if tick_candidates else None
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
    ) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        errors: List[Dict[str, Any]] = []
        for attempt in range(1, int(self.request.retries) + 1):
            try:
                frame = self.connector.get_bars_range(
                    symbol,
                    timeframe,
                    start_utc,
                    end_utc,
                    sync_retry=True,
                )
                if frame is not None and len(frame) > 0:
                    return frame, {
                        "outcome": "data",
                        "attempts": attempt,
                        "errors": errors,
                    }
                last_error = self._last_error(self.connector)
                if self._is_hard_last_error(last_error):
                    errors.append(
                        {
                            "attempt": attempt,
                            "type": "MT5LastError",
                            "message": str(last_error),
                        }
                    )
            except Exception as exc:
                errors.append(
                    {
                        "attempt": attempt,
                        "type": type(exc).__name__,
                        "message": str(exc),
                    }
                )
            if attempt < int(self.request.retries) and float(self.request.retry_sleep_seconds) > 0.0:
                self.sleep_fn(float(self.request.retry_sleep_seconds))
        return None, {
            "outcome": "error" if errors else "empty",
            "attempts": int(self.request.retries),
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
                raise ValueError("history frame has neither a DatetimeIndex nor a time column")
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
        if broker_session_aligned:
            # Broker H4/D1 opens can be 21:00/22:00 UTC and move at DST. Fetch
            # through the information cutoff and classify closure from observed
            # successor bars (or nominal close for the final/gapped bar), never
            # from Unix-epoch UTC modulo arithmetic.
            native_start = self.common_start_utc - timedelta(seconds=seconds)
            # One second beyond the cutoff is a read-only sentinel window: MT5
            # bar timestamps are second-granular, so a successor opening exactly
            # at ``as_of`` must be visible to prove the preceding DST-shifted bar
            # closed. The successor itself is removed by close<=as_of below.
            fetch_end_exclusive = self.as_of_utc + timedelta(seconds=1)
            alignment_mode = "broker_observed_successor_or_nominal_close_v1"
        else:
            native_start = ceil_utc_boundary(self.common_start_utc, seconds)
            fetch_end_exclusive = floor_utc_boundary(self.as_of_utc, seconds)
            alignment_mode = "fixed_utc_native_boundary_v1"
        if fetch_end_exclusive <= native_start:
            raise ValueError("calendar horizon contains no fully closed native bar")
        page_span = self._page_span(timeframe)
        cursor = native_start
        frames: List[pd.DataFrame] = []
        raw_rows = 0
        page_count = 0
        pages_with_data = 0
        attempts_total = 0
        empty_pages: List[Dict[str, Any]] = []
        failed_pages: List[Dict[str, Any]] = []
        truncated_pages: List[Dict[str, Any]] = []
        boundary_tolerance = timedelta(days=float(self.request.boundary_tolerance_days))
        while cursor < fetch_end_exclusive:
            page_end = min(cursor + page_span, fetch_end_exclusive)
            frame, outcome = self._fetch_page(symbol, timeframe, cursor, page_end)
            page_count += 1
            attempts_total += int(outcome["attempts"])
            page_identity = {
                "start_utc": _iso(cursor),
                "end_exclusive_utc": _iso(page_end),
                "attempts": int(outcome["attempts"]),
            }
            if outcome["outcome"] == "data" and frame is not None:
                normalized, page_raw_rows = self._normalize_page(frame, cursor, page_end)
                raw_rows += page_raw_rows
                if not normalized.empty:
                    pages_with_data += 1
                    frames.append(normalized)
                    page_first = normalized.index.min().to_pydatetime()
                    page_last = normalized.index.max().to_pydatetime()
                    expected_last = page_end - timedelta(seconds=seconds)
                    page_start_delay = max(timedelta(0), page_first - cursor)
                    page_end_lag = max(timedelta(0), expected_last - page_last)
                    if page_start_delay > boundary_tolerance or page_end_lag > boundary_tolerance:
                        truncated_pages.append(
                            {
                                **page_identity,
                                "actual_first_open_utc": _iso(page_first),
                                "actual_last_open_utc": _iso(page_last),
                                "start_delay_seconds": float(page_start_delay.total_seconds()),
                                "end_lag_seconds": float(page_end_lag.total_seconds()),
                            }
                        )
                else:
                    empty_pages.append(page_identity)
            elif outcome["outcome"] == "error":
                page_identity["errors"] = outcome.get("errors", [])
                failed_pages.append(page_identity)
            else:
                empty_pages.append(page_identity)
            cursor = page_end

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
            "request": {
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
                "last_fully_closed_bar_open_utc": None,
                "alignment_mode": alignment_mode,
                "successor_sentinel_fetch_slack_seconds": (
                    1 if broker_session_aligned else 0
                ),
                "boundary_tolerance_days": float(self.request.boundary_tolerance_days),
                "window_years": dict(self.request.window_years or {}),
                "embargo_days": float(self.request.embargo_days),
                "common_calendar_boundaries": {
                    partition: {
                        "start_ts": _iso(start),
                        "end_ts": _iso(end),
                    }
                    for partition, (start, end) in self.partition_windows.items()
                },
            },
            "source_environment": _json_safe(self.runtime_metadata),
            "instrument": _json_safe(dict(instrument_metadata or {})),
            "acquisition_started_utc": _iso(started_utc),
            "acquisition_completed_utc": _iso(_utc(self.now_fn())),
            "pagination": {
                "page_span_seconds": int(page_span.total_seconds()),
                "pages_requested": page_count,
                "pages_with_data": pages_with_data,
                "empty_pages": empty_pages,
                "failed_pages": failed_pages,
                "truncated_pages": truncated_pages,
                "attempts_total": attempts_total,
                "retries_per_page": int(self.request.retries),
            },
        }

        if failed_pages:
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
            return None, base_record
        if not frames:
            base_record.update(
                {
                    "failure_status": "no_history",
                    "error": {
                        "type": "NoHistory",
                        "message": "broker returned no bars anywhere in the requested calendar horizon",
                    },
                    "rows": {"raw_received": raw_rows, "published": 0},
                }
            )
            return None, base_record

        combined = pd.concat(frames).sort_index(kind="stable")
        pre_dedup_rows = len(combined)
        duplicate_rows = int(combined.index.duplicated(keep="last").sum())
        combined = combined[~combined.index.duplicated(keep="last")]
        if broker_session_aligned:
            index_ns = combined.index.asi8
            nominal_ns = int(seconds * 1_000_000_000)
            next_ns = np.empty_like(index_ns)
            if len(index_ns) > 1:
                next_ns[:-1] = index_ns[1:]
            next_ns[-1] = index_ns[-1] + nominal_ns
            deltas = next_ns - index_ns
            observed_regular_successor = (
                (deltas >= nominal_ns * 0.5) & (deltas <= nominal_ns * 1.5)
            )
            close_ns = np.where(
                observed_regular_successor,
                next_ns,
                index_ns + nominal_ns,
            )
            as_of_ns = pd.Timestamp(self.as_of_utc).value
            in_range = (
                (combined.index >= pd.Timestamp(self.common_start_utc))
                & (close_ns <= as_of_ns)
            )
            combined = combined.loc[in_range].copy()
            close_ns = close_ns[np.asarray(in_range)]
            combined["_SOURCE_FIRST_TS"] = combined.index
            combined["_SOURCE_LAST_TS"] = pd.to_datetime(close_ns - 1, utc=True)
            combined["_BAR_CLOSE_TS"] = pd.to_datetime(close_ns, utc=True)
        else:
            in_range = (combined.index >= pd.Timestamp(native_start)) & (
                combined.index < pd.Timestamp(fetch_end_exclusive)
            )
            combined = combined.loc[in_range].copy()
            combined["_SOURCE_FIRST_TS"] = combined.index
            combined["_SOURCE_LAST_TS"] = combined.index + pd.Timedelta(seconds=seconds) - pd.Timedelta(
                nanoseconds=1
            )
            combined["_BAR_CLOSE_TS"] = combined.index + pd.Timedelta(seconds=seconds)
        out_of_range_rows = int((~in_range).sum())
        combined = combined.sort_index()
        if combined.empty:
            base_record.update(
                {
                    "failure_status": "no_fully_closed_history",
                    "error": {
                        "type": "NoFullyClosedHistory",
                        "message": "all returned native bars were outside the horizon or still forming",
                    },
                    "rows": {
                        "raw_received": raw_rows,
                        "after_page_filter": pre_dedup_rows,
                        "duplicates_removed": duplicate_rows,
                        "out_of_range_removed": out_of_range_rows,
                        "published": 0,
                    },
                }
            )
            return None, base_record
        missing_columns = [column for column in self.REQUIRED_COLUMNS if column not in combined.columns]
        quality = self._quality_stats(combined, seconds, native_start, fetch_end_exclusive)
        if missing_columns or not quality["ohlc_invariants_valid"]:
            base_record.update(
                {
                    "failure_status": "data_quality_failed",
                    "error": {
                        "type": "DataQualityError",
                        "message": (
                            f"missing OHLC columns: {missing_columns}"
                            if missing_columns
                            else "non-finite or internally inconsistent OHLC rows detected"
                        ),
                    },
                    "rows": {
                        "raw_received": raw_rows,
                        "after_page_filter": pre_dedup_rows,
                        "duplicates_removed": duplicate_rows,
                        "out_of_range_removed": out_of_range_rows,
                        "published": 0,
                    },
                    "quality": quality,
                }
            )
            return None, base_record

        actual_first = combined.index[0].to_pydatetime()
        actual_last = combined.index[-1].to_pydatetime()
        actual_last_close = pd.Timestamp(combined["_BAR_CLOSE_TS"].iloc[-1]).to_pydatetime()
        base_record["request"]["last_fully_closed_bar_open_utc"] = _iso(actual_last)
        tolerance = boundary_tolerance
        coverage_start = self.common_start_utc if broker_session_aligned else native_start
        coverage_end = self.as_of_utc if broker_session_aligned else fetch_end_exclusive
        start_delay = max(timedelta(0), actual_first - coverage_start)
        end_lag = max(timedelta(0), coverage_end - actual_last_close)
        internal_empty_pages = [
            item
            for item in empty_pages
            if parse_utc_datetime(item["start_utc"]) >= actual_first
            and parse_utc_datetime(item["end_exclusive_utc"]) <= actual_last
        ]
        full_span = (
            start_delay <= tolerance
            and end_lag <= tolerance
            and not internal_empty_pages
            and not truncated_pages
        )
        span_seconds = max(1.0, (coverage_end - coverage_start).total_seconds())
        observed_span_seconds = max(
            0.0,
            (
                min(coverage_end, actual_last_close)
                - max(coverage_start, actual_first)
            ).total_seconds(),
        )
        coverage = {
            "status": "full_requested_span" if full_span else "partial_requested_span",
            "actual_first_open_utc": _iso(actual_first),
            "actual_last_open_utc": _iso(actual_last),
            "actual_last_close_utc": _iso(actual_last_close),
            "start_delay_seconds": float(start_delay.total_seconds()),
            "end_lag_seconds": float(end_lag.total_seconds()),
            "calendar_span_coverage_ratio": float(min(1.0, observed_span_seconds / span_seconds)),
            "internal_empty_page_count": len(internal_empty_pages),
            "truncated_page_count": len(truncated_pages),
            "market_calendar_completeness_assessed": False,
            "caveat": (
                "Continuous-calendar slot density and cadence gaps include weekends, holidays, "
                "and exchange closures; they are diagnostics, not a trading-calendar completeness claim."
            ),
        }
        session_coverage, session_coverage_details = segmented_session_coverage_by_partition_v2(
            combined,
            self.partition_windows,
        )
        coverage_partitions = (
            session_coverage_details.get("partitions", {})
            if isinstance(session_coverage_details, Mapping)
            else {}
        )
        observed_session_bars = {
            partition: (
                int((coverage_partitions.get(partition) or {}).get("observed_rows"))
                if isinstance(coverage_partitions.get(partition), Mapping)
                and (coverage_partitions.get(partition) or {}).get("observed_rows") is not None
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
        base_record.update(
            {
                "status": status,
                "failure_status": None if full_span else "partial_history",
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
                    "published": int(len(combined)),
                },
                "coverage": coverage,
                "expected_session_coverage_by_partition": session_coverage,
                "observed_session_bars_by_partition": observed_session_bars,
                "expected_session_bars_by_partition": expected_session_bars,
                "expected_session_coverage_details_by_partition": session_coverage_details,
                "quality": quality,
                "columns": [str(column) for column in combined.columns],
            }
        )
        return combined, base_record

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
        required_present = all(column in frame.columns for column in cls.REQUIRED_COLUMNS)
        invalid_ohlc_rows = len(frame)
        if required_present and len(frame):
            numeric = frame.loc[:, cls.REQUIRED_COLUMNS].apply(pd.to_numeric, errors="coerce")
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
            off_grid_rows = int(np.count_nonzero(epoch_seconds % int(timeframe_seconds)))
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
        reusable = self._validated_reusable_cells()
        results: List[Dict[str, Any]] = []
        newly_acquired = 0
        bounded_reason: Optional[str] = None
        stop = False

        for symbol in self.request.symbols:
            broker_symbol: Optional[str] = None
            resolution_error: Optional[BaseException] = None
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
                    and time.monotonic() - monotonic_started >= float(max_runtime_seconds)
                ):
                    bounded_reason = "max_runtime_seconds"
                    stop = True
                    break
                if not resolved:
                    resolved = True
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
                if resolution_error is not None or broker_symbol is None:
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
                        "source_kind": "broker_native",
                        "timezone": "UTC",
                        "source_environment": _json_safe(self.runtime_metadata),
                        "instrument": _json_safe(instrument_metadata),
                        "acquisition_started_utc": _iso(leaf_started),
                        "acquisition_completed_utc": _iso(_utc(self.now_fn())),
                        "error": {"type": type(error).__name__, "message": str(error)},
                    }
                    committed = self.store.commit_leaf(symbol, timeframe, record, None)
                else:
                    try:
                        frame, record = self._fetch_timeframe(
                            symbol,
                            broker_symbol,
                            timeframe,
                            leaf_started,
                            instrument_metadata,
                        )
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
                            "source_kind": "broker_native",
                            "timezone": "UTC",
                            "source_environment": _json_safe(self.runtime_metadata),
                            "instrument": _json_safe(instrument_metadata),
                            "acquisition_started_utc": _iso(leaf_started),
                            "acquisition_completed_utc": _iso(_utc(self.now_fn())),
                            "error": {"type": type(exc).__name__, "message": str(exc)},
                        }
                    try:
                        committed = self.store.commit_leaf(symbol, timeframe, record, frame)
                    except Exception as write_exc:
                        committed = self.store.commit_write_failure(
                            symbol,
                            timeframe,
                            record,
                            write_exc,
                        )
                newly_acquired += 1
                results.append(
                    self._manifest_result(
                        symbol,
                        timeframe,
                        committed,
                        disposition="newly_acquired",
                    )
                )
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
            "stale_staging_removed": int(self.store.stale_staging_removed),
        }
        expected_total = len(self.request.symbols) * len(self.request.timeframes)
        if stop or len(results) != expected_total:
            return {
                "schema_version": MANIFEST_SCHEMA_VERSION,
                "run_id": self.store.run_id,
                "complete": False,
                "manifest_path": None,
                "bounded_stop_reason": bounded_reason or "incomplete_target",
                "results": results,
                "counts": status_counts,
                "resume": resume_summary,
                "remaining_cells": expected_total - len(results),
            }

        completed = _utc(self.now_fn())
        manifest: Dict[str, Any] = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "immutable_record": True,
            "complete": True,
            "run_id": self.store.run_id,
            "acquisition_started_utc": _iso(started),
            "acquisition_completed_utc": _iso(completed),
            "request": {
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
                "boundary_tolerance_days": float(
                    self.request.boundary_tolerance_days
                ),
                "window_years": dict(self.request.window_years or {}),
                "embargo_days": float(self.request.embargo_days),
                "common_calendar_boundaries": {
                    partition: {"start_ts": _iso(start), "end_ts": _iso(end)}
                    for partition, (start, end) in self.partition_windows.items()
                },
            },
            "source_environment": _json_safe(self.runtime_metadata),
            "results": results,
            "counts": status_counts,
            "resume": resume_summary,
        }
        manifest_path = self.store.commit_manifest(manifest)
        manifest["manifest_path"] = str(manifest_path)
        return manifest

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
            "eligible_for_optimization": bool(record.get("eligible_for_optimization", False)),
            "eligible_for_downstream_validation": bool(
                record.get("eligible_for_downstream_validation", False)
            ),
            "rows": int((artifact or {}).get("rows", 0)) if isinstance(artifact, Mapping) else 0,
            "sha256": (artifact or {}).get("sha256") if isinstance(artifact, Mapping) else None,
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
    parser.add_argument("--symbols", nargs="+", help="Canonical symbols; defaults to config.json symbols")
    parser.add_argument("--timeframes", nargs="+", help="Defaults to all six native timeframes")
    parser.add_argument("--years", type=int)
    parser.add_argument("--train-years", type=int)
    parser.add_argument("--tuning-years", type=int)
    parser.add_argument("--lockbox-years", type=int)
    parser.add_argument("--embargo-days", type=float)
    parser.add_argument("--warmup-days", type=float)
    parser.add_argument("--safety-days", type=float)
    parser.add_argument("--boundary-tolerance-days", type=float)
    parser.add_argument("--as-of", help="Aware ISO-8601 UTC timestamp; defaults to current UTC")
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
        help="Stop between atomic cells after this elapsed time; the current cell is never interrupted.",
    )
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-sleep-seconds", type=float, default=0.5)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually open the configured MT5 terminal and acquire bars. Without this flag, print the plan only.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def _filtered_dataclass_kwargs(dataclass_type: Any, values: Mapping[str, Any]) -> Dict[str, Any]:
    allowed = {item.name for item in fields(dataclass_type)}
    return {key: value for key, value in dict(values or {}).items() if key in allowed}


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    preheld_lock: Optional[NativeHistoryRunLock] = None
    if args.execute and args.resume:
        if not args.run_id:
            raise ValueError("--resume requires the exact existing --run-id")
        config_path = Path(args.config).resolve()
        output_root = Path(args.output_root)
        if not output_root.is_absolute():
            output_root = (config_path.parent / output_root).resolve()
        preheld_lock = NativeHistoryRunLock(output_root, args.run_id).acquire()
    try:
        return _main_with_args(args, preheld_lock=preheld_lock)
    finally:
        if preheld_lock is not None:
            preheld_lock.release()


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

    pipeline_config = config.get("pipeline") if isinstance(config.get("pipeline"), Mapping) else {}
    configured_windows = (
        pipeline_config.get("evidence_window_years")
        if isinstance(pipeline_config.get("evidence_window_years"), Mapping)
        else {}
    )
    resume_contract: Optional[NativeHistoryResumeContract] = None
    if args.resume:
        resume_contract = load_native_history_resume_contract(output_root, args.run_id)
        locked = dict(resume_contract.request)
        original_cutoff = parse_utc_datetime(
            str(locked["requested_information_cutoff_utc"])
        )
        if args.as_of and _iso(parse_utc_datetime(args.as_of)) != _iso(original_cutoff):
            raise ResumeValidationError(
                "--as-of must exactly equal the original requested information cutoff"
            )
        explicit_checks = {
            "--years": (args.years, int(locked["horizon_years"])),
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
            "--embargo-days": (args.embargo_days, float(locked["embargo_days"])),
            "--warmup-days": (args.warmup_days, float(locked["warmup_days"])),
            "--safety-days": (args.safety_days, float(locked["safety_days"])),
            "--boundary-tolerance-days": (
                args.boundary_tolerance_days,
                float(locked["boundary_tolerance_days"]),
            ),
        }
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
        target = {(symbol.upper(), tf.upper()) for symbol in symbols for tf in timeframes}
        extras = sorted(set(resume_contract.existing_cells) - target)
        if extras:
            raise ResumeValidationError(
                f"existing run contains cells outside this resume target: {extras}"
            )
        as_of = original_cutoff
        horizon_years = int(locked["horizon_years"])
        window_years = dict(locked["window_years"])
        embargo_days = float(locked["embargo_days"])
        warmup_days = float(locked["warmup_days"])
        safety_days = float(locked["safety_days"])
        boundary_tolerance_days = float(locked["boundary_tolerance_days"])
        enforce_evidence_horizon = locked["required_evidence_lead_start_utc"] is not None
    else:
        as_of = parse_utc_datetime(args.as_of) if args.as_of else datetime.now(timezone.utc)
        horizon_years = args.years if args.years is not None else 10
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
        embargo_days=embargo_days,
        warmup_days=warmup_days,
        safety_days=safety_days,
        boundary_tolerance_days=boundary_tolerance_days,
        enforce_evidence_horizon=enforce_evidence_horizon,
    )
    run_id = args.run_id or default_run_id(as_of)
    plan = {
        "mode": "execute" if args.execute else "plan_only_no_connection",
        "symbols": list(request.symbols),
        "timeframes": list(request.timeframes),
        "horizon_years": request.horizon_years,
        "window_years": dict(request.window_years or {}),
        "embargo_days": float(request.embargo_days),
        "warmup_days": float(request.warmup_days),
        "safety_days": float(request.safety_days),
        "boundary_tolerance_days": float(request.boundary_tolerance_days),
        "as_of_utc": _iso(as_of),
        "output_root": str(output_root),
        "run_id": run_id,
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
