"""Read-only parity audit between broker-native bars and native M5 membership.

Native higher-timeframe bars remain the signal authority.  M5 is used only as
an audit comparator, grouped inside each native bar's own interval so broker
session anchors, short sessions, DST days, and holiday stubs are not silently
relabelled to a generic midnight grid.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

import numpy as np
import pandas as pd


PARITY_SCHEMA_VERSION = "fxpm-native-m5-parity-v1"
TIMEFRAME_SECONDS: Dict[str, int] = {
    "M5": 300,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H4": 14400,
    "D1": 86400,
}
OHLC = ("Open", "High", "Low", "Close")


@dataclass(frozen=True)
class ParityThresholds:
    min_membership_ratio: float = 0.99
    max_ohlc_mismatch_ratio: float = 0.005
    max_unassigned_m5_ratio: float = 0.01
    min_compared_native_bars: int = 20


def normalize_ohlc_frame(frame: pd.DataFrame) -> pd.DataFrame:
    local = frame.copy()
    aliases = {str(column).lower(): column for column in local.columns}
    if not isinstance(local.index, pd.DatetimeIndex):
        time_column = aliases.get("time") or aliases.get("datetime") or aliases.get("date")
        if time_column is None:
            raise ValueError("frame has no datetime index or time column")
        local.index = pd.to_datetime(local.pop(time_column), utc=True, errors="coerce")
    else:
        local.index = pd.to_datetime(local.index, utc=True, errors="coerce")
    rename = {}
    for canonical in OHLC:
        source = aliases.get(canonical.lower())
        if source is not None:
            rename[source] = canonical
    local = local.rename(columns=rename)
    missing = [column for column in OHLC if column not in local.columns]
    if missing:
        raise ValueError(f"missing OHLC columns: {missing}")
    local = local[~local.index.isna()]
    local = local[~local.index.duplicated(keep="last")].sort_index()
    for column in OHLC:
        local[column] = pd.to_numeric(local[column], errors="coerce")
    return local


def _week_key(timestamp: pd.Timestamp) -> str:
    iso = timestamp.isocalendar()
    return f"{int(iso.year):04d}-W{int(iso.week):02d}"


def _status_report(
    *,
    symbol: str,
    timeframe: str,
    status: str,
    reason_codes: list[str],
    tick_tolerance: Optional[float],
    thresholds: ParityThresholds,
    metrics: Optional[Mapping[str, Any]] = None,
    diagnostics: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "schema_version": PARITY_SCHEMA_VERSION,
        "symbol": str(symbol).upper(),
        "timeframe": str(timeframe).upper(),
        "status": status,
        "reason_codes": list(reason_codes),
        "native_is_signal_authority": True,
        "m5_role": "read_only_session_membership_audit_comparator",
        "tick_tolerance": tick_tolerance,
        "thresholds": {
            "min_membership_ratio": thresholds.min_membership_ratio,
            "max_ohlc_mismatch_ratio": thresholds.max_ohlc_mismatch_ratio,
            "max_unassigned_m5_ratio": thresholds.max_unassigned_m5_ratio,
            "min_compared_native_bars": thresholds.min_compared_native_bars,
        },
        "metrics": dict(metrics or {}),
        "diagnostics": dict(diagnostics or {}),
    }


def audit_native_vs_session_m5(
    m5_frame: pd.DataFrame,
    native_frame: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    tick_tolerance: Optional[float],
    thresholds: Optional[ParityThresholds] = None,
) -> Dict[str, Any]:
    """Audit native membership and OHLC reconstruction over the common overlap."""

    thresholds = thresholds or ParityThresholds()
    tf = str(timeframe or "").strip().upper()
    if tf not in TIMEFRAME_SECONDS:
        return _status_report(
            symbol=symbol,
            timeframe=tf,
            status="PENDING",
            reason_codes=["UNSUPPORTED_TIMEFRAME"],
            tick_tolerance=tick_tolerance,
            thresholds=thresholds,
        )
    try:
        m5 = normalize_ohlc_frame(m5_frame)
        native = normalize_ohlc_frame(native_frame)
    except Exception as exc:
        return _status_report(
            symbol=symbol,
            timeframe=tf,
            status="PENDING",
            reason_codes=[f"FRAME_INVALID:{type(exc).__name__}"],
            tick_tolerance=tick_tolerance,
            thresholds=thresholds,
            diagnostics={"error": str(exc)},
        )
    if tf == "M5":
        return _status_report(
            symbol=symbol,
            timeframe=tf,
            status="PASS",
            reason_codes=["NATIVE_BASE_SELF_AUDIT_NOT_APPLICABLE"],
            tick_tolerance=tick_tolerance,
            thresholds=thresholds,
            metrics={
                "native_bars": int(len(native)),
                "m5_bars": int(len(m5)),
                "bar_membership_ratio": 1.0 if len(native) else None,
            },
        )
    if m5.empty or native.empty:
        return _status_report(
            symbol=symbol,
            timeframe=tf,
            status="PENDING",
            reason_codes=["NO_OVERLAP_DATA"],
            tick_tolerance=tick_tolerance,
            thresholds=thresholds,
        )

    nominal_ns = int(TIMEFRAME_SECONDS[tf] * 1_000_000_000)
    m5_step_ns = int(TIMEFRAME_SECONDS["M5"] * 1_000_000_000)
    m5_ns = m5.index.asi8
    native_ns = native.index.asi8
    next_native = np.empty_like(native_ns)
    if len(native_ns) > 1:
        next_native[:-1] = native_ns[1:]
    next_native[-1] = native_ns[-1] + nominal_ns
    native_deltas = next_native - native_ns
    session_adjacent = (native_deltas >= nominal_ns * 0.5) & (native_deltas <= nominal_ns * 1.5)
    interval_ends = np.where(session_adjacent, next_native, native_ns + nominal_ns)

    m5_available_start = int(m5_ns[0])
    m5_available_end = int(m5_ns[-1] + m5_step_ns)
    overlap_mask = (native_ns >= m5_available_start) & (interval_ends <= m5_available_end)
    overlap_positions = np.flatnonzero(overlap_mask)
    if len(overlap_positions) < int(thresholds.min_compared_native_bars):
        return _status_report(
            symbol=symbol,
            timeframe=tf,
            status="PENDING",
            reason_codes=[
                f"OVERLAP_TOO_SMALL:{len(overlap_positions)}<{thresholds.min_compared_native_bars}"
            ],
            tick_tolerance=tick_tolerance,
            thresholds=thresholds,
            metrics={"native_bars_in_full_m5_overlap": int(len(overlap_positions))},
        )

    tolerance: Optional[float]
    try:
        tolerance = float(tick_tolerance) if tick_tolerance is not None else None
    except (TypeError, ValueError, OverflowError):
        tolerance = None
    if tolerance is not None and (not math.isfinite(tolerance) or tolerance <= 0.0):
        tolerance = None

    assigned = np.zeros(len(m5), dtype=bool)
    m5_values = {
        column: m5[column].to_numpy(dtype=float, copy=False)
        for column in OHLC
    }
    native_values = {
        column: native[column].to_numpy(dtype=float, copy=False)
        for column in OHLC
    }
    missing_native_bars = 0
    matched_native_bars = 0
    stub_native_bars = 0
    ohlc_mismatch_bars = 0
    mismatch_samples: list[Dict[str, Any]] = []
    weekly: Dict[str, Dict[str, Any]] = {}
    dst_transition_weeks: set[str] = set()

    regular_offsets = native_ns % nominal_ns
    overlap_lefts = np.searchsorted(m5_ns, native_ns[overlap_positions], side="left")
    overlap_rights = np.searchsorted(m5_ns, interval_ends[overlap_positions], side="left")
    for overlap_idx, position in enumerate(overlap_positions):
        start_ns = int(native_ns[position])
        end_ns = int(interval_ends[position])
        left = int(overlap_lefts[overlap_idx])
        right = int(overlap_rights[overlap_idx])
        native_ts = native.index[position]
        week = _week_key(native_ts)
        week_stats = weekly.setdefault(
            week,
            {
                "native_bars": 0,
                "matched_membership_bars": 0,
                "missing_membership_bars": 0,
                "stub_bars": 0,
                "ohlc_mismatch_bars": 0,
            },
        )
        week_stats["native_bars"] += 1
        if right <= left:
            missing_native_bars += 1
            week_stats["missing_membership_bars"] += 1
            continue
        assigned[left:right] = True
        matched_native_bars += 1
        week_stats["matched_membership_bars"] += 1
        expected_slots = max(1, int(round((end_ns - start_ns) / m5_step_ns)))
        if (right - left) < expected_slots:
            stub_native_bars += 1
            week_stats["stub_bars"] += 1
        aggregated = {
            "Open": float(m5_values["Open"][left]),
            "High": float(np.nanmax(m5_values["High"][left:right])),
            "Low": float(np.nanmin(m5_values["Low"][left:right])),
            "Close": float(m5_values["Close"][right - 1]),
        }
        mismatch_fields: Dict[str, float] = {}
        if tolerance is not None:
            for column in OHLC:
                observed = float(native_values[column][position])
                difference = abs(observed - aggregated[column])
                if not math.isfinite(difference) or difference > tolerance + 1e-12:
                    mismatch_fields[column] = difference
        if mismatch_fields:
            ohlc_mismatch_bars += 1
            week_stats["ohlc_mismatch_bars"] += 1
            if len(mismatch_samples) < 50:
                mismatch_samples.append(
                    {
                        "native_open_ts": native_ts.isoformat(),
                        "m5_first_ts": m5.index[left].isoformat(),
                        "m5_last_ts": m5.index[right - 1].isoformat(),
                        "m5_rows": right - left,
                        "differences": mismatch_fields,
                    }
                )

        if position > 0 and session_adjacent[position - 1]:
            previous_offset = int(regular_offsets[position - 1])
            current_offset = int(regular_offsets[position])
            if previous_offset != current_offset:
                dst_transition_weeks.add(week)
        interval_seconds = (end_ns - start_ns) / 1_000_000_000.0
        if abs(interval_seconds - TIMEFRAME_SECONDS[tf]) >= 3600.0:
            dst_transition_weeks.add(week)

    first_interval = int(native_ns[overlap_positions[0]])
    last_interval = int(interval_ends[overlap_positions[-1]])
    relevant_m5 = (m5_ns >= first_interval) & (m5_ns < last_interval)
    relevant_m5_count = int(relevant_m5.sum())
    unassigned_m5_rows = int((relevant_m5 & ~assigned).sum())
    compared = int(len(overlap_positions))
    membership_ratio = matched_native_bars / compared if compared else 0.0
    mismatch_ratio = ohlc_mismatch_bars / matched_native_bars if matched_native_bars else 1.0
    unassigned_ratio = unassigned_m5_rows / relevant_m5_count if relevant_m5_count else 1.0

    weekly_counts = pd.Series(
        {week: int(values["native_bars"]) for week, values in weekly.items()}, dtype=float
    ).sort_index()
    holiday_weeks: list[str] = []
    if len(weekly_counts) >= 4:
        nominal_week = float(weekly_counts.quantile(0.75, interpolation="higher"))
        interior = weekly_counts.iloc[1:-1]
        holiday_weeks = [
            str(week)
            for week, count in interior.items()
            if nominal_week > 0.0 and float(count) < 0.90 * nominal_week
        ]
    else:
        nominal_week = None

    metrics = {
        "native_bars_in_full_m5_overlap": compared,
        "matched_membership_bars": matched_native_bars,
        "missing_native_membership_bars": missing_native_bars,
        "stub_native_bars": stub_native_bars,
        "bar_membership_ratio": float(membership_ratio),
        "ohlc_compared_bars": matched_native_bars if tolerance is not None else 0,
        "ohlc_mismatch_bars": ohlc_mismatch_bars if tolerance is not None else None,
        "ohlc_mismatch_ratio": float(mismatch_ratio) if tolerance is not None else None,
        "m5_rows_in_overlap": relevant_m5_count,
        "unassigned_m5_rows": unassigned_m5_rows,
        "unassigned_m5_ratio": float(unassigned_ratio),
    }
    diagnostics = {
        "native_interval_membership": "[native_open, next_regular_native_open_or_nominal_close)",
        "mismatch_samples": mismatch_samples,
        "weekly": weekly,
        "nominal_native_bars_per_week_q75": nominal_week,
        "dst_transition_weeks": sorted(dst_transition_weeks),
        "holiday_or_short_session_candidate_weeks": holiday_weeks,
        "stub_note": (
            "A stub is fewer M5 members than a continuous interval; exchange closures can make stubs valid."
        ),
    }

    reasons: list[str] = []
    pending: list[str] = []
    if tolerance is None:
        pending.append("TICK_TOLERANCE_UNAVAILABLE")
    if membership_ratio < float(thresholds.min_membership_ratio):
        reasons.append(
            f"MEMBERSHIP_RATIO_LOW:{membership_ratio:.6f}<{thresholds.min_membership_ratio:.6f}"
        )
    if tolerance is not None and mismatch_ratio > float(thresholds.max_ohlc_mismatch_ratio):
        reasons.append(
            f"OHLC_MISMATCH_RATIO_HIGH:{mismatch_ratio:.6f}>{thresholds.max_ohlc_mismatch_ratio:.6f}"
        )
    if unassigned_ratio > float(thresholds.max_unassigned_m5_ratio):
        reasons.append(
            f"UNASSIGNED_M5_RATIO_HIGH:{unassigned_ratio:.6f}>{thresholds.max_unassigned_m5_ratio:.6f}"
        )
    status = "FAIL" if reasons else ("PENDING" if pending else "PASS")
    return _status_report(
        symbol=symbol,
        timeframe=tf,
        status=status,
        reason_codes=reasons + pending,
        tick_tolerance=tolerance,
        thresholds=thresholds,
        metrics=metrics,
        diagnostics=diagnostics,
    )
