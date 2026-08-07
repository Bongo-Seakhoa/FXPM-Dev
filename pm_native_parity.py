"""Read-only parity audit between broker-native bars and native M5 membership.

Native higher-timeframe bars remain the signal authority.  M5 is used only as
an audit comparator, grouped inside each native bar's own interval so broker
session anchors, short sessions, DST days, and holiday stubs are not silently
relabelled to a generic midnight grid.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

import numpy as np
import pandas as pd


PARITY_SCHEMA_VERSION = "fxpm-native-m5-parity-v1"
CROSS_RESOLUTION_PARITY_SCHEMA_VERSION = "fxpm-native-cross-resolution-parity-v2"
CROSS_RESOLUTION_COMPATIBILITY_CURRENT = "current"
CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307 = "frozen_pre_seq2307"
_CROSS_RESOLUTION_COMPATIBILITY_PROFILES = frozenset(
    {
        CROSS_RESOLUTION_COMPATIBILITY_CURRENT,
        CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307,
    }
)
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
    max_ohlc_mismatch_ratio: float = 0.0
    max_unassigned_m5_ratio: float = 0.01
    min_compared_native_bars: int = 20


@dataclass(frozen=True)
class _NormalizedOHLCFrame:
    frame: pd.DataFrame


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
    for canonical, candidates in (
        ("Volume", ("volume", "tick_volume")),
        ("RealVolume", ("realvolume", "real_volume")),
    ):
        if canonical in local.columns:
            continue
        source = next((aliases.get(candidate) for candidate in candidates if aliases.get(candidate)), None)
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
    for column in ("Volume", "RealVolume"):
        if column in local.columns:
            local[column] = pd.to_numeric(local[column], errors="coerce")
    return local


def _is_normalized_ohlc_frame(frame: Any) -> bool:
    """Cheap contract check for the activation reader's normalized output."""

    if not (
        isinstance(frame, pd.DataFrame)
        and frame.columns.is_unique
        and isinstance(frame.index, pd.DatetimeIndex)
        and str(frame.index.tz) == "UTC"
        and not frame.index.hasnans
        and frame.index.is_unique
        and frame.index.is_monotonic_increasing
        and all(column in frame.columns for column in OHLC)
        and all(
            sum(
                str(column).lower() == canonical.lower()
                for column in frame.columns
            )
            == 1
            for canonical in OHLC
        )
        and all(pd.api.types.is_numeric_dtype(frame[column]) for column in OHLC)
    ):
        return False
    aliases = {str(column).lower(): column for column in frame.columns}
    for canonical, candidates in (
        ("Volume", ("volume", "tick_volume")),
        ("RealVolume", ("realvolume", "real_volume")),
    ):
        if canonical in frame.columns:
            if not pd.api.types.is_numeric_dtype(frame[canonical]):
                return False
            continue
        if any(aliases.get(candidate) is not None for candidate in candidates):
            return False
    return True


def _normalized_partition_windows(
    partition_windows: Optional[Mapping[str, Tuple[Any, Any]]],
    *,
    fine: pd.DataFrame,
    coarse: pd.DataFrame,
    fine_seconds: int,
    coarse_interval_ends_ns: np.ndarray,
) -> Tuple[Dict[str, Tuple[pd.Timestamp, pd.Timestamp]], list[str]]:
    """Normalize explicit audit windows or derive one common-overlap window."""

    if partition_windows is None:
        start = max(fine.index[0], coarse.index[0])
        end = min(
            fine.index[-1] + pd.Timedelta(seconds=fine_seconds),
            pd.Timestamp(int(coarse_interval_ends_ns[-1]), tz="UTC"),
        )
        if start >= end:
            return {}, ["NO_COMMON_OVERLAP"]
        return {"common_overlap": (start, end)}, []

    windows: Dict[str, Tuple[pd.Timestamp, pd.Timestamp]] = {}
    errors: list[str] = []
    for raw_name, raw_window in partition_windows.items():
        name = str(raw_name or "").strip()
        if not name or name in windows:
            errors.append("PARTITION_NAME_INVALID")
            continue
        if not isinstance(raw_window, (tuple, list)) or len(raw_window) != 2:
            errors.append(f"PARTITION_WINDOW_INVALID:{name}")
            continue
        try:
            start = pd.Timestamp(raw_window[0])
            end = pd.Timestamp(raw_window[1])
            start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
            end = end.tz_localize("UTC") if end.tzinfo is None else end.tz_convert("UTC")
        except (TypeError, ValueError, OverflowError):
            errors.append(f"PARTITION_WINDOW_INVALID:{name}")
            continue
        if start >= end:
            errors.append(f"PARTITION_WINDOW_ORDER_INVALID:{name}")
            continue
        windows[name] = (start, end)
    if not windows and not errors:
        errors.append("PARTITION_WINDOWS_EMPTY")
    return windows, errors


def _coarse_interval_ends_ns(
    coarse: pd.DataFrame,
    *,
    coarse_seconds: int,
) -> Tuple[np.ndarray, int, str]:
    """Return authenticated close timestamps when present, else observed intervals."""

    opens_ns = coarse.index.asi8
    nominal_ns = int(coarse_seconds * 1_000_000_000)
    close_column = coarse.get("_BAR_CLOSE_TS")
    invalid = 0
    if close_column is not None:
        parsed = pd.to_datetime(close_column, utc=True, errors="coerce")
        close_ns = pd.DatetimeIndex(parsed).asi8
        valid = (close_ns > opens_ns) & (close_ns != np.iinfo(np.int64).min)
        if len(opens_ns) > 1:
            valid[:-1] &= close_ns[:-1] <= opens_ns[1:]
        invalid = int((~valid).sum())
        if invalid == 0:
            return close_ns, 0, "authenticated_bar_close_column"

    next_open = np.empty_like(opens_ns)
    if len(opens_ns) > 1:
        next_open[:-1] = opens_ns[1:]
    next_open[-1] = opens_ns[-1] + nominal_ns
    deltas = next_open - opens_ns
    adjacent = (deltas >= nominal_ns * 0.5) & (deltas <= nominal_ns * 1.5)
    inferred = np.where(adjacent, next_open, opens_ns + nominal_ns)
    method = (
        "invalid_authenticated_close_fallback"
        if close_column is not None
        else "observed_successor_or_nominal_close"
    )
    return inferred, invalid, method


def _boundary_diagnostics(
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    fine: pd.DataFrame,
    fine_positions: np.ndarray,
    fine_seconds: int,
    coarse: pd.DataFrame,
    coarse_positions: np.ndarray,
    coarse_interval_ends_ns: np.ndarray,
) -> Dict[str, Any]:
    """Expose raw edge lags without pretending calendar closures are outages."""

    fine_first = fine.index[int(fine_positions[0])] if fine_positions.size else None
    fine_last = fine.index[int(fine_positions[-1])] if fine_positions.size else None
    coarse_first = coarse.index[int(coarse_positions[0])] if coarse_positions.size else None
    coarse_last = coarse.index[int(coarse_positions[-1])] if coarse_positions.size else None
    coarse_last_close = (
        pd.Timestamp(int(coarse_interval_ends_ns[int(coarse_positions[-1])]), tz="UTC")
        if coarse_positions.size
        else None
    )

    def _seconds(value: Optional[pd.Timedelta]) -> Optional[float]:
        return float(max(pd.Timedelta(0), value).total_seconds()) if value is not None else None

    return {
        "requested_start_utc": start.isoformat(),
        "requested_end_exclusive_utc": end.isoformat(),
        "fine_first_open_utc": fine_first.isoformat() if fine_first is not None else None,
        "fine_last_open_utc": fine_last.isoformat() if fine_last is not None else None,
        "coarse_first_open_utc": coarse_first.isoformat() if coarse_first is not None else None,
        "coarse_last_open_utc": coarse_last.isoformat() if coarse_last is not None else None,
        "coarse_last_close_utc": (
            coarse_last_close.isoformat() if coarse_last_close is not None else None
        ),
        "fine_start_delay_seconds": _seconds(fine_first - start) if fine_first is not None else None,
        "fine_end_lag_seconds": (
            _seconds(end - (fine_last + pd.Timedelta(seconds=fine_seconds)))
            if fine_last is not None
            else None
        ),
        "coarse_start_delay_seconds": (
            _seconds(coarse_first - start) if coarse_first is not None else None
        ),
        "coarse_end_lag_seconds": (
            _seconds(end - coarse_last_close) if coarse_last_close is not None else None
        ),
        "caveat": (
            "Raw calendar-edge lags include weekends, holidays, and closed sessions. "
            "Eligibility is derived from bidirectional bar membership and exact volume "
            "reconciliation, not from these lags alone."
        ),
    }


def audit_native_cross_resolution(
    fine_frame: pd.DataFrame,
    coarse_frame: pd.DataFrame,
    *,
    symbol: str,
    fine_timeframe: str,
    coarse_timeframe: str,
    activation_target_timeframe: str,
    tick_tolerance: Optional[float],
    partition_windows: Optional[Mapping[str, Tuple[Any, Any]]] = None,
    thresholds: Optional[ParityThresholds] = None,
    compatibility_profile: str = CROSS_RESOLUTION_COMPATIBILITY_CURRENT,
) -> Dict[str, Any]:
    """Deterministically reconcile one native fine/coarse timeframe pair.

    The finer series is audit-only.  Coverage is measured in both directions:
    every fine bar must belong to a native coarse interval, while every coarse
    bar must be reconstructable from fine OHLC and exact tick volume.  Explicit
    partition windows are evaluated independently so a long training history
    cannot dilute a defect in tuning validation or the final lockbox.

    This certifies cross-timeframe consistency of the broker histories.  It is
    deliberately not described as exchange-calendar completeness: an interval
    omitted identically by both broker series remains observationally invisible.
    """

    if compatibility_profile not in _CROSS_RESOLUTION_COMPATIBILITY_PROFILES:
        raise ValueError(
            "unsupported cross-resolution compatibility profile: "
            f"{compatibility_profile!r}"
        )
    frozen_pre_seq2307 = (
        compatibility_profile
        == CROSS_RESOLUTION_COMPATIBILITY_FROZEN_PRE_SEQ2307
    )
    thresholds = thresholds or ParityThresholds()
    fine_tf = str(fine_timeframe or "").strip().upper()
    coarse_tf = str(coarse_timeframe or "").strip().upper()
    target_tf = str(activation_target_timeframe or "").strip().upper()

    def _report(
        status: str,
        reasons: list[str],
        *,
        metrics: Optional[Mapping[str, Any]] = None,
        partitions: Optional[Mapping[str, Any]] = None,
        diagnostics: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        report = {
            "schema_version": CROSS_RESOLUTION_PARITY_SCHEMA_VERSION,
            "symbol": str(symbol).upper(),
            "timeframe": target_tf,
            "activation_target_timeframe": target_tf,
            "fine_timeframe": fine_tf,
            "coarse_timeframe": coarse_tf,
            "status": status,
            "reason_codes": list(dict.fromkeys(reasons)),
            "native_is_signal_authority": True,
            "fine_role": "read_only_native_audit_reference",
            "coarse_role": "native_signal_authority",
            "tick_tolerance": tick_tolerance,
            "thresholds": {
                "min_coarse_membership_ratio": thresholds.min_membership_ratio,
                "max_unassigned_fine_ratio": thresholds.max_unassigned_m5_ratio,
                "max_ohlc_mismatch_ratio": thresholds.max_ohlc_mismatch_ratio,
                "min_compared_coarse_bars_per_partition": thresholds.min_compared_native_bars,
                "tick_volume_reconciliation": "exact",
                "real_volume_reconciliation": "exact_when_nonzero",
            },
            "metrics": dict(metrics or {}),
            "partitions": dict(partitions or {}),
            "diagnostics": dict(diagnostics or {}),
        }
        if frozen_pre_seq2307:
            for partition in report["partitions"].values():
                if isinstance(partition, dict):
                    partition.pop(
                        "trailing_incomplete_coarse_fine_rows_deferred", None
                    )
                    partition.pop("trailing_incomplete_coarse_interval", None)
            report["diagnostics"].pop("trailing_incomplete_coarse_policy", None)
        return report

    if fine_tf not in TIMEFRAME_SECONDS or coarse_tf not in TIMEFRAME_SECONDS:
        return _report("PENDING", ["UNSUPPORTED_TIMEFRAME_PAIR"])
    if target_tf not in {fine_tf, coarse_tf}:
        return _report("PENDING", ["ACTIVATION_TARGET_TIMEFRAME_INVALID"])
    fine_seconds = int(TIMEFRAME_SECONDS[fine_tf])
    coarse_seconds = int(TIMEFRAME_SECONDS[coarse_tf])
    if fine_seconds >= coarse_seconds or coarse_seconds % fine_seconds:
        return _report("PENDING", ["INVALID_TIMEFRAME_ORDER"])
    try:
        fine = (
            fine_frame.frame
            if isinstance(fine_frame, _NormalizedOHLCFrame)
            else normalize_ohlc_frame(fine_frame)
        )
        coarse = (
            coarse_frame.frame
            if isinstance(coarse_frame, _NormalizedOHLCFrame)
            else normalize_ohlc_frame(coarse_frame)
        )
    except Exception as exc:
        return _report(
            "PENDING",
            [f"FRAME_INVALID:{type(exc).__name__}"],
            diagnostics={"error": str(exc)},
        )
    if fine.empty or coarse.empty:
        return _report("PENDING", ["NO_OVERLAP_DATA"])

    tolerance: Optional[float]
    try:
        tolerance = float(tick_tolerance) if tick_tolerance is not None else None
    except (TypeError, ValueError, OverflowError):
        tolerance = None
    if tolerance is not None and (not math.isfinite(tolerance) or tolerance <= 0.0):
        tolerance = None

    coarse_ends_ns, invalid_close_rows, interval_method = _coarse_interval_ends_ns(
        coarse,
        coarse_seconds=coarse_seconds,
    )
    windows, window_errors = _normalized_partition_windows(
        partition_windows,
        fine=fine,
        coarse=coarse,
        fine_seconds=fine_seconds,
        coarse_interval_ends_ns=coarse_ends_ns,
    )
    if window_errors:
        return _report("PENDING", window_errors)

    fine_ns = fine.index.asi8
    fine_ends_ns = fine_ns + int(fine_seconds * 1_000_000_000)
    coarse_ns = coarse.index.asi8
    candidate = np.searchsorted(coarse_ns, fine_ns, side="right") - 1
    assigned_coarse = np.full(len(fine), -1, dtype=np.int64)
    candidate_valid = candidate >= 0
    candidate_positions = np.flatnonzero(candidate_valid)
    inside = fine_ns[candidate_positions] < coarse_ends_ns[candidate[candidate_positions]]
    assigned_positions = candidate_positions[inside]
    assigned_coarse[assigned_positions] = candidate[assigned_positions]

    fine_values = {
        column: fine[column].to_numpy(dtype=float, copy=False) for column in OHLC
    }
    coarse_values = {
        column: coarse[column].to_numpy(dtype=float, copy=False) for column in OHLC
    }
    tick_volume_available = "Volume" in fine.columns and "Volume" in coarse.columns
    fine_tick_volume = (
        fine["Volume"].to_numpy(dtype=float, copy=False)
        if tick_volume_available
        else None
    )
    coarse_tick_volume = (
        coarse["Volume"].to_numpy(dtype=float, copy=False)
        if tick_volume_available
        else None
    )
    real_volume_available = "RealVolume" in fine.columns and "RealVolume" in coarse.columns
    fine_real_volume = (
        fine["RealVolume"].to_numpy(dtype=float, copy=False)
        if real_volume_available
        else None
    )
    coarse_real_volume = (
        coarse["RealVolume"].to_numpy(dtype=float, copy=False)
        if real_volume_available
        else None
    )

    member_counts = np.zeros(len(coarse), dtype=np.int64)
    ohlc_mismatch = np.zeros(len(coarse), dtype=bool)
    tick_volume_match = np.zeros(len(coarse), dtype=bool)
    real_volume_match = np.ones(len(coarse), dtype=bool)
    real_volume_meaningful = np.zeros(len(coarse), dtype=bool)
    mismatch_samples: list[Dict[str, Any]] = []
    volume_mismatch_samples: list[Dict[str, Any]] = []
    valid_fine_positions = np.flatnonzero(assigned_coarse >= 0)
    if valid_fine_positions.size:
        group_for_fine = assigned_coarse[valid_fine_positions]
        group_starts = np.flatnonzero(
            np.r_[True, group_for_fine[1:] != group_for_fine[:-1]]
        )
        group_ends = np.r_[group_starts[1:], valid_fine_positions.size]
        coarse_groups = group_for_fine[group_starts]
        first_fine_positions = valid_fine_positions[group_starts]
        last_fine_positions = valid_fine_positions[group_ends - 1]
        group_counts = group_ends - group_starts
        member_counts[coarse_groups] = group_counts

        selected_values = {
            column: fine_values[column][valid_fine_positions] for column in OHLC
        }
        reconstructed_ohlc = {
            "Open": selected_values["Open"][group_starts],
            "High": np.maximum.reduceat(selected_values["High"], group_starts),
            "Low": np.minimum.reduceat(selected_values["Low"], group_starts),
            "Close": selected_values["Close"][group_ends - 1],
        }
        difference_by_column: Dict[str, np.ndarray] = {}
        if tolerance is not None:
            group_mismatch = np.zeros(coarse_groups.size, dtype=bool)
            for column in OHLC:
                differences = np.abs(
                    coarse_values[column][coarse_groups] - reconstructed_ohlc[column]
                )
                difference_by_column[column] = differences
                group_mismatch |= (~np.isfinite(differences)) | (
                    differences > tolerance + 1e-12
                )
            ohlc_mismatch[coarse_groups] = group_mismatch
            for group_position in np.flatnonzero(group_mismatch)[:50]:
                position = int(coarse_groups[group_position])
                mismatch_fields = {
                    column: float(differences[group_position])
                    for column, differences in difference_by_column.items()
                    if not math.isfinite(float(differences[group_position]))
                    or float(differences[group_position]) > tolerance + 1e-12
                }
                mismatch_samples.append(
                    {
                        "coarse_open_utc": coarse.index[position].isoformat(),
                        "fine_first_utc": fine.index[
                            int(first_fine_positions[group_position])
                        ].isoformat(),
                        "fine_last_utc": fine.index[
                            int(last_fine_positions[group_position])
                        ].isoformat(),
                        "fine_rows": int(group_counts[group_position]),
                        "differences": mismatch_fields,
                    }
                )

        if tick_volume_available:
            selected_tick_volume = fine_tick_volume[valid_fine_positions]
            reconstructed_tick_volume = np.add.reduceat(
                selected_tick_volume,
                group_starts,
            )
            observed_tick_volume = coarse_tick_volume[coarse_groups]
            group_tick_match = (
                np.isfinite(observed_tick_volume)
                & np.isfinite(reconstructed_tick_volume)
                & (observed_tick_volume == reconstructed_tick_volume)
            )
            tick_volume_match[coarse_groups] = group_tick_match
            for group_position in np.flatnonzero(~group_tick_match)[:50]:
                position = int(coarse_groups[group_position])
                observed = float(observed_tick_volume[group_position])
                reconstructed = float(reconstructed_tick_volume[group_position])
                volume_mismatch_samples.append(
                    {
                        "coarse_open_utc": coarse.index[position].isoformat(),
                        "field": "Volume",
                        "coarse_value": observed,
                        "fine_sum": reconstructed,
                        "difference": observed - reconstructed,
                    }
                )
        if real_volume_available:
            selected_real_volume = fine_real_volume[valid_fine_positions]
            reconstructed_real_volume = np.add.reduceat(
                selected_real_volume,
                group_starts,
            )
            observed_real_volume = coarse_real_volume[coarse_groups]
            fine_real_nonzero = np.logical_or.reduceat(
                selected_real_volume != 0.0,
                group_starts,
            )
            group_real_meaningful = (observed_real_volume != 0.0) | fine_real_nonzero
            group_real_match = (
                np.isfinite(observed_real_volume)
                & np.isfinite(reconstructed_real_volume)
                & (observed_real_volume == reconstructed_real_volume)
            )
            real_volume_meaningful[coarse_groups] = group_real_meaningful
            real_volume_match[coarse_groups[group_real_meaningful]] = group_real_match[
                group_real_meaningful
            ]
            real_mismatch_groups = np.flatnonzero(
                group_real_meaningful & ~group_real_match
            )
            remaining_samples = max(0, 50 - len(volume_mismatch_samples))
            for group_position in real_mismatch_groups[:remaining_samples]:
                position = int(coarse_groups[group_position])
                observed = float(observed_real_volume[group_position])
                reconstructed = float(reconstructed_real_volume[group_position])
                volume_mismatch_samples.append(
                    {
                        "coarse_open_utc": coarse.index[position].isoformat(),
                        "field": "RealVolume",
                        "coarse_value": observed,
                        "fine_sum": reconstructed,
                        "difference": observed - reconstructed,
                    }
                )

    partition_reports: Dict[str, Any] = {}
    all_failures: list[str] = []
    all_pending: list[str] = []
    minimum_bidirectional: Optional[float] = None
    for name, (start, end) in windows.items():
        start_ns = int(start.value)
        end_ns = int(end.value)
        trailing_partial_positions = np.empty(0, dtype=np.int64)
        trailing_partial_interval: Optional[Dict[str, str]] = None
        if target_tf == fine_tf:
            # The optimizer scores fully-contained target fine bars. Reconcile
            # every coarse reference interval containing one of those bars,
            # including an interval whose open precedes the partition boundary.
            fine_positions = np.flatnonzero(
                (fine_ns >= start_ns) & (fine_ends_ns <= end_ns)
            )
            if (
                not frozen_pre_seq2307
                and coarse_tf in {"M15", "M30", "H1"}
                and fine_positions.size
            ):
                # A common cutoff can close a target fine bar before the
                # containing native coarse bar is eligible to exist.  Such a
                # row is not evidence of a missing coarse bar: the reference
                # interval is still open.  Defer only unassigned rows in the
                # unique fixed-grid interval that straddles the trailing
                # cutoff and begins at or after the last authenticated coarse
                # close.  Internal gaps and any interval whose close is at or
                # before the cutoff remain in the comparison and fail closed.
                coarse_step_ns = int(coarse_seconds * 1_000_000_000)
                selected_bucket_open = (
                    fine_ns[fine_positions] // coarse_step_ns
                ) * coarse_step_ns
                selected_bucket_close = selected_bucket_open + coarse_step_ns
                defer_trailing_partial = (
                    (assigned_coarse[fine_positions] < 0)
                    & (selected_bucket_open < end_ns)
                    & (selected_bucket_close > end_ns)
                    & (selected_bucket_open >= int(coarse_ends_ns[-1]))
                )
                trailing_partial_positions = fine_positions[defer_trailing_partial]
                if trailing_partial_positions.size:
                    partial_open_ns = int(
                        selected_bucket_open[defer_trailing_partial][0]
                    )
                    partial_close_ns = partial_open_ns + coarse_step_ns
                    trailing_partial_interval = {
                        "open_utc": pd.Timestamp(partial_open_ns, tz="UTC").isoformat(),
                        "close_exclusive_utc": pd.Timestamp(
                            partial_close_ns, tz="UTC"
                        ).isoformat(),
                        "evidence_cutoff_utc": end.isoformat(),
                    }
                    fine_positions = fine_positions[~defer_trailing_partial]
            fine_step_ns = int(fine_seconds * 1_000_000_000)
            first_slot_offset = np.maximum(0, start_ns - coarse_ns)
            first_slot_index = (
                first_slot_offset + fine_step_ns - 1
            ) // fine_step_ns
            first_slot_open = coarse_ns + first_slot_index * fine_step_ns
            coarse_positions = np.flatnonzero(
                (coarse_ns < end_ns)
                & (coarse_ends_ns > start_ns)
                & (
                    first_slot_open + fine_step_ns
                    <= np.minimum(coarse_ends_ns, end_ns)
                )
            )
        else:
            # The optimizer scores fully-contained target coarse bars. A bar
            # straddling either boundary is excluded even if some reference
            # fine bars fall inside the wall-clock partition.
            coarse_positions = np.flatnonzero(
                (coarse_ns >= start_ns) & (coarse_ends_ns <= end_ns)
            )
            selected_coarse = np.zeros(len(coarse), dtype=bool)
            selected_coarse[coarse_positions] = True
            assigned_valid = assigned_coarse >= 0
            assigned_to_selected = np.zeros(len(fine), dtype=bool)
            assigned_to_selected[assigned_valid] = selected_coarse[
                assigned_coarse[assigned_valid]
            ]
            if coarse_tf in {"M15", "M30", "H1"}:
                coarse_step_ns = int(coarse_seconds * 1_000_000_000)
                nominal_bucket_open = (fine_ns // coarse_step_ns) * coarse_step_ns
                missing_target_candidates = (
                    (nominal_bucket_open >= start_ns)
                    & (nominal_bucket_open + coarse_step_ns <= end_ns)
                )
            else:
                # Broker-aligned H4/D1 opens are not safely derivable from UTC
                # modulo. Infer a missing target interval only when active fine
                # bars are bracketed by a previous authenticated coarse close
                # and a next coarse open, and that whole gap is contained in the
                # evidence partition. Genuine closures contain no fine activity;
                # a missing boundary-straddling target interval is not charged.
                missing_target_candidates = np.zeros(len(fine), dtype=bool)
                unassigned_positions = np.flatnonzero(assigned_coarse < 0)
                if unassigned_positions.size:
                    unassigned_ns = fine_ns[unassigned_positions]
                    previous_positions = (
                        np.searchsorted(
                            coarse_ends_ns,
                            unassigned_ns,
                            side="right",
                        )
                        - 1
                    )
                    next_positions = np.searchsorted(
                        coarse_ns,
                        unassigned_ns,
                        side="left",
                    )
                    bracketed = (
                        (previous_positions >= 0)
                        & (next_positions < len(coarse))
                    )
                    bracketed_indices = np.flatnonzero(bracketed)
                    if bracketed_indices.size:
                        previous_close = coarse_ends_ns[
                            previous_positions[bracketed_indices]
                        ]
                        next_open = coarse_ns[next_positions[bracketed_indices]]
                        fully_contained_gap = (
                            (previous_close >= start_ns)
                            & (next_open <= end_ns)
                            & (previous_close < next_open)
                        )
                        missing_target_candidates[
                            unassigned_positions[
                                bracketed_indices[fully_contained_gap]
                            ]
                        ] = True
            fine_positions = np.flatnonzero(
                assigned_to_selected | missing_target_candidates
            )
        fine_count = int(fine_positions.size)
        coarse_count = int(coarse_positions.size)
        partition_assignments = assigned_coarse[fine_positions]
        valid_partition_assignments = partition_assignments >= 0
        assignment_is_selected = np.zeros(fine_positions.size, dtype=bool)
        assignment_is_selected[valid_partition_assignments] = selected_coarse[
            partition_assignments[valid_partition_assignments]
        ] if target_tf == coarse_tf else np.isin(
            partition_assignments[valid_partition_assignments],
            coarse_positions,
            assume_unique=False,
        )
        assigned_count = int(assignment_is_selected.sum())
        unassigned_count = fine_count - assigned_count
        matched_coarse = int((member_counts[coarse_positions] > 0).sum())
        coarse_membership_ratio = matched_coarse / coarse_count if coarse_count else None
        fine_assignment_ratio = assigned_count / fine_count if fine_count else None
        unassigned_ratio = unassigned_count / fine_count if fine_count else None
        ohlc_compared = matched_coarse if tolerance is not None else 0
        ohlc_mismatches = (
            int(ohlc_mismatch[coarse_positions].sum()) if tolerance is not None else None
        )
        ohlc_mismatch_ratio = (
            float(ohlc_mismatches / ohlc_compared)
            if tolerance is not None and ohlc_compared
            else (1.0 if tolerance is not None and coarse_count else None)
        )
        tick_compared = matched_coarse if tick_volume_available else 0
        tick_matches = (
            int(tick_volume_match[coarse_positions].sum()) if tick_volume_available else 0
        )
        tick_mismatches = (
            coarse_count - tick_matches if tick_volume_available else None
        )
        tick_reconciliation_ratio = (
            tick_matches / coarse_count if tick_volume_available and coarse_count else None
        )
        meaningful_real_positions = coarse_positions[real_volume_meaningful[coarse_positions]]
        real_compared = int(meaningful_real_positions.size)
        real_mismatches = (
            int((~real_volume_match[meaningful_real_positions]).sum())
            if real_volume_available
            else None
        )
        real_reconciliation_ratio = (
            (real_compared - int(real_mismatches)) / real_compared
            if real_compared and real_mismatches is not None
            else None
        )
        coverage_candidates = [
            value
            for value in (
                coarse_membership_ratio,
                fine_assignment_ratio,
                tick_reconciliation_ratio,
                real_reconciliation_ratio,
            )
            if value is not None
        ]
        bidirectional = min(coverage_candidates) if coverage_candidates else None
        if bidirectional is not None:
            minimum_bidirectional = (
                bidirectional
                if minimum_bidirectional is None
                else min(minimum_bidirectional, bidirectional)
            )

        failures: list[str] = []
        pending: list[str] = []
        if coarse_count < int(thresholds.min_compared_native_bars):
            pending.append(
                f"PARTITION_OVERLAP_TOO_SMALL:{name}:{coarse_count}"
                f"<{int(thresholds.min_compared_native_bars)}"
            )
        if fine_count == 0:
            pending.append(f"FINE_REFERENCE_EMPTY:{name}")
        if tolerance is None:
            pending.append(f"TICK_TOLERANCE_UNAVAILABLE:{name}")
        if not tick_volume_available:
            pending.append(f"TICK_VOLUME_UNAVAILABLE:{name}")
        if invalid_close_rows:
            pending.append(f"AUTHENTICATED_BAR_CLOSE_INVALID:{invalid_close_rows}")
        if coarse_count - matched_coarse:
            failures.append(
                f"MISSING_COARSE_MEMBERSHIP:{name}:{coarse_count - matched_coarse}"
            )
        if unassigned_count:
            failures.append(f"UNASSIGNED_FINE_ROWS:{name}:{unassigned_count}")
        if (
            coarse_membership_ratio is not None
            and coarse_membership_ratio < float(thresholds.min_membership_ratio)
        ):
            failures.append(
                f"COARSE_MEMBERSHIP_RATIO_LOW:{name}:{coarse_membership_ratio:.6f}"
                f"<{float(thresholds.min_membership_ratio):.6f}"
            )
        if (
            unassigned_ratio is not None
            and unassigned_ratio > float(thresholds.max_unassigned_m5_ratio)
        ):
            failures.append(
                f"UNASSIGNED_FINE_RATIO_HIGH:{name}:{unassigned_ratio:.6f}"
                f">{float(thresholds.max_unassigned_m5_ratio):.6f}"
            )
        if (
            ohlc_mismatch_ratio is not None
            and ohlc_mismatch_ratio > float(thresholds.max_ohlc_mismatch_ratio)
        ):
            failures.append(
                f"OHLC_MISMATCH_RATIO_HIGH:{name}:{ohlc_mismatch_ratio:.6f}"
                f">{float(thresholds.max_ohlc_mismatch_ratio):.6f}"
            )
        if tick_mismatches:
            failures.append(f"TICK_VOLUME_MISMATCH:{name}:{int(tick_mismatches)}")
        if real_mismatches:
            failures.append(f"REAL_VOLUME_MISMATCH:{name}:{int(real_mismatches)}")

        partition_status = "FAIL" if failures else ("PENDING" if pending else "PASS")
        all_failures.extend(failures)
        all_pending.extend(pending)
        partition_reports[name] = {
            "status": partition_status,
            "reason_codes": failures + pending,
            "fine_rows": fine_count,
            "assigned_fine_rows": assigned_count,
            "unassigned_fine_rows": unassigned_count,
            "trailing_incomplete_coarse_fine_rows_deferred": int(
                trailing_partial_positions.size
            ),
            "trailing_incomplete_coarse_interval": trailing_partial_interval,
            "fine_assignment_ratio": fine_assignment_ratio,
            "unassigned_fine_ratio": unassigned_ratio,
            "coarse_bars": coarse_count,
            "matched_coarse_membership_bars": matched_coarse,
            "missing_coarse_membership_bars": coarse_count - matched_coarse,
            "coarse_membership_ratio": coarse_membership_ratio,
            "ohlc_compared_coarse_bars": ohlc_compared,
            "ohlc_mismatch_coarse_bars": ohlc_mismatches,
            "ohlc_mismatch_ratio": ohlc_mismatch_ratio,
            "tick_volume_compared_coarse_bars": tick_compared,
            "tick_volume_mismatch_coarse_bars": tick_mismatches,
            "tick_volume_reconciliation_ratio": tick_reconciliation_ratio,
            "real_volume_compared_coarse_bars": real_compared,
            "real_volume_mismatch_coarse_bars": real_mismatches,
            "real_volume_reconciliation_ratio": real_reconciliation_ratio,
            "bidirectional_coverage": bidirectional,
            "boundaries": _boundary_diagnostics(
                start=start,
                end=end,
                fine=fine,
                fine_positions=fine_positions,
                fine_seconds=fine_seconds,
                coarse=coarse,
                coarse_positions=coarse_positions,
                coarse_interval_ends_ns=coarse_ends_ns,
            ),
        }

    status = "FAIL" if all_failures else ("PENDING" if all_pending else "PASS")
    return _report(
        status,
        all_failures + all_pending,
        metrics={
            "partitions_evaluated": len(partition_reports),
            "partitions_passed": sum(
                report["status"] == "PASS" for report in partition_reports.values()
            ),
            "minimum_bidirectional_coverage": minimum_bidirectional,
        },
        partitions=partition_reports,
        diagnostics={
            "coarse_interval_method": interval_method,
            "invalid_authenticated_close_rows": invalid_close_rows,
            "interval_membership": "[coarse_open, authenticated_or_inferred_coarse_close)",
            "trailing_incomplete_coarse_policy": (
                "Defer only unassigned target-fine rows in a fixed-grid coarse "
                "interval whose close is after the evidence cutoff and whose "
                "open is at or after the last authenticated coarse close; "
                "internal and fully closed missing intervals remain failures."
            ),
            "ohlc_mismatch_samples": mismatch_samples,
            "volume_mismatch_samples": volume_mismatch_samples,
            "common_omission_caveat": (
                "An interval omitted identically by both broker timeframe series cannot be "
                "detected without an independent history or authenticated historical calendar."
            ),
        },
    )


def audit_normalized_native_cross_resolution(
    fine_frame: pd.DataFrame,
    coarse_frame: pd.DataFrame,
    *,
    symbol: str,
    fine_timeframe: str,
    coarse_timeframe: str,
    activation_target_timeframe: str,
    tick_tolerance: Optional[float],
    partition_windows: Optional[Mapping[str, Tuple[Any, Any]]] = None,
    thresholds: Optional[ParityThresholds] = None,
    compatibility_profile: str = CROSS_RESOLUTION_COMPATIBILITY_CURRENT,
) -> Dict[str, Any]:
    """Audit frames already normalized by the authenticated activation reader.

    A caller that does not meet the exact normalized-frame contract falls back
    to the public defensive path, preserving its validation semantics.
    """

    arguments = {
        "symbol": symbol,
        "fine_timeframe": fine_timeframe,
        "coarse_timeframe": coarse_timeframe,
        "activation_target_timeframe": activation_target_timeframe,
        "tick_tolerance": tick_tolerance,
        "partition_windows": partition_windows,
        "thresholds": thresholds,
        "compatibility_profile": compatibility_profile,
    }
    if not (
        _is_normalized_ohlc_frame(fine_frame)
        and _is_normalized_ohlc_frame(coarse_frame)
    ):
        return audit_native_cross_resolution(fine_frame, coarse_frame, **arguments)
    return audit_native_cross_resolution(
        _NormalizedOHLCFrame(fine_frame),
        _NormalizedOHLCFrame(coarse_frame),
        **arguments,
    )


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
