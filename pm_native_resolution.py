"""Per-cell resolution and cross-timeframe identity audit for native bars.

MetaTrader accepting an M5 identifier does not prove that the broker supplied
M5-resolution history.  Some symbols expose one daily-like record through an
intraday request.  These checks use timestamps and OHLC identity only; no
strategy result or return can affect source eligibility.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


RESOLUTION_AUDIT_SCHEMA_VERSION = "fxpm-native-resolution-v1"
TIMEFRAME_SECONDS = {
    "M5": 300,
    "M15": 900,
    "M30": 1_800,
    "H1": 3_600,
    "H4": 14_400,
    "D1": 86_400,
}
OHLC = ("Open", "High", "Low", "Close")


@dataclass(frozen=True)
class ResolutionAudit:
    symbol: str
    timeframe: str
    status: str
    reason_codes: Tuple[str, ...]
    rows: int
    active_sessions: int = 0
    median_bars_per_active_session: Optional[float] = None
    nominal_or_faster_spacing_ratio: Optional[float] = None
    daily_identity_overlap_rows: int = 0
    daily_timestamp_identity_ratio: Optional[float] = None
    daily_ohlc_identity_ratio: Optional[float] = None
    yearly_diagnostics: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    schema_version: str = RESOLUTION_AUDIT_SCHEMA_VERSION

    @property
    def passed(self) -> bool:
        return self.status == "PASS"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _normalize(frame: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if frame is None or frame.empty:
        return None
    local = frame
    if not isinstance(local.index, pd.DatetimeIndex):
        aliases = {str(column).lower(): column for column in local.columns}
        time_column = aliases.get("time") or aliases.get("datetime") or aliases.get("date")
        if time_column is None:
            return None
        local = local.copy(deep=False)
        local.index = pd.to_datetime(local[time_column], utc=True, errors="coerce")
    else:
        index = pd.to_datetime(local.index, utc=True, errors="coerce")
        if not pd.DatetimeIndex(local.index).equals(pd.DatetimeIndex(index)):
            local = local.copy(deep=False)
            local.index = index
    local = local.loc[~local.index.isna()]
    if local.index.has_duplicates:
        local = local.loc[~local.index.duplicated(keep="last")]
    if not local.index.is_monotonic_increasing:
        local = local.sort_index()
    return local


def _spacing_ratio(index: pd.DatetimeIndex, seconds: int) -> Optional[float]:
    if len(index) < 2:
        return None
    delta_seconds = np.diff(index.asi8).astype(np.float64) / 1_000_000_000.0
    positive = delta_seconds[delta_seconds > 0.0]
    if positive.size == 0:
        return None
    return float(np.mean(positive <= float(seconds) * 1.5))


def _daily_identity(
    frame: pd.DataFrame,
    daily: Optional[pd.DataFrame],
    *,
    tick_tolerance: Optional[float],
) -> tuple[int, Optional[float], Optional[float]]:
    if daily is None or daily.empty:
        return 0, None, None
    overlap = frame.index.intersection(daily.index)
    if len(overlap) == 0:
        return 0, 0.0, None
    timestamp_ratio = float(len(overlap) / max(1, min(len(frame), len(daily))))
    if not all(column in frame.columns and column in daily.columns for column in OHLC):
        return int(len(overlap)), timestamp_ratio, None
    try:
        tolerance = float(tick_tolerance) if tick_tolerance is not None else 0.0
    except (TypeError, ValueError, OverflowError):
        tolerance = 0.0
    if not math.isfinite(tolerance) or tolerance < 0.0:
        tolerance = 0.0
    left = frame.loc[overlap, list(OHLC)].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    right = daily.loc[overlap, list(OHLC)].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    finite = np.isfinite(left).all(axis=1) & np.isfinite(right).all(axis=1)
    if not finite.any():
        return int(len(overlap)), timestamp_ratio, None
    identical = np.max(np.abs(left[finite] - right[finite]), axis=1) <= tolerance + 1e-12
    return int(len(overlap)), timestamp_ratio, float(np.mean(identical))


def audit_native_resolution(
    frame: Optional[pd.DataFrame],
    *,
    symbol: str,
    timeframe: str,
    daily_frame: Optional[pd.DataFrame] = None,
    tick_tolerance: Optional[float] = None,
    minimum_rows: int = 20,
    daily_like_session_bar_ceiling: float = 1.25,
    daily_identity_threshold: float = 0.95,
) -> ResolutionAudit:
    """Audit one requested timeframe without making a profitability claim."""
    tf = str(timeframe or "").strip().upper()
    normalized = _normalize(frame)
    daily = _normalize(daily_frame)
    if tf not in TIMEFRAME_SECONDS:
        return ResolutionAudit(
            symbol=str(symbol).upper(),
            timeframe=tf,
            status="FAIL",
            reason_codes=("UNSUPPORTED_TIMEFRAME",),
            rows=0 if normalized is None else len(normalized),
        )
    rows = 0 if normalized is None else int(len(normalized))
    if normalized is None or rows < int(minimum_rows):
        return ResolutionAudit(
            symbol=str(symbol).upper(),
            timeframe=tf,
            status="PENDING",
            reason_codes=(f"RESOLUTION_SAMPLE_TOO_SMALL:{rows}<{int(minimum_rows)}",),
            rows=rows,
        )

    session_counts = pd.Series(1, index=normalized.index).groupby(normalized.index.normalize()).sum()
    median_session = float(session_counts.median()) if len(session_counts) else None
    spacing = _spacing_ratio(normalized.index, TIMEFRAME_SECONDS[tf])
    overlap_rows, timestamp_identity, ohlc_identity = _daily_identity(
        normalized,
        daily,
        tick_tolerance=tick_tolerance,
    )
    yearly: dict[str, dict[str, Any]] = {}
    for year, group in normalized.groupby(normalized.index.year):
        counts = pd.Series(1, index=group.index).groupby(group.index.normalize()).sum()
        yearly[str(int(year))] = {
            "rows": int(len(group)),
            "active_sessions": int(len(counts)),
            "median_bars_per_active_session": float(counts.median()) if len(counts) else None,
            "nominal_or_faster_spacing_ratio": _spacing_ratio(group.index, TIMEFRAME_SECONDS[tf]),
        }

    reasons: list[str] = []
    if tf != "D1":
        daily_rows = 0 if daily is None else int(len(daily))
        material_years = [
            value for value in yearly.values() if int(value.get("rows", 0) or 0) >= minimum_rows
        ]
        daily_like_years = [
            year
            for year, value in yearly.items()
            if int(value.get("rows", 0) or 0) >= minimum_rows
            and value.get("median_bars_per_active_session") is not None
            and float(value["median_bars_per_active_session"]) <= daily_like_session_bar_ceiling
            and float(value.get("nominal_or_faster_spacing_ratio") or 0.0) < 0.10
        ]
        if material_years and daily_like_years:
            reasons.append(
                "INTRADAY_RESOLUTION_DAILY_LIKE_YEARS:" + ",".join(daily_like_years)
            )
        if (
            median_session is not None
            and median_session <= daily_like_session_bar_ceiling
            and (spacing is None or spacing < 0.10)
        ):
            reasons.append(
                f"INTRADAY_RESOLUTION_DAILY_LIKE:median_session_bars={median_session:.3f}"
            )
        if (
            overlap_rows >= minimum_rows
            and timestamp_identity is not None
            and ohlc_identity is not None
            and timestamp_identity >= daily_identity_threshold
            and ohlc_identity >= daily_identity_threshold
            and abs(len(normalized) - daily_rows) <= max(2, int(daily_rows * 0.02))
        ):
            reasons.append(
                "INTRADAY_D1_CROSS_TIMEFRAME_IDENTITY:"
                f"timestamps={timestamp_identity:.6f}:ohlc={ohlc_identity:.6f}"
            )

    return ResolutionAudit(
        symbol=str(symbol).upper(),
        timeframe=tf,
        status="FAIL" if reasons else "PASS",
        reason_codes=tuple(dict.fromkeys(reasons)),
        rows=rows,
        active_sessions=int(len(session_counts)),
        median_bars_per_active_session=median_session,
        nominal_or_faster_spacing_ratio=spacing,
        daily_identity_overlap_rows=overlap_rows,
        daily_timestamp_identity_ratio=timestamp_identity,
        daily_ohlc_identity_ratio=ohlc_identity,
        yearly_diagnostics=yearly,
    )


__all__ = [
    "RESOLUTION_AUDIT_SCHEMA_VERSION",
    "ResolutionAudit",
    "TIMEFRAME_SECONDS",
    "audit_native_resolution",
]
