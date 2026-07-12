"""Canonical native-boundary transforms for semantic audit windows.

The acquisition generator and the offline activation auditor must apply the
same transform exactly once.  This module is intentionally dependency-free so
neither side imports the other and creates a contract cycle.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Final


TIMEFRAME_SECONDS: Final[dict[str, int]] = {
    "M5": 5 * 60,
    "M15": 15 * 60,
    "M30": 30 * 60,
    "H1": 60 * 60,
    "H4": 4 * 60 * 60,
    "D1": 24 * 60 * 60,
}
BROKER_OBSERVED_TIMEFRAMES: Final[frozenset[str]] = frozenset({"H4", "D1"})


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("native interval boundaries must be datetimes")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("native interval boundaries must be timezone-aware")
    return value.astimezone(timezone.utc)


def _floor_utc_boundary(value: datetime, seconds: int) -> datetime:
    value = _utc(value)
    epoch = int(value.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % int(seconds)), tz=timezone.utc)


def native_source_interval(
    timeframe: str,
    semantic_start: datetime,
    semantic_end_exclusive: datetime,
) -> tuple[datetime, datetime]:
    """Transform one semantic target window to exact native source boundaries.

    Fixed intraday bars use fully closed bar-open boundaries, so both ends are
    floored.  Broker-session-aligned H4/D1 bars retain the established
    predecessor/successor sentinel contract used to observe shifted opens.
    """

    normalized = str(timeframe or "").strip().upper()
    if normalized not in TIMEFRAME_SECONDS:
        raise ValueError(f"unsupported native timeframe: {timeframe!r}")
    start = _utc(semantic_start)
    end = _utc(semantic_end_exclusive)
    if start >= end:
        raise ValueError("semantic native interval must be non-empty")
    seconds = TIMEFRAME_SECONDS[normalized]
    if normalized in BROKER_OBSERVED_TIMEFRAMES:
        return (
            start - timedelta(seconds=seconds),
            end + timedelta(seconds=1),
        )
    transformed = (
        _floor_utc_boundary(start, seconds),
        _floor_utc_boundary(end, seconds),
    )
    if transformed[0] >= transformed[1]:
        raise ValueError("semantic interval contains no fully closed native bar")
    return transformed


__all__ = [
    "BROKER_OBSERVED_TIMEFRAMES",
    "TIMEFRAME_SECONDS",
    "native_source_interval",
]
