"""Bounded single-pass validation of immutable native-history CSV bytes.

This module is deliberately independent of acquisition and reseal filesystem
policy.  Its caller must authenticate the binary stream, common request, and
pagination ledger before use.  The scanner consumes the stream once, requires
an exact pinned byte count, retains only one prior row plus O(page-count)
accumulators, and recomputes the artifact and writer-quality facts from bytes.
"""

from __future__ import annotations

import csv
import hashlib
import math
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, BinaryIO, Final, Mapping

from pm_native_request_contract import (
    MAX_SOURCE_QUALITY_OMISSIONS_PER_CELL,
    PRODUCTION_ARTIFACT_COLUMNS,
    SOURCE_QUALITY_OMISSION_OPEN_UTC,
    TIMEFRAME_SECONDS,
    NativeRequestContractError,
    V5ArtifactObservation,
    build_v5_artifact_observation,
)


UTC: Final = timezone.utc
CSV_HEADER: Final[tuple[str, ...]] = ("time", *PRODUCTION_ARTIFACT_COLUMNS)
MAX_CSV_BYTES: Final = 64 * 1024 * 1024 * 1024
MAX_CSV_ROWS: Final = 50_000_000
MAX_CSV_LINE_BYTES: Final = 16 * 1024
MAX_PAGE_LEDGER_ENTRIES: Final = 10_000
MAX_MT5_DISCRETE_VALUE: Final = 2**63 - 1

_CSV_TIMESTAMP_RE: Final = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})"
    r"(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$"
)
_UTC_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)


class NativeCsvValidationError(ValueError):
    """A deterministic scanner failure with a consumer-neutral reason code."""

    def __init__(self, code: str):
        self.code = str(code)
        super().__init__(f"NATIVE_CSV_{self.code}")


def _fail(code: str) -> None:
    raise NativeCsvValidationError(code)


@dataclass(frozen=True)
class NativeCsvScanResult:
    """Immutable byte-derived artifact evidence and exact writer quality facts."""

    artifact: V5ArtifactObservation
    quality: Mapping[str, Any]


def _timestamp_ns(
    value: Any,
    *,
    code: str,
    allow_fraction: bool,
) -> tuple[int, str]:
    if not isinstance(value, str) or len(value) > 64:
        _fail(code)
    match = _CSV_TIMESTAMP_RE.fullmatch(value)
    if match is None:
        _fail(code)
    fraction = match.group(3) or ""
    if not allow_fraction and fraction and int(fraction) != 0:
        _fail(code)
    timezone_text = "+00:00" if match.group(4) == "Z" else match.group(4)
    try:
        base = datetime.fromisoformat(
            f"{match.group(1)}T{match.group(2)}{timezone_text}"
        ).astimezone(UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise NativeCsvValidationError(code) from exc
    delta = base - _UTC_EPOCH
    nanoseconds = (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + int(
        fraction.ljust(9, "0") or "0"
    )
    return nanoseconds, base.isoformat().replace("+00:00", "Z")


def _source_interval(
    common_request: Mapping[str, Any], timeframe: str
) -> tuple[int, int, str, str]:
    intervals = common_request.get("timeframe_source_request_intervals")
    interval = intervals.get(timeframe) if isinstance(intervals, Mapping) else None
    if not isinstance(interval, Mapping) or set(interval) != {
        "start_utc",
        "end_exclusive_utc",
    }:
        _fail("SOURCE_INTERVAL_INVALID")
    start_ns, start = _timestamp_ns(
        interval.get("start_utc"),
        code="SOURCE_INTERVAL_INVALID",
        allow_fraction=False,
    )
    end_ns, end = _timestamp_ns(
        interval.get("end_exclusive_utc"),
        code="SOURCE_INTERVAL_INVALID",
        allow_fraction=False,
    )
    if start_ns >= end_ns:
        _fail("SOURCE_INTERVAL_INVALID")
    return start_ns, end_ns, start, end


def _page_intervals(
    pagination: Mapping[str, Any],
    *,
    expected_start_ns: int,
    expected_end_ns: int,
) -> list[tuple[int, int, str, str]]:
    attempts = pagination.get("page_attempts")
    if (
        not isinstance(attempts, list)
        or not attempts
        or len(attempts) > MAX_PAGE_LEDGER_ENTRIES
    ):
        _fail("PAGE_LEDGER_INVALID")
    intervals: list[tuple[int, int, str, str]] = []
    previous_end: int | None = None
    for index, attempt in enumerate(attempts):
        if not isinstance(attempt, Mapping):
            _fail(f"PAGE_LEDGER_INVALID:{index}")
        start_ns, start = _timestamp_ns(
            attempt.get("start_utc"),
            code=f"PAGE_LEDGER_INVALID:{index}",
            allow_fraction=False,
        )
        end_ns, end = _timestamp_ns(
            attempt.get("end_exclusive_utc"),
            code=f"PAGE_LEDGER_INVALID:{index}",
            allow_fraction=False,
        )
        if start_ns >= end_ns or (
            previous_end is not None and start_ns != previous_end
        ):
            _fail(f"PAGE_LEDGER_ORDER_INVALID:{index}")
        intervals.append((start_ns, end_ns, start, end))
        previous_end = end_ns
    if intervals[0][0] != expected_start_ns or intervals[-1][1] != expected_end_ns:
        _fail("PAGE_LEDGER_COVERAGE_INVALID")
    return intervals


def _finite_number(value: str) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _integral_numeric_decimal(value: str) -> Decimal | None:
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        return None
    return parsed


def scan_native_csv_stream(
    stream: BinaryIO,
    *,
    expected_bytes: int,
    timeframe: str,
    common_request: Mapping[str, Any],
    pagination: Mapping[str, Any],
    maximum_bytes: int = MAX_CSV_BYTES,
    maximum_rows: int = MAX_CSV_ROWS,
    maximum_line_bytes: int = MAX_CSV_LINE_BYTES,
) -> NativeCsvScanResult:
    """Consume one pinned stream and recompute its complete V5 byte evidence.

    ``common_request`` and ``pagination`` are authority inputs, not facts
    inferred from the CSV.  Consumers must first bind them to their trusted run
    contract.  This function enforces their exact source/page interval equality
    while deriving all artifact observations solely from the supplied bytes.
    """

    if not isinstance(timeframe, str) or timeframe not in TIMEFRAME_SECONDS:
        _fail("TIMEFRAME_INVALID")
    if not isinstance(common_request, Mapping):
        _fail("COMMON_REQUEST_INVALID")
    if not isinstance(pagination, Mapping):
        _fail("PAGE_LEDGER_INVALID")
    if (
        type(expected_bytes) is not int
        or expected_bytes <= 0
        or type(maximum_bytes) is not int
        or maximum_bytes <= 0
        or maximum_bytes > MAX_CSV_BYTES
        or expected_bytes > maximum_bytes
    ):
        _fail("BARS_SIZE_INVALID")
    if type(maximum_rows) is not int or not 0 < maximum_rows <= MAX_CSV_ROWS:
        _fail("BARS_ROW_CAP_INVALID")
    if (
        type(maximum_line_bytes) is not int
        or not 0 < maximum_line_bytes <= MAX_CSV_LINE_BYTES
    ):
        _fail("BARS_LINE_CAP_INVALID")
    if not callable(getattr(stream, "readline", None)):
        _fail("BARS_STREAM_INVALID")
    try:
        position = stream.tell()
    except TypeError as exc:
        raise NativeCsvValidationError("BARS_STREAM_INVALID") from exc
    except (AttributeError, OSError, ValueError):
        if bool(getattr(stream, "closed", False)):
            _fail("BARS_STREAM_INVALID")
        position = 0
    if position != 0:
        _fail("BARS_STREAM_NOT_AT_START")

    source_start_ns, source_end_ns, _, _ = _source_interval(common_request, timeframe)
    omission_target_open_ns, _ = _timestamp_ns(
        SOURCE_QUALITY_OMISSION_OPEN_UTC,
        code="SOURCE_QUALITY_OMISSION_TIME_INVALID",
        allow_fraction=False,
    )
    page_intervals = _page_intervals(
        pagination,
        expected_start_ns=source_start_ns,
        expected_end_ns=source_end_ns,
    )
    page_rows = [0] * len(page_intervals)
    page_first: list[str | None] = [None] * len(page_intervals)
    page_last: list[str | None] = [None] * len(page_intervals)
    as_of_ns, _ = _timestamp_ns(
        common_request.get("common_as_of_utc"),
        code="AS_OF_INVALID",
        allow_fraction=False,
    )
    nominal_ns = TIMEFRAME_SECONDS[timeframe] * 1_000_000_000
    broker_session_aligned = timeframe in {"H4", "D1"}

    digest = hashlib.sha256()
    rows = 0
    bytes_read = 0
    first_open: str | None = None
    last_open: str | None = None
    last_close: str | None = None
    previous_open_ns: int | None = None
    previous_close_ns: int | None = None
    previous_close_intrinsic_invalid = False
    page_cursor = 0
    duplicate_open_count = 0
    non_monotonic_open_count = 0
    invalid_bar_close_count = 0
    nonfinite_ohlc_count = 0
    invalid_ohlc_count = 0
    missing_required_value_count = 0
    out_of_request_count = 0
    page_assignment_error_count = 0
    nonpositive_price_count = 0
    negative_volume_count = 0
    negative_spread_count = 0
    source_quality_omission_target_open_count = 0
    off_native_grid_rows = 0
    cadence_break_count = 0
    nominal_missing_continuous_slots = 0
    largest_interbar_gap_seconds = 0.0

    line_number = 0
    while True:
        try:
            raw = stream.readline(maximum_line_bytes + 1)
        except (OSError, TypeError, ValueError) as exc:
            raise NativeCsvValidationError("BARS_READ_FAILED") from exc
        if not raw:
            break
        if not isinstance(raw, bytes):
            _fail("BARS_STREAM_NOT_BINARY")
        line_number += 1
        if len(raw) > maximum_line_bytes:
            _fail("BARS_LINE_CAP_EXCEEDED")
        bytes_read += len(raw)
        if bytes_read > expected_bytes or bytes_read > maximum_bytes:
            _fail("BARS_SIZE_CHANGED")
        digest.update(raw)
        try:
            decoded = raw.decode("utf-8")
            parsed = next(csv.reader([decoded], strict=True))
        except (UnicodeDecodeError, csv.Error, StopIteration) as exc:
            raise NativeCsvValidationError("BARS_CSV_INVALID") from exc
        if line_number == 1:
            if tuple(parsed) != CSV_HEADER:
                _fail("BARS_HEADER_INVALID")
            continue
        if len(parsed) != len(CSV_HEADER):
            _fail("BARS_COLUMN_COUNT_INVALID")

        rows += 1
        if rows > maximum_rows:
            _fail("BARS_ROW_CAP_EXCEEDED")
        open_ns, opened = _timestamp_ns(
            parsed[0], code="BAR_OPEN_INVALID", allow_fraction=False
        )
        if open_ns == omission_target_open_ns:
            source_quality_omission_target_open_count += 1
        source_first_ns, _ = _timestamp_ns(
            parsed[8], code="SOURCE_FIRST_INVALID", allow_fraction=False
        )
        source_last_ns, _ = _timestamp_ns(
            parsed[9], code="SOURCE_LAST_INVALID", allow_fraction=True
        )
        close_ns, closed = _timestamp_ns(
            parsed[10], code="BAR_CLOSE_INVALID", allow_fraction=False
        )

        if first_open is None:
            first_open = opened
        last_open = opened
        last_close = closed
        if previous_open_ns is not None:
            delta_ns = open_ns - previous_open_ns
            delta_seconds = delta_ns / 1_000_000_000.0
            largest_interbar_gap_seconds = max(
                largest_interbar_gap_seconds, delta_seconds
            )
            if delta_seconds > TIMEFRAME_SECONDS[timeframe] * 1.5:
                cadence_break_count += 1
                nominal_missing_continuous_slots += max(
                    int(round(delta_seconds / TIMEFRAME_SECONDS[timeframe])) - 1,
                    0,
                )
            if open_ns == previous_open_ns:
                duplicate_open_count += 1
            elif open_ns < previous_open_ns:
                non_monotonic_open_count += 1
            if broker_session_aligned and previous_close_ns is not None:
                expected_close_ns = (
                    open_ns
                    if nominal_ns // 2 <= delta_ns <= nominal_ns * 3 // 2
                    else previous_open_ns + nominal_ns
                )
                if (
                    previous_close_intrinsic_invalid
                    or previous_close_ns != expected_close_ns
                ):
                    invalid_bar_close_count += 1
        close_intrinsic_invalid = (
            close_ns <= open_ns
            or source_first_ns != open_ns
            or source_last_ns != close_ns - 1
        )
        if not broker_session_aligned and (
            close_intrinsic_invalid or close_ns != open_ns + nominal_ns
        ):
            invalid_bar_close_count += 1

        numeric = [_finite_number(item) for item in parsed[1:8]]
        if any(item is None or not math.isfinite(item) for item in numeric):
            missing_required_value_count += 1
        ohlc = numeric[:4]
        if any(item is None or not math.isfinite(item) for item in ohlc):
            nonfinite_ohlc_count += 1
        else:
            open_price, high, low, close_price = ohlc
            assert open_price is not None
            assert high is not None
            assert low is not None
            assert close_price is not None
            if high < max(open_price, close_price) or low > min(
                open_price, close_price
            ):
                invalid_ohlc_count += 1
            if min(open_price, high, low, close_price) <= 0.0:
                nonpositive_price_count += 1
        volume, spread, real_volume = numeric[4:]
        for raw_value, parsed_value in zip(parsed[5:8], numeric[4:], strict=True):
            if parsed_value is not None and math.isfinite(parsed_value):
                exact_value = _integral_numeric_decimal(raw_value)
                if exact_value is None:
                    _fail("BARS_NONINTEGRAL_MT5_METRIC")
                if not 0 <= exact_value <= MAX_MT5_DISCRETE_VALUE:
                    _fail("BARS_MT5_METRIC_OUT_OF_RANGE")
        if (volume is not None and math.isfinite(volume) and volume < 0.0) or (
            real_volume is not None and math.isfinite(real_volume) and real_volume < 0.0
        ):
            negative_volume_count += 1
        if spread is not None and math.isfinite(spread) and spread < 0.0:
            negative_spread_count += 1
        off_native_grid = (open_ns // 1_000_000_000) % TIMEFRAME_SECONDS[timeframe] != 0
        if off_native_grid:
            off_native_grid_rows += 1
            if not broker_session_aligned:
                _fail("BARS_FIXED_CLOCK_OFF_NATIVE_GRID")
        if not (
            source_start_ns <= open_ns < source_end_ns
            and open_ns < close_ns <= as_of_ns
        ):
            out_of_request_count += 1
        while (
            page_cursor < len(page_intervals)
            and open_ns >= page_intervals[page_cursor][1]
        ):
            page_cursor += 1
        if (
            page_cursor >= len(page_intervals)
            or open_ns < page_intervals[page_cursor][0]
        ):
            page_assignment_error_count += 1
        else:
            page_rows[page_cursor] += 1
            if page_first[page_cursor] is None:
                page_first[page_cursor] = opened
            page_last[page_cursor] = opened
        previous_open_ns = open_ns
        previous_close_ns = close_ns
        previous_close_intrinsic_invalid = close_intrinsic_invalid

    if broker_session_aligned and previous_open_ns is not None:
        expected_final_close_ns = previous_open_ns + nominal_ns
        attempts = pagination.get("page_attempts")
        final_attempt = (
            attempts[-1] if isinstance(attempts, list) and attempts else None
        )
        diagnostics = (
            final_attempt.get("connector_diagnostics")
            if isinstance(final_attempt, Mapping)
            else None
        )
        admitted_count = (
            diagnostics.get("admitted_count")
            if isinstance(diagnostics, Mapping)
            else None
        )
        admitted_extent = (
            diagnostics.get("admitted_extent")
            if isinstance(diagnostics, Mapping)
            else None
        )
        raw_final_omissions = (
            final_attempt.get("source_quality_omissions")
            if isinstance(final_attempt, Mapping)
            else None
        )
        final_omission_count = (
            len(raw_final_omissions)
            if isinstance(raw_final_omissions, list)
            and 0 < len(raw_final_omissions) <= MAX_SOURCE_QUALITY_OMISSIONS_PER_CELL
            else 0
        )
        removed_successors = (
            admitted_count - page_rows[-1] - final_omission_count
            if type(admitted_count) is int and page_rows
            else None
        )
        if removed_successors == 1 and isinstance(admitted_extent, Mapping):
            successor_second = admitted_extent.get("last_epoch_second")
            if type(successor_second) is not int:
                invalid_bar_close_count += 1
            else:
                successor_ns = successor_second * 1_000_000_000
                delta_ns = successor_ns - previous_open_ns
                final_page_end_ns = page_intervals[-1][1]
                if not (
                    successor_ns > previous_open_ns
                    and successor_ns < final_page_end_ns + 1_000_000_000
                    and successor_ns <= as_of_ns
                    and successor_ns + nominal_ns > as_of_ns
                ):
                    invalid_bar_close_count += 1
                elif nominal_ns // 2 <= delta_ns <= nominal_ns * 3 // 2:
                    expected_final_close_ns = successor_ns
        elif removed_successors != 0:
            invalid_bar_close_count += 1
        if (
            previous_close_intrinsic_invalid
            or previous_close_ns != expected_final_close_ns
        ):
            invalid_bar_close_count += 1

    if (
        bytes_read != expected_bytes
        or rows <= 0
        or first_open is None
        or last_open is None
        or last_close is None
    ):
        _fail("BARS_EMPTY_OR_SIZE_INVALID")

    page_observations = [
        {
            "start_utc": start,
            "end_exclusive_utc": end,
            "rows": page_rows[index],
            "actual_first_open_utc": page_first[index],
            "actual_last_open_utc": page_last[index],
        }
        for index, (_, _, start, end) in enumerate(page_intervals)
    ]
    try:
        artifact = build_v5_artifact_observation(
            sha256=digest.hexdigest(),
            bytes=bytes_read,
            rows=rows,
            first_open_utc=first_open,
            last_open_utc=last_open,
            last_close_utc=last_close,
            columns=PRODUCTION_ARTIFACT_COLUMNS,
            page_observations=page_observations,
            duplicate_open_count=duplicate_open_count,
            non_monotonic_open_count=non_monotonic_open_count,
            invalid_bar_close_count=invalid_bar_close_count,
            nonfinite_ohlc_count=nonfinite_ohlc_count,
            invalid_ohlc_count=invalid_ohlc_count,
            missing_required_value_count=missing_required_value_count,
            out_of_request_count=out_of_request_count,
            page_assignment_error_count=page_assignment_error_count,
            nonpositive_price_count=nonpositive_price_count,
            negative_volume_count=negative_volume_count,
            negative_spread_count=negative_spread_count,
            source_quality_omission_target_open_count=(
                source_quality_omission_target_open_count
            ),
        )
    except NativeRequestContractError as exc:
        detail = exc.errors[0] if exc.errors else "UNKNOWN"
        raise NativeCsvValidationError(
            f"ARTIFACT_OBSERVATION_INVALID:{detail}"
        ) from exc

    expected_slots = max(0, int((source_end_ns - source_start_ns) // nominal_ns))
    quality = MappingProxyType(
        {
            "ohlc_invariants_valid": bool(
                nonfinite_ohlc_count == 0 and invalid_ohlc_count == 0
            ),
            "invalid_ohlc_rows": nonfinite_ohlc_count + invalid_ohlc_count,
            "monotonic_increasing": non_monotonic_open_count == 0,
            "unique_timestamps": duplicate_open_count == 0,
            "off_native_grid_rows": off_native_grid_rows,
            "expected_continuous_calendar_slots": expected_slots,
            "continuous_calendar_slot_density": (
                float(rows / expected_slots) if expected_slots > 0 else None
            ),
            "cadence_break_count": cadence_break_count,
            "nominal_missing_continuous_slots": (nominal_missing_continuous_slots),
            "largest_interbar_gap_seconds": largest_interbar_gap_seconds,
        }
    )
    return NativeCsvScanResult(artifact=artifact, quality=quality)


__all__ = [
    "CSV_HEADER",
    "MAX_CSV_BYTES",
    "MAX_CSV_LINE_BYTES",
    "MAX_CSV_ROWS",
    "MAX_MT5_DISCRETE_VALUE",
    "MAX_PAGE_LEDGER_ENTRIES",
    "NativeCsvScanResult",
    "NativeCsvValidationError",
    "scan_native_csv_stream",
]
