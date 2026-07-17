from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any

import pytest

from pm_native_csv_validation import (
    MAX_CSV_LINE_BYTES,
    NativeCsvValidationError,
    scan_native_csv_stream,
)


UTC = timezone.utc
HEADER = (
    "time,Open,High,Low,Close,Volume,Spread,RealVolume,"
    "_SOURCE_FIRST_TS,_SOURCE_LAST_TS,_BAR_CLOSE_TS\n"
)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _source_last(value: datetime) -> str:
    previous = value.astimezone(UTC) - timedelta(seconds=1)
    return previous.strftime("%Y-%m-%dT%H:%M:%S") + ".999999999+00:00"


def _bars(*rows: tuple[datetime, datetime]) -> bytes:
    values = [HEADER]
    for opened, closed in rows:
        values.append(
            ",".join(
                [
                    _iso(opened),
                    "1.00000",
                    "1.00020",
                    "0.99980",
                    "1.00010",
                    "1",
                    "0",
                    "1",
                    _iso(opened),
                    _source_last(closed),
                    _iso(closed),
                ]
            )
            + "\n"
        )
    return "".join(values).encode("utf-8")


def _contract(
    *,
    timeframe: str,
    start: datetime,
    end: datetime,
    as_of: datetime,
    admitted_count: int,
    admitted_last: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    common = {
        "common_as_of_utc": _iso(as_of),
        "timeframe_source_request_intervals": {
            timeframe: {
                "start_utc": _iso(start),
                "end_exclusive_utc": _iso(end),
            }
        },
    }
    pagination = {
        "page_attempts": [
            {
                "start_utc": _iso(start),
                "end_exclusive_utc": _iso(end),
                "connector_diagnostics": {
                    "admitted_count": admitted_count,
                    "admitted_extent": {
                        "rows": admitted_count,
                        "first_epoch_second": int(start.timestamp()),
                        "last_epoch_second": int(admitted_last.timestamp()),
                    },
                },
            }
        ]
    }
    return common, pagination


class _TrackingBytesIO(BytesIO):
    def __init__(self, value: bytes) -> None:
        super().__init__(value)
        self.read_sizes: list[int] = []

    def readline(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        return super().readline(size)


class _TellTypeErrorBytesIO(BytesIO):
    def tell(self) -> int:
        raise TypeError("malformed tell implementation")


class _ReadlineTypeErrorBytesIO(BytesIO):
    def readline(self, size: int = -1) -> bytes:
        raise TypeError("malformed readline implementation")


def test_scanner_recomputes_exact_artifact_and_quality_with_bounded_reads() -> None:
    opened = datetime(2026, 7, 13, 8, 55, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    raw = _bars((opened, closed))
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )
    stream = _TrackingBytesIO(raw)

    result = scan_native_csv_stream(
        stream,
        expected_bytes=len(raw),
        timeframe="M5",
        common_request=common,
        pagination=pagination,
    )

    assert result.artifact.bytes == len(raw)
    assert result.artifact.rows == 1
    assert result.artifact.first_open_utc == _iso(opened)
    assert result.artifact.last_close_utc == _iso(closed)
    assert result.artifact.invalid_bar_close_count == 0
    assert result.quality["ohlc_invariants_valid"] is True
    assert result.quality["continuous_calendar_slot_density"] == 1.0
    assert stream.read_sizes
    assert set(stream.read_sizes) == {MAX_CSV_LINE_BYTES + 1}


def test_scanner_counts_source_quality_omission_target_open_as_neutral_fact() -> None:
    opened = datetime(2019, 11, 25, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    raw = _bars((opened, closed))
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )

    result = scan_native_csv_stream(
        BytesIO(raw),
        expected_bytes=len(raw),
        timeframe="M5",
        common_request=common,
        pagination=pagination,
    )

    assert result.artifact.source_quality_omission_target_open_count == 1
    assert result.artifact.invalid_ohlc_count == 0
    assert result.artifact.nonpositive_price_count == 0
    assert result.quality["ohlc_invariants_valid"] is True


def test_scanner_reports_zero_when_source_quality_omission_target_is_absent() -> None:
    opened = datetime(2019, 11, 25, 0, 5, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    raw = _bars((opened, closed))
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )

    result = scan_native_csv_stream(
        BytesIO(raw),
        expected_bytes=len(raw),
        timeframe="M5",
        common_request=common,
        pagination=pagination,
    )

    assert result.artifact.source_quality_omission_target_open_count == 0
    assert result.quality["ohlc_invariants_valid"] is True


@pytest.mark.parametrize(
    ("timeframe", "opened", "successor", "expected_close"),
    [
        (
            "H4",
            datetime(2026, 7, 13, 5, 0, tzinfo=UTC),
            datetime(2026, 7, 13, 8, 0, tzinfo=UTC),
            datetime(2026, 7, 13, 8, 0, tzinfo=UTC),
        ),
        (
            "D1",
            datetime(2026, 7, 10, 8, 0, tzinfo=UTC),
            datetime(2026, 7, 13, 8, 0, tzinfo=UTC),
            datetime(2026, 7, 11, 8, 0, tzinfo=UTC),
        ),
    ],
)
def test_broker_session_close_uses_regular_forming_successor_or_nominal_gap(
    timeframe: str,
    opened: datetime,
    successor: datetime,
    expected_close: datetime,
) -> None:
    as_of = datetime(2026, 7, 13, 9, 0, tzinfo=UTC)
    start = datetime(2026, 7, 9, 0, 0, tzinfo=UTC)
    end = as_of + timedelta(seconds=1)
    raw = _bars((opened, expected_close))
    common, pagination = _contract(
        timeframe=timeframe,
        start=start,
        end=end,
        as_of=as_of,
        admitted_count=2,
        admitted_last=successor,
    )

    result = scan_native_csv_stream(
        BytesIO(raw),
        expected_bytes=len(raw),
        timeframe=timeframe,
        common_request=common,
        pagination=pagination,
    )

    assert result.artifact.invalid_bar_close_count == 0


def test_d1_final_page_internal_omission_is_not_misread_as_removed_successor() -> None:
    first_open = datetime(2019, 11, 24, tzinfo=UTC)
    omitted_open = datetime(2019, 11, 25, tzinfo=UTC)
    last_open = datetime(2019, 11, 26, tzinfo=UTC)
    last_close = last_open + timedelta(days=1)
    raw = _bars(
        (first_open, first_open + timedelta(days=1)),
        (last_open, last_close),
    )
    common, pagination = _contract(
        timeframe="D1",
        start=first_open,
        end=last_close,
        as_of=last_close,
        admitted_count=3,
        admitted_last=last_open,
    )
    pagination["page_attempts"][0]["source_quality_omissions"] = [
        {
            "bar_open_utc": _iso(omitted_open),
            "receipt_sha256": "0" * 64,
        }
    ]

    result = scan_native_csv_stream(
        BytesIO(raw),
        expected_bytes=len(raw),
        timeframe="D1",
        common_request=common,
        pagination=pagination,
    )

    assert result.artifact.invalid_bar_close_count == 0
    assert result.artifact.source_quality_omission_target_open_count == 0


def test_fully_closed_removed_successor_is_not_accepted_as_close_evidence() -> None:
    timeframe = "H4"
    opened = datetime(2026, 7, 13, 0, 0, tzinfo=UTC)
    successor = datetime(2026, 7, 13, 4, 0, tzinfo=UTC)
    as_of = datetime(2026, 7, 13, 9, 0, tzinfo=UTC)
    raw = _bars((opened, successor))
    common, pagination = _contract(
        timeframe=timeframe,
        start=datetime(2026, 7, 12, 0, 0, tzinfo=UTC),
        end=as_of + timedelta(seconds=1),
        as_of=as_of,
        admitted_count=2,
        admitted_last=successor,
    )

    result = scan_native_csv_stream(
        BytesIO(raw),
        expected_bytes=len(raw),
        timeframe=timeframe,
        common_request=common,
        pagination=pagination,
    )

    assert result.artifact.invalid_bar_close_count == 1


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (lambda raw: raw[:-1], "BARS_EMPTY_OR_SIZE_INVALID"),
        (
            lambda raw: raw.replace(b"time,Open", b"when,Open", 1),
            "BARS_HEADER_INVALID",
        ),
    ],
)
def test_scanner_fails_closed_on_byte_count_and_header(
    mutation: Any,
    error: str,
) -> None:
    opened = datetime(2026, 7, 13, 8, 55, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    original = _bars((opened, closed))
    raw = mutation(original)
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )

    with pytest.raises(NativeCsvValidationError) as caught:
        scan_native_csv_stream(
            BytesIO(raw),
            expected_bytes=len(original),
            timeframe="M5",
            common_request=common,
            pagination=pagination,
        )
    assert caught.value.code == error


@pytest.mark.parametrize(
    "extreme",
    [
        b"0001-01-01T00:00:00+23:59",
        b"9999-12-31T23:59:59-23:59",
    ],
)
def test_extreme_offset_timestamp_overflow_is_a_typed_failure(
    extreme: bytes,
) -> None:
    opened = datetime(2026, 7, 13, 8, 55, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    original = _bars((opened, closed))
    raw = original.replace(_iso(opened).encode(), extreme, 1)
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )

    with pytest.raises(NativeCsvValidationError) as caught:
        scan_native_csv_stream(
            BytesIO(raw),
            expected_bytes=len(raw),
            timeframe="M5",
            common_request=common,
            pagination=pagination,
        )
    assert caught.value.code == "BAR_OPEN_INVALID"


def test_closed_binary_stream_is_a_typed_failure() -> None:
    opened = datetime(2026, 7, 13, 8, 55, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    raw = _bars((opened, closed))
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )
    stream = BytesIO(raw)
    stream.close()

    with pytest.raises(NativeCsvValidationError) as caught:
        scan_native_csv_stream(
            stream,
            expected_bytes=len(raw),
            timeframe="M5",
            common_request=common,
            pagination=pagination,
        )
    assert caught.value.code == "BARS_STREAM_INVALID"


def test_stream_tell_type_error_is_a_typed_failure() -> None:
    with pytest.raises(NativeCsvValidationError) as caught:
        scan_native_csv_stream(
            _TellTypeErrorBytesIO(b"x"),
            expected_bytes=1,
            timeframe="M5",
            common_request={},
            pagination={},
        )
    assert caught.value.code == "BARS_STREAM_INVALID"


def test_stream_readline_type_error_is_a_typed_failure() -> None:
    opened = datetime(2026, 7, 13, 8, 55, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    raw = _bars((opened, closed))
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )

    with pytest.raises(NativeCsvValidationError) as caught:
        scan_native_csv_stream(
            _ReadlineTypeErrorBytesIO(raw),
            expected_bytes=len(raw),
            timeframe="M5",
            common_request=common,
            pagination=pagination,
        )
    assert caught.value.code == "BARS_READ_FAILED"


@pytest.mark.parametrize(
    "metrics",
    [
        b"0.5,0,1",
        b"1,0.5,1",
        b"1,0,0.5",
        b"1.0000000000000001,0,1",
    ],
)
def test_fractional_mt5_volume_and_spread_metrics_are_rejected(
    metrics: bytes,
) -> None:
    opened = datetime(2026, 7, 13, 8, 55, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    raw = _bars((opened, closed)).replace(b"1,0,1,", metrics + b",", 1)
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )

    with pytest.raises(NativeCsvValidationError) as caught:
        scan_native_csv_stream(
            BytesIO(raw),
            expected_bytes=len(raw),
            timeframe="M5",
            common_request=common,
            pagination=pagination,
        )
    assert caught.value.code == "BARS_NONINTEGRAL_MT5_METRIC"


def test_integral_decimal_mt5_metrics_remain_valid() -> None:
    opened = datetime(2026, 7, 13, 8, 55, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    raw = _bars((opened, closed)).replace(b"1,0,1,", b"100.0,2.0,0.0,", 1)
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )

    result = scan_native_csv_stream(
        BytesIO(raw),
        expected_bytes=len(raw),
        timeframe="M5",
        common_request=common,
        pagination=pagination,
    )
    assert result.artifact.rows == 1


def test_fixed_clock_off_native_grid_bar_is_rejected() -> None:
    opened = datetime(2026, 7, 13, 8, 56, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    raw = _bars((opened, closed))
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )

    with pytest.raises(NativeCsvValidationError) as caught:
        scan_native_csv_stream(
            BytesIO(raw),
            expected_bytes=len(raw),
            timeframe="M5",
            common_request=common,
            pagination=pagination,
        )
    assert caught.value.code == "BARS_FIXED_CLOCK_OFF_NATIVE_GRID"


@pytest.mark.parametrize(
    "metrics",
    [
        b"1e308,0,1",
        b"9223372036854775808,0,1",
        b"1,9223372036854775808,1",
        b"1,0,-1",
    ],
)
def test_mt5_discrete_metrics_must_fit_nonnegative_signed_int64(
    metrics: bytes,
) -> None:
    opened = datetime(2026, 7, 13, 8, 55, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    raw = _bars((opened, closed)).replace(b"1,0,1,", metrics + b",", 1)
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )

    with pytest.raises(NativeCsvValidationError) as caught:
        scan_native_csv_stream(
            BytesIO(raw),
            expected_bytes=len(raw),
            timeframe="M5",
            common_request=common,
            pagination=pagination,
        )
    assert caught.value.code == "BARS_MT5_METRIC_OUT_OF_RANGE"


def test_signed_int64_maximum_mt5_metric_remains_representable() -> None:
    opened = datetime(2026, 7, 13, 8, 55, tzinfo=UTC)
    closed = opened + timedelta(minutes=5)
    raw = _bars((opened, closed)).replace(b"1,0,1,", b"9223372036854775807,0,1,", 1)
    common, pagination = _contract(
        timeframe="M5",
        start=opened,
        end=closed,
        as_of=closed,
        admitted_count=1,
        admitted_last=opened,
    )
    result = scan_native_csv_stream(
        BytesIO(raw),
        expected_bytes=len(raw),
        timeframe="M5",
        common_request=common,
        pagination=pagination,
    )
    assert result.artifact.rows == 1


def test_non_string_timeframe_is_a_typed_failure() -> None:
    with pytest.raises(NativeCsvValidationError) as caught:
        scan_native_csv_stream(
            BytesIO(b"x"),
            expected_bytes=1,
            timeframe=["M5"],  # type: ignore[arg-type]
            common_request={},
            pagination={},
        )
    assert caught.value.code == "TIMEFRAME_INVALID"
