import numpy as np
import pandas as pd

from pm_native_resolution import audit_native_resolution


def _ohlc(index, *, offset=0.0):
    index = pd.DatetimeIndex(index)
    values = 100.0 + offset + np.arange(len(index), dtype=float) * 0.01
    return pd.DataFrame(
        {
            "Open": values,
            "High": values + 0.1,
            "Low": values - 0.1,
            "Close": values + 0.02,
        },
        index=index,
    )


def test_daily_like_m5_returned_by_terminal_fails_only_intraday_cell():
    index = pd.date_range("2025-01-01", periods=80, freq="B", tz="UTC")
    daily_like = _ohlc(index)

    m5 = audit_native_resolution(
        daily_like,
        symbol="AU200",
        timeframe="M5",
        daily_frame=daily_like,
        tick_tolerance=0.01,
    )
    d1 = audit_native_resolution(
        daily_like,
        symbol="AU200",
        timeframe="D1",
        daily_frame=daily_like,
        tick_tolerance=0.01,
    )

    assert not m5.passed
    assert any(code.startswith("INTRADAY_RESOLUTION_DAILY_LIKE") for code in m5.reason_codes)
    assert any(code.startswith("INTRADAY_D1_CROSS_TIMEFRAME_IDENTITY") for code in m5.reason_codes)
    assert d1.passed


def test_regular_session_equity_m5_passes_without_24_hour_assumption():
    points = []
    for day in pd.date_range("2025-01-02", periods=30, freq="B", tz="UTC"):
        points.extend(pd.date_range(day + pd.Timedelta(hours=14, minutes=30), periods=78, freq="5min"))
    frame = _ohlc(points)
    daily = _ohlc(pd.date_range("2025-01-02", periods=30, freq="B", tz="UTC"), offset=20.0)

    result = audit_native_resolution(
        frame,
        symbol="AIG",
        timeframe="M5",
        daily_frame=daily,
        tick_tolerance=0.01,
    )

    assert result.passed, result.reason_codes
    assert result.median_bars_per_active_session == 78.0
    assert result.nominal_or_faster_spacing_ratio > 0.95


def test_two_h4_bars_per_equity_session_are_not_misclassified_as_daily():
    points = []
    for day in pd.date_range("2025-01-02", periods=40, freq="B", tz="UTC"):
        points.extend([day + pd.Timedelta(hours=14), day + pd.Timedelta(hours=18)])
    frame = _ohlc(points)

    result = audit_native_resolution(
        frame,
        symbol="AIG",
        timeframe="H4",
    )

    assert result.passed, result.reason_codes
    assert result.median_bars_per_active_session == 2.0
    assert result.nominal_or_faster_spacing_ratio > 0.40


def test_one_material_daily_like_year_fails_even_if_other_years_are_intraday():
    daily_like = list(pd.date_range("2023-01-02", periods=100, freq="B", tz="UTC"))
    intraday = []
    for day in pd.date_range("2024-01-02", periods=260, freq="B", tz="UTC"):
        intraday.extend(pd.date_range(day + pd.Timedelta(hours=8), periods=12, freq="5min"))
    frame = _ohlc([*daily_like, *intraday])

    result = audit_native_resolution(
        frame,
        symbol="CHANGING",
        timeframe="M5",
    )

    assert not result.passed
    assert any("DAILY_LIKE_YEARS:2023" in code for code in result.reason_codes)


def test_small_or_invalid_source_is_pending_not_a_false_pass():
    small = _ohlc(pd.date_range("2025-01-01", periods=5, freq="5min", tz="UTC"))

    result = audit_native_resolution(small, symbol="NEW", timeframe="M5")

    assert result.status == "PENDING"
    assert result.reason_codes == ("RESOLUTION_SAMPLE_TOO_SMALL:5<20",)
