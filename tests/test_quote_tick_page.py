"""Synthetic tick acquisition only; no terminal, account or fill-policy authority."""

from contextlib import contextmanager
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import pm_mt5

assert Path(pm_mt5.__file__).resolve() == PRODUCT_ROOT / "pm_mt5.py"

BASE = 1711846800000  # Declared 2024-03-31T01:00:00Z, independent of local DST.


def _row(milliseconds, **changes):
    return {"time": milliseconds // 1000, "time_msc": milliseconds,
        "bid": 1.1000, "ask": 1.1002, "last": 0.0,
        "flags": 134, "volume": 0, "volume_real": 0.0, **changes}


class _SyntheticTicks:
    COPY_TICKS_ALL = 0xFFFFFFFF
    RES_S_OK = 1

    def __init__(self):
        self.rows = []
        self.error = (1, "SYNTHETIC_SUCCESS")
        self.exception = None
        self.events = []
        self.requests = []
        self.order_send = Mock(side_effect=AssertionError("Real mutation forbidden"))
        self.tick = SimpleNamespace(**_row(BASE + 128))

    def copy_ticks_range(self, *request):
        self.events.append("copy_ticks_range")
        self.requests.append(request)
        if self.exception is not None:
            raise self.exception
        return self.rows

    def last_error(self):
        self.events.append("last_error")
        return self.error

    def symbol_info_tick(self, symbol):
        assert symbol == "EURUSD.synthetic"
        return self.tick


@pytest.fixture
def observed(monkeypatch):
    fake = _SyntheticTicks()
    connector = pm_mt5.MT5Connector()
    connector._connected = True
    connector._broker_symbol_cache["EURUSD"] = "EURUSD.synthetic"
    connector.set_execution_destination("PAPER")
    leases = []

    @contextmanager
    def synthetic_lease():
        leases.append("SYNTHETIC_LEASE_NOT_OS_MUTEX_ASSURANCE")
        yield False

    monkeypatch.setattr(pm_mt5, "mt5", fake)
    monkeypatch.setattr(pm_mt5, "MT5_AVAILABLE", True)
    monkeypatch.setattr(pm_mt5, "_NATIVE_ACTIVE_CONNECTOR", connector)
    monkeypatch.setattr(pm_mt5, "_native_terminal_lease", synthetic_lease)
    monkeypatch.setattr(pm_mt5._NATIVE_LOCAL, "connector", None, raising=False)
    yield SimpleNamespace(fake=fake, connector=connector, leases=leases)
    fake.order_send.assert_not_called()


def test_exact_utc_millisecond_filter_keeps_order_duplicates_and_immutable_records(observed):
    env = observed
    first = _row(BASE + 128, bid=1.1001)
    second = _row(BASE + 128, bid=1.1000)
    env.fake.rows = [_row(BASE + 127), first, second, dict(second), _row(BASE + 130)]
    page = env.connector.get_quote_tick_page("EURUSD", BASE + 128, BASE + 129)
    assert page.complete is True and page.status == "COMPLETE" and page.raw_count == 5
    assert tuple(row.bid for row in page.ticks) == (1.1001, 1.1000, 1.1000)
    assert tuple(row.same_msc_ordinal for row in page.ticks) == (0, 1, 2)
    assert tuple(row.raw_ordinal for row in page.ticks) == (1, 2, 3)
    assert all(row.time_msc == BASE + 128 for row in page.ticks)
    assert all(row.time == datetime(2024, 3, 31, 1, 0, 0, 128000, timezone.utc) for row in page.ticks)
    assert all(row.last == 0 and row.volume == 0 and row.volume_real == 0 for row in page.ticks)
    assert env.fake.requests == [("EURUSD.synthetic",
        datetime(2024, 3, 31, 1, tzinfo=timezone.utc),
        datetime(2024, 3, 31, 1, 0, 1, tzinfo=timezone.utc), env.fake.COPY_TICKS_ALL)]
    assert env.fake.events == ["copy_ticks_range", "last_error"]
    assert env.leases and env.connector.execution_destination == "PAPER"
    assert isinstance(page.ticks, tuple)
    with pytest.raises(FrozenInstanceError):
        page.ticks[0].bid = 9
    with pytest.raises(FrozenInstanceError):
        page.complete = False


def test_structured_native_rows_and_time_not_count_bound(observed):
    env = observed
    dtype = [("time", "i8"), ("bid", "f8"), ("ask", "f8"), ("last", "f8"),
             ("volume", "u8"), ("time_msc", "i8"), ("flags", "u4"), ("volume_real", "f8")]
    row = _row(BASE + 128)
    env.fake.rows = pm_mt5.np.array([tuple(row[name] for name, _ in dtype)] * 5001, dtype=dtype)
    page = env.connector.get_quote_tick_page("EURUSD", BASE + 128, BASE + 128)
    assert len(page.ticks) == page.raw_count == 5001
    assert page.ticks[-1].same_msc_ordinal == 5000
    assert page.complete is True
    assert len(env.fake.requests[0]) == 4  # No count argument or secondary tail request.


def test_empty_success_and_supplied_future_broker_bound_do_not_invent_prices(observed):
    env = observed
    page = env.connector.get_quote_tick_page("EURUSD", BASE, BASE + 60000)
    assert page.complete is True and page.status == "EMPTY" and page.ticks == ()
    future = 2208988800128  # Declared 2040-01-01 UTC, not bounded by local now.
    env.fake.rows = [_row(future)]
    page = env.connector.get_quote_tick_page("EURUSD", future, future)
    assert page.ticks[0].time == datetime(2040, 1, 1, 0, 0, 0, 128000, timezone.utc)
    assert page.start_msc == page.end_msc == future


@pytest.mark.parametrize(("kind", "expected"), (
    ("none", "QUOTE_TICK_PAGE_NATIVE_CALL_FAILED"),
    ("negative", "QUOTE_TICK_PAGE_NATIVE_ERROR"),
    ("positive", "QUOTE_TICK_PAGE_NATIVE_ERROR"),
    ("exception", "QUOTE_TICK_PAGE_NATIVE_CALL_FAILED"),
    ("diagnostic", "QUOTE_TICK_PAGE_ERROR_DIAGNOSTIC_INVALID"),
))
def test_native_failures_retain_exact_diagnostics_and_valid_partial_rows(observed, kind, expected):
    env = observed
    env.fake.rows = [_row(BASE + 128), _row(BASE + 129)]
    if kind == "none":
        env.fake.rows, env.fake.error = None, (-4, "SYNTHETIC_NOT_FOUND")
    elif kind == "negative":
        env.fake.error = (-10005, "SYNTHETIC_TIMEOUT")
    elif kind == "positive":
        env.fake.error = (4407, "SYNTHETIC_PARTIAL_BUFFER")
    elif kind == "exception":
        env.fake.error = (-10001, "SYNTHETIC_NATIVE_EXCEPTION")
        env.fake.exception = OSError("SYNTHETIC_COPY_FAILURE")
    else:
        env.fake.error = None
    with pytest.raises(pm_mt5.MT5QuoteTickPageError, match="^" + expected + "$") as caught:
        env.connector.get_quote_tick_page("EURUSD", BASE + 128, BASE + 129)
    error = caught.value
    assert isinstance(error, pm_mt5.NativeSessionError)
    assert error.page.complete is False and error.page.status == expected
    assert error.partial_ticks == error.page.ticks
    assert len(error.partial_ticks) == (0 if kind in ("none", "exception") else 2)
    if env.fake.error is not None:
        assert (error.page.native_error_code, error.page.native_error_message) == env.fake.error
    assert error.diagnostics["start_msc"] == BASE + 128
    assert env.fake.events == ["copy_ticks_range", "last_error"]


@pytest.mark.parametrize(("damage", "expected"), (
    ("missing", "QUOTE_TICK_PAGE_RECORD_INVALID"),
    ("null", "QUOTE_TICK_PAGE_RECORD_INVALID"),
    ("nan", "QUOTE_TICK_PAGE_RECORD_INVALID"),
    ("bool", "QUOTE_TICK_PAGE_RECORD_INVALID"),
    ("flags", "QUOTE_TICK_PAGE_RECORD_INVALID"),
    ("volume", "QUOTE_TICK_PAGE_RECORD_INVALID"),
    ("timestamp", "QUOTE_TICK_PAGE_RECORD_INVALID"),
    ("zero", "QUOTE_TICK_PAGE_ZERO_QUOTE"),
    ("crossed", "QUOTE_TICK_PAGE_CROSSED_QUOTE"),
    ("order", "QUOTE_TICK_PAGE_ORDER_INVALID"),
))
def test_invalid_record_refuses_without_sorting_or_filling(observed, damage, expected):
    env = observed
    damaged = _row(BASE + 129)
    if damage == "missing":
        damaged.pop("volume_real")
    elif damage == "null":
        damaged["bid"] = None
    elif damage == "nan":
        damaged["ask"] = float("nan")
    elif damage == "bool":
        damaged["last"] = True
    elif damage == "flags":
        damaged["flags"] = -1
    elif damage == "volume":
        damaged["volume"] = 1.5
    elif damage == "timestamp":
        damaged["time"] += 1
    elif damage == "zero":
        damaged["bid"] = 0
    elif damage == "crossed":
        damaged["bid"] = damaged["ask"] + 1
    else:
        damaged["time_msc"] = BASE + 127
    env.fake.rows = [_row(BASE + 128), damaged]
    with pytest.raises(pm_mt5.MT5QuoteTickPageError, match="^" + expected + "$") as caught:
        env.connector.get_quote_tick_page("EURUSD", BASE + 128, BASE + 129)
    assert caught.value.page.complete is False
    assert tuple(row.time_msc for row in caught.value.partial_ticks) == (BASE + 128,)
    assert caught.value.page.raw_count == 2


@pytest.mark.parametrize(("start", "end"), (
    (True, BASE), (BASE + 0.5, BASE + 1), (-1, 1),
    (BASE + 1, BASE), (BASE, BASE + 60001), (None, BASE),
))
def test_invalid_window_never_calls_native_ticks(observed, start, end):
    with pytest.raises(pm_mt5.NativeSessionError, match="^QUOTE_TICK_PAGE_BOUNDS_INVALID$"):
        observed.connector.get_quote_tick_page("EURUSD", start, end)
    assert observed.fake.requests == [] and observed.fake.events == []


def test_serializer_identity_failure_precedes_tick_copy(observed, monkeypatch):
    env = observed
    env.connector.runtime_identity = SimpleNamespace(namespace="SYNTHETIC_NOT_AUTHORITY")
    gate = Mock(side_effect=pm_mt5.NativeAccountBindingError("SYNTHETIC_IDENTITY_FAILURE"))
    monkeypatch.setattr(env.connector, "_assert_native_account", gate)
    with pytest.raises(pm_mt5.NativeAccountBindingError, match="SYNTHETIC_IDENTITY_FAILURE"):
        env.connector.get_quote_tick_page("EURUSD", BASE, BASE + 1)
    gate.assert_called_once_with()
    assert env.fake.requests == [] and env.fake.events == []


def test_get_tick_preserves_raw_milliseconds_and_legacy_seconds_only_fallback(observed):
    env = observed
    value = env.connector.get_tick("EURUSD")
    assert value.time_msc == BASE + 128
    assert value.time == datetime(2024, 3, 31, 1, tzinfo=timezone.utc)
    del env.fake.tick.time_msc
    value = env.connector.get_tick("EURUSD")
    assert value.time_msc == BASE
    env.fake.tick.time_msc = None
    with pytest.raises(pm_mt5.NativeSessionError, match="^NATIVE_TICK_TIME_INVALID$"):
        env.connector.get_tick("EURUSD")
