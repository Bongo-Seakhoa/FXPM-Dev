"""Focused synthetic review repairs through public PAPER APIs and the real ledger.

All native accounts, quotes, M1 bars and failures are invented engineering inputs.
No broker call, source authenticity, statistical qualification or financial
authority is claimed. Only fake native response functions are instrumented.
"""

from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import test_paper_tick_protection as tape_fixture
from test_paper_tick_protection import tick_world, world

backend, outcomes = tape_fixture.backend, tape_fixture.outcomes
assert Path(backend.__file__).resolve() == PRODUCT_ROOT / "pm_mt5.py"
assert Path(outcomes.__file__).resolve() == PRODUCT_ROOT / "pm_live_outcomes.py"
BASE, BASE_MSC = tape_fixture.BASE, tape_fixture.BASE_MSC
_tick, _open, _row, _outs, _error = (
    tape_fixture._tick, tape_fixture._open, tape_fixture._row, tape_fixture._outs, tape_fixture._error)


def _cursor_for_single_native_record(record):
    value = {name: record[name] for name in (
        "time_msc", "bid", "ask", "last", "flags", "volume", "volume_real")}
    value["same_msc_ordinal"] = 0
    return dict(time_msc=record["time_msc"], same_msc_ordinal=0,
        prefix_sha256=hashlib.sha256((json.dumps(value, sort_keys=True,
            separators=(",", ":"), allow_nan=False) + "\n").encode()).hexdigest())


def _catchup_tape(tape):
    # Minute0 has an additional safe record after entry; minute2 has an SL which
    # must remain unconsumed if a required earlier drain minute is unavailable.
    tape.append(_tick(45_000, 1.1010, 1.1012),
        _tick(60_000, 1.1020, 1.1022), _tick(90_000, 1.1030, 1.1032),
        _tick(120_000, 1.1040, 1.1042), _tick(130_000, 1.0890, 1.0892),
        _tick(150_000, 1.1000, 1.1002), _tick(180_000))


def _window_failure(tick_world, monkeypatch, offset):
    """Native error belongs only to the requested later minute, never the quote page."""
    native_copy, native_error = tick_world.fake.copy_ticks_range, tick_world.fake.last_error
    control = dict(error=(-10005, "SYNTHETIC nonnull later-minute partial response"),
                   missing=False, current_error=None, failed_responses=[])

    def copy(symbol, start, end, flags):
        raw = native_copy(symbol, start, end, flags)
        target = start == BASE + timedelta(milliseconds=offset)
        control["current_error"] = control["error"] if target else None
        if target and control["error"] is not None:
            control["failed_responses"].append((start, control["error"], len(raw)))
            if control["missing"]:
                return None
        return raw

    def error():
        return control["current_error"] if control["current_error"] is not None else native_error()

    monkeypatch.setattr(tick_world.fake, "copy_ticks_range", copy)
    monkeypatch.setattr(tick_world.fake, "last_error", error)
    return control


def _assert_economics_unchanged(account, before, after, cash, deals):
    transitional = {"quote_cursor", "last_execution_evidence", "quote_transition_sha256", "quote_audit"}
    assert {key: value for key, value in after.items() if key not in transitional} == {
        key: value for key, value in before.items() if key not in transitional}
    assert after["status"] == "OPEN" and after["volume"] == 1.0
    assert account.ledger.paper_cash_snapshot() == cash
    assert account.ledger.paper_deals() == deals and _outs(account) == []


def test_later_drain_closed_m1_mismatch_retries_then_becomes_divergent(tick_world, monkeypatch):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    _, reservation = _open(account, tape)
    before = deepcopy(_row(account, reservation))
    cash, deals = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    _catchup_tape(tape)
    native_rates = tick_world.fake.copy_rates_range

    def changed_m1(symbol, timeframe, start, end):
        raw = native_rates(symbol, timeframe, start, end).copy()
        raw["high"][raw["time"] == (BASE_MSC + 60_000) // 1000] += 0.001
        return raw

    monkeypatch.setattr(tick_world.fake, "copy_rates_range", changed_m1)
    _error(account, "PAPER_M1_TICK_MISMATCH")
    first = deepcopy(_row(account, reservation))
    audit = first["quote_audit"]
    assert audit["pending_minute_msc"] == BASE_MSC
    assert audit["first_failure_minute_msc"] == BASE_MSC + 60_000
    assert audit["first_failure_code"] == "PAPER_M1_TICK_MISMATCH"
    assert audit["first_failure_status"] == "RETRYABLE" and audit["status"] == "PENDING"
    assert audit["terminal_failure_minute_msc"] is None and audit["terminal_failure_code"] is None
    assert first["quote_cursor"] == _cursor_for_single_native_record(_tick(45_000, 1.1010, 1.1012))
    _assert_economics_unchanged(account, before, first, cash, deals)

    tape.append(_tick(240_000))
    _error(account, "PAPER_M1_TICK_MISMATCH")
    repeated = _row(account, reservation)
    assert repeated["quote_cursor"] == first["quote_cursor"]
    assert repeated["last_execution_evidence"] == first["last_execution_evidence"]
    final = repeated["quote_audit"]
    assert final["status"] == "DIVERGENT"
    assert final["terminal_failure_minute_msc"] == BASE_MSC + 60_000
    assert final["terminal_failure_code"] == "PAPER_M1_TICK_MISMATCH"
    assert {key: final[key] for key in ("first_failure_minute_msc", "first_failure_code", "first_failure_status")} == {
        key: audit[key] for key in ("first_failure_minute_msc", "first_failure_code", "first_failure_status")}
    assert account.paper.get_position_history(before["position_id"])["quote_audit"] == final
    _assert_economics_unchanged(account, before, repeated, cash, deals)


def test_later_drain_nonnull_partial_then_explicit_native_absence_retains_verified_prefix(tick_world, monkeypatch):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    _, reservation = _open(account, tape)
    before = deepcopy(_row(account, reservation))
    cash, deals = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    _catchup_tape(tape)
    failure = _window_failure(tick_world, monkeypatch, 60_000)
    _error(account, "QUOTE_TICK_PAGE_NATIVE_ERROR")
    first = deepcopy(_row(account, reservation))
    assert failure["failed_responses"] and failure["failed_responses"][0][2] > 0
    assert first["quote_cursor"] == _cursor_for_single_native_record(_tick(45_000, 1.1010, 1.1012))
    audit = first["quote_audit"]
    assert audit["first_failure_minute_msc"] == BASE_MSC + 60_000
    assert audit["first_failure_code"] == "QUOTE_TICK_PAGE_NATIVE_ERROR"
    assert audit["first_failure_status"] == "RETRYABLE" and audit["status"] == "PENDING"
    _assert_economics_unchanged(account, before, first, cash, deals)

    failure.update(error=(-4, "SYNTHETIC required later-minute history absent"), missing=True)
    _error(account, "QUOTE_TICK_PAGE_NATIVE_CALL_FAILED")
    final = _row(account, reservation)
    assert final["quote_cursor"] == first["quote_cursor"]
    assert final["last_execution_evidence"] == first["last_execution_evidence"]
    assert failure["failed_responses"][-1][1][0] == -4
    assert final["quote_audit"] == first["quote_audit"]
    assert final["quote_audit"]["status"] == "PENDING"
    assert final["quote_audit"]["terminal_failure_minute_msc"] is None
    assert final["quote_audit"]["terminal_failure_code"] is None
    assert final["quote_audit"]["first_failure_code"] == "QUOTE_TICK_PAGE_NATIVE_ERROR"
    assert final["quote_audit"]["first_failure_status"] == "RETRYABLE"
    _assert_economics_unchanged(account, before, final, cash, deals)


@pytest.mark.parametrize("kind,code,status", [
    ("missing", "PAPER_TICK_HISTORY_CURSOR_UNAVAILABLE", "UNAVAILABLE"),
    ("changed", "PAPER_TICK_HISTORY_PREFIX_CHANGED", "DIVERGENT"),
])
def test_missing_or_changed_held_cursor_in_later_drain_is_terminal(tick_world, monkeypatch, kind, code, status):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    _, reservation = _open(account, tape)
    _catchup_tape(tape)
    failure = _window_failure(tick_world, monkeypatch, 120_000)
    _error(account, "QUOTE_TICK_PAGE_NATIVE_ERROR")
    before = deepcopy(_row(account, reservation))
    assert before["quote_cursor"] == _cursor_for_single_native_record(_tick(90_000, 1.1030, 1.1032))
    assert before["quote_audit"]["pending_minute_msc"] == BASE_MSC
    assert before["quote_audit"]["first_failure_minute_msc"] == BASE_MSC + 120_000
    cash, deals = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    failure["error"] = None
    index = next(index for index, row in enumerate(tape.rows) if row["time_msc"] == BASE_MSC + 90_000)
    if kind == "missing":
        del tape.rows[index]
    else:
        tape.rows[index] = _tick(90_000, 1.1031, 1.1033)
    tape.visible = len(tape.rows)
    _error(account, code)
    after = _row(account, reservation)
    assert after["quote_cursor"] == before["quote_cursor"]
    assert after["last_execution_evidence"] == before["last_execution_evidence"]
    audit = after["quote_audit"]
    assert audit["status"] == status
    assert audit["terminal_failure_minute_msc"] == BASE_MSC + 60_000
    assert audit["terminal_failure_code"] == code
    assert audit["first_failure_minute_msc"] == BASE_MSC + 120_000
    assert audit["first_failure_code"] == "QUOTE_TICK_PAGE_NATIVE_ERROR"
    assert audit["first_failure_status"] == "RETRYABLE"
    _assert_economics_unchanged(account, before, after, cash, deals)


def _reserve_other_slot(account, reservation="9" * 32, *, magic=987):
    return account.ledger.reserve_pre_send_intent(symbol="EURUSD", timeframe="D1", regime="TREND",
        strategy_name="EMACrossoverStrategy", magic=magic, entry_price=1.1002, volume=1.0,
        initial_stop=1.09, risk_cash=1020.0, book_role="LOCKBOX_VALIDATED_CHAMPION",
        candidate_id="synthetic-review-slot-not-qualified", reservation_id=reservation)


def _send_reserved(account, reservation="9" * 32, *, magic=987):
    return account.paper.send_market_order("EURUSD", 0, 1.0, sl=1.09, tp=1.12,
        magic=magic, reservation_id=reservation)


def test_native_context_binding_error_latches_entry_block_before_paper_body(tick_world):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    _open(account, tape)
    _reserve_other_slot(account, "2" * 32, magic=456)
    second = _send_reserved(account, "2" * 32, magic=456)
    assert second.success
    account.ledger.bind_pre_send_intent("2" * 32, position_id=second.position_id,
        opening_deal_ticket=second.deal, opening_order_ticket=second.order)
    _reserve_other_slot(account)
    before, cash, deals = deepcopy(account.ledger.paper_positions()), account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    assert len(before) == 2
    tape.append(_tick(60_000, 1.0800, 1.0802), _tick(120_000))
    requests = len(tick_world.requests)
    tick_world.fake.accounts[account.login].login = account.login + 100
    with pytest.raises(backend.NativeAccountBindingError):
        _send_reserved(account)
    assert account.ledger.paper_positions() == before and len(tick_world.requests) == requests
    tick_world.fake.accounts[account.login].login = account.login
    assert account.paper.get_account_info().login == account.login
    refused = _send_reserved(account)
    assert not refused.success and refused.retcode_description == "PAPER_ACCOUNT_IDENTITY_MISMATCH"
    assert account.ledger.paper_positions() == before and len(tick_world.requests) == requests
    assert account.ledger.paper_cash_snapshot() == cash and account.ledger.paper_deals() == deals


def test_health_and_binding_property_each_retain_observed_identity_incidents(tick_world, world):
    first = tick_world.account("EURUSD")
    for mechanism, account in (("health", first), ("property", world.open(900002))):
        _open(account, tick_world.tapes["EURUSD"])
        _reserve_other_slot(account)
        before, cash, deals = deepcopy(account.ledger.paper_positions()), account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
        tick_world.fake.accounts[account.login].login = account.login + 100
        if mechanism == "health":
            assert account.paper.is_connected() is False
        else:
            with pytest.raises(backend.NativeAccountBindingError):
                account.data.get_tick("EURUSD")
            assert account.paper.binding_failure_reason
        tick_world.fake.accounts[account.login].login = account.login
        assert account.paper.get_account_info().login == account.login
        refused = _send_reserved(account)
        assert not refused.success and refused.retcode_description == "PAPER_ACCOUNT_IDENTITY_MISMATCH"
        assert account.ledger.paper_positions() == before
        assert account.ledger.paper_cash_snapshot() == cash and account.ledger.paper_deals() == deals


@pytest.mark.parametrize("null_response", [True, False], ids=["None-minus-four", "empty-minus-four"])
def test_quiet_intermediate_native_minus_four_retries_until_actual_s_ok_empty_page(
        tick_world, monkeypatch, null_response):
    """A fake quiet minute is unknown under -4, and accepted only after actual S_OK."""
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    _, reservation = _open(account, tape)
    before = deepcopy(_row(account, reservation))
    cash, deals = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    # No invented tick or M1 bar exists in minute1; safe genuine fake rows resume in minute2.
    tape.append(_tick(45_000, 1.1010, 1.1012), _tick(120_001, 1.1020, 1.1022),
                _tick(150_000, 1.1030, 1.1032), _tick(180_000, 1.1040, 1.1042))
    failure = _window_failure(tick_world, monkeypatch, 60_000)
    failure.update(error=(-4, "SYNTHETIC quiet-range native not-found; not permanence"),
                   missing=null_response)
    code = "QUOTE_TICK_PAGE_NATIVE_CALL_FAILED" if null_response else "QUOTE_TICK_PAGE_NATIVE_ERROR"
    _error(account, code)
    blocked = deepcopy(_row(account, reservation))
    assert failure["failed_responses"][-1][2] == 0
    assert blocked["quote_cursor"] == _cursor_for_single_native_record(_tick(45_000, 1.1010, 1.1012))
    audit = blocked["quote_audit"]
    assert audit["status"] == "PENDING" and audit["overdue"] is True
    assert audit["first_failure_minute_msc"] == BASE_MSC + 60_000
    assert audit["first_failure_code"] == code and audit["first_failure_status"] == "RETRYABLE"
    assert audit["terminal_failure_minute_msc"] is audit["terminal_failure_code"] is None
    assert audit["corroborated_minute_count"] == 0  # Unknown minute1 was never silently crossed.
    _assert_economics_unchanged(account, before, blocked, cash, deals)
    with pytest.raises(backend.PaperProtectionDataError):
        account.paper.get_risk_account_info()
    assert _row(account, reservation)["quote_cursor"] == blocked["quote_cursor"]

    # The same real fake API now returns a complete successful empty interval,
    # while the actual invented M1 source also has no bar for that quiet minute.
    failure.update(error=None, missing=False)
    positions = account.paper.poll_paper_positions()
    assert len(positions) == 1 and positions[0].ticket == before["position_id"]
    resumed = _row(account, reservation)
    assert resumed["quote_cursor"] == _cursor_for_single_native_record(_tick(180_000, 1.1040, 1.1042))
    assert resumed["quote_audit"]["corroborated_minute_count"] == 3
    assert resumed["quote_audit"]["last_corroborated_minute_msc"] == BASE_MSC + 120_000
    assert resumed["quote_audit"]["pending_minute_msc"] == BASE_MSC + 180_000
    assert resumed["quote_audit"]["status"] == "PENDING"  # The still-open position is not a clean outcome.
    assert resumed["quote_audit"]["overdue"] is False
    for key in ("first_failure_minute_msc", "first_failure_code", "first_failure_status"):
        assert resumed["quote_audit"][key] == audit[key]
    assert resumed["quote_audit"]["terminal_failure_minute_msc"] is None
    assert account.paper.paper_protection_diagnostics() == {}
    assert any(request[0] == "ticks" and request[2] == BASE + timedelta(minutes=1)
               for request in tick_world.requests)
    _assert_economics_unchanged(account, before, resumed, cash, deals)


def test_native_tick_none_retains_immediate_minus_four_without_inventing_source_time(tick_world, monkeypatch):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    _, reservation = _open(account, tape)
    before = deepcopy(_row(account, reservation))
    cash, deals = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    requests = list(tick_world.requests)
    native_tick, native_error = tick_world.fake.symbol_info_tick, tick_world.fake.last_error
    control = dict(failed=True, captured=None, reads=0)
    diagnostic = (-4, "SYNTHETIC current tick temporarily absent")

    def tick(symbol):
        if control["failed"]:
            control["captured"] = diagnostic
            return None
        return native_tick(symbol)

    def error():
        if control["captured"] is not None:
            result, control["captured"] = control["captured"], None
            control["reads"] += 1
            return result
        return native_error()  # Subsequent unrelated calls see ordinary S_OK.

    monkeypatch.setattr(tick_world.fake, "symbol_info_tick", tick)
    monkeypatch.setattr(tick_world.fake, "last_error", error)
    _error(account, "PAPER_OBSERVED_QUOTE_UNAVAILABLE")
    after = _row(account, reservation)
    assert control["reads"] == 1 and account.data.last_tick_error == diagnostic
    assert tick_world.fake.last_error()[0] == 1  # Must not replace the captured failure cause.
    expected_audit = {**before["quote_audit"], "first_failure_minute_msc": BASE_MSC,
        "first_failure_code": "PAPER_OBSERVED_QUOTE_UNAVAILABLE", "first_failure_status": "RETRYABLE"}
    assert after == {**before, "quote_audit": expected_audit}
    assert after["quote_audit"]["overdue"] == before["quote_audit"]["overdue"]
    assert after["quote_cursor"] == before["quote_cursor"] and tick_world.requests == requests
    _assert_economics_unchanged(account, before, after, cash, deals)

    control["failed"] = False
    tape.append(_tick(60_000), _tick(120_000))
    assert len(account.paper.poll_paper_positions()) == 1
    restored = _row(account, reservation)
    assert account.data.last_tick_error is None
    assert restored["quote_cursor"]["time_msc"] == BASE_MSC + 120_000
    assert restored["quote_audit"]["first_failure_code"] == "PAPER_OBSERVED_QUOTE_UNAVAILABLE"
    assert restored["quote_audit"]["first_failure_status"] == "RETRYABLE"
    assert restored["quote_audit"]["terminal_failure_code"] is None
    _assert_economics_unchanged(account, before, restored, cash, deals)
