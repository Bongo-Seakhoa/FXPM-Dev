"""Explicit PAPER recovery with invented native inputs, never trading authority.

Terminal audit states arise from real native-reader failures, not audit injection.
The native serializer, identity gates, ledger, cash accounting and adapter execute;
only upstream entry approval and the finite native tape are synthetic.
"""

from copy import deepcopy
from datetime import timedelta
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

import test_paper_tick_protection as protection
from test_operational_execution import world
from test_paper_tick_protection import tick_world


backend, outcomes = protection.backend, protection.outcomes
assert Path(backend.__file__).resolve() == PRODUCT_ROOT / "pm_mt5.py"
assert Path(outcomes.__file__).resolve() == PRODUCT_ROOT / "pm_live_outcomes.py"
BASE, BASE_MSC = protection.BASE, protection.BASE_MSC
RECOVERY_OFFSET = 120_123
LABEL = "SYNTHETIC_MANUAL_RECOVERY_NO_CELL_OR_BROKER_AUTHORITY"
REQUIRES_UNLATCHED = "PAPER_MANUAL_RECOVERY_REQUIRES_TERMINAL_UNLATCHED_ROW"


def _expire_funding(world, account):
    world.fake.symbol.swap_mode = 4
    world.fake.symbol.swap_long, world.fake.symbol.swap_short = -2.0, 1.0
    utc = lambda value: value.isoformat().replace("+00:00", "Z")
    terms = dict(schema_version="fxpm.paper-funding-terms.v1", symbol="EURUSD", swap_mode=4,
        swap_long=-2.0, swap_short=1.0, currency="USD", rollover_timezone="UTC",
        coverage_start_utc=utc(BASE - timedelta(hours=1)), coverage_end_utc=utc(BASE + timedelta(seconds=65)),
        rollover_schedule=[], source_ref=LABEL, provenance_sha256="e" * 64)
    account.data.paper_funding_terms_provider = lambda _symbol, **_kwargs: deepcopy(terms)


def _terminalize(world, account, reservation, symbol, status, bid, ask):
    tape = world.tapes[symbol]
    before = protection._row(account, reservation)
    if status == "UNAVAILABLE":
        tape.rows, tape.visible = [protection._tick(RECOVERY_OFFSET, bid, ask)], 1
    else:
        assert status == "DIVERGENT"
        tape.rows[0] = {**tape.rows[0], "bid": 1.1001, "ask": 1.1003, "last": 1.1001}
        tape.append(protection._tick(RECOVERY_OFFSET, bid, ask))
    with pytest.raises(backend.PaperProtectionDataError):
        account.paper.poll_paper_positions(protection_only=True)
    row = protection._row(account, reservation)
    assert row["status"] == "OPEN" and row["quote_audit"]["status"] == status
    expected_code = "PAPER_REQUIRED_PREFIX_UNAVAILABLE" if status == "UNAVAILABLE" else "PAPER_MINUTE_PREFIX_CHANGED"
    assert row["quote_audit"]["terminal_failure_code"] == expected_code
    assert row["quote_cursor"] == before["quote_cursor"]
    assert row["last_execution_evidence"] == before["last_execution_evidence"]
    assert row["last_execution_evidence"]["event"] not in {"SL", "TP"}
    return row


def _case(world, *, status="UNAVAILABLE", side=0, bid=1.0800, ask=1.0802, expired_funding=False):
    account = world.account("EURUSD")
    if expired_funding:
        _expire_funding(world, account)
    fill, reservation = protection._open(account, world.tapes["EURUSD"], side=side)
    position = account.paper.get_position_by_ticket(fill.position_id)
    assert position is not None
    row = _terminalize(world, account, reservation, "EURUSD", status, bid, ask)
    return account, fill, reservation, position, row


@pytest.mark.parametrize("status,side,bid,ask,price", [
    ("UNAVAILABLE", 0, 1.0800, 1.0802, 1.0800),
    ("UNAVAILABLE", 1, 1.0698, 1.0700, 1.0700),
    ("DIVERGENT", 0, 1.1300, 1.1302, 1.1300),
    ("DIVERGENT", 1, 1.1300, 1.1302, 1.1302),
], ids=["missing-long-through-sl", "missing-short-through-tp", "rewritten-long-through-tp", "rewritten-short-through-sl"])
def test_explicit_full_recovery_uses_fresh_side_price_preserves_terminal_custody_and_replays_cold(
        tick_world, status, side, bid, ask, price):
    account, fill, reservation, position, held = _case(tick_world, status=status, side=side, bid=bid, ask=ask)
    ordinary = account.paper.close_position(position, operation_id="ordinary-is-not-recovery")
    assert not ordinary.success and protection._outs(account) == []
    assert protection._row(account, reservation)["quote_audit"] == held["quote_audit"]
    operation = "manual-recovery-once"
    result = account.paper.close_position(position, operation_id=operation, manual_recovery=True)
    assert result.success and result.destination == "PAPER"
    close, = protection._outs(account)
    assert (result.deal, result.order, result.volume, result.price) == (
        close["ticket"], close["order_ticket"], fill.volume, price)
    assert close["quote_cursor"] is None and close["time_msc"] == BASE_MSC + RECOVERY_OFFSET
    assert close["execution_evidence"] == dict(model_id=outcomes.PAPER_FILL_POLICY_ID,
        bid=bid, ask=ask, ask_source="NATIVE", event="MANUAL_RECOVERY_CLOSE",
        terminal_audit_status=status, terminal_audit_code=held["quote_audit"]["terminal_failure_code"])
    expected = (1 if side == 0 else -1) * (price - fill.price) * fill.volume * 100000.0
    assert close["profit"] == pytest.approx(expected)
    closed = protection._row(account, reservation)
    assert closed["status"] == "CLOSED" and closed["volume"] == 0
    for key in ("entry_quote_cursor", "quote_cursor", "last_execution_evidence", "level_history", "quote_audit"):
        assert closed[key] == held[key]
    cash, deals = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    assert account.ledger.paper_cash_adjustment() == pytest.approx(expected)

    account.ledger.close()
    account.ledger = tick_world.ledger(account.identity, account.config)
    account.paper = backend.PaperExecutionConnector(account.data, account.ledger, account.identity)
    assert protection._row(account, reservation) == closed
    tape = tick_world.tapes["EURUSD"]
    tape.append(protection._tick(180_000, 0.5, 0.5002))
    tape.native_error = (-10005, "SYNTHETIC current page unavailable after accepted recovery")
    tick_world.failed_cash.add("EURUSD")
    requests, state = len(tick_world.requests), account.ledger.state_path.read_bytes()
    replay = account.paper.close_position(position, operation_id=operation, manual_recovery=True)
    assert replay.success and (replay.deal, replay.order, replay.volume, replay.price) == (
        result.deal, result.order, result.volume, result.price)
    assert len(tick_world.requests) == requests
    assert account.ledger.state_path.read_bytes() == state
    assert account.ledger.paper_cash_snapshot() == cash and account.ledger.paper_deals() == deals
    tape.native_error = (1, LABEL)
    tick_world.failed_cash.remove("EURUSD")
    tape.append(protection._tick(240_000))
    assert account.paper.poll_paper_positions(protection_only=True) == []
    assert protection._row(account, reservation)["quote_audit"] == held["quote_audit"]

    sync = protection._financial_sync(account)
    assert sync.emitted_records == [] and sync.delivered_drift_count == 0
    records = [json.loads(line) for line in account.ledger.ledger_path.read_text(encoding="utf-8").splitlines()]
    record, = records
    assert record["realized_net_pnl"] == pytest.approx(expected)
    assert account.ledger.paper_outcome_admission(record) == status
    assert sync.paper_statistics["raw_closed_positions"] == sync.paper_statistics[status.lower() + "_closed_positions"] == 1
    assert sync.paper_statistics["clean_closed_positions"] == 0
    assert sync.paper_statistics["excluded_provisional_net_cash"] == pytest.approx(expected)
    assert protection._financial_sync(account).emitted_records == []


def test_two_terminal_positions_release_actual_risk_only_after_both_full_recoveries(tick_world):
    account = tick_world.account("EURUSD", "EURGBP")
    opened = {}
    for number, symbol in enumerate(("EURUSD", "EURGBP"), 1):
        fill, reservation = protection._open(account, tick_world.tapes[symbol], symbol=symbol, number=number)
        opened[symbol] = (fill, reservation, account.paper.get_position_by_ticket(fill.position_id))
    for symbol, bid, ask in (("EURUSD", 0.9000, 0.9002), ("EURGBP", 1.1000, 1.1002)):
        tape = tick_world.tapes[symbol]
        tape.rows, tape.visible = [protection._tick(RECOVERY_OFFSET, bid, ask)], 1
    with pytest.raises(backend.PaperProtectionDataError):
        account.paper.poll_paper_positions(protection_only=True)
    assert all(protection._row(account, item[1])["quote_audit"]["status"] == "UNAVAILABLE" for item in opened.values())
    first = account.paper.close_position(opened["EURUSD"][2], operation_id="recover-first", manual_recovery=True)
    assert first.success
    with pytest.raises(backend.PaperProtectionDataError):
        account.paper.get_risk_account_info()
    assert len(protection._outs(account)) == 1
    second = account.paper.close_position(opened["EURGBP"][2], operation_id="recover-second", manual_recovery=True)
    assert second.success and len(protection._outs(account)) == 2
    expected = sum(deal["profit"] + deal["swap"] + deal["commission"] + deal["fee"] for deal in protection._outs(account))
    risk = account.paper.get_risk_account_info()
    assert risk.balance == pytest.approx(10000.0 + expected) and risk.equity == pytest.approx(risk.balance)
    assert risk.balance < 0 and risk.sampled_broker_balance == 10000.0
    assert account.ledger.paper_cash_adjustment() == pytest.approx(expected)


def test_recovery_does_not_scan_or_reinterpret_another_saved_trigger(tick_world):
    account = tick_world.account("EURUSD", "EURGBP")
    target, target_reservation = protection._open(account, tick_world.tapes["EURUSD"], number=1)
    target_dto = account.paper.get_position_by_ticket(target.position_id)
    other, other_reservation = protection._open(account, tick_world.tapes["EURGBP"], symbol="EURGBP", number=2)
    missing, other_tape = tick_world.tapes["EURUSD"], tick_world.tapes["EURGBP"]
    missing.rows, missing.visible = [protection._tick(RECOVERY_OFFSET, 1.08, 1.0802)], 1
    other_tape.append(protection._tick(60_000), protection._tick(70_000, 1.089, 1.0892),
                      protection._tick(80_000), protection._tick(120_000))
    tick_world.failed_cash.add("EURGBP")
    with pytest.raises(backend.PaperProtectionDataError):
        account.paper.poll_paper_positions(protection_only=True)
    assert protection._row(account, target_reservation)["quote_audit"]["status"] == "UNAVAILABLE"
    latched = protection._row(account, other_reservation)
    assert latched["status"] == "OPEN" and latched["last_execution_evidence"]["event"] == "SL"
    requests = len(tick_world.requests)
    recovered = account.paper.close_position(target_dto, operation_id="only-this-terminal-row", manual_recovery=True)
    assert recovered.success
    assert all(item[1] != "EURGBP" for item in tick_world.requests[requests:])
    assert protection._row(account, other_reservation)["quote_cursor"] == latched["quote_cursor"]
    assert protection._row(account, other_reservation)["last_execution_evidence"] == latched["last_execution_evidence"]
    with pytest.raises(backend.PaperProtectionDataError):
        account.paper.get_risk_account_info()
    tick_world.failed_cash.remove("EURGBP")
    assert account.paper.poll_paper_positions(protection_only=True) == []
    settled, = [deal for deal in protection._outs(account) if deal["position_id"] == other.position_id]
    assert settled["quote_cursor"] == latched["quote_cursor"]
    assert settled["execution_evidence"] == latched["last_execution_evidence"]
    assert settled["time_msc"] == BASE_MSC + 70_000 and settled["price"] == pytest.approx(1.089)
    assert len(protection._outs(account)) == 2


def test_manual_recovery_refuses_a_nonterminal_row_without_falling_back(tick_world):
    account = tick_world.account("EURUSD")
    fill, reservation = protection._open(account, tick_world.tapes["EURUSD"])
    position = account.paper.get_position_by_ticket(fill.position_id)
    before = protection._row(account, reservation)
    result = account.paper.close_position(position, operation_id="not-terminal", manual_recovery=True)
    assert not result.success and result.retcode_description == REQUIRES_UNLATCHED
    assert protection._row(account, reservation) == before and protection._outs(account) == []
    assert account.ledger.paper_cash_adjustment() == 0.0


def test_saved_trigger_refusal_precedes_new_quote_and_expired_funding(tick_world):
    account = tick_world.account("EURUSD")
    _expire_funding(tick_world, account)
    tape = tick_world.tapes["EURUSD"]
    fill, reservation = protection._open(account, tape)
    position = account.paper.get_position_by_ticket(fill.position_id)
    tape.append(protection._tick(60_000), protection._tick(70_000, 1.089, 1.0892),
                protection._tick(80_000), protection._tick(120_000))
    protection._error(account, "PAPER_FUNDING_COVERAGE_UNAVAILABLE")
    tape.rows, tape.visible = [protection._tick(180_000)], 1
    tape.native_error = (1, "SYNTHETIC S_OK with the original saved prefix absent")
    protection._error(account, "PAPER_FUNDING_COVERAGE_UNAVAILABLE")
    before = protection._row(account, reservation)
    assert before["quote_audit"]["status"] == "UNAVAILABLE"
    assert before["last_execution_evidence"]["event"] == "SL"
    requests = len(tick_world.requests)
    result = account.paper.close_position(position, operation_id="cannot-replace-saved-stop", manual_recovery=True)
    assert not result.success and result.retcode_description == REQUIRES_UNLATCHED
    assert len(tick_world.requests) == requests
    assert protection._row(account, reservation) == before and protection._outs(account) == []
    assert account.ledger.paper_cash_adjustment() == 0.0


@pytest.mark.parametrize("invalid", ["partial", "blank-id"])
def test_manual_recovery_requires_full_remaining_volume_and_stable_operation_id(tick_world, invalid):
    account, _fill, reservation, position, before = _case(tick_world)
    if invalid == "partial":
        result = account.paper.close_position_partial(position, percent=50.0, operation_id="no-partial-recovery", manual_recovery=True)
    else:
        result = account.paper.close_position(position, operation_id="", manual_recovery=True)
    assert not result.success and result.retcode_description == "PAPER_MANUAL_RECOVERY_REQUIRES_FULL_VOLUME_AND_OPERATION_ID"
    assert protection._row(account, reservation) == before and protection._outs(account) == []
    assert account.ledger.paper_cash_adjustment() == 0.0


def test_manual_recovery_preserves_real_native_identity_refusal(tick_world):
    account, _fill, reservation, position, before = _case(tick_world)
    tick_world.fake.accounts[account.login].currency = "EUR"
    with pytest.raises(backend.NativeAccountBindingError):
        account.paper.close_position(position, operation_id="identity-cannot-be-bypassed", manual_recovery=True)
    assert protection._row(account, reservation) == before and protection._outs(account) == []
    assert account.ledger.paper_cash_adjustment() == 0.0


def test_manual_recovery_does_not_default_expired_funding_to_zero(tick_world):
    account, _fill, reservation, position, before = _case(tick_world, expired_funding=True)
    result = account.paper.close_position(position, operation_id="funding-must-be-priced", manual_recovery=True)
    assert not result.success and "PAPER_FUNDING_COVERAGE_UNAVAILABLE" in result.retcode_description
    assert protection._row(account, reservation) == before and protection._outs(account) == []
    assert account.ledger.paper_cash_adjustment() == 0.0
