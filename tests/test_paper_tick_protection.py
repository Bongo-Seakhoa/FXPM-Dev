"""Synthetic engineering checks of durable native-tick PAPER protection.

Every account, price and native response below is invented. These checks grant no
trading authority and make no market, account-profit or qualification claim.
The real native serializer, identity gates, PAPER adapter and outcome ledger run.
Generic cases use completed M1 intervals; the explicitly named forming-minute
case checks provisional execution followed by native corroboration.
"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import numpy as np
import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import test_operational_execution as operational
from test_operational_execution import world

backend, outcomes = operational.backend, operational.outcomes
assert Path(backend.__file__).resolve() == PRODUCT_ROOT / "pm_mt5.py"
assert Path(outcomes.__file__).resolve() == PRODUCT_ROOT / "pm_live_outcomes.py"

LABEL = "SYNTHETIC_TICK_PROTECTION_ENGINEERING_ONLY"
MODEL = "PAPER_NATIVE_FIRST_TOUCH_SL_QUOTE_TP_LIMIT_V1"
BASE = datetime(2026, 10, 7, tzinfo=timezone.utc)
BASE_MSC = int(BASE.timestamp() * 1000)
TICK_FIELDS = ("time", "bid", "ask", "last", "volume", "time_msc", "flags", "volume_real")
TICK_DTYPE = [
    ("time", "i8"), ("bid", "f8"), ("ask", "f8"), ("last", "f8"),
    ("volume", "u8"), ("time_msc", "i8"), ("flags", "u4"), ("volume_real", "f8"),
]
RATE_DTYPE = [
    ("time", "i8"), ("open", "f8"), ("high", "f8"), ("low", "f8"),
    ("close", "f8"), ("tick_volume", "u8"), ("spread", "i4"), ("real_volume", "u8"),
]


def _tick(offset, bid=1.1000, ask=1.1002):
    ms = BASE_MSC + offset
    return dict(time=ms // 1000, bid=bid, ask=ask, last=bid, volume=1,
                time_msc=ms, flags=6, volume_real=1.0)


class _Tape:
    def __init__(self, rows=None):
        self.rows = list(rows or [_tick(0), _tick(10_000)])
        self.visible = len(self.rows)
        self.native_error = (1, LABEL)
        self.missing_minutes = set()

    def append(self, *rows):
        self.rows.extend(rows)
        self.visible = len(self.rows)

    @property
    def observed(self):
        return self.rows[:self.visible]


@pytest.fixture
def tick_world(world, monkeypatch):
    """Only native responses are synthetic; no authority or coverage gate is patched."""
    fake = world.fake
    fake.when = BASE
    fake.symbol.chart_mode = 0  # Declared Bid chart, never inferred from a missing field.
    tapes, failed_cash, requests = {}, set(), []
    diagnostic = [1, LABEL]
    monkeypatch.setattr(fake, "COPY_TICKS_ALL", 0xFFFFFFFF, raising=False)
    monkeypatch.setattr(fake, "RES_S_OK", 1, raising=False)

    def symbol_info(symbol):
        assert symbol in tapes
        return SimpleNamespace(**{**vars(fake.symbol), "name": symbol})

    def quote(symbol):
        return SimpleNamespace(**tapes[symbol].observed[-1])

    def tick_page(symbol, start, end, flags):
        assert flags == fake.COPY_TICKS_ALL
        assert start.utcoffset() == end.utcoffset() == timedelta(0)
        requests.append(("ticks", symbol, start, end))
        diagnostic[:] = tapes[symbol].native_error
        lower, upper = int(start.timestamp()) * 1000, int(end.timestamp()) * 1000
        rows = [row for row in tapes[symbol].observed if lower <= row["time_msc"] <= upper]
        return np.array([tuple(row[name] for name in TICK_FIELDS) for row in rows], dtype=TICK_DTYPE)

    def rates(symbol, timeframe, start, end):
        assert timeframe == backend.MT5Connector.TIMEFRAME_MAP["M1"]
        assert start.utcoffset() == end.utcoffset() == timedelta(0)
        requests.append(("M1", symbol, start, end))
        diagnostic[:] = (1, LABEL)
        groups = {}
        for row in tapes[symbol].observed:
            minute = row["time_msc"] // 60_000 * 60
            groups.setdefault(minute, []).append(row)
        result = []
        for minute, rows in sorted(groups.items()):
            if minute in tapes[symbol].missing_minutes:
                continue
            if int(start.timestamp()) <= minute <= int(end.timestamp()):
                bids = [row["bid"] for row in rows]
                spread = round((rows[0]["ask"] - rows[0]["bid"]) / fake.symbol.point)
                result.append((minute, bids[0], max(bids), min(bids), bids[-1],
                               len(rows), spread, len(rows)))
        return np.array(result, dtype=RATE_DTYPE)

    def profit(side, symbol, volume, price_open, price_close):
        assert side in (0, 1) and symbol in tapes
        if symbol in failed_cash:
            return None
        return (1 if side == 0 else -1) * (price_close - price_open) * volume * 100000.0

    monkeypatch.setattr(fake, "symbol_select", lambda symbol, selected: selected and symbol in tapes)
    monkeypatch.setattr(fake, "symbol_info", symbol_info)
    monkeypatch.setattr(fake, "symbol_info_tick", quote)
    monkeypatch.setattr(fake, "copy_ticks_range", tick_page, raising=False)
    monkeypatch.setattr(fake, "copy_rates_range", rates, raising=False)
    monkeypatch.setattr(fake, "last_error", lambda: tuple(diagnostic), raising=False)
    monkeypatch.setattr(fake, "order_calc_profit", profit)

    def account(*symbols):
        for symbol in symbols or ("EURUSD",):
            tapes[symbol] = _Tape()
        return world.open()

    return SimpleNamespace(account=account, tapes=tapes, failed_cash=failed_cash,
                           requests=requests, fake=fake, ledger=world.ledger)


def _open(account, tape, *, symbol="EURUSD", side=0, number=1):
    reservation_id = f"{number:032x}"
    quote = tape.observed[-1]
    entry = quote["ask"] if side == 0 else quote["bid"]
    stop, target = (1.0900, 1.1200) if side == 0 else (1.1100, 1.0800)
    account.ledger.reserve_pre_send_intent(
        symbol=symbol, timeframe="H1", regime="TREND", strategy_name="EMACrossoverStrategy",
        magic=123, entry_price=entry, volume=1.0, initial_stop=stop,
        risk_cash=abs(entry - stop) * 100000.0, book_role="LOCKBOX_VALIDATED_CHAMPION",
        candidate_id="synthetic-owner-not-qualified", reservation_id=reservation_id,
    )
    fill = account.paper.send_market_order(symbol, side, 1.0, sl=stop, tp=target,
        magic=123, comment=LABEL, reservation_id=reservation_id)
    assert fill.success and fill.destination == "PAPER", fill
    account.ledger.bind_pre_send_intent(reservation_id, position_id=fill.position_id,
        opening_deal_ticket=fill.deal, opening_order_ticket=fill.order)
    row = account.ledger.paper_fill_for_reservation(reservation_id)
    assert row["fill_policy_id"] == MODEL
    assert row["entry_execution_evidence"]["event"] == "ENTRY"
    assert row["time_msc"] == quote["time_msc"]
    return fill, reservation_id


def _row(account, reservation):
    return account.ledger.paper_fill_for_reservation(reservation)


def _outs(account):
    return [deal for deal in account.ledger.paper_deals() if deal["entry_kind"] == "OUT"]


def _terminal_audit(previous, minute, code, status="UNAVAILABLE"):
    expected = {**deepcopy(previous), "status": status, "overdue": True,
                "terminal_failure_minute_msc": minute, "terminal_failure_code": code}
    if expected["first_failure_minute_msc"] is None:
        expected.update(first_failure_minute_msc=minute, first_failure_code=code,
                        first_failure_status=status)
    return expected


def _financial_sync(account):
    return account.ledger.sync(closing_deals=_outs(account), open_position_ids=set(),
        position_history_provider=account.paper.get_position_history, resolve_cell=operational._resolve)


def _error(account, expected_fragment):
    with pytest.raises(backend.PaperProtectionDataError) as caught:
        account.paper.poll_paper_positions()
    visible = account.paper.paper_protection_diagnostics()
    assert caught.value.symbol_errors == visible
    assert any(expected_fragment in message for messages in visible.values() for message in messages), visible
    return caught.value


@pytest.mark.parametrize("side,events,expected_price,event", [
    (0, ((70_000, 1.0890, 1.0892), (80_000, 1.1000, 1.1002)), 1.0890, "SL"),
    (0, ((70_000, 1.1210, 1.1212), (80_000, 1.0800, 1.0802)), 1.1200, "TP"),
    (1, ((70_000, 1.1000, 1.1110), (80_000, 1.1000, 1.1002)), 1.1110, "SL"),
], ids=["recovered-intrabar-sl", "tp-before-later-sl", "short-ask-only-sl"])
def test_native_first_touch_survives_latest_quote_recovery(tick_world, side, events, expected_price, event):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    fill, reservation = _open(account, tape, side=side)
    tape.append(_tick(60_000), *[_tick(*values) for values in events], _tick(120_000))
    assert account.paper.get_positions() == []
    close, = _outs(account)
    assert close["price"] == pytest.approx(expected_price)
    assert close["time_msc"] == BASE_MSC + 70_000
    assert close["execution_evidence"]["event"] == event
    assert close["execution_evidence"]["model_id"] == MODEL
    assert close["execution_evidence"]["ask_source"] == "NATIVE"
    assert _row(account, reservation)["quote_cursor"] == close["quote_cursor"]
    expected = (1 if side == 0 else -1) * (expected_price - fill.price) * fill.volume * 100000
    assert account.ledger.paper_cash_adjustment() == pytest.approx(expected)


def test_pre_entry_excursions_and_later_same_millisecond_ordinals_are_distinct(tick_world):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    tape.rows = [_tick(0), _tick(10_128, 1.0800, 1.0802),
                 _tick(10_128, 1.1300, 1.1302), _tick(10_128)]
    tape.visible = len(tape.rows)
    fill, reservation = _open(account, tape)
    entry = deepcopy(_row(account, reservation)["entry_quote_cursor"])
    assert entry["time_msc"] == BASE_MSC + 10_128 and entry["same_msc_ordinal"] == 2
    tape.append(_tick(10_128), _tick(10_128, 1.0890, 1.0892), _tick(10_128), _tick(60_000))
    assert account.paper.poll_paper_positions() == []
    close, = _outs(account)
    assert fill.price == pytest.approx(1.1002)
    assert close["price"] == pytest.approx(1.0890)
    assert close["quote_cursor"]["time_msc"] == entry["time_msc"]
    assert close["quote_cursor"]["same_msc_ordinal"] == 4
    assert close["execution_evidence"]["event"] == "SL"
    assert _row(account, reservation)["entry_quote_cursor"] == entry


@pytest.mark.parametrize("operation", ["modify", "partial"])
def test_old_protection_is_drained_before_a_mutation(tick_world, operation):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    fill, reservation = _open(account, tape)
    tape.append(_tick(60_000))
    stale = account.paper.get_position_by_ticket(fill.position_id)
    tape.append(_tick(70_000, 1.0890, 1.0892), _tick(80_000), _tick(120_000))
    if operation == "modify":
        result = account.paper.modify_position(stale, sl=1.0950)
    else:
        result = account.paper.close_position_partial(stale, percent=50, operation_id="after-old-stop")
    assert not result.success
    close, = _outs(account)
    assert close["volume"] == pytest.approx(1.0)
    assert close["price"] == pytest.approx(1.0890)
    assert close["execution_evidence"]["event"] == "SL"
    assert _row(account, reservation)["status"] == "CLOSED"
    assert len(_row(account, reservation)["level_history"]) == 1


def test_stale_dto_cannot_oversize_a_partial_or_widen_the_durable_stop(tick_world):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    fill, reservation = _open(account, tape)
    tape.append(_tick(60_000))
    stale = account.paper.get_position_by_ticket(fill.position_id)
    tape.append(_tick(70_000, 1.0930, 1.0932), _tick(80_000), _tick(120_000))
    assert account.paper.modify_position(stale, sl=1.0950).success
    assert not account.paper.modify_position(stale, sl=1.0920).success
    first = account.paper.close_position_partial(stale, percent=50, operation_id="partial-one")
    assert first.success and first.volume == pytest.approx(0.5)
    replay = account.paper.close_position_partial(stale, percent=50, operation_id="partial-one")
    assert replay.success and (replay.deal, replay.price, replay.volume) == (first.deal, first.price, first.volume)
    second = account.paper.close_position_partial(stale, percent=50, operation_id="partial-two")
    assert second.success and second.volume == pytest.approx(0.25)
    row = _row(account, reservation)
    assert row["volume"] == pytest.approx(0.25) and row["sl"] == pytest.approx(1.0950)
    assert len(_outs(account)) == 2
    tape.append(_tick(130_000, 1.0940, 1.0942), _tick(140_000), _tick(180_000))
    assert account.paper.poll_paper_positions() == []
    close = _outs(account)[-1]
    assert close["volume"] == pytest.approx(0.25)
    assert close["price"] == pytest.approx(1.0940)
    assert close["time_msc"] == BASE_MSC + 130_000


def test_close_cursor_and_original_fill_replay_survive_cold_reopen(tick_world):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    fill, reservation = _open(account, tape)
    tape.append(_tick(60_000), _tick(70_000, 1.0890, 1.0892), _tick(80_000), _tick(120_000))
    assert account.paper.poll_paper_positions() == []
    history = account.paper.get_position_history(fill.position_id)
    cash = account.ledger.paper_cash_snapshot()
    account.ledger.close()
    account.ledger = tick_world.ledger(account.identity, account.config)
    account.paper = backend.PaperExecutionConnector(account.data, account.ledger, account.identity)
    assert account.paper.get_position_history(fill.position_id) == history
    assert account.paper.poll_paper_positions() == account.paper.poll_paper_positions() == []
    replay = account.paper.send_market_order("EURUSD", 0, 1, sl=1.09, tp=1.12,
                                            magic=123, reservation_id=reservation)
    assert replay.success and (replay.position_id, replay.deal, replay.price) == (fill.position_id, fill.deal, fill.price)
    assert account.ledger.paper_cash_snapshot() == cash
    assert len(_outs(account)) == 1


@pytest.mark.parametrize("fault", ["native-partial", "missing-M1"])
def test_unverified_history_keeps_cursor_and_visible_error(tick_world, fault):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    _, reservation = _open(account, tape)
    before = deepcopy(_row(account, reservation))
    tape.append(_tick(60_000), _tick(70_000, 1.0890, 1.0892), _tick(80_000), _tick(120_000))
    if fault == "native-partial":
        tape.native_error = (-10005, "SYNTHETIC non-null partial timeout")
        expected = "QUOTE_TICK_PAGE_NATIVE_ERROR"
    else:
        tape.missing_minutes.add(BASE_MSC // 1000)
        expected = "PAPER_M1_SOURCE_UNAVAILABLE"
    error = _error(account, expected)
    assert tuple(error.partial_positions) == ()
    after = _row(account, reservation)
    if fault == "missing-M1":
        expected_audit = {**before["quote_audit"], "first_failure_minute_msc": BASE_MSC,
            "first_failure_code": expected, "first_failure_status": "RETRYABLE",
            "status": "PENDING", "overdue": True}
        assert after == {**before, "quote_audit": expected_audit}
    else:
        assert after == before
    assert after["quote_cursor"] == before["quote_cursor"]
    assert after["volume"] == before["volume"] and after["level_history"] == before["level_history"]
    assert _outs(account) == [] and account.ledger.paper_cash_adjustment() == 0
    with pytest.raises(backend.PaperProtectionDataError):
        account.paper.get_risk_account_info()


def test_healthy_symbol_still_closes_but_incomplete_account_risk_refuses(tick_world):
    account = tick_world.account("EURUSD", "EURGBP")
    bad, good = tick_world.tapes["EURUSD"], tick_world.tapes["EURGBP"]
    _, bad_reservation = _open(account, bad)
    _, good_reservation = _open(account, good, symbol="EURGBP", number=2)
    before = deepcopy(_row(account, bad_reservation))
    for tape in (bad, good):
        tape.append(_tick(60_000), _tick(70_000, 1.0890, 1.0892), _tick(80_000), _tick(120_000))
    bad.native_error = (-10005, "SYNTHETIC symbol-specific partial response")
    error = _error(account, "QUOTE_TICK_PAGE_NATIVE_ERROR")
    assert set(error.symbol_errors) == {"EURUSD"}
    assert _row(account, bad_reservation) == before
    assert _row(account, good_reservation)["status"] == "CLOSED"
    close, = _outs(account)
    assert close["symbol"] == "EURGBP" and close["price"] == pytest.approx(1.0890)
    with pytest.raises(backend.PaperProtectionDataError):
        account.paper.get_risk_account_info()
    assert len(_outs(account)) == 1


def test_native_binding_mismatch_freezes_all_position_cursors(tick_world):
    account = tick_world.account("EURUSD", "EURGBP")
    for number, symbol in enumerate(("EURUSD", "EURGBP"), 1):
        tape = tick_world.tapes[symbol]
        _open(account, tape, symbol=symbol, number=number)
        tape.append(_tick(60_000), _tick(70_000, 1.0890, 1.0892), _tick(120_000))
    before = deepcopy(account.ledger.paper_positions())
    requests = len(tick_world.requests)
    tick_world.fake.accounts[account.login].login = account.login + 1
    with pytest.raises(backend.NativeAccountBindingError):
        account.paper.poll_paper_positions()
    assert account.ledger.paper_positions() == before
    assert len(tick_world.requests) == requests and _outs(account) == []


@pytest.mark.parametrize("failure", ["funding", "cash"])
def test_unpriced_first_touch_latches_before_valuation_and_survives_cold_recovery(tick_world, failure):
    import hashlib

    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    if failure == "funding":
        tick_world.fake.symbol.swap_mode = 4
        tick_world.fake.symbol.swap_long, tick_world.fake.symbol.swap_short = -2.0, 1.0
        utc = lambda value: value.isoformat().replace("+00:00", "Z")
        terms = dict(schema_version="fxpm.paper-funding-terms.v1", symbol="EURUSD", swap_mode=4,
            swap_long=-2.0, swap_short=1.0, currency="USD", rollover_timezone="UTC",
            coverage_start_utc=utc(BASE - timedelta(hours=1)), coverage_end_utc=utc(BASE + timedelta(seconds=65)),
            rollover_schedule=[], source_ref=LABEL, provenance_sha256="e" * 64)
        account.data.paper_funding_terms_provider = lambda _symbol, **_kwargs: deepcopy(terms)
    fill, reservation = _open(account, tape)
    before = deepcopy(_row(account, reservation))
    cash_before, deals_before = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    tape.append(_tick(60_000), _tick(70_000, 1.0890, 1.0892), _tick(80_000), _tick(120_000))
    if failure == "cash":
        tick_world.failed_cash.add("EURUSD")
    expected = "PAPER_FUNDING_COVERAGE_UNAVAILABLE" if failure == "funding" else "PAPER_INVALID_NATIVE_PROFIT"
    _error(account, expected)

    # Latching observed execution evidence is distinct from recording valued cash.
    native_trigger = dict(time_msc=BASE_MSC + 70_000, same_msc_ordinal=0,
        bid=1.0890, ask=1.0892, last=1.0890, flags=6, volume=1, volume_real=1.0)
    expected_cursor = dict(time_msc=native_trigger["time_msc"], same_msc_ordinal=0,
        prefix_sha256=hashlib.sha256((json.dumps(native_trigger, sort_keys=True,
            separators=(",", ":"), allow_nan=False) + "\n").encode()).hexdigest())
    expected_evidence = dict(model_id=MODEL, bid=1.0890, ask=1.0892, ask_source="NATIVE", event="SL")
    latched = deepcopy(_row(account, reservation))
    assert latched["quote_cursor"] == expected_cursor
    assert latched["last_execution_evidence"] == expected_evidence
    assert latched.keys() == before.keys()
    transition_fields = {"quote_cursor", "last_execution_evidence", "quote_transition_sha256"}
    assert {key: value for key, value in latched.items() if key not in transition_fields} == {
        key: value for key, value in before.items() if key not in transition_fields}
    assert latched["status"] == "OPEN" and latched["volume"] == 1.0
    assert account.ledger.paper_deals() == deals_before and _outs(account) == []
    assert account.ledger.paper_cash_snapshot() == cash_before
    history = account.paper.get_position_history(fill.position_id)
    assert history["quote_cursor"] == expected_cursor
    assert history["last_execution_evidence"] == expected_evidence

    # Neither current prices nor rediscovery of unavailable history may replace the trigger.
    account.ledger.close()
    account.ledger = tick_world.ledger(account.identity, account.config)
    account.paper = backend.PaperExecutionConnector(account.data, account.ledger, account.identity)
    assert _row(account, reservation) == latched
    assert account.paper.get_position_history(fill.position_id) == history
    tape.rows, tape.visible = [_tick(180_000)], 1
    tape.native_error = (-4, "SYNTHETIC original tick history temporarily not found")
    requests_before = len(tick_world.requests)
    if failure == "funding":
        # Entry terms expired before the actual trigger. No renewal API is assumed.
        _error(account, expected)
        pending = {**latched["quote_audit"],
            "first_failure_minute_msc": latched["quote_audit"]["pending_minute_msc"],
            "first_failure_code": "QUOTE_TICK_PAGE_NATIVE_ERROR", "first_failure_status": "RETRYABLE",
            "status": "PENDING", "overdue": True}
        assert pending["terminal_failure_code"] is pending["terminal_failure_minute_msc"] is None
        assert _row(account, reservation) == {**latched, "quote_audit": pending}
        assert account.paper.get_position_history(fill.position_id) == {**history, "quote_audit": pending}
        assert account.ledger.paper_deals() == deals_before
        assert account.ledger.paper_cash_snapshot() == cash_before
        assert account.ledger.paper_statistics_summary()["raw_closed_positions"] == 0
        assert len(tick_world.requests) > requests_before
    else:
        tick_world.failed_cash.clear()
        assert account.paper.poll_paper_positions() == []
        assert len(tick_world.requests) == requests_before
        close, = _outs(account)
        assert close["time_msc"] == expected_cursor["time_msc"]
        assert close["quote_cursor"] == expected_cursor
        assert close["execution_evidence"] == expected_evidence
        assert close["price"] == pytest.approx(1.0890) and close["volume"] == 1.0
        assert close["profit"] == pytest.approx((1.0890 - fill.price) * fill.volume * 100000)
        closed = _row(account, reservation)
        assert closed["status"] == "CLOSED" and closed["volume"] == 0
        assert closed["quote_cursor"] == expected_cursor
        assert closed["last_execution_evidence"] == expected_evidence
        assert closed["entry_quote_cursor"] == before["entry_quote_cursor"]
        assert closed["level_history"] == before["level_history"]
        settled_history = account.paper.get_position_history(fill.position_id)
        settled_cash, settled_deals = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
        assert account.paper.poll_paper_positions() == []
        assert len(tick_world.requests) > requests_before
        assert tick_world.requests[requests_before][0] == "ticks"
        assert tick_world.requests[requests_before][2] == BASE
        assert "QUOTE_TICK_PAGE_NATIVE_ERROR" in account.paper.paper_protection_diagnostics()["EURUSD"]
        pending = {**closed["quote_audit"],
            "first_failure_minute_msc": closed["quote_audit"]["pending_minute_msc"],
            "first_failure_code": "QUOTE_TICK_PAGE_NATIVE_ERROR", "first_failure_status": "RETRYABLE",
            "status": "PENDING", "overdue": True}
        assert pending["terminal_failure_code"] is pending["terminal_failure_minute_msc"] is None
        assert _row(account, reservation) == {**closed, "quote_audit": pending}
        assert account.paper.get_position_history(fill.position_id) == {**settled_history, "quote_audit": pending}
        assert account.ledger.paper_cash_snapshot() == settled_cash
        assert account.ledger.paper_deals() == settled_deals and len(_outs(account)) == 1
        result = _financial_sync(account)
        assert result.emitted_records == []
        record, = [json.loads(line) for line in account.ledger.ledger_path.read_text(encoding="utf-8").splitlines()]
        assert account.ledger.paper_outcome_admission(record) == "PENDING"
        assert result.paper_statistics["raw_closed_positions"] == result.paper_statistics["pending_closed_positions"] == 1
        assert result.paper_statistics["unavailable_closed_positions"] == 0
        assert result.paper_statistics["clean_closed_positions"] == 0
        assert account.ledger.paper_cash_adjustment() == pytest.approx(close["profit"])


@pytest.mark.parametrize("chart_mode", [None, 1], ids=["unknown", "Last"])
def test_paper_entry_requires_explicit_supported_native_trigger_basis(tick_world, chart_mode):
    account = tick_world.account("EURUSD")
    tick_world.fake.symbol.chart_mode = chart_mode
    reservation = operational._reserve(account)
    result = account.paper.send_market_order("EURUSD", 0, 1, sl=reservation["initial_stop"],
        tp=1.12, magic=123, reservation_id=reservation["reservation_id"])
    assert not result.success and result.retcode_description == "PAPER_TRIGGER_BASIS_UNSUPPORTED"
    assert account.ledger.paper_positions() == []


def test_forming_minute_exit_is_provisional_until_native_closed_minute_audit(tick_world):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    fill, reservation = _open(account, tape)
    tape.append(_tick(20_000, 1.0890, 1.0892), _tick(30_000))
    assert account.paper.poll_paper_positions() == []
    provisional = deepcopy(_row(account, reservation))
    assert provisional["status"] == "CLOSED" and provisional["volume"] == 0
    assert provisional["quote_cursor"]["time_msc"] == BASE_MSC + 20_000
    assert provisional["last_execution_evidence"]["event"] == "SL"
    assert provisional["quote_audit"]["status"] == "PENDING"
    assert provisional["quote_audit"]["pending_minute_msc"] == BASE_MSC
    assert provisional["quote_audit"]["corroborated_minute_count"] == 0
    first = _financial_sync(account)
    assert first.emitted_records == [] and first.paper_statistics["pending_closed_positions"] == 1
    raw = account.ledger.ledger_path.read_bytes()
    cash, deals = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    tape.append(_tick(60_000))
    assert account.paper.poll_paper_positions() == []
    audited = _row(account, reservation)
    assert {key: value for key, value in audited.items() if key != "quote_audit"} == {
        key: value for key, value in provisional.items() if key != "quote_audit"}
    assert audited["quote_audit"]["status"] == "CLEAN"
    assert audited["quote_audit"]["pending_minute_msc"] is None
    assert audited["quote_audit"]["corroborated_minute_count"] == 1
    assert audited["quote_audit"]["last_corroborated_minute_msc"] == BASE_MSC
    assert account.paper.get_position_history(fill.position_id)["quote_audit"] == audited["quote_audit"]
    assert account.ledger.paper_cash_snapshot() == cash and account.ledger.paper_deals() == deals
    admitted = _financial_sync(account)
    record, = admitted.emitted_records
    assert account.ledger.paper_outcome_admission(record) == "CLEAN"
    assert admitted.paper_statistics["clean_closed_positions"] == 1
    assert admitted.paper_statistics["pending_closed_positions"] == 0
    assert account.ledger.ledger_path.read_bytes() == raw
    assert _financial_sync(account).emitted_records == []


def test_rewritten_earlier_minute_prefix_holds_execution_even_when_cursor_record_matches(tick_world):
    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    _, reservation = _open(account, tape)
    before = deepcopy(_row(account, reservation))
    original = deepcopy(tape.rows[0])
    # Change an earlier, different-ms record; leave the recorded entry/cursor tick intact.
    tape.rows[0] = _tick(0, 1.0980, 1.0982)
    tape.append(_tick(60_000), _tick(70_000, 1.0890, 1.0892), _tick(80_000), _tick(120_000))
    _error(account, "PAPER_MINUTE_PREFIX_CHANGED")
    divergent = _terminal_audit(before["quote_audit"], BASE_MSC, "PAPER_MINUTE_PREFIX_CHANGED", "DIVERGENT")
    assert _row(account, reservation) == {**before, "quote_audit": divergent}
    assert _row(account, reservation)["entry_quote_cursor"] == before["entry_quote_cursor"]
    assert _outs(account) == [] and account.ledger.paper_cash_adjustment() == 0
    tape.rows[0] = original
    _error(account, "PAPER_QUOTE_AUDIT_DIVERGENT")
    assert _row(account, reservation) == {**before, "quote_audit": divergent}
    assert _outs(account) == [] and account.ledger.paper_cash_adjustment() == 0


def test_one_poll_shares_same_symbol_native_pages_across_h1_d1_then_refreshes(tick_world, monkeypatch):
    from unittest.mock import Mock

    account = tick_world.account("EURUSD")
    tape = tick_world.tapes["EURUSD"]
    h1, _ = _open(account, tape)
    reservation = "2" * 32
    # A second independent, explicitly reserved D1 slot uses the same native symbol.
    account.ledger.reserve_pre_send_intent(symbol="EURUSD", timeframe="D1", regime="TREND",
        strategy_name="EMACrossoverStrategy", magic=456, entry_price=1.1002, volume=1.0,
        initial_stop=1.09, risk_cash=1020.0, book_role="LOCKBOX_VALIDATED_CHAMPION",
        candidate_id="synthetic-d1-owner-not-qualified", reservation_id=reservation)
    d1 = account.paper.send_market_order("EURUSD", 0, 1.0, sl=1.09, tp=1.12,
        magic=456, comment=LABEL, reservation_id=reservation)
    assert d1.success and d1.destination == "PAPER"
    account.ledger.bind_pre_send_intent(reservation, position_id=d1.position_id,
        opening_deal_ticket=d1.deal, opening_order_ticket=d1.order)
    assert h1.position_id != d1.position_id

    native_quote = Mock(wraps=tick_world.fake.symbol_info_tick)
    monkeypatch.setattr(tick_world.fake, "symbol_info_tick", native_quote)
    tape.append(_tick(60_000, 1.1010, 1.1012), _tick(70_000, 1.1020, 1.1022),
                _tick(80_000, 1.1030, 1.1032), _tick(120_000, 1.1040, 1.1042))
    begin = len(tick_world.requests)
    positions = account.paper.poll_paper_positions()
    first_requests = tick_world.requests[begin:]
    assert {p.identifier for p in positions} == {h1.position_id, d1.position_id}
    assert all(p.volume == 1.0 and p.sl == 1.09 and p.tp == 1.12 for p in positions)
    assert all(p.price_current == pytest.approx(1.1040) for p in positions)
    assert all(p.time_update == BASE + timedelta(seconds=120) for p in positions)
    assert native_quote.call_count == 1
    assert {request[0] for request in first_requests} == {"ticks", "M1"}
    assert len(first_requests) == len(set(first_requests)), "Identical native ranges must be shared within one poll"
    first_m1_minutes = [
        (symbol, minute) for kind, symbol, start, end in first_requests if kind == "M1"
        for minute in range(int(start.timestamp()) // 60, int(end.timestamp()) // 60 + 1)
    ]
    assert len(first_m1_minutes) == len(set(first_m1_minutes))
    assert _outs(account) == [] and account.ledger.paper_cash_adjustment() == 0

    tape.append(_tick(130_000, 1.1045, 1.1047), _tick(150_000, 1.1050, 1.1052),
                _tick(180_000, 1.1060, 1.1062))
    begin = len(tick_world.requests)
    refreshed = account.paper.poll_paper_positions()
    second_requests = tick_world.requests[begin:]
    assert {p.identifier for p in refreshed} == {h1.position_id, d1.position_id}
    assert all(p.price_current == pytest.approx(1.1060) for p in refreshed)
    assert all(p.time_update == BASE + timedelta(seconds=180) for p in refreshed)
    assert native_quote.call_count == 2
    assert {request[0] for request in second_requests} == {"ticks", "M1"}
    assert len(second_requests) == len(set(second_requests))
    assert any(kind == "ticks" and start == BASE + timedelta(seconds=180)
               for kind, _symbol, start, _end in second_requests)
    assert _outs(account) == [] and account.ledger.paper_cash_adjustment() == 0
