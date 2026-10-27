"""Operational software checks with invented accounts, quotes and caller authority.

The selected native connector and PAPER adapter are real; the native module is fake.
Reservations stand for test-only upstream approval, never qualification or live grants.
Windows writer leases and the existing durable outcome ledger remain unmocked.
"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import pm_live_outcomes as outcomes
import pm_mt5 as backend


assert Path(outcomes.__file__).resolve() == PRODUCT_ROOT / "pm_live_outcomes.py"
assert Path(backend.__file__).resolve() == PRODUCT_ROOT / "pm_mt5.py"
LABEL = "SYNTHETIC_OPERATIONAL_SOFTWARE_ONLY_NO_TRADING_AUTHORITY"


class _FakeNative:
    ORDER_TYPE_BUY, ORDER_TYPE_SELL = 0, 1
    ORDER_FILLING_FOK, ORDER_FILLING_IOC, ORDER_FILLING_RETURN = 0, 1, 2
    TRADE_ACTION_DEAL, ORDER_TIME_GTC = 1, 0
    SYMBOL_FILLING_FOK, SYMBOL_FILLING_IOC, SYMBOL_TRADE_EXECUTION_MARKET = 1, 2, 2
    RES_S_OK, COPY_TICKS_ALL = 1, 0xFFFFFFFF
    TICK_FIELDS = ("time", "bid", "ask", "last", "volume", "time_msc", "flags", "volume_real")
    TICK_DTYPE = [("time", "i8"), ("bid", "f8"), ("ask", "f8"), ("last", "f8"),
                  ("volume", "u8"), ("time_msc", "i8"), ("flags", "u4"), ("volume_real", "f8")]
    RATE_DTYPE = [("time", "i8"), ("open", "f8"), ("high", "f8"), ("low", "f8"),
                  ("close", "f8"), ("tick_volume", "u8"), ("spread", "i4"), ("real_volume", "u8")]

    def __init__(self):
        self.accounts, self.quotes, self.tick_tapes = {}, {}, {}
        self.active_login = None
        self.when = datetime.now(timezone.utc).replace(microsecond=0)
        self.symbol = SimpleNamespace(name="EURUSD", digits=5, point=0.00001,
            trade_tick_value=1.0, trade_tick_size=0.00001, trade_contract_size=100000.0,
            volume_min=0.01, volume_max=100.0, volume_step=0.01, spread=20,
            spread_float=True, swap_long=0.0, swap_short=0.0, swap_mode=0,
            trade_stops_level=10, trade_freeze_level=0, visible=True, trade_mode=4,
            path="SYNTHETIC/Forex", filling_mode=0, trade_exemode=0, chart_mode=0)
        self.order_send = Mock(side_effect=AssertionError("No real/native execution belongs in PAPER tests"))
        self.order_check = Mock(side_effect=AssertionError("PAPER must not preflight a native order"))
        self.history_deals_get = Mock(side_effect=AssertionError("PAPER closes belong to the local ledger"))

    def add_account(self, login):
        self.accounts[login] = SimpleNamespace(login=login, server="SYNTHETIC-SERVER", company=LABEL,
            balance=10000.0, equity=10000.0, margin=0.0, margin_free=10000.0,
            margin_level=0.0, profit=0.0, leverage=100, currency="USD",
            trade_allowed=True, trade_expert=True, trade_mode=0, margin_mode=2, hedge_allowed=True)
        self.set_quote(login, 1.1000, 1.1002)

    def set_quote(self, login, bid, ask, *, when=None):
        # Only explicit fixture observations append; reading a quote never invents ticks.
        milliseconds = int((when or self.when).timestamp() * 1000)
        row = dict(time=milliseconds // 1000, time_msc=milliseconds,
            bid=float(bid), ask=float(ask), last=float(bid), volume=1, flags=6, volume_real=1.0)
        self.tick_tapes.setdefault(login, []).append(row)
        self.quotes[login] = SimpleNamespace(**row)

    def last_error(self):
        return self.RES_S_OK, LABEL

    def copy_ticks_range(self, symbol, start, end, flags):
        assert symbol == "EURUSD" and flags == self.COPY_TICKS_ALL
        assert start.utcoffset() == end.utcoffset() == timedelta(0)
        lower, upper = int(start.timestamp()) * 1000, int(end.timestamp()) * 1000
        rows = [row for row in self.tick_tapes[self.active_login] if lower <= row["time_msc"] <= upper]
        return np.array([tuple(row[name] for name in self.TICK_FIELDS) for row in rows], dtype=self.TICK_DTYPE)

    def copy_rates_range(self, symbol, timeframe, start, end):
        assert symbol == "EURUSD" and timeframe == backend.MT5Connector.TIMEFRAME_MAP["M1"]
        assert start.utcoffset() == end.utcoffset() == timedelta(0)
        groups = {}
        for row in self.tick_tapes[self.active_login]:
            minute = row["time_msc"] // 60000 * 60
            groups.setdefault(minute, []).append(row)
        rates = []
        for minute, rows in sorted(groups.items()):
            if int(start.timestamp()) <= minute <= int(end.timestamp()):
                bids = [row["bid"] for row in rows]
                spread = round((rows[0]["ask"] - rows[0]["bid"]) / self.symbol.point)
                rates.append((minute, bids[0], max(bids), min(bids), bids[-1], len(rows), spread, len(rows)))
        # Empty minutes remain absent; bars contain only the explicit invented observations.
        return np.array(rates, dtype=self.RATE_DTYPE)

    def initialize(self, **_kwargs):
        return True

    def login(self, login, *, password, server):
        assert password == "SYNTHETIC_TEST_ONLY" and server == "SYNTHETIC-SERVER"
        assert login in self.accounts
        self.active_login = login
        return True

    def shutdown(self):
        return None

    def account_info(self):
        return self.accounts[self.active_login]

    def terminal_info(self):
        return SimpleNamespace(connected=True)

    def symbol_select(self, symbol, selected):
        return symbol == "EURUSD" and selected is True

    def symbol_info(self, symbol):
        assert symbol == "EURUSD"
        return self.symbol

    def symbol_info_tick(self, symbol):
        assert symbol == "EURUSD"
        return self.quotes[self.active_login]

    def positions_get(self, **_kwargs):
        return ()

    @staticmethod
    def order_calc_profit(side, symbol, volume, price_open, price_close):
        assert symbol == "EURUSD" and side in (0, 1)
        return (1 if side == 0 else -1) * (price_close - price_open) * volume * 100000.0

    @staticmethod
    def order_calc_margin(_side, _symbol, volume, price):
        return abs(volume * price * 100.0)


def _ledger_config(path):
    return SimpleNamespace(output_dir=path, log_dir=path, live_outcome_feedback_enabled=True,
        live_outcome_require_position_history=True, live_outcome_feed_cycle_drift=False,
        live_outcome_volume_tolerance=1e-8)


@pytest.fixture
def world(tmp_path, monkeypatch):
    fake = _FakeNative()
    monkeypatch.setattr(backend, "mt5", fake)
    monkeypatch.setattr(backend, "MT5_AVAILABLE", True)
    monkeypatch.setattr(backend, "_NATIVE_ACTIVE_CONNECTOR", None)
    monkeypatch.setattr(backend, "_NATIVE_LOCAL", threading.local())
    ledgers = []
    expected_native_calls = {"send": 0, "check": 0}

    def allow_fake_native_submission(count, on_send):
        assert fake.order_send.call_count == fake.order_check.call_count == 0
        expected_native_calls.update(send=count, check=count)
        fake.order_check.side_effect = lambda _request: SimpleNamespace(retcode=0, comment=LABEL)
        fake.order_send.side_effect = on_send

    def new_ledger(identity, config):
        ledger = outcomes.LiveOutcomeLedger(config, runtime_identity=identity)
        ledgers.append(ledger)
        return ledger

    def native_connector(login):
        if login not in fake.accounts:
            fake.add_account(login)
        config = backend.MT5Config(login=login, password="SYNTHETIC_TEST_ONLY", server="SYNTHETIC-SERVER",
            expected_login=login, expected_server="SYNTHETIC-SERVER", expected_currency="USD")
        connector = backend.MT5Connector(config)
        assert connector.connect()
        return connector

    def open_account(login=900001):
        data = native_connector(login)
        identity = outcomes.runtime_account_identity(data.get_account_info(), "PAPER")
        config = _ledger_config(tmp_path / str(login))
        ledger = new_ledger(identity, config)
        return SimpleNamespace(data=data, identity=identity, config=config, ledger=ledger,
            paper=backend.PaperExecutionConnector(data, ledger, identity), login=login)

    yield SimpleNamespace(fake=fake, open=open_account, ledger=new_ledger, native=native_connector,
        allow_native_submission=allow_fake_native_submission)
    for ledger in reversed(ledgers):
        ledger.close()
    assert fake.order_send.call_count == expected_native_calls["send"]
    assert fake.order_check.call_count == expected_native_calls["check"]
    fake.history_deals_get.assert_not_called()


def _reserve(account, *, reservation_id="1" * 32, side=0, timeframe="H1", magic=123):
    entry = 1.1002 if side == 0 else 1.1000
    stop = 1.0900 if side == 0 else 1.1100
    return account.ledger.reserve_pre_send_intent(symbol="EURUSD", timeframe=timeframe, regime="TREND",
        strategy_name="EMACrossoverStrategy", magic=magic, entry_price=entry, volume=1.0,
        initial_stop=stop, risk_cash=abs(entry - stop) * 100000.0,
        book_role="LOCKBOX_VALIDATED_CHAMPION", candidate_id="synthetic-owner-not-qualified",
        reservation_id=reservation_id)


def _fill(account, *, reservation_id="1" * 32, side=0, bind=True):
    reservation = _reserve(account, reservation_id=reservation_id, side=side)
    result = account.paper.send_market_order("EURUSD", side, 1.0, sl=reservation["initial_stop"],
        tp=1.12 if side == 0 else 1.08, magic=123, comment=LABEL, reservation_id=reservation_id)
    assert result.success and result.destination == "PAPER", result
    if bind:
        account.ledger.bind_pre_send_intent(reservation_id, position_id=result.position_id,
            opening_deal_ticket=result.deal, opening_order_ticket=result.order)
    return result


def _resolve(_context):
    return {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND",
        "strategy_name": "EMACrossoverStrategy", "cell_match_status": "EXACT",
        "cell_match_reason": LABEL}


def _sync(account):
    return account.ledger.sync(closing_deals=account.paper.get_recent_closing_deals(),
        open_position_ids={p.identifier for p in account.paper.get_positions()},
        position_history_provider=account.paper.get_position_history, resolve_cell=_resolve)


@pytest.mark.parametrize("method", sorted(backend._NATIVE_MUTATORS))
def test_paper_destination_refuses_each_native_mutator_before_native_io(world, method):
    connector = backend.MT5Connector()
    connector.set_execution_destination("PAPER")
    if method == "send_market_order":
        result = connector.send_market_order("EURUSD", backend.OrderType.BUY, 1.0)
    else:
        position = SimpleNamespace(ticket=1, identifier=1, destination="PAPER")
        result = getattr(connector, method)(position)
    assert result.success is False and result.retcode == 10017
    assert result.retcode_description == "PAPER_NATIVE_MUTATION_REFUSED"
    assert result.destination == "PAPER"


def test_paper_destination_refuses_direct_native_preflight_and_send(world):
    connector = backend.MT5Connector()
    connector.set_execution_destination("PAPER")
    result = connector._preflight_order_request({"symbol": "EURUSD"})
    assert not result.success and result.retcode_description == "PAPER_NATIVE_MUTATION_REFUSED"
    with pytest.raises(backend.NativeSessionError, match="PAPER_NATIVE_MUTATION_REFUSED"):
        connector._send_native_order({"symbol": "EURUSD"})
    with pytest.raises(backend.NativeSessionError, match="PAPER_DESTINATION_CANNOT_BECOME_BROKER"):
        connector.set_execution_destination("BROKER")


def _bound_native_demo(world, login):
    """Invented account pins only; this is not an issued DEMO entry authority."""
    connector = world.native(login)
    identity = outcomes.runtime_account_identity(connector.get_account_info(), "MT5_DEMO")
    connector.set_execution_destination("MT5_DEMO")
    connector.set_runtime_identity(identity)
    return connector


def test_native_account_switch_restores_bound_identity_before_reads_and_submission(world):
    first = _bound_native_demo(world, 900001)
    second = _bound_native_demo(world, 900002)
    world.fake.set_quote(900002, 1.2, 1.2002)
    submitted = []

    def on_send(request):
        submitted.append((world.fake.active_login, deepcopy(request)))
        number = len(submitted)
        return SimpleNamespace(retcode=10009, deal=100 + number, order=200 + number,
            volume=request["volume"], price=request["price"])

    world.allow_native_submission(2, on_send)
    assert world.fake.active_login == 900002
    assert first.get_account_info().login == 900001 and world.fake.active_login == 900001
    assert second.get_tick("EURUSD").ask == pytest.approx(1.2002)
    assert world.fake.active_login == 900002
    a = first.send_market_order("EURUSD", backend.OrderType.BUY, 1.0, sl=1.09, tp=1.12)
    assert a.success and world.fake.active_login == 900001
    assert second.get_account_info().login == 900002 and world.fake.active_login == 900002
    assert first.get_tick("EURUSD").ask == pytest.approx(1.1002)
    assert world.fake.active_login == 900001
    b = second.send_market_order("EURUSD", backend.OrderType.BUY, 1.0, sl=1.19, tp=1.22)
    assert b.success and world.fake.active_login == 900002
    assert [login for login, _ in submitted] == [900001, 900002]
    assert [request["price"] for _, request in submitted] == pytest.approx([1.1002, 1.2002])
    assert [call.args[0] for call in world.fake.order_check.call_args_list] == [request for _, request in submitted]


def test_native_post_send_account_change_retains_tickets_and_stops_further_entry(world):
    connector = _bound_native_demo(world, 900001)
    world.fake.add_account(900002)
    submitted = []

    def change_identity_after_acceptance(request):
        assert world.fake.active_login == 900001
        submitted.append(deepcopy(request))
        world.fake.active_login = 900002
        return SimpleNamespace(retcode=10009, deal=401, order=402,
            volume=request["volume"], price=request["price"])

    world.allow_native_submission(1, change_identity_after_acceptance)
    result = connector.send_market_order("EURUSD", backend.OrderType.BUY, 1.0, sl=1.09, tp=1.12)
    assert not result.success and result.ambiguous and result.retcode == 10012
    assert result.retcode_description == "NATIVE_POST_SEND_IDENTITY_MISMATCH_PENDING_RECONCILIATION"
    assert (result.deal, result.order, result.volume, result.price) == (401, 402, 1.0, 1.1002)
    assert len(submitted) == 1 and world.fake.active_login == 900002
    retry = connector.send_market_order("EURUSD", backend.OrderType.BUY, 1.0, sl=1.09, tp=1.12)
    assert not retry.success and retry.retcode_description == result.retcode_description
    assert len(submitted) == 1


def test_partial_close_lost_response_replays_before_new_percentage_or_quote(world):
    account = world.open()
    fill = _fill(account)
    position = account.paper.get_position_by_ticket(fill.position_id)
    assert account.paper.modify_position(position, sl=1.095).success
    position = account.paper.get_position_by_ticket(fill.position_id)
    assert not account.paper.modify_position(position, sl=1.08).success
    world.fake.set_quote(account.login, 1.11, 1.1102)
    first = account.paper.close_position_partial(position, percent=50.0, operation_id="owner-policy-partial-once")
    assert first.success and first.volume == pytest.approx(0.5)
    # Caller lost the response: it now holds the smaller, freshly sampled DTO.
    fresh = account.paper.get_position_by_ticket(fill.position_id)
    assert fresh.volume == pytest.approx(0.5)
    world.fake.set_quote(account.login, 1.111, 1.1112)
    replay = account.paper.close_position_partial(fresh, percent=50.0, operation_id="owner-policy-partial-once")
    assert replay.success and (replay.deal, replay.order, replay.volume, replay.price) == (
        first.deal, first.order, first.volume, first.price)
    assert len(account.ledger.paper_deals()) == 2
    assert account.paper.get_position_by_ticket(fill.position_id).volume == pytest.approx(0.5)
    prior = account.ledger.paper_close_operation(fill.position_id, "owner-policy-partial-once")
    prior["runtime_identity"]["login"] = -1
    assert account.ledger.paper_close_operation(fill.position_id, "owner-policy-partial-once")["runtime_identity"]["login"] == account.login
    assert account.ledger.paper_close_operation(fill.position_id, "not-yet-written") is None
    retained = account.ledger.paper_fill_for_reservation("1" * 32)
    account.ledger.paper_modify_position(fill.position_id, sl=1.095, operation_id="modify-only",
        expected_cursor=retained["quote_cursor"], quote_cursor=retained["quote_cursor"],
        execution_evidence={**retained["last_execution_evidence"], "event": "LEVEL_CHANGE"})
    with pytest.raises(outcomes.LiveOutcomeError, match="paper operation is not a close"):
        account.ledger.paper_close_operation(fill.position_id, "modify-only")
    with pytest.raises(ValueError, match="invalid paper close operation id"):
        account.ledger.paper_close_operation(fill.position_id, "bad key")
    retained = account.ledger.paper_fill_for_reservation("1" * 32)
    with pytest.raises(outcomes.LiveOutcomeError, match="conflicting paper close retry"):
        account.ledger.paper_close_position(fill.position_id, 0.25, 1.111, 270.0,
            time_msc=retained["quote_cursor"]["time_msc"], operation_id="owner-policy-partial-once",
            expected_cursor=retained["quote_cursor"], quote_cursor=retained["quote_cursor"],
            execution_evidence={**retained["last_execution_evidence"], "event": "PARTIAL_CLOSE"})
    world.fake.set_quote(account.login, 1.115, 1.1152)
    assert account.paper.close_position(account.paper.get_position_by_ticket(fill.position_id)).success
    assert account.paper.get_positions() == []
    expected = (1.11 - 1.1002) * 0.5 * 100000 + (1.115 - 1.1002) * 0.5 * 100000
    assert account.ledger.paper_cash_adjustment() == pytest.approx(expected)
    risk = account.paper.get_risk_account_info()
    assert risk.sampled_broker_balance == 10000.0 and risk.balance == pytest.approx(10000 + expected)
    provisional = _sync(account)
    assert not provisional.fail_closed_reasons and provisional.emitted_records == []
    assert len([deal for deal in account.ledger.paper_deals() if deal["entry_kind"] == "OUT"]) == 2
    assert provisional.paper_statistics["raw_closed_positions"] == provisional.paper_statistics["pending_closed_positions"] == 1
    assert provisional.paper_statistics["clean_closed_positions"] == 0
    assert provisional.paper_statistics["excluded_provisional_net_cash"] == pytest.approx(expected)
    assert provisional.paper_statistics["excluded_net_cash_by_status"]["PENDING"] == pytest.approx(expected)
    raw_records = [json.loads(line) for line in account.ledger.ledger_path.read_text(encoding="utf-8").splitlines()]
    assert len(raw_records) == 1 and raw_records[0]["realized_net_pnl"] == pytest.approx(expected)
    final_msc = max(deal["time_msc"] for deal in account.ledger.paper_deals() if deal["entry_kind"] == "OUT")
    next_minute = datetime.fromtimestamp((final_msc // 60000 + 1) * 60, timezone.utc)
    latest = world.fake.quotes[account.login]
    world.fake.set_quote(account.login, latest.bid, latest.ask, when=next_minute)
    assert account.paper.poll_paper_positions(protection_only=True) == []
    synced = _sync(account)
    assert not synced.fail_closed_reasons and len(synced.emitted_records) == 1
    assert synced.emitted_records[0]["realized_net_pnl"] == pytest.approx(expected)
    assert synced.emitted_records == raw_records
    assert synced.paper_statistics["clean_closed_positions"] == 1
    assert synced.paper_statistics["pending_closed_positions"] == 0
    assert synced.paper_statistics["excluded_provisional_net_cash"] == 0.0
    assert account.ledger.paper_cash_adjustment() == pytest.approx(expected)
    assert _sync(account).emitted_records == []
    assert len(account.ledger.ledger_path.read_text(encoding="utf-8").splitlines()) == 1


@pytest.mark.parametrize("side,exit_kind,bid,ask,fill_price", [
    (0, "SL", 0.9, 0.9002, 0.9), (0, "TP", 1.13, 1.1302, 1.12),
    (1, "SL", 1.3, 1.3002, 1.3002), (1, "TP", 1.0698, 1.07, 1.08),
])
def test_observed_stops_and_limit_targets_preserve_realized_cash_and_debt(world, side, exit_kind, bid, ask, fill_price):
    account = world.open()
    fill = _fill(account, side=side)
    world.fake.set_quote(account.login, bid, ask)
    assert account.paper.get_positions() == []
    closes = account.paper.get_recent_closing_deals()
    assert len(closes) == 1 and closes[0]["price"] == pytest.approx(fill_price)
    expected = (1 if side == 0 else -1) * (fill_price - fill.price) * 100000
    assert closes[0]["profit"] == pytest.approx(expected)
    risk = account.paper.get_risk_account_info()
    assert risk.balance == pytest.approx(10000 + expected)
    assert risk.equity == pytest.approx(risk.balance)
    assert world.fake.accounts[account.login].balance == 10000.0
    if exit_kind == "SL":
        assert risk.balance < 0, "An observed gap loss must not be floored or reset"


def test_positive_closed_fill_and_unfilled_pending_intent_survive_cold_restart(world):
    account = world.open()
    fill = _fill(account, bind=False)
    with pytest.raises(outcomes.LiveOutcomeError, match="unresolved reservation already exists"):
        _reserve(account, reservation_id="2" * 32)
    _reserve(account, reservation_id="2" * 32, timeframe="D1", magic=456)
    world.fake.set_quote(account.login, 1.08, 1.0802)
    assert account.paper.get_positions() == []
    pending = _sync(account)
    assert pending.emitted_records == []
    assert any("awaits durable original-owner binding" in reason for reason in pending.fail_closed_reasons)
    account.ledger.close()
    account.ledger = world.ledger(account.identity, account.config)
    account.paper = backend.PaperExecutionConnector(account.data, account.ledger, account.identity)
    positive = account.ledger.paper_fill_for_reservation("1" * 32)
    assert positive["status"] == "CLOSED" and positive["position_id"] == fill.position_id
    assert account.ledger.paper_fill_for_reservation("2" * 32) is None
    assert {r["reservation_id"] for r in account.ledger.unresolved_pre_send_intents()} == {"1" * 32, "2" * 32}
    second = next(r for r in account.ledger.unresolved_pre_send_intents() if r["reservation_id"] == "2" * 32)
    assert (second["timeframe"], second["magic"]) == ("D1", 456)
    with pytest.raises(outcomes.LiveOutcomeError, match="positive local fill cannot be cancelled"):
        account.ledger.cancel_pre_send_intent("1" * 32, definitive_no_fill=True, reason=LABEL)
    with pytest.raises(outcomes.LiveOutcomeError, match="non-definitive outcome must retain"):
        account.ledger.cancel_pre_send_intent("2" * 32, definitive_no_fill=False, reason=LABEL)
    bound = account.ledger.bind_pre_send_intent("1" * 32, position_id=fill.position_id,
        opening_deal_ticket=fill.deal, opening_order_ticket=fill.order)
    assert bound["candidate_id"] == "synthetic-owner-not-qualified"
    assert bound["entry_price"] == fill.price and bound["volume"] == fill.volume
    assert [r["reservation_id"] for r in account.ledger.unresolved_pre_send_intents()] == ["2" * 32]
    state = json.loads(account.ledger.state_path.read_text(encoding="utf-8"))
    assert all(alias.startswith("paper-") for alias in state["entry_intents"])
    assert account.ledger.lookup_bound_entry_intent(position_id=fill.position_id) == bound
    provisional = _sync(account)
    expected = (1.08 - fill.price) * fill.volume * 100000.0
    assert not provisional.fail_closed_reasons and provisional.emitted_records == []
    assert len([deal for deal in account.ledger.paper_deals() if deal["entry_kind"] == "OUT"]) == 1
    assert provisional.paper_statistics["raw_closed_positions"] == provisional.paper_statistics["pending_closed_positions"] == 1
    assert provisional.paper_statistics["clean_closed_positions"] == 0
    assert provisional.paper_statistics["excluded_provisional_net_cash"] == pytest.approx(expected)
    assert provisional.paper_statistics["excluded_net_cash_by_status"]["PENDING"] == pytest.approx(expected)
    raw_records = [json.loads(line) for line in account.ledger.ledger_path.read_text(encoding="utf-8").splitlines()]
    assert len(raw_records) == 1 and raw_records[0]["realized_net_pnl"] == pytest.approx(expected)
    assert account.ledger.paper_cash_adjustment() == pytest.approx(expected)
    final_msc = max(deal["time_msc"] for deal in account.ledger.paper_deals() if deal["entry_kind"] == "OUT")
    next_minute = datetime.fromtimestamp((final_msc // 60000 + 1) * 60, timezone.utc)
    latest = world.fake.quotes[account.login]
    world.fake.set_quote(account.login, latest.bid, latest.ask, when=next_minute)
    assert account.paper.poll_paper_positions(protection_only=True) == []
    assert len(_sync(account).emitted_records) == 1
    assert account.ledger.paper_statistics_summary()["clean_closed_positions"] == 1
    assert account.ledger.paper_statistics_summary()["excluded_provisional_net_cash"] == 0.0
    assert [json.loads(line) for line in account.ledger.ledger_path.read_text(encoding="utf-8").splitlines()] == raw_records
    assert account.ledger.paper_cash_adjustment() == pytest.approx(expected)
    account.ledger.close()
    account.ledger = world.ledger(account.identity, account.config)
    account.paper = backend.PaperExecutionConnector(account.data, account.ledger, account.identity)
    assert _sync(account).emitted_records == []
    assert [r["reservation_id"] for r in account.ledger.unresolved_pre_send_intents()] == ["2" * 32]


def test_two_account_namespaces_isolate_identical_local_tickets_and_broker_aliases(world):
    first, second = world.open(900001), world.open(900002)
    a, b = _fill(first), _fill(second)
    assert a.position_id == b.position_id == 1 and a.deal == b.deal and a.order == b.order
    assert first.identity.namespace != second.identity.namespace
    assert first.ledger.state_path != second.ledger.state_path
    position = first.paper.get_position_by_ticket(a.position_id)
    refusal = second.paper.modify_position(position, sl=1.095)
    assert not refusal.success and "PAPER_FOREIGN_POSITION_MUTATION_REFUSED" in refusal.retcode_description
    with pytest.raises(outcomes.LiveOutcomeError, match="cannot ingest broker or foreign"):
        second.ledger.sync(closing_deals=first.ledger.paper_deals(), open_position_ids=set(),
            position_history_provider=None, resolve_cell=_resolve)
    broker = world.native(first.login)
    identity = outcomes.runtime_account_identity(broker.get_account_info(), "MT5_DEMO")
    broker.set_execution_destination("MT5_DEMO")
    broker.set_runtime_identity(identity)
    refusal = broker.close_position(position)
    assert not refusal.success and refusal.retcode_description == "NATIVE_POSITION_NAMESPACE_MISMATCH"
    assert len(first.paper.get_positions()) == len(second.paper.get_positions()) == 1


def test_fill_persistence_failure_retains_pending_and_blocks_repeated_entry(world, monkeypatch):
    account = world.open()
    reservation = _reserve(account)
    with monkeypatch.context() as failure:
        failure.setattr(account.ledger, "_save_state", Mock(side_effect=OSError("SYNTHETIC durable-write failure")))
        result = account.paper.send_market_order("EURUSD", 0, 1.0, sl=reservation["initial_stop"],
            tp=1.12, magic=123, reservation_id="1" * 32)
    assert not result.success and result.ambiguous
    assert result.retcode_description == "PAPER_FILL_PERSISTENCE_UNKNOWN_PENDING_RECONCILIATION"
    assert len(account.ledger.unresolved_pre_send_intents()) == 1
    retry = account.paper.send_market_order("EURUSD", 0, 1.0, sl=reservation["initial_stop"],
        tp=1.12, magic=123, reservation_id="1" * 32)
    assert not retry.success and retry.retcode_description == result.retcode_description
    with pytest.raises(outcomes.LiveOutcomeError, match="non-definitive outcome must retain"):
        account.ledger.cancel_pre_send_intent("1" * 32, definitive_no_fill=False, reason=LABEL)
    account.ledger.close()
    account.ledger = world.ledger(account.identity, account.config)
    assert len(account.ledger.unresolved_pre_send_intents()) == 1


def test_nonzero_funding_requires_explicit_dated_coverage_and_uses_existing_projection(world):
    world.fake.symbol.swap_mode = 4
    world.fake.symbol.swap_long, world.fake.symbol.swap_short = -2.0, 1.0
    account = world.open()
    reservation = _reserve(account)
    unavailable = account.paper.send_market_order("EURUSD", 0, 1.0, sl=reservation["initial_stop"],
        tp=1.12, magic=123, reservation_id="1" * 32)
    assert not unavailable.success and "PAPER_FUNDING_TERMS_UNAVAILABLE" in unavailable.retcode_description
    assert account.ledger.paper_positions() == []
    now = world.fake.when
    utc = lambda value: value.isoformat().replace("+00:00", "Z")
    terms = {"schema_version": "fxpm.paper-funding-terms.v1", "symbol": "EURUSD", "swap_mode": 4,
        "swap_long": -2.0, "swap_short": 1.0, "currency": "USD", "rollover_timezone": "UTC",
        "coverage_start_utc": utc(now - timedelta(hours=1)), "coverage_end_utc": utc(now + timedelta(hours=3)),
        "rollover_schedule": [{"event_utc": utc(now + timedelta(hours=1)), "factor": 3}],
        "source_ref": LABEL, "provenance_sha256": "e" * 64}
    account.data.paper_funding_terms_provider = lambda _symbol, **_kwargs: deepcopy(terms)
    result = account.paper.send_market_order("EURUSD", 0, 1.0, sl=reservation["initial_stop"],
        tp=1.12, magic=123, reservation_id="1" * 32)
    assert result.success
    account.ledger.bind_pre_send_intent("1" * 32, position_id=result.position_id)
    assert account.ledger.paper_fill_for_reservation("1" * 32)["funding_terms"] == terms
    world.fake.set_quote(account.login, 1.11, 1.1102, when=now + timedelta(hours=2))
    position = account.paper.get_position_by_ticket(result.position_id)
    assert position.swap == pytest.approx(-6.0)
    assert account.paper.close_position(position).success
    assert account.ledger.paper_cash_snapshot()["swap_cash"] == pytest.approx(-6.0)
    assert account.ledger.paper_cash_adjustment() == pytest.approx((1.11 - result.price) * 100000 - 6)


def test_scoped_writer_exclusion_is_account_keyed_in_process_and_across_processes(world, tmp_path):
    account = world.open()
    other = _ledger_config(tmp_path / "other-output-same-account")
    with pytest.raises(outcomes.LiveOutcomeError, match="already exists in this process"):
        outcomes.LiveOutcomeLedger(other, runtime_identity=account.identity)
    child = """import sys,json
from pathlib import Path
from types import SimpleNamespace
sys.path[:0]=[sys.argv[1],sys.argv[2]]
import pm_live_outcomes as source
assert Path(source.__file__).resolve()==Path(sys.argv[1])/'pm_live_outcomes.py'
from pm_live_outcomes import LiveOutcomeLedger,RuntimeAccountIdentity,LiveOutcomeError
identity=RuntimeAccountIdentity(**json.loads(sys.argv[3]))
try:
 ledger=LiveOutcomeLedger(SimpleNamespace(output_dir=Path(sys.argv[4])),runtime_identity=identity)
except LiveOutcomeError as exc:
 print('EXPECTED_EXCLUSION:'+str(exc))
 sys.exit(0 if 'busy or unavailable' in str(exc) else 91)
else:
 ledger.close()
 sys.exit(92)
"""
    identity_args = {name: getattr(account.identity, name) for name in ("login", "server", "currency", "destination")}
    result = subprocess.run([sys.executable, "-B", "-c", child, str(PRODUCT_ROOT), str(REPO),
        json.dumps(identity_args), str(other.output_dir)], capture_output=True, text=True, timeout=20, check=False)
    assert result.returncode == 0 and "EXPECTED_EXCLUSION:" in result.stdout, (result.returncode, result.stdout, result.stderr)
    assert not (Path(other.output_dir) / "production_live").exists()
    account.ledger.close()
    replacement = world.ledger(account.identity, account.config)
    assert replacement.runtime_identity == account.identity
