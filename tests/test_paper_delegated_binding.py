"""Synthetic delegated-read mismatch custody and close-fact replay checks.

Real Native/PAPER identity gates and the durable ledger execute. Native replies,
entry reservations and recovery prices are invented; no authority is granted.
"""

from copy import deepcopy
import json
import os
from pathlib import Path
import sys
from unittest.mock import Mock

import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import test_paper_tick_protection as protection
import test_paper_manual_recovery as recovery
import test_paper_review_rework as review
from test_paper_tick_protection import tick_world, world

backend, outcomes = protection.backend, protection.outcomes
assert Path(backend.__file__).resolve() == PRODUCT_ROOT / "pm_mt5.py"
assert Path(outcomes.__file__).resolve() == PRODUCT_ROOT / "pm_live_outcomes.py"


def _delegated(paper, name):
    if name == "get_bars":
        return paper.get_bars("EURUSD", "M5", count=2)
    return getattr(paper, name)("EURUSD")


@pytest.mark.parametrize("read", ["get_bars", "get_symbol_info", "get_tick"])
def test_delegated_failure_survives_matching_native_read_without_double_count(tick_world, monkeypatch, read):
    account = tick_world.account("EURUSD")
    protection._open(account, tick_world.tapes["EURUSD"])
    review._reserve_other_slot(account)
    positions, cash, deals = deepcopy(account.ledger.paper_positions()), account.ledger.paper_cash_snapshot(), account.ledger.paper_deals()
    native_account = tick_world.fake.accounts[account.login]
    native_account.login = account.login + 100
    failed_identity = dict(login=account.login + 100, server=native_account.server.strip(),
                           currency=native_account.currency.strip().upper())

    if read == "get_tick":
        # Failure to save metadata must leave the entry latch and a retryable
        # original observation, while matching health/management can recover.
        with monkeypatch.context() as failure:
            save = Mock(side_effect=OSError("SYNTHETIC incident save unavailable"))
            failure.setattr(account.ledger, "record_paper_binding_incident", save)
            with pytest.raises(backend.NativeAccountBindingError):
                _delegated(account.paper, read)
            observation = account.data.binding_failure_observation
            assert account.ledger.paper_binding_incident() is None
            native_account.login = account.login
            assert account.paper.is_connected() is True
            refused = review._send_reserved(account)
            assert not refused.success and refused.retcode_description == "PAPER_ACCOUNT_IDENTITY_MISMATCH"
            assert save.call_count > 0
        assert account.paper.is_connected() is True
    else:
        with pytest.raises(backend.NativeAccountBindingError):
            _delegated(account.paper, read)
        observation = account.data.binding_failure_observation
        native_account.login = account.login
        assert account.data.is_connected() is True

    assert observation["sequence"] == 1
    assert observation["observed_identity"] == failed_identity
    assert account.data.binding_failure_reason == ""
    assert account.data.binding_failure_observed_identity == failed_identity
    assert account.data.binding_failure_observation == observation
    mutated = account.data.binding_failure_observation
    mutated["observed_identity"]["login"] = -1
    assert account.data.binding_failure_observation == observation
    incident = account.ledger.paper_binding_incident()
    assert incident["count"] == 1
    assert incident["observed_identity"] == failed_identity
    assert incident["reason"] == observation["reason"]
    assert incident["first_seen_utc"] == incident["last_seen_utc"] == observation["observed_at_utc"]
    assert account.paper.is_connected() is True
    assert account.paper.binding_failure_reason == ""
    refused = review._send_reserved(account)
    assert not refused.success and refused.retcode_description == "PAPER_ACCOUNT_IDENTITY_MISMATCH"
    assert account.ledger.paper_binding_incident() == incident
    assert account.paper.poll_paper_positions(protection_only=True) == []
    assert account.ledger.paper_positions() == positions
    assert account.ledger.paper_cash_snapshot() == cash and account.ledger.paper_deals() == deals


def test_deliberate_restart_after_current_identity_checks_allows_a_new_mechanical_entry(tick_world):
    account = tick_world.account("EURUSD")
    protection._open(account, tick_world.tapes["EURUSD"])
    review._reserve_other_slot(account)
    native_account = tick_world.fake.accounts[account.login]
    native_account.login = account.login + 100
    with pytest.raises(backend.NativeAccountBindingError):
        account.paper.get_symbol_info("EURUSD")
    native_account.login = account.login
    assert account.paper.get_account_info().login == account.login
    assert not review._send_reserved(account).success
    incident = account.ledger.paper_binding_incident()
    assert incident["count"] == 1
    verified = deepcopy(account.ledger.paper_positions())
    cash = account.ledger.paper_cash_snapshot()
    account.ledger.close()
    account.ledger = tick_world.ledger(account.identity, account.config)
    account.paper = backend.PaperExecutionConnector(account.data, account.ledger, account.identity)
    assert account.ledger.paper_binding_incident() == incident
    admitted_mechanically = review._send_reserved(account)
    assert admitted_mechanically.success and admitted_mechanically.destination == "PAPER"
    account.ledger.bind_pre_send_intent("9" * 32, position_id=admitted_mechanically.position_id,
        opening_deal_ticket=admitted_mechanically.deal, opening_order_ticket=admitted_mechanically.order)
    positions = account.ledger.paper_positions()
    assert len(positions) == 2
    assert next(row for row in positions if row["position_id"] == verified[0]["position_id"]) == verified[0]
    assert account.ledger.paper_cash_snapshot() == cash
    assert account.ledger.paper_binding_incident() == incident


@pytest.mark.parametrize("kind", ["ordinary", "recovery"])
def test_close_operation_kind_is_checked_in_both_directions_and_replay_is_only_a_fact(tick_world, kind):
    if kind == "recovery":
        account, _, _, position, _ = recovery._case(tick_world)
        first = account.paper.close_position(position, operation_id="typed-close-fact", manual_recovery=True)
    else:
        account = tick_world.account("EURUSD")
        fill, _ = protection._open(account, tick_world.tapes["EURUSD"])
        position = account.paper.get_position_by_ticket(fill.position_id)
        tick_world.tapes["EURUSD"].append(protection._tick(60_000, 1.1100, 1.1102))
        first = account.paper.close_position_partial(position, percent=50, operation_id="typed-close-fact")
    assert first.success
    cash, deals, state = account.ledger.paper_cash_snapshot(), account.ledger.paper_deals(), account.ledger.state_path.read_bytes()
    requests = len(tick_world.requests)
    tick_world.tapes["EURUSD"].native_error = (-10005, "SYNTHETIC no fresh data for replay")
    tick_world.failed_cash.add("EURUSD")
    wrong_kind = account.paper.close_position(position, operation_id="typed-close-fact", manual_recovery=(kind != "recovery"))
    assert not wrong_kind.success and wrong_kind.retcode_description == "PAPER_RECOVERY_OPERATION_KIND_MISMATCH"
    replay = account.paper.close_position(position, operation_id="typed-close-fact", manual_recovery=(kind == "recovery"))
    assert replay.success and (replay.deal, replay.order, replay.volume, replay.price) == (
        first.deal, first.order, first.volume, first.price)
    assert len(tick_world.requests) == requests
    assert account.ledger.state_path.read_bytes() == state
    assert account.ledger.paper_cash_snapshot() == cash and account.ledger.paper_deals() == deals
