"""Durable PAPER cursor checks on invented quote inputs; no native/source authority.

The existing ledger/Windows writer lease and durable writes execute unchanged.
Cursor hashes bind caller inputs, not tick completeness or broker authenticity.
"""

from copy import deepcopy
import hashlib
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

import pm_live_outcomes as source

assert Path(source.__file__).resolve() == PRODUCT_ROOT / "pm_live_outcomes.py"
TIME = 1_793_102_400_123


def cursor(milliseconds=0, ordinal=0):
    return {"time_msc": TIME + milliseconds, "same_msc_ordinal": ordinal,
        "prefix_sha256": hashlib.sha256(f"SYNTHETIC:{milliseconds}:{ordinal}".encode()).hexdigest()}


def evidence(event, bid=1.1, ask=1.1002):
    return {"model_id": source.PAPER_FILL_POLICY_ID, "bid": bid, "ask": ask,
            "ask_source": "NATIVE", "event": event}


@pytest.fixture
def ledger(tmp_path):
    identity = source.RuntimeAccountIdentity(990001, "SYNTHETIC-CURSOR-SERVER", "USD", "PAPER")
    config = SimpleNamespace(output_dir=tmp_path, live_outcome_feedback_enabled=True)
    instance = source.LiveOutcomeLedger(config, runtime_identity=identity)
    yield instance
    instance.close()


def opening(instance, *, tracked=True, extra=None):
    instance.reserve_pre_send_intent(symbol="EURUSD", timeframe="H1", regime="TREND",
        strategy_name="EMACrossoverStrategy", magic=123, entry_price=1.1002, volume=1.0,
        initial_stop=1.09, risk_cash=1020.0, book_role="LOCKBOX_VALIDATED_CHAMPION",
        candidate_id="SYNTHETIC_NOT_QUALIFIED", reservation_id="1" * 32)
    fill = {"price": 1.1002, "volume": 1.0, "type": 0, "time_msc": TIME,
            "sl": 1.09, "tp": 1.12, "risk_cash": 1020.0}
    if tracked:
        fill.update(quote_cursor=cursor(), fill_policy_id=source.PAPER_FILL_POLICY_ID,
                    execution_evidence=evidence("ENTRY"))
    fill.update(extra or {})
    return instance.paper_open_position("1" * 32, fill)


def test_tracked_entry_is_durable_exact_and_does_not_create_a_second_store(ledger):
    row = opening(ledger)
    assert row["entry_quote_cursor"] == row["quote_cursor"] == cursor()
    assert row["entry_execution_evidence"] == evidence("ENTRY")
    assert row["level_history"] == [{"sl": 1.09, "tp": 1.12, "effective_cursor": cursor()}]
    assert ledger.paper_deals()[0]["quote_cursor"] == cursor()
    assert ledger.paper_deals()[0]["execution_evidence"] == evidence("ENTRY")
    assert ledger.paper_cash_adjustment() == 0.0
    assert [p.name for p in ledger.state_path.parent.iterdir()] == [ledger.state_path.name]
    config, identity = ledger.config, ledger.runtime_identity
    ledger.close()
    reopened = source.LiveOutcomeLedger(config, runtime_identity=identity)
    try:
        assert reopened.paper_fill_for_reservation("1" * 32) == row
        history = reopened.paper_history(row["position_id"])
        assert history["entry_quote_cursor"] == cursor() and history["fill_policy_id"] == source.PAPER_FILL_POLICY_ID
    finally:
        reopened.close()


@pytest.mark.parametrize("change", [
    {"quote_cursor": {**cursor(), "same_msc_ordinal": True}},
    {"quote_cursor": {**cursor(), "unexpected": 1}},
    {"quote_cursor": {**cursor(), "prefix_sha256": "not-a-hash"}},
    {"quote_cursor": {**cursor(), "time_msc": 2**63 - 1}},
    {"quote_cursor": cursor(1)}, {"fill_policy_id": "UNREVIEWED_POLICY"},
    {"execution_evidence": {**evidence("ENTRY"), "ask": 1.0}},
    {"execution_evidence": {**evidence("ENTRY"), "unknown": "not swallowed"}},
])
def test_tracked_entry_refuses_malformed_or_unbound_evidence_without_consuming_reservation(ledger, change):
    with pytest.raises(ValueError):
        opening(ledger, extra=change)
    assert ledger.paper_positions() == [] and ledger.paper_deals() == []
    assert len(ledger.unresolved_pre_send_intents()) == 1


def test_cursor_advance_is_optimistic_idempotent_and_rejects_changed_same_millisecond_prefix(ledger, monkeypatch):
    row = opening(ledger)
    save = Mock(wraps=ledger._save_state)
    monkeypatch.setattr(ledger, "_save_state", save)
    command = {"expected_cursor": cursor(), "quote_cursor": cursor(1), "execution_evidence": evidence("ADVANCE", 1.101, 1.1012)}
    updated = ledger.paper_advance_quote_cursor(row["position_id"], **command)
    assert updated["quote_cursor"] == cursor(1) and save.call_count == 1
    assert ledger.paper_advance_quote_cursor(row["position_id"], **command) == updated
    assert save.call_count == 1
    with pytest.raises(source.LiveOutcomeError, match="changed before"):
        ledger.paper_advance_quote_cursor(row["position_id"], expected_cursor=cursor(),
            quote_cursor=cursor(2), execution_evidence=evidence("ADVANCE"))
    with pytest.raises(source.LiveOutcomeError, match="recorded prefix"):
        ledger.paper_advance_quote_cursor(row["position_id"], expected_cursor=cursor(1),
            quote_cursor={**cursor(1), "prefix_sha256": "0" * 64}, execution_evidence=evidence("ADVANCE"))
    with pytest.raises(source.LiveOutcomeError, match="same cursor"):
        ledger.paper_advance_quote_cursor(row["position_id"], expected_cursor=cursor(1),
            quote_cursor=cursor(1), execution_evidence=evidence("ADVANCE", 1.2, 1.2002))
    later = ledger.paper_advance_quote_cursor(row["position_id"], expected_cursor=cursor(1),
        quote_cursor=cursor(1, 1), execution_evidence=evidence("ADVANCE", 1.102, 1.1022))
    assert later["quote_cursor"] == cursor(1, 1) and save.call_count == 2
    with pytest.raises(source.LiveOutcomeError, match="backward"):
        ledger.paper_advance_quote_cursor(row["position_id"], expected_cursor=cursor(1, 1),
            quote_cursor=cursor(), execution_evidence=evidence("ADVANCE"))
    assert save.call_count == 2


def test_level_mutation_cursor_and_history_rollback_or_commit_together(ledger, monkeypatch):
    row = opening(ledger)
    before = ledger.state_path.read_bytes()
    command = {"sl": 1.095, "operation_id": "levels-once", "expected_cursor": cursor(),
               "quote_cursor": cursor(1), "execution_evidence": evidence("LEVEL_CHANGE", 1.101, 1.1012)}
    with monkeypatch.context() as failure:
        failure.setattr(ledger, "_save_state", Mock(side_effect=OSError("SYNTHETIC_FSYNC_FAILURE")))
        with pytest.raises(OSError):
            ledger.paper_modify_position(row["position_id"], **command)
    assert ledger.state_path.read_bytes() == before
    assert ledger.paper_fill_for_reservation("1" * 32) == row
    save = Mock(wraps=ledger._save_state)
    monkeypatch.setattr(ledger, "_save_state", save)
    changed = ledger.paper_modify_position(row["position_id"], **command)
    assert changed["sl"] == 1.095 and changed["quote_cursor"] == cursor(1)
    assert changed["level_history"] == row["level_history"] + [{"sl": 1.095, "tp": 1.12, "effective_cursor": cursor(1)}]
    assert save.call_count == 1 and ledger.paper_modify_position(row["position_id"], **command) == changed
    assert save.call_count == 1
    with pytest.raises(source.LiveOutcomeError, match="conflicting paper modification retry"):
        ledger.paper_modify_position(row["position_id"], **{**command, "quote_cursor": cursor(2)})


def test_tracked_default_level_retry_does_not_duplicate_effective_history(ledger, monkeypatch):
    row = opening(ledger)
    save = Mock(wraps=ledger._save_state)
    monkeypatch.setattr(ledger, "_save_state", save)
    command = {"sl": 1.095, "expected_cursor": cursor(), "quote_cursor": cursor(1),
               "execution_evidence": evidence("LEVEL_CHANGE", 1.101, 1.1012)}
    changed = ledger.paper_modify_position(row["position_id"], **command)
    assert ledger.paper_modify_position(row["position_id"], **command) == changed
    assert ledger.paper_modify_position(row["position_id"], **{**command, "expected_cursor": cursor(1)}) == changed
    assert save.call_count == 1 and len(changed["level_history"]) == 2
    explicit = {**command, "expected_cursor": cursor(1), "operation_id": "explicit-noop-once"}
    assert ledger.paper_modify_position(row["position_id"], **explicit) == changed
    assert ledger.paper_modify_position(row["position_id"], **explicit) == changed
    assert save.call_count == 2
    with pytest.raises(source.LiveOutcomeError, match="conflicting paper modification retry"):
        ledger.paper_modify_position(row["position_id"], **{**explicit, "sl": 1.096})


def test_close_price_failure_and_durable_failure_never_advance_trigger_and_cash(ledger, monkeypatch):
    row = opening(ledger)
    command = {"volume": 1.0, "price": 0.9, "profit_cash": -20020.0, "time_msc": TIME + 1,
        "operation_id": "trigger-once", "expected_cursor": cursor(), "quote_cursor": cursor(1),
        "execution_evidence": evidence("SL", 0.9, 0.9002)}
    before = ledger.state_path.read_bytes()
    for missing in ("price", "profit_cash", "swap_cash"):
        with pytest.raises(ValueError):
            ledger.paper_close_position(row["position_id"], **{**command, missing: None})
    with monkeypatch.context() as failure:
        failure.setattr(ledger, "_save_state", Mock(side_effect=OSError("SYNTHETIC_FSYNC_FAILURE")))
        with pytest.raises(OSError):
            ledger.paper_close_position(row["position_id"], **command)
    assert ledger.state_path.read_bytes() == before
    assert ledger.paper_fill_for_reservation("1" * 32) == row
    assert ledger.paper_cash_adjustment() == 0.0 and len(ledger.paper_deals()) == 1
    save = Mock(wraps=ledger._save_state)
    monkeypatch.setattr(ledger, "_save_state", save)
    close = ledger.paper_close_position(row["position_id"], **command)
    assert close["quote_cursor"] == cursor(1) and close["execution_evidence"] == evidence("SL", 0.9, 0.9002)
    assert close["time_msc"] == TIME + 1 and ledger.paper_cash_adjustment() == -20020.0
    assert ledger.paper_fill_for_reservation("1" * 32)["status"] == "CLOSED"
    assert save.call_count == 1
    assert ledger.paper_close_position(row["position_id"], **command) == close
    assert save.call_count == 1
    with pytest.raises(source.LiveOutcomeError, match="conflicting paper close retry"):
        ledger.paper_close_position(row["position_id"], **{**command, "time_msc": TIME + 2,
            "quote_cursor": cursor(2)})
    history = ledger.paper_history(row["position_id"])
    assert history["entry_quote_cursor"] == cursor() and history["quote_cursor"] == cursor(1)
    assert history["level_history"] == row["level_history"]


def test_explicit_untracked_low_level_use_preserves_old_retry_without_inventing_history(ledger):
    row = opening(ledger, tracked=False)
    assert row["quote_cursor"] is row["fill_policy_id"] is None
    with pytest.raises(source.LiveOutcomeError, match="tracked local position"):
        ledger.paper_advance_quote_cursor(row["position_id"], expected_cursor=cursor(),
            quote_cursor=cursor(1), execution_evidence=evidence("ADVANCE"))
    close = ledger.paper_close_position(row["position_id"], 1.0, 1.11, 980.0, time_msc=TIME + 1)
    retry = ledger.paper_close_position(row["position_id"], 1.0, 1.11, 980.0, time_msc=TIME + 200)
    assert close == retry and len(ledger.paper_deals()) == 2
    assert close["quote_cursor"] is close["execution_evidence"] is None


@pytest.mark.parametrize("event,bid,ask,fill_price,profit", [
    ("SL", 0.9, 0.9002, 0.9, -20020.0), ("TP", 1.13, 1.1302, 1.12, 1980.0),
])
def test_unpriced_first_touch_survives_cold_restart_and_blocks_every_later_quote_or_level(ledger, event, bid, ask, fill_price, profit):
    row = opening(ledger)
    trigger = evidence(event, bid, ask)
    latch = {"expected_cursor": cursor(), "quote_cursor": cursor(1), "execution_evidence": trigger}
    latched = ledger.paper_advance_quote_cursor(row["position_id"], **latch)
    assert latched["status"] == "OPEN" and latched["volume"] == 1.0
    assert latched["quote_cursor"] == cursor(1) and latched["last_execution_evidence"] == trigger
    assert ledger.paper_cash_adjustment() == 0.0 and len(ledger.paper_deals()) == 1
    assert ledger.paper_advance_quote_cursor(row["position_id"], **latch) == latched
    config, identity = ledger.config, ledger.runtime_identity
    ledger.close()
    active = source.LiveOutcomeLedger(config, runtime_identity=identity)
    try:
        assert active.paper_fill_for_reservation("1" * 32) == latched
        unchanged = active.state_path.read_bytes()
        close = {"volume": 1.0, "price": fill_price, "profit_cash": profit, "time_msc": TIME + 1,
                 "expected_cursor": cursor(1), "quote_cursor": cursor(1), "execution_evidence": trigger}
        with pytest.raises(ValueError):
            active.paper_close_position(row["position_id"], **{**close, "swap_cash": None})
        with pytest.raises(source.LiveOutcomeError, match="trigger awaits valuation"):
            active.paper_advance_quote_cursor(row["position_id"], expected_cursor=cursor(1),
                quote_cursor=cursor(2), execution_evidence=evidence("ADVANCE"))
        with pytest.raises(source.LiveOutcomeError, match="levels cannot change"):
            active.paper_modify_position(row["position_id"], sl=1.095, expected_cursor=cursor(1),
                quote_cursor=cursor(1), execution_evidence=evidence("LEVEL_CHANGE", bid, ask))
        with pytest.raises(source.LiveOutcomeError, match="retained first-touch"):
            active.paper_close_position(row["position_id"], **{**close, "time_msc": TIME + 2, "quote_cursor": cursor(2)})
        with pytest.raises(source.LiveOutcomeError, match="retained first-touch"):
            active.paper_close_position(row["position_id"], **{**close, "execution_evidence": evidence("MANUAL_CLOSE", bid, ask)})
        assert active.state_path.read_bytes() == unchanged
        assert active.paper_fill_for_reservation("1" * 32) == latched
        result = active.paper_close_position(row["position_id"], **close)
        assert result["quote_cursor"] == cursor(1) and result["execution_evidence"] == trigger
        assert result["price"] == fill_price and active.paper_cash_adjustment() == profit
        assert active.paper_fill_for_reservation("1" * 32)["status"] == "CLOSED"
    finally:
        active.close()


def test_tracked_default_close_retry_identity_distinguishes_later_actual_trigger_record(ledger):
    row = opening(ledger)
    first = {"volume": 0.5, "price": 1.11, "profit_cash": 490.0, "time_msc": TIME + 1,
             "expected_cursor": cursor(), "quote_cursor": cursor(1),
             "execution_evidence": evidence("PARTIAL_CLOSE", 1.11, 1.1102)}
    close = ledger.paper_close_position(row["position_id"], **first)
    assert ledger.paper_close_position(row["position_id"], **first) == close
    second = {**first, "time_msc": TIME + 2, "expected_cursor": cursor(1), "quote_cursor": cursor(2)}
    later = ledger.paper_close_position(row["position_id"], **second)
    assert later["ticket"] != close["ticket"] and later["remaining_volume"] == 0.0
    assert ledger.paper_cash_adjustment() == 980.0 and len(ledger.paper_deals()) == 3


def test_history_capacity_refuses_without_pruning_or_cursor_change(ledger, monkeypatch):
    row = opening(ledger)
    assert source.MAX_PAPER_LEVEL_HISTORY == 1024
    monkeypatch.setattr(source, "MAX_PAPER_LEVEL_HISTORY", 1)  # Tiny capacity fault, never a production waiver.
    before = ledger.state_path.read_bytes()
    with pytest.raises(source.LiveOutcomeError, match="level history capacity"):
        ledger.paper_modify_position(row["position_id"], sl=1.095, expected_cursor=cursor(),
            quote_cursor=cursor(1), execution_evidence=evidence("LEVEL_CHANGE"))
    assert ledger.state_path.read_bytes() == before and ledger.paper_fill_for_reservation("1" * 32) == row


@pytest.mark.parametrize("damage", ["cursor_extra", "quote_extra", "quote_conflict", "policy", "levels_extra", "deal_extra", "backward", "old_untracked_shape"])
def test_cold_state_refuses_malformed_tracking_and_releases_failed_constructor_lease(ledger, tmp_path, damage):
    opening(ledger)
    config, identity, path = ledger.config, ledger.runtime_identity, ledger.state_path.resolve()
    assert path.is_relative_to(tmp_path.resolve())
    original = path.read_bytes()
    body = json.loads(original)
    row = body["paper_positions"]["1"]
    if damage == "cursor_extra": row["quote_cursor"]["unknown"] = 1
    elif damage == "quote_extra": row["entry_execution_evidence"]["unknown"] = 1
    elif damage == "quote_conflict": body["paper_deals"]["2"]["execution_evidence"]["bid"] = 1.099
    elif damage == "policy": row["fill_policy_id"] = "UNKNOWN_MODEL"
    elif damage == "levels_extra": row["level_history"][0]["unknown"] = 1
    elif damage == "deal_extra": body["paper_deals"]["2"]["unknown"] = 1
    elif damage == "backward": row["quote_cursor"] = cursor(-1)
    else: row.pop("entry_quote_cursor")
    ledger.close()
    path.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(source.LiveOutcomeError):
        source.LiveOutcomeLedger(config, runtime_identity=identity)
    path.write_bytes(original)
    reopened = source.LiveOutcomeLedger(config, runtime_identity=identity)
    try:
        assert reopened.paper_fill_for_reservation("1" * 32)["quote_cursor"] == cursor()
    finally:
        reopened.close()
