from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_live_outcomes
from pm_cycle_drift import CycleDriftMonitor
from pm_live_outcomes import LiveOutcomeError, LiveOutcomeLedger


def _config(tmp_path: Path):
    return SimpleNamespace(
        output_dir=tmp_path,
        log_dir=tmp_path,
        live_outcome_feedback_enabled=True,
        live_outcome_require_position_history=True,
        live_outcome_feed_cycle_drift=True,
        live_outcome_volume_tolerance=1e-8,
        cycle_drift_monitor_enabled=True,
        cycle_drift_mode="lightweight",
    )


def _deal(
    ticket: int,
    position_id: int,
    entry: str,
    volume: float,
    price: float,
    *,
    profit: float = 0.0,
    commission: float = 0.0,
    swap: float = 0.0,
    fee: float = 0.0,
    order: int = 0,
    when: int = 1_750_000_000,
):
    return {
        "ticket": ticket,
        "order_ticket": order,
        "position_id": position_id,
        "symbol": "EURUSD",
        "entry_kind": entry,
        "volume": volume,
        "price": price,
        "profit": profit,
        "commission": commission,
        "swap": swap,
        "fee": fee,
        "magic": 123,
        "comment": "PM3:EURUSD:H1:DEMO:L:3",
        "time": when,
    }


def _exact(_context):
    return {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "regime": "TREND",
        "strategy_name": "DemoStrategy",
        "cell_match_status": "EXACT",
        "cell_match_reason": "test exact match",
    }


def _reserve(ledger, *, magic=123, reservation_id="1" * 32, demo=True):
    return ledger.reserve_pre_send_intent(
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_name="DemoStrategy",
        magic=magic,
        entry_price=1.10,
        volume=1.0,
        initial_stop=1.09,
        risk_cash=100.0,
        book_role=(
            "PROCESS_VALIDATED_CURRENT_REFIT" if demo else "LOCKBOX_VALIDATED_CHAMPION"
        ),
        candidate_id="candidate-1",
        management_capsule_id="a" * 64 if demo else "",
        management_generation_id="generation-1" if demo else "",
        reservation_id=reservation_id,
    )


def test_position_close_is_fee_complete_r_normalized_and_restart_idempotent(tmp_path):
    cfg = _config(tmp_path)
    ledger_path = tmp_path / "closed.jsonl"
    state_path = tmp_path / "closed.state.json"
    cycle_path = tmp_path / "cycle.json"
    ledger = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    ledger.record_entry_intent(
        position_id=77,
        opening_deal_ticket=1,
        opening_order_ticket=11,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_name="DemoStrategy",
        magic=123,
        entry_price=1.10,
        volume=1.0,
        initial_stop=1.09,
        risk_cash=100.0,
    )
    history = {
        "history_complete": True,
        "deals": [
            _deal(1, 77, "IN", 1.0, 1.10, commission=-2.0, order=11),
            _deal(
                2,
                77,
                "OUT",
                1.0,
                1.11,
                profit=50.0,
                commission=-2.0,
                swap=-1.0,
                fee=-0.5,
                order=12,
                when=1_750_003_600,
            ),
        ],
        "orders": [{"ticket": 11, "position_id": 77, "sl": 1.09}],
    }
    monitor = CycleDriftMonitor(cfg, state_path=cycle_path)
    first = ledger.sync(
        closing_deals=[history["deals"][1]],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: history,
        resolve_cell=_exact,
        drift_monitor=monitor,
    )

    assert len(first.emitted_records) == 1
    record = first.emitted_records[0]
    assert record["gross_profit"] == pytest.approx(50.0)
    assert record["commission"] == pytest.approx(-4.0)
    assert record["swap"] == pytest.approx(-1.0)
    assert record["fee"] == pytest.approx(-0.5)
    assert record["realized_net_pnl"] == pytest.approx(44.5)
    assert record["realized_r"] == pytest.approx(0.445)
    assert record["volume_weighted_exit"] == pytest.approx(1.11)
    assert first.delivered_drift_count == 1
    assert monitor.snapshot("EURUSD", "H1", "TREND", "DemoStrategy")["trades"] == 1

    # Both the ledger and CycleDrift event id survive a process restart.
    restarted_monitor = CycleDriftMonitor(cfg, state_path=cycle_path)
    restarted_ledger = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    second = restarted_ledger.sync(
        closing_deals=[history["deals"][1]],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: history,
        resolve_cell=_exact,
        drift_monitor=restarted_monitor,
    )
    assert second.emitted_records == []
    assert second.duplicate_deal_count == 1
    assert restarted_monitor.snapshot("EURUSD", "H1", "TREND", "DemoStrategy")["trades"] == 1
    assert len(ledger_path.read_text(encoding="utf-8").splitlines()) == 1


def test_partial_exit_waits_for_flat_position_and_balanced_history(tmp_path):
    cfg = _config(tmp_path)
    ledger = LiveOutcomeLedger(
        cfg,
        ledger_path=tmp_path / "closed.jsonl",
        state_path=tmp_path / "state.json",
    )
    entry = _deal(10, 88, "IN", 1.0, 100.0)
    partial = _deal(11, 88, "OUT", 0.4, 101.0, profit=20.0)
    final = _deal(12, 88, "OUT", 0.6, 102.0, profit=45.0)

    still_open = ledger.sync(
        closing_deals=[partial],
        open_position_ids={88},
        position_history_provider=lambda _position_id: {
            "history_complete": True,
            "deals": [entry, partial],
            "orders": [],
        },
        resolve_cell=_exact,
    )
    assert still_open.emitted_records == []
    assert still_open.pending_position_count == 1

    inconsistent = ledger.sync(
        closing_deals=[],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: {
            "history_complete": True,
            "deals": [entry, partial],
            "orders": [],
        },
        resolve_cell=_exact,
    )
    assert inconsistent.emitted_records == []
    assert any("volume mismatch" in reason for reason in inconsistent.fail_closed_reasons)

    complete = ledger.sync(
        closing_deals=[final],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: {
            "history_complete": True,
            "deals": [entry, partial, final],
            "orders": [],
        },
        resolve_cell=_exact,
    )
    assert len(complete.emitted_records) == 1
    assert complete.emitted_records[0]["exit_volume"] == pytest.approx(1.0)
    assert complete.emitted_records[0]["volume_weighted_exit"] == pytest.approx(101.6)


def test_missing_risk_is_explicit_and_does_not_invent_r(tmp_path):
    cfg = _config(tmp_path)
    ledger = LiveOutcomeLedger(cfg, ledger_path=tmp_path / "l.jsonl", state_path=tmp_path / "s.json")
    entry = _deal(20, 99, "IN", 1.0, 1.0)
    close = _deal(21, 99, "OUT", 1.0, 1.1, profit=10.0)
    result = ledger.sync(
        closing_deals=[close],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: {
            "history_complete": True,
            "deals": [entry, close],
            "orders": [],
        },
        resolve_cell=_exact,
    )
    record = result.emitted_records[0]
    assert record["realized_r"] is None
    assert record["initial_risk_cash"] is None
    assert "risk_cash_unavailable" in record["realized_r_unavailable_reason"]
    assert record["eligible_for_auto_promotion"] is False


def test_unmatched_cell_persists_as_fail_closed_evidence(tmp_path):
    cfg = _config(tmp_path)
    ledger_path = tmp_path / "l.jsonl"
    state_path = tmp_path / "s.json"
    ledger = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    entry = _deal(30, 111, "IN", 1.0, 1.0)
    close = _deal(31, 111, "OUT", 1.0, 1.1, profit=10.0)
    first = ledger.sync(
        closing_deals=[close],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: {
            "history_complete": True,
            "deals": [entry, close],
            "orders": [],
        },
        resolve_cell=lambda _context: None,
    )
    assert first.emitted_records[0]["cell_match_status"] == "UNAVAILABLE"
    assert any("lack exact cell attribution" in reason for reason in first.fail_closed_reasons)

    restarted = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    again = restarted.sync(
        closing_deals=[],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: None,
        resolve_cell=_exact,
    )
    assert any("lack exact cell attribution" in reason for reason in again.fail_closed_reasons)


def test_corrupt_state_and_corrupt_ledger_fail_closed(tmp_path):
    cfg = _config(tmp_path)
    ledger_path = tmp_path / "l.jsonl"
    state_path = tmp_path / "s.json"
    ledger = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    entry = _deal(40, 123, "IN", 1.0, 1.0)
    close = _deal(41, 123, "OUT", 1.0, 1.1, profit=10.0)
    ledger.sync(
        closing_deals=[close],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: {
            "history_complete": True,
            "deals": [entry, close],
            "orders": [],
        },
        resolve_cell=_exact,
    )
    state_path.write_text("{not-json", encoding="utf-8")
    with pytest.raises(LiveOutcomeError, match="reservations are fail-closed"):
        LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)

    state_path.unlink()
    ledger_path.write_text("not-json\n", encoding="utf-8")
    with pytest.raises(LiveOutcomeError, match="ledger is corrupt"):
        LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)


def test_cycle_drift_event_id_is_idempotent(tmp_path):
    cfg = _config(tmp_path)
    monitor = CycleDriftMonitor(cfg, state_path=tmp_path / "cycle.json")
    for _ in range(2):
        monitor.record_trade(
            "EURUSD",
            "H1",
            "TREND",
            "DemoStrategy",
            pnl_dollars=-10.0,
            event_id="mt5-position-close:999",
        )
    snapshot = monitor.snapshot("EURUSD", "H1", "TREND", "DemoStrategy")
    assert snapshot["trades"] == 1
    assert snapshot["processed_event_count"] == 1


def test_pre_send_reservation_survives_restart_and_returns_isolated_copy(tmp_path):
    cfg = _config(tmp_path)
    state_path = tmp_path / "state.json"
    ledger_path = tmp_path / "ledger.jsonl"
    ledger = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    reserved = _reserve(ledger)

    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["pre_send_reservations"]["1" * 32] == reserved
    recovered = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    exposed = recovered.unresolved_pre_send_intents()
    assert exposed == [reserved]
    exposed[0]["symbol"] = "FORGED"
    assert recovered.unresolved_pre_send_intents()[0]["symbol"] == "EURUSD"
    assert recovered.ambiguous_pre_send_intent("EURUSD", 123) is None


def test_ambiguous_send_cannot_cancel_and_is_restart_reconcilable(tmp_path):
    cfg = _config(tmp_path)
    state_path = tmp_path / "state.json"
    ledger_path = tmp_path / "ledger.jsonl"
    ledger = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    _reserve(ledger)
    with pytest.raises(LiveOutcomeError, match="must retain"):
        ledger.cancel_pre_send_intent(
            "1" * 32,
            definitive_no_fill=False,
            reason="timeout",
        )
    ambiguous = ledger.retain_ambiguous_pre_send_intent(
        "1" * 32,
        reason="broker timeout",
        broker_order_ticket=501,
    )
    assert ambiguous["status"] == "AMBIGUOUS"

    restarted = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    recovered = restarted.ambiguous_pre_send_intent("eurusd", 123)
    assert recovered["reservation_id"] == "1" * 32
    assert recovered["broker_order_ticket"] == 501
    removed = restarted.cancel_pre_send_intent(
        "1" * 32,
        definitive_no_fill=True,
        reason="broker history proves no fill",
    )
    assert removed["status"] == "AMBIGUOUS"
    assert restarted.unresolved_pre_send_intents() == []


def test_filled_reservation_binds_exact_authority_and_can_add_position_alias(tmp_path):
    cfg = _config(tmp_path)
    state_path = tmp_path / "state.json"
    ledger_path = tmp_path / "ledger.jsonl"
    ledger = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    _reserve(ledger)
    bound = ledger.bind_pre_send_intent(
        "1" * 32,
        opening_deal_ticket=601,
        opening_order_ticket=701,
        fill_price=1.1002,
        fill_volume=0.9,
        risk_cash=91.0,
    )
    assert bound["management_capsule_id"] == "a" * 64
    assert ledger.unresolved_pre_send_intents() == []
    assert ledger.lookup_bound_entry_intent(opening_deal_ticket=601) == bound

    extended = ledger.bind_pre_send_intent("1" * 32, position_id=801)
    assert extended["position_id"] == 801
    assert ledger.lookup_bound_entry_intent(position_id=801) == extended
    assert ledger.lookup_bound_entry_intent(
        position_id=801,
        opening_deal_ticket=601,
        opening_order_ticket=701,
    ) == extended
    restarted = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    assert restarted.lookup_bound_entry_intent(position_id=801) == extended


def test_binding_conflict_retains_reservation_and_alias_collision_fails(tmp_path):
    cfg = _config(tmp_path)
    ledger = LiveOutcomeLedger(
        cfg,
        ledger_path=tmp_path / "ledger.jsonl",
        state_path=tmp_path / "state.json",
    )
    _reserve(ledger)
    ledger.record_entry_intent(
        opening_order_ticket=701,
        symbol="EURUSD",
        timeframe="H1",
        regime="TREND",
        strategy_name="OtherStrategy",
        magic=999,
        entry_price=1.2,
        volume=0.1,
        initial_stop=1.1,
        risk_cash=20.0,
    )
    with pytest.raises(LiveOutcomeError, match="conflicting durable entry intent"):
        ledger.bind_pre_send_intent("1" * 32, opening_order_ticket=701)
    assert ledger.unresolved_pre_send_intents()[0]["reservation_id"] == "1" * 32


def test_demo_reservation_requires_management_authority_and_finite_risk(tmp_path):
    cfg = _config(tmp_path)
    ledger = LiveOutcomeLedger(
        cfg,
        ledger_path=tmp_path / "ledger.jsonl",
        state_path=tmp_path / "state.json",
    )
    with pytest.raises(ValueError, match="demo management authority"):
        ledger.reserve_pre_send_intent(
            symbol="EURUSD",
            timeframe="H1",
            regime="TREND",
            strategy_name="DemoStrategy",
            magic=123,
            entry_price=1.1,
            volume=1.0,
            initial_stop=1.09,
            risk_cash=100.0,
            book_role="PROCESS_VALIDATED_CURRENT_REFIT",
        )
    with pytest.raises(ValueError, match="risk_cash"):
        ledger.reserve_pre_send_intent(
            symbol="EURUSD",
            timeframe="H1",
            regime="TREND",
            strategy_name="DemoStrategy",
            magic=123,
            entry_price=1.1,
            volume=1.0,
            initial_stop=1.09,
            risk_cash=float("nan"),
            book_role="LOCKBOX_VALIDATED_CHAMPION",
        )
    assert ledger.unresolved_pre_send_intents() == []


def test_reservation_bound_blocks_new_entries_without_hiding_existing_state(tmp_path, monkeypatch):
    cfg = _config(tmp_path)
    ledger = LiveOutcomeLedger(
        cfg,
        ledger_path=tmp_path / "ledger.jsonl",
        state_path=tmp_path / "state.json",
    )
    _reserve(ledger)
    monkeypatch.setattr(pm_live_outcomes, "MAX_ENTRY_RESERVATIONS", 1)
    assert "reservation bound" in ledger.new_entry_reservation_block_reason()
    with pytest.raises(LiveOutcomeError, match="reservation bound"):
        _reserve(ledger, magic=124, reservation_id="2" * 32)
    assert ledger.unresolved_pre_send_intents()[0]["magic"] == 123
    bound = ledger.bind_pre_send_intent("1" * 32, position_id=900)
    assert ledger.lookup_bound_entry_intent(position_id=900) == bound


def test_state_toctou_and_duplicate_json_are_fail_closed(tmp_path):
    cfg = _config(tmp_path)
    state_path = tmp_path / "state.json"
    ledger = LiveOutcomeLedger(
        cfg,
        ledger_path=tmp_path / "ledger.jsonl",
        state_path=state_path,
    )
    _reserve(ledger)
    state_path.write_text(state_path.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(LiveOutcomeError, match="changed concurrently"):
        ledger.retain_ambiguous_pre_send_intent("1" * 32, reason="timeout")

    duplicate_path = tmp_path / "duplicate.json"
    duplicate_path.write_text(
        '{"schema_version":"2026-07-14-live-outcome-state-v2",'
        '"schema_version":"2026-07-14-live-outcome-state-v2"}',
        encoding="utf-8",
    )
    with pytest.raises(LiveOutcomeError, match="duplicate JSON key"):
        LiveOutcomeLedger(cfg, ledger_path=tmp_path / "missing.jsonl", state_path=duplicate_path)


def test_bound_authority_is_copied_to_close_ledger_then_aliases_are_dumped(tmp_path):
    cfg = _config(tmp_path)
    state_path = tmp_path / "state.json"
    ledger_path = tmp_path / "ledger.jsonl"
    ledger = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    _reserve(ledger)
    ledger.bind_pre_send_intent(
        "1" * 32,
        position_id=77,
        opening_deal_ticket=1,
        opening_order_ticket=11,
    )
    entry = _deal(1, 77, "IN", 1.0, 1.10, order=11)
    close = _deal(2, 77, "OUT", 1.0, 1.11, profit=50.0, order=12)
    result = ledger.sync(
        closing_deals=[close],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: {
            "history_complete": True,
            "deals": [entry, close],
            "orders": [{"ticket": 11, "position_id": 77, "sl": 1.09}],
        },
        resolve_cell=_exact,
    )
    record = result.emitted_records[0]
    assert record["entry_reservation_id"] == "1" * 32
    assert record["management_capsule_id"] == "a" * 64
    assert record["management_generation_id"] == "generation-1"
    assert ledger.lookup_bound_entry_intent(position_id=77) is None
    restarted = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    assert restarted.exact_cell_summary("EURUSD", "H1", "TREND", "DemoStrategy")[
        "exact_closed_positions"
    ] == 1


def test_pending_position_bound_preserves_existing_reconciliation(tmp_path, monkeypatch):
    monkeypatch.setattr(pm_live_outcomes, "MAX_PENDING_POSITIONS", 1)
    cfg = _config(tmp_path)
    ledger = LiveOutcomeLedger(
        cfg,
        ledger_path=tmp_path / "ledger.jsonl",
        state_path=tmp_path / "state.json",
    )
    first_close = _deal(11, 101, "OUT", 1.0, 1.1, profit=10.0)
    second_close = _deal(21, 202, "OUT", 1.0, 1.1, profit=10.0)
    staged = ledger.sync(
        closing_deals=[first_close, second_close],
        open_position_ids={101, 202},
        position_history_provider=None,
        resolve_cell=_exact,
    )
    assert staged.pending_position_count == 1
    assert any("pending position bound exhausted" in reason for reason in staged.fail_closed_reasons)

    completed = ledger.sync(
        closing_deals=[],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: {
            "history_complete": True,
            "deals": [_deal(10, 101, "IN", 1.0, 1.0), first_close],
            "orders": [],
        },
        resolve_cell=_exact,
    )
    assert len(completed.emitted_records) == 1


def test_intent_state_and_ledger_bounds_retain_evidence(tmp_path, monkeypatch):
    cfg = _config(tmp_path)
    state_path = tmp_path / "state.json"
    ledger_path = tmp_path / "ledger.jsonl"
    ledger = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    _reserve(ledger)

    monkeypatch.setattr(pm_live_outcomes, "MAX_ENTRY_INTENT_ALIASES", 0)
    with pytest.raises(LiveOutcomeError, match="alias bound"):
        ledger.bind_pre_send_intent("1" * 32, position_id=77)
    assert ledger.unresolved_pre_send_intents()[0]["reservation_id"] == "1" * 32
    monkeypatch.setattr(pm_live_outcomes, "MAX_ENTRY_INTENT_ALIASES", 30_000)

    current_size = state_path.stat().st_size
    monkeypatch.setattr(pm_live_outcomes, "MAX_STATE_BYTES", current_size + 1)
    with pytest.raises(LiveOutcomeError, match="state byte bound"):
        _reserve(ledger, magic=124, reservation_id="2" * 32)
    assert [item["reservation_id"] for item in ledger.unresolved_pre_send_intents()] == ["1" * 32]
    monkeypatch.setattr(pm_live_outcomes, "MAX_STATE_BYTES", 32 * 1024 * 1024)

    entry = _deal(30, 303, "IN", 1.0, 1.0)
    close = _deal(31, 303, "OUT", 1.0, 1.1, profit=10.0)
    monkeypatch.setattr(pm_live_outcomes, "MAX_LEDGER_BYTES", 1)
    with pytest.raises(LiveOutcomeError, match="ledger byte bound"):
        ledger.sync(
            closing_deals=[close],
            open_position_ids=set(),
            position_history_provider=lambda _position_id: {
                "history_complete": True,
                "deals": [entry, close],
                "orders": [],
            },
            resolve_cell=_exact,
        )
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert "303" in persisted["pending_positions"]
    assert persisted["pre_send_reservations"]["1" * 32]["magic"] == 123


def test_ledger_record_and_index_bounds_fail_closed(tmp_path, monkeypatch):
    cfg = _config(tmp_path)
    state_path = tmp_path / "state.json"
    ledger_path = tmp_path / "ledger.jsonl"
    ledger = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    entry = _deal(40, 404, "IN", 1.0, 1.0)
    close = _deal(41, 404, "OUT", 1.0, 1.1, profit=10.0)
    ledger.sync(
        closing_deals=[close],
        open_position_ids=set(),
        position_history_provider=lambda _position_id: {
            "history_complete": True,
            "deals": [entry, close],
            "orders": [],
        },
        resolve_cell=_exact,
    )
    monkeypatch.setattr(pm_live_outcomes, "MAX_LEDGER_RECORDS", 1)
    entry_2 = _deal(50, 505, "IN", 1.0, 1.0)
    close_2 = _deal(51, 505, "OUT", 1.0, 1.1, profit=10.0)
    with pytest.raises(LiveOutcomeError, match="record/index bound"):
        ledger.sync(
            closing_deals=[close_2],
            open_position_ids=set(),
            position_history_provider=lambda _position_id: {
                "history_complete": True,
                "deals": [entry_2, close_2],
                "orders": [],
            },
            resolve_cell=_exact,
        )
    assert len(ledger_path.read_text(encoding="utf-8").splitlines()) == 1
    monkeypatch.setattr(pm_live_outcomes, "MAX_LEDGER_RECORDS", 250_000)
    monkeypatch.setattr(pm_live_outcomes, "MAX_LEDGER_INDEX_RECORDS", 0)
    with pytest.raises(LiveOutcomeError, match="record/index bound"):
        LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)


def test_valid_legacy_state_migrates_without_losing_indexes(tmp_path):
    cfg = _config(tmp_path)
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": "2026-07-10-live-outcome-state-v1",
                "seen_deal_tickets": [7],
                "emitted_position_ids": [],
                "drift_delivered_position_ids": [],
                "pending_positions": {},
                "entry_intents": {},
                "updated_at": "",
            }
        ),
        encoding="utf-8",
    )
    migrated = LiveOutcomeLedger(
        cfg,
        ledger_path=tmp_path / "ledger.jsonl",
        state_path=state_path,
    )
    assert migrated.state_recovered
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "2026-07-14-live-outcome-state-v2"
    assert payload["seen_deal_tickets"] == [7]
    assert payload["pre_send_reservations"] == {}
