from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

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


def test_corrupt_state_recovers_from_ledger_but_corrupt_ledger_fails(tmp_path):
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
    recovered = LiveOutcomeLedger(cfg, ledger_path=ledger_path, state_path=state_path)
    assert recovered.state_recovered
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["emitted_position_ids"] == ["123"]

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
