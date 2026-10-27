"""Synthetic engineering checks of PAPER quote-audit custody and admission.

These invented caller observations exercise the real ledger and writer lease.
They prove no native tick authenticity, cell inference, account-profit claim,
broker permission or financial authority. No MT5 module or market input is used.
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
MINUTE = TIME // 60000 * 60000
RESERVATION = "1" * 32


def _cursor(milliseconds):
    return dict(time_msc=milliseconds, same_msc_ordinal=0,
                prefix_sha256=hashlib.sha256(f"SYNTHETIC:{milliseconds}".encode()).hexdigest())


def _evidence(event):
    return dict(model_id=source.PAPER_FILL_POLICY_ID, bid=1.1, ask=1.1002,
                ask_source="NATIVE", event=event)


def _pending():
    return dict(schema_version=source.PAPER_QUOTE_AUDIT_SCHEMA, pending_minute_msc=MINUTE,
        processed_count=1, prefix_sha256="a" * 64, corroborated_minute_count=0,
        last_corroborated_minute_msc=None, first_failure_minute_msc=None,
        first_failure_code=None, first_failure_status=None,
        terminal_failure_minute_msc=None, terminal_failure_code=None,
        status="PENDING", overdue=False)


def _clean(previous):
    return {**deepcopy(previous), "pending_minute_msc": None, "processed_count": 0,
            "prefix_sha256": None, "corroborated_minute_count": 1,
            "last_corroborated_minute_msc": MINUTE, "status": "CLEAN", "overdue": False}


def _failed(previous, status, code):
    value = {**deepcopy(previous), "status": status,
             "terminal_failure_minute_msc": MINUTE, "terminal_failure_code": code}
    if value["first_failure_minute_msc"] is None:
        value.update(first_failure_minute_msc=MINUTE, first_failure_code=code,
                     first_failure_status=status)
    return value


@pytest.fixture
def ledger(tmp_path):
    identity = source.RuntimeAccountIdentity(990003, "SYNTHETIC-AUDIT-SERVER", "USD", "PAPER")
    config = SimpleNamespace(output_dir=tmp_path, live_outcome_feedback_enabled=True)
    instance = source.LiveOutcomeLedger(config, runtime_identity=identity)
    yield instance
    instance.close()


def _open(instance, *, number=1, audit=None):
    reservation = f"{number:032x}"
    instance.reserve_pre_send_intent(symbol="EURUSD", timeframe="H1", regime="TREND",
        strategy_name="EMACrossoverStrategy", magic=123 + number, entry_price=1.1002,
        volume=1.0, initial_stop=1.09, risk_cash=1020.0,
        book_role="LOCKBOX_VALIDATED_CHAMPION", candidate_id="SYNTHETIC_NOT_QUALIFIED",
        reservation_id=reservation)
    row = instance.paper_open_position(reservation, dict(price=1.1002, volume=1.0, type=0,
        time_msc=TIME + number, sl=1.09, tp=1.12, risk_cash=1020.0,
        quote_cursor=_cursor(TIME + number), fill_policy_id=source.PAPER_FILL_POLICY_ID,
        execution_evidence=_evidence("ENTRY"), quote_audit=audit))
    instance.bind_pre_send_intent(reservation, position_id=row["position_id"],
        opening_deal_ticket=row["opening_deal_ticket"], opening_order_ticket=row["opening_order_ticket"])
    return row


def _update(instance, row, audit):
    return instance.paper_update_quote_audit(row["position_id"], expected_audit=row["quote_audit"],
                                             quote_audit=audit)


def _close(instance, row, profit=100.0, *, offset=10_000):
    timestamp = TIME + offset + row["position_id"]
    price = 1.1002 + profit / 100000
    quote = {**_evidence("MANUAL_CLOSE"), "bid": price, "ask": price + 0.0002}
    audit = row.get("quote_audit") or {}
    terminal = audit.get("status") in {"DIVERGENT", "UNAVAILABLE"}
    if terminal:
        quote.update(event="MANUAL_RECOVERY_CLOSE", terminal_audit_status=audit["status"],
                     terminal_audit_code=audit["terminal_failure_code"])
    return instance.paper_close_position(row["position_id"], row["volume"], price,
        profit, time_msc=timestamp, expected_cursor=row["quote_cursor"],
        quote_cursor=None if terminal else _cursor(timestamp), execution_evidence=quote,
        operation_id=f"audit-recovery:{row['position_id']}:{timestamp}" if terminal else "")


def _resolve(_context):
    return dict(symbol="EURUSD", timeframe="H1", regime="TREND", strategy_name="EMACrossoverStrategy",
                cell_match_status="EXACT", cell_match_reason="SYNTHETIC_ATTRIBUTION_ONLY")


def _sync(instance, monitor=None):
    return instance.sync(closing_deals=[deal for deal in instance.paper_deals() if deal["entry_kind"] == "OUT"],
        open_position_ids=set(), position_history_provider=instance.paper_history,
        resolve_cell=_resolve, drift_monitor=monitor)


def _records(instance):
    return [json.loads(line) for line in instance.ledger_path.read_text(encoding="utf-8").splitlines()]


def test_missing_audit_keeps_raw_financial_close_and_explicit_excluded_denominator(ledger):
    row = _open(ledger)
    _close(ledger, row)
    result = _sync(ledger)
    record, = _records(ledger)
    assert result.emitted_records == [] and result.delivered_drift_count == 0
    assert "quote_audit" not in record and "statistics_delivered" not in record
    assert ledger.paper_cash_adjustment() == 100.0
    assert ledger.paper_outcome_admission(record) == "MISSING"
    assert ledger.paper_history(row["position_id"])["quote_audit"] is None
    assert [item["position_id"] for item in ledger.paper_quote_audit_positions()] == [row["position_id"]]
    summary = ledger.exact_cell_summary("EURUSD", "H1", "TREND", "EMACrossoverStrategy")
    assert summary["raw_closed_positions"] == summary["missing_closed_positions"] == 1
    assert summary["clean_closed_positions"] == summary["exact_closed_positions"] == summary["r_observations"] == 0
    assert summary["mean_r"] is None and summary["excluded_provisional_net_cash"] == 100.0
    assert result.paper_statistics == ledger.paper_statistics_summary()
    foreign = deepcopy(record)
    foreign["runtime_identity"]["login"] += 1
    assert ledger.paper_outcome_admission(foreign) == "UNAVAILABLE"


def test_quote_audit_cas_prefix_idempotency_and_storage_rollback(ledger, monkeypatch):
    pending = _pending()
    row = _open(ledger, audit=pending)
    proposed = {**pending, "processed_count": 2, "prefix_sha256": "b" * 64}
    with pytest.raises(source.LiveOutcomeError, match="changed before"):
        ledger.paper_update_quote_audit(row["position_id"], expected_audit=None, quote_audit=proposed)
    with pytest.raises(source.LiveOutcomeError, match="recorded prefix changed"):
        _update(ledger, row, {**pending, "prefix_sha256": "b" * 64})
    before = ledger.state_path.read_bytes()
    with monkeypatch.context() as failure:
        failure.setattr(ledger, "_save_state", Mock(side_effect=OSError("SYNTHETIC_WRITE_FAILURE")))
        with pytest.raises(OSError):
            _update(ledger, row, proposed)
    assert ledger.state_path.read_bytes() == before
    assert ledger.paper_history(row["position_id"])["quote_audit"] == pending
    changed = _update(ledger, row, proposed)
    after = ledger.state_path.read_bytes()
    assert _update(ledger, row, proposed) == changed
    assert ledger.state_path.read_bytes() == after
    changed["quote_audit"]["processed_count"] = -1
    assert ledger.paper_history(row["position_id"])["quote_audit"] == proposed
    assert [item.name for item in ledger.state_path.parent.iterdir()] == [ledger.state_path.name]


def test_contiguous_batch_corroboration_is_one_save_and_cannot_move_backwards(ledger, monkeypatch):
    row = _open(ledger, audit=_pending())
    audit = {**_clean(row["quote_audit"]), "corroborated_minute_count": 30,
             "last_corroborated_minute_msc": MINUTE + 29 * 60000}
    save = Mock(wraps=ledger._save_state)
    monkeypatch.setattr(ledger, "_save_state", save)
    row = _update(ledger, row, audit)
    assert row["quote_audit"] == audit and save.call_count == 1
    with pytest.raises(source.LiveOutcomeError, match="moved backwards"):
        _update(ledger, row, _clean(audit))
    assert save.call_count == 1


@pytest.mark.parametrize("change", [
    {"extra": 1}, {"processed_count": True}, {"prefix_sha256": "bad"},
    {"pending_minute_msc": None}, {"overdue": 1},
    {"corroborated_minute_count": 1, "last_corroborated_minute_msc": MINUTE + 60000},
    {"first_failure_minute_msc": MINUTE, "first_failure_code": "BAD", "first_failure_status": "UNKNOWN"},
    {"status": "UNAVAILABLE", "terminal_failure_minute_msc": MINUTE, "terminal_failure_code": "BAD"},
], ids=["extra", "bool-count", "prefix", "orphan-prefix", "overdue-type", "gap", "failure-status", "missing-first-failure"])
def test_initial_audit_metadata_refuses_malformed_inputs_without_a_fill(ledger, change):
    with pytest.raises(ValueError):
        _open(ledger, audit={**_pending(), **change})
    assert ledger.paper_positions() == ledger.paper_deals() == []
    assert len(ledger.unresolved_pre_send_intents()) == 1


def test_closed_pending_audit_recovers_cold_and_delivers_late_clean_once(ledger):
    row = _open(ledger, audit=_pending())
    _close(ledger, row)
    first = _sync(ledger)
    assert first.emitted_records == [] and first.paper_statistics["pending_closed_positions"] == 1
    assert _sync(ledger).emitted_records == []
    config, identity = ledger.config, ledger.runtime_identity
    cash, raw = ledger.paper_cash_snapshot(), ledger.ledger_path.read_bytes()
    ledger.close()
    reopened = source.LiveOutcomeLedger(config, runtime_identity=identity)
    try:
        closed, = reopened.paper_quote_audit_positions()
        assert closed["status"] == "CLOSED" and closed["volume"] == 0
        updated = _update(reopened, closed, _clean(closed["quote_audit"]))
        assert updated["quote_audit"]["last_corroborated_minute_msc"] == MINUTE
        assert reopened.paper_quote_audit_positions() == []
        monitor = SimpleNamespace(record_trade=Mock())
        delivered = _sync(reopened, monitor)
        assert len(delivered.emitted_records) == delivered.delivered_drift_count == 1
        assert monitor.record_trade.call_args.kwargs["event_id"] == delivered.emitted_records[0]["event_id"]
        assert _sync(reopened, monitor).emitted_records == []
        assert monitor.record_trade.call_count == 1
        assert reopened.paper_cash_snapshot() == cash and reopened.ledger_path.read_bytes() == raw
        reopened.close()
        reopened = source.LiveOutcomeLedger(config, runtime_identity=identity)
        assert _sync(reopened, monitor).emitted_records == []
        assert monitor.record_trade.call_count == 1
    finally:
        reopened.close()


def test_retryable_first_failure_survives_successful_corroboration(ledger):
    row = _open(ledger, audit=_pending())
    retry = {**_pending(), "first_failure_minute_msc": MINUTE,
             "first_failure_code": "PAPER_NATIVE_TIMEOUT", "first_failure_status": "RETRYABLE", "overdue": True}
    row = _update(ledger, row, retry)
    clean = _clean(retry)
    row = _update(ledger, row, clean)
    assert row["quote_audit"]["first_failure_status"] == "RETRYABLE"
    assert row["quote_audit"]["terminal_failure_code"] is None
    with pytest.raises(source.LiveOutcomeError, match="first failure is immutable"):
        _update(ledger, row, {**clean, "first_failure_code": "PAPER_DIFFERENT_FAILURE"})
    _close(ledger, row)
    result = _sync(ledger)
    assert len(result.emitted_records) == 1
    assert ledger.paper_outcome_admission(result.emitted_records[0]) == "CLEAN"


@pytest.mark.parametrize("status", ["DIVERGENT", "UNAVAILABLE"])
def test_terminal_failure_after_retry_keeps_both_causes_and_never_becomes_clean(ledger, status):
    row = _open(ledger, audit=_pending())
    retry = {**_pending(), "first_failure_minute_msc": MINUTE,
             "first_failure_code": "PAPER_NATIVE_TIMEOUT", "first_failure_status": "RETRYABLE"}
    row = _update(ledger, row, retry)
    terminal = _failed(retry, status, "PAPER_FINAL_NATIVE_MISMATCH")
    row = _update(ledger, row, terminal)
    with pytest.raises(source.LiveOutcomeError, match="terminal paper quote audit is immutable"):
        _update(ledger, row, _clean(retry))
    assert ledger.paper_quote_audit_positions() == []
    _close(ledger, row, -100.0)
    result = _sync(ledger)
    assert result.emitted_records == []
    history = ledger.paper_history(row["position_id"])
    assert history["quote_audit"]["first_failure_code"] == "PAPER_NATIVE_TIMEOUT"
    assert history["quote_audit"]["terminal_failure_code"] == "PAPER_FINAL_NATIVE_MISMATCH"
    assert ledger.paper_outcome_admission(_records(ledger)[0]) == status
    assert ledger.paper_cash_adjustment() == -100.0


def test_single_admission_preserves_every_status_and_excluded_cash_denominator(ledger):
    kinds = (("MISSING", -10.0), ("PENDING", -20.0), ("DIVERGENT", -30.0),
             ("UNAVAILABLE", -40.0), ("CLEAN", 50.0))
    for number, (kind, profit) in enumerate(kinds, 1):
        row = _open(ledger, number=number, audit=None if kind == "MISSING" else _pending())
        if kind in {"DIVERGENT", "UNAVAILABLE"}:
            row = _update(ledger, row, _failed(row["quote_audit"], kind, "PAPER_SYNTHETIC_FAILURE"))
        elif kind == "CLEAN":
            row = _update(ledger, row, _clean(row["quote_audit"]))
        _close(ledger, row, profit)
    monitor = SimpleNamespace(record_trade=Mock())
    result = _sync(ledger, monitor)
    assert len(_records(ledger)) == 5 and len(result.emitted_records) == monitor.record_trade.call_count == 1
    summary = ledger.exact_cell_summary("EURUSD", "H1", "TREND", "EMACrossoverStrategy")
    assert summary["raw_closed_positions"] == 5
    assert sum(summary[f"{name.lower()}_closed_positions"] for name, _ in kinds) == 5
    assert all(summary[f"{name.lower()}_closed_positions"] == 1 for name, _ in kinds)
    assert summary["exact_closed_positions"] == summary["r_observations"] == 1
    assert summary["mean_r"] == pytest.approx(50.0 / 1020.0)
    assert summary["excluded_provisional_net_cash"] == -100.0
    assert summary["excluded_net_cash_by_status"] == dict(PENDING=-20.0, DIVERGENT=-30.0, UNAVAILABLE=-40.0, MISSING=-10.0)
    assert ledger.paper_cash_adjustment() == -50.0
    assert result.paper_statistics == ledger.paper_statistics_summary()
    assert _sync(ledger, monitor).emitted_records == [] and monitor.record_trade.call_count == 1


def test_legacy_unscoped_outcome_schema_and_summary_are_unchanged(ledger, tmp_path):
    row = _open(ledger)
    _close(ledger, row)
    history = ledger.paper_history(row["position_id"])
    unscoped = source.LiveOutcomeLedger(SimpleNamespace(output_dir=tmp_path / "unscoped"))
    try:
        result = unscoped.sync(closing_deals=[deal for deal in history["deals"] if deal["entry_kind"] == "OUT"],
            open_position_ids=set(), position_history_provider=lambda _id: history, resolve_cell=_resolve)
        record, = result.emitted_records
        assert record["schema_version"] == source.LEDGER_SCHEMA_VERSION and "runtime_identity" not in record
        assert record["event_id"] == f"mt5-position-close:{row['position_id']}"
        assert result.paper_statistics == {}
        assert unscoped.exact_cell_summary("EURUSD", "H1", "TREND", "EMACrossoverStrategy") == dict(
            exact_closed_positions=1, r_observations=1, mean_r=pytest.approx(100.0 / 1020.0), profit_factor_r=float("inf"))
    finally:
        unscoped.close()


def test_clean_prefix_does_not_admit_an_uncorroborated_final_close_minute(ledger):
    row = _open(ledger, audit=_pending())
    row = _update(ledger, row, _clean(row["quote_audit"]))
    _close(ledger, row, offset=70_000)
    result = _sync(ledger)
    record, = _records(ledger)
    assert result.emitted_records == [] and ledger.paper_outcome_admission(record) == "PENDING"
    closed, = ledger.paper_quote_audit_positions()
    final = {**closed["quote_audit"], "corroborated_minute_count": 2,
             "last_corroborated_minute_msc": MINUTE + 60000}
    _update(ledger, closed, final)
    assert len(_sync(ledger).emitted_records) == 1
    assert ledger.paper_outcome_admission(record) == "CLEAN"


def test_cold_state_refuses_malformed_audit_and_releases_its_failed_writer(ledger):
    row = _open(ledger, audit=_pending())
    _close(ledger, row)
    _sync(ledger)
    state_path, config, identity = ledger.state_path, ledger.config, ledger.runtime_identity
    retained = state_path.read_bytes()
    ledger.close()
    corrupted = json.loads(retained)
    corrupted["paper_positions"][str(row["position_id"])]["quote_audit"]["processed_count"] = True
    state_path.write_text(json.dumps(corrupted), encoding="utf-8")
    with pytest.raises(source.LiveOutcomeError, match="invalid paper quote audit count"):
        source.LiveOutcomeLedger(config, runtime_identity=identity)
    state_path.write_bytes(retained)
    reopened = source.LiveOutcomeLedger(config, runtime_identity=identity)
    try:
        assert reopened.paper_history(row["position_id"])["quote_audit"] == _pending()
        assert reopened.paper_statistics_summary()["pending_closed_positions"] == 1
    finally:
        reopened.close()
