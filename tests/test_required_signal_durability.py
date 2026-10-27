"""Signal-durability checks on selected source, invented decisions and mock brokers.

These execute genuine module files, not funded authority or broker execution.
"""

import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import pm_main as main
import pm_storage as storage
from pm_spread_profile import FrozenSpreadTier, TIER_SCHEMA


assert Path(main.__file__).resolve() == PRODUCT_ROOT / "pm_main.py"
assert Path(storage.__file__).resolve() == PRODUCT_ROOT / "pm_storage.py"

# Reuse the established mock admission fixture without invoking its profile fixture.
import test_live_frozen_spread as _helpers
assert _helpers.LiveTrader is main.LiveTrader


@pytest.fixture(autouse=True)
def forbid_real_mt5(monkeypatch):
    try:
        import MetaTrader5 as mt5
    except ImportError:
        yield
        return
    guards = []
    for name in (
        "initialize", "login", "order_send", "order_check", "positions_get",
        "account_info", "symbol_info", "symbol_info_tick", "copy_rates_from_pos",
        "copy_rates_range", "history_deals_get",
    ):
        guard = Mock(side_effect=AssertionError(f"Real MT5 call forbidden: {name}"))
        monkeypatch.setattr(mt5, name, guard, raising=False)
        guards.append(guard)
    yield
    for guard in guards:
        guard.assert_not_called()


def _storage(tmp_path, *, enabled=True):
    output = tmp_path / "output"
    output.mkdir(parents=True)
    return storage.StorageManager(
        SimpleNamespace(storage_signal_ledger_enabled=enabled),
        data_dir=tmp_path / "data", output_dir=output, log_dir=tmp_path / "logs",
        logger=Mock(),
    )


def _storage_fault(manager, fault, monkeypatch):
    if fault == "raises":
        monkeypatch.setattr(
            manager.signal_ledger, "append", Mock(side_effect=OSError("injected append failure"))
        )
    elif fault == "disabled":
        manager.signal_ledger.enabled = False
    elif fault == "none":
        monkeypatch.setattr(manager.signal_ledger, "append", Mock(return_value=None))
    else:
        raise AssertionError(f"Unknown fixture fault: {fault}")


@pytest.mark.parametrize("fault", ["raises", "disabled", "none"])
def test_required_storage_append_propagates_failure(tmp_path, monkeypatch, fault):
    manager = _storage(tmp_path)
    _storage_fault(manager, fault, monkeypatch)
    error = OSError if fault == "raises" else RuntimeError
    with pytest.raises(error, match="injected append failure|required signal ledger append unavailable"):
        manager.record_actionable({"action": "BLOCKED_TEST", "symbol": "EURUSD"}, required=True)
    assert not list(manager.output_dir.glob("signal_ledger_*.jsonl"))


@pytest.mark.parametrize("fault", ["raises", "disabled", "none"])
def test_default_storage_append_remains_best_effort(tmp_path, monkeypatch, fault):
    manager = _storage(tmp_path)
    _storage_fault(manager, fault, monkeypatch)
    assert manager.record_actionable({"action": "BLOCKED_TEST", "symbol": "EURUSD"}) is None
    assert not list(manager.output_dir.glob("signal_ledger_*.jsonl"))


def _entry_trader(manager, *, required="feedback"):
    trader = main.LiveTrader.__new__(main.LiveTrader)
    trader.logger = Mock()
    trader.mt5 = Mock()
    trader._decision_throttle = Mock()
    trader._actionable_log = Mock()
    trader.pipeline_config = SimpleNamespace(
        manual_new_entries_enabled=False, live_outcome_feedback_enabled=True,
    )
    trader._live_outcome_feedback_initialized = required == "feedback"
    trader._is_current_refit_demo = required == "demo"
    trader._live_outcome_feedback_error = ""
    trader._startup_entry_block_reason = ""
    trader._live_outcome_ledger = Mock()
    trader._cycle_drift_monitor = Mock()
    trader._resolve_live_spread_tier = Mock(side_effect=AssertionError("Early entry gate was bypassed"))
    trader.storage_manager = manager
    return trader


def _execute(trader):
    trader._execute_entry(
        symbol="EURUSD", broker_symbol="EURUSD", signal=1, strategy=Mock(),
        features=pd.DataFrame(), spec=SimpleNamespace(), magic=123,
        config=SimpleNamespace(symbol="EURUSD"), decision_key="SYNTHETIC-SIGNAL-DURABILITY",
        bar_time_iso="2020-01-01T00:00:00Z",
        best_candidate={"timeframe": "H1", "regime": "TREND", "strategy_name": "Synthetic"},
    )


def _records(manager):
    return [json.loads(line) for path in manager.output_dir.glob("signal_ledger_*.jsonl")
            for line in path.read_text(encoding="utf-8").splitlines()]


@pytest.mark.parametrize("required", ["feedback", "demo"])
def test_failing_dashboard_cache_still_durably_appends(tmp_path, required):
    manager = _storage(tmp_path)
    trader = _entry_trader(manager, required=required)
    trader._actionable_log.record.side_effect = OSError("injected observer failure")
    _execute(trader)
    records = _records(manager)
    assert len(records) == 1
    assert records[0]["action"] == "BLOCKED_MANUAL_KILL_SWITCH"
    assert records[0]["decision_key"] == "SYNTHETIC-SIGNAL-DURABILITY"
    assert trader._startup_entry_block_reason == trader._live_outcome_feedback_error == ""
    trader.mt5.send_market_order.assert_not_called()
    trader._live_outcome_ledger.reserve_pre_send_intent.assert_not_called()


def test_default_entry_keeps_best_effort_signal_failure(tmp_path, monkeypatch):
    manager = _storage(tmp_path)
    _storage_fault(manager, "raises", monkeypatch)
    trader = _entry_trader(manager, required="neither")
    _execute(trader)
    assert trader._actionable_log.record.call_args.args[1]["action"] == "BLOCKED_MANUAL_KILL_SWITCH"
    assert trader._startup_entry_block_reason == trader._live_outcome_feedback_error == ""
    assert not _records(manager)


@pytest.mark.parametrize("fault", ["missing", "raises", "disabled"])
def test_required_signal_failure_survives_successful_outcome_sync(tmp_path, monkeypatch, fault):
    manager = None if fault == "missing" else _storage(tmp_path / "broken")
    if manager is not None:
        _storage_fault(manager, fault, monkeypatch)
    trader = _entry_trader(manager)
    _execute(trader)
    reason = trader._startup_entry_block_reason
    assert reason.startswith("required actionable-signal persistence failed:")
    assert trader._live_outcome_feedback_error == reason

    management = {"EURUSD": SimpleNamespace(timeframe="H1")}
    trader.pm = SimpleNamespace(
        get_validated_configs=Mock(return_value=management),
        get_live_eligible_configs=Mock(return_value=management),
    )
    trader._live_outcome_ledger.sync.return_value = SimpleNamespace(
        emitted_records=[], fail_closed_reasons=[],
    )
    trader._sync_drift_monitor(
        validated_configs=management, recent_deals=[], open_positions_snapshot=[],
    )
    trader._live_outcome_ledger.sync.assert_called_once()
    assert trader._live_outcome_feedback_error == ""
    assert trader._startup_entry_block_reason == reason
    assert trader._get_live_configs() == {}
    assert trader._get_management_configs() is management
    trader.pm.get_live_eligible_configs.assert_not_called()

    trader.storage_manager = _storage(tmp_path / "recovered")
    trader.pipeline_config.manual_new_entries_enabled = True
    _execute(trader)
    assert _records(trader.storage_manager)[0]["action"] == "BLOCKED_ENTRY_READINESS"
    assert trader._startup_entry_block_reason == reason
    assert trader._live_outcome_feedback_error == ""
    trader._resolve_live_spread_tier.assert_not_called()
    trader._live_outcome_ledger.reserve_pre_send_intent.assert_not_called()
    trader.mt5.send_market_order.assert_not_called()


def _synthetic_tier():
    return FrozenSpreadTier(json.dumps({
        "schema_version": TIER_SCHEMA, "symbol": "EURUSD", "timeframe": "H1",
        "source_timeframe": "M5", "tier": "P75", "point_price": 0.00001,
        "spread_points": 9, "full_spread_price": 9 * 0.00001,
        "qualification_authority": False,
        "classification": "SYNTHETIC_DURABILITY_FIXTURE_NO_MARKET_OR_AUTHORITY",
    }))


@pytest.mark.parametrize("late_block", [False, True])
def test_final_reservation_boundary_rechecks_sticky_signal_block(tmp_path, late_block):
    trader = _helpers.paper_trader(_synthetic_tier(), [_helpers.tick(9), _helpers.tick(9)])
    trader.enable_trading = True
    trader.storage_manager = _storage(tmp_path)
    trader._live_outcome_feedback_initialized = True
    trader._live_outcome_feedback_error = ""
    trader._startup_entry_block_reason = ""
    trader._live_outcome_ledger = Mock()
    trader._live_outcome_ledger.reserve_pre_send_intent.return_value = {"reservation_id": "1" * 32}
    trader._promotion_authorization_block_reason = Mock(return_value=None)
    account = SimpleNamespace(
        balance=10000.0, equity=10000.0, margin=0.0, margin_level=0.0, margin_free=10000.0,
    )
    trader._fresh_entry_account_validation = Mock(return_value=(account, None))
    reason = "required actionable-signal persistence failed: injected late fault"

    def final_loss_check(_account):
        if late_block:
            trader._startup_entry_block_reason = reason
        return []

    trader._realized_loss_hard_block_reasons = Mock(side_effect=final_loss_check)
    trader.mt5.send_market_order.return_value = SimpleNamespace(
        success=False, ambiguous=False, retcode=10013,
        retcode_description="Synthetic definitive no fill", error_message="",
    )
    trader._execute_entry(
        symbol="EURUSD", broker_symbol="EURUSD", signal=1, strategy=Mock(),
        features=pd.DataFrame(
            {"ATR_14": [0.01, 0.01]},
            index=[_helpers.OPEN - pd.Timedelta(hours=1), _helpers.OPEN],
        ),
        spec=SimpleNamespace(pip_size=0.0001, point=0.00001, spread_avg=0.9),
        magic=123, config=SimpleNamespace(symbol="EURUSD"),
        decision_key="SYNTHETIC-FINAL-SIGNAL-RECHECK",
        bar_time_iso=str(_helpers.OPEN - pd.Timedelta(hours=1)),
        best_candidate={"timeframe": "H1", "regime": "TREND", "strategy_name": "Synthetic"},
        trade_intent=SimpleNamespace(stop_loss_pips=100, take_profit_pips=200),
        positions_snapshot=[], account_info=account,
    )
    trader._fresh_entry_account_validation.assert_called_once()
    trader._realized_loss_hard_block_reasons.assert_called_once_with(account)
    payload = trader._actionable_log.record.call_args.args[1]
    if late_block:
        assert payload["action"] == "BLOCKED_ENTRY_READINESS"
        assert payload["context"]["stage"] == "immediate_pre_send"
        assert payload["context"]["reason"] == reason
        trader._live_outcome_ledger.reserve_pre_send_intent.assert_not_called()
        trader.mt5.send_market_order.assert_not_called()
    else:
        assert payload["action"] == "FAILED_10013"
        trader._live_outcome_ledger.reserve_pre_send_intent.assert_called_once()
        trader.mt5.send_market_order.assert_called_once()
        trader._live_outcome_ledger.cancel_pre_send_intent.assert_called_once()


def test_sticky_entry_block_does_not_disable_signal_exit():
    trader = main.LiveTrader.__new__(main.LiveTrader)
    trader.enable_trading = True
    trader._startup_entry_block_reason = "required actionable-signal persistence failed: synthetic"
    trader.logger = Mock()
    trader.mt5 = Mock()
    trader.mt5.config = SimpleNamespace(
        expected_login=123, expected_server="Broker-Test", expected_currency="USD",
    )
    trader.mt5.get_account_info.return_value = SimpleNamespace(
        login=123, server="Broker-Test", currency="USD",
    )
    trader.mt5.close_position.return_value = SimpleNamespace(success=True)
    position = SimpleNamespace(symbol="EURUSD")
    trader._close_position_on_signal(position, pd.DataFrame(), None)
    trader.mt5.close_position.assert_called_once_with(
        position=position, deviation=30, comment="PM_SignalExit",
    )
    assert trader._startup_entry_block_reason.startswith("required actionable-signal persistence failed:")
