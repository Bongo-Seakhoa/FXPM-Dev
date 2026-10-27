"""Measured repair checks on fake native accounts; no market or trading authority.

The real connector/lease and App reconnect helper execute. Only the native input,
wall-clock progression and inert business-loop boundary are synthetic.
"""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


REPO = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO
STAGE = REPO  # Legacy fixture branch names still refer to this sole checkout.
VERIFY_APPLIED_SOURCE = True
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "tests"))

import pm_main as main
import pm_mt5 as backend
import pm_live_outcomes as outcomes
from test_operational_execution import world
from test_operational_main_integration import _app_config


assert Path(main.__file__).resolve() == PRODUCT_ROOT / "pm_main.py"
assert Path(backend.__file__).resolve() == PRODUCT_ROOT / "pm_mt5.py"
assert Path(outcomes.__file__).resolve() == PRODUCT_ROOT / "pm_live_outcomes.py"
LABEL = "SYNTHETIC_RECONNECT_REPAIR_NOT_ACCOUNT_OR_TRADING_AUTHORITY"


def test_health_poll_recovers_without_relaxing_bound_mode_namespace_or_native_checks(world, monkeypatch):
    connector = world.native(900001)
    connector.set_execution_destination("MT5_DEMO")
    identity = outcomes.runtime_account_identity(connector.get_account_info(), "MT5_DEMO")
    connector.set_runtime_identity(identity)
    assert connector.is_connected()
    connector.disconnect()
    assert connector.is_connected() is False
    assert connector.connect() and connector.is_connected()

    original_lease = backend._native_terminal_lease

    @contextmanager
    def unavailable_lease():
        # Inject a typed lease failure while still entering/exiting the real lease.
        with original_lease():
            raise backend.NativeSessionError("SYNTHETIC_NATIVE_LEASE_UNAVAILABLE")
            yield  # pragma: no cover - contextmanager requires a generator

    with monkeypatch.context() as fault:
        fault.setattr(backend, "_native_terminal_lease", unavailable_lease)
        assert connector.is_connected() is False
        with pytest.raises(backend.NativeSessionError, match="SYNTHETIC_NATIVE_LEASE_UNAVAILABLE"):
            connector.get_tick("EURUSD")
    assert connector.is_connected()
    assert getattr(backend._NATIVE_LOCAL, "depth", 0) == 0
    assert getattr(backend._NATIVE_LOCAL, "connector", None) is None
    actual = world.fake.accounts[900001]
    actual.trade_mode = 2
    assert connector.is_connected() is False
    with pytest.raises(backend.NativeSessionError, match="NATIVE_ACCOUNT_DESTINATION_MISMATCH"):
        connector.get_tick("EURUSD")
    actual.trade_mode = 0
    assert connector.is_connected()
    actual.currency = "EUR"
    assert connector.is_connected() is False
    refusal = connector.send_market_order("EURUSD", backend.OrderType.BUY, 1.0)
    assert not refusal.success and "currency" in refusal.retcode_description
    actual.currency = "USD"
    assert connector.is_connected() and connector.runtime_identity == identity


def test_actual_app_reconnect_error_retries_to_healthy_inert_cycle_without_early_shutdown(world, tmp_path, monkeypatch, request):
    config = _app_config(tmp_path)
    config.reconnect_attempt_interval_seconds = 5
    config.reconnect_max_attempts = 3
    config.reconnect_failure_retry_seconds = 30
    app = main.FXPortfolioManagerApp(symbols=["EURUSD"], config=config, execution_destination="PAPER")
    app.mt5 = world.native(900001)
    app._skip_storage_shutdown_housekeeping = True
    app.portfolio_manager = SimpleNamespace(get_live_eligible_configs=Mock(return_value={"EURUSD": SimpleNamespace()}))
    app._check_production_retrain_schedule = Mock()
    monkeypatch.setattr(main, "setup_logging", Mock())
    phase = {"loop": False, "healthy": False}
    attempts, waits, healthy_cycles, acquired = [], [], [], []
    world.fake.terminal_info = lambda: None if phase["loop"] and not phase["healthy"] else SimpleNamespace(connected=True)

    def initialize(**kwargs):
        attempts.append(kwargs)
        if len(attempts) == 1:
            raise backend.NativeSessionError("SYNTHETIC_RECONNECT_NATIVE_UNAVAILABLE")
        phase["healthy"] = True
        return True

    world.fake.initialize = initialize
    clock = {"now": datetime(2026, 10, 27, 12)}

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"] if tz is None else clock["now"].replace(tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(main, "datetime", Clock)
    real_ledger = main.LiveOutcomeLedger

    def ledger(*args, **kwargs):
        result = real_ledger(*args, **kwargs)
        request.addfinalizer(result.close)
        acquired.append(result)
        return result

    def loop_trader(**kwargs):
        phase["loop"] = True
        event = threading.Event()

        def inert_cycle(**_kwargs):
            assert phase["healthy"] and len(attempts) == 2
            assert app.mt5.get_account_info().login == kwargs["runtime_identity"].login
            assert not acquired[0]._closed
            healthy_cycles.append(LABEL)
            event.set()
            return False

        return SimpleNamespace(_shutdown_event=event, _runtime_account_identity=kwargs["runtime_identity"],
            _runtime_state_dir=kwargs["storage_manager"].output_dir, _is_current_refit_demo=False,
            invalidate_runtime_caches=Mock(), _get_live_configs=Mock(return_value={}),
            get_symbols_with_new_bars=Mock(return_value={}), process_all_symbols=inert_cycle,
            stop=Mock(), save_trade_log=Mock())

    def advance_to_due(event, due, *, now=None):
        assert len(waits) < 3, "Reconnect repair did not reach the healthy inert cycle"
        waits.append((now, due))
        if len(waits) == 1:
            assert due - now == timedelta(seconds=5)
            assert not event.is_set() and app.trader.stop.call_count == 0
            assert acquired[0].paper_positions() == []
        clock["now"] = max(clock["now"], due)

    app._wait_for_next_due = advance_to_due
    monkeypatch.setattr(main, "LiveOutcomeLedger", ledger)
    monkeypatch.setattr(main, "LiveTrader", loop_trader)
    app.run_trading(execution_destination="PAPER")
    assert len(attempts) == 2 and healthy_cycles == [LABEL]
    assert len(waits) == 3 and waits[1][1] == waits[1][0]
    app.trader.invalidate_runtime_caches.assert_called_once()
    app.trader.stop.assert_called_once()
    assert app._runtime_outcome_ledger is None and acquired[0]._closed
    replacement = world.ledger(acquired[0].runtime_identity, config)
    assert replacement.runtime_identity.destination == "PAPER"


def test_funded_named_namespace_binds_real_connector_before_scoped_storage_without_entry_authority(world, tmp_path, monkeypatch, request):
    config = _app_config(tmp_path)
    app = main.FXPortfolioManagerApp(symbols=["EURUSD"], config=config, execution_destination="FUNDED")
    app.mt5 = world.native(900001)
    world.fake.accounts[900001].trade_mode = 2  # Invented shape, not an actual funded account.
    app.portfolio_manager = SimpleNamespace(get_live_eligible_configs=Mock(return_value={}))
    monkeypatch.setattr(main, "setup_logging", Mock())
    events, acquired = [], []
    real_bind = app.mt5.set_runtime_identity
    real_storage, real_ledger = main.StorageManager, main.LiveOutcomeLedger

    def bind(identity):
        result = real_bind(identity)
        events.append("identity-validated-and-bound")
        return result

    def scoped_storage(*args, **kwargs):
        identity = kwargs["runtime_identity"]
        assert app.mt5.runtime_identity == identity and identity.destination == "FUNDED"
        assert events == ["identity-validated-and-bound"]
        events.append("scoped-storage")
        return real_storage(*args, **kwargs)

    def ledger(*args, **kwargs):
        assert app.mt5.runtime_identity == kwargs["runtime_identity"]
        events.append("scoped-ledger")
        result = real_ledger(*args, **kwargs)
        request.addfinalizer(result.close)
        acquired.append(result)
        return result

    monkeypatch.setattr(app.mt5, "set_runtime_identity", bind)
    monkeypatch.setattr(main, "StorageManager", scoped_storage)
    monkeypatch.setattr(main, "LiveOutcomeLedger", ledger)
    trader = Mock(side_effect=AssertionError("Flat account without winners must not start an entry loop"))
    monkeypatch.setattr(main, "LiveTrader", trader)
    assert app.run_trading(execution_destination="FUNDED") is False
    assert events == ["identity-validated-and-bound", "scoped-storage", "scoped-ledger"]
    identity = app.mt5.runtime_identity
    assert identity.destination == "FUNDED" and identity.namespace in app.storage_manager.output_dir.parts
    assert len(acquired) == 1 and acquired[0]._closed and app._runtime_outcome_ledger is None
    trader.assert_not_called()
    replacement = world.ledger(identity, config)
    assert replacement.runtime_identity == identity


def test_actual_app_waits_on_explicit_binding_mismatch_without_reconnect_then_recovers(world, tmp_path, monkeypatch, request):
    config = _app_config(tmp_path)
    app = main.FXPortfolioManagerApp(symbols=["EURUSD"], config=config, execution_destination="PAPER")
    app.mt5 = world.native(900001)
    app.logger = Mock()
    app._skip_storage_shutdown_housekeeping = True
    app.portfolio_manager = SimpleNamespace(get_live_eligible_configs=Mock(return_value={"EURUSD": SimpleNamespace()}))
    app._check_production_retrain_schedule = Mock()
    app._attempt_reconnect = Mock(wraps=app._attempt_reconnect)
    initialize = Mock(wraps=world.fake.initialize)
    monkeypatch.setattr(world.fake, "initialize", initialize)
    monkeypatch.setattr(main, "setup_logging", Mock())
    waits, healthy_cycles, acquired = [], [], []
    clock = {"now": datetime(2026, 10, 27, 12)}

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock["now"] if tz is None else clock["now"].replace(tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(main, "datetime", Clock)
    real_ledger = main.LiveOutcomeLedger

    def ledger(*args, **kwargs):
        result = real_ledger(*args, **kwargs)
        request.addfinalizer(result.close)
        acquired.append(result)
        return result

    def loop_trader(**kwargs):
        # The real native identity was already validated and bound before this point.
        assert app.mt5.runtime_identity == kwargs["runtime_identity"]
        world.fake.accounts[900001].currency = "EUR"
        event = threading.Event()

        def inert_cycle(**_kwargs):
            assert world.fake.accounts[900001].currency == "USD"
            assert app.mt5.binding_failure_reason == "" and app._last_binding_failure_notice == ""
            assert not acquired[0]._closed
            healthy_cycles.append(LABEL)
            event.set()
            return False

        return SimpleNamespace(_shutdown_event=event, _runtime_account_identity=kwargs["runtime_identity"],
            _runtime_state_dir=kwargs["storage_manager"].output_dir, _is_current_refit_demo=False,
            invalidate_runtime_caches=Mock(), _get_live_configs=Mock(return_value={}),
            get_symbols_with_new_bars=Mock(return_value={}), process_all_symbols=inert_cycle,
            stop=Mock(), save_trade_log=Mock())

    def advance_to_due(event, due, *, now=None):
        assert len(waits) < 3, "Explicit binding mismatch did not recover after restoring the real sample"
        waits.append((now, due))
        if len(waits) <= 2:
            assert due - now == timedelta(seconds=5)
            assert "currency" in app.mt5.binding_failure_reason
            assert app._last_binding_failure_notice == app.mt5.binding_failure_reason
            assert not event.is_set() and app.trader.stop.call_count == 0 and healthy_cycles == []
            assert acquired[0].paper_positions() == []
            app._attempt_reconnect.assert_not_called()
            initialize.assert_not_called()
            with pytest.raises(backend.NativeAccountBindingError):
                app.mt5.get_tick("EURUSD")
            if len(waits) == 2:
                world.fake.accounts[900001].currency = "USD"
        clock["now"] = max(clock["now"], due)

    app._wait_for_next_due = advance_to_due
    monkeypatch.setattr(main, "LiveOutcomeLedger", ledger)
    monkeypatch.setattr(main, "LiveTrader", loop_trader)
    app.run_trading(execution_destination="PAPER")
    assert len(waits) == 3 and healthy_cycles == [LABEL]
    app._attempt_reconnect.assert_not_called()
    initialize.assert_not_called()
    app.trader.invalidate_runtime_caches.assert_not_called()
    alerts = [call for call in app.logger.error.call_args_list
              if call.args and str(call.args[0]).startswith("Account identity/destination mismatch;")]
    assert len(alerts) == 1
    app.trader.stop.assert_called_once()
    assert app._last_binding_failure_notice == "" and app.mt5.binding_failure_reason == ""
    assert app._runtime_outcome_ledger is None and acquired[0]._closed


def test_demo_app_startup_snapshot_transport_error_aborts_and_releases_bound_writer(world, tmp_path, monkeypatch, request):
    """App boundary only: the mocked empty trader does not prove DEMO capsule authority."""
    from pm_demo_readiness import AUTHORIZED_DEMO_ACCOUNT_IDENTITY

    pin = AUTHORIZED_DEMO_ACCOUNT_IDENTITY
    config = _app_config(tmp_path)
    app = main.FXPortfolioManagerApp(symbols=["EURUSD"], config=config, execution_destination="MT5_DEMO")
    app.mt5 = world.native(pin.login)
    # Invented account input satisfies the real unchanged DEMO identity checks.
    actual = world.fake.accounts[pin.login]
    actual.server, actual.currency, actual.trade_mode = pin.server, pin.currency, pin.trade_mode
    app.mt5.config.expected_server, app.mt5.config.expected_currency = pin.server, pin.currency
    app.logger = Mock()
    app.portfolio_manager = SimpleNamespace(get_live_eligible_configs=Mock(
        side_effect=AssertionError("DEMO startup must not borrow funded winner eligibility")))
    monkeypatch.setattr(main, "setup_logging", Mock())
    unavailable = Mock(side_effect=backend.NativeSessionError("SYNTHETIC_STARTUP_POSITION_TRANSPORT_UNAVAILABLE"))
    monkeypatch.setattr(world.fake, "positions_get", unavailable)
    acquired, constructed = [], []
    real_ledger = main.LiveOutcomeLedger

    def ledger(*args, **kwargs):
        result = real_ledger(*args, **kwargs)
        request.addfinalizer(result.close)
        acquired.append(result)
        return result

    def empty_constructor_boundary(**kwargs):
        assert kwargs["runtime_identity"] == app.mt5.runtime_identity
        assert kwargs["runtime_identity"].destination == "MT5_DEMO"
        actor = SimpleNamespace(_running=False, _get_live_configs=Mock(return_value={}),
            _refresh_held_owner_contexts=Mock(side_effect=AssertionError("No owner inference from an unavailable snapshot")),
            _get_management_configs=Mock(side_effect=AssertionError("No management projection before a position snapshot")),
            _current_refit_demo_position_block_reason=Mock(side_effect=AssertionError("No position-risk inference from unavailable data")))
        constructed.append(actor)
        return actor

    monkeypatch.setattr(main, "LiveOutcomeLedger", ledger)
    monkeypatch.setattr(main, "LiveTrader", empty_constructor_boundary)
    assert app.run_trading(execution_destination="MT5_DEMO", execution_book=main.CURRENT_REFIT_DEMO_EXECUTION_BOOK) is False
    unavailable.assert_called_once_with()
    assert len(constructed) == len(acquired) == 1
    assert constructed[0]._running is False and app.trader is None
    constructed[0]._get_live_configs.assert_called_once()
    constructed[0]._refresh_held_owner_contexts.assert_not_called()
    constructed[0]._get_management_configs.assert_not_called()
    constructed[0]._current_refit_demo_position_block_reason.assert_not_called()
    assert app._runtime_outcome_ledger is None and acquired[0]._closed
    alerts = [call for call in app.logger.error.call_args_list
              if call.args and str(call.args[0]).startswith("MT5 startup position snapshot unavailable;")]
    assert len(alerts) == 1 and "SYNTHETIC_STARTUP_POSITION_TRANSPORT_UNAVAILABLE" in str(alerts[0])
    assert app.mt5.runtime_identity == acquired[0].runtime_identity
    assert app.mt5.runtime_identity.destination == "MT5_DEMO"
    replacement = world.ledger(acquired[0].runtime_identity, config)
    assert replacement.runtime_identity == acquired[0].runtime_identity


@pytest.mark.parametrize("failure_type", [outcomes.LiveOutcomeError, OSError, ValueError],
                         ids=["ledger-error", "filesystem-error", "state-value-error"])
def test_actual_app_retries_protection_write_failure_without_clearing_entry_block_on_transport_or_data_error(
        world, tmp_path, monkeypatch, request, failure_type):
    """Real poll/save fault; inert business boundary cannot confer entry authority."""
    from test_operational_execution import _fill

    base = datetime(2026, 10, 27, 12, tzinfo=timezone.utc)
    world.fake.when = base
    config = _app_config(tmp_path)
    config.live_loop_trigger_mode = "bar"
    app = main.FXPortfolioManagerApp(symbols=["EURUSD"], config=config, execution_destination="PAPER")
    app.mt5 = world.native(900001)
    app.logger = Mock()
    app._skip_storage_shutdown_housekeeping = True
    app.portfolio_manager = SimpleNamespace(get_live_eligible_configs=Mock(return_value={"EURUSD": SimpleNamespace()}))
    app._check_production_retrain_schedule = Mock()
    monkeypatch.setattr(main, "setup_logging", Mock())
    real_trader = main.LiveTrader
    real_ticks = world.fake.copy_ticks_range
    phase, captured = {"transport_down": False, "data_down": False, "writes_failed": 0}, {}
    waits, sweeps = [], []
    world.fake.terminal_info = lambda: None if phase["transport_down"] else SimpleNamespace(connected=True)

    def native_ticks(*args, **kwargs):
        return None if phase["data_down"] else real_ticks(*args, **kwargs)

    monkeypatch.setattr(world.fake, "copy_ticks_range", native_ticks)

    def constructor_boundary(**kwargs):
        ledger = kwargs["outcome_ledger"]
        request.addfinalizer(ledger.close)
        assert isinstance(app.mt5, backend.PaperExecutionConnector)
        assert app.mt5.runtime_identity == ledger.runtime_identity == kwargs["runtime_identity"]
        fill = _fill(SimpleNamespace(paper=app.mt5, ledger=ledger))
        for offset, bid, ask in [(60, 1.1000, 1.1002), (70, 1.0800, 1.0802),
                                 (80, 1.1000, 1.1002), (120, 1.1000, 1.1002)]:
            world.fake.set_quote(900001, bid, ask, when=base + timedelta(seconds=offset))
        real_save = ledger._save_state

        def fail_first_trigger_save():
            proposed = ledger.paper_fill_for_reservation("1" * 32)
            if (phase["writes_failed"] == 0 and proposed["status"] == "OPEN"
                    and proposed["last_execution_evidence"]["event"] == "SL"):
                phase["writes_failed"] += 1
                raise failure_type("SYNTHETIC_PAPER_TRIGGER_DURABILITY_FAILURE")
            return real_save()

        monkeypatch.setattr(ledger, "_save_state", fail_first_trigger_save)
        event = threading.Event()
        selector = Mock(return_value={})

        def runtime_only_sweep(**kwargs):
            row = ledger.paper_fill_for_reservation("1" * 32)
            sweeps.append((row["status"], actor._paper_protection_entry_block_reason))
            assert kwargs == {"symbols_filter": set(), "timeframes_filter": {}}
            return False

        actor = SimpleNamespace(_shutdown_event=event, _runtime_account_identity=kwargs["runtime_identity"],
            _runtime_state_dir=kwargs["storage_manager"].output_dir, _is_current_refit_demo=False,
            _startup_entry_block_reason="", pm=SimpleNamespace(get_live_eligible_configs=selector), logger=Mock(),
            invalidate_runtime_caches=Mock(), get_symbols_with_new_bars=Mock(return_value={}),
            process_all_symbols=Mock(side_effect=runtime_only_sweep), commit_bar_probe_times=Mock(),
            stop=Mock(), save_trade_log=Mock())
        actor._get_live_configs = real_trader._get_live_configs.__get__(actor)
        captured.update(ledger=ledger, actor=actor, event=event, selector=selector, fill=fill)
        return actor

    expected_reason = "PAPER_PROTECTION_DURABILITY_UNAVAILABLE:" + failure_type.__name__
    unrelated_hold = "SYNTHETIC_EXISTING_STARTUP_HOLD"

    def retry_or_finish(event, due, *, now=None):
        waits.append((now, due))
        actor, ledger = captured["actor"], captured["ledger"]
        assert len(waits) <= 4, "Protection did not recover through the real retry path"
        assert actor.stop.call_count == 0 and not ledger._closed
        if len(waits) <= 3:
            assert app._paper_protection_entry_block_reason == actor._paper_protection_entry_block_reason == expected_reason
            assert actor._get_live_configs() == {}
            captured["selector"].assert_not_called()
            assert ledger.paper_fill_for_reservation("1" * 32)["status"] == "OPEN"
            assert [deal for deal in ledger.paper_deals() if deal["entry_kind"] == "OUT"] == []
            assert ledger.paper_cash_adjustment() == 0.0
        if len(waits) == 1:
            assert phase["writes_failed"] == 1 and sweeps == []
            assert actor._startup_entry_block_reason == ""
            actor._startup_entry_block_reason = unrelated_hold
            phase["transport_down"] = True
        elif len(waits) == 2:
            assert sweeps == []
            phase["transport_down"] = False
            phase["data_down"] = True
        elif len(waits) == 3:
            assert phase["data_down"] and sweeps == [("OPEN", expected_reason)]
            phase["data_down"] = False
        else:
            assert app._paper_protection_entry_block_reason == actor._paper_protection_entry_block_reason == ""
            assert actor._startup_entry_block_reason == unrelated_hold
            assert sweeps == [("OPEN", expected_reason), ("CLOSED", "")]
            event.set()

    monkeypatch.setattr(main, "LiveTrader", constructor_boundary)
    app._wait_for_next_due = retry_or_finish
    app.run_trading(execution_destination="PAPER")
    actor, ledger = captured["actor"], captured["ledger"]
    assert len(waits) == 4 and phase["writes_failed"] == 1
    assert actor._startup_entry_block_reason == unrelated_hold
    assert actor._paper_protection_entry_block_reason == app._paper_protection_entry_block_reason == ""
    actor.stop.assert_called_once()
    assert ledger._closed and app._runtime_outcome_ledger is None
    failures = [call for call in app.logger.error.call_args_list
                if call.args and str(call.args[0]).startswith("PAPER protection durability failure;")]
    assert len(failures) == 1 and failures[0].args[1] == failure_type.__name__
    recoveries = [call for call in app.logger.info.call_args_list
                  if call.args and str(call.args[0]).startswith("PAPER protection recovered")]
    assert len(recoveries) == 1
    reopened = outcomes.LiveOutcomeLedger(config, runtime_identity=ledger.runtime_identity,
        ledger_path=ledger.ledger_path, state_path=ledger.state_path)
    try:
        close, = [deal for deal in reopened.paper_deals() if deal["entry_kind"] == "OUT"]
        assert close["execution_evidence"]["event"] == "SL" and close["price"] == pytest.approx(1.08)
        assert close["time_msc"] == int((base + timedelta(seconds=70)).timestamp() * 1000)
        assert reopened.paper_cash_adjustment() == pytest.approx((1.08 - captured["fill"].price) * 100000.0)
    finally:
        reopened.close()


@pytest.mark.parametrize("bar_probe_fails", [False, True], ids=["no-new-bar-runtime-sweep", "bar-probe-failure"])
def test_actual_app_books_persisted_paper_stop_before_no_bar_runtime_only_sweep(world, tmp_path, monkeypatch, request, bar_probe_fails):
    """Synthetic business boundary; protection precedes both runtime sweep and bar errors."""
    from test_operational_execution import _fill

    base = datetime(2026, 10, 27, 12, tzinfo=timezone.utc)
    world.fake.when = base
    config = _app_config(tmp_path)
    config.live_loop_trigger_mode = "bar"
    app = main.FXPortfolioManagerApp(symbols=["EURUSD"], config=config, execution_destination="PAPER")
    app.mt5 = world.native(900001)
    app._skip_storage_shutdown_housekeeping = True
    app.portfolio_manager = SimpleNamespace(get_live_eligible_configs=Mock(return_value={"EURUSD": SimpleNamespace()}))
    app._check_production_retrain_schedule = Mock()
    monkeypatch.setattr(main, "setup_logging", Mock())
    captured, probes, sweeps, waits = {}, [], [], []

    def constructor_boundary(**kwargs):
        assert isinstance(app.mt5, backend.PaperExecutionConnector)
        ledger = kwargs["outcome_ledger"]
        assert app.mt5.runtime_identity == kwargs["runtime_identity"] == ledger.runtime_identity
        assert ledger.runtime_identity.destination == "PAPER"
        request.addfinalizer(ledger.close)
        fill = _fill(SimpleNamespace(paper=app.mt5, ledger=ledger))
        assert ledger.paper_fill_for_reservation("1" * 32)["status"] == "OPEN"
        # Four explicit later observations; no interpolation or snapshot-on-read ticks.
        for offset, bid, ask in [(60, 1.1000, 1.1002), (70, 1.0800, 1.0802),
                                 (80, 1.1000, 1.1002), (120, 1.1000, 1.1002)]:
            world.fake.set_quote(900001, bid, ask, when=base + timedelta(seconds=offset))
        event = threading.Event()
        captured.update(ledger=ledger, fill=fill, event=event)

        def no_new_bars(configs):
            assert configs == {}
            probes.append((ledger.paper_fill_for_reservation("1" * 32)["status"],
                [deal for deal in ledger.paper_deals() if deal["entry_kind"] == "OUT"],
                ledger.paper_cash_adjustment()))
            if bar_probe_fails:
                raise RuntimeError("SYNTHETIC_STRATEGY_BAR_DATA_UNAVAILABLE")
            return {}

        def runtime_only_sweep(*, symbols_filter, timeframes_filter):
            row = ledger.paper_fill_for_reservation("1" * 32)
            outs = [deal for deal in ledger.paper_deals() if deal["entry_kind"] == "OUT"]
            sweeps.append((symbols_filter, timeframes_filter, row["status"], outs,
                           ledger.paper_cash_adjustment()))
            return False

        actor = SimpleNamespace(_shutdown_event=event, _runtime_account_identity=kwargs["runtime_identity"],
            _runtime_state_dir=kwargs["storage_manager"].output_dir, _is_current_refit_demo=False,
            invalidate_runtime_caches=Mock(), _get_live_configs=Mock(return_value={}),
            get_symbols_with_new_bars=Mock(side_effect=no_new_bars),
            process_all_symbols=Mock(side_effect=runtime_only_sweep), commit_bar_probe_times=Mock(),
            stop=Mock(), save_trade_log=Mock())
        captured["actor"] = actor
        return actor

    def end_inert_cycle(event, due, *, now=None):
        waits.append((now, due))
        event.set()

    monkeypatch.setattr(main, "LiveTrader", constructor_boundary)
    app._wait_for_next_due = end_inert_cycle
    app.run_trading(execution_destination="PAPER")
    actor, ledger, fill = captured["actor"], captured["ledger"], captured["fill"]
    assert [probe[0] for probe in probes] == ["CLOSED"] and len(waits) == 1
    if bar_probe_fails:
        actor.process_all_symbols.assert_not_called()
        assert sweeps == []
    else:
        # The existing empty-filter sweep still performs required account/runtime work.
        actor.process_all_symbols.assert_called_once_with(symbols_filter=set(), timeframes_filter={})
        assert len(sweeps) == 1
        symbols_filter, timeframe_filter, status, swept_outs, swept_cash = sweeps[0]
        assert symbols_filter == set() and timeframe_filter == {} and status == "CLOSED"
        assert (status, swept_outs, swept_cash) == probes[0]
    actor.get_symbols_with_new_bars.assert_called_once_with({})
    actor.commit_bar_probe_times.assert_not_called()
    _, outs, cash = probes[0]
    close, = outs
    expected = (1.08 - fill.price) * fill.volume * 100000.0
    assert close["position_id"] == fill.position_id and close["price"] == pytest.approx(1.08)
    assert close["time_msc"] == int((base + timedelta(seconds=70)).timestamp() * 1000)
    assert close["execution_evidence"]["event"] == "SL" and close["execution_evidence"]["bid"] == 1.08
    assert close["profit"] == pytest.approx(expected) and cash == pytest.approx(expected)
    assert world.fake.quotes[900001].bid == 1.1 and world.fake.accounts[900001].balance == 10000.0
    actor.stop.assert_called_once()
    assert ledger._closed and app._runtime_outcome_ledger is None
    reopened = outcomes.LiveOutcomeLedger(config, runtime_identity=ledger.runtime_identity,
        ledger_path=ledger.ledger_path, state_path=ledger.state_path)
    try:
        assert reopened.paper_fill_for_reservation("1" * 32)["status"] == "CLOSED"
        assert reopened.paper_deals()[-1] == close and reopened.paper_cash_adjustment() == pytest.approx(expected)
    finally:
        reopened.close()


def _paper_startup_fixture(world, tmp_path, monkeypatch):
    """Real constructor dependencies; all account/spec inputs are invented and unqualified."""
    import pm_core as core
    import pm_storage as storage
    from pm_pipeline import PortfolioManager

    policy_source = sys.modules[main.interval_cell_qualification_readiness_finding.__module__]
    assert Path(policy_source.__file__).resolve() == PRODUCT_ROOT / "pm_live_readiness.py"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PM_CONFIG_PATH", str(tmp_path / "absent-owned-config.json"))
    for name, value in {
        "_CONFIG_INSTRUMENT_SPECS": {}, "_CONFIG_SPEC_DEFAULTS": {},
        "_CONFIG_SPECS_LOADED": False, "_CONFIG_SPECS_LOADED_PATH": None,
        "_BROKER_SPECS_CACHE": {}, "_BROKER_SPECS_LOADED": False,
        "_BROKER_SPECS_LOADED_PATH": None,
        "_BROKER_SPECS_PATH": str(tmp_path / "invented-broker-specs.json"),
    }.items():
        monkeypatch.setattr(core, name, value)
    symbol = world.fake.symbol
    (tmp_path / "invented-broker-specs.json").write_text(json.dumps({"EURUSD": {
        "classification": LABEL, "digits": symbol.digits, "point": symbol.point,
        "tick_value": symbol.trade_tick_value, "tick_size": symbol.trade_tick_size,
        "contract_size": symbol.trade_contract_size, "volume_min": symbol.volume_min,
        "volume_max": symbol.volume_max, "volume_step": symbol.volume_step,
        "spread": symbol.spread, "swap_long": symbol.swap_long,
        "swap_short": symbol.swap_short, "swap_mode": symbol.swap_mode,
    }}), encoding="utf-8")
    config = core.PipelineConfig(data_dir=tmp_path / "empty-data", output_dir=tmp_path / "output",
        log_dir=tmp_path / "logs", winner_ledger_path=str(tmp_path / "absent-winner-ledger.json"))
    config.production_retrain_mode, config.live_loop_trigger_mode = "off", "bar"
    config._funded_live_execution = False
    app = main.FXPortfolioManagerApp(symbols=["EURUSD"], config=config, execution_destination="PAPER")
    app.mt5, app.logger = world.native(900001), Mock()
    app._skip_storage_shutdown_housekeeping = True
    app._check_production_retrain_schedule = Mock()
    app.portfolio_manager = PortfolioManager(config=config, symbols=["EURUSD"], config_file=config.winner_ledger_path)
    assert app.portfolio_manager.symbol_configs == {}
    app.portfolio_manager.get_live_eligible_configs = Mock(
        side_effect=AssertionError("PAPER must not borrow last-good winner eligibility"))
    monkeypatch.setattr(main, "setup_logging", Mock())
    monkeypatch.setattr(main, "validate_live_readiness", Mock(
        side_effect=AssertionError("PAPER must not run unrelated funded readiness checks")))
    identity = outcomes.runtime_account_identity(app.mt5.get_account_info(), "PAPER")
    app.mt5.set_execution_destination("PAPER")
    app.mt5.set_runtime_identity(identity)
    manager = storage.StorageManager(config, data_dir=config.data_dir, output_dir=config.output_dir,
        log_dir=config.log_dir, runtime_identity=identity)
    ledger = outcomes.LiveOutcomeLedger(config, ledger_path=manager.output_dir / "closed_positions.jsonl",
        state_path=manager.output_dir / "closed_positions.state.json", runtime_identity=identity)
    paper = backend.PaperExecutionConnector(app.mt5, ledger, identity)
    app._wait_for_next_due = lambda event, _due, *, now=None: event.set()
    return SimpleNamespace(app=app, config=config, ledger=ledger, paper=paper, identity=identity)


def test_public_paper_startup_blocks_an_entry_signal_but_manages_a_retained_stop(world, tmp_path, monkeypatch, request):
    """An invented upstream signal reaches real entry code; no winner or risk grant is supplied."""
    from test_operational_execution import _fill

    base = datetime(2026, 10, 27, 12, tzinfo=timezone.utc)
    world.fake.when = base
    fixture = _paper_startup_fixture(world, tmp_path, monkeypatch)
    request.addfinalizer(fixture.ledger.close)
    fill = _fill(fixture)
    assert fixture.ledger.paper_positions()[0]["position_id"] == fill.position_id
    fixture.ledger.close()
    app, captured = fixture.app, {}
    real_constructor = main.LiveTrader.__init__

    def capture_real_constructor(actor, **kwargs):
        real_constructor(actor, **kwargs)
        captured.update(actor=actor, ledger=kwargs["outcome_ledger"])
        assert actor._startup_entry_block_reason == "interval_cell_qualification_policy_unadopted"
        assert actor.enable_trading and actor.execution_destination == "PAPER"
        assert actor._get_live_configs() == {}
        assert len(captured["ledger"].paper_positions()) == 1
        send = Mock(wraps=actor.mt5.send_market_order)
        monkeypatch.setattr(actor.mt5, "send_market_order", send)
        record = Mock(wraps=actor._actionable_log.record)
        monkeypatch.setattr(actor._actionable_log, "record", record)
        real_process_all_symbols = actor.process_all_symbols

        def process_signal(**kwargs):
            assert kwargs == {"symbols_filter": {"EURUSD"}, "timeframes_filter": {"EURUSD": {"H1"}}}
            ledger = captured["ledger"]
            assert ledger.paper_fill_for_reservation("1" * 32)["status"] == "CLOSED"
            close = ledger.paper_deals()[-1]
            assert close["execution_evidence"]["event"] == "SL"
            assert close["price"] == pytest.approx(1.08)
            actor._execute_entry(symbol="EURUSD", broker_symbol="EURUSD", signal=1,
                strategy=main.StrategyRegistry.get("EMACrossoverStrategy"),
                features=main.pd.DataFrame({"ATR_14": [0.01, 0.01]}),
                spec=SimpleNamespace(pip_size=0.0001, point=0.00001, spread_avg=0.9),
                magic=456, config=SimpleNamespace(symbol="EURUSD"), decision_key=LABEL,
                bar_time_iso=base.isoformat(), best_candidate={"timeframe": "H1", "regime": "TREND",
                    "strategy_name": "EMACrossoverStrategy"},
                trade_intent=SimpleNamespace(stop_loss_pips=100, take_profit_pips=200),
                positions_snapshot=[], account_info=actor.mt5.get_account_info())
            payload = record.call_args.args[1]
            assert payload["action"] == "BLOCKED_ENTRY_READINESS"
            assert payload["context"]["entry_readiness_error"] == "interval_cell_qualification_policy_unadopted"
            send.assert_not_called()
            real_process_all_symbols(**kwargs)
            send.assert_not_called()
            captured["signal_processed"] = True
            return False

        # Only the upstream bar/signal fixture is synthetic; the normal account
        # sweep follows the refused signal without replacing its risk checks.
        actor.get_symbols_with_new_bars = Mock(return_value={"EURUSD": {"H1"}})
        actor.process_all_symbols = Mock(side_effect=process_signal)
        for offset, bid, ask in [(60, 1.1, 1.1002), (70, 1.08, 1.0802),
                                 (80, 1.1, 1.1002), (120, 1.1, 1.1002)]:
            world.fake.set_quote(900001, bid, ask, when=base + timedelta(seconds=offset))

    monkeypatch.setattr(main.LiveTrader, "__init__", capture_real_constructor)
    app.run_trading(execution_destination="PAPER", startup_entry_block_reason="")
    assert captured.get("signal_processed") is True
    app.portfolio_manager.get_live_eligible_configs.assert_not_called()
    main.validate_live_readiness.assert_not_called()
    assert not fixture.config._funded_live_execution
    ledger = captured["ledger"]
    assert ledger._closed and app._runtime_outcome_ledger is None
    reopened = outcomes.LiveOutcomeLedger(fixture.config, ledger_path=ledger.ledger_path,
        state_path=ledger.state_path, runtime_identity=fixture.identity)
    try:
        assert reopened.paper_fill_for_reservation("1" * 32)["status"] == "CLOSED"
        assert reopened.paper_cash_adjustment() == pytest.approx((1.08 - 1.1002) * 100000)
    finally:
        reopened.close()


def test_public_paper_startup_logs_the_retained_binding_incident_without_resetting_it(world, tmp_path, monkeypatch, request):
    fixture = _paper_startup_fixture(world, tmp_path, monkeypatch)
    request.addfinalizer(fixture.ledger.close)
    identity = fixture.identity
    fixture.ledger.record_paper_binding_incident(
        observed_identity={"login": identity.login, "server": identity.server, "currency": "EUR"},
        bound_identity={"login": identity.login, "server": identity.server,
            "currency": identity.currency, "destination": "PAPER"},
        reason="SYNTHETIC_RETAINED_PRIOR_CURRENCY_MISMATCH", observed_at_utc="2026-10-27T11:59:59Z")
    original = fixture.ledger.paper_binding_incident()
    fixture.ledger.close()
    app, captured = fixture.app, {}
    real_constructor = main.LiveTrader.__init__

    def capture_real_constructor(actor, **kwargs):
        real_constructor(actor, **kwargs)
        captured.update(actor=actor, ledger=kwargs["outcome_ledger"])
        actor.get_symbols_with_new_bars = Mock(return_value={})
        actor.process_all_symbols = Mock(return_value=False)
        assert actor._startup_entry_block_reason == "interval_cell_qualification_policy_unadopted"

    monkeypatch.setattr(main.LiveTrader, "__init__", capture_real_constructor)
    app.run_trading(execution_destination="PAPER")
    assert captured["actor"].process_all_symbols.call_count == 1
    warnings = [call for call in app.logger.warning.call_args_list
                if call.args and str(call.args[0]).startswith("Historical PAPER binding incident;")]
    assert len(warnings) == 1
    assert warnings[0].args[1:] == tuple(original[key] for key in (
        "first_seen_utc", "last_seen_utc", "count", "observed_identity", "bound_identity", "reason"))
    ledger = captured["ledger"]
    reopened = outcomes.LiveOutcomeLedger(fixture.config, ledger_path=ledger.ledger_path,
        state_path=ledger.state_path, runtime_identity=identity)
    try:
        assert reopened.paper_binding_incident() == original
        assert app.mt5.runtime_identity == identity and not app.mt5.binding_failure_reason
        assert reopened.paper_positions() == [] and reopened.paper_cash_adjustment() == 0.0
    finally:
        reopened.close()
