from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pm_main


def _app(now: datetime):
    app = pm_main.FXPortfolioManagerApp.__new__(pm_main.FXPortfolioManagerApp)
    app.pipeline_config = SimpleNamespace(live_config_expiry_grace_minutes=0)
    app.logger = Mock()
    app._last_demo_refit_expiry_notice_state = None
    trader = SimpleNamespace(
        _is_current_refit_demo=True,
        _current_refit_demo_configs={
            "EURUSD": SimpleNamespace(valid_until=now - timedelta(seconds=1)),
            "GBPUSD": SimpleNamespace(valid_until=now + timedelta(days=1)),
        },
        _get_live_configs=Mock(
            side_effect=AssertionError("notifier must not query entry authority")
        ),
    )
    app.trader = trader
    app.portfolio_manager = SimpleNamespace(
        get_symbols_needing_retrain=Mock(
            side_effect=AssertionError("notifier must not query retraining")
        ),
        retrain_all_needed=Mock(
            side_effect=AssertionError("notifier must not run optimizer")
        ),
    )
    return app


def test_demo_partial_expiry_notifies_once_without_authority_calls(
    monkeypatch,
) -> None:
    now = datetime(2026, 7, 26, 2, 0, tzinfo=timezone.utc)
    app = _app(now)
    monkeypatch.setenv("PM_CONFIG_PATH", r"C:\FxPM\config.json")

    app._check_demo_refit_expiry_notice(now=now)
    app._check_demo_refit_expiry_notice(now=now + timedelta(seconds=30))

    assert app.logger.warning.call_count == 1
    message, *arguments = app.logger.warning.call_args.args
    assert "DEMO_REFIT_PARTIAL_EXPIRY" in message
    assert arguments[0] == "EURUSD"
    assert arguments[1] == "GBPUSD"
    assert (
        arguments[2]
        == r"python pm_main.py --config C:\FxPM\config.json --optimize"
    )
    app.trader._get_live_configs.assert_not_called()
    app.portfolio_manager.get_symbols_needing_retrain.assert_not_called()
    app.portfolio_manager.retrain_all_needed.assert_not_called()


def test_demo_all_expired_transition_is_latched_and_management_only() -> None:
    now = datetime(2026, 7, 26, 2, 0, tzinfo=timezone.utc)
    app = _app(now)

    app._check_demo_refit_expiry_notice(now=now)
    app.trader._current_refit_demo_configs["GBPUSD"].valid_until = (
        now - timedelta(seconds=1)
    )
    app._check_demo_refit_expiry_notice(now=now + timedelta(seconds=1))
    app._check_demo_refit_expiry_notice(now=now + timedelta(minutes=1))

    assert app.logger.warning.call_count == 2
    message, *arguments = app.logger.warning.call_args.args
    assert "DEMO_REFIT_ALL_EXPIRED" in message
    assert "management-only" in message
    assert arguments[0] == "EURUSD,GBPUSD"
    assert arguments[1].endswith("--config config.json --optimize")


def test_demo_expiry_latch_resets_when_projection_becomes_fresh() -> None:
    now = datetime(2026, 7, 26, 2, 0, tzinfo=timezone.utc)
    app = _app(now)

    app._check_demo_refit_expiry_notice(now=now)
    app.trader._current_refit_demo_configs["EURUSD"].valid_until = (
        now + timedelta(days=1)
    )
    app._check_demo_refit_expiry_notice(now=now + timedelta(seconds=1))
    assert app._last_demo_refit_expiry_notice_state is None

    app.trader._current_refit_demo_configs["EURUSD"].valid_until = (
        now - timedelta(seconds=1)
    )
    app._check_demo_refit_expiry_notice(now=now + timedelta(seconds=2))

    assert app.logger.warning.call_count == 2


def test_demo_loop_polls_expiry_in_utc_while_mt5_is_disconnected(
    monkeypatch,
) -> None:
    app = _app(datetime(2026, 7, 26, 2, 0, tzinfo=timezone.utc))
    shutdown_event = threading.Event()
    trader = SimpleNamespace(
        _shutdown_event=shutdown_event,
        _running=False,
        _get_live_configs=Mock(return_value={"EURUSD": object()}),
        _get_management_configs=Mock(return_value={"EURUSD": object()}),
        _current_refit_demo_position_block_reason=Mock(return_value=None),
    )
    app.mt5 = Mock()
    app.mt5.is_connected.side_effect = [True, False]
    app.mt5.get_positions.return_value = []
    app.position_config = SimpleNamespace()
    app.storage_manager = None
    app.symbols = ["EURUSD"]
    app.config_file = "pm_configs.json"
    app.pipeline_config.production_retrain_mode = "disabled"
    app.pipeline_config.describe_retrain_schedule = Mock(return_value="disabled")
    app.pipeline_config.live_loop_trigger_mode = "bar"
    app.pipeline_config.live_bar_poll_seconds = 0.25
    app.pipeline_config.production_retrain_poll_seconds = 60
    app._attempt_reconnect = Mock()
    app._wait_for_next_due = Mock()
    app.shutdown = Mock()
    observed: list[datetime] = []

    def observe_expiry(*, now: datetime) -> None:
        observed.append(now)
        shutdown_event.set()

    app._check_demo_refit_expiry_notice = observe_expiry
    monkeypatch.setattr(pm_main, "LiveTrader", lambda **kwargs: trader)

    app.run_trading(
        execution_book=pm_main.CURRENT_REFIT_DEMO_EXECUTION_BOOK
    )

    assert len(observed) == 1
    assert observed[0].tzinfo is timezone.utc
    assert app.mt5.is_connected.call_count == 2
    app._attempt_reconnect.assert_called_once()
