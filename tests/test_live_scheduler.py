import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from pm_main import FXPortfolioManagerApp, LiveTrader
from pm_position import PositionConfig


class LiveSchedulerTests(unittest.TestCase):
    def _make_trader(self):
        mock_mt5 = Mock()
        mock_mt5.config = SimpleNamespace(
            expected_login=123, expected_server="Broker-Test", expected_currency="USD"
        )
        mock_mt5.is_connected.return_value = False

        config = SimpleNamespace(
            has_regime_configs=lambda: False,
            timeframe="H1",
        )
        mock_pm = Mock()
        mock_pm.symbols = []
        live_configs = {
            "EURUSD": config,
            "GBPUSD": config,
        }
        # Entry scheduling consumes only the strict live-eligibility view.
        # get_validated_configs is management-only and must never be its
        # compatibility fallback.
        mock_pm.get_live_eligible_configs.return_value = live_configs
        mock_pm.get_validated_configs.return_value = live_configs

        pipeline_config = SimpleNamespace(
            live_bar_settle_seconds=5,
            live_stale_retry_seconds=15,
            live_loop_trigger_mode="scheduled",
        )

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            enable_trading=False,
            pipeline_config=pipeline_config,
        )
        trader.logger = Mock()
        return trader

    def test_next_sweep_due_uses_symbol_timeframe_state_and_ignores_missing_entries(self):
        trader = self._make_trader()
        probe = datetime(2026, 4, 3, 10, 30, 0)
        trader._last_bar_times["EURUSD_H1"] = pd.Timestamp(datetime(2026, 4, 3, 10, 0, 0))

        due_at = trader.get_next_sweep_due(["H1"], now=probe)

        self.assertEqual(due_at, datetime(2026, 4, 3, 11, 0, 5))

    def test_next_sweep_due_uses_retry_only_when_no_symbol_or_global_state_exists(self):
        trader = self._make_trader()
        probe = datetime(2026, 4, 3, 10, 30, 0)

        due_at = trader.get_next_sweep_due(["H1"], now=probe)

        self.assertEqual(due_at, probe + timedelta(seconds=15))

    def test_next_sweep_due_falls_back_to_global_timeframe_store_when_available(self):
        trader = self._make_trader()
        probe = datetime(2026, 4, 3, 10, 30, 0)
        trader._latest_bar_time_by_tf["H1"] = pd.Timestamp(datetime(2026, 4, 3, 10, 0, 0))

        due_at = trader.get_next_sweep_due(["H1"], now=probe)

        self.assertEqual(due_at, datetime(2026, 4, 3, 11, 0, 5))

    def test_next_sweep_due_wakes_immediately_for_queued_exact_cell(self):
        trader = self._make_trader()
        probe = datetime(2026, 4, 3, 10, 30, 0)
        config = SimpleNamespace(
            has_regime_configs=lambda: True,
            get_available_timeframes=lambda: ["M5", "D1"],
        )
        trader.pm.get_live_eligible_configs.return_value = {"EURUSD": config}
        trader._last_bar_times["EURUSD_D1"] = pd.Timestamp(
            datetime(2026, 4, 3, 0, 0, 0)
        )
        trader._pending_specialist_timeframes["EURUSD"] = {"M5"}

        due_at = trader.get_next_sweep_due(["M5", "D1"], now=probe)

        self.assertEqual(due_at, probe)

    def test_bar_trigger_tracks_bar_timestamps_per_symbol_timeframe(self):
        trader = self._make_trader()
        trader.mt5.find_broker_symbol.side_effect = lambda symbol: symbol
        first_bars = pd.DataFrame(
            {"Close": [1.0, 1.1, 1.2]},
            index=pd.to_datetime([
                "2026-04-03 08:00:00",
                "2026-04-03 09:00:00",
                "2026-04-03 10:00:00",
            ]),
        )
        trader.mt5.get_bars.return_value = first_bars

        configs = {
            "EURUSD": SimpleNamespace(
                has_regime_configs=lambda: False,
                timeframe="H1",
            )
        }

        changed = trader.get_symbols_with_new_bars(configs)
        self.assertEqual(changed, {"EURUSD": {"H1"}})
        trader.commit_bar_probe_times(changed)
        self.assertEqual(trader.get_symbols_with_new_bars(configs), {})

        second_bars = pd.DataFrame(
            {"Close": [1.1, 1.2, 1.3]},
            index=pd.to_datetime([
                "2026-04-03 09:00:00",
                "2026-04-03 10:00:00",
                "2026-04-03 11:00:00",
            ]),
        )
        trader.mt5.get_bars.return_value = second_bars

        self.assertEqual(trader.get_symbols_with_new_bars(configs), {"EURUSD": {"H1"}})

    def test_bar_probe_is_not_consumed_until_committed(self):
        trader = self._make_trader()
        trader.mt5.find_broker_symbol.side_effect = lambda symbol: symbol
        trader.mt5.get_bars.return_value = pd.DataFrame(
            {"Close": [1.0, 1.1, 1.2]},
            index=pd.to_datetime([
                "2026-04-03 08:00:00",
                "2026-04-03 09:00:00",
                "2026-04-03 10:00:00",
            ]),
        )
        configs = {
            "EURUSD": SimpleNamespace(
                has_regime_configs=lambda: False,
                timeframe="H1",
            )
        }

        self.assertEqual(trader.get_symbols_with_new_bars(configs), {"EURUSD": {"H1"}})
        self.assertEqual(trader.get_symbols_with_new_bars(configs), {"EURUSD": {"H1"}})

    def test_bar_trigger_ignores_missing_bar_data(self):
        trader = self._make_trader()
        trader.mt5.find_broker_symbol.side_effect = lambda symbol: symbol
        trader.mt5.get_bars.return_value = None

        configs = {
            "EURUSD": SimpleNamespace(
                has_regime_configs=lambda: False,
                timeframe="H1",
            )
        }

        self.assertEqual(trader.get_symbols_with_new_bars(configs), {})

    def test_runtime_only_cycle_preserves_safety_without_signal_processing(self):
        trader = self._make_trader()
        trader.storage_manager = Mock()
        trader.mt5.get_positions.return_value = []
        trader.mt5.get_account_info.return_value = SimpleNamespace(
            login=123, server="Broker-Test", currency="USD",
            equity=1000.0,
            margin=0.0,
            margin_level=1000.0,
            trade_allowed=True,
            trade_expert=True,
        )
        trader.mt5.get_recent_closing_deals.return_value = []
        trader._run_margin_protection_cycle = Mock(return_value=False)
        trader._run_order_governance_cycle = Mock()
        trader._sync_drift_monitor = Mock()
        trader._evaluate_daily_loss_advisory = Mock()
        trader._run_portfolio_observatory = Mock()
        trader._process_symbol = Mock()

        trader.process_all_symbols(symbols_filter=set(), timeframes_filter={})

        trader.mt5.get_account_info.assert_called_once()
        trader._run_margin_protection_cycle.assert_called_once()
        trader._run_order_governance_cycle.assert_called_once()
        trader._process_symbol.assert_not_called()
        trader.mt5.get_recent_closing_deals.assert_called_once_with(
            lookback_hours=24 * 8
        )
        trader._sync_drift_monitor.assert_called_once()
        trader._evaluate_daily_loss_advisory.assert_called_once()
        trader._run_portfolio_observatory.assert_called_once()
        trader.storage_manager.on_sweep_complete.assert_not_called()


class ReconnectIdentityTests(unittest.TestCase):
    """Reconnect success requires actual identity even before trader creation."""

    def _app(self, with_trader=True):
        app = FXPortfolioManagerApp.__new__(FXPortfolioManagerApp)
        app.mt5 = Mock()
        app.mt5.config = SimpleNamespace(
            expected_login=123, expected_server="Broker-Test", expected_currency="USD"
        )
        app.mt5.connect.return_value = True
        app.mt5.get_account_info.return_value = SimpleNamespace(
            login=123, server="Broker-Test", currency="USD", equity=-1
        )
        app.pipeline_config = SimpleNamespace(reconnect_max_attempts=2,
            reconnect_attempt_interval_seconds=5, reconnect_failure_retry_seconds=30)
        app.logger = Mock()
        app.trader = Mock(_is_current_refit_demo=False) if with_trader else None
        return app

    def test_success_validates_actual_account_before_reset_and_cache_invalidation(self):
        now = datetime(2026, 4, 3, 10)
        for with_trader in (False, True):
            with self.subTest(with_trader=with_trader):
                app = self._app(with_trader)
                state = {"attempts": 1, "next_attempt_at": now}
                events = []
                app.mt5.disconnect.side_effect = lambda: events.append("disconnect")
                app.mt5.connect.side_effect = lambda: events.append("connect") or True
                account = app.mt5.get_account_info.return_value
                def read_account():
                    self.assertEqual(state["attempts"], 1)
                    events.append("account")
                    return account
                app.mt5.get_account_info.side_effect = read_account
                if with_trader:
                    app.trader.invalidate_runtime_caches.side_effect = lambda: events.append("cache")
                self.assertTrue(app._attempt_reconnect(state, now=now))
                self.assertEqual(events, ["disconnect", "connect", "account"] + (["cache"] if with_trader else []))
                self.assertEqual(state, {"attempts": 0, "next_attempt_at": now})

    def test_bad_or_missing_identity_uses_existing_retry_then_cooldown(self):
        now = datetime(2026, 4, 3, 10)
        for with_trader in (False, True):
            for value in (None, SimpleNamespace(login=124, server="Broker-Test", currency="USD"),
                          SimpleNamespace(login="bad", server="Broker-Test", currency="USD"),
                          SimpleNamespace(login=123, server="Other", currency="USD"),
                          SimpleNamespace(login=123, server="Broker-Test", currency="EUR"),
                          RuntimeError("synthetic account read error")):
                with self.subTest(with_trader=with_trader, value=value):
                    app = self._app(with_trader)
                    if isinstance(value, Exception):
                        app.mt5.get_account_info.side_effect = value
                    else:
                        app.mt5.get_account_info.return_value = value
                    state = {"attempts": 0, "next_attempt_at": now}
                    self.assertFalse(app._attempt_reconnect(state, now=now))
                    self.assertEqual(state, {"attempts": 1, "next_attempt_at": now + timedelta(seconds=5)})
                    self.assertEqual(app.mt5.disconnect.call_count, 2)
                    self.assertFalse(app._attempt_reconnect(state, now=now + timedelta(seconds=4)))
                    self.assertEqual(app.mt5.connect.call_count, 1)
                    self.assertFalse(app._attempt_reconnect(state, now=now + timedelta(seconds=5)))
                    self.assertEqual(state, {"attempts": 0, "next_attempt_at": now + timedelta(seconds=35)})
                    if with_trader:
                        app.trader.invalidate_runtime_caches.assert_not_called()

    def test_reconnect_without_trader_never_bypasses_missing_or_malformed_pins(self):
        now = datetime(2026, 4, 3, 10)
        for pins in (None, SimpleNamespace(expected_login="bad"),
                     SimpleNamespace(expected_login=123, expected_server="", expected_currency="USD"),
                     SimpleNamespace(expected_login=123, expected_server="Broker-Test", expected_currency="")):
            with self.subTest(pins=pins):
                app = self._app(False)
                app.mt5.config = pins
                state = {"attempts": 0, "next_attempt_at": now}
                self.assertFalse(app._attempt_reconnect(state, now=now))
                self.assertEqual(state["attempts"], 1)
                app.mt5.get_account_info.assert_called_once()

    def test_failed_connect_does_not_read_account_or_invalidate_caches(self):
        app = self._app()
        app.mt5.connect.return_value = False
        now = datetime(2026, 4, 3, 10)
        state = {"attempts": 0, "next_attempt_at": now}
        self.assertFalse(app._attempt_reconnect(state, now=now))
        app.mt5.get_account_info.assert_not_called()
        app.trader.invalidate_runtime_caches.assert_not_called()
        self.assertEqual(state["next_attempt_at"], now + timedelta(seconds=5))


if __name__ == "__main__":
    unittest.main()
