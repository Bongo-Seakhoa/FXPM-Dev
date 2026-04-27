import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from pm_main import LiveTrader
from pm_position import PositionConfig


class LiveSchedulerTests(unittest.TestCase):
    def _make_trader(self):
        mock_mt5 = Mock()
        mock_mt5.is_connected.return_value = False

        config = SimpleNamespace(
            has_regime_configs=lambda: False,
            timeframe="H1",
        )
        mock_pm = Mock()
        mock_pm.symbols = []
        mock_pm.get_validated_configs.return_value = {
            "EURUSD": config,
            "GBPUSD": config,
        }

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

        trader._run_margin_protection_cycle.assert_called_once()
        trader._run_order_governance_cycle.assert_called_once()
        trader._process_symbol.assert_not_called()
        trader.mt5.get_recent_closing_deals.assert_not_called()
        trader._sync_drift_monitor.assert_not_called()
        trader._evaluate_daily_loss_advisory.assert_not_called()
        trader._run_portfolio_observatory.assert_not_called()
        trader.storage_manager.on_sweep_complete.assert_not_called()


if __name__ == "__main__":
    unittest.main()
