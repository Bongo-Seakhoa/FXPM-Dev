import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd

from pm_core import PipelineConfig
from pm_main import FXPortfolioManagerApp, LiveTrader, _filter_dataclass_kwargs, _normalize_storage_section_for_pipeline
from pm_position import PositionConfig, TradeTagEncoder


class ConfigSourceOfTruthTests(unittest.TestCase):
    def _resolve_configs(self, config_data):
        pipeline_config = PipelineConfig(
            **_filter_dataclass_kwargs(PipelineConfig, config_data.get("pipeline", {}))
        )
        position_config = PositionConfig(
            **_filter_dataclass_kwargs(PositionConfig, config_data.get("position", {}))
        )

        # Mirror the backward-compatible handoff in pm_main.main().
        if "risk_per_trade_pct" not in (config_data.get("position") or {}) and hasattr(
            pipeline_config, "risk_per_trade_pct"
        ):
            position_config.risk_per_trade_pct = pipeline_config.risk_per_trade_pct

        return pipeline_config, position_config

    def test_position_risk_per_trade_pct_overrides_pipeline_value(self):
        _, position_config = self._resolve_configs(
            {
                "pipeline": {"risk_per_trade_pct": 1.0},
                "position": {"risk_per_trade_pct": 0.5},
            }
        )

        self.assertAlmostEqual(position_config.risk_per_trade_pct, 0.5, places=6)

    def test_pipeline_risk_per_trade_pct_backfills_only_when_position_value_missing(self):
        _, position_config = self._resolve_configs(
            {
                "pipeline": {"risk_per_trade_pct": 0.75},
                "position": {},
            }
        )

        self.assertAlmostEqual(position_config.risk_per_trade_pct, 0.75, places=6)

    def test_position_max_risk_pct_comes_from_position_section(self):
        _, position_config = self._resolve_configs(
            {
                "pipeline": {"max_combined_risk_pct": 2.0},
                "position": {"max_risk_pct": 1.0},
            }
        )

        self.assertAlmostEqual(position_config.max_risk_pct, 1.0, places=6)

    def test_nested_storage_section_accepts_natural_keys(self):
        normalized = _normalize_storage_section_for_pipeline(
            {
                "observe_only": False,
                "live_sync_bars": 123,
                "state_filename": "custom_state.json",
            }
        )

        self.assertFalse(normalized["storage_observe_only"])
        self.assertEqual(normalized["storage_live_sync_bars"], 123)
        self.assertEqual(normalized["state_filename"], "custom_state.json")

    def test_app_defaults_to_pipeline_winner_ledger_path(self):
        pipeline_config = PipelineConfig(winner_ledger_path="pm_configs_high_risk.json")
        app = FXPortfolioManagerApp(
            symbols=["EURUSD"],
            pipeline_config=pipeline_config,
            position_config=SimpleNamespace(),
            mt5_config=SimpleNamespace(),
        )

        self.assertEqual(app.config_file, "pm_configs_high_risk.json")


class LiveRiskIntentTests(unittest.TestCase):
    def _make_trader(self, position_config):
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = MagicMock()
        trader._decision_throttle = MagicMock()
        trader._actionable_log = MagicMock()
        trader._last_order_times = {}
        trader.position_config = position_config
        trader.pipeline_config = SimpleNamespace(
            min_trade_risk_pct=0.01,
            d1_secondary_risk_multiplier=1.0,
            secondary_trade_max_risk_pct=0.5,
            max_combined_risk_pct=2.0,
            margin_entry_block_level=100.0,
            target_annual_vol=0.10,
            use_commission=False,
        )
        trader.position_calc = MagicMock()
        trader.position_calc.calculate_stop_prices.return_value = (1.1900, 1.2200)
        trader.enable_trading = False
        trader.logger = MagicMock()
        trader._enhancement_seams = SimpleNamespace(
            risk_scalar_stack=SimpleNamespace(overlays=[]),
            execution_quality_overlay=SimpleNamespace(enabled=False),
        )
        trader._equity_peak = 0.0
        trader._check_symbol_combined_risk_cap = MagicMock(
            return_value=(True, "Symbol risk OK for XAUUSD: 0.10% / 2.00% (0 open positions, adding 1 new)")
        )
        trader._log_trade = MagicMock()

        trader.mt5.get_symbol_info.return_value = SimpleNamespace(
            visible=True,
            trade_mode=2,
            volume_min=0.01,
            volume_max=100.0,
            trade_stops_level=0,
            point=0.01,
            trade_tick_size=0.0,
            trade_tick_value=0.0,
        )
        trader.mt5.get_symbol_tick.return_value = SimpleNamespace(ask=1.2000, bid=1.1998)
        trader.mt5.normalize_volume.side_effect = lambda volume, _info: volume

        def _calc_loss_amount(_order_type, _symbol, volume, _entry_price, _stop_price):
            if abs(volume - 1.0) < 1e-9:
                return 1000.0
            if abs(volume - 0.01) < 1e-9:
                return 10.0
            return 1000.0 * float(volume)

        trader.mt5.calc_loss_amount.side_effect = _calc_loss_amount
        trader.mt5.calc_margin_required.return_value = None
        return trader

    def test_execute_entry_uses_hard_cap_not_target_risk(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.05,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )

        strategy = MagicMock()
        strategy.calculate_stops.return_value = (10.0, 20.0)
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="risk-intent",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0),
        )

        self.assertGreaterEqual(trader._check_symbol_combined_risk_cap.call_count, 1)
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "PAPER")
        self.assertAlmostEqual(actionable_payload["target_risk_pct"], 0.05, places=6)
        self.assertAlmostEqual(actionable_payload["actual_risk_pct"], 0.10, places=6)

    def test_execute_entry_uses_fresh_symbol_positions_for_combined_risk(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.05,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.mt5.get_position_by_symbol_magic.return_value = None
        fresh_positions = [
            SimpleNamespace(
                symbol="XAUUSD",
                magic=TradeTagEncoder.encode_magic("XAUUSD", "D1", "TREND"),
                ticket=42,
                comment=TradeTagEncoder.encode_comment(
                    "XAUUSD", "D1", "MomentumBurstStrategy", "LONG", risk_pct=0.05
                ),
            )
        ]
        trader.mt5.get_positions.return_value = fresh_positions

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="fresh-risk-snapshot",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0),
        )

        self.assertGreaterEqual(trader._check_symbol_combined_risk_cap.call_count, 1)
        self.assertTrue(
            any(
                call.kwargs.get("positions_snapshot")
                and call.kwargs["positions_snapshot"][0] is fresh_positions[0]
                for call in trader._check_symbol_combined_risk_cap.call_args_list
            )
        )

    def test_execute_entry_blocks_fresh_same_timeframe_pairing(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.05,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.get_positions.return_value = [
            SimpleNamespace(
                symbol="XAUUSD",
                magic=TradeTagEncoder.encode_magic("XAUUSD", "D1", "TREND"),
                ticket=42,
                comment=TradeTagEncoder.encode_comment(
                    "XAUUSD", "D1", "MomentumBurstStrategy", "LONG", risk_pct=0.05
                ),
            )
        ]
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="fresh-position-pairing",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "D1",
                "regime": "TREND",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "BLOCKED_SYMBOL_POSITION_LIMIT")
        self.assertEqual(actionable_payload["position_context"]["fresh_open_positions_total"], 1)

    def test_execute_entry_respects_dataclass_backup_hard_cap(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.05,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )

        strategy = MagicMock()
        strategy.calculate_stops.return_value = (10.0, 20.0)
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="risk-intent-backup",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "PAPER")
        self.assertAlmostEqual(actionable_payload["actual_risk_pct"], 0.10, places=6)

    def test_execute_entry_blocks_when_commission_pushes_min_lot_over_hard_cap(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.05,
                max_risk_pct=0.10,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.pipeline_config.use_commission = True

        strategy = MagicMock()
        strategy.calculate_stops.return_value = (10.0, 20.0)
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01, commission_per_lot=7.0),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="risk-intent-commission",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0),
        )

        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "BLOCKED_MIN_LOT_EXCEEDS_CAP")
        self.assertGreater(actionable_payload["actual_risk_pct"], 0.10)

    def test_execute_entry_blocks_before_submit_when_margin_required_exceeds_free_margin(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.calc_margin_required.return_value = 150.0
        trader.mt5.send_market_order = MagicMock()

        strategy = MagicMock()
        strategy.calculate_stops.return_value = (10.0, 20.0)
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01, commission_per_lot=7.0),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="risk-intent-margin",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0, margin_free=100.0),
        )

        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_MARGIN_REQUIRED")

    def test_execute_entry_blocks_nonfinite_margin_level_when_margin_is_used(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.send_market_order = MagicMock()

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="nonfinite-margin-level",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(
                balance=10000.0,
                equity=10000.0,
                margin=25.0,
                margin_level=float("nan"),
                margin_free=10000.0,
            ),
        )

        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_MARGIN_UNAVAILABLE")

    def test_execute_entry_blocks_when_required_margin_known_but_free_margin_nonfinite(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.calc_margin_required.return_value = 150.0
        trader.mt5.send_market_order = MagicMock()

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01, commission_per_lot=7.0),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="nonfinite-free-margin",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0, margin_free=float("nan")),
        )

        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_MARGIN_UNAVAILABLE")

    def test_execute_entry_blocks_nonfinite_margin_required_value(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.calc_margin_required.return_value = float("inf")
        trader.mt5.send_market_order = MagicMock()

        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01, commission_per_lot=7.0),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="nonfinite-required-margin",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            positions_snapshot=[],
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0, margin_free=10000.0),
        )

        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_MARGIN_UNAVAILABLE")

    def test_execute_entry_submits_final_refreshed_entry_price(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.get_symbol_tick.side_effect = [
            SimpleNamespace(ask=1.2000, bid=1.1998),
            SimpleNamespace(ask=1.2050, bid=1.2048),
        ]
        trader.mt5.send_market_order.return_value = SimpleNamespace(
            success=True,
            volume=0.05,
            price=1.2050,
            retcode=10009,
            retcode_description="Done",
        )

        strategy = MagicMock()
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="prechecked-price",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
            account_info=SimpleNamespace(
                balance=10000.0,
                equity=10000.0,
                margin_level=0.0,
                margin_free=10000.0,
            ),
        )

        kwargs = trader.mt5.send_market_order.call_args.kwargs
        self.assertEqual(kwargs["price"], 1.2050)
        self.assertAlmostEqual(kwargs["sl"], 1.1950, places=6)
        self.assertAlmostEqual(kwargs["tp"], 1.2250, places=6)
        self.assertIs(kwargs["symbol_info"], trader.mt5.get_symbol_info.return_value)

    def test_execute_entry_records_hidden_symbol_skip(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(visible=False, trade_mode=2)

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=None,
            spec=SimpleNamespace(),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="hidden-symbol",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
        )

        self.assertEqual(
            trader._decision_throttle.record_decision.call_args.kwargs["action"],
            "SKIPPED_SYMBOL_NOT_VISIBLE",
        )
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_SYMBOL_NOT_VISIBLE")

    def test_execute_entry_records_disabled_symbol_skip(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(visible=True, trade_mode=0)

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=MagicMock(),
            features=None,
            spec=SimpleNamespace(),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="disabled-symbol",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
            },
            trade_intent=SimpleNamespace(stop_loss_pips=10.0, take_profit_pips=20.0),
            positions_snapshot=[],
        )

        self.assertEqual(
            trader._decision_throttle.record_decision.call_args.kwargs["action"],
            "SKIPPED_SYMBOL_TRADE_DISABLED",
        )
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "SKIPPED_SYMBOL_TRADE_DISABLED")

    def test_execute_entry_secondary_snapshot_unavailable_records_cleanly(self):
        trader = self._make_trader(
            SimpleNamespace(
                risk_per_trade_pct=0.50,
                max_risk_pct=1.0,
                min_position_size=0.01,
                max_position_size=0.0,
                risk_basis="equity",
                auto_widen_sl=True,
            )
        )
        trader.enable_trading = True
        trader.mt5.get_positions.return_value = None
        trader.mt5.get_position_by_symbol_magic.return_value = None
        trader.mt5.send_market_order = MagicMock()

        strategy = MagicMock()
        strategy.calculate_stops.return_value = (10.0, 20.0)
        features = pd.DataFrame({"Open": [1.0, 1.0], "High": [1.0, 1.0], "Low": [1.0, 1.0], "Close": [1.0, 1.0]})

        trader._execute_entry(
            symbol="XAUUSD",
            broker_symbol="XAUUSD",
            signal=1,
            strategy=strategy,
            features=features,
            spec=SimpleNamespace(pip_size=0.01, pip_value=1.0, spread_avg=3.0, point=0.01),
            magic=12345,
            config=SimpleNamespace(symbol="XAUUSD"),
            decision_key="secondary-snapshot-missing",
            bar_time_iso="2026-04-02T15:15:52",
            best_candidate={
                "timeframe": "M15",
                "regime": "CHOP",
                "strategy_name": "MomentumBurstStrategy",
                "signal": 1,
                "selection_score": 0.25,
                "quality_score": 0.40,
                "freshness": 1.0,
                "regime_strength": 0.62,
            },
            is_secondary_trade=True,
            position_context={"open_positions_total": 1},
            secondary_reason="d1_plus_lower",
            positions_snapshot=None,
            account_info=SimpleNamespace(balance=10000.0, equity=10000.0, margin_level=0.0),
        )

        trader.mt5.send_market_order.assert_not_called()
        actionable_payload = trader._actionable_log.record.call_args[0][1]
        self.assertEqual(actionable_payload["action"], "BLOCKED_POSITION_SNAPSHOT_UNAVAILABLE")
        self.assertIsNone(actionable_payload["volume"])
        self.assertIsNone(actionable_payload["actual_risk_pct"])
        self.assertEqual(actionable_payload["secondary_reason"], "d1_plus_lower")
        self.assertEqual(actionable_payload["position_context"]["open_positions_total"], 1)

        throttle_context = trader._decision_throttle.record_decision.call_args.kwargs["context"]
        self.assertEqual(throttle_context["secondary_reason"], "d1_plus_lower")
        self.assertEqual(throttle_context["position_context"]["open_positions_total"], 1)


if __name__ == "__main__":
    unittest.main()
