import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pandas as pd

from pm_core import Backtester, PipelineConfig, StrategyCategory, get_instrument_spec
from pm_main import LiveTrader
from pm_order_governance import GovernanceContext, evaluate_policy
from pm_pipeline import PortfolioManager, RegimeConfig, SymbolConfig
from pm_position import PositionConfig
from pm_strategies import BaseStrategy


class FixedStopStrategy(BaseStrategy):
    @property
    def name(self) -> str:
        return "FixedStopStrategy"

    @property
    def category(self) -> StrategyCategory:
        return StrategyCategory.TREND_FOLLOWING

    def get_default_params(self):
        return {}

    def generate_signals(self, features: pd.DataFrame, symbol: str) -> pd.Series:
        values = [0] * len(features)
        if len(values) > 1:
            values[1] = 1
        return pd.Series(values, index=features.index)

    def calculate_stops(self, features, signal, symbol, spec=None, bar_index=None):
        return 10.0, 10.0


def make_features() -> pd.DataFrame:
    idx = pd.date_range("2026-04-01 00:00:00", periods=8, freq="h")
    return pd.DataFrame(
        {
            "Open": [1.1000, 1.1000, 1.1000, 1.1015, 1.1025, 1.1030, 1.1035, 1.1040],
            "High": [1.1005, 1.1006, 1.1020, 1.1030, 1.1035, 1.1040, 1.1042, 1.1045],
            "Low": [1.0995, 1.0996, 1.0990, 1.1010, 1.1020, 1.1025, 1.1030, 1.1035],
            "Close": [1.1000, 1.1001, 1.1015, 1.1025, 1.1030, 1.1035, 1.1040, 1.1042],
            "ATR_14": [0.0005] * 8,
            "REGIME_LIVE": ["TREND"] * 8,
            "REGIME": ["TREND"] * 8,
        },
        index=idx,
    )


def make_live_bars(highs, closes, start="2026-04-01 00:00:00") -> pd.DataFrame:
    idx = pd.date_range(start, periods=len(highs), freq="h")
    opens = [close - 0.0001 for close in closes]
    lows = [min(open_, close) - 0.0003 for open_, close in zip(opens, closes)]
    return pd.DataFrame(
        {
            "Open": opens,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Volume": [1000.0] * len(highs),
        },
        index=idx,
    )


class _SymbolInfoStub:
    point = 0.0001
    trade_stops_level = 2
    trade_freeze_level = 0

    def to_instrument_spec(self):
        return get_instrument_spec("EURUSD")


class Section13SurfaceTests(unittest.TestCase):
    def test_control_governance_matches_baseline_backtest(self):
        features = make_features()
        strategy = FixedStopStrategy()
        signals = strategy.generate_signals(features, "EURUSD")
        config = PipelineConfig(use_spread=False, use_commission=False, use_slippage=False)
        backtester = Backtester(config)

        baseline = backtester.run(features, signals, "EURUSD", strategy)
        control = backtester.run(
            features,
            signals,
            "EURUSD",
            strategy,
            governance_policy={"name": "control_fixed"},
        )

        self.assertEqual(baseline["total_trades"], control["total_trades"])
        self.assertAlmostEqual(baseline["profit_factor"], control["profit_factor"], places=8)
        self.assertAlmostEqual(baseline["total_return_pct"], control["total_return_pct"], places=8)
        self.assertAlmostEqual(baseline["max_drawdown_pct"], control["max_drawdown_pct"], places=8)

    def test_breakeven_policy_tightens_stop_after_one_r(self):
        decision = evaluate_policy(
            {"name": "breakeven_1r"},
            GovernanceContext(
                symbol="EURUSD",
                timeframe="H1",
                regime="TREND",
                direction=1,
                entry_price=1.1000,
                current_stop_loss=1.0990,
                current_take_profit=1.1015,
                initial_stop_loss=1.0990,
                initial_take_profit=1.1015,
                current_price=1.1012,
                current_atr=0.0005,
                highest_since_entry=1.1012,
                lowest_since_entry=1.0998,
                pip_size=0.0001,
                tp_released=False,
            ),
        )
        self.assertIsNotNone(decision.stop_loss)
        self.assertAlmostEqual(decision.stop_loss, 1.1000, places=8)

    def test_atr_trail_stop_is_clamped_below_current_price(self):
        decision = evaluate_policy(
            {"name": "atr_trail_capped", "trail_activation_atr_mult": 0.4, "trail_atr_mult": 0.1},
            GovernanceContext(
                symbol="EURUSD",
                timeframe="H1",
                regime="TREND",
                direction=1,
                entry_price=1.1000,
                current_stop_loss=1.0990,
                current_take_profit=1.1050,
                initial_stop_loss=1.0990,
                initial_take_profit=1.1050,
                current_price=1.1011,
                current_atr=0.0020,
                highest_since_entry=1.1015,
                lowest_since_entry=1.0995,
                pip_size=0.0001,
                price_step=0.0001,
                min_stop_distance=0.0002,
            ),
        )
        self.assertIsNotNone(decision.stop_loss)
        self.assertLess(decision.stop_loss, 1.1011)
        self.assertLessEqual(decision.stop_loss, 1.1009)

    def test_live_eligibility_report_requires_exact_local_winner(self):
        manager = PortfolioManager.__new__(PortfolioManager)
        manager.config = PipelineConfig(
            production_retrain_interval_weeks=2,
            production_retrain_weekday="sunday",
            production_retrain_time="00:01",
            production_retrain_anchor_date="2026-03-29",
        )
        exact_cfg = RegimeConfig(
            strategy_name="FixedStopStrategy",
            parameters={},
            quality_score=0.8,
            val_metrics={"profit_factor": 1.5, "total_return_pct": 8.0, "max_drawdown_pct": 4.0},
            trained_at=datetime(2026, 4, 12, 0, 1),
        )
        symbol_cfg = SymbolConfig(
            symbol="EURUSD",
            regime_configs={"H1": {"TREND": exact_cfg}},
            default_config=exact_cfg,
            optimized_at=datetime(2026, 4, 12, 0, 1),
            valid_until=datetime(2026, 4, 26, 0, 1),
        )
        manager.symbol_configs = {"EURUSD": symbol_cfg}

        exact_report = manager.live_eligibility_report(
            "EURUSD",
            "H1",
            "TREND",
            at_dt=datetime(2026, 4, 18, 12, 0),
        )
        missing_report = manager.live_eligibility_report(
            "EURUSD",
            "H4",
            "RANGE",
            at_dt=datetime(2026, 4, 18, 12, 0),
        )

        self.assertTrue(exact_report["config_present"])
        self.assertIn(exact_report["freshness_band"], {"fresh", "mid_cycle", "end_of_cycle"})
        self.assertFalse(missing_report["config_present"])

    def test_get_governance_policy_requires_exact_local_winner(self):
        manager = PortfolioManager.__new__(PortfolioManager)
        manager.config = PipelineConfig()
        exact_cfg = RegimeConfig(
            strategy_name="FixedStopStrategy",
            parameters={},
            quality_score=0.8,
            governance_policy={"selected_policy": "atr_trail_capped"},
        )
        symbol_cfg = SymbolConfig(
            symbol="EURUSD",
            regime_configs={"H1": {"TREND": exact_cfg}},
            default_config=RegimeConfig(
                strategy_name="FixedStopStrategy",
                parameters={},
                quality_score=0.5,
                governance_policy={"selected_policy": "control_fixed"},
            ),
        )
        manager.symbol_configs = {"EURUSD": symbol_cfg}

        exact_policy = manager.get_governance_policy("EURUSD", "H1", "TREND")
        missing_policy = manager.get_governance_policy("EURUSD", "H4", "RANGE")

        self.assertEqual(exact_policy["selected_policy"], "atr_trail_capped")
        self.assertEqual(missing_policy["selected_policy"], "control_fixed")

    def test_trade_intent_carries_local_governance_hint(self):
        strategy = FixedStopStrategy()
        features = make_features()
        intent = strategy.build_trade_intent(
            features,
            symbol="EURUSD",
            timeframe="H1",
            regime="TREND",
            signal=1,
            bar_index=1,
            signal_strength=0.8,
            selection_score=0.64,
            quality_score=0.8,
            governance_hint={"selected_policy": "atr_trail_capped"},
            metadata={"source": "unit_test"},
        )
        self.assertEqual(intent.timeframe, "H1")
        self.assertEqual(intent.regime, "TREND")
        self.assertEqual(intent.governance_hint["selected_policy"], "atr_trail_capped")
        self.assertEqual(intent.metadata["source"], "unit_test")

    def test_governance_cycle_tracks_positions_per_ticket_in_same_context(self):
        pipeline_config = PipelineConfig(
            local_governance_live_mode="shadow",
            use_spread=False,
            live_min_bars=3,
            live_bars_count=10,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        mt5.get_symbol_info.return_value = _SymbolInfoStub()
        pm = Mock()
        pm.symbols = ["EURUSD"]
        trader = LiveTrader(
            mt5_connector=mt5,
            portfolio_manager=pm,
            position_config=PositionConfig(),
            enable_trading=False,
            pipeline_config=pipeline_config,
        )
        trader._resolve_symbol_from_broker = Mock(return_value="EURUSD")
        trader._infer_position_timeframe = Mock(return_value="H1")
        trader._build_magic_lookup = Mock(return_value=({}, {111: "TREND"}))
        trader._get_live_bars = Mock(
            return_value=make_live_bars(
                highs=[1.1004, 1.1015, 1.1020, 1.1021],
                closes=[1.1002, 1.1010, 1.1018, 1.1019],
            )
        )

        cfg = RegimeConfig(
            strategy_name="FixedStopStrategy",
            parameters={},
            quality_score=0.8,
            governance_policy={
                "selected_policy": "atr_trail_capped",
                "trail_activation_atr_mult": 0.5,
                "trail_atr_mult": 0.5,
            },
        )
        validated_configs = {
            "EURUSD": SymbolConfig(
                symbol="EURUSD",
                regime_configs={"H1": {"TREND": cfg}},
            )
        }
        positions = [
            Mock(ticket=1, symbol="EURUSD", magic=111, time=pd.Timestamp("2026-04-01 00:30:00"), type=0, sl=1.0990, tp=1.1050, price_open=1.1000),
            Mock(ticket=2, symbol="EURUSD", magic=111, time=pd.Timestamp("2026-04-01 00:45:00"), type=0, sl=1.0990, tp=1.1050, price_open=1.1000),
        ]

        with patch("pm_main.FeatureComputer.compute_all", side_effect=lambda bars, **_: bars.assign(ATR_14=0.0010)):
            trader._run_order_governance_cycle(validated_configs, positions)

        self.assertIn(1, trader._order_governance_state)
        self.assertIn(2, trader._order_governance_state)
        self.assertEqual(len(trader._last_governance_bar_times), 2)
        mt5.modify_position.assert_not_called()

    def test_governance_shadow_mode_preserves_prior_shadow_stop(self):
        pipeline_config = PipelineConfig(
            local_governance_live_mode="shadow",
            use_spread=False,
            live_min_bars=3,
            live_bars_count=10,
        )
        mt5 = Mock()
        mt5.is_connected.return_value = False
        mt5.get_symbol_info.return_value = _SymbolInfoStub()
        pm = Mock()
        pm.symbols = ["EURUSD"]
        trader = LiveTrader(
            mt5_connector=mt5,
            portfolio_manager=pm,
            position_config=PositionConfig(),
            enable_trading=False,
            pipeline_config=pipeline_config,
        )
        trader._resolve_symbol_from_broker = Mock(return_value="EURUSD")
        trader._infer_position_timeframe = Mock(return_value="H1")
        trader._build_magic_lookup = Mock(return_value=({}, {111: "TREND"}))
        trader._get_live_bars = Mock(
            side_effect=[
                make_live_bars(
                    highs=[1.1004, 1.1015, 1.1020, 1.1021],
                    closes=[1.1002, 1.1010, 1.1018, 1.1019],
                    start="2026-04-01 00:00:00",
                ),
                make_live_bars(
                    highs=[1.1004, 1.1016, 1.1017, 1.1019],
                    closes=[1.1002, 1.1012, 1.1018, 1.1019],
                    start="2026-04-01 01:00:00",
                ),
            ]
        )

        cfg = RegimeConfig(
            strategy_name="FixedStopStrategy",
            parameters={},
            quality_score=0.8,
            governance_policy={
                "selected_policy": "atr_trail_capped",
                "trail_activation_atr_mult": 0.5,
                "trail_atr_mult": 0.5,
            },
        )
        validated_configs = {
            "EURUSD": SymbolConfig(
                symbol="EURUSD",
                regime_configs={"H1": {"TREND": cfg}},
            )
        }
        position = Mock(
            ticket=1,
            symbol="EURUSD",
            magic=111,
            time=pd.Timestamp("2026-04-01 00:30:00"),
            type=0,
            sl=1.0990,
            tp=1.1050,
            price_open=1.1000,
        )

        with patch("pm_main.FeatureComputer.compute_all", side_effect=lambda bars, **_: bars.assign(ATR_14=0.0010)):
            trader._run_order_governance_cycle(validated_configs, [position])
            first_state = dict(trader._get_order_governance_state(1))
            trader._run_order_governance_cycle(validated_configs, [position])
            second_state = dict(trader._get_order_governance_state(1))

        self.assertGreater(first_state["shadow_stop_loss"], 1.0990)
        self.assertAlmostEqual(second_state["shadow_stop_loss"], first_state["shadow_stop_loss"], places=8)
        mt5.modify_position.assert_not_called()


if __name__ == "__main__":
    unittest.main()
