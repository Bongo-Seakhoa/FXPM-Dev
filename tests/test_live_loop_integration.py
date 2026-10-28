"""
Integration test for the live trading loop.

Mocks MT5 and verifies the full signal processing flow:
  symbol -> bars -> features -> regime -> signal -> throttle -> decision

This test does NOT require a real MT5 connection.
"""

import unittest
import sys
import os
import json
import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _synthetic_pins(**overrides):
    values = dict(expected_login=123, expected_server="Broker-Test", expected_currency="USD")
    values.update(overrides)
    return SimpleNamespace(**values)


def _synthetic_account(**overrides):
    """Explicit synthetic identity; these are not broker records."""
    values = dict(login=123, server="Broker-Test", currency="USD", equity=1000.0,
                  margin=0.0, margin_level=1000.0, trade_allowed=True, trade_expert=True)
    values.update(overrides)
    return SimpleNamespace(**values)


def _make_ohlcv(n: int = 300, start_price: float = 1.1000) -> pd.DataFrame:
    """Generate synthetic OHLCV data with realistic price action."""
    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=n, freq="h")
    close = start_price + np.cumsum(np.random.randn(n) * 0.0005)
    high = close + np.abs(np.random.randn(n) * 0.0003)
    low = close - np.abs(np.random.randn(n) * 0.0003)
    opn = close + np.random.randn(n) * 0.0001
    high = np.maximum(high, opn)
    low = np.minimum(low, opn)
    vol = np.random.randint(100, 10000, size=n).astype(float)
    df = pd.DataFrame(
        {"Open": opn, "High": high, "Low": low, "Close": close, "Volume": vol},
        index=dates,
    )
    return df


class LiveLoopIntegrationTests(unittest.TestCase):
    """Integration tests for the live trading loop (mocked MT5)."""

    def setUp(self):
        """Set up mocked components for the live trading loop."""
        self.bars = _make_ohlcv(300)
        self.tmp_dir = os.path.join(os.getcwd(), "_test_tmp", "live_loop", self._testMethodName)
        os.makedirs(self.tmp_dir, exist_ok=True)
        self.configs_path = os.path.join(self.tmp_dir, "pm_configs.json")

        now = datetime.datetime.now()
        valid_until = (now + datetime.timedelta(days=7)).isoformat()
        configs = {
            "EURUSD": {
                "symbol": "EURUSD",
                "strategy_name": "SupertrendStrategy",
                "timeframe": "H1",
                "parameters": {
                    "atr_period": 10, "multiplier": 3.0,
                    "sl_atr_mult": 2.0, "tp_atr_mult": 3.0,
                },
                "is_validated": True,
                "validation_reason": "passed all checks",
                "optimized_at": now.isoformat(),
                "valid_until": valid_until,
                "regime_configs": {
                    "H1": {
                        "TREND": {
                            "strategy_name": "SupertrendStrategy",
                            "parameters": {
                                "atr_period": 10, "multiplier": 3.0,
                                "sl_atr_mult": 2.0, "tp_atr_mult": 3.0,
                            },
                            "quality_score": 0.75,
                            "regime_train_trades": 30,
                            "regime_val_trades": 20,
                            "train_metrics": {"win_rate": 55.0, "profit_factor": 1.5,
                                              "total_return_pct": 12.0, "max_drawdown_pct": 5.0},
                            "val_metrics": {"win_rate": 52.0, "profit_factor": 1.3,
                                            "total_return_pct": 8.0, "max_drawdown_pct": 6.0},
                            "validation_status": "validated",
                        },
                        "RANGE": {
                            "strategy_name": "BollingerBounceStrategy",
                            "parameters": {"bb_period": 20, "bb_std": 2.0,
                                           "sl_atr_mult": 2.0, "tp_atr_mult": 3.0},
                            "quality_score": 0.65,
                            "regime_train_trades": 25,
                            "regime_val_trades": 18,
                            "train_metrics": {"win_rate": 50.0, "profit_factor": 1.2},
                            "val_metrics": {"win_rate": 48.0, "profit_factor": 1.1},
                            "validation_status": "validated",
                        },
                    }
                },
            }
        }
        with open(self.configs_path, "w") as f:
            json.dump(configs, f)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_config_winner_lookup(self):
        """SymbolConfig should resolve regime winners from pm_configs.json."""
        from pm_pipeline import SymbolConfig

        with open(self.configs_path) as f:
            raw = json.load(f)

        config = SymbolConfig.from_dict(raw["EURUSD"])
        self.assertTrue(config.has_regime_configs())
        self.assertIn("H1", config.get_available_timeframes())

        trend_winner = config.get_regime_config("H1", "TREND")
        self.assertIsNotNone(trend_winner)
        self.assertEqual(trend_winner.strategy_name, "SupertrendStrategy")

        range_winner = config.get_regime_config("H1", "RANGE")
        self.assertIsNotNone(range_winner)
        self.assertEqual(range_winner.strategy_name, "BollingerBounceStrategy")

        # No CHOP winner should exist
        chop_winner = config.get_regime_config("H1", "CHOP")
        self.assertIsNone(chop_winner)

    def test_strategy_signal_generation_end_to_end(self):
        """bars -> features -> strategy -> signal should produce valid output."""
        from pm_core import FeatureComputer
        from pm_strategies import StrategyRegistry

        features = FeatureComputer.compute_all(self.bars.copy())

        strategy = StrategyRegistry.get("SupertrendStrategy",
                                        atr_period=10, multiplier=3.0,
                                        sl_atr_mult=2.0, tp_atr_mult=3.0)
        signals = strategy.generate_signals(features, "EURUSD")

        self.assertEqual(len(signals), len(features))
        unique_vals = set(signals.unique())
        self.assertTrue(unique_vals.issubset({-1, 0, 1}),
                        f"Signal values should be in {{-1, 0, 1}}, got {unique_vals}")

    def test_decision_throttle_integration(self):
        """DecisionThrottle should suppress same-bar duplicate and allow on new bar."""
        from pm_main import DecisionThrottle

        log_path = os.path.join(self.tmp_dir, "test_throttle.json")
        throttle = DecisionThrottle(log_path=log_path)

        bar_time = "2025-06-01T14:00:00"
        dk = DecisionThrottle.make_decision_key(
            "EURUSD", "SupertrendStrategy", "H1", "TREND", 1, bar_time
        )

        # First time: not suppressed
        self.assertFalse(throttle.should_suppress("EURUSD", dk, bar_time))
        throttle.record_decision(
            symbol="EURUSD", decision_key=dk, bar_time_iso=bar_time,
            timeframe="H1", regime="TREND", strategy_name="SupertrendStrategy",
            direction=1, action="EXECUTED"
        )
        # Second time same bar: suppressed
        self.assertTrue(throttle.should_suppress("EURUSD", dk, bar_time))

        # New bar: allowed again
        next_bar = "2025-06-01T15:00:00"
        self.assertFalse(throttle.should_suppress("EURUSD", dk, next_bar))

    def test_live_trader_uses_single_positions_snapshot_per_iteration(self):
        """The live runtime should fetch one positions snapshot and reuse it."""
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        mock_mt5 = Mock()
        mock_mt5.config = _synthetic_pins()
        mock_mt5.is_connected.return_value = False
        mock_mt5.get_positions.return_value = [Mock(symbol="EURUSD", magic=1)]
        mock_mt5.get_account_info.return_value = _synthetic_account()

        mock_pm = Mock()
        mock_pm.symbols = ["EURUSD", "GBPUSD"]
        exact_live_configs = {
            "EURUSD": Mock(),
            "GBPUSD": Mock(),
        }
        # Entry evaluation must be authorized by the exact funded-live
        # selector.  get_validated_configs() is deliberately management-only.
        mock_pm.get_live_eligible_configs.return_value = exact_live_configs
        mock_pm.get_validated_configs.return_value = exact_live_configs

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            enable_trading=False,
        )
        trader._process_symbol = MagicMock()

        trader.process_all_symbols()

        self.assertEqual(mock_mt5.get_positions.call_count, 1)
        self.assertEqual(mock_mt5.get_account_info.call_count, 1)
        self.assertEqual(trader._process_symbol.call_count, 2)
        for call in trader._process_symbol.call_args_list:
            self.assertIs(call.kwargs["positions_snapshot"], mock_mt5.get_positions.return_value)
            self.assertIs(call.kwargs["account_info"], mock_mt5.get_account_info.return_value)

    def test_live_trader_fails_closed_when_positions_snapshot_missing(self):
        """Missing live positions should block the iteration instead of trading blind."""
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        mock_mt5 = Mock()
        mock_mt5.config = _synthetic_pins()
        mock_mt5.is_connected.return_value = False
        mock_mt5.get_positions.return_value = None
        mock_mt5.get_account_info.return_value = _synthetic_account()

        mock_pm = Mock()
        mock_pm.symbols = ["EURUSD"]
        mock_pm.get_validated_configs.return_value = {
            "EURUSD": Mock(),
        }

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            enable_trading=False,
        )
        trader._process_symbol = MagicMock()

        trader.process_all_symbols()

        self.assertEqual(mock_mt5.get_positions.call_count, 1)
        self.assertEqual(mock_mt5.get_account_info.call_count, 0)
        trader._process_symbol.assert_not_called()

    def test_drift_monitor_sync_records_new_closing_deals(self):
        """Recent closing deals should feed the live drift monitor once."""
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        mock_mt5 = Mock()
        mock_mt5.config = _synthetic_pins()
        mock_mt5.is_connected.return_value = False
        mock_mt5.get_recent_closing_deals.return_value = [{
            "ticket": 101,
            "symbol": "EURUSD",
            "profit": 42.0,
            "comment": "PM3:EURUSD:H1:abcd:L:10",
        }]
        mock_mt5.find_broker_symbol.side_effect = lambda symbol: symbol

        mock_pm = Mock()
        mock_pm.symbols = ["EURUSD"]
        exact_live_configs = {
            "EURUSD": Mock(val_metrics={"win_rate": 50.0, "mean_r": 0.5})
        }
        mock_pm.get_live_eligible_configs.return_value = exact_live_configs
        mock_pm.get_validated_configs.return_value = exact_live_configs

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            enable_trading=False,
        )

        trader._sync_drift_monitor()
        trader._sync_drift_monitor()  # duplicate ticket should be ignored

        summary = trader.drift_monitor.get_summary("EURUSD")
        self.assertEqual(summary["trade_count"], 1)

    def test_close_on_opposite_signal_closes_before_new_entry(self):
        """Opposite-signal exit handling should close first and skip the new entry."""
        from pm_main import LiveTrader
        from pm_position import PositionConfig
        from pm_core import get_instrument_spec

        mock_mt5 = Mock()
        mock_mt5.config = _synthetic_pins()
        mock_mt5.is_connected.return_value = False
        mock_mt5.find_broker_symbol.return_value = "EURUSD"
        mock_mt5.get_positions.return_value = [
            Mock(symbol="EURUSD", type=1, magic=1, comment="PM3:EURUSD:H1:abc:L:10")
        ]
        mock_mt5.get_account_info.return_value = _synthetic_account()
        mock_mt5.get_symbol_info.return_value = Mock(
            to_instrument_spec=Mock(return_value=get_instrument_spec("EURUSD"))
        )

        mock_pm = Mock()
        mock_pm.symbols = ["EURUSD"]
        exact_live_configs = {
            "EURUSD": Mock(
                has_regime_configs=Mock(return_value=False),
                timeframe="H1",
            )
        }
        mock_pm.get_live_eligible_configs.return_value = exact_live_configs
        mock_pm.get_validated_configs.return_value = exact_live_configs
        mock_pipeline = Mock(
            margin_entry_block_level=100.0,
            margin_recovery_start_level=80.0,
            margin_panic_level=65.0,
            margin_reopen_level=100.0,
            margin_recovery_closes_per_cycle=1,
            margin_panic_closes_per_cycle=3,
            margin_reopen_cooldown_minutes=15.0,
        )
        mock_pipeline.actionable_score_margin = 1.0
        mock_pipeline.allow_d1_plus_lower_tf = True

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            enable_trading=False,
            close_on_opposite_signal=True,
            pipeline_config=mock_pipeline,
        )
        trader._evaluate_regime_candidates = MagicMock(return_value=(
            [{
                "strategy_name": "TrendStrategy",
                "timeframe": "D1",
                "regime": "TREND",
                "signal": 1,
                "selection_score": 1.0,
                "quality_score": 1.0,
                "freshness": 1.0,
                "regime_strength": 1.0,
                "features": self.bars.copy(),
                "strategy": Mock(),
                "bar_time": "2025-01-01T00:00:00",
            }],
            {},
        ))
        trader._close_position_on_signal = MagicMock()
        trader._execute_entry = MagicMock()

        trader.process_all_symbols()

        trader._close_position_on_signal.assert_called_once()
        trader._execute_entry.assert_not_called()

    def test_unknown_position_timeframe_logs_once_and_blocks_secondary_trade(self):
        """Unknown open-position timeframe should fail closed without repeated warning spam."""
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        mock_mt5 = Mock()
        mock_mt5.config = _synthetic_pins()
        mock_mt5.is_connected.return_value = False
        mock_mt5.find_broker_symbol.return_value = "EURUSD"
        mock_mt5.get_positions.return_value = [
            Mock(symbol="EURUSD", type=0, magic=987654321, comment="", ticket=4242, identifier=4242)
        ]
        mock_mt5.get_account_info.return_value = _synthetic_account()

        mock_pm = Mock()
        mock_pm.symbols = ["EURUSD"]
        exact_live_configs = {
            "EURUSD": Mock(
                has_regime_configs=Mock(return_value=False),
                timeframe="H1",
            )
        }
        mock_pm.get_live_eligible_configs.return_value = exact_live_configs
        mock_pm.get_validated_configs.return_value = exact_live_configs

        mock_pipeline = Mock(
            margin_entry_block_level=100.0,
            margin_recovery_start_level=80.0,
            margin_panic_level=65.0,
            margin_reopen_level=100.0,
            margin_recovery_closes_per_cycle=1,
            margin_panic_closes_per_cycle=3,
            margin_reopen_cooldown_minutes=15.0,
        )
        mock_pipeline.actionable_score_margin = 1.0
        mock_pipeline.allow_d1_plus_lower_tf = True

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            enable_trading=False,
            pipeline_config=mock_pipeline,
        )
        trader.logger = MagicMock()
        trader._evaluate_regime_candidates = MagicMock()

        trader.process_all_symbols()
        trader.process_all_symbols()

        trader._evaluate_regime_candidates.assert_not_called()
        warning_messages = [
            args[0]
            for args, _kwargs in trader.logger.warning.call_args_list
            if args and "timeframe unknown; blocking secondary trade" in args[0]
        ]
        self.assertEqual(len(warning_messages), 1)

    def test_no_actionable_signal_is_visible_at_info_level(self):
        """No-actionable winners should be visible in the normal INFO log stream."""
        from pm_main import LiveTrader, DecisionThrottle
        from pm_position import PositionConfig

        mock_mt5 = Mock()
        mock_mt5.config = _synthetic_pins()
        mock_mt5.is_connected.return_value = False

        mock_pm = Mock()
        mock_pm.symbols = ["EURUSD"]

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            enable_trading=False,
            pipeline_config=Mock(),
        )
        trader.logger = MagicMock()
        trader._decision_throttle = DecisionThrottle(
            log_path=os.path.join(self.tmp_dir, "last_trade_log.json")
        )

        candidate = {
            "strategy_name": "SupertrendStrategy",
            "timeframe": "H1",
            "regime": "TREND",
        }

        trader._log_no_actionable_signal(
            symbol="EURUSD",
            message="NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)",
            best_candidate=candidate,
            bar_time_iso="2026-04-02 18:00:00",
            action_type="NO_ACTIONABLE_WINNER_SIGNAL",
        )

        trader.logger.info.assert_any_call(
            "[EURUSD] NO_ACTIONABLE_WINNER_SIGNAL (no actionable winners)"
        )

    def test_process_all_symbols_logs_sweep_completion_heartbeat(self):
        """Each live sweep should emit a completion heartbeat at INFO level."""
        from pm_main import LiveTrader
        from pm_position import PositionConfig

        mock_mt5 = Mock()
        mock_mt5.config = _synthetic_pins()
        mock_mt5.is_connected.return_value = False
        mock_mt5.get_positions.return_value = []
        mock_mt5.get_account_info.return_value = _synthetic_account(
            equity=5100.0,
            trade_allowed=True,
            trade_expert=True,
        )

        mock_pm = Mock()
        mock_pm.symbols = ["EURUSD"]
        mock_pm.get_validated_configs.return_value = {
            "EURUSD": Mock(),
        }

        trader = LiveTrader(
            mt5_connector=mock_mt5,
            portfolio_manager=mock_pm,
            position_config=PositionConfig(),
            enable_trading=False,
            pipeline_config=Mock(
                margin_entry_block_level=100.0,
                margin_recovery_start_level=80.0,
                margin_panic_level=65.0,
                margin_reopen_level=100.0,
                margin_recovery_closes_per_cycle=1,
                margin_panic_closes_per_cycle=3,
                margin_reopen_cooldown_minutes=15.0,
            ),
        )
        trader.logger = MagicMock()
        trader._process_symbol = MagicMock()
        trader._sync_drift_monitor = MagicMock()

        trader.process_all_symbols()

        heartbeat_messages = [
            args[0]
            for args, _kwargs in trader.logger.info.call_args_list
            if args and str(args[0]).startswith("Live sweep complete:")
        ]
        self.assertEqual(len(heartbeat_messages), 1)

    def test_regime_detection_on_synthetic_bars(self):
        """MarketRegimeDetector should produce valid regime labels."""
        from pm_regime import MarketRegimeDetector

        detector = MarketRegimeDetector()
        result = detector.compute_regime_scores(self.bars.copy())

        self.assertIn("REGIME", result.columns)
        valid_regimes = {"TREND", "RANGE", "BREAKOUT", "CHOP"}
        actual_regimes = set(result["REGIME"].dropna().unique())
        self.assertTrue(actual_regimes.issubset(valid_regimes),
                        f"Regimes should be subset of {valid_regimes}, got {actual_regimes}")
        self.assertTrue(len(actual_regimes) > 0, "Should detect at least one regime")

    def test_full_signal_pipeline_no_mt5(self):
        """Full pipeline: bars -> features -> regime -> strategy -> signal -> SL/TP.
        Simulates what _process_symbol does without a real MT5 connection."""
        from pm_core import FeatureComputer, get_instrument_spec
        from pm_regime import MarketRegimeDetector
        from pm_strategies import StrategyRegistry

        bars = self.bars.copy()

        # Step 1: Features
        features = FeatureComputer.compute_all(bars)
        self.assertGreater(len(features.columns), 10)

        # Step 2: Regime
        detector = MarketRegimeDetector()
        regime_result = detector.compute_regime_scores(bars)
        current_regime = regime_result["REGIME"].iloc[-1]
        self.assertIn(current_regime, ["TREND", "RANGE", "BREAKOUT", "CHOP"])

        # Step 3: Signal
        strategy = StrategyRegistry.get("SupertrendStrategy",
                                        atr_period=10, multiplier=3.0,
                                        sl_atr_mult=2.0, tp_atr_mult=3.0)
        signals = strategy.generate_signals(features, "EURUSD")
        current_signal = int(signals.iloc[-2])  # signal bar -> next bar entry

        # Step 4: SL/TP
        if current_signal != 0:
            spec = get_instrument_spec("EURUSD")
            sl_pips, tp_pips = strategy.compute_sl_tp(features, len(features) - 1, spec)
            self.assertGreater(sl_pips, 0, "SL should be positive")
            self.assertGreater(tp_pips, 0, "TP should be positive")

        self.assertIn(current_signal, [-1, 0, 1])


    def test_trade_comment_v3_format(self):
        """v3 comment format (winners-only) should encode and decode correctly."""
        from pm_position import TradeTagEncoder

        comment = TradeTagEncoder.encode_comment(
            symbol="EURUSD", timeframe="H1",
            strategy_name="SupertrendStrategy",
            direction="LONG", risk_pct=1.5,
        )
        self.assertTrue(comment.startswith("PM3:"))
        decoded = TradeTagEncoder.decode_comment(comment)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["symbol"], "EURUSD")
        self.assertEqual(decoded["timeframe"], "H1")
        self.assertEqual(decoded["direction"], "LONG")
        self.assertAlmostEqual(decoded["risk_pct"], 1.5, places=1)
        # v3 should NOT have a tier field
        self.assertNotIn("tier", decoded)

    def test_trade_comment_v2_backward_compat(self):
        """v2 comment strings from existing positions should still decode."""
        from pm_position import TradeTagEncoder

        # Simulate a v2 comment that an existing open position might have
        v2_comment = "PM2:GBPUSD:M15:a1b2c:L:1:20"
        decoded = TradeTagEncoder.decode_comment(v2_comment)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["symbol"], "GBPUSD")
        self.assertEqual(decoded["timeframe"], "M15")
        self.assertEqual(decoded["direction"], "LONG")
        self.assertAlmostEqual(decoded["risk_pct"], 2.0, places=1)
        # v2 backward compat should still work without tier
        self.assertNotIn("tier", decoded)

    def test_trade_comment_v1_legacy(self):
        """v1 (legacy) comment strings should still decode."""
        from pm_position import TradeTagEncoder

        v1_comment = "PM:EURUSD:H1:SupertrendStrategy:LONG"
        decoded = TradeTagEncoder.decode_comment(v1_comment)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["symbol"], "EURUSD")
        self.assertEqual(decoded["timeframe"], "H1")
        self.assertEqual(decoded["strategy_name"], "SupertrendStrategy")
        self.assertEqual(decoded["direction"], "LONG")

    def test_regime_candidate_selection_uses_regime_live_surface(self):
        """Live winner lookup should follow the same decision-time regime surface as optimization."""
        from types import SimpleNamespace
        from pm_main import LiveTrader
        from pm_pipeline import RegimeConfig, SymbolConfig

        class _FakeStrategy:
            name = "FakeLiveStrategy"

            def generate_signals(self, features, symbol):
                return pd.Series([0, 1, 0], index=features.index)

        trader = LiveTrader.__new__(LiveTrader)
        trader.pipeline_config = SimpleNamespace(
            regime_params_file="regime_params.json",
            live_bars_count=10,
            live_min_bars=2,
            regime_min_val_profit_factor=1.0,
            regime_min_val_return_pct=0.0,
            fx_val_max_drawdown=35.0,
            regime_min_val_return_dd_ratio=0.0,
        )
        trader._candidate_cache = {}
        trader._cache_hits = 0
        trader._cache_misses = 0
        trader._last_bar_times = {}
        trader._is_symbol_timeframe_due = Mock(return_value=True)
        trader._prune_cache = Mock()
        trader.logger = Mock()
        trader.pm = None
        bars = _make_ohlcv(3)
        trader._get_live_bars = Mock(return_value=bars)

        features = bars.copy()
        features["REGIME"] = ["TREND", "RANGE", "RANGE"]
        features["REGIME_LIVE"] = ["CHOP", "TREND", "RANGE"]
        features["REGIME_STRENGTH"] = [0.1, 0.2, 0.3]
        features["REGIME_STRENGTH_LIVE"] = [0.4, 0.9, 0.2]

        config = SymbolConfig(
            symbol="EURUSD",
            regime_configs={
                "M5": {
                    "TREND": RegimeConfig(
                        strategy_name="FakeLiveStrategy",
                        promotion_status="PROMOTED",
                        parameters={},
                        quality_score=0.8,
                        val_metrics={
                            "profit_factor": 1.5,
                            "total_return_pct": 5.0,
                            "max_drawdown_pct": 2.0,
                        },
                    ),
                    "RANGE": RegimeConfig(
                        strategy_name="ShouldNotUse",
                        promotion_status="PROMOTED",
                        parameters={},
                        quality_score=0.8,
                        val_metrics={
                            "profit_factor": 1.5,
                            "total_return_pct": 5.0,
                            "max_drawdown_pct": 2.0,
                        },
                    ),
                }
            },
        )

        with patch("pm_main.FeatureComputer.compute_all", return_value=features), \
             patch("pm_main.StrategyRegistry.get", return_value=_FakeStrategy()):
            candidates, stats = trader._evaluate_regime_candidates("EURUSD", "EURUSD", config)

        self.assertEqual(stats["winner_candidates"], 1)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["regime"], "TREND")
        self.assertAlmostEqual(candidates[0]["regime_strength"], 0.9)

    def test_regime_candidate_selection_requires_regime_live(self):
        from types import SimpleNamespace
        from pm_main import LiveTrader
        from pm_pipeline import RegimeConfig, SymbolConfig

        trader = LiveTrader.__new__(LiveTrader)
        trader.pipeline_config = SimpleNamespace(
            regime_params_file="regime_params.json",
            live_bars_count=10,
            live_min_bars=2,
            regime_min_val_profit_factor=1.0,
            regime_min_val_return_pct=0.0,
            fx_val_max_drawdown=35.0,
            regime_min_val_return_dd_ratio=0.0,
        )
        trader._candidate_cache = {}
        trader._cache_hits = 0
        trader._cache_misses = 0
        trader._last_bar_times = {}
        trader._is_symbol_timeframe_due = Mock(return_value=True)
        trader._prune_cache = Mock()
        trader.logger = Mock()
        trader.pm = None
        bars = _make_ohlcv(3)
        trader._get_live_bars = Mock(return_value=bars)

        features = bars.copy()
        features["REGIME"] = ["CHOP", "TREND", "RANGE"]
        features["REGIME_STRENGTH"] = [0.4, 0.9, 0.2]

        config = SymbolConfig(
            symbol="EURUSD",
            regime_configs={
                "M5": {
                    "TREND": RegimeConfig(
                        strategy_name="ShouldNotTrade",
                        promotion_status="PROMOTED",
                        parameters={},
                        quality_score=0.8,
                        val_metrics={
                            "profit_factor": 1.5,
                            "total_return_pct": 5.0,
                            "max_drawdown_pct": 2.0,
                        },
                    ),
                }
            },
        )

        with patch("pm_main.FeatureComputer.compute_all", return_value=features), \
             patch("pm_main.StrategyRegistry.get") as registry_get:
            candidates, stats = trader._evaluate_regime_candidates("EURUSD", "EURUSD", config)

        self.assertEqual(candidates, [])
        self.assertEqual(stats["missing_regime_live"], 1)
        registry_get.assert_not_called()
        trader.logger.warning.assert_called()


class AccountIdentityGuardTests(unittest.TestCase):
    """Synthetic account changes must not reach broker mutation seams."""

    def _identity_trader(self, account=None):
        from pm_main import LiveTrader
        trader = LiveTrader.__new__(LiveTrader)
        trader.mt5 = Mock(config=_synthetic_pins())
        trader.mt5.get_account_info.return_value = account or _synthetic_account()
        trader._is_current_refit_demo = False
        trader.pipeline_config = SimpleNamespace(
            live_min_account_equity=100.0, live_min_account_equity_currency="USD"
        )
        trader.logger = Mock()
        trader.enable_trading = True
        return trader

    def test_pure_identity_comparator_requires_all_pins_and_actual_identity(self):
        from pm_main import _account_identity_block_reason
        good = _synthetic_account(server="broker-test", currency="usd")
        self.assertIsNone(_account_identity_block_reason(good, connector_config=_synthetic_pins()))
        cases = [
            (None, _synthetic_pins()),
            (_synthetic_account(login=124), _synthetic_pins()),
            (_synthetic_account(login="bad"), _synthetic_pins()),
            (_synthetic_account(server="Other"), _synthetic_pins()),
            (_synthetic_account(currency="EUR"), _synthetic_pins()),
            (good, None),
            (good, _synthetic_pins(expected_login="bad")),
            (good, _synthetic_pins(expected_login=MagicMock())),
            (good, _synthetic_pins(expected_server="")),
            (good, _synthetic_pins(expected_server=Mock())),
            (good, _synthetic_pins(expected_currency="")),
            (good, _synthetic_pins(expected_currency=Mock())),
        ]
        for account, pins in cases:
            with self.subTest(account=account, pins=pins):
                self.assertIsNotNone(_account_identity_block_reason(account, connector_config=pins))
        for baseline in (_synthetic_account(login=124), _synthetic_account(server="Other"),
                         _synthetic_account(currency="EUR")):
            with self.subTest(baseline=baseline):
                self.assertIn("changed during decision", _account_identity_block_reason(
                    good, connector_config=_synthetic_pins(), baseline_account=baseline
                ))

    def test_management_identity_has_no_equity_or_permission_gate(self):
        for equity in (1.0, 0.0, -100.0):
            with self.subTest(equity=equity):
                account = _synthetic_account(equity=equity, trade_allowed=False, trade_expert=False)
                trader = self._identity_trader(account)
                fresh, reason = trader._fresh_management_account_validation()
                self.assertIs(fresh, account)
                self.assertIsNone(reason)
                entry, reason = trader._fresh_entry_account_validation(account)
                self.assertIsNone(entry)
                self.assertIsNotNone(reason)

    def test_entry_validation_retains_equity_permission_and_minimum_error_order(self):
        for account, expected in [
            (_synthetic_account(equity=0, trade_allowed=False), "non-positive"),
            (_synthetic_account(trade_allowed=False, trade_expert=False), "disallows trading"),
            (_synthetic_account(trade_expert=False), "disallows expert trading"),
            (_synthetic_account(equity=50), "below live minimum"),
            (_synthetic_account(currency="EUR"), "required USD"),
        ]:
            with self.subTest(expected=expected):
                trader = self._identity_trader(account)
                trader.mt5.config = None
                fresh, reason = trader._fresh_entry_account_validation(account)
                self.assertIsNone(fresh)
                self.assertIn(expected, reason)

    def _sweep_trader(self):
        from pm_main import LiveTrader
        from pm_position import PositionConfig
        mt5 = Mock(config=_synthetic_pins())
        mt5.is_connected.return_value = False
        mt5.get_positions.return_value = []
        mt5.get_account_info.return_value = _synthetic_account()
        pm = Mock(symbols=[])
        pm.get_validated_configs.return_value = {}
        pm.get_live_eligible_configs.return_value = {}
        trader = LiveTrader(mt5, pm, PositionConfig(), enable_trading=False)
        trader.logger = Mock()
        trader._run_margin_protection_cycle = Mock(return_value=False)
        trader._run_order_governance_cycle = Mock()
        trader._process_symbol = Mock()
        return trader

    def test_sweep_blocks_wrong_missing_and_failed_identity_before_management(self):
        for value in (_synthetic_account(login=124), None, RuntimeError("synthetic read error")):
            with self.subTest(value=value):
                trader = self._sweep_trader()
                if isinstance(value, Exception):
                    trader.mt5.get_account_info.side_effect = value
                else:
                    trader.mt5.get_account_info.return_value = value
                peak = trader._equity_peak
                self.assertFalse(trader.process_all_symbols())
                trader.mt5.get_account_info.assert_called_once()
                self.assertEqual(trader._equity_peak, peak)
                trader._run_margin_protection_cycle.assert_not_called()
                trader._run_order_governance_cycle.assert_not_called()
                trader._process_symbol.assert_not_called()

    def test_sweep_rejects_post_margin_account_switch(self):
        trader = self._sweep_trader()
        trader._run_margin_protection_cycle.return_value = True
        trader.mt5.get_account_info.side_effect = [_synthetic_account(), _synthetic_account(login=124)]
        self.assertFalse(trader.process_all_symbols())
        self.assertEqual(trader.mt5.get_account_info.call_count, 2)
        trader._run_order_governance_cycle.assert_not_called()
        trader._process_symbol.assert_not_called()

    def _governance_trader(self, decision):
        from pm_core import get_instrument_spec
        trader = self._identity_trader()
        trader.pipeline_config = SimpleNamespace(
            local_governance_live_mode="apply", regime_params_file="regime_params.json",
            live_bars_count=10, live_min_bars=3, use_spread=False,
        )
        trader._last_governance_bar_times = {}
        trader._get_order_governance_state = Mock(return_value={})
        trader._set_order_governance_state = Mock()
        trader._resolve_symbol_from_broker = Mock(return_value="EURUSD")
        trader._infer_position_timeframe = Mock(return_value="H1")
        trader._build_magic_lookup = Mock(return_value=({}, {111: "TREND"}))
        trader._resolve_exact_regime_config = Mock(return_value=(
            "H1", "TREND", SimpleNamespace(governance_policy={"selected_policy": "breakeven_1r"})
        ))
        trader._symbol_info_to_instrument_spec = Mock(return_value=get_instrument_spec("EURUSD"))
        trader.mt5.get_symbol_info.return_value = SimpleNamespace(
            point=0.0001, trade_stops_level=0, trade_freeze_level=0
        )
        trader.mt5.get_symbol_tick.return_value = None
        for method in (trader.mt5.close_position_partial, trader.mt5.close_position, trader.mt5.modify_position):
            method.return_value = SimpleNamespace(success=True)
        bars = _make_ohlcv(5).assign(ATR_14=0.0010, REGIME_LIVE="TREND")
        trader._get_live_bars = Mock(return_value=bars)
        position = SimpleNamespace(ticket=1, symbol="EURUSD", magic=111, type=0,
                                   time=bars.index[0], sl=1.099, tp=1.105, price_open=1.1)
        config = SimpleNamespace(has_regime_configs=lambda: True)
        return trader, {"EURUSD": config}, [position], bars

    def test_each_governance_mutation_requires_fresh_identity(self):
        for seam in ("partial", "close", "modify"):
            for value in (_synthetic_account(login=124), _synthetic_account(login="bad"),
                          None, RuntimeError("synthetic confirmation failed")):
                with self.subTest(seam=seam, value=value):
                    decision = SimpleNamespace(stop_loss=1.1 if seam == "modify" else None,
                        take_profit=None, tp_released=False,
                        partial_close_pct=50.0 if seam == "partial" else 0.0, exit_now=seam == "close")
                    trader, configs, positions, bars = self._governance_trader(decision)
                    if isinstance(value, Exception):
                        trader.mt5.get_account_info.side_effect = value
                    else:
                        trader.mt5.get_account_info.return_value = value
                    with patch("pm_main.FeatureComputer.compute_all", return_value=bars), \
                         patch("pm_main.evaluate_policy", return_value=decision):
                        trader._run_order_governance_cycle(configs, positions)
                    trader.mt5.get_account_info.assert_called_once()
                    trader.mt5.close_position_partial.assert_not_called()
                    trader.mt5.close_position.assert_not_called()
                    trader.mt5.modify_position.assert_not_called()
                    trader._set_order_governance_state.assert_not_called()
                    self.assertEqual(trader._last_governance_bar_times, {})

    def test_account_switch_between_partial_and_modify_blocks_second_action(self):
        decision = SimpleNamespace(stop_loss=1.1, take_profit=None, tp_released=False,
                                   partial_close_pct=50.0, exit_now=False)
        trader, configs, positions, bars = self._governance_trader(decision)
        trader.mt5.get_account_info.side_effect = [_synthetic_account(equity=0), _synthetic_account(login=124)]
        with patch("pm_main.FeatureComputer.compute_all", return_value=bars), \
             patch("pm_main.evaluate_policy", return_value=decision):
            trader._run_order_governance_cycle(configs, positions)
        self.assertEqual(trader.mt5.get_account_info.call_count, 2)
        trader.mt5.close_position_partial.assert_called_once()
        trader.mt5.modify_position.assert_not_called()
        self.assertTrue(trader._set_order_governance_state.call_args.args[1]["partial_taken"])
        self.assertNotIn("last_applied_at", trader._set_order_governance_state.call_args.args[1])

    def test_account_switch_between_partial_and_full_close_blocks_second_action(self):
        decision = SimpleNamespace(stop_loss=None, take_profit=None, tp_released=False,
                                   partial_close_pct=50.0, exit_now=True)
        trader, configs, positions, bars = self._governance_trader(decision)
        trader.mt5.get_account_info.side_effect = [_synthetic_account(), _synthetic_account(login=124)]
        with patch("pm_main.FeatureComputer.compute_all", return_value=bars), \
             patch("pm_main.evaluate_policy", return_value=decision):
            trader._run_order_governance_cycle(configs, positions)
        self.assertEqual(trader.mt5.get_account_info.call_count, 2)
        trader.mt5.close_position_partial.assert_called_once()
        trader.mt5.close_position.assert_not_called()
        trader.mt5.modify_position.assert_not_called()
        self.assertTrue(trader._set_order_governance_state.call_args.args[1]["partial_taken"])

    def test_demo_management_uses_authorised_identity_without_new_equity_gate(self):
        from pm_demo_readiness import AUTHORIZED_DEMO_ACCOUNT_IDENTITY
        identity = AUTHORIZED_DEMO_ACCOUNT_IDENTITY
        account = _synthetic_account(login=identity.login, server=identity.server,
                                     currency=identity.currency, equity=0)
        trader = self._identity_trader(account)
        trader._is_current_refit_demo = True
        trader.mt5.config = None
        fresh, reason = trader._fresh_management_account_validation()
        self.assertIs(fresh, account)
        self.assertIsNone(reason)
        trader.mt5.get_account_info.return_value = _synthetic_account(equity=0)
        self.assertIsNotNone(trader._fresh_management_account_validation()[1])

    def test_authorised_governance_actions_preserve_low_equity_management(self):
        for seam in ("partial", "close", "modify"):
            with self.subTest(seam=seam):
                decision = SimpleNamespace(stop_loss=1.1 if seam == "modify" else None,
                    take_profit=None, tp_released=False,
                    partial_close_pct=50.0 if seam == "partial" else 0.0, exit_now=seam == "close")
                trader, configs, positions, bars = self._governance_trader(decision)
                trader.mt5.get_account_info.return_value = _synthetic_account(equity=-1)
                with patch("pm_main.FeatureComputer.compute_all", return_value=bars), \
                     patch("pm_main.evaluate_policy", return_value=decision):
                    trader._run_order_governance_cycle(configs, positions)
                mutation = {"partial": trader.mt5.close_position_partial,
                            "close": trader.mt5.close_position, "modify": trader.mt5.modify_position}[seam]
                trader.mt5.get_account_info.assert_called_once()
                mutation.assert_called_once()

    def test_signal_close_requires_actual_identity_and_retains_paper_behavior(self):
        position = SimpleNamespace(symbol="EURUSD")
        for value in (_synthetic_account(login=124), None, RuntimeError("synthetic read error")):
            with self.subTest(value=value):
                trader = self._identity_trader()
                if isinstance(value, Exception):
                    trader.mt5.get_account_info.side_effect = value
                else:
                    trader.mt5.get_account_info.return_value = value
                trader._close_position_on_signal(position, None, None)
                trader.mt5.get_account_info.assert_called_once()
                trader.mt5.close_position.assert_not_called()
        trader = self._identity_trader(_synthetic_account(equity=-1))
        trader.mt5.close_position.return_value = SimpleNamespace(success=True)
        trader._close_position_on_signal(position, None, None)
        trader.mt5.get_account_info.assert_called_once()
        trader.mt5.close_position.assert_called_once()
        trader = self._identity_trader()
        trader.enable_trading = False
        trader._close_position_on_signal(position, None, None)
        trader.mt5.get_account_info.assert_not_called()
        trader.mt5.close_position.assert_not_called()


if __name__ == "__main__":
    unittest.main()
