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
import tempfile
import datetime

import numpy as np
import pandas as pd

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _make_ohlcv(n: int = 300, start_price: float = 1.1000) -> pd.DataFrame:
    """Generate synthetic OHLCV data with realistic price action."""
    np.random.seed(42)
    dates = pd.date_range("2025-01-01", periods=n, freq="h")
    close = start_price + np.cumsum(np.random.randn(n) * 0.0005)
    high = close + np.abs(np.random.randn(n) * 0.0003)
    low = close - np.abs(np.random.randn(n) * 0.0003)
    opn = close + np.random.randn(n) * 0.0001
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
        self.tmp_dir = tempfile.mkdtemp()
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


if __name__ == "__main__":
    unittest.main()
