import unittest

import numpy as np
import pandas as pd

from pm_core import FeatureComputer
from pm_strategies import StrategyRegistry


class PipelinePerfSmokeTests(unittest.TestCase):
    def _make_data(self, n: int = 600) -> pd.DataFrame:
        np.random.seed(123)
        base = 100 + np.cumsum(np.random.randn(n) * 0.25)
        return pd.DataFrame(
            {
                "Open": base - 0.05,
                "High": base + np.abs(np.random.randn(n) * 0.2),
                "Low": base - np.abs(np.random.randn(n) * 0.2),
                "Close": base,
                "Volume": np.random.randint(100, 1000, n),
            },
            index=pd.date_range("2024-01-01", periods=n, freq="h"),
        )

    def test_lazy_feature_builder_preserves_signals_across_strategy_basket(self):
        df = self._make_data()
        full = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")

        strategies = [
            StrategyRegistry.get("AroonTrendStrategy", period=20, strength_level=70),
            StrategyRegistry.get("EMAPullbackContinuationStrategy", fast_period=13, slow_period=50),
            StrategyRegistry.get("KeltnerPullbackStrategy", kc_period=20, kc_mult=2.5, ema_slope_bars=5),
            StrategyRegistry.get("EMARibbonADXStrategy", ema_fast=8, ema_mid=21, ema_slow=50, adx_period=14),
        ]

        for strategy in strategies:
            with self.subTest(strategy=strategy.name):
                lazy = FeatureComputer.compute_for_strategy(df, strategy, symbol="TEST", timeframe="H1")
                signals_lazy = strategy.generate_signals(lazy.copy(), "TEST")
                signals_full = strategy.generate_signals(full.copy(), "TEST")

                self.assertTrue(signals_lazy.equals(signals_full))
                self.assertLess(len(lazy.columns), len(full.columns))

    def test_lazy_feature_builder_keeps_stop_features_available(self):
        df = self._make_data(n=120)
        strategy = StrategyRegistry.get("AroonTrendStrategy", period=14, strength_level=60)
        lazy = FeatureComputer.compute_for_strategy(df, strategy, symbol="TEST", timeframe="H1")

        self.assertIn("ATR_14", lazy.columns)
        self.assertNotIn("MACD", lazy.columns)
        self.assertNotIn("REGIME", lazy.columns)


if __name__ == "__main__":
    unittest.main()
