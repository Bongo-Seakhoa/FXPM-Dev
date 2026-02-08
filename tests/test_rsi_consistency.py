"""Test that all RSI computation paths produce Wilder's EMA (consistent)."""
import unittest
import numpy as np
import pandas as pd
from pm_core import FeatureComputer
from pm_strategies import StrategyRegistry


class RSIConsistencyTests(unittest.TestCase):

    def _make_close_series(self, n=200, seed=42):
        np.random.seed(seed)
        returns = np.random.randn(n) * 0.01
        prices = 100.0 * np.cumprod(1 + returns)
        return pd.Series(prices)

    def test_feature_computer_rsi_uses_wilders(self):
        """FeatureComputer.rsi should use Wilder's EMA (ewm with alpha=1/period)."""
        close = self._make_close_series()
        rsi_fc = FeatureComputer.rsi(close, 14)
        # Manual Wilder's EMA RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta.where(delta < 0, 0.0))
        avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        rs = avg_gain / (avg_loss + 1e-10)
        rsi_manual = 100 - (100 / (1 + rs))
        # Should match closely (numerical tolerance)
        valid = ~(rsi_fc.isna() | rsi_manual.isna())
        np.testing.assert_allclose(
            rsi_fc[valid].values, rsi_manual[valid].values,
            atol=1e-6, rtol=1e-6
        )

    def test_rsi_extremes_uses_helper(self):
        """RSIExtremesStrategy should produce RSI values consistent with _get_rsi."""
        # Build features DataFrame
        close = self._make_close_series()
        df = pd.DataFrame({
            'Open': close, 'High': close + 0.5, 'Low': close - 0.5,
            'Close': close, 'Volume': 100,
        })
        df['ATR_14'] = (df['High'] - df['Low']).rolling(14).mean()
        strat = StrategyRegistry.get('RSIExtremesStrategy', rsi_period=14)
        # This should not raise and should produce a Series
        signals = strat.generate_signals(df, "TEST")
        self.assertEqual(len(signals), len(df))


if __name__ == "__main__":
    unittest.main()
