"""Test that regime warmup bars are correctly flagged."""
import unittest
import pandas as pd
import numpy as np
from pm_regime import MarketRegimeDetector, RegimeParams
from pm_regime_tuner import compute_regime_quality_metrics


class RegimeWarmupTests(unittest.TestCase):

    def _make_ohlcv(self, n=500):
        np.random.seed(42)
        close = 100 + np.cumsum(np.random.randn(n) * 0.5)
        return pd.DataFrame({
            'Open': close - 0.1,
            'High': close + abs(np.random.randn(n) * 0.3),
            'Low': close - abs(np.random.randn(n) * 0.3),
            'Close': close,
            'Volume': np.random.randint(100, 1000, n),
        }, index=pd.date_range('2024-01-01', periods=n, freq='h'))

    def test_warmup_bars_property(self):
        """warmup_bars should equal max(bb_squeeze_lookback, atr_lookback, 50)."""
        params = RegimeParams()
        detector = MarketRegimeDetector(params)
        expected = max(params.bb_squeeze_lookback, params.atr_lookback, 50)
        self.assertEqual(detector.warmup_bars, expected)

    def test_regime_warmup_column_exists(self):
        """Output should have REGIME_WARMUP boolean column."""
        detector = MarketRegimeDetector()
        df = self._make_ohlcv()
        result = detector.compute_regime_scores(df)
        self.assertIn('REGIME_WARMUP', result.columns)

    def test_warmup_bars_flagged_true(self):
        """First warmup_bars rows should have REGIME_WARMUP=True."""
        detector = MarketRegimeDetector()
        df = self._make_ohlcv()
        result = detector.compute_regime_scores(df)
        wb = detector.warmup_bars
        warmup_flags = result['REGIME_WARMUP'].iloc[:wb]
        self.assertTrue(warmup_flags.all(), f"Expected all True in first {wb} bars")

    def test_post_warmup_flagged_false(self):
        """Bars after warmup should have REGIME_WARMUP=False."""
        detector = MarketRegimeDetector()
        df = self._make_ohlcv()
        result = detector.compute_regime_scores(df)
        wb = detector.warmup_bars
        post_warmup = result['REGIME_WARMUP'].iloc[wb:]
        self.assertFalse(post_warmup.any(), "Expected all False after warmup")

    def test_tuner_quality_metrics_exclude_warmup_region(self):
        regime_series = pd.Series(["CHOP"] * 50 + ["TREND"] * 120)
        gap_series = pd.Series([0.0] * 50 + [0.2] * 120)
        price_series = pd.Series(np.linspace(100.0, 120.0, len(regime_series)))

        with_warmup = compute_regime_quality_metrics(
            regime_series,
            gap_series,
            price_series,
            warmup_bars=50,
        )
        without_warmup = compute_regime_quality_metrics(
            regime_series,
            gap_series,
            price_series,
            warmup_bars=0,
        )

        self.assertLess(with_warmup["total_changes"], without_warmup["total_changes"])
        self.assertGreater(without_warmup["total_changes"], 0)


if __name__ == "__main__":
    unittest.main()
