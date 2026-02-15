import unittest

import numpy as np
import pandas as pd

from pm_regime import MarketRegimeDetector, RegimeParams


class RegimeWarmupTests(unittest.TestCase):
    def _make_ohlcv(self, n=500):
        np.random.seed(42)
        close = 100 + np.cumsum(np.random.randn(n) * 0.5)
        return pd.DataFrame(
            {
                "Open": close - 0.1,
                "High": close + abs(np.random.randn(n) * 0.3),
                "Low": close - abs(np.random.randn(n) * 0.3),
                "Close": close,
                "Volume": np.random.randint(100, 1000, n),
            },
            index=pd.date_range("2024-01-01", periods=n, freq="h"),
        )

    def test_warmup_bars_property(self):
        params = RegimeParams()
        detector = MarketRegimeDetector(params)
        expected = max(params.bb_squeeze_lookback, params.atr_lookback, 50)
        self.assertEqual(detector.warmup_bars, expected)

    def test_regime_warmup_column_exists(self):
        detector = MarketRegimeDetector()
        result = detector.compute_regime_scores(self._make_ohlcv())
        self.assertIn("REGIME_WARMUP", result.columns)

    def test_warmup_bars_flagged_true(self):
        detector = MarketRegimeDetector()
        result = detector.compute_regime_scores(self._make_ohlcv())
        wb = detector.warmup_bars
        self.assertTrue(result["REGIME_WARMUP"].iloc[:wb].all())

    def test_post_warmup_flagged_false(self):
        detector = MarketRegimeDetector()
        result = detector.compute_regime_scores(self._make_ohlcv())
        wb = detector.warmup_bars
        self.assertFalse(result["REGIME_WARMUP"].iloc[wb:].any())


if __name__ == "__main__":
    unittest.main()
