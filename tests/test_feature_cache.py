import unittest
import warnings
import pandas as pd

from pm_core import FeatureComputer
from pm_strategies import EMACrossoverStrategy


class FeatureCacheTests(unittest.TestCase):
    def setUp(self):
        FeatureComputer.clear_cache()

    def test_compute_all_cache(self):
        df = pd.DataFrame({
            "Open": [1, 2, 3, 4, 5],
            "High": [2, 3, 4, 5, 6],
            "Low": [0.5, 1.5, 2.5, 3.5, 4.5],
            "Close": [1, 2, 3, 4, 5],
            "Volume": [100, 100, 100, 100, 100],
        })
        f1 = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        f2 = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        self.assertTrue(f1.equals(f2))

    def test_compute_all_cache_returns_isolated_copy(self):
        df = pd.DataFrame({
            "Open": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "High": [2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            "Low": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5],
            "Close": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "Volume": [100] * 10,
        })
        f1 = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        f1["TMP_MUTATED"] = 1
        f2 = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        self.assertNotIn("TMP_MUTATED", f2.columns)

    def test_strategy_helpers_do_not_warn_on_slice_views(self):
        df = pd.DataFrame({
            "Open": list(range(1, 80)),
            "High": list(range(2, 81)),
            "Low": [x - 0.5 for x in range(1, 80)],
            "Close": list(range(1, 80)),
            "Volume": [100] * 79,
        })
        features = FeatureComputer.compute_all(df, symbol="TEST", timeframe="H1")
        sliced = features.iloc[:60]
        strategy = EMACrossoverStrategy()

        with warnings.catch_warnings():
            warnings.simplefilter("error", pd.errors.SettingWithCopyWarning)
            signals = strategy.generate_signals(sliced, "EURUSD")
        self.assertEqual(len(signals), len(sliced))


if __name__ == "__main__":
    unittest.main()
