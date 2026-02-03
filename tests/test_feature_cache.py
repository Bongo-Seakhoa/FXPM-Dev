import unittest
import pandas as pd

from pm_core import FeatureComputer


class FeatureCacheTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
