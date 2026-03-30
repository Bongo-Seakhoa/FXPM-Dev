import os
import shutil
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from pm_core import DataLoader


class ResampleCacheTests(unittest.TestCase):
    def _write_csv(self, path, start_time, rows):
        times = [start_time + timedelta(minutes=5 * i) for i in range(rows)]
        df = pd.DataFrame({
            "time": times,
            "open": [1.0 + i * 0.001 for i in range(rows)],
            "high": [1.1 + i * 0.001 for i in range(rows)],
            "low": [0.9 + i * 0.001 for i in range(rows)],
            "close": [1.0 + i * 0.001 for i in range(rows)],
            "volume": [100] * rows,
        })
        df.to_csv(path, index=False)

    def test_resample_cache_invalidates_on_source_change(self):
        data_dir = Path("artifact/fxpm_runtime/.tmp_pytest/test_resample_cache")
        shutil.rmtree(data_dir, ignore_errors=True)
        data_dir.mkdir(parents=True, exist_ok=True)
        try:
            csv_path = os.path.join(data_dir, "EURUSD_M5.csv")
            start = datetime(2024, 1, 1, 0, 0, 0)

            # Initial data
            self._write_csv(csv_path, start, 120)
            loader = DataLoader(data_dir)

            # Lower the MIN_BARS threshold for test speed
            original_min = DataLoader.MIN_BARS.get("H1", 5000)
            DataLoader.MIN_BARS["H1"] = 1
            try:
                h1_a = loader.get_data("EURUSD", "H1")
            finally:
                DataLoader.MIN_BARS["H1"] = original_min
            self.assertIsNotNone(h1_a)

            # Ensure cache file exists
            cache_dir = os.path.join(data_dir, ".cache")
            cache_file = os.path.join(cache_dir, "EURUSD_H1.pkl")
            self.assertTrue(os.path.exists(cache_file))

            # Modify source CSV (new bars)
            self._write_csv(csv_path, start, 180)

            # New loader should invalidate cache based on mtime/rows/last_index
            loader2 = DataLoader(data_dir)
            DataLoader.MIN_BARS["H1"] = 1
            try:
                h1_b = loader2.get_data("EURUSD", "H1")
            finally:
                DataLoader.MIN_BARS["H1"] = original_min
            self.assertIsNotNone(h1_b)
            self.assertNotEqual(len(h1_a), len(h1_b))
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
