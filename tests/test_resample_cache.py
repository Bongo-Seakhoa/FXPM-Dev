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

    def test_cached_frames_are_returned_as_copies(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "EURUSD_M5.csv")
            start = datetime(2024, 1, 1, 0, 0, 0)
            self._write_csv(csv_path, start, 180)

            loader = DataLoader(tmp)
            original_h1_min = DataLoader.MIN_BARS.get("H1", 5000)
            original_m5_min = DataLoader.MIN_BARS.get("M5", 50000)
            DataLoader.MIN_BARS["H1"] = 1
            DataLoader.MIN_BARS["M5"] = 1
            try:
                m5_a = loader.get_data("EURUSD", "M5")
                h1_a = loader.get_data("EURUSD", "H1")
                self.assertIsNotNone(m5_a)
                self.assertIsNotNone(h1_a)

                m5_a["TMP_COL"] = 1
                h1_a["TMP_COL"] = 1

                m5_b = loader.get_data("EURUSD", "M5")
                h1_b = loader.get_data("EURUSD", "H1")
            finally:
                DataLoader.MIN_BARS["H1"] = original_h1_min
                DataLoader.MIN_BARS["M5"] = original_m5_min

            self.assertNotIn("TMP_COL", m5_b.columns)
            self.assertNotIn("TMP_COL", h1_b.columns)


if __name__ == "__main__":
    unittest.main()
