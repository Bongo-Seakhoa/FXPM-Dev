import os
import shutil
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from pm_core import DataLoader


class DataLoaderContractTests(unittest.TestCase):
    def _make_local_tmpdir(self) -> Path:
        path = Path("artifact/fxpm_runtime/.tmp_pytest/test_data_loader_contract")
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _write_csv(self, path, rows):
        start = datetime(2024, 1, 1, 0, 0, 0)
        times = [start + timedelta(minutes=5 * i) for i in range(rows)]
        df = pd.DataFrame({
            "time": times,
            "open": [1.0 + i * 0.001 for i in range(rows)],
            "high": [1.1 + i * 0.001 for i in range(rows)],
            "low": [0.9 + i * 0.001 for i in range(rows)],
            "close": [1.0 + i * 0.001 for i in range(rows)],
            "volume": [100] * rows,
        })
        df.to_csv(path, index=False)

    def test_loader_does_not_fallback_to_symbol_wildcard(self):
        tmp = self._make_local_tmpdir()
        try:
            self._write_csv(os.path.join(tmp, "EURUSD_misc.csv"), 5)
            loader = DataLoader(tmp)
            self.assertIsNone(loader.load_symbol("EURUSD", "M5"))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_invalid_ohlc_rows_are_dropped(self):
        tmp = self._make_local_tmpdir()
        try:
            path = os.path.join(tmp, "EURUSD_M5.csv")
            df = pd.DataFrame({
                "time": [datetime(2024, 1, 1), datetime(2024, 1, 1, 0, 5)],
                "open": [1.0, 1.1],
                "high": [1.2, 1.0],  # second row invalid
                "low": [0.9, 1.2],
                "close": [1.1, 1.1],
                "volume": [100, 100],
            })
            df.to_csv(path, index=False)

            loader = DataLoader(tmp)
            loaded = loader.load_symbol("EURUSD", "M5")
            self.assertIsNotNone(loaded)
            self.assertEqual(len(loaded), 1)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
