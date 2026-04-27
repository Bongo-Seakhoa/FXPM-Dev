import json
import os
import shutil
import unittest
from datetime import datetime
import uuid
from unittest.mock import MagicMock, patch

import pandas as pd

from pm_dashboard.jobs import DataDownloadScheduler, HistoricalDataDownloader


class DashboardDataJobsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pm_root = os.path.join(os.getcwd(), f".tmp_dashboard_jobs_{uuid.uuid4().hex}")
        os.makedirs(self.pm_root, exist_ok=True)
        self.data_dir = os.path.join(self.pm_root, "data")
        os.makedirs(self.data_dir, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.pm_root, ignore_errors=True)

    def _write_m5(self, symbol: str = "EURUSD", bars: int = 288) -> str:
        idx = pd.date_range("2026-02-01 00:00:00", periods=bars, freq="5min")
        df = pd.DataFrame(
            {
                "Open": [1.1000 + (i * 0.0001) for i in range(bars)],
                "High": [1.1003 + (i * 0.0001) for i in range(bars)],
                "Low": [1.0997 + (i * 0.0001) for i in range(bars)],
                "Close": [1.1001 + (i * 0.0001) for i in range(bars)],
                "Volume": [1000 for _ in range(bars)],
            },
            index=idx,
        )
        df.index.name = "time"
        path = os.path.join(self.data_dir, f"{symbol}_M5.csv")
        df.to_csv(path)
        return path

    def test_load_historical_data_from_root_data_resamples(self) -> None:
        self._write_m5("EURUSD", bars=288)  # one day
        jobs = HistoricalDataDownloader(self.pm_root, mt5_connector=None)

        start = datetime(2026, 2, 1, 0, 0, 0)
        end = datetime(2026, 2, 1, 23, 59, 59)
        h1 = jobs.load_historical_data("EURUSD", "H1", start, end)

        self.assertIsNotNone(h1)
        self.assertGreater(len(h1), 0)
        self.assertIn("Open", h1.columns)
        self.assertIn("Close", h1.columns)

    def test_get_max_bars_from_root_config(self) -> None:
        cfg = {
            "pipeline": {
                "max_bars": 123456
            },
            "symbols": ["EURUSD"],
        }
        with open(os.path.join(self.pm_root, "config.json"), "w", encoding="utf-8") as handle:
            json.dump(cfg, handle)

        jobs = HistoricalDataDownloader(self.pm_root, mt5_connector=None)
        self.assertEqual(jobs.get_max_bars_from_config(), 123456)

    def test_root_config_cache_refreshes_on_file_change(self) -> None:
        first = {
            "symbols": ["EURUSD"],
            "pipeline": {"max_bars": 1111},
        }
        second = {
            "symbols": ["GBPUSD"],
            "pipeline": {"max_bars": 2222},
        }
        config_path = os.path.join(self.pm_root, "config.json")
        with open(config_path, "w", encoding="utf-8") as handle:
            json.dump(first, handle)

        jobs = HistoricalDataDownloader(self.pm_root, mt5_connector=None)
        self.assertEqual(jobs.get_symbols_from_config(), ["EURUSD"])
        self.assertEqual(jobs.get_max_bars_from_config(), 1111)

        with open(config_path, "w", encoding="utf-8") as handle:
            json.dump(second, handle)

        self.assertEqual(jobs.get_symbols_from_config(), ["GBPUSD"])
        self.assertEqual(jobs.get_max_bars_from_config(), 2222)

    def test_refresh_without_mt5_reports_failure(self) -> None:
        cfg = {
            "symbols": ["EURUSD", "GBPUSD"],
            "pipeline": {"max_bars": 1000},
        }
        with open(os.path.join(self.pm_root, "config.json"), "w", encoding="utf-8") as handle:
            json.dump(cfg, handle)

        jobs = HistoricalDataDownloader(self.pm_root, mt5_connector=None)
        result = jobs.refresh_all_m5_data()

        self.assertFalse(result["success"])
        self.assertEqual(result["symbols_total"], 2)
        self.assertEqual(result["updated"], 0)
        self.assertEqual(result["failed"], 2)

    def test_refresh_symbol_merges_and_atomically_replaces_csv(self) -> None:
        self._write_m5("EURUSD", bars=3)
        idx = pd.date_range("2026-02-01 00:10:00", periods=3, freq="5min")
        fresh = pd.DataFrame(
            {
                "Open": [1.2000, 1.2001, 1.2002],
                "High": [1.2003, 1.2004, 1.2005],
                "Low": [1.1997, 1.1998, 1.1999],
                "Close": [1.2001, 1.2002, 1.2003],
                "Volume": [2000, 2000, 2000],
            },
            index=idx,
        )
        fresh.index.name = "time"

        mt5 = MagicMock()
        mt5.is_connected.return_value = True
        mt5.find_broker_symbol.return_value = "EURUSD"
        mt5.get_bars.return_value = fresh

        with patch("pm_dashboard.jobs.MT5_AVAILABLE", True):
            jobs = HistoricalDataDownloader(self.pm_root, mt5_connector=mt5)
            ok = jobs.refresh_symbol_m5("EURUSD", max_bars=4)

        self.assertTrue(ok)
        path = os.path.join(self.data_dir, "EURUSD_M5.csv")
        merged = pd.read_csv(path, index_col=0, parse_dates=True)
        self.assertEqual(len(merged), 4)
        self.assertTrue(merged.index.is_unique)
        self.assertAlmostEqual(float(merged.iloc[-1]["Close"]), 1.2003, places=6)
        leftovers = [name for name in os.listdir(self.data_dir) if ".tmp." in name]
        self.assertEqual(leftovers, [])

    def test_load_historical_data_resolves_requested_symbol_to_broker_suffixed_local_file(self) -> None:
        self._write_m5("EURUSD.A", bars=288)
        jobs = HistoricalDataDownloader(self.pm_root, mt5_connector=None)

        start = datetime(2026, 2, 1, 0, 0, 0)
        end = datetime(2026, 2, 1, 23, 59, 59)
        frame = jobs.load_historical_data("EURUSD", "M5", start, end)

        self.assertIsNotNone(frame)
        self.assertGreater(len(frame), 0)

    def test_load_historical_data_rejects_unsupported_timeframe(self) -> None:
        self._write_m5("EURUSD", bars=288)
        jobs = HistoricalDataDownloader(self.pm_root, mt5_connector=None)

        with self.assertRaises(ValueError):
            jobs.load_historical_data("EURUSD", "M2", datetime(2026, 2, 1), datetime(2026, 2, 2))

    def test_scheduler_next_due_rolls_to_next_day_after_run(self) -> None:
        scheduler = DataDownloadScheduler(MagicMock(), run_time="00:30")
        scheduler._last_run_date = datetime(2026, 4, 2).date()

        due_at = scheduler._next_due_at(datetime(2026, 4, 2, 1, 0, 0))

        self.assertEqual(due_at, datetime(2026, 4, 3, 0, 30, 0))

    def test_scheduler_next_due_uses_same_day_target_before_run(self) -> None:
        scheduler = DataDownloadScheduler(MagicMock(), run_time="06:15")

        due_at = scheduler._next_due_at(datetime(2026, 4, 2, 5, 0, 0))

        self.assertEqual(due_at, datetime(2026, 4, 2, 6, 15, 0))


if __name__ == "__main__":
    unittest.main()
