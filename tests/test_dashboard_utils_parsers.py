import json
import os
import shutil
import unittest
import uuid

from pm_dashboard.parsers import normalize_record
from pm_dashboard.utils import DEFAULT_CONFIG, load_pm_configs, parse_timestamp


class DashboardUtilsParsersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pm_root = os.path.join(os.getcwd(), f".tmp_dashboard_utils_{uuid.uuid4().hex}")
        os.makedirs(self.pm_root, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.pm_root, ignore_errors=True)

    def test_parse_timestamp_supports_epoch_strings_and_utc_suffixes(self) -> None:
        epoch_ts = parse_timestamp("1739059200")
        utc_ts = parse_timestamp("2026-02-09 12:30:00 UTC")

        self.assertIsNotNone(epoch_ts)
        self.assertIsNotNone(utc_ts)
        self.assertEqual(epoch_ts.year, 2025)
        self.assertEqual(utc_ts.year, 2026)
        self.assertEqual(utc_ts.hour, 12)

    def test_load_pm_configs_returns_deep_copy(self) -> None:
        path = os.path.join(self.pm_root, "pm_configs.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({"EURUSD": {"timeframe": "M5"}}, handle)

        loaded = load_pm_configs(self.pm_root)
        loaded["EURUSD"]["timeframe"] = "H1"
        loaded_again = load_pm_configs(self.pm_root)

        self.assertEqual(loaded_again["EURUSD"]["timeframe"], "M5")

    def test_normalize_record_deep_copies_raw_and_position_context(self) -> None:
        record = {
            "symbol": "EURUSD",
            "timeframe": "M5",
            "regime": "TREND",
            "strategy": "MomentumBurstStrategy",
            "direction": "BUY",
            "entry_price": 1.1000,
            "stop_loss_price": 1.0950,
            "take_profit_price": 1.1100,
            "timestamp": "2026-02-09T00:00:00",
            "position_context": {"ticket": 123},
        }

        entry = normalize_record(record, "test.json", DEFAULT_CONFIG, {}, None)
        self.assertIsNotNone(entry)
        self.assertEqual(entry.position_context["ticket"], 123)

        record["position_context"]["ticket"] = 999
        record["entry_price"] = 1.2000

        self.assertEqual(entry.position_context["ticket"], 123)
        self.assertEqual(entry.raw["position_context"]["ticket"], 123)
        self.assertEqual(entry.entry_price, 1.1000)


if __name__ == "__main__":
    unittest.main()
