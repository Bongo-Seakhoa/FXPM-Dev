"""Spread tier diagnostics on an invented CSV: cutoff first, profile rule for quantiles."""

import json
import math
from pathlib import Path
import sys
import tempfile
import unittest

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import spread_tier_diagnostics as diag  # noqa: E402


SPREADS = [0, 0, 1, 1, 2, 2, 3, 4, 5, 10, 20, 50]  # 12 pre-cutoff rows, skewed


class QuantileRuleTests(unittest.TestCase):
    def test_higher_order_statistic_matches_hand_count(self):
        values = np.array(SPREADS, dtype=float)
        # n=12: ceil(11*0.75)=9 -> sorted[9]=10 ; ceil(11*0.95)=11 -> sorted[11]=50
        self.assertEqual(diag.quantile_higher(values, 0.75), 10.0)
        self.assertEqual(diag.quantile_higher(values, 0.95), 50.0)
        with self.assertRaises(diag.SpreadDiagnosticsError):
            diag.quantile_higher(np.array([]), 0.5)

    def test_statistics_fields(self):
        stats = diag.spread_statistics(np.array(SPREADS, dtype=float))
        mean = sum(SPREADS) / len(SPREADS)
        sd = math.sqrt(sum((v - mean) ** 2 for v in SPREADS) / (len(SPREADS) - 1))
        self.assertAlmostEqual(stats["mean"], mean)
        self.assertAlmostEqual(stats["sd"], sd)
        self.assertAlmostEqual(stats["zero_share"], 2 / 12)
        base, stress = stats["tiers"]["P75"], stats["tiers"]["P95"]
        self.assertEqual((base["lane"], stress["lane"]), ("BASE", "STRESS"))
        self.assertAlmostEqual(base["tier_over_mean"], 10.0 / mean)
        self.assertAlmostEqual(stress["sd_distance"], (50.0 - mean) / sd)
        self.assertAlmostEqual(base["share_at_or_below"], 10 / 12)
        with self.assertRaises(diag.SpreadDiagnosticsError):
            diag.spread_statistics(np.array([1.0]))
        with self.assertRaises(diag.SpreadDiagnosticsError):
            diag.spread_statistics(np.array([1.0, -1.0]))


class CutoffAndProfileTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="tierdiag-")
        self.root = Path(self._tmp.name)
        cell = self.root / "runs" / "run-x" / "EURUSD" / "H1"
        cell.mkdir(parents=True)
        lines = ["time,Open,High,Low,Close,Volume,Spread,RealVolume"]
        for index, spread in enumerate(SPREADS + [999, 999]):  # two rows after the cutoff
            hour = 10 + index
            lines.append(f"2024-07-14 {hour:02d}:00:00+00:00,1,1,1,1,1,{spread},0")
        (cell / "bars.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.profile = self.root / "profile.json"
        self.profile.write_text(json.dumps({"cells": [{
            "symbol": "EURUSD", "timeframe": "H1", "status": "ACTIVE", "source_timeframe": "M5",
            "tiers": {"P75": {"spread_points": 9, "usability": "POSITIVE_QUANTILE", "share_at_or_below": 0.81},
                      "P95": {"spread_points": 16, "usability": "POSITIVE_QUANTILE", "share_at_or_below": 0.95}}}]}),
            encoding="utf-8")

    def tearDown(self):
        self._tmp.cleanup()

    def test_cutoff_is_applied_before_any_statistic(self):
        cutoff = "2024-07-14T21:00:00+00:00"  # keeps the 12 fixture rows, drops the two 999 rows
        payload = diag.diagnose(self.root, "run-x", [("EURUSD", "H1"), ("GBPUSD", "H1")], cutoff,
                                profile_path=self.profile, activation_id="act-1")
        self.assertEqual(payload["schema_version"], diag.SCHEMA_VERSION)
        self.assertEqual(len(payload["cells"]), 1)
        cell = payload["cells"][0]
        self.assertEqual(cell["rows"], 12)
        self.assertEqual(cell["rows_after_cutoff_excluded"], 2)
        self.assertEqual(cell["tiers"]["P95"]["spread_points"], 50.0)
        frozen = cell["frozen_profile"]
        self.assertEqual(frozen["P75"]["lane"], "BASE")
        self.assertAlmostEqual(frozen["P95"]["tier_over_bar_mean"], 16 / cell["mean"])
        self.assertEqual(payload["skipped"], [{"symbol": "GBPUSD", "timeframe": "H1",
                                               "reason": "BARS_MISSING:GBPUSD:H1"}])
        self.assertEqual(len(payload["tool_sha256"]), 64)
        with self.assertRaises(diag.SpreadDiagnosticsError):
            diag.diagnose(self.root, "run-x", [("EURUSD", "H1")], "2024-07-14 21:00:00")

    def test_cli_writes_once_and_refuses_overwrite(self):
        out = self.root / "out" / "diag.json"
        argv = ["--native-root", str(self.root), "--run-id", "run-x", "--cells", "EURUSD/H1",
                "--cutoff-utc", "2024-07-14T21:00:00+00:00", "--profile", str(self.profile),
                "--out", str(out)]
        self.assertEqual(diag.main(argv), 0)
        payload = json.loads(out.read_text(encoding="ascii"))
        self.assertEqual(payload["cells"][0]["symbol"], "EURUSD")
        with self.assertRaises(FileExistsError):
            diag.main(argv)
        with self.assertRaises(diag.SpreadDiagnosticsError):
            diag._parse_cells("EURUSD")


if __name__ == "__main__":
    unittest.main()
