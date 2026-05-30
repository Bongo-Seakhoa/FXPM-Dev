import unittest

from tools.benchmark import is_improvement, pooled_summary_by_asset_class


def _record(mean_pf, row_pf=None):
    row_pf = mean_pf if row_pf is None else row_pf
    return {
        "summary": {
            "mean_return_pct": 10.0,
            "mean_profit_factor": mean_pf,
            "mean_max_dd_pct": 5.0,
            "mean_ulcer_index": 2.0,
            "mean_time_underwater_pct": 40.0,
            "total_trades": 100,
            "mean_win_rate": 50.0,
        },
        "rows": [
            {
                "symbol": "EURUSD",
                "return_pct": 10.0,
                "profit_factor": row_pf,
                "max_dd_pct": 5.0,
                "ulcer_index": 2.0,
                "time_underwater_pct": 40.0,
            }
        ],
    }


class BenchmarkGateTests(unittest.TestCase):
    def test_tiny_pf_gain_from_zero_is_not_infinite_improvement(self):
        verdict = is_improvement(_record(0.0), _record(0.01))

        self.assertEqual(verdict["verdict"], "NO_CHANGE")
        self.assertEqual(verdict["deltas_pct"]["pf"], 1.0)
        self.assertAlmostEqual(verdict["absolute_deltas"]["pf"], 0.01)

    def test_material_absolute_pf_drop_is_regression(self):
        verdict = is_improvement(_record(1.0), _record(0.9))

        self.assertEqual(verdict["verdict"], "REGRESSION")
        self.assertAlmostEqual(verdict["absolute_deltas"]["pf"], -0.1)

    def test_pooled_summary_by_asset_class_keeps_classes_separate(self):
        rows = [
            {
                "symbol": "EURUSD",
                "asset_class": "fx",
                "return_pct": 2.0,
                "profit_factor": 1.1,
                "max_dd_pct": 3.0,
                "ulcer_index": 1.0,
                "time_underwater_pct": 20.0,
                "trades": 10,
                "win_rate": 55.0,
            },
            {
                "symbol": "DOGUSD",
                "asset_class": "crypto",
                "return_pct": -50.0,
                "profit_factor": 0.2,
                "max_dd_pct": 60.0,
                "ulcer_index": 40.0,
                "time_underwater_pct": 99.0,
                "trades": 20,
                "win_rate": 30.0,
            },
        ]

        summary = pooled_summary_by_asset_class(rows)

        self.assertEqual(set(summary), {"crypto", "fx"})
        self.assertEqual(summary["fx"]["n"], 1)
        self.assertEqual(summary["crypto"]["mean_return_pct"], -50.0)


if __name__ == "__main__":
    unittest.main()
