import unittest

from tools.benchmark import is_improvement, pooled_summary, pooled_summary_by_asset_class, validate_like_for_like


def _record(mean_pf, row_pf=None, mean_return=10.0, median_return=None, mean_dd=5.0, median_dd=None):
    row_pf = mean_pf if row_pf is None else row_pf
    if median_return is None:
        median_return = mean_return
    if median_dd is None:
        median_dd = mean_dd
    return {
        "summary": {
            "mean_return_pct": mean_return,
            "median_return_pct": median_return,
            "mean_profit_factor": mean_pf,
            "median_profit_factor": mean_pf,
            "mean_max_dd_pct": mean_dd,
            "median_max_dd_pct": median_dd,
            "mean_ulcer_index": 2.0,
            "median_ulcer_index": 2.0,
            "mean_time_underwater_pct": 40.0,
            "median_time_underwater_pct": 40.0,
            "total_trades": 100,
            "mean_win_rate": 50.0,
            "median_win_rate": 50.0,
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

    def test_mean_only_outlier_gain_needs_review_when_median_disagrees(self):
        verdict = is_improvement(
            _record(1.1, mean_return=5.0, median_return=5.0),
            _record(1.1, mean_return=25.0, median_return=4.0),
        )

        self.assertEqual(verdict["verdict"], "NEEDS_REVIEW")
        self.assertGreater(verdict["deltas_pct"]["return"], 5.0)
        self.assertLess(verdict["deltas_pct"]["median_return"], 0.0)

    def test_pooled_summary_clamps_non_finite_profit_factor(self):
        summary = pooled_summary([
            {
                "symbol": "EURUSD",
                "return_pct": 2.0,
                "profit_factor": float("inf"),
                "max_dd_pct": 3.0,
                "ulcer_index": 1.0,
                "time_underwater_pct": 20.0,
                "trades": 10,
                "win_rate": 55.0,
            }
        ])

        self.assertEqual(summary["mean_profit_factor"], 10.0)
        self.assertEqual(summary["median_profit_factor"], 10.0)

    def test_compare_rejects_symbol_population_mismatch(self):
        base = {**_record(1.1), "spread_mode": "conservative_max", "symbols": ["EURUSD"]}
        cand = {**_record(1.2), "spread_mode": "conservative_max", "symbols": ["EURUSD", "GBPUSD"]}

        errors = validate_like_for_like(base, cand)

        self.assertTrue(any("requested symbol set mismatch" in err for err in errors))

    def test_compare_rejects_spread_mode_mismatch(self):
        base = {**_record(1.1), "spread_mode": "fixed_spec", "symbols": ["EURUSD"]}
        cand = {**_record(1.2), "spread_mode": "conservative_max", "symbols": ["EURUSD"]}

        errors = validate_like_for_like(base, cand)

        self.assertTrue(any("spread_mode mismatch" in err for err in errors))

    def test_compare_rejects_matching_partial_benchmarks(self):
        base = {**_record(1.1), "spread_mode": "conservative_max", "symbols": ["EURUSD", "GBPUSD"]}
        cand = {**_record(1.2), "spread_mode": "conservative_max", "symbols": ["EURUSD", "GBPUSD"]}

        errors = validate_like_for_like(base, cand)

        self.assertTrue(any("baseline benchmark incomplete" in err for err in errors))
        self.assertTrue(any("candidate benchmark incomplete" in err for err in errors))

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
