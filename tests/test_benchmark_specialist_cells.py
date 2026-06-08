import unittest

from tools.benchmark_specialist_cells import (
    classify_ledger_cell,
    compare_cell,
    compare_records,
    promotion_safety_summary,
)


class SpecialistCellBenchmarkTests(unittest.TestCase):
    def _cell(self, **overrides):
        cell = {
            "strategy_name": "ExampleStrategy",
            "parameters": {"period": 10},
            "promotion_status": "PROMOTE",
            "train_metrics": {"total_trades": 30, "total_return_pct": 5.0, "profit_factor": 1.2},
            "val_metrics": {
                "total_trades": 20,
                "total_return_pct": 4.0,
                "profit_factor": 1.25,
                "max_drawdown_pct": 6.0,
            },
            "holdout_metrics": {
                "total_trades": 6,
                "total_return_pct": 1.0,
                "profit_factor": 1.1,
                "max_drawdown_pct": 4.0,
            },
            "historical_audit_metrics": {
                "severe_failure": False,
                "metrics": {
                    "total_trades": 8,
                    "total_return_pct": 2.0,
                    "profit_factor": 1.2,
                    "max_drawdown_pct": 7.0,
                },
            },
            "walk_forward_audit": {
                "window_count": 2,
                "summary": {
                    "positive_windows": 2,
                    "avg_profit_factor": 1.1,
                    "avg_total_return_pct": 1.5,
                },
            },
            "validation_evidence": {
                "selection_score_raw": 12.0,
                "validation_reason": "validated",
            },
        }
        cell.update(overrides)
        return cell

    def test_cell_classifier_uses_specialist_ledger_evidence(self):
        row = classify_ledger_cell("EURUSD", "H1", "TREND", self._cell())

        self.assertEqual(row["cell_key"], "EURUSD|H1|TREND")
        self.assertEqual(row["status"], "PASS_STRICT")
        self.assertEqual(row["historical_profit_factor"], 1.2)

    def test_cell_classifier_fails_one_specialist_without_offset(self):
        bad = self._cell(
            historical_audit_metrics={
                "severe_failure": True,
                "metrics": {
                    "total_trades": 12,
                    "total_return_pct": -3.0,
                    "profit_factor": 0.8,
                    "max_drawdown_pct": 9.0,
                },
            }
        )
        row = classify_ledger_cell("EURUSD", "H1", "TREND", bad)

        self.assertEqual(row["status"], "FAIL")
        self.assertIn("historical_audit.severe_failure", row["reasons"])

    def test_compare_flags_new_bad_carrier_per_cell(self):
        base = classify_ledger_cell(
            "EURUSD",
            "H1",
            "TREND",
            {"strategy_name": "NO_TRADE", "validation_evidence": {"no_trade": True}},
        )
        cand = classify_ledger_cell(
            "EURUSD",
            "H1",
            "TREND",
            self._cell(
                historical_audit_metrics={
                    "severe_failure": False,
                    "metrics": {
                        "total_trades": 12,
                        "total_return_pct": -1.0,
                        "profit_factor": 0.9,
                        "max_drawdown_pct": 4.0,
                    },
                }
            ),
        )

        comparison = compare_cell(base, cand)

        self.assertEqual(comparison["verdict"], "NEW_BAD_CARRIER")

    def test_compare_flags_failed_to_good_as_gain(self):
        base = classify_ledger_cell(
            "EURUSD",
            "H1",
            "TREND",
            self._cell(
                historical_audit_metrics={
                    "severe_failure": False,
                    "metrics": {
                        "total_trades": 12,
                        "total_return_pct": -1.0,
                        "profit_factor": 0.9,
                        "max_drawdown_pct": 4.0,
                    },
                }
            ),
        )
        cand = classify_ledger_cell("EURUSD", "H1", "TREND", self._cell())

        comparison = compare_cell(base, cand)

        self.assertEqual(comparison["verdict"], "GAINED_GOOD_CARRIER")

    def test_compare_taxonomy_covers_no_trade_and_dropped_bad_carrier(self):
        bad = classify_ledger_cell(
            "GBPUSD",
            "H1",
            "CHOP",
            self._cell(
                historical_audit_metrics={
                    "severe_failure": False,
                    "metrics": {
                        "total_trades": 20,
                        "total_return_pct": -50.0,
                        "profit_factor": 0.55,
                        "max_drawdown_pct": 45.0,
                    },
                }
            ),
        )
        no_trade = classify_ledger_cell(
            "GBPUSD",
            "H1",
            "CHOP",
            {"strategy_name": "NO_TRADE", "validation_evidence": {"no_trade": True}},
        )

        self.assertEqual(compare_cell(no_trade, no_trade)["verdict"], "NO_TRADE_BOTH")
        self.assertEqual(compare_cell(bad, no_trade)["verdict"], "DROPPED_BAD_CARRIER")
        self.assertEqual(compare_cell(bad, bad)["verdict"], "RETAINED_BAD_CARRIER")

    def test_compare_distinguishes_same_and_replaced_good_carriers(self):
        base = classify_ledger_cell("EURUSD", "H4", "RANGE", self._cell())
        same = classify_ledger_cell("EURUSD", "H4", "RANGE", self._cell())
        replaced = classify_ledger_cell(
            "EURUSD",
            "H4",
            "RANGE",
            self._cell(strategy_name="OtherStrategy", parameters={"period": 30}),
        )

        self.assertEqual(compare_cell(base, same)["verdict"], "RETAINED_SAME_GOOD_CARRIER")
        self.assertEqual(compare_cell(base, replaced)["verdict"], "REPLACED_WITH_GOOD_CARRIER")

    def test_no_aggregate_pass_bad_specialist_is_always_a_blocker(self):
        base_rows = [
            classify_ledger_cell(
                "XAUUSD",
                "H1",
                "TREND",
                {"strategy_name": "NO_TRADE", "validation_evidence": {"no_trade": True}},
            ),
            classify_ledger_cell(
                "GBPUSD",
                "H1",
                "CHOP",
                {"strategy_name": "NO_TRADE", "validation_evidence": {"no_trade": True}},
            ),
        ]
        candidate_rows = [
            classify_ledger_cell(
                "XAUUSD",
                "H1",
                "TREND",
                self._cell(
                    historical_audit_metrics={
                        "severe_failure": False,
                        "metrics": {
                            "total_trades": 1446,
                            "total_return_pct": 460.0,
                            "profit_factor": 2.2,
                            "max_drawdown_pct": 12.0,
                        },
                    }
                ),
            ),
            classify_ledger_cell(
                "GBPUSD",
                "H1",
                "CHOP",
                self._cell(
                    historical_audit_metrics={
                        "severe_failure": False,
                        "metrics": {
                            "total_trades": 1132,
                            "total_return_pct": -50.0,
                            "profit_factor": 0.55,
                            "max_drawdown_pct": 45.0,
                        },
                    }
                ),
            ),
        ]

        comparison = compare_records(
            {
                "label": "baseline",
                "decision_scope": "symbol_timeframe_regime_specialist_only",
                "aggregate_decision_allowed": False,
                "rows": base_rows,
            },
            {
                "label": "candidate",
                "decision_scope": "symbol_timeframe_regime_specialist_only",
                "aggregate_decision_allowed": False,
                "rows": candidate_rows,
            },
        )

        self.assertFalse(comparison["aggregate_decision_allowed"])
        self.assertEqual(comparison["navigation_counts"]["GAINED_GOOD_CARRIER"], 1)
        self.assertEqual(comparison["navigation_counts"]["NEW_BAD_CARRIER"], 1)
        self.assertEqual(comparison["navigation_counts"]["blockers"], 1)
        self.assertEqual(comparison["blockers"][0]["cell_key"], "GBPUSD|H1|CHOP")
        self.assertNotIn("mean_return_pct", comparison)
        self.assertNotIn("median_return_pct", comparison)
        self.assertNotIn("accepted", comparison)

    def test_promotion_safety_flags_promoted_bad_specialist(self):
        bad = classify_ledger_cell(
            "CHFJPY",
            "H4",
            "CHOP",
            self._cell(
                promotion_status="BLOCKED",
                promotion_reasons=["profitability failed"],
                historical_audit_metrics={
                    "severe_failure": False,
                    "metrics": {
                        "total_trades": 20,
                        "total_return_pct": -0.32,
                        "profit_factor": 0.92,
                        "max_drawdown_pct": 3.0,
                    },
                },
            ),
        )
        good = classify_ledger_cell("AUDUSD", "H4", "CHOP", self._cell())
        summary = promotion_safety_summary(
            {
                "label": "cells",
                "decision_scope": "symbol_timeframe_regime_specialist_only",
                "aggregate_decision_allowed": False,
                "rows": [bad, good],
            },
            {
                "counts": {"PROMOTE": 2},
                "rows": [
                    {
                        "symbol": "CHFJPY",
                        "timeframe": "H4",
                        "regime": "CHOP",
                        "strategy_name": "BadStrategy",
                        "final_verdict": "PROMOTE",
                    },
                    {
                        "symbol": "AUDUSD",
                        "timeframe": "H4",
                        "regime": "CHOP",
                        "strategy_name": "GoodStrategy",
                        "final_verdict": "PROMOTE",
                    },
                ],
            },
        )

        self.assertFalse(summary["aggregate_decision_allowed"])
        self.assertEqual(summary["navigation_counts"]["final_promotes"], 2)
        self.assertEqual(summary["navigation_counts"]["unsafe_promotes"], 1)
        self.assertEqual(summary["unsafe_promotes"][0]["cell_key"], "CHFJPY|H4|CHOP")
        self.assertEqual(summary["navigation_counts"]["fail_not_reject"], 1)

    def test_promotion_safety_accepts_clean_promote(self):
        good = classify_ledger_cell("AUDUSD", "H4", "CHOP", self._cell())
        summary = promotion_safety_summary(
            {
                "label": "cells",
                "decision_scope": "symbol_timeframe_regime_specialist_only",
                "aggregate_decision_allowed": False,
                "rows": [good],
            },
            {
                "counts": {"PROMOTE": 1},
                "rows": [
                    {
                        "symbol": "AUDUSD",
                        "timeframe": "H4",
                        "regime": "CHOP",
                        "strategy_name": "GoodStrategy",
                        "final_verdict": "PROMOTE",
                    },
                ],
            },
        )

        self.assertEqual(summary["navigation_counts"]["unsafe_promotes"], 0)
        self.assertEqual(summary["navigation_counts"]["fail_not_reject"], 0)
        self.assertEqual(summary["final_promotes"][0]["cell_key"], "AUDUSD|H4|CHOP")


if __name__ == "__main__":
    unittest.main()
