import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

from promotion_replay_audit import apply_audit, evaluate_gates


GATES = dict(min_pf=1.05, max_dd_pct=30.0, min_trades=30)


class EvaluateGatesTests(unittest.TestCase):
    def test_pass_row(self):
        verdict, reasons = evaluate_gates(
            {"profit_factor": "1.53", "max_drawdown_pct": "4.8", "total_trades": "162", "error": ""},
            **GATES)
        self.assertEqual(verdict, "PASS")
        self.assertEqual(reasons, [])

    def test_fail_each_gate(self):
        verdict, reasons = evaluate_gates(
            {"profit_factor": "0.66", "max_drawdown_pct": "70.3", "total_trades": "20", "error": ""},
            **GATES)
        self.assertEqual(verdict, "FAIL")
        self.assertEqual(len(reasons), 3)

    def test_inf_pf_passes_pf_gate(self):
        verdict, _ = evaluate_gates(
            {"profit_factor": "inf", "max_drawdown_pct": "5.0", "total_trades": "40", "error": ""},
            **GATES)
        self.assertEqual(verdict, "PASS")

    def test_replay_error_is_unverified(self):
        verdict, reasons = evaluate_gates({"error": "no_data"}, **GATES)
        self.assertEqual(verdict, "UNVERIFIED")
        self.assertIn("replay_error:no_data", reasons)


def _result(symbol, tf, regime, strategy, verdict, reasons=None):
    return {
        "cell": {"symbol": symbol, "timeframe": tf, "regime": regime, "strategy_name": strategy},
        "verdict": verdict,
        "reasons": reasons or [],
        "evidence": {"verdict": verdict, "schema_version": "test"},
    }


def _ledger_entry(symbol_field, tf, regime, strategy, status="PROMOTED"):
    return {
        "symbol": symbol_field,
        "timeframe": tf,
        "strategy_name": strategy,
        "promotion_status": status,
        "promotion_reasons": [],
        "regime_configs": {
            tf: {regime: {"strategy_name": strategy, "promotion_status": status}}
        },
    }


class ApplyAuditTests(unittest.TestCase):
    def test_fail_blocks_symbol_and_leaf(self):
        ledger = {"FORD": _ledger_entry("FORD", "H4", "CHOP", "DoubleEMATrendStrategy")}
        counters = apply_audit(
            ledger, [_result("FORD", "H4", "CHOP", "DoubleEMATrendStrategy", "FAIL", ["pf"])])
        self.assertEqual(ledger["FORD"]["promotion_status"], "BLOCKED")
        leaf = ledger["FORD"]["regime_configs"]["H4"]["CHOP"]
        self.assertEqual(leaf["promotion_status"], "BLOCKED")
        self.assertEqual(counters["blocked_symbol"], 1)
        self.assertEqual(counters["blocked_leaf"], 1)
        self.assertTrue(any("replay_audit" in r for r in ledger["FORD"]["promotion_reasons"]))

    def test_pass_annotates_without_status_change(self):
        ledger = {"XAUUSD": _ledger_entry("XAUUSD", "H4", "TREND", "EMAPullbackContinuationStrategy")}
        apply_audit(
            ledger, [_result("XAUUSD", "H4", "TREND", "EMAPullbackContinuationStrategy", "PASS")])
        self.assertEqual(ledger["XAUUSD"]["promotion_status"], "PROMOTED")
        self.assertEqual(ledger["XAUUSD"]["replay_audit"]["verdict"], "PASS")

    def test_case_insensitive_ledger_key(self):
        # Regression: key 'Palladium' vs enumerated symbol 'PALLADIUM' must still block.
        ledger = {"Palladium": _ledger_entry("Palladium", "H4", "TREND", "StochasticReversalStrategy",
                                             status="PROMOTE_SMALL")}
        counters = apply_audit(
            ledger, [_result("PALLADIUM", "H4", "TREND", "StochasticReversalStrategy", "FAIL", ["dd"])])
        self.assertEqual(ledger["Palladium"]["promotion_status"], "BLOCKED")
        self.assertEqual(
            ledger["Palladium"]["regime_configs"]["H4"]["TREND"]["promotion_status"], "BLOCKED")
        self.assertEqual(counters["blocked_symbol"] + counters["blocked_leaf"], 2)

    def test_unverified_not_blocked_by_default(self):
        ledger = {"EURUSD": _ledger_entry("EURUSD", "D1", "CHOP", "VWAPTrendContinuationStrategy")}
        counters = apply_audit(
            ledger, [_result("EURUSD", "D1", "CHOP", "VWAPTrendContinuationStrategy",
                             "UNVERIFIED", ["replay_error:no_data"])])
        self.assertEqual(ledger["EURUSD"]["promotion_status"], "PROMOTED")
        self.assertEqual(counters["unverified"], 1)


if __name__ == "__main__":
    unittest.main()
