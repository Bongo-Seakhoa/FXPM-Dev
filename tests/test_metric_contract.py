import math
import unittest

from pm_core import Backtester, PipelineConfig
from pm_dashboard.analytics import compute_performance_metrics
from pm_metrics import (
    CALMAR_LEGACY_BASIS,
    METRIC_CONTRACT_VERSION,
    profit_factor_from_gross,
    return_drawdown_metrics,
)


class MetricContractTests(unittest.TestCase):
    def test_profit_factor_no_loss_sentinel_is_shared(self):
        self.assertTrue(math.isinf(profit_factor_from_gross(12.5, 0.0)))
        self.assertEqual(profit_factor_from_gross(0.0, 0.0), 0.0)

    def test_return_drawdown_metrics_expose_legacy_basis_and_annualized_calmar(self):
        metrics = return_drawdown_metrics(
            initial_equity=10000.0,
            final_equity=11000.0,
            max_drawdown_pct=5.0,
            periods=252,
            periods_per_year=252.0,
        )

        self.assertAlmostEqual(metrics["return_drawdown_ratio"], 2.0, places=6)
        self.assertAlmostEqual(metrics["calmar_ratio"], metrics["return_drawdown_ratio"], places=6)
        self.assertAlmostEqual(metrics["calmar_ratio_annualized"], 2.0, places=6)
        self.assertEqual(metrics["calmar_ratio_basis"], CALMAR_LEGACY_BASIS)
        self.assertEqual(metrics["metric_contract_version"], METRIC_CONTRACT_VERSION)

    def test_dashboard_metrics_emit_contract_fields(self):
        metrics = compute_performance_metrics(
            [
                {"timestamp": "2026-02-09T03:00:00", "status": "CLOSED", "pnl": 10.0, "direction": "LONG"},
                {"timestamp": "2026-02-09T04:00:00", "status": "CLOSED", "pnl": 5.0, "direction": "LONG"},
            ],
            initial_capital=10000.0,
        )

        self.assertTrue(math.isinf(metrics["profit_factor"]))
        self.assertTrue(math.isinf(metrics["long_profit_factor"]))
        self.assertEqual(metrics["calmar_ratio_basis"], CALMAR_LEGACY_BASIS)
        self.assertEqual(metrics["metric_contract_version"], METRIC_CONTRACT_VERSION)
        self.assertIn("calmar_ratio_annualized", metrics)

    def test_backtester_empty_result_emits_contract_fields(self):
        result = Backtester(PipelineConfig())._empty_result()

        self.assertEqual(result["calmar_ratio_basis"], CALMAR_LEGACY_BASIS)
        self.assertEqual(result["metric_contract_version"], METRIC_CONTRACT_VERSION)
        self.assertIn("return_drawdown_ratio", result)


if __name__ == "__main__":
    unittest.main()
