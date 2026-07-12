import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace

from pm_core import PipelineConfig
from pm_main import LiveTrader


def _trader(**overrides):
    trader = LiveTrader.__new__(LiveTrader)  # no full init; helper only reads pipeline_config
    trader.pipeline_config = PipelineConfig(**overrides)
    return trader


class ProbationMultiplierTests(unittest.TestCase):
    def test_fresh_cell_is_derated(self):
        trader = _trader(probation_enabled=True, probation_days=14, probation_risk_multiplier=0.5)
        rc = SimpleNamespace(trained_at=datetime.now() - timedelta(days=1))
        mult, on_probation = trader._probation_multiplier(rc, SimpleNamespace(optimized_at=None))
        self.assertTrue(on_probation)
        self.assertEqual(mult, 0.5)

    def test_seasoned_cell_without_forward_evidence_stays_derated(self):
        trader = _trader(probation_enabled=True, probation_days=14, probation_risk_multiplier=0.5)
        rc = SimpleNamespace(trained_at=datetime.now() - timedelta(days=30), strategy_name="Demo")
        mult, on_probation = trader._probation_multiplier(rc, SimpleNamespace(optimized_at=None))
        self.assertTrue(on_probation)
        self.assertEqual(mult, 0.5)

    def test_disabled_passthrough(self):
        trader = _trader(probation_enabled=False)
        rc = SimpleNamespace(trained_at=datetime.now())
        mult, on_probation = trader._probation_multiplier(rc, SimpleNamespace(optimized_at=None))
        self.assertEqual((mult, on_probation), (1.0, False))

    def test_missing_timestamps_remain_derated(self):
        trader = _trader(probation_enabled=True, probation_days=14)
        mult, on_probation = trader._probation_multiplier(
            SimpleNamespace(trained_at=None), SimpleNamespace(optimized_at=None))
        self.assertEqual((mult, on_probation), (0.5, True))

    def test_seasoned_cell_graduates_only_with_exact_positive_forward_book(self):
        trader = _trader(
            probation_enabled=True,
            probation_days=14,
            probation_min_live_outcomes=30,
        )
        trader._live_outcome_ledger = SimpleNamespace(
            exact_cell_summary=lambda *_args: {
                "exact_closed_positions": 30,
                "r_observations": 30,
                "mean_r": 0.10,
                "profit_factor_r": 1.20,
            }
        )
        rc = SimpleNamespace(
            trained_at=datetime.now() - timedelta(days=30),
            strategy_name="Demo",
        )
        self.assertEqual(
            trader._probation_multiplier(
                rc,
                SimpleNamespace(optimized_at=None),
                symbol="EURUSD",
                timeframe="H1",
                regime="TREND",
            ),
            (1.0, False),
        )

    def test_symbol_level_optimized_at_fallback_and_iso_string(self):
        trader = _trader(probation_enabled=True, probation_days=14, probation_risk_multiplier=0.4)
        stamp = (datetime.now() - timedelta(days=2)).isoformat()
        mult, on_probation = trader._probation_multiplier(
            SimpleNamespace(trained_at=None), SimpleNamespace(optimized_at=stamp))
        self.assertTrue(on_probation)
        self.assertAlmostEqual(mult, 0.4)

    def test_multiplier_clamped_conservative_only(self):
        trader = _trader(probation_enabled=True, probation_days=14, probation_risk_multiplier=1.7)
        rc = SimpleNamespace(trained_at=datetime.now())
        mult, _ = trader._probation_multiplier(rc, SimpleNamespace(optimized_at=None))
        self.assertLessEqual(mult, 1.0)


if __name__ == "__main__":
    unittest.main()
