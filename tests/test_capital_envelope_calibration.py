from __future__ import annotations

import math
import unittest

from tools.calibrate_capital_envelope import (
    derive_capital_envelope,
    exact_binary_trade_math,
    margin_level_from_utilization,
)


class CapitalEnvelopeCalibrationTests(unittest.TestCase):
    def test_funded_envelope_is_derived_from_explicit_stress_budgets(self) -> None:
        envelope = derive_capital_envelope()

        self.assertEqual(envelope.total_initial_stop_risk_ceiling_pct, 8.0)
        self.assertEqual(envelope.factor_group_initial_stop_risk_ceiling_pct, 2.0)
        self.assertEqual(envelope.same_symbol_initial_stop_risk_ceiling_pct, 1.0)
        self.assertEqual(envelope.full_size_position_capacity, 32)
        self.assertEqual(envelope.full_size_positions_per_factor, 8)
        self.assertAlmostEqual(
            envelope.total_initial_stop_risk_ceiling_pct
            * envelope.portfolio_gap_stress_multiple,
            envelope.portfolio_stressed_loss_budget_pct,
        )
        self.assertAlmostEqual(
            envelope.factor_group_initial_stop_risk_ceiling_pct
            * envelope.factor_gap_stress_multiple,
            envelope.factor_stressed_loss_budget_pct,
        )

    def test_margin_band_maps_to_mt5_margin_levels(self) -> None:
        self.assertAlmostEqual(margin_level_from_utilization(50.0), 200.0)
        self.assertAlmostEqual(margin_level_from_utilization(100.0 / 3.0), 300.0)

    def test_thin_edge_math_is_not_misrepresented(self) -> None:
        result = exact_binary_trade_math(0.35, 2.0)
        self.assertAlmostEqual(result["gross_expectancy_r"], 0.05)
        self.assertAlmostEqual(result["break_even_win_probability"], 1.0 / 3.0)
        self.assertAlmostEqual(result["binary_kelly_fraction"], 0.025)

    def test_invalid_margin_band_rejected(self) -> None:
        with self.assertRaises(ValueError):
            derive_capital_envelope(
                margin_utilization_floor_pct=60.0,
                margin_utilization_ceiling_pct=50.0,
            )

    def test_cap_rounds_down_to_trade_increment(self) -> None:
        envelope = derive_capital_envelope(
            portfolio_stressed_loss_budget_pct=20.0,
            portfolio_gap_stress_multiple=3.0,
        )
        self.assertTrue(math.isclose(envelope.total_initial_stop_risk_ceiling_pct, 6.5))


if __name__ == "__main__":
    unittest.main()
