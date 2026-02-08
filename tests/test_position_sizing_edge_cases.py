"""Test position sizing edge cases: volume=0, min_lot, max_lot boundaries."""
import unittest
from pm_core import InstrumentSpec
from pm_position import PositionCalculator, PositionConfig


class PositionSizingEdgeCaseTests(unittest.TestCase):

    def setUp(self):
        self.config = PositionConfig(risk_per_trade_pct=1.0)
        self.calc = PositionCalculator(self.config)

    def test_volume_below_min_lot_returns_zero(self):
        """When risk is too small for even min lot, volume should be 0."""
        spec = InstrumentSpec(
            symbol="EURUSD", pip_position=4, pip_value=10.0, min_lot=0.01,
            max_lot=100.0, volume_step=0.01,
        )
        # Very small equity + very large SL → volume rounds to 0
        result = self.calc.calculate_position_size(
            equity=10.0, sl_pips=500.0, spec=spec
        )
        self.assertEqual(result.volume, 0.0)

    def test_normal_sizing_returns_positive(self):
        """Standard sizing should return a positive volume."""
        spec = InstrumentSpec(
            symbol="EURUSD", pip_position=4, pip_value=10.0, min_lot=0.01,
            max_lot=100.0, volume_step=0.01,
        )
        result = self.calc.calculate_position_size(
            equity=10000.0, sl_pips=20.0, spec=spec
        )
        self.assertGreater(result.volume, 0.0)

    def test_max_lot_respected(self):
        """Volume should never exceed max_lot."""
        spec = InstrumentSpec(
            symbol="EURUSD", pip_position=4, pip_value=10.0, min_lot=0.01,
            max_lot=0.5, volume_step=0.01,
        )
        result = self.calc.calculate_position_size(
            equity=1_000_000.0, sl_pips=5.0, spec=spec
        )
        self.assertLessEqual(result.volume, 0.5)


if __name__ == "__main__":
    unittest.main()
