import unittest

from pm_core import InstrumentSpec
from pm_position import PositionCalculator, PositionConfig


class PositionSizingEdgeCaseTests(unittest.TestCase):
    def setUp(self):
        self.config = PositionConfig(risk_per_trade_pct=1.0)
        self.calc = PositionCalculator(self.config)

    def test_volume_below_min_lot_returns_zero(self):
        spec = InstrumentSpec(
            symbol="EURUSD",
            pip_position=4,
            pip_value=10.0,
            min_lot=0.01,
            max_lot=100.0,
            volume_step=0.01,
        )
        # Very small equity + very large SL => risk-sized volume below min lot.
        result = self.calc.calculate_position_size(equity=10.0, sl_pips=500.0, spec=spec)
        self.assertEqual(result.volume, 0.0)

    def test_normal_sizing_returns_positive(self):
        spec = InstrumentSpec(
            symbol="EURUSD",
            pip_position=4,
            pip_value=10.0,
            min_lot=0.01,
            max_lot=100.0,
            volume_step=0.01,
        )
        result = self.calc.calculate_position_size(equity=10000.0, sl_pips=20.0, spec=spec)
        self.assertGreater(result.volume, 0.0)

    def test_max_lot_respected(self):
        spec = InstrumentSpec(
            symbol="EURUSD",
            pip_position=4,
            pip_value=10.0,
            min_lot=0.01,
            max_lot=0.5,
            volume_step=0.01,
        )
        result = self.calc.calculate_position_size(equity=1_000_000.0, sl_pips=5.0, spec=spec)
        self.assertLessEqual(result.volume, 0.5)


if __name__ == "__main__":
    unittest.main()
