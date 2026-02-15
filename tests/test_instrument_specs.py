import tempfile
import unittest
from dataclasses import dataclass

from pm_core import (
    InstrumentSpec,
    _create_spec_from_broker_data,
    get_instrument_spec,
    set_broker_specs_path,
    set_instrument_specs,
    sync_instrument_spec_from_mt5,
)


class InstrumentSpecTests(unittest.TestCase):
    def setUp(self):
        # Ensure broker specs path points to a temp location (no file)
        tmp = tempfile.TemporaryDirectory()
        self._tmpdir = tmp
        set_broker_specs_path(tmp.name + "/no_broker_specs.json")

    def tearDown(self):
        self._tmpdir.cleanup()
        set_instrument_specs({}, {})

    def test_config_override_and_defaults(self):
        defaults = {"commission_per_lot": 12.0}
        specs = {
            "AAA": {"pip_position": 4, "pip_value": 10.0, "spread_avg": 1.5, "min_lot": 0.01, "max_lot": 100.0},
            "BBB": {"inherit": "AAA", "spread_avg": 2.0},
        }
        set_instrument_specs(specs=specs, defaults=defaults)

        a = get_instrument_spec("AAA")
        b = get_instrument_spec("BBB")

        self.assertEqual(a.pip_position, 4)
        self.assertEqual(a.pip_value, 10.0)
        self.assertEqual(a.spread_avg, 1.5)
        self.assertEqual(a.commission_per_lot, 12.0)

        # Inherit should copy AAA and override spread_avg
        self.assertEqual(b.pip_position, 4)
        self.assertEqual(b.pip_value, 10.0)
        self.assertEqual(b.spread_avg, 2.0)
        self.assertEqual(b.commission_per_lot, 12.0)

    def test_symbol_suffix_normalization(self):
        specs = {
            "EURUSD": {"pip_position": 4, "pip_value": 10.0, "spread_avg": 1.2, "min_lot": 0.01, "max_lot": 100.0},
        }
        set_instrument_specs(specs=specs, defaults={})
        spec = get_instrument_spec("EURUSD.a")
        self.assertEqual(spec.pip_position, 4)
        self.assertEqual(spec.pip_value, 10.0)
        self.assertEqual(spec.spread_avg, 1.2)
        self.assertEqual(spec.symbol, "EURUSD.a")

    def test_broker_stops_level_maps_to_instrument_spec_field(self):
        broker_data = {
            "digits": 5,
            "point": 0.00001,
            "spread": 12,
            "volume_min": 0.01,
            "volume_max": 100.0,
            "volume_step": 0.01,
            "trade_stops_level": 35,
        }
        spec = _create_spec_from_broker_data("EURUSD", broker_data)
        self.assertEqual(spec.stops_level, 35)

    def test_sync_from_mt5_updates_spec(self):
        @dataclass
        class MockMT5SymbolInfo:
            trade_tick_value: float = 1.0
            trade_tick_size: float = 0.00001
            volume_step: float = 0.01
            volume_min: float = 0.01
            volume_max: float = 50.0
            spread: int = 15
            point: float = 0.00001
            digits: int = 5
            trade_contract_size: float = 100000.0
            trade_stops_level: int = 10
            swap_long: float = -5.0
            swap_short: float = 2.0

        spec = InstrumentSpec(
            symbol="EURUSD",
            pip_position=4,
            pip_value=10.0,
            spread_avg=1.0,
            min_lot=0.01,
            max_lot=100.0,
            tick_value=0.0,
            tick_size=0.0,
            volume_step=0.01,
        )

        mt5_info = MockMT5SymbolInfo()
        sync_instrument_spec_from_mt5(spec, mt5_info)

        self.assertEqual(spec.tick_value, 1.0)
        self.assertEqual(spec.tick_size, 0.00001)
        self.assertEqual(spec.volume_step, 0.01)
        self.assertEqual(spec.min_lot, 0.01)
        self.assertEqual(spec.max_lot, 50.0)
        self.assertEqual(spec.point, 0.00001)
        self.assertEqual(spec.digits, 5)
        self.assertEqual(spec.contract_size, 100000.0)
        self.assertEqual(spec.stops_level, 10)
        self.assertEqual(spec.swap_long, -5.0)
        self.assertEqual(spec.swap_short, 2.0)
        self.assertAlmostEqual(spec.spread_avg, 1.5, places=2)

    def test_sync_from_mt5_graceful_none(self):
        spec = InstrumentSpec(
            symbol="EURUSD",
            pip_position=4,
            pip_value=10.0,
            spread_avg=1.0,
            min_lot=0.01,
            max_lot=100.0,
        )

        orig_tick_value = spec.tick_value
        orig_spread = spec.spread_avg
        result = sync_instrument_spec_from_mt5(spec, None)

        self.assertIs(result, spec)
        self.assertEqual(spec.tick_value, orig_tick_value)
        self.assertEqual(spec.spread_avg, orig_spread)


if __name__ == "__main__":
    unittest.main()
