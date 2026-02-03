import tempfile
import unittest

from pm_core import (
    set_instrument_specs,
    set_broker_specs_path,
    get_instrument_spec,
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


if __name__ == "__main__":
    unittest.main()
