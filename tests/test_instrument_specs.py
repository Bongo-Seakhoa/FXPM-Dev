import unittest
from dataclasses import dataclass
from pathlib import Path
import shutil

from pm_core import (
    InstrumentSpec,
    _create_spec_from_broker_data,
    get_instrument_spec,
    InstrumentSpec,
    load_broker_specs,
    set_broker_specs_path,
    set_instrument_specs,
    sync_instrument_spec_from_mt5,
)
from pm_mt5 import MT5SymbolInfo


class InstrumentSpecTests(unittest.TestCase):
    def setUp(self):
        path = Path("artifact/fxpm_runtime/.tmp_pytest/test_instrument_specs")
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        self._tmpdir = path
        set_broker_specs_path(str(path / "no_broker_specs.json"))

    def tearDown(self):
        shutil.rmtree(self._tmpdir, ignore_errors=True)
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

    def test_mt5_symbol_info_to_spec_uses_configured_commission(self):
        specs = {
            "EURUSD": {
                "pip_position": 4,
                "pip_value": 10.0,
                "spread_avg": 1.2,
                "min_lot": 0.01,
                "max_lot": 100.0,
                "commission_per_lot": 0.0,
            },
        }
        set_instrument_specs(specs=specs, defaults={})
        symbol_info = MT5SymbolInfo(
            symbol="EURUSD.a",
            digits=5,
            point=0.00001,
            trade_tick_value=1.0,
            trade_tick_size=0.00001,
            trade_contract_size=100000.0,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            spread=12,
            spread_float=True,
            swap_long=-1.0,
            swap_short=0.5,
            trade_stops_level=10,
        )

        spec = symbol_info.to_instrument_spec()

        self.assertEqual(spec.commission_per_lot, 0.0)

    def test_broker_spec_preserves_explicit_zero_commission(self):
        spec = _create_spec_from_broker_data(
            "US100",
            {
                "digits": 2,
                "point": 0.01,
                "tick_size": 0.01,
                "tick_value": 1.0,
                "contract_size": 1.0,
                "pip_value": 1.0,
                "spread": 150,
                "volume_min": 0.1,
                "volume_max": 100.0,
                "volume_step": 0.1,
                "commission_per_lot": 0.0,
            },
        )

        self.assertEqual(spec.commission_per_lot, 0.0)

    def test_missing_broker_specs_path_clears_previous_cache(self):
        valid_path = self._tmpdir / "broker_specs.json"
        valid_path.write_text('{"EURUSD": {"digits": 5, "point": 0.00001}}', encoding="utf-8")

        set_broker_specs_path(str(valid_path))
        self.assertIn("EURUSD", load_broker_specs())

        set_broker_specs_path(str(self._tmpdir / "missing_broker_specs.json"))
        self.assertEqual(load_broker_specs(), {})

    def test_sync_from_mt5_updates_spec(self):
        """Test that sync_instrument_spec_from_mt5 updates InstrumentSpec with MT5 values."""
        # Create a mock MT5SymbolInfo
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

        # Create initial spec with config values
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

        # Create mock MT5 info
        mt5_info = MockMT5SymbolInfo()

        # Sync from MT5
        sync_instrument_spec_from_mt5(spec, mt5_info)

        # Verify updates
        self.assertEqual(spec.tick_value, 1.0)
        self.assertEqual(spec.tick_size, 0.00001)
        self.assertEqual(spec.volume_step, 0.01)
        self.assertEqual(spec.min_lot, 0.01)
        self.assertEqual(spec.max_lot, 50.0)  # Changed from 100.0
        self.assertEqual(spec.point, 0.00001)
        self.assertEqual(spec.digits, 5)
        self.assertEqual(spec.contract_size, 100000.0)
        self.assertEqual(spec.stops_level, 10)
        self.assertEqual(spec.swap_long, -5.0)
        self.assertEqual(spec.swap_short, 2.0)

        # Verify spread conversion (15 points * 0.00001 / 0.0001 = 1.5 pips)
        self.assertAlmostEqual(spec.spread_avg, 1.5, places=2)

    def test_sync_from_mt5_graceful_none(self):
        """Test that sync_instrument_spec_from_mt5 handles None MT5 info gracefully."""
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

        # Sync with None should return unchanged spec
        result = sync_instrument_spec_from_mt5(spec, None)

        self.assertIs(result, spec)
        self.assertEqual(spec.tick_value, orig_tick_value)
        self.assertEqual(spec.spread_avg, orig_spread)

    def test_sync_from_mt5_cross_pair_tick_value(self):
        """Test that cross pairs get correct tick_value from MT5."""
        @dataclass
        class MockMT5SymbolInfo:
            trade_tick_value: float = 0.65  # Cross pair tick value in USD
            trade_tick_size: float = 0.00001
            volume_step: float = 0.01
            volume_min: float = 0.01
            volume_max: float = 100.0
            spread: int = 25
            point: float = 0.00001
            digits: int = 5
            trade_contract_size: float = 100000.0
            trade_stops_level: int = 10
            swap_long: float = -2.0
            swap_short: float = 1.0

        # Create spec for cross pair (AUDNZD)
        spec = InstrumentSpec(
            symbol="AUDNZD",
            pip_position=4,
            pip_value=6.0,  # Config value (likely stale)
            spread_avg=2.5,
            min_lot=0.01,
            max_lot=100.0,
        )

        mt5_info = MockMT5SymbolInfo()
        sync_instrument_spec_from_mt5(spec, mt5_info)

        # Verify tick_value was updated to MT5 real value
        self.assertEqual(spec.tick_value, 0.65)
        # Verify spread conversion (25 points * 0.00001 / 0.0001 = 2.5 pips)
        self.assertAlmostEqual(spec.spread_avg, 2.5, places=2)


if __name__ == "__main__":
    unittest.main()
