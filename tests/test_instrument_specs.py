import unittest
import os
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
from pm_symbol import infer_asset_class, is_fiat_fx_symbol


class InstrumentSpecTests(unittest.TestCase):
    def setUp(self):
        path = Path("artifact/fxpm_runtime/.tmp_pytest/test_instrument_specs")
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        self._tmpdir = path
        self._old_pm_config_path = os.environ.get("PM_CONFIG_PATH")
        os.environ["PM_CONFIG_PATH"] = str(path / "missing_config.json")
        set_broker_specs_path(str(path / "no_broker_specs.json"))
        set_instrument_specs({}, {})

    def tearDown(self):
        shutil.rmtree(self._tmpdir, ignore_errors=True)
        set_instrument_specs({}, {})
        if self._old_pm_config_path is None:
            os.environ.pop("PM_CONFIG_PATH", None)
        else:
            os.environ["PM_CONFIG_PATH"] = self._old_pm_config_path

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

    def test_python_mt5_weekday_fields_never_claim_authenticated_terms(self):
        symbol_info = MT5SymbolInfo(
            symbol="EURUSD",
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
            swap_sunday=0.0,
            swap_monday=1.0,
            swap_tuesday=1.0,
            swap_wednesday=3.0,
            swap_thursday=1.0,
            swap_friday=1.0,
            swap_saturday=0.0,
        )

        spec = symbol_info.to_instrument_spec()

        self.assertEqual(spec.swap_wednesday, 3.0)
        self.assertEqual(spec.swap_sunday, 0.0)
        self.assertEqual(spec.swap_terms_provenance_status, "INCOMPLETE")
        self.assertEqual(spec.swap_terms_snapshot_sha256, "")

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
        self.assertEqual(spec.asset_class, "index")

    def test_asset_class_inference_avoids_crypto_fx_collision(self):
        self.assertTrue(is_fiat_fx_symbol("USDJPY"))
        self.assertFalse(is_fiat_fx_symbol("BTCETH"))
        self.assertEqual(infer_asset_class("BTCETH"), "crypto")
        self.assertEqual(infer_asset_class("DOGUSD"), "crypto")
        self.assertEqual(infer_asset_class("USDJPY"), "fx")

    def test_broker_spec_asset_class_prefers_snapshot_metadata(self):
        explicit = _create_spec_from_broker_data(
            "SYNTH1",
            {
                "asset_class": "index",
                "digits": 2,
                "point": 0.01,
                "tick_size": 0.01,
                "tick_value": 1.0,
                "contract_size": 1.0,
            },
        )
        equity = _create_spec_from_broker_data(
            "AAPL",
            {
                "path": "Stocks\\US\\NASDAQ",
                "digits": 2,
                "point": 0.01,
                "tick_size": 0.01,
                "tick_value": 1.0,
                "contract_size": 1.0,
            },
        )

        self.assertEqual(explicit.asset_class, "index")
        self.assertEqual(equity.asset_class, "equity")

    def test_broker_spec_construction_fallback_remains_valid(self):
        spec = _create_spec_from_broker_data(
            "DOGUSD",
            {
                "digits": 5,
                "point": 0.00001,
                "tick_size": 0.00001,
                "tick_value": 1.0,
                "contract_size": 1000.0,
                "volume_min": "not-a-number",
            },
        )

        self.assertEqual(spec.symbol, "DOGUSD")
        self.assertEqual(spec.pip_position, 4)
        self.assertEqual(spec.asset_class, "crypto")

    def test_missing_broker_specs_path_clears_previous_cache(self):
        valid_path = self._tmpdir / "broker_specs.json"
        valid_path.write_text('{"EURUSD": {"digits": 5, "point": 0.00001}}', encoding="utf-8")

        set_broker_specs_path(str(valid_path))
        self.assertIn("EURUSD", load_broker_specs())

        set_broker_specs_path(str(self._tmpdir / "missing_broker_specs.json"))
        self.assertEqual(load_broker_specs(), {})

    def test_broker_specs_override_config_execution_fields_and_preserve_commission(self):
        broker_path = self._tmpdir / "broker_specs.json"
        broker_path.write_text(
            """{
              "EURUSD": {
                "digits": 3,
                "point": 0.001,
                "tick_size": 0.001,
                "tick_value": 9.5,
                "contract_size": 100000,
                "volume_min": 0.01,
                "volume_max": 50,
                "volume_step": 0.01,
                "spread": 25,
                "swap_long": -1.0,
                "swap_short": 0.5,
                "stops_level": 12
              }
            }""",
            encoding="utf-8",
        )
        set_broker_specs_path(str(broker_path))
        set_instrument_specs(
            specs={
                "EURUSD": {
                    "pip_position": 4,
                    "pip_value": 10.0,
                    "spread_avg": 1.2,
                    "min_lot": 0.01,
                    "max_lot": 100.0,
                    "commission_per_lot": 0.0,
                }
            },
            defaults={},
        )

        spec = get_instrument_spec("EURUSD")

        self.assertEqual(spec.digits, 3)
        self.assertAlmostEqual(spec.point, 0.001, places=8)
        self.assertAlmostEqual(spec.tick_value, 9.5, places=8)
        self.assertAlmostEqual(spec.spread_avg, 2.5, places=8)
        self.assertEqual(spec.max_lot, 50)
        self.assertEqual(spec.stops_level, 12)
        self.assertEqual(spec.commission_per_lot, 0.0)

    def test_broker_spec_lookup_accepts_case_variant_aliases(self):
        broker_path = self._tmpdir / "broker_specs.json"
        broker_path.write_text(
            """{
              "Palladium": {
                "symbol": "Palladium",
                "broker_symbol": "Palladium",
                "asset_class": "metal",
                "digits": 2,
                "point": 0.01,
                "tick_size": 0.01,
                "tick_value": 1.0,
                "contract_size": 100.0,
                "volume_min": 0.01,
                "volume_max": 50.0,
                "volume_step": 0.01,
                "spread": 647
              }
            }""",
            encoding="utf-8",
        )
        set_broker_specs_path(str(broker_path))

        spec = get_instrument_spec("PALLADIUM")

        self.assertEqual(spec.symbol, "PALLADIUM")
        self.assertEqual(spec.digits, 2)
        self.assertAlmostEqual(spec.point, 0.01, places=8)
        self.assertAlmostEqual(spec.tick_value, 1.0, places=8)
        self.assertAlmostEqual(spec.contract_size, 100.0, places=8)
        self.assertAlmostEqual(spec.spread_avg, 647.0, places=8)
        self.assertEqual(spec.asset_class, "metal")

    def test_offline_specs_infer_symbol_class_points(self):
        eurusd = get_instrument_spec("EURUSD")
        usdjpy = get_instrument_spec("USDJPY")
        xauusd = get_instrument_spec("XAUUSD")
        btcusd = get_instrument_spec("BTCUSD")
        us30 = get_instrument_spec("US30")

        self.assertEqual(eurusd.digits, 5)
        self.assertEqual(eurusd.asset_class, "fx")
        self.assertAlmostEqual(eurusd.point, 0.00001, places=8)
        self.assertEqual(usdjpy.digits, 3)
        self.assertEqual(usdjpy.asset_class, "fx")
        self.assertAlmostEqual(usdjpy.point, 0.001, places=8)
        self.assertEqual(xauusd.digits, 2)
        self.assertEqual(xauusd.asset_class, "metal")
        self.assertAlmostEqual(xauusd.point, 0.01, places=8)
        self.assertEqual(btcusd.digits, 2)
        self.assertEqual(btcusd.asset_class, "crypto")
        self.assertAlmostEqual(btcusd.point, 0.01, places=8)
        self.assertEqual(us30.digits, 0)
        self.assertEqual(us30.asset_class, "index")
        self.assertAlmostEqual(us30.point, 1.0, places=8)

    def test_mt5_symbol_info_to_spec_carries_asset_class_from_path(self):
        symbol_info = MT5SymbolInfo(
            symbol="AAPL",
            digits=2,
            point=0.01,
            trade_tick_value=1.0,
            trade_tick_size=0.01,
            trade_contract_size=1.0,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            spread=2,
            spread_float=True,
            swap_long=0.0,
            swap_short=0.0,
            trade_stops_level=0,
            path="Stocks\\US\\NASDAQ",
        )

        spec = symbol_info.to_instrument_spec()

        self.assertEqual(symbol_info.asset_class, "equity")
        self.assertEqual(spec.asset_class, "equity")

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
        self.assertEqual(spec.swap_terms_provenance_status, "INCOMPLETE")
        self.assertEqual(spec.swap_terms_snapshot_sha256, "")
        self.assertTrue(
            all(
                getattr(spec, f"swap_{weekday}") is None
                for weekday in (
                    "sunday", "monday", "tuesday", "wednesday",
                    "thursday", "friday", "saturday",
                )
            )
        )

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
