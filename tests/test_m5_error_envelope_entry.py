"""SYNTHETIC_METADATA_AND_FAILURE_ONLY_ENTRY_CHECKS; no native/fit proof.

The two partial-hour shapes use invented dates, not a calendar classification.
No successful fitted/source verifier or capability is substituted.
"""
from copy import deepcopy
import importlib
from pathlib import Path
import unittest


class M5ErrorEnvelopeEntryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[1]
        cls.planner = importlib.import_module("pm_production_search_planner")
        cls.horizon = importlib.import_module("pm_training_horizon")
        for module, name in ((cls.planner, "pm_production_search_planner.py"),
                (cls.horizon, "pm_training_horizon.py"),
                (cls.planner.search_execution, "pm_authoritative_search_execution.py")):
            if Path(module.__file__).resolve() != (root / name).resolve():
                raise RuntimeError("repository source origin mismatch: " + name)

    def test_native_default_retains_its_original_fitted_type_refusal(self):
        with self.assertRaises(self.planner.ProductionSearchPlannerError) as caught:
            self.planner.verify_preprotected_state_error_envelope(object(), {})
        self.assertEqual(caught.exception.code, "FITTED_TIMEFRAME_INVALID")

    def test_explicit_m5_window_requires_original_m5_fitted_authentication(self):
        with self.assertRaises(self.planner.search_execution.AuthoritativeSearchExecutionError) as caught:
            self.planner.verify_preprotected_state_error_envelope(object(), {},
                m5_view_window={"source_kind": "m5_derived"})
        self.assertEqual(caught.exception.code, "M5_DERIVED_FITTED_BINDING_INVALID")

    def test_both_partial_hour_shapes_keep_recorded_end_before_friday_cutoff(self):
        cell = {"symbol": "UNITUSD", "timeframe": "H1"}
        anchor = {"selection_cutoff_utc": "2024-07-12T00:00:00Z"}
        for base_end, view_end in (("2024-07-11T23:55:00Z", "2024-07-12T00:00:00+00:00"),
                ("2024-07-11T22:55:00Z", "2024-07-11T23:00:00+00:00")):
            native = {"source_state": "AVAILABLE", "symbol": "UNITUSD", "timeframe": "M5",
                "prefix_index_sha256": "1" * 64, "prefix_content_sha256": "2" * 64,
                "prefix_start_utc": "2024-07-11T20:00:00Z",
                "prefix_end_exclusive_utc": base_end, "prefix_last_close_utc": base_end,
                "prefix_rows": 47 if "23:55" in base_end else 35}
            window = {"schema_version": "fxpm.m5-derived-price-window.v1", "source_kind": "m5_derived",
                "symbol": "UNITUSD", "timeframe": "H1", "m5_view_contract_sha256": "3" * 64,
                "view_key": "4" * 64, "view_payload_sha256": "5" * 64,
                "view_membership_sha256": "6" * 64, "view_frame_sha256": "7" * 64,
                "view_start_utc": "2024-07-11T20:00:00+00:00",
                "view_end_exclusive_utc": view_end, "view_last_close_utc": view_end,
                "view_rows": 4 if "23:55" in base_end else 3,
                "base_native_prefix_index_sha256": native["prefix_index_sha256"],
                "base_native_prefix_content_sha256": native["prefix_content_sha256"]}
            before = deepcopy((window, native))
            with self.subTest(base_end=base_end, view_end=view_end):
                self.assertEqual(self.horizon._normalize_m5_window(window, cell, native, anchor), window)
                self.assertEqual((window, native), before)
                # Closing exactly at the final bucket's open has no member.
                absent = "2024-07-11T23:00:00Z" if "23:55" in base_end else "2024-07-11T22:00:00Z"
                with self.assertRaises(self.horizon._HorizonContractError):
                    self.horizon._normalize_m5_window(window, cell,
                        {**native, "prefix_end_exclusive_utc": absent, "prefix_last_close_utc": absent}, anchor)


if __name__ == "__main__":
    unittest.main()
