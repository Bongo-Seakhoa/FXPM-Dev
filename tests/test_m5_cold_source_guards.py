"""Metadata-only negative guards; NO_NATIVE_VERIFICATION_OR_SAVED_OWNER_PROOF."""
from pathlib import Path
import unittest
import pm_historical_production_adapter as adapter

if Path(adapter.__file__).resolve() != Path(__file__).resolve().parents[1] / "pm_historical_production_adapter.py":
    raise RuntimeError("C4_TEST_REQUIRES_EXACT_CANDIDATE_ADAPTER_MODULE")


class _OrdinaryM5RequestMetadata:
    def to_dict(self):
        return {"schema_version": "fxpm.declared-m5-slot-producer-request.v1", "source_kind": "m5_derived"}


class M5ColdSourceGuardTests(unittest.TestCase):
    def test_m5_plan_restore_requires_actual_verified_fitted_input(self):
        with self.assertRaises(adapter.HistoricalProductionAdapterError) as caught:
            adapter._restore_declared_slot_plan(None, request=_OrdinaryM5RequestMetadata(),
                process=None, completion={}, sink=None)
        self.assertEqual(str(caught.exception), "DECLARED_M5_VERIFIED_FITTED_SOURCE_REQUIRED")

    def test_m5_frozen_chain_cannot_fall_back_to_native_without_verified_fit(self):
        with self.assertRaises(adapter.HistoricalProductionAdapterError) as caught:
            adapter._verify_declared_slot_frozen_chain(None, completion={}, parent={},
                body=_OrdinaryM5RequestMetadata().to_dict(), request=None, plan=None,
                source=None, fit=None, entries={}, precommit=None)
        self.assertEqual(str(caught.exception), "DECLARED_M5_VERIFIED_FITTED_SOURCE_REQUIRED")
