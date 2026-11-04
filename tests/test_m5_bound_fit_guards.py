"""Negative protocol units: NO_NATIVE_VERIFICATION_OR_ISSUER_POSITIVE.

Ordinary metadata doubles exercise refusal boundaries only. No product, source
session capability, fitted owner or original request is manufactured here.
"""
from pathlib import Path
import unittest
import pm_authoritative_search_execution as execution

if Path(execution.__file__).resolve() != Path(__file__).resolve().parents[1] / "pm_authoritative_search_execution.py":
    raise RuntimeError("C4_TEST_REQUIRES_EXACT_CANDIDATE_EXECUTION_MODULE")


class _OrdinaryRequestMetadata:
    def to_dict(self):
        return {"schema_version": execution.historical.DECLARED_SLOT_REQUEST_SCHEMA}


class M5BoundFitGuardTests(unittest.TestCase):
    def test_reacquired_source_requires_exact_concrete_argument_shape(self):
        for value in (None, {}, {"owner": object()}):
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaises(execution.AuthoritativeSearchExecutionError) as caught:
                    execution._m5_bound_source_inputs(None, request=None, process=None, config=None,
                        source_context=value, source_session_entry=None,
                        source_session_sha256="0" * 64, snapshot_issued_at_utc="2026-01-01T00:00:00Z")
                self.assertEqual(caught.exception.code, "BOUND_M5_REACQUIRED_SOURCE_REQUIRED")

    def test_concrete_shape_cannot_substitute_fake_owner_or_cost_edition(self):
        value = {key: None for key in ("owner", "request_lease", "calibration_lease", "cost_edition",
                                     "source_session", "source_session_entry")}
        with self.assertRaises(execution.AuthoritativeSearchExecutionError) as caught:
            execution._m5_bound_source_inputs(None, request=None, process=None, config=None,
                source_context=value, source_session_entry=None,
                source_session_sha256="0" * 64, snapshot_issued_at_utc="2026-01-01T00:00:00Z")
        self.assertEqual(caught.exception.code, "BOUND_M5_REACQUIRED_SOURCE_CHANGED")

    def test_native_reopen_rejects_an_m5_context_before_source_fallback(self):
        with self.assertRaises(execution.AuthoritativeSearchExecutionError) as caught:
            execution.reopen_bound_fitted_timeframe(None, request=_OrdinaryRequestMetadata(),
                process=None, frozen_pipeline_config=None, header=None, m5_source_context={})
        self.assertEqual(caught.exception.code, "BOUND_FIT_NATIVE_REJECTS_M5_CONTEXT")

    def test_m5_writer_requires_original_capture_authority(self):
        with self.assertRaises(execution.historical_adapter.HistoricalProductionAdapterError) as caught:
            execution._publish_declared_m5_bound_fit(object(), None, (), None)
        self.assertEqual(str(caught.exception), "DECLARED_SLOT_CAPTURE_INVALID")
