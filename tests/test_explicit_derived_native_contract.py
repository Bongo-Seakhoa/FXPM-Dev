"""Invented metadata-only contract examples, never native receipt substitutes.

These tests call pure helpers only. They open no native scope or source file.
"""
import json
import unittest

from pm_explicit_derived_history import Interval, PROTECTED_CUTOFF
from pm_explicit_derived_native import (
    NativeInputsError, aligned_prefix_geometry, require_m5_coverage,
    instrument_session_class,
)


class ExplicitDerivedNativeContractTests(unittest.TestCase):
    def test_hourly_geometry_includes_every_whole_closed_bar_before_0805(self):
        acquisition = Interval("2024-01-01T00:05:00+00:00", "2025-01-01T00:00:00+00:00")
        geometry = aligned_prefix_geometry("H1", acquisition, PROTECTED_CUTOFF)
        self.assertEqual(geometry.acquisition_interval, acquisition)
        self.assertEqual(geometry.source_prefix_interval,
                         Interval(acquisition.start_utc, PROTECTED_CUTOFF))
        self.assertEqual(geometry.interval,
                         Interval("2024-01-01T01:00:00+00:00", "2024-07-14T08:00:00+00:00"))
        self.assertEqual(geometry.excluded_partial_edges.leading,
                         Interval(acquisition.start_utc, "2024-01-01T01:00:00+00:00"))
        self.assertEqual(geometry.excluded_partial_edges.trailing,
                         Interval("2024-07-14T08:00:00+00:00", PROTECTED_CUTOFF))

    def test_aligned_start_and_m5_cutoff_do_not_remove_eligible_members(self):
        acquisition = Interval("2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00")
        geometry = aligned_prefix_geometry("M5", acquisition, PROTECTED_CUTOFF)
        self.assertEqual(geometry.interval, Interval(acquisition.start_utc, PROTECTED_CUTOFF))
        self.assertIsNone(geometry.excluded_partial_edges.leading)
        self.assertIsNone(geometry.excluded_partial_edges.trailing)

    def test_required_whole_bucket_coverage_cannot_be_silently_narrowed(self):
        target = Interval("2024-01-01T01:00:00+00:00", "2024-07-14T08:00:00+00:00")
        require_m5_coverage(Interval("2024-01-01T00:05:00+00:00", PROTECTED_CUTOFF), target)
        for source in (
            Interval("2024-01-01T01:05:00+00:00", PROTECTED_CUTOFF),
            Interval("2024-01-01T00:05:00+00:00", "2024-07-14T07:55:00+00:00"),
        ):
            with self.assertRaises(NativeInputsError) as caught:
                require_m5_coverage(source, target)
            self.assertEqual(caught.exception.code, "M5_REQUIRED_INTERVAL_COVERAGE_UNSUPPORTED")

    def test_session_validation_mode_is_not_an_h4_or_d1_constructor(self):
        acquisition = Interval("2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00")
        for timeframe in ("H4", "D1"):
            with self.assertRaises(NativeInputsError) as caught:
                aligned_prefix_geometry(timeframe, acquisition, PROTECTED_CUTOFF)
            self.assertEqual(caught.exception.code, "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR")

    def test_authenticated_catalog_class_strips_only_the_exact_symbol_leaf(self):
        first = instrument_session_class("SYN_A", {
            "asset_class": "SYNTHETIC_CLASS", "path": "Catalog\\Group\\SYN_A"})
        second = instrument_session_class("SYN_B", {
            "asset_class": "SYNTHETIC_CLASS", "path": "Catalog\\Group\\SYN_B"})
        self.assertEqual(first, second)
        self.assertEqual(json.loads(first)["catalog_parent_path"], "Catalog/Group")
        unchanged = instrument_session_class("SYN_A", {
            "asset_class": "SYNTHETIC_CLASS", "path": "Catalog\\Group\\OTHER"})
        self.assertEqual(json.loads(unchanged)["catalog_parent_path"], "Catalog/Group/OTHER")

    def test_unknown_class_and_catalog_do_not_infer_a_symbol_calendar(self):
        with self.assertRaises(NativeInputsError) as caught:
            instrument_session_class("SYNTHETIC_SYMBOL", {})
        self.assertEqual(caught.exception.code, "INSTRUMENT_SESSION_CLASS_UNAVAILABLE")
