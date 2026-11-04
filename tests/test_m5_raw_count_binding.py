"""Invented metadata tests of private joins; no Frozen plan or authority is issued."""
import copy
import unittest

import pm_winner_raw_evidence as raw


def window():
    return {
        "schema_version": "fxpm.m5-derived-price-window.v1", "source_kind": "m5_derived",
        "symbol": "EURUSD", "timeframe": "H1", "m5_view_contract_sha256": "1" * 64,
        "view_key": "2" * 64, "view_payload_sha256": "3" * 64,
        "view_membership_sha256": "4" * 64, "view_frame_sha256": "5" * 64,
        "view_start_utc": "2024-01-01T00:00:00+00:00",
        "view_end_exclusive_utc": "2024-01-02T00:00:00+00:00",
        "view_last_close_utc": "2024-01-02T00:00:00+00:00", "view_rows": 24,
        "base_native_prefix_index_sha256": "6" * 64,
        "base_native_prefix_content_sha256": "7" * 64,
    }


def descriptors(value):
    return [dict(symbol="EURUSD", timeframe="H1", source_timeframe="M5", tier=tier,
                 m5_derived_cost_binding={"source_kind": "m5_derived"},
                 m5_execution_view_binding=copy.deepcopy(value)) for tier in ("P75", "P95")]


def count(value):
    return {
        "source_kind": "m5_derived", "base_timeframe": "M5", "view_timeframe": "H1",
        "m5_view_contract_sha256": value["m5_view_contract_sha256"],
        "view_key": value["view_key"], "view_payload_sha256": value["view_payload_sha256"],
        "view_membership_sha256": value["view_membership_sha256"],
        "m5_view_window_sha256": raw._sha_json(value),
        "native_prefix_index_sha256": value["base_native_prefix_index_sha256"],
        "native_prefix_content_sha256": value["base_native_prefix_content_sha256"],
        "source_bar_count": 12, "fit_start_utc": "2024-01-01T12:00:00Z",
        "fit_end_exclusive_utc": "2024-01-02T00:00:00Z",
        "latest_closed_bar_utc": "2024-01-02T00:00:00Z",
    }


class M5RawCountBindingTests(unittest.TestCase):
    def test_unmarked_native_descriptors_keep_native_route(self):
        self.assertIsNone(raw._m5_execution_window_from_descriptors(
            [{"tier": "P75"}, {"tier": "P95"}], symbol="EURUSD", timeframe="H1"))

    def test_marked_pair_requires_both_matching_actual_windows(self):
        value = window()
        pair = descriptors(value)
        observed = raw._m5_execution_window_from_descriptors(pair, symbol="EURUSD", timeframe="H1")
        self.assertEqual(observed, value)
        observed["view_rows"] = 1
        self.assertEqual(pair[0]["m5_execution_view_binding"]["view_rows"], 24)
        cases = [descriptors(value) for _ in range(4)]
        cases[0][0].pop("m5_execution_view_binding")
        cases[1][1]["m5_execution_view_binding"]["view_key"] = "f" * 64
        cases[2][1].pop("m5_derived_cost_binding")
        cases[3][0]["source_timeframe"] = "H1"
        for pair in cases:
            with self.subTest(pair=pair), self.assertRaises(raw.WinnerRawEvidenceError):
                raw._m5_execution_window_from_descriptors(pair, symbol="EURUSD", timeframe="H1")

    def test_window_rejects_clock_kind_bad_hash_and_false_geometry(self):
        for key, bad in (("source_kind", "m5_derived_control_clock.v1"), ("timeframe", "H4"),
                         ("view_rows", True), ("view_rows", 0), ("view_key", "not-a-hash"),
                         ("view_last_close_utc", "2023-12-31T23:00:00Z"),
                         ("view_end_exclusive_utc", "2024-01-01T23:00:00Z"),
                         ("view_start_utc", "2024-01-01T00:00:00.001Z")):
            value = window(); value[key] = bad
            with self.subTest(field=key, value=bad), self.assertRaises(raw.WinnerRawEvidenceError):
                raw._m5_execution_window_from_descriptors(descriptors(value), symbol="EURUSD", timeframe="H1")

    def test_count_joins_native_identity_and_distinct_target_payload(self):
        value = window(); receipt = count(value)
        self.assertNotEqual(value["base_native_prefix_content_sha256"], value["view_payload_sha256"])
        raw._validate_m5_source_count_view(receipt, value,
            native_index_sha256=value["base_native_prefix_index_sha256"],
            horizon_artifact={"source_snapshot_sha256": value["view_payload_sha256"]})

    def test_count_rejects_swapped_native_content_future_or_extra_rows(self):
        value = window()
        for key, bad in (("native_prefix_content_sha256", value["view_payload_sha256"]),
                         ("native_prefix_index_sha256", "f" * 64),
                         ("m5_view_window_sha256", "f" * 64), ("source_bar_count", 25),
                         ("source_bar_count", True), ("view_timeframe", "M5"),
                         ("fit_start_utc", "2023-12-31T23:00:00Z"),
                         ("fit_end_exclusive_utc", "2024-01-02T01:00:00Z"),
                         ("latest_closed_bar_utc", "2024-01-02T01:00:00Z")):
            receipt = count(value); receipt[key] = bad
            with self.subTest(field=key, value=bad), self.assertRaises(raw.WinnerRawEvidenceError):
                raw._validate_m5_source_count_view(receipt, value,
                    native_index_sha256=value["base_native_prefix_index_sha256"],
                    horizon_artifact={"source_snapshot_sha256": value["view_payload_sha256"]})
        for native_index, snapshot in (("f" * 64, value["view_payload_sha256"]),
                                       (value["base_native_prefix_index_sha256"], value["base_native_prefix_content_sha256"])):
            with self.subTest(index=native_index, snapshot=snapshot), self.assertRaises(raw.WinnerRawEvidenceError):
                raw._validate_m5_source_count_view(count(value), value,
                    native_index_sha256=native_index, horizon_artifact={"source_snapshot_sha256": snapshot})
