"""SYNTHETIC_METADATA_ARITHMETIC_ONLY; no native verification or plan issuance.

These invented dictionaries exercise private normalization/math boundaries.
No fitted object, source product, context, seal, model or issuer is invoked.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]

import pm_training_horizon as horizon
import pm_production_search_planner as planner

for module, filename in ((horizon, "pm_training_horizon.py"), (planner, "pm_production_search_planner.py")):
    if Path(module.__file__).resolve() != (REPO_ROOT / filename).resolve():
        raise RuntimeError("M5 horizon tests require repository module origins")


PROVENANCE = "SYNTHETIC_METADATA_ARITHMETIC_ONLY_NOT_NATIVE_VERIFICATION"


def _sha(label):
    return hashlib.sha256((PROVENANCE + ":" + label).encode("ascii")).hexdigest()


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def _native():
    value = {key: _sha(key) for key in ("activation_manifest_sha256", "activation_entry_sha256",
        "artifact_sha256", "provenance_sha256", "request_map_sha256", "source_identity_sha256",
        "continuity_policy_sha256", "continuity_receipt_sha256", "continuity_geometry_sha256",
        "prefix_schema_sha256", "prefix_index_sha256", "prefix_content_sha256", "segments_sha256")}
    value.update(source_state="AVAILABLE", symbol="UNITUSD", timeframe="M5",
        activation_id="SYNTHETIC_NOT_AUTHENTICATED", source_terminal_receipt_sha256=None,
        prefix_start_utc="2024-01-02T00:00:00Z", prefix_end_exclusive_utc="2024-01-02T01:00:00Z",
        prefix_last_close_utc="2024-01-02T01:00:00Z", prefix_rows=12)
    return value


def _window(native):
    return {"schema_version": "fxpm.m5-derived-price-window.v1", "source_kind": "m5_derived",
        "symbol": "UNITUSD", "timeframe": "M15", "m5_view_contract_sha256": _sha("VIEW_CONTRACT"),
        "view_key": _sha("VIEW_KEY"), "view_payload_sha256": _sha("VIEW_PAYLOAD"),
        "view_membership_sha256": _sha("VIEW_MEMBERSHIP"), "view_frame_sha256": _sha("VIEW_FRAME"),
        "view_start_utc": "2024-01-02T00:00:00+00:00", "view_end_exclusive_utc": "2024-01-02T01:00:00+00:00",
        "view_last_close_utc": "2024-01-02T01:00:00+00:00", "view_rows": 4,
        "base_native_prefix_index_sha256": native["prefix_index_sha256"],
        "base_native_prefix_content_sha256": native["prefix_content_sha256"]}


def _candidate(start="2024-01-01T00:00:00Z"):
    body = {"candidate_id": "UNIT_CANDIDATE", "lookback_kind": "NATIVE_OBSERVATIONS",
        "lookback_value": 5, "realized_start_utc": start}
    return {**body, "candidate_definition_sha256": _digest(body), "folds": []}


class M5HorizonWindowContractTests(unittest.TestCase):
    def test_native_candidate_boundary_contract_is_unchanged(self):
        candidate = _candidate();native = _native();before = deepcopy((candidate, native))
        result, boundary = horizon._normalize_candidate(candidate, native,
            {"selection_cutoff_utc": "2024-01-02T01:00:00Z"}, {})
        self.assertIs(boundary, True)
        self.assertEqual(result, candidate)
        self.assertEqual((candidate, native), before)

    def test_legacy_lookback_policy_counts_target_observations_not_base_m5_rows(self):
        candidate = _candidate("2024-01-02T00:00:00Z")
        anchor = {"selection_cutoff_utc": "2024-01-02T01:00:00Z"}
        result, boundary = horizon._normalize_candidate_bounds(candidate,
            "2024-01-02T00:00:00Z", "2024-01-02T01:00:00Z", 4, anchor, {})
        self.assertTrue(boundary)
        self.assertEqual(result["folds"], [])
        with self.assertRaises(horizon._HorizonContractError):
            horizon._normalize_candidate(deepcopy(candidate), _native(), anchor, {})

    def test_exact_window_accepts_root_iso_spelling_without_rewriting_identity(self):
        native = _native();window = _window(native);before = deepcopy(window)
        cell = {"symbol": "UNITUSD", "timeframe": "M15", "regime": "TREND"}
        normalized = horizon._normalize_m5_window(window, cell, native,
            {"selection_cutoff_utc": "2024-01-02T01:00:00Z"})
        self.assertEqual(normalized, before)
        self.assertEqual(_digest(normalized), _digest(before))
        self.assertEqual(horizon._m5_window_utc(window["view_last_close_utc"], "unit close").strftime(
            "%Y-%m-%dT%H:%M:%SZ"), native["prefix_last_close_utc"])

    def test_window_refuses_wrong_identity_shape_timeframe_count_and_time_bounds(self):
        native = _native();window = _window(native)
        cell = {"symbol": "UNITUSD", "timeframe": "M15", "regime": "TREND"}
        changes = ({"base_native_prefix_index_sha256": _sha("OTHER_INDEX")},
            {"base_native_prefix_content_sha256": _sha("OTHER_CONTENT")}, {"timeframe": "H1"},
            {"view_rows": 5}, {"view_rows": True}, {"view_frame_sha256": "not-a-digest"},
            {"view_end_exclusive_utc": "2024-01-02T01:15:00+00:00"},
            {"view_last_close_utc": "2024-01-02T01:00:00+01:00"})
        for change in changes:
            with self.subTest(change=change), self.assertRaises(horizon._HorizonContractError):
                horizon._normalize_m5_window({**window, **change}, cell, native,
                    {"selection_cutoff_utc": "2024-01-02T01:00:00Z"})
        with self.assertRaises(horizon._HorizonContractError):
            horizon._normalize_m5_window({**window, "native_timeframe": "M15"}, cell, native,
                {"selection_cutoff_utc": "2024-01-02T01:00:00Z"})

    def test_source_binding_keeps_native_m5_identity_and_adds_view_payload_separately(self):
        native = _native();window = _window(native);before = deepcopy(native)
        binding = horizon._request_source_binding({"native_prefix": native,
            "source_kind": "m5_derived", "target_view_window": window})
        self.assertEqual(binding["artifact_sha256"], native["artifact_sha256"])
        self.assertEqual(binding["prefix_content_sha256"], native["prefix_content_sha256"])
        self.assertEqual(binding["base_timeframe"], "M5")
        self.assertEqual(binding["view_timeframe"], "M15")
        self.assertEqual(binding["source_snapshot_sha256"], window["view_payload_sha256"])
        self.assertNotEqual(binding["source_snapshot_sha256"], binding["prefix_content_sha256"])
        self.assertEqual(binding["m5_view_window_sha256"], _digest(window))
        self.assertEqual(native, before)

    def test_planner_math_and_replay_binding_do_not_relabel_native_source(self):
        native = _native();window = _window(native);before = deepcopy(native)
        self.assertEqual(planner._planning_source_value(native, None, "timeframe"), "M5")
        self.assertEqual(planner._planning_source_value(native, window, "timeframe"), "M15")
        self.assertEqual(planner._planning_source_bounds(native, window)["rows"], 4)
        self.assertEqual(native, before)
        identity = {"schema_version": "UNIT_BASE_SCHEMA", "native_prefix_content_sha256": native["prefix_content_sha256"]}
        self.assertIs(planner._m5_state_binding(identity, None, "UNUSED"), identity)
        replay = planner._m5_state_binding(identity, window,
            "fxpm.production-m5-derived-full-prefix-state-replay.v1")
        self.assertEqual(replay["native_prefix_content_sha256"], native["prefix_content_sha256"])
        self.assertEqual(replay["m5_view_window"], window)
        self.assertEqual(replay["m5_view_window_sha256"], _digest(window))
        self.assertNotIn("source_kind", identity)


if __name__ == "__main__":
    unittest.main()
