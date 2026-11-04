"""SYNTHETIC_METADATA_ONLY_NOT_NATIVE_VERIFICATION_OR_PLAN_ISSUANCE.

Use the original state-contract authority and the actual planner call expression.
Invented base/window metadata never becomes a NativePrefixProduct or capability.
Root must supply a coherent source assembly before explicitly running this file.
"""
from __future__ import annotations

import ast
from copy import deepcopy
import hashlib
import importlib
import json
from pathlib import Path
import unittest


PROVENANCE = "SYNTHETIC_METADATA_ONLY_NOT_NATIVE_VERIFICATION_OR_PLAN_ISSUANCE"


def _sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def _base():
    return {"index_sha256": "1" * 64, "content_sha256": "2" * 64,
        "start_utc": "2024-07-12T22:00:00Z", "end_exclusive_utc": "2024-07-12T23:55:00Z",
        "last_close_utc": "2024-07-12T23:55:00Z", "rows": 23}


def _window(base):
    return {"schema_version": "fxpm.m5-derived-price-window.v1", "source_kind": "m5_derived",
        "symbol": "UNITUSD", "timeframe": "H1", "m5_view_contract_sha256": "3" * 64,
        "view_key": "4" * 64, "view_payload_sha256": "5" * 64,
        "view_membership_sha256": "6" * 64, "view_frame_sha256": "7" * 64,
        "view_start_utc": "2024-07-12T22:00:00+00:00",
        "view_end_exclusive_utc": "2024-07-13T00:00:00+00:00",
        "view_last_close_utc": "2024-07-13T00:00:00+00:00", "view_rows": 2,
        "base_native_prefix_index_sha256": base["index_sha256"],
        "base_native_prefix_content_sha256": base["content_sha256"]}


def _horizon_native(base):
    return {"source_state": "AVAILABLE", "symbol": "UNITUSD", "timeframe": "M5",
        "prefix_index_sha256": base["index_sha256"],
        "prefix_content_sha256": base["content_sha256"],
        "prefix_start_utc": base["start_utc"],
        "prefix_end_exclusive_utc": base["end_exclusive_utc"],
        "prefix_last_close_utc": base["last_close_utc"], "prefix_rows": base["rows"]}


class M5PathStateAndPartialWindowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[1]
        cls.planner = importlib.import_module("pm_production_search_planner")
        cls.horizon = importlib.import_module("pm_training_horizon")
        for module, name in ((cls.planner, "pm_production_search_planner.py"),
                (cls.horizon, "pm_training_horizon.py")):
            if Path(module.__file__).resolve() != (root / name).resolve():
                raise RuntimeError("planner repository source origin mismatch: " + name)
        tree = ast.parse((root / "pm_production_search_planner.py").read_bytes())
        function = next(node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "_state_domain_ceiling_records")
        calls = [node for node in ast.walk(function) if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name) and node.func.id == "_domain_state_replay_evidence"]
        if len(calls) != 1:
            raise RuntimeError("actual domain replay call is no longer uniquely selectable")
        cls.replay_call = compile(ast.Expression(body=deepcopy(calls[0])),
            str(root / "pm_production_search_planner.py"), "eval")
        # Original registry/domain builders only: no substituted authority, seal,
        # source product, fitted frame, issuer or verifier is supplied.
        planner = cls.planner
        process = {"candidate_domain_sha256": planner.state_contract.STRATEGY_CANDIDATE_DOMAIN_SHA256,
            "semantics": planner._process_semantics(planner._strategy_domains())}
        cls.authority = planner._state_authority_for_candidate(process["candidate_domain_sha256"])
        cls.domains = planner._effective_state_domains(process,
            symbol="UNITUSD", timeframe="H1", regime="TREND")

    def _replay(self, base, window, domain):
        return eval(self.replay_call, vars(self.planner), {
            "native_prefix": base, "m5_view_window": window, "effective_domain": domain,
            "initial_error": 1.0, "tolerance": 0.01, "authority": self.authority})

    def _path_domain(self):
        base = _base()
        window = _window(base)
        for domain in self.domains:
            evidence, receipts = self._replay(base, window, domain)
            if receipts:
                return domain, evidence, receipts
        self.fail("the real registered process has no path-state domain to exercise")

    def test_actual_domain_call_retains_base_digests_and_separate_target_window(self):
        base = _base()
        window = _window(base)
        before = deepcopy((base, window))
        domain, evidence, receipts = self._path_domain()
        self.assertTrue(evidence)
        for record in receipts:
            self.assertEqual(record["native_prefix_index_sha256"], base["index_sha256"])
            self.assertEqual(record["native_prefix_content_sha256"], base["content_sha256"])
            self.assertEqual(record["prefix_start_utc"], base["start_utc"])
            self.assertEqual(record["prefix_end_exclusive_utc"], base["end_exclusive_utc"])
            self.assertEqual(record["prefix_rows"], base["rows"])
            self.assertEqual(record["m5_view_window"], window)
            self.assertEqual(record["m5_view_window_sha256"], _sha(window))
            self.assertNotEqual(record["prefix_end_exclusive_utc"], window["view_end_exclusive_utc"])
        _, native_records = self._replay(base, None, domain)
        self.assertTrue(native_records)
        for record in native_records:
            self.assertEqual(record["schema_version"], "fxpm.production-domain-state-replay.v1")
            self.assertEqual(record["native_prefix_index_sha256"], base["index_sha256"])
            self.assertNotIn("m5_view_window", record)
        self.assertEqual((base, window), before)

    def test_actual_path_replay_hash_changes_with_base_or_target_window(self):
        base = _base()
        window = _window(base)
        domain, _, records = self._path_domain()
        expected = [record["identity_sha256"] for record in records]
        variants = (({**base, "index_sha256": "8" * 64}, window),
            ({**base, "content_sha256": "9" * 64}, window),
            (base, {**window, "view_payload_sha256": "a" * 64}))
        for changed_base, changed_window in variants:
            with self.subTest(base=changed_base, window=changed_window):
                _, actual = self._replay(changed_base, changed_window, domain)
                self.assertNotEqual([record["identity_sha256"] for record in actual], expected)
        bounds = self.planner._planning_source_bounds(base, window)
        self.assertEqual(bounds["rows"], window["view_rows"])
        self.assertNotIn("index_sha256", bounds)
        self.assertNotIn("content_sha256", bounds)

    def test_friday_partial_final_bucket_closes_saturday_without_lookahead(self):
        base = _base()
        window = _window(base)
        native = _horizon_native(base)
        before = deepcopy((native, window))
        actual = self.horizon._normalize_m5_window(window,
            {"symbol": "UNITUSD", "timeframe": "H1"}, native,
            {"selection_cutoff_utc": "2024-07-14T08:05:00Z"})
        self.assertEqual(actual, window)
        self.assertEqual(_sha(actual), _sha(window))
        self.assertEqual((native, window), before)

    def test_final_bucket_requires_a_member_and_both_clocks_before_anchor(self):
        base = _base()
        window = _window(base)
        native = _horizon_native(base)
        cell = {"symbol": "UNITUSD", "timeframe": "H1"}
        anchor = {"selection_cutoff_utc": "2024-07-14T08:05:00Z"}
        changes = ({"prefix_end_exclusive_utc": "2024-07-12T23:00:00Z"},
            {"prefix_end_exclusive_utc": "2024-07-12T22:55:00Z"},
            {"prefix_end_exclusive_utc": "2024-07-14T08:10:00Z"})
        for change in changes:
            with self.subTest(native_change=change), self.assertRaises(self.horizon._HorizonContractError):
                self.horizon._normalize_m5_window(window, cell, {**native, **change}, anchor)
        with self.assertRaises(self.horizon._HorizonContractError):
            self.horizon._normalize_m5_window(window, cell, native,
                {"selection_cutoff_utc": "2024-07-12T23:50:00Z"})


if __name__ == "__main__":
    unittest.main()
