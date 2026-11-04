"""SYNTHETIC_ENVELOPE_METADATA_ONLY_NO_NATIVE_FITTED_OR_MODEL_PROOF.

Exercise original price-envelope arithmetic and metadata validation on toy data.
Source-contract checks bind actual dispatch; no successful public verifier,
NativePrefixProduct, guarded request, fitted object, seal or issuer is mocked.
"""
import ast
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
from types import SimpleNamespace
import unittest

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
EXECUTION = ROOT / "pm_authoritative_search_execution.py"
OLD_EXECUTION = Path(__file__).resolve().parent / "fixtures/m5_original_metadata_recipes.source.txt"
PLANNER = ROOT / "pm_production_search_planner.py"
PLANNER_SHA = "555c1ce8955962395b7ca77c4d49814530a2dd0dfc444125cb881b9c4f600f27"


def parsed(path):
    return ast.parse(path.read_bytes(), filename=str(path))


def function(path, name):
    return next(node for node in parsed(path).body
        if isinstance(node, ast.FunctionDef) and node.name == name)


def selected(path, names, constants=(), namespace=None):
    raw = path.read_bytes()
    if path == PLANNER and hashlib.sha256(raw).hexdigest() != PLANNER_SHA:
        raise AssertionError("frozen planner004 source identity changed")
    tree = ast.parse(raw, filename=str(path))
    definitions = [node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    if {node.name for node in definitions} != set(names):
        raise AssertionError("actual pure definitions missing")
    assignments = [node for node in tree.body if isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(isinstance(target, ast.Name) and target.id in constants
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target]))]
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    ns = {"__name__": "synthetic_envelope_metadata_only", **(namespace or {})}
    exec(compile(ast.fix_missing_locations(ast.Module(
        body=[future, *assignments, *definitions], type_ignores=[])), str(path), "exec"), ns)
    return ns


def planner_helpers():
    return selected(PLANNER, {"ProductionSearchPlannerError", "_fail", "_canonical_bytes",
        "_sha", "_hex64", "_utc", "_frame_sha256", "_state_price_error_bound",
        "_validated_state_error_envelope", "_planning_source_bounds"},
        {"MAX_PROCESS_BYTES", "STATE_ERROR_ENVELOPE_SCHEMA", "STATE_INITIAL_ERROR_FLOOR", "_SHA_CHARS"},
        {"pd": pd, "math": math, "hashlib": hashlib, "json": json,
         "datetime": datetime, "timedelta": timedelta, "UTC": UTC})


def fixture(p):
    # All values/identities are ordinary invented DATA. No source authentication.
    index = pd.date_range("2024-01-02T00:00:00Z", periods=3, freq="h")
    frame = pd.DataFrame({"Open": [1.0, 2.0, 3.0], "High": [1.4, 2.4, 3.4],
        "Low": [0.5, 1.5, 2.5], "Close": [1.1, 2.1, 3.1]}, index=index)
    native = {"prefix_rows": 36, "prefix_start_utc": "2024-01-02T00:00:00Z",
        "prefix_end_exclusive_utc": "2024-01-02T03:00:00Z",
        "prefix_last_close_utc": "2024-01-02T03:00:00Z", "timeframe": "M5"}
    window = {"view_rows": 3, "view_start_utc": "2024-01-02T00:00:00Z",
        "view_end_exclusive_utc": "2024-01-02T03:00:00Z",
        "view_last_close_utc": "2024-01-02T03:00:00Z"}
    body = {"schema_version": p["STATE_ERROR_ENVELOPE_SCHEMA"],
        "derivation": "MAX_100_DOUBLE_MAX_ABS_AND_RANGE_ON_PREPROTECTED_PREFIX",
        "preprotected_cutoff_rule": "FULL_PREFIX_ONLY_WHEN_FOUR_FOLD_PROTECTED_AXIS_UNAVAILABLE",
        "preprotected_cutoff_utc": window["view_end_exclusive_utc"],
        "four_fold_protected_axis_available": False, "source_rows": len(frame), "post_cutoff_rows": 0,
        "source_last_close_utc": window["view_last_close_utc"],
        "source_price_surface_sha256": p["_frame_sha256"](frame),
        "initial_error_bound_hex": p["_state_price_error_bound"](frame).hex(),
        "tolerance_hex": float(0.0001).hex(),
        "full_prefix_verification": "WITHIN_FROZEN_PREPROTECTED_ENVELOPE",
        "protected_market_values_can_only_fail_closed": True,
        "regime_parameters_sha256": "9"*64, "regime_warmup_bars": 1}
    return frame, native, window, {**body, "receipt_sha256": p["_sha"](body)}


class M5StateErrorEnvelopeMetadataTests(unittest.TestCase):
    def test_actual_envelope_uses_target_rows_and_rejects_base_m5_shape(self):
        p = planner_helpers()
        _, native, window, receipt = fixture(p)
        bounds = p["_planning_source_bounds"](native, window)
        actual = p["_validated_state_error_envelope"](receipt, native_prefix=bounds)
        self.assertEqual(actual, (100.0, 0.0001, 1))
        self.assertEqual(bounds["rows"], 3)
        self.assertEqual(native["prefix_rows"], 36)
        with self.assertRaises(p["ProductionSearchPlannerError"]) as caught:
            p["_validated_state_error_envelope"](receipt, native_prefix=native)
        self.assertEqual(caught.exception.code, "STATE_ERROR_ENVELOPE_INVALID")

    def test_price_hash_and_changed_geometry_refuse_after_resigning_metadata(self):
        p = planner_helpers()
        frame, native, window, receipt = fixture(p)
        changed = frame.copy(deep=True)
        changed.iloc[-1, changed.columns.get_loc("High")] = 1000.0
        self.assertNotEqual(p["_frame_sha256"](frame), p["_frame_sha256"](changed))
        self.assertGreater(p["_state_price_error_bound"](changed),
                           float.fromhex(receipt["initial_error_bound_hex"]))
        invalid_prices = frame.copy(deep=True)
        invalid_prices.iloc[0, 0] = float("nan")
        with self.assertRaises(p["ProductionSearchPlannerError"]) as invalid:
            p["_state_price_error_bound"](invalid_prices)
        self.assertEqual(invalid.exception.code, "STATE_PRICE_SURFACE_INVALID")
        mutations = (("source_rows", 36), ("post_cutoff_rows", 1),
            ("source_last_close_utc", "2024-01-02T04:00:00Z"),
            ("preprotected_cutoff_utc", "2024-01-02T02:00:00Z"),
            ("four_fold_protected_axis_available", 0))
        for key, value in mutations:
            modified = {name: item for name, item in receipt.items() if name != "receipt_sha256"}
            modified[key] = value
            modified["receipt_sha256"] = p["_sha"](modified)
            with self.subTest(field=key):
                with self.assertRaises(p["ProductionSearchPlannerError"]) as refused:
                    p["_validated_state_error_envelope"](modified,
                        native_prefix=p["_planning_source_bounds"](native, window))
                self.assertEqual(refused.exception.code, "STATE_ERROR_ENVELOPE_INVALID")

    def test_explicit_dispatch_and_native_call_compatibility_with_missing_request_refusal(self):
        current = function(EXECUTION, "_reverify_cell_state_error_envelope")
        original = function(OLD_EXECUTION, "_reverify_cell_state_error_envelope")
        work = next(node for node in current.body if isinstance(node, ast.Try))
        dispatch = next(node for node in work.body if isinstance(node, ast.If))
        condition = ast.unparse(dispatch.test)
        self.assertIn("type(declared_request) is historical.DeclaredSlotProducerRequest", condition)
        self.assertIn("historical.declared_slot_source_kind(declared_request.to_dict())", condition)
        self.assertIn("'m5_derived'", condition)
        m5_call = next(node for node in ast.walk(dispatch.body[-1])
            if isinstance(node, ast.Call))
        window_keyword = next(keyword for keyword in m5_call.keywords
            if keyword.arg == "m5_view_window")
        self.assertEqual(ast.unparse(window_keyword.value), "receipt['m5_view_window']")
        old_call = next(node for node in ast.walk(original)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == "verify_preprotected_state_error_envelope")
        native_call = next(node for node in ast.walk(dispatch.orelse[-1])
            if isinstance(node, ast.Call))
        self.assertEqual(ast.dump(native_call, include_attributes=False),
                         ast.dump(old_call, include_attributes=False))
        public = function(PLANNER, "verify_preprotected_state_error_envelope")
        explicit = next(node for node in public.body if isinstance(node, ast.If))
        self.assertEqual(ast.unparse(explicit.test), "m5_view_window is not None")
        self.assertTrue(any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == "_validated_m5_frame" for node in ast.walk(explicit)))
        freeze = next(node for node in ast.walk(explicit) if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name) and node.func.id == "_freeze_state_error_envelope")
        self.assertEqual(ast.unparse(next(keyword.value for keyword in freeze.keywords
            if keyword.arg == "m5_view_window")), "actual_window")
        e = selected(EXECUTION, {"AuthoritativeSearchExecutionError", "_fail"})
        # Negative-only original branch: no request or capability is constructed.
        # Any public-verifier lookup would be a test failure, never a success stub.
        class NoPublicCalls:
            def __getattr__(self, name):
                raise AssertionError("public verifier must not be reached")
        ns = {**e, "declared_request": None,
            "historical": SimpleNamespace(DeclaredSlotProducerRequest=type("OpaqueUnusedType", (), {})),
            "receipt": {"source_kind": "m5_derived"}, "planner": NoPublicCalls(), "fitted": None}
        with self.assertRaises(e["AuthoritativeSearchExecutionError"]) as refused:
            exec(compile(ast.fix_missing_locations(ast.Module(
                body=[deepcopy(dispatch)], type_ignores=[])), str(EXECUTION), "exec"), ns)
        self.assertEqual(refused.exception.code, "STATE_ERROR_ENVELOPE_M5_WINDOW_INVALID")


if __name__ == "__main__":
    unittest.main()
