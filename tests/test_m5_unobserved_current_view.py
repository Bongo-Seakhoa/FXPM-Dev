"""SYNTHETIC_METADATA_COMPATIBILITY_ONLY_NO_NATIVE_OR_MODEL_POSITIVE_PROOF.

Actual original recipe AST and invented PRICE/metadata values exercise the lower
hash/clock joins. No NativePrefixProduct, fitted capability or verifier is faked.
"""
import ast
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

import pandas as pd


_REPAIR = Path(__file__).resolve().parents[1]
_ORIGINAL = Path(__file__).resolve().parent / "fixtures/m5_original_metadata_recipes.source.txt"


def _definitions(path, names, namespace, constants=()):
    tree = ast.parse(path.read_bytes(), filename=str(path))
    selected = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
        and node.name in names]
    if {node.name for node in selected} != set(names):
        raise RuntimeError("actual metadata helper definitions are missing")
    declarations = [node for node in tree.body if isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(isinstance(target, ast.Name) and target.id in constants
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target]))]
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    scope = {"__name__": "synthetic_metadata_recipe_checks", **namespace}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *declarations, *selected],
        type_ignores=[])), str(path), "exec"), scope)
    return scope


def _helpers():
    execution = _definitions(_REPAIR / "pm_authoritative_search_execution.py",
        {"AuthoritativeSearchExecutionError", "_fail", "_canonical_bytes", "_sha256", "_hex64",
         "_frame_sha256", "_train_regime_support_window", "_regime_fit_evidence"},
        {"pd": pd, "hashlib": hashlib, "json": json}, {"_SHA", "REGIME_FIT_EVIDENCE_VERSION"})
    adapter = _definitions(_REPAIR / "pm_historical_production_adapter.py",
        {"HistoricalProductionAdapterError", "_fail", "_canonical_bytes", "_sha256",
         "_declared_slot_detector_cutoff", "_declared_m5_unobserved_current_view"},
        {"datetime": datetime, "timedelta": timedelta, "json": json, "hashlib": hashlib})
    return SimpleNamespace(**execution), SimpleNamespace(**adapter)


def _original_recipes(execution):
    """Extract only the original hot metadata statements, never label/model code."""
    path = _ORIGINAL
    tree = ast.parse(path.read_bytes(), filename=str(path))
    support = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
        and node.name == "_build_train_regime_support")
    native = next(node for node in support.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "native" for target in node.targets))
    window = next(node for node in support.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "window" for target in node.targets))
    m5 = next(node for node in support.body if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare) and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "m5_view_evidence")
    hot = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
        and node.name == "_fit_verified_search_frame")
    evidence = next(node for node in hot.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "evidence" for target in node.targets))
    derived = next(node for node in hot.body if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare) and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "derived_binding")
    windows = ast.parse("def original_window(*, frame, product, symbol, timeframe, m5_view_evidence=None):\n    pass\n").body[0]
    windows.body = deepcopy([native, window, m5]) + [ast.Return(value=ast.Name(id="window", ctx=ast.Load()))]
    fits = ast.parse("def original_evidence(*, source, product, support, detector_record, derived_binding=None):\n    pass\n").body[0]
    fits.body = deepcopy([evidence, derived]) + [ast.Return(value=ast.Name(id="evidence", ctx=ast.Load()))]
    scope = dict(execution.__dict__)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[windows, fits], type_ignores=[])),
        str(path), "exec"), scope)
    return SimpleNamespace(**scope)


def _fixture():
    frame = pd.DataFrame({"Open": [1.0, 1.1, 1.2], "Close": [1.05, 1.15, 1.25],
        "_BAR_CLOSE_TS": pd.to_datetime(["2024-01-02T01:00:00+00:00", "2024-01-02T02:00:00+00:00",
                                         "2024-01-02T03:00:00+00:00"], utc=True)},
        index=pd.DatetimeIndex(["2024-01-02T00:00:00+00:00", "2024-01-02T01:00:00+00:00",
                                "2024-01-02T02:00:00+00:00"], dtype="datetime64[ns, UTC]", name="time"))
    native = {"prefix_content_sha256": "1"*64, "prefix_start_utc": "2024-01-02T00:00:00Z",
        "prefix_end_exclusive_utc": "2024-01-02T03:00:00Z", "prefix_rows": 36,
        "source_accessed_at_utc": "2026-11-03T00:00:01Z"}
    product = SimpleNamespace(native_prefix=native, index_sha256="2"*64)
    view = {"source_kind": "m5_derived", "base_timeframe": "M5", "view_timeframe": "H1",
        "m5_view_contract_sha256": "3"*64, "view_key": "4"*64, "view_payload_sha256": "5"*64,
        "view_membership_sha256": "6"*64, "base_native_prefix_index_sha256": product.index_sha256,
        "source_accessed_at_utc": native["source_accessed_at_utc"]}
    source = SimpleNamespace(receipt_sha256="7"*64)
    support = {"receipt_sha256": "8"*64, "eligible_rows": 2,
        "first_ts": pd.Timestamp(frame.index[0]).isoformat(), "last_ts": pd.Timestamp(frame.index[1]).isoformat(),
        "bindings": {}}
    detector = {"record_sha256": "9"*64,
        "fit_cutoff_utc": pd.Timestamp(frame["_BAR_CLOSE_TS"].iloc[-1]).isoformat()}
    derived = SimpleNamespace(view_evidence=view)
    window = {"view_start_utc": pd.Timestamp(frame.index[0]).isoformat(),
        "view_end_exclusive_utc": detector["fit_cutoff_utc"],
        "view_last_close_utc": detector["fit_cutoff_utc"], "view_rows": len(frame)}
    return frame, product, source, support, detector, derived, window


def _current_metadata(execution, values):
    frame, product, source, support, detector, derived, window = values
    train_window = execution._train_regime_support_window(frame=frame, product=product,
        symbol="UNITUSD", timeframe="H1", m5_view_evidence=dict(derived.view_evidence))
    fit_evidence = execution._regime_fit_evidence(source=source, product=product, support=support,
        detector_record=detector, derived_binding=derived)
    return train_window, fit_evidence


class M5UnobservedCurrentViewTests(unittest.TestCase):
    def test_original_native_and_m5_metadata_shapes_and_hashes_are_unchanged(self):
        execution, _ = _helpers()
        original = _original_recipes(execution)
        frame, product, source, support, detector, derived, _ = _fixture()
        for view in (None, dict(derived.view_evidence)):
            with self.subTest(m5=view is not None):
                arguments = dict(frame=frame, product=product, symbol="UNITUSD", timeframe="H1", m5_view_evidence=view)
                actual = execution._train_regime_support_window(**arguments)
                expected = original.original_window(**arguments)
                self.assertEqual(actual, expected)
                self.assertEqual(execution._canonical_bytes(actual), execution._canonical_bytes(expected))
                self.assertEqual(execution._sha256(actual), execution._sha256(expected))
                self.assertEqual(actual["schema_version"], "fxpm.historical-regime-fit-window.v1" if view is None
                    else "fxpm.historical-m5-derived-regime-fit-window.v1")
                if view is None:
                    self.assertNotIn("view_binding", actual)
                    self.assertNotIn("base_m5_prefix", actual)
                else:
                    self.assertEqual((actual["view_rows"], actual["base_m5_prefix"]["prefix_rows"]), (3, 36))
                fits = dict(source=source, product=product, support=support,
                    detector_record=detector, derived_binding=None if view is None else derived)
                actual_fit, expected_fit = execution._regime_fit_evidence(**fits), original.original_evidence(**fits)
                self.assertEqual(actual_fit, expected_fit)
                self.assertEqual(execution._sha256(actual_fit), execution._sha256(expected_fit))
                self.assertEqual("m5_view" in actual_fit, view is not None)

    def test_actual_plus00_window_and_support_clocks_pass_new_current_view_join_without_rewriting(self):
        execution, adapter = _helpers()
        values = _fixture()
        _, _, _, support, detector, _, window = values
        train_window, evidence = _current_metadata(execution, values)
        support["bindings"]["train_window_sha256"] = execution._sha256(train_window)
        fit = {"regime_fit_evidence_sha256": execution._sha256(evidence), "regime_detector_record": detector}
        before = deepcopy((support, fit, window))
        self.assertTrue(all(value.endswith("+00:00") for value in
            (support["first_ts"], support["last_ts"], window["view_start_utc"], window["view_end_exclusive_utc"])))
        self.assertIsNone(adapter._declared_m5_unobserved_current_view(support=support, fit_body=fit,
            window=window, train_window=train_window, fit_evidence=evidence))
        self.assertEqual((support, fit, window), before)

    def test_wrong_current_price_binding_support_or_fit_evidence_hash_refuses(self):
        execution, adapter = _helpers()
        for change in ("price", "view", "support_window", "fit_evidence", "support_receipt", "source_receipt"):
            values = _fixture()
            frame, _, source, support, detector, derived, window = values
            saved_window, saved_evidence = _current_metadata(execution, values)
            support["bindings"]["train_window_sha256"] = execution._sha256(saved_window)
            fit = {"regime_fit_evidence_sha256": execution._sha256(saved_evidence), "regime_detector_record": detector}
            if change == "price":
                frame.iloc[0, frame.columns.get_loc("Close")] = 1.75
            elif change == "view":
                derived.view_evidence["view_payload_sha256"] = "a"*64
            elif change == "support_window":
                support["bindings"]["train_window_sha256"] = "b"*64
            elif change == "fit_evidence":
                fit["regime_fit_evidence_sha256"] = "b"*64
            elif change == "support_receipt":
                support["receipt_sha256"] = "b"*64
            else:
                source.receipt_sha256 = "b"*64
            current_window, current_evidence = _current_metadata(execution, values)
            with self.subTest(change=change), self.assertRaisesRegex(adapter.HistoricalProductionAdapterError,
                    "DECLARED_M5_UNOBSERVED_SUPPORT_INVALID"):
                adapter._declared_m5_unobserved_current_view(support=support, fit_body=fit, window=window,
                    train_window=current_window, fit_evidence=current_evidence)

    def test_current_view_join_keeps_canonical_utc_and_actual_support_bounds_strict(self):
        execution, adapter = _helpers()
        for change in ("z_tag", "offset", "before_view", "detector_end", "empty_support"):
            values = _fixture()
            _, _, _, support, detector, _, window = values
            train_window, evidence = _current_metadata(execution, values)
            support["bindings"]["train_window_sha256"] = execution._sha256(train_window)
            fit = {"regime_fit_evidence_sha256": execution._sha256(evidence), "regime_detector_record": detector}
            if change == "z_tag":
                window["view_last_close_utc"] = window["view_last_close_utc"].replace("+00:00", "Z")
            elif change == "offset":
                support["first_ts"] = "2024-01-02T01:00:00+01:00"
            elif change == "before_view":
                support["first_ts"] = "2024-01-01T23:00:00+00:00"
            elif change == "detector_end":
                detector["fit_cutoff_utc"] = "2024-01-02T02:00:00+00:00"
            else:
                support["eligible_rows"] = 0
            with self.subTest(change=change), self.assertRaises(adapter.HistoricalProductionAdapterError):
                adapter._declared_m5_unobserved_current_view(support=support, fit_body=fit, window=window,
                    train_window=train_window, fit_evidence=evidence)


if __name__ == "__main__":
    unittest.main()
