"""Private support-window computations: SYNTHETIC_NO_NATIVE_VERIFICATION.

Toy pandas frames and SimpleNamespace metadata carry no native seal, issuer,
source validation or financial evidence. No detector or public fitter runs.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

import pandas as pd


CANDIDATE_ROOT = Path(__file__).resolve().parents[1]
EXECUTION_PATH = CANDIDATE_ROOT / "pm_authoritative_search_execution.py"
candidate_path = str(CANDIDATE_ROOT)
if candidate_path not in sys.path:
    sys.path.insert(0, candidate_path)

import pm_authoritative_search_execution as execution

if Path(execution.__file__).resolve() != EXECUTION_PATH.resolve():
    raise RuntimeError("SYNTHETIC support-window tests require the exact execution candidate")


PROVENANCE = "SYNTHETIC_NO_NATIVE_VERIFICATION"


def _sha(label):
    return hashlib.sha256((PROVENANCE + ":" + label).encode("ascii")).hexdigest()


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def _frame():
    opens = pd.date_range("2024-01-02T00:00:00+00:00", periods=4, freq="15min")
    return pd.DataFrame({"Open": [1.0, 1.1, 1.2, 1.3], "High": [1.2, 1.3, 1.4, 1.5],
        "Low": [0.9, 1.0, 1.1, 1.2], "Close": [1.1, 1.2, 1.3, 1.4],
        "Volume": [3, 3, 3, 3], "Spread": [4, 4, 4, 4],
        "_BAR_CLOSE_TS": opens + pd.Timedelta(minutes=15)}, index=opens)


def _surface(frame):
    return pd.DataFrame({"REGIME_LIVE": ["TREND"] * len(frame),
        "REGIME_WARMUP": [False] * len(frame)}, index=frame.index)


def _product(*, rows=12):
    # This value is accepted ONLY by the private arithmetic/binding helper.
    return SimpleNamespace(index_sha256=_sha("M5_PREFIX_INDEX_METADATA"), native_prefix={
        "activation_id": "SYNTHETIC_NOT_AUTHENTICATED",
        "activation_manifest_sha256": _sha("M5_ACTIVATION_METADATA"),
        "artifact_sha256": _sha("BASE_M5_SOURCE_BYTES_METADATA"),
        "prefix_content_sha256": _sha("M5_PREFIX_CONTENT_METADATA"),
        "prefix_start_utc": "2024-01-02T00:00:00Z",
        "prefix_end_exclusive_utc": "2024-01-02T01:00:00Z", "prefix_rows": rows,
        "source_accessed_at_utc": "2026-11-03T00:00:00Z"})


def _evidence(product):
    return {"source_kind": "m5_derived", "base_timeframe": "M5", "view_timeframe": "M15",
        "m5_view_contract_sha256": _sha("VIEW_CONTRACT_METADATA"), "view_key": _sha("VIEW_KEY_METADATA"),
        "view_payload_sha256": _sha("VIEW_PAYLOAD_METADATA"),
        "view_membership_sha256": _sha("VIEW_MEMBERSHIP_METADATA"),
        "base_native_prefix_index_sha256": product.index_sha256,
        "source_accessed_at_utc": product.native_prefix["source_accessed_at_utc"]}


def _old_window(product, timeframe):
    native = product.native_prefix
    return {"schema_version": "fxpm.historical-regime-fit-window.v1", "symbol": "UNITUSD",
        "timeframe": timeframe, "prefix_content_sha256": native["prefix_content_sha256"],
        "prefix_start_utc": native["prefix_start_utc"],
        "prefix_end_exclusive_utc": native["prefix_end_exclusive_utc"], "prefix_rows": native["prefix_rows"]}


class M5RegimeSupportWindowTests(unittest.TestCase):
    def support(self, frame, product, *, evidence=None, surface=None, timeframe="M15"):
        return execution._build_train_regime_support(frame=frame,
            surface=_surface(frame) if surface is None else surface, product=product,
            symbol="UNITUSD", timeframe=timeframe, parameters_sha256=_sha("PARAMETERS"),
            detector_code_sha256=_sha("DETECTOR_CODE_METADATA_ONLY"), m5_view_evidence=evidence)

    def assert_code(self, code, callback, *args, **kwargs):
        with self.assertRaises(execution.AuthoritativeSearchExecutionError) as caught:
            callback(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_native_window_hash_keeps_the_existing_contract(self):
        frame = _frame();product = _product(rows=len(frame))
        receipt = self.support(frame, product)
        self.assertEqual(receipt["bindings"]["train_window_sha256"], _digest(_old_window(product, "M15")))
        self.assertEqual(receipt["bindings"]["source_sha256"], product.native_prefix["artifact_sha256"])
        changed = frame.copy(deep=True);changed.loc[changed.index[0], "Close"] = 1.15
        self.assertEqual(self.support(changed, product)["bindings"]["train_window_sha256"],
            receipt["bindings"]["train_window_sha256"])

    def test_derived_window_is_not_a_relabelled_m5_prefix_and_keeps_base_source(self):
        frame = _frame();product = _product();evidence = _evidence(product)
        receipt = self.support(frame, product, evidence=evidence)
        self.assertNotEqual(len(frame), product.native_prefix["prefix_rows"])
        self.assertNotEqual(receipt["bindings"]["train_window_sha256"], _digest(_old_window(product, "M15")))
        self.assertEqual(receipt["bindings"]["source_sha256"], product.native_prefix["artifact_sha256"])
        self.assertEqual(receipt["bindings"]["activation_manifest_sha256"],
            product.native_prefix["activation_manifest_sha256"])

    def test_actual_view_values_rows_and_closes_change_hash_with_identical_m5_bindings(self):
        frame = _frame();product = _product();evidence = _evidence(product)
        baseline = self.support(frame, product, evidence=evidence)
        values = frame.copy(deep=True);values.loc[values.index[0], "Close"] = 1.15
        closes = frame.copy(deep=True)
        closes.loc[closes.index[-1], "_BAR_CLOSE_TS"] = closes["_BAR_CLOSE_TS"].iloc[-1] - pd.Timedelta(minutes=1)
        variants = (values, frame.iloc[:-1].copy(deep=True), closes)
        for changed in variants:
            with self.subTest(rows=len(changed), last_close=changed["_BAR_CLOSE_TS"].iloc[-1]):
                receipt = self.support(changed, product, evidence=evidence)
                self.assertNotEqual(receipt["bindings"]["train_window_sha256"],
                    baseline["bindings"]["train_window_sha256"])
                self.assertEqual(receipt["bindings"]["source_sha256"], baseline["bindings"]["source_sha256"])

    def test_wrong_surface_index_timeframe_and_declared_view_bindings_refuse(self):
        frame = _frame();product = _product();evidence = _evidence(product)
        shifted = _surface(frame);shifted.index = shifted.index + pd.Timedelta(minutes=1)
        self.assert_code("REGIME_SURFACE_INVALID", self.support, frame, product, evidence=evidence, surface=shifted)
        for changes in ({"view_timeframe": "M5"}, {"base_timeframe": "M15"},
                {"base_native_prefix_index_sha256": _sha("WRONG_PREFIX_INDEX")},
                {"source_accessed_at_utc": "2026-11-03T00:00:01Z"},
                {"m5_view_contract_sha256": "not-a-digest"}, {"view_payload_sha256": False}):
            with self.subTest(changes=changes):
                self.assert_code("M5_DERIVED_REGIME_WINDOW_INVALID", self.support, frame, product,
                    evidence={**evidence, **changes})
        missing = deepcopy(evidence);missing.pop("view_membership_sha256")
        self.assert_code("M5_DERIVED_REGIME_WINDOW_INVALID", self.support, frame, product, evidence=missing)
        self.assert_code("M5_DERIVED_REGIME_WINDOW_INVALID", self.support,
            frame.drop(columns=["_BAR_CLOSE_TS"]), product, evidence=evidence)


if __name__ == "__main__":
    unittest.main()
