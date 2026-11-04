"""Invented provenance DATA only; no Native product, Raw seal, or financial proof."""
import ast
from copy import deepcopy
from pathlib import Path
import unittest


def _helpers():
    source = Path(__file__).resolve().parents[1] / "pm_winner_raw_evidence.py"
    tree = ast.parse(source.read_bytes(), filename=str(source))
    wanted = {"WinnerRawEvidenceError", "_fail", "_validate_oos_source_content_binding"}
    body = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in wanted]
    if {n.name for n in body} != wanted:
        raise RuntimeError("RAW_REPAIR_HELPERS_CHANGED")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    namespace = {"__name__": "invented_provenance_data_only"}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *body], type_ignores=[])), str(source), "exec"), namespace)
    return namespace


class OosSourceSnapshotBindingTests(unittest.TestCase):
    def test_native_content_snapshot_rule_is_unchanged(self):
        helpers = _helpers()
        validate = helpers["_validate_oos_source_content_binding"]
        validate({"native_prefix_content_sha256": "a"*64}, {"source_snapshot_sha256": "a"*64}, None)
        with self.assertRaisesRegex(helpers["WinnerRawEvidenceError"], "OOS_SOURCE_PROVENANCE_SPLICE"):
            validate({"native_prefix_content_sha256": "a"*64}, {"source_snapshot_sha256": "b"*64}, None)

    def test_derived_base_content_and_view_payload_have_separate_meanings(self):
        helpers = _helpers()
        source = {"native_prefix_content_sha256": "a"*64, "native_prefix_index_sha256": "b"*64}
        horizon = {"source_snapshot_sha256": "c"*64}
        window = {"base_native_prefix_content_sha256": "a"*64,
                  "base_native_prefix_index_sha256": "b"*64, "view_payload_sha256": "c"*64}
        before = deepcopy((source, horizon, window))
        helpers["_validate_oos_source_content_binding"](source, horizon, window)
        self.assertEqual((source, horizon, window), before)
        self.assertNotEqual(source["native_prefix_content_sha256"], horizon["source_snapshot_sha256"])

    def test_derived_base_view_aliases_and_changed_index_are_refused(self):
        helpers = _helpers()
        for field in ("base_content", "index", "view_payload", "old_native_alias"):
            source = {"native_prefix_content_sha256": "a"*64, "native_prefix_index_sha256": "b"*64}
            horizon = {"source_snapshot_sha256": "c"*64}
            window = {"base_native_prefix_content_sha256": "a"*64,
                      "base_native_prefix_index_sha256": "b"*64, "view_payload_sha256": "c"*64}
            if field == "base_content":
                source["native_prefix_content_sha256"] = "d"*64
            elif field == "index":
                source["native_prefix_index_sha256"] = "d"*64
            elif field == "view_payload":
                horizon["source_snapshot_sha256"] = "d"*64
            else:
                source["native_prefix_content_sha256"] = "c"*64
            with self.subTest(field=field), self.assertRaisesRegex(helpers["WinnerRawEvidenceError"], "OOS_SOURCE_PROVENANCE_SPLICE"):
                helpers["_validate_oos_source_content_binding"](source, horizon, window)
