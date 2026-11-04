"""SYNTHETIC_METADATA_AND_SOURCE_CONTRACT_ONLY, not a native cold witness.

Malformed stored-byte fixtures exercise refusals before issuance. AST checks
protect the no-writer branch; no NativePrefixProduct, Fitted, sink or context
positive is manufactured. No filesystem fixture, model or detector is used.
"""
import ast
import hashlib
import json
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]

import pm_training_horizon as horizon
import pm_production_search_planner as planner

for module, filename in ((horizon, "pm_training_horizon.py"), (planner, "pm_production_search_planner.py")):
    if Path(module.__file__).resolve() != (REPO_ROOT / filename).resolve():
        raise RuntimeError("Readonly M5 tests require repository module origins")


PROVENANCE = "SYNTHETIC_METADATA_AND_SOURCE_CONTRACT_ONLY"


def _encoded(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False,
        sort_keys=True, separators=(",", ":")).encode("ascii")


def _sha(value):
    return hashlib.sha256(value).hexdigest()


class M5PlanReadonlyReopenTests(unittest.TestCase):
    def test_missing_wrong_schema_and_changed_digest_stop_before_reconstruction(self):
        for encoded, expected in ((b"", "0" * 64), (_encoded({}), _sha(_encoded({}))),
                (_encoded({"schema_version": "SYNTHETIC_WRONG_SCHEMA"}), "0" * 64)):
            with self.subTest(encoded=encoded), self.assertRaises(horizon._HorizonContractError):
                horizon._rebuild_m5_stored_plan_body(encoded, expected)

    def test_original_stored_stamp_cannot_be_replaced_under_its_existing_hash(self):
        body = {"schema_version": "fxpm.m5-derived-historical-horizon-plan.v1", "source_kind": "m5_derived",
            "anchor": {"evidence_accessed_at_utc": "2026-11-03T00:00:00Z"}, "provenance": PROVENANCE}
        original = _encoded(body)
        changed = {**body, "anchor": {"evidence_accessed_at_utc": "2026-11-03T00:00:01Z"}}
        with self.assertRaises(horizon._HorizonContractError) as caught:
            horizon._rebuild_m5_stored_plan_body(_encoded(changed), _sha(original))
        self.assertIn("digest changed", str(caught.exception))

    def test_noncanonical_stored_bytes_cannot_be_silently_recreated(self):
        for encoded in (b'{ "schema_version": "unit" }', b'{"a":1,"a":1}'):
            with self.subTest(encoded=encoded), self.assertRaises(horizon._HorizonContractError) as caught:
                horizon._rebuild_m5_stored_plan_body(encoded, _sha(encoded))
            self.assertIn("not canonical", str(caught.exception))

    def test_readonly_source_branch_has_no_writer_builder_or_fresh_issuer_fallback(self):
        source = Path(planner.__file__).read_text(encoding="utf-8")
        functions = {node.name: node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)}
        reopen = functions["reopen_declared_slot_m5_derived_observed_cell_plan"]
        calls = {ast.unparse(node.func) for node in ast.walk(reopen) if isinstance(node, ast.Call)}
        forbidden = {"_build_observed_cell_plan", "_build_observed_cell_plan_source",
            "build_declared_slot_m5_derived_observed_cell_plan", "authority_spool.open_symbol_sink",
            "authority_spool.freeze_generation_context", "freeze_production_search_process"}
        self.assertFalse(calls & forbidden)
        self.assertFalse(any(name.endswith((".put_json", ".register_artifact", ".now", ".utcnow")) for name in calls))
        verifier = functions["_verify_production_cell_search_plan_source"]
        branch = next(node for node in ast.walk(verifier) if isinstance(node, ast.If)
            and isinstance(node.test, ast.Name) and node.test.id == "read_only")
        readonly_calls = {ast.unparse(node.func) for statement in branch.body
            for node in ast.walk(statement) if isinstance(node, ast.Call)}
        self.assertIn("_read_existing_m5_plan_json", readonly_calls)
        self.assertFalse(any(name.endswith((".put_json", ".register_artifact")) for name in readonly_calls))
        verifier_calls = [node for node in ast.walk(reopen) if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name) and node.func.id == "_verify_production_cell_search_plan_source"]
        self.assertEqual(len(verifier_calls), 1)
        self.assertTrue(any(keyword.arg == "read_only" and isinstance(keyword.value, ast.Constant)
            and keyword.value.value is True for keyword in verifier_calls[0].keywords))


if __name__ == "__main__":
    unittest.main()
