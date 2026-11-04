"""SYNTHETIC_POST_SOURCE_ABORT_METADATA_ONLY_NO_NATIVE_AUTHORITY_PROOF.

These sources exercise ordinary metadata, actual path guards and AST control
boundaries. No original M5 owner/process/source capability or successful Native
verifier is manufactured. Genuine positive abort/retirement is unexercised.
"""
import ast
from copy import deepcopy
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "pm_historical_scheduler.py"


def definitions():
    tree = ast.parse(SOURCE.read_bytes(), filename=str(SOURCE))
    names = {"HistoricalSchedulerError", "_fail", "_declared_digest", "_declared_m5_utc",
        "_declared_m5_grid_utc", "_declared_m5_refusal_eligible", "_declared_m5_refusal_body",
        "_declared_m5_refusal_archived_row", "_declared_m5_source_abort_eligible",
        "_declared_m5_source_abort_paths_clear", "_declared_m5_capture_source_abort",
        "_declared_m5_source_abort_body", "_declared_m5_abort_grid_identity"}
    selected = [node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    if {node.name for node in selected} != names:
        raise RuntimeError("actual post-source abort helpers missing")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    scope = {"__name__": "synthetic_post_source_abort_metadata_only", "__file__": str(SOURCE)}
    exec(compile(ast.fix_missing_locations(ast.Module(
        body=[future, *selected], type_ignores=[])), str(SOURCE), "exec"), scope)
    return scope


def metadata():
    cell = {"cell_id": "c"*64, "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"}
    binding = {"generation_context_sha256": "1"*64, "run_id": "SYNTHETIC_DATA_NOT_ISSUED",
        "cell_id": cell["cell_id"], "cell_contract_sha256": "2"*64,
        "search_process_receipt_sha256": "3"*64, "precommit_sha256": "4"*64}
    job = {key: value for key, value in binding.items() if key != "cell_contract_sha256"}
    job.update(m5_view_contract_sha256="5"*64, config_sha256="6"*64,
        effective_config_sha256="7"*64, view_budgets_sha256="8"*64, view_plan_sha256="9"*64,
        required_result_keys=["SYNTHETIC_SLOT_1", "SYNTHETIC_ACCOUNT"], terminal_result_key="SYNTHETIC_ACCOUNT")
    flags = {key: False for key in ("qualification_authority", "capital_authority",
        "live_authority", "order_authority", "full_native_context_eligibility", "financial_consumer_ready")}
    report = {"schema_version": "fxpm.m5-abort-before-consumption.v1",
        "status": "ABORTED_SOURCE_BEFORE_CONSUMPTION", "reason": "SOURCE_HEADER_FAILURE",
        "binding": job, "budgets": {key: 1000 for key in ("max_source_rows", "max_source_bytes",
            "max_view_rows", "max_view_bytes", "max_cache_rows", "max_cache_bytes")},
        "job_binding_sha256": "a"*64, "view_plan_sha256": job["view_plan_sha256"],
        "source_identity": {"symbol": "XAGUSD", "timeframe": "M5",
            "activation_id": "SYNTHETIC_DATA_NOT_ISSUED", "activation_sha256": "b"*64,
            "bars_sha256": "c"*64, "sidecar_sha256": "d"*64, "provenance_sha256": "e"*64,
            "request_sha256": "f"*64, "point": 0.01, "spread_unit": "points"},
        "product_routes": [{"cutoff_utc": "2024-01-02T00:00:00Z",
            "source_accessed_at_utc": "2026-11-04T02:00:00Z", "index_sha256": "a"*64,
            "source_identity_sha256": "b"*64, "activation_id": "SYNTHETIC_DATA_NOT_ISSUED",
            "activation_manifest_sha256": "b"*64, "activation_entry_sha256": "c"*64}],
        "declared_view_ids": ["SYNTHETIC_VIEW_1"], "requests_all_pending": True,
        "active_leases_before_close": 0, "observed_consumer_bindings": [],
        "value_cache_empty_before_close": True, "prefix_routes_revalidated": True,
        "job_view_read_observations": [], "owner_closed": True,
        "source_complete": False, "job_complete": False, "retirement_authority": False,
        "caller_obligations_not_authenticated": ["ORIGINAL_DISPATCH_LEASE_HELD",
            "FRESH_PRE_CONTROLLER_PHASE", "ACTUAL_STOP_GRID_READBACK_AND_DISTINCT_RETIREMENT_FENCE"], **flags}
    return {"binding": binding, "workspace_owner_sha256": "8"*64,
        "result": {"status": "FAILED", "reason_code": "SOURCE_HEADER_FAILURE",
            "recorded_before_dispatch": False, "run_receipt": None, "account_descriptor": None},
        "slot": {"slot_id": "SYNTHETIC_SLOT_1", "activation_utc": "2024-01-02T00:00:00Z",
            "end_exclusive_utc": "2024-01-30T00:00:00Z", "native_cutoff_utc": "2024-01-02T00:00:00Z"},
        "cell": cell, "code_sha256": {"pm_historical_scheduler.py": "5"*64,
            "pm_end_grid.py": "6"*64, "pm_authority_spool.py": "7"*64},
        "recorded_at_utc": "2026-11-04T02:00:00Z", "abort_report": report}


class PostSourceAbortMetadataTests(unittest.TestCase):
    def test_only_fresh_unused_owner_before_controller_under_actual_lease_is_eligible(self):
        h = definitions()
        eligible = h["_declared_m5_source_abort_eligible"]
        facts = dict(resumed=False, source_attempted=True, owner_created=True,
            owner_present=True, controller_started=False, dispatch_lease_held=True, phase="SOURCE_HEADER")
        self.assertTrue(eligible(**facts))
        for name in ("resumed", "source_attempted", "owner_created", "owner_present",
                     "controller_started", "dispatch_lease_held"):
            changed = {**facts, name: not facts[name]}
            with self.subTest(changed_fact=name):
                self.assertFalse(eligible(**changed))
                changed[name] = int(facts[name])
                with self.assertRaises(h["HistoricalSchedulerError"]) as invalid:
                    eligible(**changed)
                self.assertEqual(invalid.exception.code, "DECLARED_M5_ABORT_FACTS_INVALID")
        for phase in ("PRE_SOURCE", "PRODUCER_CONTROLLER", "ACCOUNT", "FINALIZATION", None):
            self.assertFalse(eligible(**{**facts, "phase": phase}))

    def test_abort_fence_is_distinct_preserves_actual_report_and_never_claims_completion(self):
        h = definitions()
        values = metadata()
        original = deepcopy(values)
        body = h["_declared_m5_source_abort_body"](**values)
        self.assertEqual(values, original)
        self.assertIs(body["abort_report"], values["abort_report"])
        self.assertEqual(len(body["abort_report"]), 27)
        self.assertEqual(body["schema_version"], "fxpm.declared-m5-post-source-abort-fence.v1")
        self.assertEqual(body["refusal"]["status"], "REFUSED_SOURCE_NOT_COMPLETED")
        self.assertEqual(body["refusal"]["scheduler_status"], "FAILED")
        self.assertEqual(body["refusal"]["reason_code"], values["result"]["reason_code"])
        self.assertEqual(body["source_disposition"], {"stage": "FRESH_POST_SOURCE_PRE_CONTROLLER_ABORT",
            "fresh_workspace": True, "resumed": False, "source_acquisition_attempted": True,
            "source_owner_created": True, "source_products_acquired": True, "controller_started": False})
        self.assertIsNone(body["bound_fitted_header"])
        self.assertEqual(body["end_grid_rows"], {})  # Not yet a retirement-ready fence.
        for key in ("qualification_authority", "capital_authority", "live_authority",
                    "order_authority", "full_native_context_eligibility", "financial_consumer_ready"):
            self.assertIs(body[key], False)
        for key in ("source_complete", "job_complete", "retirement_authority"):
            self.assertIs(body["abort_report"][key], False)
        self.assertFalse(set(body) & {"source_summary", "account_descriptor", "consumer_binding_sha256",
            "source_contract_sha256", "run_receipt", "owner"})
        for mutation in ("reason", "process", "owner_open", "source_complete", "job_complete", "authority", "terminal"):
            bad = deepcopy(values)
            if mutation == "reason":
                bad["abort_report"]["reason"] = "OTHER_FAILURE"
            elif mutation == "process":
                bad["abort_report"]["binding"]["search_process_receipt_sha256"] = "e"*64
            elif mutation == "owner_open":
                bad["abort_report"]["owner_closed"] = False
            elif mutation in ("source_complete", "job_complete"):
                bad["abort_report"][mutation] = True
            elif mutation == "authority":
                bad["abort_report"]["retirement_authority"] = True
            else:
                bad["result"]["status"] = "TERMINAL_STOP"
            with self.subTest(mutation=mutation), self.assertRaises(h["HistoricalSchedulerError"]) as invalid:
                h["_declared_m5_source_abort_body"](**bad)
            self.assertEqual(invalid.exception.code, "DECLARED_M5_ABORT_REPORT_BINDING_INVALID")

    def test_grid_projection_uses_original_spool_eight_string_field_contract(self):
        import pm_authority_spool as spool
        self.assertEqual(Path(spool.__file__).resolve(), (ROOT / "pm_authority_spool.py").resolve())
        h = definitions()
        report = metadata()["abort_report"]
        original = deepcopy(report)
        projected = h["_declared_m5_abort_grid_identity"](report)
        expected = ("symbol", "timeframe", "activation_sha256", "bars_sha256",
            "sidecar_sha256", "provenance_sha256", "request_sha256", "spread_unit")
        self.assertEqual(spool.POST_SOURCE_ABORT_GRID_SOURCE_FIELDS, expected)
        self.assertEqual(tuple(projected), expected)
        self.assertEqual(projected, {key: report["source_identity"][key] for key in expected})
        self.assertEqual(len(projected), 8)
        self.assertNotIn("point", projected)
        self.assertNotIn("activation_id", projected)
        self.assertEqual(len(report["source_identity"]), 10)
        self.assertEqual(report, original)

    def test_plain_owner_cannot_manufacture_abort_and_ineligible_paths_do_not_touch_sources(self):
        h = definitions()
        args = dict(owner=object(), context=None, precommit=None, process=None, cell_id="c"*64,
            source_error=RuntimeError("SYNTHETIC_METADATA_ONLY"), resumed=False,
            source_attempted=True, owner_created=True, controller_started=False,
            dispatch_lease_held=True, phase="SOURCE_HEADER")
        with self.assertRaises(h["HistoricalSchedulerError"]) as invalid:
            h["_declared_m5_capture_source_abort"](**args)
        self.assertEqual(invalid.exception.code, "DECLARED_M5_ABORT_ORIGINAL_OWNER_REQUIRED")
        for name in ("resumed", "controller_started", "owner_created", "source_attempted", "dispatch_lease_held"):
            changed = {**args, name: not args[name]}
            self.assertIsNone(h["_declared_m5_capture_source_abort"](**changed))

    def test_abort_archive_restores_primary_refusal_and_recording_only_facts(self):
        h = definitions()
        values = metadata()
        body = h["_declared_m5_source_abort_body"](**values)
        record = {"result_kind": "POST_SOURCE_ABORT_SOURCE_NOT_COMPLETED",
            "terminal_refusal": body["refusal"], "generation_context_sha256": "1"*64,
            "retirement_receipt_path": "SYNTHETIC_PATH_NOT_ISSUED", "retirement_receipt_sha256": "2"*64,
            "source_fence": {"path": "SYNTHETIC_PATH_NOT_ISSUED", "sha256": "3"*64},
            "end_grid_rows": {}, "precommit_sha256": "4"*64,
            "source_abort": {"owner_closed": True, "source_complete": False, "job_complete": False,
                "retirement_authority": False}}
        result = h["_declared_m5_refusal_archived_row"]({}, record)
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["reason_code"], "SOURCE_HEADER_FAILURE")
        self.assertEqual(result["terminal_refusal_workspace_status"], "RELEASED_RECORDING_ONLY")
        self.assertEqual(result["source_abort"], record["source_abort"])
        self.assertIs(result["genuine_cold_reopen_available"], False)
        self.assertIsNone(result["run_receipt"])
        self.assertIsNone(result["account_descriptor"])
        self.assertNotIn("source_summary", result)

    def test_capture_is_inside_held_dispatch_lease_and_controller_flag_precedes_calls(self):
        tree = ast.parse(SOURCE.read_bytes())
        run = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
            and node.name == "_declared_m5_run_one_cell")
        held = next(node for node in ast.walk(run) if isinstance(node, ast.With)
            and any(isinstance(item.context_expr, ast.Call)
                and isinstance(item.context_expr.func, ast.Name)
                and item.context_expr.func.id == "_declared_dispatch_lease" for item in node.items))
        captured = [node for node in ast.walk(held) if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name) and node.func.id == "_declared_m5_capture_source_abort"]
        self.assertEqual(len(captured), 1)
        self.assertEqual(ast.unparse(next(kw.value for kw in captured[0].keywords
            if kw.arg == "dispatch_lease_held")), "acquired")
        caught = next(node for node in ast.walk(held) if isinstance(node, ast.ExceptHandler)
            and node.name == "source_error")
        self.assertTrue(any(isinstance(node, ast.Raise) and node.exc is None for node in caught.body))
        flag = next(node for node in ast.walk(held) if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "controller_started"
                for target in node.targets) and isinstance(node.value, ast.Constant) and node.value.value is True)
        controllers = [node for node in ast.walk(held) if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute) and node.func.attr in {
                "execute_declared_cell_owner_slots", "reopen_declared_cell_owner_progress"}]
        self.assertEqual(len(controllers), 2)
        for call in controllers:
            self.assertLess(flag.lineno, call.lineno)
        close = next(node for node in ast.walk(run) if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute) and node.func.attr == "close")
        self.assertGreater(close.lineno, captured[0].lineno)
        final = next(node for node in ast.walk(run) if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name) and node.func.id == "_declared_m5_record_terminal_refusal")
        self.assertEqual(ast.unparse(next(kw.value for kw in final.keywords if kw.arg == "abort_report")),
            "abort_report")
        reuse = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
            and node.name == "_declared_m5_existing_refusal_record")
        self.assertIn("fxpm.declared-m5-post-source-abort-fence.v1",
            {node.value for node in ast.walk(reuse) if isinstance(node, ast.Constant) and type(node.value) is str})


class PostSourceAbortOwnedPathTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        value = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if value is None:
            raise unittest.SkipTest("requires explicitly precreated retained QA root")
        root = Path(value)
        if (not value or not root.is_absolute() or not root.is_dir() or root.is_symlink()
                or getattr(root.stat(), "st_file_attributes", 0) & 0x400):
            raise RuntimeError("FXPM_DERIVED_QA_ROOT must be existing absolute nonreparse owned directory")
        for component in (root, *root.parents):
            if component.is_symlink() or getattr(component.lstat(), "st_file_attributes", 0) & 0x400:
                raise RuntimeError("FXPM_DERIVED_QA_ROOT reparse chain refused")
        cls.parent = root

    def test_every_original_progress_namespace_or_present_header_retains(self):
        import pm_authority_spool as spool
        import pm_historical_search_producer as producer
        self.assertEqual(Path(spool.__file__).resolve(), (ROOT / "pm_authority_spool.py").resolve())
        self.assertEqual(Path(producer.__file__).resolve(), (ROOT / "pm_historical_search_producer.py").resolve())
        h = definitions()
        precommit = SimpleNamespace(receipt_sha256="a"*64)  # Routing DATA, not an issued precommit.
        cell_id = "b"*64
        base = f"declared-slots/{precommit.receipt_sha256}/{cell_id}"
        names = (base + "/attempts", base + "/commits", base + "/run.lock",
            producer._declared_run_path(precommit, cell_id),
            producer._declared_terminal_path(precommit, cell_id),
            f"declared-slots/{precommit.receipt_sha256}/requests/{cell_id}",
            producer._declared_m5_cell_source_path(precommit, cell_id))
        for name in names:
            with self.subTest(present_namespace=name):
                case = self.parent / ("post-source-path-" + uuid.uuid4().hex)
                case.mkdir(exist_ok=False)
                context = SimpleNamespace(root=case)  # Ordinary owned path guard only.
                self.assertTrue(h["_declared_m5_source_abort_paths_clear"](context, precommit, cell_id))
                path = spool._local_path(case, name)
                path.parent.mkdir(parents=True, exist_ok=True)
                if name.endswith(("/attempts", "/commits", "/requests/" + cell_id)):
                    path.mkdir(exist_ok=False)
                else:
                    path.write_bytes(b"SYNTHETIC_PRESENCE_ONLY_NOT_A_HEADER_OR_CAPABILITY")
                before = path.lstat()
                self.assertFalse(h["_declared_m5_source_abort_paths_clear"](context, precommit, cell_id))
                self.assertEqual(path.lstat().st_mtime_ns, before.st_mtime_ns)


if __name__ == "__main__":
    unittest.main()
