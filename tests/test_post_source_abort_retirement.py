"""Owned software I/O with invented abort DATA, never an issued M5 owner report.

These fixtures test retirement guards, not native verification, process issuance,
runtime phase authentication, or the real owner's positive abort/close boundary.
"""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import unittest
import uuid

import pm_authority_spool as spool
import pm_book_multiplicity as book
import pm_end_grid as grid


LABEL = "INVENTED_ABORT_DATA_NOT_ISSUED_NO_NATIVE_PROCESS_OR_FINANCIAL_VERIFICATION"
FLAGS = ("qualification_authority", "capital_authority", "live_authority", "order_authority",
    "full_native_context_eligibility", "financial_consumer_ready")


def _sha(value):
    return hashlib.sha256(spool._canonical_json_bytes(value, 4 * 1024**2)).hexdigest()


def _owner_data_digest(value):
    return hashlib.sha256(json.dumps(value, allow_nan=False, sort_keys=True,
        ensure_ascii=True, separators=(",", ":")).encode("ascii")).hexdigest()


def _write(path, value):
    encoded = spool._canonical_json_bytes(value, 4 * 1024**2)
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    if path.read_bytes() != encoded:
        raise AssertionError("owned abort metadata readback changed")
    return hashlib.sha256(encoded).hexdigest()


class PostSourceAbortRetirementTests(unittest.TestCase):
    def setUp(self):
        configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if configured is None:
            self.skipTest("explicit retained FXPM_DERIVED_QA_ROOT is required")
        parent = Path(configured)
        self.assertTrue(configured and parent.is_absolute() and parent.is_dir())
        spool._reject_reparse_chain(parent)
        self.assertEqual(Path(spool.__file__).resolve(),
            Path(__file__).resolve().parents[1] / "pm_authority_spool.py")
        self.parent = parent / ("post_source_abort_" + uuid.uuid4().hex)
        self.parent.mkdir(exist_ok=False)
        (self.parent / "DATA_NOT_ISSUED.txt").write_text(LABEL, encoding="ascii")

    def fixture(self, *, fence_change=None, row_change=None, terminal_result_key="terminal"):
        case = self.parent / uuid.uuid4().hex
        case.mkdir()
        retained = case / "retained"
        retained.mkdir()
        limits = spool.SpoolLimits(max_artifact_bytes=1024**2, max_symbol_bytes=2 * 1024**2,
            max_artifacts_per_symbol=100, max_manifest_bytes=128 * 1024,
            max_json_bytes=256 * 1024, stream_chunk_bytes=64 * 1024,
            max_generation_bytes=4 * 1024**2, min_free_bytes=1)
        cell = book.compute_cell_id("XAGUSD", "H1", "TREND")
        contract = _sha({"fixture": LABEL, "kind": "cell-contract-DATA"})
        context = spool.freeze_generation_context(case / "working",
            generation_id="SOFTWARE_ABORT_" + uuid.uuid4().hex,
            native_universe_sha256=_sha({"fixture": LABEL, "kind": "NO_NATIVE"}),
            config_sha256=_sha({"fixture": LABEL, "kind": "config-DATA"}), limits=limits,
            owned_workspace={"cell_id": cell, "cell_contract_sha256": contract})
        process = _sha({"fixture": LABEL, "kind": "process-DATA-NOT-ISSUED"})
        precommit_body = {"fixture": LABEL, "kind": "precommit-DATA-NOT-ISSUED"}
        precommit = _sha(precommit_body)
        direct = f"declared-slots/{precommit}/precommit.json"
        precommit_path = context.root / direct
        precommit_path.parent.mkdir(parents=True)
        self.assertEqual(_write(precommit_path, precommit_body), precommit)
        sink = spool.open_symbol_sink(context, "ABORT_SOFTWARE_IO")
        payload = sink.put_json("owned-abort-payload", "software_fixture", {"fixture": LABEL})
        sink.seal()
        owner_sha = spool.read_owned_generation_workspace(context)[1]
        budgets = {key: value for key, value in (("max_source_rows", 6), ("max_source_bytes", 4096),
            ("max_view_rows", 2), ("max_view_bytes", 4096), ("max_cache_rows", 4), ("max_cache_bytes", 8192))}
        job = {"m5_view_contract_sha256": _sha({"fixture": LABEL, "kind": "view-contract-DATA"}),
            "generation_context_sha256": context.context_sha256, "config_sha256": context.config_sha256,
            "effective_config_sha256": _sha({"fixture": LABEL, "kind": "effective-config-DATA"}),
            "search_process_receipt_sha256": process, "precommit_sha256": precommit,
            "run_id": context.generation_id, "cell_id": cell, "view_budgets_sha256": _sha(budgets),
            "view_plan_sha256": _sha({"fixture": LABEL, "kind": "value-plan-DATA"}),
            "required_result_keys": ["software-slot-01", terminal_result_key], "terminal_result_key": terminal_result_key}
        source = {"symbol": "XAGUSD", "timeframe": "M5", "activation_id": "SOFTWARE_ACTIVATION_DATA",
            **{key: _sha({"fixture": LABEL, "kind": key}) for key in
                ("activation_sha256", "bars_sha256", "sidecar_sha256", "provenance_sha256", "request_sha256")},
            "point": 0.001, "spread_unit": "points"}
        report = {"schema_version": "fxpm.m5-abort-before-consumption.v1",
            "status": "ABORTED_SOURCE_BEFORE_CONSUMPTION", "reason": "SOFTWARE_ABORT_METADATA",
            "binding": job, "budgets": budgets, "job_binding_sha256": _owner_data_digest(job),
            "view_plan_sha256": job["view_plan_sha256"], "source_identity": source,
            "product_routes": [{"cutoff_utc": "2023-06-23T00:00:00+00:00",
                "source_accessed_at_utc": "2026-11-04T00:00:00+00:00",
                "index_sha256": _sha({"fixture": LABEL, "kind": "index-DATA"}),
                "source_identity_sha256": _sha({"fixture": LABEL, "kind": "native-source-identity-DATA"}),
                "activation_id": source["activation_id"], "activation_manifest_sha256": source["activation_sha256"],
                "activation_entry_sha256": _sha({"fixture": LABEL, "kind": "activation-entry-DATA"})}],
            "declared_view_ids": ["calibration", "selection"], "requests_all_pending": True,
            "active_leases_before_close": 0, "observed_consumer_bindings": [],
            "value_cache_empty_before_close": True, "prefix_routes_revalidated": True,
            "job_view_read_observations": [], "owner_closed": True,
            "source_complete": False, "job_complete": False, "retirement_authority": False,
            "caller_obligations_not_authenticated": ["ORIGINAL_DISPATCH_LEASE_HELD",
                "FRESH_PRE_CONTROLLER_PHASE", "ACTUAL_STOP_GRID_READBACK_AND_DISTINCT_RETIREMENT_FENCE"],
            **{key: False for key in FLAGS}}
        refusal = {"status": "REFUSED_SOURCE_NOT_COMPLETED", "scheduler_status": "FAILED",
            "reason_code": report["reason"], "recorded_at_utc": "2026-11-04T00:00:00+00:00",
            "slot_id": "software-slot-01", "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND",
            "cycle_start_utc": "2023-06-24T00:00:00+00:00", "cycle_end_utc": "2023-07-22T00:00:00+00:00",
            "information_cutoff_utc": "2023-06-23T00:00:00+00:00"}
        codes = {name: _sha({"fixture": LABEL, "code_identity_DATA": name}) for name in
            ("pm_historical_scheduler.py", "pm_end_grid.py", "pm_authority_spool.py")}
        fields = {key: refusal[key] for key in ("symbol", "timeframe", "regime", "cycle_start_utc",
            "cycle_end_utc", "information_cutoff_utc")}
        fields.update(outcome="STOP", reason_code=refusal["reason_code"], source_kind="m5_derived",
            source_contract_sha256=None, view_key=None, source_identity={key: source[key]
                for key in spool.POST_SOURCE_ABORT_GRID_SOURCE_FIELDS},
            job_binding_sha256=report["job_binding_sha256"], cost_tier="NONE",
            process_edition_sha256=process, code_sha256=codes, produced_at_utc=refusal["recorded_at_utc"])
        fields.update(row_change or {})
        row = grid.build_end_grid_row(**fields)
        row_receipt = grid.EndGridWriter(retained).append(row)
        self.assertEqual([value.row_sha256 for value in grid.read_end_grid(retained, "XAGUSD")], [row.row_sha256])
        body = {"schema_version": spool.POST_SOURCE_ABORT_FENCE_SCHEMA, "workspace_owner_sha256": owner_sha,
            "binding": {"generation_context_sha256": context.context_sha256, "run_id": context.generation_id,
                "cell_id": cell, "cell_contract_sha256": contract,
                "search_process_receipt_sha256": process, "precommit_sha256": precommit},
            "refusal": refusal, "abort_report": report, "bound_fitted_header": None,
            "source_disposition": {"stage": "FRESH_POST_SOURCE_PRE_CONTROLLER_ABORT", "fresh_workspace": True,
                "resumed": False, "source_acquisition_attempted": True, "source_owner_created": True,
                "source_products_acquired": True, "controller_started": False},
            "end_grid_rows": {refusal["slot_id"]: {"path": row_receipt["path"], "row_sha256": row.row_sha256}},
            "code_sha256": codes, **{key: False for key in FLAGS}}
        if fence_change is not None:
            fence_change(body)
        fence_path = retained / "post-source-abort.json"
        fence_sha = _write(fence_path, body)
        protected = case / "protected.txt"
        protected.write_text(LABEL, encoding="ascii")
        args = {"expected_context_sha256": context.context_sha256, "workspace_owner_sha256": owner_sha,
            "source_fence_path": fence_path, "source_fence_sha256": fence_sha,
            "retirement_receipt_path": retained / "retirement.json", "protected_paths": (protected, retained,
                Path(spool.__file__), Path(grid.__file__)), "owned_direct_files": (direct,), "unit_lock_paths": (),
            "max_paths": 100, "max_nodes": 300, "max_bytes": 4 * 1024**2}
        return context, payload, args, body

    def assert_retained(self, context, payload, args):
        self.assertTrue((context.root / payload["descriptor"]["relative_path"]).is_file())
        self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())
        self.assertFalse(args["retirement_receipt_path"].exists())

    def test_original_stop_and_owned_spool_roundtrip_record_abort_without_completion(self):
        self._assert_abort_roundtrip("terminal")
        self._assert_abort_roundtrip("résultat")

    def _assert_abort_roundtrip(self, terminal_result_key):
        context, payload, args, body = self.fixture(terminal_result_key=terminal_result_key)
        reference = body["end_grid_rows"][body["refusal"]["slot_id"]]
        grid_path = Path(reference["path"])
        original_row, = grid.read_end_grid(grid_path.parent, grid_path.stem)
        expected_keys = {"symbol", "timeframe", "activation_sha256", "bars_sha256",
            "sidecar_sha256", "provenance_sha256", "request_sha256", "spread_unit"}
        self.assertEqual(set(original_row.source_identity), expected_keys)
        self.assertEqual(len(original_row.source_identity), 8)
        self.assertLessEqual(len(original_row.source_identity), grid.MAX_SOURCE_IDENTITY_ENTRIES)
        self.assertEqual(len(body["abort_report"]["source_identity"]), 10)
        before = args["source_fence_path"].read_bytes()
        result = spool.retire_completed_generation(context.root, **args)
        self.assertEqual(result["status"], "ARCHIVED_RECORD_ONLY")
        self.assertEqual(result["result_kind"], spool.POST_SOURCE_ABORT_RESULT_KIND)
        self.assertEqual(result["terminal_refusal"], body["refusal"])
        summary = result["source_abort"]
        self.assertEqual(summary["report_sha256"], _sha(body["abort_report"]))
        self.assertEqual(summary["job_binding_sha256"], body["abort_report"]["job_binding_sha256"])
        self.assertTrue(summary["owner_closed"])
        for key in ("job_complete", "source_complete", "retirement_authority"):
            self.assertIs(summary[key], False)
        for key in FLAGS:
            self.assertIs(result[key], False)
        self.assertNotIn("source_summary", body)
        self.assertNotIn("abort_report", result)
        self.assertFalse((context.root / payload["descriptor"]["relative_path"]).exists())
        self.assertEqual(args["source_fence_path"].read_bytes(), before)
        self.assertEqual(spool.read_generation_retirement(context.root), result)
        self.assertEqual(spool.retire_completed_generation(context.root, **args), result)

    def test_live_consumed_unclosed_or_authoritative_report_keeps_payload(self):
        changes = ({"owner_closed": False}, {"requests_all_pending": False}, {"active_leases_before_close": 1},
            {"active_leases_before_close": False}, {"observed_consumer_bindings": ["DATA"]},
            {"value_cache_empty_before_close": False}, {"prefix_routes_revalidated": False},
            {"job_view_read_observations": ["DATA"]}, {"source_complete": True}, {"job_complete": True},
            {"retirement_authority": True}, {"qualification_authority": True})
        for change in changes:
            context, payload, args, _ = self.fixture(fence_change=lambda body:
                body["abort_report"].update(change))
            with self.subTest(change=change), self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_ABORT_REPORT_INVALID"):
                spool.retire_completed_generation(context.root, **args)
            self.assert_retained(context, payload, args)

    def test_resumed_constructor_unknown_and_controller_started_are_not_eligible(self):
        for key, value in (("resumed", True), ("fresh_workspace", False), ("source_acquisition_attempted", False),
                ("source_owner_created", False), ("source_products_acquired", None), ("controller_started", True),
                ("controller_started", 0)):
            context, payload, args, _ = self.fixture(fence_change=lambda body:
                body["source_disposition"].update({key: value}))
            with self.subTest(key=key), self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_ABORT_SOURCE_STATE_INVALID"):
                spool.retire_completed_generation(context.root, **args)
            self.assert_retained(context, payload, args)

    def test_actual_context_precommit_plan_and_job_hash_bindings_cannot_be_spliced(self):
        changes = (lambda body: body["binding"].update(precommit_sha256="f" * 64),
            lambda body: body["abort_report"]["binding"].update(config_sha256="f" * 64),
            lambda body: body["abort_report"]["binding"].update(search_process_receipt_sha256="f" * 64),
            lambda body: body["abort_report"].update(view_plan_sha256="f" * 64),
            lambda body: body["abort_report"].update(job_binding_sha256="f" * 64))
        for change in changes:
            context, payload, args, _ = self.fixture(fence_change=change)
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_ABORT_BINDING_INVALID"):
                spool.retire_completed_generation(context.root, **args)
            self.assert_retained(context, payload, args)

    def test_source_routes_and_identity_metadata_have_no_native_hash_domain_alias(self):
        changes = (lambda body: body["abort_report"]["source_identity"].update(timeframe="H1"),
            lambda body: body["abort_report"]["source_identity"].update(point=True),
            lambda body: body["abort_report"]["product_routes"][0].update(activation_manifest_sha256="f" * 64),
            lambda body: body["abort_report"]["product_routes"][0].update(source_identity_sha256=None),
            lambda body: body["abort_report"]["product_routes"].append(deepcopy(body["abort_report"]["product_routes"][0])))
        for change in changes:
            context, payload, args, _ = self.fixture(fence_change=change)
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_ABORT_(SOURCE_IDENTITY|ROUTE)_INVALID"):
                spool.retire_completed_generation(context.root, **args)
            self.assert_retained(context, payload, args)

    def test_original_stop_readback_must_match_real_job_source_and_code_metadata(self):
        changes = ({"job_binding_sha256": "f" * 64}, {"source_identity": {}},
            {"consumer_binding_sha256": "f" * 64}, {"source_contract_sha256": "f" * 64},
            {"code_sha256": {"pm_historical_scheduler.py": "f" * 64}})
        for change in changes:
            context, payload, args, _ = self.fixture(row_change=change)
            with self.subTest(change=change), self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_ABORT_GRID_READBACK_REQUIRED"):
                spool.retire_completed_generation(context.root, **args)
            self.assert_retained(context, payload, args)

    def test_header_direct_namespace_unknown_files_and_protected_paths_still_retain(self):
        context, payload, args, _ = self.fixture(fence_change=lambda body:
            body.update(bound_fitted_header={"path": "UNKNOWN_NOT_A_READER_RESULT"}))
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_ABORT_HEADER_UNSUPPORTED"):
            spool.retire_completed_generation(context.root, **args)
        self.assert_retained(context, payload, args)
        context, payload, args, _ = self.fixture()
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_ABORT_SOURCE_STATE_INVALID"):
            spool.retire_completed_generation(context.root, **{**args, "owned_direct_files": ()})
        self.assert_retained(context, payload, args)
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_PROTECTED_PATH_OVERLAP"):
            spool.retire_completed_generation(context.root,
                **{**args, "protected_paths": (*args["protected_paths"], context.root)})
        self.assert_retained(context, payload, args)
        unknown = context.root / "unknown.txt"
        unknown.write_text(LABEL, encoding="ascii")
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_FOREIGN_FILE"):
            spool.retire_completed_generation(context.root, **args)
        self.assert_retained(context, payload, args)


if __name__ == "__main__":
    unittest.main()
