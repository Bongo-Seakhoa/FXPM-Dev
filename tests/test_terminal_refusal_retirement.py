"""Invented owned negative-result I/O; no native, fitted or financial proof.

Bindings/code hashes below are labelled software DATA, not public process or
precommit issuance. Future I/O is exclusive, retained and opt-in. No verifier,
owner.finish, source summary or successful financial capability is substituted.
"""
from copy import deepcopy
import ast
import hashlib
import json
import os
from pathlib import Path
import unittest
import uuid

import pm_authority_spool as spool
import pm_book_multiplicity as book
import pm_end_grid as grid


LABEL = "INVENTED_NEGATIVE_SOFTWARE_DATA_NO_NATIVE_PROCESS_OR_FINANCIAL_VERIFICATION"


def _sha(value):
    return hashlib.sha256(grid.canonical_bytes(value)).hexdigest()


def _write(path, body):
    encoded = grid.canonical_bytes(body)
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    if path.read_bytes() != encoded:
        raise AssertionError("owned software metadata readback changed")
    return hashlib.sha256(encoded).hexdigest()


class TerminalRefusalRetirementTests(unittest.TestCase):
    def setUp(self):
        configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if configured is None:
            self.skipTest("explicit retained FXPM_DERIVED_QA_ROOT is required")
        parent = Path(configured)
        self.assertTrue(configured and parent.is_absolute() and parent.is_dir())
        spool._reject_reparse_chain(parent)
        expected = Path(__file__).resolve().parents[1] / "pm_authority_spool.py"
        self.assertEqual(Path(spool.__file__).resolve(), expected)
        self.parent = parent / ("terminal_refusal_" + uuid.uuid4().hex)
        self.parent.mkdir(exist_ok=False)
        (self.parent / "NO_NATIVE_VERIFICATION.txt").write_text(LABEL, encoding="ascii")

    def fixture(self, *, fence_change=None, row_change=None):
        case = self.parent / uuid.uuid4().hex
        case.mkdir()
        retained = case / "retained"
        retained.mkdir()
        # Existing software-test scale, never a production resource policy.
        limits = spool.SpoolLimits(max_artifact_bytes=1024**2, max_symbol_bytes=2 * 1024**2,
            max_artifacts_per_symbol=100, max_manifest_bytes=128 * 1024,
            max_json_bytes=256 * 1024, stream_chunk_bytes=64 * 1024,
            max_generation_bytes=4 * 1024**2, min_free_bytes=1)
        cell = book.compute_cell_id("XAGUSD", "H1", "TREND")
        contract_sha = _sha({"fixture_provenance": LABEL, "kind": "cell-contract-DATA"})
        context = spool.freeze_generation_context(case / "working",
            generation_id="SOFTWARE_REFUSAL_" + uuid.uuid4().hex,
            native_universe_sha256=_sha({"fixture_provenance": LABEL, "kind": "NO_NATIVE"}),
            config_sha256=_sha({"fixture_provenance": LABEL, "kind": "config-DATA"}),
            limits=limits, owned_workspace={"cell_id": cell, "cell_contract_sha256": contract_sha})
        process_sha = _sha({"fixture_provenance": LABEL, "kind": "process-DATA-NOT-ISSUED"})
        precommit_data = {"fixture_provenance": LABEL, "generation_context_sha256": context.context_sha256,
            "search_process_receipt_sha256": process_sha, "kind": "precommit-DATA-NOT-ISSUED"}
        precommit_sha = _sha(precommit_data)
        relative = f"declared-slots/{precommit_sha}/precommit.json"
        direct = context.root / relative
        direct.parent.mkdir(parents=True)
        self.assertEqual(_write(direct, precommit_data), precommit_sha)
        sink = spool.open_symbol_sink(context, "NEGATIVE_SOFTWARE_IO")
        payload = sink.put_json("owned-negative-payload", "software_fixture",
            {"fixture_provenance": LABEL, "value": "retire-only-this-owned-JSON"})
        sink.seal()
        owner_sha = spool.read_owned_generation_workspace(context)[1]
        codes = {name: _sha({"fixture_provenance": LABEL, "code_identity_DATA": name})
            for name in ("pm_historical_scheduler.py", "pm_end_grid.py", "pm_authority_spool.py")}
        refusal = {"status": "REFUSED_SOURCE_NOT_COMPLETED", "scheduler_status": "NOT_RUN",
            "reason_code": "SOFTWARE_METADATA_REFUSAL", "recorded_at_utc": "2026-11-04T00:00:00+00:00",
            "slot_id": "software-slot-01", "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND",
            "cycle_start_utc": "2023-06-24T00:00:00+00:00",
            "cycle_end_utc": "2023-07-22T00:00:00+00:00",
            "information_cutoff_utc": "2023-06-23T00:00:00+00:00"}
        fields = {key: refusal[key] for key in
            ("symbol", "timeframe", "regime", "cycle_start_utc", "cycle_end_utc", "information_cutoff_utc")}
        fields.update(outcome="STOP", reason_code=refusal["reason_code"], source_kind="m5_derived",
            source_contract_sha256=None, view_key=None, source_identity={}, cost_tier="NONE",
            process_edition_sha256=process_sha, code_sha256=codes,
            produced_at_utc=refusal["recorded_at_utc"])
        fields.update(row_change or {})
        row = grid.build_end_grid_row(**fields)
        receipt = grid.EndGridWriter(retained).append(row)
        self.assertEqual([item.row_sha256 for item in grid.read_end_grid(retained, "XAGUSD")], [row.row_sha256])
        fence = {"schema_version": spool.TERMINAL_REFUSAL_FENCE_SCHEMA,
            "workspace_owner_sha256": owner_sha,
            "binding": {"generation_context_sha256": context.context_sha256, "run_id": context.generation_id,
                "cell_id": cell, "cell_contract_sha256": contract_sha,
                "search_process_receipt_sha256": process_sha, "precommit_sha256": precommit_sha},
            "refusal": refusal,
            "source_disposition": {"stage": "FRESH_PRE_SOURCE_NO_ACQUISITION", "fresh_workspace": True,
                "resumed": False, "source_acquisition_attempted": False,
                "source_owner_created": False, "source_products_acquired": False},
            "end_grid_rows": {refusal["slot_id"]: {"path": receipt["path"], "row_sha256": row.row_sha256}},
            "code_sha256": codes, "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False,
            "full_native_context_eligibility": False, "financial_consumer_ready": False}
        if fence_change is not None:
            fence_change(fence)
        fence_path = retained / "negative-fence.json"
        fence_sha = _write(fence_path, fence)
        protected = case / "protected.txt"
        protected.write_text(LABEL, encoding="ascii")
        args = {"expected_context_sha256": context.context_sha256, "workspace_owner_sha256": owner_sha,
            "source_fence_path": fence_path, "source_fence_sha256": fence_sha,
            "retirement_receipt_path": retained / "retirement.json",
            "protected_paths": (protected, retained, Path(spool.__file__), Path(grid.__file__)),
            "owned_direct_files": (relative,), "unit_lock_paths": (),
            "max_paths": 100, "max_nodes": 300, "max_bytes": 4 * 1024**2}
        return context, payload, args, fence

    def assert_retained(self, context, payload, args):
        self.assertTrue((context.root / payload["descriptor"]["relative_path"]).is_file())
        self.assertFalse((context.root / spool.RETIREMENT_RELATIVE_PATH).exists())
        self.assertFalse(args["retirement_receipt_path"].exists())

    def test_negative_retirement_preserves_refusal_and_original_recording_only_semantics(self):
        context, payload, args, fence = self.fixture()
        retained_bytes = args["source_fence_path"].read_bytes()
        result = spool.retire_completed_generation(context.root, **args)
        self.assertEqual(result["status"], "ARCHIVED_RECORD_ONLY")
        self.assertEqual(result["result_kind"], "TERMINAL_REFUSAL_SOURCE_NOT_COMPLETED")
        self.assertEqual(result["terminal_refusal"], fence["refusal"])
        self.assertEqual(result["terminal_refusal"]["scheduler_status"], "NOT_RUN")
        self.assertEqual(result["precommit_sha256"], fence["binding"]["precommit_sha256"])
        self.assertEqual(result["cell_contract_sha256"], fence["binding"]["cell_contract_sha256"])
        self.assertIs(result["payloads_retained"], False)
        self.assertIs(result["genuine_cold_reopen_available"], False)
        self.assertNotIn("source_summary", fence)
        self.assertFalse((context.root / payload["descriptor"]["relative_path"]).exists())
        self.assertEqual(args["source_fence_path"].read_bytes(), retained_bytes)
        self.assertEqual(spool.read_generation_retirement(context.root), result)
        self.assertEqual(spool.retire_completed_generation(context.root, **args), result)

    def test_source_started_resumed_and_nonboolean_dispositions_keep_payloads(self):
        for key, value in (("resumed", True), ("source_acquisition_attempted", True),
                ("source_owner_created", True), ("source_products_acquired", True),
                ("fresh_workspace", False), ("resumed", 0), ("fresh_workspace", 1)):
            context, payload, args, _ = self.fixture(
                fence_change=lambda body: body["source_disposition"].update({key: value}))
            with self.subTest(key=key, value=value), self.assertRaisesRegex(spool.AuthoritySpoolError,
                    "RETIREMENT_REFUSAL_SOURCE_STATE_INVALID"):
                spool.retire_completed_generation(context.root, **args)
            self.assert_retained(context, payload, args)

    def test_finished_summary_or_true_authority_cannot_be_laundered_as_refusal(self):
        for change in (lambda body: body.update(source_summary={"release_eligible": True}),
                lambda body: body.update(qualification_authority=True),
                lambda body: body.update(financial_consumer_ready=0)):
            context, payload, args, _ = self.fixture(fence_change=change)
            with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_TERMINAL_REFUSAL_INVALID"):
                spool.retire_completed_generation(context.root, **args)
            self.assert_retained(context, payload, args)

    def test_actual_workspace_and_grid_bindings_cannot_be_spliced(self):
        for key in ("generation_context_sha256", "cell_contract_sha256", "search_process_receipt_sha256"):
            context, payload, args, _ = self.fixture(
                fence_change=lambda body: body["binding"].update({key: "f" * 64}))
            with self.subTest(key=key), self.assertRaisesRegex(spool.AuthoritySpoolError,
                    "RETIREMENT_REFUSAL_(WORKSPACE_BINDING_INVALID|GRID_READBACK_REQUIRED)"):
                spool.retire_completed_generation(context.root, **args)
            self.assert_retained(context, payload, args)
        context, payload, args, _ = self.fixture(fence_change=lambda body:
            body["code_sha256"].update({"pm_historical_scheduler.py": "f" * 64}))
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_REFUSAL_GRID_READBACK_REQUIRED"):
            spool.retire_completed_generation(context.root, **args)
        self.assert_retained(context, payload, args)

    def test_preprocess_or_unissued_precommit_identity_is_not_invented(self):
        for key in ("search_process_receipt_sha256", "precommit_sha256"):
            context, payload, args, _ = self.fixture(
                fence_change=lambda body: body["binding"].update({key: None}))
            with self.subTest(key=key), self.assertRaisesRegex(spool.AuthoritySpoolError,
                    "RETIREMENT_REFUSAL_WORKSPACE_BINDING_INVALID"):
                spool.retire_completed_generation(context.root, **args)
            self.assert_retained(context, payload, args)

    def test_sparse_stop_cannot_claim_unearned_cost_job_trade_or_source_values(self):
        changes = ({"cost_tier": "P75"}, {"job_binding_sha256": "a" * 64},
            {"consumer_binding_sha256": "b" * 64}, {"source_contract_sha256": "c" * 64},
            {"source_identity": {"bars_sha256": "d" * 64}})
        for change in changes:
            context, payload, args, _ = self.fixture(row_change=change)
            with self.subTest(change=change), self.assertRaisesRegex(spool.AuthoritySpoolError,
                    "RETIREMENT_REFUSAL_GRID_READBACK_REQUIRED"):
                spool.retire_completed_generation(context.root, **args)
            self.assert_retained(context, payload, args)

    def test_direct_namespace_and_unknown_files_still_refuse_before_unlink(self):
        context, payload, args, _ = self.fixture()
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_REFUSAL_SOURCE_STATE_INVALID"):
            spool.retire_completed_generation(context.root, **{**args, "owned_direct_files": ()})
        self.assert_retained(context, payload, args)
        foreign = context.root / "unknown-owner.txt"
        foreign.write_text(LABEL, encoding="ascii")
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_FOREIGN_FILE"):
            spool.retire_completed_generation(context.root, **args)
        self.assertEqual(foreign.read_text(encoding="ascii"), LABEL)
        self.assert_retained(context, payload, args)

    def test_negative_mode_preserves_original_protected_path_and_fence_digest_guards(self):
        context, payload, args, _ = self.fixture()
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_PROTECTED_PATH_OVERLAP"):
            spool.retire_completed_generation(context.root,
                **{**args, "protected_paths": (*args["protected_paths"], context.root)})
        self.assert_retained(context, payload, args)
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "RETIREMENT_TERMINAL_REFUSAL_INVALID"):
            spool.retire_completed_generation(context.root, **{**args, "source_fence_sha256": "f" * 64})
        self.assert_retained(context, payload, args)

    def test_actual_worker_refusal_body_passes_original_negative_fence_check(self):
        context, payload, args, original = self.fixture()
        source = Path(spool.__file__).resolve().with_name("pm_historical_scheduler.py")
        names = {"HistoricalSchedulerError", "_fail", "_declared_digest", "_declared_m5_utc",
            "_declared_m5_grid_utc", "_declared_m5_refusal_eligible", "_declared_m5_refusal_body"}
        tree = ast.parse(source.read_bytes(), filename=str(source))
        selected = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
            and node.name in names]
        self.assertEqual({node.name for node in selected}, names)
        future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
        scope = {"__name__": "INVENTED_WORKER_BODY_METADATA_ONLY", "__file__": str(source)}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *selected],
            type_ignores=[])), str(source), "exec"), scope)
        saved = original["refusal"]
        # Real helper code, ordinary DATA-NOT-ISSUED arguments from this owned
        # fixture. This neither authenticates a process nor simulates a verifier.
        body = scope["_declared_m5_refusal_body"](
            binding=original["binding"], workspace_owner_sha256=original["workspace_owner_sha256"],
            result={"status": saved["scheduler_status"], "reason_code": saved["reason_code"]},
            slot={"slot_id": saved["slot_id"], "activation_utc": saved["cycle_start_utc"],
                "end_exclusive_utc": saved["cycle_end_utc"], "native_cutoff_utc": saved["information_cutoff_utc"]},
            cell={"cell_id": original["binding"]["cell_id"], **{key: saved[key]
                for key in ("symbol", "timeframe", "regime")}},
            code_sha256=original["code_sha256"], recorded_at_utc=saved["recorded_at_utc"],
            resumed=False, source_attempted=False, owner_created=False, owner_present=False)
        self.assertEqual(body["end_grid_rows"], {})
        # These references were earned by the ORIGINAL writer and readback in
        # fixture(), before the actual worker body is submitted to the Core.
        body["end_grid_rows"] = deepcopy(original["end_grid_rows"])
        path = args["source_fence_path"].with_name("actual-worker-negative-fence.json")
        digest = _write(path, body)
        checked = spool._retirement_fence(path, digest, context_sha256=context.context_sha256,
            generation_id=context.generation_id, cell_id=original["binding"]["cell_id"],
            workspace_sha256=original["workspace_owner_sha256"], working_root=context.root)
        self.assertEqual(checked, body)
        self.assertEqual(checked["refusal"], original["refusal"])
        self.assertNotIn("source_summary", checked)
        self.assertTrue((context.root / payload["descriptor"]["relative_path"]).is_file())


if __name__ == "__main__":
    unittest.main()
