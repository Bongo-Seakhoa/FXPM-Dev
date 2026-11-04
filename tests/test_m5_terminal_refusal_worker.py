"""SYNTHETIC_REFUSAL_METADATA_ONLY_NO_NATIVE_OR_RUNTIME_PROOF.

Actual private predicates and original EndGrid data writer are exercised on
invented metadata. No successful verifier, process/precommit capability, Native
product, model, account, finish or retirement completion is substituted.
"""
import ast
from contextlib import contextmanager
from copy import deepcopy
import os
from pathlib import Path
import unittest
import uuid


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "pm_historical_scheduler.py"


def definitions():
    tree = ast.parse(SOURCE.read_bytes(), filename=str(SOURCE))
    names = {"HistoricalSchedulerError", "_fail", "_declared_digest", "_declared_m5_utc",
        "_declared_m5_grid_utc", "_declared_m5_refusal_eligible", "_declared_m5_refusal_slot",
        "_declared_m5_refusal_body", "_declared_m5_refusal_archived_row",
        "_declared_m5_record_terminal_refusal", "_declared_m5_reuse_recorded_rows", "_declared_m5_existing_refusal_record", "_declared_m5_external_record",
        "_declared_m5_stop_commit_reason"}
    selected = [node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    if {node.name for node in selected} != names:
        raise RuntimeError("actual terminal-refusal helpers missing")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    scope = {"__name__": "synthetic_refusal_metadata_only", "__file__": str(SOURCE)}
    exec(compile(ast.fix_missing_locations(ast.Module(
        body=[future, *selected], type_ignores=[])), str(SOURCE), "exec"), scope)
    return scope


def metadata():
    cell = {"cell_id": "c"*64, "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"}
    binding = {"generation_context_sha256": "1"*64, "run_id": "SYNTHETIC_METADATA_NOT_ISSUED",
        "cell_id": cell["cell_id"], "cell_contract_sha256": "2"*64,
        "search_process_receipt_sha256": "3"*64, "precommit_sha256": "4"*64}
    slot = {"slot_id": "SYNTHETIC_SLOT_1", "activation_utc": "2024-01-02T00:00:00Z",
        "end_exclusive_utc": "2024-01-30T00:00:00Z", "native_cutoff_utc": "2024-01-02T00:00:00Z"}
    code = {"pm_historical_scheduler.py": "5"*64, "pm_end_grid.py": "6"*64,
        "pm_authority_spool.py": "7"*64}
    return {"binding": binding, "workspace_owner_sha256": "8"*64,
        "result": {"status": "NOT_RUN", "reason_code": "NATIVE_DRIFT",
            "recorded_before_dispatch": False, "run_receipt": None, "account_descriptor": None},
        "slot": slot, "cell": cell, "code_sha256": code,
        "recorded_at_utc": "2026-11-04T02:00:00Z",
        "resumed": False, "source_attempted": False, "owner_created": False, "owner_present": False}


class TerminalRefusalMetadataTests(unittest.TestCase):
    def test_fresh_slice_does_not_infer_absence_from_owner_none_or_resume(self):
        h = definitions()
        eligible = h["_declared_m5_refusal_eligible"]
        self.assertTrue(eligible(resumed=False, source_attempted=False,
            owner_created=False, owner_present=False))
        for key in ("resumed", "source_attempted", "owner_created", "owner_present"):
            args = dict(resumed=False, source_attempted=False, owner_created=False, owner_present=False)
            args[key] = True
            with self.subTest(fact=key):
                self.assertFalse(eligible(**args))
                args[key] = 0
                with self.assertRaises(h["HistoricalSchedulerError"]) as invalid:
                    eligible(**args)
                self.assertEqual(invalid.exception.code, "DECLARED_M5_REFUSAL_FACTS_INVALID")
        values = metadata()
        body = h["_declared_m5_refusal_body"](**values)
        self.assertEqual(body["schema_version"], "fxpm.declared-m5-terminal-refusal-fence.v1")
        self.assertIs(body["source_disposition"]["source_products_acquired"], False)
        values["resumed"] = True
        resumed = h["_declared_m5_refusal_body"](**values)
        self.assertEqual(resumed["schema_version"], "fxpm.declared-m5-terminal-refusal-result.v1")
        self.assertIsNone(resumed["source_disposition"]["source_products_acquired"])
        values["resumed"], values["source_attempted"] = False, True
        attempted = h["_declared_m5_refusal_body"](**values)
        self.assertIsNone(attempted["source_disposition"]["source_products_acquired"])

    def test_refusal_binds_real_fields_without_trade_account_job_or_source_summary(self):
        h = definitions()
        values = metadata()
        original = deepcopy(values)
        body = h["_declared_m5_refusal_body"](**values)
        self.assertEqual(values, original)
        self.assertEqual(body["binding"], values["binding"])
        self.assertEqual(body["refusal"]["scheduler_status"], "NOT_RUN")
        self.assertEqual(body["refusal"]["reason_code"], "NATIVE_DRIFT")
        self.assertEqual(body["refusal"]["recorded_at_utc"], "2026-11-04T02:00:00+00:00")
        for key in ("qualification_authority", "capital_authority", "live_authority",
                    "order_authority", "full_native_context_eligibility", "financial_consumer_ready"):
            self.assertIs(body[key], False)
        self.assertFalse(set(body) & {"source_summary", "account_descriptor", "owner",
            "job_binding_sha256", "consumer_binding_sha256", "source_contract_sha256"})
        # Empty row references are deliberately not a retirement-ready fence.
        self.assertEqual(body["end_grid_rows"], {})
        for change in ("missing_process", "missing_precommit", "foreign_cell", "empty_reason", "bad_code"):
            bad = deepcopy(values)
            if change == "missing_process":
                bad["binding"]["search_process_receipt_sha256"] = None
            elif change == "missing_precommit":
                bad["binding"]["precommit_sha256"] = ""
            elif change == "foreign_cell":
                bad["binding"]["cell_id"] = "d"*64
            elif change == "empty_reason":
                bad["result"]["reason_code"] = ""
            else:
                bad["code_sha256"] = None
            with self.subTest(change=change):
                with self.assertRaises(h["HistoricalSchedulerError"]) as refused:
                    h["_declared_m5_refusal_body"](**bad)
                self.assertEqual(refused.exception.code, "DECLARED_M5_REFUSAL_BINDING_INVALID")

    def test_cycle_selection_requires_earned_frontier_and_never_guesses_resumed_cycle(self):
        h = definitions()
        first = metadata()["slot"]
        second = {**first, "slot_id": "SYNTHETIC_SLOT_2",
            "activation_utc": first["end_exclusive_utc"], "end_exclusive_utc": "2024-02-27T00:00:00Z",
            "native_cutoff_utc": first["end_exclusive_utc"]}
        slots = [first, second]
        choose = h["_declared_m5_refusal_slot"]
        self.assertEqual(choose(slots, phase="PRE_SOURCE", run_body=None, resumed=False), first)
        self.assertIsNone(choose(slots, phase="PRE_SOURCE", run_body=None, resumed=True))
        self.assertIsNone(choose(slots, phase="PRODUCER_CONTROLLER", run_body=None, resumed=False))
        stopped = {"terminal_stop": True, "slot_commits": [
            {"slot_id": first["slot_id"], "status": "NO_TRADE"},
            {"slot_id": second["slot_id"], "status": "STOP"}]}
        self.assertEqual(choose(slots, phase="PRODUCER_CONTROLLER", run_body=stopped, resumed=True), second)
        complete = {"terminal_stop": False, "slot_commits": [
            {"slot_id": first["slot_id"], "status": "OWNER"},
            {"slot_id": second["slot_id"], "status": "NO_TRADE"}]}
        self.assertEqual(choose(slots, phase="ACCOUNT", run_body=complete, resumed=False), second)
        values = metadata()
        source_slot = {**first, "ordinal": 1}
        commit = {**{key: values["binding"][key] for key in
            ("generation_context_sha256", "precommit_sha256", "cell_id")},
            "slot_id": first["slot_id"], "ordinal": 1,
            "completion": {"status": "STOP", "cell_id": values["cell"]["cell_id"],
                "slot_id": first["slot_id"], "reason_code": "SOURCE_IO_FAILURE"}}
        technical = h["_declared_m5_stop_commit_reason"](commit,
            context_sha256=values["binding"]["generation_context_sha256"],
            precommit_sha256=values["binding"]["precommit_sha256"],
            cell_id=values["cell"]["cell_id"], slot=source_slot)
        primary = {"status": "TERMINAL_STOP", "reason_code": "NEW_TERMINAL_STOP"}
        values.update(result={**primary, "reason_code": technical}, slot=source_slot,
            source_attempted=True, owner_created=True, owner_present=True)
        refusal = h["_declared_m5_refusal_body"](**values)
        self.assertEqual(primary["reason_code"], "NEW_TERMINAL_STOP")
        self.assertEqual(refusal["refusal"]["reason_code"], "SOURCE_IO_FAILURE")
        bad_commit = deepcopy(commit)
        bad_commit["precommit_sha256"] = "f"*64
        with self.assertRaises(h["HistoricalSchedulerError"]) as wrong_commit:
            h["_declared_m5_stop_commit_reason"](bad_commit,
                context_sha256=values["binding"]["generation_context_sha256"],
                precommit_sha256=values["binding"]["precommit_sha256"],
                cell_id=values["cell"]["cell_id"], slot=source_slot)
        self.assertEqual(wrong_commit.exception.code, "DECLARED_M5_REFUSAL_STOP_COMMIT_INVALID")
        changed = deepcopy(complete)
        changed["slot_commits"].reverse()
        with self.assertRaises(h["HistoricalSchedulerError"]) as refused:
            choose(slots, phase="ACCOUNT", run_body=changed, resumed=False)
        self.assertEqual(refused.exception.code, "DECLARED_M5_REFUSAL_SLOT_INVALID")

    def test_negative_archive_preserves_status_reason_and_recording_only_semantics(self):
        h = definitions()
        for status in ("NOT_RUN", "FAILED"):
            values = metadata()
            values["result"]["status"] = status
            refusal = h["_declared_m5_refusal_body"](**values)["refusal"]
            archive = {"result_kind": "TERMINAL_REFUSAL_SOURCE_NOT_COMPLETED",
                "terminal_refusal": refusal, "generation_context_sha256": values["binding"]["generation_context_sha256"],
                "precommit_sha256": values["binding"]["precommit_sha256"],
                "retirement_receipt_path": "SYNTHETIC_RECORD_ONLY", "retirement_receipt_sha256": "9"*64,
                "source_fence": {"SYNTHETIC_REFERENCE": True}, "end_grid_rows": {}}
            result = deepcopy(values["result"])
            h["_declared_m5_refusal_archived_row"](result, archive)
            self.assertEqual((result["status"], result["reason_code"]), (status, "NATIVE_DRIFT"))
            self.assertIs(result["recorded_before_dispatch"], False)
            self.assertIsNone(result["run_receipt"])
            self.assertIsNone(result["account_descriptor"])
            self.assertIs(result["genuine_cold_reopen_available"], False)
            self.assertEqual(result["terminal_refusal_workspace_status"], "RELEASED_RECORDING_ONLY")
            self.assertNotEqual(result["status"], "ARCHIVED")
            # Actual outer-finally recorder must preserve an earned archive,
            # rather than relabel it as an unissued retained admission.
            archived_result = deepcopy(result)
            h["_declared_m5_record_terminal_refusal"](result, context=None, workspace_sha=None,
                precommit=None, process=None, frozen=None, config=None, contract={}, row={},
                cell_contract_sha256=None, resumed=False, source_attempted=False, owner_created=False,
                owner=None, phase="PRE_SOURCE", run_body=None)
            self.assertEqual(result, archived_result)
            self.assertNotIn("terminal_refusal_record_status", result)

        incomplete = {"status": "NOT_RUN", "reason_code": "RETIREMENT_INCOMPLETE",
            "recorded_before_dispatch": False, "run_receipt": None, "account_descriptor": None}
        saved = deepcopy(incomplete)
        h["_declared_m5_record_terminal_refusal"](incomplete, context=None, workspace_sha=None,
            precommit=None, process=None, frozen=None, config=None, contract={}, row={},
            cell_contract_sha256=None, resumed=False, source_attempted=False, owner_created=False,
            owner=None, phase="PRE_SOURCE", run_body=None)
        self.assertEqual(incomplete, saved)
        self.assertNotIn("terminal_refusal_record_status", incomplete)

    def test_absent_earned_identities_keep_admission_and_source_flags_precede_acquisition(self):
        h = definitions()
        result = metadata()["result"]
        before = deepcopy(result)
        h["_declared_m5_record_terminal_refusal"](result, context=None, workspace_sha=None,
            precommit=None, process=None, frozen=None, config=None, contract={}, row={},
            cell_contract_sha256=None, resumed=False, source_attempted=False, owner_created=False,
            owner=None, phase="PRE_SOURCE", run_body=None)
        self.assertEqual(result["terminal_refusal_record_status"], "ADMISSION_ONLY_RETAINED")
        for key in before:
            self.assertEqual(result[key], before[key])
        self.assertNotIn("terminal_refusal_end_grid", result)
        root = ast.parse(SOURCE.read_bytes())
        work = next(node for node in root.body if isinstance(node, ast.FunctionDef)
            and node.name == "_declared_m5_run_one_cell")
        found = 0
        for node in ast.walk(work):
            if isinstance(node, ast.If):
                for branch in (node.body, node.orelse):
                    for position, statement in enumerate(branch):
                        if isinstance(statement, ast.Assign) and isinstance(statement.value, ast.Call):
                            call = statement.value
                            name = call.func.attr if isinstance(call.func, ast.Attribute) else ""
                            if name in {"open_recorded_derived_costs_route", "reopen_declared_m5_cell_source_context"}:
                                found += 1
                                prior = branch[position - 1]
                                self.assertIn("source_attempted", ast.unparse(prior))
                                self.assertIn("True", ast.unparse(prior))
        self.assertEqual(found, 2)
        main_try = next(node for node in work.body if isinstance(node, ast.Try))
        final = main_try.finalbody[0]
        self.assertIsInstance(final, ast.Try)
        self.assertIn("_declared_m5_record_terminal_refusal", "\n".join(ast.unparse(node) for node in final.body))
        self.assertIn("owner.close()", "\n".join(ast.unparse(node) for node in final.finalbody))
        # Actual With/outer-Finally nodes, with work removed. These collaborators
        # record control ordering only; they confer no lease or source authority.
        dispatch = next(node for node in ast.walk(main_try) if isinstance(node, ast.With)
            and any(isinstance(item.context_expr, ast.Call)
                and isinstance(item.context_expr.func, ast.Name)
                and item.context_expr.func.id == "_declared_dispatch_lease" for item in node.items))
        skeleton_with = deepcopy(dispatch)
        skeleton_with.body = [ast.Return(ast.Name(id="result", ctx=ast.Load()))]
        skeleton = ast.parse("def metadata_lease_boundary():\n    pass\n").body[0]
        skeleton.body = [ast.Try(body=[skeleton_with], handlers=[], orelse=[],
            finalbody=deepcopy(main_try.finalbody))]
        events, held = [], [False]

        @contextmanager
        def ordinary_lease(_):
            events.append("enter")
            held[0] = True
            try:
                yield True
            finally:
                held[0] = False
                events.append("exit")

        def record_only(*args, **kwargs):
            self.assertFalse(held[0])
            events.append("record")

        ns = {name: None for name in ("context", "workspace_sha", "precommit", "process", "frozen",
            "config", "contract", "row", "digest", "owner", "run_body", "root", "abort_report")}
        ns.update(result={"status": "NOT_RUN", "reason_code": "NATIVE_DRIFT"},
            resumed=False, source_attempted=False, owner_created=False, phase="PRE_SOURCE",
            _declared_dispatch_lease=ordinary_lease, _declared_m5_record_terminal_refusal=record_only)
        exec(compile(ast.fix_missing_locations(ast.Module(
            body=[skeleton], type_ignores=[])), str(SOURCE), "exec"), ns)
        self.assertIs(ns["metadata_lease_boundary"](), ns["result"])
        self.assertNotIn("terminal_refusal_record_error", ns["result"])
        self.assertEqual(events, ["enter", "exit", "record"])


class OriginalStopWriterOwnedFixtureTests(unittest.TestCase):
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

    def test_original_stop_writer_roundtrip_is_sparse_honest_and_idempotent(self):
        import pm_end_grid as grid
        case = self.parent / ("terminal-refusal-" + uuid.uuid4().hex)
        case.mkdir(exist_ok=False)
        # Original data-row writer only: none of these hashes authenticates Native input.
        values = metadata()
        values["cell"]["cell_id"] = grid.book.compute_cell_id("XAGUSD", "H1", "TREND")
        values["binding"]["cell_id"] = values["cell"]["cell_id"]
        body = definitions()["_declared_m5_refusal_body"](**values)
        refusal = body["refusal"]
        row = grid.build_end_grid_row(symbol="XAGUSD", timeframe="H1", regime="TREND",
            cycle_start_utc=refusal["cycle_start_utc"], cycle_end_utc=refusal["cycle_end_utc"],
            information_cutoff_utc=refusal["information_cutoff_utc"], outcome="STOP", reason_code="NATIVE_DRIFT",
            source_kind="m5_derived", source_contract_sha256=None, no_trade_run_receipt_sha256=None,
            view_key=None, view_contract_sha256=None, source_identity={}, cost_tier="NONE",
            process_edition_sha256=values["binding"]["search_process_receipt_sha256"],
            code_sha256=values["code_sha256"], metrics={}, strategy_family=None, configuration_sha256=None,
            job_binding_sha256=None, consumer_binding_sha256=None, produced_at_utc=refusal["recorded_at_utc"])
        writer = grid.EndGridWriter(case)
        first, second = writer.append(row), writer.append(row)
        self.assertIs(first["appended"], True)
        self.assertIs(second["appended"], False)
        retained = grid.read_end_grid(case, "XAGUSD")
        self.assertEqual(len(retained), 1)
        self.assertEqual(retained[0].to_bytes(), row.to_bytes())
        self.assertEqual(first["row_sha256"], retained[0].row_sha256)
        for name in ("source_contract_sha256", "view_key", "view_contract_sha256",
                     "job_binding_sha256", "consumer_binding_sha256", "no_trade_run_receipt_sha256"):
            self.assertIsNone(getattr(retained[0], name))
        self.assertEqual(retained[0].source_identity, {})
        self.assertEqual(retained[0].cost_tier, "NONE")

    def test_actual_recorder_reuses_original_stop_bytes_and_reason_without_rewriting(self):
        import pm_end_grid as grid
        h = definitions()
        case = self.parent / ("terminal-refusal-reentry-" + uuid.uuid4().hex)
        case.mkdir(exist_ok=False)
        values = metadata()
        values["cell"]["cell_id"] = grid.book.compute_cell_id("XAGUSD", "H1", "TREND")
        values["binding"]["cell_id"] = values["cell"]["cell_id"]
        values["slot"]["ordinal"] = 1
        body = h["_declared_m5_refusal_body"](**values)
        refusal = body["refusal"]

        def ordinary_row(outcome, process_sha256):
            return grid.build_end_grid_row(symbol="XAGUSD", timeframe="H1", regime="TREND",
                cycle_start_utc=refusal["cycle_start_utc"], cycle_end_utc=refusal["cycle_end_utc"],
                information_cutoff_utc=refusal["information_cutoff_utc"],
                outcome=outcome, reason_code="SOURCE_IO_FAILURE", source_kind="m5_derived",
                source_contract_sha256=None, no_trade_run_receipt_sha256=None,
                view_key=None, view_contract_sha256=None, source_identity={}, cost_tier="NONE",
                process_edition_sha256=process_sha256, code_sha256=values["code_sha256"], metrics={},
                strategy_family=None, configuration_sha256=None, job_binding_sha256=None,
                consumer_binding_sha256=None, produced_at_utc=refusal["recorded_at_utc"])

        row = ordinary_row("STOP", values["binding"]["search_process_receipt_sha256"])
        receipt = grid.EndGridWriter(case).append(row)
        path = Path(receipt["path"])
        before_bytes, before_mtime = path.read_bytes(), path.stat().st_mtime_ns
        before_names = sorted(item.name for item in case.iterdir())
        reuse = h["_declared_m5_reuse_recorded_rows"]
        for label in ("NEW_TERMINAL_STOP", "RECORDED_TERMINAL_STOP", "WINDOW_EXPIRED"):
            result = {"status": "NOT_RUN" if label == "WINDOW_EXPIRED" else "TERMINAL_STOP",
                "reason_code": label, "recorded_before_dispatch": False,
                "run_receipt": None, "account_descriptor": None}
            primary = deepcopy(result)
            with self.subTest(current_dispatch_reason=label):
                self.assertTrue(reuse(result, end_grid_root=case, source_fence_path=case / "refusal.json", cell=values["cell"],
                    slots=[values["slot"]],
                    process_sha256=values["binding"]["search_process_receipt_sha256"]))
                for key, value in primary.items():
                    self.assertEqual(result[key], value)
                self.assertEqual(result["terminal_refusal_reason_code"], "SOURCE_IO_FAILURE")
                self.assertEqual(result["terminal_refusal_record_status"], "EXISTING_DURABLE_STOP")
                self.assertEqual(result["terminal_refusal_existing_fence_status"], "ABSENT")
                reference = result["terminal_refusal_end_grid"][values["slot"]["slot_id"]]
                self.assertIs(reference["appended"], False)
                self.assertIs(reference["read_only_existing"], True)
                self.assertEqual(reference["readback_row_sha256"], row.row_sha256)
                self.assertNotIn("terminal_refusal_record_error", result)
                self.assertNotIn("terminal_refusal_record", result)
        self.assertEqual(path.read_bytes(), before_bytes)
        self.assertEqual(path.stat().st_mtime_ns, before_mtime)
        self.assertEqual(sorted(item.name for item in case.iterdir()), before_names)
        # A late fence already records the old phase/code facts. Reentry reads
        # those actual bytes; it must not rebuild with current resumed facts.
        body["end_grid_rows"] = {values["slot"]["slot_id"]: {
            "path": str(path), "row_sha256": row.row_sha256}}
        fence_path = case / "refusal.json"
        saved_fence = h["_declared_m5_external_record"](fence_path, body, maximum_bytes=4 * 1024**2)
        old_fence, old_fence_mtime = fence_path.read_bytes(), fence_path.stat().st_mtime_ns
        changed = deepcopy(values)
        changed.update(resumed=True, source_attempted=True, owner_created=True, owner_present=True)
        changed["code_sha256"] = {key: "f"*64 for key in changed["code_sha256"]}
        rebuilt = h["_declared_m5_refusal_body"](**changed)
        self.assertNotEqual(rebuilt["source_disposition"], body["source_disposition"])
        self.assertNotEqual(rebuilt["code_sha256"], body["code_sha256"])
        late = {"status": "NOT_RUN", "reason_code": "WINDOW_EXPIRED"}
        self.assertTrue(reuse(late, end_grid_root=case, source_fence_path=fence_path,
            cell=values["cell"], slots=[values["slot"]],
            process_sha256=values["binding"]["search_process_receipt_sha256"]))
        self.assertEqual(late["reason_code"], "WINDOW_EXPIRED")
        self.assertEqual(late["terminal_refusal_reason_code"], "SOURCE_IO_FAILURE")
        self.assertEqual(late["terminal_refusal_record"]["sha256"], saved_fence["sha256"])
        self.assertIs(late["terminal_refusal_record"]["written"], False)
        self.assertIs(late["terminal_refusal_record"]["read_only_existing"], True)
        self.assertEqual(fence_path.read_bytes(), old_fence)
        self.assertEqual(fence_path.stat().st_mtime_ns, old_fence_mtime)
        self.assertEqual(path.read_bytes(), before_bytes)
        with self.assertRaises(h["HistoricalSchedulerError"]) as foreign:
            reuse({}, end_grid_root=case, source_fence_path=case / "refusal.json", cell=values["cell"], slots=[values["slot"]],
                process_sha256="f"*64)
        self.assertEqual(foreign.exception.code, "DECLARED_M5_GRID_PROCESS_CONFLICT")
        non_stop_case = self.parent / ("terminal-refusal-conflict-" + uuid.uuid4().hex)
        non_stop_case.mkdir(exist_ok=False)
        grid.EndGridWriter(non_stop_case).append(
            ordinary_row("UNAVAILABLE", values["binding"]["search_process_receipt_sha256"]))
        existing_result = {"status": "FAILED", "reason_code": "GRID_READBACK_MISMATCH"}
        self.assertTrue(reuse(existing_result, end_grid_root=non_stop_case,
            source_fence_path=non_stop_case / "refusal.json", cell=values["cell"], slots=[values["slot"]],
            process_sha256=values["binding"]["search_process_receipt_sha256"]))
        self.assertEqual(existing_result["terminal_refusal_record_status"], "EXISTING_CELL_PROCESS_ROWS")
        self.assertNotIn("terminal_refusal_end_grid", existing_result)
        self.assertEqual(len(grid.read_end_grid(non_stop_case, "XAGUSD")), 1)
        # Ordinary unissued DATA row, not an authenticated process/source:
        # reproduce a partial multi-slot result publication, then fail at slot2.
        partial = self.parent / ("terminal-refusal-partial-" + uuid.uuid4().hex)
        partial.mkdir(exist_ok=False)
        view_contract = grid.store_view_contract(partial, {
            "schema_version": "SYNTHETIC_DATA_NOT_ISSUED",
            "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False})
        first_result = grid.build_end_grid_row(symbol="XAGUSD", timeframe="H1", regime="TREND",
            cycle_start_utc=refusal["cycle_start_utc"], cycle_end_utc=refusal["cycle_end_utc"],
            information_cutoff_utc=refusal["information_cutoff_utc"],
            outcome="NO_TRADE", reason_code="SYNTHETIC_DATA_NOT_ISSUED", source_kind="m5_derived",
            source_contract_sha256=None, no_trade_run_receipt_sha256="9"*64,
            view_key="a"*64, view_contract_sha256=view_contract,
            source_identity={"activation_id": "SYNTHETIC_DATA_NOT_ISSUED", "m5_source_sha256": "b"*64,
                "constructor_implementation_sha256": "c"*64, "gap_findings_sha256": "d"*64},
            cost_tier="P75", process_edition_sha256=values["binding"]["search_process_receipt_sha256"],
            code_sha256=values["code_sha256"], metrics={}, strategy_family=None, configuration_sha256=None,
            job_binding_sha256="e"*64, consumer_binding_sha256="f"*64,
            produced_at_utc=refusal["recorded_at_utc"])
        first_receipt = grid.EndGridWriter(partial).append(first_result)
        partial_file = Path(first_receipt["path"])
        original_partial = partial_file.read_bytes()
        slot2 = {**values["slot"], "slot_id": "SYNTHETIC_SLOT_2", "ordinal": 2,
            "activation_utc": values["slot"]["end_exclusive_utc"],
            "native_cutoff_utc": values["slot"]["end_exclusive_utc"],
            "end_exclusive_utc": "2024-02-27T00:00:00Z"}
        finalization_failure = {"status": "FAILED", "reason_code": "DECLARED_M5_GRID_READBACK_MISMATCH"}
        self.assertTrue(reuse(finalization_failure, end_grid_root=partial,
            source_fence_path=partial / "refusal.json", cell=values["cell"],
            slots=[values["slot"], slot2],
            process_sha256=values["binding"]["search_process_receipt_sha256"]))
        self.assertEqual(finalization_failure["terminal_refusal_record_status"], "EXISTING_CELL_PROCESS_ROWS")
        self.assertNotIn("terminal_refusal_end_grid", finalization_failure)
        self.assertEqual(finalization_failure["terminal_refusal_existing_fence_status"], "ABSENT")
        self.assertFalse((partial / "refusal.json").exists())
        self.assertEqual(partial_file.read_bytes(), original_partial)
        self.assertEqual([item.outcome for item in grid.read_end_grid(partial, "XAGUSD")], ["NO_TRADE"])
        # The actual recorder invokes this same helper and returns before every write.
        source = ast.parse(SOURCE.read_bytes())
        recorder = next(node for node in source.body if isinstance(node, ast.FunctionDef)
            and node.name == "_declared_m5_record_terminal_refusal")
        reuse_if = next(node for node in ast.walk(recorder) if isinstance(node, ast.If)
            and isinstance(node.test, ast.Call) and isinstance(node.test.func, ast.Name)
            and node.test.func.id == "_declared_m5_reuse_recorded_rows")
        self.assertTrue(any(isinstance(node, ast.Return) for node in reuse_if.body))
        for call in ast.walk(recorder):
            if isinstance(call, ast.Call):
                name = call.func.id if isinstance(call.func, ast.Name) else (
                    call.func.attr if isinstance(call.func, ast.Attribute) else "")
                if name in {"_declared_m5_refusal_body", "build_end_grid_row",
                            "_declared_m5_external_record", "retire_completed_generation"}:
                    self.assertGreater(call.lineno, reuse_if.end_lineno)


if __name__ == "__main__":
    unittest.main()
