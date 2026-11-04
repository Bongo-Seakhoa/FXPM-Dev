"""Synthetic metadata projection and STOP-row tests, not source authentication.

The dictionaries and stored objects are invented documentary DATA, not issued
sessions, Native products or process receipts. Stored-object tests use original
Spool readers and the metadata validator, never a Native completion verifier.
The sole failure-path double raises; no successful verifier is replaced.
"""
import ast
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
import uuid

import pm_authority_spool as spool
import pm_end_grid as grid


SOURCE = Path(__file__).resolve().parents[1] / "pm_historical_scheduler.py"
LABEL = "SYNTHETIC_METADATA_ONLY_NO_NATIVE_PROCESS_OR_AUTHENTICATION_PROOF"


def _selected_scope():
    names = {"HistoricalSchedulerError", "_fail", "_declared_digest", "_declared_m5_stop_grid_identity",
        "_declared_m5_stop_commit_reason", "_declared_m5_recorded_stop_grid_identity"}
    tree = ast.parse(SOURCE.read_bytes(), filename=str(SOURCE))
    nodes = [n for n in tree.body if isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name in names]
    if {n.name for n in nodes} != names:
        raise AssertionError("actual selected scheduler definitions changed")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    scope = {"__name__": LABEL, "__file__": str(SOURCE)}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *nodes], type_ignores=[])),
        str(SOURCE), "exec"), scope)
    return tree, scope


def _session_data():
    return {"source_kind": "m5_derived", "source_state": "AVAILABLE", "cost_status": "COST_READY",
        "failure": None, "unverified_caller_source_or_fit_accepted": False,
        "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False,
        "m5_source_identity": {"symbol": "XAGUSD", "timeframe": "M5",
            "activation_id": "SYNTHETIC_ACTIVATION_DATA", "activation_sha256": "a" * 64,
            "bars_sha256": "b" * 64, "sidecar_sha256": "c" * 64,
            "provenance_sha256": "d" * 64, "request_sha256": "e" * 64,
            "point": 0.001, "spread_unit": "points"}}


class M5StopGridSourceIdentityTests(unittest.TestCase):
    def setUp(self):
        self.tree, self.scope = _selected_scope()
        self.project = self.scope["_declared_m5_stop_grid_identity"]
        self.assertEqual(Path(spool.__file__).resolve().parent, Path(grid.__file__).resolve().parent)

    def stored_fixture(self):
        configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if configured is None:
            self.skipTest("explicit existing retained FXPM_DERIVED_QA_ROOT is required for stored metadata")
        parent = Path(configured)
        self.assertTrue(configured and parent.is_absolute() and parent.is_dir())
        spool._reject_reparse_chain(parent)
        case = parent / ("stop_recorded_metadata_" + uuid.uuid4().hex)
        case.mkdir(exist_ok=False)
        (case / "DATA_NOT_ISSUED.txt").write_text(LABEL, encoding="ascii")
        import pm_historical_production_adapter as adapter
        import pm_historical_search_producer as producer
        limits = spool.SpoolLimits(max_artifact_bytes=1024**2, max_symbol_bytes=2 * 1024**2,
            max_artifacts_per_symbol=100, max_manifest_bytes=128 * 1024,
            max_json_bytes=256 * 1024, stream_chunk_bytes=64 * 1024,
            max_generation_bytes=4 * 1024**2, min_free_bytes=1)
        context = spool.freeze_generation_context(case / "spool", generation_id="SYNTHETIC_" + uuid.uuid4().hex,
            native_universe_sha256="a" * 64, config_sha256="b" * 64, limits=limits)
        sink = spool.open_symbol_sink(context, "XAGUSD")
        namespace = "synthetic-recorded-stop"
        index_data = sink.put_json(namespace + "/index-data", "synthetic_index_data",
            {"classification": LABEL, "not_a_native_product": True})
        reopen_data = sink.put_json(namespace + "/reopen-data", "declared_m5_reopen_context",
            {"classification": LABEL, "not_a_source_capability": True})
        profile = "c" * 64
        pins = {"view_plan_sha256": "d" * 64, "cost_edition_identity": {"profile_sha256": profile}}
        digest = lambda value: hashlib.sha256(json.dumps(value, ensure_ascii=True, allow_nan=False,
            sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()
        body = {"schema_version": "fxpm.declared-m5-slot-producer-request.v1", "source_kind": "m5_derived",
            "symbol": "XAGUSD", "timeframe": "H1", "artifact_namespace": namespace,
            "effective_config_sha256": "e" * 64,
            "anchor": {"selection_cutoff_utc": "2020-01-02T00:00:00Z"},
            "spread_profile_identity": {"profile_sha256": profile}, "m5_source_pins": pins,
            "m5_view_contract_sha256": digest(pins), "source_access_window_utc": {
                "start_utc": "2026-01-01T00:00:00Z", "end_exclusive_utc": "2026-01-02T00:00:00Z"}}
        parent_body = {"classification": LABEL, "effective_config_sha256": body["effective_config_sha256"],
            "precommitted_at_utc": "2026-01-01T00:00:00Z"}
        precommit = SimpleNamespace(receipt_sha256=digest(parent_body), to_dict=lambda: deepcopy(parent_body))
        slot = {"slot_id": "synthetic-slot-01", "ordinal": 1}
        cell = {"cell_id": "f" * 64, "symbol": "XAGUSD"}
        observed = {"view_key": "1" * 64, "payload_sha256": "2" * 64, "membership_sha256": "3" * 64,
            "source_kind": "m5_derived", "timeframe": "H1", "cutoff_utc": body["anchor"]["selection_cutoff_utc"],
            "interval": {"start_utc": "2020-01-01T00:00:00Z", "end_exclusive_utc": "2020-01-02T00:00:00Z"},
            "coverage": {}, "constructor_implementation_sha256": "4" * 64,
            "gap_findings_sha256": "5" * 64, "cost_status": "PROFILE_UNVERIFIED"}
        value = dict.fromkeys(adapter.DECLARED_M5_SLOT_SOURCE_SESSION_KEYS)
        value.update(schema_version=adapter.DECLARED_M5_SLOT_SOURCE_SESSION_SCHEMA, source_kind="m5_derived",
            request_sha256=digest(body), generation_context_sha256=context.context_sha256,
            config_sha256=context.config_sha256, effective_config_sha256=body["effective_config_sha256"],
            spread_profile_identity=deepcopy(body["spread_profile_identity"]), m5_source_pins=deepcopy(pins),
            m5_view_contract_sha256=body["m5_view_contract_sha256"], view_plan_sha256=pins["view_plan_sha256"],
            session_opened_at_utc="2026-01-01T00:00:01Z", source_accessed_at_utc="2026-01-01T00:00:02Z",
            selection_cutoff_utc=body["anchor"]["selection_cutoff_utc"], source_state="AVAILABLE",
            native_prefix_index_descriptor=index_data["descriptor"], native_prefix_index_sha256=index_data["descriptor"]["sha256"],
            native_source_route={"symbol": "XAGUSD", "timeframe": "M5", "cutoff_utc": body["anchor"]["selection_cutoff_utc"],
                "source_state": "AVAILABLE", "index_sha256": index_data["descriptor"]["sha256"],
                "index_descriptor": index_data["descriptor"]},
            source_group_decision_sha256="6" * 64, regime_fit_decision_sha256="7" * 64,
            m5_source_identity=deepcopy(_session_data()["m5_source_identity"]), m5_request_view_id="request-data",
            m5_calibration_view_id="calibration-data", m5_request_view_observation=deepcopy(observed),
            m5_calibration_view_observation=deepcopy(observed), cost_status="COST_READY", failure=None,
            m5_reopen_context_entry=reopen_data, unverified_caller_source_or_fit_accepted=False,
            qualification_authority=False, capital_authority=False, live_authority=False, order_authority=False)
        value["fitted_view_evidence"] = {"source_kind": "m5_derived", "base_timeframe": "M5", "view_timeframe": "H1",
            "m5_view_contract_sha256": value["m5_view_contract_sha256"], "view_key": observed["view_key"],
            "view_payload_sha256": observed["payload_sha256"], "view_membership_sha256": observed["membership_sha256"],
            "base_native_prefix_index_sha256": value["native_prefix_index_sha256"],
            "source_accessed_at_utc": value["source_accessed_at_utc"]}
        entry = sink.put_json(namespace + "/source-session", "declared_slot_source_session", value)
        completion = {"status": "STOP", "cell_id": cell["cell_id"], "slot_id": slot["slot_id"],
            "reason_code": "SYNTHETIC_RECORDED_STOP", "completed_at_utc": "2026-01-01T00:00:03Z",
            "request_sha256": value["request_sha256"], "request_body": body, "source_session_entry": entry}
        commit = {"schema_version": producer.DECLARED_SLOT_COMMIT_SCHEMA,
            "generation_context_sha256": context.context_sha256, "precommit_sha256": precommit.receipt_sha256,
            "cell_id": cell["cell_id"], "slot_id": slot["slot_id"], "ordinal": slot["ordinal"],
            "completion": completion, "artifact_roster": [index_data, reopen_data, entry]}
        committed = sink.put_json(namespace + "/stop-commit-data", "declared_slot_commit", commit)
        sink.seal()
        pointer = spool._local_path(context.root, producer._declared_commit_path(precommit, cell["cell_id"], 1))
        pointer.parent.mkdir(parents=True)
        encoded = spool._canonical_json_bytes({"schema_version": producer._DECLARED_SLOT_POINTER_SCHEMA,
            "commit_descriptor": committed["descriptor"]}, context.limits.max_json_bytes)
        with pointer.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        reopened = producer._read_declared_commit(context,
            producer._declared_commit_path(precommit, cell["cell_id"], 1))
        self.assertEqual(reopened[0], commit)
        self.assertEqual(reopened[1], committed["descriptor"]["sha256"])
        return context, precommit, cell, slot, reopened[0], entry, value

    def test_known_metadata_projects_exact_eight_fields_without_mutating_full_identity(self):
        session = _session_data()
        before = deepcopy(session)
        projected = self.project(session, symbol="XAGUSD")
        fields = ("symbol", "timeframe", "activation_sha256", "bars_sha256", "sidecar_sha256",
            "provenance_sha256", "request_sha256", "spread_unit")
        self.assertEqual(projected, {key: before["m5_source_identity"][key] for key in fields})
        self.assertEqual(len(projected), 8)
        self.assertLessEqual(len(projected), grid.MAX_SOURCE_IDENTITY_ENTRIES)
        self.assertNotIn("activation_id", projected)
        self.assertNotIn("point", projected)
        self.assertEqual(session, before)
        self.assertEqual(len(session["m5_source_identity"]), 10)

    def test_absent_failed_and_unavailable_sessions_remain_unknown(self):
        self.assertEqual(self.project(None, symbol="XAGUSD"), {})
        for change in ({"failure": {"failure_code": "SYNTHETIC_SOURCE_FAILURE"}},
                {"source_state": "FAILED"}, {"cost_status": "COST_UNAVAILABLE"}):
            session = _session_data()
            session.update(change)
            with self.subTest(change=change):
                self.assertEqual(self.project(session, symbol="XAGUSD"), {})

    def test_corrupt_ready_identity_and_nonfalse_authorities_cannot_be_projected(self):
        changes = [lambda value: value.update(source_kind="broker_native"),
            lambda value: value.update(unverified_caller_source_or_fit_accepted=True),
            lambda value: value.update(m5_source_identity=None)]
        changes += [lambda value, key=key, bad=bad: value.update({key: bad}) for key, bad in
            (("qualification_authority", True), ("capital_authority", True),
             ("live_authority", 0), ("order_authority", None))]
        changes += [lambda value, change=change: value["m5_source_identity"].update(change) for change in
            ({"symbol": "EURUSD"}, {"timeframe": "H1"}, {"spread_unit": "price"},
             {"bars_sha256": "NOT_A_DIGEST"}, {"request_sha256": False})]
        for change in changes:
            session = _session_data()
            change(session)
            with self.assertRaises(self.scope["HistoricalSchedulerError"]) as caught:
                self.project(session, symbol="XAGUSD")
            self.assertEqual(caught.exception.code, "DECLARED_M5_STOP_SOURCE_IDENTITY_INVALID")

    def test_actual_stop_construction_keeps_unknown_and_known_rows_honest(self):
        recorder = next(n for n in self.tree.body if isinstance(n, ast.FunctionDef)
            and n.name == "_declared_m5_record_terminal_refusal")
        assignment = next(n for n in ast.walk(recorder) if isinstance(n, ast.Assign)
            and any(isinstance(v, ast.Name) and v.id == "stopped" for v in n.targets))
        for session in (_session_data(), None):
            identity = self.project(session, symbol="XAGUSD")
            scope = {"grid": grid, "grid_identity": identity, "abort_report": None,
                "row": {"cell": {"symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"}},
                "refusal": {"cycle_start_utc": "2020-01-01T00:00:00+00:00",
                    "cycle_end_utc": "2020-01-29T00:00:00+00:00",
                    "information_cutoff_utc": "2019-12-31T00:00:00+00:00",
                    "reason_code": "SYNTHETIC_ORIGINAL_STOP_REASON"},
                "process": SimpleNamespace(receipt_sha256="1" * 64),
                "code": {"pm_historical_scheduler.py": "2" * 64},
                "produced": "2026-01-01T00:00:00+00:00"}
            exec(compile(ast.fix_missing_locations(ast.Module(body=[assignment], type_ignores=[])),
                str(SOURCE), "exec"), scope)
            row = scope["stopped"]
            self.assertEqual(row.outcome, "STOP")
            self.assertEqual(row.reason_code, "SYNTHETIC_ORIGINAL_STOP_REASON")
            self.assertEqual(row.source_identity, identity)
            self.assertEqual(row.cost_tier, "NONE")
            self.assertEqual(row.metrics, {})
            for key in ("view_key", "view_contract_sha256", "job_binding_sha256", "consumer_binding_sha256",
                    "source_contract_sha256", "no_trade_run_receipt_sha256", "strategy_family", "configuration_sha256"):
                self.assertIsNone(getattr(row, key))
            for key in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
                self.assertIs(getattr(row, key), False)
            self.assertEqual(grid.row_from_dict(row.to_dict()).to_bytes(), row.to_bytes())

    def test_actual_enrichment_failure_keeps_unknown_identity_and_original_stop(self):
        recorder = next(n for n in self.tree.body if isinstance(n, ast.FunctionDef)
            and n.name == "_declared_m5_record_terminal_refusal")
        node = next(n for n in ast.walk(recorder) if isinstance(n, ast.If)
            and any(isinstance(v, ast.Name) and v.id == "identity_error" for v in ast.walk(n)))
        class AuthenticationFailureUnitDouble(RuntimeError):
            code = "S" * 400
        def reject_only(*args, **kwargs):
            raise AuthenticationFailureUnitDouble(LABEL)
        result = {"status": "TERMINAL_STOP", "reason_code": "SYNTHETIC_ORIGINAL_STOP_REASON"}
        scope = {"abort_report": None, "result": result, "grid_identity": {}, "context": None,
            "saved": ({"classification": LABEL},), "precommit": None, "process": None, "frozen": None,
            "row": {"cell": {"symbol": "XAGUSD"}}, "slot": {},
            "_declared_m5_recorded_stop_grid_identity": reject_only}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])),
            str(SOURCE), "exec"), scope)
        self.assertEqual(scope["grid_identity"], {})
        self.assertEqual(result["status"], "TERMINAL_STOP")
        self.assertEqual(result["reason_code"], "SYNTHETIC_ORIGINAL_STOP_REASON")
        self.assertEqual(result["terminal_refusal_source_identity_error"], "S" * 256)

    def test_original_stored_commit_reader_and_metadata_validator_retain_recorded_identity(self):
        context, precommit, cell, slot, commit, entry, value = self.stored_fixture()
        before = deepcopy(commit)
        recorded = self.scope["_declared_m5_recorded_stop_grid_identity"](context, commit=commit,
            precommit=precommit, cell=cell, slot=slot)
        self.assertEqual(recorded, self.project(value, symbol="XAGUSD"))
        self.assertEqual(len(recorded), 8)
        self.assertEqual(commit, before)
        self.assertEqual(entry, commit["completion"]["source_session_entry"])
        self.assertNotIn("view_key", recorded)
        # These are actual stored JSON/parser checks, not proof that the
        # invented session was published by a verified Native producer.

    def test_original_stored_reader_and_record_bindings_refuse_tampering(self):
        context, precommit, cell, slot, commit, entry, _ = self.stored_fixture()
        resolver = self.scope["_declared_m5_recorded_stop_grid_identity"]
        for change in (lambda value: value.update(generation_context_sha256="0" * 64),
                lambda value: value.update(artifact_roster=[]),
                lambda value: value["completion"]["request_body"].update(artifact_namespace="foreign-data")):
            changed = deepcopy(commit)
            change(changed)
            with self.assertRaises(self.scope["HistoricalSchedulerError"]):
                resolver(context, commit=changed, precommit=precommit, cell=cell, slot=slot)
        path = spool._local_path(context.root, entry["descriptor"]["relative_path"])
        with path.open("ab") as stream:
            stream.write(b" ")
        import pm_historical_production_adapter as adapter
        with self.assertRaises(adapter.HistoricalProductionAdapterError):
            resolver(context, commit=commit, precommit=precommit, cell=cell, slot=slot)


if __name__ == "__main__":
    unittest.main()
