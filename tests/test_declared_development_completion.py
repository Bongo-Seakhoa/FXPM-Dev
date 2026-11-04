"""Synthetic metadata tests of actual extracted producer helpers; no native verification."""
import ast
from copy import deepcopy
import hashlib
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
import pm_historical_anchor_orchestration as o
import pm_production_anchor_plan as anchors
if Path(o.__file__).resolve() != _ROOT / "pm_historical_anchor_orchestration.py":
    raise RuntimeError("DEVELOPMENT_TEST_REQUIRES_CANDIDATE_ORCHESTRATION")


class SoftwareRefusal(ValueError):
    pass


def _refuse(code):
    raise SoftwareRefusal(code)


def _parent(count=1):
    launch = "2023-06-24T00:00:00Z"
    roster = anchors.derive_declared_owner_slot_roster_v2(launch_activation_utc=launch,
        frozen_refit_clock={"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
        config_identity_refs={"config_sha256": "1" * 64, "effective_config_sha256": "2" * 64},
        extend_frozen_clock_backward=True, protected_end_exclusive_utc="2024-07-14T08:05:00Z")
    scope = o._declared_development_scope_body(roster, [slot["slot_id"] for slot in roster["slots"][:count]])
    return {"schema_version": o.DECLARED_M5_DEVELOPMENT_SLOT_PRECOMMIT_SCHEMA,
        "slot_roster": roster, "slot_roster_sha256": hashlib.sha256(o._declared_json(roster)).hexdigest(),
        "launch_activation_utc": launch, "cells": [{"symbol": "XAGUSD"}],
        "development_scope": scope, "development_scope_sha256": hashlib.sha256(o._declared_json(scope)).hexdigest()}


def _helpers(adapter):
    path = _ROOT / "pm_historical_search_producer.py"
    tree = ast.parse(path.read_bytes(), filename=str(path))
    names = {"_declared_completion_schema", "_declared_base_completion", "_declared_run_body"}
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    if {node.name for node in selected} != names:
        raise RuntimeError("ACTUAL_PRODUCER_HELPER_STRUCTURE_CHANGED")
    namespace = {"historical": o, "historical_adapter": adapter, "_fail": _refuse,
                 "_now_utc_seconds": lambda: "2024-07-01T00:00:00Z"}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {"DECLARED_CELL_RUN_SCHEMA", "DECLARED_DEVELOPMENT_CELL_RUN_SCHEMA"}:
                    namespace[target.id] = ast.literal_eval(node.value)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), namespace)
    return SimpleNamespace(**namespace)


def _adapter():
    # Ordinary dispatch unit double, never an original verifier or native authority.
    return SimpleNamespace(DECLARED_SLOT_COMPLETION_SCHEMA="fxpm.declared-slot-completion.v1",
        declared_slot_completion_schema=lambda body: "fxpm.declared-development-slot-completion.v1")


class DeclaredDevelopmentCompletionTests(unittest.TestCase):
    def test_missing_and_wrong_development_dispatch_refuse(self):
        body = _parent()
        for adapter in (SimpleNamespace(DECLARED_SLOT_COMPLETION_SCHEMA="legacy"),
                        SimpleNamespace(declared_slot_completion_schema=lambda body: "legacy")):
            helpers = _helpers(adapter)
            with self.assertRaisesRegex(SoftwareRefusal, "DECLARED_DEVELOPMENT_COMPLETION_NOT_IMPLEMENTED"):
                helpers._declared_completion_schema(body)

    def test_development_base_completion_retains_scope_and_false_authorities(self):
        body = _parent(); body.update(schema_version=o.DECLARED_M5_DEVELOPMENT_SLOT_REQUEST_SCHEMA,
            precommit_sha256="a" * 64, cell_id="b" * 64, slot_id=body["slot_roster"]["slots"][0]["slot_id"])
        request = SimpleNamespace(to_dict=lambda: deepcopy(body), receipt_sha256="c" * 64)
        row = _helpers(_adapter())._declared_base_completion(request, status="STOP",
            reason_code="SYNTHETIC_SOURCE_UNAVAILABLE", source_entry=None)
        self.assertEqual(row["schema_version"], "fxpm.declared-development-slot-completion.v1")
        self.assertEqual(row["development_scope_sha256"], body["development_scope_sha256"])
        self.assertEqual(row["request_body"], body)
        self.assertIs(row["design_contaminated"], True)
        self.assertIs(row["pristine_validation"], False)
        for key in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
            self.assertIs(row[key], False)

    def test_legacy_base_completion_keeps_original_schema_and_fields(self):
        body = {"schema_version": o.DECLARED_SLOT_REQUEST_SCHEMA_V2,
            "precommit_sha256": "a" * 64, "cell_id": "b" * 64, "slot_id": "original-slot"}
        request = SimpleNamespace(to_dict=lambda: deepcopy(body), receipt_sha256="c" * 64)
        row = _helpers(_adapter())._declared_base_completion(request, status="STOP", reason_code="SYNTHETIC", source_entry=None)
        self.assertEqual(row["schema_version"], "fxpm.declared-slot-completion.v1")
        self.assertNotIn("development_scope_sha256", row)
        self.assertNotIn("design_contaminated", row)
        self.assertNotIn("pristine_validation", row)

    def run_body(self, parent, summaries, declared_slots, terminal_stop=False):
        return _helpers(_adapter())._declared_run_body(SimpleNamespace(context_sha256="1" * 64),
            precommit=SimpleNamespace(receipt_sha256="2" * 64, to_dict=lambda: deepcopy(parent)),
            cell_id="3" * 64, summaries=summaries, declared_slots=declared_slots,
            terminal_stop=terminal_stop, previous=None, symbol_receipt=None, terminal_failure=None)

    def test_complete_selected_prefix_is_development_not_full13(self):
        for count in (1, 2, 3):
            parent = _parent(count)
            summaries = [{"slot_id": slot["slot_id"], "status": "NO_TRADE"} for slot in parent["slot_roster"]["slots"][:count]]
            row = self.run_body(parent, summaries, count)
            self.assertEqual(row["schema_version"], "fxpm.declared-development-cell-owner-run.v1")
            self.assertEqual(row["result_kind"], "DEVELOPMENT_PARTIAL")
            self.assertEqual(row["development_status"], "DEVELOPMENT_COMPLETE")
            self.assertEqual(row["declared_slots"], count)
            self.assertEqual(row["accepted_roster_slots"], 13)
            self.assertEqual(row["development_scope_sha256"], parent["development_scope_sha256"])
            self.assertNotIn("calendar_days", row)
            self.assertIs(row["qualification_authority"], False)

    def test_wrong_count_order_foreign_and_incomplete_prefix_refuse(self):
        parent = _parent(2); slots = parent["slot_roster"]["slots"]
        valid = [{"slot_id": slot["slot_id"], "status": "NO_TRADE"} for slot in slots[:2]]
        cases = ((valid, 13), (list(reversed(valid)), 2), ([dict(valid[0], slot_id="foreign")], 2),
                 (valid[:1], 2), ([valid[0], valid[0]], 2), (valid + [valid[0]], 2))
        for summaries, count in cases:
            with self.subTest(summaries=summaries, count=count), self.assertRaisesRegex(
                    SoftwareRefusal, "DECLARED_DEVELOPMENT_COMPLETION_PREFIX_INVALID"):
                self.run_body(parent, summaries, count)

    def test_stopped_prefix_is_not_success(self):
        parent = _parent(2)
        row = self.run_body(parent, [], 2, terminal_stop=True)
        self.assertEqual(row["development_status"], "STOP")
        self.assertEqual(row["completed_slots"], 0)
        self.assertIs(row["terminal_stop"], True)
        self.assertIs(row["qualification_authority"], False)

    def test_legacy_run_receipt_has_no_development_claim(self):
        parent = {"schema_version": o.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2}
        row = self.run_body(parent, [], 13, terminal_stop=True)
        self.assertEqual(row["schema_version"], "fxpm.declared-cell-owner-run.v1")
        self.assertNotIn("development_scope", row)
        self.assertNotIn("development_status", row)
        self.assertNotIn("accepted_roster_slots", row)


def _cell_source_helpers():
    """Actual metadata/cleanup bodies, without a successful verifier substitute."""
    import json
    import re
    path = _ROOT / "pm_historical_search_producer.py"
    tree = ast.parse(path.read_bytes(), filename=str(path))
    names = {"HistoricalSearchProducerError", "_fail", "_canonical_bytes", "_sha", "_hex64",
        "_declared_m5_cell_source_body", "_declared_m5_cell_source_path",
        "_read_declared_m5_cell_source_context", "_declared_cell_m5_arguments",
        "_release_declared_m5_reacquired_context", "_reopen_declared_m5_source_material",
        "_declared_m5_cost_route_matches", "_declared_m5_reuse_arguments"}
    selected = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef))
        and node.name in names]
    if {node.name for node in selected} != names:
        raise RuntimeError("ACTUAL_CELL_SOURCE_HELPERS_MISSING")
    constants = [node for node in tree.body if isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(isinstance(target, ast.Name) and target.id in {
            "_MAX_JSON_BYTES", "_HEX", "DECLARED_M5_CELL_SOURCE_CONTEXT_SCHEMA",
            "_DECLARED_M5_REOPEN_CONTEXT_MAX_BYTES", "_DECLARED_M5_CELL_SOURCE_POINTER_SCHEMA",
            "_DECLARED_M5_CELL_SOURCE_TYPE"}
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target]))]
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    namespace = {"__name__": "synthetic_cell_source_helpers", "historical": o,
        "hashlib": hashlib, "json": json, "re": re}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *constants, *selected],
        type_ignores=[])), str(path), "exec"), namespace)
    return SimpleNamespace(**namespace)


def _cell_source_fixture(count=2):
    """Ordinary DATA only; none of these routes has passed a native reader."""
    from dataclasses import asdict
    import json
    import pandas as pd
    sys.path.insert(0, str(_ROOT))
    import pm_m5_job_view as job
    from pm_explicit_derived_history import Interval, SourceIdentity, Stratum
    if Path(job.__file__).resolve() != _ROOT / "pm_m5_job_view.py":
        raise RuntimeError("CELL_TEST_REQUIRES_CANDIDATE_JOB_VIEW")
    provenance = "SYNTHETIC_METADATA_ONLY_NO_NATIVE_VERIFICATION_OR_SOURCE_HEADER_ISSUANCE"
    digest = lambda value: hashlib.sha256(json.dumps(value, ensure_ascii=True, allow_nan=False,
        sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()
    parent = _parent(count)
    parent.update(source_kind="m5_derived", effective_config_sha256="2" * 64,
        search_process_receipt_sha256="3" * 64, run_id="UNIT_CELL_RUN",
        spread_profile_identity={"profile_sha256": "4" * 64})
    parent["cells"][0].update(cell_id="5" * 64, timeframe="H1", regime="TREND")
    source = SourceIdentity("XAGUSD", "M5", provenance, *[str(i) * 64 for i in (1, 2, 3, 4, 5)], 0.001)
    slots = o.declared_slot_selected_slots(parent)
    requests, mapping = [], {}
    for slot in slots:
        cutoff = pd.Timestamp(slot["native_cutoff_utc"])
        view_id = "unit-view-" + str(slot["ordinal"])
        mapping[slot["slot_id"]] = view_id
        requests.append(job.ViewRequest(view_id, source, "H1",
            Interval((cutoff-pd.Timedelta(days=1)).isoformat(), cutoff.isoformat()), cutoff.isoformat(),
            "6" * 64, "7" * 64, slot["slot_id"], job.RECIPE_SHA256,
            stratum=Stratum("H1", "UNIT_CLASS", "UNIT_ALIGNMENT")))
    cutoff = job.PROTECTED_CUTOFF
    requests.append(job.ViewRequest("unit-calibration", source, "H1",
        Interval(requests[0].interval.start_utc, cutoff), cutoff, "6" * 64, "7" * 64,
        "unit-terminal", job.RECIPE_SHA256, consumer=job.CalibrationViewBinding("4" * 64),
        stratum=Stratum("H1", "UNIT_CLASS", "UNIT_ALIGNMENT"),
        purpose="COST_EDITION_REPRODUCTION_ONLY"))
    requests = tuple(requests)
    budgets = job.ViewBudgets(20, 262144, 20, 262144, 40, 524288)
    pins = {"view_plan_sha256": job.value_plan_sha256(requests),
        "cost_edition_identity": {"fixture_provenance": provenance, "profile_sha256": "4" * 64}}
    parent.update(m5_source_pins=pins, m5_view_contract_sha256=digest(pins))
    context = SimpleNamespace(context_sha256="8" * 64, config_sha256="1" * 64,
        limits=SimpleNamespace(max_json_bytes=4*1024*1024))
    binding = job.DerivedJobBinding(parent["m5_view_contract_sha256"], context.context_sha256,
        context.config_sha256, parent["effective_config_sha256"], parent["search_process_receipt_sha256"],
        "9" * 64, parent["run_id"], parent["cells"][0]["cell_id"], digest(asdict(budgets)),
        pins["view_plan_sha256"], tuple([slot["slot_id"] for slot in slots] + ["unit-terminal"]), "unit-terminal")
    cutoffs = tuple(sorted({request.cutoff_utc for request in requests}, key=pd.Timestamp))
    metadata = job._encode_m5_reopen_metadata(binding=binding, budgets=budgets, requests=requests,
        product_cutoffs=cutoffs, source_identity=source)
    return context, {"parent": parent, "precommit_sha256": "9" * 64,
        "cell_id": parent["cells"][0]["cell_id"], "cell_contract_sha256": "a" * 64,
        "owner_metadata": metadata, "fine_product_routes": [{"symbol": "XAGUSD", "timeframe": "M5",
            "cutoff_utc": value, "fixture_provenance": provenance} for value in cutoffs],
        "cost_edition_route": {"cost_edition_identity": pins["cost_edition_identity"], "fixture_provenance": provenance},
        "slot_view_ids": mapping, "calibration_view_id": "unit-calibration"}


class DeclaredCellSourceContextTests(unittest.TestCase):
    def test_metadata_binds_complete_selected_map_contract_routes_and_false_authorities(self):
        helpers = _cell_source_helpers()
        context, values = _cell_source_fixture()
        body = helpers._declared_m5_cell_source_body(context, **values)
        self.assertEqual(body["slot_view_ids"], values["slot_view_ids"])
        self.assertEqual(body["cell_contract_sha256"], values["cell_contract_sha256"])
        self.assertEqual(body["fine_product_routes"], values["fine_product_routes"])
        self.assertEqual(body["owner_metadata"], values["owner_metadata"])
        values["slot_view_ids"].clear()
        self.assertEqual(len(body["slot_view_ids"]), 2)
        for key in ("unverified_caller_source_or_fit_accepted", "qualification_authority",
                    "capital_authority", "live_authority", "order_authority"):
            self.assertIs(body[key], False)

    def test_foreign_missing_duplicate_or_calibration_slot_ids_refuse(self):
        helpers = _cell_source_helpers()
        for change in ("foreign", "missing", "duplicate", "calibration"):
            context, values = _cell_source_fixture()
            keys = list(values["slot_view_ids"])
            if change == "foreign":
                values["slot_view_ids"]["not-selected"] = values["slot_view_ids"].pop(keys[0])
            elif change == "missing":
                values["slot_view_ids"].pop(keys[0])
            elif change == "duplicate":
                values["slot_view_ids"][keys[1]] = values["slot_view_ids"][keys[0]]
            else:
                values["slot_view_ids"][keys[0]] = values["calibration_view_id"]
            with self.subTest(change=change), self.assertRaises(helpers.HistoricalSearchProducerError) as caught:
                helpers._declared_m5_cell_source_body(context, **values)
            self.assertEqual(caught.exception.code, "DECLARED_M5_CELL_VIEW_IDS_INVALID")

    def test_changed_context_policy_cost_route_and_product_cutoff_refuse(self):
        helpers = _cell_source_helpers()
        for change in ("context", "policy", "cost", "cutoff"):
            context, values = _cell_source_fixture()
            if change == "context":
                context.context_sha256 = "b" * 64
            elif change == "policy":
                values["parent"]["effective_config_sha256"] = "b" * 64
            elif change == "cost":
                values["cost_edition_route"]["cost_edition_identity"] = {"different": True}
            else:
                values["fine_product_routes"][0]["cutoff_utc"] = "2023-01-01T00:00:00Z"
            with self.subTest(change=change), self.assertRaises(helpers.HistoricalSearchProducerError) as caught:
                helpers._declared_m5_cell_source_body(context, **values)
            expected = "DECLARED_SLOT_PROFILE_BINDING_MISMATCH" if change == "cost" else "DECLARED_M5_CELL_SOURCE_BINDING_INVALID"
            self.assertEqual(caught.exception.code, expected)

    def test_missing_header_refuses_without_publication_or_source_reissue(self):
        helpers = _cell_source_helpers()
        # A failure-only absence callback does not stand in for a successful source verifier.
        helpers._read_declared_m5_cell_source_context.__globals__["_read_declared_record"] = lambda *args, **kwargs: None
        context, values = _cell_source_fixture(1)
        with self.assertRaises(helpers.HistoricalSearchProducerError) as caught:
            helpers._read_declared_m5_cell_source_context(context, precommit=SimpleNamespace(receipt_sha256="9" * 64),
                parent=values["parent"], cell_id=values["cell_id"], cell_contract_sha256=values["cell_contract_sha256"])
        self.assertEqual(caught.exception.code, "DECLARED_M5_CELL_SOURCE_CONTEXT_MISSING")

    def test_native_dispatch_refuses_every_m5_argument_before_owner_access(self):
        helpers = _cell_source_helpers()
        names = ("m5_owner", "slot_view_ids", "calibration_view_id", "cost_edition", "cell_contract_sha256")
        for name in names:
            arguments = dict.fromkeys(names)
            arguments[name] = object()
            with self.subTest(name=name), self.assertRaises(helpers.HistoricalSearchProducerError) as caught:
                helpers._declared_cell_m5_arguments({"schema_version": o.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2}, **arguments)
            self.assertEqual(caught.exception.code, "DECLARED_NATIVE_REQUEST_REJECTS_M5_ARGUMENTS")

    def test_acquired_leases_release_but_caller_owner_stays_open_even_on_release_failure(self):
        helpers = _cell_source_helpers()
        for owned in (False, True):
            events = []
            class UnitLease:
                def __init__(self, name, fail=False):
                    self.name, self.fail = name, fail
                def release(self):
                    events.append(self.name)
                    if self.fail:
                        raise RuntimeError("SYNTHETIC_LEASE_RELEASE_FAILURE")
            source = {"calibration_lease": UnitLease("calibration", True), "request_lease": UnitLease("request"),
                "owner": SimpleNamespace(close=lambda: events.append("owner"))}
            with self.subTest(owned=owned), self.assertRaisesRegex(RuntimeError, "SYNTHETIC_LEASE_RELEASE_FAILURE"):
                helpers._release_declared_m5_reacquired_context(source, close_owner=owned)
            self.assertEqual(events, ["calibration", "request", "owner"] if owned else ["calibration", "request"])

    def test_adapter_rejects_mixed_live_context_and_partial_provided_owner_pair(self):
        path = _ROOT / "pm_historical_production_adapter.py"
        tree = ast.parse(path.read_bytes(), filename=str(path))
        guard = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
            and node.name == "_declared_m5_completion_reuse_arguments")
        fail = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_fail")
        error_name = next(node.exc.func.id for node in ast.walk(fail) if isinstance(node, ast.Raise))
        error = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == error_name)
        future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
        namespace = {}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[future, error, fail, guard], type_ignores=[])),
            str(path), "exec"), namespace)
        cases = ((None, object(), None), (None, None, object()), ({}, object(), object()))
        for live, owner, edition in cases:
            with self.subTest(live=live), self.assertRaises(namespace[error_name]) as caught:
                namespace[guard.name](m5_source_context=live, m5_owner=owner, m5_cost_edition=edition)
            self.assertEqual(str(caught.exception), "DECLARED_M5_REUSE_ARGUMENTS_INVALID")


class DeclaredM5ColdDispatchTests(unittest.TestCase):
    def test_missing_original_header_refuses_before_any_bound_fit_or_model_call(self):
        helpers = _cell_source_helpers()
        # Failure-only storage absence; no successful verifier or native product is supplied.
        helpers._reopen_declared_m5_source_material.__globals__["_declared_partial_unit_header"] = lambda *args: None
        with self.assertRaises(helpers.HistoricalSearchProducerError) as caught:
            helpers._reopen_declared_m5_source_material(object(), request=object(), process=object(),
                frozen_pipeline_config=object(), body={}, m5_source_context=None)
        self.assertEqual(caught.exception.code, "DECLARED_UNIT_FITTED_STATE_REOPEN_UNAVAILABLE")

    def test_changed_original_source_entry_refuses_before_bound_fit_reopen(self):
        helpers = _cell_source_helpers()
        header = {"source_session_entry": {"fixture": "SYNTHETIC_ORIGINAL_ENTRY"}}
        helpers._reopen_declared_m5_source_material.__globals__["_declared_partial_unit_header"] = lambda *args: header
        source = dict.fromkeys(("owner", "request_lease", "calibration_lease", "cost_edition", "source_session"))
        source["source_session_entry"] = {"fixture": "SYNTHETIC_DIFFERENT_ENTRY"}
        with self.assertRaises(helpers.HistoricalSearchProducerError) as caught:
            helpers._reopen_declared_m5_source_material(object(), request=object(), process=object(),
                frozen_pipeline_config=object(), body={}, m5_source_context=source)
        self.assertEqual(caught.exception.code, "DECLARED_M5_REOPEN_CONTEXT_INVALID")

    def test_recorded_snapshot_metadata_guard_refuses_route_identity_or_profile_drift(self):
        helpers = _cell_source_helpers()
        _, values = _cell_source_fixture(1)
        body, route = values["parent"], values["cost_edition_route"]
        self.assertIs(helpers._declared_m5_cost_route_matches(body, route,
            profile_sha256="4"*64, actual_route=deepcopy(route)), True)
        for change in ("profile", "identity", "route"):
            actual, saved, profile = deepcopy(route), deepcopy(route), "4"*64
            if change == "profile":
                profile = "f"*64
            elif change == "identity":
                saved["cost_edition_identity"] = {"fixture": "WRONG_UNIT_IDENTITY"}
                actual = deepcopy(saved)
            else:
                actual["fixture_provenance"] = "CHANGED_ROUTE"
            with self.subTest(change=change):
                self.assertIs(helpers._declared_m5_cost_route_matches(body, saved,
                    profile_sha256=profile, actual_route=actual), False)

    def test_provided_cost_reuse_requires_both_original_typed_objects(self):
        helpers = _cell_source_helpers()
        for owner, edition in ((object(), None), (None, object()), (object(), object())):
            with self.subTest(owner=owner, edition=edition), self.assertRaises(helpers.HistoricalSearchProducerError) as caught:
                helpers._declared_m5_reuse_arguments(m5_owner=owner, cost_edition=edition)
            self.assertEqual(caught.exception.code, "DECLARED_M5_REUSE_ARGUMENTS_INVALID")


if __name__ == "__main__":
    unittest.main()
