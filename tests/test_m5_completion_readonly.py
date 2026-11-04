"""SYNTHETIC_METADATA_AND_SOURCE_CONTRACT_ONLY_NO_NATIVE_OR_FIT_POSITIVE.

Selected original definitions and invented metadata test dispatch/first refusals.
Recorder collaborators are UNIT_DOUBLES, never successful public verifiers.
No source, fitted capability, plan, issuer, spool, or native receipt is fabricated.
"""
import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest


ROOT = Path(__file__).resolve().parents[1]
EXECUTION = ROOT / "pm_authoritative_search_execution.py"
ADAPTER = ROOT / "pm_historical_production_adapter.py"


def tree(path):
    return ast.parse(path.read_bytes(), filename=str(path))


def definition(path, name):
    return next(node for node in ast.walk(tree(path))
        if isinstance(node, ast.FunctionDef) and node.name == name)


def load_definitions(path, names, namespace=None):
    parsed = tree(path)
    nodes = [node for node in parsed.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    if {node.name for node in nodes} != set(names):
        raise AssertionError("selected actual definitions missing")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    ns = {"__name__": "synthetic_metadata_only", **(namespace or {})}
    exec(compile(ast.fix_missing_locations(ast.Module(
        body=[future, *nodes], type_ignores=[])), str(path), "exec"), ns)
    return ns


def helpers():
    execution = load_definitions(EXECUTION,
        {"AuthoritativeSearchExecutionError", "_fail", "_check_m5_bound_reopen_window",
         "_require_executable_bound_fit", "_declared_fitted_for_execution",
         "_publish_declared_m5_bound_fit", "execute_historical_observed_cell",
         "_execute_current_observed_cell", "_make_declared_unit_header"})
    adapter = load_definitions(ADAPTER,
        {"HistoricalProductionAdapterError", "_fail", "_declared_m5_headerless_shape",
         "_declared_slot_is_m5_source"})
    return execution, adapter


def shape(status="NO_TRADE"):
    return {"status": status,
        "reason_code": "SOURCE_BOUNDARY_SHORTFALL_ALL_CANDIDATES",
        "plan_transport": {}, "horizon_plan": {},
        "technical_failure_entry": {"SYNTHETIC_SHAPE_ONLY": True} if status == "STOP" else None,
        "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False}


class CompletionReadOnlyMetadataTests(unittest.TestCase):
    def test_readonly_window_mode_does_not_read_now_and_default_still_expires(self):
        e, _ = helpers()
        calls = []
        e["_now_utc_seconds"] = lambda: calls.append("now") or "2030-01-01T00:00:00Z"
        body = {"source_access_window_utc": {
            "start_utc": "2026-01-01T00:00:00Z", "end_exclusive_utc": "2026-01-03T00:00:00Z"}}
        original = deepcopy(body)
        e["_check_m5_bound_reopen_window"](body, verification_only=True)
        self.assertEqual(calls, [])
        self.assertEqual(body, original)
        with self.assertRaises(e["AuthoritativeSearchExecutionError"]) as caught:
            e["_check_m5_bound_reopen_window"](body, verification_only=False)
        self.assertEqual(caught.exception.code, "BOUND_FIT_ORIGINAL_WINDOW_CLOSED")
        self.assertEqual(calls, ["now"])
        for mode in (0, 1, None, "readonly"):
            with self.subTest(mode=mode):
                with self.assertRaises(e["AuthoritativeSearchExecutionError"]) as invalid:
                    e["_check_m5_bound_reopen_window"](body, verification_only=mode)
                self.assertEqual(invalid.exception.code, "BOUND_FIT_VERIFICATION_MODE_INVALID")

    def test_readonly_owner_refuses_execution_and_publication_before_dependencies(self):
        e, _ = helpers()
        # Ordinary first-guard DATA, not an actual _BoundFitOwner or fitted capability.
        fitted = SimpleNamespace(_bound_fit_owner=SimpleNamespace(verification_only=True))
        for invoke in (
            lambda: e["_require_executable_bound_fit"](fitted),
            lambda: e["_declared_fitted_for_execution"](None, fitted, None),
            lambda: e["_publish_declared_m5_bound_fit"](None, fitted, None, None),
            lambda: e["execute_historical_observed_cell"](capture=None, fitted=fitted, regime=None,
                search_process=None, cell_plan=None, config=None),
            lambda: e["_execute_current_observed_cell"](process=None, sink=None, cell_plan=None,
                fitted=fitted, config=None, _generation=None),
            lambda: e["_make_declared_unit_header"](None, fitted, None, None, None, None, None, None),
        ):
            with self.assertRaises(e["AuthoritativeSearchExecutionError"]) as caught:
                invoke()
            self.assertEqual(caught.exception.code, "BOUND_FIT_VERIFICATION_ONLY_NOT_EXECUTABLE")
        e["_require_executable_bound_fit"](SimpleNamespace(_bound_fit_owner=None))
        e["_require_executable_bound_fit"](
            SimpleNamespace(_bound_fit_owner=SimpleNamespace(verification_only=False)))

    def test_actual_reopen_publication_branch_is_inert_only_in_readonly_mode(self):
        node = definition(EXECUTION, "_reopen_m5_bound_fitted_timeframe")
        branch = next(statement for statement in node.body if isinstance(statement, ast.If)
            and any(isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                and call.func.attr == "_publish_exact_json" for call in ast.walk(statement)))
        writes = []
        # Execute ONLY the original conditional writer branch with an in-memory recorder.
        # This is not a spool/verifier substitute and proves no actual durability.
        ns = {"context": SimpleNamespace(writable=True, limits=SimpleNamespace(max_json_bytes=128)),
            "body": {}, "receipt": {}, "_sha256": lambda _: "a"*64,
            "_declared_unit_relative": lambda *_: "owned/header.json",
            "authority_spool": SimpleNamespace(
                _publish_exact_json=lambda *args: writes.append(args))}
        block = compile(ast.fix_missing_locations(ast.Module(
            body=[deepcopy(branch)], type_ignores=[])), str(EXECUTION), "exec")
        ns["verification_only"] = True
        exec(block, ns)
        self.assertEqual(writes, [])
        ns["verification_only"] = False
        exec(block, ns)
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][1], "owned/fit-reopens/" + "a"*64 + ".json")

    def test_headerless_shape_rejects_frozen_owner_raw_and_wrong_terminal_labels(self):
        _, a = helpers()
        check = a["_declared_m5_headerless_shape"]
        # Shape acceptance is not the saved-byte/source/fit/failure verification.
        check(shape())
        check(shape("STOP"))
        forbidden = ("owner", "winner_request_entry", "winner_decision_entry",
            "winner_request_sha256", "winner_decision_sha256", "frozen_plan_receipt",
            "frozen_descriptor_graph", "horizon_winner_cross_binding",
            "raw_production_receipt", "production_descriptor_graph")
        mutations = [(key, {}) for key in forbidden]
        mutations += [("status", "OWNER"), ("status", "MISSING"),
            ("reason_code", "NO_WINNER"), ("technical_failure_entry", {}),
            ("plan_transport", None), ("horizon_plan", None)]
        mutations += [(key, value) for key in
            ("qualification_authority", "capital_authority", "live_authority", "order_authority")
            for value in (True, 0, None)]
        for key, value in mutations:
            body = shape()
            body[key] = value
            with self.subTest(key=key, value=value):
                with self.assertRaises(a["HistoricalProductionAdapterError"]) as caught:
                    check(body)
                self.assertEqual(str(caught.exception), "DECLARED_M5_HEADERLESS_COMPLETION_INVALID")
        stop = shape("STOP")
        stop["technical_failure_entry"] = None
        with self.assertRaises(a["HistoricalProductionAdapterError"]):
            check(stop)

    def test_actual_dispatch_expressions_select_m5_only_for_declared_m5_kind(self):
        _, a = helpers()
        # Classification UNIT_DOUBLE only; no successful public verifier is stubbed.
        a["orchestration"] = SimpleNamespace(declared_slot_source_kind=lambda b: b.get("source_kind"))
        calls = []
        planner = SimpleNamespace(
            verify_m5_derived_production_cell_search_plan=lambda: calls.append("M5"),
            verify_production_cell_search_plan=lambda: calls.append("NATIVE"))
        factory = definition(ADAPTER, "_make_declared_slot_capture_authority")
        declared_open = next(node for node in factory.body
            if isinstance(node, ast.FunctionDef) and node.name == "open_capture")
        capture_expr = next(node for node in ast.walk(declared_open)
            if isinstance(node, ast.IfExp))
        for schema in ("fxpm.declared-m5-slot-producer-request.v1",
                       "fxpm.declared-development-m5-slot-producer-request.v1"):
            expr = compile(ast.Expression(deepcopy(capture_expr)), str(ADAPTER), "eval")
            verifier = eval(expr, {"planner": planner,
                "_declared_slot_is_m5_source": a["_declared_slot_is_m5_source"],
                "body": {"schema_version": schema, "source_kind": "m5_derived"}})
            self.assertIs(verifier, planner.verify_m5_derived_production_cell_search_plan)
        verifier = eval(expr, {"planner": planner,
            "_declared_slot_is_m5_source": a["_declared_slot_is_m5_source"],
            "body": {"schema_version": "fxpm.declared-slot-producer-request.v2"}})
        self.assertIs(verifier, planner.verify_production_cell_search_plan)
        for body in (
            {"schema_version": "fxpm.declared-development-m5-slot-producer-request.v1", "source_kind": "native"},
            {"schema_version": "unknown", "source_kind": "m5_derived"},
        ):
            with self.assertRaises(a["HistoricalProductionAdapterError"]):
                a["_declared_slot_is_m5_source"](body)
        declared = type("UnitDeclaredCapture", (), {})
        other = type("UnitOtherCapture", (), {})
        executor_expr = next(node for node in ast.walk(
            definition(EXECUTION, "_verified_historical_cell_plan_inputs"))
            if isinstance(node, ast.IfExp))
        code = compile(ast.Expression(deepcopy(executor_expr)), str(EXECUTION), "eval")
        for cls, kind, expected in (
            (declared, "m5_derived", planner.verify_m5_derived_production_cell_search_plan),
            (declared, "native", planner.verify_production_cell_search_plan),
            (other, "m5_derived", planner.verify_production_cell_search_plan),
        ):
            capture = cls()
            capture.request_body = {"source_kind": kind}
            chosen = eval(code, {"production_planner": planner, "capture": capture,
                "historical_adapter": SimpleNamespace(DeclaredSlotSearchCapture=declared),
                "historical": SimpleNamespace(declared_slot_source_kind=lambda b: b["source_kind"])})
            self.assertIs(chosen, expected)
        self.assertEqual(calls, [])

    def test_saved_plan_reader_has_no_issue_write_or_model_fallback_and_native_defaults_hold(self):
        reader = definition(ADAPTER, "_read_declared_m5_headerless_plan")
        calls = [call.func.attr if isinstance(call.func, ast.Attribute) else call.func.id
            for call in ast.walk(reader) if isinstance(call, ast.Call)
            and isinstance(call.func, (ast.Attribute, ast.Name))]
        for required in ("_read_existing_m5_plan_json", "reopen_m5_derived_historical_horizon_plan",
                         "_verify_state_context_plan_binding", "_strict_m5_attempt_artifacts"):
            self.assertIn(required, calls)
        self.assertFalse(set(calls) & {"ProductionCellSearchPlan", "FittedNativeSearchTimeframe",
            "put_json", "open_sink", "register", "fit", "_reverify_fitted_timeframe",
            "build_declared_slot_m5_derived_observed_cell_plan"})
        completion = definition(ADAPTER, "_verify_declared_m5_slot_completion_request")
        reopen = next(call for call in ast.walk(completion)
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
            and call.func.attr == "reopen_bound_fitted_timeframe")
        self.assertTrue(next(keyword.value.value for keyword in reopen.keywords
            if keyword.arg == "verification_only"))
        wrapper = definition(EXECUTION, "reopen_bound_fitted_timeframe")
        self.assertIs(wrapper.args.kw_defaults[-1].value, False)
        native_guard = next(statement for statement in wrapper.body if isinstance(statement, ast.If)
            and any(isinstance(call, ast.Call) and call.args
                and isinstance(call.args[0], ast.Constant)
                and call.args[0].value == "BOUND_FIT_NATIVE_REJECTS_M5_CONTEXT"
                for call in ast.walk(statement)))
        self.assertIn("verification_only is not False", ast.unparse(native_guard.test))
        reverify = definition(EXECUTION, "_reverify_fitted_timeframe")
        self.assertTrue(any("_bound_fit_owner" in ast.unparse(statement)
            for statement in reverify.body[:3]))


if __name__ == "__main__":
    unittest.main()
