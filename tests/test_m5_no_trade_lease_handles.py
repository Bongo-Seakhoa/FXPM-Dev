"""SYNTHETIC_DATA_ONLY: actual lease bookkeeping and selected scheduler statements.

The ordinary cost-consumer double proves handle use, not native authentication,
frozen-cost validity, account completion or financial eligibility. Only the IO
test writes invented Grid metadata, under an explicitly owned retained QA root.
"""
import ast
from dataclasses import asdict, replace
import hashlib
import importlib.util
import os
from pathlib import Path
import stat
from types import SimpleNamespace
from typing import Any, Sequence
import unittest
import uuid

import pm_end_grid as grid
import pm_m5_job_view as job


PROVENANCE = "SYNTHETIC_DATA_ONLY_NO_NATIVE_OR_FROZEN_COST_VERIFICATION"
REPO_ROOT = Path(job.__file__).resolve().parent
SCHEDULER_SOURCE = Path(__file__).resolve().parents[1] / "pm_historical_scheduler.py"


def _existing_fixtures():
    path = REPO_ROOT / "tests" / "test_m5_job_view.py"
    spec = importlib.util.spec_from_file_location("_no_trade_existing_job_fixtures", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if module.job is not job or Path(grid.__file__).resolve().parent != REPO_ROOT:
        raise RuntimeError("SYNTHETIC_TEST_MODULE_ORIGIN_MISMATCH")
    return module


def _selected_scheduler_statements():
    tree = ast.parse(SCHEDULER_SOURCE.read_text(encoding="utf-8"))
    functions = {node.name: node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))}
    function = functions["_declared_m5_append_grid"]
    loop = next(node for node in function.body if isinstance(node, ast.For))
    branch = next(node for node in loop.body if isinstance(node, ast.If)
                  and ast.unparse(node.test) == "actual is not None")
    record = next(node for node in loop.body if isinstance(node, ast.Assign)
                  and any(isinstance(target, ast.Name) and target.id == "record" for target in node.targets))
    support = [functions[name] for name in ("HistoricalSchedulerError", "_fail",
                                           "_declared_m5_utc", "_declared_m5_grid_utc")]
    namespace = {"Any": Any, "Sequence": Sequence, "views": job, "grid": grid}
    exec(compile(ast.Module(body=support, type_ignores=[]), str(SCHEDULER_SOURCE), "exec"), namespace)
    return namespace, compile(ast.Module(body=branch.orelse, type_ignores=[]), str(SCHEDULER_SOURCE), "exec"), \
        compile(ast.Module(body=[record], type_ignores=[]), str(SCHEDULER_SOURCE), "exec")


class _SoftwareOwner:
    """Ordinary adapter to the real private state; never a M5JobViews instance."""
    provenance = PROVENANCE

    def __init__(self, fixtures):
        self.fixtures = fixtures
        self.source_identity = fixtures._source("XAGUSD")
        self.request = fixtures._request("slot-view", source=self.source_identity,
            consumer=None, result_key="UNIT_SLOT")
        self.calibration = replace(self.request, view_id="calibration-view",
            purpose="COST_EDITION_REPRODUCTION_ONLY", result_key=fixtures.TERMINAL,
            consumer=job.CalibrationViewBinding(fixtures._sha("UNIT_PROFILE")))
        budgets = fixtures._budgets()
        self.binding = fixtures._binding(budgets, (self.request, self.calibration),
            required_result_keys=("UNIT_SLOT", fixtures.TERMINAL))
        self.state = job._JobState(self.binding, (self.request, self.calibration), budgets=budgets)
        source = replace(fixtures._archive(), symbol="XAGUSD", source_identity=self.source_identity)
        self.loader = fixtures._UnitLoader(self.binding, budgets, source=source)
        self.job_binding_sha256 = self.state.binding_sha256
        self.handles, self.events = [], []

    def acquire(self, view_id, *, consumer, result_key):
        self.events.append(("acquire", view_id, len(self.state.active)))
        handle = self.state.acquire(view_id, self.loader, consumer=consumer, result_key=result_key)
        self.handles.append(handle)
        return handle

    def view(self, handle):
        return self.state.view(handle)

    def consumer_binding(self, handle):
        return self.state.consumer_binding(handle)

    def consumer_bindings_for(self, key):
        return self.state.consumer_bindings_for(key)

    def route_at(self, cutoff):
        if cutoff != self.request.cutoff_utc:
            raise AssertionError("unexpected synthetic cutoff")
        return {"source_identity_sha256": self.fixtures._sha("UNIT_SOURCE_IDENTITY")}


class _CostConsumerBoundary:
    """Ownership-only double; documentary P75 metadata is not a verified tier."""
    provenance = PROVENANCE

    def __init__(self, fixtures, *, fail=False):
        self.sha256 = fixtures._sha("UNIT_PROFILE")
        self.fail, self.calls = fail, []

    def bind_view(self, owner, *, context, calibration_lease, request_lease, point):
        self.owner, self.calibration, self.request = owner, calibration_lease, request_lease
        self.calls.append("bind_view")
        request = self._live()
        if type(owner.consumer_binding(calibration_lease)) is not job.CalibrationViewBinding:
            raise AssertionError("calibration handle lost its purpose")
        if type(owner.consumer_binding(request_lease)) is not job.SlotViewBinding:
            raise AssertionError("request handle lost its consumer")
        if len(owner.state.active) != 2:
            raise AssertionError("both actual leases must be active at the boundary")
        if self.fail:
            raise ValueError("SYNTHETIC_CONSUMER_FAILURE")
        self.window = {"source_kind": "m5_derived", "view_key": request.view_key,
                       "m5_view_contract_sha256": owner.binding.m5_view_contract_sha256}
        return self

    def _live(self):
        self.owner.view(self.calibration)
        return self.owner.view(self.request)

    def resolve(self, symbol, timeframe, tier, *, point, source_identity_sha256):
        self._live()
        if (symbol, timeframe, tier) != ("XAGUSD", "M15", "P75"):
            raise AssertionError("unexpected synthetic baseline arguments")
        self.calls.append("resolve")
        return self

    def descriptor(self):
        self._live()
        self.calls.append("descriptor")
        return {"provenance": PROVENANCE, "m5_execution_view_binding": dict(self.window)}

    def from_descriptor(self, descriptor, **kwargs):
        self._live()
        self.calls.append("from_descriptor")
        if descriptor["m5_execution_view_binding"] != self.window:
            raise AssertionError("synthetic documentary window changed")
        return self


def _qa_grid_root():
    configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
    if configured is None:
        raise unittest.SkipTest("FXPM_DERIVED_QA_ROOT is required for retained invented Grid IO")
    root = Path(configured)
    if not configured or not root.is_absolute():
        raise RuntimeError("EXPLICIT_ABSOLUTE_QA_ROOT_REQUIRED")
    for path in (*reversed(root.parents), root):
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise RuntimeError("QA_ROOT_REPARSE_REFUSED")
    if not root.is_dir():
        raise RuntimeError("EXISTING_QA_DIRECTORY_REQUIRED")
    owned = root / ("no-trade-handles-" + uuid.uuid4().hex)
    owned.mkdir(exist_ok=False)
    return owned


class NoTradeLeaseHandleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixtures = _existing_fixtures()
        cls.support, cls.branch_code, cls.record_code = _selected_scheduler_statements()

    def setUp(self):
        self.owner = _SoftwareOwner(self.fixtures)
        self.edition = _CostConsumerBoundary(self.fixtures)

    def _run_branch(self, *, calibration_id="calibration-view"):
        namespace = dict(self.support, owner=self.owner, edition=self.edition, context=object(),
            row={"cell": {"symbol": "XAGUSD", "timeframe": "M15", "regime": "TREND"}},
            key="UNIT_SLOT", commits={"UNIT_SLOT": {"completion": {
                "request_sha256": self.fixtures._sha("UNIT_SLOT_REQUEST")}}},
            dev={"slot_view_ids": {"UNIT_SLOT": "slot-view"}, "calibration_view_id": calibration_id})
        exec(self.branch_code, namespace)
        return namespace

    def _assert_released(self):
        self.assertEqual(self.owner.state.active, {})
        self.assertFalse(self.owner.state.closed)
        self.assertTrue(self.owner.state.cache)
        for handle in self.owner.handles:
            with self.assertRaises(job.M5JobViewError) as caught:
                self.owner.view(handle)
            self.assertEqual(caught.exception.code, "M5_VIEW_LEASE_RELEASED")

    def test_actual_enter_returns_value_and_old_as_binding_refuses_then_unwinds(self):
        handle = self.owner.acquire("slot-view", consumer=job.SlotViewBinding(
            self.fixtures._sha("UNIT_SLOT_REQUEST"), "UNIT_SLOT"), result_key="UNIT_SLOT")
        with self.assertRaises(job.M5JobViewError) as caught:
            with handle as value:
                self.assertIs(type(value), job.DerivedPrefixView)
                self.assertIs(self.owner.view(handle), value)
                self.owner.view(value)
        self.assertEqual(caught.exception.code, "M5_VIEW_LEASE_NOT_OWNED")
        self._assert_released()

    def test_actual_no_trade_branch_passes_live_handles_and_releases_both(self):
        result = self._run_branch()
        self.assertEqual(self.owner.events, [("acquire", "slot-view", 0),
                                             ("acquire", "calibration-view", 1)])
        self.assertEqual(self.edition.calls, ["bind_view", "resolve", "descriptor", "from_descriptor"])
        self.assertEqual((result["outcome"], result["reason"], result["tier"]),
                         ("NO_TRADE", "DECLARED_DEVELOPMENT_VERIFIED_NO_TRADE", "P75"))
        self.assertIsNone(result["source_contract"])
        self.assertEqual(result["window"]["view_key"], result["live_view"].view_key)
        self.assertEqual(set(self.owner.state.status.values()), {"USED"})
        self._assert_released()

    def test_undeclared_calibration_acquisition_releases_request_without_cost_call(self):
        with self.assertRaises(job.M5JobViewError) as caught:
            self._run_branch(calibration_id="NOT_DECLARED")
        self.assertEqual(caught.exception.code, "M5_VIEW_NOT_DECLARED")
        self.assertEqual(self.edition.calls, [])
        self.assertEqual(self.owner.state.status["calibration-view"], "PENDING")
        self._assert_released()

    def test_cost_consumer_exception_unwinds_both_without_finalizing_owner(self):
        self.edition.fail = True
        with self.assertRaisesRegex(ValueError, "SYNTHETIC_CONSUMER_FAILURE"):
            self._run_branch()
        self.assertEqual(self.edition.calls, ["bind_view"])
        self.assertEqual(set(self.owner.state.status.values()), {"USED"})
        self._assert_released()

    def test_actual_selected_grid_row_roundtrips_after_handle_release(self):
        root = _qa_grid_root()
        result = self._run_branch()
        f = self.fixtures
        contract = {"schema_version": "synthetic.no-trade-lease-reconstruction.v1",
            "provenance": PROVENANCE, "interval": asdict(self.owner.request.interval),
            "view_key": result["window"]["view_key"], "cutoff_utc": self.owner.request.cutoff_utc}
        result.update(source=self.owner.source_identity, grid=grid,
            requests={"slot-view": asdict(self.owner.request)}, view_id="slot-view",
            contract_sha=grid.store_view_contract(root, contract),
            run_descriptor={"sha256": f._sha("UNIT_RUN_ARTIFACT")},
            process=SimpleNamespace(receipt_sha256=self.owner.binding.search_process_receipt_sha256),
            consumers=self.owner.consumer_bindings_for("UNIT_SLOT"),
            code={"pm_historical_scheduler.py": hashlib.sha256(SCHEDULER_SOURCE.read_bytes()).hexdigest()},
            slot={"activation_utc": f._stamp(60), "end_exclusive_utc": f._stamp(120),
                  "native_cutoff_utc": f._stamp(30)}, produced=f._stamp(120))
        exec(self.record_code, result)
        record = result["record"]
        receipt = grid.EndGridWriter(root).append(record)
        rows = grid.read_end_grid(root, "XAGUSD")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].to_bytes(), record.to_bytes())
        self.assertEqual(receipt["row_sha256"], record.row_sha256)
        self.assertTrue(receipt["appended"])
        self.assertEqual((record.outcome, record.cost_tier), ("NO_TRADE", "P75"))
        self.assertIsNone(record.source_contract_sha256)
        self.assertEqual(record.no_trade_run_receipt_sha256, f._sha("UNIT_RUN_ARTIFACT"))
        self.assertEqual(record.consumer_binding_sha256, result["consumers"][0])
        self.assertEqual(grid.load_view_contract(root, record.view_contract_sha256), contract)
        for name in grid.AUTHORITY_FLAGS:
            self.assertIs(getattr(record, name), False)
        self._assert_released()
