"""Abort-state unit data: SYNTHETIC_PROTOCOL_ONLY_NO_NATIVE_VERIFICATION.

Existing pure job-view fixture helpers supply real _JobState and lease classes.
The public M5 constructor is NEVER bypassed to claim authentication: __new__
fixtures deliberately have no original context/products, and the unchanged
original final revalidator must refuse them. No verifier, receipt or seal is
mocked. A genuine successful abort report remains unexercised by this file.
"""
from dataclasses import replace
from pathlib import Path
import sys
import unittest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
import pm_m5_job_view as job
from test_m5_job_view import _archive, _binding, _budgets, _request, _rows, _sha, _UnitLoader

if Path(job.__file__).resolve() != _ROOT / "pm_m5_job_view.py":
    raise RuntimeError("ABORT_TEST_REQUIRES_EXACT_CANDIDATE_OWNER")


def _unverified_owner():
    """Real class/state, intentionally invalid original auth; no native object."""
    request = _request(consumer=None)
    budgets = _budgets()
    binding = _binding(budgets, (request,))
    state = job._JobState(binding, (request,), budgets=budgets)
    owner = object.__new__(job.M5JobViews)
    owner._state = state
    owner._context = None
    owner._data_dir = Path("C:/SYNTHETIC_NOT_OPENED")
    owner._products, owner._routes = {}, {}
    owner._source = _archive()
    owner._base_rows, owner._observation = None, None
    owner._spans, owner._examined = (request.interval,), ()
    return owner, state, request, budgets


class AbortBeforeConsumptionTests(unittest.TestCase):
    def assert_code(self, code, fn, *args):
        with self.assertRaises(job.M5JobViewError) as caught:
            fn(*args)
        self.assertEqual(caught.exception.code, code)

    def test_unused_real_class_refuses_original_auth_and_closes_without_report(self):
        owner, state, request, _ = _unverified_owner()
        self.assertIs(owner._final_revalidate.__func__, job.M5JobViews._final_revalidate)
        self.assertEqual(owner.pending_view_ids, (request.view_id,))
        self.assertEqual(owner.consumer_binding_sha256, ())
        self.assert_code("M5_JOB_ORIGINAL_PREFIX_PRODUCTS_REQUIRED",
                         owner.abort_before_consumption, "SYNTHETIC_HEADER_FAILURE")
        self.assertTrue(state.closed)
        self.assertEqual((state.active, state.cache, state.requests, state.uses), ({}, {}, {}, {}))
        self.assertEqual((owner._products, owner._routes), ({}, {}))
        self.assertIsNone(owner._context)
        self.assertIsNone(owner._source)
        self.assert_code("M5_JOB_CLOSED", owner.reopen_metadata)

    def test_active_real_lease_is_refused_without_closing_or_invalidating_it(self):
        owner, state, request, budgets = _unverified_owner()
        lease = state.acquire(request.view_id, _UnitLoader(state.binding, budgets),
            consumer=job.SlotViewBinding(_sha("actual-unit-slot"), "UNIT_SLOT"))
        self.assert_code("M5_JOB_ABORT_UNUSED_STATE_REQUIRED",
                         owner.abort_before_consumption, "SYNTHETIC_FAILURE")
        self.assertFalse(state.closed)
        self.assertIs(state.active[id(lease)], lease)
        self.assertIsNotNone(lease.view)
        lease.release()
        owner.close()

    def test_released_used_view_is_not_relabelled_unused(self):
        owner, state, request, budgets = _unverified_owner()
        lease = state.acquire(request.view_id, _UnitLoader(state.binding, budgets),
            consumer=job.SlotViewBinding(_sha("released-unit-slot"), "UNIT_SLOT"))
        lease.release()
        self.assertFalse(state.active)
        self.assertTrue(state.consumer_sha256)
        self.assert_code("M5_JOB_ABORT_UNUSED_STATE_REQUIRED",
                         owner.abort_before_consumption, "SYNTHETIC_FAILURE")
        self.assertFalse(state.closed)
        self.assertEqual(state.status[request.view_id], "USED")
        owner.close()

    def test_unused_and_stopped_states_cannot_become_an_unused_abort(self):
        for status in ("UNUSED", "STOPPED"):
            with self.subTest(status=status):
                owner, state, request, _ = _unverified_owner()
                if status == "UNUSED":
                    owner.release_unused(request.view_id, "UNIT_UNUSED_NOT_ABORT")
                else:
                    state.status[request.view_id] = status
                self.assert_code("M5_JOB_ABORT_UNUSED_STATE_REQUIRED",
                                 owner.abort_before_consumption, "SYNTHETIC_FAILURE")
                self.assertFalse(state.closed)
                self.assertEqual(state.status[request.view_id], status)
                owner.close()

    def test_pending_cached_or_examined_state_is_refused_without_teardown(self):
        mutations = ("source_usage", "cache", "cache_usage", "base_rows", "observation", "examined")
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                owner, state, request, _ = _unverified_owner()
                if mutation == "source_usage":
                    state._retain_source_row("canonical_m5", _rows()[0])
                elif mutation in ("cache", "cache_usage"):
                    getattr(state, mutation)["UNIT_CACHE"] = (1, 1)
                elif mutation == "base_rows":
                    owner._base_rows = ()
                elif mutation == "observation":
                    owner._observation = object()
                else:
                    owner._examined = (request.interval,)
                self.assert_code("M5_JOB_ABORT_UNUSED_STATE_REQUIRED",
                                 owner.abort_before_consumption, "SYNTHETIC_FAILURE")
                self.assertFalse(state.closed)
                owner.close()

    def test_bad_reason_incomplete_ledgers_and_closed_owner_are_refused(self):
        for reason in (None, "", " padded", "x" * 513):
            with self.subTest(reason=reason):
                owner, state, _, _ = _unverified_owner()
                self.assert_code("M5_JOB_ABORT_REASON_INVALID", owner.abort_before_consumption, reason)
                self.assertFalse(state.closed)
                owner.close()
        for ledger in ("status", "seen"):
            with self.subTest(ledger=ledger):
                owner, state, _, _ = _unverified_owner()
                getattr(state, ledger).clear()
                self.assert_code("M5_JOB_ABORT_UNUSED_STATE_REQUIRED",
                                 owner.abort_before_consumption, "SYNTHETIC_FAILURE")
                self.assertFalse(state.closed)
                owner.close()
        owner, _, _, _ = _unverified_owner()
        owner.close()
        self.assert_code("M5_JOB_CLOSED", owner.abort_before_consumption, "SYNTHETIC_FAILURE")

    def test_report_encoder_normalizes_real_binding_tuple_before_plain_report_detach(self):
        import ast
        from dataclasses import asdict
        owner, state, _, _ = _unverified_owner()
        try:
            actual = {"binding": asdict(state.binding), "budgets": asdict(state.budgets),
                "declared_view_ids": sorted(state.requests), "source_complete": False, "job_complete": False}
            self.assertIs(type(actual["binding"]["required_result_keys"]), tuple)
            self.assert_code("M5_REOPEN_METADATA_INVALID", job._bounded_reopen_plain, actual)
            detached = job._bounded_reopen_plain(actual, _encoding=True)
            self.assertIs(type(detached["binding"]["required_result_keys"]), list)
            self.assertEqual(detached["binding"]["required_result_keys"], list(state.binding.required_result_keys))
            plain = job._bounded_reopen_plain(detached)
            self.assertEqual(plain, detached)
            plain["binding"]["required_result_keys"].append("DETACHED_UNIT_VALUE")
            self.assertEqual(detached["binding"]["required_result_keys"], list(state.binding.required_result_keys))
            tree = ast.parse(Path(job.__file__).read_bytes())
            cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "M5JobViews")
            abort = next(node for node in cls.body if isinstance(node, ast.FunctionDef)
                and node.name == "abort_before_consumption")
            report = next(node.value for node in ast.walk(abort) if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == "report" for target in node.targets))
            self.assertIsInstance(report, ast.Call)
            self.assertEqual(ast.unparse(report.func), "_bounded_reopen_plain")
            keyword, = [item for item in report.keywords if item.arg == "_encoding"]
            self.assertIsInstance(keyword.value, ast.Constant)
            self.assertIs(keyword.value.value, True)
        finally:
            owner.close()
