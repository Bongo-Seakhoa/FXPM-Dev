"""W5.1 stage monitor, design C, candidate 006.  Sources only (E4974/E4975 rework scope): written for Root's QA
run against the applied governor, module and spawn helper; the author did not execute them.

Validation map (E4961, E4965, E4968):
  V2  test_env_unset_never_imports_and_behaviour_is_unchanged, test_other_workers_never_import,
      test_spawned_child_without_env_has_no_module_and_no_route
  V3  test_direct_envelope_identical_*, test_spawned_envelope_identical_including_traceback
  V4  test_counts_and_exclusive_time, test_unwind_*, test_recursion_*, test_resolution_refusals_*, test_static_and_bound_*
  V5  test_real_stage_map_entries_and_checkpoints_resolve, test_exact_worker_identity
  V6  test_output_file_*, test_cumulative_rows_*, test_cap_*, test_footer_refused_*, test_short_write_*,
      test_zero_write_*, test_oserror_write_*, test_reader_*
  C2  test_checkpoint_in_first_cell_gives_full_coverage, test_checkpoint_in_second_cell_is_late_coverage,
      test_work_before_the_checkpoint_is_not_counted, test_unavailable_checkpoint_refuses_full_coverage,
      test_cell_entries_count_cells_without_discovery, test_never_imported_and_late_loaded_are_disclosed
  C3  test_partial_arm_is_torn_down_and_recorded, test_partial_teardown_keeps_the_tool_id,
      test_callbacks_inert_after_stop, test_stop_sample_failure_still_tears_down
  C4  test_other_thread_events_are_ignored_and_counted, test_open_frames_make_timing_incomplete,
      test_invalid_interval_is_counted_not_clamped
  C5  test_reader_rejects_malformed_footers, test_reader_rejects_malformed_rows, test_empty_roster_refused
"""

import itertools
import json
import os
from pathlib import Path
import secrets
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock

import pm_memory_governor as governor
import pm_stage_monitor as monitor

try:  # the tests directory may or may not be a package under the QA runner
    from tests import stage_monitor_spawn_helpers as helpers
except ImportError:  # pragma: no cover - import layout dependent
    import stage_monitor_spawn_helpers as helpers


# -- toy stage functions, cell entries and checkpoints ---------------------------------------------------------------
def leaf(n):
    return n * 2


def outer(n):
    return leaf(n) + leaf(n)


def boom():
    raise ValueError("boom")


def wrapper_that_catches():
    try:
        boom()
    except ValueError:
        return "caught"


def recursive(n):
    return 1 if n == 0 else recursive(n - 1) + 1


def generator_function():
    yield 1


def native_checkpoint():
    return "checkpoint"


def native_cell(body, *, before=None):
    """A toy cell route: optional work before the checkpoint (uncovered by design), the checkpoint, then the work."""
    if before is not None:
        before()
    native_checkpoint()
    return body()


def m5_checkpoint():
    return "m5 checkpoint"


def m5_cell(body):
    m5_checkpoint()
    return body()


class Holder:
    @staticmethod
    def static_leaf():
        return "static"

    def method_leaf(self):
        return "method"


TOY_TARGETS = (("OUTER", outer), ("LEAF", leaf), ("BOOM", boom), ("RECURSIVE", recursive),
               ("STATIC", Holder.static_leaf), ("METHOD", Holder.method_leaf))
TOY_MAP = monitor.expected_map(TOY_TARGETS)
TOY_CELL_ENTRIES = (("NATIVE_CELL", native_cell),)
TOY_CHECKPOINTS = (("NATIVE", native_checkpoint),)
HELPER_TARGETS = (("OUTER", helpers.outer), ("LEAF", helpers.leaf), ("BOOM", helpers.spawned_boom))
HELPER_MAP = monitor.expected_map(HELPER_TARGETS)


class FakeClock:
    """Deterministic clock: every call advances by one second."""

    def __init__(self):
        self.ticks = itertools.count()

    def __call__(self):
        return float(next(self.ticks))


def write_late_module(root, body="def late_leaf():\n    return 'late'\n"):
    name = "fxpm_w51_late_" + secrets.token_hex(4)
    (root / f"{name}.py").write_text(body, encoding="utf-8")
    return name


class _Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="stage_monitor_")
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.monitors = []
        self._meta_path = tuple(sys.meta_path)
        self._expected_retained_tools = set()
        self._test_codes = set()
        self.addCleanup(self._stop_all)

    def _stop_all(self):
        for item in list(self.monitors):
            self._test_codes.update(item._installed_masks)
            item.stop()
        self.monitors.clear()
        retained = {tool_id for tool_id in monitor._TOOL_IDS
                    if sys.monitoring.get_tool(tool_id) == monitor.TOOL_NAME}
        helper_codes = {fn.__code__ for _, fn in HELPER_TARGETS}
        for tool_id in retained:
            sys.monitoring.set_events(tool_id, 0)
            for code in self._test_codes | helper_codes:
                sys.monitoring.set_local_events(tool_id, code, 0)
            for event in (sys.monitoring.events.PY_START, sys.monitoring.events.PY_RETURN, sys.monitoring.events.PY_UNWIND):
                sys.monitoring.register_callback(tool_id, event, None)
            sys.monitoring.free_tool_id(tool_id)
        self.assertEqual(retained, self._expected_retained_tools, "unexpected monitor registration at cleanup")
        self.assertEqual(tuple(sys.meta_path), self._meta_path)

    def make(self, **kw):
        kw.setdefault("targets", TOY_TARGETS)
        kw.setdefault("cell_entries", TOY_CELL_ENTRIES)
        kw.setdefault("checkpoints", TOY_CHECKPOINTS)
        kw.setdefault("row_interval_seconds", 0)
        item = monitor.StageMonitor("toy", str(self.root), **kw)
        self.monitors.append(item)
        return item

    def finish(self, item, duration=1.0):
        self._test_codes.update(item._installed_masks)
        status = item.stop(task_duration_seconds=duration)
        if item in self.monitors:
            self.monitors.remove(item)
        return status

    def read(self, item, expected=None):
        return monitor.read_monitor_file(item._writer.path, expected if expected is not None else TOY_MAP)

    def late_module(self):
        name = write_late_module(self.root)
        sys.path.insert(0, str(self.root))
        self.addCleanup(lambda: sys.path.remove(str(self.root)))
        self.addCleanup(lambda: sys.modules.pop(name, None))
        return name


class ResolutionTests(_Base):
    def test_resolution_refusals_are_listed_not_raised(self):
        resolved, unresolved = monitor.resolve_targets((
            ("A", leaf), ("B", generator_function), ("C", len), ("D", Holder),
            ("E", "no_such_module_fxpm", "nothing"), ("F", "os", "path.no_such_function"), ("G", "os", "path"),
            ("H", leaf), ("I", Holder.static_leaf), ("J", "json", "JSONDecoder.decode"),
        ), import_modules=True)
        self.assertEqual(sorted(stage for stage, _ in resolved.values()), ["A", "I", "J"])
        reasons = {row["stage"]: row["reason"] for row in unresolved}
        self.assertIn("GENERATOR_OR_COROUTINE_FUNCTION", reasons["B"])
        self.assertIn("NOT_A_PLAIN_FUNCTION", reasons["C"])
        self.assertIn("NOT_A_PLAIN_FUNCTION", reasons["D"])
        self.assertIn("ModuleNotFoundError", reasons["E"])
        self.assertIn("AttributeError", reasons["F"])
        self.assertIn("NOT_A_PLAIN_FUNCTION", reasons["G"])
        self.assertIn("DUPLICATE_CODE_OBJECT", reasons["H"])
        _, lazy = monitor.resolve_targets((("K", "no_such_module_fxpm_2", "f"),))
        self.assertIn("MODULE_NOT_IMPORTED", lazy[0]["reason"])
        self.assertNotIn("no_such_module_fxpm_2", sys.modules)

    def test_real_stage_map_entries_and_checkpoints_resolve(self):
        additions = {
            ("BAR_PREP", "pm_authoritative_search_execution", "reopen_bound_fitted_timeframe"),
            ("BAR_PREP", "pm_authoritative_search_execution", "_reopen_m5_bound_fitted_timeframe"),
            ("BAR_PREP", "pm_authoritative_search_execution", "fit_m5_derived_timeframe_for_search"),
            ("ACCOUNT_FINALIZATION", "pm_historical_scheduler", "_declared_m5_finalize"),
            ("ACCOUNT_FINALIZATION", "pm_authority_spool", "retire_completed_generation"),
        }
        self.assertTrue(additions <= set(monitor.STAGE_MAP))
        self.assertEqual(
            {entry for entry in monitor.STAGE_MAP if entry[0].startswith("REGIME")},
            {("REGIME", "pm_regime", "MarketRegimeDetector.compute_regime_scores"),
             ("REGIME", "pm_core", "FeatureComputer._append_regime_features")},
        )
        self.assertNotIn("produce_declared_m5_development_cell_account_artifact",
                         {name for _, _, name in monitor.STAGE_MAP})
        resolved, unresolved = monitor.resolve_targets(monitor.STAGE_MAP, import_modules=True)
        self.assertEqual(unresolved, [], unresolved)
        self.assertEqual(len(resolved), len(monitor.STAGE_MAP))
        self.assertEqual(sorted(set(stage for stage, _ in resolved.values())), sorted(monitor.STAGES))
        hooks, hook_unresolved = monitor.resolve_targets(monitor.CELL_ENTRIES + monitor.CHECKPOINTS, import_modules=True)
        self.assertEqual(hook_unresolved, [])
        self.assertEqual(len(hooks), 4)
        self.assertTrue(set(hooks).isdisjoint(set(resolved)))
        self.assertEqual(list(monitor.expected_map(monitor.STAGE_MAP).values())[0], "BAR_PREP")
        # V5 explicitly imports product dependencies before this cleanup comparison.
        self._meta_path = tuple(sys.meta_path)

    def test_empty_roster_refused(self):
        with self.assertRaisesRegex(monitor.StageMonitorError, "EMPTY_ROSTER"):
            monitor.StageMonitor("toy", str(self.root), targets=())

    def test_targets_present_at_start_are_resolved_at_start(self):
        meta_path_before = list(sys.meta_path)
        item = self.make()
        statuses = {info["status"] for info in item.status()["coverage"].values()}
        self.assertEqual(statuses, {monitor.RESOLVED_AT_START})
        self.assertEqual(item.pending_modules(), set())
        self.assertEqual({info["status"] for info in item.status()["hooks"].values()}, {"ARMED"})
        self.assertTrue(item.start())
        self.assertEqual(list(sys.meta_path), meta_path_before)   # no import hook: discovery happens only at checkpoints
        self.finish(item)

    def test_checkpoint_in_first_cell_gives_full_coverage(self):
        name = self.late_module()
        item = self.make(targets=(("LATE", name, "late_leaf"), ("LEAF", leaf)))
        expected = monitor.expected_map((("LATE", name, "late_leaf"), ("LEAF", leaf)))
        self.assertEqual(item.status()["coverage"][f"{name}.late_leaf"]["status"], monitor.PENDING)
        self.assertTrue(item.start())
        import importlib
        module = importlib.import_module(name)                     # the child's own import; the monitor imports nothing
        native_cell(lambda: (module.late_leaf(), module.late_leaf(), leaf(1)))
        native_cell(lambda: module.late_leaf())
        self.finish(item, 5.0)
        _, footer = self.read(item, expected)
        info = footer["coverage"][f"{name}.late_leaf"]
        self.assertEqual(info["status"], monitor.RESOLVED_AT_CHECKPOINT)
        self.assertEqual(info["cells_started_at_resolution"], 1)
        self.assertEqual(info["checkpoint"], "NATIVE")
        self.assertEqual(footer["stages"]["LATE"]["count"], 3)
        self.assertEqual((footer["cells_started"], footer["cells_completed"]), (2, 2))
        self.assertEqual(footer["hooks"][f"{__name__}.native_checkpoint"]["fired"], 2)
        summary = monitor.summarize(footer)
        self.assertTrue(summary["coverage"]["complete"], summary["coverage"]["failed_checks"])
        self.assertEqual(summary["fully_covered_from_cell_ordinal"], 1)

    def test_checkpoint_in_second_cell_is_late_coverage(self):
        name = self.late_module()
        targets = (("LATE", name, "late_leaf"), ("LEAF", leaf))
        item = self.make(targets=targets)
        self.assertTrue(item.start())
        native_cell(lambda: leaf(1))                                # cell 1: the module is not imported yet
        import importlib
        module = importlib.import_module(name)
        native_cell(lambda: module.late_leaf())                     # cell 2: discovered at its checkpoint
        self.finish(item, 5.0)
        _, footer = self.read(item, monitor.expected_map(targets))
        info = footer["coverage"][f"{name}.late_leaf"]
        self.assertEqual((info["status"], info["cells_started_at_resolution"]), (monitor.RESOLVED_AT_CHECKPOINT, 2))
        summary = monitor.summarize(footer)
        self.assertFalse(summary["coverage"]["complete"])
        self.assertEqual(summary["coverage"]["incomplete_reason"], "LATE_COVERAGE")
        self.assertEqual(summary["fully_covered_from_cell_ordinal"], 2)
        self.assertEqual(summary["late_coverage"], {f"{name}.late_leaf": 2})

    def test_work_before_the_checkpoint_is_not_counted(self):
        name = self.late_module()
        targets = (("LATE", name, "late_leaf"),)
        item = self.make(targets=targets)
        self.assertTrue(item.start())
        import importlib
        module = importlib.import_module(name)
        native_cell(lambda: module.late_leaf(), before=module.late_leaf)   # one call precedes the checkpoint
        self.finish(item, 5.0)
        _, footer = self.read(item, monitor.expected_map(targets))
        self.assertEqual(footer["stages"]["LATE"]["count"], 1)
        self.assertEqual(footer["coverage"][f"{name}.late_leaf"]["cells_started_at_resolution"], 1)
        summary = monitor.summarize(footer)
        self.assertTrue(summary["coverage"]["complete"], summary["coverage"]["failed_checks"])
        self.assertEqual(summary["fully_covered_from_cell_ordinal"], 1)

    def test_unavailable_checkpoint_refuses_full_coverage(self):
        name = self.late_module()
        targets = (("LATE", name, "late_leaf"), ("LEAF", leaf))
        item = self.make(targets=targets, checkpoints=(("NATIVE", "no_such_module_fxpm_3", "cp"),))
        self.assertEqual(item.status()["hooks"]["no_such_module_fxpm_3.cp"]["status"], monitor.UNRESOLVED)
        self.assertTrue(item.start())
        import importlib
        module = importlib.import_module(name)
        native_cell(lambda: module.late_leaf())
        self.finish(item, 5.0)
        _, footer = self.read(item, monitor.expected_map(targets))
        self.assertEqual(footer["coverage"][f"{name}.late_leaf"]["status"], monitor.LOADED_AFTER_LAST_CHECKPOINT)
        summary = monitor.summarize(footer)
        self.assertFalse(summary["coverage"]["complete"])
        self.assertIn("CHECKPOINT_UNAVAILABLE", summary["coverage"]["failed_checks"])
        self.assertIn("INCOMPLETE_COVERAGE", summary["coverage"]["failed_checks"])
        self.assertEqual(summary["checkpoints_unarmed"], ["no_such_module_fxpm_3.cp"])
        self.assertIsNone(summary["fully_covered_from_cell_ordinal"])

    def test_armed_checkpoint_without_a_hit_refuses_cell_coverage(self):
        item = self.make()
        self.assertTrue(item.start())
        with mock.patch.object(sys.modules[__name__], "native_checkpoint", return_value=None):
            native_cell(lambda: leaf(1))
        self.finish(item)
        _, footer = self.read(item)
        summary = monitor.summarize(footer)
        self.assertEqual(summary["coverage"]["incomplete_reason"], "CHECKPOINT_UNREACHED")
        self.assertEqual(summary["checkpoint_routes_missed"], ["NATIVE"])
        self.assertIsNone(summary["fully_covered_from_cell_ordinal"])

    def test_extra_checkpoint_cannot_cover_the_next_cell(self):
        item = self.make()
        self.assertTrue(item.start())
        self.assertEqual(native_cell(lambda: leaf(1), before=native_checkpoint), 2)
        with mock.patch.object(sys.modules[__name__], "native_checkpoint", return_value=None):
            self.assertEqual(native_cell(lambda: leaf(2)), 4)
        self.finish(item)
        _, footer = self.read(item)
        self.assertEqual((footer["cells_started"], footer["cells_completed"]), (2, 2))
        self.assertEqual(footer["hooks"][f"{__name__}.native_checkpoint"]["fired"], 1)
        summary = monitor.summarize(footer)
        self.assertEqual(summary["coverage"]["incomplete_reason"], "CHECKPOINT_UNREACHED")
        self.assertEqual(summary["checkpoint_routes_missed"], ["NATIVE"])

    def test_checkpoints_require_the_matching_cell_route(self):
        item = self.make(cell_entries=TOY_CELL_ENTRIES + (("M5_CELL", m5_cell),),
                         checkpoints=TOY_CHECKPOINTS + (("M5", m5_checkpoint),))
        self.assertTrue(item.start())
        self.assertEqual(native_checkpoint(), "checkpoint")
        self.assertEqual(m5_checkpoint(), "m5 checkpoint")
        self.assertEqual({hook["fired"] for hook in item.status()["hooks"].values()}, {0})
        with mock.patch.object(sys.modules[__name__], "native_checkpoint", m5_checkpoint):
            self.assertEqual(native_cell(lambda: leaf(1)), 2)
        self.assertEqual(m5_cell(lambda: leaf(2)), 4)
        self.finish(item)
        _, footer = self.read(item)
        self.assertEqual((footer["cells_started"], footer["cells_completed"]), (2, 2))
        self.assertEqual(footer["hooks"][f"{__name__}.native_checkpoint"]["fired"], 0)
        self.assertEqual(footer["hooks"][f"{__name__}.m5_checkpoint"]["fired"], 1)
        summary = monitor.summarize(footer)
        self.assertEqual(summary["checkpoint_routes_missed"], ["NATIVE"])
        self.assertIsNone(summary["fully_covered_from_cell_ordinal"])

    def test_cell_entries_count_cells_without_discovery(self):
        name = self.late_module()
        targets = (("LATE", name, "late_leaf"), ("LEAF", leaf))
        item = self.make(targets=targets, checkpoints=())
        self.assertTrue(item.start())
        import importlib
        module = importlib.import_module(name)
        native_cell(lambda: module.late_leaf())
        native_cell(lambda: module.late_leaf())
        self.finish(item, 5.0)
        _, footer = self.read(item, monitor.expected_map(targets))
        self.assertEqual((footer["cells_started"], footer["cells_completed"]), (2, 2))
        self.assertEqual(footer["coverage"][f"{name}.late_leaf"]["status"], monitor.LOADED_AFTER_LAST_CHECKPOINT)
        self.assertEqual(footer["stages"]["LATE"]["count"], 0)

    def test_never_imported_and_late_loaded_are_disclosed(self):
        never = "fxpm_w51_never_" + secrets.token_hex(4)
        late = "fxpm_w51_lateload_" + secrets.token_hex(4)
        targets = (("NEVER", never, "f"), ("LATE", late, "g"), ("LEAF", leaf))
        item = self.make(targets=targets)
        self.assertTrue(item.start())
        leaf(2)
        module = types.ModuleType(late)
        module.g = lambda: None
        sys.modules[late] = module
        self.addCleanup(lambda: sys.modules.pop(late, None))
        self.finish(item, 3.0)
        _, footer = self.read(item, monitor.expected_map(targets))
        self.assertEqual(footer["coverage"][f"{never}.f"]["status"], monitor.MODULE_NEVER_IMPORTED)
        self.assertEqual(footer["coverage"][f"{late}.g"]["status"], monitor.LOADED_AFTER_LAST_CHECKPOINT)
        summary = monitor.summarize(footer)
        self.assertFalse(summary["coverage"]["complete"])
        self.assertEqual(summary["coverage"]["incomplete_reason"], "INCOMPLETE_COVERAGE")
        self.assertEqual(summary["not_covered"], sorted([f"{never}.f", f"{late}.g"]))
        self.assertIsNone(summary["fully_covered_from_cell_ordinal"])


class AccountingTests(_Base):
    def test_counts_and_exclusive_time(self):
        item = self.make(clock=FakeClock())
        self.assertTrue(item.start())
        self.assertEqual(outer(3), 12)
        status = self.finish(item, 100.0)
        self.assertTrue(status["footer_written"])
        rows, footer = self.read(item)
        self.assertEqual(rows, [])
        stages = footer["stages"]
        # ticks: start clock, outer start, leaf start, leaf return, leaf start, leaf return, outer return, stop clock
        self.assertEqual(stages["LEAF"]["count"], 2)
        self.assertEqual(stages["LEAF"]["inclusive_seconds"], 2.0)
        self.assertEqual(stages["LEAF"]["exclusive_seconds"], 2.0)
        self.assertEqual(stages["OUTER"]["count"], 1)
        self.assertEqual(stages["OUTER"]["inclusive_seconds"], 5.0)
        self.assertEqual(stages["OUTER"]["exclusive_seconds"], 3.0)
        timing = footer["timing"]
        self.assertEqual(timing["attributed_exclusive_seconds"], 5.0)
        self.assertEqual(timing["observation_interval_seconds"], 7.0)
        self.assertEqual(timing[monitor.UNATTRIBUTED], 2.0)
        self.assertEqual(timing["task_duration_seconds"], 100.0)
        self.assertTrue(timing["timing_complete"])
        self.assertEqual(stages["BOOM"]["status"], "UNOBSERVED")
        self.assertEqual(footer["functions"][f"{__name__}.leaf"]["count"], 2)
        self.assertEqual(footer["functions"][f"{__name__}.boom"]["count"], 0)
        summary = monitor.summarize(footer)
        self.assertTrue(summary["coverage"]["complete"], summary["coverage"]["failed_checks"])
        self.assertIsNone(summary["fully_covered_from_cell_ordinal"])
        nesting = {"OUTER": {"LEAF": {"count": 2, "inclusive_seconds": 2.0}}}
        self.assertEqual(footer["stage_nesting"], nesting)
        self.assertEqual(summary["stage_nesting"], nesting)

    def test_unwind_by_exception_is_counted_once_and_leaves_an_empty_stack(self):
        item = self.make(clock=FakeClock())
        self.assertTrue(item.start())
        self.assertEqual(wrapper_that_catches(), "caught")
        with self.assertRaisesRegex(ValueError, "boom"):
            boom()
        self.finish(item, 10.0)
        _, footer = self.read(item)
        self.assertEqual(footer["stages"]["BOOM"]["count"], 2)
        self.assertEqual(footer["stages"]["BOOM"]["inclusive_seconds"], 2.0)
        self.assertEqual(footer["faults"]["stack_mismatches"], 0)
        self.assertEqual(footer["timing"]["open_frames_at_stop"], 0)

    def test_recursion_counts_every_frame_and_keeps_exclusive_additive(self):
        item = self.make(clock=FakeClock())
        self.assertTrue(item.start())
        self.assertEqual(recursive(2), 3)
        self.finish(item, 50.0)
        _, footer = self.read(item)
        row = footer["stages"]["RECURSIVE"]
        self.assertEqual(row["count"], 3)
        self.assertEqual(row["inclusive_seconds"], 9.0)
        self.assertEqual(row["exclusive_seconds"], 5.0)
        self.assertEqual(footer["timing"]["attributed_exclusive_seconds"], 5.0)
        self.assertEqual(footer["stage_nesting"],
                         {"RECURSIVE": {"RECURSIVE": {"count": 2, "inclusive_seconds": 4.0}}})

    def test_regime_stage_exposes_fit_and_feature_paths(self):
        """Invented stage calls with the nesting of fit and label preparation."""
        def scores():
            return 1

        def labels():
            return scores() + 1

        def bar_prep():
            return scores()

        def features():
            return labels()

        targets = (("BAR_PREP", bar_prep), ("FEATURES", features),
                   ("REGIME", scores), ("REGIME", labels))
        item = self.make(targets=targets, clock=FakeClock())
        self.assertTrue(item.start())
        self.assertEqual(bar_prep(), 1)
        self.assertEqual(features(), 2)
        self.finish(item)
        _, footer = self.read(item, monitor.expected_map(targets))
        self.assertEqual(footer["stages"]["REGIME"]["count"], 3)
        self.assertEqual(footer["stages"]["REGIME"]["inclusive_seconds"], 5.0)
        self.assertEqual(footer["stages"]["REGIME"]["exclusive_seconds"], 4.0)
        nesting = {
            "BAR_PREP": {"REGIME": {"count": 1, "inclusive_seconds": 1.0}},
            "FEATURES": {"REGIME": {"count": 1, "inclusive_seconds": 3.0}},
            "REGIME": {"REGIME": {"count": 1, "inclusive_seconds": 1.0}},
        }
        self.assertEqual(footer["stage_nesting"], nesting)
        self.assertEqual(monitor.summarize(footer)["stage_nesting"], nesting)

    def test_invalid_parent_does_not_publish_nesting_edges(self):
        ticks = iter((0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 0.5, 6.0))
        item = self.make(clock=FakeClock())
        item._clock = lambda: next(ticks)
        self.assertTrue(item.start())
        self.assertEqual(outer(2), 8)
        self.finish(item)
        _, footer = self.read(item)
        self.assertEqual(footer["faults"]["invalid_intervals"], 1)
        self.assertEqual(footer["stages"]["OUTER"]["count"], 0)
        self.assertEqual(footer["stages"]["LEAF"]["count"], 2)
        self.assertEqual(footer["stage_nesting"], {})

    def test_static_and_bound_methods_are_monitored(self):
        item = self.make()
        self.assertTrue(item.start())
        Holder.static_leaf()
        Holder().method_leaf()
        self.finish(item)
        _, footer = self.read(item)
        self.assertEqual(footer["stages"]["STATIC"]["count"], 1)
        self.assertEqual(footer["stages"]["METHOD"]["count"], 1)

    def test_other_thread_events_are_ignored_and_counted(self):
        item = self.make()
        self.assertTrue(item.start())
        thread = threading.Thread(target=outer, args=(2,))
        thread.start()
        thread.join()
        outer(1)
        self.finish(item)
        _, footer = self.read(item)
        self.assertEqual(footer["stages"]["OUTER"]["count"], 1)
        self.assertEqual(footer["stages"]["LEAF"]["count"], 2)
        self.assertGreaterEqual(footer["faults"]["other_thread_events"], 6)
        self.assertEqual(footer["timing"]["scope"], "WORKER_THREAD_ONLY")
        self.assertFalse(monitor.summarize(footer)["coverage"]["complete"])

    def test_open_frames_make_timing_incomplete(self):
        item = self.make()
        self.assertTrue(item.start())
        item._stack.append([outer.__code__, 0.0, 0.0])
        self.finish(item)
        _, footer = self.read(item)
        self.assertEqual(footer["timing"]["open_frames_at_stop"], 1)
        self.assertFalse(footer["timing"]["timing_complete"])
        self.assertEqual(footer["timing"]["timing_incomplete_reason"], "OPEN_FRAMES_AT_STOP")
        self.assertIsNone(footer["timing"][monitor.UNATTRIBUTED])
        self.assertIn("TIMING_OPEN_FRAMES_AT_STOP", monitor.summarize(footer)["coverage"]["failed_checks"])

    def test_invalid_interval_is_counted_not_clamped(self):
        # the constructor measures clock overhead with its own samples, so the scripted clock is installed after it
        ticks = iter([0.0, 10.0, 5.0, 10.0, 11.0, 20.0])          # start, leaf start, leaf return (backwards), start, return, stop

        def scripted_clock():
            return next(ticks, 99.0)

        item = self.make(targets=(("LEAF", leaf), ("OUTER", outer)), clock=FakeClock())
        item._clock = scripted_clock
        self.assertTrue(item.start())                               # samples 0.0
        leaf(1)                                                     # start 10.0, return 5.0: invalid, discarded
        leaf(1)                                                     # start 10.0, return 11.0: valid 1.0
        self.finish(item, 50.0)                                     # stop samples 20.0
        _, footer = self.read(item, monitor.expected_map((("LEAF", leaf), ("OUTER", outer))))
        self.assertEqual(footer["faults"]["invalid_intervals"], 1)
        self.assertEqual(footer["stages"]["LEAF"]["count"], 1)
        self.assertEqual(footer["stages"]["LEAF"]["inclusive_seconds"], 1.0)
        self.assertFalse(footer["timing"]["timing_complete"])
        self.assertEqual(footer["timing"]["timing_incomplete_reason"], "INVALID_INTERVALS")
        self.assertIsNone(footer["timing"][monitor.UNATTRIBUTED])


class FaultTests(_Base):
    def test_callback_fault_is_counted_and_the_monitored_code_is_unaffected(self):
        item = self.make()
        self.assertTrue(item.start())
        with mock.patch.object(item, "_mine", side_effect=RuntimeError("timer broke")):
            self.assertEqual(outer(1), 4)
        status = self.finish(item)
        self.assertGreaterEqual(status["faults"]["callback_faults"], 3)
        _, footer = self.read(item)
        self.assertEqual(footer["faults"], status["faults"])
        self.assertFalse(footer["timing"]["timing_complete"])
        self.assertEqual(footer["timing"]["timing_incomplete_reason"], "CALLBACK_FAULTS")
        self.assertIsNone(footer["timing"][monitor.UNATTRIBUTED])

    def test_callback_interrupt_escapes_and_cleanup_releases_tool(self):
        item = self.make(clock=FakeClock())
        self.assertTrue(item.start())
        tool_id = item.tool_id
        interrupt = KeyboardInterrupt("callback interrupt")
        try:
            with mock.patch.object(item, "_clock", side_effect=interrupt):
                with self.assertRaises(KeyboardInterrupt) as caught:
                    leaf(1)
            self.assertIs(caught.exception, interrupt)
        finally:
            status = self.finish(item)
        self.assertTrue(status["tool_id_released"])
        self.assertIsNone(sys.monitoring.get_tool(tool_id))
        _, footer = self.read(item)
        self.assertTrue(footer["teardown"]["complete"])
        self.assertFalse(footer["timing"]["timing_complete"])
        self.assertEqual(footer["timing"]["timing_incomplete_reason"], "INTERRUPTED")

    def test_stop_clock_interrupt_escapes_after_cleanup(self):
        item = self.make(clock=FakeClock())
        self.assertTrue(item.start())
        tool_id = item.tool_id
        self.assertEqual(leaf(1), 2)
        interrupt = KeyboardInterrupt("stop clock interrupt")
        with mock.patch.object(item, "_clock", side_effect=interrupt):
            with self.assertRaises(KeyboardInterrupt) as caught:
                self.finish(item)
        self.assertIs(caught.exception, interrupt)
        self.assertIsNone(sys.monitoring.get_tool(tool_id))
        self.assertIsNone(item._writer._fd)
        _, footer = self.read(item)
        self.assertTrue(footer["teardown"]["complete"])
        self.assertTrue(footer["teardown"]["tool_id_released"])
        self.assertEqual(footer["timing"]["timing_incomplete_reason"], "INTERRUPTED")
        self.assertIsNone(footer["timing"][monitor.UNATTRIBUTED])

    def test_clock_fault_is_counted_and_the_monitored_code_is_unaffected(self):
        state = {"n": 0, "fail": False}

        def flaky_clock():
            state["n"] += 1
            if state["fail"]:
                raise OSError("no clock")
            return float(state["n"])

        item = self.make(clock=flaky_clock)
        self.assertTrue(item.start())
        state["fail"] = True
        self.assertEqual(outer(1), 4)
        status = self.finish(item)
        self.assertGreaterEqual(status["faults"]["callback_faults"], 1)
        self.assertTrue(status["footer_written"])
        _, footer = self.read(item)
        self.assertFalse(footer["timing"]["timing_complete"])

    def test_tool_ids_in_use_leave_monitoring_inactive_without_raising(self):
        taken = []
        try:
            for tool_id in monitor._TOOL_IDS:
                if sys.monitoring.get_tool(tool_id) is None:
                    sys.monitoring.use_tool_id(tool_id, "test-occupant")
                    taken.append(tool_id)
            item = self.make()
            self.assertFalse(item.start())
            self.assertIsNone(item.tool_id)
            outer(1)
            status = self.finish(item, 2.0)
            self.assertEqual(status["faults"]["tool_id_unavailable"], 1)
            _, footer = self.read(item)
            self.assertFalse(footer["monitoring_active_at_stop"])
            self.assertEqual(footer["stages"]["OUTER"]["status"], "UNOBSERVED")
            summary = monitor.summarize(footer)
            self.assertFalse(summary["coverage"]["complete"])
            self.assertEqual(summary["coverage"]["incomplete_reason"], "FAULTS")
        finally:
            for tool_id in taken:
                sys.monitoring.free_tool_id(tool_id)

    def test_partial_arm_is_torn_down_and_recorded(self):
        item = self.make()
        with mock.patch.object(sys.monitoring, "set_events", side_effect=ValueError("refused")):
            self.assertFalse(item.start())
        self.assertIsNone(item.tool_id)
        self.assertTrue(item.tool_id_released)
        self.assertEqual(item.faults["start_faults"], 1)
        self.assertEqual(item.teardown_failed_steps, [])
        for tool_id in monitor._TOOL_IDS:
            self.assertNotEqual(sys.monitoring.get_tool(tool_id), monitor.TOOL_NAME)
        self.finish(item)

    def test_partial_teardown_keeps_the_tool_id(self):
        item = self.make()
        self.assertTrue(item.start())
        tool_id = item.tool_id
        real_set_local = sys.monitoring.set_local_events

        def failing_clear(tid, code, events):
            if events == 0 and code is leaf.__code__:
                raise ValueError("clear refused")
            return real_set_local(tid, code, events)

        self._expected_retained_tools.add(tool_id)
        with mock.patch.object(sys.monitoring, "set_local_events", side_effect=failing_clear):
            status = self.finish(item)
        self.assertEqual(status["tool_id_released"], False)
        self.assertEqual(status["tool_id"], tool_id)
        self.assertEqual(len(status["teardown_failed_steps"]), 1)
        self.assertIn("local_mask:", status["teardown_failed_steps"][0])
        self.assertEqual(sys.monitoring.get_tool(tool_id), monitor.TOOL_NAME)
        _, footer = self.read(item)
        self.assertFalse(footer["teardown"]["complete"])
        self.assertEqual(monitor.summarize(footer)["coverage"]["incomplete_reason"], "FAULTS")

    def test_teardown_leaves_a_foreign_tool_id_alone(self):
        item = self.make()
        self.assertTrue(item.start())
        self._expected_retained_tools.add(item.tool_id)  # The mocked lookup does not reassign the id.
        with mock.patch.object(sys.monitoring, "get_tool", return_value="foreign"), \
                mock.patch.object(sys.monitoring, "set_events") as global_clear, \
                mock.patch.object(sys.monitoring, "set_local_events") as local_clear, \
                mock.patch.object(sys.monitoring, "register_callback") as callback_clear, \
                mock.patch.object(sys.monitoring, "free_tool_id") as release:
            status = self.finish(item)
        for operation in (global_clear, local_clear, callback_clear, release):
            operation.assert_not_called()
        self.assertFalse(status["tool_id_released"])
        self.assertEqual(status["teardown_failed_steps"], ["tool_id_not_owned"])

    def test_callbacks_inert_after_stop(self):
        item = self.make()
        self.assertTrue(item.start())
        self.finish(item)
        counted_before = dict(item.faults)
        outer(1)
        item._on_start(leaf.__code__, 0)
        item._on_return(leaf.__code__, 0, None)
        self.assertEqual(item.faults, counted_before)
        self.assertEqual(item._stack, [])

    def _call_with_stop_sample_fault(self, problem):
        refs = governor._STAGE_MONITOR_WORKER_REFS + ((__name__, "outer"),)
        real_monotonic = governor.time.monotonic

        def failing_inside_stop(*args):
            # only the stop helper's own sample fails; the governor's start and envelope samples keep working
            frame = sys._getframe(1)
            while frame is not None:
                if frame.f_code.co_name == "_stop_stage_monitor":
                    raise problem
                frame = frame.f_back
            return real_monotonic()

        with mock.patch.object(governor, "_STAGE_MONITOR_WORKER_REFS", refs), \
                mock.patch.dict(os.environ, {monitor.ENV_DIRECTORY: str(self.root)}, clear=False), \
                mock.patch.object(monitor, "STAGE_MAP", TOY_TARGETS), \
                mock.patch.object(monitor, "CELL_ENTRIES", TOY_CELL_ENTRIES), \
                mock.patch.object(monitor, "CHECKPOINTS", TOY_CHECKPOINTS), \
                mock.patch.object(governor.time, "monotonic", side_effect=failing_inside_stop):
            return governor._measured_worker_call(outer, governor.MemoryTask(key="job", args=(2,)))

    def _assert_stop_sample_cleanup(self):
        files = sorted(self.root.glob("stage_monitor.job.*.jsonl"))
        self.assertEqual(len(files), 1)
        _, footer = monitor.read_monitor_file(str(files[0]), TOY_MAP)
        self.assertIsNone(footer["timing"]["task_duration_seconds"])
        self.assertTrue(footer["teardown"]["complete"])
        self.assertTrue(footer["teardown"]["tool_id_released"])
        for tool_id in monitor._TOOL_IDS:
            self.assertNotEqual(sys.monitoring.get_tool(tool_id), monitor.TOOL_NAME)


    def test_stop_sample_failure_still_tears_down(self):
        envelope = self._call_with_stop_sample_fault(OSError("clock gone"))
        self.assertEqual(envelope.value, 8)
        self._assert_stop_sample_cleanup()

    def test_governor_stop_sample_interrupt_still_tears_down(self):
        interrupt = KeyboardInterrupt("governor stop sample")
        with self.assertRaises(KeyboardInterrupt) as caught:
            self._call_with_stop_sample_fault(interrupt)
        self.assertIs(caught.exception, interrupt)
        self._assert_stop_sample_cleanup()


class OutputTests(_Base):
    def test_output_file_is_fresh_exclusive_and_bounded(self):
        first = self.make()
        second = self.make()
        self.assertNotEqual(first._writer.path, second._writer.path)
        self.assertTrue(first._writer.file_name.startswith(f"stage_monitor.toy.{os.getpid()}."))
        with mock.patch.object(monitor.secrets, "token_hex", return_value=first._writer.file_name.split(".")[3]):
            with self.assertRaisesRegex(monitor.StageMonitorError, "PATH_EXISTS"):
                self.make()
        with self.assertRaisesRegex(monitor.StageMonitorError, "DIRECTORY_MISSING"):
            monitor.StageMonitor("toy", str(self.root / "missing"), targets=TOY_TARGETS)
        with self.assertRaisesRegex(monitor.StageMonitorError, "LABEL_INVALID"):
            monitor.StageMonitor("bad label", str(self.root), targets=TOY_TARGETS)
        with self.assertRaisesRegex(monitor.StageMonitorError, "MAX_FILE_BYTES_INVALID"):
            monitor.StageMonitor("toy", str(self.root), targets=TOY_TARGETS, max_file_bytes=10)
        with self.assertRaisesRegex(monitor.StageMonitorError, "ROW_INTERVAL_INVALID"):
            monitor.StageMonitor("toy", str(self.root), targets=TOY_TARGETS, row_interval_seconds=-1)

    def test_constructor_failure_after_open_releases_the_handle(self):
        with mock.patch.object(monitor, "_measure_clock_sampling_overhead", side_effect=RuntimeError("clock")):
            with mock.patch.object(monitor.os, "close", wraps=os.close) as close:
                with self.assertRaises(RuntimeError):
                    monitor.StageMonitor("toy", str(self.root), targets=TOY_TARGETS)
        close.assert_called_once()

    def test_cumulative_rows_are_written_between_top_level_calls_and_the_footer_closes(self):
        item = self.make(clock=FakeClock(), row_interval_seconds=1)
        self.assertTrue(item.start())
        outer(1)
        outer(1)
        self.finish(item, 30.0)
        rows, footer = self.read(item)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["kind"], "cumulative")
        self.assertEqual(rows[0]["stages"]["OUTER"]["count"], 1)
        self.assertEqual(rows[1]["stages"]["OUTER"]["count"], 2)
        self.assertEqual(footer["writer_counters"]["rows"], 2)
        self.assertEqual(footer["writer_counters"]["bytes_before_footer"], item._writer.bytes_written)

    def test_cap_stops_rows_and_keeps_the_footer(self):
        item = self.make(clock=FakeClock(), row_interval_seconds=1, max_file_bytes=monitor.MIN_MAX_FILE_BYTES)
        self.assertTrue(item.start())
        for _ in range(400):
            outer(1)
        status = self.finish(item, 1000.0)
        self.assertTrue(status["writer_counters"]["full"])
        self.assertGreater(status["writer_counters"]["dropped_full"], 0)
        self.assertTrue(status["footer_written"])
        self.assertLessEqual(os.path.getsize(item._writer.path), monitor.MIN_MAX_FILE_BYTES)
        rows, footer = self.read(item)
        self.assertEqual(len(rows), footer["writer_counters"]["rows"])
        self.assertFalse(monitor.summarize(footer)["coverage"]["complete"])

    def test_footer_refused_is_an_honest_incomplete_outcome(self):
        item = self.make()
        self.assertTrue(item.start())
        outer(1)
        item._writer.max_file_bytes = 16
        status = self.finish(item)
        self.assertFalse(status["footer_written"])
        self.assertTrue(status["footer_refused"])
        rows, footer = self.read(item)
        self.assertEqual((rows, footer), ([], None))
        self.assertEqual(monitor.summarize(footer)["coverage"]["incomplete_reason"], "NO_FOOTER")

    def test_short_write_corrupts_the_tail_and_suppresses_the_footer(self):
        item = self.make(clock=FakeClock(), row_interval_seconds=1)
        self.assertTrue(item.start())
        real_write = os.write
        with mock.patch.object(monitor.os, "write", side_effect=lambda fd, data: real_write(fd, data[:7])):
            outer(1)
        status = self.finish(item)
        self.assertTrue(status["writer_counters"]["corrupt_tail"])
        self.assertEqual(status["writer_counters"]["short_writes"], 1)
        self.assertFalse(status["footer_written"])
        self.assertEqual(os.path.getsize(item._writer.path), 7)
        with self.assertRaisesRegex(monitor.StageMonitorError, "TRUNCATED_TAIL"):
            self.read(item)

    def test_zero_write_corrupts_the_tail(self):
        item = self.make(clock=FakeClock(), row_interval_seconds=1)
        self.assertTrue(item.start())
        with mock.patch.object(monitor.os, "write", return_value=0):
            outer(1)
        status = self.finish(item)
        self.assertTrue(status["writer_counters"]["corrupt_tail"])
        self.assertEqual((status["writer_counters"]["short_writes"], status["writer_counters"]["bytes_before_footer"]), (1, 0))
        self.assertFalse(status["footer_written"])
        self.assertEqual(os.path.getsize(item._writer.path), 0)
        self.assertEqual(self.read(item), ([], None))

    def test_oserror_write_corrupts_the_tail(self):
        item = self.make(clock=FakeClock(), row_interval_seconds=1)
        self.assertTrue(item.start())
        with mock.patch.object(monitor.os, "write", side_effect=OSError("disk")):
            outer(1)
        status = self.finish(item)
        self.assertTrue(status["writer_counters"]["corrupt_tail"])
        self.assertEqual(status["writer_counters"]["write_errors"], 1)
        self.assertFalse(status["footer_written"])
        self.assertEqual(os.path.getsize(item._writer.path), 0)


class ReaderTests(_Base):
    EXPECTED = {"m.g": "SLICES", "m.f": "KERNEL"}

    def good_footer(self, row_bytes):
        return {"schema_version": monitor.SCHEMA_VERSION, "kind": "footer", "writer": "w", "pid": 1,
                "roster": list(self.EXPECTED), "roster_stages": dict(self.EXPECTED), "targets_total": 2, "stages_total": 2,
                "cells_started": 1, "cells_completed": 1, "monitoring_active_at_stop": True,
                "clock_sampling_overhead_seconds": 0.0000001,
                "timing": {"observation_interval_seconds": 10.0, "attributed_exclusive_seconds": 4.0, monitor.UNATTRIBUTED: 6.0,
                           "task_duration_seconds": 11.0, "open_frames_at_stop": 0, "timing_complete": True,
                           "timing_incomplete_reason": None, "scope": "WORKER_THREAD_ONLY"},
                "stages": {"KERNEL": {"count": 2, "inclusive_seconds": 4.0, "exclusive_seconds": 4.0, "status": "OBSERVED"},
                           "SLICES": {"count": 0, "inclusive_seconds": 0.0, "exclusive_seconds": 0.0, "status": "UNOBSERVED"}},
                "stage_nesting": {},
                "functions": {"m.f": {"stage": "KERNEL", "count": 2, "inclusive_seconds": 4.0, "exclusive_seconds": 4.0},
                              "m.g": {"stage": "SLICES", "count": 0, "inclusive_seconds": 0.0, "exclusive_seconds": 0.0}},
                "coverage": {"m.f": {"stage": "KERNEL", "status": monitor.RESOLVED_AT_START, "reason": None,
                                     "cells_started_at_resolution": 0, "checkpoint": None},
                             "m.g": {"stage": "SLICES", "status": monitor.RESOLVED_AT_CHECKPOINT, "reason": None,
                                     "cells_started_at_resolution": 1, "checkpoint": "NATIVE"}},
                "hooks": {"s.cell": {"kind": "CELL_ENTRY", "role": "NATIVE_CELL", "status": "ARMED", "reason": None, "fired": 1},
                          "s.cp": {"kind": "CHECKPOINT", "role": "NATIVE", "status": "ARMED", "reason": None, "fired": 1}},
                "faults": {key: 0 for key in monitor._FAULT_KEYS},
                "teardown": {"complete": True, "failed_steps": [], "tool_id_released": True},
                "writer_counters": {"rows": 1, "bytes_before_footer": row_bytes, "dropped_full": 0, "write_errors": 0,
                                    "short_writes": 0, "corrupt_tail": False, "full": False,
                                    "max_file_bytes": monitor.DEFAULT_MAX_FILE_BYTES}}

    def good_row(self):
        return monitor._encode({"schema_version": monitor.SCHEMA_VERSION, "kind": "cumulative", "writer": "w", "pid": 1,
                                "at_utc": "2026-01-01T00:00:00+00:00", "monitor_elapsed_seconds": 1.0, "cells_started": 1,
                                "stages": {"KERNEL": {"count": 1, "inclusive_seconds": 1.0, "exclusive_seconds": 1.0, "status": "OBSERVED"},
                                           "SLICES": {"count": 0, "inclusive_seconds": 0.0, "exclusive_seconds": 0.0, "status": "UNOBSERVED"}}})

    def write(self, parts):
        path = self.root / f"r_{secrets.token_hex(3)}.jsonl"
        path.write_bytes(b"".join(parts))
        return str(path)

    def test_reader_accepts_a_consistent_file_and_summarizes(self):
        row = self.good_row()
        path = self.write([row, monitor._encode(self.good_footer(len(row)))])
        rows, footer = monitor.read_monitor_file(path, self.EXPECTED)
        self.assertEqual(len(rows), 1)
        summary = monitor.summarize(footer)
        by = {r["stage"]: r for r in summary["stages"]}
        self.assertEqual(by["KERNEL"]["share_of_attributed"], 1.0)
        self.assertEqual(by["SLICES"]["status"], "UNOBSERVED")
        self.assertIsNone(by["SLICES"]["inclusive_seconds"])
        self.assertIsNone(by["SLICES"]["exclusive_seconds"])
        self.assertIsNone(by["SLICES"]["share_of_attributed"])
        self.assertEqual([r["stage"] for r in summary["stages"]], list(self.EXPECTED.values()))
        self.assertEqual(summary["stage_nesting"], {})
        self.assertEqual(summary[monitor.UNATTRIBUTED], 6.0)
        self.assertTrue(summary["coverage"]["complete"], summary["coverage"]["failed_checks"])
        self.assertEqual(summary["fully_covered_from_cell_ordinal"], 1)
        self.assertEqual(json.loads(json.dumps(summary)), summary)
        self.assertEqual(monitor.summarize(None)["coverage"]["incomplete_reason"], "NO_FOOTER")
        with self.assertRaisesRegex(monitor.StageMonitorError, "STAGE_MONITOR_ROW_INVALID:1"):
            monitor.read_monitor_file(path)   # the production map rejects the toy row first
        with self.assertRaisesRegex(monitor.StageMonitorError, "EXPECTED_MAP_INVALID"):
            monitor.read_monitor_file(path, {})

    def test_scripted_clock_rounding_reads_back_writer_footer(self):
        ticks = iter((0.0, 0.0, 3.0000006, 10.0000004))
        item = self.make(clock=FakeClock())
        item._clock = lambda: next(ticks)
        self.assertTrue(item.start())
        self.assertEqual(leaf(1), 2)
        self.assertTrue(self.finish(item)["footer_written"])
        self.assertIsNone(next(ticks, None))
        _, footer = self.read(item)
        timing = footer["timing"]
        self.assertTrue(timing["timing_complete"])
        self.assertEqual(timing["observation_interval_seconds"], 10.0)
        self.assertEqual(timing["attributed_exclusive_seconds"], 3.000001)
        self.assertAlmostEqual(timing[monitor.UNATTRIBUTED], 10.0 - 3.000001, delta=2e-6)

    def test_production_hook_binding_and_zero_cells(self):
        item = self.make(targets=monitor.STAGE_MAP, cell_entries=monitor.CELL_ENTRIES,
                         checkpoints=monitor.CHECKPOINTS)
        self.assertTrue(item.start())
        self.finish(item)
        rows, footer = monitor.read_monitor_file(item._writer.path)
        self.assertEqual(rows, [])
        summary = monitor.summarize(footer)
        self.assertFalse(summary["coverage"]["complete"])
        self.assertIn("NO_CELLS", summary["coverage"]["failed_checks"])
        self.assertIsNone(summary["fully_covered_from_cell_ordinal"])

        name = next(name for name, hook in footer["hooks"].items() if hook["kind"] == "CHECKPOINT")
        renamed = dict(footer["hooks"])
        renamed["synthetic.renamed_checkpoint"] = renamed.pop(name)
        wrong_role = dict(footer["hooks"])
        wrong_role[name] = dict(wrong_role[name], role="OTHER")
        for case, hooks in (("renamed_checkpoint", renamed), ("wrong_checkpoint_role", wrong_role)):
            with self.subTest(case=case):
                path = self.write([monitor._encode(dict(footer, hooks=hooks))])
                with self.assertRaisesRegex(monitor.StageMonitorError, "HOOKS_INVALID"):
                    monitor.read_monitor_file(path)

    def test_reader_rejects_malformed_footers(self):
        row = self.good_row()
        base = self.good_footer(len(row))

        def with_coverage(name, **changes):
            coverage = {k: dict(v) for k, v in base["coverage"].items()}
            coverage[name].update(changes)
            return dict(base, coverage=coverage)

        cases = (
            ("ROSTER_MISMATCH", dict(base, roster=list(reversed(self.EXPECTED)))),
            ("ROSTER_MISMATCH", dict(base, roster_stages={"m.f": "KERNEL", "m.g": "KERNEL"})),
            ("ROSTER_MISMATCH", dict(base, targets_total=3)),
            ("COVERAGE_MISMATCH", dict(base, coverage={"m.f": base["coverage"]["m.f"]})),
            ("COVERAGE_INVALID", with_coverage("m.g", status="NEW")),
            ("COVERAGE_INVALID", with_coverage("m.g", stage="KERNEL")),
            ("COVERAGE_INVALID", with_coverage("m.f", cells_started_at_resolution=1)),
            ("COVERAGE_INVALID", with_coverage("m.g", cells_started_at_resolution=None)),
            ("COVERAGE_INVALID", with_coverage("m.g", checkpoint=None)),
            ("COVERAGE_INVALID", with_coverage("m.g", checkpoint="OTHER")),
            ("COVERAGE_INVALID", with_coverage("m.g", cells_started_at_resolution=2)),
            ("COVERAGE_INVALID", with_coverage("m.g", status=monitor.UNRESOLVED, cells_started_at_resolution=None, checkpoint=None, reason=None)),
            ("FUNCTIONS_MISMATCH", dict(base, functions={"m.f": base["functions"]["m.f"]})),
            ("FUNCTIONS_MISMATCH", dict(base, functions=dict(base["functions"], **{"x.y": base["functions"]["m.f"]}))),
            ("STAGES_INVALID", dict(base, stages={})),
            ("STAGES_INVALID", dict(base, stages={"OTHER": base["stages"]["SLICES"], "KERNEL": base["stages"]["KERNEL"]})),
            ("METRIC_INVALID", dict(base, functions=dict(base["functions"], **{"m.f": dict(base["functions"]["m.f"], count=-1)}))),
            ("METRIC_INVALID", dict(base, functions=dict(base["functions"], **{"m.f": dict(base["functions"]["m.f"], exclusive_seconds=9.0)}))),
            ("METRIC_INVALID", dict(base, functions=dict(base["functions"], **{"m.f": dict(base["functions"]["m.f"], stage="SLICES")}))),
            ("AGGREGATE_MISMATCH", dict(base, stages=dict(base["stages"], KERNEL=dict(base["stages"]["KERNEL"], count=3)))),
            ("AGGREGATE_MISMATCH", dict(base, stages=dict(base["stages"], KERNEL=dict(base["stages"]["KERNEL"], exclusive_seconds=3.0, inclusive_seconds=3.0)))),
            ("AGGREGATE_MISMATCH", dict(base, stages=dict(base["stages"], SLICES=dict(base["stages"]["SLICES"], status="OBSERVED")))),
            ("TIMING_INVALID", dict(base, timing=dict(base["timing"], attributed_exclusive_seconds=1.0))),
            ("TIMING_INVALID", dict(base, timing=dict(base["timing"], observation_interval_seconds=None))),
            ("TIMING_INVALID", dict(base, timing=dict(base["timing"], **{monitor.UNATTRIBUTED: 5.0}))),
            ("TIMING_INVALID", dict(base, timing=dict(base["timing"], observation_interval_seconds=3.0, **{monitor.UNATTRIBUTED: -1.0}))),
            ("TIMING_INVALID", dict(base, timing=dict(base["timing"], timing_complete=False, timing_incomplete_reason=None))),
            ("TIMING_INVALID", dict(base, timing=dict(base["timing"], timing_complete=False, timing_incomplete_reason="X", **{monitor.UNATTRIBUTED: 6.0}))),
            ("FAULTS_INVALID", dict(base, faults={"callback_faults": 0})),
            ("TIMING_INVALID", dict(base, faults=dict(base["faults"], callback_faults=1))),
            ("TIMING_INVALID", dict(base, faults=dict(base["faults"], stack_mismatches=1))),
            ("TIMING_INVALID", dict(base, faults=dict(base["faults"], invalid_intervals=1))),
            ("HOOKS_INVALID", dict(base, hooks={**base["hooks"],
                "s.cp": dict(base["hooks"]["s.cp"], status=monitor.UNRESOLVED)})),
            ("CELL_COUNTS_INVALID", dict(base, cells_started=2)),
            ("CELL_COUNTS_INVALID", dict(base, cells_completed=2)),
            ("CHECKPOINT_COUNTS_INVALID", dict(base, hooks={**base["hooks"],
                "s.cp": dict(base["hooks"]["s.cp"], fired=2)})),
            ("FOOTER_INVALID:2:stage_nesting", {k: v for k, v in base.items() if k != "stage_nesting"}),
            ("NESTING_INVALID", dict(base, stage_nesting={"KERNEL": {
                "OTHER": {"count": 1, "inclusive_seconds": 1.0}}})),
            ("NESTING_INVALID", dict(base, stage_nesting={"KERNEL": {
                "KERNEL": {"count": True, "inclusive_seconds": 1.0}}})),
            ("NESTING_MISMATCH:KERNEL", dict(base, stage_nesting={"KERNEL": {
                "KERNEL": {"count": 1, "inclusive_seconds": 1.0}}})),
            ("HOOKS_INVALID", dict(base, hooks={"s.cp": {"kind": "OTHER", "role": "NATIVE", "status": "ARMED", "fired": 1}})),
            ("COUNTERS_INVALID", dict(base, writer_counters=dict(base["writer_counters"], dropped_full=-1))),
            ("FOOTER_MISMATCH", dict(base, writer_counters=dict(base["writer_counters"], rows=2))),
            ("FOOTER_MISMATCH", dict(base, writer_counters=dict(base["writer_counters"], bytes_before_footer=len(row) + 1))),
            ("FOOTER_ON_CORRUPT_TAIL", dict(base, writer_counters=dict(base["writer_counters"], corrupt_tail=True))),
            ("TEARDOWN_INVALID", dict(base, teardown={"complete": True, "failed_steps": ["x"], "tool_id_released": False})),
            ("FOOTER_INVALID", {k: v for k, v in base.items() if k != "timing"}),
        )
        for index, (code, footer) in enumerate(cases, start=1):
            with self.subTest(case=f"{index:02d}:{code}"):
                path = self.write([row, monitor._encode(footer)])
                with self.assertRaisesRegex(monitor.StageMonitorError, code):
                    monitor.read_monitor_file(path, self.EXPECTED)
        tiny = dict(base, writer_counters=dict(base["writer_counters"], max_file_bytes=10))
        path = self.write([row, monitor._encode(tiny)])
        with self.assertRaisesRegex(monitor.StageMonitorError, "FILE_EXCEEDS_DECLARED_BOUND"):
            monitor.read_monitor_file(path, self.EXPECTED)
        path = self.write([row, monitor._encode(base), row])
        with self.assertRaisesRegex(monitor.StageMonitorError, "ROW_AFTER_FOOTER:3"):
            monitor.read_monitor_file(path, self.EXPECTED)
        path = self.write([row[:-1]])
        with self.assertRaisesRegex(monitor.StageMonitorError, "TRUNCATED_TAIL"):
            monitor.read_monitor_file(path, self.EXPECTED)
        path = self.write([b"not json\n"])
        with self.assertRaisesRegex(monitor.StageMonitorError, "LINE_INVALID:1"):
            monitor.read_monitor_file(path, self.EXPECTED)
        with self.assertRaisesRegex(monitor.StageMonitorError, "READ_BOUND_INVALID"):
            monitor.read_monitor_file(path, self.EXPECTED, max_read_bytes=10)
        path = self.write([b"x" * (monitor.MAX_LINE_BYTES + 1)])
        with self.assertRaisesRegex(monitor.StageMonitorError, "FILE_TOO_LARGE"):
            monitor.read_monitor_file(path, self.EXPECTED, max_read_bytes=monitor.MAX_LINE_BYTES)

    def test_reader_rejects_malformed_rows(self):
        bad_rows = (
            ("ROW_INVALID:1", {"schema_version": monitor.SCHEMA_VERSION, "kind": "cumulative", "stages": {}}),
            ("ROW_INVALID:1", {"schema_version": monitor.SCHEMA_VERSION, "kind": "cumulative", "writer": "w", "pid": 1,
                               "monitor_elapsed_seconds": 1.0, "cells_started": 1, "stages": {"KERNEL": {}}}),
            ("ROW_METRIC_INVALID:1:KERNEL", {"schema_version": monitor.SCHEMA_VERSION, "kind": "cumulative", "writer": "w", "pid": 1,
                                             "monitor_elapsed_seconds": 1.0, "cells_started": 1,
                                             "stages": {"KERNEL": {"count": -1, "inclusive_seconds": 0.0, "exclusive_seconds": 0.0},
                                                        "SLICES": {"count": 0, "inclusive_seconds": 0.0, "exclusive_seconds": 0.0}}}),
        )
        for index, (code, body) in enumerate(bad_rows, start=1):
            with self.subTest(case=f"{index:02d}:{code}"):
                path = self.write([monitor._encode(body)])
                with self.assertRaisesRegex(monitor.StageMonitorError, code):
                    monitor.read_monitor_file(path, self.EXPECTED)


class GovernorHookTests(_Base):
    """The governor's child entry with and without the monitor, directly and in spawned children (V2, V3, C1, C3)."""

    @staticmethod
    def envelope_core(envelope):
        return (envelope.task_key, envelope.value, envelope.error_type, envelope.error_message, envelope.traceback_text)

    @staticmethod
    def outcome_core(outcome):
        return (outcome.status, outcome.value, outcome.error_type, outcome.error_message, outcome.traceback_text)

    def run_task(self, worker, *args):
        return governor._measured_worker_call(worker, governor.MemoryTask(key="job", args=tuple(args)))

    def files(self):
        return sorted(self.root.glob("stage_monitor.job.*.jsonl"))

    def allow(self, *names):
        refs = governor._STAGE_MONITOR_WORKER_REFS + tuple((helpers.__name__, name) for name in names)
        patch = mock.patch.object(governor, "_STAGE_MONITOR_WORKER_REFS", refs)
        patch.start()
        self.addCleanup(patch.stop)

    def toy_maps(self):
        """Bind the module maps to the spawn helpers' functions, which the governor-run workers actually call."""
        for attr, value in (("STAGE_MAP", HELPER_TARGETS), ("CELL_ENTRIES", ()), ("CHECKPOINTS", ())):
            patch = mock.patch.object(monitor, attr, value)
            patch.start()
            self.addCleanup(patch.stop)

    def test_exact_worker_identity(self):
        import pm_historical_scheduler
        self.assertTrue(governor._stage_monitor_allows(pm_historical_scheduler.declared_symbol_worker))
        spoof = types.FunctionType(outer.__code__, outer.__globals__, "declared_symbol_worker")
        spoof.__module__ = "pm_historical_scheduler"
        spoof.__qualname__ = "declared_symbol_worker"
        self.assertFalse(governor._stage_monitor_allows(spoof))
        self.assertFalse(governor._stage_monitor_allows(outer))
        self.allow("spawned_ok")
        self.assertTrue(governor._stage_monitor_allows(helpers.spawned_ok))

    def test_env_unset_never_imports_and_behaviour_is_unchanged(self):
        self.allow("spawned_ok")
        with mock.patch.dict(os.environ, {monitor.ENV_DIRECTORY: ""}, clear=False):
            with mock.patch.object(governor.importlib, "import_module") as spy:
                envelope = self.run_task(helpers.spawned_ok, 2)
        spy.assert_not_called()
        self.assertEqual(envelope.value, 8)
        self.assertEqual(envelope.error_type, "")
        self.assertEqual(self.files(), [])

    def test_other_workers_never_import(self):
        with mock.patch.dict(os.environ, {monitor.ENV_DIRECTORY: str(self.root)}, clear=False):
            with mock.patch.object(governor.importlib, "import_module") as spy:
                envelope = self.run_task(outer, 2)
        spy.assert_not_called()
        self.assertEqual(envelope.value, 8)
        self.assertEqual(self.files(), [])

    def test_direct_envelope_identical_on_return_and_raise(self):
        self.allow("spawned_ok", "spawned_boom")
        self.toy_maps()
        with mock.patch.dict(os.environ, {monitor.ENV_DIRECTORY: ""}, clear=False):
            plain_ok = self.run_task(helpers.spawned_ok, 2)
            plain_err = self.run_task(helpers.spawned_boom)
        with mock.patch.dict(os.environ, {monitor.ENV_DIRECTORY: str(self.root)}, clear=False):
            timed_ok = self.run_task(helpers.spawned_ok, 2)
            timed_err = self.run_task(helpers.spawned_boom)
        self.assertEqual(self.envelope_core(plain_ok), self.envelope_core(timed_ok))
        self.assertEqual(self.envelope_core(plain_err), self.envelope_core(timed_err))
        self.assertEqual(timed_err.error_type, "builtins.ValueError")
        files = self.files()
        self.assertEqual(len(files), 2)
        counts = []
        for path in files:
            _, footer = monitor.read_monitor_file(str(path), HELPER_MAP)
            self.assertTrue(footer["monitoring_active_at_stop"])
            self.assertTrue(footer["teardown"]["complete"])
            self.assertIsNotNone(footer["timing"]["task_duration_seconds"])
            counts.append((footer["stages"]["OUTER"]["count"], footer["stages"]["LEAF"]["count"], footer["stages"]["BOOM"]["count"]))
        self.assertEqual(sorted(counts), [(0, 0, 1), (1, 2, 0)])    # spawned_ok(2): outer once, leaf twice; spawned_boom: one unwind
        for tool_id in monitor._TOOL_IDS:
            self.assertNotEqual(sys.monitoring.get_tool(tool_id), monitor.TOOL_NAME)

    def test_direct_envelope_identical_under_injected_faults(self):
        self.allow("spawned_ok")
        self.toy_maps()
        with mock.patch.dict(os.environ, {monitor.ENV_DIRECTORY: ""}, clear=False):
            plain = self.run_task(helpers.spawned_ok, 5)
        with mock.patch.dict(os.environ, {monitor.ENV_DIRECTORY: str(self.root)}, clear=False):
            with mock.patch.object(monitor.StageMonitor, "_mine", side_effect=RuntimeError("timer broke")):
                faulted = self.run_task(helpers.spawned_ok, 5)
        self.assertEqual(self.envelope_core(plain), self.envelope_core(faulted))
        fault_file, = self.files()
        _, fault_footer = monitor.read_monitor_file(str(fault_file), HELPER_MAP)
        self.assertGreater(fault_footer["faults"]["callback_faults"], 0)
        self.assertFalse(fault_footer["timing"]["timing_complete"])
        blocker = self.root / "not_a_dir"
        blocker.write_bytes(b"x")
        with mock.patch.dict(os.environ, {monitor.ENV_DIRECTORY: str(blocker)}, clear=False):
            unwritable = self.run_task(helpers.spawned_ok, 5)
        self.assertEqual(self.envelope_core(plain), self.envelope_core(unwritable))
        self.assertEqual(self.files(), [fault_file])
        self.assertEqual(blocker.read_bytes(), b"x")
        taken = []
        try:
            for tool_id in monitor._TOOL_IDS:
                if sys.monitoring.get_tool(tool_id) is None:
                    sys.monitoring.use_tool_id(tool_id, "test-occupant")
                    taken.append(tool_id)
            with mock.patch.dict(os.environ, {monitor.ENV_DIRECTORY: str(self.root)}, clear=False):
                busy = self.run_task(helpers.spawned_ok, 5)
        finally:
            for tool_id in taken:
                sys.monitoring.free_tool_id(tool_id)
        self.assertEqual(self.envelope_core(plain), self.envelope_core(busy))
        busy_file, = set(self.files()) - {fault_file}
        _, busy_footer = monitor.read_monitor_file(str(busy_file), HELPER_MAP)
        self.assertEqual(busy_footer["faults"]["tool_id_unavailable"], 1)
        self.assertFalse(busy_footer["monitoring_active_at_stop"])
        real_write = os.write
        with mock.patch.dict(os.environ, {monitor.ENV_DIRECTORY: str(self.root)}, clear=False):
            with mock.patch.object(monitor.os, "write", side_effect=lambda fd, data: real_write(fd, data[:3])):
                short = self.run_task(helpers.spawned_ok, 5)
        self.assertEqual(self.envelope_core(plain), self.envelope_core(short))
        short_file, = set(self.files()) - {fault_file, busy_file}
        self.assertEqual(short_file.stat().st_size, 3)
        with self.assertRaisesRegex(monitor.StageMonitorError, "TRUNCATED_TAIL"):
            monitor.read_monitor_file(str(short_file), HELPER_MAP)
        for tool_id in monitor._TOOL_IDS:
            self.assertNotEqual(sys.monitoring.get_tool(tool_id), monitor.TOOL_NAME)

    # -- spawned children ------------------------------------------------------------------------------------------
    def spawned(self, worker, *args, env):
        with mock.patch.dict(os.environ, env, clear=False):
            outcomes = list(governor.MemoryGovernor(requested_workers=1).iter_completed(
                worker, [governor.MemoryTask(key="job", args=tuple(args))]))
        self.assertEqual(len(outcomes), 1)
        return outcomes[0]

    def test_spawned_child_without_env_has_no_module_and_no_route(self):
        outcome = self.spawned(helpers.spawned_probe, env={monitor.ENV_DIRECTORY: "", "FXPM_STAGE_MONITOR_TEST_ALLOW": "1"})
        self.assertEqual(outcome.status, "COMPLETED")
        self.assertEqual(outcome.value, {"monitor_loaded": False, "route_variable": ""})
        self.assertEqual(self.files(), [])

    def test_spawned_envelope_identical_including_traceback(self):
        allow = {"FXPM_STAGE_MONITOR_TEST_ALLOW": "1"}
        plain_ok = self.spawned(helpers.spawned_ok, 3, env={monitor.ENV_DIRECTORY: "", **allow})
        plain_err = self.spawned(helpers.spawned_boom, env={monitor.ENV_DIRECTORY: "", **allow})
        timed_ok = self.spawned(helpers.spawned_ok, 3, env={monitor.ENV_DIRECTORY: str(self.root), **allow})
        timed_err = self.spawned(helpers.spawned_boom, env={monitor.ENV_DIRECTORY: str(self.root), **allow})
        blocker = self.root / "not_a_dir"
        blocker.write_bytes(b"x")
        faulted = self.spawned(helpers.spawned_ok, 3, env={monitor.ENV_DIRECTORY: str(blocker), **allow})
        self.assertEqual(self.outcome_core(plain_ok), self.outcome_core(timed_ok))
        self.assertEqual(self.outcome_core(plain_err), self.outcome_core(timed_err))
        self.assertEqual(self.outcome_core(plain_ok), self.outcome_core(faulted))
        self.assertEqual(plain_ok.value, 12)
        self.assertEqual(plain_err.error_type, "builtins.ValueError")
        self.assertIn("spawned boom", plain_err.traceback_text)
        files = self.files()
        self.assertEqual(len(files), 2)   # the unwritable-directory run left nothing behind
        for path in files:
            _, footer = monitor.read_monitor_file(str(path))
            self.assertIsNotNone(footer)
            self.assertEqual(footer["roster"], list(monitor.expected_map(monitor.STAGE_MAP)))
            self.assertTrue(footer["teardown"]["complete"])
            self.assertTrue(footer["teardown"]["tool_id_released"])
            summary = monitor.summarize(footer)
            self.assertFalse(summary["coverage"]["complete"])  # the toy child never imports the scheduler or stage modules
            self.assertIn("CHECKPOINT_UNAVAILABLE", summary["coverage"]["failed_checks"])
            self.assertIn("INCOMPLETE_COVERAGE", summary["coverage"]["failed_checks"])


if __name__ == "__main__":
    unittest.main()
