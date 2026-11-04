"""Native cutoff wire-format tests: INVENTED_METADATA_NO_NATIVE_VERIFICATION.

Only the actual worker request expression and original native batch parser run
in future QA. No prefix production, owner construction, data or model is used.
"""
from copy import deepcopy
from datetime import datetime, timezone
import ast
import inspect
from pathlib import Path
import sys
import textwrap
import unittest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
import pm_historical_scheduler as scheduler
import pm_native_prefix_production as native

if Path(scheduler.__file__).resolve() != _ROOT / "pm_historical_scheduler.py":
    raise RuntimeError("CUTOFF_TEST_REQUIRES_EXACT_WORKER_SOURCE")


def _worker_native_requests(cutoffs, accessed):
    tree = ast.parse(textwrap.dedent(inspect.getsource(scheduler._declared_m5_new_owner)))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute)
             and node.func.attr == "produce_activated_native_prefixes"]
    if len(calls) != 1:
        raise RuntimeError("WORKER_NATIVE_BOUNDARY_CHANGED")
    expression = next(kw.value for kw in calls[0].keywords if kw.arg == "requests")
    code = compile(ast.fix_missing_locations(ast.Expression(expression)),
                   "<actual worker native request expression>", "eval")
    return eval(code, {"__builtins__": {"tuple": tuple, "__import__": __import__},
        "_declared_m5_utc": scheduler._declared_m5_utc,
        "cutoffs": tuple(cutoffs), "accessed": accessed})


class NativeCutoffBoundaryTests(unittest.TestCase):
    def accessed(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def test_recorded_three_cutoffs_reach_actual_native_parser_without_axis_rewrite(self):
        axes = [{"cutoff_utc": "2023-06-23T00:00:00Z"},
                {"cutoff_utc": "2023-07-22T00:00:00Z"},
                {"cutoff_utc": "2024-07-14T08:05:00+00:00"}]
        before = deepcopy(axes)
        accessed = self.accessed()
        raw_requests = tuple({"cutoff_utc": axis["cutoff_utc"],
                              "source_accessed_at_utc": accessed} for axis in axes)
        with self.assertRaises(native.NativePrefixError) as caught:
            native._batch_requests(raw_requests)
        self.assertEqual(caught.exception.code, "PREFIX_CUTOFF_INVALID")
        wire = _worker_native_requests(tuple(axis["cutoff_utc"] for axis in axes), accessed)
        parsed = native._batch_requests(wire)
        self.assertEqual([item["cutoff_utc"] for item in wire],
            ["2023-06-23T00:00:00Z", "2023-07-22T00:00:00Z", "2024-07-14T08:05:00Z"])
        self.assertEqual(axes, before)
        self.assertEqual([cutoff for cutoff, _ in parsed],
                         [scheduler._declared_m5_utc(axis["cutoff_utc"]) for axis in axes])
        self.assertTrue(all(item["source_accessed_at_utc"] == accessed for item in wire))

    def test_z_and_zero_offset_are_the_same_native_cutoff_instant(self):
        accessed = self.accessed()
        z = _worker_native_requests(("2024-07-14T08:05:00Z",), accessed)
        offset = _worker_native_requests(("2024-07-14T08:05:00+00:00",), accessed)
        self.assertEqual(z, offset)
        self.assertEqual(native._batch_requests(z), native._batch_requests(offset))

    def test_nonzero_fractional_cutoffs_refuse_and_native_parser_remains_strict(self):
        accessed = self.accessed()
        for cutoff in ("2024-07-14T08:05:00.5+00:00", "2024-07-14T08:05:00.000001Z",
                       "2024-07-14T08:05:00", "2024-07-14T09:05:00+01:00"):
            with self.subTest(worker_cutoff=cutoff):
                with self.assertRaises(scheduler.HistoricalSchedulerError) as caught:
                    _worker_native_requests((cutoff,), accessed)
                self.assertEqual(caught.exception.code, "DECLARED_M5_TIMESTAMP_INVALID")
        for cutoff in ("2024-07-14T08:05:00.5Z", "2024-07-14T08:05:00.000Z",
                       "2024-07-14T08:05:00+00:00"):
            with self.subTest(native_cutoff=cutoff):
                with self.assertRaises(native.NativePrefixError) as caught:
                    native._batch_requests(({"cutoff_utc": cutoff,
                                             "source_accessed_at_utc": accessed},))
                self.assertEqual(caught.exception.code, "PREFIX_CUTOFF_INVALID")
