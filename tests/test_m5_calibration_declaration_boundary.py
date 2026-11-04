"""Calibration declaration joins: INVENTED_METADATA_NO_NATIVE_VERIFICATION.

Only actual source expressions, plain immutable data classes and pure plan
hashing are exercised in future QA. No owner, recorded edition, producer seal,
native product, source payload or observed consumer is created or faked.
"""
from dataclasses import replace
import ast
import inspect
from pathlib import Path
from types import SimpleNamespace
import sys
import textwrap
import unittest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
import pm_historical_scheduler as scheduler
import pm_historical_search_producer as producer
import pm_m5_job_view as views
from pm_explicit_derived_history import Interval, SourceIdentity, Stratum

if Path(scheduler.__file__).resolve() != _ROOT / "pm_historical_scheduler.py":
    raise RuntimeError("CALIBRATION_TEST_REQUIRES_EXACT_WORKER_SOURCE")

PROFILE = scheduler._DECLARED_M5_PROFILE
CALIBRATION = "metadata-calibration"


def _declared_consumer(view_id):
    tree = ast.parse(textwrap.dedent(inspect.getsource(scheduler._declared_m5_new_owner)))
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute) and n.func.attr == "ViewRequest"]
    if len(calls) != 1:
        raise RuntimeError("WORKER_VIEW_DECLARATION_CHANGED")
    expression = next(kw.value for kw in calls[0].keywords if kw.arg == "consumer")
    # This is ordinary profile metadata, not a substituted RecordedDerivedCosts.
    edition_metadata = SimpleNamespace(sha256=PROFILE)
    return eval(compile(ast.fix_missing_locations(ast.Expression(expression)),
        "<actual worker consumer declaration>", "eval"),
        {"views": views, "edition": edition_metadata,
         "axis": {"view_id": view_id}, "dev": {"calibration_view_id": CALIBRATION}})


def _request(view_id, purpose, consumer=None):
    source = SourceIdentity("SYNTHETIC_METADATA", "M5", "NO_NATIVE_VERIFICATION",
        "1"*64, "2"*64, "3"*64, "4"*64, "5"*64, 0.01)
    return views.ViewRequest(view_id=view_id, source=source, timeframe="H1",
        interval=Interval("2024-01-01T00:00:00+00:00", "2024-01-01T01:00:00+00:00"),
        cutoff_utc="2024-01-01T01:00:00+00:00", constructor_implementation_sha256="6"*64,
        gap_findings_sha256="7"*64, result_key="metadata-terminal",
        recipe_sha256=scheduler._DECLARED_M5_RECIPE, consumer=consumer,
        stratum=Stratum("H1", "SYNTHETIC_METADATA_CLASS", "SYNTHETIC_FIXED_UTC"),
        purpose=purpose)


def _producer_calibration_refusal(request, profile=PROFILE):
    tree = ast.parse(textwrap.dedent(inspect.getsource(producer._declared_m5_cell_source_body)))
    guards = [n for n in ast.walk(tree) if isinstance(n, ast.If)
              and any(isinstance(x, ast.Attribute) and isinstance(x.value, ast.Name)
                      and x.value.id == "calibration" and x.attr == "purpose"
                      for x in ast.walk(n.test))]
    if len(guards) != 1:
        raise RuntimeError("PRODUCER_CALIBRATION_GUARD_CHANGED")
    return eval(compile(ast.fix_missing_locations(ast.Expression(guards[0].test)),
        "<actual producer calibration refusal predicate>", "eval"),
        {"calibration": request, "parent": {"spread_profile_identity": {"profile_sha256": profile}},
         "job_view": views})


class CalibrationDeclarationBoundaryTests(unittest.TestCase):
    def test_worker_declares_only_known_calibration_profile_consumer(self):
        consumer = _declared_consumer(CALIBRATION)
        self.assertIs(type(consumer), views.CalibrationViewBinding)
        self.assertEqual(consumer.profile_sha256, PROFILE)
        self.assertIsNone(_declared_consumer("metadata-slot"))
        self.assertIsNone(_declared_consumer("metadata-account"))

    def test_original_producer_guard_accepts_bound_profile_and_refuses_invalid_declaration(self):
        good = _request(CALIBRATION, "COST_EDITION_REPRODUCTION_ONLY", _declared_consumer(CALIBRATION))
        self.assertIs(_producer_calibration_refusal(good), False)
        invalid = (replace(good, consumer=None),
                   replace(good, consumer=views.CalibrationViewBinding("a"*64)),
                   replace(good, purpose="FINANCIAL_INPUT"))
        for request in invalid:
            with self.subTest(consumer=request.consumer, purpose=request.purpose):
                self.assertIs(_producer_calibration_refusal(request), True)

    def test_calibration_consumer_does_not_rekey_values_or_view_plan(self):
        before = (_request("metadata-slot", "FINANCIAL_INPUT"),
                  _request(CALIBRATION, "COST_EDITION_REPRODUCTION_ONLY"),
                  _request("metadata-account", "FINANCIAL_INPUT"))
        after = tuple(replace(request, consumer=_declared_consumer(request.view_id)) for request in before)
        self.assertEqual(views.value_plan_sha256(before), views.value_plan_sha256(after))
        self.assertEqual([views._value_request_body(row) for row in before],
                         [views._value_request_body(row) for row in after])
        self.assertIsNone(after[0].consumer)
        self.assertIsNone(after[2].consumer)
