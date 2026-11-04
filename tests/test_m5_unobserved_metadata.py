"""SYNTHETIC_METADATA_GUARDS_ONLY_NO_NATIVE_OR_FITTED_POSITIVE_PROOF.

These predicates validate ordinary DATA. Incomplete receipt shapes below cannot
pass the original public source/fit verifiers. No such verifier is mocked.
"""
import ast
from copy import deepcopy
import inspect
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest


_REPAIR = Path(__file__).resolve().parents[1]
_CANDIDATE = _REPAIR
_REPO = _REPAIR
sys.path.insert(0, str(_REPO))
import pm_book_multiplicity as book


def _adapter_helpers():
    path = _REPAIR / "pm_historical_production_adapter.py"
    tree = ast.parse(path.read_bytes(), filename=str(path))
    names = {"HistoricalProductionAdapterError", "_fail", "_declared_m5_unobserved_shape",
        "_declared_m5_unobserved_zero_support"}
    selected = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
        and node.name in names]
    if {node.name for node in selected} != names:
        raise RuntimeError("actual isolated unobserved guard definitions are missing")
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    scope = {"__name__": "synthetic_unobserved_metadata_guards", "book": book}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *selected], type_ignores=[])),
        str(path), "exec"), scope)
    return SimpleNamespace(**scope)


def _shape():
    # This is only a shape predicate input, not a usable source or fit receipt.
    completion = {"status": "NO_TRADE", "reason_code": "REGIME_UNOBSERVED",
        "source_group": {"fixture": "INCOMPLETE_UNIT_DATA"}, "regime_fit": {"fixture": "INCOMPLETE_UNIT_DATA"},
        "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False}
    session = {"source_kind": "m5_derived", "failure": None, "cost_status": "COST_READY"}
    return completion, session


def _support():
    target = book.REGIME_ORDER[0]
    body = {"symbol": "UNITUSD", "timeframe": "H1", "regime": target}
    support = {"symbol": "UNITUSD", "timeframe": "H1", "counts": dict.fromkeys(book.REGIME_ORDER, 0),
        "eligible_rows": 0, "support_state_by_regime": dict.fromkeys(book.REGIME_ORDER, "UNOBSERVED"),
        "observed_regimes": [], "unobserved_regimes": list(book.REGIME_ORDER)}
    return body, {"fit_state": book.REGIME_FITTED, "train_regime_support": support}


class M5UnobservedMetadataGuardTests(unittest.TestCase):
    def test_only_zero_work_no_trade_ready_shape_with_exact_false_authorities_is_allowed(self):
        helpers = _adapter_helpers()
        completion, session = _shape()
        before = deepcopy((completion, session))
        self.assertIsNone(helpers._declared_m5_unobserved_shape(completion, session))
        self.assertEqual((completion, session), before)
        cases = ("status", "reason", "owner", "plan_transport", "horizon_plan", "raw_production_receipt",
            "winner_request_entry", "technical_failure_entry", "authority_zero", "authority_true",
            "failure", "cost", "source_kind")
        for change in cases:
            completion, session = _shape()
            if change == "status":
                completion["status"] = "STOP"
            elif change == "reason":
                completion["reason_code"] = "SOURCE_FAILURE"
            elif change in {"owner", "plan_transport", "horizon_plan", "raw_production_receipt",
                            "winner_request_entry", "technical_failure_entry"}:
                completion[change] = {"fixture": "FORBIDDEN_IN_ZERO_WORK_LEAF"}
            elif change == "authority_zero":
                completion["qualification_authority"] = 0
            elif change == "authority_true":
                completion["order_authority"] = True
            elif change == "failure":
                session["failure"] = {"phase": "PRE_SOURCE"}
            elif change == "cost":
                session["cost_status"] = "COST_UNAVAILABLE"
            else:
                session["source_kind"] = "broker_native"
            with self.subTest(change=change), self.assertRaisesRegex(helpers.HistoricalProductionAdapterError,
                    "DECLARED_M5_UNOBSERVED_BINDING_MISMATCH"):
                helpers._declared_m5_unobserved_shape(completion, session)

    def test_zero_target_support_requires_exact_integer_accounting_and_target_timeframe(self):
        helpers = _adapter_helpers()
        body, fit = _support()
        self.assertIs(helpers._declared_m5_unobserved_zero_support(body, fit), fit["train_regime_support"])
        # Other regimes may be observed; only the actual target must be unobserved.
        other = book.REGIME_ORDER[1]
        support = fit["train_regime_support"]
        support["counts"][other] = 2
        support["eligible_rows"] = 2
        support["observed_regimes"] = [other]
        support["unobserved_regimes"] = [name for name in book.REGIME_ORDER if name != other]
        support["support_state_by_regime"][other] = "EMPIRICALLY_OBSERVED"
        self.assertIs(helpers._declared_m5_unobserved_zero_support(body, fit), support)
        for change in ("bool", "positive", "missing", "negative", "rows", "timeframe", "states", "fit_state"):
            body, fit = _support()
            support = fit["train_regime_support"]
            if change == "bool":
                support["counts"][body["regime"]] = False
            elif change == "positive":
                support["counts"][body["regime"]] = 1
                support["eligible_rows"] = 1
            elif change == "missing":
                support["counts"].pop(other)
            elif change == "negative":
                support["counts"][other] = -1
                support["eligible_rows"] = -1
            elif change == "rows":
                support["eligible_rows"] = 1
            elif change == "timeframe":
                support["timeframe"] = "M5"
            elif change == "states":
                support["support_state_by_regime"] = None
            else:
                fit["fit_state"] = "TECHNICAL_FAILURE"
            with self.subTest(change=change), self.assertRaisesRegex(helpers.HistoricalProductionAdapterError,
                    "DECLARED_M5_UNOBSERVED_SUPPORT_INVALID"):
                helpers._declared_m5_unobserved_zero_support(body, fit)

    def test_actual_source_validator_accepts_neutral_keyword_but_rejects_ambiguous_clocks_before_inputs(self):
        path = _REPAIR / "pm_authoritative_search_execution.py"
        tree = ast.parse(path.read_bytes(), filename=str(path))
        names = {"AuthoritativeSearchExecutionError", "_fail", "_m5_bound_source_inputs"}
        selected = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef))
            and node.name in names]
        self.assertEqual({node.name for node in selected}, names)
        future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
        scope = {"__name__": "synthetic_time_ambiguity_refusal"}
        exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *selected], type_ignores=[])),
            str(path), "exec"), scope)
        actual = scope["_m5_bound_source_inputs"]
        arguments = {"request": object(), "process": object(), "config": object(), "source_context": None,
            "source_session_entry": None, "source_session_sha256": "1"*64}
        # Binding arguments does not execute a product/source verifier.
        inspect.signature(actual).bind(object(), **arguments, snapshot_issued_at_utc="2026-11-03T00:00:01Z")
        inspect.signature(actual).bind(object(), **arguments, source_validation_completed_at_utc="2026-11-03T00:00:02Z")
        with self.assertRaises(scope["AuthoritativeSearchExecutionError"]) as caught:
            actual(object(), **arguments, snapshot_issued_at_utc="2026-11-03T00:00:01Z",
                source_validation_completed_at_utc="2026-11-03T00:00:02Z")
        self.assertEqual(caught.exception.code, "BOUND_M5_VALIDATION_TIME_AMBIGUOUS")


if __name__ == "__main__":
    unittest.main()
