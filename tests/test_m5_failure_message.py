"""SYNTHETIC_FAILURE_DIAGNOSTIC_METADATA_ONLY_NO_NATIVE_PROOF.

Actual producer constructors, the original strict spool encoder and the actual
M5 reader's failure block are used. The extracted block is not the full session
verifier; no source/process/fitted authority, receipt or model is manufactured.
"""
import ast
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
PRODUCER = ROOT / "pm_historical_search_producer.py"
ADAPTER = ROOT / "pm_historical_production_adapter.py"


def producer_parts():
    tree = ast.parse(PRODUCER.read_bytes(), filename=str(PRODUCER))
    names = {"HistoricalSearchProducerError", "_fail", "_canonical_bytes", "_now_utc_seconds",
        "_declared_failure_details", "_declared_m5_failure_message"}
    selected = [node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    if {node.name for node in selected} != names:
        raise RuntimeError("actual producer diagnostic helpers missing")
    utc = next(node for node in tree.body if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name) and node.target.id == "_UTC")
    scope = {"json": json, "datetime": datetime, "timezone": timezone,
        "__name__": "synthetic_failure_metadata_only", "__file__": str(PRODUCER)}
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    exec(compile(ast.fix_missing_locations(ast.Module(
        body=[future, utc, *selected], type_ignores=[])), str(PRODUCER), "exec"), scope)
    factory = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
        and node.name == "_make_declared_slot_source_authority")
    m5 = next(node for node in factory.body if isinstance(node, ast.FunctionDef) and node.name == "execute_m5")
    assignments = [node for node in ast.walk(m5) if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name)
            and target.value.id == "source_body" and isinstance(target.slice, ast.Constant)
            and target.slice.value == "failure" for target in node.targets)]
    if len(assignments) != 1:
        raise RuntimeError("actual M5 failure assignment ambiguous")
    return scope, assignments[0]


def actual_m5_failure(exc, phase="FIT"):
    scope, assignment = producer_parts()
    scope.update(exc=exc, phase=phase, source_body={})
    exec(compile(ast.fix_missing_locations(ast.Module(body=[assignment], type_ignores=[])),
        str(PRODUCER), "exec"), scope)
    return scope["source_body"]["failure"]


def reader_parts():
    import pm_historical_anchor_orchestration as orchestration
    tree = ast.parse(ADAPTER.read_bytes(), filename=str(ADAPTER))
    names = {"HistoricalProductionAdapterError", "_fail", "_reason", "_canonical_bytes"}
    selected = [node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    if {node.name for node in selected} != names:
        raise RuntimeError("actual adapter metadata helpers missing")
    reader = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
        and node.name == "_validate_declared_m5_source_session")
    start = next(index for index, node in enumerate(reader.body) if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "failure" for target in node.targets))
    # Only the actual failure validation block and canonical return are executed.
    # Earlier full-session authority/source/profile checks are not substituted.
    fragment = ast.parse("def decode_failure_metadata(value, *, opened, completed, accessed=None):\n    pass\n").body[0]
    fragment.body = reader.body[start:]
    scope = {"json": json, "orchestration": orchestration,
        "__name__": "synthetic_reader_failure_metadata_only", "__file__": str(ADAPTER)}
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    exec(compile(ast.fix_missing_locations(ast.Module(
        body=[future, *selected, fragment], type_ignores=[])), str(ADAPTER), "exec"), scope)
    return scope


def legacy_failure():
    return {"exception_type": "ValueError", "failure_code": "DECLARED_SLOT_SOURCE_FAILURE",
        "failure_observed_at_utc": "2026-11-04T08:00:10Z", "phase": "FIT"}


def decode(scope, failure, **changes):
    args = {"opened": datetime(2026, 11, 4, 8, 0, 0, tzinfo=timezone.utc),
        "completed": datetime(2026, 11, 4, 8, 0, 20, tzinfo=timezone.utc),
        "accessed": datetime(2026, 11, 4, 8, 0, 5, tzinfo=timezone.utc)}
    args.update(changes)
    return scope["decode_failure_metadata"]({"failure": failure}, **args)["failure"]


class M5FailureMessageTests(unittest.TestCase):
    def test_m5_handler_retains_known_message_and_original_native_details_stay_unchanged(self):
        import pm_authority_spool as spool
        exc = ValueError("owned failure β 🚀\nsecond line")
        failure = actual_m5_failure(exc)
        self.assertEqual(set(failure), {"exception_type", "failure_code",
            "failure_observed_at_utc", "phase", "message"})
        self.assertEqual(failure["message"], str(exc))
        self.assertEqual(failure["exception_type"], "ValueError")
        self.assertEqual(failure["failure_code"], "DECLARED_SLOT_SOURCE_FAILURE")
        self.assertEqual(failure["phase"], "FIT")
        self.assertEqual(json.loads(spool._canonical_json_bytes(failure, 8192)), failure)
        h, _ = producer_parts()
        native = h["_declared_failure_details"](exc)
        self.assertEqual(set(native), {"exception_type", "failure_code", "failure_observed_at_utc"})
        self.assertNotIn("message", native)

    def test_unicode_length_malformed_text_and_raising_str_are_bounded_json_safe(self):
        import pm_authority_spool as spool
        class BrokenStr(Exception):
            def __str__(self):
                raise ValueError("SYNTHETIC_STR_FAILURE")
        class InvalidStrReturn(Exception):
            def __str__(self):
                return None
        cases = [ValueError("🚀"*205), ValueError("\ud800prefix\udfff"),
            ValueError(""), BrokenStr(), InvalidStrReturn()]
        for number, exc in enumerate(cases):
            with self.subTest(synthetic_case=number):
                failure = actual_m5_failure(exc)
                message = failure["message"]
                self.assertIs(type(message), str)
                self.assertLessEqual(len(message), 200)
                message.encode("utf-8")  # No surviving surrogate codepoints.
                self.assertEqual(json.loads(spool._canonical_json_bytes(failure, 8192)), failure)
                if number == 0:
                    self.assertEqual(message, "🚀"*200)
                elif number == 1:
                    self.assertEqual(message, "\\ud800prefix\\udfff")
                elif number == 2:
                    self.assertEqual(message, "")
                else:
                    self.assertEqual(message, "<exception message unavailable>")

    def test_reader_accepts_exact_legacy_four_or_new_five_preserving_plain_text(self):
        h = reader_parts()
        for message in (None, "", "owned failure β 🚀", "🚀"*200):
            failure = legacy_failure()
            if message is not None:
                failure["message"] = message
            original = deepcopy(failure)
            with self.subTest(message_present=message is not None, characters=0 if message is None else len(message)):
                self.assertEqual(decode(h, failure), failure)
                self.assertEqual(failure, original)

    def test_reader_rejects_unknown_missing_type_oversize_and_malformed_message(self):
        h = reader_parts()
        class StrSubclass(str):
            pass
        invalid = []
        for message in (None, 7, True, [], {}, StrSubclass("owned"), "x"*201, "\ud800"):
            invalid.append({**legacy_failure(), "message": message})
        invalid.append({**legacy_failure(), "message": "owned", "unknown": "diagnostic"})
        invalid.append({**legacy_failure(), "unknown": "diagnostic"})
        missing = legacy_failure()
        del missing["phase"]
        invalid.append(missing)
        for number, failure in enumerate(invalid):
            with self.subTest(synthetic_case=number), self.assertRaises(h["HistoricalProductionAdapterError"]) as refused:
                decode(h, failure)
            self.assertEqual(str(refused.exception), "DECLARED_M5_SOURCE_SESSION_INVALID")

    def test_existing_phase_code_and_causal_time_guards_remain_for_both_shapes(self):
        h = reader_parts()
        for with_message in (False, True):
            for mutation in ("phase", "code", "type", "before_open", "after_complete", "before_access"):
                failure = legacy_failure()
                args = {}
                if with_message:
                    failure["message"] = "diagnostic only"
                if mutation == "phase":
                    failure["phase"] = "UNKNOWN"
                elif mutation == "code":
                    failure["failure_code"] = ""
                elif mutation == "type":
                    failure["exception_type"] = False
                elif mutation == "before_open":
                    failure["failure_observed_at_utc"] = "2026-11-04T07:59:59Z"
                elif mutation == "after_complete":
                    failure["failure_observed_at_utc"] = "2026-11-04T08:00:21Z"
                else:
                    args["accessed"] = datetime(2026, 11, 4, 8, 0, 11, tzinfo=timezone.utc)
                with self.subTest(message_present=with_message, mutation=mutation):
                    with self.assertRaises(h["HistoricalProductionAdapterError"]) as refused:
                        decode(h, failure, **args)
                    expected = ("DECLARED_M5_SOURCE_BINDING_MISMATCH" if mutation in {
                        "before_open", "after_complete", "before_access"} else "DECLARED_M5_SOURCE_SESSION_INVALID")
                    self.assertEqual(str(refused.exception), expected)


if __name__ == "__main__":
    unittest.main()
