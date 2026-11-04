"""SYNTHETIC_JSON_METADATA_ONLY_NO_NATIVE_OR_FIT_PROOF.

Real immutable value dataclasses and the original strict spool encoder are used.
Only publication statements are selected from the actual M5 producer; no public
source verifier, process, fitted capability, model, detector or issuer is faked.
"""
import ast
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timezone
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "pm_historical_search_producer.py"


def source_parts():
    tree = ast.parse(SOURCE.read_bytes(), filename=str(SOURCE))
    names = {"HistoricalSearchProducerError", "_fail", "_canonical_bytes",
        "_declared_m5_view_observation", "_declared_failure_details", "_now_utc_seconds"}
    selected = [node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    if {node.name for node in selected} != names:
        raise RuntimeError("actual producer JSON helpers missing")
    constants = [node for node in tree.body if (
        isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in {
            "DECLARED_M5_SLOT_SOURCE_SESSION_KEYS", "DECLARED_M5_SLOT_SOURCE_SESSION_SCHEMA"}
            for target in node.targets)) or (
        isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "_UTC")]
    if len(constants) != 3:
        raise RuntimeError("actual M5 source-session constants missing")
    scope = {"__name__": "synthetic_json_metadata_only", "__file__": str(SOURCE),
        "json": json, "datetime": datetime, "timezone": timezone}
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    exec(compile(ast.fix_missing_locations(ast.Module(
        body=[future, *constants, *selected], type_ignores=[])), str(SOURCE), "exec"), scope)
    factory = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
        and node.name == "_make_declared_slot_source_authority")
    execute_m5 = next(node for node in factory.body if isinstance(node, ast.FunctionDef)
        and node.name == "execute_m5")
    return scope, execute_m5


def owned_view():
    import pm_m5_job_view as views
    from pm_explicit_derived_history import Bar, DerivedBar, DerivedSeries, Interval, SourceIdentity, Stratum
    whole = Interval("2024-01-02T00:00:00+00:00", "2024-01-02T02:00:00+00:00")
    available = Interval(whole.start_utc, "2024-01-02T01:00:00+00:00")
    omitted = Interval(available.end_exclusive_utc, whole.end_exclusive_utc)
    gap = Interval("2024-01-02T00:15:00+00:00", "2024-01-02T00:20:00+00:00")
    source = SourceIdentity("SYNTHETIC", "M5", "SYNTHETIC_DATA_NOT_ISSUED",
        "1"*64, "2"*64, "3"*64, "4"*64, "5"*64, 0.01)
    bar = Bar(available.start_utc, available.end_exclusive_utc, 2, 3, 1, 2.5, 12, 8, 3)
    derived = DerivedBar(bar, 12, "6"*64, (3, 4, 2, 5))
    series = DerivedSeries(Stratum("H1", "SYNTHETIC_CLASS", "SYNTHETIC_ALIGNMENT"),
        source, available, available.end_exclusive_utc, (derived,), 12, 1, "7"*64)
    coverage = views.ViewCoverage(whole, (available,), (available,), (omitted,), (omitted,),
        available, (omitted,), (gap,), (0, 7))
    return views.DerivedPrefixView(view_key="8"*64, source=source, source_metadata_digest="9"*64,
        constructor_implementation_sha256="a"*64, gap_findings_sha256="b"*64,
        timeframe="H1", interval=available, cutoff_utc=available.end_exclusive_utc,
        rows=(bar,), member_spans=(), series=series, coverage=coverage,
        payload_sha256="c"*64, membership_sha256="d"*64,
        cost_status="COST_DATA_ONLY_PROFILE_UNVERIFIED",
        constructor_evidence_status="SYNTHETIC_DATA_NOT_NATIVE_AUTHORITY")


class StrictJsonSinkUnitDouble:
    """Serialization unit double, not an authority or a successful verifier."""
    def __init__(self):
        self.records = []

    def put_json(self, artifact_id, artifact_type, value):
        import pm_authority_spool as spool
        encoded = spool._canonical_json_bytes(value, 1024 * 1024)
        self.records.append((artifact_id, artifact_type, encoded))
        return {"unit_double_only": True}


def publication_statements(execute_m5, technical):
    def observation_assignment(node):
        return (isinstance(node, ast.Assign) and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Subscript)
            and isinstance(node.targets[0].value, ast.Name)
            and node.targets[0].value.id == "source_body"
            and isinstance(node.targets[0].slice, ast.Constant)
            and node.targets[0].slice.value in {
                "m5_request_view_observation", "m5_calibration_view_observation"})
    observations = [node for node in ast.walk(execute_m5) if observation_assignment(node)]
    if len(observations) != 2:
        raise RuntimeError("actual paired observation assignments changed")
    suffix = "/technical-source-session" if technical else "/source-session"
    def publication(node):
        return (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute) and node.value.func.attr == "put_json"
            and isinstance(node.value.func.value, ast.Name) and node.value.func.value.id == "sink"
            and isinstance(node.value.args[0], ast.JoinedStr)
            and isinstance(node.value.args[0].values[-1], ast.Constant)
            and node.value.args[0].values[-1].value == suffix)
    publishes = [node for node in ast.walk(execute_m5) if publication(node)]
    if len(publishes) != 1:
        raise RuntimeError("actual source publication ambiguous")
    if not technical:
        return [*observations, publishes[0]]
    handler = next(node for node in ast.walk(execute_m5) if isinstance(node, ast.ExceptHandler)
        and any(isinstance(child, ast.Assign) and any(isinstance(target, ast.Subscript)
            and isinstance(target.value, ast.Name) and target.value.id == "source_body"
            and isinstance(target.slice, ast.Constant) and target.slice.value == "failure"
            for target in child.targets) for child in node.body))
    first = next(index for index, node in enumerate(handler.body) if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Subscript) and isinstance(target.slice, ast.Constant)
            and target.slice.value == "failure" for target in node.targets))
    last = handler.body.index(publishes[0])
    # Preserve actual failure detail assignment, exact-field guard and put_json.
    return [*observations, *handler.body[first:last+1]]


class M5SourceObservationJsonTests(unittest.TestCase):
    def test_real_nested_coverage_projects_to_exact_json_without_input_mutation(self):
        import pm_authority_spool as spool
        h, _ = source_parts()
        view = owned_view()
        original = deepcopy(view)
        observed = h["_declared_m5_view_observation"](view)
        self.assertEqual(view, original)
        self.assertEqual(set(observed), {"view_key", "payload_sha256", "membership_sha256", "source_kind",
            "timeframe", "cutoff_utc", "interval", "coverage", "constructor_implementation_sha256",
            "gap_findings_sha256", "cost_status"})
        self.assertEqual(observed["interval"], asdict(view.interval))
        self.assertEqual(observed["coverage"]["examined_intervals"], [asdict(view.interval)])
        self.assertEqual(observed["coverage"]["observed_gaps"], [asdict(view.coverage.observed_gaps[0])])
        self.assertEqual(observed["coverage"]["empty_clock_ordinals"], [0, 7])
        self.assertTrue(all(type(value) is int for value in observed["coverage"]["empty_clock_ordinals"]))
        self.assertIs(type(view.coverage.examined_intervals), tuple)
        self.assertEqual(json.loads(spool._canonical_json_bytes(observed, 1024*1024)), observed)
        # This is the original raw-asdict boundary, not a claimed CASE004 value dump.
        raw = {**observed, "coverage": asdict(view.coverage)}
        self.assertEqual(h["_canonical_bytes"](raw), h["_canonical_bytes"](observed))
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "JSON_VALUE_TYPE_INVALID"):
            spool._canonical_json_bytes(raw, 1024*1024)

    def test_actual_normal_and_technical_publication_statements_pass_strict_spool_boundary(self):
        h, execute_m5 = source_parts()
        for technical in (False, True):
            with self.subTest(technical_source_session=technical):
                sink = StrictJsonSinkUnitDouble()
                source_body = {key: None for key in h["DECLARED_M5_SLOT_SOURCE_SESSION_KEYS"]}
                source_body.update(schema_version=h["DECLARED_M5_SLOT_SOURCE_SESSION_SCHEMA"],
                    source_kind="m5_derived", source_state="SYNTHETIC_DATA_NOT_ISSUED")
                for key in ("qualification_authority", "capital_authority", "live_authority", "order_authority",
                            "unverified_caller_source_or_fit_accepted"):
                    source_body[key] = False
                scope = {**h, "view": owned_view(), "calibration": owned_view(), "source_body": source_body,
                    "body": {"artifact_namespace": "SYNTHETIC_METADATA_NOT_ISSUED"},
                    "sink": sink, "exc": ValueError("SYNTHETIC_METADATA_FAILURE"), "phase": "FIT"}
                statements = publication_statements(execute_m5, technical)
                exec(compile(ast.fix_missing_locations(ast.Module(body=statements, type_ignores=[])),
                    str(SOURCE), "exec"), scope)
                self.assertEqual(len(sink.records), 1)
                artifact_id, artifact_type, encoded = sink.records[0]
                self.assertEqual(artifact_type, "declared_slot_source_session")
                expected_suffix = "/technical-source-session" if technical else "/source-session"
                self.assertTrue(artifact_id.endswith(expected_suffix))
                decoded = json.loads(encoded)
                self.assertEqual(set(decoded), h["DECLARED_M5_SLOT_SOURCE_SESSION_KEYS"])
                for name in ("m5_request_view_observation", "m5_calibration_view_observation"):
                    self.assertEqual(decoded[name]["coverage"]["empty_clock_ordinals"], [0, 7])
                if technical:
                    self.assertEqual(decoded["failure"]["exception_type"], "ValueError")
                    self.assertEqual(decoded["failure"]["failure_code"], "DECLARED_SLOT_SOURCE_FAILURE")
                    self.assertEqual(decoded["failure"]["phase"], "FIT")
                else:
                    self.assertIsNone(decoded["failure"])

    def test_unknown_and_examined_empty_gap_states_remain_distinct(self):
        import pm_authority_spool as spool
        h, _ = source_parts()
        view = owned_view()
        unknown = replace(view, coverage=replace(view.coverage, observed_gaps=None))
        empty = replace(view, coverage=replace(view.coverage, observed_gaps=()))
        unknown_body = h["_declared_m5_view_observation"](unknown)
        empty_body = h["_declared_m5_view_observation"](empty)
        self.assertIsNone(unknown_body["coverage"]["observed_gaps"])
        self.assertEqual(empty_body["coverage"]["observed_gaps"], [])
        self.assertNotEqual(spool._canonical_json_bytes(unknown_body, 1024*1024),
            spool._canonical_json_bytes(empty_body, 1024*1024))

    def test_nonjson_and_nonfinite_values_still_refuse_without_permissive_fallback(self):
        import pm_authority_spool as spool
        h, _ = source_parts()
        view = owned_view()
        invalid = [replace(view, cost_status=object()),
            replace(view, coverage=replace(view.coverage, empty_clock_ordinals=(object(),))),
            replace(view, cost_status=float("nan")), replace(view, cost_status=float("inf"))]
        for number, bad in enumerate(invalid):
            with self.subTest(invalid_metadata=number), self.assertRaises(h["HistoricalSearchProducerError"]) as refused:
                h["_declared_m5_view_observation"](bad)
            self.assertEqual(refused.exception.code, "CANONICAL_JSON_INVALID")
        with self.assertRaisesRegex(spool.AuthoritySpoolError, "JSON_NONFINITE_NUMBER"):
            spool._canonical_json_bytes({"value": float("nan")}, 1024*1024)


if __name__ == "__main__":
    unittest.main()
