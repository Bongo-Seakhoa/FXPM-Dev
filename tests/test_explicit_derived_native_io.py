"""Retained owned IO/planning tests: NO_NATIVE_VERIFICATION_PERFORMED.

Private stub DATA selects ordinary adapter branches. It has no verification
methods, seals or receipts. The real public __enter__ and original native
authority proof are not replaced, exercised successfully or certified here.
"""

from dataclasses import dataclass, replace
from datetime import datetime
import csv
import hashlib
import io
import os
from pathlib import Path
import unittest
from uuid import uuid4

import pm_explicit_derived_native as native
from pm_explicit_derived_history import Interval, PROTECTED_CUTOFF, SourceIdentity, Stratum


NO_NATIVE_VERIFICATION_PERFORMED = True
FIELDS = ("time", "_BAR_CLOSE_TS", "Open", "High", "Low", "Close", "Volume", "RealVolume", "Spread")
ACQUISITION = Interval("2024-07-14T07:00:00+00:00", "2024-07-14T09:00:00+00:00")
READ_INTERVAL = Interval("2024-07-14T07:55:00+00:00", PROTECTED_CUTOFF)
CLASS = "SYNTHETIC_TEST_ONLY_NOT_A_SESSION_CALENDAR"


def digest(value):
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def source_row(opened="2024-07-14T07:55:00+00:00", closed="2024-07-14T08:00:00+00:00"):
    return [opened, closed, "1.0", "2.0", "0.5", "1.5", "7", "3", "4"]


@dataclass(frozen=True)
class _UnverifiedAuthorityData:
    entries: dict
    classification: str = "NO_NATIVE_VERIFICATION_PERFORMED"
    run_manifest: dict | None = None
    run_manifest_sha256: str | None = None


@dataclass(frozen=True)
class _UnverifiedBindingsData:
    classification: str = "NO_NATIVE_VERIFICATION_PERFORMED"


class _UnverifiedPlanningData(native.NativeInputs):
    """Inject owned metadata exceptions without substituting any native verifier."""
    def _cell(self, symbol, timeframe):
        self.synthetic_cell_calls.append((symbol, timeframe))
        if self._authority.entries[symbol][timeframe]["status"] == "technical_error":
            raise AssertionError("TECHNICAL_TARGET_CELL_OR_PROVENANCE_MUST_NEVER_BE_OPENED")
        error = self.synthetic_cell_errors.get((symbol, timeframe))
        if error is not None:
            raise error
        return super()._cell(symbol, timeframe)


class NativeOwnedIOTests(unittest.TestCase):
    def setUp(self):
        configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if configured is None:
            raise unittest.SkipTest("requires an explicitly precreated retained FXPM_DERIVED_QA_ROOT")
        if not configured:
            raise RuntimeError("FXPM_DERIVED_QA_ROOT_REQUIRED_NO_TEMP_FALLBACK")
        parent = Path(configured)
        if not parent.is_absolute() or not parent.is_dir():
            raise RuntimeError("FXPM_DERIVED_QA_ROOT_MUST_BE_AN_EXISTING_OWNED_ABSOLUTE_DIRECTORY")
        self.root = parent / ("native-io-" + uuid4().hex)
        self.root.mkdir(exist_ok=False)
        with (self.root / "NO_NATIVE_VERIFICATION_PERFORMED.txt").open("x", encoding="utf-8") as handle:
            handle.write("Invented retained IO/planning data only. No native verification, receipt or financial run.\n")

    def _inputs(self, planning=False):
        kind = _UnverifiedPlanningData if planning else native.NativeInputs
        value = kind(self.root, digest("SYNTHETIC_ACTIVATION"), digest("SYNTHETIC_POINTER"))
        value.native_root = self.root
        value._scope = object()
        value._authority = _UnverifiedAuthorityData({})
        value.authority_bindings = _UnverifiedBindingsData()
        if planning:
            value.synthetic_cell_errors = {}
            value.synthetic_cell_calls = []
        self.assertFalse(hasattr(value._scope, "verify_receipt_backed_authority"))
        self.assertFalse(hasattr(value._scope, "revalidate_receipt_backed_authority"))
        return value

    def _bind(self, inputs, cell, meta=None):
        inputs._registered[id(cell)] = cell
        inputs._metadata[id(cell)] = meta or {}
        return cell

    def _csv_cell(self, inputs, rows, fields=FIELDS):
        stream = io.StringIO(newline="")
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(fields)
        writer.writerows(rows)
        raw = stream.getvalue().encode("utf-8")
        path = self.root / ("synthetic-" + uuid4().hex + ".csv")
        with path.open("xb") as handle:
            handle.write(raw)
        identity = SourceIdentity("SYNTHETIC_TEST_ONLY", "M5", "NO_NATIVE_VERIFICATION_PERFORMED",
            digest("activation"), hashlib.sha256(raw).hexdigest(), digest("sidecar"),
            digest("provenance"), digest("request-mapping-only"), 0.0001)
        geometry = native.aligned_prefix_geometry("M5", ACQUISITION, PROTECTED_CUTOFF)
        cell = native.SourceCell(identity.symbol, "M5", "SYNTHETIC_UNVERIFIED_DATA", str(path),
            str(self.root / "UNVERIFIED_SIDE_METADATA_NOT_READ"),
            str(self.root / "UNVERIFIED_PROVENANCE_NOT_READ"), identity, READ_INTERVAL,
            Stratum("M5", CLASS, digest("M5-alignment")), geometry,
            len(rows), len(raw), PROTECTED_CUTOFF, digest("synthetic-cell-metadata"))
        meta = {"path": path, "sha256": identity.bars_sha256, "expected_rows": len(rows),
                "expected_bytes": len(raw), "point": identity.point,
                "protected_cutoff": datetime.fromisoformat(PROTECTED_CUTOFF)}
        return self._bind(inputs, cell, meta), raw

    def _assert_authenticated(self, inputs, cell, raw):
        pin = next(pin for pin in inputs.pins if pin.path == cell.bars_path)
        self.assertEqual(pin.sha256, hashlib.sha256(raw).hexdigest())
        self.assertEqual(pin.bytes, len(raw))
        self.assertEqual(pin.rows, cell.expected_rows)
        self.assertFalse(inputs._failed_streams)
        self.assertFalse(inputs._inflight)
        self.assertEqual(inputs._revalidate_read_pins(), inputs.pins)

    def _quality_stop(self, inputs, cell, raw, code):
        with self.assertRaises(native.NativeInputsError) as caught:
            tuple(inputs.iter_bars(cell, cell.interval))
        self.assertEqual(caught.exception.code, code)
        self.assertFalse(caught.exception.abort_attempt)
        self._assert_authenticated(inputs, cell, raw)
        return caught.exception

    def _metadata_cell(self, inputs, symbol, timeframe, status):
        from pm_native_request_contract import FIXED_ALIGNMENT_MODE
        geometry = native.aligned_prefix_geometry(timeframe, ACQUISITION, PROTECTED_CUTOFF)
        alignment = hashlib.sha256(native.canonical_json_bytes(
            {"contract": "V5", "mode": FIXED_ALIGNMENT_MODE, "timeframe": timeframe})).hexdigest()
        identity = SourceIdentity(symbol, timeframe, "NO_NATIVE_VERIFICATION_PERFORMED",
            digest("activation"), digest((symbol, timeframe, "bars")), digest("sidecar"),
            digest("provenance"), digest("request-mapping-only"), 0.0001)
        cell = native.SourceCell(symbol, timeframe, status,
            str(self.root / (symbol + "-" + timeframe + "-NOT_MATERIALIZED.csv")),
            "SYNTHETIC_METADATA_ONLY", "SYNTHETIC_METADATA_ONLY", identity,
            geometry.interval, Stratum(timeframe, CLASS, alignment), geometry,
            1, 1, PROTECTED_CUTOFF, digest((symbol, timeframe, "metadata")),
            geometry.interval)
        inputs._cell_cache[(symbol, timeframe)] = cell
        return self._bind(inputs, cell)

    def _planning(self, controls=("B_CONTROL", "A_CONTROL"), special=None):
        inputs = self._inputs(planning=True)
        entries, roster = {}, []
        for index in range(35):
            symbol = "T" + str(index).zfill(2)
            timeframe = (special or {}).get(index, "H1")
            roster.append((symbol, timeframe))
            entries[symbol] = {
                timeframe: {"symbol": symbol, "timeframe": timeframe, "status": "technical_error",
                            "reason_codes": ["SYNTHETIC_NATIVE_TECHNICAL_ONLY"]},
                "M5": {"symbol": symbol, "timeframe": "M5", "status": "evidence_candidate"}}
            self._metadata_cell(inputs, symbol, "M5", "evidence_candidate")
        for symbol in controls:
            entries[symbol] = {tf: {"symbol": symbol, "timeframe": tf, "status": "evidence_candidate"}
                               for tf in ("H1", "M5")}
            for timeframe in ("H1", "M5"):
                self._metadata_cell(inputs, symbol, timeframe, "evidence_candidate")
        run_request = {
            "timeframe_source_request_intervals": {
                tf: {"start_utc": "2024-07-14T06:00:00+00:00",
                     "end_exclusive_utc": ACQUISITION.end_exclusive_utc}
                for tf in ("M5", "M15", "M30", "H1", "H4", "D1")},
            "timeframe_calendar_boundaries": {
                tf: {"final_lockbox": {"start_ts": PROTECTED_CUTOFF,
                                       "end_ts": ACQUISITION.end_exclusive_utc}}
                for tf in ("M5", "M15", "M30", "H1", "H4", "D1")}}
        inputs._authority = _UnverifiedAuthorityData(entries,
            run_manifest={"request": run_request}, run_manifest_sha256=digest("UNVERIFIED_RUN_METADATA_ONLY"))
        return inputs, tuple(roster)

    def test_cutoff_filters_prices_but_authenticates_entire_file(self):
        inputs = self._inputs()
        future = source_row("2024-07-14T08:05:00+00:00", "2024-07-14T08:10:00+00:00")
        future[2:6] = ["LOCKBOX_PRICE_MUST_NOT_BE_PARSED"] * 4
        cell, raw = self._csv_cell(inputs, [source_row(),
            source_row("2024-07-14T08:00:00+00:00", PROTECTED_CUTOFF), future])
        values = tuple(inputs.iter_bars(cell, cell.interval))
        self.assertEqual(len(values), 2)
        self.assertEqual(values[-1].close_utc, PROTECTED_CUTOFF)
        self._assert_authenticated(inputs, cell, raw)

    def test_abandoned_reader_authenticates_to_eof(self):
        inputs = self._inputs()
        cell, raw = self._csv_cell(inputs, [source_row(),
            source_row("2024-07-14T08:00:00+00:00", PROTECTED_CUTOFF),
            source_row("2024-07-14T08:05:00+00:00", "2024-07-14T08:10:00+00:00")])
        stream = iter(inputs.iter_bars(cell, cell.interval))
        self.assertEqual(next(stream).open_utc, READ_INTERVAL.start_utc)
        stream.close()
        self._assert_authenticated(inputs, cell, raw)

    def test_schema_refusals_are_per_cell_after_complete_authentication(self):
        cases = [FIELDS[1:], ("time", "time", *FIELDS[2:])]
        for fields in cases:
            with self.subTest(fields=fields):
                inputs = self._inputs()
                cell, raw = self._csv_cell(inputs, [source_row()[:len(fields)]], fields)
                self._quality_stop(inputs, cell, raw, "SOURCE_CSV_SCHEMA_UNSUPPORTED")

    def test_non_numeric_price_preserves_cause_and_authentication(self):
        inputs = self._inputs()
        row = source_row()
        row[2] = "SYNTHETIC_NON_NUMERIC_" + "x" * 1000
        cell, raw = self._csv_cell(inputs, [row, source_row("2024-07-14T08:00:00+00:00", PROTECTED_CUTOFF)])
        error = self._quality_stop(inputs, cell, raw, "SOURCE_PRICE_PARSE_INVALID")
        details = native.exception_details(error)
        self.assertEqual(details["cause_type"], "ValueError")
        self.assertLessEqual(len(details["cause_message"].encode("utf-8")), 512)

    def test_short_rows_are_typed_and_authenticated(self):
        inputs = self._inputs()
        cell, raw = self._csv_cell(inputs, [source_row()[:2],
            source_row("2024-07-14T08:00:00+00:00", PROTECTED_CUTOFF)])
        self._quality_stop(inputs, cell, raw, "SOURCE_CSV_ROW_INVALID")

    def test_extra_fields_are_typed_and_authenticated(self):
        inputs = self._inputs()
        cell, raw = self._csv_cell(inputs, [source_row() + ["EXTRA"]])
        self._quality_stop(inputs, cell, raw, "SOURCE_CSV_ROW_INVALID")

    def test_invalid_integral_values_are_per_cell(self):
        for index, code in [(6, "SOURCE_VOLUME_INVALID"), (7, "SOURCE_REAL_VOLUME_INVALID"),
                            (8, "SOURCE_SPREAD_INVALID")]:
            with self.subTest(field=index):
                inputs = self._inputs()
                row = source_row()
                row[index] = "not-an-integral-count"
                cell, raw = self._csv_cell(inputs, [row])
                self._quality_stop(inputs, cell, raw, code)

    def test_out_of_order_axis_is_not_sorted_or_promoted_to_integrity_failure(self):
        inputs = self._inputs()
        cell, raw = self._csv_cell(inputs, [source_row(),
            source_row("2024-07-14T07:50:00+00:00", "2024-07-14T07:55:00+00:00")])
        self._quality_stop(inputs, cell, raw, "SOURCE_UTC_AXIS_INVALID")

    def test_sha_mismatch_aborts_attempt(self):
        inputs = self._inputs()
        cell, _ = self._csv_cell(inputs, [source_row()])
        bad = replace(cell, source_identity=replace(cell.source_identity, bars_sha256="0" * 64))
        self._bind(inputs, bad, inputs._metadata[id(cell)])
        with self.assertRaises(native.NativeInputsError) as caught:
            tuple(inputs.iter_bars(bad, bad.interval))
        self.assertEqual(caught.exception.code, "SOURCE_DIGEST_ROWS_OR_BYTES_MISMATCH")
        self.assertTrue(caught.exception.abort_attempt)
        with self.assertRaises(native.NativeInputsError) as pending:
            inputs._revalidate_read_pins()
        self.assertTrue(pending.exception.abort_attempt)

    def test_row_count_mismatch_aborts_attempt(self):
        inputs = self._inputs()
        cell, _ = self._csv_cell(inputs, [source_row()])
        bad = self._bind(inputs, replace(cell, expected_rows=2))
        with self.assertRaises(native.NativeInputsError) as caught:
            tuple(inputs.iter_bars(bad, bad.interval))
        self.assertEqual(caught.exception.code, "SOURCE_DIGEST_ROWS_OR_BYTES_MISMATCH")
        self.assertTrue(caught.exception.abort_attempt)

    def test_byte_count_mismatch_aborts_before_untrusted_read(self):
        inputs = self._inputs()
        cell, _ = self._csv_cell(inputs, [source_row()])
        bad = self._bind(inputs, replace(cell, expected_bytes=cell.expected_bytes + 1))
        with self.assertRaises(native.NativeInputsError) as caught:
            tuple(inputs.iter_bars(bad, bad.interval))
        self.assertEqual(caught.exception.code, "SOURCE_SIZE_MISMATCH")
        self.assertTrue(caught.exception.abort_attempt)

    def test_final_file_pin_check_refuses_owned_input_drift(self):
        inputs = self._inputs()
        cell, raw = self._csv_cell(inputs, [source_row()])
        tuple(inputs.iter_bars(cell, cell.interval))
        path = Path(cell.bars_path)
        with path.with_name(path.name + ".retained-preimage").open("xb") as handle:
            handle.write(raw)
        with path.open("ab") as handle:
            handle.write(b"\n")
        with self.assertRaises(native.NativeInputsError) as caught:
            inputs._revalidate_read_pins()
        self.assertEqual(caught.exception.code, "NATIVE_INPUT_CHANGED")
        self.assertTrue(caught.exception.abort_attempt)

    def test_original_calibration_reader_uses_only_owned_prefix(self):
        inputs = self._inputs()
        future = source_row("2024-07-14T08:05:00+00:00", "2024-07-14T08:10:00+00:00")
        future[8] = "LOCKBOX_SPREAD_NOT_RETAINED"
        cell, raw = self._csv_cell(inputs, [source_row(),
            source_row("2024-07-14T08:00:00+00:00", PROTECTED_CUTOFF), future])
        values = inputs.calibration_source(cell, PROTECTED_CUTOFF)
        self.assertEqual(values["source_rows"], 3)
        self.assertEqual(values["spreads"].tolist(), [4.0, 4.0])
        self.assertEqual(len(values["opens_ns"]), 2)
        self.assertFalse(values["spreads"].flags.writeable)
        self._assert_authenticated(inputs, cell, raw)

    def test_calibration_format_refusal_authenticates_before_per_cell_stop(self):
        inputs = self._inputs()
        cell, raw = self._csv_cell(inputs, [source_row()[:-1]], FIELDS[:-1])
        with self.assertRaises(native.NativeInputsError) as caught:
            inputs.calibration_source(cell, PROTECTED_CUTOFF)
        self.assertEqual(caught.exception.code, "CALIBRATION_SOURCE_INVALID")
        self.assertFalse(caught.exception.abort_attempt)
        self.assertIsNotNone(caught.exception.__cause__)
        self._assert_authenticated(inputs, cell, raw)

    def test_calibration_integrity_failure_aborts(self):
        inputs = self._inputs()
        cell, _ = self._csv_cell(inputs, [source_row()])
        inputs._metadata[id(cell)]["sha256"] = "0" * 64
        with self.assertRaises(native.NativeInputsError) as caught:
            inputs.calibration_source(cell, PROTECTED_CUTOFF)
        self.assertEqual(caught.exception.code, "CALIBRATION_SOURCE_AUTHENTICATION_FAILED")
        self.assertTrue(caught.exception.abort_attempt)

    def test_plan_requires_exactly_35_targets(self):
        inputs, roster = self._planning()
        with self.assertRaises(native.NativeInputsError) as caught:
            inputs.plan(roster[:-1], PROTECTED_CUTOFF)
        self.assertEqual(caught.exception.code, "EXACT_35_TARGET_ROSTER_REQUIRED")

    def test_plan_keeps_h4_d1_unsupported_without_constructor(self):
        inputs, roster = self._planning(special={0: "H4", 1: "D1"})
        plan = inputs.plan(roster, PROTECTED_CUTOFF)
        self.assertEqual(len(plan.targets), 33)
        self.assertEqual({(row.symbol, row.timeframe, row.reason_code) for row in plan.stops},
                         {("T00", "H4", "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR"),
                          ("T01", "D1", "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR")})
        for row in plan.stops:
            self.assertIsNone(row.extent)
            self.assertEqual(row.target_spec["native_status"], "technical_error")
            self.assertEqual(row.target_spec["native_reason_codes"], ["SYNTHETIC_NATIVE_TECHNICAL_ONLY"])
            self.assertIsNone(row.target_spec["point"])
            self.assertIsNone(row.target_spec["instrument_session_class"])
            self.assertEqual(row.target_spec["source_identity_from"], "UNRESOLVED_UNSUPPORTED_ALIGNMENT")
            self.assertEqual(row.readiness_label, "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY")
            self.assertFalse(row.full_native_context_eligibility)
            self.assertFalse(row.financial_consumer_ready)

    def test_plan_no_available_whole_bar_stops_only_affected_target(self):
        inputs, roster = self._planning()
        original = inputs._cell_cache[("T00", "M5")]
        inputs._cell_cache[("T00", "M5")] = self._bind(inputs, replace(original,
            observed_interval=Interval("2024-07-14T08:00:00+00:00", PROTECTED_CUTOFF)))
        plan = inputs.plan(roster, PROTECTED_CUTOFF)
        self.assertEqual(len(plan.targets), 34)
        self.assertEqual([(row.symbol, row.reason_code) for row in plan.stops],
                         [("T00", "NO_AVAILABLE_WHOLE_BARS")])

    def test_plan_chooses_lexical_control_before_results(self):
        inputs, roster = self._planning()
        plan = inputs.plan(roster, PROTECTED_CUTOFF)
        self.assertEqual(len(plan.targets), 35)
        self.assertEqual([row.native.symbol for row in plan.controls], ["A_CONTROL"])
        self.assertFalse(plan.stops)
        self.assertFalse(plan.qualification_authority)
        self.assertFalse(plan.capital_authority)
        self.assertFalse(plan.live_authority)
        self.assertFalse(plan.order_authority)

    def test_plan_freezes_available_extent_without_opening_technical_target(self):
        inputs, roster = self._planning()
        plan = inputs.plan(roster, PROTECTED_CUTOFF)
        target = next(row for row in plan.targets if row.symbol == "T00")
        required = {"start_utc": "2024-07-14T06:00:00+00:00",
                    "end_exclusive_utc": "2024-07-14T08:00:00+00:00"}
        available = {"start_utc": ACQUISITION.start_utc,
                     "end_exclusive_utc": "2024-07-14T08:00:00+00:00"}
        self.assertEqual(target.extent, {
            "required_interval": required,
            "m5_acquisition_interval": {"start_utc": ACQUISITION.start_utc,
                                        "end_exclusive_utc": ACQUISITION.end_exclusive_utc},
            "m5_observed_interval": {"start_utc": ACQUISITION.start_utc,
                                     "end_exclusive_utc": PROTECTED_CUTOFF},
            "available_interval": available,
            "omitted_intervals": [{"start_utc": required["start_utc"],
                                    "end_exclusive_utc": available["start_utc"]}]})
        self.assertEqual(target.interval, Interval(**available))
        self.assertEqual(target.m5.interval, target.interval)
        self.assertEqual(target.m5.geometry.interval,
                         Interval(ACQUISITION.start_utc, PROTECTED_CUTOFF))
        self.assertEqual(target.target_spec["native_status"], "technical_error")
        self.assertEqual(target.target_spec["native_reason_codes"], ["SYNTHETIC_NATIVE_TECHNICAL_ONLY"])
        self.assertEqual(target.target_spec["point"], target.m5.source_identity.point)
        self.assertEqual(target.target_spec["instrument_session_class"], CLASS)
        self.assertEqual(target.target_spec["source_identity_from"], "ACCEPTED_SAME_SYMBOL_M5")
        self.assertFalse(any((symbol, timeframe) in roster
                             for symbol, timeframe in inputs.synthetic_cell_calls))
        for value in (target, plan):
            self.assertEqual(value.data_scope, "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY")
            self.assertEqual(value.readiness_label, value.data_scope)
            self.assertFalse(value.full_native_context_eligibility)
            self.assertFalse(value.financial_consumer_ready)
        self.assertEqual(plan.scope, "ONE_AVAILABLE_HISTORY_M5_DERIVED_DATA_ONLY_ATTEMPT")
        self.assertEqual(plan.schema_version, "fxpm.explicit-derived-plan.v2")
        encoded = native.canonical_json_bytes(plan.to_dict())
        self.assertIn(b'"omitted_intervals"', encoded)

    def test_plan_frozen_authority_data_matches_plain_target_specification(self):
        from pm_native_activation import _freeze_json
        from types import MappingProxyType

        plain_inputs, roster = self._planning()
        plain = plain_inputs.plan(roster, PROTECTED_CUTOFF)
        frozen_inputs, frozen_roster = self._planning()
        authority = frozen_inputs._authority
        frozen_inputs._authority = replace(authority,
            entries=_freeze_json(authority.entries),
            run_manifest=_freeze_json(authority.run_manifest))
        self.assertIsInstance(frozen_inputs._authority.entries, MappingProxyType)
        frozen_reasons = frozen_inputs._authority.entries["T00"]["H1"]["reason_codes"]
        self.assertIsInstance(frozen_reasons, list)
        self.assertIsNot(type(frozen_reasons), list)
        frozen = frozen_inputs.plan(frozen_roster, PROTECTED_CUTOFF)
        self.assertEqual(len(plain.targets), 35)
        self.assertEqual(len(frozen.targets), 35)
        self.assertFalse(frozen.stops)
        plain_specs = [row.target_spec for row in plain.targets]
        frozen_specs = [row.target_spec for row in frozen.targets]
        self.assertEqual(frozen_specs, plain_specs)
        self.assertEqual(native.canonical_json_bytes(frozen_specs),
                         native.canonical_json_bytes(plain_specs))
        self.assertFalse(any((symbol, timeframe) in frozen_roster
                             for symbol, timeframe in frozen_inputs.synthetic_cell_calls))

    def test_control_freezes_own_observed_span_and_projects_both_sources(self):
        inputs, roster = self._planning()
        original = inputs._cell_cache[("A_CONTROL", "H1")]
        acquisition = Interval("2024-07-14T06:00:00+00:00", ACQUISITION.end_exclusive_utc)
        geometry = native.aligned_prefix_geometry("H1", acquisition, PROTECTED_CUTOFF)
        replacement = replace(original, interval=geometry.interval, geometry=geometry,
                              observed_interval=geometry.interval)
        inputs._cell_cache[("A_CONTROL", "H1")] = self._bind(inputs, replacement)
        plan = inputs.plan(roster, PROTECTED_CUTOFF)
        control = plan.controls[0]
        self.assertEqual(control.native.symbol, "A_CONTROL")
        self.assertEqual(control.extent["required_interval"], {
            "start_utc": "2024-07-14T06:00:00+00:00",
            "end_exclusive_utc": "2024-07-14T08:00:00+00:00"})
        self.assertEqual(control.extent["available_interval"], {
            "start_utc": ACQUISITION.start_utc,
            "end_exclusive_utc": "2024-07-14T08:00:00+00:00"})
        self.assertEqual(control.extent["omitted_intervals"], [{
            "start_utc": "2024-07-14T06:00:00+00:00",
            "end_exclusive_utc": ACQUISITION.start_utc}])
        self.assertEqual(control.native.interval, control.interval)
        self.assertEqual(control.m5.interval, control.interval)
        self.assertEqual(control.native.geometry.interval, geometry.interval)
        self.assertEqual(control.m5.geometry.interval,
                         Interval(ACQUISITION.start_utc, PROTECTED_CUTOFF))
        self.assertEqual(control.readiness_label, "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY")
        self.assertFalse(control.full_native_context_eligibility)
        self.assertFalse(control.financial_consumer_ready)

    def test_missing_authenticated_observed_bounds_stops_only_affected_target(self):
        inputs, roster = self._planning()
        original = inputs._cell_cache[("T00", "M5")]
        inputs._cell_cache[("T00", "M5")] = self._bind(inputs, replace(original, observed_interval=None))
        plan = inputs.plan(roster, PROTECTED_CUTOFF)
        self.assertEqual(len(plan.targets), 34)
        stop = next(row for row in plan.stops if row.symbol == "T00")
        self.assertEqual(stop.reason_code, "AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE")
        self.assertIsNone(stop.extent)
        self.assertEqual(stop.target_spec["source_identity_from"], "UNRESOLVED_ACCEPTED_M5_METADATA")

    def test_planning_target_oserror_aborts_before_later_targets(self):
        inputs, roster = self._planning()
        inputs.synthetic_cell_errors[("T00", "M5")] = FileNotFoundError("SYNTHETIC_MISSING_M5_METADATA")
        with self.assertRaises(native.NativeInputsError) as caught:
            inputs.plan(roster, PROTECTED_CUTOFF)
        self.assertEqual(caught.exception.code, "PLANNING_IO_ABORT")
        self.assertTrue(caught.exception.abort_attempt)
        self.assertIsInstance(caught.exception.__cause__, FileNotFoundError)
        self.assertNotIn(("T01", "M5"), inputs.synthetic_cell_calls)

    def test_planning_control_oserror_aborts_without_lexical_fallback(self):
        inputs, roster = self._planning()
        inputs.synthetic_cell_errors[("A_CONTROL", "H1")] = OSError("SYNTHETIC_CONTROL_IO_FAILURE")
        with self.assertRaises(native.NativeInputsError) as caught:
            inputs.plan(roster, PROTECTED_CUTOFF)
        self.assertEqual(caught.exception.code, "PLANNING_IO_ABORT")
        self.assertTrue(caught.exception.abort_attempt)
        self.assertIsInstance(caught.exception.__cause__, OSError)
        self.assertNotIn(("B_CONTROL", "H1"), inputs.synthetic_cell_calls)

    def test_authenticated_observed_end_is_last_m5_open_plus_300_seconds(self):
        entry = {"evidence_metadata": {"observed_interval": {
            "start_open_utc": "2024-07-14T07:00:00+00:00",
            "end_open_utc": "2024-07-14T08:00:00+00:00", "rows": 2},
            "page_gap_diagnostics": {"actual_first_open_utc": "2024-07-14T07:00:00+00:00",
                                     "actual_last_open_utc": "2024-07-14T08:00:00+00:00",
                                     "actual_last_close_utc": PROTECTED_CUTOFF}}}
        self.assertEqual(native._observed_interval(entry, "M5", 2),
                         Interval("2024-07-14T07:00:00+00:00", PROTECTED_CUTOFF))
        with self.assertRaises(native.NativeInputsError) as caught:
            native._observed_interval(entry, "M5", 3)
        self.assertEqual(caught.exception.code, "AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE")
        with self.assertRaises(native.NativeInputsError) as caught:
            native._observed_interval({"evidence_metadata": {}}, "M5", 2)
        self.assertEqual(caught.exception.code, "AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE")

    def test_no_control_stops_entire_affected_stratum(self):
        inputs, roster = self._planning(controls=())
        plan = inputs.plan(roster, PROTECTED_CUTOFF)
        self.assertFalse(plan.targets)
        self.assertFalse(plan.controls)
        self.assertEqual(len(plan.stops), 35)
        self.assertEqual({row.reason_code for row in plan.stops}, {"CONTROL_STRATUM_UNSUPPORTED"})
        for row in plan.stops:
            self.assertIsNotNone(row.target_spec)
            self.assertIsNotNone(row.extent)
            self.assertEqual(row.readiness_label, "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY")
            self.assertFalse(row.full_native_context_eligibility)
            self.assertFalse(row.financial_consumer_ready)

    def test_plan_preserves_bounded_metadata_exception_details(self):
        inputs, roster = self._planning()
        inputs.synthetic_cell_errors[("T00", "M5")] = ValueError(
            "SIDECAR_IDENTITY_MISMATCH:SYNTHETIC_TEST_ONLY:" + "x" * 1000)
        plan = inputs.plan(roster, PROTECTED_CUTOFF)
        stop = next(row for row in plan.stops if row.symbol == "T00")
        self.assertEqual(stop.error_type, "ValueError")
        self.assertTrue(stop.error_message.startswith("SIDECAR_IDENTITY_MISMATCH"))
        self.assertLessEqual(len(stop.error_message.encode("utf-8")), 512)

    def test_control_catalog_preserves_key_error_name(self):
        inputs, roster = self._planning()
        inputs.synthetic_cell_errors[("A_CONTROL", "H1")] = KeyError("SYNTHETIC_REQUIRED_FIELD")
        plan = inputs.plan(roster, PROTECTED_CUTOFF)
        self.assertEqual(plan.controls[0].native.symbol, "B_CONTROL")
        diagnostic = plan.control_catalog_diagnostics[0]
        self.assertEqual(diagnostic.error_type, "KeyError")
        self.assertIn("SYNTHETIC_REQUIRED_FIELD", diagnostic.error_message)

    def test_plan_does_not_swallow_integrity_failure(self):
        inputs, roster = self._planning()
        inputs.synthetic_cell_errors[("T00", "M5")] = native.NativeInputsError(
            "NATIVE_METADATA_IDENTITY_MISMATCH", detail="SYNTHETIC_TEST_ONLY", abort_attempt=True)
        with self.assertRaises(native.NativeInputsError) as caught:
            inputs.plan(roster, PROTECTED_CUTOFF)
        self.assertTrue(caught.exception.abort_attempt)

    def test_error_metadata_is_bounded_without_losing_exception_type(self):
        details = native.exception_details(KeyError("SYNTHETIC:" + "\u03b1" * 2000))
        self.assertEqual(details["error_type"], "KeyError")
        self.assertIn("SYNTHETIC", details["error_message"])
        self.assertLessEqual(len(details["error_message"].encode("utf-8")), 512)

    def test_invalid_expected_identity_refuses_before_native_entry(self):
        with self.assertRaises(native.NativeInputsError) as caught:
            native.NativeInputs(self.root, "not-a-native-identity", digest("pointer"))
        self.assertEqual(caught.exception.code, "EXPECTED_NATIVE_IDENTITIES_INVALID")


if __name__ == "__main__":
    unittest.main()
