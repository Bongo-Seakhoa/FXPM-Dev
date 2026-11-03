"""Owned ordinary CSV mechanics: NO_NATIVE_VERIFICATION_PERFORMED.

These tests call the extracted file reader with plain metadata. They never
construct a registered source, inject a NativeInputs registry, substitute a
native verifier, or certify the real receipt-backed coarse-source entry.
"""

from dataclasses import replace
from datetime import datetime
import csv
import hashlib
import io
import os
from pathlib import Path
import stat
from types import SimpleNamespace
import unittest
from uuid import uuid4

from pm_explicit_derived_history import Interval, PROTECTED_CUTOFF, canonical_json_bytes
from pm_explicit_derived_native import (
    AuthorityBindings, NativeInputs, NativeInputsError, _CsvReadContract, _CsvReadState,
    _clock_observed_bounds, _read_csv_bars,
)


FIELDS = ("time", "_BAR_CLOSE_TS", "Open", "High", "Low", "Close", "Volume", "RealVolume", "Spread")
NO_NATIVE_VERIFICATION_PERFORMED = True


def row(opened, closed, *, spread="4"):
    return [opened, closed, "100", "104", "99", "103", "33", "6", spread]


class UnverifiedClockFixtureReader:
    """Actual hook bodies on ordinary DATA, not an original NativeInputs owner."""
    classification = "NO_NATIVE_VERIFICATION_PERFORMED"
    pins = NativeInputs.pins
    _record = NativeInputs._record
    _read_json = NativeInputs._read_json
    _entry = NativeInputs._entry
    _cell = NativeInputs._cell
    _known = NativeInputs._known
    _project_cell = NativeInputs._project_cell
    _target_spec = NativeInputs._target_spec
    _clock_common_request = NativeInputs._clock_common_request
    _clock_policy = NativeInputs._clock_policy
    target_spec = NativeInputs.target_spec
    m5_source = NativeInputs.m5_source
    project_m5_source = NativeInputs.project_m5_source
    clock_target_metadata = NativeInputs.clock_target_metadata
    clock_source = NativeInputs.clock_source
    clock_catalog = NativeInputs.clock_catalog
    iter_clock_bars = NativeInputs.iter_clock_bars
    iter_bars = NativeInputs.iter_bars
    _iter_owned_bars = NativeInputs._iter_owned_bars

    def __init__(self, data_dir, activation_sha256, pointer_sha256):
        NativeInputs.__init__(self, data_dir, activation_sha256, pointer_sha256)

    def _live(self):
        if self.classification != "NO_NATIVE_VERIFICATION_PERFORMED" or self._closed:
            raise RuntimeError("UNVERIFIED_OWNED_FIXTURE_DATA_NOT_ACTIVE")

    def revalidate_owned_files_only(self):
        return NativeInputs._revalidate_read_pins(self)


def build_unverified_clock_inputs(root: Path) -> UnverifiedClockFixtureReader:
    """Explicitly create tiny owned fixtures; no verifier, scope or seal is made."""
    configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
    if configured is None:
        raise unittest.SkipTest("FXPM_DERIVED_QA_ROOT_NOT_CONFIGURED")
    if not configured:
        raise RuntimeError("FXPM_DERIVED_QA_ROOT_REQUIRED_NO_TEMP_FALLBACK")
    parent, root = Path(configured), Path(root)
    if (not parent.is_absolute() or not parent.is_dir() or not root.is_absolute()
            or not root.is_relative_to(parent) or root == parent or not root.parent.is_dir()):
        raise RuntimeError("UNVERIFIED_CLOCK_FIXTURE_REQUIRES_NEW_CONTAINED_OWNED_ROOT")
    for directory in (root.parent, *root.parent.parents):
        identity = directory.lstat()
        if stat.S_ISLNK(identity.st_mode) or getattr(identity, "st_file_attributes", 0) & 0x400:
            raise RuntimeError("UNVERIFIED_CLOCK_FIXTURE_REPARSE_PATH_REFUSED")
    root.mkdir(exist_ok=False)
    from pm_native_activation import _freeze_json
    from pm_native_request_contract import (
        PRODUCTION_SYMBOL_ORDER, TIMEFRAME_SECONDS, build_v5_leaf_request,
        build_v5_manifest_request, build_v5_request_contract,
    )

    label = "NO_NATIVE_VERIFICATION_PERFORMED"
    with (root / (label + ".txt")).open("x", encoding="utf-8") as handle:
        handle.write("Invented sparse files and ordinary authority-shaped DATA; no original native verification.\n")
    data_dir, native_root = root / "data", root / "data" / "native"
    native_root.mkdir(parents=True, exist_ok=False)
    common = build_v5_request_contract(
        requested_information_cutoff_utc=datetime.fromisoformat("2026-07-14T08:05:00+00:00"))
    manifest_request = build_v5_manifest_request(common, symbols=PRODUCTION_SYMBOL_ORDER)
    environment = {
        "metadata_status": "available",
        "account": {"login": 42, "server": label, "company": label, "currency": "USD"},
        "configured_connection": {"server": label, "terminal_path": "C:/SYNTHETIC_ONLY/terminal.exe",
            "expected_login": 42, "expected_server": label, "expected_currency": "USD"},
        "terminal": {"name": label, "company": label, "path": "C:/SYNTHETIC_ONLY",
            "data_path": "C:/SYNTHETIC_ONLY/Data", "commondata_path": "C:/SYNTHETIC_ONLY/Common",
            "build": 5432, "version": [5, 5432, label], "max_bars": 10000}}
    run = {"schema_version": "fxpm.unverified-clock-fixture-run.v1", "classification": label,
           "request": manifest_request, "source_environment": environment}
    activation_id = "SYNTHETIC_UNVERIFIED_CLOCK_HOOKS_001"
    entries, leaves = {}, {}

    def write_json(path, value):
        raw = canonical_json_bytes(value)
        with path.open("xb") as handle:
            handle.write(raw)
        return hashlib.sha256(raw).hexdigest()

    fine = []
    for bucket, opened in enumerate(("2024-01-01T00:", "2024-01-01T03:")):
        for index, minute in enumerate(("00", "05", "10")):
            base = float(100 + bucket * 10 + index)
            fine.append([opened + minute + ":00+00:00", opened + str(int(minute) + 5).zfill(2) + ":00+00:00",
                         base, base + 2.0, base - 1.0, base + 1.0, 10 + index, 1 + index,
                         (4, 1, 6)[index] if bucket == 0 else (6, 9, 4)[index]])
    fine.append(["2024-01-01T07:55:00+00:00", "2024-01-01T08:00:00+00:00",
                 112.0, 114.0, 111.0, 113.0, 0, 0, 4])
    coarse = [["2024-01-01T00:00:00+00:00", "2024-01-01T03:00:00+00:00",
               100.0, 104.0, 99.0, 103.0, 33, 6, 4],
              ["2024-01-01T03:00:00+00:00", "2024-01-01T08:00:00+00:00",
               110.0, 114.0, 109.0, 113.0, 33, 6, 6]]

    def write_leaf(symbol, timeframe, rows):
        leaf = native_root / "owned-leaves" / symbol / timeframe
        leaf.mkdir(parents=True, exist_ok=False)
        text = io.StringIO(newline="")
        writer = csv.writer(text, lineterminator="\n")
        writer.writerow(FIELDS)
        writer.writerows(rows)
        raw = text.getvalue().encode("utf-8")
        with (leaf / "bars.csv").open("xb") as handle:
            handle.write(raw)
        bars_sha = hashlib.sha256(raw).hexdigest()
        request = build_v5_leaf_request(common, timeframe,
            last_fully_closed_bar_open_utc=datetime.fromisoformat(rows[-1][0]))
        leaves[(symbol, timeframe)] = request
        provenance = {"schema_version": "fxpm.unverified-clock-fixture-provenance.v1",
            "classification": label, "canonical_symbol": symbol, "broker_symbol": symbol,
            "timeframe": timeframe, "timeframe_seconds": TIMEFRAME_SECONDS[timeframe],
            "source_kind": "broker_native", "timezone": "UTC", "timestamp_semantics": "bar_open_utc",
            "bar_close_column": "_BAR_CLOSE_TS", "sha256": bars_sha,
            "artifact": {"relative_path": "bars.csv", "sha256": bars_sha, "bytes": len(raw), "rows": len(rows)},
            "instrument": {"status": "available", "symbol": symbol, "point": 0.25,
                           "digits": 2, "asset_class": "SYNTHETIC_ONLY", "path": "SYNTHETIC/" + symbol},
            "source_environment": environment, "request": request}
        provenance_sha = write_json(leaf / "provenance.json", provenance)
        sidecar = {"schema_version": "fxpm.unverified-clock-fixture-sidecar.v1", "classification": label,
            "activation_id": activation_id, "symbol": symbol, "timeframe": timeframe, "sha256": bars_sha,
            "bar_close_column": "_BAR_CLOSE_TS", "timezone": "UTC", "timestamp_semantics": "bar_open_utc",
            "cell_calendar_boundaries": request["cell_calendar_boundaries"],
            "source_provenance_relative_path": (leaf / "provenance.json").relative_to(native_root).as_posix(),
            "source_provenance_sha256": provenance_sha}
        sidecar_sha = write_json(leaf / "sidecar.json", sidecar)
        return {"symbol": symbol, "timeframe": timeframe, "status": "evidence_candidate", "reason_codes": [],
            "bars_relative_path": (leaf / "bars.csv").relative_to(native_root).as_posix(), "bars_sha256": bars_sha,
            "sidecar_relative_path": (leaf / "sidecar.json").relative_to(native_root).as_posix(),
            "sidecar_sha256": sidecar_sha, "evidence_metadata": {"observed_interval": {
                "start_open_utc": rows[0][0], "end_open_utc": rows[-1][0], "rows": len(rows)},
                "page_gap_diagnostics": {"actual_first_open_utc": rows[0][0],
                    "actual_last_open_utc": rows[-1][0], "actual_last_close_utc": rows[-1][1]}}}

    for symbol in ("AUDCAD", "AUDCHF", "GBPNZD"):
        entries[symbol] = {"M5": write_leaf(symbol, "M5", fine)}
        entries[symbol]["H4"] = (write_leaf(symbol, "H4", coarse) if symbol != "GBPNZD" else
            {"symbol": symbol, "timeframe": "H4", "status": "technical_error",
             "reason_codes": ["SYNTHETIC_TECHNICAL_TARGET_METADATA_ONLY"], "classification": label})
    run_sha = write_json(native_root / "fixture-run.json", run)
    activation_sha = write_json(native_root / "fixture-data-catalog.json",
        {"schema_version": "fxpm.unverified-clock-fixture-catalog.v1", "classification": label, "entries": entries})
    marker_sha = hashlib.sha256(label.encode("ascii")).hexdigest()
    reader = UnverifiedClockFixtureReader(data_dir, activation_sha, marker_sha)
    reader.root, reader.native_root = root, native_root
    reader.target_pair = ("GBPNZD", "H4")
    reader.common_request, reader.manifest_request, reader.leaf_requests = common, manifest_request, leaves
    reader._authority = SimpleNamespace(activation_id=activation_id, activation_manifest_sha256=activation_sha,
        run_manifest=_freeze_json(run), entries=_freeze_json(entries), run_manifest_sha256=run_sha,
        classification=label)
    reader.authority_bindings = AuthorityBindings(activation_id, activation_sha, marker_sha,
                                                 run_sha, marker_sha, marker_sha)
    reader._read_json("fixture-run.json", run_sha)
    reader._read_json("fixture-data-catalog.json", activation_sha)
    return reader


class ExplicitClockNativeIOTests(unittest.TestCase):
    def setUp(self):
        configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if configured is None:
            raise unittest.SkipTest("FXPM_DERIVED_QA_ROOT_NOT_CONFIGURED")
        if not configured:
            raise RuntimeError("FXPM_DERIVED_QA_ROOT_REQUIRED_NO_TEMP_FALLBACK")
        parent = Path(configured)
        if not parent.is_absolute() or not parent.is_dir():
            raise RuntimeError("FXPM_DERIVED_QA_ROOT_MUST_BE_AN_EXISTING_OWNED_ABSOLUTE_DIRECTORY")
        for directory in (parent, *parent.parents):
            identity = directory.lstat()
            if (stat.S_ISLNK(identity.st_mode)
                    or getattr(identity, "st_file_attributes", 0) & 0x400):
                raise RuntimeError("FXPM_DERIVED_QA_ROOT_REPARSE_PATH_REFUSED")
        self.root = parent / ("clock-native-io-" + uuid4().hex)
        self.root.mkdir(exist_ok=False)
        with (self.root / "NO_NATIVE_VERIFICATION_PERFORMED.txt").open("x", encoding="utf-8") as handle:
            handle.write("Invented retained ordinary file-reader inputs only; no native verification.\n")

    def source(self, rows, *, fields=FIELDS, timeframe="H4"):
        text = io.StringIO(newline="")
        writer = csv.writer(text, lineterminator="\n")
        writer.writerow(fields)
        writer.writerows(rows)
        raw = text.getvalue().encode("utf-8")
        path = self.root / ("invented-" + uuid4().hex + ".csv")
        with path.open("xb") as handle:
            handle.write(raw)
        contract = _CsvReadContract(path, "SYNTHETIC_ONLY", timeframe,
            hashlib.sha256(raw).hexdigest(), len(raw), len(rows), PROTECTED_CUTOFF)
        return contract, raw

    def stream(self, contract, interval, *, clock=True):
        state = _CsvReadState()
        values = _read_csv_bars(contract, interval, contract.path.stat(), state,
                                clock_prefix=clock)
        return values, state

    def assert_authenticated(self, state, contract, raw):
        self.assertTrue(state.authenticated)
        self.assertEqual(state.sha256, hashlib.sha256(raw).hexdigest())
        self.assertEqual(state.bytes_read, len(raw))
        self.assertEqual(state.rows, contract.expected_rows)
        self.assertIsNotNone(state.after)

    def test_h4_literal_three_and_five_hour_closes_are_preserved(self):
        rows = [row("2024-01-01T00:00:00+00:00", "2024-01-01T03:00:00+00:00"),
                row("2024-01-01T03:00:00+00:00", "2024-01-01T08:00:00+00:00")]
        contract, raw = self.source(rows)
        values, state = self.stream(contract, Interval(rows[0][0], rows[-1][1]))
        result = tuple(values)
        self.assertEqual([(v.open_utc, v.close_utc) for v in result],
                         [(r[0], r[1]) for r in rows])
        self.assertEqual([v.volume for v in result], [33, 33])
        self.assert_authenticated(state, contract, raw)

    def test_d1_literal_twenty_three_and_twenty_five_hour_closes_are_preserved(self):
        rows = [row("2024-01-01T00:00:00+00:00", "2024-01-01T23:00:00+00:00"),
                row("2024-01-01T23:00:00+00:00", "2024-01-03T00:00:00+00:00")]
        contract, raw = self.source(rows, timeframe="D1")
        values, state = self.stream(contract, Interval(rows[0][0], rows[-1][1]))
        self.assertEqual([(v.open_utc, v.close_utc) for v in values],
                         [(r[0], r[1]) for r in rows])
        self.assert_authenticated(state, contract, raw)

    def test_clock_cross_cutoff_row_skips_prices_but_hashes_the_whole_file(self):
        rows = [row("2024-07-14T04:00:00+00:00", "2024-07-14T08:00:00+00:00"),
                row("2024-07-14T08:00:00+00:00", "2024-07-14T12:00:00+00:00")]
        rows[1][2:] = ["LOCKBOX_VALUES_MUST_NOT_BE_PARSED"] * 7
        contract, raw = self.source(rows)
        values, state = self.stream(contract, Interval(rows[0][0], PROTECTED_CUTOFF))
        result = tuple(values)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].close_utc, rows[0][1])
        self.assert_authenticated(state, contract, raw)

    def test_fixed_reader_retains_its_original_cross_cutoff_straddle_refusal(self):
        rows = [row("2024-07-14T04:00:00+00:00", "2024-07-14T08:00:00+00:00"),
                row("2024-07-14T08:00:00+00:00", "2024-07-14T12:00:00+00:00")]
        contract, raw = self.source(rows)
        values, state = self.stream(contract, Interval(rows[0][0], PROTECTED_CUTOFF), clock=False)
        with self.assertRaises(NativeInputsError) as caught:
            tuple(values)
        self.assertEqual(caught.exception.code, "SOURCE_ROW_STRADDLES_FROZEN_INTERVAL")
        self.assert_authenticated(state, contract, raw)

    def test_frozen_inner_boundary_straddle_is_not_silently_trimmed(self):
        rows = [row("2024-01-01T00:00:00+00:00", "2024-01-01T03:00:00+00:00"),
                row("2024-01-01T03:00:00+00:00", "2024-01-01T08:00:00+00:00")]
        contract, raw = self.source(rows)
        values, state = self.stream(contract,
            Interval("2024-01-01T01:00:00+00:00", rows[-1][1]))
        with self.assertRaises(NativeInputsError) as caught:
            tuple(values)
        self.assertEqual(caught.exception.code, "SOURCE_ROW_STRADDLES_FROZEN_INTERVAL")
        self.assert_authenticated(state, contract, raw)

    def test_abandoned_reader_close_authenticates_remaining_rows(self):
        rows = [row("2024-01-01T00:00:00+00:00", "2024-01-01T03:00:00+00:00"),
                row("2024-01-01T03:00:00+00:00", "2024-01-01T08:00:00+00:00")]
        contract, raw = self.source(rows)
        values, state = self.stream(contract, Interval(rows[0][0], rows[-1][1]))
        self.assertEqual(next(values).open_utc, rows[0][0])
        self.assertFalse(state.authenticated)
        values.close()
        self.assert_authenticated(state, contract, raw)

    def test_duplicate_overlap_and_reverse_rows_refuse_after_eof_authentication(self):
        first = row("2024-01-01T00:00:00+00:00", "2024-01-01T03:00:00+00:00")
        cases = ([first, first],
                 [first, row("2024-01-01T02:00:00+00:00", "2024-01-01T05:00:00+00:00")],
                 [row("2024-01-01T03:00:00+00:00", "2024-01-01T08:00:00+00:00"), first])
        for rows in cases:
            with self.subTest(rows=rows):
                contract, raw = self.source(rows)
                values, state = self.stream(contract,
                    Interval("2024-01-01T00:00:00+00:00", "2024-01-01T08:00:00+00:00"))
                with self.assertRaises(NativeInputsError) as caught:
                    tuple(values)
                self.assertEqual(caught.exception.code, "SOURCE_UTC_AXIS_INVALID")
                self.assert_authenticated(state, contract, raw)

    def test_sha_rows_and_bytes_mismatches_cannot_become_authenticated(self):
        rows = [row("2024-01-01T00:00:00+00:00", "2024-01-01T03:00:00+00:00")]
        original, _ = self.source(rows)
        for contract in (replace(original, sha256="0" * 64),
                         replace(original, expected_rows=2),
                         replace(original, expected_bytes=original.expected_bytes + 1)):
            with self.subTest(contract=contract):
                values, state = self.stream(contract, Interval(rows[0][0], rows[0][1]))
                with self.assertRaises(NativeInputsError) as caught:
                    tuple(values)
                self.assertEqual(caught.exception.code, "SOURCE_DIGEST_ROWS_OR_BYTES_MISMATCH")
                self.assertTrue(caught.exception.abort_attempt)
                self.assertFalse(state.authenticated)

    def test_schema_failure_still_authenticates_all_bytes_and_rows(self):
        rows = [["2024-01-01T00:00:00+00:00", "1"],
                ["2024-01-01T03:00:00+00:00", "2"]]
        contract, raw = self.source(rows, fields=("time", "Open"))
        values, state = self.stream(contract,
            Interval("2024-01-01T00:00:00+00:00", "2024-01-01T08:00:00+00:00"))
        with self.assertRaises(NativeInputsError) as caught:
            tuple(values)
        self.assertEqual(caught.exception.code, "SOURCE_CSV_SCHEMA_UNSUPPORTED")
        self.assert_authenticated(state, contract, raw)

    def test_short_row_failure_still_authenticates_the_whole_file(self):
        rows = [row("2024-01-01T00:00:00+00:00", "2024-01-01T03:00:00+00:00")[:-1]]
        contract, raw = self.source(rows)
        values, state = self.stream(contract,
            Interval("2024-01-01T00:00:00+00:00", "2024-01-01T08:00:00+00:00"))
        with self.assertRaises(NativeInputsError) as caught:
            tuple(values)
        self.assertEqual(caught.exception.code, "SOURCE_CSV_ROW_INVALID")
        self.assert_authenticated(state, contract, raw)

    def test_selected_nonnumeric_price_and_invalid_spread_remain_quality_refusals(self):
        original = row("2024-01-01T00:00:00+00:00", "2024-01-01T03:00:00+00:00")
        for column, value, code in ((2, "NOT_A_NUMBER", "SOURCE_PRICE_PARSE_INVALID"),
                                    (8, "-1", "SOURCE_SPREAD_INVALID")):
            with self.subTest(code=code):
                changed = original.copy()
                changed[column] = value
                contract, raw = self.source([changed])
                values, state = self.stream(contract, Interval(original[0], original[1]))
                with self.assertRaises(NativeInputsError) as caught:
                    tuple(values)
                self.assertEqual(caught.exception.code, code)
                self.assertFalse(caught.exception.abort_attempt)
                self.assert_authenticated(state, contract, raw)

    def test_owned_file_drift_on_abandonment_refuses_and_retains_preimage(self):
        rows = [row("2024-01-01T00:00:00+00:00", "2024-01-01T03:00:00+00:00"),
                row("2024-01-01T03:00:00+00:00", "2024-01-01T08:00:00+00:00")]
        contract, raw = self.source(rows)
        with (self.root / "drift-preimage.csv").open("xb") as handle:
            handle.write(raw)
        values, state = self.stream(contract, Interval(rows[0][0], rows[-1][1]))
        next(values)
        with contract.path.open("ab") as handle:
            handle.write(b"\n")
        with self.assertRaises(NativeInputsError) as caught:
            values.close()
        self.assertEqual(caught.exception.code, "SOURCE_CHANGED_DURING_SCAN")
        self.assertTrue(caught.exception.abort_attempt)
        self.assertFalse(state.authenticated)

    def test_coarse_observed_bounds_use_the_literal_recorded_close(self):
        entry = {"evidence_metadata": {"observed_interval": {
            "start_open_utc": "2024-01-01T00:00:00+00:00",
            "end_open_utc": "2024-01-01T03:00:00+00:00", "rows": 2},
            "page_gap_diagnostics": {"actual_last_close_utc": "2024-01-01T08:00:00+00:00"}}}
        self.assertEqual(_clock_observed_bounds(entry, 2),
                         Interval("2024-01-01T00:00:00+00:00", "2024-01-01T08:00:00+00:00"))

    def test_missing_coarse_close_stays_unknown_instead_of_using_nominal_duration(self):
        entry = {"evidence_metadata": {"observed_interval": {
            "start_open_utc": "2024-01-01T00:00:00+00:00",
            "end_open_utc": "2024-01-01T03:00:00+00:00", "rows": 2}}}
        self.assertIsNone(_clock_observed_bounds(entry, 2))

    def test_inconsistent_observed_row_count_or_close_refuses(self):
        for count, closed in ((3, "2024-01-01T08:00:00+00:00"),
                              (2, "2024-01-01T03:00:00+00:00")):
            with self.subTest(count=count, closed=closed):
                entry = {"evidence_metadata": {"observed_interval": {
                    "start_open_utc": "2024-01-01T00:00:00+00:00",
                    "end_open_utc": "2024-01-01T03:00:00+00:00", "rows": count},
                    "page_gap_diagnostics": {"actual_last_close_utc": closed}}}
                with self.assertRaises(NativeInputsError) as caught:
                    _clock_observed_bounds(entry, 2)
                self.assertEqual(caught.exception.code, "AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE")

    def test_real_metadata_hooks_accept_original_frozen_manifest_shape_and_raw_leaf(self):
        from types import MappingProxyType
        from pm_native_request_contract import COMMON_REQUEST_KEYS, validate_v5_leaf_request

        reader = build_unverified_clock_inputs(self.root / "owned-hook-inputs")
        self.assertNotIsInstance(reader, NativeInputs)
        self.assertIsNone(reader._scope)
        self.assertFalse(hasattr(reader._authority, "verify_receipt_backed_authority"))
        request = reader._authority.run_manifest["request"]
        self.assertIsInstance(request, MappingProxyType)
        self.assertEqual(set(request) - set(COMMON_REQUEST_KEYS),
            {"symbols", "timeframes", "retries", "window_years", "common_calendar_boundaries"})
        policy = reader._clock_policy("H4")
        self.assertEqual(policy["alignment_mode"], "broker_observed_successor_or_nominal_close_v1")
        catalog = reader.clock_catalog("H4")
        self.assertEqual([candidate.native.symbol for candidate in catalog.candidates], ["AUDCAD", "AUDCHF"])
        self.assertFalse(catalog.metadata_refusals)
        cell = reader.clock_source("AUDCAD", "H4")
        raw_leaf = reader.leaf_requests[("AUDCAD", "H4")]
        projected_leaf = validate_v5_leaf_request(raw_leaf, reader.common_request, "H4")
        self.assertNotIn("alignment_mode", projected_leaf)
        self.assertNotIn("successor_sentinel_fetch_slack_seconds", projected_leaf)
        self.assertEqual(raw_leaf["successor_sentinel_fetch_slack_seconds"], 1)
        self.assertEqual(cell.source_identity.request_sha256,
                         hashlib.sha256(canonical_json_bytes(raw_leaf)).hexdigest())
        self.assertNotEqual(cell.source_identity.request_sha256,
                            hashlib.sha256(canonical_json_bytes(projected_leaf)).hexdigest())
        interval = Interval("2024-01-01T00:00:00+00:00", "2024-01-01T08:00:00+00:00")
        self.assertEqual([(value.open_utc, value.close_utc) for value in reader.iter_clock_bars(cell, interval)],
            [("2024-01-01T00:00:00+00:00", "2024-01-01T03:00:00+00:00"),
             ("2024-01-01T03:00:00+00:00", "2024-01-01T08:00:00+00:00")])
        fine = reader.project_m5_source(reader.m5_source("AUDCHF"), interval)
        self.assertEqual(len(tuple(reader.iter_bars(fine, interval))), 7)
        pins = reader.revalidate_owned_files_only()
        self.assertTrue(any(pin.kind == "csv" and pin.path == cell.bars_path and pin.rows == 2 for pin in pins))
        self.assertTrue(any(pin.kind == "csv" and pin.path == fine.bars_path and pin.rows == 7 for pin in pins))
        self.assertFalse(any(pin.kind == "csv" and pin.path == catalog.candidates[0].m5.bars_path for pin in pins))
        self.assertIsNone(reader._scope)

    def test_null_and_malformed_coarse_observation_metadata_refuses_with_typed_error(self):
        cases = (None, {"observed_interval": None}, {"page_gap_diagnostics": None},
                 {"observed_interval": {"start_open_utc": "2024-01-01T00:00:00+00:00"},
                  "page_gap_diagnostics": {"actual_last_close_utc": "2024-01-01T08:00:00+00:00"}})
        for evidence in cases:
            with self.subTest(evidence=evidence):
                with self.assertRaises(NativeInputsError) as caught:
                    _clock_observed_bounds({"evidence_metadata": evidence}, 2)
                self.assertEqual(caught.exception.code, "AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE")

    def test_clock_registration_and_metadata_identity_errors_abort_the_attempt(self):
        for code in ("CLOCK_SOURCE_CELL_NOT_OWNED", "CLOCK_SOURCE_METADATA_CHANGED"):
            with self.subTest(code=code):
                self.assertTrue(NativeInputsError(code).abort_attempt)


if __name__ == "__main__":
    unittest.main()
