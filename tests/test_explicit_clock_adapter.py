"""SYNTHETIC_PROTOCOL_ONLY_NO_NATIVE_VERIFICATION.

Private protocol seams use ordinary invented metadata/readers and a publisher
spy. They prove no original native scope, EOF, audit, receipt or live entry.
Public entry refusal checks keep these fixtures outside that authority path.
"""
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import stat
import unittest
import uuid

from pm_explicit_derived_history import Bar, Interval, SourceIdentity, Stratum
from pm_explicit_derived_native import NativeInputsError
from pm_explicit_clock_adapter import (
    ClockAdapterError, PROTECTED_CUTOFF, MAX_CLOCK_TARGETS, R3_RESTRICTION, REQUEST_SEMANTICS,
    prepare_frozen_clock_plan, populate_frozen_clock_plan,
    _prepare_protocol, _populate_protocol, _checked_eof_roles,
)


_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LABEL = "SYNTHETIC_PROTOCOL_ONLY_NO_NATIVE_VERIFICATION"
_CLASS = "SYNTHETIC_CATALOG_CLASS_NOT_CALENDAR_PROOF"
_INTERVAL = Interval(_START.isoformat(), (_START + timedelta(minutes=480)).isoformat())


def _stamp(minutes):
    return (_START + timedelta(minutes=minutes)).isoformat()


def _identity(symbol, timeframe, point=0.25):
    def digest(suffix):
        return hashlib.sha256((symbol + timeframe + suffix).encode("ascii")).hexdigest()
    return SourceIdentity(symbol, timeframe, _LABEL, "1" * 64,
        digest("bars"), digest("sidecar"), digest("provenance"), digest("request"), point)


@dataclass(frozen=True)
class _Geometry:
    acquisition_interval: Interval = _INTERVAL


@dataclass(frozen=True)
class _FineMetadata:
    symbol: str
    source_identity: SourceIdentity
    timeframe: str = "M5"
    native_status: str = "evidence_candidate"
    interval: Interval = _INTERVAL
    geometry: _Geometry = _Geometry()
    observed_interval: Interval | None = _INTERVAL
    stratum: Stratum = Stratum("M5", _CLASS, "SYNTHETIC_FIXED_M5_NOT_BROKER_CLOCK")
    bars_path: str = ""
    expected_rows: int = 6
    expected_bytes: int = 3456
    metadata_digest: str = "6" * 64


@dataclass(frozen=True)
class _ClockMetadata:
    symbol: str
    timeframe: str
    source_identity: SourceIdentity
    native_status: str = "evidence_candidate"
    acquisition_interval: Interval = _INTERVAL
    prefix_interval: Interval = _INTERVAL
    observed_interval: Interval | None = None
    broker_source_sha256: str = "2" * 64
    broker_policy_sha256: str = "3" * 64
    instrument_session_class: str = _CLASS
    alignment_mode: str = "broker_observed_successor_or_nominal_close_v1"
    bars_path: str = ""
    expected_rows: int = 2
    expected_bytes: int = 1234
    metadata_digest: str = "7" * 64


@dataclass(frozen=True)
class _Candidate:
    native: _ClockMetadata
    m5: _FineMetadata


@dataclass(frozen=True)
class _Status:
    symbol: str
    native_status: str = "evidence_candidate"
    m5_status: str = "evidence_candidate"
    eligible: bool = True
    reason_code: str | None = None


@dataclass(frozen=True)
class _Catalog:
    timeframe: str
    candidates: tuple
    metadata_refusals: tuple = ()
    status_roster: tuple = ()


@dataclass(frozen=True)
class _PlainBindings:
    activation_id: str = _LABEL
    activation_sha256: str = "1" * 64


@dataclass(frozen=True)
class _PlainPin:
    path: str
    sha256: str
    bytes: int
    stat_identity: tuple = (1, 2, 3, 4)
    kind: str = "metadata"
    rows: int | None = None


def _fine(symbol, *, point=0.25):
    return _FineMetadata(symbol, _identity(symbol, "M5", point),
        bars_path="C:/SYNTHETIC_PROTOCOL_ONLY/" + symbol + "-M5.csv")


def _clock(symbol, timeframe="H4", *, point=0.25):
    return _ClockMetadata(symbol, timeframe, _identity(symbol, timeframe, point),
        bars_path="C:/SYNTHETIC_PROTOCOL_ONLY/" + symbol + "-" + timeframe + ".csv")


def _fine_bars():
    values = []
    for bucket, opened in enumerate((0, 180)):
        for index, spread in enumerate((4, 1, 6) if bucket == 0 else (6, 9, 4)):
            base = 100 + bucket * 10 + index
            values.append(Bar(_stamp(opened + index * 5), _stamp(opened + (index + 1) * 5),
                base, base + 2, base - 1, base + 1, 10 + index, 1 + index, spread))
    return tuple(values)


def _native_bars():
    return (Bar(_stamp(0), _stamp(180), 100, 104, 99, 103, 33, 6, 4),
            Bar(_stamp(180), _stamp(480), 110, 114, 109, 113, 33, 6, 6))


class _OrdinaryProtocolReader:
    """Not NativeInputs; no verifier/registry/native authority is supplied."""
    def __init__(self, *, events=None):
        self.events = [] if events is None else events
        self.authority_bindings = _PlainBindings()
        self.pins = (_PlainPin("SYNTHETIC-metadata.json", "8" * 64, 10),)
        self.fine = {symbol: _fine(symbol) for symbol in
                     ("A_CLOCK", "B_CONTROL", "C_SPARE", "T_TARGET", "U_TARGET")}
        self.controls = {symbol: _clock(symbol) for symbol in ("A_CLOCK", "B_CONTROL", "C_SPARE")}
        self.target_starts = {}
        self.policy = "3" * 64
        self.failures = {}
        self.bad_d = False
        self.projection_error = None
        self.empty_symbols = set()
        self.csv_during_metadata = False

    def clock_catalog(self, timeframe):
        self.events.append(("catalog_metadata", timeframe))
        return _Catalog(timeframe, tuple(_Candidate(replace(cell, timeframe=timeframe,
            source_identity=_identity(symbol, timeframe, cell.source_identity.point)), self.fine[symbol])
            for symbol, cell in sorted(self.controls.items())),
            status_roster=tuple(_Status(symbol) for symbol in sorted(self.controls)))

    def clock_target_metadata(self, symbol, timeframe):
        self.events.append(("technical_target_metadata_only", symbol, timeframe))
        start = self.target_starts.get(symbol, -60)
        return {"target_spec": {"symbol": symbol, "timeframe": timeframe,
            "native_status": "technical_error", "native_reason_codes": ["SYNTHETIC_TARGET_UNAVAILABLE"],
            "native_entry_sha256": "9" * 64, "run_manifest_sha256": "a" * 64,
            "run_request_sha256": "b" * 64,
            "target_source_request_interval": asdict(Interval(_stamp(start), _stamp(480))),
            "target_calendar_boundaries": {"final_lockbox": {"start_ts": PROTECTED_CUTOFF}},
            "point": None, "instrument_session_class": None, "source_identity_from": "UNRESOLVED"},
            "timeframe": timeframe, "broker_source_sha256": "2" * 64,
            "broker_policy_sha256": self.policy,
            "alignment_mode": "broker_observed_successor_or_nominal_close_v1"}

    def m5_source(self, symbol):
        self.events.append(("m5_metadata", symbol))
        if self.csv_during_metadata:
            self.pins += (_PlainPin("SYNTHETIC-prior.csv", "d" * 64, 123, kind="csv", rows=1),)
        return self.fine[symbol]

    def project_m5_source(self, cell, interval):
        self.events.append(("m5_projection", cell.symbol, interval.start_utc))
        if self.projection_error is not None:
            raise self.projection_error
        return replace(cell, interval=interval)

    def iter_clock_bars(self, cell, interval):
        self.events.append(("clock_values", cell.symbol, interval.start_utc))
        if cell.symbol not in self.controls:
            raise AssertionError("technical target coarse values must never be read")
        rows = _native_bars()
        if self.bad_d and cell.symbol == "B_CONTROL":
            rows = (replace(rows[0], open=100.0), rows[1])
        for value in rows:
            if _stamp(0) <= value.open_utc and value.open_utc >= interval.start_utc and value.close_utc <= interval.end_exclusive_utc:
                yield value

    def iter_bars(self, cell, interval):
        self.events.append(("m5_values", cell.symbol, interval.start_utc))
        if cell.symbol == "A_CLOCK":
            raise AssertionError("C M5 is eligibility metadata only")
        error = self.failures.get(cell.symbol)
        if error is not None:
            raise error
        if cell.symbol in self.empty_symbols:
            return
        for value in _fine_bars():
            if value.open_utc >= interval.start_utc and value.close_utc <= interval.end_exclusive_utc:
                yield value


class _OrdinaryPublisherSpy:
    """Plain observer; no spool, receipt, seal, verification or native proof."""
    def __init__(self, plan, events):
        self.plan_sha256 = plan.sha256
        self.events = events
        self.templates = []
        self.controls = []
        self.series = []
        self.stops = []

    def freeze_template(self, stratum_id, template, runtime_context):
        self.events.append(("template_persist_protocol", stratum_id))
        self.templates.append((template, runtime_context))

    def control_compared(self, **value):
        self.events.append(("control_compared_protocol", value["stratum_id"]))
        self.controls.append(value)

    def add_series(self, series, identification, *, statistics):
        if statistics is not None:
            raise AssertionError("initial clock edition is PRICE_ONLY")
        self.series.append(series)

    def stop(self, symbol, timeframe, reason_code, *, details):
        self.stops.append((symbol, timeframe, reason_code, details))


class ExplicitClockAdapterProtocolTests(unittest.TestCase):
    def prepare_protocol(self, inputs=None, targets=(("T_TARGET", "H4"),)):
        inputs = _OrdinaryProtocolReader() if inputs is None else inputs
        plan = _prepare_protocol(inputs, targets, cutoff_utc=PROTECTED_CUTOFF)
        return inputs, plan, _OrdinaryPublisherSpy(plan, inputs.events)

    def test_metadata_plan_freezes_complete_ranked_roster_without_any_value_stream(self):
        inputs, plan, _ = self.prepare_protocol()
        payload = plan.to_dict()
        self.assertFalse(any(event[0] in ("clock_values", "m5_values") for event in inputs.events))
        row = payload["strata"][0]
        self.assertEqual([item["symbol"] for item in row["eligible_roster"]], ["A_CLOCK", "B_CONTROL", "C_SPARE"])
        self.assertIsNone(row["template"])
        self.assertEqual(row["c_native"]["symbol"], "A_CLOCK")
        self.assertEqual(row["d_native"]["symbol"], "B_CONTROL")
        self.assertEqual(payload["r3_restriction"], R3_RESTRICTION)
        self.assertEqual(payload["requested_interval_semantics"], REQUEST_SEMANTICS)
        self.assertIs(payload["full_native_context_eligibility"], False)
        self.assertIs(payload["financial_consumer_ready"], False)
        self.assertTrue(all(item["native_metadata_sha256"] and item["m5_metadata_sha256"] for item in row["eligible_roster"]))

    def test_public_entries_reject_plain_protocol_fixture_before_any_metadata_or_value_access(self):
        inputs = _OrdinaryProtocolReader()
        with self.assertRaises(ClockAdapterError) as caught:
            prepare_frozen_clock_plan(inputs, (("T_TARGET", "H4"),))
        self.assertEqual(caught.exception.code, "CLOCK_LIVE_NATIVE_INPUTS_REQUIRED")
        with self.assertRaises(ClockAdapterError):
            populate_frozen_clock_plan(object(), object(), inputs)
        self.assertEqual(inputs.events, [])

    def test_declared_target_symbols_are_excluded_before_fixed_c_d_selection(self):
        _, plan, _ = self.prepare_protocol(targets=(("A_CLOCK", "H4"), ("T_TARGET", "H4")))
        row = plan.to_dict()["strata"][0]
        self.assertEqual(row["target_symbols_excluded"], ["A_CLOCK", "T_TARGET"])
        self.assertEqual([item["symbol"] for item in row["eligible_roster"]], ["B_CONTROL", "C_SPARE"])
        self.assertEqual((row["c_native"]["symbol"], row["d_native"]["symbol"]), ("B_CONTROL", "C_SPARE"))

    def test_r3_global_point_restriction_is_new_explicit_metadata_grouping(self):
        inputs = _OrdinaryProtocolReader()
        inputs.controls["A_CLOCK"] = _clock("A_CLOCK", point=0.5)
        inputs.fine["A_CLOCK"] = _fine("A_CLOCK", point=0.5)
        _, plan, _ = self.prepare_protocol(inputs)
        row = plan.to_dict()["strata"][0]
        self.assertEqual(row["common_point"], 0.25)
        self.assertEqual([item["symbol"] for item in row["eligible_roster"]], ["B_CONTROL", "C_SPARE"])
        self.assertNotEqual(row["coarse_alignment_mode"], row["d_m5"]["stratum"]["alignment_identity"])

    def test_native_m5_class_or_pair_point_mismatch_is_metadata_refusal(self):
        inputs = _OrdinaryProtocolReader()
        inputs.fine["B_CONTROL"] = replace(inputs.fine["B_CONTROL"],
            stratum=Stratum("M5", "SYNTHETIC_DIFFERENT_CLASS", "SYNTHETIC_FIXED_M5"))
        _, plan, _ = self.prepare_protocol(inputs)
        self.assertEqual([row["symbol"] for row in plan.to_dict()["strata"][0]["eligible_roster"]], ["A_CLOCK", "C_SPARE"])
        self.assertTrue(any(row.get("reason_code") == "CLOCK_CONTROL_NATIVE_M5_METADATA_MISMATCH"
                            for row in plan.to_dict()["catalog_diagnostics"]))

    def test_coarse_broker_policy_mismatch_stops_without_m5_alignment_inference(self):
        inputs = _OrdinaryProtocolReader()
        inputs.policy = "c" * 64
        _, plan, _ = self.prepare_protocol(inputs)
        payload = plan.to_dict()
        self.assertEqual(payload["strata"], [])
        self.assertEqual(payload["stops"][0]["reason_code"], "CLOCK_DISTINCT_CONTROLS_UNAVAILABLE")
        self.assertEqual(payload["unavailable_strata"][0]["eligible_roster"], [])

    def test_one_common_metadata_interval_then_literal_c_clipping_precedes_d_values(self):
        inputs = _OrdinaryProtocolReader()
        inputs.target_starts["U_TARGET"] = 120
        inputs, plan, publisher = self.prepare_protocol(inputs,
            (("T_TARGET", "H4"), ("U_TARGET", "H4")))
        before = plan.payload_json
        roles = _populate_protocol(publisher, plan, inputs)
        self.assertEqual(plan.payload_json, before)
        self.assertEqual(plan.to_dict()["strata"][0]["metadata_common_interval"]["start_utc"], _stamp(120))
        template, context = publisher.templates[0]
        self.assertEqual(template.interval.start_utc, _stamp(180))
        self.assertEqual(context["control_extent"]["required_interval"]["start_utc"], _stamp(120))
        self.assertEqual([row["extent"]["available_interval"]["start_utc"] for row in context["target_extents"]], [_stamp(180)] * 2)
        freeze_index = next(i for i, event in enumerate(inputs.events) if event[0] == "template_persist_protocol")
        d_index = next(i for i, event in enumerate(inputs.events) if event[:2] == ("m5_values", "B_CONTROL"))
        self.assertLess(freeze_index, d_index)
        self.assertIn(("clock_values", "A_CLOCK", _stamp(0)), inputs.events)
        self.assertEqual(len(publisher.series), 2)
        self.assertFalse(any(row["role"] == "c_m5" for row in roles))

    def test_d_fidelity_failure_stops_group_without_spare_control_or_target_read(self):
        inputs = _OrdinaryProtocolReader()
        inputs.bad_d = True
        inputs, plan, publisher = self.prepare_protocol(inputs,
            (("T_TARGET", "H4"), ("U_TARGET", "H4")))
        _populate_protocol(publisher, plan, inputs)
        self.assertEqual([row[2] for row in publisher.stops], ["CONTROL_PRICE_FIDELITY_MISMATCH"] * 2)
        self.assertEqual(publisher.controls, [])
        self.assertEqual(publisher.series, [])
        self.assertFalse(any(event[:2] == ("clock_values", "C_SPARE") or
            event[:2] in (("m5_values", "T_TARGET"), ("m5_values", "U_TARGET")) for event in inputs.events))

    def test_projection_refusal_retains_c_template_and_extents_before_d_values(self):
        inputs, plan, publisher = self.prepare_protocol()
        inputs.projection_error = NativeInputsError("SYNTHETIC_M5_PROJECTION_REFUSAL", abort_attempt=False)
        _populate_protocol(publisher, plan, inputs)
        self.assertEqual(len(publisher.templates), 1)
        self.assertEqual(publisher.templates[0][1]["target_extents"][0]["symbol"], "T_TARGET")
        self.assertEqual(publisher.stops[0][2], "SYNTHETIC_M5_PROJECTION_REFUSAL")
        self.assertFalse(any(event[:2] == ("m5_values", "B_CONTROL") for event in inputs.events))

    def test_no_whole_c_bucket_stops_without_d_read_or_new_span(self):
        inputs = _OrdinaryProtocolReader()
        inputs.target_starts["T_TARGET"] = 20
        inputs.fine["T_TARGET"] = replace(inputs.fine["T_TARGET"],
            observed_interval=Interval(_stamp(0), _stamp(170)))
        inputs, plan, publisher = self.prepare_protocol(inputs)
        _populate_protocol(publisher, plan, inputs)
        self.assertEqual(publisher.stops[0][2], "CLOCK_COMMON_WHOLE_BUCKETS_EMPTY")
        self.assertEqual(publisher.templates, [])
        self.assertFalse(any(event[:2] == ("m5_values", "B_CONTROL") for event in inputs.events))

    def test_typed_authentication_abort_is_propagated_without_sealed_success_claim(self):
        inputs = _OrdinaryProtocolReader()
        error = NativeInputsError("SYNTHETIC_AUTHENTICATION_DRIFT", abort_attempt=True)
        inputs.failures["B_CONTROL"] = error
        inputs, plan, publisher = self.prepare_protocol(inputs)
        with self.assertRaises(NativeInputsError) as caught:
            _populate_protocol(publisher, plan, inputs)
        self.assertIs(caught.exception, error)
        self.assertEqual(publisher.series, [])

    def test_target_data_failure_is_isolated_without_changing_frozen_group(self):
        inputs = _OrdinaryProtocolReader()
        inputs.failures["T_TARGET"] = NativeInputsError("SYNTHETIC_SOURCE_PRICE_INVALID", abort_attempt=False)
        inputs, plan, publisher = self.prepare_protocol(inputs,
            (("T_TARGET", "H4"), ("U_TARGET", "H4")))
        frozen = plan.payload_json
        _populate_protocol(publisher, plan, inputs)
        self.assertEqual(plan.payload_json, frozen)
        self.assertEqual(publisher.stops[0][:3], ("T_TARGET", "H4", "SYNTHETIC_SOURCE_PRICE_INVALID"))
        self.assertEqual([series.source.symbol for series in publisher.series], ["U_TARGET"])

    def test_metadata_failure_is_stopped_before_group_intersection(self):
        inputs = _OrdinaryProtocolReader()
        inputs.fine["U_TARGET"] = replace(inputs.fine["U_TARGET"], observed_interval=None)
        _, plan, _ = self.prepare_protocol(inputs, (("T_TARGET", "H4"), ("U_TARGET", "H4")))
        payload = plan.to_dict()
        self.assertEqual([row["symbol"] for row in payload["targets"]], ["T_TARGET"])
        self.assertEqual(payload["stops"][0]["symbol"], "U_TARGET")

    def test_metadata_and_owner_mutation_refuse_before_any_values(self):
        inputs, plan, publisher = self.prepare_protocol()
        plan.strata[0].row["common_point"] = 0.5
        with self.assertRaises(ClockAdapterError) as caught:
            _populate_protocol(publisher, plan, inputs)
        self.assertEqual(caught.exception.code, "CLOCK_METADATA_PLAN_MUTATED")
        self.assertFalse(any(event[0] in ("clock_values", "m5_values") for event in inputs.events))
        _, other, _ = self.prepare_protocol()
        with self.assertRaises(ClockAdapterError):
            _populate_protocol(publisher, other, inputs)

    def test_csv_kind_and_exact_path_hash_bytes_rows_are_required_for_eof_record_join(self):
        identity = asdict(_identity("T_TARGET", "M5"))
        role = {"bars_path": "C:/SYNTHETIC_PROTOCOL_ONLY/T_TARGET-M5.csv",
                "source_identity": identity, "expected_bytes": 3456, "expected_rows": 6}
        good = _PlainPin(role["bars_path"], identity["bars_sha256"], 3456, kind="csv", rows=6)
        _checked_eof_roles((role,), (good,))
        for pin in (replace(good, kind="metadata"), replace(good, path=good.path + ".other"),
                    replace(good, sha256="f" * 64), replace(good, bytes=3455), replace(good, rows=5)):
            with self.subTest(pin=pin), self.assertRaises(ClockAdapterError):
                _checked_eof_roles((role,), (pin,))

    def test_shared_physical_csv_can_support_multiple_logical_roles_without_read_count_claim(self):
        identity = asdict(_identity("T_TARGET", "M5"))
        role = {"bars_path": "C:/SYNTHETIC_PROTOCOL_ONLY/T_TARGET-M5.csv",
                "source_identity": identity, "expected_bytes": 3456, "expected_rows": 6}
        pin = _PlainPin(role["bars_path"], identity["bars_sha256"], 3456, kind="csv", rows=6)
        _checked_eof_roles((dict(role, role="d_m5"), dict(role, role="target_m5")), (pin,))

    def test_empty_observed_target_is_explicit_stop_and_other_target_continues(self):
        inputs = _OrdinaryProtocolReader()
        inputs.empty_symbols.add("T_TARGET")
        inputs, plan, publisher = self.prepare_protocol(inputs,
            (("T_TARGET", "H4"), ("U_TARGET", "H4")))
        _populate_protocol(publisher, plan, inputs)
        self.assertEqual(publisher.stops[0][:3], ("T_TARGET", "H4", "CLOCK_TARGET_OBSERVED_PREFIX_EMPTY"))
        self.assertEqual([series.source.symbol for series in publisher.series], ["U_TARGET"])

    def test_equivalent_cutoff_spelling_is_normalized_before_publication_plan(self):
        inputs = _OrdinaryProtocolReader()
        plan = _prepare_protocol(inputs, (("T_TARGET", "H4"),), cutoff_utc="2024-07-14T08:05:00Z")
        self.assertEqual(plan.to_dict()["cutoff_utc"], PROTECTED_CUTOFF)

    def test_documented_target_cap_refuses_before_any_metadata_access(self):
        inputs = _OrdinaryProtocolReader()
        targets = tuple(("SYNTHETIC_TARGET_" + str(index), "H4") for index in range(MAX_CLOCK_TARGETS + 1))
        with self.assertRaises(ClockAdapterError) as caught:
            _prepare_protocol(inputs, targets, cutoff_utc=PROTECTED_CUTOFF)
        self.assertEqual(caught.exception.code, "CLOCK_TARGET_CAP_EXCEEDED")
        self.assertEqual(inputs.events, [])

    def test_prior_or_metadata_phase_csv_pin_refuses_before_plan_publication(self):
        for stage in ("prior", "during-metadata"):
            with self.subTest(stage=stage):
                inputs = _OrdinaryProtocolReader()
                if stage == "prior":
                    inputs.pins += (_PlainPin("SYNTHETIC-prior.csv", "d" * 64, 123, kind="csv", rows=1),)
                else:
                    inputs.csv_during_metadata = True
                with self.assertRaises(ClockAdapterError) as caught:
                    _prepare_protocol(inputs, (("T_TARGET", "H4"),), cutoff_utc=PROTECTED_CUTOFF)
                self.assertEqual(caught.exception.code, "CLOCK_METADATA_PLAN_HAS_PRIOR_CSV")
                if stage == "prior":
                    self.assertEqual(inputs.events, [])
                self.assertFalse(any(event[0] in ("clock_values", "m5_values") for event in inputs.events))

    def test_owned_real_hooks_adapter_and_original_spool_cold_roundtrip_without_native_authority(self):
        """M2 coupled file/helper test; no original scope entry or receipt verifier."""
        from pm_authority_spool import SpoolLimits, freeze_generation_context
        from pm_explicit_derived_native import NativeInputs
        from pm_explicit_clock_publication import (
            ClockEditionPublisher, reopen_clock_edition, SOURCE_CONTEXT_SCHEMA, AUTHENTICATION,
        )
        from test_explicit_clock_native_io import build_unverified_clock_inputs

        raw = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if raw is None:
            self.skipTest("requires explicitly precreated retained owned QA root")
        base = Path(raw)
        if (not raw or not base.is_absolute() or not base.is_dir() or base.is_symlink()
                or getattr(base.lstat(), "st_file_attributes", 0)
                    & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)):
            raise RuntimeError("FXPM_DERIVED_QA_ROOT must be an existing absolute owned directory")
        case = base / ("clock-adapter-coupled-" + uuid.uuid4().hex)
        case.mkdir(exist_ok=False)
        reader = build_unverified_clock_inputs(case / "owned-inputs")
        self.assertNotIsInstance(reader, NativeInputs)
        self.assertIsNone(reader._scope)
        plan = _prepare_protocol(reader, (reader.target_pair,), cutoff_utc=PROTECTED_CUTOFF)
        payload = plan.to_dict()
        self.assertEqual(len(payload["strata"]), 1)
        self.assertFalse(any(pin["kind"] == "csv" for pin in payload["metadata_read_pins"]))
        implementation = hashlib.sha256(b"SYNTHETIC_M2_NO_NATIVE_VERIFICATION").hexdigest()
        context = freeze_generation_context(case / "spool",
            generation_id="SYNTHETIC_CLOCK_ADAPTER_COUPLED_NO_NATIVE_VERIFICATION",
            native_universe_sha256=reader.authority_bindings.activation_sha256,
            config_sha256=plan.sha256,
            limits=SpoolLimits(max_artifact_bytes=256 * 1024, max_json_bytes=256 * 1024,
                max_manifest_bytes=256 * 1024, max_symbol_bytes=2 * 1024 * 1024,
                max_generation_bytes=4 * 1024 * 1024, max_artifacts_per_symbol=128,
                stream_chunk_bytes=4096, min_free_bytes=1))
        publisher = ClockEditionPublisher(context, plan=payload, implementation_sha256=implementation)
        roles = _populate_protocol(publisher, plan, reader)
        pins = tuple(reader.revalidate_owned_files_only())
        _checked_eof_roles(roles, pins)
        scope = {name: payload[name] for name in (
            "data_scope", "readiness_label", "full_native_context_eligibility",
            "financial_consumer_ready", "qualification_authority", "capital_authority",
            "live_authority", "order_authority")}
        publisher.bind_source_context({"schema_version": SOURCE_CONTEXT_SCHEMA,
            "authentication_semantics": AUTHENTICATION,
            "authority_bindings": asdict(reader.authority_bindings),
            "metadata_read_pins": [asdict(pin) for pin in pins], "consumed_roles": list(roles),
            "fixture_provenance": "OWNED_REAL_HOOK_PROTOCOL_NO_NATIVE_VERIFICATION_PERFORMED",
            "original_scope_entered": False, "original_receipt_backed_revalidation_performed": False,
            "fixture_owned_file_revalidation_only": True, **scope})
        receipt = publisher.finish()
        edition = reopen_clock_edition(context.root, receipt,
            expected_generation_id=context.generation_id,
            expected_native_universe_sha256=context.native_universe_sha256,
            expected_plan_sha256=plan.sha256, expected_implementation_sha256=implementation)
        self.assertEqual(len(edition["entries"]), 1)
        entry = edition["entries"][0]
        self.assertEqual((entry["symbol"], entry["timeframe"]), reader.target_pair)
        self.assertEqual(entry["status"], "PRICE_DATA_PUBLISHED")
        self.assertEqual(entry["cost_status"], "PRICE_ONLY")
        self.assertEqual(len(entry["bars"]), 2)
        self.assertEqual(entry["target_spec"]["native_status"], "technical_error")
        self.assertEqual(edition["profile_disposition"], "PRICE_ONLY_NO_PROFILE_ADOPTED")
        self.assertIsNone(edition["statistics"])
        self.assertNotIn("profile", edition)
        for name in ("full_native_context_eligibility", "financial_consumer_ready",
                     "qualification_authority", "capital_authority", "live_authority", "order_authority"):
            self.assertIs(edition[name], False)
        self.assertEqual({role["role"] for role in roles}, {"c_native", "d_native", "d_m5", "target_m5"})
        c_m5 = payload["strata"][0]["c_m5"]["bars_path"]
        self.assertNotIn(c_m5, [pin.path for pin in pins if pin.kind == "csv"])
        self.assertFalse(edition["source_context"]["original_scope_entered"])
        self.assertFalse(edition["source_context"]["original_receipt_backed_revalidation_performed"])
