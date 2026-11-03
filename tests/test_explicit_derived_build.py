"""Owned data-only orchestration fixtures, not native verification substitutes.

The real native verifier is never patched, replaced or reported as exercised.
The ordinary in-memory reader below supplies invented caller-owned observations.
"""
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import unittest
import uuid

import numpy as np
from pm_authority_spool import SpoolLimits, freeze_generation_context
from pm_explicit_derived_history import (
    Bar, Interval, SourceIdentity, Stratum, PROTECTED_CUTOFF, RECIPE_SHA256,
    canonical_json_bytes,
)
from pm_explicit_derived_native import NativeInputsError
from pm_explicit_derived_publication import (
    DerivedEditionPublisher, PublicationError, reopen_derived_edition, require_cost_ready,
)
from pm_explicit_derived_build import (
    populate_frozen_plan, build_approved_scope, RELATIVE_QA, DATA_SCOPE,
    _no_buildable_targets_error,
)


_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
_STRATUM = Stratum("M15", "SYNTHETIC_TEST_ONLY_CLASS", "SYNTHETIC_FIXED_M15")
_IMPLEMENTATION = "a" * 64
_NATIVE = "b" * 64
_SCOPE_ADOPTION = "fe4097091c10673cfa77d3cd823c7638f4cc8647e446d98cf1c15d0018ad1ac1"
_EXTENT_FIELDS = ("required_interval", "m5_acquisition_interval", "m5_observed_interval",
                  "available_interval", "omitted_intervals")


def _stamp(minutes):
    return (_START + timedelta(minutes=minutes)).isoformat()


def _identity(symbol, timeframe):
    return SourceIdentity(symbol, timeframe, "SYNTHETIC_TEST_ONLY_NO_NATIVE_VERIFICATION",
                          "1" * 64, "2" * 64, "3" * 64, "4" * 64, "5" * 64, 0.25)


_INTERVAL = Interval(_stamp(0), _stamp(120))


def _extent(*, missing_prefix=False):
    available = asdict(_INTERVAL)
    return {"required_interval": asdict(Interval(_stamp(-60), _stamp(120)))
                if missing_prefix else dict(available),
            "m5_acquisition_interval": dict(available),
            "m5_observed_interval": dict(available),
            "available_interval": dict(available),
            "omitted_intervals": [asdict(Interval(_stamp(-60), _stamp(0)))]
                if missing_prefix else []}


def _target_spec(symbol):
    """Invented technical target metadata; only same-symbol M5 supplies identity."""
    return {"symbol": symbol, "timeframe": "M15", "native_status": "technical_error",
            "native_reason_codes": ["SYNTHETIC_TEST_ONLY_NATIVE_TARGET_UNAVAILABLE"],
            "native_entry_sha256": "6" * 64, "run_manifest_sha256": "7" * 64,
            "run_request_sha256": "8" * 64,
            "target_source_request_interval": _extent(missing_prefix=True)["required_interval"],
            "target_calendar_boundaries": {
                "training": {"start_ts": _stamp(-60), "end_ts": _stamp(0)},
                "validation": {"start_ts": _stamp(0), "end_ts": _stamp(120)},
                "final_lockbox": {"start_ts": PROTECTED_CUTOFF}},
            "point": 0.25, "instrument_session_class": _STRATUM.instrument_session_class,
            "source_identity_from": "ACCEPTED_SAME_SYMBOL_M5"}


@dataclass(frozen=True)
class _Cell:
    source_identity: SourceIdentity
    interval: Interval = _INTERVAL
    cutoff_utc: str = PROTECTED_CUTOFF
    stratum: Stratum = _STRATUM


@dataclass(frozen=True)
class _Target:
    symbol: str
    timeframe: str
    stratum: Stratum
    interval: Interval
    m5: _Cell
    target_spec: dict
    extent: dict
    readiness_label: str = DATA_SCOPE
    data_scope: str = DATA_SCOPE
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False


@dataclass(frozen=True)
class _Control:
    stratum: Stratum
    interval: Interval
    native: _Cell
    m5: _Cell
    extent: dict
    readiness_label: str = DATA_SCOPE
    data_scope: str = DATA_SCOPE
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False


@dataclass(frozen=True)
class _Stop:
    symbol: str
    timeframe: str
    reason_code: str
    error_type: str = "SYNTHETIC_METADATA_ERROR"
    error_message: str = "SYNTHETIC_TEST_ONLY missing field"
    target_spec: dict | None = None
    extent: dict | None = None
    readiness_label: str = DATA_SCOPE
    data_scope: str = DATA_SCOPE
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False


@dataclass(frozen=True)
class _Plan:
    targets: tuple
    controls: tuple
    stops: tuple = ()
    cutoff_utc: str = PROTECTED_CUTOFF
    scope: str = "ONE_AVAILABLE_HISTORY_M5_DERIVED_DATA_ONLY_ATTEMPT"
    schema_version: str = "fxpm.explicit-derived-plan.v2"
    data_scope: str = DATA_SCOPE
    readiness_label: str = DATA_SCOPE
    full_native_context_eligibility: bool = False
    financial_consumer_ready: bool = False


def _fine_rows():
    return tuple(Bar(_stamp(i * 5), _stamp((i + 1) * 5),
        100.0 + i, 102.0 + i, 99.0 + i, 101.0 + i, 10, 1,
        ((4, 1, 6) if (i // 3) % 2 == 0 else (6, 9, 4))[i % 3]) for i in range(24))


def _native_rows():
    return tuple(Bar(_stamp(j * 15), _stamp((j + 1) * 15),
        100.0 + 3 * j, 104.0 + 3 * j, 99.0 + 3 * j, 103.0 + 3 * j,
        30, 3, 4 if j % 2 == 0 else 6) for j in range(8))


class _OwnedDataReader:
    """No authority, receipt, native scope, issuer or verifier interface."""
    def __init__(self, *, bad_native=False, errors=None, calibration_error=None):
        self.bad_native = bad_native
        self.errors = errors or {}
        self.calibration_error = calibration_error
        self.calls = []
        self.calibration_calls = []

    def iter_bars(self, cell, interval):
        key = (cell.source_identity.symbol, cell.source_identity.timeframe)
        self.calls.append(key)
        if key in self.errors:
            raise self.errors[key]
        rows = _native_rows() if key[1] == "M15" else _fine_rows()
        if self.bad_native and key[1] == "M15":
            rows = (replace(rows[0], open=100.5), *rows[1:])
        yield from rows

    def calibration_source(self, cell, cutoff):
        self.calibration_calls.append(cell.source_identity.symbol)
        if self.calibration_error is not None:
            raise self.calibration_error
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        first_ns = int((_START - epoch).total_seconds()) * 1_000_000_000
        return {"closes_ns": np.asarray([first_ns + j * 900_000_000_000 for j in range(8)], dtype=np.int64),
                "spreads": np.asarray(range(1, 9), dtype=np.float64)}


class ExplicitDerivedBuildTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        raw = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if raw is None:
            raise unittest.SkipTest("requires an explicitly precreated retained FXPM_DERIVED_QA_ROOT")
        if not raw or not Path(raw).is_absolute() or not Path(raw).is_dir():
            raise RuntimeError("precreated owned FXPM_DERIVED_QA_ROOT required")
        cls.parent = Path(raw)

    def setUp(self):
        self.root = self.parent / ("build-" + uuid.uuid4().hex)
        self.root.mkdir(exist_ok=False)

    def prepare(self, symbols=("SYN_TARGET",), *, controls=True, stop=None):
        targets = tuple(_Target(symbol, "M15", _STRATUM, _INTERVAL,
            _Cell(_identity(symbol, "M5")), _target_spec(symbol),
            _extent(missing_prefix=True)) for symbol in symbols)
        control = _Control(_STRATUM, _INTERVAL, _Cell(_identity("SYN_CONTROL", "M15")),
                           _Cell(_identity("SYN_CONTROL", "M5")), _extent())
        plan = _Plan(targets, (control,) if controls else (), (stop,) if stop else ())
        outer_targets = [{"symbol": t.symbol, "timeframe": t.timeframe} for t in targets]
        if stop:
            outer_targets.append({"symbol": stop.symbol, "timeframe": stop.timeframe})
        payload = {"schema_version": "fxpm.explicit-derived-plan.v2",
                   "recipe_sha256": RECIPE_SHA256, "targets": outer_targets,
                   "scope_adoption_sha256": _SCOPE_ADOPTION,
                   "data_scope": DATA_SCOPE,
                   "readiness_label": DATA_SCOPE,
                   "full_native_context_eligibility": False,
                   "financial_consumer_ready": False,
                   "native_plan": asdict(plan),
                   "fixture_provenance": "SYNTHETIC_TEST_ONLY_NO_NATIVE_VERIFICATION_PERFORMED"}
        digest = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        context = freeze_generation_context(self.root / "spool",
            generation_id="synthetic-data-" + uuid.uuid4().hex,
            native_universe_sha256=_NATIVE, config_sha256=digest,
            limits=SpoolLimits(max_artifact_bytes=256 * 1024, max_json_bytes=128 * 1024,
                max_manifest_bytes=256 * 1024, max_symbol_bytes=1024 * 1024,
                max_generation_bytes=2 * 1024 * 1024, max_artifacts_per_symbol=128,
                stream_chunk_bytes=4096, min_free_bytes=1))
        publisher = DerivedEditionPublisher(context, plan=payload, implementation_sha256=_IMPLEMENTATION)
        return plan, publisher, context, digest

    def finish(self, publisher, context, digest):
        receipt = publisher.finish()
        return reopen_derived_edition(context.root, receipt,
            expected_generation_id=context.generation_id,
            expected_native_universe_sha256=_NATIVE,
            expected_implementation_sha256=_IMPLEMENTATION, expected_plan_sha256=digest)

    def assert_data_only(self, value):
        self.assertEqual(value["data_scope"], DATA_SCOPE)
        self.assertEqual(value["readiness_label"], DATA_SCOPE)
        self.assertIs(value["full_native_context_eligibility"], False)
        self.assertIs(value["financial_consumer_ready"], False)

    def assert_target_extent(self, value, target, *, control_extent):
        self.assert_data_only(value)
        self.assertEqual({field: value[field] for field in _EXTENT_FIELDS}, target.extent)
        self.assertEqual(value["target_spec"], target.target_spec)
        self.assertEqual(value["control_extent"], control_extent)
        self.assertNotEqual(value["required_interval"], value["available_interval"])
        self.assertEqual(value["omitted_intervals"],
                         [asdict(Interval(_stamp(-60), _stamp(0)))])

    def test_successful_data_flow_uses_real_spool_and_serializes_control_tuples(self):
        plan, publisher, context, digest = self.prepare()
        reader = _OwnedDataReader()
        populate_frozen_plan(publisher, plan, reader)
        edition = self.finish(publisher, context, digest)
        entry = require_cost_ready(edition, "SYN_TARGET", "M15")
        self.assert_data_only(edition)
        self.assert_data_only(edition["profile"])
        self.assert_target_extent(entry, plan.targets[0], control_extent=plan.controls[0].extent)
        self.assert_target_extent(entry["profile"], plan.targets[0],
                                  control_extent=plan.controls[0].extent)
        published = publisher.entries[("SYN_TARGET", "M15")]
        self.assert_target_extent(published, plan.targets[0], control_extent=plan.controls[0].extent)
        lineage_path = context.root / published["lineage"]["descriptor"]["relative_path"]
        lineage = json.loads(lineage_path.read_bytes())
        self.assert_target_extent(lineage, plan.targets[0], control_extent=plan.controls[0].extent)
        lineage_body = {key: value for key, value in lineage.items() if key != "lineage_sha256"}
        self.assertEqual(lineage["lineage_sha256"],
                         hashlib.sha256(canonical_json_bytes(lineage_body)).hexdigest())
        self.assertEqual(len(entry["bars"]), 8)
        self.assertEqual(entry["cost_status"], "DERIVED_DATA_COST_READY_NOT_FINANCIAL_AUTHORITY")
        self.assertEqual(reader.calls[:2], [("SYN_CONTROL", "M5"), ("SYN_CONTROL", "M15")])
        self.assertNotIn(("SYN_TARGET", "M15"), reader.calls)
        for name in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
            self.assertIs(edition[name], False)

    def test_control_fidelity_failure_stops_only_its_targets_before_calibration(self):
        plan, publisher, context, digest = self.prepare()
        reader = _OwnedDataReader(bad_native=True)
        populate_frozen_plan(publisher, plan, reader)
        edition = self.finish(publisher, context, digest)
        self.assertEqual(edition["entries"][0]["reason_code"], "CONTROL_PRICE_FIDELITY_MISMATCH")
        self.assert_target_extent(edition["entries"][0], plan.targets[0],
                                  control_extent=plan.controls[0].extent)
        self.assertEqual(edition["entries"][0]["cost_status"], "COST_UNAVAILABLE")
        self.assertEqual(reader.calibration_calls, [])

    def test_typed_target_parse_failure_does_not_stop_another_target(self):
        plan, publisher, context, digest = self.prepare(("SYN_BAD", "SYN_GOOD"))
        error = NativeInputsError("SOURCE_PRICE_INVALID", detail="SYNTHETIC bad number", abort_attempt=False)
        populate_frozen_plan(publisher, plan, _OwnedDataReader(errors={("SYN_BAD", "M5"): error}))
        edition = self.finish(publisher, context, digest)
        bad = next(row for row in edition["entries"] if row["symbol"] == "SYN_BAD")
        self.assertEqual(bad["reason_code"], "SOURCE_PRICE_INVALID")
        self.assert_target_extent(bad, plan.targets[0], control_extent=plan.controls[0].extent)
        good = require_cost_ready(edition, "SYN_GOOD", "M15")
        self.assertEqual(len(good["bars"]), 8)
        self.assert_target_extent(good, plan.targets[1], control_extent=plan.controls[0].extent)

    def test_unauthenticated_input_error_aborts_without_sealing(self):
        plan, publisher, context, digest = self.prepare()
        error = NativeInputsError("SOURCE_DIGEST_ROWS_OR_BYTES_MISMATCH", abort_attempt=True)
        with self.assertRaises(NativeInputsError) as caught:
            populate_frozen_plan(publisher, plan,
                _OwnedDataReader(errors={("SYN_CONTROL", "M5"): error}))
        self.assertIs(caught.exception, error)
        self.assertIs(publisher.finished, False)
        self.assertEqual(publisher.entries, {})
        self.assertEqual(plan.targets[0].extent["omitted_intervals"],
                         [asdict(Interval(_stamp(-60), _stamp(0)))])

    def test_missing_frozen_control_is_an_explicit_stop(self):
        plan, publisher, context, digest = self.prepare(controls=False)
        reader = _OwnedDataReader()
        populate_frozen_plan(publisher, plan, reader)
        edition = self.finish(publisher, context, digest)
        self.assertEqual(edition["entries"][0]["reason_code"], "FROZEN_CONTROL_UNAVAILABLE")
        self.assert_target_extent(edition["entries"][0], plan.targets[0], control_extent=None)
        self.assertEqual(reader.calls, [])

    def test_calibration_format_message_is_retained_without_cost_readiness(self):
        plan, publisher, context, digest = self.prepare()
        populate_frozen_plan(publisher, plan,
            _OwnedDataReader(calibration_error=ValueError("SYNTHETIC missing spread column")))
        edition = self.finish(publisher, context, digest)
        stats = edition["entries"][0]["profile"]["statistics"]
        self.assertEqual(stats["error_type"], "ValueError")
        self.assertIn("SYNTHETIC missing spread column", stats["error_message"])
        self.assertEqual(edition["entries"][0]["cost_status"], "COST_UNAVAILABLE")
        self.assert_target_extent(edition["entries"][0], plan.targets[0],
                                  control_extent=plan.controls[0].extent)
        self.assert_target_extent(edition["entries"][0]["profile"], plan.targets[0],
                                  control_extent=plan.controls[0].extent)
        with self.assertRaises(PublicationError):
            require_cost_ready(edition, "SYN_TARGET", "M15")

    def test_metadata_stop_is_retained_without_reading_its_data(self):
        stop = _Stop("SYN_UNSUPPORTED", "H4", "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR")
        plan, publisher, context, digest = self.prepare((), controls=False, stop=stop)
        reader = _OwnedDataReader()
        populate_frozen_plan(publisher, plan, reader)
        edition = self.finish(publisher, context, digest)
        self.assertEqual(edition["entries"][0]["reason_code"], stop.reason_code)
        self.assert_data_only(edition["entries"][0])
        for field in (*_EXTENT_FIELDS, "target_spec", "control_extent"):
            self.assertIsNone(edition["entries"][0][field])
        self.assertEqual(reader.calls, [])

    def test_actual_entry_refuses_existing_owned_root_before_native_authentication(self):
        existing = self.root / RELATIVE_QA / "explicit_m5_derived_001"
        existing.mkdir(parents=True)
        with self.assertRaises(PublicationError) as caught:
            build_approved_scope(self.root, expected_implementation_sha256=_IMPLEMENTATION)
        self.assertEqual(caught.exception.code, "DERIVED_ATTEMPT_ROOT_ALREADY_EXISTS_NO_RETRY")

    def test_all_stop_error_retains_reason_counts_before_any_publication(self):
        stops = tuple(_Stop("SYN_" + str(i), "H4", "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR")
                      for i in range(25)) + tuple(
            _Stop("SYN_" + str(i), "H1", "AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE")
            for i in range(25, 35))
        error = _no_buildable_targets_error(_Plan((), (), stops))
        self.assertIsInstance(error, PublicationError)
        self.assertEqual(error.code, "NO_BUILDABLE_TARGETS_UNDER_APPROVED_SCOPE")
        self.assertEqual(error.stop_count, 35)
        self.assertEqual(error.stop_code_counts, {
            "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR": 25,
            "AUTHENTICATED_OBSERVED_BOUNDS_UNAVAILABLE": 10})
        self.assertFalse((self.root / "spool").exists())
