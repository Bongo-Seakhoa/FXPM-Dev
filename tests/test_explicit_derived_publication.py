"""Synthetic publication tests; every fixture is owned and retained.

FXPM_DERIVED_QA_ROOT must name a precreated owned parent supplied by an
explicitly authorized launcher. No native source, profile, terms or financial
configuration is read. Original spool receipts here authenticate invented test
artifacts only and never grant native or financial authority.
"""
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import unittest
import uuid

import numpy as np
import pm_authority_spool as spool
from pm_explicit_derived_history import (
    Bar, Interval, SourceIdentity, Stratum, apply_spread_rule,
    PROTECTED_CUTOFF, build_lineage, canonical_json_bytes, derive_bars, identify_spread_rule,
)
from pm_explicit_derived_publication import (
    DerivedEditionPublisher, PublicationError, calibrated_statistics,
    reopen_derived_edition, require_cost_ready,
)


_RECIPE = "85e321bc794c59cf26e725bbb7772b2590a7b0256e2f6e6086ee3bf52189c1f4"
_IMPLEMENTATION = "a" * 64
_NATIVE_UNIVERSE = "b" * 64
_SYMBOL = "SYNTHETIC_TEST_ONLY"
_AUTHORITIES = ("qualification_authority", "capital_authority",
                "live_authority", "order_authority")
_DATA_SCOPE = "AVAILABLE_DATA_ONLY_NOT_EXECUTION_READY"
_ADOPTION = "fe4097091c10673cfa77d3cd823c7638f4cc8647e446d98cf1c15d0018ad1ac1"
_EXTENT_FIELDS = ("required_interval", "m5_acquisition_interval", "m5_observed_interval",
                  "available_interval", "omitted_intervals")
_CONTEXT_FIELDS = ("data_scope", "readiness_label", "full_native_context_eligibility",
                   "financial_consumer_ready", "target_spec", "control_extent",
                   *_EXTENT_FIELDS, *_AUTHORITIES)
_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _stamp(minutes):
    return (_START + timedelta(minutes=minutes)).isoformat()


def _ns(minutes):
    return (int((_START - _EPOCH).total_seconds()) + minutes * 60) * 1_000_000_000


def _identity(timeframe):
    return SourceIdentity(
        symbol=_SYMBOL, timeframe=timeframe,
        activation_id="SYNTHETIC_TEST_ONLY_ACTIVATION",
        activation_sha256="1" * 64, bars_sha256="2" * 64,
        sidecar_sha256="3" * 64, provenance_sha256="4" * 64,
        request_sha256="5" * 64, point=0.25, spread_unit="points",
    )


def _price_series(*, missing_member=False, constant_control=False,
                  source=None, interval=None, stratum=None, cutoff=None):
    rows = []
    for i in range(24):
        candidates = (4, 1, 6) if (i // 3) % 2 == 0 else (6, 9, 4)
        spread = 5 if constant_control else candidates[i % 3]
        if missing_member and i == 22:
            spread = None
        rows.append(Bar(
            open_utc=_stamp(i * 5), close_utc=_stamp((i + 1) * 5),
            open=100.0 + i, high=102.0 + i, low=99.0 + i,
            close=101.0 + i, volume=10, real_volume=1, spread=spread,
        ))
    kwargs = {} if cutoff is None else {"cutoff_utc": cutoff}
    return derive_bars(
        rows, stratum=(Stratum("M15", "SYNTHETIC_TEST_ONLY_SESSION", "SYNTHETIC_FIXED_M15")
                       if stratum is None else stratum),
        source=_identity("M5") if source is None else source,
        interval=Interval(_stamp(0), _stamp(120)) if interval is None else interval,
        **kwargs,
    )


def _identification(*, ambiguous=False, control_source=None, native_source=None,
                    interval=None, stratum=None):
    control = _price_series(constant_control=ambiguous, source=control_source,
                            interval=interval, stratum=stratum)
    native = tuple(replace(row.value, spread=(5 if ambiguous else row.spread_candidates[0]))
                   for row in control.bars)
    return identify_spread_rule(control, native,
                                native_source=_identity("M15") if native_source is None else native_source)


def _target(*, missing_member=False, ambiguous=False):
    identification = _identification(ambiguous=ambiguous)
    return apply_spread_rule(_price_series(missing_member=missing_member), identification), identification


def _fine():
    # Nearby999-point quotes are deliberately not exact prior closes.
    closes, spreads = [], []
    for i in range(8):
        closes.extend((_ns(i * 15) - 1, _ns(i * 15)))
        spreads.extend((999, i))
    return {"closes_ns": np.asarray(closes, dtype=np.int64),
            "spreads": np.asarray(spreads, dtype=np.float64)}


def _extent(required_start=-60):
    available = {"start_utc": _stamp(0), "end_exclusive_utc": _stamp(120)}
    return {"required_interval": {"start_utc": _stamp(required_start), "end_exclusive_utc": _stamp(120)},
            "m5_acquisition_interval": deepcopy(available),
            "m5_observed_interval": deepcopy(available), "available_interval": deepcopy(available),
            "omitted_intervals": ([{"start_utc": _stamp(required_start), "end_exclusive_utc": _stamp(0)}]
                                   if required_start < 0 else [])}


def _target_spec(series):
    return {"symbol": _SYMBOL, "timeframe": "M15", "native_status": "technical_error",
            "native_reason_codes": ["SYNTHETIC_NATIVE_FAILURE"], "native_entry_sha256": "6" * 64,
            "run_manifest_sha256": "7" * 64, "run_request_sha256": "8" * 64,
            "target_source_request_interval": {"start_utc": _stamp(-60), "end_exclusive_utc": _stamp(180)},
            "target_calendar_boundaries": {"optimizer_train": {"start_ts": _stamp(-60), "end_ts": _stamp(30)},
                "tuning_validation": {"start_ts": _stamp(30), "end_ts": _stamp(60)},
                "final_lockbox": {"start_ts": PROTECTED_CUTOFF}},
            "point": series.source.point,
            "instrument_session_class": series.stratum.instrument_session_class,
            "source_identity_from": "ACCEPTED_SAME_SYMBOL_M5"}


def _plan(timeframe="M15", *, series=None, identification=None, fully_covered=False):
    # Invented metadata only: the technical target is never a native SourceIdentity.
    targets, controls, stops = [], [], []
    labels = {"readiness_label": _DATA_SCOPE, "full_native_context_eligibility": False,
              "financial_consumer_ready": False}
    if timeframe == "M15":
        if series is None:
            series, default_identification = _target()
            identification = default_identification if identification is None else identification
        if identification is None:
            identification = _identification()
        available = asdict(series.interval)
        target_extent, control_extent = _extent(0 if fully_covered else -60), _extent(0)
        m5 = {"source_identity": asdict(series.source), "cutoff_utc": series.cutoff_utc,
              "interval": deepcopy(available)}
        targets = [{"symbol": _SYMBOL, "timeframe": timeframe,
                    "stratum": asdict(series.stratum), "interval": available, "m5": m5,
                    "target_spec": _target_spec(series), "extent": target_extent, **labels}]
        controls = [{"stratum": asdict(identification.stratum),
                     "interval": asdict(identification.interval), "extent": control_extent,
                     "m5": {"source_identity": asdict(identification.control_source),
                            "cutoff_utc": series.cutoff_utc, "interval": asdict(identification.interval)},
                     "native": {"source_identity": asdict(identification.native_source),
                                "cutoff_utc": series.cutoff_utc, "interval": asdict(identification.interval)}, **labels}]
    else:
        stops = [{"symbol": _SYMBOL, "timeframe": timeframe,
                  "reason_code": "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR", "target_spec": None,
                  "extent": None, **labels}]
    native_plan = {"schema_version": "fxpm.explicit-derived-plan.v2",
                   "scope": "ONE_AVAILABLE_HISTORY_M5_DERIVED_DATA_ONLY_ATTEMPT",
                   "cutoff_utc": PROTECTED_CUTOFF, "targets": targets, "controls": controls, "stops": stops,
                   "control_catalog_diagnostics": [], "authority_bindings": {},
                   "metadata_read_pins": [], **{key: False for key in _AUTHORITIES}}
    return {"schema_version": "fxpm.explicit-derived-plan.v2", "recipe_sha256": _RECIPE,
            "data_scope": _DATA_SCOPE, "readiness_label": _DATA_SCOPE,
            "scope_adoption_sha256": _ADOPTION, "full_native_context_eligibility": False,
            "financial_consumer_ready": False,
            "targets": [{"symbol": _SYMBOL, "timeframe": timeframe}], "native_plan": native_plan,
            "fixture_provenance": "SYNTHETIC_TEST_ONLY_NOT_NATIVE_AUTHORITY"}


def _digest(value):
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


class ExplicitDerivedPublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        raw = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if raw is None:
            raise unittest.SkipTest("requires an explicitly precreated retained FXPM_DERIVED_QA_ROOT")
        if not raw or not Path(raw).is_absolute():
            raise RuntimeError("FXPM_DERIVED_QA_ROOT must name an explicit absolute owned parent")
        parent = Path(raw)
        if parent.is_symlink() or not parent.is_dir():
            raise RuntimeError("FXPM_DERIVED_QA_ROOT must be precreated and not a symlink")
        cls.qa_parent = parent.resolve(strict=True)

    def setUp(self):
        self.fixture_root = self.qa_parent / ("p-" + uuid.uuid4().hex)
        self.fixture_root.mkdir(exist_ok=False)
        # No tearDown: successful and failed fixture bytes remain available.

    def assert_publication_refusal(self, action, code):
        with self.assertRaises(PublicationError) as caught:
            action()
        self.assertEqual(caught.exception.code, code)

    def assert_false_authorities(self, value):
        for key in _AUTHORITIES:
            self.assertIs(value[key], False, key)

    def assert_data_only(self, value):
        self.assertEqual(value["data_scope"], _DATA_SCOPE)
        self.assertEqual(value["readiness_label"], _DATA_SCOPE)
        self.assertIs(value["full_native_context_eligibility"], False)
        self.assertIs(value["financial_consumer_ready"], False)
        self.assert_false_authorities(value)

    def make_context(self, plan):
        limits = spool.SpoolLimits(
            max_artifact_bytes=256 * 1024, max_json_bytes=256 * 1024,
            max_manifest_bytes=256 * 1024, max_symbol_bytes=1024 * 1024,
            max_generation_bytes=2 * 1024 * 1024, max_artifacts_per_symbol=128,
            stream_chunk_bytes=4096, min_free_bytes=1,
        )
        context = spool.freeze_generation_context(
            self.fixture_root / ("s-" + uuid.uuid4().hex),
            generation_id="synthetic-derived-" + uuid.uuid4().hex,
            native_universe_sha256=_NATIVE_UNIVERSE, config_sha256=_digest(plan), limits=limits,
        )
        return context

    def make_publisher(self, plan=None):
        plan = _plan() if plan is None else plan
        context = self.make_context(plan)
        publisher = DerivedEditionPublisher(context, plan=plan, implementation_sha256=_IMPLEMENTATION)
        return context, publisher, _digest(plan)

    def publish(self, *, missing_member=False, ambiguous=False, fine=None):
        series, identification = _target(missing_member=missing_member, ambiguous=ambiguous)
        statistics = calibrated_statistics(series, _fine() if fine is None else fine)
        context, publisher, plan_sha = self.make_publisher(
            _plan(series=series, identification=identification))
        publisher.diagnostic("synthetic-control", {"identification": asdict(identification),
                                                   "nested_tuple": ("SYNTHETIC", (1, 2))})
        publisher.add_series(series, identification, statistics=statistics)
        receipt = publisher.finish()
        return context, receipt, plan_sha, series, statistics

    def reopen(self, context, receipt, plan_sha, **overrides):
        args = dict(expected_generation_id=context.generation_id,
                    expected_native_universe_sha256=_NATIVE_UNIVERSE,
                    expected_implementation_sha256=_IMPLEMENTATION,
                    expected_plan_sha256=plan_sha)
        args.update(overrides)
        return reopen_derived_edition(context.root, receipt, **args)

    def manifest(self, context, receipt, plan_sha, **overrides):
        args = dict(expected_generation_id=context.generation_id,
                    expected_symbol=receipt["symbol"],
                    expected_native_universe_sha256=_NATIVE_UNIVERSE,
                    expected_config_sha256=plan_sha)
        args.update(overrides)
        return spool.verify_symbol_receipt(context.root, receipt, **args)

    def test_calibration_exact_prior_close_higher_quantiles_and_zero_retention(self):
        series, _ = _target()
        fine = _fine()
        closes_before, spreads_before = fine["closes_ns"].copy(), fine["spreads"].copy()
        stats = calibrated_statistics(series, fine)
        self.assertEqual(stats["quantile_method"], "higher_order_statistic_ceil((n-1)*p)")
        self.assertEqual(stats["status"], "CALCULATED")
        self.assertEqual((stats["rows"], stats["valid_rows"], stats["invalid_rows"],
                          stats["zero_rows"]), (8, 8, 0, 1))
        self.assertEqual((stats["target_prefix_rows"], stats["matched_rows"],
                          stats["missing_exact_prior_close_rows"]), (8, 8, 0))
        self.assertEqual(stats["tiers"]["P75"],
                         {"spread_points": 6, "full_spread_price": 1.5,
                          "half_spread_price": 0.75, "share_at_or_below": 7 / 8,
                          "usability": "POSITIVE_QUANTILE"})
        self.assertEqual(stats["tiers"]["P95"],
                         {"spread_points": 7, "full_spread_price": 1.75,
                          "half_spread_price": 0.875, "share_at_or_below": 1.0,
                          "usability": "POSITIVE_QUANTILE"})
        self.assertEqual(stats["first_target_open_utc"], _stamp(0))
        self.assertEqual(stats["last_target_close_utc"], _stamp(120))
        self.assertEqual(stats["last_matched_open_utc"], _stamp(105))
        np.testing.assert_array_equal(fine["closes_ns"], closes_before)
        np.testing.assert_array_equal(fine["spreads"], spreads_before)

    def test_missing_exact_prior_close_is_reported_without_nearest_fill(self):
        series, _ = _target()
        fine = _fine()
        keep = fine["closes_ns"] != _ns(30)
        stats = calibrated_statistics(series, {key: value[keep] for key, value in fine.items()})
        self.assertEqual((stats["target_prefix_rows"], stats["matched_rows"],
                          stats["missing_exact_prior_close_rows"]), (8, 7, 1))
        self.assertEqual(stats["rows"], 7)
        self.assertEqual(stats["tiers"]["P95"]["spread_points"], 7)
        context, receipt, plan_sha, _, _ = self.publish(
            fine={key: value[keep] for key, value in fine.items()})
        edition = self.reopen(context, receipt, plan_sha)
        data = require_cost_ready(edition, _SYMBOL, "M15")
        self.assertEqual(data["cost_status"], "DERIVED_DATA_COST_READY_NOT_FINANCIAL_AUTHORITY")
        self.assertIs(data["financial_consumer_ready"], False)
        self.assertEqual(data["profile"]["calibration_coverage"], {
            "rule": "AVAILABLE_EXACT_PRIOR_CLOSE_OBSERVATIONS_ONLY_NO_NEAREST_FILL",
            "target_prefix_rows": 8, "matched_rows": 7,
            "missing_exact_prior_close_rows": 1, "invalid_rows": 0,
            "financial_consumer_ready": False})

    def test_original_spool_roundtrip_profile_joins_and_detached_output(self):
        context, receipt, plan_sha, series, stats = self.publish()
        edition = self.reopen(context, receipt, plan_sha)
        self.assertIs(type(receipt), dict)
        self.assertEqual(receipt["schema_version"], spool.COMPACT_RECEIPT_SCHEMA)
        self.assertEqual(edition["source_kind"], "m5_derived")
        self.assertEqual(edition["profile"]["schema_version"],
                         "fxpm.explicit-derived-spread-profile.v1")
        self.assertEqual(edition["plan_sha256"], plan_sha)
        self.assertEqual(edition["financial_consumer_integration"],
                         "NOT_GRANTED_OR_IMPLEMENTED_BY_THIS_DATA_READER")
        self.assert_false_authorities(edition)
        self.assert_false_authorities(edition["profile"])
        self.assertIs(edition["financial_consumer_ready"], False)
        self.assertIs(edition["profile"]["financial_consumer_ready"], False)
        self.assertEqual(len(edition["entries"]), 1)
        entry = require_cost_ready(edition, _SYMBOL, "M15")
        self.assert_false_authorities(entry)
        self.assert_false_authorities(entry["profile"])
        self.assertEqual(entry["cost_status"], "DERIVED_DATA_COST_READY_NOT_FINANCIAL_AUTHORITY")
        self.assertEqual(tuple(entry["bars"]), tuple(row.value for row in series.bars))
        self.assertTrue(all(type(bar) is Bar for bar in entry["bars"]))
        self.assertFalse(any(hasattr(bar, "_seal") for bar in entry["bars"]))
        self.assertEqual(entry["profile"]["statistics"], stats)
        self.assertEqual(entry["profile"]["membership_sha256"], series.membership_sha256)
        for value in (edition, edition["profile"], entry, entry["profile"]):
            self.assert_data_only(value)
        for name in _EXTENT_FIELDS:
            self.assertEqual(entry[name], _extent()[name])
            self.assertEqual(entry["profile"][name], _extent()[name])
        self.assertEqual(entry["target_spec"], _target_spec(series))
        self.assertNotIn("bars_sha256", entry["target_spec"])
        self.assertNotIn("activation_id", entry["target_spec"])
        self.assertEqual(entry["control_extent"], _extent(0))
        artifacts = self.manifest(context, receipt, plan_sha)["artifacts"]
        csv_row = next(row for row in artifacts if row["artifact_type"] == "explicit_derived_bars")
        self.assertEqual(entry["profile"]["target_bars_sha256"], csv_row["descriptor"]["sha256"])
        lineage_row = next(row for row in artifacts if row["artifact_type"] == "explicit_derived_lineage")
        lineage = json.loads(self.fixture_artifact_bytes(context, lineage_row))
        self.assert_data_only(lineage)
        self.assertEqual(lineage["required_interval"], _extent()["required_interval"])
        self.assertEqual(lineage["available_interval"], asdict(series.interval))
        self.assertNotEqual(lineage["required_interval"], lineage["available_interval"])
        self.assertEqual(lineage["omitted_intervals"], _extent()["omitted_intervals"])
        self.assertEqual(lineage["target_spec"], entry["target_spec"])
        self.assertEqual(lineage["control_extent"], entry["control_extent"])
        digest = lineage.pop("lineage_sha256")
        self.assertEqual(digest, _digest(lineage))
        entry["profile"]["statistics"]["tiers"]["P75"]["spread_points"] = 999
        self.assertEqual(edition["entries"][0]["profile"]["statistics"]["tiers"]["P75"]["spread_points"], 6)
        edition["profile"]["groups"][0]["statistics"]["tiers"]["P95"]["spread_points"] = 999
        cold_again = self.reopen(context, receipt, plan_sha)
        self.assertEqual(cold_again["profile"]["groups"][0]["statistics"]["tiers"]["P95"]["spread_points"], 7)

    def test_positive_floor_does_not_make_missing_target_spread_ready(self):
        context, receipt, plan_sha, _, stats = self.publish(missing_member=True)
        self.assertGreater(stats["tiers"]["P75"]["spread_points"], 0)
        edition = self.reopen(context, receipt, plan_sha)
        self.assertEqual(edition["entries"][0]["cost_status"], "COST_UNAVAILABLE")
        self.assertEqual(len(edition["entries"][0]["bars"]), 8)
        self.assertIsNone(edition["entries"][0]["bars"][-1].spread)
        self.assert_publication_refusal(lambda: require_cost_ready(edition, _SYMBOL, "M15"),
                                        "DERIVED_COST_UNAVAILABLE")

    def test_positive_floor_does_not_override_ambiguous_control(self):
        context, receipt, plan_sha, _, stats = self.publish(ambiguous=True)
        self.assertGreater(stats["tiers"]["P75"]["spread_points"], 0)
        edition = self.reopen(context, receipt, plan_sha)
        self.assertEqual(edition["entries"][0]["cost_status"], "COST_UNAVAILABLE")
        self.assertTrue(all(bar.spread is None for bar in edition["entries"][0]["bars"]))
        self.assert_publication_refusal(lambda: require_cost_ready(edition, _SYMBOL, "M15"),
                                        "DERIVED_COST_UNAVAILABLE")

    def test_invalid_matched_fine_spread_remains_unavailable(self):
        fine = _fine()
        fine["spreads"][5] = np.nan
        context, receipt, plan_sha, _, stats = self.publish(fine=fine)
        self.assertEqual(stats["invalid_rows"], 1)
        self.assertEqual(stats["invalid_nonfinite"], 1)
        edition = self.reopen(context, receipt, plan_sha)
        self.assertEqual(edition["entries"][0]["cost_status"], "COST_UNAVAILABLE")
        self.assert_publication_refusal(lambda: require_cost_ready(edition, _SYMBOL, "M15"),
                                        "DERIVED_COST_UNAVAILABLE")

    def assert_corrupt_artifact_refuses(self, artifact_type):
        context, receipt, plan_sha, _, _ = self.publish()
        artifacts = self.manifest(context, receipt, plan_sha)["artifacts"]
        descriptor = next(row["descriptor"] for row in artifacts
                          if row["artifact_type"] == artifact_type)
        relative = Path(descriptor["relative_path"])
        self.assertFalse(relative.is_absolute())
        target = (context.root / relative).resolve(strict=True)
        self.assertTrue(target.is_relative_to(self.fixture_root.resolve(strict=True)))
        raw = target.read_bytes()
        self.assertTrue(raw)
        target.write_bytes(bytes((raw[0] ^ 1,)) + raw[1:])
        with self.assertRaises(spool.AuthoritySpoolError):
            self.reopen(context, receipt, plan_sha)

    def test_corrupted_price_csv_refuses_original_spool_validation(self):
        self.assert_corrupt_artifact_refuses("explicit_derived_bars")

    def test_corrupted_membership_refuses_original_spool_validation(self):
        self.assert_corrupt_artifact_refuses("explicit_derived_membership")

    def test_context_and_plan_mismatches_preserve_original_spool_refusal(self):
        context, receipt, plan_sha, _, _ = self.publish()
        pairs = (
            ({"expected_generation_id": "SYNTHETIC_WRONG_GENERATION"},
             {"expected_generation_id": "SYNTHETIC_WRONG_GENERATION"}),
            ({"expected_native_universe_sha256": "c" * 64},
             {"expected_native_universe_sha256": "c" * 64}),
            ({"expected_plan_sha256": "d" * 64},
             {"expected_config_sha256": "d" * 64}),
        )
        for reopen_args, spool_args in pairs:
            with self.subTest(reopen_args=reopen_args):
                with self.assertRaises(spool.AuthoritySpoolError) as direct:
                    self.manifest(context, receipt, plan_sha, **spool_args)
                with self.assertRaises(spool.AuthoritySpoolError) as public:
                    self.reopen(context, receipt, plan_sha, **reopen_args)
                self.assertEqual(public.exception.args, direct.exception.args)

    def test_implementation_mismatch_is_explicit_binding_refusal(self):
        context, receipt, plan_sha, _, _ = self.publish()
        self.assert_publication_refusal(
            lambda: self.reopen(context, receipt, plan_sha,
                                expected_implementation_sha256="e" * 64),
            "EDITION_BINDING_MISMATCH",
        )

    def test_unsupported_stop_has_no_fictitious_bar_or_profile_group(self):
        context, publisher, plan_sha = self.make_publisher(_plan("H4"))
        publisher.stop(_SYMBOL, "H4", "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR")
        receipt = publisher.finish()
        edition = self.reopen(context, receipt, plan_sha)
        self.assert_false_authorities(edition)
        self.assertEqual(edition["profile"]["groups"], [])
        self.assertEqual(len(edition["entries"]), 1)
        entry = edition["entries"][0]
        self.assertEqual((entry["status"], entry["cost_status"], entry["reason_code"]),
                         ("STOP", "COST_UNAVAILABLE", "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR"))
        self.assertNotIn("bars", entry)
        self.assert_data_only(entry)
        self.assertTrue(all(entry[name] is None for name in _EXTENT_FIELDS))
        self.assertIsNone(entry["target_spec"])
        self.assertIsNone(entry["control_extent"])
        self.assertEqual(len(edition["profile"]["unavailable"]), 1)
        self.assert_publication_refusal(lambda: require_cost_ready(edition, _SYMBOL, "H4"),
                                        "DERIVED_COST_UNAVAILABLE")

    def test_unplanned_target_and_repeated_finish_are_refused(self):
        _, publisher, _ = self.make_publisher()
        self.assert_publication_refusal(
            lambda: publisher.stop("SYNTHETIC_UNPLANNED", "M15", "SYNTHETIC_STOP"),
            "TARGET_NOT_PLANNED",
        )
        publisher.stop(_SYMBOL, "M15", "SYNTHETIC_STOP")
        publisher.finish()
        self.assert_publication_refusal(publisher.finish, "EDITION_ALREADY_FINISHED")
        self.assert_publication_refusal(
            lambda: publisher.stop(_SYMBOL, "M15", "SYNTHETIC_STOP"),
            "EDITION_ALREADY_FINISHED",
        )


    def fixture_artifact_bytes(self, context, row):
        relative = Path(row["descriptor"]["relative_path"])
        self.assertFalse(relative.is_absolute())
        target = (context.root / relative).resolve(strict=True)
        self.assertTrue(target.is_relative_to(self.fixture_root.resolve(strict=True)))
        return target.read_bytes()

    def reseal_single_entry_fixture(self, baseline, *, change_plan=None, change_profile=None,
                                    change_entry=None, change_lineage=None):
        """Real original spool seals deliberately inconsistent invented metadata.

        No verifier is patched. Only this test's one-entry owned graph is copied;
        the new graph has valid byte/receipt/lineage joins so plan/roster checks run.
        """
        context, receipt, plan_sha, series, _ = baseline
        roster = {row["artifact_id"]: row for row in self.manifest(context, receipt, plan_sha)["artifacts"]}
        activation = json.loads(self.fixture_artifact_bytes(context, roster["activation"]))
        plan = json.loads(self.fixture_artifact_bytes(context, activation["plan"]))
        profile = json.loads(self.fixture_artifact_bytes(context, activation["profile"]))
        if change_plan is not None:
            change_plan(plan)
        if change_profile is not None:
            change_profile(profile)
        new_context = self.make_context(plan)
        sink = spool.open_symbol_sink(new_context, "EXPLICIT_DERIVED_EDITION",
                                      manifest_domain=spool.GENERAL_MANIFEST_DOMAIN)
        plan_row = sink.put_json("plan", "explicit_derived_plan", plan)
        profile["plan_sha256"] = _digest(plan)
        profile_row = sink.put_json("profile", "explicit_derived_profile", profile)
        self.assertEqual(len(activation["entries"]), 1)
        entry = deepcopy(activation["entries"][0])
        if change_entry is not None:
            change_entry(entry)
        if entry["status"] == "PRICE_DATA_PUBLISHED":
            for field in ("bars", "membership", "identification"):
                row = entry[field]
                raw = self.fixture_artifact_bytes(context, row)
                if row["descriptor"]["format"] == spool.CANONICAL_JSON_FORMAT:
                    entry[field] = sink.put_json(row["artifact_id"], row["artifact_type"], json.loads(raw))
                else:
                    entry[field] = sink.put_opaque(row["artifact_id"], row["artifact_type"],
                                                   (raw,), format=row["descriptor"]["format"])
            lineage = json.loads(build_lineage(
                series, output_sha256=entry["bars"]["descriptor"]["sha256"],
                implementation_sha256=_IMPLEMENTATION,
                derived_profile_sha256=profile_row["descriptor"]["sha256"]))
            lineage.pop("lineage_sha256")
            lineage["series_interval"] = asdict(series.interval)
            lineage.update({name: deepcopy(entry[name]) for name in _CONTEXT_FIELDS})
            if change_lineage is not None:
                change_lineage(lineage)
            lineage["lineage_sha256"] = _digest(lineage)
            old_lineage = entry["lineage"]
            entry["lineage"] = sink.put_json(old_lineage["artifact_id"], old_lineage["artifact_type"], lineage)
        activation.update(plan_sha256=_digest(plan), plan=plan_row,
                          profile=profile_row, entries=[entry])
        sink.put_json("activation", "explicit_derived_activation", activation)
        return new_context, sink.seal(), _digest(plan)

    def test_diagnostic_tuples_are_detached_for_original_json_spool(self):
        context, publisher, plan_sha = self.make_publisher(_plan("H4"))
        value = {"tuple": (("first", 0), ("last", 1))}
        publisher.diagnostic("synthetic-tuple", value)
        value["tuple"] = ("CHANGED_AFTER_PUBLICATION",)
        publisher.stop(_SYMBOL, "H4", "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR")
        receipt = publisher.finish()
        artifacts = self.manifest(context, receipt, plan_sha)["artifacts"]
        diagnostic = next(row for row in artifacts if row["artifact_id"] == "diagnostic:synthetic-tuple")
        self.assertEqual(json.loads(self.fixture_artifact_bytes(context, diagnostic)),
                         {"tuple": [["first", 0], ["last", 1]]})
        self.reopen(context, receipt, plan_sha)

    def test_hot_plan_join_rejects_self_consistent_wrong_metadata(self):
        series, identification = _target()
        context, publisher, plan_sha = self.make_publisher(
            _plan(series=series, identification=identification))
        stats = calibrated_statistics(series, _fine())
        different_source = replace(_identity("M5"), bars_sha256="9" * 64)
        other_stratum = Stratum("M15", "SYNTHETIC_OTHER_SESSION", "SYNTHETIC_FIXED_M15")
        variants = [
            (apply_spread_rule(_price_series(source=different_source), identification), identification),
            (apply_spread_rule(_price_series(interval=Interval(_stamp(0), _stamp(135))), identification), identification),
            (apply_spread_rule(_price_series(cutoff=_stamp(120)), identification), identification),
        ]
        other_id = _identification(stratum=other_stratum)
        variants.append((apply_spread_rule(_price_series(stratum=other_stratum), other_id), other_id))
        for bad_id in (
            _identification(control_source=replace(_identity("M5"), provenance_sha256="8" * 64)),
            _identification(native_source=replace(_identity("M15"), sidecar_sha256="7" * 64)),
            _identification(interval=Interval(_stamp(0), _stamp(135))),
        ):
            variants.append((apply_spread_rule(_price_series(), bad_id), bad_id))
        for bad_series, bad_id in variants:
            with self.subTest(source=bad_series.source, interval=bad_series.interval,
                              stratum=bad_series.stratum, control=bad_id.evidence_sha256):
                self.assert_publication_refusal(
                    lambda: publisher.add_series(bad_series, bad_id, statistics=stats),
                    "EDITION_BINDING_MISMATCH")
        publisher.add_series(series, identification, statistics=stats)
        self.reopen(context, publisher.finish(), plan_sha)

    def test_frozen_plan_is_not_replaced_by_caller_dictionary_mutation(self):
        series, identification = _target()
        plan = _plan(series=series, identification=identification)
        context, publisher, plan_sha = self.make_publisher(plan)
        plan["native_plan"]["targets"][0]["m5"]["source_identity"]["bars_sha256"] = "9" * 64
        publisher.plan["native_plan"]["targets"][0]["interval"]["end_exclusive_utc"] = _stamp(135)
        publisher.add_series(series, identification, statistics=calibrated_statistics(series, _fine()))
        self.reopen(context, publisher.finish(), plan_sha)

    def test_cold_plan_join_rejects_valid_spool_graph_with_wrong_frozen_metadata(self):
        baseline = self.publish()
        mutations = (
            lambda p: p["native_plan"]["targets"][0]["m5"]["source_identity"].update(bars_sha256="9" * 64),
            lambda p: p["native_plan"]["targets"][0]["interval"].update(end_exclusive_utc=_stamp(135)),
            lambda p: p["native_plan"]["targets"][0]["stratum"].update(instrument_session_class="SYNTHETIC_OTHER"),
            lambda p: p["native_plan"].update(cutoff_utc=_stamp(120)),
            lambda p: p["native_plan"]["controls"][0]["m5"]["source_identity"].update(provenance_sha256="8" * 64),
            lambda p: p["native_plan"]["controls"][0]["native"]["source_identity"].update(sidecar_sha256="7" * 64),
            lambda p: p["native_plan"]["controls"][0]["interval"].update(end_exclusive_utc=_stamp(135)),
        )
        for index, mutation in enumerate(mutations):
            with self.subTest(frozen_metadata_mutation=index):
                context, receipt, plan_sha = self.reseal_single_entry_fixture(baseline, change_plan=mutation)
                self.manifest(context, receipt, plan_sha)
                self.assert_publication_refusal(lambda: self.reopen(context, receipt, plan_sha),
                                                "EDITION_BINDING_MISMATCH")

    def test_cold_reader_reconciles_groups_and_unavailable_one_to_one(self):
        baseline = self.publish()
        def phantom_group(profile):
            extra = deepcopy(profile["groups"][0])
            extra["symbol"] = "SYNTHETIC_UNPLANNED"
            profile["groups"].append(extra)
        def duplicate_group(profile):
            profile["groups"].append(deepcopy(profile["groups"][0]))
        def phantom_unavailable(profile):
            profile["unavailable"].append({"symbol": "SYNTHETIC_UNPLANNED", "timeframe": "H4",
                                           "status": "STOP", "reason_code": "SYNTHETIC_STOP",
                                           "cost_status": "COST_UNAVAILABLE"})
        for mutation in (phantom_group, duplicate_group, phantom_unavailable):
            with self.subTest(profile_mutation=mutation.__name__):
                context, receipt, plan_sha = self.reseal_single_entry_fixture(baseline, change_profile=mutation)
                self.manifest(context, receipt, plan_sha)
                self.assert_publication_refusal(lambda: self.reopen(context, receipt, plan_sha),
                                                "EDITION_BINDING_MISMATCH")
        context, publisher, plan_sha = self.make_publisher(_plan("H4"))
        publisher.stop(_SYMBOL, "H4", "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR")
        stopped = (context, publisher.finish(), plan_sha, None, None)
        context, receipt, plan_sha = self.reseal_single_entry_fixture(
            stopped, change_profile=lambda p: p["unavailable"].clear())
        self.assert_publication_refusal(lambda: self.reopen(context, receipt, plan_sha),
                                        "EDITION_BINDING_MISMATCH")


    def test_each_target_and_control_extent_assertion_is_bound_hot(self):
        series, identification = _target()
        context, publisher, plan_sha = self.make_publisher()
        stats = calibrated_statistics(series, _fine())
        for field in _EXTENT_FIELDS:
            for kind, baseline in (("extent", _extent()), ("control_extent", _extent(0))):
                with self.subTest(field=field, extent_kind=kind):
                    changed = deepcopy(baseline)
                    if field == "omitted_intervals":
                        changed[field] = [] if changed[field] else [{"start_utc": _stamp(-15), "end_exclusive_utc": _stamp(0)}]
                    elif field == "required_interval":
                        changed[field]["start_utc"] = _stamp(-75)
                        changed["omitted_intervals"] = [{"start_utc": _stamp(-75), "end_exclusive_utc": _stamp(0)}]
                    elif field == "available_interval":
                        changed[field]["end_exclusive_utc"] = _stamp(105)
                        changed["omitted_intervals"].append({"start_utc": _stamp(105), "end_exclusive_utc": _stamp(120)})
                    else:
                        changed[field]["start_utc"] = _stamp(-5)
                    self.assert_publication_refusal(
                        lambda: publisher.add_series(series, identification, statistics=stats, **{kind: changed}),
                        "EDITION_BINDING_MISMATCH")
        wrong_spec = _target_spec(series)
        wrong_spec["native_entry_sha256"] = "9" * 64
        self.assert_publication_refusal(
            lambda: publisher.add_series(series, identification, statistics=stats, target_spec=wrong_spec),
            "EDITION_BINDING_MISMATCH")
        publisher.add_series(series, identification, statistics=stats,
                             target_spec=_target_spec(series), extent=_extent(), control_extent=_extent(0))
        self.reopen(context, publisher.finish(), plan_sha)

    def test_each_published_extent_field_is_bound_cold_even_with_valid_spool_hashes(self):
        baseline = self.publish()
        for field in _EXTENT_FIELDS:
            for location in ("entry", "profile", "lineage"):
                with self.subTest(field=field, location=location):
                    def mutation(value):
                        if field == "omitted_intervals":
                            value[field] = []
                        else:
                            value[field]["start_utc"] = _stamp(-75)
                    kwargs = {}
                    if location == "entry":
                        kwargs["change_entry"] = mutation
                    elif location == "profile":
                        kwargs["change_profile"] = lambda profile: mutation(profile["groups"][0])
                    else:
                        kwargs["change_lineage"] = mutation
                    context, receipt, plan_sha = self.reseal_single_entry_fixture(baseline, **kwargs)
                    self.manifest(context, receipt, plan_sha)
                    self.assert_publication_refusal(lambda: self.reopen(context, receipt, plan_sha),
                                                    "EDITION_BINDING_MISMATCH")
        for field in _EXTENT_FIELDS:
            with self.subTest(control_extent_field=field):
                def mutation(value):
                    if field == "omitted_intervals":
                        value["control_extent"][field] = [{"start_utc": _stamp(-15), "end_exclusive_utc": _stamp(0)}]
                    else:
                        value["control_extent"][field]["start_utc"] = _stamp(-75)
                context, receipt, plan_sha = self.reseal_single_entry_fixture(baseline, change_entry=mutation)
                self.manifest(context, receipt, plan_sha)
                self.assert_publication_refusal(lambda: self.reopen(context, receipt, plan_sha),
                                                "EDITION_BINDING_MISMATCH")

    def test_target_spec_and_eligibility_labels_cannot_drift_cold(self):
        baseline = self.publish()
        mutations = (
            lambda row: row["target_spec"].update(native_entry_sha256="9" * 64),
            lambda row: row["target_spec"].update(native_status="evidence_candidate"),
            lambda row: row.update(readiness_label="EXECUTION_READY"),
            lambda row: row.update(data_scope="BROKER_NATIVE"),
            lambda row: row.update(full_native_context_eligibility=True),
            lambda row: row.update(financial_consumer_ready=True),
        )
        for mutation in mutations:
            context, receipt, plan_sha = self.reseal_single_entry_fixture(baseline, change_entry=mutation)
            self.assert_publication_refusal(lambda: self.reopen(context, receipt, plan_sha),
                                            "EDITION_BINDING_MISMATCH")

    def test_fully_covered_synthetic_context_still_is_not_native_or_execution_ready(self):
        series, identification = _target()
        context, publisher, plan_sha = self.make_publisher(
            _plan(series=series, identification=identification, fully_covered=True))
        publisher.add_series(series, identification, statistics=calibrated_statistics(series, _fine()))
        edition = self.reopen(context, publisher.finish(), plan_sha)
        entry = require_cost_ready(edition, _SYMBOL, "M15")
        for row in (entry, entry["profile"]):
            self.assert_data_only(row)
            self.assertEqual(row["required_interval"], row["available_interval"])
            self.assertEqual(row["omitted_intervals"], [])

    def test_known_target_stop_retains_required_available_and_omitted_context(self):
        context, publisher, plan_sha = self.make_publisher()
        publisher.stop(_SYMBOL, "M15", "SYNTHETIC_CONTROL_FAILURE")
        edition = self.reopen(context, publisher.finish(), plan_sha)
        entry = edition["entries"][0]
        self.assert_data_only(entry)
        self.assertNotIn("bars", entry)
        for name in _EXTENT_FIELDS:
            self.assertEqual(entry[name], _extent()[name])
        self.assertEqual(entry["control_extent"], _extent(0))
        self.assertEqual(entry["target_spec"]["native_status"], "technical_error")
        self.assert_publication_refusal(lambda: require_cost_ready(edition, _SYMBOL, "M15"),
                                        "DERIVED_COST_UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()
