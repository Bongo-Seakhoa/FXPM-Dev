"""Invented retained-spool contract fixtures, not native authentication evidence.

This source is opt-in. The plain source identities and EOF report below are
synthetic join inputs; only the original spool's own file custody is exercised.
No native authority verifier, native CSV, profile, calculation or model is used.
"""
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import unittest
import uuid

from pm_authority_spool import (
    SpoolLimits, AuthoritySpoolError, freeze_generation_context,
    open_symbol_sink, verify_symbol_receipt, GENERAL_MANIFEST_DOMAIN,
    CANONICAL_JSON_FORMAT,
)
from pm_explicit_derived_history import Bar, Interval, SourceIdentity, canonical_json_bytes
from pm_explicit_observed_clock import (
    ClockStratum, ClockExtent, ObservedClockError, CLOCK_POLICY, PROTECTED_CUTOFF,
    freeze_observed_clock, derive_on_observed_clock, identify_clock_spread_rule,
    apply_clock_spread_rule,
)
from pm_explicit_clock_publication import (
    ClockEditionPublisher, ClockPublicationError, reopen_clock_edition,
    EDITION_SYMBOL, PLAN_SCHEMA, RUNTIME_SCHEMA, SOURCE_CONTEXT_SCHEMA, CONTROL_SCHEMA,
    PROFILE_DISPOSITION, REQUEST_SEMANTICS, R3_RESTRICTION, AUTHENTICATION, SCOPE,
    _checked_plan,
)


CHILD_SCOPE = {name: value for name, value in SCOPE.items() if name != "source_kind"}


def _digest(value):
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _copy(value):
    return json.loads(canonical_json_bytes(value))


def _stamp(minutes):
    return (datetime(2024, 1, 2, tzinfo=timezone.utc) + timedelta(minutes=minutes)).isoformat(timespec="seconds")


def _interval(start, end):
    return Interval(_stamp(start), _stamp(end))


def _source(symbol, timeframe):
    return SourceIdentity(symbol, timeframe, "SYNTHETIC-NOT-NATIVE-AUTHORITY",
        _digest("activation"), _digest([symbol, timeframe, "bars"]),
        _digest([symbol, timeframe, "sidecar"]), _digest([symbol, timeframe, "provenance"]),
        _digest([symbol, timeframe, "request"]), 0.0001)


def _cell(symbol, timeframe):
    cell = {"symbol": symbol, "timeframe": timeframe, "native_status": "evidence_candidate",
        "source_identity": asdict(_source(symbol, timeframe)),
        "bars_path": "SYNTHETIC_NOT_READ/" + symbol + "/" + timeframe + ".csv",
        "sidecar_path": "SYNTHETIC_NOT_READ/sidecar.json",
        "provenance_path": "SYNTHETIC_NOT_READ/provenance.json",
        "observed_interval": asdict(_interval(0, 480)),
        "expected_rows": 6, "expected_bytes": 123, "cutoff_utc": PROTECTED_CUTOFF,
        "metadata_digest": _digest([symbol, timeframe, "metadata"]),
        "bars_consumed": False, "consumption_planned": (symbol, timeframe) != ("AAA", "M5")}
    if timeframe == "M5":
        cell.update(interval=asdict(_interval(0, 480)), stratum={"timeframe": "M5",
            "instrument_session_class": "SYNTHETIC_CLASS", "alignment_identity": "SYNTHETIC_FIXED_M5_LABEL"},
            geometry={"acquisition_interval": asdict(_interval(0, 480)),
                "source_prefix_interval": asdict(_interval(0, 480)), "interval": asdict(_interval(0, 480)),
                "excluded_partial_edges": {"leading": None, "trailing": None}})
    else:
        cell.update(acquisition_interval=asdict(_interval(0, 480)), prefix_interval=asdict(_interval(0, 480)),
            broker_source_sha256=_digest("broker"), broker_policy_sha256=_digest("coarse-policy"),
            instrument_session_class="SYNTHETIC_CLASS", alignment_mode="SYNTHETIC_COARSE_SESSION",
            request_sha256=_digest("request"), calendar_sha256=_digest("calendar"))
    return cell


def _roster_row(symbol):
    native, m5 = ({key: value for key, value in _cell(symbol, tf).items()
                   if key not in ("bars_consumed", "consumption_planned")} for tf in ("H4", "M5"))
    return {"symbol": symbol, "native": native, "m5": m5,
        "native_source": native["source_identity"], "m5_source": m5["source_identity"],
        "catalog_class": "SYNTHETIC_CLASS", "point": 0.0001,
        "broker_source_sha256": _digest("broker"), "coarse_broker_policy": _digest("coarse-policy"),
        "native_status": "evidence_candidate", "m5_status": "evidence_candidate",
        "native_metadata_sha256": native["metadata_digest"], "m5_metadata_sha256": m5["metadata_digest"]}


def _extent(required_start=0):
    return ClockExtent(_interval(required_start, 480), _interval(0, 480), _interval(0, 480),
        _interval(0, 480), (_interval(required_start, 0),) if required_start < 0 else ())


def _m5_rows():
    result = []
    for ordinal, start in enumerate((0, 180)):
        for index, spread in enumerate((2 + ordinal, 1, 7 + ordinal)):
            price = 1 + ordinal * 0.1 + index * 0.01
            result.append(Bar(_stamp(start + index * 5), _stamp(start + (index + 1) * 5),
                price, price + 0.03, price - 0.03, price + 0.01, 1, 2, spread))
    return tuple(result)


def _fixture():
    stratum = ClockStratum("H4", _digest("broker"), "SYNTHETIC_CLASS", CLOCK_POLICY)
    group = {"stratum": asdict(stratum), "common_point": 0.0001,
        "coarse_broker_policy": _digest("coarse-policy"),
        "coarse_alignment_mode": "SYNTHETIC_COARSE_SESSION",
        "metadata_common_interval": asdict(_interval(0, 480)),
        "control_extent": asdict(_extent()), "target_symbols_excluded": ["ZZZ"],
        "c_native": _cell("AAA", "H4"), "c_m5": _cell("AAA", "M5"),
        "d_native": _cell("BBB", "H4"), "d_m5": _cell("BBB", "M5"),
        "eligible_roster": [_roster_row(symbol) for symbol in ("AAA", "BBB", "CCC")],
        "template": None, "resolution_status": "METADATA_ONLY_C_AXES_NOT_READ", **CHILD_SCOPE}
    sid = _digest({"stratum": group["stratum"], "common_point": group["common_point"],
        "coarse_broker_policy": group["coarse_broker_policy"], "r3_restriction": R3_RESTRICTION})
    group["stratum_id"] = sid
    target = {"symbol": "ZZZ", "timeframe": "H4", "stratum_id": sid, "stratum": asdict(stratum),
        "m5": _cell("ZZZ", "M5"), "extent": asdict(_extent(-60)),
        "common_point": 0.0001, "coarse_broker_policy": _digest("coarse-policy"),
        "broker_source_sha256": _digest("broker"),
        "requested_interval_semantics": REQUEST_SEMANTICS, **CHILD_SCOPE,
        "target_spec": {"symbol": "ZZZ", "timeframe": "H4", "native_status": "technical_error",
            "native_reason_codes": ["SYNTHETIC_MISSING_NATIVE"],
            "target_source_request_interval": asdict(_interval(-60, 480)),
            "point": 0.0001, "instrument_session_class": "SYNTHETIC_CLASS",
            "source_identity_from": "ACCEPTED_SAME_SYMBOL_M5"}}
    authority = {"activation_id": "SYNTHETIC-NOT-NATIVE-AUTHORITY",
        **{name: _digest(name) for name in ("pointer_sha256", "run_manifest_sha256",
            "semantic_receipt_sha256", "semantic_verifier_build_digest")},
        "activation_sha256": _digest("activation")}
    plan = {"schema_version": PLAN_SCHEMA, **SCOPE,
        "profile_disposition": PROFILE_DISPOSITION, "cutoff_utc": PROTECTED_CUTOFF,
        "requested_interval_semantics": REQUEST_SEMANTICS, "authority_bindings": authority,
        "metadata_read_pins": [], "strata": [group], "targets": [target], "stops": [],
        "requested_targets": [{"symbol": "ZZZ", "timeframe": "H4"}],
        "provenance": "INVENTED_JOIN_FIXTURE_NO_NATIVE_AUTHENTICATION"}
    template = freeze_observed_clock((_interval(0, 180), _interval(180, 480)), stratum=stratum,
        clock_source=_source("AAA", "H4"), corroboration_source=_source("BBB", "H4"),
        interval=_interval(0, 480))
    runtime = {"schema_version": RUNTIME_SCHEMA, "stratum_id": sid,
        "template_sha256": template.template_sha256, "available_interval": asdict(template.interval),
        "control_extent": asdict(_extent()),
        "target_extents": [{"symbol": "ZZZ", "timeframe": "H4", "extent": asdict(_extent(-60))}]}

    d_series = derive_on_observed_clock(_m5_rows(), template=template,
        source=_source("BBB", "M5"), extent=_extent())
    native = tuple(replace(row.value, spread=row.spread_candidates[0]) for row in d_series.bars)
    identification = identify_clock_spread_rule(d_series, native, native_source=_source("BBB", "H4"))
    target_series = derive_on_observed_clock(_m5_rows(), template=template,
        source=_source("ZZZ", "M5"), extent=_extent(-60))
    target_series = apply_clock_spread_rule(target_series, identification)
    return plan, sid, template, runtime, d_series, native, identification, target_series


class ExplicitClockPublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        raw = os.environ.get("FXPM_DERIVED_QA_ROOT")
        if raw is None:
            raise unittest.SkipTest("requires explicitly precreated retained QA root")
        root = Path(raw)
        if not raw or not root.is_absolute() or not root.is_dir() or root.is_symlink() or (
                getattr(root.lstat(), "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)):
            raise RuntimeError("FXPM_DERIVED_QA_ROOT must be an existing absolute owned directory")
        cls.qa_root = root

    def setUp(self):
        (self.plan, self.sid, self.template, self.runtime, self.d_series,
         self.native, self.identification, self.series) = _fixture()

    def _root(self):
        root = self.qa_root / ("clock-publication-" + uuid.uuid4().hex)
        root.mkdir(exist_ok=False)
        return root

    def _context(self, plan=None):
        return freeze_generation_context(self._root(), generation_id="SYNTHETIC_CLOCK_PUBLICATION",
            native_universe_sha256=_digest("synthetic-universe-not-native-authority"),
            config_sha256=_digest(self.plan if plan is None else plan),
            limits=SpoolLimits(max_artifact_bytes=256 * 1024, max_json_bytes=256 * 1024,
                max_manifest_bytes=256 * 1024, max_symbol_bytes=2 * 1024 * 1024,
                max_generation_bytes=4 * 1024 * 1024, max_artifacts_per_symbol=128,
                stream_chunk_bytes=4096, min_free_bytes=1))

    def _publisher(self, plan=None):
        context = self._context(plan)
        return ClockEditionPublisher(context, plan=self.plan if plan is None else plan,
            implementation_sha256=_digest("synthetic-implementation"))

    def _compared(self):
        publisher = self._publisher()
        publisher.freeze_template(self.sid, self.template, self.runtime)
        publisher.control_compared(stratum_id=self.sid, template=self.template,
            d_series=self.d_series, d_native_bars=self.native, identification=self.identification)
        return publisher

    def _report(self, publisher, *, target=True):
        # Invented plain report tests the join contract, never a real EOF assertion.
        group = self.plan["strata"][0]
        cells = [(role, group[role]) for role in ("c_native", "d_native", "d_m5")]
        if target:
            cells.append(("target_m5", self.plan["targets"][0]["m5"]))
        return {"schema_version": SOURCE_CONTEXT_SCHEMA,
            **CHILD_SCOPE,
            "authority_bindings": self.plan["authority_bindings"],
            "authentication_semantics": AUTHENTICATION,
            "full_native_context_eligibility": False, "financial_consumer_ready": False,
            **{name: False for name in ("qualification_authority", "capital_authority", "live_authority", "order_authority")},
            "provenance": "SYNTHETIC_PLAIN_JOIN_INPUT_NOT_AUTHENTICATED_NATIVE_EOF",
            "metadata_read_pins": [{"path": cell["bars_path"], "sha256": cell["source_identity"]["bars_sha256"],
                "bytes": cell["expected_bytes"], "rows": cell["expected_rows"], "kind": "csv",
                "stat_identity": [1, 2, 3]} for _, cell in cells],
            "consumed_roles": [{"role": role, "stratum_id": self.sid, "symbol": cell["symbol"],
                "timeframe": cell["timeframe"], "source_identity": cell["source_identity"],
                "bars_path": cell["bars_path"], "expected_rows": cell["expected_rows"],
                "expected_bytes": cell["expected_bytes"], "bars_consumed": True} for role, cell in cells]}

    def _finished(self):
        publisher = self._compared()
        publisher.add_series(self.series, self.identification)
        publisher.bind_source_context(self._report(publisher))
        return publisher, publisher.finish()

    def _reopen(self, publisher, receipt, **overrides):
        expected = {"expected_generation_id": publisher.context.generation_id,
            "expected_native_universe_sha256": publisher.context.native_universe_sha256,
            "expected_plan_sha256": publisher.plan_sha256,
            "expected_implementation_sha256": publisher.implementation_sha256}
        return reopen_clock_edition(publisher.context.root, receipt, **{**expected, **overrides})

    def _reseal_json_mutation(self, publisher, receipt, artifact_id, mutate):
        """Create another genuine owned spool, changing one semantic JSON body."""
        manifest = verify_symbol_receipt(publisher.context.root, receipt,
            expected_generation_id=publisher.context.generation_id, expected_symbol=EDITION_SYMBOL,
            expected_native_universe_sha256=publisher.context.native_universe_sha256,
            expected_config_sha256=publisher.plan_sha256,
            expected_manifest_domain=GENERAL_MANIFEST_DOMAIN)
        context = self._context()
        sink = open_symbol_sink(context, EDITION_SYMBOL, manifest_domain=GENERAL_MANIFEST_DOMAIN)
        replacements = {}

        def references(value):
            if type(value) is dict:
                if value.get("artifact_id") in replacements and "descriptor" in value:
                    return replacements[value["artifact_id"]]
                return {key: references(item) for key, item in value.items()}
            if type(value) is list:
                return [references(item) for item in value]
            return value

        rows = sorted(manifest["artifacts"], key=lambda row: row["artifact_id"] == "activation")
        for row in rows:
            descriptor = row["descriptor"]
            raw = (publisher.context.root / descriptor["relative_path"]).read_bytes()
            if descriptor["format"] == CANONICAL_JSON_FORMAT:
                body = references(json.loads(raw))
                if row["artifact_id"] == artifact_id:
                    mutate(body)
                new = sink.put_json(row["artifact_id"], row["artifact_type"], body)
            else:
                new = sink.put_opaque(row["artifact_id"], row["artifact_type"], (raw,), format=descriptor["format"])
            replacements[row["artifact_id"]] = new
        receipt = sink.seal()
        return context, receipt

    def test_original_spool_roundtrip_is_price_only_detached_and_has_no_profile(self):
        publisher, receipt = self._finished()
        edition = self._reopen(publisher, receipt)
        self.assertEqual(edition["entries"][0]["bars"], [asdict(x.value) for x in self.series.bars])
        self.assertEqual(edition["profile_disposition"], PROFILE_DISPOSITION)
        self.assertIsNone(edition["statistics"])
        self.assertNotIn("profile", edition)
        for name, expected in SCOPE.items():
            self.assertEqual(edition[name], expected)
            self.assertEqual(edition["entries"][0][name], expected)
        row = edition["entries"][0]
        self.assertEqual(row["required_interval"], asdict(_interval(-60, 480)))
        self.assertEqual(row["omitted_intervals"], [asdict(_interval(-60, 0))])
        row["target_spec"]["symbol"] = "MUTATED_OUTPUT"
        self.assertEqual(self._reopen(publisher, receipt)["entries"][0]["target_spec"]["symbol"], "ZZZ")

    def test_metadata_plan_is_persisted_before_template_and_input_mutation_is_private(self):
        original = _copy(self.plan)
        publisher = self._publisher()
        self.assertEqual(publisher.sink.artifact_roster()[0]["artifact_id"], "plan")
        self.plan["targets"][0]["target_spec"]["symbol"] = "MUTATED_INPUT"
        publisher.freeze_template(self.sid, self.template, self.runtime)
        stored = json.loads((publisher.context.root / publisher.plan_artifact["descriptor"]["relative_path"]).read_bytes())
        self.assertEqual(stored["targets"][0]["target_spec"]["symbol"], "ZZZ")
        self.assertEqual(original["strata"][0]["template"], None)

    def test_rank_reselection_common_point_policy_class_and_target_distinct_refuse(self):
        for mutation in ("rank", "point", "policy", "class", "target"):
            with self.subTest(mutation=mutation):
                plan = _copy(self.plan)
                group = plan["strata"][0]
                if mutation == "rank":
                    group["eligible_roster"][0], group["eligible_roster"][1] = group["eligible_roster"][1], group["eligible_roster"][0]
                elif mutation == "point":
                    group["d_m5"]["source_identity"]["point"] = 0.01
                elif mutation == "policy":
                    group["c_native"]["broker_policy_sha256"] = _digest("other")
                elif mutation == "class":
                    plan["targets"][0]["m5"]["stratum"]["instrument_session_class"] = "OTHER"
                else:
                    plan["targets"][0]["symbol"] = "AAA"
                with self.assertRaises(ClockPublicationError):
                    self._publisher(plan)

    def test_runtime_context_cannot_change_required_source_or_available_bounds(self):
        for field in ("required_interval", "m5_acquisition_interval", "available_interval", "omitted_intervals"):
            with self.subTest(field=field):
                publisher = self._publisher()
                runtime = _copy(self.runtime)
                runtime["target_extents"][0]["extent"][field] = [] if field == "omitted_intervals" else asdict(_interval(-120, 480))
                with self.assertRaises(ClockPublicationError):
                    publisher.freeze_template(self.sid, self.template, runtime)

    def test_actual_d_calendar_ohlcv_must_pass_before_identification_is_recorded(self):
        publisher = self._publisher()
        publisher.freeze_template(self.sid, self.template, self.runtime)
        bad = (replace(self.native[0], high=self.native[0].high + 1), *self.native[1:])
        with self.assertRaises(ObservedClockError):
            publisher.control_compared(stratum_id=self.sid, template=self.template,
                d_series=self.d_series, d_native_bars=bad, identification=self.identification)
        self.assertFalse(any(row["artifact_id"].startswith("control:") for row in publisher.sink.artifact_roster()))

    def test_supplied_identification_cannot_replace_recomputed_d_result(self):
        publisher = self._publisher()
        publisher.freeze_template(self.sid, self.template, self.runtime)
        with self.assertRaises(ClockPublicationError):
            publisher.control_compared(stratum_id=self.sid, template=self.template,
                d_series=self.d_series, d_native_bars=self.native,
                identification=replace(self.identification, evidence_sha256=_digest("forged-self-hash")))

    def test_strict_clock_type_no_control_and_statistics_refuse(self):
        publisher = self._publisher()
        with self.assertRaises(ClockPublicationError):
            publisher.add_series({"source": asdict(self.series.source)}, self.identification)
        with self.assertRaises(ClockPublicationError):
            publisher.add_series(self.series, self.identification)
        publisher = self._compared()
        with self.assertRaises(ClockPublicationError) as caught:
            publisher.add_series(self.series, self.identification, statistics={"tiers": {"P75": 99}})
        self.assertEqual(caught.exception.code, "CLOCK_PROFILE_NOT_ADOPTED")

    def test_self_consistent_target_source_and_extent_cannot_replace_frozen_plan(self):
        for attack in ("source", "extent"):
            with self.subTest(attack=attack):
                publisher = self._compared()
                source = replace(self.series.source, bars_sha256=_digest("other-source")) if attack == "source" else self.series.source
                extent = _extent(-120) if attack == "extent" else self.series.extent
                changed = derive_on_observed_clock(_m5_rows(), template=self.template,
                    source=source, extent=extent)
                changed = apply_clock_spread_rule(changed, self.identification)
                with self.assertRaises(ClockPublicationError) as caught:
                    publisher.add_series(changed, self.identification)
                self.assertEqual(caught.exception.code, "CLOCK_TARGET_JOIN_MISMATCH")

    def test_source_context_requires_real_role_shape_csv_pin_and_frozen_authority(self):
        for mutation in ("missing-target", "metadata-not-csv", "authority", "hash"):
            with self.subTest(mutation=mutation):
                publisher = self._compared()
                publisher.add_series(self.series, self.identification)
                report = self._report(publisher)
                if mutation == "missing-target":
                    report["consumed_roles"].pop()
                elif mutation == "metadata-not-csv":
                    report["metadata_read_pins"][0]["kind"] = "metadata"
                elif mutation == "authority":
                    report = _copy(report)
                    report["authority_bindings"]["activation_id"] = "OTHER"
                else:
                    report["metadata_read_pins"][0]["sha256"] = _digest("other")
                with self.assertRaises(ClockPublicationError):
                    publisher.bind_source_context(report)

    def test_metadata_only_c_m5_is_not_a_required_consumed_role(self):
        publisher, receipt = self._finished()
        report = self._reopen(publisher, receipt)["source_context"]
        self.assertNotIn("c_m5", [row["role"] for row in report["consumed_roles"]])

    def test_missing_spread_remains_publishable_price_data_with_no_cost_or_profile_claim(self):
        publisher = self._publisher()
        publisher.freeze_template(self.sid, self.template, self.runtime)
        native = tuple(replace(row, spread=None) for row in self.native)
        identification = identify_clock_spread_rule(self.d_series, native,
            native_source=_source("BBB", "H4"))
        self.assertEqual(identification.cost_status, "COST_UNAVAILABLE")
        publisher.control_compared(stratum_id=self.sid, template=self.template,
            d_series=self.d_series, d_native_bars=native, identification=identification)
        series = derive_on_observed_clock(_m5_rows(), template=self.template,
            source=self.series.source, extent=self.series.extent)
        publisher.add_series(apply_clock_spread_rule(series, identification), identification)
        publisher.bind_source_context(self._report(publisher))
        edition = self._reopen(publisher, publisher.finish())
        self.assertEqual(edition["entries"][0]["cost_status"], "PRICE_ONLY")
        self.assertTrue(all(row["spread"] is None for row in edition["entries"][0]["bars"]))
        self.assertIsNone(edition["statistics"])
        self.assertFalse(edition["financial_consumer_ready"])

    def test_stop_without_template_has_unknown_extent_and_no_fictitious_bars(self):
        publisher = self._publisher()
        publisher.stop("ZZZ", "H4", "SYNTHETIC_C_UNAVAILABLE", {"synthetic": True})
        report = self._report(publisher, target=False)
        report["consumed_roles"] = []
        report["metadata_read_pins"] = []
        publisher.bind_source_context(report)
        edition = self._reopen(publisher, publisher.finish())
        row = edition["entries"][0]
        self.assertEqual(row["status"], "STOP")
        self.assertNotIn("bars", row)
        self.assertIsNone(row["available_interval"])
        self.assertEqual(row["required_interval"], asdict(_interval(-60, 480)))
        self.assertEqual(row["extent_resolution"], "METADATA_ONLY_NO_C_TEMPLATE")
        self.assertFalse(row["financial_consumer_ready"])

    def test_finish_requires_all_results_then_bound_context_and_is_single_use(self):
        publisher = self._compared()
        with self.assertRaises(ClockPublicationError):
            publisher.bind_source_context(self._report(publisher))
        with self.assertRaises(ClockPublicationError):
            publisher.finish()
        publisher.add_series(self.series, self.identification)
        publisher.bind_source_context(self._report(publisher))
        with self.assertRaises(ClockPublicationError):
            publisher.diagnostic("after-context", {"synthetic": True})
        publisher.finish()
        with self.assertRaises(ClockPublicationError):
            publisher.finish()

    def test_cold_expected_plan_implementation_and_generation_bindings_refuse(self):
        publisher, receipt = self._finished()
        for field in ("expected_plan_sha256", "expected_implementation_sha256", "expected_generation_id"):
            with self.subTest(field=field):
                with self.assertRaises((ClockPublicationError, AuthoritySpoolError)):
                    self._reopen(publisher, receipt, **{field: "OTHER" if field.endswith("id") else _digest("other")})

    def test_cold_d_recomputes_actual_fidelity_even_with_valid_original_spool(self):
        publisher, receipt = self._finished()
        context, changed = self._reseal_json_mutation(publisher, receipt, "control:" + self.sid,
            lambda body: body["native_bars"][0].update(high=body["native_bars"][0]["high"] + 1))
        with self.assertRaises(ObservedClockError):
            reopen_clock_edition(context.root, changed, expected_generation_id=context.generation_id,
                expected_native_universe_sha256=context.native_universe_sha256,
                expected_plan_sha256=publisher.plan_sha256,
                expected_implementation_sha256=publisher.implementation_sha256)

    def test_cold_lineage_context_and_entry_roster_changes_refuse(self):
        for attack in ("lineage", "context", "duplicate"):
            with self.subTest(attack=attack):
                publisher, receipt = self._finished()
                if attack == "lineage":
                    artifact = _digest(["ZZZ", "H4"])[:24] + ":lineage"

                    def mutate(body):
                        body["required_interval"] = asdict(_interval(0, 480))
                        body.pop("lineage_sha256")
                        body["lineage_sha256"] = _digest(body)
                else:
                    artifact = "activation"

                    def mutate(body):
                        if attack == "context":
                            body["entries"][0]["full_native_context_eligibility"] = True
                        else:
                            body["entries"].append(_copy(body["entries"][0]))
                context, changed = self._reseal_json_mutation(publisher, receipt, artifact, mutate)
                with self.assertRaises(ClockPublicationError):
                    reopen_clock_edition(context.root, changed, expected_generation_id=context.generation_id,
                        expected_native_universe_sha256=context.native_universe_sha256,
                        expected_plan_sha256=publisher.plan_sha256,
                        expected_implementation_sha256=publisher.implementation_sha256)

    def test_original_spool_detects_owned_artifact_corruption(self):
        publisher, receipt = self._finished()
        entry = next(row for row in publisher.sink.artifact_roster() if row["artifact_type"] == "clock_price_bars")
        path = publisher.context.root / entry["descriptor"]["relative_path"]
        path.write_bytes(path.read_bytes() + b"SYNTHETIC_CORRUPTION\n")
        with self.assertRaises(AuthoritySpoolError):
            self._reopen(publisher, receipt)

    def test_child_plan_and_source_context_scope_flags_require_exact_types_and_values(self):
        plan = _copy(self.plan)
        plan["stops"] = [{"symbol": "SYN_STOP", "timeframe": "D1",
            "reason_code": "SYNTHETIC_METADATA_STOP", **CHILD_SCOPE}]
        plan["requested_targets"].append({"symbol": "SYN_STOP", "timeframe": "D1"})
        plan["unavailable_strata"] = [{"reason_code": "SYNTHETIC_METADATA_GROUP_UNAVAILABLE",
            **CHILD_SCOPE}]
        # Constructor and cold reader share this pure plan validator.
        _checked_plan(plan)
        for section in ("strata", "targets", "stops", "unavailable_strata"):
            for flag, expected in CHILD_SCOPE.items():
                for mutation in (True, 0, "missing") + (("WRONG_SCOPE",) if type(expected) is str else ()):
                    with self.subTest(section=section, flag=flag, mutation=mutation):
                        changed = _copy(plan)
                        if mutation == "missing":
                            changed[section][0].pop(flag)
                        else:
                            changed[section][0][flag] = mutation
                        with self.assertRaises(ClockPublicationError):
                            _checked_plan(changed)
        invalid_roster = _copy(plan)
        invalid_roster["unavailable_strata"] = {}
        with self.assertRaises(ClockPublicationError):
            _checked_plan(invalid_roster)
        publisher = self._compared()
        publisher.add_series(self.series, self.identification)
        baseline = self._report(publisher)
        for flag, expected in CHILD_SCOPE.items():
            for mutation in (True, 0, "missing") + (("WRONG_SCOPE",) if type(expected) is str else ()):
                with self.subTest(source_context_flag=flag, mutation=mutation):
                    report = _copy(baseline)
                    if mutation == "missing":
                        report.pop(flag)
                    else:
                        report[flag] = mutation
                    with self.assertRaises(ClockPublicationError):
                        publisher.bind_source_context(report)
        publisher.bind_source_context(baseline)

    def test_consumption_report_rejects_unplanned_and_non_true_roles(self):
        publisher = self._compared()
        publisher.add_series(self.series, self.identification)
        baseline = self._report(publisher)
        for mutation in (False, 0, 1, "missing"):
            with self.subTest(consumed=mutation):
                report = _copy(baseline)
                if mutation == "missing":
                    report["consumed_roles"][0].pop("bars_consumed")
                else:
                    report["consumed_roles"][0]["bars_consumed"] = mutation
                with self.assertRaises(ClockPublicationError) as caught:
                    publisher.bind_source_context(report)
                self.assertEqual(caught.exception.code, "CLOCK_CONSUMPTION_NOT_PLANNED")
        report = _copy(baseline)
        cell = self.plan["strata"][0]["c_m5"]
        report["consumed_roles"].append({"role": "c_m5", "stratum_id": self.sid,
            "symbol": cell["symbol"], "timeframe": cell["timeframe"],
            "source_identity": cell["source_identity"], "bars_path": cell["bars_path"],
            "expected_rows": cell["expected_rows"], "expected_bytes": cell["expected_bytes"],
            "bars_consumed": True})
        report["metadata_read_pins"].append({"path": cell["bars_path"],
            "sha256": cell["source_identity"]["bars_sha256"], "bytes": cell["expected_bytes"],
            "rows": cell["expected_rows"], "kind": "csv", "stat_identity": [1, 2, 3]})
        with self.assertRaises(ClockPublicationError) as caught:
            publisher.bind_source_context(report)
        self.assertEqual(caught.exception.code, "CLOCK_CONSUMPTION_NOT_PLANNED")
        publisher.bind_source_context(baseline)

    def test_prefrozen_metadata_plan_refuses_prior_csv_consumption(self):
        _checked_plan(_copy(self.plan))
        plan = _copy(self.plan)
        plan["metadata_read_pins"].append({"path": "SYNTHETIC_ALREADY_READ.csv", "kind": "csv",
            "sha256": _digest("synthetic-csv"), "bytes": 123, "rows": 6,
            "stat_identity": [1, 2, 3]})
        with self.assertRaises(ClockPublicationError) as caught:
            _checked_plan(plan)
        self.assertEqual(caught.exception.code, "CLOCK_PLAN_CONTAINS_CSV_PIN")

    def test_role_states_refuse_impossible_reads_but_preserve_d_quality_stop(self):
        publisher = self._publisher()
        publisher.stop("ZZZ", "H4", "SYNTHETIC_C_UNRESOLVED")
        with self.assertRaises(ClockPublicationError) as caught:
            publisher.bind_source_context(self._report(publisher, target=False))
        self.assertEqual(caught.exception.code, "CLOCK_CONSUMED_ROLE_STATE_MISMATCH")
        publisher = self._publisher()
        publisher.freeze_template(self.sid, self.template, self.runtime)
        publisher.stop("ZZZ", "H4", "SYNTHETIC_D_FULL_FIDELITY_REFUSAL")
        baseline = self._report(publisher, target=False)
        no_fine = _copy(baseline)
        no_fine["consumed_roles"] = [row for row in no_fine["consumed_roles"] if row["role"] != "d_m5"]
        with self.assertRaises(ClockPublicationError) as caught:
            publisher.bind_source_context(no_fine)
        self.assertEqual(caught.exception.code, "CLOCK_CONSUMED_ROLE_STATE_MISMATCH")
        with self.assertRaises(ClockPublicationError) as caught:
            publisher.bind_source_context(self._report(publisher, target=True))
        self.assertEqual(caught.exception.code, "CLOCK_CONSUMED_ROLE_STATE_MISMATCH")
        # EOF-completed D streams need not produce a successful control record.
        publisher.bind_source_context(baseline)
        edition = self._reopen(publisher, publisher.finish())
        self.assertEqual(edition["entries"][0]["status"], "STOP")
        self.assertEqual(edition["entries"][0]["reason_code"], "SYNTHETIC_D_FULL_FIDELITY_REFUSAL")
        self.assertNotIn("bars", edition["entries"][0])

    def test_control_record_scope_and_entry_field_closure_are_checked_cold(self):
        for attack in ("control-scope", "entry-extra", "price-unresolved"):
            with self.subTest(attack=attack):
                publisher, receipt = self._finished()
                self._reopen(publisher, receipt)
                control = next(row for row in publisher.sink.artifact_roster()
                    if row["artifact_id"] == "control:" + self.sid)
                body = json.loads((publisher.context.root / control["descriptor"]["relative_path"]).read_bytes())
                self.assertEqual(body["schema_version"], CONTROL_SCHEMA)
                for name, expected in SCOPE.items():
                    self.assertEqual(body[name], expected)
                self.assertEqual(body["plan_sha256"], publisher.plan_sha256)
                self.assertEqual(body["template_sha256"], self.template.template_sha256)
                self.assertEqual(body["implementation_sha256"], publisher.implementation_sha256)
                if attack == "control-scope":
                    identity = "control:" + self.sid

                    def mutate(value):
                        value["financial_consumer_ready"] = True
                else:
                    identity = "activation"

                    def mutate(value):
                        if attack == "entry-extra":
                            value["entries"][0]["undeclared_context"] = "SYNTHETIC_EXTRA_FIELD"
                        else:
                            value["entries"][0].update(extent_resolution="METADATA_ONLY_NO_C_TEMPLATE",
                                available_interval=None, omitted_intervals=None, control_extent=None)
                context, changed = self._reseal_json_mutation(publisher, receipt, identity, mutate)
                with self.assertRaises(ClockPublicationError):
                    reopen_clock_edition(context.root, changed, expected_generation_id=context.generation_id,
                        expected_native_universe_sha256=context.native_universe_sha256,
                        expected_plan_sha256=publisher.plan_sha256,
                        expected_implementation_sha256=publisher.implementation_sha256)

    def test_stop_before_later_template_keeps_explicit_unknown_c_context_cold(self):
        publisher = self._publisher()
        publisher.stop("ZZZ", "H4", "SYNTHETIC_TARGET_STOP_BEFORE_C")
        publisher.freeze_template(self.sid, self.template, self.runtime)
        report = self._report(publisher, target=False)
        c_native = self.plan["strata"][0]["c_native"]["bars_path"]
        report["consumed_roles"] = [row for row in report["consumed_roles"] if row["role"] == "c_native"]
        report["metadata_read_pins"] = [pin for pin in report["metadata_read_pins"] if pin["path"] == c_native]
        publisher.bind_source_context(report)
        row = self._reopen(publisher, publisher.finish())["entries"][0]
        self.assertEqual(row["extent_resolution"], "METADATA_ONLY_NO_C_TEMPLATE")
        self.assertIsNone(row["available_interval"])
        self.assertIsNone(row["omitted_intervals"])
        self.assertEqual(row["required_interval"], asdict(_interval(-60, 480)))
        self.assertNotIn("bars", row)


if __name__ == "__main__":
    unittest.main()
