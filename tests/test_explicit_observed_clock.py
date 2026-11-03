"""Invented in-memory clocks only: SYNTHETIC_TEST_ONLY_NO_NATIVE_VERIFICATION.

No original verifier, native scope, receipt or capability is patched or supplied.
These sources are not evidence that any actual control calendar is available.
"""
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import unittest

from pm_explicit_derived_history import (
    Bar, Interval, SourceIdentity, Stratum, DerivedSeries, DerivedHistoryError,
    derive_bars, require_spread_ready,
)
from pm_explicit_observed_clock import (
    ClockStratum, ClockExtent, ClockDerivedSeries, ObservedClockError,
    DATA_SCOPE, SOURCE_KIND, SPREAD_CANDIDATES,
    freeze_observed_clock, derive_on_observed_clock,
    identify_clock_spread_rule, apply_clock_spread_rule, clock_lineage,
)


_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
_PROVENANCE = "SYNTHETIC_TEST_ONLY_NO_NATIVE_VERIFICATION"


def _stamp(minutes):
    return (_START + timedelta(minutes=minutes)).isoformat()


def _source(symbol, timeframe, *, point=0.25):
    def digest(label):
        return hashlib.sha256((symbol + ":" + timeframe + ":" + label).encode("ascii")).hexdigest()
    return SourceIdentity(symbol, timeframe, _PROVENANCE, "1" * 64,
        digest("bars"), digest("sidecar"), digest("provenance"), digest("request"), point)


def _template(timeframe="H4", widths=(180, 300), *, gap=0):
    second = widths[0] + gap
    clock = (Interval(_stamp(0), _stamp(widths[0])),
             Interval(_stamp(second), _stamp(second + widths[1])))
    return freeze_observed_clock(clock,
        stratum=ClockStratum(timeframe, "2" * 64, "SYNTHETIC_DECLARED_CLASS_NOT_CALENDAR_PROOF"),
        clock_source=_source("A_CLOCK", timeframe),
        corroboration_source=_source("B_CONTROL", timeframe),
        interval=Interval(_stamp(0), clock[-1].end_exclusive_utc))


def _extent(template, *, omitted_prefix=False):
    available = template.interval
    return ClockExtent(Interval(_stamp(-60), available.end_exclusive_utc)
        if omitted_prefix else available, available, available, available,
        (Interval(_stamp(-60), _stamp(0)),) if omitted_prefix else ())


def _fine_rows(template, *, constant_spread=None, omit_bucket=None):
    rows = []
    for index, interval in enumerate(template.clock):
        if index == omit_bucket:
            continue
        opened = datetime.fromisoformat(interval.start_utc)
        base = 100 + 10 * index
        spreads = (4, 1, 6) if index % 2 == 0 else (6, 9, 4)
        for member in range(3):
            at = opened + timedelta(minutes=5 * member)
            rows.append(Bar(at.isoformat(), (at + timedelta(minutes=5)).isoformat(),
                base + member, base + member + 2, base + member - 1,
                base + member + 1, 10 + member, 1 + member,
                constant_spread if constant_spread is not None else spreads[member]))
    return tuple(rows)


def _native_d(template, *, constant_spread=None):
    return tuple(Bar(interval.start_utc, interval.end_exclusive_utc,
        100 + 10 * index, 104 + 10 * index, 99 + 10 * index, 103 + 10 * index,
        33, 6, constant_spread if constant_spread is not None else 4 if index % 2 == 0 else 6)
        for index, interval in enumerate(template.clock))


def _derive(template, *, symbol="B_CONTROL", rows=None, omitted_prefix=False):
    return derive_on_observed_clock(_fine_rows(template) if rows is None else rows,
        template=template, source=_source(symbol, "M5"),
        extent=_extent(template, omitted_prefix=omitted_prefix))


class ExplicitObservedClockTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ObservedClockError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def assert_data_only(self, value):
        for field in ("qualification_authority", "capital_authority", "live_authority",
                      "order_authority", "full_native_context_eligibility", "financial_consumer_ready"):
            self.assertIs(value[field] if isinstance(value, dict) else getattr(value, field), False)
        self.assertEqual(value["data_scope"] if isinstance(value, dict) else value.data_scope, DATA_SCOPE)

    def test_h4_observed_three_and_five_hour_axes_transfer_through_distinct_d(self):
        template = _template()
        control = _derive(template)
        evidence = identify_clock_spread_rule(control, _native_d(template),
            native_source=template.corroboration_source)
        self.assertEqual(evidence.matching_candidates, ("first",))
        self.assertEqual(tuple(name for name, _ in evidence.mismatch_counts), SPREAD_CANDIDATES)
        target = apply_clock_spread_rule(_derive(template, symbol="T_TARGET"), evidence)
        self.assertEqual(tuple(row.value.spread for row in target.bars), (4, 6))
        self.assertEqual(target.cost_status, "CLOCK_SPREAD_AVAILABLE_PROFILE_UNVERIFIED")
        self.assertEqual(tuple((row.value.open_utc, row.value.close_utc) for row in target.bars),
            tuple((row.start_utc, row.end_exclusive_utc) for row in template.clock))
        self.assert_data_only(template)
        self.assert_data_only(evidence)
        self.assert_data_only(target)
        self.assertNotIsInstance(target, DerivedSeries)

    def test_d1_observed_twenty_three_and_twenty_five_hour_axes_are_not_normalized(self):
        template = _template("D1", (23 * 60, 25 * 60))
        control = _derive(template)
        evidence = identify_clock_spread_rule(control, _native_d(template),
            native_source=template.corroboration_source)
        self.assertEqual(evidence.compared_bar_count, 2)
        self.assertEqual(tuple((datetime.fromisoformat(row.value.close_utc) -
            datetime.fromisoformat(row.value.open_utc)).total_seconds() for row in control.bars),
            (23 * 3600, 25 * 3600))

    def test_each_unique_rule_applies_its_own_target_observation(self):
        template = _template()
        control = _derive(template)
        rows = _fine_rows(template)
        target = _derive(template, symbol="T_TARGET", rows=tuple(
            replace(row, spread=spread) for row, spread in
            zip(rows, (8, 2, 10, 7, 12, 3), strict=True)))
        cases = (("first", (4, 6), (8, 7)), ("last", (6, 4), (10, 3)),
                 ("min", (1, 4), (2, 3)), ("max", (6, 9), (10, 12)))
        for rule, control_spreads, target_spreads in cases:
            with self.subTest(rule=rule):
                native = tuple(replace(row, spread=spread) for row, spread in
                    zip(_native_d(template), control_spreads, strict=True))
                evidence = identify_clock_spread_rule(control, native,
                    native_source=template.corroboration_source)
                self.assertEqual(evidence.matching_candidates, (rule,))
                self.assertEqual(evidence.selected_spread_rule, rule)
                self.assertEqual(tuple(name for name, _ in evidence.mismatch_counts), SPREAD_CANDIDATES)
                applied = apply_clock_spread_rule(target, evidence)
                self.assertEqual(tuple(row.value.spread for row in applied.bars), target_spreads)
                self.assertEqual(applied.cost_status, "CLOCK_SPREAD_AVAILABLE_PROFILE_UNVERIFIED")
                self.assert_data_only(evidence)
                self.assert_data_only(applied)

    def test_clock_c_cannot_also_be_corroboration_d(self):
        template = _template()
        self.assert_code("DISTINCT_PREFROZEN_C_D_REQUIRED", freeze_observed_clock,
            template.clock, stratum=template.stratum, clock_source=template.clock_source,
            corroboration_source=template.clock_source, interval=template.interval)
        self.assert_code("DISTINCT_D_CONTROL_BINDING_INVALID", identify_clock_spread_rule,
            _derive(template, symbol="A_CLOCK"), _native_d(template),
            native_source=template.corroboration_source)

    def test_c_d_lexical_order_is_frozen_not_swapped_after_outcomes(self):
        template = _template()
        self.assert_code("DISTINCT_PREFROZEN_C_D_REQUIRED", freeze_observed_clock,
            template.clock, stratum=template.stratum, clock_source=template.corroboration_source,
            corroboration_source=template.clock_source, interval=template.interval)

    def test_d_clock_mismatch_refuses_even_when_prices_and_spreads_match(self):
        template = _template()
        native = _native_d(template)
        native = (replace(native[0], close_utc=_stamp(175)), native[1])
        self.assert_code("CONTROL_CLOCK_FIDELITY_MISMATCH", identify_clock_spread_rule,
            _derive(template), native, native_source=template.corroboration_source)

    def test_d_price_and_volume_fidelity_precedes_spread_identification_without_tolerance(self):
        template = _template()
        for field, value in (("open", 100.000000000001), ("volume", 34), ("real_volume", 7)):
            with self.subTest(field=field):
                native = _native_d(template)
                native = (replace(native[0], **{field: value, "spread": None}), native[1])
                self.assert_code("CONTROL_PRICE_FIDELITY_MISMATCH", identify_clock_spread_rule,
                    _derive(template), native, native_source=template.corroboration_source)

    def test_equal_numeric_price_with_different_canonical_representation_refuses(self):
        template = _template()
        original = _native_d(template)
        for field in ("open", "high", "low", "close"):
            with self.subTest(field=field):
                changed = replace(original[0], **{field: float(getattr(original[0], field))})
                self.assertEqual(getattr(changed, field), getattr(original[0], field))
                self.assertIs(type(getattr(changed, field)), float)
                self.assert_code("CONTROL_PRICE_FIDELITY_MISMATCH", identify_clock_spread_rule,
                    _derive(template), (changed, original[1]),
                    native_source=template.corroboration_source)

    def test_empty_d_bucket_fails_full_clock_coverage_before_spreads(self):
        template = _template()
        control = _derive(template, rows=_fine_rows(template, omit_bucket=1))
        self.assertEqual(control.empty_clock_ordinals, (1,))
        self.assert_code("CONTROL_COVERAGE_MISMATCH", identify_clock_spread_rule,
            control, _native_d(template), native_source=template.corroboration_source)

    def test_empty_target_bucket_is_retained_without_an_invented_price_bar(self):
        template = _template()
        target = _derive(template, symbol="T_TARGET", rows=_fine_rows(template, omit_bucket=1))
        self.assertEqual(len(target.bars), 1)
        self.assertEqual(target.empty_clock_ordinals, (1,))
        self.assertEqual(target.source_member_count, 3)
        lineage = clock_lineage(target)
        self.assertEqual(lineage["empty_clock_ordinals"], (1,))
        self.assertIsNone(lineage["target_native_status"])
        self.assert_data_only(lineage)

    def test_reversed_overlapping_and_duplicate_clock_intervals_refuse(self):
        template = _template()
        bad_clocks = (
            (Interval(_stamp(5), _stamp(0)), template.clock[1]),
            (template.clock[0], Interval(_stamp(120), _stamp(480))),
            (template.clock[0], template.clock[0]),
        )
        for clock in bad_clocks:
            with self.subTest(clock=clock), self.assertRaises(ObservedClockError):
                freeze_observed_clock(clock, stratum=template.stratum,
                    clock_source=template.clock_source, corroboration_source=template.corroboration_source,
                    interval=template.interval)

    def test_duplicate_reversed_and_overlapping_m5_members_refuse(self):
        template = _template()
        rows = _fine_rows(template)
        cases = ((rows[0], rows[0]), (rows[1], rows[0]),
                 (rows[0], replace(rows[1], open_utc=_stamp(3), close_utc=_stamp(8))))
        for supplied in cases:
            with self.subTest(supplied=supplied), self.assertRaises(ObservedClockError):
                _derive(template, rows=supplied)

    def test_straddling_m5_member_refuses_without_rounding_or_partial_volume(self):
        template = _template()
        row = replace(_fine_rows(template)[0], open_utc=_stamp(178), close_utc=_stamp(183))
        self.assert_code("M5_STRADDLING_CLOCK_BOUNDARY", _derive, template, rows=(row,))

    def test_m5_observation_in_recorded_clock_gap_is_not_dropped(self):
        template = _template(gap=60)
        row = replace(_fine_rows(template)[0], open_utc=_stamp(200), close_utc=_stamp(205))
        self.assert_code("M5_UNASSIGNED_INTERVAL", _derive, template, rows=(row,))

    def test_invalid_prices_fractional_volumes_and_mixed_real_volume_refuse(self):
        template = _template()
        rows = _fine_rows(template)
        for changed in (replace(rows[0], open=float("nan")), replace(rows[0], volume=1.5),
                        replace(rows[0], volume=True), replace(rows[0], spread=-1)):
            with self.subTest(changed=changed), self.assertRaises(ObservedClockError):
                _derive(template, rows=(changed, *rows[1:]))
        self.assert_code("M5_REAL_VOLUME_SCHEMA_MIXED", _derive, template,
            rows=(rows[0], replace(rows[1], real_volume=None), *rows[2:]))

    def test_ambiguous_four_rule_match_leaves_costs_unavailable(self):
        template = _template()
        control = _derive(template, rows=_fine_rows(template, constant_spread=5))
        evidence = identify_clock_spread_rule(control, _native_d(template, constant_spread=5),
            native_source=template.corroboration_source)
        self.assertEqual(evidence.matching_candidates, SPREAD_CANDIDATES)
        self.assertIsNone(evidence.selected_spread_rule)
        self.assertEqual(evidence.reason_code, "AMBIGUOUS_CONTROL_MATCH")
        target = apply_clock_spread_rule(_derive(template, symbol="T_TARGET"), evidence)
        self.assertEqual(target.cost_status, "COST_UNAVAILABLE")
        self.assertTrue(all(row.value.spread is None for row in target.bars))

    def test_missing_or_unmatched_native_spread_adds_no_fifth_proxy_rule(self):
        template = _template()
        for spread, reason in ((None, "MISSING_CONTROL_SPREAD"), (77, "NO_CONTROL_MATCH")):
            with self.subTest(spread=spread):
                native = tuple(replace(row, spread=spread) for row in _native_d(template))
                evidence = identify_clock_spread_rule(_derive(template), native,
                    native_source=template.corroboration_source)
                self.assertEqual(evidence.matching_candidates, ())
                self.assertEqual(evidence.reason_code, reason)
                self.assertEqual(len(evidence.mismatch_counts), 4)

    def test_missing_target_m5_spread_is_not_filled_from_its_control(self):
        template = _template()
        evidence = identify_clock_spread_rule(_derive(template), _native_d(template),
            native_source=template.corroboration_source)
        rows = _fine_rows(template)
        target = _derive(template, symbol="T_TARGET", rows=(replace(rows[0], spread=None), *rows[1:]))
        applied = apply_clock_spread_rule(target, evidence)
        self.assertIsNone(applied.bars[0].value.spread)
        self.assertEqual(applied.cost_status, "COST_UNAVAILABLE")

    def test_cutoff_change_and_outside_frozen_m5_members_refuse(self):
        template = _template()
        changed = replace(template, cutoff_utc="2024-07-14T08:10:00+00:00")
        self.assert_code("CLOCK_PREFIX_INVALID", derive_on_observed_clock, (),
            template=changed, source=_source("T_TARGET", "M5"), extent=_extent(template))
        row = replace(_fine_rows(template)[0], open_utc=_stamp(-5), close_utc=_stamp(0))
        self.assert_code("M5_OUTSIDE_FROZEN_INTERVAL", _derive, template, rows=(row,))

    def test_lineage_binds_omitted_prefix_controls_and_ordered_membership(self):
        template = _template()
        evidence = identify_clock_spread_rule(_derive(template), _native_d(template),
            native_source=template.corroboration_source)
        target = apply_clock_spread_rule(_derive(template, symbol="T_TARGET", omitted_prefix=True), evidence)
        lineage = clock_lineage(target, identification=evidence)
        self.assertNotEqual(lineage["required_interval"], lineage["available_interval"])
        self.assertEqual(lineage["omitted_intervals"], (asdict(Interval(_stamp(-60), _stamp(0))),))
        self.assertEqual(lineage["template"]["clock_source"]["symbol"], "A_CLOCK")
        self.assertEqual(lineage["control_identification"]["native_control_source"]["symbol"], "B_CONTROL")
        self.assertEqual([row["member_count"] for row in lineage["buckets"]], [3, 3])
        self.assertTrue(all(len(row["membership_sha256"]) == 64 for row in lineage["buckets"]))
        body = {name: value for name, value in lineage.items() if name != "lineage_sha256"}
        encoded = json.dumps(body, allow_nan=False, ensure_ascii=True, sort_keys=True,
                             separators=(",", ":")).encode("ascii")
        self.assertEqual(lineage["lineage_sha256"], hashlib.sha256(encoded).hexdigest())
        self.assert_data_only(lineage)

    def test_template_extent_series_and_evidence_mutations_refuse(self):
        template = _template()
        control = _derive(template)
        evidence = identify_clock_spread_rule(control, _native_d(template),
            native_source=template.corroboration_source)
        mutations = (
            replace(control, template=replace(template, axes_sha256="f" * 64)),
            replace(control, extent=replace(control.extent,
                required_interval=Interval(_stamp(-60), template.interval.end_exclusive_utc))),
            replace(control, bars=(replace(control.bars[0], member_count=4), control.bars[1])),
            replace(control, qualification_authority=True),
            replace(control, series_sha256="f" * 64),
        )
        for value in mutations:
            with self.subTest(value=value), self.assertRaises(ObservedClockError):
                clock_lineage(value)
        self.assert_code("CLOCK_SPREAD_BINDING_MISMATCH", apply_clock_spread_rule,
            control, replace(evidence, selected_spread_rule="last"))
        applied = apply_clock_spread_rule(control, evidence)
        self.assert_code("CLOCK_SPREAD_IDENTIFICATION_REQUIRED", clock_lineage, applied)

    def test_clock_types_remain_separate_from_original_fixed_series(self):
        template = _template()
        clock_series = _derive(template)
        self.assertIs(type(clock_series), ClockDerivedSeries)
        self.assertEqual(clock_series.source_kind, SOURCE_KIND)
        with self.assertRaises(DerivedHistoryError):
            require_spread_ready(clock_series)
        with self.assertRaises(DerivedHistoryError) as caught:
            derive_bars(_fine_rows(template), stratum=Stratum("H4", "SYN_CLASS", "SYN_ALIGNMENT"),
                source=_source("T_TARGET", "M5"), interval=template.interval)
        self.assertEqual(caught.exception.code, "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR")
