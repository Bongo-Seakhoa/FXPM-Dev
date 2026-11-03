"""Invented in-memory tests for explicit derived history; no native authority.

All bars, identities and session labels below are SYNTHETIC_TEST_ONLY.
No canonical files, broker terms, data fixtures, profiles or APIs are used.
"""
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import math
import unittest

from pm_explicit_derived_history import (
    Bar,
    DerivedHistoryError,
    Interval,
    SourceIdentity,
    Stratum,
    apply_spread_rule,
    derive_bars,
    identify_spread_rule,
    require_spread_ready,
)


_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
_RULES = ("first", "last", "min", "max")


def _stamp(minutes):
    return (_START + timedelta(minutes=minutes)).isoformat()


def _source(timeframe="M5", symbol="SYNTHETIC_TEST_ONLY", **changes):
    values = dict(
        symbol=symbol,
        timeframe=timeframe,
        activation_id="SYNTHETIC_TEST_ONLY_ACTIVATION",
        activation_sha256="1" * 64,
        bars_sha256="2" * 64,
        sidecar_sha256="3" * 64,
        provenance_sha256="4" * 64,
        request_sha256="5" * 64,
        point=0.0001,
        spread_unit="points",
    )
    values.update(changes)
    return SourceIdentity(**values)


def _stratum(session="SYNTHETIC_TEST_ONLY_SESSION", timeframe="M15"):
    return Stratum(timeframe, session, "SYNTHETIC_TEST_ONLY_FIXED_" + timeframe)


def _rows():
    # Two M15 buckets distinguish every candidate over the whole interval.
    prices = (
        (10.0, 13.0, 9.0, 12.0),
        (12.0, 14.0, 10.0, 11.0),
        (11.0, 16.0, 8.0, 15.0),
        (15.0, 17.0, 12.0, 14.0),
        (14.0, 18.0, 11.0, 16.0),
        (16.0, 19.0, 10.0, 13.0),
    )
    spreads = (4, 1, 6, 6, 9, 4)
    return [
        Bar(
            open_utc=_stamp(i * 5),
            close_utc=_stamp((i + 1) * 5),
            open=o, high=h, low=l, close=c,
            volume=(i + 1) * 10,
            real_volume=i + 1,
            spread=spreads[i],
        )
        for i, (o, h, l, c) in enumerate(prices)
    ]


def _series(rows=None, *, stratum=None, source=None, interval=None, cutoff=None):
    return derive_bars(
        _rows() if rows is None else rows,
        stratum=_stratum() if stratum is None else stratum,
        source=_source() if source is None else source,
        interval=Interval(_stamp(0), _stamp(30)) if interval is None else interval,
        cutoff_utc=_stamp(30) if cutoff is None else cutoff,
    )


def _native_bars(series, spreads):
    return tuple(
        replace(row.value, spread=spread)
        for row, spread in zip(series.bars, spreads, strict=True)
    )


def _identify(series, spreads, native_source=None):
    return identify_spread_rule(
        series,
        _native_bars(series, spreads),
        native_source=(
            _source(series.stratum.timeframe, series.source.symbol)
            if native_source is None else native_source
        ),
    )


class ExplicitDerivedHistoryTests(unittest.TestCase):
    def assert_refusal(self, action, code=None):
        with self.assertRaises(DerivedHistoryError) as caught:
            action()
        self.assertIsInstance(caught.exception.code, str)
        self.assertTrue(caught.exception.code)
        if code is not None:
            self.assertEqual(caught.exception.code, code)
        return caught.exception

    def test_observed_membership_and_exact_ohlcv(self):
        result = _series()
        self.assertEqual(len(result.bars), 2)
        self.assertEqual(result.source_member_count, 6)
        expected = (
            (_stamp(0), _stamp(15), 10.0, 16.0, 8.0, 15.0, 60, 6),
            (_stamp(15), _stamp(30), 15.0, 19.0, 10.0, 13.0, 150, 15),
        )
        actual = tuple(
            (r.value.open_utc, r.value.close_utc, r.value.open, r.value.high,
             r.value.low, r.value.close, r.value.volume, r.value.real_volume)
            for r in result.bars
        )
        self.assertEqual(actual, expected)
        self.assertEqual(tuple(r.member_count for r in result.bars), (3, 3))
        self.assertEqual(result.bars[0].spread_candidates, (4, 6, 1, 6))
        self.assertEqual(result.bars[1].spread_candidates, (6, 4, 4, 9))
        self.assertEqual(result.cost_status, "PRICE_SOURCE_ONLY_NOT_EXECUTION_READY")
        self.assertIsNone(result.selected_spread_rule)
        self.assertTrue(all(r.value.spread is None for r in result.bars))
        self.assertEqual(len(result.membership_sha256), 64)
        self.assertNotEqual(result.bars[0].membership_sha256,
                            result.bars[1].membership_sha256)

    def test_sparse_observed_buckets_do_not_invent_empty_bars(self):
        rows = _rows()
        shifted = [replace(r, open_utc=_stamp(30 + i * 5),
                           close_utc=_stamp(35 + i * 5))
                   for i, r in enumerate(rows[3:])]
        result = _series(rows[:3] + shifted,
                         interval=Interval(_stamp(0), _stamp(45)), cutoff=_stamp(45))
        self.assertEqual(tuple(r.value.open_utc for r in result.bars),
                         (_stamp(0), _stamp(30)))
        self.assertEqual(result.source_member_count, 6)
        self.assertGreater(result.gap_count, 0)
        self.assertEqual(tuple(r.member_count for r in result.bars), (3, 3))

    def test_close_equal_to_cutoff_is_allowed_but_later_is_refused(self):
        self.assertEqual(_series().bars[-1].value.close_utc, _stamp(30))
        self.assert_refusal(lambda: _series(cutoff=_stamp(29)),
                            "SOURCE_CUTOFF_VIOLATION")

    def test_partial_edges_and_outside_rows_are_not_silently_trimmed(self):
        for interval in (Interval(_stamp(5), _stamp(30)),
                         Interval(_stamp(0), _stamp(25))):
            with self.subTest(interval=interval):
                self.assert_refusal(lambda: _series(interval=interval),
                                    "UNSUPPORTED_INTERVAL_COVERAGE")
        extra = replace(_rows()[-1], open_utc=_stamp(30), close_utc=_stamp(35))
        self.assert_refusal(lambda: _series(_rows() + [extra]),
                            "SOURCE_ROW_OUTSIDE_INTERVAL")
        straddling = replace(_rows()[-1], close_utc=_stamp(35))
        self.assert_refusal(lambda: _series(_rows()[:-1] + [straddling]))

    def test_duplicate_and_unordered_source_refuse_without_sorting(self):
        rows = _rows()
        self.assert_refusal(lambda: _series([rows[0], rows[0]] + rows[1:]),
                            "SOURCE_DUPLICATE_TIMESTAMP")
        self.assert_refusal(lambda: _series([rows[1], rows[0]] + rows[2:]),
                            "SOURCE_ORDER_INVALID")

    def test_nonfinite_prices_and_nonintegral_volumes_refuse(self):
        cases = (("open", math.nan), ("high", math.inf),
                 ("low", -math.inf), ("close", math.nan),
                 ("volume", 1.5), ("volume", math.inf),
                 ("real_volume", 1.5), ("real_volume", math.nan))
        for field, value in cases:
            with self.subTest(field=field, value=value):
                rows = _rows()
                rows[0] = replace(rows[0], **{field: value})
                self.assert_refusal(lambda: _series(rows), "SOURCE_BAR_INVALID")
        rows = _rows()
        rows[0] = replace(rows[0], low=20.0)
        self.assert_refusal(lambda: _series(rows), "SOURCE_BAR_INVALID")

    def test_real_volume_is_not_invented(self):
        result = _series([replace(r, real_volume=None) for r in _rows()])
        self.assertEqual(tuple(r.value.real_volume for r in result.bars), (None, None))
        self.assertEqual(tuple(r.value.volume for r in result.bars), (60, 150))

    def test_nonzero_offset_is_not_reinterpreted_as_utc(self):
        rows = _rows()
        rows[0] = replace(rows[0], open_utc="2024-01-01T01:00:00+01:00",
                          close_utc="2024-01-01T01:05:00+01:00")
        self.assert_refusal(lambda: _series(rows))

    def test_h4_d1_validation_tag_is_not_a_boundary_constructor(self):
        for timeframe in ("H4", "D1"):
            with self.subTest(timeframe=timeframe):
                stratum = Stratum(timeframe, "SYNTHETIC_TEST_ONLY_SESSION",
                                  "BROKER_SESSION_ALIGNMENT_MODE")
                self.assert_refusal(lambda: _series(stratum=stratum),
                                    "UNSUPPORTED_ALIGNMENT_CONSTRUCTOR")

    def test_output_is_frozen_detached_and_input_is_unchanged(self):
        rows = _rows()
        before = tuple(rows)
        result = _series(rows)
        self.assertEqual(tuple(rows), before)
        with self.assertRaises(FrozenInstanceError):
            result.cost_status = "FORGED_READY"
        with self.assertRaises(FrozenInstanceError):
            result.bars[0].value.close = 999.0
        with self.assertRaises(FrozenInstanceError):
            result.bars[0].member_count = 999
        rows.clear()
        self.assertEqual(len(result.bars), 2)
        self.assertEqual(result.bars[0].value.close, 15.0)
        self.assertIsInstance(result.bars, tuple)

    def test_each_of_four_spread_rules_can_uniquely_match_whole_interval(self):
        control = _series()
        expected = {"first": (4, 6), "last": (6, 4),
                    "min": (1, 4), "max": (6, 9)}
        for rule, native_spreads in expected.items():
            with self.subTest(rule=rule):
                identified = _identify(control, native_spreads)
                self.assertEqual(identified.cost_status, "SOURCE_RULE_MATCHED")
                self.assertEqual(identified.selected_spread_rule, rule)
                self.assertEqual(identified.matching_candidates, (rule,))
                self.assertEqual(identified.compared_bar_count, 2)
                self.assertEqual(tuple(k for k, _ in identified.mismatch_counts), _RULES)
                counts = dict(identified.mismatch_counts)
                self.assertEqual(counts[rule], 0)
                self.assertTrue(all(counts[k] > 0 for k in _RULES if k != rule))

    def test_no_rule_and_ambiguous_rules_stay_cost_unavailable(self):
        control = _series()
        unavailable = _identify(control, (5, 5))
        self.assertEqual(unavailable.cost_status, "COST_UNAVAILABLE")
        self.assertEqual(unavailable.matching_candidates, ())
        self.assertIsNone(unavailable.selected_spread_rule)
        constant = _series([replace(r, spread=5) for r in _rows()])
        ambiguous = _identify(constant, (5, 5))
        self.assertEqual(ambiguous.cost_status, "COST_UNAVAILABLE")
        self.assertEqual(ambiguous.matching_candidates, _RULES)
        self.assertIsNone(ambiguous.selected_spread_rule)
        for identification in (unavailable, ambiguous):
            applied = apply_spread_rule(control, identification)
            self.assertEqual(applied.cost_status, "COST_UNAVAILABLE")
            self.assert_refusal(lambda: require_spread_ready(applied),
                                "DERIVED_SPREAD_NOT_READY")

    def test_one_matching_candidate_per_bar_is_not_a_whole_interval_rule(self):
        # Bar1 matches first; bar2 matches last/min; no one rule matches both.
        identified = _identify(_series(), (4, 4))
        self.assertEqual(identified.mismatch_counts,
                         (("first", 1), ("last", 1), ("min", 1), ("max", 2)))
        self.assertEqual(identified.cost_status, "COST_UNAVAILABLE")
        self.assertIsNone(identified.selected_spread_rule)

    def test_price_fidelity_and_complete_control_roster_precede_spread_match(self):
        control = _series()
        native = list(_native_bars(control, (4, 6)))
        native[1] = replace(native[1], high=native[1].high + 1.0)
        self.assert_refusal(
            lambda: identify_spread_rule(control, native, native_source=_source("M15")),
            "CONTROL_PRICE_FIDELITY_MISMATCH",
        )
        self.assert_refusal(
            lambda: identify_spread_rule(control, _native_bars(control, (4, 6))[:1],
                                         native_source=_source("M15")),
            "CONTROL_PRICE_FIDELITY_MISMATCH",
        )

    def test_missing_control_spread_cannot_silently_select_a_rule(self):
        control = _series()
        native_missing = _identify(control, (4, None))
        self.assertEqual(native_missing.cost_status, "COST_UNAVAILABLE")
        self.assertIsNone(native_missing.selected_spread_rule)
        rows = _rows()
        rows[4] = replace(rows[4], spread=None)
        source_missing = _identify(_series(rows), (4, 6))
        self.assertEqual(source_missing.cost_status, "COST_UNAVAILABLE")
        self.assertIsNone(source_missing.selected_spread_rule)

    def test_invalid_source_and_native_spreads_refuse(self):
        for bad in (-1, 1.5, math.nan, math.inf, True):
            with self.subTest(bad=bad):
                rows = _rows()
                rows[0] = replace(rows[0], spread=bad)
                self.assert_refusal(lambda: _series(rows), "SOURCE_SPREAD_INVALID")
                self.assert_refusal(lambda: _identify(_series(), (4, bad)),
                                    "CONTROL_SPREAD_INVALID")

    def test_control_point_units_and_source_identity_must_match(self):
        control = _series()
        cases = ((_source("M15", point=0.001), "CONTROL_UNITS_MISMATCH"),
                 (_source("M30"), "CONTROL_IDENTITY_MISMATCH"),
                 (_source("M15", symbol="SYNTHETIC_OTHER"),
                  "CONTROL_IDENTITY_MISMATCH"))
        for native, code in cases:
            with self.subTest(native=native):
                self.assert_refusal(lambda: _identify(control, (4, 6), native), code)
        self.assert_refusal(lambda: _identify(
            control, (4, 6), _source("M15", spread_unit="pips")),
            "CONTROL_UNITS_MISMATCH")

    def test_applied_rule_keeps_price_only_input_and_profile_limit(self):
        control = _series()
        identified = _identify(control, (4, 6))
        target_rows = _rows()
        before = tuple(target_rows)
        target = _series(target_rows, source=_source(symbol="SYNTHETIC_TARGET"))
        ready = apply_spread_rule(target, identified)
        self.assertEqual(ready.cost_status, "DERIVED_SPREAD_AVAILABLE_PROFILE_UNVERIFIED")
        self.assertEqual(ready.selected_spread_rule, "first")
        self.assertEqual(tuple(r.value.spread for r in ready.bars), (4, 6))
        self.assertEqual(ready.source.symbol, "SYNTHETIC_TARGET")
        self.assertEqual(tuple(target_rows), before)
        self.assertEqual(target.cost_status, "PRICE_SOURCE_ONLY_NOT_EXECUTION_READY")
        self.assertTrue(all(r.value.spread is None for r in target.bars))
        require_spread_ready(ready)

    def test_one_missing_target_member_keeps_price_only_diagnostic(self):
        identification = _identify(_series(), (4, 6))
        rows = _rows()
        rows[4] = replace(rows[4], spread=None)
        target = _series(rows)
        result = apply_spread_rule(target, identification)
        self.assertEqual(result.cost_status, "COST_UNAVAILABLE")
        self.assertEqual(len(result.bars), 2)
        self.assertEqual(tuple(r.value.close for r in result.bars), (15.0, 13.0))
        self.assert_refusal(lambda: require_spread_ready(result),
                            "DERIVED_SPREAD_NOT_READY")

    def test_readiness_checks_every_emitted_bar_even_with_ready_status(self):
        ready = apply_spread_rule(_series(), _identify(_series(), (4, 6)))
        for bad in (None, math.nan, math.inf, -1, 1.5, True):
            with self.subTest(second_bar_spread=bad):
                bad_second = replace(ready.bars[1],
                                     value=replace(ready.bars[1].value, spread=bad))
                tampered = replace(ready, bars=(ready.bars[0], bad_second))
                self.assert_refusal(lambda: require_spread_ready(tampered),
                                    "DERIVED_SPREAD_NOT_READY")

    def test_rule_cannot_be_borrowed_across_strata(self):
        identification = _identify(_series(), (4, 6))
        other = _series(stratum=_stratum(session="SYNTHETIC_DIFFERENT_SESSION"))
        self.assert_refusal(lambda: apply_spread_rule(other, identification),
                            "STRATUM_MISMATCH")

    def test_failure_is_local_to_its_stratum_not_same_timeframe(self):
        bad_stratum = _stratum(session="SYNTHETIC_FAILING_SESSION")
        bad_control = _series(stratum=bad_stratum)
        wrong = list(_native_bars(bad_control, (4, 6)))
        wrong[0] = replace(wrong[0], close=11.0)
        failure = self.assert_refusal(
            lambda: identify_spread_rule(bad_control, wrong, native_source=_source("M15")),
            "CONTROL_PRICE_FIDELITY_MISMATCH",
        )
        self.assertEqual(failure.stratum, bad_stratum)
        good_stratum = _stratum(session="SYNTHETIC_INDEPENDENT_SESSION")
        good_control = _series(stratum=good_stratum)
        result = apply_spread_rule(good_control, _identify(good_control, (4, 6)))
        self.assertEqual(result.stratum, good_stratum)
        self.assertEqual(result.cost_status, "DERIVED_SPREAD_AVAILABLE_PROFILE_UNVERIFIED")


if __name__ == "__main__":
    unittest.main()
