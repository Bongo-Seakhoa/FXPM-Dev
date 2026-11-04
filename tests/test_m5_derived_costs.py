"""Invented cost-join software tests: NO_NATIVE_VERIFICATION_PERFORMED.

The declared statistics are fixture metadata, not a new calibration or market
measurement. Pure helpers and detached tier descriptors do not establish a
positive original-owner or retained-edition authentication path.
"""
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import sys
import unittest

_CANDIDATE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_CANDIDATE_ROOT))

import pm_m5_derived_costs as costs
from pm_explicit_derived_history import (
    Bar, Interval, PROTECTED_CUTOFF, SourceIdentity, Stratum,
    apply_spread_rule, canonical_json_bytes, derive_bars, identify_spread_rule,
)
from pm_spread_profile import FrozenSpreadTier, TIER_SCHEMA

if Path(costs.__file__).resolve() != _CANDIDATE_ROOT / "pm_m5_derived_costs.py":
    raise RuntimeError("C6_TEST_REQUIRES_EXACT_CANDIDATE_COST_MODULE")


def _digest(value):
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _source(symbol, timeframe="M5"):
    return SourceIdentity(symbol, timeframe, "NO_NATIVE_VERIFICATION_PERFORMED",
                          "1" * 64, "2" * 64, "3" * 64, "4" * 64, "5" * 64, 0.01)


def _rows():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    spreads = (4, 1, 6, 6, 4, 9)
    return tuple(Bar((start + timedelta(minutes=5 * index)).isoformat(),
                     (start + timedelta(minutes=5 * (index + 1))).isoformat(),
                     100.0, 102.0, 99.0, 101.0, 10, None, spread)
                 for index, spread in enumerate(spreads))


def _fixture():
    interval = Interval("2024-01-01T00:00:00+00:00", "2024-01-01T00:30:00+00:00")
    stratum = Stratum("M15", "NO_NATIVE_VERIFICATION_FIXTURE", "SYNTHETIC_FIXED_UTC")
    control_source = _source("SYNTHETIC_CONTROL")
    control = derive_bars(_rows(), stratum=stratum, source=control_source,
                          interval=interval, cutoff_utc=PROTECTED_CUTOFF)
    native_fixture = tuple(replace(row.value, spread=value)
                           for row, value in zip(control.bars, (1, 4), strict=True))
    identification = identify_spread_rule(control, native_fixture,
                                          native_source=_source("SYNTHETIC_CONTROL", "M15"))
    source = _source("SYNTHETIC_TARGET")
    raw = derive_bars(_rows(), stratum=stratum, source=source,
                      interval=interval, cutoff_utc=PROTECTED_CUTOFF)
    full = apply_spread_rule(raw, identification)
    digest = hashlib.sha256()
    for chunk in costs.publication._csv_chunks(full):
        digest.update(chunk)
    group = {
        "symbol": source.symbol, "timeframe": "M15", "source_kind": "m5_derived",
        "cost_status": costs.publication.COST_READY, "fine_source": asdict(source),
        "available_interval": asdict(interval), "membership_sha256": full.membership_sha256,
        "spread_identification_sha256": identification.evidence_sha256,
        "target_bars_sha256": digest.hexdigest(),
        "statistics": {
            "status": "CALCULATED", "quantile_method": costs.QUANTILE_METHOD,
            "point_price": 0.01, "target_prefix_rows": 2, "invalid_rows": 0, "valid_rows": 2,
            "tiers": {
                "P75": {"usability": "POSITIVE_QUANTILE", "spread_points": 4,
                        "full_spread_price": 4 * 0.01, "half_spread_price": 4 * 0.01 / 2},
                "P95": {"usability": "POSITIVE_QUANTILE", "spread_points": 9,
                        "full_spread_price": 9 * 0.01, "half_spread_price": 9 * 0.01 / 2},
            },
        },
    }
    return source, raw, full, identification, group


def _pins(profile_sha="9" * 64):
    receipt = {"generation_id": "NO_NATIVE_VERIFICATION_FIXTURE",
               "native_universe_sha256": "6" * 64, "config_sha256": "7" * 64}
    return costs.DerivedCostEditionPins(
        _CANDIDATE_ROOT, canonical_json_bytes(receipt), _digest(receipt),
        receipt["generation_id"], "6" * 64, "7" * 64, "8" * 64, profile_sha)


def _shifted_start_fixture():
    """Invented M5 DATA; zero-volume interior member isolates count drift."""
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    spreads = (4, 1, 6, 6, 4, 9, 6, 4, 7, 7, 4, 9)
    rows = tuple(Bar((start + timedelta(minutes=5 * index)).isoformat(),
                     (start + timedelta(minutes=5 * (index + 1))).isoformat(),
                     100.0, 102.0, 99.0, 101.0, 0 if index == 8 else 10,
                     None, spread) for index, spread in enumerate(spreads))
    interval = Interval(start.isoformat(), (start + timedelta(hours=1)).isoformat())
    stratum = Stratum("M30", "NO_NATIVE_VERIFICATION_FIXTURE", "SYNTHETIC_FIXED_UTC")
    control = derive_bars(rows, stratum=stratum, source=_source("SYNTHETIC_CONTROL"),
                          interval=interval, cutoff_utc=PROTECTED_CUTOFF)
    native_fixture = tuple(replace(row.value, spread=value)
                           for row, value in zip(control.bars, (1, 4), strict=True))
    identification = identify_spread_rule(control, native_fixture,
        native_source=_source("SYNTHETIC_CONTROL", "M30"))
    source = _source("SYNTHETIC_TARGET")
    raw = derive_bars(rows, stratum=stratum, source=source,
                      interval=interval, cutoff_utc=PROTECTED_CUTOFF)
    full = apply_spread_rule(raw, identification)
    request_interval = Interval((start + timedelta(minutes=30)).isoformat(),
                                interval.end_exclusive_utc)
    return full, identification, rows[6:], request_interval


class DerivedCostJoinTests(unittest.TestCase):
    def setUp(self):
        self.source, self.raw, self.full, self.identification, self.group = _fixture()

    def validate(self, group=None, series=None, point=0.01):
        return costs._validate_full_series(self.group if group is None else group,
                                           self.raw if series is None else series,
                                           self.identification, self.source, point)

    def test_original_min_application_reproduces_declared_output_identity(self):
        self.assertEqual(self.identification.selected_spread_rule, "min")
        self.assertEqual(self.validate(), self.full)
        self.assertEqual([row.value.spread for row in self.full.bars], [1, 4])

    def test_recorded_target_hash_cannot_be_replaced_by_membership_hash(self):
        group = deepcopy(self.group)
        group["target_bars_sha256"] = group["membership_sha256"]
        with self.assertRaises(costs.DerivedCostError) as caught:
            self.validate(group)
        self.assertEqual(caught.exception.code, "COST_UNAVAILABLE")
        self.assertEqual(caught.exception.reason_code, "COST_TARGET_OUTPUT_MISMATCH")

    def test_membership_root_and_identification_must_match_separately(self):
        for name in ("membership_sha256", "spread_identification_sha256"):
            with self.subTest(name=name):
                group = deepcopy(self.group)
                group[name] = "a" * 64
                with self.assertRaises(costs.DerivedCostError) as caught:
                    self.validate(group)
                self.assertEqual(caught.exception.reason_code, "COST_MEMBERSHIP_OR_IDENTIFICATION_MISMATCH")

    def test_source_point_and_available_interval_are_exact_bindings(self):
        mutations = (("point", None), ("fine_source", None), ("available_interval", None))
        for name, _ in mutations:
            with self.subTest(name=name):
                group = deepcopy(self.group)
                if name == "point":
                    action = lambda: self.validate(point=0.02)
                elif name == "fine_source":
                    group[name]["bars_sha256"] = "a" * 64
                    action = lambda: self.validate(group)
                else:
                    group[name]["start_utc"] = "2024-01-01T00:15:00+00:00"
                    action = lambda: self.validate(group)
                with self.assertRaises(costs.DerivedCostError):
                    action()

    def test_required_p75_p95_order_and_price_consistency_cannot_fallback(self):
        for mutation in ("zero_p75", "p95_below_p75", "wrong_full_price", "wrong_method"):
            with self.subTest(mutation=mutation):
                group = deepcopy(self.group)
                stats = group["statistics"]
                if mutation == "zero_p75":
                    stats["tiers"]["P75"]["spread_points"] = 0
                elif mutation == "p95_below_p75":
                    stats["tiers"]["P95"]["spread_points"] = 3
                elif mutation == "wrong_full_price":
                    stats["tiers"]["P75"]["full_spread_price"] = 0.03
                else:
                    stats["quantile_method"] = "UNADOPTED_METHOD"
                with self.assertRaises(costs.DerivedCostError) as caught:
                    self.validate(group)
                self.assertEqual(caught.exception.code, "COST_UNAVAILABLE")

    def test_profile_status_must_be_cost_ready(self):
        group = deepcopy(self.group)
        group["cost_status"] = "COST_UNAVAILABLE"
        with self.assertRaises(costs.DerivedCostError) as caught:
            self.validate(group)
        self.assertEqual(caught.exception.reason_code, "COST_CELL_UNAVAILABLE")

    def test_a_different_matched_selector_does_not_extend_the_method(self):
        alternative = replace(self.identification, selected_spread_rule="max",
                              matching_candidates=("max",))
        with self.assertRaises(costs.DerivedCostError) as caught:
            costs._apply_min(self.raw, alternative)
        self.assertEqual(caught.exception.reason_code, "COST_CONTROL_MIN_RULE_UNAVAILABLE")

    def test_later_start_constructor_membership_is_view_local_and_accepted(self):
        full, identification, rows, interval = _shifted_start_fixture()
        self.assertEqual(identification.matching_candidates, ("min",))
        request = derive_bars(rows, stratum=full.stratum, source=full.source,
            interval=interval, cutoff_utc=interval.end_exclusive_utc)
        selected = apply_spread_rule(request, identification)
        expected = full.bars[1]
        self.assertNotEqual(selected.bars[0].membership_sha256, expected.membership_sha256)
        self.assertNotEqual(selected.membership_sha256, full.membership_sha256)
        self.assertEqual(selected.bars[0].value, expected.value)
        self.assertEqual(selected.bars[0].member_count, expected.member_count)
        self.assertEqual(selected.bars[0].spread_candidates, expected.spread_candidates)
        self.assertEqual(costs._validate_financial_request(selected, full, identification), selected)

    def test_shifted_start_price_count_and_candidate_mutations_are_refused(self):
        full, identification, rows, interval = _shifted_start_fixture()
        self.assertEqual(identification.matching_candidates, ("min",))
        expected = full.bars[1]
        mutations = {
            "price": (replace(rows[0], high=103.0), *rows[1:]),
            "member_count": rows[:2] + rows[3:],
            "candidate": (replace(rows[0], spread=7), *rows[1:]),
        }
        for name, changed in mutations.items():
            with self.subTest(mutation=name):
                request = derive_bars(changed, stratum=full.stratum, source=full.source,
                    interval=interval, cutoff_utc=interval.end_exclusive_utc)
                selected = apply_spread_rule(request, identification)
                observed = selected.bars[0]
                if name == "price":
                    self.assertNotEqual(observed.value, expected.value)
                    self.assertEqual(observed.member_count, expected.member_count)
                    self.assertEqual(observed.spread_candidates, expected.spread_candidates)
                elif name == "member_count":
                    self.assertEqual(observed.value, expected.value)
                    self.assertNotEqual(observed.member_count, expected.member_count)
                    self.assertEqual(observed.spread_candidates, expected.spread_candidates)
                else:
                    self.assertEqual(observed.value, expected.value)
                    self.assertEqual(observed.member_count, expected.member_count)
                    self.assertNotEqual(observed.spread_candidates, expected.spread_candidates)
                with self.assertRaises(costs.DerivedCostError) as caught:
                    costs._validate_financial_request(selected, full, identification)
                self.assertEqual(caught.exception.reason_code, "COST_REQUEST_CAUSAL_PREFIX_MISMATCH")

    def test_causal_prefix_matches_actual_completed_members(self):
        prefix = derive_bars(_rows()[:3], stratum=self.raw.stratum, source=self.source,
            interval=Interval(self.raw.interval.start_utc, "2024-01-01T00:15:00+00:00"),
            cutoff_utc="2024-01-01T00:15:00+00:00")
        actual = costs._validate_prefix(prefix, self.full, self.identification)
        self.assertEqual(actual.bars, self.full.bars[:1])
        with self.assertRaises(costs.DerivedCostError) as caught:
            costs._validate_prefix(replace(prefix, cutoff_utc="2024-01-01T00:10:00+00:00"),
                                   self.full, self.identification)
        self.assertEqual(caught.exception.code, "COST_UNAVAILABLE")

    def test_financial_request_requires_original_min_application(self):
        with self.assertRaises(costs.DerivedCostError) as caught:
            costs._validate_financial_request(self.raw, self.full, self.identification)
        self.assertEqual(caught.exception.reason_code, "COST_REQUEST_MIN_RULE_NOT_FROZEN")

    def test_financial_request_refuses_a_genuinely_different_selector(self):
        control_rows = tuple(replace(row, spread=spread) for row, spread in
                             zip(_rows(), (6, 1, 4, 6, 4, 9), strict=True))
        control = derive_bars(control_rows, stratum=self.raw.stratum,
            source=_source("SYNTHETIC_CONTROL"), interval=self.raw.interval,
            cutoff_utc=PROTECTED_CUTOFF)
        native_fixture = tuple(replace(row.value, spread=value)
                               for row, value in zip(control.bars, (6, 9), strict=True))
        other_identification = identify_spread_rule(control, native_fixture,
            native_source=_source("SYNTHETIC_CONTROL", "M15"))
        self.assertEqual(other_identification.matching_candidates, ("max",))
        other_series = apply_spread_rule(self.raw, other_identification)
        self.assertEqual([row.value.spread for row in other_series.bars], [6, 9])
        with self.assertRaises(costs.DerivedCostError) as caught:
            costs._validate_financial_request(other_series, self.full, self.identification)
        self.assertEqual(caught.exception.reason_code, "COST_REQUEST_MIN_RULE_NOT_FROZEN")

    def test_financial_request_accepts_already_min_applied_registered_values(self):
        actual = costs._validate_financial_request(self.full, self.full, self.identification)
        self.assertEqual(actual, self.full)
        self.assertEqual(actual.selected_spread_rule, "min")
        self.assertEqual(actual.spread_identification_sha256, self.identification.evidence_sha256)
        self.assertEqual([row.value.spread for row in actual.bars], [1, 4])

    def test_changed_prefix_values_cannot_borrow_member_digests(self):
        changed = replace(self.full.bars[0], value=replace(self.full.bars[0].value, high=103.0))
        forged = replace(self.full, bars=(changed, *self.full.bars[1:]))
        with self.assertRaises(costs.DerivedCostError) as caught:
            costs._validate_prefix(forged, self.full, self.identification)
        self.assertEqual(caught.exception.code, "COST_UNAVAILABLE")

    def test_real_frozen_tiers_keep_genuine_m5_and_identical_nonprice_fields(self):
        bound = costs.BoundDerivedCosts(_pins(), self.group, self.source, "b" * 64, 0.01)
        pair = {tier: bound.resolve(self.source.symbol, "M15", tier, point=0.01,
                                    source_identity_sha256="b" * 64) for tier in ("P75", "P95")}
        for tier in pair.values():
            self.assertIs(type(tier), FrozenSpreadTier)
            tier.validate_execution(self.source.symbol, "M15", 0.01)
            body = tier.descriptor()
            self.assertEqual(body["schema_version"], TIER_SCHEMA)
            self.assertEqual(body["source_timeframe"], "M5")
            self.assertEqual(body["source_binding"]["target_bars_sha256"], self.source.bars_sha256)
            self.assertEqual(body["m5_derived_cost_binding"]["derived_target_bars_sha256"],
                             self.group["target_bars_sha256"])
        bodies = [value.descriptor() for value in pair.values()]
        for body in bodies:
            for field in ("tier", "spread_points", "full_spread_price"):
                body.pop(field)
        self.assertEqual(bodies[0], bodies[1])

    def test_from_descriptor_exact_compare_refuses_extra_or_changed_provenance(self):
        bound = costs.BoundDerivedCosts(_pins(), self.group, self.source, "b" * 64, 0.01)
        original = bound.resolve(self.source.symbol, "M15", "P75", point=0.01,
                                 source_identity_sha256="b" * 64)
        body = original.descriptor()
        self.assertEqual(bound.from_descriptor(body, symbol=self.source.symbol,
            timeframe="M15", point=0.01, source_identity_sha256="b" * 64)._json, original._json)
        body["m5_derived_cost_binding"]["membership_sha256"] = "c" * 64
        with self.assertRaises(costs.DerivedCostError) as caught:
            bound.from_descriptor(body, symbol=self.source.symbol, timeframe="M15",
                                  point=0.01, source_identity_sha256="b" * 64)
        self.assertEqual(caught.exception.reason_code, "COST_TIER_DESCRIPTOR_MISMATCH")

    def _execution_window_fixture(self):
        return {
            "schema_version": "fxpm.m5-derived-price-window.v1", "source_kind": "m5_derived",
            "symbol": self.source.symbol, "timeframe": "M15", "m5_view_contract_sha256": "c" * 64,
            "view_key": "d" * 64, "view_payload_sha256": "e" * 64,
            "view_membership_sha256": self.full.membership_sha256, "view_frame_sha256": "f" * 64,
            "view_start_utc": self.full.interval.start_utc,
            "view_end_exclusive_utc": self.full.interval.end_exclusive_utc,
            "view_last_close_utc": self.full.bars[-1].value.close_utc, "view_rows": len(self.full.bars),
            "base_native_prefix_index_sha256": "a" * 64,
            "base_native_prefix_content_sha256": "b" * 64,
        }

    def test_execution_view_window_is_identical_and_detached_across_tiers(self):
        window = self._execution_window_fixture()
        expected = deepcopy(window)
        bound = costs.BoundDerivedCosts(_pins(), self.group, self.source, "b" * 64, 0.01,
                                       execution_window=window)
        window["view_rows"] = 999
        bodies = [bound.resolve(self.source.symbol, "M15", tier, point=0.01,
                    source_identity_sha256="b" * 64).descriptor() for tier in ("P75", "P95")]
        for body in bodies:
            self.assertEqual(body["m5_execution_view_binding"], expected)
        for body in bodies:
            for field in ("tier", "spread_points", "full_spread_price"):
                body.pop(field)
        self.assertEqual(bodies[0], bodies[1])

    def test_from_descriptor_refuses_execution_window_tampering(self):
        window = self._execution_window_fixture()
        bound = costs.BoundDerivedCosts(_pins(), self.group, self.source, "b" * 64, 0.01,
                                       execution_window=window)
        original = bound.resolve(self.source.symbol, "M15", "P75", point=0.01,
                                 source_identity_sha256="b" * 64).descriptor()
        changed = deepcopy(original)
        changed["m5_execution_view_binding"]["view_payload_sha256"] = "a" * 64
        with self.assertRaises(costs.DerivedCostError) as caught:
            bound.from_descriptor(changed, symbol=self.source.symbol, timeframe="M15", point=0.01,
                                  source_identity_sha256="b" * 64)
        self.assertEqual(caught.exception.reason_code, "COST_TIER_DESCRIPTOR_MISMATCH")

    def test_cost_reopen_route_codec_preserves_recorded_identity_and_detaches(self):
        pins = _pins()
        route = costs._cost_reopen_route(pins)
        restored = costs._decode_cost_reopen_route(route)
        self.assertEqual(restored.identity(), pins.identity())
        self.assertEqual(restored.root, pins.root)
        self.assertEqual(restored.receipt_json, pins.receipt_json)
        route["cost_edition_identity"]["profile_sha256"] = "f" * 64
        self.assertEqual(restored.profile_sha256, pins.profile_sha256)

    def test_cost_reopen_route_refuses_receipt_identity_change_and_unknown_fields(self):
        for mutation in ("receipt", "extra"):
            route = costs._cost_reopen_route(_pins())
            if mutation == "receipt":
                route["receipt"]["generation_id"] = "changed"
            else:
                route["authority"] = True
            with self.assertRaises(costs.DerivedCostError) as caught:
                costs._decode_cost_reopen_route(route)
            self.assertEqual(caught.exception.code, "COST_UNAVAILABLE")

    def test_receipt_identity_is_canonical_body_not_routing_path(self):
        pins = _pins()
        self.assertEqual(costs._receipt(pins)["generation_id"], pins.generation_id)
        self.assertEqual(set(pins.identity()), {"receipt_sha256", "generation_id",
            "native_universe_sha256", "plan_sha256", "implementation_sha256", "profile_sha256"})
        with self.assertRaises(costs.DerivedCostError):
            costs._receipt(replace(pins, receipt_sha256="d" * 64))

    def test_profile_pin_and_recorded_method_policy_are_independent(self):
        profile = {
            "source_kind": "m5_derived", "recipe_sha256": costs.publication.RECIPE_SHA256,
            "implementation_sha256": "8" * 64, "plan_sha256": "7" * 64,
            "protected_cutoff_utc": PROTECTED_CUTOFF,
            "calculation_policy": costs.CALCULATION_POLICY,
            "source_convention": costs.SOURCE_CONVENTION,
        }
        pins = _pins(_digest(profile))
        self.assertEqual(costs._profile(profile, pins), profile)
        for field in ("recipe_sha256", "implementation_sha256", "plan_sha256",
                      "protected_cutoff_utc", "calculation_policy", "source_convention"):
            with self.subTest(field=field):
                changed = deepcopy(profile)
                changed[field] = "UNADOPTED"
                with self.assertRaises(costs.DerivedCostError) as caught:
                    costs._profile(changed, replace(pins, profile_sha256=_digest(changed)))
                self.assertEqual(caught.exception.reason_code, "COST_PROFILE_POLICY_MISMATCH")
        with self.assertRaises(costs.DerivedCostError) as caught:
            costs._profile(profile, replace(pins, profile_sha256="e" * 64))
        self.assertEqual(caught.exception.reason_code, "COST_PROFILE_IDENTITY_MISMATCH")


    def test_actual_financial_consumer_type_guard_includes_no_trade_without_mock_verification(self):
        import ast
        import inspect
        import textwrap
        import pm_m5_job_view as job
        self.assertEqual(Path(job.__file__).resolve(), _CANDIDATE_ROOT / "pm_m5_job_view.py")
        tree = ast.parse(textwrap.dedent(inspect.getsource(costs.RecordedDerivedCosts.bind_view)))
        predicate = next(node for node in ast.walk(tree) if isinstance(node, ast.Compare)
            and isinstance(node.left, ast.Call) and isinstance(node.left.func, ast.Name)
            and node.left.func.id == "type" and len(node.left.args) == 1
            and isinstance(node.left.args[0], ast.Name) and node.left.args[0].id == "request"
            and len(node.ops) == 1 and isinstance(node.ops[0], ast.In))
        self.assertEqual({node.id for node in predicate.comparators[0].elts},
            {"SlotViewBinding", "AccountViewBinding", "NoTradeAccountViewBinding"})
        # Only the actual type-membership predicate executes, not bind_view or any verifier.
        code = compile(ast.fix_missing_locations(ast.Expression(predicate)),
            "<synthetic financial-consumer type guard>", "eval")
        scope = {"type": type, "SlotViewBinding": job.SlotViewBinding,
            "AccountViewBinding": job.AccountViewBinding, "NoTradeAccountViewBinding": job.NoTradeAccountViewBinding}
        consumers = ((job.SlotViewBinding("1" * 64, "UNIT_SLOT"), True),
            (job.AccountViewBinding("UNIT_OWNER_MEASUREMENT", "2024-01-02T00:30:00+00:00", "2" * 64, "3" * 64), True),
            (job.NoTradeAccountViewBinding("UNIT_CALLER_VERIFIED_ALL_NO_TRADE",
                "2024-01-02T00:30:00+00:00", "4" * 64, "5" * 64), True),
            (job.CalibrationViewBinding("6" * 64), False), (object(), False))
        for consumer, accepted in consumers:
            with self.subTest(consumer=type(consumer).__name__):
                self.assertIs(eval(code, {"__builtins__": {}}, {**scope, "request": consumer}), accepted)



if __name__ == "__main__":
    unittest.main()
