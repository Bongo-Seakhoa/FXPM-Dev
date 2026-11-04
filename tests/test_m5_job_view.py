"""Cell-owned M5 contracts: SYNTHETIC_UNIT_DOUBLES_ONLY.

All archive, artifact and external-row values are invented DATA. No native seal,
canonical-source read, physical append/readback, durable writer or financial
qualification is proved. Private revalidation callbacks return () deliberately.
"""
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import unittest

import pm_m5_job_view as job
from pm_explicit_derived_history import Bar, Interval, SourceIdentity, SpreadIdentification, Stratum, PROTECTED_CUTOFF
from pm_explicit_observed_clock import ClockStratum, ClockExtent, freeze_observed_clock

UNIT_PROVENANCE = "SYNTHETIC_UNIT_DOUBLES_ONLY_NOT_NATIVE_AUTHORITY_OR_DURABILITY_PROOF"
FALSE_FIELDS = ("full_native_context_eligibility", "financial_consumer_ready",
                "qualification_authority", "capital_authority", "live_authority", "order_authority")
TERMINAL = "unit-cell-terminal"


def _sha(label):
    return hashlib.sha256((UNIT_PROVENANCE + ":" + label).encode("ascii")).hexdigest()


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def _stamp(minutes):
    return (datetime(2024, 1, 2, tzinfo=timezone.utc) + timedelta(minutes=minutes)).isoformat(timespec="seconds")


def _interval(start=0, end=30):
    return Interval(_stamp(start), _stamp(end))


def _budgets(**changes):
    values = dict(max_source_rows=16, max_source_bytes=131072, max_view_rows=16,
        max_view_bytes=131072, max_cache_rows=32, max_cache_bytes=262144)
    values.update(changes)
    return job.ViewBudgets(**values)


def _source(symbol="UNITUSD", timeframe="M5"):
    return SourceIdentity(symbol, timeframe, "UNIT_SOURCE_NOT_NATIVE_AUTHORITY",
        _sha("activation"), _sha(symbol + timeframe + "bars"), _sha(symbol + timeframe + "sidecar"),
        _sha(symbol + timeframe + "provenance"), _sha(symbol + timeframe + "request"), 0.0001)


def _rows():
    return tuple(Bar(_stamp(i), _stamp(i + 5), 1.0, 1.1, 0.9, 1.05, 2, 3, 4)
        for i in range(0, 30, 5))


def _archive():
    span = _interval()
    return job.M5ArchiveSource("UNITUSD", _source(), _sha("UNIT_SOURCE_IDENTITY"),
        span, span, "UNIT_CLASS", 6, 1, 1, _sha("UNIT_METADATA"),
        "C:/SYNTHETIC_UNIT_DOUBLES_ONLY_NOT_OPENED/bars.csv",
        "C:/SYNTHETIC_UNIT_DOUBLES_ONLY_NOT_OPENED/provenance.json")


def _request(view_id="view-a", **changes):
    values = dict(view_id=view_id, source=_source(), timeframe="M15", interval=_interval(),
        cutoff_utc=PROTECTED_CUTOFF, constructor_implementation_sha256=_sha("UNIT_CONSTRUCTOR"),
        gap_findings_sha256=_sha("UNIT_GAPS"), result_key=TERMINAL, recipe_sha256=job.RECIPE_SHA256,
        consumer=job.SlotViewBinding(_sha("UNIT_SLOT_REQUEST"), "UNIT_SLOT"),
        stratum=Stratum("M15", "UNIT_CLASS", "UNIT_FIXED_ALIGNMENT"))
    values.update(changes)
    return job.ViewRequest(**values)


def _binding(budgets, requests=None, **changes):
    requests = (_request(),) if requests is None else requests
    values = {name: _sha(name) for name in ("m5_view_contract_sha256", "generation_context_sha256",
        "config_sha256", "effective_config_sha256", "search_process_receipt_sha256", "precommit_sha256")}
    values.update(run_id="UNIT_RUN", cell_id="UNIT_CELL", view_budgets_sha256=_digest(asdict(budgets)),
        view_plan_sha256=job.value_plan_sha256(requests), required_result_keys=(TERMINAL,), terminal_result_key=TERMINAL)
    values.update(changes)
    return job.DerivedJobBinding(**values)


class _UnitLoader:
    """Pure callback, never a NativePrefixProduct or verifier stand-in."""
    provenance = UNIT_PROVENANCE

    def __init__(self, binding, budgets, *, rows=None, source=None):
        self.binding, self.budgets = binding, budgets
        self.rows = _rows() if rows is None else rows
        self.source = _archive() if source is None else source
        self.calls = []

    def __call__(self, request):
        self.calls.append(request.view_id)
        rows = tuple(row for row in self.rows
            if request.interval.start_utc <= row.open_utc < request.interval.end_exclusive_utc
            and row.close_utc <= request.cutoff_utc)
        return job._build_view(self.binding, request, rows, source=self.source,
            examined_interval=self.source.prefix_interval, budgets=self.budgets)


class M5JobViewTests(unittest.TestCase):
    def test_reopen_metadata_codec_roundtrips_typed_original_arguments(self):
        metadata = job._encode_m5_reopen_metadata(binding=self.binding, budgets=self.budgets,
            requests=(self.request,), product_cutoffs=(self.request.cutoff_utc,), source_identity=self.request.source)
        binding, budgets, requests, cutoffs, source = job._decode_m5_reopen_metadata(metadata)
        self.assertEqual((binding, budgets, requests, source),
                         (self.binding, self.budgets, (self.request,), self.request.source))
        self.assertEqual(cutoffs, (self.request.cutoff_utc,))
        metadata["requests"][0]["view_id"] = "changed"
        self.assertEqual(requests[0].view_id, self.request.view_id)

    def test_reopen_metadata_rejects_unknown_fields_boolean_budgets_and_pin_drift(self):
        for mutation in ("extra", "bool_budget", "plan"):
            metadata = job._encode_m5_reopen_metadata(binding=self.binding, budgets=self.budgets,
                requests=(self.request,), product_cutoffs=(self.request.cutoff_utc,), source_identity=self.request.source)
            if mutation == "extra":
                metadata["unexpected"] = True
            elif mutation == "bool_budget":
                metadata["budgets"]["max_source_rows"] = True
            else:
                metadata["view_plan_sha256"] = _sha("changed-plan")
            with self.assertRaises(job.M5JobViewError):
                job._decode_m5_reopen_metadata(metadata)

    def test_reopen_metadata_stops_clock_requests_without_changing_live_support(self):
        clock = replace(self.request, timeframe="H4", stratum=None)
        self.assert_code("M5_REOPEN_CLOCK_NOT_ENABLED", job._encode_m5_reopen_metadata,
            binding=self.binding, budgets=self.budgets, requests=(clock,),
            product_cutoffs=(clock.cutoff_utc,), source_identity=clock.source)

    def test_reopen_metadata_restores_original_typed_spread_identification(self):
        from pm_explicit_derived_history import derive_bars, identify_spread_rule
        control_rows = tuple(replace(row, spread=spread) for row, spread in
                             zip(_rows(), (6, 1, 4, 6, 4, 9), strict=True))
        control = derive_bars(control_rows, stratum=self.request.stratum,
            source=_source("CONTROL"), interval=self.request.interval, cutoff_utc=PROTECTED_CUTOFF)
        native_fixture = tuple(replace(row.value, spread=spread) for row, spread in
                               zip(control.bars, (1, 4), strict=True))
        identification = identify_spread_rule(control, native_fixture,
                                              native_source=_source("CONTROL", "M15"))
        selected = replace(self.request, spread_identification=identification)
        binding = _binding(self.budgets, (selected,))
        metadata = job._encode_m5_reopen_metadata(binding=binding, budgets=self.budgets,
            requests=(selected,), product_cutoffs=(selected.cutoff_utc,), source_identity=selected.source)
        self.assertEqual(job._decode_m5_reopen_metadata(metadata)[2][0].spread_identification,
                         identification)
        self.assertEqual(identification.matching_candidates, ("min",))

    def test_reopen_metadata_bound_refuses_oversized_plain_payload(self):
        self.assert_code("M5_REOPEN_METADATA_LIMIT", job._bounded_reopen_plain,
                         {"oversized": "x" * job.MAX_REOPEN_METADATA_BYTES})
        self.assert_code("M5_REOPEN_METADATA_LIMIT", job._bounded_reopen_plain,
                         {"nodes": [None] * 100000})

    def setUp(self):
        self.budgets = _budgets()
        self.request = _request()
        self.binding = _binding(self.budgets, (self.request,))

    def assert_code(self, code, callback, *args, **kwargs):
        with self.assertRaises(job.M5JobViewError) as caught:
            callback(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def state(self, requests=None, *, budgets=None, binding=None):
        budgets = self.budgets if budgets is None else budgets
        requests = (self.request,) if requests is None else requests
        binding = _binding(budgets, requests) if binding is None else binding
        return job._JobState(binding, requests, budgets=budgets)

    def result(self, state, key=TERMINAL, **changes):
        values = dict(result_sha256=_sha("CALLER_ARTIFACT_NOT_WRITTEN:" + key),
            job_binding_sha256=state.binding_sha256, consumer_binding_sha256=state.consumer_bindings_for(key),
            result_descriptor_sha256=_sha("CALLER_REPORTED_GRID_ROW:" + key))
        values.update(changes)
        return job.CallerValidatedResultBinding(**values)

    def grid(self, key=TERMINAL, **changes):
        sha = _sha("CALLER_REPORTED_GRID_ROW:" + key)
        values = dict(path="C:/SYNTHETIC_UNIT_DOUBLES_ONLY_NOT_OPENED/end-grid.jsonl",
            row_sha256=sha, appended=True, readback_row_sha256=sha)
        values.update(changes)
        return job.EndGridResultBinding(**values)

    def finish(self, state, *, results=None, grids=None, revalidate=None):
        return state.finish({TERMINAL: self.result(state)} if results is None else results,
            end_grid_rows={TERMINAL: self.grid()} if grids is None else grids,
            revalidate=(lambda: ()) if revalidate is None else revalidate)

    def released_state(self):
        state = self.state()
        state.acquire("view-a", _UnitLoader(state.binding, self.budgets)).release()
        return state

    def clock_request(self, timeframe="H4"):
        span = _interval()
        template = freeze_observed_clock((span,), stratum=ClockStratum(timeframe, _sha("UNIT_BROKER"), "UNIT_CLASS"),
            clock_source=_source("AAA", timeframe), corroboration_source=_source("BBB", timeframe), interval=span)
        return _request(timeframe=timeframe, stratum=None, clock_template=template,
            clock_extent=ClockExtent(span, span, span, span, ()))

    def test_public_owner_rejects_ordinary_context_before_genuine_product_access(self):
        self.assert_code("M5_JOB_ORIGINAL_PREFIX_PRODUCTS_REQUIRED", job.M5JobViews, object(),
            canonical_data_dir=Path("C:/SYNTHETIC_UNIT_DOUBLES_ONLY_NOT_OPENED"), binding=self.binding,
            fine_products=(), requests=(self.request,), budgets=self.budgets)

    def test_job_requests_budgets_and_value_only_views_are_frozen_data_only(self):
        view = _UnitLoader(self.binding, self.budgets)(self.request)
        for value, field, change in ((self.binding, "run_id", "OTHER"), (self.budgets, "max_source_rows", 999),
                (self.request, "view_id", "OTHER"), (view, "rows", ()), (view.rows[0], "close", 99)):
            with self.subTest(field=field):
                with self.assertRaises(FrozenInstanceError):
                    setattr(value, field, change)
        self.assertIsInstance(view.rows, tuple)
        self.assertFalse(hasattr(view, "job_binding_sha256"))
        self.assertFalse(hasattr(view, "consumer_binding_sha256"))
        self.assertEqual(view.view_key, job._request(self.binding, self.request))
        self.assertEqual(view.data_scope, job.DATA_SCOPE)
        self.assertEqual(view.profile_status, "PROFILE_UNVERIFIED")
        self.assertEqual(view.constructor_implementation_sha256, self.request.constructor_implementation_sha256)
        self.assertEqual(view.gap_findings_sha256, self.request.gap_findings_sha256)
        for field in FALSE_FIELDS:
            self.assertIs(getattr(view, field), False)
        for field in ("job_binding_sha256", "consumer_binding_sha256", "m5_view_contract_sha256"):
            self.assertIsNone(getattr(job.DerivedViewLease, field).fset)

    def test_requests_are_a_finite_immutable_unique_roster_bound_to_job_plan(self):
        for requests in ((), [self.request], (self.request, self.request)):
            with self.subTest(requests=requests):
                self.assert_code("M5_VIEW_PLAN_INVALID", job._JobState,
                    self.binding, requests, budgets=self.budgets)
        self.assert_code("M5_JOB_BINDING_INVALID", self.state,
            binding=replace(self.binding, search_process_receipt_sha256="unissued"))
        self.assert_code("M5_VIEW_PLAN_BINDING_MISMATCH", self.state,
            binding=replace(self.binding, view_plan_sha256=_sha("OTHER_PLAN")))
        axes = dict(view_id=self.request.view_id, symbol=self.request.source.symbol,
            timeframe=self.request.timeframe, interval=asdict(self.request.interval), cutoff_utc=self.request.cutoff_utc,
            recipe_sha256=self.request.recipe_sha256, constructor_implementation_sha256=self.request.constructor_implementation_sha256,
            gap_findings_sha256=self.request.gap_findings_sha256, purpose=self.request.purpose,
            source_kind=self.request.source_kind, stratum=asdict(self.request.stratum),
            clock_template_sha256=None, clock_axes_sha256=None, spread_identification_sha256=None)
        self.assertEqual(job.declared_view_plan_sha256((axes,)), self.binding.view_plan_sha256)
        self.assert_code("M5_VIEW_PLAN_INVALID", job.declared_view_plan_sha256,
            ({**axes, "source": asdict(self.request.source)},))

    def test_multiple_saved_consumers_and_cutoffs_belong_to_one_cell(self):
        short = replace(self.request, view_id="saved-slot", interval=_interval(0, 15), cutoff_utc=_stamp(15),
            consumer=job.SlotViewBinding(_sha("SAVED_REQUEST"), "SAVED_SLOT"))
        account = replace(self.request, view_id="account", consumer=job.AccountViewBinding(
            "UNIT_ACCOUNT", _stamp(30), _sha("SAVED_OWNER"), _sha("GENUINE_TRADE_CONTRACT_UNIT_VALUE")))
        state = self.state((short, account));loader = _UnitLoader(state.binding, self.budgets)
        first, second = state.acquire(short.view_id, loader), state.acquire(account.view_id, loader)
        self.assertEqual(len(first.view.rows), 1)
        self.assertEqual(len(second.view.rows), 2)
        digests = tuple(sorted((first.consumer_binding_sha256, second.consumer_binding_sha256)))
        self.assertNotEqual(*digests)
        first.release();second.release()
        summary = self.finish(state)
        self.assertEqual(dict(summary.results)[TERMINAL].consumer_binding_sha256, digests)

    def test_source_choice_and_examined_window_mismatch_refuse(self):
        source = replace(_archive(), source_identity=replace(_source(), bars_sha256=_sha("OTHER_SOURCE")))
        self.assert_code("M5_VIEW_SOURCE_BINDING_INVALID", job._build_view,
            self.binding, self.request, _rows(), source=source, examined_interval=_interval(), budgets=self.budgets)
        self.assert_code("M5_VIEW_EXAMINED_INTERVAL_MISMATCH", job._build_view,
            self.binding, self.request, _rows(), source=_archive(), examined_interval=_interval(5, 30), budgets=self.budgets)

    def test_cutoff_and_outside_rows_refuse_instead_of_shortening(self):
        for cutoff in ("2024-07-14T08:10:00+00:00", _stamp(25)):
            with self.subTest(cutoff=cutoff):
                self.assert_code("M5_VIEW_CUTOFF_VIOLATION", job._request, self.binding,
                    replace(self.request, cutoff_utc=cutoff))
        rows = (*_rows()[:-1], replace(_rows()[-1], close_utc=_stamp(35)))
        self.assert_code("M5_VIEW_CUTOFF_OR_INTERVAL_VIOLATION", job._build_view,
            self.binding, self.request, rows, source=_archive(), examined_interval=_interval(), budgets=self.budgets)

    def test_examined_coverage_keeps_leading_inner_trailing_and_empty_gaps_honest(self):
        view = _UnitLoader(self.binding, self.budgets, rows=(_rows()[1], _rows()[4]))(self.request)
        self.assertEqual(view.coverage.examined_intervals, (_interval(),))
        self.assertEqual(view.coverage.unexamined_intervals, ())
        self.assertEqual(view.coverage.observed_gaps, (_interval(0, 5), _interval(10, 20), _interval(25, 30)))
        self.assertEqual(job._observed_gaps(_interval(), ()), (_interval(),))
        self.assertEqual(job._observed_gaps(_interval(), _rows()), ())
        self.assertEqual(view.coverage.available_interval, _interval(5, 25))
        self.assertEqual(view.coverage.omitted_intervals, (_interval(0, 5), _interval(25, 30)))

    def test_examined_hull_and_retained_windows_have_distinct_missing_intervals(self):
        required = _interval(0, 45)
        self.assertEqual(job._missing(required, (_interval(0, 45),)), ())
        self.assertEqual(job._missing(required, (_interval(0, 15), _interval(30, 45))), (_interval(15, 30),))
        self.assertEqual(job._missing(required, ()), (required,))

    def test_budget_fields_require_positive_exact_ints_and_matching_job_binding(self):
        for value in (0, -1, True, 1.5):
            with self.subTest(value=value):
                budgets = _budgets(max_source_rows=value)
                self.assert_code("M5_VIEW_BUDGETS_INVALID", self.state,
                    budgets=budgets, binding=_binding(budgets))
        self.assert_code("M5_VIEW_BUDGET_BINDING_MISMATCH", self.state,
            budgets=_budgets(max_source_rows=17), binding=self.binding)

    def test_each_logical_row_or_byte_bound_refuses_before_view_exposure(self):
        for field, code in (("max_source_rows", "M5_VIEW_SOURCE_ROW_BUDGET"),
                ("max_source_bytes", "M5_VIEW_SOURCE_BYTE_BUDGET"), ("max_view_rows", "M5_VIEW_ROW_BUDGET"),
                ("max_view_bytes", "M5_VIEW_BYTE_BUDGET"), ("max_cache_rows", "M5_VIEW_CACHE_ROW_BUDGET"),
                ("max_cache_bytes", "M5_VIEW_CACHE_BYTE_BUDGET")):
            with self.subTest(field=field):
                budgets = _budgets(**{field: 1})
                self.assert_code(code, job._build_view, _binding(budgets), self.request, _rows(),
                    source=_archive(), examined_interval=_interval(), budgets=budgets)

    def test_source_retention_guard_is_preappend_and_discard_releases_logical_usage(self):
        budgets = _budgets(max_source_rows=1);state = self.state(budgets=budgets)
        state._retain_source_row("UNIT_SOURCE", _rows()[0]);before = dict(state.source_usage)
        self.assert_code("M5_VIEW_SOURCE_ROW_BUDGET", state._retain_source_row, "UNIT_SOURCE", _rows()[1])
        self.assertEqual(state.source_usage, before)
        state._discard_source("UNIT_SOURCE");self.assertEqual(state.source_usage, {})
        state._retain_source_row("UNIT_SOURCE", _rows()[1]);state.close()
        self.assertEqual(state.source_usage, {})
        for limits, code in (({"max_source_bytes": 1}, "M5_VIEW_SOURCE_BYTE_BUDGET"),
                            ({"max_cache_bytes": 1}, "M5_VIEW_CACHE_BYTE_BUDGET")):
            with self.subTest(limits=limits):
                state = self.state(budgets=_budgets(**limits))
                self.assert_code(code, state._retain_source_row, "UNIT_SOURCE", _rows()[0])
                self.assertEqual(state.source_usage, {})
                state.close()

    def test_cache_row_budget_counts_source_and_unique_derived_values_together(self):
        budgets = _budgets(max_cache_rows=2);state = self.state(budgets=budgets)
        state._retain_source_row("UNIT_SOURCE", _rows()[0])
        self.assert_code("M5_VIEW_CACHE_ROW_BUDGET", state.acquire, "view-a", _UnitLoader(state.binding, budgets))
        self.assertEqual(state.cache, {})
        self.assertEqual(state.cache_usage, {})
        self.assertEqual(state.source_usage["UNIT_SOURCE"][0], 1)
        state.close();self.assertEqual(state.source_usage, {})

    def test_repeated_leases_reuse_cell_values_until_terminal_grid_and_final_revalidation(self):
        state = self.state((self.request, replace(self.request, view_id="view-b")))
        loader = _UnitLoader(state.binding, self.budgets)
        first, second = state.acquire("view-a", loader), state.acquire("view-b", loader)
        self.assertIs(first.view, second.view)
        first.release();second.release()
        self.assertEqual(len(state.cache), 1)
        self.assert_code("M5_VIEW_LEASE_RELEASED", lambda: first.view)
        again = state.acquire("view-a", loader);again.release()
        self.assertEqual(loader.calls, ["view-a"])
        calls = []
        summary = self.finish(state, revalidate=lambda: calls.append(len(state.cache)) or ())
        self.assertEqual(calls, [1])
        self.assertEqual(state.cache, {})
        self.assertTrue(summary.owned_values_released)

    def test_equal_values_share_within_cell_but_leases_and_owners_stay_private(self):
        other = replace(self.request, view_id="view-b", consumer=job.SlotViewBinding(_sha("OTHER_REQUEST"), "OTHER_SLOT"))
        state = self.state((self.request, other));loader = _UnitLoader(state.binding, self.budgets)
        first, second = state.acquire("view-a", loader), state.acquire("view-b", loader)
        self.assertIs(first.view, second.view)
        self.assertNotEqual(first.consumer_binding_sha256, second.consumer_binding_sha256)
        self.assertEqual(first.job_binding_sha256, state.binding_sha256)
        self.assertEqual(first.m5_view_contract_sha256, state.binding.m5_view_contract_sha256)
        foreign = self.state((other,))
        self.assert_code("M5_VIEW_LEASE_NOT_OWNED", foreign.view, first)
        self.assert_code("M5_VIEW_LEASE_NOT_OWNED", foreign._release, first)
        first.release();second.release();state.close();foreign.close()

    def test_value_key_binds_source_constructor_gap_recipe_policy_and_selector_not_purpose(self):
        base = job._request(self.binding, self.request)
        purpose = replace(self.request, view_id="OTHER", result_key="OTHER_RESULT",
            consumer=job.SlotViewBinding(_sha("OTHER_REQUEST"), "OTHER_SLOT"), execution_required=True)
        purpose_binding = replace(self.binding, required_result_keys=tuple(sorted((TERMINAL, "OTHER_RESULT"))))
        self.assertEqual(base, job._request(purpose_binding, purpose))
        self.assertEqual(job.value_plan_sha256((self.request,)), job.value_plan_sha256((replace(self.request,
            consumer=purpose.consumer, result_key=purpose.result_key, execution_required=True),)))
        calibration = replace(self.request, purpose="COST_EDITION_REPRODUCTION_ONLY",
            consumer=job.CalibrationViewBinding(_sha("RECORDED_PROFILE_UNIT_VALUE")))
        self.assertEqual(base, job._request(self.binding, calibration))
        self.assertNotEqual(job.value_plan_sha256((self.request,)), job.value_plan_sha256((calibration,)))
        variants = (replace(self.request, source=replace(self.request.source, bars_sha256=_sha("OTHER_SOURCE"))),
            replace(self.request, constructor_implementation_sha256=_sha("OTHER_CONSTRUCTOR")),
            replace(self.request, gap_findings_sha256=_sha("OTHER_GAPS")),
            replace(self.request, stratum=replace(self.request.stratum, alignment_identity="OTHER_POLICY")))
        for changed in variants:
            self.assertNotEqual(base, job._request(self.binding, changed))
        self.assertEqual(job.value_plan_sha256((self.request,)), job.value_plan_sha256((variants[0],)))
        for changed in variants[1:]:
            self.assertNotEqual(job.value_plan_sha256((self.request,)), job.value_plan_sha256((changed,)))
        self.assert_code("M5_VIEW_REQUEST_INVALID", job._request, self.binding,
            replace(self.request, recipe_sha256=_sha("OTHER_RECIPE")))
        for field in ("constructor_implementation_sha256", "gap_findings_sha256", "recipe_sha256"):
            self.assert_code("M5_VIEW_REQUEST_INVALID", job._request, self.binding, replace(self.request, **{field: "unissued"}))
        # Inert unissued selector metadata exercises keys, never a successful verifier.
        selector = SpreadIdentification(self.request.stratum, _source("CONTROL", "M5"), _source("CONTROL", "M15"),
            _interval(), 2, (("first", 0), ("last", 2), ("min", 1), ("max", 2)), ("first",), "first",
            "SOURCE_RULE_MATCHED", "UNIT_KEY_ONLY_NOT_VERIFIED", _sha("D"), _sha("N"), _sha("E"))
        selected = replace(self.request, spread_identification=selector)
        alternate = replace(selected, spread_identification=replace(selector, selected_spread_rule="last",
            matching_candidates=("last",), mismatch_counts=(("first", 2), ("last", 0), ("min", 1), ("max", 2))))
        self.assertNotEqual(job._request(self.binding, selected), job._request(self.binding, alternate))

    def test_execution_refusal_precedes_an_already_cached_equal_value(self):
        forbidden = replace(self.request, view_id="execution-view", execution_required=True)
        state = self.state((self.request, forbidden));loader = _UnitLoader(state.binding, self.budgets)
        lease = state.acquire("view-a", loader)
        self.assert_code("M5_VIEW_COST_PROFILE_UNVERIFIED", state.acquire, "execution-view", loader)
        self.assertEqual(loader.calls, ["view-a"]);lease.release()
        rows = {row.view_id: row for row in self.finish(state).views}
        self.assertEqual(rows["execution-view"].status, "STOPPED")
        self.assertEqual(rows["execution-view"].stop_reason, "M5_VIEW_COST_PROFILE_UNVERIFIED")

    def test_only_registered_ids_are_reacquirable_without_future_cutoff_fallback(self):
        state = self.state();loader = _UnitLoader(state.binding, self.budgets)
        self.assert_code("M5_VIEW_NOT_DECLARED", state.acquire, "undeclared-future-cutoff", loader)
        self.assertEqual(loader.calls, [])
        first = state.acquire("view-a", loader);first.release()
        second = state.acquire("view-a", loader)
        self.assertEqual(second.view.cutoff_utc, self.request.cutoff_utc)
        self.assertEqual(loader.calls, ["view-a"]);second.release();state.close()

    def test_wrong_value_identity_cannot_enter_cell_cache(self):
        for field in ("view_key", "constructor_implementation_sha256", "gap_findings_sha256"):
            with self.subTest(field=field):
                state = self.state();view = _UnitLoader(state.binding, self.budgets)(self.request)
                self.assert_code("M5_VIEW_OWNER_BINDING_MISMATCH", state.acquire, "view-a",
                    lambda _: replace(view, **{field: _sha("OTHER_VALUE")}))
                self.assertEqual(state.cache, {});state.close()

    def test_unregistered_lease_access_properties_and_foreign_release_refuse(self):
        state, other = self.state(), self.state()
        forged = job.DerivedViewLease(state, "view-a", _UnitLoader(state.binding, self.budgets)(self.request),
            self.request.consumer, TERMINAL)
        self.assert_code("M5_VIEW_LEASE_NOT_OWNED", lambda: forged.view)
        self.assert_code("M5_VIEW_LEASE_NOT_OWNED", lambda: forged.consumer_binding_sha256)
        lease = state.acquire("view-a", _UnitLoader(state.binding, self.budgets))
        self.assert_code("M5_VIEW_LEASE_NOT_OWNED", other._release, lease)
        lease.release()
        self.assert_code("M5_VIEW_LEASE_RELEASED", lease.release)
        self.assert_code("M5_VIEW_LEASE_RELEASED", lambda: lease.job_binding_sha256)
        state.close();other.close()

    def test_owner_liveness_is_checked_on_acquire_and_borrowed_access(self):
        state = self.state();lease = state.acquire("view-a", _UnitLoader(state.binding, self.budgets))
        drift = RuntimeError("UNIT_DOUBLE_ORIGINAL_OWNER_NO_LONGER_LIVE")
        def not_live():
            raise drift
        state.live_check = not_live
        with self.assertRaises(RuntimeError) as caught:
            _ = lease.view
        self.assertIs(caught.exception, drift);state.close()
        self.assert_code("M5_VIEW_LEASE_RELEASED", lambda: lease.view)
        pending = self.state();pending.live_check = not_live;loader = _UnitLoader(pending.binding, self.budgets)
        with self.assertRaises(RuntimeError) as caught:
            pending.acquire("view-a", loader)
        self.assertIs(caught.exception, drift);self.assertEqual(loader.calls, []);pending.close()

    def test_consumer_exception_releases_lease_without_swallowing_or_finalizing_cell(self):
        state = self.state();failure = RuntimeError("UNIT_CONSUMER_FAILURE")
        with self.assertRaises(RuntimeError) as caught:
            with state.acquire("view-a", _UnitLoader(state.binding, self.budgets)):
                raise failure
        self.assertIs(caught.exception, failure)
        self.assertEqual(state.active, {});self.assertEqual(len(state.cache), 1)
        self.assertFalse(state.closed);state.close();self.assertEqual(state.cache, {})

    def test_finish_requires_every_value_closed_or_explicitly_unused(self):
        state = self.state((self.request, replace(self.request, view_id="view-b")));calls = []
        callback = lambda: calls.append(UNIT_PROVENANCE) or ()
        self.assert_code("M5_JOB_CONSUMERS_INCOMPLETE", self.finish, state, revalidate=callback)
        lease = state.acquire("view-a", _UnitLoader(state.binding, self.budgets))
        self.assert_code("M5_JOB_CONSUMERS_INCOMPLETE", self.finish, state, revalidate=callback)
        lease.release()
        self.assert_code("M5_JOB_CONSUMERS_INCOMPLETE", self.finish, state, revalidate=callback)
        self.assertEqual(calls, []);state.close()

    def test_caller_result_mismatch_cannot_trigger_final_revalidation(self):
        for changes in ({"result_sha256": "not-digest"}, {"job_binding_sha256": _sha("OTHER_JOB")},
                {"consumer_binding_sha256": ()}, {"consumer_binding_sha256": (_sha("OTHER_CONSUMER"),)},
                {"result_descriptor_sha256": "not-descriptor"}, {"existing_producer_receipt_sha256": False}):
            with self.subTest(changes=changes):
                state = self.released_state();calls = []
                self.assert_code("M5_JOB_RESULT_BINDING_INVALID", self.finish, state,
                    results={TERMINAL: self.result(state, **changes)}, revalidate=lambda: calls.append("called") or ())
                self.assertEqual(calls, []);state.close()

    def test_final_revalidation_failure_closes_values_without_success_summary(self):
        for shape in ([], (object(),)):
            with self.subTest(invalid_observation_shape=type(shape).__name__):
                state = self.released_state()
                self.assert_code("M5_JOB_SOURCE_PIN_RESULT_INVALID", self.finish, state,
                    revalidate=lambda: shape)
                self.assertTrue(state.closed);self.assertEqual(state.cache, {})
        state = self.released_state();drift = RuntimeError("UNIT_FINAL_REVALIDATION_DRIFT")
        def revalidate():
            raise drift
        with self.assertRaises(RuntimeError) as caught:
            self.finish(state, revalidate=revalidate)
        self.assertIs(caught.exception, drift)
        self.assertTrue(state.closed);self.assertEqual(state.cache, {});self.assertEqual(state.requests, {})

    def test_unused_is_not_stop_pass_or_examined_coverage_and_never_loads(self):
        state = self.state((self.request, replace(self.request, view_id="view-b")))
        loader = _UnitLoader(state.binding, self.budgets);state.acquire("view-a", loader).release()
        self.assert_code("M5_VIEW_UNUSED_REASON_INVALID", state.release_unused, "view-b", "")
        state.release_unused("view-b", "UNIT_NOT_NEEDED")
        self.assertEqual(loader.calls, ["view-a"])
        self.assert_code("M5_VIEW_NOT_AVAILABLE", state.acquire, "view-b", loader)
        rows = {row.view_id: row for row in self.finish(state).views}
        self.assertEqual(rows["view-b"].status, "UNUSED")
        for field in ("payload_sha256", "membership_sha256", "row_count", "source_member_count", "coverage", "cost_status"):
            self.assertIsNone(getattr(rows["view-b"], field))
        self.assertEqual(rows["view-a"].status, "USED")

    def test_execution_and_held_clock_source_kind_refuse_before_constructor(self):
        request = replace(self.request, execution_required=True)
        state = self.state((request,));calls = []
        self.assert_code("M5_VIEW_COST_PROFILE_UNVERIFIED", state.acquire, "view-a", lambda _: calls.append("called"))
        self.assertEqual(calls, []);state.close()
        for request in (self.clock_request(), self.clock_request("D1"),
                        replace(self.request, source_kind="m5_derived_control_clock.v1")):
            state = self.state((request,));calls = []
            self.assert_code("M5_VIEW_CONTROL_CLOCK_SOURCE_HELD", state.acquire, request.view_id,
                lambda _: calls.append("must-not-load"))
            self.assertEqual(calls, []);state.close()

    def test_account_trade_contract_is_distinct_from_m5_view_contract(self):
        trade = _sha("GENUINE_TRADE_CONTRACT_UNIT_VALUE")
        self.assertNotEqual(trade, self.binding.m5_view_contract_sha256)
        account = job.AccountViewBinding("UNIT_ACCOUNT", _stamp(30), _sha("SAVED_OWNER"), trade)
        request = replace(self.request, consumer=account);state = self.state((request,))
        lease = state.acquire("view-a", _UnitLoader(state.binding, self.budgets))
        self.assertEqual(state.consumer_binding(lease), account);lease.release();self.finish(state)
        self.assert_code("M5_VIEW_CONSUMER_BINDING_INVALID", job._request, self.binding,
            replace(request, consumer=replace(account, source_contract_sha256="not-trade-digest")))
        self.assert_code("M5_VIEW_MEASUREMENT_END_MISMATCH", job._request, self.binding,
            replace(request, consumer=replace(account, measurement_end_utc=_stamp(25))))

    def test_summary_is_compact_and_caller_validation_is_not_durability_proof(self):
        state = self.released_state();result = self.result(state);calls = []
        self.assertNotEqual(result.result_sha256, result.result_descriptor_sha256)
        summary = self.finish(state, results={TERMINAL: result}, revalidate=lambda: calls.append("called") or ())
        self.assertEqual(calls, ["called"]);self.assertEqual(dict(summary.results)[TERMINAL], result)
        self.assertEqual(summary.budgets, self.budgets)
        self.assertEqual(summary.result_assurance, "CALLER_VALIDATED_EXISTING_RESULT_NOT_DURABILITY_PROOF_BY_JOB_VIEW")
        self.assertTrue(summary.owned_values_released)
        self.assertTrue(summary.release_eligible)
        self.assertEqual(summary.source_pins, ())
        self.assertFalse(summary.financial_consumer_ready);self.assertFalse(summary.full_native_context_eligibility)
        self.assertFalse(hasattr(summary.views[0], "rows"))
        self.assertEqual(summary.views[0].constructor_implementation_sha256, self.request.constructor_implementation_sha256)
        self.assertEqual(summary.views[0].gap_findings_sha256, self.request.gap_findings_sha256)
        self.assertEqual(state.cache, {});self.assertEqual(state.requests, {});self.assertTrue(state.closed)
        self.assert_code("M5_JOB_CLOSED", state.acquire, "view-a", lambda _: None)

    def test_prefix_stability_completed_values_and_membership_at_earlier_cutoff(self):
        early = replace(self.request, view_id="early", interval=_interval(0, 15), cutoff_utc=_stamp(15))
        later = replace(self.request, view_id="later", cutoff_utc=_stamp(30))
        loader = _UnitLoader(_binding(self.budgets, (early, later)), self.budgets)
        short, full = loader(early), loader(later)
        values = tuple(row for row in full.rows if row.close_utc <= early.cutoff_utc)
        members = tuple(row for row in full.series.bars if row.value.close_utc <= early.cutoff_utc)
        self.assertEqual(short.rows, values);self.assertEqual(short.series.bars, members)
        self.assertEqual(short.payload_sha256, _digest([asdict(row) for row in values]))
        self.assertNotEqual(short.payload_sha256, full.payload_sha256)

    def test_keyed_results_require_frozen_keys_and_actual_seen_consumers(self):
        slot = replace(self.request, result_key="slot-result")
        account = replace(self.request, view_id="account", consumer=job.AccountViewBinding(
            "UNIT_ACCOUNT", _stamp(30), _sha("OWNER"), _sha("TRADE")))
        requests = (slot, account)
        for wrong in (None, "missing", "unknown", "consumer"):
            with self.subTest(wrong=wrong):
                binding = _binding(self.budgets, requests, required_result_keys=("slot-result", TERMINAL))
                state = self.state(requests, binding=binding);loader = _UnitLoader(binding, self.budgets)
                for request in requests:
                    state.acquire(request.view_id, loader).release()
                results = {key: self.result(state, key) for key in binding.required_result_keys}
                if wrong=="missing":
                    results.pop("slot-result")
                elif wrong=="unknown":
                    results["not-declared"] = results[TERMINAL]
                elif wrong=="consumer":
                    results["slot-result"] = replace(results["slot-result"],
                        consumer_binding_sha256=state.consumer_bindings_for(TERMINAL))
                calls = []
                grids = {key: self.grid(key) for key in binding.required_result_keys}
                if wrong is None:
                    summary = self.finish(state, results=results, grids=grids,
                        revalidate=lambda: calls.append("called") or ())
                    self.assertEqual(dict(summary.results), results)
                    self.assertEqual(calls, ["called"])
                else:
                    code = "M5_JOB_RESULT_BINDING_INVALID" if wrong=="consumer" else "M5_JOB_RESULT_COVERAGE_INVALID"
                    self.assert_code(code, self.finish, state, results=results, grids=grids,
                        revalidate=lambda: calls.append("called") or ())
                    self.assertEqual(calls, []);state.close()

    def test_terminal_grid_requires_matching_append_readback_and_artifact_descriptor(self):
        for change in ({"appended": 1}, {"row_sha256": _sha("OTHER_ROW")},
                {"readback_row_sha256": _sha("OTHER_READBACK")}, {"path": ""}):
            with self.subTest(change=change):
                state = self.released_state();calls = []
                self.assert_code("M5_JOB_END_GRID_BINDING_INVALID", self.finish, state,
                    grids={TERMINAL: self.grid(**change)}, revalidate=lambda: calls.append("called") or ())
                self.assertEqual(calls, []);state.close()
        for grids in ({}, {TERMINAL: self.grid(), "unknown": self.grid()}):
            state = self.released_state()
            self.assert_code("M5_JOB_END_GRID_COVERAGE_INVALID", self.finish, state, grids=grids);state.close()
        state = self.released_state()
        summary = self.finish(state, grids={TERMINAL: self.grid(appended=False)})
        self.assertIs(dict(summary.end_grid_rows)[TERMINAL].appended, False)
        state = self.released_state();state.spool_root = Path("C:/SYNTHETIC_UNIT_SPOOL_NOT_OPENED")
        self.assert_code("M5_JOB_END_GRID_INSIDE_SPOOL", self.finish, state,
            grids={TERMINAL: self.grid(path="C:/SYNTHETIC_UNIT_SPOOL_NOT_OPENED/end-grid.jsonl")})
        state.close()

    def test_unreached_nonterminal_result_can_be_unused_but_terminal_cannot(self):
        slot = replace(self.request, result_key="unreached")
        account = replace(self.request, view_id="account")
        requests = (slot, account);binding = _binding(self.budgets, requests, required_result_keys=("unreached", TERMINAL))
        state = self.state(requests, binding=binding);state.release_unused(slot.view_id, "UNIT_NOT_REACHED")
        state.acquire(account.view_id, _UnitLoader(binding, self.budgets)).release()
        summary = self.finish(state, results={"unreached": job.UnusedResultBinding("UNIT_NOT_REACHED"), TERMINAL: self.result(state)})
        self.assertFalse(hasattr(dict(summary.results)["unreached"], "result_sha256"))
        state = self.released_state()
        self.assert_code("M5_JOB_UNUSED_RESULT_INVALID", self.finish, state,
            results={TERMINAL: job.UnusedResultBinding("UNIT_NOT_A_TERMINAL_ARTIFACT")});state.close()

    def test_late_consumer_binding_cannot_extend_frozen_result_keys(self):
        request = replace(self.request, consumer=None);state = self.state((request,))
        loader = _UnitLoader(state.binding, self.budgets)
        self.assert_code("M5_VIEW_CONSUMER_REQUIRED", state.acquire, "view-a", loader)
        self.assertEqual(loader.calls, []);state.close()
        state = self.state((request,));loader = _UnitLoader(state.binding, self.budgets)
        account = job.AccountViewBinding("UNIT_POSTSEARCH_ACCOUNT", _stamp(30), _sha("ACTUAL_SAVED_OWNER_UNIT_VALUE"), _sha("TRADE"))
        self.assert_code("M5_JOB_RESULT_KEY_NOT_DECLARED", state.acquire, "view-a", loader,
            consumer=account, result_key="not-frozen")
        self.assertEqual(loader.calls, [])
        state.close()
        state = self.state((request,));loader = _UnitLoader(state.binding, self.budgets)
        lease = state.acquire("view-a", loader, consumer=account, result_key=TERMINAL)
        digest = lease.consumer_binding_sha256
        self.assertEqual(state.consumer_binding(lease), account);lease.release()
        self.assertEqual(state.consumer_bindings_for(TERMINAL), (digest,));self.finish(state)

    def test_calibration_reproduction_cannot_open_a_financial_view(self):
        request = replace(self.request, purpose="COST_EDITION_REPRODUCTION_ONLY",
            consumer=job.CalibrationViewBinding(_sha("RECORDED_PROFILE_UNIT_VALUE")))
        refused = self.state((request,));calls = []
        self.assert_code("M5_VIEW_PURPOSE_MISMATCH", refused.acquire, "view-a",
            lambda _: calls.append("must-not-load"), consumer=self.request.consumer)
        self.assertEqual(calls, []);refused.close()
        refused = self.state();calls = []
        self.assert_code("M5_VIEW_PURPOSE_MISMATCH", refused.acquire, "view-a",
            lambda _: calls.append("must-not-load"), consumer=request.consumer)
        self.assertEqual(calls, []);refused.close()
        state = self.state((request,));lease = state.acquire("view-a", _UnitLoader(state.binding, self.budgets))
        self.assertIsInstance(lease.view, job.DerivedPrefixView)
        self.assert_code("M5_VIEW_CALIBRATION_NOT_FINANCIAL", state.financial_view, lease)
        lease.release();self.finish(state)


    def test_no_trade_account_consumer_roundtrips_without_owner_or_trade_aliases(self):
        consumer = job.NoTradeAccountViewBinding("UNIT_ALL_SELECTED_NO_TRADE", _stamp(30),
            _sha("CALLER_VERIFIED_PRODUCER_NO_TRADE_RUN_UNIT_METADATA"), self.binding.precommit_sha256)
        request = replace(self.request, consumer=consumer)
        binding = _binding(self.budgets, (request,))
        metadata = job._encode_m5_reopen_metadata(binding=binding, budgets=self.budgets,
            requests=(request,), product_cutoffs=(consumer.measurement_end_utc, request.cutoff_utc),
            source_identity=request.source)
        self.assertEqual(metadata["requests"][0]["consumer"]["kind"], "no_trade_account")
        decoded = job._decode_m5_reopen_metadata(metadata)[2][0].consumer
        self.assertIs(type(decoded), job.NoTradeAccountViewBinding)
        self.assertEqual(decoded, consumer)
        self.assertFalse(hasattr(decoded, "saved_owner_sha256"))
        self.assertFalse(hasattr(decoded, "source_contract_sha256"))
        with self.assertRaises(FrozenInstanceError):
            decoded.precommit_sha256 = _sha("changed")

    def test_no_trade_account_consumer_requires_its_actual_job_precommit_and_hash_metadata(self):
        consumer = job.NoTradeAccountViewBinding("UNIT_ALL_SELECTED_NO_TRADE", _stamp(30),
            _sha("CALLER_VERIFIED_PRODUCER_RUN_UNIT_METADATA"), self.binding.precommit_sha256)
        for changes in ({"precommit_sha256": _sha("OTHER_PRECOMMIT")},
                {"precommit_sha256": True}, {"no_trade_run_receipt_sha256": "not-a-run-hash"},
                {"measurement_purpose": ""}):
            with self.subTest(changes=changes):
                self.assert_code("M5_VIEW_CONSUMER_BINDING_INVALID", job._consumer,
                    self.binding, replace(consumer, **changes))

    def test_no_trade_account_known_and_late_consumers_obey_measurement_end_and_purpose(self):
        consumer = job.NoTradeAccountViewBinding("UNIT_ALL_SELECTED_NO_TRADE", _stamp(25),
            _sha("CALLER_VERIFIED_PRODUCER_RUN_UNIT_METADATA"), self.binding.precommit_sha256)
        self.assert_code("M5_VIEW_MEASUREMENT_END_MISMATCH", job._request,
            self.binding, replace(self.request, consumer=consumer))
        request = replace(self.request, consumer=None)
        state = self.state((request,));loader = _UnitLoader(state.binding, self.budgets)
        self.assert_code("M5_VIEW_MEASUREMENT_END_MISMATCH", state.acquire,
            request.view_id, loader, consumer=consumer)
        self.assertEqual(loader.calls, []);state.close()
        request = replace(request, purpose="COST_EDITION_REPRODUCTION_ONLY")
        state = self.state((request,));loader = _UnitLoader(state.binding, self.budgets)
        self.assert_code("M5_VIEW_PURPOSE_MISMATCH", state.acquire, request.view_id, loader,
            consumer=replace(consumer, measurement_end_utc=_stamp(30)))
        self.assertEqual(loader.calls, []);state.close()



if __name__ == "__main__":
    unittest.main()
