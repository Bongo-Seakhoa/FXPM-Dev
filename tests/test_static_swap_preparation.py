"""Invented swap-preparation controls; no source or financial qualification."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import pm_core as core
import pm_authoritative_search_execution as execution


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _overlay_case(tmp_path, monkeypatch, *, mode=6):
    """Mock delivered terms/projection; actual Core binding and pointer checks run."""
    label = "INVENTED_STATIC_SWAP_UNIT_ONLY_NOT_SOURCE_AUTHORITY"
    data = tmp_path / "invented-static-swap-data"
    pointer = data / "native" / "terms" / "ACTIVE.json"
    pointer.parent.mkdir(parents=True)
    index = pd.date_range("2026-01-05T22:00:00Z", periods=3, freq="h")
    frame = pd.DataFrame({"Open": [1.25, 1.5, 1.75], "High": [1.3, 1.6, 1.8],
        "Low": [1.2, 1.4, 1.7], "Close": [1.26, 1.51, 1.76],
        "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1)}, index=index)
    binding = {"symbol": "EURUSD", "timeframe": "H1", "activation_id": label,
        "activation_manifest_sha256": _digest([label, "price-activation"]),
        "bars_sha256": hashlib.sha256(frame.to_csv().encode()).hexdigest(),
        "sidecar_sha256": _digest([label, "sidecar"])}
    frame.attrs[core.NATIVE_SOURCE_BINDING_ATTR] = binding
    state = {"fixture_provenance": label, "source_provenance_authenticated": False,
        "schedule_provenance_status": "INVENTED_UNAUTHENTICATED",
        "price_activation_id": binding["activation_id"],
        "price_activation_manifest_sha256": binding["activation_manifest_sha256"],
        "target_bars_sha256": binding["bars_sha256"],
        "target_sidecar_sha256": binding["sidecar_sha256"],
        "terms_activation_id": label + "-terms", "terms_activation_manifest_sha256": _digest([label, "terms"]),
        "terms_snapshot_sha256": _digest([label, "snapshot"]),
        "schedule_sha256": _digest([label, "schedule"]),
        "projection_contract": label + "-projection", "events": [{"fixture_provenance": label}],
        "terms": {"swap_mode": mode, "swap_long": -2.123456789, "swap_short": 1.987654321}}
    resolutions, projections = [], []

    def revise_pointer(revision):
        raw = json.dumps({"fixture_provenance": label, "revision": revision}, sort_keys=True).encode()
        pointer.write_bytes(raw)
        state["terms_active_pointer_sha256"] = hashlib.sha256(raw).hexdigest()

    revise_pointer(0)
    def resolve(native_root, symbol, timeframe):
        assert Path(native_root) == data / "native"
        assert (symbol, timeframe) == ("EURUSD", "H1")
        resolutions.append(state["terms_active_pointer_sha256"])
        return copy.deepcopy(state)

    def project(events, actual_frame):
        assert events == state["events"]
        projections.append(True)
        pre = np.zeros(len(actual_frame), dtype=np.int32)
        close = np.zeros(len(actual_frame), dtype=np.int32)
        pre[1], close[-1] = 1, 3
        return SimpleNamespace(pre_open_units=pre, at_close_units=close,
            digest=_digest([label, pre.tolist(), close.tolist()]), event_count=2)

    monkeypatch.setattr(core, "resolve_active_swap_contract", resolve)
    monkeypatch.setattr(core, "project_session_events", project)
    config = core.PipelineConfig(data_dir=str(data), research_mode=True, swap_model_enabled=True)
    pricing = core.Backtester(config)
    spec = core.InstrumentSpec("EURUSD", pip_position=4, contract_size=100000.125, swap_mode=mode,
        swap_long=-9.0, swap_short=9.0)
    return SimpleNamespace(frame=frame, pricing=pricing, spec=spec, state=state,
        projections=projections, resolutions=resolutions, revise_pointer=revise_pointer)


def _expected_rate(pricing, spec, raw, prices, mode):
    # Preserve the existing scalar conversion's exact operation order.
    return np.array([pricing._swap_value_per_lot_dollars(spec, raw, entry_price=float(value), swap_mode=mode)
                     if np.isfinite(value) else 0.0 for value in prices], dtype=np.float64)


def _assert_outputs_equal(left, right):
    assert len(left) == len(right) == 6
    for actual, expected in zip(left[:4], right[:4], strict=True):
        np.testing.assert_array_equal(actual, expected)
    assert left[4:] == right[4:]


def _matched_case(monkeypatch):
    from test_authoritative_search_execution import _tiny_prepared_matched_source_case
    return _tiny_prepared_matched_source_case(monkeypatch)


def test_matched_witness_owner_prepares_static_swap_once_for_repeated_witnesses(monkeypatch):
    """Real tiny kernels/witnesses; their provenance envelope is explicitly invented."""
    case = _matched_case(monkeypatch)
    preparations = []
    original = execution.Backtester._prepare_causal_swap_inputs
    def prepare(self, *args, **kwargs):
        preparations.append(True)
        return original(self, *args, **kwargs)
    monkeypatch.setattr(execution.Backtester, "_prepare_causal_swap_inputs", prepare)
    cache = {}
    for _ in range(3):
        assert execution._verify_route_a_matched_fact_source(copy.deepcopy(case.facts), case.geometry,
            **case.arguments, _prepared_source_cache=cache)
    assert preparations == [True]
    assert len(case.swap_calls) == 3
    changed = copy.deepcopy(case.facts)
    changed["trades"][0]["executed_price_swap_capture"]["swap_dollars_exact"] += 0.5
    with pytest.raises((execution.AuthoritativeSearchExecutionError, ValueError)):
        execution._verify_route_a_matched_fact_source(changed, case.geometry,
            **case.arguments, _prepared_source_cache=cache)
    assert preparations == [True]


def test_existing_prepared_source_geometry_config_and_spec_changes_get_new_swap_state(monkeypatch):
    case = _matched_case(monkeypatch)
    preparations = []
    original = execution.Backtester._prepare_causal_swap_inputs
    def prepare(self, *args, **kwargs):
        preparations.append(True)
        return original(self, *args, **kwargs)
    monkeypatch.setattr(execution.Backtester, "_prepare_causal_swap_inputs", prepare)
    variants = [(case.geometry, case.arguments),
        ({**case.geometry, "receipt_sha256": "e" * 64}, case.arguments),
        (case.geometry, {**case.arguments, "config": copy.deepcopy(case.arguments["config"])}),
        (case.geometry, {**case.arguments, "spec": copy.deepcopy(case.arguments["spec"])})]
    cache = {}
    for ordinal, (geometry, arguments) in enumerate(variants, 1):
        assert execution._verify_route_a_matched_fact_source(copy.deepcopy(case.facts), geometry,
            **arguments, _prepared_source_cache=cache)
        assert len(preparations) == ordinal


def test_mode6_reuse_keeps_entry_price_rates_exact_readonly_axes_and_fresh_contract(tmp_path, monkeypatch):
    case = _overlay_case(tmp_path, monkeypatch)
    prepared = {}
    first_prices = np.array([1.237918273, 1.91782731, np.nan])
    second_prices = np.array([1.277312345, 1.912312367, np.nan])
    first = case.pricing._build_causal_swap_inputs(case.frame, case.spec, first_prices, "H1",
        _static_preparation=prepared)
    first[-1]["invented_caller_mutation"] = True
    second = case.pricing._build_causal_swap_inputs(case.frame, case.spec, second_prices, "H1",
        _static_preparation=prepared)
    assert len(first) == len(second) == 6 and first[4] is second[4] is True
    assert case.projections == [True]
    assert "invented_caller_mutation" not in second[-1]
    assert second[-1]["source_provenance_authenticated"] is False
    for result, prices in ((first, first_prices), (second, second_prices)):
        np.testing.assert_array_equal(result[0], [0, 1, 0])
        np.testing.assert_array_equal(result[1], [0, 0, 3])
        for values in result[:2]:
            assert values.flags.writeable is False
            with pytest.raises(ValueError):
                values[1] = 999
        for array, raw in zip(result[2:4], (-2.123456789, 1.987654321), strict=True):
            np.testing.assert_array_equal(array, _expected_rate(case.pricing, case.spec, raw, prices, 6))
    assert first[2][0] != second[2][0] and first[3][0] != second[3][0]
    assert not np.shares_memory(first[2], second[2]) and not np.shares_memory(first[3], second[3])


def test_changed_terms_reproject_and_crossed_current_source_binding_still_refuses(tmp_path, monkeypatch):
    case = _overlay_case(tmp_path, monkeypatch)
    prices, prepared = np.array([1.25, 1.5, np.nan]), {}
    before = case.pricing._build_causal_swap_inputs(case.frame, case.spec, prices, "H1",
        _static_preparation=prepared)
    case.state["terms"]["swap_long"] -= 0.75
    case.state["terms_snapshot_sha256"] = _digest(["INVENTED_CHANGED_TERMS", 1])
    case.revise_pointer(1)
    with pytest.raises(core.BrokerTermsError, match="activation changed during Backtester lifetime"):
        case.pricing._build_causal_swap_inputs(case.frame, case.spec, prices, "H1", _static_preparation=prepared)
    assert case.projections == [True] and len(case.resolutions) == 1
    pricing, new_prepared = core.Backtester(case.pricing.config), {}
    after = pricing._build_causal_swap_inputs(case.frame, case.spec, prices, "H1",
        _static_preparation=new_prepared)
    assert case.projections == [True, True] and len(case.resolutions) == 2
    assert before[2][0] != after[2][0]
    np.testing.assert_array_equal(after[2], _expected_rate(pricing, case.spec,
        case.state["terms"]["swap_long"], prices, 6))
    crossed = case.frame.copy(deep=True)
    crossed.attrs = copy.deepcopy(case.frame.attrs)
    crossed.attrs[core.NATIVE_SOURCE_BINDING_ATTR]["bars_sha256"] = "f" * 64
    with pytest.raises(core.BrokerTermsError, match="feature source binding does not match"):
        pricing._build_causal_swap_inputs(crossed, case.spec, prices, "H1", _static_preparation=new_prepared)
    assert case.projections == [True, True]


def test_disabled_unavailable_and_research_outputs_match_standalone_behavior():
    index = pd.date_range("2026-01-05T22:00:00Z", periods=3, freq="h")
    base = pd.DataFrame({"Open": 1.25, "High": 1.3, "Low": 1.2, "Close": 1.26,
        "_BAR_CLOSE_TS": index + pd.Timedelta(hours=1)}, index=index)
    prices = np.array([1.25, 1.5, np.nan])
    spec = core.InstrumentSpec("EURUSD", pip_position=4, swap_mode=6, contract_size=100000.0)
    for kind in ("disabled", "unavailable", "research"):
        frame = base.drop(columns=["_BAR_CLOSE_TS"]) if kind == "unavailable" else base.copy(deep=True)
        pricing = core.Backtester(core.PipelineConfig(research_mode=True, swap_model_enabled=kind != "disabled"))
        if kind == "research":
            projection = SimpleNamespace(pre_open_units=np.array([0, 1, 0], dtype=np.int32),
                at_close_units=np.array([0, 0, 3], dtype=np.int32), digest=_digest([kind]), event_count=2)
            terms = {"fixture_provenance": "INVENTED_RESEARCH_CONTEXT_NOT_AUTHORITY", "research_context": True,
                "terms": {"swap_mode": 6, "swap_long": -2.0, "swap_short": 1.0}}
            projects, resolved_copies = [], []
            def project(*args, **kwargs):
                projects.append(True)
                return projection
            def resolved_terms():
                resolved_copies.append(True)
                return copy.deepcopy(terms)
            pricing._research_swap_context = SimpleNamespace(project=project, resolved_terms=resolved_terms,
                schedule_sha256=_digest([kind, "schedule"]),
                terms_snapshot_sha256=_digest([kind, "terms"]),
                stress_policy_sha256=_digest([kind, "stress"]), cutoff_utc="2026-01-06T02:00:00Z")
        standalone = pricing._build_causal_swap_inputs(frame, spec, prices, "H1")
        prepared = {}
        for _ in range(2):
            cached = pricing._build_causal_swap_inputs(frame, spec, prices, "H1", _static_preparation=prepared)
            _assert_outputs_equal(cached, standalone)
        assert cached[-1]["status"] == ("DISABLED" if kind == "disabled" else "UNAVAILABLE")
        if kind == "research":
            assert cached[-1]["mode"] == "research_d1_session_projection"
            assert cached[-1]["source_provenance_authenticated"] is False
            # One standalone preparation and one cached miss; the hit copies no terms/events.
            assert projects == [True, True] and resolved_copies == [True, True]


def test_standalone_default_reprojects_each_call_and_revalidates_current_binding(tmp_path, monkeypatch):
    case = _overlay_case(tmp_path, monkeypatch)
    prices = np.array([1.25, 1.5, np.nan])
    first = case.pricing._build_causal_swap_inputs(case.frame, case.spec, prices, "H1")
    second = case.pricing._build_causal_swap_inputs(case.frame, case.spec, prices, "H1")
    _assert_outputs_equal(first, second)
    assert case.projections == [True, True]
    crossed = case.frame.copy(deep=True)
    crossed.attrs = copy.deepcopy(case.frame.attrs)
    crossed.attrs[core.NATIVE_SOURCE_BINDING_ATTR]["activation_id"] = "INVENTED_CROSSED_ACTIVATION"
    with pytest.raises(core.BrokerTermsError, match="feature source binding does not match"):
        case.pricing._build_causal_swap_inputs(crossed, case.spec, prices, "H1")
    assert case.projections == [True, True]
