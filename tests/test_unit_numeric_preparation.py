"""Invented software controls; no source, model, OWNER or financial qualification."""
from __future__ import annotations

import copy
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

import pm_authoritative_search_execution as execution
import pm_core
import pm_historical_anchor_orchestration as historical
import pm_historical_production_adapter as adapter
import pm_historical_scheduler as scheduler
import pm_historical_search_producer as producer


def _frame():
    from test_authoritative_search_execution import _d1_frame
    return _d1_frame(124)


def _digest(body):
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _identity_inputs():
    """Structural key inputs only, never an authenticated Native product."""
    frame = _frame()
    native = dict(symbol="EURUSD", activation_id="INVENTED-UNIT-NUMERIC",
        activation_manifest_sha256="a" * 64, activation_entry_sha256="b" * 64,
        artifact_sha256="c" * 64, prefix_content_sha256="d" * 64,
        prefix_rows=len(frame), prefix_start_utc=frame.index[0].isoformat(),
        prefix_end_exclusive_utc=(frame.index[-1] + pd.Timedelta(days=1)).isoformat())
    product = SimpleNamespace(native_prefix=native,
        index={"cutoff_utc": native["prefix_end_exclusive_utc"]}, index_sha256="e" * 64)
    tiers = {name: SimpleNamespace(descriptor=lambda: {"profile_sha256": "f" * 64})
             for name in ("P75", "P95")}
    return dict(product=product, timeframe="D1", config_sha256="1" * 64, tiers=tiers)


def _combined(frame):
    surface = execution.pm_regime.MarketRegimeDetector(
        execution.pm_regime.DEFAULT_PARAMS_BY_TIMEFRAME["D1"]).compute_regime_scores(frame)
    result = frame.copy(deep=True)
    for name in surface.columns:
        result[name] = surface[name]
    result.attrs = copy.deepcopy(frame.attrs)
    return result


def test_four_regime_fit_calls_share_real_numeric_stream_and_keep_fresh_metadata(monkeypatch):
    """Exercise the actual fit seam; source/probe/Book carriers are labelled stubs."""
    original_compute = execution.pm_regime.MarketRegimeDetector.compute_regime_scores
    frame = _frame()
    oracle = original_compute(execution.pm_regime.MarketRegimeDetector(
        execution.pm_regime.DEFAULT_PARAMS_BY_TIMEFRAME["D1"]), frame)
    computes, probes, source_calls, fit_calls = [], [], [], []

    def compute(detector, actual_frame):
        computes.append(True)
        return original_compute(detector, actual_frame)

    class InventedProbe:
        def __init__(self, config):
            self.config = config

        def build_evidence_source_seed_result(self, actual_frame, symbol, **kwargs):
            probes.append((actual_frame.attrs["invented_cell"], kwargs["spread_tier"]))
            return {"classification": "INVENTED_PROBE_NOT_SOURCE_EVIDENCE"}

    def source(**body):
        source_calls.append(body)
        return SimpleNamespace(receipt_sha256=_digest(body))

    def fit(**body):
        fit_calls.append(body)
        bound = {**body, "source_group_decision": body["source_group_decision"].receipt_sha256}
        return SimpleNamespace(receipt_sha256=_digest(bound))

    @dataclass
    class InventedInstrument:
        point: float = 0.00001

    monkeypatch.setattr(execution.pm_regime.MarketRegimeDetector, "compute_regime_scores", compute)
    monkeypatch.setattr(execution, "Backtester", InventedProbe)
    monkeypatch.setattr(execution.raw_winner, "build_trade_source_contract_from_backtester_result", lambda _: {})
    monkeypatch.setattr(execution.pm_core, "build_backtest_frozen_tier_pair_contract", lambda *a, **k: {})
    monkeypatch.setattr(execution.raw_winner, "_current_pair_inputs", lambda seeds, pair:
        (seeds, pair, {"source_contract_sha256": "2" * 64}))
    monkeypatch.setattr(execution.book, "create_historical_source_group_decision", source)
    monkeypatch.setattr(execution.book, "create_regime_detector_record", lambda **body:
        {"record_sha256": _digest({k: v for k, v in body.items() if k != "source_group_decision"})})
    monkeypatch.setattr(execution.book, "create_regime_fit_decision", fit)
    config = execution.PipelineConfig(research_mode=True)
    fitted_results = []
    with producer._UnitNumericPreparation() as owner:
        for ordinal, regime in enumerate(execution.book.REGIME_ORDER):
            arguments = _identity_inputs()
            product = arguments["product"]
            product.index_sha256 = str(ordinal + 3) * 64
            current = frame.copy(deep=True)
            current.attrs = {"invented_cell": regime, "fresh": {"ordinal": ordinal}}
            fitted = execution._fit_verified_search_frame(product=product, config=config,
                frame=current, native=product.native_prefix, symbol="EURUSD", timeframe="D1",
                instrument_spec=InventedInstrument(), execution_spec_receipt={"invented_cell": regime},
                tiers=arguments["tiers"], observations=pd.Series(1.0, index=current.index),
                fine_product=None, fine_frame=None, acquisition_attempts_exact=1,
                source_reason_code="INVENTED_STRUCTURAL_SOURCE_SEAM_ONLY",
                fit_cutoff_utc=product.index["cutoff_utc"],
                _numeric_preparation=owner, _numeric_config_sha256=arguments["config_sha256"])
            retained = list(execution._REGIME_COLUMNS)
            pd.testing.assert_frame_equal(fitted._frame.loc[:, retained], oracle.loc[:, retained])
            pd.testing.assert_frame_equal(fitted._frame.loc[:, current.columns], current)
            assert fitted._frame.attrs == current.attrs
            assert fitted.product is product
            assert fitted.execution_spec_receipt == {"invented_cell": regime}
            fitted_results.append(fitted)
        assert len(computes) == 1
        assert len(probes) == 8 and len(source_calls) == len(fit_calls) == 4
        assert len({value.source_group_decision.receipt_sha256 for value in fitted_results}) == 4
        assert len({value.regime_fit_decision.receipt_sha256 for value in fitted_results}) == 4
        assert [value["source_group_evidence_sha256"] for value in source_calls] == [str(i) * 64 for i in range(3, 7)]
    assert owner._frames == {}


def test_each_changed_identity_or_regime_recipe_recomputes_without_admission_refusal():
    base = _identity_inputs()
    variants = []
    for field in ("prefix_content_sha256", "activation_manifest_sha256"):
        value = copy.deepcopy(base)
        value["product"].native_prefix[field] = "7" * 64
        variants.append(value)
    value = copy.deepcopy(base)
    value["product"].index["cutoff_utc"] = "2023-01-01T00:00:00Z"
    variants.append(value)
    variants.append({**base, "config_sha256": "8" * 64})
    variants.append({**base, "timeframe": "H1"})
    view = dict(view_timeframe="H1", view_payload_sha256="9" * 64, view_membership_sha256="a" * 64)
    variants.append({**base, "derived_binding": SimpleNamespace(view_evidence=view)})
    variants.append({**base, "derived_binding": SimpleNamespace(view_evidence={**view, "view_payload_sha256": "b" * 64})})
    variants.append({**base, "derived_binding": SimpleNamespace(view_evidence={**view, "view_membership_sha256": "c" * 64})})
    frame, calls = _frame(), []
    with producer._UnitNumericPreparation() as owner:
        baseline = execution._unit_numeric_identity(**base)
        keys = [(baseline, "params-A"), *[(execution._unit_numeric_identity(**value), "params-A") for value in variants], (baseline, "params-B")]
        assert all(key[0] is not None for key in keys)
        assert len(set(keys)) == len(keys)
        for key in keys:
            def compute():
                calls.append(key)
                return _combined(frame)
            expected = owner.prepare(key, frame, compute, source_columns=tuple(frame.columns))
            actual = owner.prepare(key, frame, compute, source_columns=tuple(frame.columns))
            pd.testing.assert_frame_equal(actual, expected)
        assert calls == keys


def test_missing_accepted_identity_facts_compute_without_reuse_or_new_refusal():
    base = _identity_inputs()
    variants = [{**base, "config_sha256": None}]
    for field in ("prefix_content_sha256", "activation_entry_sha256"):
        value = copy.deepcopy(base)
        del value["product"].native_prefix[field]
        variants.append(value)
    value = copy.deepcopy(base)
    del value["product"].index["cutoff_utc"]
    variants.append(value)
    variants.append({**base, "tiers": {"P75": SimpleNamespace(descriptor=lambda: {})}})
    variants.append({**base, "derived_binding": SimpleNamespace(view_evidence={"view_timeframe": "H1"})})
    frame, calls = _frame(), []
    with producer._UnitNumericPreparation() as owner:
        for arguments in variants:
            key = execution._unit_numeric_identity(**arguments)
            assert key is None
            for _ in range(2):
                owner.prepare(key, frame, lambda: calls.append(True) or frame.copy())
        assert len(calls) == 2 * len(variants)
        assert owner._frames == {}


def test_generated_values_and_precomputed_tags_do_not_alias_cell_inputs_or_cache():
    frame = _frame()
    frame.attrs = {"current_cell": {"name": "first"}}
    attr = pm_core.FeatureComputer._PRECOMPUTED_ATTR
    calls = []
    def compute():
        calls.append(True)
        result = frame.copy(deep=True)
        result["INVENTED_NUMERIC"] = range(len(frame))
        result.attrs[attr] = {"recipe": [1, 2]}
        return result
    with producer._UnitNumericPreparation() as owner:
        first = owner.prepare("invented-key", frame, compute, source_columns=tuple(frame.columns))
        first.loc[first.index[0], "INVENTED_NUMERIC"] = -1
        first.attrs[attr]["recipe"].append(3)
        second_frame = frame.copy(deep=True)
        second_frame.attrs = {"current_cell": {"name": "second"}}
        second = owner.prepare("invented-key", second_frame, compute, source_columns=tuple(frame.columns))
        assert second.iloc[0]["INVENTED_NUMERIC"] == 0
        assert second.attrs == {"current_cell": {"name": "second"}, attr: {"recipe": [1, 2]}}
        second.attrs["current_cell"]["name"] = "changed"
        second.iloc[0, second.columns.get_loc("INVENTED_NUMERIC")] = -2
        third = owner.prepare("invented-key", second_frame, compute, source_columns=tuple(frame.columns))
        assert third.iloc[0]["INVENTED_NUMERIC"] == 0
        assert third.attrs["current_cell"]["name"] == "second"
        assert second_frame.attrs == {"current_cell": {"name": "second"}}
        assert calls == [True]


def test_actual_feature_helper_reuses_only_same_recipe_and_preserves_legacy_fallback(monkeypatch):
    frame, calls = _frame(), []
    original = execution.FeatureComputer.compute_required
    def compute(actual_frame, request, **kwargs):
        calls.append(tuple(sorted(request)))
        return original(actual_frame, request, **kwargs)
    monkeypatch.setattr(execution.FeatureComputer, "compute_required", compute)
    with producer._UnitNumericPreparation() as owner:
        fitted = SimpleNamespace(_numeric_preparation=owner, _numeric_key=("invented-source",),
            _numeric_source_columns=tuple(frame.columns))
        for request in ({"ATR_14"}, {"ATR_14"}, {"SMA_5"}):
            strategy = SimpleNamespace(get_feature_request=lambda request=request: request)
            result = execution._compute_fitted_strategy_features(frame=frame, strategy=strategy,
                fitted=fitted, symbol="EURUSD", timeframe="D1")
            assert request <= set(result.columns)
        assert calls == [("ATR_14",), ("SMA_5",)]
        strategy = SimpleNamespace(get_feature_request=lambda: {"ATR_14"})
        legacy = execution._compute_fitted_strategy_features(frame=frame, strategy=strategy,
            fitted=SimpleNamespace(), symbol="EURUSD", timeframe="D1")
        assert "ATR_14" in legacy and calls[-1] == ("ATR_14",)
        assert len(calls) == 3
        full_calls = []
        def full(actual_frame, **kwargs):
            full_calls.append(True)
            execution.FeatureComputer._validate_regime_params_override(
                kwargs["regime_params_override"], kwargs["regime_params_sha256"])
            result = actual_frame.copy(deep=True)
            result["INVENTED_FULL_FEATURE"] = 1.0
            return result
        monkeypatch.setattr(execution.FeatureComputer, "compute_all", full)
        parameters = execution._default_regime_parameters("D1")
        parameter_sha = execution.pm_regime.regime_params_sha256(parameters)
        fitted.regime_parameters = dict(parameters)
        fitted.regime_fit_decision = SimpleNamespace(to_dict=lambda: {"regime_params_sha256": parameter_sha})
        execution._compute_fitted_strategy_features(frame=frame, strategy=None,
            fitted=fitted, symbol="EURUSD", timeframe="D1")
        field = next(name for name, value in parameters.items() if type(value) is float)
        fitted.regime_parameters[field] += 0.125
        with pytest.raises(ValueError, match="regime_params_sha256"):
            execution._compute_fitted_strategy_features(frame=frame, strategy=None,
                fitted=fitted, symbol="EURUSD", timeframe="D1")
        assert full_calls == [True, True]


@pytest.mark.parametrize("cancel", [False, True], ids=["normal", "baseexception"])
def test_unit_exit_discards_numeric_values_and_closed_owner_cannot_reuse(cancel):
    class Cancelled(BaseException):
        pass
    frame, calls = _frame(), []
    owner = producer._UnitNumericPreparation()
    try:
        with owner:
            owner.prepare("key", frame, lambda: frame.copy())
            assert owner._frames
            if cancel:
                raise Cancelled()
    except Cancelled:
        assert cancel
    assert owner._frames == {}
    for _ in range(2):
        owner.prepare("key", frame, lambda: calls.append(True) or frame.copy())
    assert calls == [True, True] and owner._frames == {}


def _issued_env(tmp_path, monkeypatch):
    from prototype_support import _declared_environment
    from tests.test_declared_slot_owner_surface import _request
    tmp_path.mkdir(parents=True, exist_ok=True)
    env = _declared_environment(tmp_path, monkeypatch)
    return env, _request(env)


class _ForbiddenPreparation:
    def __init__(self):
        self.calls = []
    def prepare(self, *args, **kwargs):
        self.calls.append(True)
        raise AssertionError("invalid current authority reached numeric lookup")


def test_expired_current_source_window_refuses_before_numeric_lookup(tmp_path, monkeypatch):
    from prototype_support import _DeclaredClock
    env, request = _issued_env(tmp_path, monkeypatch)
    owner = _ForbiddenPreparation()
    monkeypatch.setattr(_DeclaredClock, "value", datetime(2026, 10, 22, 13, tzinfo=timezone.utc))
    def forbidden(*args, **kwargs):
        raise AssertionError("expired authority reached Native or financial work")
    monkeypatch.setattr(producer.native_prefix, "produce_activated_native_prefix", forbidden)
    monkeypatch.setattr(execution, "fit_native_timeframe_for_search", forbidden)
    try:
        completion = producer.execute_declared_slot_owner_search(env.context, request=request,
            process=env.process, frozen_pipeline_config=env.frozen, _numeric_preparation=owner)
        source = adapter._reopen_ascii_json_entry(env.context, completion["source_session_entry"],
            expected_type="declared_slot_source_session", code="INVENTED_TEST_SOURCE_INVALID")
        assert completion["status"] == "STOP"
        assert source["failure_code"] == "DECLARED_SLOT_SOURCE_WINDOW_INVALID"
        assert source["source_accessed_at_utc"] is None and owner.calls == []
    finally:
        historical._release_declared_slot_producer_request(request, env.context)


@pytest.mark.parametrize("crossed", ["copied-request", "other-context"])
def test_crossed_cell_capability_refuses_before_numeric_lookup(tmp_path, monkeypatch, crossed):
    env, request = _issued_env(tmp_path / "first", monkeypatch)
    owner = _ForbiddenPreparation()
    supplied_request, supplied_context = request, env.context
    if crossed == "copied-request":
        supplied_request = replace(request)
    else:
        other, other_request = _issued_env(tmp_path / "second", monkeypatch)
        supplied_context = other.context
        historical._release_declared_slot_producer_request(other_request, other.context)
    try:
        with pytest.raises(historical.HistoricalAnchorOrchestrationError) as caught:
            producer.execute_declared_slot_owner_search(supplied_context, request=supplied_request,
                process=env.process, frozen_pipeline_config=env.frozen, _numeric_preparation=owner)
        assert str(caught.value) == "DECLARED_SLOT_REQUEST_CAPABILITY_INVALID"
        assert owner.calls == []
    finally:
        historical._release_declared_slot_producer_request(request, env.context)


def test_native_request_rejects_m5_owner_arguments_before_numeric_lookup(tmp_path, monkeypatch):
    """Real native early refusal; this does not assert a positive M5 lease proof."""
    env, request = _issued_env(tmp_path, monkeypatch)
    owner = _ForbiddenPreparation()
    try:
        with pytest.raises(producer.HistoricalSearchProducerError) as caught:
            producer.execute_declared_slot_owner_search(env.context, request=request, process=env.process,
                frozen_pipeline_config=env.frozen, m5_owner=object(), _numeric_preparation=owner)
        assert caught.value.code == "DECLARED_NATIVE_REQUEST_REJECTS_M5_ARGUMENTS"
        assert owner.calls == []
    finally:
        historical._release_declared_slot_producer_request(request, env.context)


def _unit_contract():
    return {"schema_version": scheduler.DECLARED_SCHEDULER_SCHEMA, "_one_period_unit": True,
        "cells": [{"cell": {"symbol": "EURUSD", "timeframe": "D1", "regime": regime}}
                  for regime in execution.book.REGIME_ORDER]}


def test_scheduler_shares_one_numeric_owner_across_four_cells_then_closes(monkeypatch):
    owners, calls, frame = [], [], _frame()
    def run(contract, row, *, _numeric_preparation):
        owners.append(_numeric_preparation)
        _numeric_preparation.prepare("same", frame, lambda: calls.append(True) or frame.copy())
        return {"status": "COMPLETED", "independent_one_period": True}
    monkeypatch.setattr(scheduler, "_declared_run_one_cell", run)
    result = scheduler.declared_symbol_worker(_unit_contract(), "EURUSD")
    assert len(result["cells"]) == 4 and result["closes_admission"] is False
    assert len({id(owner) for owner in owners}) == 1 and calls == [True]
    assert owners[0]._frames == {}
    owners[0].prepare("same", frame, lambda: calls.append(True) or frame.copy())
    assert calls == [True, True]


def test_scheduler_cancel_clears_numeric_owner_without_swallowing_baseexception(monkeypatch):
    class Cancelled(BaseException):
        pass
    owners, frame = [], _frame()
    def run(contract, row, *, _numeric_preparation):
        owners.append(_numeric_preparation)
        _numeric_preparation.prepare("same", frame, lambda: frame.copy())
        raise Cancelled()
    monkeypatch.setattr(scheduler, "_declared_run_one_cell", run)
    with pytest.raises(Cancelled):
        scheduler.declared_symbol_worker(_unit_contract(), "EURUSD")
    assert len(owners) == 1 and owners[0]._frames == {}


def test_slot_attempt_progress_brackets_consumer_commit_and_releases_request(tmp_path, monkeypatch):
    """Actual issued generator lifecycle; completion is explicitly non-OWNER."""
    env, seed_request = _issued_env(tmp_path, monkeypatch)
    historical._release_declared_slot_producer_request(seed_request, env.context)
    events, requests = [], []
    completion = {"classification": "INVENTED_COMMIT_TIMING_ONLY"}
    def execute(context, *, request, **kwargs):
        requests.append(request)
        events.append("source-result")
        return completion
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", execute)
    monkeypatch.setattr(scheduler, "_declared_request_progress", lambda *a, **k:
        lambda stage, *args: events.append(stage))
    cell = env.arguments["cell_roster"][0]
    with producer._declared_slot_attempt(env.context, precommit=env.precommit,
        process=env.process, frozen_pipeline_config=env.frozen,
        cell_id=cell["cell_id"], slot=env.roster["slots"][0],
        symbol=cell["symbol"], attempt_id="INVENTED-COMMIT-TIMING-ONLY", resumed=True
    ) as (yielded, roster, failure):
        assert yielded is completion and roster == [] and failure is None
        assert events == ["source-result", "commit_start"]
        historical._verify_declared_request_identity(requests[0], env.context, require_live=True)
        events.append("consumer-commit")
    assert events == ["source-result", "commit_start", "consumer-commit", "commit_end"]
    with pytest.raises(historical.HistoricalAnchorOrchestrationError, match="DECLARED_SLOT_REQUEST_CAPABILITY_INVALID"):
        historical._verify_declared_request_identity(requests[0], env.context, require_live=True)
