"""Fresh v17 owner/custody/Raw wiring on invented source; no financial proof.

SOURCE DRAFT. Runtime execution requires the reviewed coherent 246/248/251 merge.
The default case instruments only the strategy feature/signal surface to emit
zero signals. It retains original source, regime fit, frozen domains/geometry,
authentication, actual Backtester, stage issuer, publication and cold readers.
"""
from __future__ import annotations

import copy
import json
from collections import Counter

import pandas as pd
import pytest

import pm_authoritative_search_execution as execution
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_historical_search_producer as producer
import pm_production_search_planner as planner
import pm_authority_spool as spool
import pm_winner_raw_evidence as raw
import pm_winner_selection as winner

from tests.test_authoritative_search_execution import (
    _build_owned_invented_declared_inputs,
    _invented_fixture_external_io_guard,
    _require_invented_fixture_real_clocks,
    _route_a_batch_progress,
    _route_a_owned_invented_native_source,
    _route_a_owned_utc_window,
)


def _run_fresh_full_panel_owner_fixture(tmp_path, monkeypatch, native_fixture,
        record_property, *, instrument_models):
    """Use real issuers/readers; instrumentation grants no model/economic proof."""
    _require_invented_fixture_real_clocks()
    assert callable(planner.freeze_route_a_full_panel_production_search_process)
    assert callable(execution._verify_route_a_prepared_stage_origin)
    assert callable(execution._verify_route_a_matched_fact_source)
    assert callable(raw.verify_matched_floor_publication_chain)
    provenance = ("FRESH_REAL_ISSUER_CUSTODY_RAW_WITH_INVENTED_ZERO_SIGNAL_MODEL"
        if instrument_models else "MANUAL_FRESH_REAL_MODELS_ON_INVENTED_SOURCE_NO_FINANCIAL_AUTHORITY")
    record_property("fixture_provenance", provenance)
    record_property("instrumented_model_surface", str(instrument_models).lower())
    record_property("financial_or_performance_evidence", "false")
    record_property("synthetic_source_input_provenance", native_fixture.fixture_provenance)
    record_property("synthetic_native_input_root", str(native_fixture.data_dir / "native"))
    record_property("source_inputs_reused", str(getattr(native_fixture, "source_inputs_reused", False)).lower())
    progress_path = tmp_path / "full_panel_owner_progress.jsonl"
    record_property("full_panel_owner_progress_path", str(progress_path))
    progress = _route_a_batch_progress(progress_path)
    calls = Counter()
    state = {"owner_closed": False, "phase": "INPUTS", "snapshot": None,
        "units": None, "published": [], "cache_replay": None, "origin_negatives": False,
        "array_source_negative": False, "held_source_negative": False,
        "genuine_held_source_negative": False, "cold_source_calls": 0}
    failures, retained_adapters, retained_stages = [], [], []

    def checked(function):
        def wrapped(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except AssertionError as error:
                failures.append(error)
                raise
        return wrapped

    original_run = execution.Backtester.run
    original_pair = execution._route_a_pair_check
    original_prepare = execution._FullPanelOwnerPreparation.prepare_owned
    original_factory = execution._prepare_route_a_full_panel_owner_evidence
    original_loop = execution._continue_declared_attempt_units
    original_publish = execution._publish_declared_completed_unit
    original_origin = execution._verify_route_a_prepared_stage_origin
    original_source = execution._verify_route_a_matched_fact_source
    original_cold = raw.verify_matched_floor_publication_chain
    original_header = execution._make_declared_unit_header
    original_source_reopen = execution._reopen_route_a_matched_source_material
    original_fit = execution.fit_native_timeframe_for_search

    @checked
    def model_run(*args, **kwargs):
        calls["backtester"] += 1
        return original_run(*args, **kwargs)

    @checked
    def fit(*args, **kwargs):
        calls["source_fit"] += 1
        return original_fit(*args, **kwargs)

    @checked
    def source_reopen(*args, **kwargs):
        calls["cold_source_reopen"] += 1
        return original_source_reopen(*args, **kwargs)

    @checked
    def pair_check(*args, **kwargs):
        calls["pair_check"] += 1
        return original_pair(*args, **kwargs)

    @checked
    def instrumented_surface(*, frame, strategy, fitted, symbol, timeframe):
        # This is deliberately invented model output, never an authority verdict.
        assert frame.equals(fitted._frame)
        assert "REGIME_LIVE" in frame
        calls["instrumented_surface"] += 1
        return execution._FullPrefixStrategySurface(frame.copy(deep=True),
            pd.Series(0, index=frame.index, dtype="int32"))

    @checked
    def forbidden_ordinary_builder(*args, **kwargs):
        calls["ordinary_builder"] += 1
        raise AssertionError("Fresh full panel must not replay the ordinary full-attempt builder")

    @checked
    def header(*args, **kwargs):
        calls["header"] += 1
        assert calls["header"] == 1 and not state["owner_closed"]
        return original_header(*args, **kwargs)

    @checked
    def restore(*args, **kwargs):
        calls["restore"] += 1
        raise AssertionError("This positive case must issue a fresh header, not restore an old owner")

    @checked
    def prepare(preparation, **kwargs):
        calls["prepare"] += 1
        assert calls["prepare"] == 1 and state["units"] is None
        state["snapshot"] = copy.deepcopy(kwargs["owner_snapshot"])
        snapshot = state["snapshot"]
        assert snapshot["original_trial_count"] == len(snapshot["attempt_roster"])
        assert len(snapshot["attempt_roster"]) > 1
        assert any(row["status"] == "PLANNED" for row in snapshot["attempt_roster"])
        assert any(row["status"] == "PROCESS_REJECTED" for row in snapshot["attempt_roster"])
        original = next(row for row in snapshot["attempt_roster"] if row["status"] == "PLANNED")
        public = {"strategy_id": original["strategy_id"], "parameters": copy.deepcopy(original["parameters"]),
            "fold_ordinal": 0, "partition_role": "final_lockbox", "spread_tier": "P75", "bookkeeping": {}}
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
            execution._route_a_batch_specifications([public])
        assert caught.value.code == "ROUTE_A_UNIT_BATCH_LOCKBOX_REFUSED"
        actual_prepare = kwargs["prepare_attempt"]
        invoked = []

        @checked
        def observed_attempt(ordinal):
            # The real prepare_owned has already checked custody and reentry.
            state["phase"] = "OWNER_BATCH"
            invoked.append(ordinal)
            before = dict(calls)
            first = actual_prepare(ordinal)
            assert len(first[0]["folds"]) == 4 and len(first[1]) == 12
            assert {row["partition_role"] for row in first[1]} == {
                "optimizer_train", "tuning_validation", "final_lockbox"}
            if state["cache_replay"] is None:
                retained_adapters.append(actual_prepare)
                boundary = dict(calls)
                second = actual_prepare(ordinal)
                assert calls["backtester"] == boundary.get("backtester", 0)
                assert calls["pair_check"] == boundary.get("pair_check", 0)
                assert execution._canonical_bytes(first) == execution._canonical_bytes(second)
                state["cache_replay"] = {"ordinal": ordinal,
                    "first_backtester_calls": boundary.get("backtester", 0) - before.get("backtester", 0),
                    "first_pair_calls": boundary.get("pair_check", 0) - before.get("pair_check", 0),
                    "repeat_backtester_calls": 0, "repeat_pair_calls": 0}
                assert state["cache_replay"]["first_backtester_calls"] == 24
                assert state["cache_replay"]["first_pair_calls"] == 12
                progress("OBSERVED_CACHE_REPLAY", "same-original-private-24-requests-no-new-kernels-or-pairs")
            return first

        supplied = dict(kwargs, prepare_attempt=observed_attempt)
        stage = original_prepare(preparation, **supplied)
        planned = [row["ordinal"] for row in snapshot["attempt_roster"] if row["status"] == "PLANNED"]
        assert invoked == planned
        assert len(stage.rows) == snapshot["original_trial_count"]
        retained_stages.append((stage, preparation.capture, preparation.handle.frozen))
        return stage

    @checked
    def factory(**kwargs):
        calls["factory"] += 1
        assert calls["factory"] == 1 and calls["header"] == 1
        assert state["units"] is None and not state["published"]
        body = kwargs["capture"].request.to_dict()
        assert execution._read_declared_unit_json(kwargs["capture"].context,
            execution._declared_unit_relative(body, "evidence-completed.json")) is None
        for ordinal in range(len(kwargs["attempts"])):
            assert execution._read_declared_unit_json(kwargs["capture"].context,
                execution._declared_unit_relative(body, f"{ordinal:06d}.json")) is None
        stage = original_factory(**kwargs)
        assert retained_adapters and stage is retained_stages[0][0]
        boundary = dict(calls)
        with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
            retained_adapters[0](state["cache_replay"]["ordinal"])
        assert caught.value.code == "ROUTE_A_UNIT_BATCH_CLOSED"
        assert calls["backtester"] == boundary.get("backtester", 0)
        assert calls["pair_check"] == boundary.get("pair_check", 0)
        state["owner_closed"] = True
        state["post_owner_counts"] = dict(calls)
        progress("OBSERVED_OWNER_CLOSED", "before-first-unit-publication")
        return stage

    @checked
    def loop(context, body, binding_sha256, units, produce):
        calls["loop"] += 1
        assert calls["loop"] == 1 and state["owner_closed"]
        assert state["units"] is None
        state["units"] = copy.deepcopy(list(units))
        assert [row["ordinal"] for row in units] == list(range(state["snapshot"]["original_trial_count"]))
        return original_loop(context, body, binding_sha256, units, produce)

    @checked
    def publish(context, body, binding_sha256, unit, value, **kwargs):
        calls["unit_publish"] += 1
        assert state["owner_closed"]
        assert calls["backtester"] == state["post_owner_counts"].get("backtester", 0)
        assert calls["pair_check"] == state["post_owner_counts"].get("pair_check", 0)
        expected = {"attempt_id", "outcome"} if unit["status"] == "PROCESS_REJECTED" else {"attempt_id", "folds"}
        assert set(value) == expected
        if unit["status"] == "PLANNED":
            assert len(value["folds"]) == 4
            assert all(len(fold["lanes"]) == 6 for fold in value["folds"])
        row = original_publish(context, body, binding_sha256, unit, value, **kwargs)
        state["published"].append(copy.deepcopy(unit))
        return row

    @checked
    def origin(stage, *, capture, frozen, unit, raw_body):
        calls["stage_origin"] += 1
        assert state["owner_closed"] and stage is retained_stages[0][0]
        actual = original_origin(stage, capture=capture, frozen=frozen, unit=unit, raw_body=raw_body)
        assert len(actual[0]) == 12
        assert actual[1]["matched_base_facts_sha256"] == execution._sha256(actual[0])
        if not state["origin_negatives"]:
            identities = json.loads(stage.unit_identities_bytes.decode("ascii"))
            other = next(row for row in identities if row["ordinal"] != unit["ordinal"])
            with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
                original_origin(stage, capture=capture, frozen=frozen, unit=other, raw_body=raw_body)
            assert caught.value.code == "ROUTE_A_FULL_PANEL_STAGE_RAW_BODY_CHANGED"
            spliced = copy.deepcopy(raw_body)
            spliced["attempt_id"] = "INVENTED-CROSS-UNIT-RAW-ID"
            with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
                original_origin(stage, capture=capture, frozen=frozen, unit=unit, raw_body=spliced)
            assert caught.value.code == "ROUTE_A_FULL_PANEL_STAGE_RAW_BODY_CHANGED"
            state["origin_negatives"] = True
        return actual

    @checked
    def source(facts, geometry, *, source_frame, spec, tiers, config, timeframe, _prepared_source_cache=None):
        state["cold_source_calls"] += 1
        result = original_source(facts, geometry, source_frame=source_frame, spec=spec,
            tiers=tiers, config=config, timeframe=timeframe, _prepared_source_cache=_prepared_source_cache)
        assert result is True
        if not state["array_source_negative"]:
            changed = copy.deepcopy(facts)
            vectors = changed["half_spread_arrays"]["P95"]
            values = execution._route_a_decode_spread_array(vectors["entry_half_spread"], "entry_half_spread").copy()
            finite = execution.np.flatnonzero(execution.np.isfinite(values))
            assert len(finite)
            values[int(finite[0])] += 1.0
            vectors["entry_half_spread"] = execution._route_a_encode_spread_array(values, "entry_half_spread")
            intrabar = execution._route_a_decode_spread_array(vectors["intrabar_half_spread"], "intrabar_half_spread")
            changed["numerical_execution_cost_attestations"]["P95"] = execution.Backtester(config)._numerical_execution_cost_attestation(
                entry_half_spread=values, intrabar_half_spread=intrabar,
                spread_meta=changed["spread_model_metadata"]["P95"], spec=spec)
            with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
                original_source(changed, geometry, source_frame=source_frame, spec=spec,
                    tiers=tiers, config=config, timeframe=timeframe)
            assert caught.value.code == "MATCHED_FLOOR_SPREAD_ARRAY_SOURCE_CHANGED"
            state["array_source_negative"] = True
        if not state["held_source_negative"] or (facts["trades"] and not state["genuine_held_source_negative"]):
            changed = copy.deepcopy(facts)
            if changed["trades"] and "held_bar_inputs" in changed["trades"][0]["base_fill_witness"]:
                witness = changed["trades"][0]["base_fill_witness"]
                witness["held_bar_inputs"][0]["ohlc"]["Close"] += 1.0
                fields = {"direction", "entry_bar", "exit_bar", "partition_bar_count", "entry_time_utc", "exit_time_utc",
                    "entry_mid_open", "p75_entry_half_price", "observed_entry_price", "observed_exit_price",
                    "original_stop_price", "original_target_price", "exit_reason", "held_bar_inputs"}
                inputs = {key: witness[key] for key in fields}
                changed["trades"][0]["base_fill_witness"] = {**inputs,
                    **execution._route_a_replay_base_fill_inputs(inputs),
                    "held_bar_inputs_sha256": execution._sha256(inputs["held_bar_inputs"])}
                state["genuine_held_source_negative"] = True
            else:
                # Negative insertion only; never reported as an actual completed trade.
                frame = source_frame.loc[(source_frame.index >= pd.Timestamp(geometry["frame_start_utc"]))
                    & (source_frame.index < pd.Timestamp(geometry["end_exclusive_utc"]))]
                assert len(frame)
                ohlc = {key: float(frame.iloc[0][key]) for key in ("Open", "High", "Low", "Close")}
                ohlc["Close"] += 1.0
                witness = {"entry_bar": 0, "entry_time_utc": frame.index[0].isoformat(),
                    "entry_mid_open": float(frame.iloc[0]["Open"]),
                    "held_bar_inputs": [{"bar": 0, "open_utc": frame.index[0].isoformat(), "ohlc": ohlc,
                        "p75_intrabar_half_price": 0.0}], "status": "MISMATCH",
                    "reason_code": "INVENTED_NEGATIVE_INSERTION_NOT_A_COMPLETED_TRADE"}
                witness["held_bar_inputs_sha256"] = execution._sha256(witness["held_bar_inputs"])
                changed["trades"] = [{"base_fill_witness": witness}]
            changed["base_fill_witness_sha256"] = execution._sha256([row["base_fill_witness"] for row in changed["trades"]])
            with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
                original_source(changed, geometry, source_frame=source_frame, spec=spec,
                    tiers=tiers, config=config, timeframe=timeframe)
            assert caught.value.code == "MATCHED_FLOOR_HELD_BAR_SOURCE_CHANGED"
            state["held_source_negative"] = True
        return result

    @checked
    def cold(*args, **kwargs):
        calls["cold_publication_chain"] += 1
        return original_cold(*args, **kwargs)

    with _invented_fixture_external_io_guard(monkeypatch) as external_io_attempts:
        with monkeypatch.context() as scoped:
            # Only the fixture's ordinary constructor call is redirected. The new
            # constructor executes its real versioned freezer/verifier unchanged.
            scoped.setattr(planner, "freeze_production_search_process", planner.freeze_route_a_full_panel_production_search_process)
            env = _build_owned_invented_declared_inputs(tmp_path,
                source_access_window_utc=_route_a_owned_utc_window(), native_fixture=native_fixture)
        assert env.process.to_dict()["schema_version"] == planner.ROUTE_A_FULL_PANEL_PROCESS_SCHEMA
        assert "winner_selection_binding" in env.process.to_dict()
        committed = orchestration.verify_declared_slot_owner_run_precommit(env.context, env.precommit, process=env.process)
        cell = next(row for row in committed["cells"] if row["cell_id"] == env.cell_id)
        sink = spool.open_symbol_sink(env.context, cell["symbol"])
        request = orchestration.declared_slot_producer_request(env.context, precommit=env.precommit,
            process=env.process, cell_id=env.cell_id, slot_id=env.slot_id,
            attempt_id="fresh-full-panel-invented-model-wiring" if instrument_models else "manual-fresh-full-panel-actual-models",
            sink=sink)
        assert execution._read_declared_unit_header(env.context, request.to_dict()) is None
        try:
            with monkeypatch.context() as scoped:
                scoped.setattr(execution.Backtester, "run", model_run)
                scoped.setattr(execution, "fit_native_timeframe_for_search", fit)
                scoped.setattr(execution, "_reopen_route_a_matched_source_material", source_reopen)
                scoped.setattr(execution, "_route_a_pair_check", pair_check)
                scoped.setattr(execution, "_build_planned_attempt_evidence", forbidden_ordinary_builder)
                scoped.setattr(execution._FullPanelOwnerPreparation, "prepare_owned", prepare)
                scoped.setattr(execution, "_prepare_route_a_full_panel_owner_evidence", factory)
                scoped.setattr(execution, "_continue_declared_attempt_units", loop)
                scoped.setattr(execution, "_publish_declared_completed_unit", publish)
                scoped.setattr(execution, "_verify_route_a_prepared_stage_origin", origin)
                scoped.setattr(execution, "_verify_route_a_matched_fact_source", source)
                scoped.setattr(raw, "verify_matched_floor_publication_chain", cold)
                scoped.setattr(execution, "_make_declared_unit_header", header)
                scoped.setattr(adapter.DeclaredSlotSearchCapture, "restore_completed_unit_handle", restore)
                if instrument_models:
                    scoped.setattr(execution, "_build_full_prefix_strategy_surface", instrumented_surface)
                progress("STARTED", provenance)
                completion = producer.execute_declared_slot_owner_search(env.context, request=request,
                    process=env.process, frozen_pipeline_config=env.frozen_pipeline_config)
                if failures:
                    raise failures[0]
                assert completion["status"] in {"OWNER", "NO_TRADE"}
                assert completion["raw_production_receipt"] is not None
                assert completion["winner_request_entry"] is not None and completion["winner_decision_entry"] is not None
                assert completion["production_descriptor_graph"] is not None
                assert state["owner_closed"] and state["cache_replay"] is not None
                assert state["origin_negatives"]
                assert state["published"] == state["units"]
                snapshot = state["snapshot"]
                planned = sum(row["status"] == "PLANNED" for row in snapshot["attempt_roster"])
                rejected = sum(row["status"] == "PROCESS_REJECTED" for row in snapshot["attempt_roster"])
                assert planned and rejected and planned + rejected == len(state["published"])
                assert calls["ordinary_builder"] == calls["restore"] == 0
                assert calls["backtester"] == planned * 4 * 6
                assert calls["pair_check"] == planned * 4 * 3
                assert calls["stage_origin"] == planned
                assert len({row["raw_attempt_id"] for row in state["published"]}) == snapshot["original_trial_count"]
                before_cold = dict(calls)
                saved = producer._declared_saved_raw_completion(env.context, request=request,
                    process=env.process, frozen_pipeline_config=env.frozen_pipeline_config)
                if failures:
                    raise failures[0]
                assert execution._canonical_bytes(saved) == execution._canonical_bytes(completion)
                for name in ("backtester", "pair_check", "source_fit", "factory", "prepare", "header", "loop", "unit_publish", "stage_origin"):
                    assert calls[name] == before_cold.get(name, 0)
                assert calls["cold_publication_chain"] > before_cold.get("cold_publication_chain", 0)
                assert calls["cold_source_reopen"] > before_cold.get("cold_source_reopen", 0)
                assert state["published"] == state["units"]
                assert state["cold_source_calls"] > 0
                assert state["array_source_negative"] and state["held_source_negative"]
                if not instrument_models:
                    assert state["genuine_held_source_negative"], "Actual-model coverage needs a genuine completed BASE held witness"
                boundary = dict(calls)
                with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
                    retained_adapters[0](state["cache_replay"]["ordinal"])
                assert caught.value.code == "ROUTE_A_UNIT_BATCH_CLOSED"
                assert calls["backtester"] == boundary.get("backtester", 0)
                state["phase"] = "COMPLETE"
                receipt = {"fixture_provenance": provenance, "original_trials": snapshot["original_trial_count"],
                    "planned_trials": planned, "rejected_trials": rejected,
                    "planned_raw_lanes": planned * 24, "paired_groups": planned * 12,
                    "actual_counters": dict(calls), "cache_replay": state["cache_replay"],
                    "real_issuer_custody_raw_cold_wiring": True,
                    "actual_strategy_model_evidence": not instrument_models,
                    "genuine_completed_BASE_held_source_tamper_observed": state["genuine_held_source_negative"],
                    "instrumented_empty_case_held_tamper_kind": ("INVENTED_NEGATIVE_INSERTION" if instrument_models else None),
                    "financial_or_performance_evidence": False}
                record_property("full_panel_owner_receipt", execution._canonical_bytes(receipt).decode("ascii"))
                progress("COMPLETED", provenance, original_trials=receipt["original_trials"], planned_trials=planned, rejected_trials=rejected)
        finally:
            orchestration._release_declared_slot_producer_request(request, env.context)
            retained_adapters.clear()
            retained_stages.clear()
        assert not external_io_attempts
    return receipt


def test_fresh_full_panel_owner_instrumented_models_strict_raw_and_cold_origin(
        tmp_path, monkeypatch, _route_a_owned_invented_native_source, record_property):
    """Genuine owner/custody/cache/Raw proof with explicitly invented zero-signal models."""
    _run_fresh_full_panel_owner_fixture(tmp_path, monkeypatch,
        _route_a_owned_invented_native_source, record_property, instrument_models=True)
