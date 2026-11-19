"""INVENTED metadata; real routing and custody APIs, no market or model evaluation."""

import copy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_historical_scheduler as scheduler
import pm_historical_search_producer as producer
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_production_anchor_plan as anchors
import pm_production_config_binding as binding
import pm_production_search_planner as planner
import pm_winner_selection as winner


def _context(tmp_path):
    return SimpleNamespace(root=tmp_path, limits=spool.SpoolLimits())


def _row(kind):
    row = {"run_id": "INVENTED-NATIVE-K1-ROUTING",
        "cell": {"symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"}}
    if kind in {"native_one", "native_handover", "native_final", "m5_one", "m5_handover", "m5_final"}:
        mode, count = (("handover", 2) if kind.endswith("handover")
            else ("final_study_13", 13) if kind.endswith("final") else ("one_period", 1))
        row.update(period_selection={"mode": mode,
            "selected_slot_ids": [f"INVENTED-SLOT-{index}" for index in range(count)]},
            slot_roster_sha256="a" * 64)
        if kind.endswith("final"):
            row["final_study_13"] = True
    if kind.startswith("m5_"):
        row["m5_source"] = {"provenance": "INVENTED routing-only marker"}
    elif kind != "legacy":
        row["native_finalization"] = {"provenance": "INVENTED routing-only marker"}
    return row


@pytest.mark.parametrize("kind,development,expected", (
    ("native_one", False, "route_a"),
    ("development", True, "route_a"),
    ("native_handover", False, "route_a"),
    ("native_final", False, "route_a"),
    ("legacy", False, "default"),
    ("m5_one", False, "route_a"),
    ("m5_handover", False, "route_a"),
    ("m5_final", False, "route_a"),
))
def test_fresh_ordinary_modes_select_route_a_and_legacy_stays_default(tmp_path, monkeypatch, kind, development, expected):
    context, row = _context(tmp_path), _row(kind)
    original = copy.deepcopy(row)
    observed = []
    returned = {"route_a": object(), "default": object()}

    def freeze(edition):
        def invoke(actual_context, *, run_id):
            assert actual_context is context and run_id == row["run_id"]
            observed.append(edition)
            return returned[edition]
        return invoke

    monkeypatch.setattr(producer, "_read_declared_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(planner, "freeze_route_a_full_panel_production_search_process", freeze("route_a"))
    monkeypatch.setattr(planner, "freeze_production_search_process", freeze("default"))
    actual = scheduler._declared_initial_process(context, row, development=development)
    assert actual is returned[expected] and observed == [expected]
    assert row == original
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("saved_schema", (planner.PROCESS_SCHEMA, planner.ROUTE_A_FULL_PANEL_PROCESS_SCHEMA))
def test_saved_process_is_reopened_without_edition_promotion(tmp_path, monkeypatch, saved_schema):
    context, row = _context(tmp_path), _row("native_one")
    process = SimpleNamespace(to_dict=lambda: {"run_id": row["run_id"], "schema_version": saved_schema})
    saved_entry = {"artifact_id": planner.PROCESS_ARTIFACT_ID,
        "descriptor": {"sha256": "b" * 64}}
    saved_record = ({"artifacts": [saved_entry]}, b"INVENTED manifest bytes", {"sha256": "c" * 64})
    receipt = {"provenance": "INVENTED already-registered manifest receipt"}
    calls = []

    def reopen(actual_context, *, spool_receipt, expected_receipt_sha256):
        assert actual_context is context and spool_receipt is receipt
        assert expected_receipt_sha256 == saved_entry["descriptor"]["sha256"]
        calls.append("reopen")
        return process

    def forbidden(*args, **kwargs):
        raise AssertionError("A saved process must not be frozen again or promoted")

    monkeypatch.setattr(producer, "_read_declared_record", lambda *args, **kwargs: saved_record)
    monkeypatch.setattr(producer, "_declared_symbol_receipt", lambda *args, **kwargs: receipt)
    monkeypatch.setattr(planner, "reopen_production_search_process", reopen)
    monkeypatch.setattr(planner, "freeze_production_search_process", forbidden)
    monkeypatch.setattr(planner, "freeze_route_a_full_panel_production_search_process", forbidden)
    assert scheduler._declared_initial_process(context, row, development=False) is process
    assert calls == ["reopen"] and process.to_dict()["schema_version"] == saved_schema
    assert not list(tmp_path.iterdir())


def _real_config_context(tmp_path):
    # This documentary config has no observations, account, credentials or live authority.
    payload = {"_meta": {"scope": "UNISSUED_TECHNICAL_BENCHMARK",
        "fixture_classification": "INVENTED_K1_ROUTE_METADATA_ONLY"},
        "pipeline": {"data_dir": str(tmp_path / "data"), "output_dir": str(tmp_path / "outputs"),
            "log_dir": str(tmp_path / "logs"), "research_mode": True,
            "manual_kill_switch_path": str(tmp_path / "unused-stop.json"),
            "regime_params_file": str(tmp_path / "absent-regime-params.json"),
            "initial_capital": 10000.0, "discovery_risk_per_trade_pct": 1.0,
            "risk_per_trade_pct": 0.25, "backtest_spread_mode": "conservative_max",
            "oos_validation_mode": "fixed_holdout"},
        "symbols": ["EURUSD"], "position": {}, "storage": {}}
    path = tmp_path / "invented-config.json"
    path.write_text(json.dumps(payload, allow_nan=False), encoding="utf-8")
    owned, effective, refs = binding.resolve_effective_pipeline_config_identity(path)
    context = spool.freeze_generation_context(tmp_path / "spool",
        generation_id="INVENTED-K1-PUBLIC-ROUTE", native_universe_sha256="c" * 64,
        config_sha256=refs["config_sha256"])
    frozen = binding.freeze_production_pipeline_config(context, active_config_path=path,
        config_payload=owned, supplied_config=effective)
    return context, frozen, refs


def _public_ordinary_route(tmp_path, *, kind, mode, count):
    context, frozen, refs = _real_config_context(tmp_path)
    roster = anchors.derive_declared_owner_slot_roster_v2(
        launch_activation_utc="2023-06-24T00:00:00Z",
        frozen_refit_clock={"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
        config_identity_refs=refs, extend_frozen_clock_backward=True,
        protected_end_exclusive_utc="2024-07-14T08:05:00Z")
    selected = orchestration.declared_ordinary_period_selection(roster, mode=mode,
        final_study_13=mode == "final_study_13", symbols=("EURUSD",))
    suffix = {"one_period": "one", "handover": "handover", "final_study_13": "final"}[mode]
    row = _row(kind + "_" + suffix)
    row.update(run_id=context.generation_id, period_selection=selected,
        slot_roster_sha256=hashlib.sha256(orchestration._declared_json(roster)).hexdigest())
    process = scheduler._declared_initial_process(context, row, development=False)
    body = process.to_dict()
    assert body["schema_version"] == planner.ROUTE_A_FULL_PANEL_PROCESS_SCHEMA
    assert planner.full_panel_generation(process) is winner.V17_GENERATION
    assert body["winner_selection_binding"]["selection_version"] == winner.V17_GENERATION.selection_version
    assert body["winner_selection_binding"]["selection_policy_sha256"] == winner.V17_GENERATION.policy_sha256
    assert len(body["semantics"]["strategy_domains"]) == 100
    assert body["semantics"]["attempt_policy"]["attempt_count_per_observed_cell"] == 300
    cell = {"cell_id": winner.compute_cell_id("EURUSD", "H1", "TREND"),
        "symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"}
    now = datetime.now(timezone.utc).replace(microsecond=0)
    extra = {}
    issue = orchestration.precommit_declared_slot_owner_run_v2
    if kind == "m5":
        from test_development_cell_prefix import _prefix_m5_pins
        extra["m5_source_pins"] = _prefix_m5_pins(SimpleNamespace(context=context,
            cell=cell, profile_sha="d" * 64))
        issue = orchestration.precommit_declared_m5_slot_owner_run
    precommit = issue(context,
        run_id=context.generation_id, process=process, slot_roster=roster, cell_roster=[cell],
        frozen_pipeline_config=frozen, spread_profile_identity={"profile_sha256": "d" * 64},
        source_access_window_utc={"start_utc": (now-timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_exclusive_utc": (now+timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")},
        launch_activation_utc="2023-06-24T00:00:00Z", period_selection=selected,
        final_study_13=mode == "final_study_13", **extra)
    parent = orchestration.verify_declared_slot_owner_run_precommit(context, precommit, process=process)
    assert parent["schema_version"] == (orchestration.DECLARED_M5_SLOT_PRECOMMIT_SCHEMA
        if kind == "m5" else orchestration.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2)
    assert orchestration.declared_slot_source_kind(parent) == ("m5_derived" if kind == "m5" else "broker_native")
    assert orchestration.declared_slot_transport_edition(parent) == orchestration.DECLARED_WINNER_TRANSPORT_V16
    assert list(orchestration.declared_slot_selected_slots(parent)) == roster["slots"][:count]
    assert not {"development_scope", "development_scope_sha256"} & set(parent)
    sink = spool.open_symbol_sink(context, "EURUSD")
    request = orchestration.declared_slot_producer_request(context, precommit=precommit,
        process=process, cell_id=cell["cell_id"], slot_id=roster["slots"][count-1]["slot_id"],
        attempt_id="INVENTED-PUBLIC-K1-ROUTING", sink=sink)
    try:
        issued = orchestration.verify_declared_slot_producer_request(context, request,
            precommit=precommit, process=process)
        assert issued["search_process_receipt_sha256"] == process.receipt_sha256
        assert producer._declared_capture_opener(context, process=process,
            transport_edition=orchestration.declared_slot_transport_edition(issued)) is adapter.open_matched_floor_declared_slot_capture
        assert producer._declared_observed_cell_executor(context, process=process,
            transport_edition=orchestration.declared_slot_transport_edition(issued)) is producer.search_execution.execute_route_a_full_panel_historical_observed_cell
        assert issued["anchor"]["ordinal"] == count
        if kind == "m5":
            from test_m5_matched_cross_binding import _refusal
            assert _refusal(planner.full_panel_generation(process)) == "PROVENANCE_CROSS_BINDING_ROSTER_INVALID"
    finally:
        orchestration._release_declared_slot_producer_request(request, context)
    if count < 13:
        with pytest.raises(orchestration.HistoricalAnchorOrchestrationError, match="^DECLARED_SLOT_SLOT_INVALID$"):
            orchestration.declared_slot_producer_request(context, precommit=precommit,
                process=process, cell_id=cell["cell_id"], slot_id=roster["slots"][count]["slot_id"],
                attempt_id="INVENTED-UNSELECTED-ORDINARY", sink=sink)


def test_fresh_native_one_period_public_issuer_routes_to_actual_v17_producer(tmp_path):
    _public_ordinary_route(tmp_path, kind="native", mode="one_period", count=1)


@pytest.mark.parametrize("kind,mode,count", (
    ("native", "handover", 2), ("native", "final_study_13", 13),
    ("m5", "one_period", 1), ("m5", "handover", 2), ("m5", "final_study_13", 13),
), ids=("native-handover-2", "native-final13-13", "m5-one-1", "m5-handover-2", "m5-final13-13"))
def test_fresh_added_modes_public_issuers_route_to_actual_v17_producer(tmp_path, kind, mode, count):
    _public_ordinary_route(tmp_path, kind=kind, mode=mode, count=count)


@pytest.mark.parametrize("full_panel", (False, True))
def test_actual_saved_process_reopens_original_bytes_without_freezer(tmp_path, monkeypatch, full_panel):
    context, _, _ = _real_config_context(tmp_path)
    freeze = (planner.freeze_route_a_full_panel_production_search_process if full_panel
        else planner.freeze_production_search_process)
    original = freeze(context, run_id=context.generation_id)

    def forbidden(*args, **kwargs):
        raise AssertionError("The public saved-process path must not issue another process")

    monkeypatch.setattr(planner, "freeze_production_search_process", forbidden)
    monkeypatch.setattr(planner, "freeze_route_a_full_panel_production_search_process", forbidden)
    row = _row("native_one")
    row["run_id"] = context.generation_id
    reopened = scheduler._declared_initial_process(context, row, development=False)
    assert reopened is not original and reopened.receipt_sha256 == original.receipt_sha256
    assert reopened._body_json == original._body_json and reopened.spool_receipt == original.spool_receipt
    assert reopened.to_dict()["schema_version"] == (planner.ROUTE_A_FULL_PANEL_PROCESS_SCHEMA
        if full_panel else planner.PROCESS_SCHEMA)


from test_development_cell_prefix import prefix_metadata_factory


@pytest.mark.parametrize("workers", (2, 4))
def test_final13_parallel_worker_reopens_actual_route_a_before_numerical_work(
        prefix_metadata_factory, monkeypatch, workers):
    """INVENTED metadata: actual claim/worker custody and dispatcher, no market work."""
    env = prefix_metadata_factory(full_panel=True, symbol="EURUSD")
    selected = orchestration.declared_ordinary_period_selection(env.roster,
        mode="final_study_13", final_study_13=True, symbols=("EURUSD",))
    precommit = orchestration.precommit_declared_slot_owner_run_v2(env.context,
        **env.kwargs, period_selection=selected, final_study_13=True)
    parent = orchestration.verify_declared_slot_owner_run_precommit(env.context,
        precommit, process=env.process)
    assert planner.full_panel_generation(env.process) is winner.V17_GENERATION
    assert len(orchestration.declared_slot_selected_slots(parent)) == 13
    assert orchestration.declared_slot_transport_edition(parent) == orchestration.DECLARED_WINNER_TRANSPORT_V16
    row = {"cell": env.cell, "run_id": env.context.generation_id, "root": str(env.context.root),
        "launch_activation_utc": env.kwargs["launch_activation_utc"],
        "frozen_refit_clock": env.roster["frozen_refit_clock"],
        "extend_frozen_clock_backward": True,
        "protected_end_exclusive_utc": env.roster["protected_end_exclusive_utc"],
        "period_selection": selected, "final_study_13": True,
        "slot_roster_sha256": hashlib.sha256(orchestration._declared_json(env.roster)).hexdigest(),
        "slot_parallel_workers": workers,
        "source_access_window_policy": {"lead_seconds": 0, "validity_seconds": 3600},
        "native_finalization": {"end_grid_root": str(env.root / "grid"),
            "source_fence_path": str(env.root / "fence/source.json"),
            "retirement": {"receipt_path": str(env.root / "retirement/receipt.json"),
                "max_paths": 181815, "max_nodes": 2000000, "max_bytes": 549755813888}}}
    contract = {"schema_version": scheduler.DECLARED_SCHEDULER_SCHEMA,
        "active_config_path": str(env.frozen.active_config_path),
        "native_universe_sha256": env.context.native_universe_sha256,
        "config_sha256": env.context.config_sha256, "profile_sha256": env.profile_sha,
        "cells": [row]}
    checked, _ = scheduler._declared_contract(contract)
    assert checked["cells"][0]["slot_parallel_workers"] == workers
    for mode in ("one_period", "handover"):
        refused = copy.deepcopy(contract)
        refused["cells"][0].update(final_study_13=False,
            period_selection=orchestration.declared_ordinary_period_selection(
                env.roster, mode=mode, symbols=("EURUSD",)))
        with pytest.raises(scheduler.HistoricalSchedulerError,
                match="^DECLARED_SLOT_PARALLEL_FULL_ROSTER_REQUIRED$"):
            scheduler._declared_contract(refused)
    slot = env.roster["slots"][-1]
    spec = producer._parallel_slot_spec(env.context, precommit, env.process, env.frozen,
        env.cell["cell_id"], slot, "INVENTED-PARALLEL-ROUTE-A")
    assert spec["process_sha256"] == env.process.receipt_sha256
    assert spec["process_spool_receipt"] == env.process.spool_receipt
    root = producer._parallel_slot_root(precommit.receipt_sha256, env.cell["cell_id"])
    claim_path = producer._parallel_slot_path(root, "claims", slot["ordinal"])
    claim = spool._publish_exact_json(env.context, claim_path,
        {"schema_version": producer._PARALLEL_CLAIM_SCHEMA, "binding": spec},
        env.context.limits.max_json_bytes)
    observed = []
    original_dispatcher = producer._declared_observed_cell_executor

    def observe_dispatcher(context, *, process, transport_edition):
        assert process is not env.process and process._body_json == env.process._body_json
        assert transport_edition == orchestration.DECLARED_WINNER_TRANSPORT_V16
        selected_executor = original_dispatcher(context, process=process,
            transport_edition=transport_edition)
        assert selected_executor is producer.search_execution.execute_route_a_full_panel_historical_observed_cell
        assert producer._declared_capture_opener(context, process=process,
            transport_edition=transport_edition) is adapter.open_matched_floor_declared_slot_capture
        observed.append("actual-route-a-dispatcher")
        return selected_executor

    def documentary_completion(context, *, request, process, frozen_pipeline_config, **kwargs):
        assert observed == ["actual-route-a-dispatcher"]
        body = orchestration.verify_declared_slot_producer_request(context, request,
            process=process)
        assert body["slot_id"] == slot["slot_id"] and body["anchor"]["ordinal"] == 13
        observed.append("invented-completion-before-source")
        completion = producer._declared_base_completion(request, status="STOP",
            reason_code="INVENTED_METADATA_BOUNDARY_REACHED", source_entry=None)
        completion["fixture_provenance"] = "INVENTED_ROUTING_ONLY_NOT_SOURCE_OR_OWNER_EVIDENCE"
        return completion

    def forbid_source(*args, **kwargs):
        raise AssertionError("Parallel routing witness must stop before source or numerical work")

    monkeypatch.setattr(producer, "_declared_observed_cell_executor", observe_dispatcher)
    monkeypatch.setattr(producer, "_declared_saved_raw_completion", documentary_completion)
    monkeypatch.setattr(producer.native_prefix, "produce_activated_native_prefix", forbid_source)
    monkeypatch.setattr(producer.search_execution, "fit_native_timeframe_for_search", forbid_source)
    import pm_portfolio_replay as portfolio
    monkeypatch.setattr(portfolio, "active_native_portfolio_contract", forbid_source)
    result_sha = producer.declared_slot_worker(spec)
    result, digest = producer._parallel_slot_read_result(env.context,
        producer._parallel_slot_path(root, "results", 13), claim["sha256"], spec,
        expected_sha=result_sha)
    assert digest == result_sha and result["failure"] is None
    assert observed == ["actual-route-a-dispatcher", "invented-completion-before-source"]
    assert result["completion"]["reason_code"] == "INVENTED_METADATA_BOUNDARY_REACHED"
    assert result["completion"]["fixture_provenance"].startswith("INVENTED_ROUTING_ONLY")
    assert not (env.context.root / "declared-slots" / precommit.receipt_sha256 / "source-access-renewal").exists()
