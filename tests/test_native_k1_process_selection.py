"""INVENTED routing metadata and freezer doubles; no market or model evaluation."""

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
    if kind in {"native_one", "native_handover", "native_final", "m5_one"}:
        mode, count = {"native_handover": ("handover", 2),
            "native_final": ("final_study_13", 13)}.get(kind, ("one_period", 1))
        row.update(period_selection={"mode": mode,
            "selected_slot_ids": [f"INVENTED-SLOT-{index}" for index in range(count)]},
            slot_roster_sha256="a" * 64)
        if kind == "native_final":
            row["final_study_13"] = True
    if kind == "m5_one":
        row["m5_source"] = {"provenance": "INVENTED routing-only marker"}
    elif kind != "legacy":
        row["native_finalization"] = {"provenance": "INVENTED routing-only marker"}
    return row


@pytest.mark.parametrize("kind,development,expected", (
    ("native_one", False, "route_a"),
    ("development", True, "route_a"),
    ("native_handover", False, "default"),
    ("native_final", False, "default"),
    ("legacy", False, "default"),
    ("m5_one", False, "default"),
))
def test_only_fresh_native_one_period_adds_route_a(tmp_path, monkeypatch, kind, development, expected):
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


def test_fresh_native_one_period_public_issuer_routes_to_actual_v17_producer(tmp_path):
    context, frozen, refs = _real_config_context(tmp_path)
    roster = anchors.derive_declared_owner_slot_roster_v2(
        launch_activation_utc="2023-06-24T00:00:00Z",
        frozen_refit_clock={"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
        config_identity_refs=refs, extend_frozen_clock_backward=True,
        protected_end_exclusive_utc="2024-07-14T08:05:00Z")
    selected = orchestration.declared_ordinary_period_selection(roster, symbols=("EURUSD",))
    row = _row("native_one")
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
    precommit = orchestration.precommit_declared_slot_owner_run_v2(context,
        run_id=context.generation_id, process=process, slot_roster=roster, cell_roster=[cell],
        frozen_pipeline_config=frozen, spread_profile_identity={"profile_sha256": "d" * 64},
        source_access_window_utc={"start_utc": (now-timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_exclusive_utc": (now+timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")},
        launch_activation_utc="2023-06-24T00:00:00Z", period_selection=selected)
    parent = orchestration.verify_declared_slot_owner_run_precommit(context, precommit, process=process)
    assert parent["schema_version"] == orchestration.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2
    assert orchestration.declared_slot_source_kind(parent) == "broker_native"
    assert orchestration.declared_slot_transport_edition(parent) == orchestration.DECLARED_WINNER_TRANSPORT_V16
    assert list(orchestration.declared_slot_selected_slots(parent)) == roster["slots"][:1]
    assert not {"development_scope", "development_scope_sha256"} & set(parent)
    sink = spool.open_symbol_sink(context, "EURUSD")
    request = orchestration.declared_slot_producer_request(context, precommit=precommit,
        process=process, cell_id=cell["cell_id"], slot_id=roster["slots"][0]["slot_id"],
        attempt_id="INVENTED-PUBLIC-K1-ROUTING", sink=sink)
    try:
        issued = orchestration.verify_declared_slot_producer_request(context, request,
            precommit=precommit, process=process)
        assert issued["search_process_receipt_sha256"] == process.receipt_sha256
        assert producer._declared_capture_opener(context, process=process,
            transport_edition=orchestration.declared_slot_transport_edition(issued)) is adapter.open_matched_floor_declared_slot_capture
    finally:
        orchestration._release_declared_slot_producer_request(request, context)


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
