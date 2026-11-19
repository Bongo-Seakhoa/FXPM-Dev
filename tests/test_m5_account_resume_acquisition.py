"""INVENTED upstream metadata; actual saved-account worker/finalizer view-state join.

Native/cost acquisition and account semantics are labelled stand-ins. Three
invented M5 bars exercise the real M5JobViews/_JobState acquire, derived view,
consumer binding, release and pending checks. This is not cold native-source,
financial, whole finalization, Grid or retirement proof.
"""
from dataclasses import asdict
import hashlib
import json
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_causal_evidence_producer as causal
import pm_forward_oos_replay_producer as forward
import pm_historical_anchor_orchestration as orchestration
import pm_historical_scheduler as scheduler
import pm_historical_search_producer as producer
import pm_m5_job_view as views
import pm_portfolio_replay as portfolio
import pm_production_config_binding as binding
from pm_explicit_derived_history import Bar, Interval, SourceIdentity, Stratum


_PROVENANCE = "INVENTED_UPSTREAM_ONLY_REAL_M5_VIEW_STATE_NO_FINANCIAL_OR_NATIVE_PROOF"


def _sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def _three_bar_owner(context, *, precommit_sha, process_sha, cell_id, terminal):
    start, end = "2024-01-02T00:00:00+00:00", "2024-01-02T00:15:00+00:00"
    interval = Interval(start, end)
    source = SourceIdentity("UNITUSD", "M5", _PROVENANCE, _sha("activation"),
        _sha("bars"), _sha("sidecar"), _sha("provenance"), _sha("request"), 0.0001)
    budgets = views.ViewBudgets(16, 131072, 16, 131072, 32, 262144)
    request = views.ViewRequest("account-view", source, "M15", interval, end,
        _sha("constructor"), _sha("gaps"), terminal, views.RECIPE_SHA256,
        stratum=Stratum("M15", "INVENTED_CLASS", "INVENTED_FIXED_ALIGNMENT"))
    job_binding = views.DerivedJobBinding(_sha("m5-contract"), context.context_sha256,
        context.config_sha256, _sha("effective-config"), process_sha, precommit_sha,
        context.generation_id, cell_id, _sha(asdict(budgets)),
        views.value_plan_sha256((request,)), (terminal,), terminal)
    # Explicit upstream-origin seam: no public native-product constructor is claimed.
    owner = object.__new__(views.M5JobViews)
    owner._state = views._JobState(job_binding, (request,), budgets=budgets)
    owner._context = context
    owner._data_dir = context.root
    owner._products = {end: object()}
    owner._routes = {}
    owner._source = views.M5ArchiveSource("UNITUSD", source, _sha("source-identity"),
        interval, interval, "INVENTED_CLASS", 3, 1, 1, _sha("metadata"),
        "INVENTED_NOT_OPENED.csv", "INVENTED_NOT_OPENED.json")
    owner._spans, owner._hull = (interval,), interval
    stamps = (start, "2024-01-02T00:05:00+00:00", "2024-01-02T00:10:00+00:00", end)
    owner._base_rows = tuple(Bar(stamps[i], stamps[i + 1], 1.0, 1.1, 0.9, 1.05,
        2, 3, 4) for i in range(3))
    owner._examined, owner._observation = (interval,), None
    return owner, end


def test_saved_m5_account_acquires_actual_terminal_view_before_finalization(tmp_path, monkeypatch):
    """The old unconditional body handoff skips acquire and cannot reach the boundary."""
    cell_id, contract_sha = _sha("cell"), _sha("contract")
    context = spool.freeze_generation_context((tmp_path / "retained-m5-resume").resolve(),
        generation_id="INVENTED-M5-resume", native_universe_sha256=_sha("universe"),
        config_sha256=_sha("config"),
        owned_workspace={"cell_id": cell_id, "cell_contract_sha256": contract_sha})
    precommit = SimpleNamespace(receipt_sha256=_sha("precommit"),
        to_dict=lambda: {"fixture_provenance": _PROVENANCE})
    process = SimpleNamespace(receipt_sha256=_sha("process"))
    terminal, slot_id = "INVENTED-terminal", "INVENTED-slot"
    owner, end = _three_bar_owner(context, precommit_sha=precommit.receipt_sha256,
        process_sha=process.receipt_sha256, cell_id=cell_id, terminal=terminal)
    consumer = views.NoTradeAccountViewBinding("DEVELOPMENT_PARTIAL_ACCOUNT", end,
        _sha("saved-run"), precommit.receipt_sha256)
    expected_consumer = views._consumer(owner.binding, consumer)
    assert owner.consumer_bindings_for(terminal) == ()
    assert owner.pending_view_ids == ("account-view",)
    run_body = {"terminal_stop": False, "slot_commits": [{"slot_id": slot_id, "status": "NO_TRADE"}]}
    body = {"fixture_provenance": _PROVENANCE, "progress_status": "DEVELOPMENT_COMPLETE",
        "terminal_consumer_binding_sha256": [expected_consumer],
        "saved_cell_run_receipt_sha256": _sha("saved-run")}
    sink = spool.open_symbol_sink(context, "UNITUSD")
    entry = sink.put_json("invented-saved-account", "software_fixture", body)
    commit = {"slot_id": slot_id, "fixture_provenance": _PROVENANCE}
    commit_entry = sink.put_json("invented-saved-commit", "declared_slot_commit", commit)
    record = (commit, commit_entry["descriptor"]["sha256"], commit_entry["descriptor"])
    artifact = SimpleNamespace(descriptor=entry["descriptor"])
    slot = {"slot_id": slot_id, "ordinal": 1, "end_exclusive_utc": end}
    edition = SimpleNamespace(reopen_route=lambda: {"fixture_provenance": _PROVENANCE})
    config = SimpleNamespace(data_dir=str(tmp_path / "INVENTED_NOT_OPENED"))
    frozen = SimpleNamespace(materialize=lambda _: config)
    dev = {"development_slot_ids": [slot_id], "slot_view_ids": {slot_id: "slot-view"},
        "calibration_view_id": "calibration", "account_view_id": "account-view",
        "terminal_result_key": terminal, "view_axes": [{"view_id": "account-view", "cutoff_utc": end}],
        "m5_source_pins": {"view_plan_sha256": owner.view_plan_sha256},
        "end_grid_root": str(tmp_path / "INVENTED_NOT_FINALIZED"),
        "source_fence_path": str(tmp_path / "INVENTED_NOT_FINALIZED.json")}
    row = {"descriptor_index": 0, "root": str(context.root), "run_id": context.generation_id,
        "cell": {"cell_id": cell_id, "symbol": "UNITUSD", "timeframe": "M15", "regime": "TREND"},
        "m5_development": dev, "development": {"source_kind": "m5_derived", "selected_slot_ids": [slot_id]}}
    contract = {"schema_version": scheduler.DECLARED_DEVELOPMENT_CELL_SCHEDULER_SCHEMA, "cells": [row],
        "native_universe_sha256": context.native_universe_sha256, "config_sha256": context.config_sha256,
        "profile_sha256": _sha("profile"), "active_config_path": str(tmp_path / "INVENTED_NOT_OPENED.json"),
        "spool_limits": asdict(context.limits), "cost_edition_route": edition.reopen_route()}
    declared = context.root / "declared-slots" / precommit.receipt_sha256 / "precommit.json"
    declared.parent.mkdir(parents=True)
    declared.write_text('{"fixture_provenance":"INVENTED_NOT_A_PRECOMMIT"}', encoding="ascii")

    # These stand-ins route an already-saved account to the real worker/finalizer join.
    monkeypatch.setattr(scheduler, "_declared_development_contract", lambda value: (value, {}))
    monkeypatch.setattr(scheduler, "_declared_m5_dependencies", lambda: None)
    monkeypatch.setattr(scheduler, "_declared_m5_cell_contract_sha256", lambda *_: contract_sha)
    monkeypatch.setattr(scheduler, "_declared_reopen_process", lambda *_: process)
    monkeypatch.setattr(scheduler, "_declared_m5_check_roster", lambda *_, **__: [slot])
    monkeypatch.setattr(scheduler, "_declared_capture_period_facts", lambda *_, **__: None)
    monkeypatch.setattr(scheduler, "_declared_m5_reuse_recorded_rows", lambda *_, **__: False)
    monkeypatch.setattr(scheduler, "_declared_m5_source_resume", lambda *_, **__: True)
    monkeypatch.setattr(scheduler, "_declared_run_descriptor", lambda *_, **__: {"sha256": _sha("saved-run")})
    monkeypatch.setattr(scheduler, "_declared_m5_record_terminal_refusal", lambda *_, **__: None)
    monkeypatch.setattr(binding, "load_effective_pipeline_config", lambda _: ({}, config))
    monkeypatch.setattr(binding, "freeze_production_pipeline_config", lambda *_, **__: frozen)
    monkeypatch.setattr(orchestration, "reopen_declared_slot_owner_run_precommit", lambda *_, **__: precommit)
    monkeypatch.setattr(orchestration, "declared_slot_selected_slots", lambda _: [slot])
    monkeypatch.setattr(orchestration, "_declared_slot_effective_access", lambda *args: (
        {"start_utc": "2000-01-01T00:00:00+00:00", "end_exclusive_utc": "2100-01-01T00:00:00+00:00"}, None))
    monkeypatch.setattr(portfolio, "active_native_portfolio_contract", lambda *_, **__: ({}, []))
    monkeypatch.setattr(orchestration, "_declared_slot_native_snapshot_status", lambda *_: "MATCH")
    monkeypatch.setattr(forward, "declared_m5_development_cell_account_request", lambda *_, **__: {"cutoff_utc": end})
    monkeypatch.setattr(producer, "reopen_declared_m5_cell_source_context", lambda *_, **__: {
        "owner": owner, "cost_edition": edition, "slot_view_ids": dev["slot_view_ids"], "calibration_view_id": "calibration"})

    def completed(*_, _verified_progress_out, **__):
        _verified_progress_out["commits"] = {slot_id: record}
        return SimpleNamespace(to_dict=lambda: run_body)

    monkeypatch.setattr(producer, "execute_declared_cell_owner_slots", completed)
    monkeypatch.setattr(forward, "_prepare_declared_m5_development_cell_account_artifact",
        lambda *_, **__: (artifact, {"fixture_provenance": _PROVENANCE}, dict(body)))
    monkeypatch.setattr(forward, "_produce_declared_m5_development_cell_account_from_inputs",
        lambda *_, **__: pytest.fail("saved account must not be produced again"))
    monkeypatch.setattr(causal, "_verify_declared_cell_account_manifest",
        lambda context, body, descriptor, **__: spool._verify_artifact_file(context, descriptor))
    observed = []

    def acquire_saved_transport(context, saved, *, m5_owner, **kwargs):
        assert m5_owner is owner and saved == body
        assert owner.consumer_bindings_for(terminal) == ()
        lease = owner.acquire("account-view", consumer=consumer, result_key=terminal)
        try:
            actual_view = owner.view(lease)
            assert len(actual_view.rows) == 1 and actual_view.series.bars[0].member_count == 3
            assert owner.consumer_binding(lease) == consumer
        finally:
            lease.release()
        observed.append((owner.consumer_bindings_for(terminal), owner.pending_view_ids))

    monkeypatch.setattr(forward, "verify_declared_m5_development_cell_account_transport", acquire_saved_transport)

    class ReachedActualConsumerBoundary(BaseException):
        pass

    def after_consumer_checks(*args, **kwargs):
        assert observed == [((expected_consumer,), ())]
        raise ReachedActualConsumerBoundary()

    monkeypatch.setattr(scheduler, "_declared_retain_account_result", after_consumer_checks)
    with pytest.raises(ReachedActualConsumerBoundary):
        scheduler._declared_m5_run_one_cell(contract, row)
    assert observed == [((expected_consumer,), ())]
    assert owner._state.closed is True
