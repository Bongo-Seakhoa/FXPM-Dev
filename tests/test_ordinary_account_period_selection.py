"""INVENTED metadata and account carriers; no issued owner, model or financial authority."""
from datetime import datetime, timedelta, timezone
import copy
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_authority_spool as spool
import pm_forward_oos_replay_producer as forward
import pm_historical_anchor_orchestration as historical
import pm_historical_search_producer as producer
import pm_production_anchor_plan as anchors


MODES = (("one_period", 1), ("handover", 2), ("final_study_13", 13))


def _parent(mode=None, *, m5=False):
    start = datetime(2026, 3, 28, tzinfo=timezone.utc) - timedelta(days=28 * 40)
    launch = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    roster = anchors.derive_declared_owner_slot_roster_v2(launch_activation_utc=launch,
        frozen_refit_clock={"anchor_activation_utc": "2026-03-29T00:01:00Z", "interval_calendar_days": 28},
        config_identity_refs={"config_sha256": "0" * 64, "effective_config_sha256": "1" * 64},
        extend_frozen_clock_backward=True,
        protected_end_exclusive_utc=historical._DECLARED_PROTECTED_END_EXCLUSIVE_UTC)
    body = {"schema_version": historical.DECLARED_M5_SLOT_PRECOMMIT_SCHEMA if m5
            else historical.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2,
        "launch_activation_utc": launch, "slot_roster": roster,
        "slot_roster_sha256": hashlib.sha256(historical._declared_json(roster)).hexdigest(),
        "cells": [{"cell_id": "d" * 64, "symbol": "EURUSD", "timeframe": "H1", "regime": "TREND"}]}
    if mode is not None:
        body["period_selection"] = historical.declared_ordinary_period_selection(
            body["slot_roster"], mode=mode, final_study_13=mode == "final_study_13", symbols=("EURUSD",))
    return body


def _run(parent):
    slots = historical.declared_slot_selected_slots(parent)
    return producer._declared_run_body(SimpleNamespace(context_sha256="a" * 64),
        precommit=SimpleNamespace(receipt_sha256="b" * 64, to_dict=lambda: parent),
        cell_id=parent["cells"][0]["cell_id"],
        summaries=[{"slot_id": slot["slot_id"], "status": "NO_TRADE"} for slot in slots],
        declared_slots=len(slots), terminal_stop=False, previous="e" * 64,
        symbol_receipt=None, terminal_failure=None)


@pytest.mark.parametrize("mode,count", MODES)
def test_ordinary_account_measurement_names_only_the_selected_span(mode, count):
    parent = _parent(mode)
    before = copy.deepcopy(parent)
    actual = forward._declared_cell_measurement(parent)
    assert actual["refit_cycles"] == count and actual["calendar_days"] == 28 * count
    assert actual["cycle_days"] == 28 and actual["risk_per_trade_pct"] == 0.25
    assert actual["terminal_policy"] == "MARK_OPEN"
    assert actual["end_exclusive_utc"] == parent["slot_roster"]["slots"][count - 1]["end_exclusive_utc"]
    assert parent == before


def test_absent_selection_keeps_the_legacy_h364_measurement():
    actual = forward._declared_cell_measurement(_parent())
    assert actual["refit_cycles"] == 13 and actual["calendar_days"] == 364


@pytest.mark.parametrize("mode,count", MODES)
def test_ordinary_run_is_complete_for_its_selection_without_development_tags(mode, count):
    parent = _parent(mode)
    actual = _run(parent)
    assert actual["schema_version"] == producer.DECLARED_CELL_RUN_SCHEMA
    assert actual["declared_slots"] == actual["completed_slots"] == count
    assert actual["period_selection"] == parent["period_selection"]
    assert "development_scope" not in actual and "development_status" not in actual
    assert all(actual[key] is False for key in (
        "qualification_authority", "capital_authority", "live_authority", "order_authority"))


def test_one_period_run_cannot_claim_thirteen_declared_slots():
    parent = _parent("one_period")
    with pytest.raises(producer.HistoricalSearchProducerError) as error:
        producer._declared_run_body(SimpleNamespace(context_sha256="a" * 64),
            precommit=SimpleNamespace(receipt_sha256="b" * 64, to_dict=lambda: parent),
            cell_id=parent["cells"][0]["cell_id"], summaries=[{
                "slot_id": parent["slot_roster"]["slots"][0]["slot_id"], "status": "NO_TRADE"}],
            declared_slots=13, terminal_stop=False, previous="e" * 64,
            symbol_receipt=None, terminal_failure=None)
    assert error.value.code == "DECLARED_ORDINARY_COMPLETION_PREFIX_INVALID"


def _m5_inputs():
    parent = _parent("one_period", m5=True)
    slot = parent["slot_roster"]["slots"][0]
    run = _run(parent)
    inputs = {"parent": parent, "progress": SimpleNamespace(
        slot_commit_artifacts=(), slot_summaries=(), run_receipt=run),
        "slot_schedule": [{"slot_id": slot["slot_id"], "ordinal": 1,
            "start_utc": slot["activation_utc"], "end_exclusive_utc": slot["end_exclusive_utc"],
            "status": "NO_TRADE", "owner_id": None}],
        "progress_status": "COMPLETE", "reason_code": None, "owner_records": []}
    return parent, inputs


def test_ordinary_m5_body_and_consumer_keep_ordinary_classification():
    parent, inputs = _m5_inputs()
    context = SimpleNamespace(generation_id="INVENTED", context_sha256="a" * 64,
        native_universe_sha256="f" * 64, config_sha256="0" * 64)
    frozen = SimpleNamespace(active_config_path=Path("INVENTED-NOT-READ.json"),
        config_sha256="0" * 64, effective_config_sha256="1" * 64)
    body = forward._declared_m5_cell_body(context,
        precommit=SimpleNamespace(receipt_sha256="b" * 64),
        process=SimpleNamespace(receipt_sha256="c" * 64), frozen_pipeline_config=frozen, inputs=inputs)
    assert body["schema_version"] == forward.DECLARED_CELL_RAW_ACCOUNT_SCHEMA
    assert body["source_kind"] == "m5_derived" and body["result_kind"] == "ORDINARY_SELECTED_PERIODS"
    assert body["progress_status"] == "COMPLETE" and body["measurement"]["refit_cycles"] == 1
    assert body["design_contaminated"] is True and body["pristine_validation"] is False
    assert "development_scope" not in body and "development_scope_sha256" not in body
    consumers = forward._declared_m5_account_consumers(inputs, precommit_sha256="b" * 64)
    assert len(consumers) == 1 and consumers[0].measurement_purpose == "ORDINARY_SELECTED_PERIODS_ACCOUNT"
    assert consumers[0].precommit_sha256 == "b" * 64


def test_ordinary_m5_consumer_refuses_a_development_run_receipt():
    _, inputs = _m5_inputs()
    inputs["progress"].run_receipt["schema_version"] = producer.DECLARED_DEVELOPMENT_CELL_RUN_SCHEMA
    with pytest.raises(forward.ForwardOOSReplayProductionError) as error:
        forward._declared_m5_account_consumers(inputs, precommit_sha256="b" * 64)
    assert error.value.code == "DECLARED_DEVELOPMENT_RUN_BINDING_MISMATCH"


@pytest.mark.parametrize("mode,count", MODES)
def test_retirement_selection_reuses_the_same_authenticated_prefix_policy(mode, count):
    parent = _parent(mode)
    body = {"symbol": "EURUSD", "accepted_slot_ids": [slot["slot_id"] for slot in parent["slot_roster"]["slots"]],
        "period_selection": parent["period_selection"]}
    assert spool._retirement_ordinary_period_selection(body) == parent["period_selection"]
    assert len(body["period_selection"]["selected_slot_ids"]) == count


@pytest.mark.parametrize("mode,allowed", (("one_period", False), ("final_study_13", True)))
def test_retirement_unadopted_symbol_needs_the_explicit_final_study(mode, allowed):
    parent = _parent(mode)
    body = {"symbol": "USDCHF", "accepted_slot_ids": [slot["slot_id"] for slot in parent["slot_roster"]["slots"]],
        "period_selection": parent["period_selection"]}
    if allowed:
        assert spool._retirement_ordinary_period_selection(body) == body["period_selection"]
    else:
        with pytest.raises(spool.AuthoritySpoolError) as error:
            spool._retirement_ordinary_period_selection(body)
        assert str(error.value) == "RETIREMENT_ORDINARY_PERIOD_SELECTION_INVALID"


@pytest.mark.parametrize("damage", ("crossed", "duplicate", "wrong_count"))
def test_retirement_selection_refuses_crossed_or_inflated_prefixes(damage):
    parent = _parent("handover")
    body = {"symbol": "EURUSD", "accepted_slot_ids": [slot["slot_id"] for slot in parent["slot_roster"]["slots"]],
        "period_selection": copy.deepcopy(parent["period_selection"])}
    if damage == "crossed":
        body["period_selection"]["selected_slot_ids"][0] = body["accepted_slot_ids"][4]
    elif damage == "duplicate":
        body["accepted_slot_ids"][1] = body["accepted_slot_ids"][0]
    else:
        body["period_selection"]["selected_slot_ids"].append(body["accepted_slot_ids"][2])
    with pytest.raises(spool.AuthoritySpoolError) as error:
        spool._retirement_ordinary_period_selection(body)
    assert str(error.value) == "RETIREMENT_ORDINARY_PERIOD_SELECTION_INVALID"


@pytest.mark.parametrize("m5", (False, True), ids=("native", "m5"))
@pytest.mark.parametrize("changed", (False, True), ids=("exact", "wrong_span"))
def test_retained_ordinary_account_matches_the_fence_selection(tmp_path, m5, changed):
    parent = _parent("one_period", m5=m5)
    selected = parent["period_selection"]
    binding = {"generation_context_sha256": "a" * 64, "run_id": "INVENTED",
        "precommit_sha256": "b" * 64, "search_process_receipt_sha256": "c" * 64,
        "cell_id": parent["cells"][0]["cell_id"], "terminal_result_key": "INVENTED-TERMINAL"}
    body = {"schema_version": forward.DECLARED_CELL_RAW_ACCOUNT_SCHEMA, "generation_id": "INVENTED",
        "generation_context_sha256": "a" * 64, "declared_precommit_sha256": "b" * 64,
        "search_process_receipt_sha256": "c" * 64, "cell": parent["cells"][0],
        "progress_status": "COMPLETE", "slot_schedule": [{"slot_id": selected["selected_slot_ids"][0]}],
        "measurement": {"refit_cycles": 2 if changed else 1, "calendar_days": 56 if changed else 28},
        "account": {"fixture_provenance": "INVENTED_NOT_AN_ACCOUNT_ENGINE_RESULT"},
        **{key: False for key in ("qualification_authority", "capital_authority", "live_authority", "order_authority")}}
    if m5:
        body.update(source_kind="m5_derived", result_kind="ORDINARY_SELECTED_PERIODS",
            design_contaminated=True, pristine_validation=False, saved_cell_run_receipt_sha256="e" * 64)
    encoded = spool._canonical_json_bytes(body, 1024**2)
    digest = hashlib.sha256(encoded).hexdigest()
    grid_root = tmp_path / "grid"
    grid_root.mkdir()
    path = grid_root / ("declared-account-" + digest + ".json")
    path.write_bytes(encoded)
    reference = {"path": str(path), "sha256": digest, "size_bytes": len(encoded)}
    fence = {"schema_version": spool.M5_ORDINARY_COMPLETION_FENCE_SCHEMA if m5
            else spool.NATIVE_SELECTED_PERIOD_COMPLETION_FENCE_SCHEMA,
        "symbol": "EURUSD", "accepted_slot_ids": [slot["slot_id"] for slot in parent["slot_roster"]["slots"]],
        "period_selection": selected, "retained_account_result": reference,
        "completion": {"account_artifact_sha256": digest, "cell_run_receipt_sha256": "e" * 64},
        "source_summary": {"results": [["INVENTED-TERMINAL", {"result_sha256": digest}]]}}
    if changed:
        with pytest.raises(spool.AuthoritySpoolError) as error:
            spool._retirement_retained_account(fence, binding=binding, grid_root=grid_root,
                working_root=tmp_path / "working", required=True)
        assert str(error.value) == "RETIREMENT_RETAINED_ACCOUNT_INVALID"
    else:
        assert spool._retirement_retained_account(fence, binding=binding, grid_root=grid_root,
            working_root=tmp_path / "working", required=True) == body


def test_public_ordinary_m5_verifier_keeps_cold_input_reopening(monkeypatch):
    calls = []
    body = {"schema_version": forward.DECLARED_CELL_RAW_ACCOUNT_SCHEMA, "source_kind": "m5_derived"}

    def cold(context, supplied, **kwargs):
        calls.append(kwargs)
        return supplied

    monkeypatch.setattr(forward, "_verify_declared_m5_cell_account_transport_with_inputs", cold)
    assert forward.verify_declared_m5_cell_account_transport(object(), body,
        process=object(), frozen_pipeline_config=object()) is body
    assert len(calls) == 1 and "_inputs" not in calls[0] and "_precommit" not in calls[0]
