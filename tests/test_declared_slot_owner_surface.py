"""Fixed synthetic engineering controls confer no market or profit authority."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import hashlib
import json

import pytest

import pm_authoritative_search_execution as execution
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_historical_search_producer as producer
import pm_production_anchor_plan as anchor_plan
import pm_production_search_planner as planner


from prototype_support import (CELL_ID, LAUNCH, PROTECTED_END, _roster_arguments, _DeclaredClock, _declared_environment)
PROCESS_SHA = "2" * 64
WINNER_SHA = "1" * 64  # Invented mathematical value, never Winner evidence.
SLOT_IDS = (
    "owner-slot-01-20230305T000100Z", "owner-slot-02-20230402T000100Z",
    "owner-slot-03-20230430T000100Z", "owner-slot-04-20230528T000100Z",
    "owner-slot-05-20230625T000100Z", "owner-slot-06-20230723T000100Z",
    "owner-slot-07-20230820T000100Z", "owner-slot-08-20230917T000100Z",
    "owner-slot-09-20231015T000100Z", "owner-slot-10-20231112T000100Z",
    "owner-slot-11-20231210T000100Z", "owner-slot-12-20240107T000100Z",
    "owner-slot-13-20240204T000100Z",
)
SLOT_DATES = (
    "2023-03-05", "2023-04-02", "2023-04-30", "2023-05-28",
    "2023-06-25", "2023-07-23", "2023-08-20", "2023-09-17",
    "2023-10-15", "2023-11-12", "2023-12-10", "2024-01-07",
    "2024-02-04", "2024-03-03",
)




def _canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")


def test_declared_roster_reuses_exact_thirteen_slot_geometry():
    arguments = _roster_arguments()
    roster = anchor_plan.derive_declared_owner_slot_roster(**arguments)
    assert anchor_plan.verify_declared_owner_slot_roster(roster, **arguments) == roster
    assert roster["slot_count"] == 13
    assert roster["slot_calendar_days"] == 28
    assert roster["total_calendar_days"] == 364
    assert roster["start_utc"] == LAUNCH
    assert roster["end_exclusive_utc"] == "2024-03-03T00:01:00Z"
    for index, slot in enumerate(roster["slots"]):
        assert slot == {
            "slot_id": SLOT_IDS[index], "ordinal": index + 1,
            "activation_utc": SLOT_DATES[index] + "T00:01:00Z",
            "end_exclusive_utc": SLOT_DATES[index + 1] + "T00:01:00Z",
            "native_cutoff_utc": SLOT_DATES[index] + "T00:00:00Z",
            "owner_status": "UNMATERIALIZED", "owner": None,
        }
    launch = datetime(2026, 3, 29, 0, 1, tzinfo=timezone.utc) - 40 * timedelta(days=28)
    assert launch == datetime(2023, 3, 5, 0, 1, tzinfo=timezone.utc)
    assert launch + timedelta(days=364) < datetime(2024, 7, 14, 8, 5, tzinfo=timezone.utc)
    for authority in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
        assert roster[authority] is False


@pytest.mark.parametrize("slot,winner,process,expected", [
    (SLOT_IDS[0], WINNER_SHA, PROCESS_SHA,
     "da7b777e6ac60e3fec956ecd3ecaa053d89f2eeabc6f51ba5f1d3f65f5984745"),
    (SLOT_IDS[1], WINNER_SHA, PROCESS_SHA,
     "1a17cfc7bd181aaf8dee3153f6d851d949889b4eb2163a830b7f531bd89d262f"),
    (SLOT_IDS[0], "3" * 64, PROCESS_SHA,
     "3ab96dfc5fc8001be57fde7f3e87ddfc1a3398f96d278cef929f996c17e02471"),
    (SLOT_IDS[0], WINNER_SHA, "4" * 64,
     "23f4009ea96466f5e34e85250badc299d5f99253ad4bfa947820aed4ad877e81"),
])
def test_declared_synthetic_owner_hash_is_exact(slot, winner, process, expected):
    values = {
        "cell_id": CELL_ID, "slot_id": slot,
        "winner_decision_sha256": winner, "process_receipt_sha256": process,
    }
    assert hashlib.sha256(_canonical(values)).hexdigest() == expected
    assert orchestration.declared_slot_owner_id(**values) == expected
    assert orchestration.declared_slot_owner_id(**dict(reversed(tuple(values.items())))) == expected


@pytest.mark.parametrize("actual_receipt", [None, "f" * 64], ids=["missing-v1", "mismatched"])
def test_real_executor_preserves_actual_process_receipt_early_refusal(monkeypatch, actual_receipt):
    """Isolate the real negative gate; the planner stub grants no OWNER proof."""
    request = orchestration._producer_request(
        SimpleNamespace(receipt_sha256="a" * 64),
        {
            "run_id": "declared-synthetic-negative-v1",
            "generation_id": "declared-synthetic-negative-v1",
            "process_precommit_receipt_sha256": "c" * 64,
            "optimizer_producer_release_sha256": adapter.HISTORICAL_PRODUCER_RELEASE_SHA256,
        },
        {"anchor_id": "structural-negative-anchor"},
    )
    original_request_bytes = request._body_json
    body = request.to_dict()
    assert body["schema_version"] == orchestration.PRODUCER_REQUEST_SCHEMA
    assert "search_process_receipt_sha256" not in body
    if actual_receipt is not None:
        body["search_process_receipt_sha256"] = actual_receipt
    capture = object.__new__(adapter.HistoricalSymbolSearchCapture)
    capture.context, capture.sink, capture.request_body = object(), object(), body
    process = SimpleNamespace(receipt_sha256=PROCESS_SHA)
    plan = SimpleNamespace(
        mode="HISTORICAL_ANCHOR", symbol="AUDUSD", timeframe="H1", regime="TREND",
        search_process_receipt_sha256=PROCESS_SHA, horizon_plan=object(),
        attempt_artifacts=lambda: ({"classification": "DECLARED_NEGATIVE_GATE_ONLY"},),
    )
    fitted = SimpleNamespace(source_group_decision=SimpleNamespace(
        to_dict=lambda: {"symbol": "AUDUSD", "timeframe": "H1"},
    ))
    verified = []
    def structural_plan_verification(*args, **kwargs):
        verified.append(True)
        return True
    def forbidden(*args, **kwargs):
        raise AssertionError("Invalid actual receipt reached financial or later capture work")
    monkeypatch.setattr(planner, "verify_production_cell_search_plan", structural_plan_verification)
    monkeypatch.setattr(execution, "_verify_capture_horizon_plan_identity", forbidden)
    monkeypatch.setattr(execution.Backtester, "run", forbidden)
    with pytest.raises(execution.AuthoritativeSearchExecutionError) as caught:
        execution.execute_historical_observed_cell(
            capture=capture, fitted=fitted, regime="TREND", search_process=process,
            cell_plan=plan, config=execution.PipelineConfig(research_mode=True),
        )
    assert caught.value.code == "HISTORICAL_CELL_PLAN_EXECUTION_BINDING_INVALID"
    assert verified == [True]
    assert request._body_json == original_request_bytes






@pytest.fixture
def declared_env(tmp_path, monkeypatch):
    return _declared_environment(tmp_path, monkeypatch)


def _request(env, ordinal=1, attempt="attempt-001"):
    return orchestration.declared_slot_producer_request(
        env.context, precommit=env.precommit, process=env.process,
        cell_id=CELL_ID, slot_id=SLOT_IDS[ordinal - 1],
        attempt_id=attempt, sink=env.sink,
    )


def test_declared_precommit_and_request_bind_original_candidate_config(declared_env):
    env = declared_env
    materialized = env.frozen.materialize(env.context)
    assert materialized.discovery_risk_per_trade_pct == 1.0
    assert materialized.risk_per_trade_pct == 0.25
    body = orchestration.verify_declared_slot_owner_run_precommit(
        env.context, env.precommit, process=env.process,
    )
    assert body["launch_activation_utc"] == LAUNCH
    assert body["protected_end_exclusive_utc"] == PROTECTED_END
    assert body["slot_roster"] == env.roster
    assert body["slot_roster_sha256"] == hashlib.sha256(_canonical(env.roster)).hexdigest()
    assert body["config_sha256"] == env.context.config_sha256
    assert body["effective_config_sha256"] == env.frozen.effective_config_sha256
    assert body["search_process_receipt_sha256"] == env.process.receipt_sha256
    assert body["spread_profile_identity"] == {"profile_sha256": "e" * 64}
    request = _request(env)
    original = request._body_json
    observed = orchestration.verify_declared_slot_producer_request(
        env.context, request, precommit=env.precommit, process=env.process,
    )
    assert request._body_json == original
    assert observed["search_process_receipt_sha256"] == env.process.receipt_sha256
    assert observed["process_precommit_receipt_sha256"] == env.precommit.receipt_sha256
    assert observed["anchor"]["selection_cutoff_utc"] == "2023-03-05T00:00:00Z"
    assert observed["anchor"]["forward_start_utc"] == LAUNCH
    assert observed["artifact_namespace"].endswith(SLOT_IDS[0] + "/attempt-001")
    assert request.symbol_sink("AUDUSD") is env.sink
    assert observed["confers_causal_or_current_authority"] is False
    for authority in ("qualification_authority", "capital_authority", "live_authority", "order_authority"):
        assert body[authority] is False


def test_declared_actual_cell_plan_seal_binds_issued_request_receipt(declared_env, monkeypatch):
    """Real receipt sealing on declared metadata, not genuine fitted evidence."""
    env = declared_env
    request = _request(env)
    marker = {"classification": "STRUCTURAL_PLAN_METADATA_NOT_OWNER_EVIDENCE"}
    monkeypatch.setattr(orchestration, "historical_horizon_plan_binding", lambda plan: dict(marker))
    monkeypatch.setattr(
        planner, "_verify_state_error_envelope_fitted_binding",
        lambda envelope, **kwargs: None,
    )
    fitted = SimpleNamespace(
        product=SimpleNamespace(
            index_sha256="3" * 64,
            native_prefix={
                "symbol": "AUDUSD", "timeframe": "H1",
                "prefix_content_sha256": "4" * 64,
                "prefix_start_utc": "2020-01-01T00:00:00Z",
                "prefix_end_exclusive_utc": "2023-03-05T00:00:00Z",
                "prefix_last_close_utc": "2023-03-05T00:00:00Z", "prefix_rows": 100,
            },
        ),
        source_group_decision=SimpleNamespace(receipt_sha256="5" * 64),
        regime_fit_decision=SimpleNamespace(receipt_sha256="6" * 64),
        train_regime_support={"receipt_sha256": "7" * 64},
    )
    # Preserve real roster cardinalities; these are unique declared metadata,
    # without optimizer execution or an authenticated strategy/Winner.
    attempts = [{**marker, "ordinal": i} for i in range(planner.ATTEMPT_COUNT)]
    requirements = [{"requirement_sha256": hashlib.sha256(f"requirement-{i}".encode("ascii")).hexdigest()}
                    for i in range(planner.ATTEMPT_COUNT)]
    domains = [{"record_sha256": hashlib.sha256(f"domain-{i}".encode("ascii")).hexdigest()}
               for i in range(planner.BASELINE_ATTEMPTS)]
    encoded, entry_encoded = planner._seal_cell_plan(
        env.context, sink=env.sink, mode="DECLARED_SLOT_UNISSUED",
        namespace=request.to_dict()["artifact_namespace"], process=env.process,
        causal_precommit_receipt_sha256=env.precommit.receipt_sha256,
        fitted=fitted, regime="TREND", horizon_plan=object(), candidate=marker,
        dependency_payload=marker, dependency_geometry=marker,
        attempt_artifacts=attempts, state_requirements=requirements,
        state_domain_records=domains, state_context_selection=marker,
        state_error_envelope=marker, boundary_limited=False,
        declared_slot_request=request,
    )
    body, entry = json.loads(encoded), json.loads(entry_encoded)
    assert body["mode"] == "DECLARED_SLOT_UNISSUED"
    assert body["declared_slot_request_sha256"] == request.receipt_sha256
    assert body["search_process_receipt_sha256"] == env.process.receipt_sha256
    assert body["causal_process_precommit_receipt_sha256"] == env.precommit.receipt_sha256
    assert body["artifact_namespace"] == request.to_dict()["artifact_namespace"]
    assert body["attempt_count"] == planner.ATTEMPT_COUNT
    assert len(body["state_domain_ceiling_records"]) == planner.BASELINE_ATTEMPTS
    assert body["confers_winner_causal_current_capital_publication_or_live_authority"] is False
    declared_receipt_sha = body.pop("receipt_sha256")
    assert declared_receipt_sha == hashlib.sha256(_canonical(body)).hexdigest()
    assert entry["descriptor"]["sha256"] == hashlib.sha256(encoded).hexdigest()
    assert (env.context.root / entry["descriptor"]["relative_path"]).read_bytes() == encoded


@pytest.mark.parametrize("field", [
    "effective_config_sha256", "search_process_receipt_sha256",
    "spread_profile_identity", "source_access_window_utc", "extra_field",
])
def test_declared_precommit_copied_seal_cannot_authenticate_changed_bytes(declared_env, field):
    env = declared_env
    body = env.precommit.to_dict()
    if field == "spread_profile_identity":
        body[field] = {"profile_sha256": "f" * 64}
    elif field == "source_access_window_utc":
        body[field] = {"start_utc": "2026-10-22T10:00:00Z", "end_exclusive_utc": "2026-10-22T14:00:00Z"}
    else:
        body[field] = "f" * 64
    forged = replace(env.precommit, _body_json=_canonical(body))
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.verify_declared_slot_owner_run_precommit(
            env.context, forged, process=env.process,
        )
    assert str(caught.value) == "DECLARED_SLOT_PRECOMMIT_CAPABILITY_INVALID"


def _structural_readonly_profile_metadata(monkeypatch):
    """Declared profile identity only; no real profile or OWNER assertion."""
    calls = []
    def declared_profile(path):
        calls.append(path)
        return SimpleNamespace(
            sha256="e" * 64,
            classification="STRUCTURAL_PROFILE_IDENTITY_NOT_OWNER_EVIDENCE",
        )
    monkeypatch.setattr(execution.spread_profile, "load_spread_profile", declared_profile)
    return calls


def test_declared_precommit_reopens_saved_bytes_through_legitimate_metadata_issuer(declared_env, monkeypatch):
    env = declared_env
    _structural_readonly_profile_metadata(monkeypatch)
    path = env.context.root / f"declared-slots/{env.precommit.receipt_sha256}/precommit.json"
    before = path.read_bytes()
    restored = orchestration.reopen_declared_slot_owner_run_precommit(
        env.context, receipt_sha256=env.precommit.receipt_sha256,
        process=env.process, frozen_pipeline_config=env.frozen,
    )
    assert restored is not env.precommit
    assert restored.receipt_sha256 == env.precommit.receipt_sha256
    assert restored._body_json == env.precommit._body_json == before
    assert orchestration.verify_declared_slot_owner_run_precommit(
        env.context, restored, process=env.process,
    ) == env.precommit.to_dict()
    assert path.read_bytes() == before
    monkeypatch.setattr(
        execution.spread_profile, "load_spread_profile",
        lambda profile_path: SimpleNamespace(
            sha256="f" * 64,
            classification="STRUCTURAL_PROFILE_IDENTITY_NOT_OWNER_EVIDENCE",
        ),
    )
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.reopen_declared_slot_owner_run_precommit(
            env.context, receipt_sha256=env.precommit.receipt_sha256,
            process=env.process, frozen_pipeline_config=env.frozen,
        )
    assert str(caught.value) == "DECLARED_SLOT_PROFILE_BINDING_MISMATCH"
    assert path.read_bytes() == before


@pytest.mark.parametrize("mutation", ["copied-object", "coordinated-attempt-namespace", "extra-field"])
def test_declared_request_requires_issued_live_object_not_copied_seal(declared_env, mutation):
    env = declared_env
    request = _request(env)
    body = request.to_dict()
    if mutation == "coordinated-attempt-namespace":
        body["attempt_id"] = "unissued-attempt"
        body["artifact_namespace"] = body["artifact_namespace"].rsplit("/", 1)[0] + "/unissued-attempt"
    elif mutation == "extra-field":
        body["extra_field"] = "not-issued"
    forged = replace(request, _body_json=_canonical(body))
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.verify_declared_slot_producer_request(
            env.context, forged, precommit=env.precommit, process=env.process,
        )
    assert str(caught.value) == "DECLARED_SLOT_REQUEST_CAPABILITY_INVALID"
    assert orchestration.verify_declared_slot_producer_request(
        env.context, request, precommit=env.precommit, process=env.process,
    ) == request.to_dict()


@pytest.mark.parametrize("field,bad,code", [
    ("spread_profile_identity", {}, "DECLARED_SLOT_PROFILE_INVALID"),
    ("spread_profile_identity", {"profile_sha256": "not-a-hash"}, "DECLARED_SLOT_PROFILE_INVALID"),
    ("cell_roster", [], "DECLARED_SLOT_CELL_INVALID"),
    ("source_access_window_utc", {"start_utc": "2026-10-22T11:00:00+02:00", "end_exclusive_utc": "2026-10-22T13:00:00Z"}, "DECLARED_SLOT_SOURCE_WINDOW_INVALID"),
    ("source_access_window_utc", {"start_utc": 0, "end_exclusive_utc": "2026-10-22T13:00:00Z"}, "DECLARED_SLOT_SOURCE_WINDOW_INVALID"),
    ("source_access_window_utc", {"start_utc": "2026-10-22T10:00:00Z", "end_exclusive_utc": "2026-10-22T12:00:00Z"}, "DECLARED_SLOT_SOURCE_WINDOW_INVALID"),
])
def test_declared_precommit_rejects_invalid_inputs_before_source_access(declared_env, field, bad, code):
    env = declared_env
    arguments = dict(env.arguments)
    arguments[field] = bad
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.precommit_declared_slot_owner_run(env.context, **arguments)
    assert str(caught.value) == code


def test_declared_precommit_rejects_changed_roster_and_config_refs(declared_env):
    env = declared_env
    bad = json.loads(json.dumps(env.roster))
    bad["slots"][0]["end_exclusive_utc"] = "2023-04-02T00:02:00Z"
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.precommit_declared_slot_owner_run(
            env.context, **{**env.arguments, "slot_roster": bad},
        )
    assert str(caught.value) == "DECLARED_SLOT_ROSTER_INVALID"
    wrong_refs = anchor_plan.derive_declared_owner_slot_roster(**_roster_arguments())
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.precommit_declared_slot_owner_run(
            env.context, **{**env.arguments, "slot_roster": wrong_refs},
        )
    assert str(caught.value) == "DECLARED_SLOT_CONFIG_BINDING_MISMATCH"


def test_declared_precommit_requires_independent_launch_input(declared_env):
    arguments = dict(declared_env.arguments)
    arguments.pop("launch_activation_utc")
    with pytest.raises(TypeError, match="launch_activation_utc"):
        orchestration.precommit_declared_slot_owner_run(declared_env.context, **arguments)


@pytest.mark.parametrize("change", ["launch", "protected-end"])
def test_declared_precommit_refuses_self_consistent_roster_outside_independent_bindings(
    declared_env, change,
):
    env = declared_env
    roster_arguments = _roster_arguments(env.context.config_sha256, env.frozen.effective_config_sha256)
    if change == "launch":
        roster_arguments["launch_activation_utc"] = "2023-04-02T00:01:00Z"
    else:
        roster_arguments["protected_end_exclusive_utc"] = "2024-08-14T08:05:00Z"
    roster = anchor_plan.derive_declared_owner_slot_roster(**roster_arguments)
    assert anchor_plan.verify_declared_owner_slot_roster(roster, **roster_arguments) == roster
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.precommit_declared_slot_owner_run(
            env.context, **{**env.arguments, "slot_roster": roster},
        )
    assert str(caught.value) == "DECLARED_SLOT_ROSTER_INVALID"


@pytest.mark.parametrize("change,code", [
    ("roster-launch", "DECLARED_SLOT_ROSTER_INVALID"),
    ("protected-end", "DECLARED_SLOT_PROTECTED_END_INVALID"),
], ids=["roster-launch", "protected-end"])
def test_declared_precommit_saved_independent_bindings_are_rechecked(declared_env, change, code):
    env = declared_env
    body = env.precommit.to_dict()
    if change == "roster-launch":
        arguments = _roster_arguments(env.context.config_sha256, env.frozen.effective_config_sha256)
        arguments["launch_activation_utc"] = "2023-04-02T00:01:00Z"
        body["slot_roster"] = anchor_plan.derive_declared_owner_slot_roster(**arguments)
        body["slot_roster_sha256"] = hashlib.sha256(_canonical(body["slot_roster"])).hexdigest()
    else:
        body["protected_end_exclusive_utc"] = "2024-08-14T08:05:00Z"
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration._validate_declared_precommit_body(env.context, body)
    assert str(caught.value) == code


def test_declared_request_release_removes_live_capability_without_revival(declared_env):
    env = declared_env
    request = _request(env)
    saved = request._body_json
    orchestration._verify_declared_request_identity(request, env.context, require_live=True)
    orchestration._release_declared_slot_producer_request(request, env.context)
    orchestration._release_declared_slot_producer_request(request, env.context)
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.verify_declared_slot_producer_request(
            env.context, request, precommit=env.precommit, process=env.process,
        )
    assert str(caught.value) == "DECLARED_SLOT_REQUEST_CAPABILITY_INVALID"
    assert request._body_json == saved


def test_declared_book_canonical_detector_utc_is_preserved():
    canonical_book_value = "2023-03-05T00:00:00+00:00"
    observed = adapter._declared_slot_detector_cutoff(canonical_book_value)
    assert observed.isoformat() == canonical_book_value
    assert observed == datetime(2023, 3, 5, tzinfo=timezone.utc)
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError):
        orchestration._utc(canonical_book_value, "OLD_Z_ONLY_CONTRACT")


@pytest.mark.parametrize("value", [
    "2023-03-05T00:00:00Z", "2023-03-05T00:00:00",
    "2023-03-05T01:00:00+01:00", "2023-03-05 00:00:00+00:00",
], ids=["z-only", "naive", "nonzero-offset", "space-separator"])
def test_declared_detector_refuses_noncanonical_or_nonzero_offset_utc(value):
    with pytest.raises(adapter.HistoricalProductionAdapterError) as caught:
        adapter._declared_slot_detector_cutoff(value)
    assert str(caught.value) == "DECLARED_SLOT_DETECTOR_INVALID"


def test_declared_full_canonical_owner_slot_commits_and_cold_reopens(tmp_path):
    """No target stubs: real canonical process, source, planner, raw/Winner and commit."""
    from audit_reports.fxpm_workshop_20261007.declared_slot_owner_surface_verification.real_control import run_first_slot
    work = run_first_slot(tmp_path / "fixed-full300-control")
    actual = json.loads((work / "actual-first-slot-result.json").read_text(encoding="utf-8"))
    cold = json.loads((work / "cold-reopen-result.json").read_text(encoding="utf-8"))
    assert actual["status"] == "OWNER" and actual["planned_attempt_count"] == 300
    assert actual["max_json_nodes"] == 2_000_000 and actual["max_json_bytes"] == 64 * 1024**2
    assert actual["max_manifest_bytes"] == 128 * 1024**2
    assert actual["commit_json_nodes"] <= actual["max_json_nodes"] and actual["commit_bytes"] <= actual["max_json_bytes"]
    assert actual["slot_roster_json_nodes"] <= actual["max_json_nodes"]
    assert cold["all_commits_reverified_from_disk"] is True
    assert cold["request_restore_mode"] == "live=False" and cold["compact_commit_roster_verified"] is True
    assert [row["status"] for row in cold["commits"]] == ["OWNER"]


def test_declared_full_canonical_postfreeze_stop_commits_and_cold_reopens(tmp_path):
    """One labelled fault; real source, canonical freeze, commit and cold verifier."""
    import os
    from audit_reports.fxpm_workshop_20261007.declared_slot_owner_surface_verification.real_control import (
        POSTFREEZE_FAULT_CODE, run_first_slot,
    )
    work = run_first_slot(tmp_path / "fixed-full300-postfreeze-stop",
        controlled_postfreeze_fault=True)
    actual = json.loads((work / "actual-first-slot-result.json").read_text(encoding="utf-8"))
    completion = json.loads((work / "actual-first-slot-completion.json").read_text(encoding="utf-8"))
    observed = json.loads((work / "postfreeze-fault-observation.json").read_text(encoding="utf-8"))
    cold = json.loads((work / "cold-reopen-result.json").read_text(encoding="utf-8"))
    assert (actual["status"], actual["reason_code"]) == ("STOP", POSTFREEZE_FAULT_CODE)
    assert actual["planned_attempt_count"] == observed["canonical_plan_attempt_count"] == 300
    assert actual["committed_roster_matches_slot_sink"] is True
    assert observed["injection_count"] == 1 and observed["triggered_after_freeze"] is True
    assert observed["tier_partition_calls"] == 0
    assert observed["slot_sink_artifact_ids"] == observed["selected_commit_roster_artifact_ids"]
    assert all(completion.get(name) is not None for name in (
        "frozen_plan_receipt", "frozen_descriptor_graph", "horizon_winner_cross_binding"))
    assert all(completion.get(name) is None for name in (
        "owner", "winner_request_entry", "winner_decision_entry", "raw_production_receipt",
        "production_descriptor_graph", "winner_request_sha256", "winner_decision_sha256"))
    assert cold["pid"] != os.getpid() and cold["terminal_stop"] is True
    assert cold["all_commits_reverified_from_disk"] is True
    assert cold["request_restore_mode"] == "live=False" and cold["compact_commit_roster_verified"] is True
    assert [row["status"] for row in cold["commits"]] == ["STOP"]
    assert all(completion[name] is False and cold[name] is False for name in (
        "qualification_authority", "capital_authority", "live_authority", "order_authority"))


@pytest.mark.parametrize("capture_type,expected", [
    (adapter.HistoricalSymbolSearchCapture, "old-anchor/adapter/H1/TREND"),
    (adapter.BoundedRawWinnerCanaryCapture, "old-anchor/adapter/H1/TREND"),
    (adapter.DeclaredSlotSearchCapture, "declared-slot/owned/attempt/adapter/H1/TREND"),
])
def test_declared_adapter_prefix_hook_preserves_anchor_and_canary_ids(capture_type, expected):
    """Pure path policy on labelled structural objects; no issued capture."""
    capture = object.__new__(capture_type)
    capture.anchor = {"anchor_id": "old-anchor"}
    capture.raw_artifact_namespace = "declared-slot/owned/attempt"
    assert capture._adapter_cell_prefix("H1", "TREND") == expected


@pytest.fixture(scope="module")
def retained_declared_green_stop():
    """Deferred only: authenticate the actual GREEN witness named by root's protocol."""
    from pathlib import Path
    import pm_authority_spool as spool
    import pm_production_config_binding as binding
    from audit_reports.fxpm_workshop_20261007.declared_slot_owner_surface_verification import real_control

    _, protocol = real_control._protocol(controlled_postfreeze_fault=True)
    retained = protocol["retained_postfreeze_stop_capsule"]
    capsule_path = Path(retained["path"]).resolve(strict=True)
    assert capsule_path.is_relative_to((real_control.QA / "results").resolve())
    assert real_control.identity(capsule_path) == retained["identity"]
    capsule = json.loads(capsule_path.read_text(encoding="utf-8"))
    assert capsule["classification"] == real_control.CLASSIFICATION
    assert capsule["expected_statuses"] == ["STOP"] and capsule["terminal_stop"] is True
    context = spool.reopen_generation_context(Path(capsule["generation_root"]),
        expected_generation_id=capsule["generation_id"],
        expected_native_universe_sha256=capsule["native_universe_sha256"],
        expected_config_sha256=capsule["config_sha256"])
    assert len(capsule["commit_files"]) == 1
    relative = next(iter(capsule["commit_files"]))
    assert real_control.identity(context.root / relative) == capsule["commit_files"][relative]
    committed, digest, descriptor = producer._read_declared_commit(context, relative)
    assert descriptor == capsule["commit_descriptors"][0] and digest == descriptor["sha256"]
    assert committed["completion"]["reason_code"] == real_control.POSTFREEZE_FAULT_CODE
    config_path = Path(capsule["config_path"])
    assert real_control.identity(config_path) == capsule["config_file_identity"]
    payload, effective = binding.load_effective_pipeline_config(config_path)
    frozen = binding.freeze_production_pipeline_config(context, active_config_path=config_path,
        config_payload=payload, supplied_config=effective)
    assert frozen.effective_config_sha256 == capsule["effective_config_sha256"]
    source_entry = committed["completion"]["source_session_entry"]
    assert source_entry in committed["artifact_roster"]
    source_session = adapter._reopen_ascii_json_entry(context, source_entry,
        expected_type="declared_slot_source_session", code="DECLARED_SLOT_SOURCE_INVALID")
    product = adapter._reopen_declared_slot_native_source(context,
        body=committed["completion"]["request_body"], config=frozen.materialize(context),
        source_session=source_session)
    return SimpleNamespace(capsule=capsule, context=context, committed=committed, frozen=frozen,
        native_artifact_descriptors=tuple(dict(item) for item in product.artifact_descriptors))


@pytest.fixture(scope="module")
def retained_declared_write_inputs(retained_declared_green_stop):
    """Restore actual issued v1 metadata as cold capabilities, without live grants."""
    import pm_authority_spool as spool
    retained = retained_declared_green_stop
    capsule, context = retained.capsule, retained.context
    process = planner.reopen_production_search_process(
        context, spool_receipt=capsule["process_spool_receipt"],
        expected_receipt_sha256=capsule["process_receipt_sha256"],
    )
    assert process.to_dict()["schema_version"] == planner.ORIGINAL_PROCESS_SCHEMA
    precommit = orchestration.reopen_declared_slot_owner_run_precommit(
        context, receipt_sha256=capsule["precommit_sha256"],
        process=process, frozen_pipeline_config=retained.frozen,
    )
    body = retained.committed["completion"]["request_body"]
    encoded = orchestration._declared_json(body)
    orchestration._verify_declared_issued_bytes(context, orchestration._declared_request_path(body), encoded)
    sink = spool.open_symbol_sink(context, body["symbol"])
    request = orchestration.DeclaredSlotProducerRequest(
        encoded, precommit, sink, orchestration._DECLARED_SLOT_SEAL,
    )
    orchestration._register_declared_request(request, context, live=False)
    assert orchestration.verify_declared_slot_producer_request(
        context, request, precommit=precommit, process=process,
    ) == body
    yield SimpleNamespace(context=context, process=process, precommit=precommit, request=request,
        sink=sink, frozen=retained.frozen, cell_id=capsule["cell_id"],
        slot=precommit.to_dict()["slot_roster"]["slots"][0],
        completion=retained.committed["completion"], roster=retained.committed["artifact_roster"])
    orchestration._release_declared_slot_producer_request(request, context)


@pytest.fixture(scope="module")
def retained_declared_mixed_write_inputs(retained_declared_write_inputs, tmp_path_factory):
    """Synthetic cross-edition pairing with actual issued bytes and identical identities."""
    import pm_authority_spool as spool
    original = retained_declared_write_inputs
    work = tmp_path_factory.mktemp("synthetic-current-v1-binding")
    context = spool.freeze_generation_context(
        work / "spool", generation_id=original.context.generation_id,
        native_universe_sha256=original.context.native_universe_sha256,
        config_sha256=original.context.config_sha256, limits=original.context.limits,
    )
    assert context.context_sha256 == original.context.context_sha256
    process = planner.freeze_production_search_process(context, run_id="synthetic-current-v1-binding")
    assert planner.verify_frozen_production_search_process(context, process) is True
    precommit_body = original.precommit.to_dict()
    spool._publish_exact_json(context,
        f"declared-slots/{original.precommit.receipt_sha256}/precommit.json",
        precommit_body, context.limits.max_json_bytes)
    precommit = orchestration.reopen_declared_slot_owner_run_precommit(
        context, receipt_sha256=original.precommit.receipt_sha256,
        process=None, frozen_pipeline_config=original.frozen,
    )
    assert precommit._body_json == original.precommit._body_json
    assert orchestration.verify_declared_slot_owner_run_precommit(context, precommit) == precommit_body
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.verify_declared_slot_owner_run_precommit(context, precommit, process=process)
    assert str(caught.value) == "DECLARED_SLOT_PROCESS_BINDING_MISMATCH"
    body = original.request.to_dict()
    spool._publish_exact_json(context, orchestration._declared_request_path(body),
        body, context.limits.max_json_bytes)
    encoded = orchestration._declared_json(body)
    orchestration._verify_declared_issued_bytes(context, orchestration._declared_request_path(body), encoded)
    sink = spool.open_symbol_sink(context, body["symbol"])
    request = orchestration.DeclaredSlotProducerRequest(encoded, precommit, sink, orchestration._DECLARED_SLOT_SEAL)
    orchestration._register_declared_request(request, context, live=False)
    assert orchestration.verify_declared_slot_producer_request(context, request, precommit=precommit) == body
    (work / "assurance-provenance.json").write_bytes(_canonical({
        "classification": "SYNTHETIC_CROSS_EDITION_BINDING_NEGATIVE_NO_TRADING_AUTHORITY",
        "original_issued_bytes_modified": False, "new_fit_or_evaluation": False,
        "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False,
    }))
    yield SimpleNamespace(context=context, process=process, precommit=precommit, request=request,
        sink=sink, frozen=original.frozen, cell_id=original.cell_id, slot=original.slot,
        completion=original.completion, roster=original.roster)
    orchestration._release_declared_slot_producer_request(request, context)


def _forbid_declared_write_effects(monkeypatch):
    import pm_authority_spool as spool
    calls = []
    def forbidden(*args, **kwargs):
        calls.append(True)
        raise AssertionError("retained edition reached a write, lease or write-capable verifier")
    for name in ("_publish_exact_json", "write_canonical_json_artifact", "open_symbol_sink",
                 "reserve_transient_publication", "_ensure_directory"):
        monkeypatch.setattr(spool, name, forbidden)
    for name in ("put_json", "put_opaque", "seal"):
        monkeypatch.setattr(spool.SymbolAuthoritySink, name, forbidden)
    monkeypatch.setattr(adapter, "verify_declared_slot_completion", forbidden)
    return calls


@pytest.mark.parametrize("operation", ["request", "source-execute", "driver", "finalize", "commit", "terminal"])
@pytest.mark.parametrize("pair", ["original-v1", "mixed-current-with-v1"])
def test_declared_original_and_mixed_pairs_refuse_before_write(
    retained_declared_write_inputs, retained_declared_mixed_write_inputs, monkeypatch, operation, pair,
):
    env = retained_declared_write_inputs if pair == "original-v1" else retained_declared_mixed_write_inputs
    process = env.process
    calls = _forbid_declared_write_effects(monkeypatch)
    operations = {
        "request": lambda: orchestration.declared_slot_producer_request(env.context,
            precommit=env.precommit, process=process, cell_id=env.cell_id,
            slot_id=env.slot["slot_id"], attempt_id="forbidden-original-reissue", sink=env.sink),
        "source-execute": lambda: producer.execute_declared_slot_owner_search(env.context,
            request=env.request, process=process, frozen_pipeline_config=env.frozen),
        "driver": lambda: producer.execute_declared_cell_owner_slots(context=env.context,
            precommit=env.precommit, process=process, cell_id=env.cell_id, frozen_pipeline_config=env.frozen),
        "finalize": lambda: producer._execute_declared_cell_owner_slots_unlocked(context=env.context,
            precommit=env.precommit, process=process, cell_id=env.cell_id, frozen_pipeline_config=env.frozen),
        "commit": lambda: producer._commit_declared_slot_completion(env.context,
            precommit=env.precommit, process=process, cell_id=env.cell_id, slot=env.slot,
            completion=env.completion, artifact_roster=env.roster, previous_commit_sha256=None,
            frozen_pipeline_config=env.frozen),
        "terminal": lambda: producer._commit_declared_terminal_failure(env.context,
            precommit=env.precommit, process=process, cell_id=env.cell_id, completed=[],
            last_commit_sha256=None, failure={}),
    }
    error_type = (planner.ProductionSearchPlannerError if pair == "original-v1"
                  else orchestration.HistoricalAnchorOrchestrationError)
    with pytest.raises(error_type) as caught:
        operations[operation]()
    assert str(caught.value) == (
        "RETAINED_PROCESS_IS_READ_ONLY" if pair == "original-v1" else "DECLARED_SLOT_PROCESS_BINDING_MISMATCH"
    )
    assert calls == []


def _clone_declared_green_stop(tmp_path, retained):
    """Copy only issued generation closure; reuse unchanged fixed native inputs read-only."""
    from pathlib import Path
    import shutil
    import pm_authority_spool as spool
    import pm_production_config_binding as binding
    from audit_reports.fxpm_workshop_20261007.declared_slot_owner_surface_verification import real_control

    capsule = retained.capsule
    request_body = retained.committed["completion"]["request_body"]
    request_bytes = orchestration._declared_json(request_body)
    assert hashlib.sha256(request_bytes).hexdigest() == retained.committed["completion"]["request_sha256"]
    request_relative = orchestration._declared_request_path(request_body)
    orchestration._verify_declared_issued_bytes(retained.context, request_relative, request_bytes)
    work = tmp_path / "compositional-green-stop"
    work.mkdir()
    context = spool.freeze_generation_context(work / "spool", generation_id=capsule["generation_id"],
        native_universe_sha256=capsule["native_universe_sha256"], config_sha256=capsule["config_sha256"],
        limits=retained.context.limits)
    relative_paths = set(capsule["issued_files"]) - set(capsule["commit_files"])
    relative_paths.difference_update(item["relative_path"] for item in capsule["commit_descriptors"])
    relative_paths.update(entry["descriptor"]["relative_path"] for entry in retained.committed["artifact_roster"])
    relative_paths.update(item["relative_path"] for item in retained.native_artifact_descriptors)
    relative_paths.add(request_relative)
    for relative in sorted(relative_paths):
        origin, target = retained.context.root / relative, context.root / relative
        assert origin.resolve(strict=True).is_relative_to(retained.context.root) and not origin.is_symlink()
        assert target.resolve().is_relative_to(context.root)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(origin, target)
    orchestration._verify_declared_issued_bytes(context, request_relative, request_bytes)
    config_path = work / "fixed-synthetic-config.json"
    original_config = Path(capsule["config_path"])
    assert real_control.identity(original_config) == capsule["config_file_identity"]
    shutil.copyfile(original_config, config_path)
    payload, effective = binding.load_effective_pipeline_config(config_path)
    frozen = binding.freeze_production_pipeline_config(context, active_config_path=config_path,
        config_payload=payload, supplied_config=effective)
    process = planner.reopen_production_search_process(context,
        spool_receipt=capsule["process_spool_receipt"],
        expected_receipt_sha256=capsule["process_receipt_sha256"])
    assert process.receipt_sha256 == capsule["process_receipt_sha256"]
    precommit = orchestration.reopen_declared_slot_owner_run_precommit(context,
        receipt_sha256=capsule["precommit_sha256"], process=process, frozen_pipeline_config=frozen)
    completion = json.loads(json.dumps(retained.committed["completion"]))
    roster = json.loads(json.dumps(retained.committed["artifact_roster"]))
    sink = spool.open_symbol_sink(context, completion["request_body"]["symbol"])
    for entry in roster:
        sink.register_artifact(entry["artifact_id"], entry["artifact_type"], entry["descriptor"])
    real_control.write_json(work / "assurance-provenance.json", {
        "classification": "COMPOSITIONAL_SYNTHETIC_STOP_ASSURANCE_NOT_FULL300_PRODUCTION",
        "original_commit_sha256": capsule["commit_descriptors"][0]["sha256"],
        "source_and_profile_modified": False, "new_fit_or_evaluation": False,
        "qualification_authority": False, "capital_authority": False, "live_authority": False})
    return SimpleNamespace(work=work, context=context, config_path=config_path, frozen=frozen,
        process=process, precommit=precommit, completion=completion, roster=roster, sink=sink,
        cell_id=capsule["cell_id"], slot=precommit.to_dict()["slot_roster"]["slots"][0])


def _verify_cloned_declared_stop(env):
    return adapter.verify_declared_slot_completion(env.context, precommit=env.precommit, process=env.process,
        completion=env.completion, artifact_roster=env.roster, frozen_pipeline_config=env.frozen)


def _replace_declared_json_entry(env, entry, value):
    import pm_authority_spool as spool
    descriptor = (spool.write_canonical_json_artifact(env.context, value)
        if entry["descriptor"]["format"] == spool.CANONICAL_JSON_FORMAT else
        spool.write_opaque_artifact(env.context, (_canonical(value),), format=entry["descriptor"]["format"]))
    replacement = {**entry, "descriptor": descriptor}
    env.roster = [replacement if item["artifact_id"] == entry["artifact_id"] else item for item in env.roster]
    return replacement


def _add_compositional_production_trace(env):
    """Byte-only invented leftovers; these are not semantically valid raw production."""
    import pm_winner_raw_evidence as raw
    body = env.completion["request_body"]
    namespace = body["artifact_namespace"]
    prefix = f"{namespace}/winner-search/{body['timeframe']}/{body['regime']}"
    diagnostic = {"classification": "INVENTED_COMPOSITIONAL_AFTER_PRODUCTION_STOP_TRACE",
        "qualification_authority": False, "capital_authority": False, "live_authority": False}
    source_sha = hashlib.sha256(_canonical(diagnostic)).hexdigest()
    paths = {
        "attempt": (f"{prefix}/attempt-0000/fold-0000/attempt-replay-receipt", "winner_attempt_replay_receipt"),
        "source": (f"{prefix}/source-observations/{source_sha}", "winner_source_observation_receipt"),
        "request": (f"{prefix}/authority/winner-request", "winner_request"),
        "decision": (f"{prefix}/authority/winner-decision", "winner_decision"),
        "production": (f"{prefix}/authority/production-receipt", "winner_production_receipt"),
        "graph": (f"{namespace}/adapter/{body['timeframe']}/{body['regime']}/production-descriptor-graph",
            "historical_raw_winner_descriptor_graph"),
    }
    added = {name: env.sink.put_opaque(artifact_id, artifact_type,
        (_canonical(diagnostic),), format=raw.ASCII_JSON_FORMAT)
        for name, (artifact_id, artifact_type) in paths.items()}
    env.roster.extend(added.values())
    return added


@pytest.mark.parametrize("field", ["winner_decision_entry", "raw_production_receipt",
    "production_descriptor_graph", "winner_request_sha256", "winner_decision_sha256"])
def test_declared_green_stop_refuses_production_claims(tmp_path, retained_declared_green_stop, field):
    env = _clone_declared_green_stop(tmp_path, retained_declared_green_stop)
    trace = _add_compositional_production_trace(env)
    values = {"winner_decision_entry": trace["decision"],
        "raw_production_receipt": trace["production"]["descriptor"],
        "production_descriptor_graph": trace["graph"]["descriptor"],
        "winner_request_sha256": "f" * 64, "winner_decision_sha256": "f" * 64}
    env.completion[field] = values[field]
    with pytest.raises(adapter.HistoricalProductionAdapterError, match="DECLARED_SLOT_COMPLETION_INVALID"):
        _verify_cloned_declared_stop(env)


@pytest.mark.parametrize("damage", ["moved-graph-missing", "old-namespace-rekey"])
def test_declared_green_stop_requires_declared_graph_roster_identity(
    tmp_path, retained_declared_green_stop, damage,
):
    env = _clone_declared_green_stop(tmp_path, retained_declared_green_stop)
    descriptor = env.completion["frozen_descriptor_graph"]
    graph_entry = next(entry for entry in env.roster if entry["descriptor"] == descriptor)
    if damage == "moved-graph-missing":
        env.roster = [entry for entry in env.roster if entry["artifact_id"] != graph_entry["artifact_id"]]
    else:
        body = env.completion["request_body"]
        old_id = f"{body['slot_id']}/adapter/{body['timeframe']}/{body['regime']}/frozen-descriptor-graph"
        env.roster = [{**entry, "artifact_id": old_id} if entry["artifact_id"] == graph_entry["artifact_id"]
            else entry for entry in env.roster]
    assert env.completion["frozen_descriptor_graph"] == descriptor
    with pytest.raises(adapter.HistoricalProductionAdapterError) as caught:
        _verify_cloned_declared_stop(env)
    assert str(caught.value) == "DECLARED_SLOT_ORIGINAL_ARTIFACT_MISSING"


@pytest.mark.parametrize("damage", ["frozen-leaf-missing", "frozen-graph-request", "frozen-receipt-id",
    "cross-request", "cross-pointer-missing", "failure-attempt-count", "frozen-body"])
def test_declared_green_stop_authenticates_frozen_chain(tmp_path, retained_declared_green_stop, monkeypatch, damage):
    import pm_winner_raw_evidence as raw
    env = _clone_declared_green_stop(tmp_path, retained_declared_green_stop)
    body = env.completion["request_body"]
    prefix = f"{body['artifact_namespace']}/winner-search/{body['timeframe']}/{body['regime']}/frozen/"
    graph_entry = next(entry for entry in env.roster if entry["descriptor"] == env.completion["frozen_descriptor_graph"])
    graph = json.loads((env.context.root / graph_entry["descriptor"]["relative_path"]).read_text(encoding="utf-8"))
    if damage == "frozen-leaf-missing":
        removed = next(entry["artifact_id"] for entry in env.roster
            if entry["artifact_id"].startswith(prefix) and entry["artifact_id"] != prefix + "receipt")
        env.roster = [entry for entry in env.roster if entry["artifact_id"] != removed]
    elif damage == "frozen-graph-request":
        graph["producer_request_sha256"] = "f" * 64
    elif damage == "frozen-receipt-id":
        for collection in (env.roster, graph["artifacts"]):
            for entry in collection:
                if entry["artifact_id"] == prefix + "receipt":
                    entry["artifact_id"] += "-alias"
    elif damage in {"cross-request", "failure-attempt-count"}:
        field = "horizon_winner_cross_binding" if damage == "cross-request" else "technical_failure_entry"
        old_entry = (env.completion[field] if field == "technical_failure_entry" else
            next(entry for entry in env.roster if entry["descriptor"] == env.completion[field]))
        value = json.loads((env.context.root / old_entry["descriptor"]["relative_path"]).read_text(encoding="utf-8"))
        value["producer_request_sha256" if damage == "cross-request" else "attempted_trials_exact"] = (
            "f" * 64 if damage == "cross-request" else 299)
        replacement = _replace_declared_json_entry(env, old_entry, value)
        env.completion[field] = replacement if field == "technical_failure_entry" else replacement["descriptor"]
    elif damage == "cross-pointer-missing":
        env.completion["horizon_winner_cross_binding"] = None
    else:
        old_entry = next(entry for entry in env.roster
            if entry["artifact_id"].startswith(prefix) and entry["artifact_id"] != prefix + "receipt")
        value = json.loads((env.context.root / old_entry["descriptor"]["relative_path"]).read_text(encoding="utf-8"))
        value["invented_assurance_tamper"] = True
        replacement = _replace_declared_json_entry(env, old_entry, value)
        graph["artifacts"] = [replacement if entry["artifact_id"] == old_entry["artifact_id"] else entry
            for entry in graph["artifacts"]]
    if damage in {"frozen-graph-request", "frozen-receipt-id", "frozen-body"}:
        graph["artifacts"].sort(key=lambda entry: entry["artifact_id"])
        graph["artifact_roster_sha256"] = hashlib.sha256(_canonical(graph["artifacts"])).hexdigest()
        graph["artifact_count"] = len(graph["artifacts"])
        graph["artifact_total_bytes"] = sum(entry["descriptor"]["size_bytes"] for entry in graph["artifacts"])
        env.completion["frozen_descriptor_graph"] = _replace_declared_json_entry(env, graph_entry, graph)["descriptor"]
    expected = {
        "frozen-leaf-missing": (adapter.HistoricalProductionAdapterError, "DECLARED_SLOT_RAW_GRAPH_BINDING_MISMATCH"),
        "frozen-graph-request": (adapter.HistoricalProductionAdapterError, "DECLARED_SLOT_RAW_GRAPH_BINDING_MISMATCH"),
        "frozen-receipt-id": (adapter.HistoricalProductionAdapterError, "DECLARED_SLOT_RAW_BINDING_MISMATCH"),
        "cross-request": (adapter.HistoricalProductionAdapterError, "DECLARED_SLOT_RAW_BINDING_MISMATCH"),
        "cross-pointer-missing": (adapter.HistoricalProductionAdapterError, "DECLARED_SLOT_FROZEN_INVALID"),
        "failure-attempt-count": (adapter.HistoricalProductionAdapterError, "DECLARED_SLOT_FAILURE_INVALID"),
        "frozen-body": (raw.WinnerRawEvidenceError, "FROZEN_PLAN_RECEIPT_MISMATCH"),
    }
    observed = {"native_verified": False, "frozen_chain_entered": False}
    original_native = adapter._reopen_declared_slot_native_source
    original_frozen = adapter._verify_declared_slot_frozen_chain

    def verified_native(*args, **kwargs):
        product = original_native(*args, **kwargs)
        observed["native_verified"] = True
        return product

    def entered_frozen(*args, **kwargs):
        observed["frozen_chain_entered"] = True
        return original_frozen(*args, **kwargs)

    monkeypatch.setattr(adapter, "_reopen_declared_slot_native_source", verified_native)
    monkeypatch.setattr(adapter, "_verify_declared_slot_frozen_chain", entered_frozen)
    error_type, error_code = expected[damage]
    with pytest.raises(error_type) as caught:
        _verify_cloned_declared_stop(env)
    assert getattr(caught.value, "code", str(caught.value)) == error_code
    assert observed["native_verified"] is True
    assert observed["frozen_chain_entered"] is (damage != "cross-pointer-missing")


@pytest.mark.parametrize("held_v2", [False, True], ids=["ordinary-stop", "v2-held-stop"])
def test_declared_green_stop_with_compositional_trace_refuses_original_commit(
    tmp_path, retained_declared_green_stop, held_v2, monkeypatch,
):
    env = _clone_declared_green_stop(tmp_path, retained_declared_green_stop)
    _add_compositional_production_trace(env)
    post_failure = ({"exception_type": "OSError", "failure_code": "QA_COMPOSITIONAL_POST_COMPLETION_FAILURE",
        "failure_observed_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
        if held_v2 else None)
    calls = _forbid_declared_write_effects(monkeypatch)
    with pytest.raises(planner.ProductionSearchPlannerError) as caught:
        producer._commit_declared_slot_completion(env.context, precommit=env.precommit,
            process=env.process, cell_id=env.cell_id, slot=env.slot, completion=env.completion,
            artifact_roster=env.roster, previous_commit_sha256=None, frozen_pipeline_config=env.frozen,
            post_completion_failure=post_failure)
    assert caught.value.code == "RETAINED_PROCESS_IS_READ_ONLY"
    assert calls == []


def test_declared_candidate_risk_gate_uses_materialized_original_config(tmp_path, monkeypatch):
    env = _declared_environment(tmp_path, monkeypatch, candidate_risk=0.25, issue=False)
    assert env.frozen.materialize(env.context).discovery_risk_per_trade_pct == 0.25
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.precommit_declared_slot_owner_run(env.context, **env.arguments)
    assert str(caught.value) == "DECLARED_SLOT_CONFIG_INVALID"


def test_declared_precommit_rejects_other_genuine_process_receipt(declared_env, monkeypatch):
    import pm_authority_spool as spool
    env = declared_env
    # A process seals its one immutable PROCESS manifest. Use independent
    # metadata storage with the same identities, never reseal the original.
    other_context = spool.freeze_generation_context(
        env.context.root.parent / "other-genuine-process-spool",
        generation_id=env.context.generation_id,
        native_universe_sha256=env.context.native_universe_sha256,
        config_sha256=env.context.config_sha256,
        limits=env.context.limits,
    )
    assert other_context.context_sha256 == env.context.context_sha256
    other = planner.freeze_production_search_process(other_context, run_id="other-declared-qa-run")
    assert planner.verify_frozen_production_search_process(other_context, other) is True
    assert other.receipt_sha256 != env.process.receipt_sha256
    saved = env.precommit._body_json
    spool._publish_exact_json(
        other_context, f"declared-slots/{env.precommit.receipt_sha256}/precommit.json",
        env.precommit.to_dict(), other_context.limits.max_manifest_bytes,
    )
    _structural_readonly_profile_metadata(monkeypatch)
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.reopen_declared_slot_owner_run_precommit(
            other_context, receipt_sha256=env.precommit.receipt_sha256,
            process=other, frozen_pipeline_config=env.frozen,
        )
    assert str(caught.value) == "DECLARED_SLOT_PROCESS_BINDING_MISMATCH"
    assert env.precommit._body_json == saved
    assert planner.verify_frozen_production_search_process(env.context, env.process) is True


@pytest.mark.parametrize("field,bad", [
    ("cell_id", "not-a-cell"), ("slot_id", ""),
    ("winner_decision_sha256", "not-a-winner-digest"),
    ("process_receipt_sha256", "not-a-process-digest"),
])
def test_declared_owner_hash_rejects_invalid_identity_inputs(field, bad):
    values = {
        "cell_id": CELL_ID, "slot_id": SLOT_IDS[0],
        "winner_decision_sha256": WINNER_SHA, "process_receipt_sha256": PROCESS_SHA,
    }
    values[field] = bad
    with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
        orchestration.declared_slot_owner_id(**values)
    assert str(caught.value) == "DECLARED_SLOT_OWNER_ID_INVALID"


class _TrackedStructuralFitted:
    """Stable metadata refs; forbid materialization, not resident identity reads."""
    def __init__(self):
        self.source_accesses = 0
        self._source_group_decision = SimpleNamespace(to_dict=self._read_source)
        self._regime_fit_decision = SimpleNamespace(to_dict=self._read_fit)

    def _read_source(self):
        self.source_accesses += 1
        pytest.fail("Source evidence was materialized")

    def _read_fit(self):
        pytest.fail("Fit evidence was materialized")

    @property
    def source_group_decision(self):
        return self._source_group_decision

    @property
    def regime_fit_decision(self):
        return self._regime_fit_decision


def _structural_live_capture(env, monkeypatch):
    # The real opener/tag verifier runs; semantic source/plan evidence is
    # explicitly stubbed. This fixture proves no genuine source or OWNER.
    request = _request(env)
    fitted = _TrackedStructuralFitted()
    config = env.frozen.materialize(env.context)
    session = SimpleNamespace(_fitted=fitted, _config=config)
    monkeypatch.setattr(
        producer, "_verified_declared_slot_source_session",
        lambda *args, **kwargs: session,
    )
    monkeypatch.setattr(planner, "verify_production_cell_search_plan", lambda *args, **kwargs: True)
    monkeypatch.setattr(orchestration, "historical_horizon_plan_binding", lambda *args, **kwargs: {})
    monkeypatch.setattr(adapter, "_released_horizon_plan_body", lambda *args, **kwargs: {})
    plan = SimpleNamespace(
        mode="DECLARED_SLOT_UNISSUED", _declared_slot_request=request,
        symbol="AUDUSD", timeframe="H1", regime="TREND",
        receipt_sha256="a" * 64, horizon_plan=object(),
    )
    capture = adapter.open_declared_slot_capture(
        env.context, request=request, process=env.process,
        plan=plan, source_session=session,
    )
    assert adapter._verify_declared_slot_capture(capture) is True
    fitted.source_accesses = 0
    return capture, fitted, config, plan


@pytest.mark.parametrize("route", ["search-failure", "after-production"])
def test_declared_structural_capture_failure_routes_record_terminal_stop(declared_env, monkeypatch, route):
    """Capture lifecycle composition only; no genuine full300 production is represented."""
    import pm_winner_raw_evidence as raw
    env = declared_env
    capture, _, _, _ = _structural_live_capture(env, monkeypatch)
    completion_body_calls = []

    def structural_completion(_capture, *, status, reason_code, handle, completed):
        completion_body_calls.append(completed is not None)
        if completed is not None:
            raise OSError("INVENTED_AFTER_PRODUCTION_COMPLETION_FAULT")
        return {"classification": "STRUCTURAL_CAPTURE_LIFECYCLE_NOT_FULL300_PRODUCTION",
            "status": status, "reason_code": reason_code, "owner": None,
            "winner_request_entry": None, "winner_decision_entry": None,
            "raw_production_receipt": None, "production_descriptor_graph": None,
            "qualification_authority": False, "capital_authority": False, "live_authority": False}

    monkeypatch.setattr(adapter, "_declared_capture_completion_body", structural_completion)
    if route == "search-failure":
        failure = execution._infrastructure_failure(code="QA_STRUCTURAL_SEARCH_FAILURE", details=(),
            symbol="AUDUSD", timeframe="H1", regime="TREND")
        capture.record_search_infrastructure_failure("H1", "TREND", failure=failure, attempted_trials_exact=0)
    else:
        descriptor = capture.sink.put_json(f"{capture.raw_artifact_namespace}/structural-frozen-marker",
            "structural_non_authority_marker", {"classification": "INVENTED_STRUCTURAL_FROZEN_MARKER"})["descriptor"]
        handle = adapter.FrozenHistoricalCell(symbol=capture.symbol, timeframe="H1", regime="TREND",
            frozen=SimpleNamespace(attempt_ledger={"attempt_count": 1}),
            frozen_receipt_descriptor=descriptor, frozen_descriptor_graph=descriptor,
            cross_binding_descriptor=descriptor, artifact_namespace=capture.raw_artifact_namespace,
            _attempt_artifacts=(), _horizon_execution_contracts=(), _source_count_receipts=(),
            _native_prefix_product=None, _cross_binding_bytes=b"", _capture_nonce="structural-only")
        capture._active = handle
        completed = adapter._RawWinnerCellCompletion(production=SimpleNamespace(decision=SimpleNamespace(
            to_dict=lambda: {"winner": None}, status="NO_PROCESS_WINNER")),
            production_receipt_descriptor=descriptor, production_descriptor_graph=descriptor,
            all_process_rejected=False)
        monkeypatch.setattr(adapter.DeclaredSlotSearchCapture, "_complete_raw_winner_cell", lambda *_args, **_kwargs: completed)
        with pytest.raises(OSError, match="INVENTED_AFTER_PRODUCTION_COMPLETION_FAULT"):
            capture.complete_observed_cell(handle, attempt_evidence=(),
                evidence_access_started_at_utc="2026-10-22T12:00:00Z",
                evidence_access_completed_at_utc="2026-10-22T12:00:01Z")
        assert capture._completion is None and capture._finished is False and capture._active is handle
        capture.record_raw_evidence_failure(handle, raw.WinnerRawEvidenceIncomplete("QA_STRUCTURAL_AFTER_PRODUCTION"),
            exception_type="OSError")
    observed = capture.completion
    assert completion_body_calls == ([False] if route == "search-failure" else [True, False])
    assert observed["status"] == "STOP" and capture._finished is True and capture._active is None
    assert observed["technical_failure_entry"]["artifact_id"].startswith(capture.raw_artifact_namespace + "/")
    assert all(observed.get(name) is None for name in (
        "owner", "winner_request_entry", "winner_decision_entry", "raw_production_receipt", "production_descriptor_graph"))
    assert observed["qualification_authority"] is False


@pytest.mark.parametrize("mutation", ["fitted", "config", "capture-copy", "request-body"])
def test_declared_executor_identity_refuses_before_any_source_access(declared_env, monkeypatch, mutation):
    """Structural E2193 identity gate, without a genuine-owner assertion."""
    from copy import copy
    env = declared_env
    capture, fitted, config, plan = _structural_live_capture(env, monkeypatch)
    supplied_fitted, supplied_config = fitted, config
    if mutation == "fitted":
        supplied_fitted = _TrackedStructuralFitted()
    elif mutation == "config":
        supplied_config = execution.PipelineConfig(research_mode=True)
    elif mutation == "capture-copy":
        capture = copy(capture)
    else:
        capture.request_body = {**capture.request_body, "attempt_id": "copied-body"}
    def forbidden(*args, **kwargs):
        raise AssertionError("Declared identity refusal reached downstream or financial work")
    monkeypatch.setattr(execution, "_verify_capture_horizon_plan_identity", forbidden)
    monkeypatch.setattr(execution.Backtester, "run", forbidden)
    error = execution.AuthoritativeSearchExecutionError if mutation in {"fitted", "config"} else adapter.HistoricalProductionAdapterError
    with pytest.raises(error) as caught:
        execution.execute_historical_observed_cell(
            capture=capture, fitted=supplied_fitted, regime="TREND",
            search_process=env.process, cell_plan=plan, config=supplied_config,
        )
    expected = "DECLARED_SLOT_EXECUTION_IDENTITY_SUBSTITUTION" if mutation in {"fitted", "config"} else "DECLARED_SLOT_CAPTURE_BINDING_MISMATCH"
    assert (caught.value.code if isinstance(caught.value, execution.AuthoritativeSearchExecutionError) else str(caught.value)) == expected
    assert fitted.source_accesses == 0
    assert supplied_fitted.source_accesses == 0


def test_declared_late_source_access_refuses_before_native_or_fit_call(declared_env, monkeypatch):
    env = declared_env
    request = _request(env)
    monkeypatch.setattr(_DeclaredClock, "value", datetime(2026, 10, 22, 13, tzinfo=timezone.utc))
    calls = []
    def forbidden(*args, **kwargs):
        calls.append(True)
        raise AssertionError("Expired source window reached native or fit work")
    monkeypatch.setattr(producer.native_prefix, "produce_activated_native_prefix", forbidden)
    monkeypatch.setattr(execution, "fit_native_timeframe_for_search", forbidden)
    completion = producer.execute_declared_slot_owner_search(
        env.context, request=request, process=env.process, frozen_pipeline_config=env.frozen,
    )
    assert (completion["status"], completion["reason_code"]) == ("STOP", "DECLARED_SLOT_SOURCE_FAILURE")
    source = adapter._reopen_ascii_json_entry(env.context, completion["source_session_entry"],
        expected_type="declared_slot_source_session", code="TEST_INVALID_SOURCE")
    assert source["failure_code"] == "DECLARED_SLOT_SOURCE_WINDOW_INVALID"
    assert source["failure_phase"] == "PRE_SOURCE" and source["source_accessed_at_utc"] is None
    assert source["native_snapshot_entry"] is None
    assert adapter.verify_declared_slot_completion(env.context, precommit=env.precommit,
        process=env.process, completion=completion, artifact_roster=env.sink.artifact_roster(),
        frozen_pipeline_config=env.frozen) == completion
    assert calls == []


def test_declared_source_facade_refuses_duck_typed_config_before_methods_or_source_access(
    declared_env, monkeypatch,
):
    env = declared_env
    request = _request(env)
    calls = []
    def forbidden(label):
        def callback(*args, **kwargs):
            calls.append(label)
            raise AssertionError("Unauthenticated config reached a callback or source access")
        return callback
    proxy = SimpleNamespace(
        config_sha256=env.context.config_sha256,
        effective_config_sha256=env.frozen.effective_config_sha256,
        verify_current=forbidden("proxy.verify_current"),
        materialize=forbidden("proxy.materialize"),
    )
    monkeypatch.setattr(execution.spread_profile, "load_spread_profile", forbidden("profile"))
    monkeypatch.setattr(producer.native_prefix, "produce_activated_native_prefix", forbidden("native"))
    monkeypatch.setattr(execution, "fit_native_timeframe_for_search", forbidden("fit"))
    with pytest.raises(producer.HistoricalSearchProducerError) as caught:
        producer.execute_declared_slot_owner_search(
            env.context, request=request, process=env.process,
            frozen_pipeline_config=proxy,
        )
    assert caught.value.code == "DECLARED_SLOT_CONFIG_INVALID"
    assert calls == []


def _structural_native_snapshot(activation_id="structural-A", manifest_sha="b" * 64):
    """Invented contract metadata for guard tests; no source or OWNER evidence."""
    import pm_book_multiplicity as book
    import pm_native_activation as activation
    return {"pointer_schema": activation.ACTIVE_POINTER_SCHEMA,
        "manifest_schema": activation.ACTIVATION_MANIFEST_SCHEMA,
        "symbols": list(book.PINNED_SYMBOL_ORDER), "timeframes": list(book.TIMEFRAME_ORDER),
        "activation_id": activation_id, "activation_manifest_sha256": manifest_sha,
        "activation_entry_count": len(book.PINNED_SYMBOL_ORDER) * len(book.TIMEFRAME_ORDER),
        "eligible_symbols": sorted(book.PINNED_SYMBOL_ORDER), "excluded_symbols": [],
        "universe_eligibility_explicit": True, "activation_roster_sha256": "c" * 64,
        "bars_sha256_by_source": {}, "sidecar_sha256_by_source": {}}


def _structural_snapshot_backend(env, monkeypatch, *, observed, errors=(), state=None,
                                 consumed_snapshot=None):
    """Stub native I/O, retaining the real live/cold snapshot guards and spool.

    These labelled metadata products do not assert native authentication, a fit,
    or a strategy outcome. Genuine native/full300 proofs use the separate real
    executor controls. The guard under test and completion verifier are real.
    """
    import pm_portfolio_replay as portfolio_replay
    native = producer.native_prefix
    state = native.TECHNICAL_TERMINAL if state is None else state
    consumed_snapshot = observed if consumed_snapshot is None else consumed_snapshot
    calls, products = [], {}
    monkeypatch.setattr(execution.spread_profile, "load_spread_profile",
        lambda path: SimpleNamespace(sha256="e" * 64))
    def observe(*args, **kwargs):
        calls.append("observe")
        return json.loads(json.dumps(observed)), list(errors)
    monkeypatch.setattr(portfolio_replay, "active_native_portfolio_contract", observe)
    def produce(**kwargs):
        calls.append("produce")
        cutoff = kwargs["cutoff_utc"]
        slot_id = next(slot["slot_id"] for slot in env.roster["slots"] if slot["native_cutoff_utc"] == cutoff)
        namespace = next(entry["artifact_id"].rsplit("/", 1)[0]
            for entry in env.sink.artifact_roster()
            if entry["artifact_type"] == "declared_slot_native_snapshot" and f"/{slot_id}/" in entry["artifact_id"])
        index = {"source_state": state, "symbol": kwargs["symbol"], "timeframe": kwargs["timeframe"],
            "cutoff_utc": cutoff, "source_accessed_at_utc": kwargs["source_accessed_at_utc"],
            "activation": {"activation_id": consumed_snapshot.get("activation_id", "structural-unavailable"),
                "manifest_sha256": consumed_snapshot.get("activation_manifest_sha256", "d" * 64)}}
        entry = env.sink.put_json(namespace + "/structural-native-index", "structural_native_index", index)
        descriptor = entry["descriptor"]
        product = native.NativePrefixProduct(source_state=state, index=index,
            index_descriptor=descriptor, index_sha256=descriptor["sha256"], native_prefix={},
            instrument_snapshot=None, prefix_path=None, prefix_descriptor=None,
            artifact_descriptors=(descriptor,), _seal=native._PRODUCT_SEAL)
        products[product.index_sha256] = product
        return product
    def reverify(product, context, *, require_current_activation):
        assert context is env.context and require_current_activation is True
        calls.append("reverify")
        return product
    def reopen(**kwargs):
        calls.append("cold-native-backend")
        product = products[kwargs["expected_index_sha256"]]
        assert kwargs["pinned_activation_manifest_sha256"] == product.index["activation"]["manifest_sha256"]
        assert kwargs["production_contract_required"] is True
        return product
    def forbidden(*args, **kwargs):
        pytest.fail("Snapshot guard test reached fitting or financial execution")
    monkeypatch.setattr(native, "produce_activated_native_prefix", produce)
    monkeypatch.setattr(native, "reverify_activated_native_prefix_product", reverify)
    monkeypatch.setattr(native, "verify_activated_native_prefix", reopen)
    monkeypatch.setattr(execution, "fit_native_timeframe_for_search", forbidden)
    monkeypatch.setattr(execution, "execute_historical_observed_cell", forbidden)
    return calls


def _snapshot_completion(env, request):
    return producer.execute_declared_slot_owner_search(env.context, request=request,
        process=env.process, frozen_pipeline_config=env.frozen)


def _verify_snapshot_completion(env, completion):
    namespace = completion["request_body"]["artifact_namespace"] + "/"
    return adapter.verify_declared_slot_completion(env.context, precommit=env.precommit,
        process=env.process, completion=completion,
        artifact_roster=[entry for entry in env.sink.artifact_roster()
            if entry["artifact_id"].startswith(namespace)], frozen_pipeline_config=env.frozen)


@pytest.mark.parametrize("errors", [(), ("NATIVE_UNRELATED_CELL_ERROR",)])
def test_declared_snapshot_live_full_replacement_is_typed_stop_even_with_errors(tmp_path, monkeypatch, errors):
    frozen = _structural_native_snapshot()
    replacement = _structural_native_snapshot("structural-B", "d" * 64)
    env = _declared_environment(tmp_path, monkeypatch, native_snapshot=frozen)
    calls = _structural_snapshot_backend(env, monkeypatch, observed=replacement, errors=errors)
    completion = _snapshot_completion(env, _request(env))
    assert (completion["status"], completion["reason_code"]) == (
        "STOP", "DECLARED_SLOT_NATIVE_SNAPSHOT_MISMATCH")
    assert completion["owner"] is None and calls == ["observe"]
    assert _verify_snapshot_completion(env, completion) == completion
    assert calls == ["observe"]


@pytest.mark.parametrize("different", ["activation_id", "activation_manifest_sha256"])
def test_declared_snapshot_a_b_a_consumed_product_is_typed_stop_after_reverify(tmp_path, monkeypatch, different):
    frozen = _structural_native_snapshot()
    consumed = {**frozen, different: "structural-B" if different == "activation_id" else "d" * 64}
    env = _declared_environment(tmp_path, monkeypatch, native_snapshot=frozen)
    calls = _structural_snapshot_backend(env, monkeypatch, observed=frozen,
        consumed_snapshot=consumed, state=producer.native_prefix.AVAILABLE)
    completion = _snapshot_completion(env, _request(env))
    assert (completion["status"], completion["reason_code"]) == (
        "STOP", "DECLARED_SLOT_CONSUMED_NATIVE_SNAPSHOT_MISMATCH")
    assert calls == ["observe", "produce", "reverify"]
    # No second global snapshot observation can stand in for consumed binding.
    import pm_portfolio_replay as portfolio_replay
    monkeypatch.setattr(portfolio_replay, "active_native_portfolio_contract",
        lambda *args, **kwargs: pytest.fail("Cold completion consulted CURRENT"))
    assert _verify_snapshot_completion(env, completion) == completion
    assert calls == ["observe", "produce", "reverify", "cold-native-backend"]


@pytest.mark.parametrize("observed_kind,state,status,reason", [
    ("match", "TECHNICAL_TERMINAL", "MISSING", "TERMINAL_TECHNICAL"),
    ("match", "SOURCE_UNAVAILABLE", "MISSING", "SOURCE_UNAVAILABLE"),
    ("invalid", "SOURCE_UNAVAILABLE", "STOP", "DECLARED_SLOT_NATIVE_SNAPSHOT_INVALID"),
    ("unavailable", "TECHNICAL_FAILURE", "STOP", "TECHNICAL_FAILURE"),
    ("unavailable", "SOURCE_UNAVAILABLE", "STOP", "DECLARED_SLOT_NATIVE_SNAPSHOT_UNAVAILABLE"),
])
def test_declared_snapshot_availability_preserves_native_terminal_distinctions(
    tmp_path, monkeypatch, observed_kind, state, status, reason,
):
    frozen = _structural_native_snapshot()
    env = _declared_environment(tmp_path, monkeypatch, native_snapshot=frozen)
    observed = frozen if observed_kind in {"match", "invalid"} else {key: frozen[key]
        for key in ("pointer_schema", "manifest_schema", "symbols", "timeframes")}
    calls = _structural_snapshot_backend(env, monkeypatch, observed=observed,
        errors=(() if observed_kind == "match" else ("NATIVE_UNRELATED_CELL_ERROR",)
            if observed_kind == "invalid" else ("NATIVE_ACTIVE_POINTER_UNREADABLE:FileNotFoundError",)),
        state=getattr(producer.native_prefix, state))
    completion = _snapshot_completion(env, _request(env))
    assert (completion["status"], completion["reason_code"]) == (status, reason)
    assert completion["owner"] is None and completion["source_group"] is None
    assert calls == ["observe", "produce", "reverify"]
    assert _verify_snapshot_completion(env, completion) == completion


def test_declared_snapshot_cold_verification_reuses_frozen_bytes_without_current(tmp_path, monkeypatch):
    frozen = _structural_native_snapshot()
    env = _declared_environment(tmp_path, monkeypatch, native_snapshot=frozen)
    calls = _structural_snapshot_backend(env, monkeypatch, observed=frozen)
    completions = [_snapshot_completion(env, _request(env, ordinal=ordinal)) for ordinal in (1, 2)]
    sessions = [adapter._reopen_ascii_json_entry(env.context, value["source_session_entry"],
        expected_type="declared_slot_source_session", code="TEST_INVALID_SOURCE") for value in completions]
    assert sessions[0]["native_snapshot_entry"]["artifact_id"] != sessions[1]["native_snapshot_entry"]["artifact_id"]
    assert sessions[0]["native_snapshot_entry"]["descriptor"] == sessions[1]["native_snapshot_entry"]["descriptor"]
    import pm_portfolio_replay as portfolio_replay
    monkeypatch.setattr(portfolio_replay, "active_native_portfolio_contract",
        lambda *args, **kwargs: pytest.fail("Cold completion consulted CURRENT"))
    for completion in completions:
        assert _verify_snapshot_completion(env, completion) == completion
    assert calls.count("observe") == 2 and calls.count("cold-native-backend") == 2


@pytest.mark.parametrize("damage", ["absent", "replacement", "errors"])
def test_declared_snapshot_cold_completion_requires_exact_saved_frozen_binding(tmp_path, monkeypatch, damage):
    frozen = _structural_native_snapshot()
    env = _declared_environment(tmp_path, monkeypatch, native_snapshot=frozen)
    _structural_snapshot_backend(env, monkeypatch, observed=frozen)
    completion = _snapshot_completion(env, _request(env))
    session = adapter._reopen_ascii_json_entry(env.context, completion["source_session_entry"],
        expected_type="declared_slot_source_session", code="TEST_INVALID_SOURCE")
    namespace = completion["request_body"]["artifact_namespace"]
    if damage == "absent":
        session["native_snapshot_entry"] = session["native_snapshot_errors"] = None
    elif damage == "replacement":
        session["native_snapshot_entry"] = env.sink.put_json(namespace + "/replacement-snapshot",
            "declared_slot_native_snapshot", _structural_native_snapshot("structural-B", "d" * 64))
    else:
        session["native_snapshot_errors"] = ["NATIVE_UNRELATED_CELL_ERROR"]
    damaged = {**completion, "source_session_entry": env.sink.put_json(namespace + "/damaged-source-session",
        "declared_slot_source_session", session)}
    with pytest.raises(adapter.HistoricalProductionAdapterError) as caught:
        _verify_snapshot_completion(env, damaged)
    assert str(caught.value) == "DECLARED_SLOT_NATIVE_SNAPSHOT_BINDING_MISMATCH"


class _InterruptedStructuralAttempt(BaseException):
    pass


def _structural_completion(request, status="NO_TRADE", reason="NO_QUALIFIED_WINNER"):
    body = request.to_dict()
    assert status in {"NO_TRADE", "MISSING", "STOP"}
    return {
        "classification": "STRUCTURAL_STUB_NOT_OWNER_EVIDENCE",
        "schema_version": adapter.DECLARED_SLOT_COMPLETION_SCHEMA,
        "cell_id": body["cell_id"], "slot_id": body["slot_id"],
        "status": status, "reason_code": reason,
        "request": body, "request_body": body, "request_sha256": request.receipt_sha256,
        "completed_at_utc": _DeclaredClock.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _install_structural_completion_verifier(monkeypatch):
    def verify(context, *, precommit, process, completion, artifact_roster, frozen_pipeline_config):
        assert frozen_pipeline_config.materialize(context).discovery_risk_per_trade_pct == 1.0
        assert completion["classification"] == "STRUCTURAL_STUB_NOT_OWNER_EVIDENCE"
        assert completion["status"] in {"NO_TRADE", "MISSING", "STOP"}
        assert completion["cell_id"] == CELL_ID
        assert completion["slot_id"] in SLOT_IDS
        assert completion["request"]["search_process_receipt_sha256"] == process.receipt_sha256
        assert completion["request"]["precommit_sha256"] == precommit.receipt_sha256
        return json.loads(json.dumps(completion))
    monkeypatch.setattr(adapter, "verify_declared_slot_completion", verify)


def _structural_driver_trace(env, monkeypatch, *, stop_at=None, interrupt_at=None):
    """Real durability/sink mechanics with two explicit nonauthorising stubs."""
    import pm_authority_spool as spool
    _install_structural_completion_verifier(monkeypatch)
    state = SimpleNamespace(calls=[], namespaces=[], requests=[], entries=[], seals=[], interrupt_at=interrupt_at)
    real_seal = spool.SymbolAuthoritySink.seal
    def seal(sink):
        state.seals.append(len(state.calls))
        return real_seal(sink)
    monkeypatch.setattr(spool.SymbolAuthoritySink, "seal", seal)
    def materialize(context, *, request, process, frozen_pipeline_config):
        assert state.seals == [], "A shared symbol sink was sealed before slot work ended"
        body = orchestration.verify_declared_slot_producer_request(
            context, request, precommit=env.precommit, process=process,
        )
        ordinal = body["anchor"]["ordinal"]
        state.calls.append(ordinal)
        state.namespaces.append(body["artifact_namespace"])
        state.requests.append(request)
        entry = request.symbol_sink("AUDUSD").put_json(
            body["artifact_namespace"] + "/structural-metadata",
            "declared_structural_not_owner_evidence",
            {"classification": "STRUCTURAL_STUB_NOT_OWNER_EVIDENCE", "slot_id": body["slot_id"], "attempt_id": body["attempt_id"]},
        )
        state.entries.append(entry)
        if state.interrupt_at == ordinal:
            state.interrupt_at = None
            raise _InterruptedStructuralAttempt()
        if ordinal == stop_at:
            return _structural_completion(request, "STOP", "DECLARED_TECHNICAL_FAILURE")
        if ordinal == 2:
            return _structural_completion(request, "MISSING", "AUTHENTICATED_SOURCE_ABSENCE_STUB")
        reason = "REGIME_UNOBSERVED" if ordinal == 1 else "INSUFFICIENT_SUPPORT" if ordinal == 3 else "NO_QUALIFIED_WINNER"
        return _structural_completion(request, "NO_TRADE", reason)
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", materialize)
    return state


def _execute(env):
    return producer.execute_declared_cell_owner_slots(
        context=env.context, precommit=env.precommit, process=env.process,
        frozen_pipeline_config=env.frozen, cell_id=CELL_ID,
    )


def _progress(env):
    return producer.reopen_declared_cell_owner_progress(
        env.context, precommit=env.precommit, process=env.process, cell_id=CELL_ID,
        frozen_pipeline_config=env.frozen,
    )


def _committed_rows(env, progress):
    """Small labelled structural fixtures only; real cold replay streams each body."""
    return [producer._read_declared_commit(env.context,
        producer._declared_commit_path(env.precommit, CELL_ID, row["ordinal"]))[0]
        for row in progress.slot_summaries]


def _committed_snapshot(env, progress):
    paths = [env.context.root / producer._declared_commit_path(env.precommit, CELL_ID, row["ordinal"])
        for row in progress.slot_summaries]
    paths.extend(env.context.root / row["descriptor"]["relative_path"] for row in progress.slot_commit_artifacts)
    paths.extend(env.context.root / entry["descriptor"]["relative_path"]
        for row in _committed_rows(env, progress) for entry in row["artifact_roster"])
    if progress.symbol_receipt is not None:
        paths.append(env.context.root / progress.symbol_receipt["symbol_manifest"]["relative_path"])
    if progress.run_receipt is not None:
        paths.append(env.context.root / producer._declared_run_path(env.precommit, CELL_ID))
    return {path: path.read_bytes() for path in paths}


def test_declared_structural_thirteen_outcomes_and_completed_resume_make_no_search_calls(declared_env, monkeypatch):
    env = declared_env
    state = _structural_driver_trace(env, monkeypatch)
    receipt = _execute(env).to_dict()
    assert state.calls == list(range(1, 14))
    assert state.seals == [13]
    assert len(set(state.namespaces)) == 13
    assert receipt["completed_slots"] == receipt["declared_slots"] == 13
    assert receipt["terminal_stop"] is False
    assert [row["status"] for row in receipt["slot_commits"]] == ["NO_TRADE", "MISSING"] + ["NO_TRADE"] * 11
    progress = _progress(env)
    assert len(progress.slot_summaries) == 13
    assert progress.first_uncommitted_slot is None and progress.terminal_stop is False
    assert [row["completion"]["reason_code"] for row in _committed_rows(env, progress)[:3]] == [
        "REGIME_UNOBSERVED", "AUTHENTICATED_SOURCE_ABSENCE_STUB", "INSUFFICIENT_SUPPORT",
    ]
    before = _committed_snapshot(env, progress)
    def forbidden(*args, **kwargs):
        raise AssertionError("Completed durable run started another slot")
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", forbidden)
    monkeypatch.setattr(orchestration, "declared_slot_producer_request", forbidden)
    assert _execute(env).to_dict() == receipt
    assert state.calls == list(range(1, 14))
    assert _committed_snapshot(env, _progress(env)) == before


def test_declared_structural_commits_keep_only_own_roster_and_release_requests(declared_env, monkeypatch):
    """Real driver/commit custody with the explicitly labelled structural verifier."""
    env = declared_env
    state = _structural_driver_trace(env, monkeypatch, stop_at=4)
    receipt = _execute(env).to_dict()
    progress = _progress(env)
    assert receipt["completed_slots"] == len(progress.slot_summaries) == 4
    previous = None
    for index, row in enumerate(_committed_rows(env, progress)):
        request = state.requests[index]
        namespace = request.to_dict()["artifact_namespace"] + "/"
        assert len(row["artifact_roster"]) == 1
        assert all(entry["artifact_id"].startswith(namespace) for entry in row["artifact_roster"])
        assert row["previous_commit_sha256"] == previous
        path = env.context.root / producer._declared_commit_path(env.precommit, CELL_ID, row["ordinal"])
        previous = producer._read_declared_commit(env.context, path.relative_to(env.context.root).as_posix())[1]
        with pytest.raises(orchestration.HistoricalAnchorOrchestrationError) as caught:
            orchestration._verify_declared_request_identity(request, env.context, require_live=True)
        assert str(caught.value) == "DECLARED_SLOT_REQUEST_CAPABILITY_INVALID"
    assert len(progress.slot_commit_artifacts) == 4
    assert progress.symbol_receipt == receipt["symbol_receipt"]
    assert progress.run_receipt == receipt


def test_declared_structural_resume_ignores_partial_objects_and_uses_fresh_namespace(declared_env, monkeypatch):
    env = declared_env
    state = _structural_driver_trace(env, monkeypatch, interrupt_at=4)
    with pytest.raises(_InterruptedStructuralAttempt):
        _execute(env)
    assert state.calls == [1, 2, 3, 4] and state.seals == []
    progress = _progress(env)
    assert len(progress.slot_summaries) == 3
    assert progress.first_uncommitted_slot["slot_id"] == SLOT_IDS[3]
    assert progress.terminal_stop is False
    assert not (env.context.root / producer._declared_commit_path(env.precommit, CELL_ID, 4)).exists()
    before = _committed_snapshot(env, progress)
    orphan = env.context.root / state.entries[-1]["descriptor"]["relative_path"]
    orphan_bytes = orphan.read_bytes()
    old_request = state.requests[-1]
    old_namespace = state.namespaces[-1]
    receipt = _execute(env).to_dict()
    assert state.calls == [1, 2, 3, 4] + list(range(4, 14))
    assert state.requests[4] is not old_request
    assert state.namespaces[4] != old_namespace
    assert state.seals == [14]
    assert receipt["completed_slots"] == 13
    assert orphan.read_bytes() == orphan_bytes
    for path, raw in before.items():
        assert path.read_bytes() == raw
    assert len(_progress(env).slot_summaries) == 13


def test_declared_structural_resume_honours_atomic_commit_before_memory_advance(declared_env, monkeypatch):
    env = declared_env
    state = _structural_driver_trace(env, monkeypatch)
    real_commit = producer._commit_declared_slot_completion
    def interrupted_commit(context, **kwargs):
        result = real_commit(context, **kwargs)
        if kwargs["slot"]["ordinal"] == 3:
            raise _InterruptedStructuralAttempt()
        return result
    monkeypatch.setattr(producer, "_commit_declared_slot_completion", interrupted_commit)
    with pytest.raises(_InterruptedStructuralAttempt):
        _execute(env)
    assert state.calls == [1, 2, 3] and state.seals == []
    progress = _progress(env)
    assert len(progress.slot_summaries) == 3
    assert progress.first_uncommitted_slot["slot_id"] == SLOT_IDS[3]
    before = _committed_snapshot(env, progress)
    monkeypatch.setattr(producer, "_commit_declared_slot_completion", real_commit)
    receipt = _execute(env).to_dict()
    assert state.calls == list(range(1, 14))
    assert receipt["completed_slots"] == 13
    for path, raw in before.items():
        assert path.read_bytes() == raw


def test_declared_structural_terminal_stop_forbids_all_later_slot_calls(declared_env, monkeypatch):
    env = declared_env
    state = _structural_driver_trace(env, monkeypatch, stop_at=4)
    receipt = _execute(env).to_dict()
    assert state.calls == [1, 2, 3, 4] and state.seals == [4]
    assert receipt["terminal_stop"] is True and receipt["completed_slots"] == 4
    progress = _progress(env)
    assert progress.terminal_stop is True and progress.first_uncommitted_slot is None
    assert progress.slot_summaries[-1]["status"] == "STOP"
    assert _committed_rows(env, progress)[-1]["completion"]["reason_code"] == "DECLARED_TECHNICAL_FAILURE"
    for ordinal in range(5, 14):
        assert not (env.context.root / producer._declared_commit_path(env.precommit, CELL_ID, ordinal)).exists()
    before = _committed_snapshot(env, progress)
    def forbidden(*args, **kwargs):
        raise AssertionError("Terminal STOP attempted another slot")
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", forbidden)
    monkeypatch.setattr(orchestration, "declared_slot_producer_request", forbidden)
    assert _execute(env).to_dict() == receipt
    assert state.calls == [1, 2, 3, 4]
    assert _committed_snapshot(env, _progress(env)) == before


def test_declared_structural_completed_slot_then_error_is_durably_terminal(declared_env, monkeypatch):
    """Exercise driver durability only; the completion remains a labelled stub."""
    env = declared_env
    state = _structural_driver_trace(env, monkeypatch)
    materialize = producer.execute_declared_slot_owner_search
    failure = {"exception_type": "OSError", "failure_code": "DECLARED_TEST_CLEANUP_FAILURE",
        "failure_observed_at_utc": "2026-10-22T12:00:00Z"}
    def completed_then_failed(*args, **kwargs):
        completion = materialize(*args, **kwargs)
        raise producer._DeclaredSlotPostCompletionFailure(completion, failure)
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", completed_then_failed)
    receipt = _execute(env).to_dict()
    assert state.calls == [1] and state.seals == [1]
    assert receipt["completed_slots"] == 1 and receipt["terminal_stop"] is True
    assert receipt["slot_commits"][0]["status"] == "NO_TRADE"
    progress = _progress(env)
    assert progress.slot_summaries[0]["status"] == "NO_TRADE"
    assert progress.terminal_stop is True and progress.first_uncommitted_slot is None
    assert progress.terminal_failure["failure"] == failure
    assert progress.terminal_failure["last_commit_sha256"] == progress.last_commit_sha256
    committed = _committed_rows(env, progress)[0]
    assert committed["schema_version"] == producer._DECLARED_SLOT_POST_COMPLETION_COMMIT_SCHEMA
    assert committed["post_completion_failure"] == {"failure": failure,
        "recorded_at_utc": "2026-10-22T12:00:00Z", "time_order_anomalies": []}
    saved = _committed_snapshot(env, progress)
    terminal = env.context.root / producer._declared_terminal_path(env.precommit, CELL_ID)
    terminal_bytes = terminal.read_bytes() if terminal.exists() else None
    def forbidden(*args, **kwargs):
        raise AssertionError("Persisted post-completion failure attempted another slot")
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", forbidden)
    assert _execute(env).to_dict() == receipt
    assert _committed_snapshot(env, _progress(env)) == saved
    assert (terminal.read_bytes() if terminal.exists() else None) == terminal_bytes


def test_declared_structural_postcompletion_failure_survives_crash_after_commit(declared_env, monkeypatch):
    """Real commit/pointer crash boundary with the existing labelled completion/verifier stubs."""
    env = declared_env
    state = _structural_driver_trace(env, monkeypatch)
    materialize = producer.execute_declared_slot_owner_search
    real_commit = producer._commit_declared_slot_completion
    failure = {"exception_type": "OSError", "failure_code": "DECLARED_TEST_CLEANUP_FAILURE",
        "failure_observed_at_utc": "2026-10-22T12:00:00Z"}
    retained = {}

    def completed_then_failed(*args, **kwargs):
        completion = materialize(*args, **kwargs)
        retained["completion"] = json.loads(json.dumps(completion))
        raise producer._DeclaredSlotPostCompletionFailure(completion, failure)

    def crash_after_commit(context, **kwargs):
        real_commit(context, **kwargs)
        assert kwargs["slot"]["ordinal"] == 1
        relative = producer._declared_commit_path(env.precommit, CELL_ID, 1)
        body, digest, descriptor = producer._read_declared_commit(context, relative)
        assert body["completion"] == retained["completion"]
        assert body["completion"]["status"] == "NO_TRADE"
        retained["commit_sha256"] = digest
        retained["bytes"] = {
            context.root / relative: (context.root / relative).read_bytes(),
            context.root / descriptor["relative_path"]: (context.root / descriptor["relative_path"]).read_bytes(),
        }
        raise _InterruptedStructuralAttempt()

    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", completed_then_failed)
    monkeypatch.setattr(producer, "_commit_declared_slot_completion", crash_after_commit)
    with pytest.raises(_InterruptedStructuralAttempt):
        _execute(env)
    assert state.calls == [1] and state.seals == []
    assert not (env.context.root / producer._declared_terminal_path(env.precommit, CELL_ID)).exists()
    progress = _progress(env)
    assert len(progress.slot_summaries) == 1
    assert progress.slot_summaries[0]["status"] == "NO_TRADE"
    assert _committed_rows(env, progress)[0]["completion"] == retained["completion"]
    assert progress.last_commit_sha256 == retained["commit_sha256"]
    for path, original in retained["bytes"].items():
        assert path.read_bytes() == original
    assert progress.terminal_stop is True
    assert progress.first_uncommitted_slot is None
    assert progress.terminal_failure["failure"] == failure

    def forbidden(*args, **kwargs):
        raise AssertionError("Known committed failure allowed a later slot after crash")
    monkeypatch.setattr(producer, "_commit_declared_slot_completion", real_commit)
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", forbidden)
    monkeypatch.setattr(orchestration, "declared_slot_producer_request", forbidden)
    receipt = _execute(env).to_dict()
    assert receipt["completed_slots"] == 1 and receipt["terminal_stop"] is True
    assert receipt["slot_commits"][0]["status"] == "NO_TRADE"
    assert state.calls == [1]
    for path, original in retained["bytes"].items():
        assert path.read_bytes() == original


def test_declared_structural_postcompletion_failure_preserves_backward_clock_observations(declared_env, monkeypatch):
    """Observed UTC clock reversals cannot discard a completed structural slot or its stop."""
    env = declared_env
    state = _structural_driver_trace(env, monkeypatch)
    materialize = producer.execute_declared_slot_owner_search
    failure = {"exception_type": "OSError", "failure_code": "DECLARED_TEST_CLEANUP_FAILURE",
        "failure_observed_at_utc": "2026-10-22T11:59:59Z"}
    original_completion = {}

    def completed_then_clock_reversed(*args, **kwargs):
        completion = materialize(*args, **kwargs)
        original_completion.update(json.loads(json.dumps(completion)))
        _DeclaredClock.value = datetime(2026, 10, 22, 11, 59, 59, tzinfo=timezone.utc)
        assert _DeclaredClock.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") == failure["failure_observed_at_utc"]
        _DeclaredClock.value = datetime(2026, 10, 22, 11, 59, 58, tzinfo=timezone.utc)
        raise producer._DeclaredSlotPostCompletionFailure(completion, failure)

    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", completed_then_clock_reversed)
    receipt = _execute(env).to_dict()
    assert state.calls == [1] and receipt["completed_slots"] == 1 and receipt["terminal_stop"] is True
    progress = _progress(env)
    committed = _committed_rows(env, progress)[0]
    assert committed["completion"] == original_completion
    assert committed["completion"]["status"] == "NO_TRADE"
    assert committed["completion"]["completed_at_utc"] == "2026-10-22T12:00:00Z"
    anomalies = ["FAILURE_OBSERVED_BEFORE_COMPLETION", "FAILURE_RECORDED_BEFORE_OBSERVED"]
    assert committed["post_completion_failure"] == {"failure": failure,
        "recorded_at_utc": "2026-10-22T11:59:58Z", "time_order_anomalies": anomalies}
    assert progress.terminal_failure["failure"] == failure
    assert progress.terminal_failure["recorded_at_utc"] == "2026-10-22T11:59:58Z"
    assert progress.terminal_failure["time_order_anomalies"] == anomalies
    assert progress.terminal_stop is True and progress.first_uncommitted_slot is None
    saved = _committed_snapshot(env, progress)

    def forbidden(*args, **kwargs):
        raise AssertionError("Backward clock lost the committed terminal failure")
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", forbidden)
    monkeypatch.setattr(orchestration, "declared_slot_producer_request", forbidden)
    assert _execute(env).to_dict() == receipt
    assert _committed_snapshot(env, _progress(env)) == saved


@pytest.mark.parametrize("sibling", ["matching", "failure-splice", "legacy-schema"])
def test_declared_structural_atomic_failure_requires_exact_optional_sibling(declared_env, monkeypatch, sibling):
    env = declared_env
    _install_structural_completion_verifier(monkeypatch)
    request = _request(env)
    failure = {"exception_type": "OSError", "failure_code": "DECLARED_TEST_CLEANUP_FAILURE",
        "failure_observed_at_utc": "2026-10-22T12:00:00Z"}
    producer._commit_declared_slot_completion(env.context, precommit=env.precommit, process=env.process,
        cell_id=CELL_ID, slot=env.roster["slots"][0], completion=_structural_completion(request),
        artifact_roster=[], previous_commit_sha256=None, frozen_pipeline_config=env.frozen,
        post_completion_failure=failure)
    orchestration._release_declared_slot_producer_request(request, env.context)
    original = _progress(env)
    assert original.terminal_stop is True and original.first_uncommitted_slot is None
    saved = _committed_snapshot(env, original)
    body = json.loads(json.dumps(original.terminal_failure))
    if sibling == "failure-splice":
        body["failure"]["failure_code"] = "DECLARED_TEST_DIFFERENT_FAILURE"
    elif sibling == "legacy-schema":
        body["schema_version"] = "fxpm.declared-cell-owner-terminal.v1"
        body.pop("time_order_anomalies")
    terminal = env.context.root / producer._declared_terminal_path(env.precommit, CELL_ID)
    terminal.parent.mkdir(parents=True, exist_ok=True)
    with terminal.open("xb") as stream:
        stream.write(_canonical(body))
    terminal_bytes = terminal.read_bytes()
    if sibling == "matching":
        reopened = _progress(env)
        assert reopened.terminal_failure == original.terminal_failure
        assert reopened.terminal_stop is True and reopened.first_uncommitted_slot is None
    else:
        with pytest.raises(producer.HistoricalSearchProducerError) as caught:
            _progress(env)
        assert caught.value.code == "DECLARED_SLOT_TERMINAL_BINDING_MISMATCH"
    assert terminal.read_bytes() == terminal_bytes
    for path, original_bytes in saved.items():
        assert path.read_bytes() == original_bytes


@pytest.mark.parametrize("damage,expected", [
    ("none", None),
    ("commit-sha", "DECLARED_SLOT_TERMINAL_BINDING_MISMATCH"),
    ("recorded-time", "DECLARED_SLOT_TERMINAL_INVALID"),
    ("standalone-v2", "DECLARED_SLOT_TERMINAL_INVALID"),
])
def test_declared_structural_legacy_v1_failure_sibling_preserves_exact_contract(declared_env, monkeypatch, damage, expected):
    env = declared_env
    _install_structural_completion_verifier(monkeypatch)
    request = _request(env)
    summary, descriptor = producer._commit_declared_slot_completion(env.context, precommit=env.precommit,
        process=env.process, cell_id=CELL_ID, slot=env.roster["slots"][0],
        completion=_structural_completion(request), artifact_roster=[], previous_commit_sha256=None,
        frozen_pipeline_config=env.frozen)
    orchestration._release_declared_slot_producer_request(request, env.context)
    commit_path = producer._declared_commit_path(env.precommit, CELL_ID, 1)
    committed, digest, _descriptor = producer._read_declared_commit(env.context, commit_path)
    assert committed["schema_version"] == producer.DECLARED_SLOT_COMMIT_SCHEMA == "fxpm.declared-slot-commit.v1"
    assert "post_completion_failure" not in committed
    saved = {env.context.root / commit_path: (env.context.root / commit_path).read_bytes(),
        env.context.root / descriptor["relative_path"]: (env.context.root / descriptor["relative_path"]).read_bytes()}
    failure = {"exception_type": "OSError", "failure_code": "DECLARED_TEST_CLEANUP_FAILURE",
        "failure_observed_at_utc": "2026-10-22T12:00:00Z"}
    producer._commit_declared_terminal_failure(env.context, precommit=env.precommit, process=env.process, cell_id=CELL_ID,
        completed=[summary], last_commit_sha256=digest, failure=failure)
    terminal = env.context.root / producer._declared_terminal_path(env.precommit, CELL_ID)
    body = json.loads(terminal.read_bytes())
    assert body["schema_version"] == "fxpm.declared-cell-owner-terminal.v1"
    assert "time_order_anomalies" not in body
    if damage == "commit-sha":
        body["last_commit_sha256"] = "f" * 64
    elif damage == "recorded-time":
        body["recorded_at_utc"] = "2026-10-22T11:59:59Z"
    elif damage == "standalone-v2":
        body["schema_version"] = producer._DECLARED_CELL_POST_COMPLETION_TERMINAL_SCHEMA
        body["time_order_anomalies"] = []
    if damage != "none":
        terminal.write_bytes(_canonical(body))
    terminal_bytes = terminal.read_bytes()

    def forbidden(*args, **kwargs):
        raise AssertionError("A legacy terminal sibling allowed another slot")
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", forbidden)
    monkeypatch.setattr(orchestration, "declared_slot_producer_request", forbidden)
    if expected is None:
        progress = _progress(env)
        assert progress.terminal_failure == body
        assert progress.terminal_stop is True and progress.first_uncommitted_slot is None
        assert _committed_rows(env, progress)[0] == committed
        receipt = _execute(env).to_dict()
        assert receipt["completed_slots"] == 1 and receipt["terminal_stop"] is True
        assert receipt["slot_commits"][0]["status"] == "NO_TRADE"
    else:
        with pytest.raises(producer.HistoricalSearchProducerError) as caught:
            _progress(env)
        assert caught.value.code == expected
    assert terminal.read_bytes() == terminal_bytes
    for path, original_bytes in saved.items():
        assert path.read_bytes() == original_bytes


def _structural_commit_body(env, ordinal, *, previous=None, status="NO_TRADE", artifact_roster=()):
    """Declared non-OWNER objects for negative durable-chain checks only."""
    request = _request(env, ordinal, attempt=f"structural-prefix-{ordinal:02d}")
    return {
        "schema_version": producer.DECLARED_SLOT_COMMIT_SCHEMA,
        "precommit_sha256": env.precommit.receipt_sha256,
        "generation_context_sha256": env.context.context_sha256,
        "cell_id": CELL_ID, "slot_id": SLOT_IDS[ordinal - 1], "ordinal": ordinal,
        "previous_commit_sha256": previous,
        "completion": _structural_completion(request, status),
        "artifact_roster": list(artifact_roster),
    }


def _publish_structural_commit(env, ordinal, body):
    import pm_authority_spool as spool
    descriptor = spool.write_canonical_json_artifact(env.context, body)
    spool._publish_exact_json(
        env.context, producer._declared_commit_path(env.precommit, CELL_ID, ordinal),
        {"schema_version": "fxpm.declared-slot-commit-pointer.v1", "commit_descriptor": descriptor},
        env.context.limits.max_json_bytes,
    )
    return descriptor


@pytest.mark.parametrize("damage,expected", [
    ("hole", "DECLARED_SLOT_PREFIX_GAP"),
    ("previous", "DECLARED_SLOT_PREFIX_BINDING_MISMATCH"),
    ("context", "DECLARED_SLOT_PREFIX_BINDING_MISMATCH"),
    ("slot", "DECLARED_SLOT_PREFIX_BINDING_MISMATCH"),
    ("artifact-conflict", "DECLARED_SLOT_PREFIX_BINDING_MISMATCH"),
    ("malformed", "DECLARED_SLOT_COMMIT_INVALID"),
    ("after-stop", "DECLARED_SLOT_AFTER_TERMINAL_STOP"),
])
def test_declared_structural_prefix_refuses_holes_conflicts_corruption_and_after_stop(
    declared_env, monkeypatch, damage, expected,
):
    env = declared_env
    _install_structural_completion_verifier(monkeypatch)
    first_entries, second_entries = [], []
    if damage == "artifact-conflict":
        first = env.sink.put_json("structural-prefix/A", "declared_structural_not_owner_evidence", {"declared": "A"})
        second = env.sink.put_json("structural-prefix/B", "declared_structural_not_owner_evidence", {"declared": "B"})
        first_entries = [first]
        # Both descriptor bytes are actual test-owned metadata. Reusing A's ID
        # with B's descriptor is deliberately conflicting, not genuine evidence.
        second_entries = [{**second, "artifact_id": first["artifact_id"]}]
    previous = None
    if damage not in {"hole", "malformed"}:
        first = _structural_commit_body(
            env, 1, status="STOP" if damage == "after-stop" else "NO_TRADE",
            artifact_roster=first_entries,
        )
        previous = _publish_structural_commit(env, 1, first)["sha256"]
    if damage == "malformed":
        path = env.context.root / producer._declared_commit_path(env.precommit, CELL_ID, 1)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'{"interrupted-or-corrupt":')
    else:
        second = _structural_commit_body(env, 2, previous=previous, artifact_roster=second_entries)
        if damage == "previous":
            second["previous_commit_sha256"] = "f" * 64
        elif damage == "context":
            second["generation_context_sha256"] = "f" * 64
        elif damage == "slot":
            second["slot_id"] = SLOT_IDS[2]
        _publish_structural_commit(env, 2, second)
    def forbidden(*args, **kwargs):
        raise AssertionError("Invalid durable prefix attempted another source slot")
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", forbidden)
    with pytest.raises(producer.HistoricalSearchProducerError) as caught:
        _progress(env)
    assert caught.value.code == expected


def test_declared_structural_commit_refuses_reexecuting_committed_slot(declared_env, monkeypatch):
    env = declared_env
    _install_structural_completion_verifier(monkeypatch)
    request = _request(env)
    kwargs = {
        "precommit": env.precommit, "process": env.process, "cell_id": CELL_ID,
        "slot": env.roster["slots"][0], "completion": _structural_completion(request),
        "artifact_roster": [], "previous_commit_sha256": None,
        "frozen_pipeline_config": env.frozen,
    }
    first, descriptor = producer._commit_declared_slot_completion(env.context, **kwargs)
    path = env.context.root / producer._declared_commit_path(env.precommit, CELL_ID, 1)
    before = path.read_bytes()
    assert producer._read_declared_commit(env.context, path.relative_to(env.context.root).as_posix())[1] == descriptor["sha256"]
    assert first["slot_id"] == SLOT_IDS[0]
    with pytest.raises(producer.HistoricalSearchProducerError) as caught:
        producer._commit_declared_slot_completion(env.context, **kwargs)
    assert caught.value.code == "DECLARED_SLOT_COMMITTED_SLOT_REEXECUTION"
    assert path.read_bytes() == before


def test_declared_run_kernel_lock_refuses_second_driver_before_source_and_releases(
    declared_env, monkeypatch,
):
    """Real OS exclusion on test-owned bytes; no PID or time-based inference."""
    import pm_native_storage as storage
    import pm_authority_spool as spool
    env = declared_env
    state = _structural_driver_trace(env, monkeypatch)
    path = env.context.root / f"declared-slots/{env.precommit.receipt_sha256}/{CELL_ID}/run.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0")
    real_open_sink = spool.open_symbol_sink
    spool_accesses = []
    def forbidden_sink(*args, **kwargs):
        spool_accesses.append(True)
        raise AssertionError("A second driver reached the symbol spool while locked")
    monkeypatch.setattr(spool, "open_symbol_sink", forbidden_sink)
    with path.open("r+b") as owner:
        assert storage._try_kernel_lock(owner) is True
        try:
            with path.open("r+b") as other:
                assert storage._try_kernel_lock(other) is False
            with pytest.raises(producer.HistoricalSearchProducerError) as caught:
                _execute(env)
            assert caught.value.code == "DECLARED_SLOT_RUN_ACTIVE"
            assert state.calls == [] and state.seals == [] and spool_accesses == []
        finally:
            storage._release_kernel_lock(owner)
    assert path.exists()
    monkeypatch.setattr(spool, "open_symbol_sink", real_open_sink)
    receipt = _execute(env).to_dict()
    assert state.calls == list(range(1, 14)) and state.seals == [13]
    assert receipt["completed_slots"] == 13
    assert path.exists()


def _declared_transport_environment(tmp_path, monkeypatch, edition, *, native_snapshot=None):
    """Issue real metadata on invented inputs; no source, fit or OWNER evidence."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    env = _declared_environment(tmp_path, monkeypatch, issue=False, native_snapshot=native_snapshot)
    issuer = (orchestration.precommit_declared_slot_owner_run_v2 if edition == "v16"
              else orchestration.precommit_declared_slot_owner_run)
    env.precommit = issuer(env.context, **env.arguments)
    return env


@pytest.mark.parametrize("edition", ["v15", "v16"])
def test_declared_transport_structural_completion_and_cold_progress(tmp_path, monkeypatch, edition):
    """Real metadata/commit/cold-read mechanics with explicit non-OWNER stubs."""
    env = _declared_transport_environment(tmp_path, monkeypatch, edition)
    parent_before = env.precommit._body_json
    parent = orchestration.verify_declared_slot_owner_run_precommit(env.context, env.precommit, process=env.process)
    expected = (orchestration.DECLARED_WINNER_TRANSPORT_V16 if edition == "v16"
                else orchestration.DECLARED_WINNER_TRANSPORT_V15)
    assert orchestration.declared_slot_transport_edition(parent) == expected
    assert ("winner_transport_edition" in parent) is (edition == "v16")
    state = _structural_driver_trace(env, monkeypatch)
    receipt = _execute(env).to_dict()
    assert receipt["completed_slots"] == 13 and receipt["terminal_stop"] is False
    assert state.calls == list(range(1, 14))
    for request in state.requests:
        body = request.to_dict()
        assert orchestration.declared_slot_transport_edition(body) == expected
        assert ("winner_transport_edition" in body) is (edition == "v16")
    progress = _progress(env)
    assert len(progress.slot_summaries) == 13 and progress.first_uncommitted_slot is None
    rows = _committed_rows(env, progress)
    assert all(row["completion"]["classification"] == "STRUCTURAL_STUB_NOT_OWNER_EVIDENCE" for row in rows)
    before = _committed_snapshot(env, progress)
    def forbidden(*args, **kwargs):
        pytest.fail("Cold completed progress attempted another search or request")
    monkeypatch.setattr(producer, "execute_declared_slot_owner_search", forbidden)
    monkeypatch.setattr(orchestration, "declared_slot_producer_request", forbidden)
    assert _execute(env).to_dict() == receipt
    assert _committed_snapshot(env, _progress(env)) == before
    assert env.precommit._body_json == parent_before


def _transport_gate_completion(request, status, *, frozen=None):
    """Malformed/mutation carrier for the edition gate, never raw OWNER proof."""
    body = request.to_dict()
    return {"classification": "EDITION_GATE_MUTATION_NOT_OWNER_EVIDENCE", "request_body": body,
        "request_sha256": request.receipt_sha256, "cell_id": body["cell_id"], "slot_id": body["slot_id"],
        "status": status, "frozen_plan_receipt": frozen}


@pytest.mark.parametrize("expected_edition", ["v15", "v16"])
@pytest.mark.parametrize("status", ["OWNER", "STOP", "NO_TRADE"])
@pytest.mark.parametrize("boundary", ["commit", "cold"])
def test_declared_transport_cross_edition_refuses_before_full_completion_verifier(
        tmp_path, monkeypatch, expected_edition, status, boundary):
    """Both public commit routes reject crossed request editions before raw proof."""
    env = _declared_transport_environment(tmp_path / "expected", monkeypatch, expected_edition)
    other = _declared_transport_environment(tmp_path / "other", monkeypatch,
        "v15" if expected_edition == "v16" else "v16")
    completion = _transport_gate_completion(_request(other), status)
    def forbidden(*args, **kwargs):
        pytest.fail("Edition mismatch reached full adapter verification")
    monkeypatch.setattr(adapter, "verify_declared_slot_completion", forbidden)
    slot = env.roster["slots"][0]
    path = env.context.root / producer._declared_commit_path(env.precommit, CELL_ID, 1)
    if boundary == "cold":
        # A deliberately malformed persisted record exercises the real cold
        # refusal. It is not a committed or authenticated completion fixture.
        body = {"schema_version": producer.DECLARED_SLOT_COMMIT_SCHEMA,
            "precommit_sha256": env.precommit.receipt_sha256,
            "generation_context_sha256": env.context.context_sha256, "cell_id": CELL_ID,
            "slot_id": slot["slot_id"], "ordinal": 1, "previous_commit_sha256": None,
            "completion": completion, "artifact_roster": []}
        _publish_structural_commit(env, 1, body)
        operation = lambda: _progress(env)
    else:
        operation = lambda: producer._commit_declared_slot_completion(env.context,
            precommit=env.precommit, process=env.process, cell_id=CELL_ID, slot=slot,
            completion=completion, artifact_roster=[], previous_commit_sha256=None,
            frozen_pipeline_config=env.frozen)
    with pytest.raises(producer.HistoricalSearchProducerError) as caught:
        operation()
    assert caught.value.code == "DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"
    if boundary == "commit":
        assert not path.exists()


@pytest.mark.parametrize("edition", ["v15", "v16"])
@pytest.mark.parametrize("matching", [True, False], ids=["matching", "crossed"])
def test_declared_transport_frozen_schema_discriminator_reopens_actual_bytes(
        tmp_path, monkeypatch, edition, matching):
    """Exercise only the edition discriminator, not complete raw authentication."""
    import pm_winner_raw_evidence as raw
    env = _declared_transport_environment(tmp_path, monkeypatch, edition)
    request = _request(env)
    expected = orchestration.declared_slot_transport_edition(env.precommit.to_dict())
    schema = raw.V16_FROZEN_PLAN_SCHEMA if edition == "v16" else raw.CURRENT_FROZEN_PLAN_SCHEMA
    if not matching:
        schema = raw.CURRENT_FROZEN_PLAN_SCHEMA if edition == "v16" else raw.V16_FROZEN_PLAN_SCHEMA
    encoded = _canonical({"schema_version": schema,
        "classification": "SCHEMA_DISCRIMINATOR_ONLY_NOT_OWNER_EVIDENCE"})
    entry = env.sink.put_opaque(request.to_dict()["artifact_namespace"] + "/schema-discriminator",
        "synthetic_schema_discriminator_not_raw_evidence", (encoded,), format=raw.ASCII_JSON_FORMAT)
    completion = _transport_gate_completion(request, "OWNER", frozen=entry["descriptor"])
    if matching:
        assert producer._verify_declared_completion_transport(env.context,
            completion=completion, expected_edition=expected) is None
    else:
        with pytest.raises(producer.HistoricalSearchProducerError) as caught:
            producer._verify_declared_completion_transport(env.context,
                completion=completion, expected_edition=expected)
        assert caught.value.code == "DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"


@pytest.mark.parametrize("edition", ["v15", "v16"])
def test_declared_transport_owner_requires_frozen_receipt_before_adapter(tmp_path, monkeypatch, edition):
    env = _declared_transport_environment(tmp_path, monkeypatch, edition)
    completion = _transport_gate_completion(_request(env), "OWNER")
    with pytest.raises(producer.HistoricalSearchProducerError) as caught:
        producer._verify_declared_completion_transport(env.context, completion=completion,
            expected_edition=orchestration.declared_slot_transport_edition(env.precommit.to_dict()))
    assert caught.value.code == "DECLARED_SLOT_TRANSPORT_EDITION_MISMATCH"
