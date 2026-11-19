"""Invented metadata integration. No actual Native, model or financial proof."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import pm_authoritative_search_execution as execution
import pm_authority_spool as spool
import pm_book_multiplicity as book
import pm_causal_evidence_producer as causal
import pm_end_grid as grid
import pm_historical_anchor_orchestration as orchestration
import pm_historical_production_adapter as adapter
import pm_historical_scheduler as scheduler
import pm_historical_search_producer as producer


def _invented_completed_cell(tmp_path, monkeypatch, obstruction, *, before_first_completion=None, first_observed=False):
    root = tmp_path / "working"
    grid_root = tmp_path / "grid"
    grid_root.mkdir()
    cell = {"cell_id": book.compute_cell_id("XAGUSD", "H1", "TREND"),
            "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"}
    contract_sha = "1" * 64
    limits = spool.SpoolLimits(max_artifact_bytes=1024**2, max_symbol_bytes=2 * 1024**2,
        max_artifacts_per_symbol=100, max_manifest_bytes=128 * 1024, max_json_bytes=256 * 1024,
        stream_chunk_bytes=64 * 1024, max_generation_bytes=4 * 1024**2, min_free_bytes=1)
    context = spool.freeze_generation_context(root, generation_id="INVENTED_WITNESS_QA",
        native_universe_sha256="2" * 64, config_sha256="3" * 64, limits=limits,
        owned_workspace={"cell_id": cell["cell_id"], "cell_contract_sha256": contract_sha})
    precommit = SimpleNamespace(receipt_sha256="4" * 64, to_dict=lambda: {
        "source_identity": {"invented-metadata-only": "5" * 64}})
    process = SimpleNamespace(receipt_sha256="6" * 64)
    profile_sha = "7" * 64
    run = {"relative_path": producer._declared_run_path(precommit, cell["cell_id"]),
           "sha256": "8" * 64, "size_bytes": 1}
    policy = {"end_grid_root": str(grid_root), "source_fence_path": str(tmp_path / "fence.json"),
              "retirement": {"receipt_path": str(tmp_path / "retirement.json"),
                             "max_paths": 100, "max_nodes": 300, "max_bytes": 4 * 1024**2}}
    row = {"root": str(root), "cell": cell, "run_id": context.generation_id, "native_finalization": policy}
    config = SimpleNamespace(data_dir=tmp_path / "invented-input")
    contract = {"active_config_path": str(tmp_path / "invented-config.json"), "profile_sha256": profile_sha}
    slot_root = f"declared-slots/{precommit.receipt_sha256}"
    base = slot_root + "/" + cell["cell_id"]
    label = {"fixture_provenance": "INVENTED_NATIVE_WITNESS_INVENTORY_QA"}

    def publish(relative, payload):
        spool._publish_exact_json(context, relative, payload, context.limits.max_json_bytes)

    publish(slot_root + "/precommit.json", label)
    if before_first_completion is None:
        publish(base + "/run.lock", label)
        publish(run["relative_path"], label)
    slots, proofs, summaries, no_plan = [], {}, [], {}
    start = datetime(2023, 6, 24, tzinfo=timezone.utc)
    sink = spool.open_symbol_sink(context, "XAGUSD")
    for ordinal in range(1, 14):
        slot_start = start + timedelta(days=28 * (ordinal - 1))
        slot_id = f"owner-slot-{ordinal:02d}-{slot_start:%Y%m%dT%H%M%SZ}"
        cutoff = (slot_start - timedelta(hours=1)).isoformat()
        slot = {"slot_id": slot_id, "ordinal": ordinal, "activation_utc": slot_start.isoformat(),
                "end_exclusive_utc": (slot_start + timedelta(days=28)).isoformat(), "native_cutoff_utc": cutoff}
        slots.append(slot)
        attempt = "attempt-invented"
        request = {"schema_version": orchestration.DECLARED_SLOT_REQUEST_SCHEMA_V2,
                   "precommit_sha256": precommit.receipt_sha256, "cell_id": cell["cell_id"],
                   "slot_id": slot_id, "attempt_id": attempt,
                   "artifact_namespace": f"invented/{slot_id}", "anchor": {"selection_cutoff_utc": cutoff}}
        if ordinal == 1 and first_observed:
            request["source_access_window_utc"] = {
                "start_utc": "2026-11-01T00:00:00Z", "end_exclusive_utc": "2026-12-01T00:00:00Z"}
        req_sha = scheduler._declared_m5_hash(request)
        request_root = f"{slot_root}/requests/{cell['cell_id']}/{slot_id}"
        start_root = f"{base}/attempts/{slot_id}"
        if ordinal == 1 and before_first_completion is not None:
            before_first_completion(context, precommit, process, cell, slot, request)
        else:
            publish(f"{request_root}/{attempt}.json", request)
            publish(f"{start_root}/{attempt}.json", {"schema_version": "fxpm.declared-slot-attempt-start.v1",
                                                    "slot_id": slot_id, "request_sha256": req_sha})
        publish(producer._declared_commit_path(precommit, cell["cell_id"], ordinal), label)
        if ordinal == 1 and first_observed:
            # Invented observed-completion proof data; the header reader and retirement are real.
            reference = sink.put_json(request["artifact_namespace"] + "/invented-bound-fit",
                "invented_bound_fit_reference", label)
            header = {"schema_version": execution._DECLARED_UNIT_HEADER_SCHEMA,
                "request_body": request, "fitted_state_entry": reference,
                "fit_state_origin_contract": "RECOMPUTED_OR_REOPENED_FROM_PRECOMMITTED_BOUND_STATE",
                "plan_transport": dict(label), "horizon_plan": dict(label),
                "binding": {"generation_context_sha256": context.context_sha256,
                    "producer_request_sha256": req_sha,
                    "executor_release_sha256": execution.AUTHORITATIVE_SEARCH_EXECUTION_RELEASE_SHA256,
                    "producer_source_sha256": execution._file_sha256(Path(execution.__file__).with_name(
                        "pm_historical_search_producer.py")),
                    "fitted_state_reference_sha256": scheduler._declared_m5_hash(reference), **label},
                "qualification_authority": False, "live_authority": False, **label}
            publish(execution._declared_unit_relative(request, "header.json"), header)
            publish(execution._declared_unit_relative(request, "evidence-completed.json"), {
                "binding_sha256": scheduler._declared_m5_hash(header["binding"]), "unit_count": 0,
                "unit_receipt_sha256": [], "evidence_access_completed_at_utc": cutoff})
            publish(execution._declared_unit_relative(request, "raw-completion.json"), label)
            assert execution._read_declared_unit_header(context, request) == header
        else:
            no_plan[slot_id] = {"request_sha256": req_sha}
        summaries.append({"status": "NO_TRADE", "request_sha256": req_sha,
                          "commit_sha256": scheduler._declared_m5_hash({"invented_slot": slot_id})})
        view = {"schema_version": "fxpm.broker-native-end-grid-view.v1", "symbol": "XAGUSD",
                "timeframe": "H1", "cutoff_utc": cutoff, "activation_id": "INVENTED_ACTIVATION",
                "activation_manifest_sha256": "9" * 64, "activation_entry_sha256": "a" * 64,
                "native_bars_sha256": "b" * 64, "continuity_policy_sha256": "c" * 64,
                "continuity_geometry_sha256": "d" * 64}
        tier = {"semantics": "BOUND_BASELINE_DESCRIPTOR_NO_SELECTED_WINNER", "tier": "P75",
                "bound_baseline_descriptor": label}
        selection = {"outcome": "NO_TRADE", "reason_code": "REGIME_UNOBSERVED", "cost_tier": "P75",
                     "source_contract_sha256": "e" * 64, "strategy_family": None, "configuration_sha256": None}
        if ordinal == 1 and not first_observed:
            # The original verifier and the twelve unrelated slot proofs are fixture seams.
            # The first slot exercises the actual Grid proof and exact source/session/index joins.
            index = {"symbol": "XAGUSD", "timeframe": "H1", "cutoff_utc": cutoff, "source_state": "AVAILABLE",
                     "source_accessed_at_utc": cutoff,
                     "activation": {"activation_id": view["activation_id"],
                                    "manifest_sha256": view["activation_manifest_sha256"],
                                    "entry_sha256": view["activation_entry_sha256"]},
                     "source": {"bars_sha256": view["native_bars_sha256"]},
                     "continuity": {"policy": {"sha256": view["continuity_policy_sha256"]},
                                    "geometry_sha256": view["continuity_geometry_sha256"]}}
            index_entry = sink.put_json(request["artifact_namespace"] + "/index", "invented_index", index)
            index_descriptor = index_entry["descriptor"]
            session = {"request_sha256": req_sha, "selection_cutoff_utc": cutoff, "source_state": "AVAILABLE",
                       "source_accessed_at_utc": cutoff, "native_prefix_index_descriptor": index_descriptor,
                       "native_prefix_index_sha256": index_descriptor["sha256"], "source_group_decision_sha256": "f" * 64}
            source_entry = sink.put_json(request["artifact_namespace"] + "/source-session",
                "declared_slot_source_session", session)
            witness = sink.put_json(request["artifact_namespace"] + "/" + adapter._DECLARED_NATIVE_UNOBSERVED_WITNESS_SUFFIX,
                adapter._DECLARED_NATIVE_UNOBSERVED_WITNESS_TYPE, label)
            source = SimpleNamespace(receipt_sha256="f" * 64, to_dict=lambda: {
                "symbol": "XAGUSD", "timeframe": "H1", "source_group_evidence_sha256": index_descriptor["sha256"],
                "activation_id": "INVENTED_ACTIVATION", "activation_manifest_sha256": "9" * 64,
                "activation_entry_sha256": "a" * 64, "native_source_sha256": "b" * 64, "source_contract_sha256": "e" * 64})
            completion = {"status": "NO_TRADE", "reason_code": "REGIME_UNOBSERVED", "owner": None,
                          "request_body": request, "request_sha256": req_sha, "source_session_entry": source_entry,
                          "source_group": {}, "regime_fit": {}, "plan_transport": None, "horizon_plan": None,
                          "frozen_plan_receipt": None}
            monkeypatch.setattr(adapter, "_rebuild_source_decision", lambda value: source)
            monkeypatch.setattr(adapter, "_rebuild_regime_fit_decision", lambda *args: object())

            def accepted_baseline(_context, *, entry, **kwargs):
                assert entry == witness and kwargs["profile_sha256"] == profile_sha
                return label

            monkeypatch.setattr(adapter, "_reopen_declared_native_unobserved_p75", accepted_baseline)
            assert adapter._declared_native_unobserved_no_plan(completion)
            proofs[slot_id] = scheduler._declared_native_grid_proof(context, row=row, slot=slot,
                commit={"completion": completion, "artifact_roster": [source_entry, witness]},
                profile_sha=profile_sha, no_plan=True)
            if obstruction == "present-unit":
                spool._local_path(root, execution._declared_unit_relative(request, "header.json")).parent.mkdir(parents=True)
            if obstruction == "abandoned":
                prior = {**request, "attempt_id": "attempt-earlier"}
                publish(request_root + "/attempt-earlier.json", prior)
                publish(start_root + "/attempt-earlier.json", {"schema_version": "fxpm.declared-slot-attempt-start.v1",
                    "slot_id": slot_id, "request_sha256": scheduler._declared_m5_hash(prior)})
        else:
            if ordinal == 1 and first_observed:
                selection = {**selection, "reason_code": "INVENTED_OBSERVED_NO_WINNER"}
            proofs[slot_id] = (view, tier, "0" * 64, "1" * 64, cutoff, selection)
    if before_first_completion is not None:
        publish(base + "/run.lock", label)
        publish(run["relative_path"], label)
    monkeypatch.setattr(orchestration, "declared_slot_selected_slots", lambda parent: slots)
    monkeypatch.setattr(scheduler, "_declared_native_completed_records",
        lambda *args, **kwargs: (precommit.to_dict(), slots, proofs, summaries, run, no_plan))
    owner_sha = spool.read_owned_generation_workspace(context)[1]
    account = {"schema_version": "fxpm.declared-cell-raw-account.v4", **label,
        "progress_status": "COMPLETE", "generation_id": context.generation_id,
        "generation_context_sha256": context.context_sha256,
        "declared_precommit_sha256": precommit.receipt_sha256,
        "search_process_receipt_sha256": process.receipt_sha256,
        "saved_cell_run_receipt_sha256": run["sha256"], "cell": cell,
        "account": dict(label), "qualification_authority": False, "capital_authority": False,
        "live_authority": False, "order_authority": False}
    account_entry = sink.put_json("invented/final-account", "invented_final_account", account)
    sink.seal()
    artifact = SimpleNamespace(descriptor=account_entry["descriptor"])
    return context, row, contract, config, precommit, process, artifact, run, owner_sha, contract_sha


def test_invented_pre_header_attempt_recovery_reaches_original_grid_and_retirement(tmp_path, monkeypatch):
    """Real dispatch, lease, header reader and retirement; issuer/fit/completion proofs are invented."""
    observed = {}

    def capacity_then_resume(context, precommit, process, cell, slot, body):
        request_path = (f"declared-slots/{precommit.receipt_sha256}/requests/"
                        f"{cell['cell_id']}/{slot['slot_id']}/{body['attempt_id']}.json")
        start_path = (f"declared-slots/{precommit.receipt_sha256}/{cell['cell_id']}/"
                      f"attempts/{slot['slot_id']}/{body['attempt_id']}.json")
        calls = []

        def invented_issuer(_context, **kwargs):
            assert _context is context and kwargs["attempt_id"] == body["attempt_id"]
            assert kwargs["cell_id"] == cell["cell_id"] and kwargs["slot_id"] == slot["slot_id"]
            spool._publish_exact_json(context, request_path, body, context.limits.max_json_bytes)
            return SimpleNamespace(to_dict=lambda: dict(body), receipt_sha256=scheduler._declared_m5_hash(body))

        def invented_fit_boundary(_context, **kwargs):
            assert _context is context
            assert kwargs["request"].to_dict() == body
            with execution._declared_unit_lease(context, body):
                calls.append(kwargs["request"].receipt_sha256)
                if len(calls) == 1:
                    raise MemoryError("INVENTED_SOFTWARE_PRE_HEADER_FIT_CAPACITY")
                return {"status": "NO_TRADE", "fixture_provenance": "INVENTED_OBSERVED_OWNER_COMPLETION_QA"}

        arguments = dict(precommit=precommit, process=process, frozen_pipeline_config=None,
                         cell_id=cell["cell_id"], slot=slot, symbol=cell["symbol"], attempt_id=body["attempt_id"])
        with monkeypatch.context() as patch:
            patch.setattr(orchestration, "declared_slot_producer_request", invented_issuer)
            patch.setattr(orchestration, "_release_declared_slot_producer_request", lambda *args: None)
            patch.setattr(producer, "execute_declared_slot_owner_search", invented_fit_boundary)
            with pytest.raises(MemoryError, match="INVENTED_SOFTWARE_PRE_HEADER_FIT_CAPACITY"):
                with producer._declared_slot_attempt(context, **arguments):
                    pytest.fail("capacity stop must occur before a completion is yielded")
            unit_directory = spool._local_path(context.root, execution._declared_unit_relative(body, "header.json")).parent
            assert {path.name for path in unit_directory.iterdir()} == {"units.lock"}
            assert (unit_directory / "units.lock").read_bytes() == b"0"
            assert not spool._local_path(context.root, producer._declared_commit_path(precommit, cell["cell_id"], 1)).exists()
            assert not spool._local_path(context.root, producer._declared_run_path(precommit, cell["cell_id"])).exists()
            before = {name: spool._local_path(context.root, name).read_bytes() for name in (request_path, start_path)}
            assert producer._declared_pending_unit_attempt(context, precommit, cell["cell_id"], slot) == body["attempt_id"]
            with producer._declared_slot_attempt(context, **arguments, resumed=True) as (completion, _, failure):
                assert completion["status"] == "NO_TRADE" and failure is None
            assert len(calls) == 2 and calls[0] == calls[1]
            assert {name: spool._local_path(context.root, name).read_bytes() for name in before} == before
            starts = producer._declared_prior_attempt_starts(context, precommit, cell["cell_id"], slot)
            assert len(starts) == 1 and starts[0]["relative_path"] == start_path
            observed.update(attempt_id=body["attempt_id"], original=before,
                request=dict(body), lock=execution._declared_unit_relative(body, "units.lock"))

    context, row, contract, config, precommit, process, artifact, run, owner_sha, contract_sha = (
        _invented_completed_cell(tmp_path, monkeypatch, None,
            before_first_completion=capacity_then_resume, first_observed=True))
    assert observed["attempt_id"] == "attempt-invented"
    assert all(spool._local_path(context.root, name).read_bytes() == value for name, value in observed["original"].items())
    _, slots, _, _, _, no_plan = scheduler._declared_native_completed_records(context)
    direct, locks, observations = scheduler._declared_owned_direct_files(context, precommit, row,
        max_paths=row["native_finalization"]["retirement"]["max_paths"],
        verified_no_plan=lambda slot: no_plan.get(slot["slot_id"]))
    assert set(observed["original"]).issubset(direct)
    assert locks == (observed["lock"],)
    assert observations[0]["route"] == "ORIGINAL_OBSERVED_UNIT_METADATA" and observations[0]["unit_count"] == 0
    assert slots[0]["slot_id"] not in no_plan
    assert observed["request"]["source_access_window_utc"] == {
        "start_utc": "2026-11-01T00:00:00Z", "end_exclusive_utc": "2026-12-01T00:00:00Z"}
    result = scheduler._declared_native_finalize_account(
        {"status": "REOPENED", "run_receipt": run, "account_descriptor": artifact.descriptor},
        context=context, contract=contract, row=row, config=config, precommit=precommit, process=process,
        artifact=artifact, run_body={"fixture_provenance": "INVENTED_PRE_HEADER_RECOVERY_QA"},
        workspace_sha=owner_sha, cell_contract_sha256=contract_sha)
    assert result["status"] == "ARCHIVED"
    assert len(grid.read_end_grid(tmp_path / "grid", row["cell"]["symbol"])) == 13
    archived = spool.read_generation_retirement(context.root, expected_completed_fence_schema=spool.NATIVE_COMPLETION_FENCE_SCHEMA)
    assert archived["status"] == "ARCHIVED_RECORD_ONLY"
    assert archived["retained_account_result"] == result["retained_account_result"]
    assert archived["retained_unit_locks"] == [observed["lock"]]


@pytest.mark.parametrize(("obstruction", "reason"), [
    (None, None), ("present-unit", "RETIREMENT_UNEXPECTED_UNIT_NAMESPACE"),
    ("abandoned", "RETIREMENT_NO_PLAN_VERIFICATION_REQUIRED"),
])
def test_native_unobserved_grid_and_retirement_inventory(tmp_path, monkeypatch, obstruction, reason):
    context, row, contract, config, precommit, process, artifact, run, owner_sha, contract_sha = (
        _invented_completed_cell(tmp_path, monkeypatch, obstruction))
    result = scheduler._declared_native_finalize_account(
        {"status": "REOPENED", "run_receipt": run, "account_descriptor": artifact.descriptor},
        context=context, contract=contract, row=row, config=config, precommit=precommit, process=process,
        artifact=artifact, run_body={"invented": True}, workspace_sha=owner_sha, cell_contract_sha256=contract_sha)
    rows = grid.read_end_grid(tmp_path / "grid", "XAGUSD")
    if reason:
        assert result["native_finalization_status"] == "REFUSED_RETAINED"
        assert result["native_finalization_reason_code"] == reason
        assert result["run_receipt"] == run
        assert rows == () and not (tmp_path / "fence.json").exists()
        assert not (context.root / spool.RETIREMENT_RELATIVE_PATH).exists()
    else:
        assert result["status"] == "ARCHIVED"
        assert len(rows) == 13
        assert all(row.outcome == "NO_TRADE" and row.cost_tier == "P75"
                   and row.reason_code == "REGIME_UNOBSERVED" for row in rows)
        archived = spool.read_generation_retirement(
            context.root, expected_completed_fence_schema=spool.NATIVE_COMPLETION_FENCE_SCHEMA)
        assert archived["status"] == "ARCHIVED_RECORD_ONLY"
        assert len(archived["end_grid_rows"]) == 13
        assert archived["retained_unit_locks"] == []


def test_native_shortfall_still_refuses_without_tier_proof():
    completion = {"status": "NO_TRADE", "reason_code": "SOURCE_BOUNDARY_SHORTFALL_ALL_CANDIDATES",
                  "owner": None, "frozen_plan_receipt": None}
    assert not adapter._declared_native_unobserved_no_plan(completion)
    with pytest.raises(scheduler.HistoricalSchedulerError, match="DECLARED_NATIVE_FINALIZATION_TIER_PROOF_REQUIRED"):
        scheduler._declared_native_grid_proof(None, row={"cell": {}}, slot={},
            commit={"completion": completion}, profile_sha="0" * 64, no_plan=False)


@pytest.mark.parametrize("changed", [False, True], ids=["verified-map", "changed-descriptor"])
def test_native_completed_records_joins_no_plan_to_verified_commit(monkeypatch, changed):
    cell = {"cell_id": "1" * 64, "symbol": "XAGUSD"}
    row = {"cell": cell}
    precommit = SimpleNamespace(receipt_sha256="2" * 64, to_dict=lambda: {})
    process = SimpleNamespace(receipt_sha256="3" * 64)
    context = SimpleNamespace(context_sha256="4" * 64, limits=spool.SpoolLimits())
    slots, stored, entries, summaries, schedule = [], {}, [], [], []
    previous = None
    start = datetime(2023, 6, 24, tzinfo=timezone.utc)
    for ordinal in range(1, 14):
        slot_start = start + timedelta(days=28 * (ordinal - 1))
        slot_id = f"invented-slot-{ordinal}"
        slot = {"slot_id": slot_id, "ordinal": ordinal, "activation_utc": slot_start.isoformat(),
                "end_exclusive_utc": (slot_start + timedelta(days=28)).isoformat()}
        slots.append(slot)
        completion = {"status": "NO_TRADE", "owner": None,
                      "reason_code": "REGIME_UNOBSERVED" if ordinal == 1 else "INVENTED_PLANNED_NO_TRADE",
                      "request_body": {"schema_version": orchestration.DECLARED_SLOT_REQUEST_SCHEMA_V2},
                      "plan_transport": None if ordinal == 1 else {"invented": True},
                      "request_sha256": scheduler._declared_m5_hash({"invented_request": slot_id}),
                      "completed_at_utc": "2026-11-05T00:00:00Z"}
        commit = {"schema_version": producer.DECLARED_SLOT_COMMIT_SCHEMA,
                  "generation_context_sha256": context.context_sha256, "precommit_sha256": precommit.receipt_sha256,
                  "previous_commit_sha256": previous, "cell_id": cell["cell_id"], "slot_id": slot_id,
                  "ordinal": ordinal, "completion": completion}
        previous = scheduler._declared_m5_hash(commit)
        descriptor = {"sha256": previous}
        relative = producer._declared_commit_path(precommit, cell["cell_id"], ordinal)
        stored[relative] = (commit, previous, descriptor)
        entries.append(producer._declared_commit_artifact(slot, descriptor))
        summaries.append(producer._declared_slot_summary(commit, previous))
        schedule.append({"slot_id": slot_id, "ordinal": ordinal, "start_utc": slot["activation_utc"],
                         "end_exclusive_utc": slot["end_exclusive_utc"], "status": "NO_TRADE", "owner_id": None})
    run_body = {"schema_version": producer.DECLARED_CELL_RUN_SCHEMA, "terminal_stop": False,
                "completed_slots": 13, "declared_slots": 13, "generation_context_sha256": context.context_sha256,
                "precommit_sha256": precommit.receipt_sha256, "cell_id": cell["cell_id"], "symbol_receipt": {},
                "last_commit_sha256": previous,
                "slot_commits": [{"slot_id": slot["slot_id"], "status": "NO_TRADE"} for slot in slots]}
    run = scheduler._declared_run_descriptor(precommit, row, run_body)
    account = {"progress_status": "COMPLETE", "account": {}, "generation_context_sha256": context.context_sha256,
               "declared_precommit_sha256": precommit.receipt_sha256, "search_process_receipt_sha256": process.receipt_sha256,
               "cell": cell, "declared_commit_artifacts": entries, "slot_summaries": summaries,
               "slot_schedule": schedule, "owner_records": [],
               "qualification_authority": False, "capital_authority": False, "live_authority": False,
               "order_authority": False}

    def read_record(_context, relative, **kwargs):
        if relative == run["relative_path"]:
            return run_body, run["sha256"], {"size_bytes": run["size_bytes"]}
        return {"artifacts": entries}, "6" * 64, {}

    def forbidden(*args):
        pytest.fail("a changed commit descriptor cannot supply a no-plan proof")

    seen = []

    def grid_proof(_context, *, slot, commit, no_plan, **kwargs):
        assert slot["slot_id"] == commit["slot_id"]
        seen.append((slot["slot_id"], no_plan))
        return {"invented_grid_proof": slot["slot_id"]}

    monkeypatch.setattr(orchestration, "declared_slot_selected_slots", lambda parent: slots)
    monkeypatch.setattr(producer, "_read_declared_record", read_record)
    monkeypatch.setattr(producer, "_declared_symbol_receipt", lambda *args: {})
    if changed:
        first = producer._declared_commit_path(precommit, cell["cell_id"], 1)
        stored[first] = ({}, "7" * 64, {"sha256": "7" * 64})
        monkeypatch.setattr(adapter, "_declared_native_unobserved_no_plan", forbidden)
    monkeypatch.setattr(producer, "_read_declared_commit", lambda _context, relative: stored[relative])
    monkeypatch.setattr(causal, "_read_descriptor_json", lambda *args, **kwargs: account)
    monkeypatch.setattr(causal, "_verify_declared_cell_account_manifest", lambda *args: None)
    monkeypatch.setattr(scheduler, "_declared_native_grid_proof", grid_proof)
    args = {"row": row, "precommit": precommit, "process": process,
            "artifact": SimpleNamespace(descriptor={"sha256": "8" * 64}),
            "run_body": run_body, "profile_sha": "9" * 64}
    if changed:
        with pytest.raises(scheduler.HistoricalSchedulerError, match="DECLARED_NATIVE_FINALIZATION_SLOT_PROOF_REQUIRED"):
            scheduler._declared_native_completed_records(context, **args)
        assert seen == []
    else:
        _, selected, proofs, actual_summaries, saved, no_plan = (
            scheduler._declared_native_completed_records(context, **args))
        assert selected == slots and actual_summaries == summaries and saved == run
        assert list(proofs) == [slot["slot_id"] for slot in slots]
        assert seen == [(slot["slot_id"], slot["ordinal"] == 1) for slot in slots]
        assert no_plan == {slots[0]["slot_id"]: {"request_sha256": summaries[0]["request_sha256"]}}
