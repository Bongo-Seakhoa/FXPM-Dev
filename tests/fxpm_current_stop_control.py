"""Current full-panel post-freeze STOP control on owned invented inputs.  Software only.

Final location: tests/fxpm_current_stop_control.py (lane 271 apply map).

One labelled fault is raised at the first partition boundary, after the real freeze is saved and before any real
partition or backtest runs.  A cold-reopen capsule is written only when the committed completion is exactly the
expected STOP.  Any other outcome fails by assertion and leaves no capsule.  Root launch only; the protocol must
authorise it.  No market observation, no model evidence, no qualification, capital, live or order authority.
"""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(os.environ["FXPM_QA_SOURCE_IMAGE_ROOT"]).resolve(strict=True)
AUTHORITATIVE_CHECKOUT = Path(os.environ["FXPM_QA_AUTHORITATIVE_CHECKOUT"]).resolve(strict=True)
QA = AUTHORITATIVE_CHECKOUT / "audit_reports" / "fxpm_workshop_20261007" / "declared_slot_owner_surface_verification"
COLD_REOPEN = Path(__file__).with_name("fxpm_current_stop_cold_reopen.py")
PROTOCOL_ENV = "FXPM_QA_CURRENT_STOP_PROTOCOL"
PROTOCOL_SHA256_ENV = "FXPM_QA_CURRENT_STOP_PROTOCOL_SHA256"
CLASSIFICATION = "INVENTED_CURRENT_FULL_PANEL_POSTFREEZE_STOP_SOFTWARE_ONLY"
POSTFREEZE_FAULT_CODE = "QA_DECLARED_POSTFREEZE_PROOF"
POSTFREEZE_FAULT_PROTOCOL = {
    "classification": "INVENTED_TEST_ONLY_POSTFREEZE_FAILURE_NOT_AN_ECONOMIC_OUTCOME",
    "trigger": "FIRST_PARTITION_BOUNDARY_AFTER_SAVED_FREEZE_BEFORE_REAL_PARTITION",
    "required_hook_site": "_run_tier_partition",
    "failure_code": POSTFREEZE_FAULT_CODE,
    "expected_injection_count": 1,
    "expected_tier_partition_calls": 0,
    "expected_backtest_run_calls": 0,
    "qualification_authority": False,
}
_AUTHORITY_FLAGS = ("qualification_authority", "capital_authority", "live_authority", "order_authority")
_FROZEN_FIELDS = ("frozen_plan_receipt", "frozen_descriptor_graph", "horizon_winner_cross_binding")
_PRODUCTION_FIELDS = ("owner", "winner_request_entry", "winner_decision_entry", "raw_production_receipt",
    "production_descriptor_graph", "winner_request_sha256", "winner_decision_sha256")


def identity(path):
    digest, size = hashlib.sha256(), 0
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return {"sha256": digest.hexdigest(), "bytes": size}


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, allow_nan=False, ensure_ascii=False, sort_keys=True, indent=2)
        stream.flush()
        os.fsync(stream.fileno())


def _protocol(*, controlled_postfreeze_fault=True):
    assert controlled_postfreeze_fault is True, "CURRENT_CONTROL_IS_POSTFREEZE_STOP_ONLY"
    path = Path(os.environ[PROTOCOL_ENV]).resolve(strict=True)
    assert path.is_relative_to(QA), "OWNED_CURRENT_STOP_PROTOCOL_REQUIRED"
    assert identity(path)["sha256"] == os.environ[PROTOCOL_SHA256_ENV]
    body = json.loads(path.read_text(encoding="utf-8"))
    assert body["status"] == "FROZEN_FOR_ROOT_LAUNCH" and body["execution_authorized"] is True
    assert body["execution_owner"] == "root only" and body["classification"] == CLASSIFICATION
    assert body.get("controlled_postfreeze_fault") == POSTFREEZE_FAULT_PROTOCOL
    first = body["first_slot"]
    assert first["expected_status"] == "STOP" and first["expected_reason_code"] == POSTFREEZE_FAULT_CODE
    assert first["source_fault"] == "NONE" and first["expected_attempt_count"] == 300
    return path, body


@contextmanager
def _postfreeze_fault_scope(*, sink, request):
    """Raise once at the first partition boundary; never delegate to the real partition or backtest."""
    import pm_authoritative_search_execution as execution

    original_build = execution._build_planned_attempt_evidence
    original_partition = execution._run_tier_partition
    original_run = execution.Backtester.run
    body = request.to_dict()
    frozen_id = (f"{body['artifact_namespace']}/winner-search/"
        f"{body['timeframe']}/{body['regime']}/frozen/receipt")
    observed = {**POSTFREEZE_FAULT_PROTOCOL, "injection_count": 0, "hook_site": None,
        "tier_partition_calls": 0, "backtest_run_calls": 0, "frozen_receipt_descriptor": None,
        "triggered_after_freeze": False, "hooks_in_place_at_exit": False}

    def inject(site):
        entry = sink._artifacts.get(frozen_id)
        assert entry is not None, "CONTROLLED_FAULT_REQUIRES_REAL_FREEZE"
        observed["injection_count"] += 1
        assert observed["injection_count"] == 1, "CONTROLLED_FAULT_FIRED_MORE_THAN_ONCE"
        observed["hook_site"] = site
        observed["frozen_receipt_descriptor"] = dict(entry["descriptor"])
        observed["triggered_after_freeze"] = True
        raise execution.AuthoritativeSearchExecutionError(POSTFREEZE_FAULT_CODE)

    def partition_boundary(*args, **kwargs):
        # The common boundary of both routes.  The real partition is never called from here.
        inject("_run_tier_partition")

    def ordinary_builder(*args, **kwargs):
        # Tripwire only: the ordinary route is not the route under test.  It never delegates, and the completion
        # verifier refuses a capsule whose fault fired here.
        inject("_build_planned_attempt_evidence")

    def forbidden_backtest_run(self, *args, **kwargs):
        observed["backtest_run_calls"] += 1
        raise AssertionError("CONTROLLED_STOP_REACHED_BACKTEST_RUN")

    execution._build_planned_attempt_evidence = ordinary_builder
    execution._run_tier_partition = partition_boundary
    execution.Backtester.run = forbidden_backtest_run
    try:
        yield observed
    finally:
        # Every caller resolves these names from the module at call time, so while the hooks are bound no caller
        # can reach the real partition; the backtest guard is the independent second fact.
        observed["hooks_in_place_at_exit"] = (
            execution._run_tier_partition is partition_boundary
            and execution._build_planned_attempt_evidence is ordinary_builder
            and execution.Backtester.run is forbidden_backtest_run)
        execution._build_planned_attempt_evidence = original_build
        execution._run_tier_partition = original_partition
        execution.Backtester.run = original_run


def _verify_postfreeze_fault_completion(completion, observed):
    assert observed["injection_count"] == 1 and observed["triggered_after_freeze"] is True
    assert observed["hook_site"] == POSTFREEZE_FAULT_PROTOCOL["required_hook_site"], (
        "CONTROLLED_FAULT_FIRED_OFF_THE_COMMON_PARTITION_BOUNDARY")
    assert observed["tier_partition_calls"] == 0, "CONTROLLED_FAULT_EVALUATED_A_TIER"
    assert observed["backtest_run_calls"] == 0, "CONTROLLED_FAULT_REACHED_A_BACKTEST"
    assert observed["hooks_in_place_at_exit"] is True, "CONTROLLED_FAULT_HOOKS_WERE_REPLACED"
    assert (completion["status"], completion["reason_code"]) == ("STOP", POSTFREEZE_FAULT_CODE)
    assert all(completion.get(name) is not None for name in _FROZEN_FIELDS)
    assert completion["frozen_plan_receipt"] == observed["frozen_receipt_descriptor"]
    assert all(completion.get(name) is None for name in _PRODUCTION_FIELDS)
    assert len(completion["plan_transport"]["attempt_artifacts"]) == 300
    assert all(completion[name] is False for name in _AUTHORITY_FLAGS)


def run_first_slot(work, *, controlled_postfreeze_fault=True):
    """Run once on fresh invented inputs; commit the first declared slot only if it is the expected STOP."""
    protocol_path, protocol = _protocol(controlled_postfreeze_fault=controlled_postfreeze_fault)
    work = Path(work).resolve()
    work.mkdir(parents=True, exist_ok=True)
    import pytest
    import pm_authority_spool as spool
    import pm_historical_anchor_orchestration as orchestration
    import pm_historical_search_producer as producer
    import pm_production_search_planner as planner
    from tests.test_authoritative_search_execution import (
        _build_owned_invented_declared_inputs,
        _invented_fixture_external_io_guard,
        _reopen_invented_native_source_for_new_issuers,
        _route_a_owned_utc_window,
    )

    with pytest.MonkeyPatch.context() as outer:
        with _invented_fixture_external_io_guard(outer) as external_io_attempts:
            with pytest.MonkeyPatch.context() as scoped:
                # Only the builder's ordinary constructor call is redirected; the full-panel freezer and its
                # verifier run unchanged.  No other edition is tried.
                scoped.setattr(planner, "freeze_production_search_process",
                    planner.freeze_route_a_full_panel_production_search_process)
                reused = os.environ.get("FXPM_QA_REUSED_INVENTED_NATIVE_ROOT")
                native_fixture = (_reopen_invented_native_source_for_new_issuers(work, reused)
                    if reused else None)
                env = _build_owned_invented_declared_inputs(work,
                    source_access_window_utc=_route_a_owned_utc_window(), native_fixture=native_fixture)
            context, process, precommit = env.context, env.process, env.precommit
            frozen, cell_id = env.frozen_pipeline_config, env.cell_id
            assert process.to_dict()["schema_version"] == planner.ROUTE_A_FULL_PANEL_PROCESS_SCHEMA
            assert (planner.frozen_search_execution_edition(context, process)
                == planner.ROUTE_A_FULL_PANEL_EXECUTION_EDITION), "CURRENT_CONTROL_REQUIRES_FULL_PANEL_EDITION"
            config_path = Path(frozen.active_config_path).resolve(strict=True)
            assert config_path.is_relative_to(work), "OWNED_INVENTED_CONFIG_REQUIRED"
            assert Path(context.root).resolve(strict=True).is_relative_to(work), "OWNED_INVENTED_SPOOL_REQUIRED"
            committed_precommit = orchestration.verify_declared_slot_owner_run_precommit(
                context, precommit, process=process)
            cell = next(row for row in committed_precommit["cells"] if row["cell_id"] == cell_id)
            slots = committed_precommit["slot_roster"]["slots"]
            assert len(slots) == 13, "CURRENT_CONTROL_REQUIRES_FULL_SLOT_ROSTER"
            slot = slots[0]
            assert slot["ordinal"] == 1, "CURRENT_CONTROL_COMMITS_THE_FIRST_DECLARED_SLOT"
            write_json(work / "issued-control.json", {"classification": CLASSIFICATION,
                "protocol_identity": identity(protocol_path), "precommit_sha256": precommit.receipt_sha256,
                "process_receipt_sha256": process.receipt_sha256,
                "process_schema_version": process.to_dict()["schema_version"],
                "cell": {"cell_id": cell_id, "symbol": cell["symbol"], "timeframe": cell["timeframe"],
                    "regime": cell["regime"]},
                "slot_id": slot["slot_id"], "slot_ordinal": slot["ordinal"],
                "config_file_identity": identity(config_path),
                "effective_config_sha256": frozen.effective_config_sha256,
                "controlled_postfreeze_fault": protocol["controlled_postfreeze_fault"],
                "fixture_provenance": env.fixture_provenance})
            sink = spool.open_symbol_sink(context, cell["symbol"])
            request = orchestration.declared_slot_producer_request(context, precommit=precommit,
                process=process, cell_id=cell_id, slot_id=slot["slot_id"],
                attempt_id="current-full-panel-postfreeze-stop-control", sink=sink)
            try:
                with _postfreeze_fault_scope(sink=sink, request=request) as fault_observation:
                    completion = producer.execute_declared_slot_owner_search(context, request=request,
                        process=process, frozen_pipeline_config=frozen)
                _verify_postfreeze_fault_completion(completion, fault_observation)
                write_json(work / "actual-first-slot-completion.json", completion)
                prefix = request.to_dict()["artifact_namespace"] + "/"
                roster_entries = [entry for entry in sink._artifacts.values()
                    if entry["artifact_id"].startswith(prefix)]
                write_json(work / "postfreeze-fault-observation.json", {**fault_observation,
                    "completion_status": completion["status"], "completion_reason_code": completion["reason_code"],
                    "canonical_plan_attempt_count": len(completion["plan_transport"]["attempt_artifacts"]),
                    "slot_sink_artifact_ids": sorted(sink._artifacts),
                    "selected_commit_roster_artifact_ids": sorted(entry["artifact_id"] for entry in roster_entries)})
                summary, descriptor = producer._commit_declared_slot_completion(context, precommit=precommit,
                    process=process, cell_id=cell_id, slot=slot, completion=completion,
                    artifact_roster=roster_entries, previous_commit_sha256=None, frozen_pipeline_config=frozen)
                committed, digest, saved_descriptor = producer._read_declared_commit(context,
                    producer._declared_commit_path(precommit, cell_id, slot["ordinal"]))
                assert saved_descriptor == descriptor and digest == descriptor["sha256"]
                assert committed["completion"]["status"] == "STOP"
                assert committed["completion"]["reason_code"] == POSTFREEZE_FAULT_CODE
                del summary
                committed_ids = [entry["artifact_id"] for entry in committed["artifact_roster"]]
                assert len(committed_ids) == len(set(committed_ids))
                assert set(committed_ids) == set(sink._artifacts), "CONTROLLED_STOP_ROSTER_LOST_SLOT_ARTIFACTS"
            finally:
                orchestration._release_declared_slot_producer_request(request, context)
            assert not external_io_attempts
    write_json(work / "actual-first-slot-result.json", {"classification": CLASSIFICATION,
        "status": completion["status"], "reason_code": completion["reason_code"],
        "planned_attempt_count": len(completion["plan_transport"]["attempt_artifacts"]),
        "slot_entry_count": len(roster_entries), "commit_sha256": digest,
        "commit_bytes": descriptor["size_bytes"], "post_completion_failure": None,
        "expected_owner_observed": False, "qualification_authority": False,
        "controlled_postfreeze_fault": fault_observation, "committed_roster_matches_slot_sink": True})
    _cold_reopen(work, context=context, config_path=config_path, frozen=frozen, process=process,
        precommit=precommit, cell_id=cell_id, slot=slot, completion=completion)
    _verify_postfreeze_fault_completion(completion, fault_observation)
    return work


def _cold_reopen(work, *, context, config_path, frozen, process, precommit, cell_id, slot, completion):
    import pm_authority_spool as spool
    import pm_historical_search_producer as producer
    import pm_production_search_planner as planner

    paths = {f"declared-slots/{precommit.receipt_sha256}/precommit.json",
        process.spool_receipt["symbol_manifest"]["relative_path"]}
    manifest = spool.verify_symbol_receipt(context.root, process.spool_receipt,
        expected_generation_id=context.generation_id, expected_symbol=planner.PROCESS_SPOOL_SYMBOL,
        expected_native_universe_sha256=context.native_universe_sha256, expected_config_sha256=context.config_sha256)
    paths.update(entry["descriptor"]["relative_path"] for entry in manifest["artifacts"])
    commit_relative = producer._declared_commit_path(precommit, cell_id, slot["ordinal"])
    paths.add(commit_relative)
    _, _, commit_descriptor = producer._read_declared_commit(context, commit_relative)
    paths.add(commit_descriptor["relative_path"])
    issued = {relative: identity(context.root / relative) for relative in sorted(paths)}
    progress = producer.reopen_declared_cell_owner_progress(context, precommit=precommit, process=process,
        cell_id=cell_id, frozen_pipeline_config=frozen)
    terminal_path = context.root / producer._declared_terminal_path(precommit, cell_id)
    if terminal_path.exists():
        issued[terminal_path.relative_to(context.root).as_posix()] = identity(terminal_path)
    body = precommit.to_dict()
    capsule = {"classification": CLASSIFICATION, "source_identity": body["source_identity"],
        "source_image_root": str(ROOT), "authoritative_checkout": str(AUTHORITATIVE_CHECKOUT),
        "generation_root": str(context.root), "generation_id": context.generation_id,
        "native_universe_sha256": context.native_universe_sha256, "config_sha256": context.config_sha256,
        "config_path": str(config_path), "config_file_identity": identity(config_path),
        "effective_config_sha256": frozen.effective_config_sha256, "run_id": process.to_dict()["run_id"],
        "process_receipt_sha256": process.receipt_sha256, "process_spool_receipt": process.spool_receipt,
        "process_schema_version": process.to_dict()["schema_version"],
        "precommit_sha256": precommit.receipt_sha256, "launch_activation_utc": body["launch_activation_utc"],
        "protected_end_exclusive_utc": body["protected_end_exclusive_utc"],
        "cell_id": cell_id, "expected_statuses": [completion["status"]],
        "terminal_stop": progress.terminal_stop, "issued_files": issued,
        "commit_files": {commit_relative: identity(context.root / commit_relative)},
        "commit_descriptors": [commit_descriptor], "finalized": False,
        "expected_symbol_receipt": None, "expected_run_receipt": None}
    cold_env = os.environ.copy()
    assert Path(cold_env["FXPM_QA_SOURCE_IMAGE_ROOT"]).resolve(strict=True) == ROOT
    assert Path(cold_env["FXPM_QA_AUTHORITATIVE_CHECKOUT"]).resolve(strict=True) == AUTHORITATIVE_CHECKOUT
    cold_env["FXPM_QA_SOURCE_IMAGE_ROOT"] = str(ROOT)
    cold_env["FXPM_QA_AUTHORITATIVE_CHECKOUT"] = str(AUTHORITATIVE_CHECKOUT)
    capsule_path = work / "cold-reopen-capsule.json"
    refused_path = work / "cold-reopen-capsule.REFUSED.json"
    capsule_created, capsule_identity, observed_exit = False, None, None
    try:
        with capsule_path.open("x", encoding="utf-8") as stream:
            capsule_created = True
            created = os.fstat(stream.fileno())
            capsule_identity = (created.st_dev, created.st_ino)
            json.dump(capsule, stream, allow_nan=False, ensure_ascii=False, sort_keys=True, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        command = [sys.executable, "-B", str(COLD_REOPEN), "--capsule", str(capsule_path),
            "--capsule-sha256", identity(capsule_path)["sha256"], "--output", str(work / "cold-reopen-result.json")]
        with (work / "cold-reopen.stdout.log").open("x", encoding="utf-8") as out, \
                (work / "cold-reopen.stderr.log").open("x", encoding="utf-8") as err:
            result = subprocess.run(command, cwd=ROOT, env=cold_env, stdout=out, stderr=err)
            observed_exit = result.returncode
        write_json(work / "cold-reopen-exit.json", {"command": command, "exit_code": observed_exit})
        if observed_exit != 0:
            raise AssertionError("CURRENT_COLD_REOPEN_FAILED; inspect retained cold logs")
    except BaseException as original:
        if observed_exit is not None:
            try:
                original.add_note("Cold child exit observed: " + str(observed_exit))
            except BaseException:
                pass
        if capsule_created:
            try:
                current = capsule_path.stat(follow_symlinks=False)
                if capsule_identity is None or (current.st_dev, current.st_ino) != capsule_identity:
                    raise RuntimeError("Capsule path ownership changed; preserve the unknown path")
                os.link(capsule_path, refused_path)
                retained = refused_path.stat(follow_symlinks=False)
                current = capsule_path.stat(follow_symlinks=False)
                if ((retained.st_dev, retained.st_ino) != capsule_identity
                        or (current.st_dev, current.st_ino) != capsule_identity):
                    raise RuntimeError("Capsule path changed during refusal retention; preserve both paths")
                capsule_path.unlink()
            except BaseException as refusal_error:
                try:
                    original.add_note("Could not retain the owned capsule as REFUSED: " + type(refusal_error).__name__)
                except BaseException:
                    pass
        raise
