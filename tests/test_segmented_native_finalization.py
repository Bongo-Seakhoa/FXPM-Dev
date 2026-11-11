"""Synthetic native-finalization routing and segmented retirement boundaries."""

from copy import deepcopy
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import uuid

import pytest

import pm_authority_spool as spool
import pm_end_grid as grid
import pm_historical_anchor_orchestration as orchestration
import pm_historical_scheduler as scheduler


def _policy(parent):
    return {"end_grid_root": str(parent / "grid"), "source_fence_path": str(parent / "fence.json"),
            "retirement": {"receipt_path": str(parent / "receipt.json"),
                           "max_paths": 100, "max_nodes": 300, "max_bytes": 4 * 1024**2}}


def test_native_policy_opt_in_is_one_exact_optional_key(tmp_path):
    original = _policy(tmp_path)
    four = scheduler._declared_native_finalization_paths(original)
    for segment_size in (1, 2, 181817):
        five = deepcopy(original)
        five["retirement"]["segment_max_records"] = segment_size
        assert scheduler._declared_native_finalization_paths(five) == four
    assert set(original["retirement"]) == {"receipt_path", "max_paths", "max_nodes", "max_bytes"}
    unknown = deepcopy(original)
    unknown["retirement"]["segments"] = 2
    with pytest.raises(scheduler.HistoricalSchedulerError, match="FINALIZATION_POLICY_INVALID"):
        scheduler._declared_native_finalization_paths(unknown)


@pytest.mark.parametrize("value", [0, -1, 181818, True, 2.0, "2", None])
def test_native_segment_size_refuses_invalid_types_and_boundaries(tmp_path, value):
    policy = _policy(tmp_path)
    policy["retirement"]["segment_max_records"] = value
    with pytest.raises(scheduler.HistoricalSchedulerError, match="FINALIZATION_POLICY_INVALID"):
        scheduler._declared_native_finalization_paths(policy)


@pytest.fixture
def completed_cell(monkeypatch):
    retained = Path(os.environ["FXPM_DERIVED_QA_ROOT"])
    assert retained.is_absolute() and retained.is_dir()
    spool._reject_reparse_chain(retained)
    parent = retained / ("segmented_native_" + uuid.uuid4().hex)
    parent.mkdir(exist_ok=False)
    helper = importlib.import_module("tests.test_native_regime_unobserved_finalization_001")
    result = helper._invented_completed_cell(parent, monkeypatch, None)
    context, row, contract, config, precommit, process, artifact, run, _, _ = result
    row["descriptor_index"] = 0
    row["native_finalization"]["retirement"]["segment_max_records"] = 2
    contract.update(schema_version=scheduler.DECLARED_SCHEDULER_SCHEMA,
                    native_universe_sha256="2" * 64, config_sha256="3" * 64, cells=[row])
    monkeypatch.setattr(scheduler, "_declared_native_spool_limits", lambda expected: context.limits)

    def bind_owner():
        digest = scheduler._declared_native_cell_contract_sha256(contract, row, context.limits)
        path = context.root / spool.WORKSPACE_OWNER_RELATIVE_PATH
        owner = json.loads(path.read_bytes())
        owner["cell_contract_sha256"] = digest
        path.write_bytes(spool._canonical_json_bytes(owner, spool.MAX_CONTEXT_BYTES))
        return spool.read_owned_generation_workspace(context)[1], digest

    env = SimpleNamespace(context=context, row=row, contract=contract, config=config, precommit=precommit,
        process=process, artifact=artifact, run=run, parent=parent, bind_owner=bind_owner)
    env.owner_sha, env.contract_sha = bind_owner()
    return env


def _finalize(env, *, parallel_files=()):
    result = scheduler._declared_cell_row(env.row, "REOPENED", pid=os.getpid())
    result.update(run_receipt=env.run, account_descriptor=env.artifact.descriptor,
                  precommit_sha256=env.precommit.receipt_sha256)
    return scheduler._declared_native_finalize_account(result,
        context=env.context, contract=env.contract, row=env.row, config=env.config,
        precommit=env.precommit, process=env.process, artifact=env.artifact,
        run_body={"fixture_provenance": "INVENTED_COMPLETED_RECORD_JOIN"}, workspace_sha=env.owner_sha,
        cell_contract_sha256=env.contract_sha, parallel_files=parallel_files)


def _assert_no_finalization_writes(env):
    assert grid.read_end_grid(env.parent / "grid", "XAGUSD") == ()
    assert not (env.parent / "fence.json").exists()
    assert not (env.parent / "retirement.json").exists()
    assert not (env.context.root / spool.RETIREMENT_RELATIVE_PATH).exists()
    assert not (env.context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH).exists()
    assert not list(env.context.root.glob("generation-retirement.segment.*.json"))


def test_native_finalization_v2_s2_reaches_confirmed_archive(completed_cell):
    env = completed_cell
    result = _finalize(env)
    assert result["status"] == "ARCHIVED"
    assert result["source_kind"] == "broker_native"
    assert result["run_receipt"] is None and result["account_descriptor"] is None
    assert len(grid.read_end_grid(env.parent / "grid", "XAGUSD")) == 13
    assert scheduler._declared_confirm_receipt(env.contract, result) is None
    assert result["archive_record_confirmed"] is True
    retained = spool.read_generation_retirement(env.context.root,
        expected_completed_fence_schema=spool.NATIVE_COMPLETION_FENCE_SCHEMA)
    assert retained["intent_segment_count"] > 1
    assert retained["cell_contract_sha256"] == env.contract_sha
    assert retained["precommit_sha256"] == env.precommit.receipt_sha256
    assert all(retained[name] is False for name in (
        "qualification_authority", "capital_authority", "live_authority", "order_authority"))


@pytest.mark.parametrize("segmented", [False, True], ids=["four-key-retained-refusal", "five-key-retained-refusal"])
def test_path_bound_refuses_before_any_native_finalization_write(completed_cell, segmented):
    env = completed_cell
    retirement = env.row["native_finalization"]["retirement"]
    retirement["max_paths"] = 1
    if not segmented:
        retirement.pop("segment_max_records")
    env.owner_sha, env.contract_sha = env.bind_owner()
    before = {path: path.read_bytes() for path in env.context.root.rglob("*") if path.is_file()}
    result = _finalize(env)
    assert result["native_finalization_status"] == "REFUSED_RETAINED"
    assert result["native_finalization_reason_code"] in {
        "RETIREMENT_DIRECT_PATH_INVALID", "RETIREMENT_PAYLOAD_BOUND_EXCEEDED"}
    assert result["run_receipt"] == env.run
    assert result["account_descriptor"] == env.artifact.descriptor
    _assert_no_finalization_writes(env)
    assert {path: path.read_bytes() for path in before} == before


def test_twenty_six_parallel_files_count_toward_the_same_total_path_bound(completed_cell):
    env = completed_cell
    no_plan = scheduler._declared_native_completed_records(env.context, row=env.row,
        precommit=env.precommit, process=env.process, artifact=env.artifact,
        run_body={}, profile_sha=env.contract["profile_sha256"])[-1]
    direct, locks, _ = scheduler._declared_owned_direct_files(env.context, env.precommit, env.row,
        max_paths=100, verified_no_plan=lambda slot: no_plan[slot["slot_id"]])
    before, _, _, _ = spool._retirement_inventory(env.context.root, frozenset(direct),
        max_paths=100, max_nodes=300, max_bytes=4 * 1024**2, retained_unit_locks=locks)
    parallel_files = []
    for ordinal in range(13):
        for kind in ("claim", "result"):
            relative = f"invented-parallel/slot-{ordinal + 1:02d}.{kind}.json"
            path = env.context.root / relative
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(b'{"fixture_provenance":"INVENTED_PARALLEL_CUSTODY"}')
            parallel_files.append(relative)
    env.row["native_finalization"]["retirement"]["max_paths"] = len(before) + 25
    env.owner_sha, env.contract_sha = env.bind_owner()
    result = _finalize(env, parallel_files=tuple(parallel_files))
    assert result["native_finalization_status"] == "REFUSED_RETAINED"
    assert result["native_finalization_reason_code"] == "RETIREMENT_PAYLOAD_BOUND_EXCEEDED"
    _assert_no_finalization_writes(env)
    assert all((env.context.root / name).exists() for name in parallel_files)


def test_five_key_preflight_accounts_for_control_segments_before_writes(completed_cell):
    env = completed_cell
    no_plan = scheduler._declared_native_completed_records(env.context, row=env.row,
        precommit=env.precommit, process=env.process, artifact=env.artifact, run_body={},
        profile_sha=env.contract["profile_sha256"])[-1]
    direct, locks, _ = scheduler._declared_owned_direct_files(env.context, env.precommit, env.row,
        max_paths=100, verified_no_plan=lambda slot: no_plan[slot["slot_id"]])
    _, _, _, nodes = spool._retirement_inventory(env.context.root, frozenset(direct),
        max_paths=100, max_nodes=300, max_bytes=4 * 1024**2, retained_unit_locks=locks)
    env.row["native_finalization"]["retirement"]["max_nodes"] = nodes + 3
    env.owner_sha, env.contract_sha = env.bind_owner()
    result = _finalize(env)
    assert result["native_finalization_status"] == "REFUSED_RETAINED"
    assert result["native_finalization_reason_code"] == "RETIREMENT_CONTROL_CAPACITY"
    _assert_no_finalization_writes(env)


def test_five_key_protected_source_list_is_stable_across_import_state(completed_cell, monkeypatch):
    env = completed_cell
    before = scheduler._declared_native_finalization_preflight(env.contract, env.row, env.config,
        env.context.limits, fresh=False)
    monkeypatch.delitem(sys.modules, "pm_training_horizon", raising=False)
    after = scheduler._declared_native_finalization_preflight(env.contract, env.row, env.config,
        env.context.limits, fresh=False)
    assert after == before
    assert any(path.name == "pm_training_horizon.py" for path in after)


def test_m5_fixture_authenticates_its_constructor_source_and_keeps_four_keys():
    helper = importlib.import_module("tests.test_m5_development_scheduler")
    fixture = helper.M5DevelopmentSchedulerTests("test_archived_projection_drops_working_receipts_and_denies_cold_reopen")
    fixture.setUp()
    contract = fixture.declaration()
    checked, _ = scheduler._declared_m5_contract(contract)
    source = Path(orchestration.__file__).with_name("pm_authority_spool.py")
    actual = hashlib.sha256(source.read_bytes()).hexdigest()
    constructors = checked["cells"][0]["m5_development"]["m5_source_pins"]["constructor_identity"]
    assert constructors["pm_authority_spool.py"] == actual
    changed = deepcopy(contract)
    changed["cells"][0]["m5_development"]["retirement"]["segment_max_records"] = 2
    with pytest.raises(scheduler.HistoricalSchedulerError,
                       match="DECLARED_M5_RETIREMENT_POLICY_REQUIRED"):
        scheduler._declared_m5_contract(changed)

def _interrupted_native_preparation(env, monkeypatch, stage):
    original = spool._retirement_publish_v2
    interrupted = False

    def interrupt_after_control(root, relative, *args, **kwargs):
        nonlocal interrupted
        digest = original(root, relative, *args, **kwargs)
        hit = ((stage == "intent" and relative == spool.RETIREMENT_INTENT_RELATIVE_PATH)
            or (stage == "segment" and relative.startswith("generation-retirement.segment.")))
        if hit and not interrupted:
            interrupted = True
            raise RuntimeError("INVENTED_INTERRUPTED_NATIVE_RETIREMENT_PREPARATION")
        return digest

    with monkeypatch.context() as fault:
        fault.setattr(spool, "_retirement_publish_v2", interrupt_after_control)
        result = _finalize(env)
    assert interrupted
    assert result["status"] == "NOT_RUN"
    assert result["reason_code"] == "RETIREMENT_INCOMPLETE"
    assert result["run_receipt"] is None and result["account_descriptor"] is None
    assert not (env.context.root / spool.RETIREMENT_RELATIVE_PATH).exists()
    intent_path = env.context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH
    saved = json.loads(intent_path.read_bytes())["arguments"]
    return {
        **saved,
        "expected_context_sha256": env.context.context_sha256,
        "source_fence_path": Path(saved["source_fence_path"]),
        "retirement_receipt_path": Path(saved["retirement_receipt_path"]),
        "protected_paths": tuple(Path(value) for value in saved["protected_paths"]),
        "owned_direct_files": tuple(saved["owned_direct_files"]),
        "unit_lock_paths": tuple(saved["unit_lock_paths"]),
    }


@pytest.mark.parametrize("stage", ["intent", "segment"])
def test_prepared_native_retirement_requires_explicit_public_resume(
    completed_cell, monkeypatch, stage
):
    env = completed_cell
    resume_args = _interrupted_native_preparation(env, monkeypatch, stage)
    snapshot = {path: path.read_bytes()
        for path in env.parent.rglob("*") if path.is_file()}

    def unexpected_work(*args, **kwargs):
        raise AssertionError("ordinary dispatch must leave pending preparation intact")

    with monkeypatch.context() as guard:
        guard.setattr(spool, "_retirement_inventory", unexpected_work)
        deferred = _finalize(env)
        assert deferred["status"] == "NOT_RUN"
        assert deferred["reason_code"] == "RETIREMENT_INCOMPLETE"
        assert deferred["run_receipt"] is None and deferred["account_descriptor"] is None
        with pytest.raises(scheduler.HistoricalSchedulerError, match="RETIREMENT_INCOMPLETE"):
            scheduler._declared_native_retirement_preflight(
                env.context, env.owner_sha, protected=resume_args["protected_paths"],
                direct=resume_args["owned_direct_files"], locks=resume_args["unit_lock_paths"],
                fence_path=resume_args["source_fence_path"],
                fence_sha=resume_args["source_fence_sha256"],
                retirement=env.row["native_finalization"]["retirement"],
            )
        guard.setattr(spool, "reopen_generation_context", unexpected_work)
        guard.setattr(scheduler, "_declared_native_finalize_account", unexpected_work)
        refused = scheduler._declared_run_one_cell(env.contract, env.row)
    assert refused["status"] == "NOT_RUN"
    assert refused["reason_code"] == "RETIREMENT_INCOMPLETE"
    assert {path: path.read_bytes()
        for path in env.parent.rglob("*") if path.is_file()} == snapshot

    record = spool.retire_completed_generation(env.context.root, **resume_args)
    assert record["status"] == "ARCHIVED_RECORD_ONLY"
    archived = scheduler._declared_run_one_cell(env.contract, env.row)
    assert archived["status"] == "ARCHIVED"
    assert scheduler._declared_confirm_receipt(env.contract, archived) is None
    assert archived["archive_record_confirmed"] is True


@pytest.mark.parametrize("mutation", ["changed-intent", "unplanned-segment"])
def test_pending_native_dispatch_does_not_authenticate_or_remove_changed_controls(
    completed_cell, monkeypatch, mutation
):
    env = completed_cell
    resume_args = _interrupted_native_preparation(env, monkeypatch, "segment")
    if mutation == "changed-intent":
        (env.context.root / spool.RETIREMENT_INTENT_RELATIVE_PATH).write_bytes(b"{}")
    else:
        (env.context.root / ("generation-retirement.segment." + "f" * 64 + ".json")).write_bytes(b"{}")
    snapshot = {path: path.read_bytes()
        for path in env.parent.rglob("*") if path.is_file()}
    refused = scheduler._declared_run_one_cell(env.contract, env.row)
    assert refused["status"] == "NOT_RUN"
    assert refused["reason_code"] == "RETIREMENT_INCOMPLETE"
    with pytest.raises(spool.AuthoritySpoolError):
        spool.retire_completed_generation(env.context.root, **resume_args)
    assert {path: path.read_bytes()
        for path in env.parent.rglob("*") if path.is_file()} == snapshot



def test_four_key_payload_count_refuses_after_direct_count_fits(completed_cell):
    env = completed_cell
    retirement = env.row["native_finalization"]["retirement"]
    retirement.pop("segment_max_records")
    env.owner_sha, env.contract_sha = env.bind_owner()
    no_plan = scheduler._declared_native_completed_records(env.context, row=env.row,
        precommit=env.precommit, process=env.process, artifact=env.artifact,
        run_body={}, profile_sha=env.contract["profile_sha256"])[-1]
    direct, locks, _ = scheduler._declared_owned_direct_files(
        env.context, env.precommit, env.row, max_paths=100,
        verified_no_plan=lambda slot: no_plan[slot["slot_id"]])
    records, _, _, _ = spool._retirement_inventory(
        env.context.root, frozenset(direct), max_paths=100,
        max_nodes=300, max_bytes=4 * 1024**2, retained_unit_locks=locks)
    retirement["max_paths"] = len(records) - 1
    assert len(direct) <= retirement["max_paths"] < len(records)
    env.owner_sha, env.contract_sha = env.bind_owner()
    before = {path: path.read_bytes()
        for path in env.parent.rglob("*") if path.is_file()}

    result = _finalize(env)

    assert result["native_finalization_status"] == "REFUSED_RETAINED"
    assert result["native_finalization_reason_code"] == "RETIREMENT_PAYLOAD_BOUND_EXCEEDED"
    assert result["run_receipt"] == env.run
    assert result["account_descriptor"] == env.artifact.descriptor
    _assert_no_finalization_writes(env)
    assert {path: path.read_bytes()
        for path in env.parent.rglob("*") if path.is_file()} == before
