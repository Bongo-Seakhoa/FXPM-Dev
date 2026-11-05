"""Synthetic scheduler routing proof. No Native, model or financial validation."""

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import os
import uuid

import pytest

import pm_authority_spool as spool
import pm_end_grid as grid
import pm_forward_oos_replay_producer as forward
import pm_historical_anchor_orchestration as orchestration
import pm_historical_scheduler as scheduler
import pm_historical_search_producer as producer
import pm_production_anchor_plan as anchors
import pm_production_config_binding as binding


def test_missing_native_tier_retains_recorded_account_before_window(monkeypatch):
    configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
    if not configured:
        pytest.skip("explicit retained FXPM_DERIVED_QA_ROOT is required")
    parent = Path(configured)
    assert parent.is_absolute() and parent.is_dir()
    spool._reject_reparse_chain(parent)
    case = parent / ("native_recorded_account_" + uuid.uuid4().hex)
    case.mkdir()
    (case / "NO_NATIVE_VERIFICATION.txt").write_text(
        "INVENTED_SCHEDULER_ROUTING_ONLY", encoding="ascii")
    root = case / "working"
    root.mkdir()
    (root / spool.CONTEXT_RELATIVE_PATH).write_bytes(b"synthetic context")
    (root / spool.WORKSPACE_OWNER_RELATIVE_PATH).write_bytes(b"synthetic owner")
    saved = root / "declared-slots" / ("1" * 64)
    saved.mkdir(parents=True)
    (saved / "precommit.json").write_bytes(b"synthetic precommit")
    payload = root / "retained-fixture.json"
    payload.write_bytes(b"synthetic saved payload")

    limits = spool.SpoolLimits(**scheduler._DECLARED_NATIVE_FINALIZATION_LIMITS)
    context = SimpleNamespace(root=root, limits=limits, config_sha256="2" * 64)
    digest = "3" * 64
    cell = {"cell_id": "4" * 64, "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"}
    row = {
        "descriptor_index": 0, "root": str(root), "run_id": "SOFTWARE_RECORDED",
        "cell": cell, "launch_activation_utc": "2024-01-01T00:00:00Z",
        "frozen_refit_clock": {}, "extend_frozen_clock_backward": False,
        "protected_end_exclusive_utc": "2025-01-01T00:00:00Z",
        "native_finalization": {
            "end_grid_root": str(case / "grid"),
            "source_fence_path": str(case / "fence.json"),
            "retirement": {"receipt_path": str(case / "retirement.json")},
        },
    }
    contract = {"active_config_path": str(case / "config.json"),
                "native_universe_sha256": "5" * 64, "config_sha256": context.config_sha256,
                "profile_sha256": "6" * 64}
    original = {"schema_version": orchestration.DECLARED_SLOT_PRECOMMIT_SCHEMA_V2,
                "slot_roster": {"schema_version": anchors.DECLARED_OWNER_SLOT_ROSTER_V2_SCHEMA},
                "cells": [cell], "run_id": row["run_id"],
                "spread_profile_identity": {"profile_sha256": contract["profile_sha256"]}}
    precommit = SimpleNamespace(receipt_sha256="1" * 64, to_dict=lambda: original)
    process = SimpleNamespace(receipt_sha256="7" * 64)
    frozen = SimpleNamespace(effective_config_sha256="8" * 64, materialize=lambda context: object())
    run = {"fixture": "verified saved native run stand-in"}
    run_descriptor = {"sha256": "9" * 64, "relative_path": "synthetic-run.json", "size_bytes": 1}
    account = SimpleNamespace(descriptor={"sha256": "a" * 64, "relative_path": "synthetic-account.json",
                                          "size_bytes": 1})
    progress = SimpleNamespace(run_receipt=run, terminal_stop=False, first_uncommitted_slot=None)

    def forbidden(*args, **kwargs):
        pytest.fail("recorded-account route must not reopen production or publish refused evidence")

    def missing_tier(*args, **kwargs):
        scheduler._fail("DECLARED_NATIVE_FINALIZATION_TIER_PROOF_REQUIRED")

    monkeypatch.setattr(spool, "read_generation_retirement", lambda *args, **kwargs: None)
    monkeypatch.setattr(spool, "reopen_generation_context", lambda *args, **kwargs: context)
    monkeypatch.setattr(spool, "read_owned_generation_workspace",
                        lambda context: ({"cell_id": cell["cell_id"], "cell_contract_sha256": digest}, "b" * 64))
    monkeypatch.setattr(scheduler, "_declared_native_cell_contract_sha256", lambda *args: digest)
    monkeypatch.setattr(scheduler, "_declared_dispatch_lease", lambda context: nullcontext(True))
    monkeypatch.setattr(binding, "load_effective_pipeline_config", lambda path: ({}, object()))
    monkeypatch.setattr(binding, "freeze_production_pipeline_config", lambda *args, **kwargs: frozen)
    monkeypatch.setattr(scheduler, "_declared_reopen_process", lambda *args: process)
    monkeypatch.setattr(orchestration, "reopen_declared_slot_owner_run_precommit",
                        lambda *args, **kwargs: precommit)
    monkeypatch.setattr(anchors, "verify_declared_owner_slot_roster", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler, "_declared_progress_or_busy", lambda *args: (progress, False))
    monkeypatch.setattr(scheduler, "_declared_run_descriptor", lambda *args: run_descriptor)
    monkeypatch.setattr(forward, "discover_declared_cell_account_artifact", lambda *args, **kwargs: account)
    monkeypatch.setattr(scheduler, "_declared_native_finalization_preflight", lambda *args, **kwargs: ())
    monkeypatch.setattr(scheduler, "_declared_native_completed_records", missing_tier)
    monkeypatch.setattr(orchestration, "_declared_slot_effective_access", forbidden)
    monkeypatch.setattr(producer, "execute_declared_cell_owner_slots", forbidden)
    monkeypatch.setattr(scheduler, "_declared_account", forbidden)
    monkeypatch.setattr(grid, "store_view_contract", forbidden)
    monkeypatch.setattr(grid.EndGridWriter, "append_many_same_symbol", forbidden)
    monkeypatch.setattr(spool, "retire_completed_generation", forbidden)

    refused = scheduler._declared_run_one_cell(contract, row)
    assert refused["status"] == "REOPENED"
    assert refused["native_finalization_status"] == "REFUSED_RETAINED"
    assert refused["native_finalization_reason_code"] == "DECLARED_NATIVE_FINALIZATION_TIER_PROOF_REQUIRED"
    assert refused["run_receipt"] == run_descriptor
    assert refused["account_descriptor"] == account.descriptor
    assert payload.read_bytes() == b"synthetic saved payload"
    assert all(not (case / name).exists() for name in ("grid", "fence.json", "retirement.json"))

    seen = []
    def retained_finalization_entry(result, **kwargs):
        seen.append((kwargs["run_body"], kwargs["artifact"], result["recorded_before_dispatch"]))
        return result

    monkeypatch.setattr(scheduler, "_declared_native_finalize_account", retained_finalization_entry)
    reopened = scheduler._declared_run_one_cell(contract, row)
    assert reopened["status"] == "REOPENED"
    assert seen == [(run, account, True)]


def test_native_retirement_preflight_refuses_unowned_inventory_and_renewal(monkeypatch):
    configured = os.environ.get("FXPM_DERIVED_QA_ROOT")
    if not configured:
        pytest.skip("explicit retained FXPM_DERIVED_QA_ROOT is required")
    parent = Path(configured)
    assert parent.is_absolute() and parent.is_dir()
    spool._reject_reparse_chain(parent)
    case = parent / ("native_retirement_refusal_" + uuid.uuid4().hex)
    root = case / "invented_working"
    foreign = root / "invented_foreign_directory"
    foreign.mkdir(parents=True)
    held = foreign / "held.txt"
    held.write_bytes(b"synthetic retained payload")
    context = SimpleNamespace(root=root, context_sha256="1" * 64, limits=spool.SpoolLimits())
    retirement = {"receipt_path": str(case / "retirement.json"), "max_paths": 20,
                  "max_nodes": 20, "max_bytes": 1024**2}
    fence = case / "fence.json"
    with pytest.raises(spool.AuthoritySpoolError, match="RETIREMENT_FOREIGN_DIRECTORY"):
        scheduler._declared_native_retirement_preflight(context, "2" * 64,
            protected=(fence,), direct=(), locks=(), fence_path=str(fence), fence_sha="3" * 64,
            retirement=retirement)

    precommit = SimpleNamespace(receipt_sha256="4" * 64)
    renewal = root / "declared-slots" / precommit.receipt_sha256 / "source-access-renewal"
    renewal.mkdir(parents=True)
    saved = renewal / "synthetic.json"
    saved.write_bytes(b"synthetic renewal namespace")

    def forbidden(*args, **kwargs):
        pytest.fail("the renewal refusal must precede proof assembly and publication")

    monkeypatch.setattr(scheduler, "_declared_native_finalization_preflight", lambda *args, **kwargs: (fence,))
    monkeypatch.setattr(scheduler, "_declared_native_completed_records", forbidden)
    monkeypatch.setattr(grid, "store_view_contract", forbidden)
    monkeypatch.setattr(spool, "retire_completed_generation", forbidden)
    row = {"native_finalization": {"source_fence_path": str(fence), "retirement": retirement}}
    run_receipt = {"relative_path": "invented-run.json"}
    result = scheduler._declared_native_finalize_account(
        {"status": "REOPENED", "run_receipt": run_receipt, "account_descriptor": {"synthetic": True}},
        context=context, contract={}, row=row, config=None, precommit=precommit, process=None,
        artifact=None, run_body=None, workspace_sha="2" * 64, cell_contract_sha256="5" * 64)
    assert result["native_finalization_status"] == "REFUSED_RETAINED"
    assert result["native_finalization_reason_code"] == "DECLARED_NATIVE_FINALIZATION_RENEWAL_NOT_OWNED"
    assert result["status"] == "REOPENED"
    assert result["run_receipt"] == run_receipt
    assert result["account_descriptor"] == {"synthetic": True}
    assert held.read_bytes() == b"synthetic retained payload"
    assert saved.read_bytes() == b"synthetic renewal namespace"
    assert not fence.exists() and not Path(retirement["receipt_path"]).exists()
    assert not (root / spool.RETIREMENT_RELATIVE_PATH).exists()


# E4193 authorized the eight values in full13_research_bindings_002.draft.json:218.
_EXPECTED_NATIVE_LIMITS = {
    "max_artifact_bytes": 1073741824,
    "max_symbol_bytes": 8589934592,
    "max_generation_bytes": 8589934592,
    "max_artifacts_per_symbol": 100000,
    "max_manifest_bytes": 134217728,
    "max_json_bytes": 67108864,
    "stream_chunk_bytes": 1048576,
    "min_free_bytes": 17179869184,
}


def _native_limit_fixture(tmp_path):
    row = {
        "descriptor_index": 0, "root": str(tmp_path / "working"), "run_id": "INVENTED_LIMIT_BINDING",
        "cell": {"cell_id": "4" * 64, "symbol": "XAGUSD", "timeframe": "H1", "regime": "TREND"},
        "native_finalization": {
            "end_grid_root": str(tmp_path / "grid"),
            "source_fence_path": str(tmp_path / "fence.json"),
            "retirement": {"receipt_path": str(tmp_path / "retirement.json")},
        },
    }
    contract = {
        "schema_version": scheduler.DECLARED_SCHEDULER_SCHEMA, "cells": [row],
        "active_config_path": str(tmp_path / "config.json"),
        "native_universe_sha256": "5" * 64, "config_sha256": "6" * 64, "profile_sha256": "7" * 64,
    }
    return contract, row


def test_native_opt_in_limit_values_match_original_authorization():
    actual = scheduler._DECLARED_NATIVE_FINALIZATION_LIMITS
    assert actual == _EXPECTED_NATIVE_LIMITS
    assert spool.validate_spool_limits(spool.SpoolLimits(**actual)) == spool.SpoolLimits(**_EXPECTED_NATIVE_LIMITS)


@pytest.mark.parametrize("free_delta", [-1, 0], ids=["below-floor", "at-floor"])
def test_native_opt_in_fresh_floor_and_exact_freeze_limits(tmp_path, monkeypatch, free_delta):
    contract, row = _native_limit_fixture(tmp_path)
    root = Path(row["root"])
    limits = spool.SpoolLimits(**_EXPECTED_NATIVE_LIMITS)
    digest = scheduler._declared_native_cell_contract_sha256(contract, row, limits)
    calls = []
    config = object()
    marker = root / spool.WORKSPACE_OWNER_RELATIVE_PATH

    def forbidden(*args, **kwargs):
        pytest.fail("fresh limit routing must not reopen context or freeze pipeline configuration")

    def free_probe(path):
        assert path == root
        calls.append("disk")
        return limits.min_free_bytes + free_delta

    def load_config(path):
        assert path == Path(contract["active_config_path"])
        calls.append("config")
        return {}, config

    def preflight(actual_contract, actual_row, actual_config, actual_limits, *, fresh):
        assert (actual_contract, actual_row, actual_config, actual_limits, fresh) == (contract, row, config, limits, True)
        calls.append("preflight")

    def freeze(path, **kwargs):
        assert path == root
        assert kwargs == {
            "generation_id": row["run_id"], "native_universe_sha256": contract["native_universe_sha256"],
            "config_sha256": contract["config_sha256"], "limits": limits,
            "owned_workspace": {"cell_id": row["cell"]["cell_id"], "cell_contract_sha256": digest},
        }
        calls.append("freeze")
        # This marker exercises ownership routing without invoking real freeze or quota I/O.
        root.mkdir()
        marker.write_bytes(b"invented workspace marker")
        return SimpleNamespace(root=root, limits=kwargs["limits"])

    def read_owner(context):
        assert context.root == root
        calls.append("owner")
        return {"cell_id": row["cell"]["cell_id"], "cell_contract_sha256": digest}, "8" * 64

    def held(context):
        assert context.root == root
        calls.append("lease")
        return nullcontext(False)

    monkeypatch.setattr(spool, "read_generation_retirement", lambda *args, **kwargs: None)
    monkeypatch.setattr(spool, "reopen_generation_context", forbidden)
    monkeypatch.setattr(spool, "freeze_generation_context", freeze)
    monkeypatch.setattr(spool, "read_owned_generation_workspace", read_owner)
    monkeypatch.setattr(binding, "load_effective_pipeline_config", load_config)
    monkeypatch.setattr(binding, "freeze_production_pipeline_config", forbidden)
    monkeypatch.setattr(scheduler, "_declared_disk_free_bytes", free_probe)
    monkeypatch.setattr(scheduler, "_declared_native_finalization_preflight", preflight)
    monkeypatch.setattr(scheduler, "_declared_dispatch_lease", held)

    result = scheduler._declared_run_one_cell(contract, row)
    assert result["status"] == "NOT_RUN"
    if free_delta:
        assert result["reason_code"] == "DISK_LOW"
        assert calls == ["disk"]
        assert not root.exists()
    else:
        assert result["reason_code"] == "BUSY"
        assert calls == ["disk", "config", "preflight", "freeze", "owner", "lease"]
        assert list(root.iterdir()) == [marker]


@pytest.mark.parametrize(("saved_state", "expected_reason"), [
    ("missing-owner", "DECLARED_NATIVE_FINALIZATION_OWNED_WORKSPACE_REQUIRED"),
    ("wrong-limits", "DECLARED_NATIVE_WORKSPACE_OWNER_CHANGED"),
    ("wrong-digest", "DECLARED_NATIVE_WORKSPACE_OWNER_CHANGED"),
], ids=["missing-owner", "wrong-limits", "wrong-digest"])
def test_native_opt_in_reopen_rejects_invalid_ownership_before_work(tmp_path, monkeypatch, saved_state, expected_reason):
    contract, row = _native_limit_fixture(tmp_path)
    root = Path(row["root"])
    root.mkdir()
    context_path = root / spool.CONTEXT_RELATIVE_PATH
    context_path.write_bytes(b"invented saved context")
    marker = root / spool.WORKSPACE_OWNER_RELATIVE_PATH
    if saved_state != "missing-owner":
        marker.write_bytes(b"invented saved owner")
    limits = spool.SpoolLimits(**_EXPECTED_NATIVE_LIMITS)
    digest = scheduler._declared_native_cell_contract_sha256(contract, row, limits)
    saved_limits = (spool.SpoolLimits(**{**_EXPECTED_NATIVE_LIMITS, "min_free_bytes": 5 * 1024**3})
                    if saved_state == "wrong-limits" else limits)
    context = SimpleNamespace(root=root, limits=saved_limits)
    calls = []

    def forbidden(*args, **kwargs):
        pytest.fail("invalid saved native ownership must refuse before lease, config or new context")

    def reopen(path, **kwargs):
        assert path == root
        assert kwargs == {
            "expected_generation_id": row["run_id"],
            "expected_native_universe_sha256": contract["native_universe_sha256"],
            "expected_config_sha256": contract["config_sha256"],
        }
        calls.append("reopen")
        return context

    def read_owner(actual):
        assert actual is context
        calls.append("owner")
        return {"cell_id": row["cell"]["cell_id"],
                "cell_contract_sha256": "0" * 64 if saved_state == "wrong-digest" else digest}, "8" * 64

    monkeypatch.setattr(spool, "read_generation_retirement", lambda *args, **kwargs: None)
    monkeypatch.setattr(spool, "reopen_generation_context", reopen)
    monkeypatch.setattr(spool, "read_owned_generation_workspace", read_owner)
    monkeypatch.setattr(spool, "freeze_generation_context", forbidden)
    monkeypatch.setattr(scheduler, "_declared_dispatch_lease", forbidden)
    monkeypatch.setattr(binding, "load_effective_pipeline_config", forbidden)
    monkeypatch.setattr(binding, "freeze_production_pipeline_config", forbidden)

    result = scheduler._declared_run_one_cell(contract, row)
    assert (result["status"], result["reason_code"]) == ("FAILED", expected_reason)
    assert calls == (["reopen"] if saved_state == "missing-owner" else ["reopen", "owner"])
    assert context_path.read_bytes() == b"invented saved context"
    if saved_state == "missing-owner":
        assert not marker.exists()
    else:
        assert marker.read_bytes() == b"invented saved owner"


def test_native_opt_in_archive_reopen_and_confirmation_bind_same_exact_limits(tmp_path, monkeypatch):
    contract, row = _native_limit_fixture(tmp_path)
    root = Path(row["root"])
    limits = spool.SpoolLimits(**_EXPECTED_NATIVE_LIMITS)
    record = {
        "status": "ARCHIVED_RECORD_ONLY", "generation_id": row["run_id"],
        "cell_id": row["cell"]["cell_id"], "generation_context_sha256": "9" * 64,
        "cell_contract_sha256": scheduler._declared_native_cell_contract_sha256(contract, row, limits),
        "retirement_receipt_path": row["native_finalization"]["retirement"]["receipt_path"],
        "retirement_receipt_sha256": "a" * 64, "precommit_sha256": "b" * 64,
        "source_fence": {"path": row["native_finalization"]["source_fence_path"], "sha256": "c" * 64},
        "end_grid_rows": {},
    }
    calls = []
    bound_limits = []
    cell_digest = scheduler._declared_native_cell_contract_sha256

    def capture_digest(actual_contract, actual_row, actual_limits):
        assert actual_contract is contract and actual_row is row
        bound_limits.append(actual_limits)
        return cell_digest(actual_contract, actual_row, actual_limits)

    def read_archive(path, *, expected_context_sha256=None, expected_completed_fence_schema):
        assert path == root
        assert expected_completed_fence_schema == spool.NATIVE_COMPLETION_FENCE_SCHEMA
        calls.append(expected_context_sha256)
        return record

    def forbidden(*args, **kwargs):
        pytest.fail("archived native identity must confirm before opening a working context or config")

    monkeypatch.setattr(scheduler, "_declared_native_cell_contract_sha256", capture_digest)
    monkeypatch.setattr(spool, "read_generation_retirement", read_archive)
    monkeypatch.setattr(spool, "reopen_generation_context", forbidden)
    monkeypatch.setattr(spool, "freeze_generation_context", forbidden)
    monkeypatch.setattr(binding, "load_effective_pipeline_config", forbidden)

    result = scheduler._declared_run_one_cell(contract, row)
    assert (result["status"], result["source_kind"]) == ("ARCHIVED", "broker_native")
    assert result["archive_record"] == {
        "path": record["retirement_receipt_path"], "sha256": record["retirement_receipt_sha256"],
    }
    assert scheduler._declared_confirm_receipt(contract, result) is None
    assert result["archive_record_confirmed"] is True
    assert calls == [None, record["generation_context_sha256"]]
    assert bound_limits == [limits, limits]
    assert not root.exists()
