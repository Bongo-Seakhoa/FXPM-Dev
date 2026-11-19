"""Invented filesystem fixtures; no trading or host-resource admission authority."""

import errno
from pathlib import Path

import pytest

import pm_authority_spool_quota as quota


_PAYLOAD = b"published despite retired capacity metadata"


def _publish(root, *, batched, name, maximum=1, minimum=2**40):
    path = root / name
    if batched:
        with quota.publication_batch(root, max_generation_bytes=maximum,
                min_free_bytes=minimum, window_bytes=64) as batch:
            batch.reserve(len(_PAYLOAD))
            with path.open("xb") as stream:
                written = stream.write(_PAYLOAD)
            batch.published(written)
    else:
        with quota.publication_lease(root, max_generation_bytes=maximum,
                min_free_bytes=minimum) as lease:
            lease.consume(len(_PAYLOAD))
            with path.open("xb") as stream:
                written = stream.write(_PAYLOAD)
            lease.materialize(written)
            lease.commit(written)
    return path


@pytest.mark.parametrize("batched", [False, True], ids=["ordinary", "batch"])
def test_retired_capacity_metadata_reaches_actual_write(tmp_path, batched):
    root = tmp_path / "quota"
    root.mkdir()
    previous = b"existing committed bytes already exceed the old one-byte cap"
    (root / "previous.bin").write_bytes(previous)
    path = _publish(root, batched=batched, name="first.bin")
    assert path.read_bytes() == _PAYLOAD
    state = quota._read_state(root)
    assert state["committed_bytes"] == 0  # Legacy recorded metadata, not a filesystem recount.
    assert state["reservations"] == []
    assert state["max_generation_bytes"] == 1
    assert state["min_free_bytes"] == 2**40
    # New caller limits do not reinterpret the saved numeric compatibility metadata.
    second = _publish(root, batched=batched, name="second.bin", maximum=2, minimum=0)
    assert second.read_bytes() == _PAYLOAD
    assert quota._read_state(root)["committed_bytes"] == 0
    assert quota._read_state(root)["reservations"] == []


@pytest.mark.parametrize("batched", [False, True], ids=["ordinary", "batch"])
def test_actual_write_error_remains_visible_and_uncertain_membership_is_reconciled(tmp_path, batched):
    root = tmp_path / "quota"
    root.mkdir()
    previous = b"old"
    (root / "previous.bin").write_bytes(previous)
    fault = OSError(errno.ENOSPC, "injected OS error after an actual partial publication")
    with pytest.raises(OSError) as caught:
        if batched:
            with quota.publication_batch(root, max_generation_bytes=1,
                    min_free_bytes=2**40, window_bytes=64) as batch:
                batch.reserve(len(_PAYLOAD))
                batch._publication_started(len(_PAYLOAD))
                with (root / "uncertain.bin").open("xb") as stream:
                    stream.write(_PAYLOAD)
                    raise fault
        else:
            with quota.publication_lease(root, max_generation_bytes=1,
                    min_free_bytes=2**40) as lease:
                lease.consume(len(_PAYLOAD))
                with (root / "uncertain.bin").open("xb") as stream:
                    stream.write(_PAYLOAD)
                    raise fault
    assert caught.value is fault and caught.value.errno == errno.ENOSPC
    assert (root / "uncertain.bin").read_bytes() == _PAYLOAD
    state = quota._read_state(root)
    assert state["reservations"]
    _publish(root, batched=False, name="recovery.bin")
    recovered = quota._read_state(root)
    assert recovered["committed_bytes"] == 0
    assert recovered["reservations"] == []


def test_legacy_true_ram_hold_option_reaches_dispatch_without_high_rss_hold(tmp_path, monkeypatch):
    import hashlib
    import pm_historical_scheduler as scheduler
    import pm_memory_governor as memory

    # Invented descriptor metadata exercises admission; no account or model is run.
    contract = {
        "schema_version": scheduler.DECLARED_SCHEDULER_SCHEMA,
        "active_config_path": str(tmp_path / "invented-config.json"),
        "native_universe_sha256": "a" * 64,
        "config_sha256": "b" * 64,
        "profile_sha256": "c" * 64,
        "cells": [{
            "root": str(tmp_path / "cell"), "run_id": "invented-resource-test",
            "cell": {"cell_id": hashlib.sha256(b"invented-resource-test").hexdigest(),
                "symbol": "AUDUSD", "timeframe": "H1", "regime": "TREND"},
            "launch_activation_utc": "2023-06-24T00:00:00Z",
            "frozen_refit_clock": {"anchor_activation_utc": "2024-06-22T00:00:00Z",
                "interval_calendar_days": 28},
            "extend_frozen_clock_backward": True,
            "protected_end_exclusive_utc": "2024-07-14T08:05:00Z",
            "source_access_window_policy": {"lead_seconds": 1, "validity_seconds": 3600},
        }],
    }

    class DispatchReached(Exception):
        pass

    def inspect_dispatch(self, worker, tasks, **kwargs):
        assert kwargs["admission_gate"]() is True
        assert next(iter(tasks)).key == "AUDUSD"
        stats = memory.ResourceStats()
        observer = memory._ResourceObserver(kwargs["resource_policy"], None, stats)
        assert observer.hold({"soft_ram_target_bytes": 1, "own_rss_bytes": 2**40}, active=True) is False
        assert stats.hold_events == 0
        assert kwargs["admission_gate"]() is True
        raise DispatchReached

    monkeypatch.setattr(memory.MemoryGovernor, "iter_completed", inspect_dispatch)
    with pytest.raises(DispatchReached):
        scheduler.run_declared_symbol_scheduler(contract=contract, requested_workers=1,
            soft_ram_hold=True, telemetry_path=tmp_path / "resource.jsonl")


def test_spool_publication_and_cold_receipt_ignore_retired_capacity_metadata(tmp_path):
    import pm_authority_spool as spool

    limits = spool.SpoolLimits(max_artifact_bytes=4096, max_symbol_bytes=1,
        max_artifacts_per_symbol=3, max_manifest_bytes=4096, max_json_bytes=4096,
        stream_chunk_bytes=64, max_generation_bytes=1, min_free_bytes=2**40)
    context = spool.freeze_generation_context(tmp_path / "spool",
        generation_id="invented-resource-test", native_universe_sha256="a" * 64,
        config_sha256="b" * 64, limits=limits)
    sink = spool.open_symbol_sink(context, "AUDUSD")
    first = sink.put_opaque("first", "events", [b"first payload"], format="invented-binary-v1")
    second = sink.put_opaque("second", "events", [b"second payload"], format="invented-binary-v1")
    receipt = sink.seal()
    manifest = spool.verify_symbol_receipt(context.root, receipt,
        expected_generation_id="invented-resource-test", expected_symbol="AUDUSD",
        expected_native_universe_sha256="a" * 64, expected_config_sha256="b" * 64)
    assert manifest["artifact_total_bytes"] == len(b"first payloadsecond payload")
    assert manifest["artifacts"] == [first, second]
    assert quota._read_state(context.root)["committed_bytes"] == 0
    assert quota._read_state(context.root)["reservations"] == []
    # Cold verification above still authenticates content instead of trusting the total.
    artifact = context.root.joinpath(*Path(first["descriptor"]["relative_path"]).parts)
    artifact.write_bytes(b"tampered data")
    with pytest.raises(spool.AuthoritySpoolError, match="ARTIFACT_INTEGRITY_MISMATCH"):
        spool.verify_symbol_receipt(context.root, receipt,
            expected_generation_id="invented-resource-test", expected_symbol="AUDUSD",
            expected_native_universe_sha256="a" * 64, expected_config_sha256="b" * 64)


def _actual_capacity_error(kind):
    if kind == "memory":
        return MemoryError("invented allocation failure")
    if kind.startswith("win"):
        error = OSError(errno.EIO, "invented Windows resource failure")
        error.winerror = int(kind[3:])
        return error
    if kind == "enomem":
        return OSError(errno.ENOMEM, "invented native allocation failure")
    code = errno.ENOSPC if kind == "space" else getattr(errno, "EDQUOT", errno.ENOSPC)
    return OSError(code, "invented OS capacity failure")


@pytest.mark.parametrize("kind", ["space", "quota", "win112", "win39", "memory", "enomem", "win8", "win14", "win1455"])
@pytest.mark.parametrize("link", ["direct", "cause", "context"])
def test_actual_capacity_reason_uses_numeric_or_memory_type_through_wrappers(kind, link):
    import pm_authority_spool as spool

    error = _actual_capacity_error(kind)
    outer = error
    if link != "direct":
        outer = spool.AuthoritySpoolError("invented wrapper with no capacity wording")
        setattr(outer, "__cause__" if link == "cause" else "__context__", error)
    expected = "MEMORY_ERROR" if kind in {"memory", "enomem", "win8", "win14", "win1455"} else "DISK_LOW"
    assert spool.resource_capacity_reason(outer) == expected
    assert spool.is_resource_capacity_error(outer) is True


@pytest.mark.parametrize("with_capacity", [False, True])
def test_capacity_reason_visits_both_branches_and_terminates_cycles(with_capacity):
    import pm_authority_spool as spool

    first, second = RuntimeError("invented first"), RuntimeError("invented second")
    first.__cause__, second.__context__ = second, first
    if with_capacity:
        first.__context__ = _actual_capacity_error("memory")
    assert spool.resource_capacity_reason(first) == ("MEMORY_ERROR" if with_capacity else None)


@pytest.mark.parametrize("error", [PermissionError(errno.EACCES, "invented permission failure"),
    OSError(errno.EIO, "invented I/O failure"), OSError(None, "ENOSPC MemoryError disk full"),
    RuntimeError("DISK_LOW MEMORY_ERROR ENOSPC"), ValueError("memory allocation failure")])
def test_capacity_reason_does_not_interpret_permission_integrity_or_message_text(error):
    import pm_authority_spool as spool

    assert spool.resource_capacity_reason(error) is None
    assert spool.is_resource_capacity_error(error) is False


def _retained_account_fixture(tmp_path):
    import hashlib
    import json
    import pm_authority_spool as spool

    grid_root, working_root = tmp_path / "grid", tmp_path / "generation"
    grid_root.mkdir()
    working_root.mkdir()
    binding = {"run_id": "invented-retention-test", "generation_context_sha256": "a" * 64,
        "precommit_sha256": "b" * 64, "search_process_receipt_sha256": "c" * 64, "cell_id": "d" * 64}
    account = {"schema_version": "fxpm.declared-cell-raw-account.v4",
        "generation_id": binding["run_id"], "generation_context_sha256": binding["generation_context_sha256"],
        "declared_precommit_sha256": binding["precommit_sha256"],
        "search_process_receipt_sha256": binding["search_process_receipt_sha256"],
        "cell": {"cell_id": binding["cell_id"], "symbol": "AUDUSD", "timeframe": "H1", "regime": "TREND"},
        "progress_status": "COMPLETE", "account": {"trades": [{"invented_original_payload": [1, 2, 3]}]},
        "qualification_authority": False, "capital_authority": False, "live_authority": False, "order_authority": False}
    encoded = json.dumps(account, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    digest = hashlib.sha256(encoded).hexdigest()
    path = grid_root / ("declared-account-" + digest + ".json")
    path.write_bytes(encoded)
    fence = {"schema_version": spool.NATIVE_COMPLETION_FENCE_SCHEMA,
        "completion": {"account_artifact_sha256": digest, "cell_run_receipt_sha256": "e" * 64},
        "retained_account_result": {"path": str(path), "sha256": digest, "size_bytes": len(encoded)}}
    return spool, grid_root, working_root, binding, account, encoded, path, fence


def test_retained_account_reads_original_full_json_without_reencoding(tmp_path):
    spool, grid_root, working_root, binding, account, encoded, path, fence = _retained_account_fixture(tmp_path)
    observed = spool._retirement_retained_account(fence, binding=binding, grid_root=grid_root,
        working_root=working_root, required=True)
    assert observed == account
    assert path.read_bytes() == encoded
    assert observed["account"]["trades"] == [{"invented_original_payload": [1, 2, 3]}]


@pytest.mark.parametrize("fault", ["missing", "tamper", "size", "binding", "missing_ref"])
def test_retained_account_refuses_missing_tampered_or_cross_bound_results(tmp_path, fault):
    spool, grid_root, working_root, binding, account, encoded, path, fence = _retained_account_fixture(tmp_path)
    if fault == "missing":
        path.unlink()
    elif fault == "tamper":
        path.write_bytes(encoded.replace(b"[1,2,3]", b"[3,2,1]"))
    elif fault == "size":
        fence["retained_account_result"]["size_bytes"] += 1
    elif fault == "binding":
        binding["precommit_sha256"] = "f" * 64
    else:
        del fence["retained_account_result"]
    with pytest.raises((spool.AuthoritySpoolError, OSError)):
        spool._retirement_retained_account(fence, binding=binding, grid_root=grid_root,
            working_root=working_root, required=True)


def test_legacy_absent_retained_ref_is_readable_only_when_not_required(tmp_path):
    spool, grid_root, working_root, binding, account, encoded, path, fence = _retained_account_fixture(tmp_path)
    del fence["retained_account_result"]
    assert spool._retirement_retained_account(fence, binding=binding, grid_root=grid_root,
        working_root=working_root, required=False) is None
