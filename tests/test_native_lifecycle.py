from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

import pytest

from pm_native_lifecycle import (
    BrokerAccountHighWaterReceipt,
    GENESIS_SHA256,
    GrowthBudget,
    LifecycleBoundError,
    LifecycleCASMismatch,
    LifecycleDatabaseFullError,
    LifecycleIntegrityError,
    LifecycleLimits,
    LifecycleRecoveryRequired,
    LifecycleRootError,
    LifecycleStateError,
    NativeLifecycleAuthority,
    OperationKind,
    OperationState,
    OperationalDomain,
)


AUTH_KEY = b"fxpm-native-lifecycle-test-key!!"  # exactly 32 bytes
BROKER_HIGHWATER_KEY = b"independent-broker-highwater-key!!"
PROVISIONING_IDENTITY = "pytest-native-lifecycle-v1"
REPO_ROOT = Path(__file__).resolve().parents[1]


class LifecycleIntegrationEngine:
    @property
    def identity(self) -> Mapping[str, Any]:
        return {
            "engine_id": "tests.lifecycle-integration-engine",
            "schema_version": "v1",
            "code_sha256": "3" * 64,
        }

    def initial_state(self, *args: Any) -> Mapping[str, Any]:
        del args
        return {"bars": 0}

    def validate_state_evidence(self, evidence: Any, payload: Any) -> None:
        if evidence.node != "controller:REGIME_HYSTERESIS":
            raise ValueError("unexpected evidence node")
        if payload["controller_state"] != "authenticated-replay":
            raise ValueError("unexpected evidence payload")

    def apply_closed_bar(
        self, state: Mapping[str, Any], bar: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        del bar
        return {"bars": int(state["bars"]) + 1}

    def initial_order_state(self, *args: Any) -> Mapping[str, Any]:
        del args
        return {"applied": []}

    def apply_order_result(
        self,
        state: Mapping[str, Any],
        action: str,
        request: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return {
            "applied": [
                *state["applied"],
                {"action": action, "request": dict(request), "result": dict(result)},
            ]
        }


def _limits(**overrides) -> LifecycleLimits:
    values = {
        "max_hot_bytes": 16 * 1024 * 1024,
        "max_cold_bytes": 16 * 1024 * 1024,
        "min_volume_free_bytes": 1,
        "max_hot_files": 10_000,
        "max_cold_files": 10_000,
        "max_complete_hot_runs": 20,
        "max_inflight_runs": 4,
        "max_operations": 200,
        "max_active_operations": 16,
        "max_payload_bytes": 64 * 1024,
        "max_files_per_operation": 2_000,
        "max_pins": 32,
        "max_pin_payload_bytes": 64 * 1024,
        "max_pointer_bytes": 64 * 1024,
        "max_scan_entries": 20_000,
        "max_db_bytes": 2 * 1024 * 1024,
        "page_size": 4096,
        "journal_size_limit": 64 * 1024,
        "db_busy_deadline_seconds": 2.0,
        "lock_deadline_seconds": 4.0,
        "lease_seconds": 30.0,
    }
    values.update(overrides)
    return LifecycleLimits(**values)


def _roots(tmp_path: Path, *, runtime: bool = False) -> dict[str, Path]:
    roots = {
        "native": tmp_path / "native-evidence",
        "cold": tmp_path / "cold-evidence",
        "state": tmp_path / "local-authority-state",
    }
    roots["native"].mkdir(parents=True)
    roots["cold"].mkdir(parents=True)
    if runtime:
        roots["runtime"] = tmp_path / "operational-runtime"
        roots["runtime"].mkdir(parents=True)
    return roots


def _authority(
    roots: dict[str, Path],
    limits: LifecycleLimits,
    *,
    provision: bool,
    operational_domain: OperationalDomain | None = None,
) -> NativeLifecycleAuthority:
    domains = {"runtime_state": operational_domain} if operational_domain else None
    return NativeLifecycleAuthority(
        native_root=roots["native"],
        cold_root=roots["cold"],
        state_root=roots["state"],
        limits=limits,
        auth_key=AUTH_KEY,
        broker_highwater_key=BROKER_HIGHWATER_KEY,
        operational_domains=domains,
        provision=provision,
        provisioning_identity=PROVISIONING_IDENTITY,
    )


def _abort(lease, operation_id: str) -> None:
    lease.transition(operation_id, OperationState.ABORTING)
    lease.transition(operation_id, OperationState.ABORTED)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _broker_highwater(
    account_scope_sha256: str, observed_sequence_highwater: int
) -> BrokerAccountHighWaterReceipt:
    return BrokerAccountHighWaterReceipt.issue(
        BROKER_HIGHWATER_KEY,
        account_scope_sha256=account_scope_sha256,
        observed_sequence_highwater=observed_sequence_highwater,
        source_snapshot_sha256=hashlib.sha256(
            f"broker-history-{account_scope_sha256}-{observed_sequence_highwater}".encode()
        ).hexdigest(),
        observed_at="2026-01-01T00:00:00Z",
        observation_id=f"observation-{observed_sequence_highwater}",
    )


def _operation_identity(reservation: Any, operation_sequence: int) -> str:
    return (
        f"{reservation.stream_identity_sha256[:32]}:"
        f"{1:020d}:{operation_sequence:020d}"
    )


def _witness_order_highwater(
    authority: NativeLifecycleAuthority,
    reservation: Any,
    operation_sequence: int,
) -> Any:
    current = authority.read_runtime_witness(reservation)
    if current.max_order_operation_sequence >= operation_sequence:
        return current
    return authority.cas_runtime_witness(
        reservation,
        current,
        {
            "writer_epoch": current.writer_epoch or 1,
            "reducer_contract_sha256": (
                current.reducer_contract_sha256
                if current.reducer_contract_sha256 != GENESIS_SHA256
                else hashlib.sha256(b"test-runtime-reducer-contract").hexdigest()
            ),
            "max_bar_sequence": current.max_bar_sequence,
            "bar_chain_sha256": current.bar_chain_sha256,
            "checkpoint_generation": current.checkpoint_generation,
            "checkpoint_sha256": current.checkpoint_sha256,
            "max_order_operation_sequence": operation_sequence,
            "max_order_event_sequence": operation_sequence,
            "order_chain_sha256": hashlib.sha256(
                f"test-order-chain-{operation_sequence}".encode()
            ).hexdigest(),
        },
    )


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_growth_budget_formulas_distinguish_resume_and_non_run_operations() -> None:
    new = GrowthBudget.new_acquisition(expected_run_bytes=1_000, file_count=20)
    resume = GrowthBudget.resume_acquisition(
        expected_final_run_bytes=1_000,
        existing_materialized_run_bytes=640,
        remaining_file_count=7,
    )
    audit = GrowthBudget.audit(expected_bytes=90, file_count=2)
    terms = GrowthBudget.terms(expected_bytes=80, file_count=3)
    archive = GrowthBudget.archive(expected_archive_bytes=300)
    restore = GrowthBudget.restore(missing_payload_bytes=300, missing_file_count=4)

    assert (new.native_peak_bytes, new.complete_run_delta, new.inflight_run_delta) == (
        1_000,
        1,
        1,
    )
    assert (resume.native_peak_bytes, resume.complete_run_delta, resume.inflight_run_delta) == (
        360,
        1,
        0,
    )
    assert audit.complete_run_delta == terms.complete_run_delta == 0
    assert audit.inflight_run_delta == terms.inflight_run_delta == 0
    assert archive.native_peak_bytes == 0 and archive.cold_peak_bytes == 300
    assert restore.native_final_bytes == 300 and restore.native_peak_bytes == 600
    with pytest.raises(ValueError, match="exceed"):
        GrowthBudget.resume_acquisition(
            expected_final_run_bytes=10,
            existing_materialized_run_bytes=11,
            remaining_file_count=0,
        )
    with pytest.raises(ValueError, match="exact integer"):
        GrowthBudget.audit(expected_bytes=1.5, file_count=1)


def test_explicit_provisioning_reopen_and_sqlite_identity(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    limits = _limits()
    with pytest.raises(LifecycleIntegrityError, match="state_root is missing"):
        _authority(roots, limits, provision=False)

    authority = _authority(roots, limits, provision=True)
    with sqlite3.connect(authority.db_path) as connection:
        assert connection.execute("PRAGMA application_id").fetchone()[0] == 0x4658504D
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 4
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "delete"
        assert connection.execute("PRAGMA quick_check").fetchone()[0].lower() == "ok"
        assert connection.execute("PRAGMA page_size").fetchone()[0] == limits.page_size
    # max_page_count is a connection-local enforcement setting; every
    # authority connection reapplies and verifies it.
    connection = authority._connect()
    try:
        assert connection.execute("PRAGMA max_page_count").fetchone()[0] == (
            limits.max_db_bytes // limits.page_size
        )
    finally:
        connection.close()
    reopened = _authority(roots, limits, provision=False)
    assert reopened.authority_id == authority.authority_id
    with pytest.raises(LifecycleIntegrityError, match="already exists"):
        _authority(roots, limits, provision=True)

    with pytest.raises(LifecycleIntegrityError, match="provisioning_binding"):
        NativeLifecycleAuthority(
            native_root=roots["native"],
            cold_root=roots["cold"],
            state_root=roots["state"],
            limits=limits,
            auth_key=AUTH_KEY,
            broker_highwater_key=BROKER_HIGHWATER_KEY,
            provision=False,
            provisioning_identity="wrong-provisioner",
        )


def test_one_native_root_cannot_be_split_across_caller_selected_state_roots(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    limits = _limits()
    authority = _authority(roots, limits, provision=True)
    alternate_state = tmp_path / "alternate-authority-state"

    with pytest.raises(LifecycleIntegrityError, match="already bound"):
        NativeLifecycleAuthority(
            native_root=roots["native"],
            cold_root=roots["cold"],
            state_root=alternate_state,
            limits=limits,
            auth_key=AUTH_KEY,
            broker_highwater_key=BROKER_HIGHWATER_KEY,
            provision=True,
            provisioning_identity=PROVISIONING_IDENTITY,
        )

    reopened = _authority(roots, limits, provision=False)
    assert reopened.authority_id == authority.authority_id
    assert authority.lock_path.parent == roots["native"].parent
    assert authority.fence_path.parent == roots["native"].parent


def test_concurrent_provisioners_with_different_state_roots_have_one_winner(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    limits = _limits(lock_deadline_seconds=5.0)
    barrier = tmp_path / "provision-barrier"
    barrier.mkdir()
    first_state = tmp_path / "first-state"
    second_state = tmp_path / "second-state"
    script = r"""
import json,sys,time
from pathlib import Path
import pm_native_lifecycle as lifecycle
from pm_native_lifecycle import LifecycleIntegrityError,LifecycleLimits,NativeLifecycleAuthority
barrier=Path(sys.argv[8]); marker=barrier/(sys.argv[9]+'.ready')
original=lifecycle._OSFileLock.acquire
def synchronized(lock):
 marker.write_text('ready',encoding='ascii')
 deadline=time.monotonic()+5
 while len(list(barrier.glob('*.ready')))<2:
  if time.monotonic()>=deadline: raise RuntimeError('barrier timeout')
  time.sleep(0.01)
 original(lock)
lifecycle._OSFileLock.acquire=synchronized
try:
 NativeLifecycleAuthority(native_root=Path(sys.argv[1]),cold_root=Path(sys.argv[2]),state_root=Path(sys.argv[3]),limits=LifecycleLimits(**json.loads(sys.argv[4])),auth_key=bytes.fromhex(sys.argv[5]),broker_highwater_key=bytes.fromhex(sys.argv[6]),provision=True,provisioning_identity=sys.argv[7])
 print('PROVISIONED',flush=True)
except LifecycleIntegrityError:
 print('BLOCKED',flush=True)
"""
    common = [
        str(roots["native"]),
        str(roots["cold"]),
        "STATE_PLACEHOLDER",
        json.dumps(asdict(limits), sort_keys=True),
        AUTH_KEY.hex(),
        BROKER_HIGHWATER_KEY.hex(),
        PROVISIONING_IDENTITY,
        str(barrier),
    ]
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                *[*common[:2], str(state), *common[3:]],
                tag,
            ],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for state, tag in ((first_state, "first"), (second_state, "second"))
    ]
    results: list[str] = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=15)
        assert process.returncode == 0, stdout + stderr
        results.append(stdout.strip())
    assert sorted(results) == ["BLOCKED", "PROVISIONED"]
    winning_states = [
        state for state in (first_state, second_state) if tuple(state.glob("*.sqlite3"))
    ]
    assert len(winning_states) == 1
    NativeLifecycleAuthority(
        native_root=roots["native"],
        cold_root=roots["cold"],
        state_root=winning_states[0],
        limits=limits,
        auth_key=AUTH_KEY,
        broker_highwater_key=BROKER_HIGHWATER_KEY,
        provisioning_identity=PROVISIONING_IDENTITY,
    )


def test_missing_or_replaced_database_is_never_silently_recreated(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    limits = _limits()
    authority = _authority(roots, limits, provision=True)
    authority.db_path.unlink()
    with pytest.raises(LifecycleIntegrityError, match="DB is missing"):
        _authority(roots, limits, provision=False)

    # Explicitly provision a fresh, separate authority, then corrupt its
    # SQLite identity. Reopen must fail before any lease is issued.
    roots2 = _roots(tmp_path / "second")
    authority2 = _authority(roots2, limits, provision=True)
    with sqlite3.connect(authority2.db_path) as connection:
        connection.execute("PRAGMA application_id=0")
    with pytest.raises(LifecycleIntegrityError, match="application_id"):
        _authority(roots2, limits, provision=False)

    roots3 = _roots(tmp_path / "third")
    authority3 = _authority(roots3, limits, provision=True)
    authority3.fence_path.unlink()
    with pytest.raises(LifecycleIntegrityError, match="fence anchor is missing"):
        _authority(roots3, limits, provision=False)


def test_external_fence_detects_valid_old_database_rollback(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    authority = _authority(roots, _limits(), provision=True)
    old_database = authority.db_path.read_bytes()
    with authority.lease("advance-fence") as lease:
        assert lease.fencing_token == 1

    # Preserve the path/file identity while restoring a byte-valid old DB.
    with authority.db_path.open("r+b") as handle:
        handle.seek(0)
        handle.write(old_database)
        handle.truncate()
        handle.flush()
        os.fsync(handle.fileno())
    with pytest.raises(LifecycleRecoveryRequired, match="rollback|ambiguous"):
        authority.list_operations()


def test_pending_fence_recovers_commit_after_final_anchor_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    roots = _roots(tmp_path)
    authority = _authority(roots, _limits(), provision=True)
    original = authority._write_fence_anchor
    calls = 0

    def fail_final_write(*, committed_token: int, pending_token: int | None) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected final anchor write failure")
        original(committed_token=committed_token, pending_token=pending_token)

    monkeypatch.setattr(authority, "_write_fence_anchor", fail_final_write)
    with pytest.raises(OSError, match="injected"):
        authority.lease("faulted-acquire")
    monkeypatch.setattr(authority, "_write_fence_anchor", original)

    pending = authority._read_fence_anchor()
    assert pending["committed_token"] == 0
    assert pending["pending_token"] == 1
    # The next lock holder reconciles DB=1 against pending=1, then advances to 2.
    with authority.lease("recover-acquire") as recovered:
        assert recovered.fencing_token == 2
    final = authority._read_fence_anchor()
    assert final["committed_token"] == 2
    assert final["pending_token"] is None


def test_cloud_and_repository_state_roots_are_rejected_by_default(tmp_path: Path) -> None:
    cloud_native = tmp_path / "OneDrive" / "native"
    cold = tmp_path / "cold"
    cloud_native.mkdir(parents=True)
    cold.mkdir()
    with pytest.raises(LifecycleRootError, match="OneDrive"):
        NativeLifecycleAuthority(
            native_root=cloud_native,
            cold_root=cold,
            state_root=tmp_path / "state",
            limits=_limits(),
            auth_key=AUTH_KEY,
            broker_highwater_key=BROKER_HIGHWATER_KEY,
            provision=True,
            provisioning_identity=PROVISIONING_IDENTITY,
        )

    native2, cold2 = tmp_path / "native2", tmp_path / "cold2"
    native2.mkdir()
    cold2.mkdir()
    fake_repo = tmp_path / "repo"
    (fake_repo / ".git").mkdir(parents=True)
    with pytest.raises(LifecycleRootError, match="Git worktree"):
        NativeLifecycleAuthority(
            native_root=native2,
            cold_root=cold2,
            state_root=fake_repo / "authority-state",
            limits=_limits(),
            auth_key=AUTH_KEY,
            broker_highwater_key=BROKER_HIGHWATER_KEY,
            provision=True,
            provisioning_identity=PROVISIONING_IDENTITY,
        )


def test_unsafe_root_bypass_and_overlapping_native_cold_roots_are_rejected(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    with pytest.raises(LifecycleRootError, match="allow_unsafe_roots"):
        NativeLifecycleAuthority(
            native_root=roots["native"],
            cold_root=roots["cold"],
            state_root=roots["state"],
            limits=_limits(),
            auth_key=AUTH_KEY,
            broker_highwater_key=BROKER_HIGHWATER_KEY,
            provision=True,
            provisioning_identity=PROVISIONING_IDENTITY,
            allow_unsafe_roots=True,
        )
    with pytest.raises(LifecycleRootError, match="disjoint"):
        NativeLifecycleAuthority(
            native_root=roots["native"],
            cold_root=roots["native"],
            state_root=roots["state"],
            limits=_limits(),
            auth_key=AUTH_KEY,
            broker_highwater_key=BROKER_HIGHWATER_KEY,
            provision=True,
            provisioning_identity=PROVISIONING_IDENTITY,
        )


def test_hardlinked_lock_fence_and_evidence_aliases_fail_closed(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    authority = _authority(roots, _limits(), provision=True)

    lock_alias = tmp_path / "lock-alias"
    try:
        os.link(authority.lock_path, lock_alias)
    except OSError as exc:  # pragma: no cover - filesystem capability dependent
        pytest.skip(f"hard links are unavailable on this test filesystem: {exc}")
    try:
        with pytest.raises(LifecycleIntegrityError, match="unaliased"):
            with authority.lease("aliased-lock"):
                pass
    finally:
        lock_alias.unlink()

    fence_alias = tmp_path / "fence-alias"
    os.link(authority.fence_path, fence_alias)
    try:
        with pytest.raises(LifecycleIntegrityError, match="fence anchor"):
            authority.list_operations()
    finally:
        fence_alias.unlink()

    evidence = roots["native"] / "immutable-evidence.bin"
    evidence_alias = roots["native"] / "evidence-alias.bin"
    evidence.write_bytes(b"immutable")
    os.link(evidence, evidence_alias)
    try:
        with authority.lease("aliased-evidence") as lease:
            with pytest.raises(LifecycleIntegrityError, match="unaliased"):
                lease.pin_evidence("aliased-pin", [evidence])
    finally:
        evidence_alias.unlink()


@pytest.mark.parametrize("drive_type", [4, 6])
def test_mapped_network_and_ramdisk_roots_are_not_durable_authorities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drive_type: int
) -> None:
    if os.name != "nt":
        pytest.skip("GetDriveTypeW policy is Windows-specific")
    import pm_native_lifecycle as lifecycle

    roots = _roots(tmp_path)
    monkeypatch.setattr(lifecycle, "_windows_drive_type", lambda _path: drive_type)
    with pytest.raises(LifecycleRootError, match=f"drive type {drive_type}"):
        _authority(roots, _limits(), provision=True)


def test_bound_root_and_runtime_subroot_replacement_fail_closed(tmp_path: Path) -> None:
    roots = _roots(tmp_path, runtime=True)
    stream_root = roots["runtime"] / "EURUSD" / "M5"
    stream_root.mkdir(parents=True)
    domain = OperationalDomain(
        roots["runtime"], max_bytes=500_000, max_files=30, max_active_reservations=6
    )
    authority = _authority(roots, _limits(), provision=True, operational_domain=domain)
    stream_sha = hashlib.sha256(b"root-replacement").hexdigest()
    reservation = authority.reserve_runtime_storage(
        operational_root=stream_root,
        stream_identity_sha256=stream_sha,
        requested_peak_bytes=100_000,
    )

    moved_stream = stream_root.with_name("M5-replaced")
    stream_root.rename(moved_stream)
    stream_root.mkdir()
    with pytest.raises(LifecycleIntegrityError, match="stream root was replaced"):
        authority.verify_runtime_storage(
            reservation,
            operational_root=stream_root,
            stream_identity_sha256=stream_sha,
            requested_peak_bytes=100_000,
        )

    moved_native = roots["native"].with_name("native-evidence-replaced")
    roots["native"].rename(moved_native)
    roots["native"].mkdir()
    with pytest.raises(LifecycleIntegrityError, match="root was replaced"):
        authority.lease("must-not-enter")

def test_strict_state_machine_and_crash_reopen_never_assume_path_success(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    limits = _limits()
    authority = _authority(roots, limits, provision=True)
    first = authority.lease("writer-one")
    first.prepare_operation("audit-crash", GrowthBudget.audit(expected_bytes=100, file_count=1))
    with pytest.raises(LifecycleStateError, match="PREPARED -> STAGED"):
        first.transition("audit-crash", OperationState.STAGED)
    first.transition("audit-crash", OperationState.WRITING)
    first.close()

    # A superficially complete path is deliberately misleading. Startup
    # recovery reports the DB state and does not inspect/adopt it.
    run = roots["native"] / "runs" / "misleading"
    run.mkdir(parents=True)
    (run / "run_manifest.json").write_text('{"complete":true}', encoding="utf-8")
    reopened = _authority(roots, limits, provision=False)
    with reopened.lease("recovery-owner") as recovered:
        assert len(recovered.recovery_report) == 1
        report = recovered.recovery_report[0]
        assert report.previous_state == OperationState.WRITING.value
        assert report.current_state == OperationState.RECOVERY_BLOCKED.value
        assert "No filesystem path" in report.instruction
        assert recovered.reservation("audit-crash").state == "RECOVERY_BLOCKED"
        resumed = recovered.resolve_recovery(
            "audit-crash", action="RESUME", note="operator verified the staged audit is absent"
        )
        assert resumed.state == "WRITING"
        recovered.transition("audit-crash", OperationState.STAGED)
        _abort(recovered, "audit-crash")


def test_live_operation_rows_are_authenticated_before_recovery_or_admission(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path / "state-tamper")
    authority = _authority(
        roots, _limits(max_hot_bytes=1_000), provision=True
    )
    lease = authority.lease("crashed-writer")
    lease.prepare_operation(
        "reserved-900", GrowthBudget.audit(expected_bytes=900, file_count=0)
    )
    lease.close()

    # An offline writer can trivially recompute plain SHA-256 columns. It must
    # not hide an active reservation by inventing a terminal state and thereby
    # admit a second reservation beyond the institution-wide cap.
    with sqlite3.connect(authority.db_path) as connection:
        connection.execute(
            "UPDATE operations SET state='COMMITTED' WHERE operation_id='reserved-900'"
        )
        connection.commit()
    with pytest.raises(LifecycleIntegrityError, match="operation ledger state HMAC"):
        authority.lease("must-not-admit")

    roots_two = _roots(tmp_path / "budget-tamper")
    authority_two = _authority(
        roots_two, _limits(max_hot_bytes=1_000), provision=True
    )
    lease_two = authority_two.lease("budget-writer")
    lease_two.prepare_operation(
        "reserved-900", GrowthBudget.audit(expected_bytes=900, file_count=0)
    )
    lease_two.close()
    zero = asdict(GrowthBudget.audit(expected_bytes=0, file_count=0))
    zero_bytes = (
        json.dumps(zero, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")
    with sqlite3.connect(authority_two.db_path) as connection:
        connection.execute(
            "UPDATE operations SET budget_json=?,budget_sha256=? "
            "WHERE operation_id='reserved-900'",
            (zero_bytes.decode("utf-8"), hashlib.sha256(zero_bytes).hexdigest()),
        )
        connection.commit()
    with pytest.raises(LifecycleIntegrityError, match="operation record HMAC"):
        authority_two.lease("tampered-budget-must-fail")


def test_materialization_requires_preapproved_top_up(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    authority = _authority(roots, _limits(), provision=True)
    with authority.lease("bounded-writer") as lease:
        lease.prepare_operation(
            "audit-growth", GrowthBudget.audit(expected_bytes=100, file_count=2)
        )
        observed = lease.record_materialized(
            "audit-growth", native_peak_bytes=80, native_final_bytes=80, native_files=1
        )
        assert observed.materialized_native_peak_bytes == 80
        with pytest.raises(LifecycleBoundError, match="top-up required"):
            lease.record_materialized(
                "audit-growth", native_peak_bytes=120, native_final_bytes=120
            )
        enlarged = lease.top_up(
            "audit-growth", native_peak_bytes=50, native_final_bytes=50, native_files=1
        )
        assert enlarged.budget.native_peak_bytes == 150
        final = lease.record_materialized(
            "audit-growth", native_peak_bytes=120, native_final_bytes=120, native_files=2
        )
        assert final.materialized_native_final_bytes == 120
        _abort(lease, "audit-growth")


def test_external_runtime_quota_receipt_is_exact_authenticated_and_current(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    stream_root = roots["runtime"] / "EURUSD" / "M5"
    stream_root.mkdir(parents=True)
    domain = OperationalDomain(
        root=roots["runtime"], max_bytes=512 * 1024, max_files=30, max_active_reservations=6
    )
    authority = _authority(roots, _limits(), provision=True, operational_domain=domain)
    stream_sha = hashlib.sha256(b"EURUSD-M5-state").hexdigest()
    reservation = authority.reserve_runtime_storage(
        operational_root=stream_root,
        stream_identity_sha256=stream_sha,
        requested_peak_bytes=128 * 1024,
    )

    assert reservation.authority_identity_sha256 == authority.identity_sha256
    assert Path(reservation.operational_domain_root).resolve() == roots["runtime"].resolve()
    assert reservation.stream_subpath == "EURUSD/M5"
    assert reservation.reserved_peak_bytes == 128 * 1024
    assert reservation.admitted_streams_cap == 6
    # This is an exact structural adapter, not a look-alike with extra or
    # missing fields. The state runtime can materialize its own frozen type.
    from pm_state_runtime import StorageReservation

    state_runtime_receipt = StorageReservation(**reservation.canonical())
    authority.verify_runtime_storage(
        state_runtime_receipt,
        operational_root=stream_root,
        stream_identity_sha256=stream_sha,
        requested_peak_bytes=128 * 1024,
    )
    # Reopen is idempotent and does not invalidate the first durable receipt.
    assert authority.reserve_runtime_storage(
        operational_root=stream_root,
        stream_identity_sha256=stream_sha,
        requested_peak_bytes=128 * 1024,
    ) == reservation

    forged = type(reservation)(
        **{**reservation.canonical(), "stream_subpath": "EURUSD/D1"}
    )
    with pytest.raises(LifecycleIntegrityError):
        authority.verify_runtime_storage(
            forged,
            operational_root=stream_root,
            stream_identity_sha256=stream_sha,
            requested_peak_bytes=128 * 1024,
        )

    connection = authority._connect()
    try:
        connection.execute(
            "UPDATE runtime_reservations SET receipt_mac='' WHERE reservation_id=?",
            (reservation.reservation_id,),
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(LifecycleIntegrityError, match="receipt MAC"):
        authority.reserve_runtime_storage(
            operational_root=stream_root,
            stream_identity_sha256=stream_sha,
            requested_peak_bytes=128 * 1024,
        )


def test_runtime_domain_aggregate_allows_only_one_reservation_that_fits(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    first_root = roots["runtime"] / "EURUSD" / "M5"
    second_root = roots["runtime"] / "USDJPY" / "M5"
    first_root.mkdir(parents=True)
    second_root.mkdir(parents=True)
    domain = OperationalDomain(
        root=roots["runtime"], max_bytes=150_000, max_files=20, max_active_reservations=6
    )
    authority = _authority(roots, _limits(), provision=True, operational_domain=domain)
    authority.reserve_runtime_storage(
        operational_root=first_root,
        stream_identity_sha256=hashlib.sha256(b"first").hexdigest(),
        requested_peak_bytes=100_000,
    )
    nested_root = first_root / "nested-stream"
    nested_root.mkdir()
    with pytest.raises(LifecycleIntegrityError, match="cannot overlap"):
        authority.reserve_runtime_storage(
            operational_root=nested_root,
            stream_identity_sha256=hashlib.sha256(b"nested").hexdigest(),
            requested_peak_bytes=10_000,
        )
    with pytest.raises(LifecycleBoundError, match="domain cap"):
        authority.reserve_runtime_storage(
            operational_root=second_root,
            stream_identity_sha256=hashlib.sha256(b"second").hexdigest(),
            requested_peak_bytes=100_000,
        )


def test_runtime_monotonic_witness_blocks_rollback_and_stale_cas(tmp_path: Path) -> None:
    roots = _roots(tmp_path, runtime=True)
    stream_root = roots["runtime"] / "EURUSD" / "M5"
    stream_root.mkdir(parents=True)
    domain = OperationalDomain(
        roots["runtime"], max_bytes=500_000, max_files=30, max_active_reservations=6
    )
    authority = _authority(roots, _limits(), provision=True, operational_domain=domain)
    stream_sha = hashlib.sha256(b"witness-stream").hexdigest()
    reservation = authority.reserve_runtime_storage(
        operational_root=stream_root,
        stream_identity_sha256=stream_sha,
        requested_peak_bytes=100_000,
    )
    genesis = authority.read_runtime_witness(reservation)
    reducer_hash = hashlib.sha256(b"runtime-reducer-contract-v1").hexdigest()
    assert genesis.witness_version == genesis.writer_epoch == 0
    assert genesis.reducer_contract_sha256 == GENESIS_SHA256
    assert genesis.max_bar_sequence == genesis.max_order_event_sequence == 0
    assert genesis.bar_chain_sha256 == genesis.checkpoint_sha256 == GENESIS_SHA256
    assert genesis.order_chain_sha256 == GENESIS_SHA256

    order_hash = hashlib.sha256(b"signed-order-intent-1").hexdigest()
    first = authority.cas_runtime_witness(
        reservation,
        genesis,
        {
            "writer_epoch": 1,
            "reducer_contract_sha256": reducer_hash,
            "max_bar_sequence": 0,
            "bar_chain_sha256": GENESIS_SHA256,
            "checkpoint_generation": 0,
            "checkpoint_sha256": GENESIS_SHA256,
            "max_order_operation_sequence": 1,
            "max_order_event_sequence": 1,
            "order_chain_sha256": order_hash,
        },
    )
    assert first.witness_version == 1
    assert first.max_order_operation_sequence == first.max_order_event_sequence == 1

    # A rolled-back state process holding the old witness cannot reuse op 1.
    with pytest.raises(LifecycleCASMismatch, match="stale"):
        authority.cas_runtime_witness(
            reservation,
            genesis,
            {
                "writer_epoch": 1,
                "reducer_contract_sha256": reducer_hash,
                "max_bar_sequence": 0,
                "bar_chain_sha256": GENESIS_SHA256,
                "checkpoint_generation": 0,
                "checkpoint_sha256": GENESIS_SHA256,
                "max_order_operation_sequence": 1,
                "max_order_event_sequence": 1,
                "order_chain_sha256": order_hash,
            },
        )
    with pytest.raises(LifecycleCASMismatch, match="hash changed"):
        authority.cas_runtime_witness(
            reservation,
            first,
            {
                "writer_epoch": 1,
                "reducer_contract_sha256": reducer_hash,
                "max_bar_sequence": 0,
                "bar_chain_sha256": hashlib.sha256(b"forged").hexdigest(),
                "checkpoint_generation": 0,
                "checkpoint_sha256": GENESIS_SHA256,
                "max_order_operation_sequence": 1,
                "max_order_event_sequence": 1,
                "order_chain_sha256": order_hash,
            },
        )
    with pytest.raises(ValueError, match="bounded"):
        authority.cas_runtime_witness(
            reservation,
            first,
            {
                "writer_epoch": 10**30,
                "reducer_contract_sha256": reducer_hash,
                "max_bar_sequence": 0,
                "bar_chain_sha256": GENESIS_SHA256,
                "checkpoint_generation": 0,
                "checkpoint_sha256": GENESIS_SHA256,
                "max_order_operation_sequence": 1,
                "max_order_event_sequence": 1,
                "order_chain_sha256": order_hash,
            },
        )
    assert authority.read_runtime_witness(reservation) == first

    connection = authority._connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM runtime_witnesses").fetchone()[0] == 1
    finally:
        connection.close()
    authority.release_runtime_storage(reservation)
    with pytest.raises(LifecycleStateError, match="inactive"):
        authority.read_runtime_witness(reservation)


def test_broker_execution_claim_is_account_global_exact_and_out_of_order_safe(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    first_root = roots["runtime"] / "EURUSD" / "M5"
    second_root = roots["runtime"] / "USDJPY" / "M5"
    first_root.mkdir(parents=True)
    second_root.mkdir(parents=True)
    domain = OperationalDomain(
        roots["runtime"], max_bytes=500_000, max_files=30, max_active_reservations=6
    )
    authority = _authority(roots, _limits(), provision=True, operational_domain=domain)
    first = authority.reserve_runtime_storage(
        operational_root=first_root,
        stream_identity_sha256=hashlib.sha256(b"broker-first-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    second = authority.reserve_runtime_storage(
        operational_root=second_root,
        stream_identity_sha256=hashlib.sha256(b"broker-second-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    _witness_order_highwater(authority, first, 3)
    _witness_order_highwater(authority, second, 2)
    account = hashlib.sha256(b"MT5|Demo-Server|10001").hexdigest()
    first_payload = hashlib.sha256(b"authenticated-fill-777").hexdigest()
    first_operation = _operation_identity(first, 1)
    receipt_777 = _broker_highwater(account, 777)
    claim = authority.claim_broker_execution(
        first,
        account_scope_sha256=account,
        broker_execution_id="777",
        broker_execution_sequence=777,
        operation_id=first_operation,
        operation_sequence=1,
        payload_sha256=first_payload,
        broker_highwater_receipt=receipt_777,
    )
    assert claim.claim_sequence == 1
    assert claim.stream_identity_sha256 == first.stream_identity_sha256
    from pm_state_runtime import BrokerExecutionClaim as StateBrokerExecutionClaim

    state_claim = StateBrokerExecutionClaim(**claim.canonical())
    # Exact retries are stable even though acquiring the retry lease advances
    # the global fence token.
    assert authority.claim_broker_execution(
        first,
        account_scope_sha256=account,
        broker_execution_id="777",
        broker_execution_sequence=777,
        operation_id=first_operation,
        operation_sequence=1,
        payload_sha256=first_payload,
        broker_highwater_receipt=receipt_777,
    ) == claim
    authority.verify_broker_execution_claim(
        state_claim,
        first,
        account_scope_sha256=account,
        broker_execution_id="777",
        broker_execution_sequence=777,
        operation_id=first_operation,
        operation_sequence=1,
        payload_sha256=first_payload,
        broker_highwater_receipt=receipt_777,
    )

    with pytest.raises(LifecycleCASMismatch, match="already claimed"):
        authority.claim_broker_execution(
            second,
            account_scope_sha256=account,
            broker_execution_id="777",
            broker_execution_sequence=777,
            operation_id=_operation_identity(second, 1),
            operation_sequence=1,
            payload_sha256=hashlib.sha256(b"conflicting-fill").hexdigest(),
            broker_highwater_receipt=receipt_777,
        )
    with pytest.raises(ValueError, match="canonical decimal"):
        authority.claim_broker_execution(
            second,
            account_scope_sha256=account,
            broker_execution_id="different-id",
            broker_execution_sequence=777,
            operation_id=_operation_identity(second, 2),
            operation_sequence=2,
            payload_sha256=hashlib.sha256(b"same-sequence").hexdigest(),
            broker_highwater_receipt=receipt_777,
        )

    receipt_900 = _broker_highwater(account, 900)
    later_ticket = authority.claim_broker_execution(
        second,
        account_scope_sha256=account,
        broker_execution_id="900",
        broker_execution_sequence=900,
        operation_id=_operation_identity(second, 2),
        operation_sequence=2,
        payload_sha256=hashlib.sha256(b"fill-900").hexdigest(),
        broker_highwater_receipt=receipt_900,
    )
    delayed_lower_ticket = authority.claim_broker_execution(
        first,
        account_scope_sha256=account,
        broker_execution_id="850",
        broker_execution_sequence=850,
        operation_id=_operation_identity(first, 2),
        operation_sequence=2,
        payload_sha256=hashlib.sha256(b"delayed-fill-850").hexdigest(),
        broker_highwater_receipt=receipt_900,
    )
    assert (later_ticket.claim_sequence, delayed_lower_ticket.claim_sequence) == (2, 3)
    with pytest.raises(LifecycleIntegrityError, match="different broker account"):
        authority.claim_broker_execution(
            first,
            account_scope_sha256=hashlib.sha256(b"other-account").hexdigest(),
            broker_execution_id="901",
            broker_execution_sequence=901,
            operation_id=_operation_identity(first, 3),
            operation_sequence=3,
            payload_sha256=hashlib.sha256(b"wrong-account").hexdigest(),
            broker_highwater_receipt=_broker_highwater(
                hashlib.sha256(b"other-account").hexdigest(), 901
            ),
        )


def test_broker_highwater_is_mandatory_forgery_safe_and_never_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    stream_root = roots["runtime"] / "EURUSD" / "M5"
    stream_root.mkdir(parents=True)
    authority = _authority(
        roots,
        _limits(),
        provision=True,
        operational_domain=OperationalDomain(
            roots["runtime"], max_bytes=200_000, max_files=10,
            max_active_reservations=2,
        ),
    )
    reservation = authority.reserve_runtime_storage(
        operational_root=stream_root,
        stream_identity_sha256=hashlib.sha256(b"highwater-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    _witness_order_highwater(authority, reservation, 1)
    archive_scan_modes: list[bool] = []
    identity_scan_modes: list[bool] = []
    original_archive_loader = authority._load_archive_head
    original_identity_verifier = authority._verify_database_identity

    def tracked_archive_loader(
        connection: sqlite3.Connection, *, verify_rows: bool = True
    ):
        archive_scan_modes.append(verify_rows)
        return original_archive_loader(connection, verify_rows=verify_rows)

    monkeypatch.setattr(authority, "_load_archive_head", tracked_archive_loader)

    def tracked_identity_verifier(
        connection: sqlite3.Connection, *, deep: bool = False
    ) -> None:
        identity_scan_modes.append(deep)
        original_identity_verifier(connection, deep=deep)

    monkeypatch.setattr(
        authority, "_verify_database_identity", tracked_identity_verifier
    )
    account = hashlib.sha256(b"highwater-account").hexdigest()
    payload = hashlib.sha256(b"highwater-fill").hexdigest()
    values = {
        "account_scope_sha256": account,
        "broker_execution_id": "1",
        "broker_execution_sequence": 1,
        "operation_id": _operation_identity(reservation, 1),
        "operation_sequence": 1,
        "payload_sha256": payload,
    }
    with pytest.raises(LifecycleIntegrityError, match="high-water receipt is required"):
        authority.claim_broker_execution(
            reservation, broker_highwater_receipt=None, **values
        )

    receipt = _broker_highwater(account, 2)
    forged = asdict(receipt)
    forged["mac"] = "0" * 64
    with pytest.raises(LifecycleIntegrityError, match="high-water receipt HMAC"):
        authority.claim_broker_execution(
            reservation, broker_highwater_receipt=forged, **values
        )
    with pytest.raises(LifecycleIntegrityError, match="high-water account mismatch"):
        authority.claim_broker_execution(
            reservation,
            broker_highwater_receipt=_broker_highwater(
                hashlib.sha256(b"wrong-highwater-account").hexdigest(), 2
            ),
            **values,
        )

    claim = authority.claim_broker_execution(
        reservation, broker_highwater_receipt=receipt, **values
    )
    with pytest.raises(LifecycleRecoveryRequired, match="high-water receipt rolled back"):
        authority.claim_broker_execution(
            reservation,
            broker_highwater_receipt=_broker_highwater(account, 1),
            **values,
        )
    authority.verify_broker_execution_claim(
        claim, reservation, broker_highwater_receipt=receipt, **values
    )
    assert archive_scan_modes and not any(archive_scan_modes)
    assert identity_scan_modes and not any(identity_scan_modes)
    authority.audit_lifecycle_archive()
    assert archive_scan_modes[-1] is True
    assert identity_scan_modes[-1] is True
    with sqlite3.connect(authority.db_path) as connection:
        connection.execute("DROP TRIGGER broker_claim_delete_guard")
        connection.execute(
            "CREATE TRIGGER broker_claim_delete_guard AFTER DELETE ON "
            "broker_execution_claims BEGIN SELECT 1; END"
        )
    with pytest.raises(LifecycleIntegrityError, match="guard-trigger schema"):
        authority.audit_lifecycle_archive()


def test_broker_execution_claim_cap_and_tamper_fail_closed(tmp_path: Path) -> None:
    roots = _roots(tmp_path, runtime=True)
    stream_root = roots["runtime"] / "EURUSD" / "M5"
    second_stream_root = roots["runtime"] / "USDJPY" / "M5"
    stream_root.mkdir(parents=True)
    second_stream_root.mkdir(parents=True)
    domain = OperationalDomain(
        roots["runtime"], max_bytes=300_000, max_files=10, max_active_reservations=2
    )
    limits = _limits(
        max_broker_execution_claims=2,
        max_broker_account_scopes=1,
        max_broker_reorder_sequences=1,
        max_operation_reorder_sequences=1,
    )
    authority = _authority(roots, limits, provision=True, operational_domain=domain)
    reservation = authority.reserve_runtime_storage(
        operational_root=stream_root,
        stream_identity_sha256=hashlib.sha256(b"tamper-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    second_reservation = authority.reserve_runtime_storage(
        operational_root=second_stream_root,
        stream_identity_sha256=hashlib.sha256(b"second-cap-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    _witness_order_highwater(authority, reservation, 4)
    _witness_order_highwater(authority, second_reservation, 1)
    account = hashlib.sha256(b"account-tamper").hexdigest()
    payload = hashlib.sha256(b"fill-1").hexdigest()
    receipt_1 = _broker_highwater(account, 1)
    claim = authority.claim_broker_execution(
        reservation,
        account_scope_sha256=account,
        broker_execution_id="1",
        broker_execution_sequence=1,
        operation_id=_operation_identity(reservation, 1),
        operation_sequence=1,
        payload_sha256=payload,
        broker_highwater_receipt=receipt_1,
    )
    receipt_2 = _broker_highwater(account, 2)
    second_claim = authority.claim_broker_execution(
        reservation,
        account_scope_sha256=account,
        broker_execution_id="2",
        broker_execution_sequence=2,
        operation_id=_operation_identity(reservation, 2),
        operation_sequence=2,
        payload_sha256=hashlib.sha256(b"fill-2").hexdigest(),
        broker_highwater_receipt=receipt_2,
    )
    assert second_claim.claim_sequence == 2
    receipt_3 = _broker_highwater(account, 3)
    third_payload = hashlib.sha256(b"fill-3").hexdigest()
    third_claim = authority.claim_broker_execution(
        reservation,
        account_scope_sha256=account,
        broker_execution_id="3",
        broker_execution_sequence=3,
        operation_id=_operation_identity(reservation, 3),
        operation_sequence=3,
        payload_sha256=third_payload,
        broker_highwater_receipt=receipt_3,
    )
    assert third_claim.claim_sequence == 3
    authority.verify_broker_execution_claim(
        claim,
        reservation,
        account_scope_sha256=account,
        broker_execution_id="1",
        broker_execution_sequence=1,
        operation_id=_operation_identity(reservation, 1),
        operation_sequence=1,
        payload_sha256=payload,
        broker_highwater_receipt=receipt_1,
    )
    with pytest.raises(LifecycleCASMismatch, match="retired floor"):
        authority.claim_broker_execution(
            reservation,
            account_scope_sha256=account,
            broker_execution_id="1",
            broker_execution_sequence=1,
            operation_id=_operation_identity(reservation, 4),
            operation_sequence=4,
            payload_sha256=payload,
            broker_highwater_receipt=receipt_3,
        )
    with pytest.raises(LifecycleBoundError, match="account-scope row cap"):
        authority.claim_broker_execution(
            second_reservation,
            account_scope_sha256=hashlib.sha256(b"second-account").hexdigest(),
            broker_execution_id="4",
            broker_execution_sequence=4,
            operation_id=_operation_identity(second_reservation, 1),
            operation_sequence=1,
            payload_sha256=hashlib.sha256(b"second-account-fill").hexdigest(),
            broker_highwater_receipt=_broker_highwater(
                hashlib.sha256(b"second-account").hexdigest(), 4
            ),
        )

    connection = authority._connect()
    try:
        assert connection.execute(
            "SELECT COUNT(*) FROM broker_execution_claims"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type='table' AND name='broker_execution_tombstones'"
        ).fetchone()[0] == 0
        connection.execute(
            "UPDATE broker_execution_claims SET receipt_mac='' WHERE claim_id=?",
            (second_claim.claim_id,),
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(LifecycleIntegrityError, match="receipt HMAC"):
        authority.verify_broker_execution_claim(
            second_claim,
            reservation,
            account_scope_sha256=account,
            broker_execution_id="2",
            broker_execution_sequence=2,
            operation_id=_operation_identity(reservation, 2),
            operation_sequence=2,
            payload_sha256=hashlib.sha256(b"fill-2").hexdigest(),
            broker_highwater_receipt=receipt_2,
        )
    # The hardened authority refuses even a management connection while the
    # complete ledger is corrupt; use a raw forensic handle to restore the
    # injected byte before testing the independent head-MAC failure.
    connection = sqlite3.connect(authority.db_path)
    try:
        connection.execute(
            "UPDATE broker_execution_claims SET receipt_mac=? WHERE claim_id=?",
            (second_claim.receipt_mac, second_claim.claim_id),
        )
        connection.execute(
            "UPDATE broker_account_heads SET record_hmac='' "
            "WHERE account_scope_sha256=?",
            (account,),
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(LifecycleIntegrityError, match="head HMAC"):
        authority.claim_broker_execution(
            reservation,
            account_scope_sha256=account,
            broker_execution_id="3",
            broker_execution_sequence=3,
            operation_id=_operation_identity(reservation, 3),
            operation_sequence=3,
            payload_sha256=third_payload,
            broker_highwater_receipt=receipt_3,
        )


def test_broker_compaction_authenticates_an_unrelated_account_head(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    first_root = roots["runtime"] / "EURUSD" / "M5"
    second_root = roots["runtime"] / "USDJPY" / "M5"
    first_root.mkdir(parents=True)
    second_root.mkdir(parents=True)
    limits = _limits(
        max_broker_execution_claims=3,
        max_broker_account_scopes=2,
        max_broker_reorder_sequences=1,
        max_operation_reorder_sequences=0,
    )
    authority = _authority(
        roots,
        limits,
        provision=True,
        operational_domain=OperationalDomain(
            roots["runtime"], max_bytes=300_000, max_files=20,
            max_active_reservations=3,
        ),
    )
    first = authority.reserve_runtime_storage(
        operational_root=first_root,
        stream_identity_sha256=hashlib.sha256(b"account-a-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    second = authority.reserve_runtime_storage(
        operational_root=second_root,
        stream_identity_sha256=hashlib.sha256(b"account-b-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    _witness_order_highwater(authority, first, 2)
    _witness_order_highwater(authority, second, 2)
    account_a = hashlib.sha256(b"compaction-account-a").hexdigest()
    account_b = hashlib.sha256(b"compaction-account-b").hexdigest()
    first_claim_id = ""
    for sequence in (1, 2):
        claim = authority.claim_broker_execution(
            first,
            account_scope_sha256=account_a,
            broker_execution_id=str(sequence),
            broker_execution_sequence=sequence,
            operation_id=_operation_identity(first, sequence),
            operation_sequence=sequence,
            payload_sha256=hashlib.sha256(
                f"compaction-a-{sequence}".encode()
            ).hexdigest(),
            broker_highwater_receipt=_broker_highwater(account_a, sequence),
        )
        if sequence == 1:
            first_claim_id = claim.claim_id
    authority.claim_broker_execution(
        second,
        account_scope_sha256=account_b,
        broker_execution_id="1",
        broker_execution_sequence=1,
        operation_id=_operation_identity(second, 1),
        operation_sequence=1,
        payload_sha256=hashlib.sha256(b"compaction-b-1").hexdigest(),
        broker_highwater_receipt=_broker_highwater(account_b, 1),
    )

    with sqlite3.connect(authority.db_path) as connection:
        original_mac = connection.execute(
            "SELECT record_hmac FROM broker_account_heads "
            "WHERE account_scope_sha256=?",
            (account_a,),
        ).fetchone()[0]
        connection.execute(
            "UPDATE broker_account_heads SET retired_execution_floor=1 "
            "WHERE account_scope_sha256=?",
            (account_a,),
        )

    with pytest.raises(LifecycleIntegrityError, match="head HMAC"):
        authority.claim_broker_execution(
            second,
            account_scope_sha256=account_b,
            broker_execution_id="2",
            broker_execution_sequence=2,
            operation_id=_operation_identity(second, 2),
            operation_sequence=2,
            payload_sha256=hashlib.sha256(b"compaction-b-2").hexdigest(),
            broker_highwater_receipt=_broker_highwater(account_b, 2),
        )
    with sqlite3.connect(authority.db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM broker_execution_claims WHERE claim_id=?",
            (first_claim_id,),
        ).fetchone()[0] == 1
        connection.execute(
            "UPDATE broker_account_heads SET retired_execution_floor=0," 
            "record_hmac=? WHERE account_scope_sha256=?",
            (original_mac, account_a),
        )
    audit = authority.audit_broker_ledger()
    assert audit["total_claim_count"] == 3
    assert audit["live_claim_count"] == 3


def test_broker_execution_claim_database_rollback_is_fenced(tmp_path: Path) -> None:
    roots = _roots(tmp_path, runtime=True)
    stream_root = roots["runtime"] / "EURUSD" / "M5"
    stream_root.mkdir(parents=True)
    domain = OperationalDomain(
        roots["runtime"], max_bytes=300_000, max_files=10, max_active_reservations=2
    )
    authority = _authority(roots, _limits(), provision=True, operational_domain=domain)
    reservation = authority.reserve_runtime_storage(
        operational_root=stream_root,
        stream_identity_sha256=hashlib.sha256(b"rollback-claim-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    _witness_order_highwater(authority, reservation, 1)
    old_database = authority.db_path.read_bytes()
    account = hashlib.sha256(b"rollback-account").hexdigest()
    payload = hashlib.sha256(b"rollback-fill").hexdigest()
    highwater = _broker_highwater(account, 7001)
    operation_id = _operation_identity(reservation, 1)
    claim = authority.claim_broker_execution(
        reservation,
        account_scope_sha256=account,
        broker_execution_id="7001",
        broker_execution_sequence=7001,
        operation_id=operation_id,
        operation_sequence=1,
        payload_sha256=payload,
        broker_highwater_receipt=highwater,
    )
    with authority.db_path.open("r+b") as handle:
        handle.seek(0)
        handle.write(old_database)
        handle.truncate()
        handle.flush()
        os.fsync(handle.fileno())
    with pytest.raises(LifecycleRecoveryRequired, match="rollback|ambiguous"):
        authority.verify_broker_execution_claim(
            claim,
            reservation,
            account_scope_sha256=account,
            broker_execution_id="7001",
            broker_execution_sequence=7001,
            operation_id=operation_id,
            operation_sequence=1,
            payload_sha256=payload,
            broker_highwater_receipt=highwater,
        )


def test_released_runtime_archives_operation_floor_and_old_claims_remain_verifiable(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    first_root = roots["runtime"] / "EURUSD" / "M5"
    second_root = roots["runtime"] / "USDJPY" / "M5"
    first_root.mkdir(parents=True)
    second_root.mkdir(parents=True)
    limits = _limits(
        max_operations=1,
        max_broker_execution_claims=2,
        max_broker_reorder_sequences=1,
        max_operation_reorder_sequences=1,
    )
    authority = _authority(
        roots,
        limits,
        provision=True,
        operational_domain=OperationalDomain(
            roots["runtime"], 400_000, 20, 4
        ),
    )
    first = authority.reserve_runtime_storage(
        operational_root=first_root,
        stream_identity_sha256=hashlib.sha256(b"released-claim-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    _witness_order_highwater(authority, first, 3)
    account = hashlib.sha256(b"released-claim-account").hexdigest()
    first_claims = []
    for sequence in (1, 2, 3):
        payload = hashlib.sha256(f"released-fill-{sequence}".encode()).hexdigest()
        highwater = _broker_highwater(account, sequence)
        claim = authority.claim_broker_execution(
            first,
            account_scope_sha256=account,
            broker_execution_id=str(sequence),
            broker_execution_sequence=sequence,
            operation_id=_operation_identity(first, sequence),
            operation_sequence=sequence,
            payload_sha256=payload,
            broker_highwater_receipt=highwater,
        )
        first_claims.append((claim, payload, highwater))

    authority.release_runtime_storage(first)
    second = authority.reserve_runtime_storage(
        operational_root=second_root,
        stream_identity_sha256=hashlib.sha256(b"replacement-claim-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    _witness_order_highwater(authority, second, 2)
    for operation_sequence, broker_sequence in enumerate((4, 5), start=1):
        authority.claim_broker_execution(
            second,
            account_scope_sha256=account,
            broker_execution_id=str(broker_sequence),
            broker_execution_sequence=broker_sequence,
            operation_id=_operation_identity(second, operation_sequence),
            operation_sequence=operation_sequence,
            payload_sha256=hashlib.sha256(
                f"replacement-fill-{broker_sequence}".encode()
            ).hexdigest(),
            broker_highwater_receipt=_broker_highwater(account, broker_sequence),
        )

    for sequence, (claim, payload, highwater) in enumerate(first_claims, start=1):
        authority.verify_broker_execution_claim(
            claim,
            first,
            account_scope_sha256=account,
            broker_execution_id=str(sequence),
            broker_execution_sequence=sequence,
            operation_id=_operation_identity(first, sequence),
            operation_sequence=sequence,
            payload_sha256=payload,
            broker_highwater_receipt=highwater,
        )
    audit = authority.audit_broker_ledger()
    assert audit["total_claim_count"] == 5
    assert audit["live_claim_count"] == 2
    with sqlite3.connect(authority.db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM runtime_claim_heads "
            "WHERE stream_identity_sha256=?",
            (first.stream_identity_sha256,),
        ).fetchone()[0] == 0
        archived = connection.execute(
            "SELECT max_operation_sequence,retired_operation_floor "
            "FROM runtime_tombstones WHERE reservation_id=?",
            (first.reservation_id,),
        ).fetchone()
        assert tuple(archived) == (3, 3)


def test_broker_recent_window_stays_fixed_across_two_thousand_claims(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    stream_root = roots["runtime"] / "EURUSD" / "M5"
    stream_root.mkdir(parents=True)
    limits = _limits(
        max_broker_execution_claims=32,
        max_broker_reorder_sequences=8,
        max_operation_reorder_sequences=8,
        broker_full_audit_batch_rows=7,
        lease_seconds=300.0,
    )
    authority = _authority(
        roots,
        limits,
        provision=True,
        operational_domain=OperationalDomain(
            roots["runtime"], max_bytes=200_000, max_files=10,
            max_active_reservations=2,
        ),
    )
    reservation = authority.reserve_runtime_storage(
        operational_root=stream_root,
        stream_identity_sha256=hashlib.sha256(b"bounded-broker-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    claim_total = 2_048
    _witness_order_highwater(authority, reservation, claim_total + 1)
    account = hashlib.sha256(b"bounded-broker-account").hexdigest()
    first_claim = None
    first_payload = ""
    first_highwater = None
    first_footprint = 0
    with authority.lease("bounded-broker-load") as lease:
        for sequence in range(1, claim_total + 1):
            payload = hashlib.sha256(
                f"bounded-fill-{sequence}".encode()
            ).hexdigest()
            highwater = _broker_highwater(account, sequence)
            claim = lease._claim_broker_execution(
                reservation,
                account_scope_sha256=account,
                broker_execution_id=str(sequence),
                broker_execution_sequence=sequence,
                operation_id=_operation_identity(reservation, sequence),
                operation_sequence=sequence,
                payload_sha256=payload,
                broker_highwater_receipt=highwater,
            )
            if sequence == 1:
                first_claim = claim
                first_payload = payload
                first_highwater = highwater
            if sequence == claim_total // 2:
                first_footprint = authority.db_path.stat().st_size
    second_footprint = authority.db_path.stat().st_size

    assert first_claim is not None and first_highwater is not None
    assert second_footprint <= first_footprint + limits.page_size
    audit = authority.audit_broker_ledger()
    assert audit["total_claim_count"] == claim_total
    assert audit["live_claim_count"] == limits.max_broker_execution_claims
    authority.verify_broker_execution_claim(
        first_claim,
        reservation,
        account_scope_sha256=account,
        broker_execution_id="1",
        broker_execution_sequence=1,
        operation_id=_operation_identity(reservation, 1),
        operation_sequence=1,
        payload_sha256=first_payload,
        broker_highwater_receipt=first_highwater,
    )
    with pytest.raises(LifecycleCASMismatch, match="retired floor"):
        authority.claim_broker_execution(
            reservation,
            account_scope_sha256=account,
            broker_execution_id="1",
            broker_execution_sequence=1,
            operation_id=_operation_identity(reservation, claim_total + 1),
            operation_sequence=claim_total + 1,
            payload_sha256=hashlib.sha256(b"reused-old-ticket").hexdigest(),
            broker_highwater_receipt=_broker_highwater(
                account, claim_total + 1
            ),
        )
    with pytest.raises(LifecycleCASMismatch, match="operation.*retired floor"):
        authority.claim_broker_execution(
            reservation,
            account_scope_sha256=account,
            broker_execution_id=str(claim_total + 1),
            broker_execution_sequence=claim_total + 1,
            operation_id=_operation_identity(reservation, 1),
            operation_sequence=1,
            payload_sha256=hashlib.sha256(b"reused-old-operation").hexdigest(),
            broker_highwater_receipt=_broker_highwater(
                account, claim_total + 1
            ),
        )


def test_real_state_runtimes_share_execution_authority_and_ticket_zero_is_no_effect(
    tmp_path: Path,
) -> None:
    from pm_state_contract import DEFAULT_REGISTRY_HASH
    from pm_state_runtime import (
        AuthenticatedStateEvidence,
        AuthenticatedStateRuntime,
        OrderTransitionError,
        RuntimeLimits,
        SourceEpoch,
        WinnerRecord,
        WinnerRoster,
    )
    from tests.test_state_runtime import (
        BROKER_RESULT_KEYS,
        CADENCE_KEYS,
        REDUCER_KEYS,
        broker_no_effect,
        broker_result,
        issue_reducer_release,
    )

    roots = _roots(tmp_path, runtime=True)
    domain = OperationalDomain(
        roots["runtime"],
        max_bytes=12 * 1024 * 1024,
        max_files=20,
        max_active_reservations=6,
    )
    authority = _authority(
        roots,
        _limits(
            max_broker_execution_claims=2,
            max_broker_reorder_sequences=1,
            max_operation_reorder_sequences=1,
        ),
        provision=True,
        operational_domain=domain,
    )
    state_key = b"lifecycle-real-state-integration-key"
    source = SourceEpoch(
        broker="MT5",
        server="Demo-Server",
        account_id="10001",
        data_source="MT5_NATIVE",
        source_instance_id="terminal-1",
        session_calendar_id="mt5-demo-utc-v1",
        source_artifact_sha256=hashlib.sha256(
            b"lifecycle-native-v5-artifact"
        ).hexdigest(),
        epoch_id="epoch-integration",
        started_at="2026-01-01T00:00:00Z",
    )
    roster = WinnerRoster(
        roster_id="lifecycle-integration-roster",
        created_at="2026-01-01T00:00:00Z",
        registry_hash=DEFAULT_REGISTRY_HASH,
        winners=tuple(
            WinnerRecord(
                cell_id=f"{symbol}:M5:ALL",
                symbol=symbol,
                timeframe="M5",
                regime="ALL",
                strategy_name="ADXDIStrengthStrategy",
                include_regime_controller=True,
                initial_error_bound=1.0,
                tolerance=1e-6,
            )
            for symbol in ("EURUSD", "USDJPY")
        ),
    )
    state_limits = RuntimeLimits(
        max_db_bytes=2 * 1024 * 1024,
        max_total_storage_bytes=5 * 1024 * 1024,
        max_json_bytes=32 * 1024,
        max_state_bytes=128 * 1024,
        max_bar_events=8,
        max_bar_bytes=128 * 1024,
        max_bar_segments=4,
        max_segment_events=2,
        max_segment_bytes=32 * 1024,
        max_order_operations=4,
        max_order_events=12,
        max_pending_orders=4,
        busy_timeout_seconds=1.0,
        busy_retry_seconds=0.01,
        scan_batch_rows=2,
    )

    def build_runtime(symbol: str) -> AuthenticatedStateRuntime:
        engine = LifecycleIntegrationEngine()
        evidence = AuthenticatedStateEvidence.issue_for_default_contract(
            state_key,
            source,
            roster,
            "controller:REGIME_HYSTERESIS",
            {"controller_state": "authenticated-replay"},
            symbol=symbol,
            timeframe="M5",
        )
        runtime = AuthenticatedStateRuntime(
            roots["runtime"] / symbol / "M5",
            hmac_key=state_key,
            cadence_public_keys=CADENCE_KEYS,
            reducer_release_public_keys=REDUCER_KEYS,
            broker_result_public_keys=BROKER_RESULT_KEYS,
            source=source,
            roster=roster,
            symbol=symbol,
            timeframe="M5",
            engine=engine,
            reducer_release=issue_reducer_release(
                engine,
                release_id="lifecycle-integration-engine-v1",
                issued_at="2026-01-01T00:00:00Z",
            ),
            storage_authority=authority,
            state_evidence=(evidence,),
            limits=state_limits,
            provision=True,
        )
        runtime.bootstrap_checkpoint()
        assert runtime.trading_ready
        return runtime

    first = build_runtime("EURUSD")
    second = build_runtime("USDJPY")
    try:
        first_intent = first.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        second_intent = second.begin_order_intent(
            client_sequence=1, action="BUY", request={"lots": 0.1}
        )
        executed = broker_result(
            first_intent,
            {"ticket": 777, "retcode": 10009, "status": "FILLED"},
            highwater_receipt=_broker_highwater(
                first.execution_account_scope_sha256, 777
            ).canonical(),
        )
        first_result_receipt = first.record_order_result(first_intent, executed)
        first.apply_order_result_state(first_result_receipt)
        conflicting = broker_result(
            second_intent,
            {"ticket": 777, "retcode": 10009, "status": "FILLED"},
            highwater_receipt=_broker_highwater(
                second.execution_account_scope_sha256, 777
            ).canonical(),
        )
        with pytest.raises(OrderTransitionError, match="already claimed"):
            second.record_order_result(second_intent, conflicting)
        assert second.recovery.pending_orders[0].phase == "INTENT"

        rejected = broker_no_effect(
            second_intent,
            {"ticket": 0, "retcode": 10019, "status": "REJECTED"},
            reconciliation_receipt_id="mt5-request-2-retcode-10019",
        )
        second.apply_order_result_state(
            second.record_order_result(second_intent, rejected)
        )
        assert second.trading_ready
        for client_sequence, ticket in ((2, 778), (3, 779)):
            later_intent = first.begin_order_intent(
                client_sequence=client_sequence,
                action="BUY",
                request={"lots": 0.1},
            )
            later_result = broker_result(
                later_intent,
                {"ticket": ticket, "retcode": 10009, "status": "FILLED"},
                highwater_receipt=_broker_highwater(
                    first.execution_account_scope_sha256, ticket
                ).canonical(),
            )
            first.apply_order_result_state(
                first.record_order_result(later_intent, later_result)
            )

        # Ticket 777 and operation 1 have both left the bounded recent table.
        # Exact RESULT retry must validate the stored signed claim through the
        # authenticated floors; it must not try to acquire the identity again.
        assert first.record_order_result(
            first_intent, executed
        ) == first_result_receipt
        connection = authority._connect()
        try:
            rows = connection.execute(
                "SELECT broker_execution_id,broker_execution_sequence "
                "FROM broker_execution_claims ORDER BY broker_execution_sequence"
            ).fetchall()
            assert [tuple(row) for row in rows] == [("778", 778), ("779", 779)]
        finally:
            connection.close()
        audit = authority.audit_broker_ledger()
        assert audit["total_claim_count"] == 3
        assert audit["live_claim_count"] == 2
    finally:
        first.close()
        second.close()


def test_operational_materialization_is_bounded_and_receipts_become_stale(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    domain = OperationalDomain(
        root=roots["runtime"], max_bytes=200_000, max_files=20, max_active_reservations=6
    )
    authority = _authority(roots, _limits(), provision=True, operational_domain=domain)
    budget = GrowthBudget.operational_growth(
        kind=OperationKind.OPTIMIZER_WORKSPACE,
        domain="runtime_state",
        final_bytes=50_000,
        peak_bytes=80_000,
        file_count=4,
    )
    with authority.lease("optimizer") as lease:
        lease.prepare_operation("workspace", budget)
        old_receipt = lease.reservation_receipt("workspace")
        authority.verify_reservation_receipt(
            old_receipt, required_domain="runtime_state"
        )
        lease.record_materialized(
            "workspace",
            operational={"runtime_state": {"peak": 20_000, "final": 10_000, "files": 1}},
        )
        with pytest.raises(LifecycleIntegrityError, match="materialization is stale"):
            authority.verify_reservation_receipt(
                old_receipt, required_domain="runtime_state"
            )
        current = lease.reservation_receipt("workspace")
        assert current["authority_id"] == authority.authority_id
        assert current["operational_domains"][0]["reserved_peak_bytes"] == 80_000
        authority.verify_reservation_receipt(current, required_domain="runtime_state")
        with pytest.raises(LifecycleBoundError, match="top-up required"):
            lease.record_materialized(
                "workspace",
                operational={
                    "runtime_state": {"peak": 80_001, "final": 10_000, "files": 1}
                },
            )
        _abort(lease, "workspace")


def _advance_to_publishing(lease, operation_id: str) -> None:
    lease.transition(operation_id, OperationState.WRITING)
    lease.transition(operation_id, OperationState.STAGED)
    lease.transition(operation_id, OperationState.PUBLISHING)


def test_pointer_cas_receipt_verifies_and_drift_never_overwrites(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    pointer = roots["native"] / "ACTIVE.json"
    pointer.write_text('{"generation":1}\n', encoding="utf-8")
    authority = _authority(roots, _limits(), provision=True)
    with authority.lease("publisher") as lease:
        lease.prepare_operation("publish-ok", GrowthBudget.audit(expected_bytes=4096, file_count=1))
        _advance_to_publishing(lease, "publish-ok")
        receipt = lease.cas_publish_pointer(
            "publish-ok",
            pointer_relative_path="ACTIVE.json",
            expected_sha256=_sha(pointer),
            payload={"generation": 2},
        )
        assert lease.reservation("publish-ok").state == "PUBLISHED"
        authority.verify_pointer_receipt(receipt)
        lease.transition("publish-ok", OperationState.COMMITTED)

        lease.prepare_operation(
            "publish-drift", GrowthBudget.audit(expected_bytes=4096, file_count=1)
        )
        _advance_to_publishing(lease, "publish-drift")
        expected = _sha(pointer)

        def drift() -> None:
            pointer.write_text('{"external":"drift"}\n', encoding="utf-8")

        with pytest.raises(LifecycleCASMismatch, match="drifted"):
            lease.cas_publish_pointer(
                "publish-drift",
                pointer_relative_path="ACTIVE.json",
                expected_sha256=expected,
                payload={"generation": 3},
                _pre_replace_hook=drift,
            )
        assert json.loads(pointer.read_text(encoding="utf-8")) == {"external": "drift"}
        assert lease.reservation("publish-drift").state == "PUBLISHING"
        assert not list(pointer.parent.glob(".ACTIVE.json.*.tmp"))


def test_pointer_current_head_survives_operation_compaction_and_rejects_replay(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    pointer = roots["native"] / "ACTIVE.json"
    pointer.write_text('{"generation":1}\n', encoding="utf-8")
    authority = _authority(roots, _limits(max_operations=1), provision=True)
    with authority.lease("pointer-head") as lease:
        lease.prepare_operation(
            "publish-one", GrowthBudget.audit(expected_bytes=4096, file_count=1)
        )
        _advance_to_publishing(lease, "publish-one")
        first_receipt = lease.cas_publish_pointer(
            "publish-one",
            pointer_relative_path="ACTIVE.json",
            expected_sha256=_sha(pointer),
            payload={"generation": 2},
        )
        generation_two = pointer.read_bytes()
        lease.transition("publish-one", OperationState.COMMITTED)

        # Preparing the second operation compacts the first terminal row.
        lease.prepare_operation(
            "publish-two", GrowthBudget.audit(expected_bytes=4096, file_count=1)
        )
        _advance_to_publishing(lease, "publish-two")
        second_receipt = lease.cas_publish_pointer(
            "publish-two",
            pointer_relative_path="ACTIVE.json",
            expected_sha256=_sha(pointer),
            payload={"generation": 3},
        )
        lease.transition("publish-two", OperationState.COMMITTED)

        # Compact the second operation as well.  The fixed current-head ledger,
        # not a live operation row, remains the durable receipt authority.
        lease.prepare_operation(
            "unrelated", GrowthBudget.audit(expected_bytes=1, file_count=0)
        )
        authority.verify_pointer_receipt(second_receipt)

        # Restoring the exact bytes covered by the old receipt must not revive
        # that receipt after a newer publication has become current.
        pointer.write_bytes(generation_two)
        with pytest.raises(LifecycleIntegrityError, match="current acknowledged"):
            authority.verify_pointer_receipt(first_receipt)
        with pytest.raises(LifecycleIntegrityError, match="no longer matches"):
            authority.verify_pointer_receipt(second_receipt)


def test_pointer_head_offline_mutation_invalidates_fixed_ledger_state(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    pointer = roots["native"] / "ACTIVE.json"
    pointer.write_text('{"generation":1}\n', encoding="utf-8")
    authority = _authority(roots, _limits(), provision=True)
    with authority.lease("pointer-head-tamper") as lease:
        lease.prepare_operation(
            "publish", GrowthBudget.audit(expected_bytes=4096, file_count=1)
        )
        _advance_to_publishing(lease, "publish")
        receipt = lease.cas_publish_pointer(
            "publish",
            pointer_relative_path="ACTIVE.json",
            expected_sha256=_sha(pointer),
            payload={"generation": 2},
        )

    with sqlite3.connect(authority.db_path) as connection:
        connection.execute(
            "UPDATE pointer_heads SET receipt_json=receipt_json || ' '"
        )
    with pytest.raises(LifecycleIntegrityError, match="pointer ledger state HMAC"):
        authority.verify_pointer_receipt(receipt)


def test_pointer_replace_with_failed_acknowledgement_is_explicitly_ambiguous(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    roots = _roots(tmp_path)
    pointer = roots["native"] / "ACTIVE.json"
    pointer.write_text('{"generation":1}\n', encoding="utf-8")
    authority = _authority(roots, _limits(), provision=True)
    with authority.lease("ambiguous-publisher") as lease:
        lease.prepare_operation(
            "publish-ambiguous", GrowthBudget.audit(expected_bytes=4096, file_count=1)
        )
        _advance_to_publishing(lease, "publish-ambiguous")
        original_transaction = authority._transaction

        def fail_acknowledgement(connection, callback):
            raise LifecycleDatabaseFullError("injected acknowledgement failure")

        monkeypatch.setattr(authority, "_transaction", fail_acknowledgement)
        with pytest.raises(LifecycleRecoveryRequired, match="acknowledgement failed"):
            lease.cas_publish_pointer(
                "publish-ambiguous",
                pointer_relative_path="ACTIVE.json",
                expected_sha256=_sha(pointer),
                payload={"generation": 2},
            )
        monkeypatch.setattr(authority, "_transaction", original_transaction)

        assert json.loads(pointer.read_text(encoding="utf-8")) == {"generation": 2}
        assert lease.reservation("publish-ambiguous").state == "PUBLISHING"
        assert not list(pointer.parent.glob(".ACTIVE.json.*.tmp"))


def test_authenticated_pin_detects_bound_evidence_change(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    evidence = roots["native"] / "runs" / "run-1" / "manifest.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text('{"complete":true}\n', encoding="utf-8")
    authority = _authority(roots, _limits(), provision=True)
    with authority.lease("pin-writer") as lease:
        pin = lease.pin_evidence("winner-generation", [evidence])
        assert pin["pin_id"] == "winner-generation"
    authority.verify_pin("winner-generation")
    evidence.write_text('{"complete":false}\n', encoding="utf-8")
    with pytest.raises(LifecycleIntegrityError, match="changed"):
        authority.verify_pin("winner-generation")


def test_tampered_pin_cannot_be_retired_into_an_audit_clean_tombstone(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    evidence = roots["native"] / "runs" / "run-1" / "manifest.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text('{"complete":true}\n', encoding="utf-8")
    authority = _authority(roots, _limits(), provision=True)
    with authority.lease("pin-writer") as lease:
        lease.pin_evidence("must-remain-visible", [evidence])

    evidence.write_text('{"complete":false}\n', encoding="utf-8")
    with pytest.raises(LifecycleIntegrityError, match="pinned evidence changed"):
        authority.retire_pin("must-remain-visible")
    with sqlite3.connect(authority.db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM pins WHERE pin_id='must-remain-visible'"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM pin_tombstones "
            "WHERE pin_id='must-remain-visible'"
        ).fetchone()[0] == 0
    audit = authority.audit_lifecycle_archive()
    assert audit["pin_count"] == 0


def test_hard_row_payload_file_and_database_caps_fail_before_unbounded_growth(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    limits = _limits(max_operations=2, max_payload_bytes=64, max_files_per_operation=2)
    authority = _authority(roots, limits, provision=True)
    with authority.lease("row-caps") as lease:
        cyclic: dict[str, Any] = {}
        cyclic["self"] = cyclic
        with pytest.raises(LifecycleIntegrityError, match="reference cycle"):
            lease.prepare_operation(
                "payload-cycle",
                GrowthBudget.audit(expected_bytes=0, file_count=0),
                payload=cyclic,
            )
        with pytest.raises(LifecycleBoundError, match="payload"):
            lease.prepare_operation(
                "payload-too-large",
                GrowthBudget.audit(expected_bytes=0, file_count=0),
                payload={"value": "x" * 100},
            )
        with pytest.raises(LifecycleBoundError, match="file count"):
            lease.prepare_operation(
                "files-too-many", GrowthBudget.audit(expected_bytes=0, file_count=3)
            )
        for operation_id in ("one", "two"):
            lease.prepare_operation(
                operation_id, GrowthBudget.audit(expected_bytes=0, file_count=0)
            )
            _abort(lease, operation_id)
        lease.prepare_operation(
            "three", GrowthBudget.audit(expected_bytes=0, file_count=0)
        )
        _abort(lease, "three")
        with pytest.raises(LifecycleStateError, match="retired operation_id"):
            lease.prepare_operation(
                "one", GrowthBudget.audit(expected_bytes=0, file_count=0)
            )
        assert len(authority.list_operations()) <= limits.max_operations
    assert authority.db_path.stat().st_size <= limits.max_db_bytes
    assert len(authority.list_operations()) <= limits.max_operations
    with sqlite3.connect(authority.db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM operation_tombstones"
        ).fetchone()[0] >= 2


def test_archive_head_recomputes_every_tombstone_record_authentication(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path)
    authority = _authority(
        roots,
        _limits(max_operations=1),
        provision=True,
    )
    with authority.lease("archive-integrity") as lease:
        lease.prepare_operation(
            "retired-one", GrowthBudget.audit(expected_bytes=0, file_count=0)
        )
        _abort(lease, "retired-one")
        lease.prepare_operation(
            "current-two", GrowthBudget.audit(expected_bytes=0, file_count=0)
        )
        _abort(lease, "current-two")

    # Preserve the archive head and its covered tombstone_sha256 while
    # corrupting a field from which that digest must be recomputed. A summary
    # that merely trusted the stored digest would miss this mutation.
    with sqlite3.connect(authority.db_path) as connection:
        connection.execute(
            "UPDATE operation_tombstones SET source_record_sha256=? "
            "WHERE operation_id='retired-one'",
            ("0" * 64,),
        )
        connection.commit()
    with pytest.raises(LifecycleIntegrityError, match="tombstone digest mismatch"):
        authority.audit_lifecycle_archive()


def test_actual_sqlite_full_rolls_back_the_failed_operation(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    limits = _limits(
        max_db_bytes=256 * 1024,
        journal_size_limit=16 * 1024,
        max_operations=1_000,
        max_active_operations=1_000,
        max_payload_bytes=4096,
    )
    authority = _authority(roots, limits, provision=True)
    failed_id = None
    with authority.lease("db-fill") as lease:
        for index in range(250):
            operation_id = f"fill-{index:04d}"
            try:
                lease.prepare_operation(
                    operation_id,
                    GrowthBudget.audit(expected_bytes=0, file_count=0),
                    payload={"opaque": f"{index:04d}-" + ("x" * 3000)},
                )
            except LifecycleDatabaseFullError:
                failed_id = operation_id
                break
        assert failed_id is not None, "test did not reach SQLite's fixed max_page_count"
        with pytest.raises(LifecycleStateError, match="unknown"):
            lease.reservation(failed_id)
    assert authority.db_path.stat().st_size <= limits.max_db_bytes


def test_reservation_only_work_never_mutates_native_or_cold_data(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    native_file = roots["native"] / "runs" / "existing" / "bars.csv"
    cold_file = roots["cold"] / "objects" / "existing.tar.gz"
    native_file.parent.mkdir(parents=True)
    cold_file.parent.mkdir(parents=True)
    native_file.write_bytes(b"time,close\n1,2\n")
    cold_file.write_bytes(b"immutable-cold")
    before_native, before_cold = _snapshot(roots["native"]), _snapshot(roots["cold"])

    authority = _authority(roots, _limits(), provision=True)
    with authority.lease("read-only-preflight") as lease:
        lease.prepare_operation(
            "reserve-only", GrowthBudget.audit(expected_bytes=1024, file_count=1)
        )
        _abort(lease, "reserve-only")

    assert _snapshot(roots["native"]) == before_native
    assert _snapshot(roots["cold"]) == before_cold
    assert "MetaTrader5" not in sys.modules


def test_two_processes_serialize_on_the_same_root_key(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    limits = _limits(lock_deadline_seconds=5.0)
    _authority(roots, limits, provision=True)
    limit_json = json.dumps(asdict(limits), sort_keys=True)
    holder_script = r"""
import json,sys,time
from pathlib import Path
from pm_native_lifecycle import LifecycleLimits,NativeLifecycleAuthority
a=NativeLifecycleAuthority(native_root=Path(sys.argv[1]),cold_root=Path(sys.argv[2]),state_root=Path(sys.argv[3]),limits=LifecycleLimits(**json.loads(sys.argv[4])),auth_key=bytes.fromhex(sys.argv[5]),broker_highwater_key=bytes.fromhex(sys.argv[6]),provisioning_identity=sys.argv[7])
with a.lease('holder'):
 print('LOCKED',flush=True)
 time.sleep(0.8)
"""
    contender_script = r"""
import json,sys,time
from pathlib import Path
from pm_native_lifecycle import LifecycleLimits,NativeLifecycleAuthority
started=time.monotonic()
a=NativeLifecycleAuthority(native_root=Path(sys.argv[1]),cold_root=Path(sys.argv[2]),state_root=Path(sys.argv[3]),limits=LifecycleLimits(**json.loads(sys.argv[4])),auth_key=bytes.fromhex(sys.argv[5]),broker_highwater_key=bytes.fromhex(sys.argv[6]),provisioning_identity=sys.argv[7])
with a.lease('contender'):
 print(time.monotonic()-started,flush=True)
"""
    args = [
        str(roots["native"]),
        str(roots["cold"]),
        str(roots["state"]),
        limit_json,
        AUTH_KEY.hex(),
        BROKER_HIGHWATER_KEY.hex(),
        PROVISIONING_IDENTITY,
    ]
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_script, *args],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "LOCKED"
        started = time.monotonic()
        contender = subprocess.run(
            [sys.executable, "-c", contender_script, *args],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        elapsed = time.monotonic() - started
        assert float(contender.stdout.strip()) >= 0.05
        assert elapsed >= 0.55
    finally:
        stdout, stderr = holder.communicate(timeout=10)
        assert holder.returncode == 0, stdout + stderr


def test_two_process_reservations_share_one_domain_cap(tmp_path: Path) -> None:
    roots = _roots(tmp_path, runtime=True)
    first_root = roots["runtime"] / "EURUSD" / "M5"
    second_root = roots["runtime"] / "USDJPY" / "M5"
    first_root.mkdir(parents=True)
    second_root.mkdir(parents=True)
    limits = _limits(lock_deadline_seconds=5.0)
    domain = OperationalDomain(
        roots["runtime"], max_bytes=150_000, max_files=20, max_active_reservations=6
    )
    _authority(roots, limits, provision=True, operational_domain=domain)
    script = r"""
import hashlib,json,sys,time
from pathlib import Path
from pm_native_lifecycle import LifecycleBoundError,LifecycleLimits,NativeLifecycleAuthority,OperationalDomain
domain=OperationalDomain(Path(sys.argv[4]),max_bytes=150000,max_files=20,max_active_reservations=6)
a=NativeLifecycleAuthority(native_root=Path(sys.argv[1]),cold_root=Path(sys.argv[2]),state_root=Path(sys.argv[3]),limits=LifecycleLimits(**json.loads(sys.argv[5])),auth_key=bytes.fromhex(sys.argv[6]),broker_highwater_key=bytes.fromhex(sys.argv[7]),operational_domains={'runtime_state':domain},provisioning_identity=sys.argv[8])
try:
 a.reserve_runtime_storage(operational_root=Path(sys.argv[9]),stream_identity_sha256=hashlib.sha256(sys.argv[10].encode()).hexdigest(),requested_peak_bytes=100000)
 print('RESERVED',flush=True)
 if sys.argv[11]=='hold':
  with a.lease('post-reservation-holder'):
   time.sleep(0.7)
except LifecycleBoundError:
 print('BLOCKED',flush=True)
"""
    common = [
        str(roots["native"]),
        str(roots["cold"]),
        str(roots["state"]),
        str(roots["runtime"]),
        json.dumps(asdict(limits), sort_keys=True),
        AUTH_KEY.hex(),
        BROKER_HIGHWATER_KEY.hex(),
        PROVISIONING_IDENTITY,
    ]
    first = subprocess.Popen(
        [sys.executable, "-c", script, *common, str(first_root), "first", "hold"],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert first.stdout is not None
        assert first.stdout.readline().strip() == "RESERVED"
        second = subprocess.run(
            [sys.executable, "-c", script, *common, str(second_root), "second", "nohold"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        assert second.stdout.strip() == "BLOCKED"
    finally:
        stdout, stderr = first.communicate(timeout=10)
        assert first.returncode == 0, stdout + stderr


def test_two_process_streams_cannot_claim_the_same_account_execution(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    first_root = roots["runtime"] / "EURUSD" / "M5"
    second_root = roots["runtime"] / "USDJPY" / "M5"
    first_root.mkdir(parents=True)
    second_root.mkdir(parents=True)
    limits = _limits(lock_deadline_seconds=5.0)
    domain = OperationalDomain(
        roots["runtime"], max_bytes=400_000, max_files=20, max_active_reservations=6
    )
    authority = _authority(roots, limits, provision=True, operational_domain=domain)
    first_stream = hashlib.sha256(b"subprocess-execution-first").hexdigest()
    second_stream = hashlib.sha256(b"subprocess-execution-second").hexdigest()
    first_reservation = authority.reserve_runtime_storage(
        operational_root=first_root,
        stream_identity_sha256=first_stream,
        requested_peak_bytes=100_000,
    )
    second_reservation = authority.reserve_runtime_storage(
        operational_root=second_root,
        stream_identity_sha256=second_stream,
        requested_peak_bytes=100_000,
    )
    _witness_order_highwater(authority, first_reservation, 1)
    _witness_order_highwater(authority, second_reservation, 1)
    account = hashlib.sha256(b"subprocess-account").hexdigest()
    script = r"""
import hashlib,json,sys
from pathlib import Path
from pm_native_lifecycle import BrokerAccountHighWaterReceipt,LifecycleCASMismatch,LifecycleLimits,NativeLifecycleAuthority,OperationalDomain
domain=OperationalDomain(Path(sys.argv[4]),max_bytes=400000,max_files=20,max_active_reservations=6)
a=NativeLifecycleAuthority(native_root=Path(sys.argv[1]),cold_root=Path(sys.argv[2]),state_root=Path(sys.argv[3]),limits=LifecycleLimits(**json.loads(sys.argv[5])),auth_key=bytes.fromhex(sys.argv[6]),broker_highwater_key=bytes.fromhex(sys.argv[7]),operational_domains={'runtime_state':domain},provisioning_identity=sys.argv[8])
r=a.reserve_runtime_storage(operational_root=Path(sys.argv[9]),stream_identity_sha256=sys.argv[10],requested_peak_bytes=100000)
receipt=BrokerAccountHighWaterReceipt.issue(bytes.fromhex(sys.argv[7]),account_scope_sha256=sys.argv[11],observed_sequence_highwater=777,source_snapshot_sha256=hashlib.sha256(b'broker-history-777').hexdigest(),observed_at='2026-01-01T00:00:00Z',observation_id='subprocess-777')
operation_id=f'{sys.argv[10][:32]}:{1:020d}:{1:020d}'
try:
 a.claim_broker_execution(r,account_scope_sha256=sys.argv[11],broker_execution_id='777',broker_execution_sequence=777,operation_id=operation_id,operation_sequence=1,payload_sha256=sys.argv[12],broker_highwater_receipt=receipt)
 print('CLAIMED',flush=True)
except LifecycleCASMismatch:
 print('CONFLICT',flush=True)
"""
    common = [
        str(roots["native"]),
        str(roots["cold"]),
        str(roots["state"]),
        str(roots["runtime"]),
        json.dumps(asdict(limits), sort_keys=True),
        AUTH_KEY.hex(),
        BROKER_HIGHWATER_KEY.hex(),
        PROVISIONING_IDENTITY,
    ]
    process_args = (
        [
            *common,
            str(first_root),
            first_stream,
            account,
            hashlib.sha256(b"eurusd-subprocess-fill").hexdigest(),
        ],
        [
            *common,
            str(second_root),
            second_stream,
            account,
            hashlib.sha256(b"usdjpy-subprocess-fill").hexdigest(),
        ],
    )
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script, *args],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for args in process_args
    ]
    results: list[str] = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=15)
        assert process.returncode == 0, stdout + stderr
        results.append(stdout.strip())
    assert sorted(results) == ["CLAIMED", "CONFLICT"]
    connection = authority._connect()
    try:
        assert connection.execute(
            "SELECT COUNT(*) FROM broker_execution_claims"
        ).fetchone()[0] == 1
    finally:
        connection.close()


def test_pin_and_runtime_witness_tombstones_allow_bounded_reuse_cycles(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    first_runtime = roots["runtime"] / "EURUSD" / "M5"
    second_runtime = roots["runtime"] / "USDJPY" / "M5"
    first_runtime.mkdir(parents=True)
    second_runtime.mkdir(parents=True)
    evidence = roots["native"] / "evidence.json"
    evidence.write_text('{"sealed":true}\n', encoding="utf-8")
    limits = _limits(max_operations=1, max_pins=1)
    authority = _authority(
        roots,
        limits,
        provision=True,
        operational_domain=OperationalDomain(
            roots["runtime"],
            max_bytes=300_000,
            max_files=10,
            max_active_reservations=2,
        ),
    )
    with authority.lease("pin-one") as lease:
        lease.pin_evidence("pin-one", [evidence])
    authority.retire_pin("pin-one")
    with authority.lease("pin-two") as lease:
        lease.pin_evidence("pin-two", [evidence])
        with pytest.raises(LifecycleStateError, match="retired pin_id"):
            lease.pin_evidence("pin-one", [evidence])

    first = authority.reserve_runtime_storage(
        operational_root=first_runtime,
        stream_identity_sha256=hashlib.sha256(b"retired-stream-one").hexdigest(),
        requested_peak_bytes=100_000,
    )
    authority.release_runtime_storage(first)
    second = authority.reserve_runtime_storage(
        operational_root=second_runtime,
        stream_identity_sha256=hashlib.sha256(b"active-stream-two").hexdigest(),
        requested_peak_bytes=100_000,
    )
    assert second.stream_identity_sha256 != first.stream_identity_sha256
    with pytest.raises(LifecycleIntegrityError, match="retired runtime"):
        authority.reserve_runtime_storage(
            operational_root=first_runtime,
            stream_identity_sha256=first.stream_identity_sha256,
            requested_peak_bytes=100_000,
        )
    with sqlite3.connect(authority.db_path) as connection:
        head = connection.execute(
            "SELECT pin_count,runtime_count FROM lifecycle_archive_head"
        ).fetchone()
        assert tuple(head) == (1, 1)


def test_claim_deletion_and_account_deletion_fail_before_reclaim_write(
    tmp_path: Path,
) -> None:
    roots = _roots(tmp_path, runtime=True)
    stream_root = roots["runtime"] / "EURUSD" / "M5"
    stream_root.mkdir(parents=True)
    authority = _authority(
        roots,
        _limits(
            max_broker_execution_claims=4,
            max_broker_reorder_sequences=1,
            max_operation_reorder_sequences=1,
        ),
        provision=True,
        operational_domain=OperationalDomain(
            roots["runtime"],
            max_bytes=300_000,
            max_files=10,
            max_active_reservations=2,
        ),
    )
    reservation = authority.reserve_runtime_storage(
        operational_root=stream_root,
        stream_identity_sha256=hashlib.sha256(b"deletion-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    _witness_order_highwater(authority, reservation, 3)
    account = hashlib.sha256(b"deletion-account").hexdigest()
    for sequence in (1, 2):
        authority.claim_broker_execution(
            reservation,
            account_scope_sha256=account,
            broker_execution_id=str(sequence),
            broker_execution_sequence=sequence,
            operation_id=_operation_identity(reservation, sequence),
            operation_sequence=sequence,
            payload_sha256=hashlib.sha256(f"fill-{sequence}".encode()).hexdigest(),
            broker_highwater_receipt=_broker_highwater(account, sequence),
        )
    with sqlite3.connect(authority.db_path) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute(
            "DELETE FROM broker_execution_claims WHERE account_scope_sha256=? "
            "AND claim_sequence=1",
            (account,),
        )
        connection.commit()
    with pytest.raises(
        (LifecycleIntegrityError, LifecycleRecoveryRequired),
        match="deletion|count|sequence|ledger",
    ):
        authority.claim_broker_execution(
            reservation,
            account_scope_sha256=account,
            broker_execution_id="3",
            broker_execution_sequence=3,
            operation_id=_operation_identity(reservation, 3),
            operation_sequence=3,
            payload_sha256=hashlib.sha256(b"fill-3").hexdigest(),
            broker_highwater_receipt=_broker_highwater(account, 3),
        )
    with sqlite3.connect(authority.db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM broker_execution_claims WHERE broker_execution_id='3'"
        ).fetchone()[0] == 0

    # Restore the missing row from a fresh authority is intentionally
    # impossible without external evidence. A separate fixture attacks head
    # deletion, which must fail at the same pre-write gate.
    roots_two = _roots(tmp_path / "head", runtime=True)
    root_two = roots_two["runtime"] / "EURUSD" / "M5"
    root_two.mkdir(parents=True)
    authority_two = _authority(
        roots_two,
        _limits(),
        provision=True,
        operational_domain=OperationalDomain(
            roots_two["runtime"], 300_000, 10, 2
        ),
    )
    reservation_two = authority_two.reserve_runtime_storage(
        operational_root=root_two,
        stream_identity_sha256=hashlib.sha256(b"head-stream").hexdigest(),
        requested_peak_bytes=100_000,
    )
    _witness_order_highwater(authority_two, reservation_two, 2)
    account_two = hashlib.sha256(b"head-account").hexdigest()
    authority_two.claim_broker_execution(
        reservation_two,
        account_scope_sha256=account_two,
        broker_execution_id="11",
        broker_execution_sequence=11,
        operation_id=_operation_identity(reservation_two, 1),
        operation_sequence=1,
        payload_sha256=hashlib.sha256(b"head-fill").hexdigest(),
        broker_highwater_receipt=_broker_highwater(account_two, 11),
    )
    with sqlite3.connect(authority_two.db_path) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute(
            "DELETE FROM broker_account_heads WHERE account_scope_sha256=?",
            (account_two,),
        )
        connection.commit()
    with pytest.raises(
        LifecycleIntegrityError,
        match="foreign-key|account head|ledger state HMAC",
    ):
        authority_two.claim_broker_execution(
            reservation_two,
            account_scope_sha256=account_two,
            broker_execution_id="12",
            broker_execution_sequence=12,
            operation_id=_operation_identity(reservation_two, 2),
            operation_sequence=2,
            payload_sha256=hashlib.sha256(b"head-fill-2").hexdigest(),
            broker_highwater_receipt=_broker_highwater(account_two, 12),
        )
