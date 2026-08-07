import json
import shutil
import io
import hashlib
import multiprocessing
import os
import tarfile
from pathlib import Path

import pytest
import pm_native_storage as storage

from pm_native_storage import (
    PIN_SCHEMA,
    NativeStorageBoundError,
    NativeStorageIntegrityError,
    NativeStoragePolicy,
    NativeStorageProtectedError,
    admit_native_run,
    build_protection_graph,
    create_lossless_archive,
    preflight_growth,
    reconcile_native_storage,
    retire_complete_native_runs,
    retire_native_run,
    restore_lossless_archive,
    sha256_file,
    verify_lossless_archive,
)


def _retire_process(native: str, cold: str, queue) -> None:
    try:
        result = retire_native_run(
            Path(native), Path(cold), "race-run", policy=_archive_policy()
        )
        queue.put(("ok", result is not None))
    except Exception as exc:  # pragma: no cover - failure is asserted in parent
        queue.put(("error", f"{type(exc).__name__}: {exc}"))


def _write_json(path: Path, value) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    return path


def _write_file(path: Path, value: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return path


def _fixture(tmp_path: Path):
    native = tmp_path / "data" / "native"
    bars = _write_file(native / "runs" / "active-run" / "EURUSD" / "M5" / "bars.csv", b"time,close\n1,2\n")
    provenance = _write_json(
        native / "runs" / "active-run" / "EURUSD" / "M5" / "provenance.json",
        {"run_id": "active-run", "complete": True},
    )
    run_manifest = _write_json(
        native / "runs" / "active-run" / "run_manifest.json",
        {
            "run_id": "active-run",
            "complete": True,
            "results": [
                {
                    "bars_relative_path": "runs/active-run/EURUSD/M5/bars.csv",
                    "bars_sha256": sha256_file(bars),
                    "provenance_relative_path": "runs/active-run/EURUSD/M5/provenance.json",
                    "provenance_sha256": sha256_file(provenance),
                }
            ],
        },
    )
    sidecar = _write_json(native / "audits" / "active-a1" / "EURUSD" / "M5" / "bars.meta.json", {"ok": True})
    parity = _write_json(native / "audits" / "active-a1" / "EURUSD" / "M5" / "parity.json", {"ok": True})
    activation = _write_json(
        native / "activations" / "active-a1.json",
        {
            "activation_id": "active-a1",
            "run_id": "active-run",
            "run_manifest_relative_path": "runs/active-run/run_manifest.json",
            "run_manifest_sha256": sha256_file(run_manifest),
            "entries": {
                "EURUSD": {
                    "M5": {
                        "bars_relative_path": "runs/active-run/EURUSD/M5/bars.csv",
                        "bars_sha256": sha256_file(bars),
                        "sidecar_relative_path": "audits/active-a1/EURUSD/M5/bars.meta.json",
                        "sidecar_sha256": sha256_file(sidecar),
                        "parity_relative_path": "audits/active-a1/EURUSD/M5/parity.json",
                        "parity_sha256": sha256_file(parity),
                    }
                }
            },
        },
    )
    _write_json(
        native / "ACTIVE.json",
        {
            "activation_id": "active-a1",
            "manifest_relative_path": "activations/active-a1.json",
            "manifest_sha256": sha256_file(activation),
        },
    )

    snapshot = _write_json(native / "terms" / "snapshots" / "snapshot.json", {"complete": True})
    schedule = _write_json(native / "terms" / "schedules" / "EURUSD.json", {"events": []})
    terms_manifest = _write_json(
        native / "terms" / "activations" / "terms-a1.json",
        {
            "activation_id": "terms-a1",
            "price_activation_id": "active-a1",
            "price_activation_manifest_sha256": sha256_file(activation),
            "terms_snapshot_relative_path": "terms/snapshots/snapshot.json",
            "terms_snapshot_sha256": sha256_file(snapshot),
            "symbols": {
                "EURUSD": {
                    "schedule_relative_path": "terms/schedules/EURUSD.json",
                    "schedule_sha256": sha256_file(schedule),
                }
            },
        },
    )
    _write_json(
        native / "terms" / "ACTIVE.json",
        {
            "activation_id": "terms-a1",
            "manifest_relative_path": "terms/activations/terms-a1.json",
            "manifest_sha256": sha256_file(terms_manifest),
            "price_activation_manifest_sha256": sha256_file(activation),
        },
    )

    old_bars = _write_file(native / "runs" / "old-run" / "EURUSD" / "D1" / "bars.csv", b"time,close\n2,3\n")
    _write_json(
        native / "runs" / "old-run" / "run_manifest.json",
        {"run_id": "old-run", "complete": True},
    )

    unfinished = _write_file(native / "runs" / "writing-run" / "EURUSD" / "M5" / "bars.csv", b"partial")
    _write_json(
        native / "runs" / ".writing-run.writer.owner.json",
        {"run_id": "writing-run", "active": True},
    )
    _write_file(native / "runs" / ".writing-run.writer.lock", b"1")
    orphan = _write_json(native / "audits" / "audit-in-progress" / "EURUSD" / "M5" / "parity.json", {"partial": True})

    pinned = _write_json(native / "activations" / "pinned-evidence.json", {"evidence": "keep"})
    _write_json(
        native / "PINS" / "winner-generation.json",
        {
            "schema_version": PIN_SCHEMA,
            "pin_id": "winner-generation",
            "roots": [
                {
                    "relative_path": "activations/pinned-evidence.json",
                    "sha256": sha256_file(pinned),
                }
            ],
        },
    )
    return native, old_bars, unfinished, orphan, snapshot


def _archive_policy() -> NativeStoragePolicy:
    return NativeStoragePolicy(
        max_hot_bytes=64 * 1024 * 1024,
        max_cold_bytes=64 * 1024 * 1024,
        min_filesystem_free_bytes=1,
        max_complete_hot_runs=10,
        max_inflight_runs=2,
    )


def _minimal_run(native: Path, run_id: str, payload: bytes = b"exact-native-bytes") -> Path:
    run = native / "runs" / run_id
    bars = _write_file(run / "EURUSD" / "M5" / "bars.csv", payload)
    _write_json(
        run / "run_manifest.json",
        {
            "run_id": run_id,
            "complete": True,
            "bars_relative_path": f"runs/{run_id}/EURUSD/M5/bars.csv",
            "bars_sha256": sha256_file(bars),
        },
    )
    return run


def test_protection_graph_covers_active_terms_pins_and_inflight(tmp_path):
    native, old_bars, unfinished, orphan, snapshot = _fixture(tmp_path)
    graph = build_protection_graph(native)

    assert graph.is_protected(native / "ACTIVE.json")
    assert graph.is_protected(native / "runs" / "active-run")
    assert graph.is_protected(snapshot)
    assert graph.is_protected(native / "activations" / "pinned-evidence.json")
    assert graph.is_protected(unfinished)
    assert graph.is_protected(orphan)
    assert not graph.is_protected(old_bars)
    assert graph.protected_bytes > 0


def test_protection_graph_rejects_tampered_bound_evidence(tmp_path):
    native, _old_bars, _unfinished, _orphan, snapshot = _fixture(tmp_path)
    snapshot.write_text('{"tampered": true}', encoding="utf-8")
    with pytest.raises(NativeStorageIntegrityError, match="SHA-256 binding mismatch"):
        build_protection_graph(native)


def test_protection_graph_supports_explicit_external_native_root(tmp_path):
    external = tmp_path / "external-market-evidence"
    external.mkdir()
    graph = build_protection_graph(external)
    assert graph.native_root == external.resolve()
    assert not graph.protected


def test_preflight_fails_before_growth_on_any_hard_bound(tmp_path):
    native, _old_bars, _unfinished, _orphan, _snapshot = _fixture(tmp_path)
    cold = tmp_path / "cold"
    cold.mkdir()
    hot = sum(path.stat().st_size for path in native.rglob("*") if path.is_file())
    policy = NativeStoragePolicy(
        max_hot_bytes=hot + 99,
        max_cold_bytes=1024,
        min_filesystem_free_bytes=500,
        max_complete_hot_runs=2,
        max_inflight_runs=1,
    )
    decision = preflight_growth(
        native,
        cold,
        expected_growth_bytes=100,
        policy=policy,
        filesystem_free_bytes=599,
    )
    assert not decision.allowed
    assert any("projected hot bytes" in reason for reason in decision.reasons)
    assert any("projected free bytes" in reason for reason in decision.reasons)
    assert any("complete hot runs" in reason for reason in decision.reasons)
    assert any("inflight runs" in reason for reason in decision.reasons)
    with pytest.raises(NativeStorageBoundError):
        decision.require_allowed()


def test_lossless_archive_is_deterministic_verified_and_restorable(tmp_path):
    native, old_bars, _unfinished, _orphan, _snapshot = _fixture(tmp_path)
    graph = build_protection_graph(native)
    cold_a, cold_b = tmp_path / "cold-a", tmp_path / "cold-b"

    first = create_lossless_archive(
        native,
        cold_a,
        [native / "runs" / "old-run"],
        graph=graph,
        policy=_archive_policy(),
    )
    second = create_lossless_archive(
        native,
        cold_b,
        [native / "runs" / "old-run"],
        graph=graph,
        policy=_archive_policy(),
    )
    assert first["object_id"] == second["object_id"]
    assert first["archive_sha256"] == second["archive_sha256"]
    archive = cold_a / first["archive_relative_path"]
    manifest = verify_lossless_archive(archive, expected_sha256=first["archive_sha256"])
    assert manifest["file_count"] == 2

    expected = old_bars.read_bytes()
    shutil.rmtree(native / "runs" / "old-run")
    restored = restore_lossless_archive(
        native, cold_a, first["object_id"], policy=_archive_policy()
    )
    assert restored["restored_files"] == 2
    assert old_bars.read_bytes() == expected
    assert restore_lossless_archive(
        native, cold_a, first["object_id"], policy=_archive_policy()
    )["restored_files"] == 0


def test_archive_refuses_active_evidence_and_restore_refuses_collision(tmp_path):
    native, _old_bars, _unfinished, _orphan, _snapshot = _fixture(tmp_path)
    graph = build_protection_graph(native)
    cold = tmp_path / "cold"
    with pytest.raises(NativeStorageProtectedError):
        create_lossless_archive(
            native,
            cold,
            [native / "runs" / "active-run"],
            graph=graph,
            policy=_archive_policy(),
        )

    index = create_lossless_archive(
        native,
        cold,
        [native / "runs" / "old-run"],
        graph=graph,
        policy=_archive_policy(),
    )
    shutil.rmtree(native / "runs" / "old-run")
    collision = native / "runs" / "old-run" / "EURUSD" / "D1" / "bars.csv"
    _write_file(collision, b"different")
    with pytest.raises(NativeStorageIntegrityError, match="overwrite non-identical"):
        restore_lossless_archive(native, cold, index["object_id"], policy=_archive_policy())


def test_archive_bit_flip_is_detected(tmp_path):
    native, _old_bars, _unfinished, _orphan, _snapshot = _fixture(tmp_path)
    cold = tmp_path / "cold"
    index = create_lossless_archive(
        native,
        cold,
        [native / "runs" / "old-run"],
        graph=build_protection_graph(native),
        policy=_archive_policy(),
    )
    archive = cold / index["archive_relative_path"]
    content = bytearray(archive.read_bytes())
    content[len(content) // 2] ^= 1
    archive.write_bytes(content)
    with pytest.raises(NativeStorageIntegrityError):
        verify_lossless_archive(archive, expected_sha256=index["archive_sha256"])


def test_archive_and_restore_enforce_their_own_byte_bounds(tmp_path):
    native, _old_bars, _unfinished, _orphan, _snapshot = _fixture(tmp_path)
    cold = tmp_path / "bounded-cold"
    tiny_cold_policy = NativeStoragePolicy(
        max_hot_bytes=64 * 1024 * 1024,
        max_cold_bytes=1,
        min_filesystem_free_bytes=1,
        max_complete_hot_runs=10,
        max_inflight_runs=2,
    )
    with pytest.raises(NativeStorageBoundError, match="cold-store cap"):
        create_lossless_archive(
            native,
            cold,
            [native / "runs" / "old-run"],
            graph=build_protection_graph(native),
            policy=tiny_cold_policy,
        )
    assert not cold.exists()

    index = create_lossless_archive(
        native,
        cold,
        [native / "runs" / "old-run"],
        graph=build_protection_graph(native),
        policy=_archive_policy(),
    )
    shutil.rmtree(native / "runs" / "old-run")
    tiny_hot_policy = NativeStoragePolicy(
        max_hot_bytes=1,
        max_cold_bytes=64 * 1024 * 1024,
        min_filesystem_free_bytes=1,
        max_complete_hot_runs=10,
        max_inflight_runs=2,
    )
    with pytest.raises(NativeStorageBoundError, match="hot-store byte cap"):
        restore_lossless_archive(
            native, cold, index["object_id"], policy=tiny_hot_policy
        )


@pytest.mark.parametrize(
    "member_name,member_type",
    [
        ("payload/runs/old/symlink", tarfile.SYMTYPE),
        ("payload/runs/old/hardlink", tarfile.LNKTYPE),
        ("payload/runs/old/device", tarfile.CHRTYPE),
        ("payload/../../escape", tarfile.REGTYPE),
        ("payload/C:\\escape", tarfile.REGTYPE),
    ],
)
def test_archive_verifier_rejects_links_devices_and_traversal(
    tmp_path, member_name, member_type
):
    archive = tmp_path / "malicious.tar.gz"
    relative = member_name[len("payload/") :]
    payload = b"x"
    manifest = {
        "schema_version": "fxpm-native-lossless-archive-v1",
        "files": [
            {
                "relative_path": relative,
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        ],
        "file_count": 1,
        "uncompressed_bytes": len(payload),
    }
    manifest_bytes = (json.dumps(manifest) + "\n").encode()
    with tarfile.open(archive, "w:gz") as tar:
        first = tarfile.TarInfo("ARCHIVE_MANIFEST.json")
        first.size = len(manifest_bytes)
        tar.addfile(first, io.BytesIO(manifest_bytes))
        member = tarfile.TarInfo(member_name)
        member.type = member_type
        member.size = len(payload) if member_type == tarfile.REGTYPE else 0
        member.linkname = "../../escape" if member_type in (tarfile.SYMTYPE, tarfile.LNKTYPE) else ""
        tar.addfile(member, io.BytesIO(payload) if member_type == tarfile.REGTYPE else None)
    with pytest.raises(NativeStorageIntegrityError):
        verify_lossless_archive(archive)


def test_scandir_consumes_only_cap_plus_one_with_poison_iterator(tmp_path, monkeypatch):
    root = tmp_path / "bounded"
    root.mkdir()
    for index in range(3):
        _write_file(root / f"{index}.txt", b"x")

    real_scandir = storage.os.scandir
    observations = {"next": 0, "closed": False}

    class PoisonScanner:
        def __init__(self, path):
            self._scanner = real_scandir(path)

        def __enter__(self):
            self._scanner.__enter__()
            return self

        def __exit__(self, *args):
            observations["closed"] = True
            return self._scanner.__exit__(*args)

        def __iter__(self):
            return self

        def __next__(self):
            observations["next"] += 1
            if observations["next"] > 3:
                raise AssertionError("iterator consumed beyond cap+1")
            return next(self._scanner)

    monkeypatch.setattr(storage.os, "scandir", PoisonScanner)
    with pytest.raises(NativeStorageBoundError, match="directory entry cap"):
        storage.tree_bytes(root, max_directory_entries=2)
    assert observations == {"next": 3, "closed": True}


def test_duplicate_key_json_is_rejected_before_lifecycle_decision(tmp_path):
    native = tmp_path / "native"
    run = native / "runs" / "duplicate-run"
    run.mkdir(parents=True)
    (run / "run_manifest.json").write_bytes(
        b'{"run_id":"duplicate-run","complete":true,"complete":false}'
    )
    cold = tmp_path / "cold"
    with pytest.raises(NativeStorageIntegrityError, match="duplicate keys"):
        preflight_growth(
            native,
            cold,
            expected_growth_bytes=1,
            policy=_archive_policy(),
            filesystem_free_bytes=10_000,
        )


def test_retirement_is_journalled_verified_and_exactly_restorable(tmp_path):
    native, cold = tmp_path / "native", tmp_path / "cold"
    run = _minimal_run(native, "retire-run", b"\x00\xffexact\r\nbytes")
    expected = {
        path.relative_to(run).as_posix(): path.read_bytes()
        for path in run.rglob("*")
        if path.is_file()
    }

    result = retire_native_run(
        native, cold, "retire-run", policy=_archive_policy()
    )
    assert result is not None
    assert not run.exists()
    transaction_id = result["transaction_id"]
    assert (cold / "transactions" / "intents" / f"{transaction_id}.json").is_file()
    assert (cold / "transactions" / "completed" / f"{transaction_id}.json").is_file()

    restored = restore_lossless_archive(
        native, cold, result["object_id"], policy=_archive_policy()
    )
    assert restored["restored_files"] == len(expected)
    assert {
        path.relative_to(run).as_posix(): path.read_bytes()
        for path in run.rglob("*")
        if path.is_file()
    } == expected
    assert reconcile_native_storage(native, cold, policy=_archive_policy())


def test_explicit_retirement_refuses_active_pinned_and_inflight_runs(tmp_path):
    native, _old, _unfinished, _orphan, _snapshot = _fixture(tmp_path)
    cold = tmp_path / "cold"
    with pytest.raises(NativeStorageProtectedError, match="active, pinned or inflight"):
        retire_native_run(native, cold, "active-run", policy=_archive_policy())
    with pytest.raises(NativeStorageProtectedError, match="active, pinned or inflight"):
        retire_native_run(native, cold, "writing-run", policy=_archive_policy())

    pinned = _minimal_run(native, "pinned-run")
    manifest = pinned / "run_manifest.json"
    _write_json(
        native / "PINS" / "pinned-run.json",
        {
            "schema_version": PIN_SCHEMA,
            "pin_id": "pinned-run",
            "roots": [
                {
                    "relative_path": "runs/pinned-run/run_manifest.json",
                    "sha256": sha256_file(manifest),
                }
            ],
        },
    )
    with pytest.raises(NativeStorageProtectedError, match="active, pinned or inflight"):
        retire_native_run(native, cold, "pinned-run", policy=_archive_policy())


def test_retirement_refuses_hardlinked_and_symlinked_candidate_bytes(tmp_path):
    native, cold = tmp_path / "native", tmp_path / "cold"
    hard_run = _minimal_run(native, "hard-run")
    bars = hard_run / "EURUSD" / "M5" / "bars.csv"
    try:
        os.link(bars, tmp_path / "outside-hardlink")
    except OSError:
        pytest.skip("filesystem does not support hard links")
    with pytest.raises(NativeStorageIntegrityError, match="hard-linked"):
        retire_native_run(native, cold, "hard-run", policy=_archive_policy())

    bars.unlink()
    _write_file(bars, b"replacement")
    symlink_run = _minimal_run(native, "symlink-run")
    linked = symlink_run / "EURUSD" / "M5" / "linked.csv"
    try:
        linked.symlink_to(tmp_path / "outside-hardlink")
    except OSError:
        pytest.skip("filesystem does not permit symlink creation")
    with pytest.raises(NativeStorageIntegrityError, match="links/reparse"):
        retire_native_run(native, cold, "symlink-run", policy=_archive_policy())


@pytest.mark.parametrize(
    "crash_point",
    [
        "archive_verified",
        "intent_published",
        "hot_renamed",
        "before_stage_cleanup",
        "stage_file_unlinked",
        "stage_cleanup_complete",
        "completion_published",
    ],
)
def test_restart_reconciliation_never_loses_both_copies(tmp_path, crash_point):
    native, cold = tmp_path / "native", tmp_path / "cold"
    run = _minimal_run(native, "crash-run", b"crash-safe-exact")

    def inject(point):
        if point == crash_point:
            raise RuntimeError(f"crash at {point}")

    with pytest.raises(RuntimeError, match="crash at"):
        retire_native_run(
            native,
            cold,
            "crash-run",
            policy=_archive_policy(),
            crash_injector=inject,
        )

    if crash_point == "archive_verified":
        assert run.is_dir()
        assert reconcile_native_storage(native, cold, policy=_archive_policy()) == ()
    else:
        reconciled = reconcile_native_storage(native, cold, policy=_archive_policy())
        assert reconciled
        assert not run.exists()
    indexes = list((cold / "indexes").glob("*.json"))
    assert len(indexes) == 1
    index = json.loads(indexes[0].read_text(encoding="utf-8"))
    verify_lossless_archive(
        cold / index["archive_relative_path"],
        expected_sha256=index["archive_sha256"],
    )


def test_tampered_archive_before_cleanup_preserves_or_restores_hot(tmp_path):
    native, cold = tmp_path / "native", tmp_path / "cold"
    run = _minimal_run(native, "tamper-run", b"must-survive")

    def inject(point):
        if point == "hot_renamed":
            raise RuntimeError("crash")

    with pytest.raises(RuntimeError):
        retire_native_run(
            native,
            cold,
            "tamper-run",
            policy=_archive_policy(),
            crash_injector=inject,
        )
    index_path = next((cold / "indexes").glob("*.json"))
    index = json.loads(index_path.read_text(encoding="utf-8"))
    archive = cold / index["archive_relative_path"]
    payload = bytearray(archive.read_bytes())
    payload[len(payload) // 2] ^= 1
    archive.write_bytes(payload)

    with pytest.raises(NativeStorageIntegrityError, match="restored hot"):
        reconcile_native_storage(native, cold, policy=_archive_policy())
    assert (run / "EURUSD" / "M5" / "bars.csv").read_bytes() == b"must-survive"


def test_two_process_retirement_has_exactly_one_winner(tmp_path):
    native, cold = tmp_path / "native", tmp_path / "cold"
    _minimal_run(native, "race-run")
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    workers = [
        context.Process(target=_retire_process, args=(str(native), str(cold), queue))
        for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(30)
        assert worker.exitcode == 0
    outcomes = [queue.get(timeout=5) for _ in workers]
    assert all(kind == "ok" for kind, _value in outcomes), outcomes
    assert sorted(value for _kind, value in outcomes) == [False, True]


def test_deterministic_selection_and_admission_retire_only_complete_runs(tmp_path):
    native, cold = tmp_path / "native", tmp_path / "cold"
    _minimal_run(native, "z-run")
    _minimal_run(native, "a-run")
    incomplete = native / "runs" / "00-inflight"
    _write_file(incomplete / "partial.csv", b"partial")

    retired = retire_complete_native_runs(
        native, cold, policy=_archive_policy(), max_runs=1
    )
    assert len(retired) == 1
    assert not (native / "runs" / "a-run").exists()
    assert (native / "runs" / "z-run").is_dir()
    assert incomplete.is_dir()

    policy = NativeStoragePolicy(
        max_hot_bytes=64 * 1024 * 1024,
        max_cold_bytes=64 * 1024 * 1024,
        min_filesystem_free_bytes=1,
        max_complete_hot_runs=1,
        max_inflight_runs=2,
    )
    reservation = admit_native_run(
        native,
        cold,
        "next-run",
        expected_growth_bytes=1,
        expected_growth_files=1,
        policy=policy,
        retire_if_needed=True,
        max_retirements=1,
        filesystem_free_bytes=64 * 1024 * 1024,
    )
    assert reservation["run_id"] == "next-run"
    assert reservation["expected_growth_bytes"] == 1
    assert reservation["expected_growth_files"] == 1
    assert not (native / "runs" / "z-run").exists()
    assert incomplete.is_dir()


def test_hot_and_cold_file_caps_are_hard_failures(tmp_path):
    native, cold = tmp_path / "native", tmp_path / "cold"
    _minimal_run(native, "file-cap-run")
    usage_files = len(
        storage._walk_regular_files(
            native,
            max_files=100,
            max_entries=100,
            max_directory_entries=100,
        )
    )
    policy = NativeStoragePolicy(
        max_hot_bytes=64 * 1024 * 1024,
        max_cold_bytes=64 * 1024 * 1024,
        min_filesystem_free_bytes=1,
        max_complete_hot_runs=10,
        max_inflight_runs=2,
        max_hot_files=usage_files,
        max_cold_files=2,
    )
    decision = preflight_growth(
        native,
        cold,
        expected_growth_bytes=1,
        expected_growth_files=1,
        policy=policy,
        filesystem_free_bytes=1024,
    )
    assert not decision.allowed
    assert any("projected hot files" in reason for reason in decision.reasons)
    with pytest.raises(NativeStorageBoundError, match="cold-store file cap"):
        create_lossless_archive(
            native,
            cold,
            [native / "runs" / "file-cap-run"],
            graph=build_protection_graph(native),
            policy=policy,
        )
