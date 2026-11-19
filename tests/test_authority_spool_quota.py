"""Invented filesystem contracts; no source, model, financial or host admission authority."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
import multiprocessing
import os
import subprocess
import sys
import time
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest

import pm_authority_spool_quota as quota


def _publish(
    root: Path,
    relative_path: str,
    payload: bytes,
    *,
    maximum: int,
    reserve: int = 0,
) -> None:
    with quota.publication_lease(
        root,
        max_generation_bytes=maximum,
        min_free_bytes=reserve,
    ) as lease:
        lease.consume(len(payload))
        target = root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        lease.commit(len(payload))


def _process_publish(
    root_text: str,
    relative_path: str,
    ready_queue,
    start_event,
    result_queue,
) -> None:
    ready_queue.put(relative_path)
    if not start_event.wait(10):
        result_queue.put(("worker-timeout", relative_path))
        return
    try:
        _publish(
            Path(root_text),
            relative_path,
            b"123456",
            maximum=10,
        )
    except quota.AuthoritySpoolQuotaError as exc:
        result_queue.put((exc.code, relative_path))
    except BaseException as exc:  # pragma: no cover - reported to parent process
        result_queue.put((f"unexpected:{type(exc).__name__}:{exc}", relative_path))
    else:
        result_queue.put(("ok", relative_path))


def _process_hold_reservation(
    root_text: str,
    relative_path: str,
    ready_queue,
    release_event,
    result_queue,
) -> None:
    try:
        root = Path(root_text)
        with quota.publication_lease(
            root,
            max_generation_bytes=24,
            min_free_bytes=0,
        ) as lease:
            lease.consume(4)
            ready_queue.put(relative_path)
            if not release_event.wait(10):
                result_queue.put(("worker-timeout", relative_path))
                return
            target = root / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as stream:
                stream.write(b"1234")
                stream.flush()
                os.fsync(stream.fileno())
            lease.materialize(4)
            lease.commit(4)
    except quota.AuthoritySpoolQuotaError as exc:
        result_queue.put((exc.code, relative_path))
    except BaseException as exc:  # pragma: no cover - reported to parent process
        result_queue.put((f"unexpected:{type(exc).__name__}:{exc}", relative_path))
    else:
        result_queue.put(("ok", relative_path))


def _process_crash_with_materialized_temp(
    root_text: str,
    ready_event,
) -> None:
    root = Path(root_text)
    with quota.publication_lease(
        root,
        max_generation_bytes=10,
        min_free_bytes=0,
    ) as lease:
        lease.consume(7)
        target = root / "tmp" / "crashed.part"
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(b"1234567")
            stream.flush()
            os.fsync(stream.fileno())
        lease.materialize(7)
        ready_event.set()
        os._exit(37)


def _state(root: Path) -> dict:
    return json.loads(
        (root / quota.QUOTA_STATE_NAME).read_text(encoding="ascii")
    )


def test_quota_state_replace_retries_transient_permission_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    value = quota._state(
        max_generation_bytes=100,
        min_free_bytes=10,
        committed_bytes=0,
    )
    real_replace = os.replace
    calls = 0
    sleeps: list[float] = []

    def transient_replace(source: Path, target: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise PermissionError("synthetic non-delete-sharing handle")
        real_replace(source, target)

    monkeypatch.setattr(quota.os, "replace", transient_replace)
    monkeypatch.setattr(quota.time, "sleep", sleeps.append)
    quota._write_state(root, value)

    assert quota._read_state(root) == value
    assert calls == 2
    assert sleeps == [quota.STATE_REPLACE_INITIAL_DELAY_SECONDS]
    assert not list(root.glob(f"{quota.QUOTA_TEMP_PREFIX}*.tmp"))


def test_quota_state_replace_exhaustion_preserves_prior_state_and_cleans_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    target = root / quota.QUOTA_STATE_NAME
    prior = b"prior-state-must-survive"
    target.write_bytes(prior)
    value = quota._state(
        max_generation_bytes=100,
        min_free_bytes=10,
        committed_bytes=0,
    )
    calls = 0
    sleeps: list[float] = []

    def denied_replace(_source: Path, _target: Path) -> None:
        nonlocal calls
        calls += 1
        raise PermissionError("synthetic persistent handle")

    monkeypatch.setattr(quota.os, "replace", denied_replace)
    monkeypatch.setattr(quota.time, "sleep", sleeps.append)
    with pytest.raises(
        quota.AuthoritySpoolQuotaError,
        match="SPOOL_QUOTA_STATE_WRITE_FAILED:PermissionError",
    ):
        quota._write_state(root, value)

    assert calls == quota.STATE_REPLACE_ATTEMPTS
    assert len(sleeps) == quota.STATE_REPLACE_ATTEMPTS - 1
    assert target.read_bytes() == prior
    assert not list(root.glob(f"{quota.QUOTA_TEMP_PREFIX}*.tmp"))


def test_publications_span_domains_above_legacy_capacity_metadata(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    _publish(root, "historical/EURUSD/a.bin", b"123456", maximum=10)
    _publish(root, "current/EURUSD/over-cap.bin", b"12345", maximum=10)
    _publish(root, "current/EURUSD/b.bin", b"7890", maximum=10)
    assert (root / "historical/EURUSD/a.bin").read_bytes() == b"123456"
    assert (root / "current/EURUSD/over-cap.bin").read_bytes() == b"12345"
    assert (root / "current/EURUSD/b.bin").read_bytes() == b"7890"

    assert _state(root)["reservations"] == []


def test_legacy_free_space_metadata_does_not_block_actual_publication(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    _publish(root, "object.bin", b"x" * 20, maximum=1, reserve=2**40)
    assert (root / "object.bin").read_bytes() == b"x" * 20
    assert _state(root)["reservations"] == []


def test_simultaneous_memberships_overlap_above_legacy_capacity_metadata(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    with quota.publication_lease(root, max_generation_bytes=100, min_free_bytes=80) as first:
        first.consume(15)
        with quota.publication_lease(root, max_generation_bytes=100, min_free_bytes=80) as second:
            second.consume(5)
            second.consume(1)
            records = _state(root)["reservations"]
            assert len(records) == 2
            second.cancel_after_cleanup()
        assert len(_state(root)["reservations"]) == 1
        first.cancel_after_cleanup()
    assert _state(root)["reservations"] == []



def test_known_abandoned_membership_recovers_without_payload_recount(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    target = root / "payload" / "object.bin"
    target.parent.mkdir()
    with pytest.raises(RuntimeError, match="injected-death"):
        with quota.publication_lease(root, max_generation_bytes=100, min_free_bytes=0) as lease:
            lease.consume(7)
            target.write_bytes(b"1234567")
            lease.materialize(7)
            raise RuntimeError("injected-death")
    assert len(_state(root)["reservations"]) == 1
    real_scandir = quota.os.scandir
    def no_payload_scan(path):
        assert Path(path) != target.parent, "quota admission descended into payload bytes"
        return real_scandir(path)
    monkeypatch.setattr(quota.os, "scandir", no_payload_scan)
    with quota.publication_lease(root, max_generation_bytes=100, min_free_bytes=0) as recovered:
        assert recovered.committed_bytes == 0
        assert len(_state(root)["reservations"]) == 1
        recovered.cancel_after_cleanup()
    assert target.read_bytes() == b"1234567"
    assert _state(root)["status"] == "CLEAN"
    assert _state(root)["reservations"] == []



def test_legacy_numeric_state_is_preserved_as_recorded_metadata(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    quota._write_state(root, quota._state(max_generation_bytes=100, min_free_bytes=10,
        committed_bytes=120))
    target = root / "object.bin"
    target.write_bytes(b"old payload")
    for count, (maximum, reserve) in enumerate(((101, 10), (100, 11)), start=1):
        with quota.publication_lease(root, max_generation_bytes=maximum, min_free_bytes=reserve) as lease:
            assert lease.committed_bytes == 120
            lease.consume(1)
            with target.open("ab") as output:
                written = output.write(b"x")
            lease.materialize(written)
            lease.commit(written)
        current = _state(root)
        assert current["max_generation_bytes"] == 100
        assert current["min_free_bytes"] == 10
        assert current["committed_bytes"] == 120
        assert current["reservations"] == []
        assert target.read_bytes() == b"old payload" + b"x" * count


def test_oversized_integer_state_fails_with_typed_read_error(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    malformed = (
        '{"schema_version":"'
        + quota.QUOTA_STATE_SCHEMA
        + '","status":"CLEAN","max_generation_bytes":'
        + ("1" * 5000)
        + ',"min_free_bytes":0,"committed_bytes":0,"content_sha256":"'
        + ("0" * 64)
        + '"}'
    )
    (root / quota.QUOTA_STATE_NAME).write_text(
        malformed,
        encoding="ascii",
    )

    with pytest.raises(
        quota.AuthoritySpoolQuotaError,
        match="SPOOL_QUOTA_STATE_READ_FAILED:ValueError",
    ):
        with quota.publication_lease(
            root,
            max_generation_bytes=100,
            min_free_bytes=0,
        ):
            pytest.fail("malformed state was admitted")


def test_concurrent_threads_publish_all_objects_above_legacy_capacity_metadata(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    barrier = threading.Barrier(2)

    def attempt(name: str) -> str:
        barrier.wait(timeout=5)
        try:
            _publish(root, f"{name}/object.bin", b"123456", maximum=10)
        except quota.AuthoritySpoolQuotaError as exc:
            return exc.code
        return "ok"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(attempt, ("historical", "current")))

    assert outcomes == ["ok", "ok"]
    objects = list(root.glob("*/object.bin"))
    assert len(objects) == 2
    assert sum(path.stat().st_size for path in objects) == 12

    assert _state(root)["reservations"] == []


@pytest.mark.skipif(
    os.environ.get("FXPM_SKIP_MULTIPROCESS_TESTS") == "1",
    reason="multiprocessing explicitly disabled by environment",
)
def test_concurrent_processes_publish_all_objects_above_legacy_capacity_metadata(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    context = multiprocessing.get_context("spawn")
    ready_queue = context.Queue()
    result_queue = context.Queue()
    start_event = context.Event()
    workers = [
        context.Process(
            target=_process_publish,
            args=(
                str(root),
                f"{domain}/object.bin",
                ready_queue,
                start_event,
                result_queue,
            ),
        )
        for domain in ("historical", "current")
    ]
    try:
        for worker in workers:
            worker.start()
        assert {ready_queue.get(timeout=10) for _ in workers} == {
            "historical/object.bin",
            "current/object.bin",
        }
        start_event.set()
        for worker in workers:
            worker.join(15)
            assert worker.exitcode == 0
        outcomes = [result_queue.get(timeout=5)[0] for _ in workers]
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)
        ready_queue.close()
        result_queue.close()
        ready_queue.join_thread()
        result_queue.join_thread()

    assert outcomes == ["ok", "ok"]
    objects = list(root.glob("*/object.bin"))
    assert len(objects) == 2
    assert sum(path.stat().st_size for path in objects) == 12

    assert _state(root)["reservations"] == []


@pytest.mark.skipif(
    os.environ.get("FXPM_SKIP_MULTIPROCESS_TESTS") == "1",
    reason="multiprocessing explicitly disabled by environment",
)
def test_six_process_reservations_overlap_without_holding_root_lock(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    context = multiprocessing.get_context("spawn")
    ready_queue = context.Queue()
    result_queue = context.Queue()
    release_event = context.Event()
    workers = [
        context.Process(
            target=_process_hold_reservation,
            args=(
                str(root),
                f"{name}/object.bin",
                ready_queue,
                release_event,
                result_queue,
            ),
        )
        for name in ("first", "second", "third", "fourth", "fifth", "sixth")
    ]
    try:
        for worker in workers:
            worker.start()
        assert {ready_queue.get(timeout=10) for _ in workers} == {
            "first/object.bin",
            "second/object.bin",
            "third/object.bin",
            "fourth/object.bin",
            "fifth/object.bin",
            "sixth/object.bin",
        }
        release_event.set()
        for worker in workers:
            worker.join(15)
            assert worker.exitcode == 0
        assert sorted(result_queue.get(timeout=5)[0] for _ in workers) == [
            "ok",
            "ok",
            "ok",
            "ok",
            "ok",
            "ok",
        ]
    finally:
        release_event.set()
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)
        ready_queue.close()
        result_queue.close()
        ready_queue.join_thread()
        result_queue.join_thread()

    assert _state(root)["status"] == "CLEAN"



@pytest.mark.skipif(
    os.environ.get("FXPM_SKIP_MULTIPROCESS_TESTS") == "1",
    reason="multiprocessing explicitly disabled by environment",
)
def test_killed_writer_temp_is_reconciled_before_new_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    ready_path = tmp_path / "writer-ready"
    product_root = Path(quota.__file__).resolve().parent
    workspace_root = Path.cwd().resolve()
    child_code = r"""
from pathlib import Path
import os
import sys
import time
product_root, workspace_root, root, ready = map(Path, sys.argv[1:])
sys.path[:0] = [str(product_root), str(workspace_root)]
import pm_authority_spool_quota as quota
if Path(quota.__file__).resolve().parent != product_root.resolve():
    raise RuntimeError("KILLED_WRITER_QUOTA_ORIGIN_MISMATCH")
with quota.publication_lease(root, max_generation_bytes=10, min_free_bytes=0) as lease:
    lease.consume(7)
    target = root / "tmp" / "crashed.part"
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as stream:
        stream.write(b"1234567")
        stream.flush()
        os.fsync(stream.fileno())
    lease.materialize(7)
    with ready.open("xb") as marker:
        marker.write(b"READY")
        marker.flush()
        os.fsync(marker.fileno())
    while True:
        time.sleep(0.1)
"""
    worker = subprocess.Popen([sys.executable, "-E", "-B", "-c", child_code,
        str(product_root), str(workspace_root), str(root), str(ready_path)],
        cwd=workspace_root, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    ready = False
    alive_at_ready = False
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if ready_path.exists():
                ready = ready_path.read_bytes() == b"READY"
                if ready:
                    break
            if worker.poll() is not None:
                break
            time.sleep(0.01)
        alive_at_ready = ready and worker.poll() is None
        if worker.poll() is None:
            worker.kill()
        _stdout, stderr = worker.communicate(timeout=10)
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.communicate(timeout=5)
    diagnostics = stderr.decode("utf-8", errors="replace")
    assert ready, f"real killed-writer child was not ready: {diagnostics}"
    assert alive_at_ready, f"real writer exited before the parent kill: {diagnostics}"
    assert worker.returncode is not None and worker.returncode != 0

    assert _state(root)["status"] == "DIRTY"
    real_scandir = quota.os.scandir
    def no_temp_scan(path):
        assert Path(path) != root / "tmp", "quota recovery descended into crashed payload bytes"
        return real_scandir(path)
    monkeypatch.setattr(quota.os, "scandir", no_temp_scan)
    with quota.publication_lease(
        root,
        max_generation_bytes=10,
        min_free_bytes=0,
    ) as recovered:
        assert recovered.committed_bytes == 0  # Fresh recorded metadata, not a payload recount.
        recovered.consume(3)
        extra = root / "recovered.bin"
        extra.write_bytes(b"890")
        recovered.materialize(3)
        recovered.commit(3)

    assert (root / "tmp" / "crashed.part").read_bytes() == b"1234567"
    assert _state(root)["status"] == "CLEAN"



def test_reservation_roster_allows_scheduler_concurrency_without_count_ceiling(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    reservation_count = 12
    with ExitStack() as stack:
        leases = [
            stack.enter_context(
                quota.publication_lease(
                    root,
                    max_generation_bytes=100,
                    min_free_bytes=0,
                )
            )
            for _ in range(reservation_count)
        ]
        for lease in leases:
            lease.consume(1)
        dirty = _state(root)
        assert dirty["status"] == "DIRTY"
        assert len(dirty["reservations"]) == reservation_count
        for lease in reversed(leases):
            lease.cancel_after_cleanup()
    assert _state(root)["status"] == "CLEAN"






def test_consume_materialize_are_local_while_membership_and_guard_stay_live(tmp_path, monkeypatch):
    root = tmp_path / "hot-loop"
    root.mkdir()
    with quota.publication_lease(root, max_generation_bytes=100, min_free_bytes=0) as first:
        with quota.publication_lease(root, max_generation_bytes=100, min_free_bytes=0) as second:
            before = (root / quota.QUOTA_STATE_NAME).read_bytes()
            members = _state(root)["reservations"]
            assert len(members) == 2
            guards = {record["token"]: quota._guard_path(root, record["token"]) for record in members}
            identities = {token: (path.stat().st_dev, path.stat().st_ino) for token, path in guards.items()}
            def forbidden(*args, **kwargs):
                pytest.fail("consume/materialize performed quota state, lock or scan I/O")
            published = root / "local-published.bin"
            with published.open("xb") as output, monkeypatch.context() as hot:
                hot.setattr(quota, "_read_state", forbidden)
                hot.setattr(quota, "_write_state", forbidden)
                hot.setattr(quota, "_exclusive_file_lock", forbidden)
                hot.setattr(quota.os, "scandir", forbidden)
                for _ in range(64):
                    first.consume(3)
                    first.materialize(output.write(b"ab"))
                    first.materialize(output.write(b"c"))
                assert first.in_progress_bytes == 192
                with pytest.raises(quota.AuthoritySpoolQuotaError, match="SPOOL_QUOTA_MATERIALIZED_EXCEEDS_RESERVED"):
                    first.materialize(1)
            assert (root / quota.QUOTA_STATE_NAME).read_bytes() == before
            assert all(quota._guard_is_live(root, token) for token in guards)
            assert {token: (path.stat().st_dev, path.stat().st_ino) for token, path in guards.items()} == identities
            second.cancel_after_cleanup()
        assert len(_state(root)["reservations"]) == 1
        assert quota._guard_is_live(root, first.token)
        assert published.read_bytes() == b"abc" * 64
        published.unlink()
        first.cancel_after_cleanup()
    assert _state(root)["reservations"] == []


def test_local_stream_refusals_keep_the_membership_and_exact_progress(tmp_path):
    root = tmp_path / "local-errors"
    root.mkdir()
    with quota.publication_lease(root, max_generation_bytes=100, min_free_bytes=0) as lease:
        before = (root / quota.QUOTA_STATE_NAME).read_bytes()
        for method, code in ((lease.consume, "SPOOL_QUOTA_CONSUME_BYTES_INVALID"),
                (lease.materialize, "SPOOL_QUOTA_MATERIALIZE_BYTES_INVALID")):
            for invalid in (-1, True):
                with pytest.raises(quota.AuthoritySpoolQuotaError, match=code):
                    method(invalid)
        lease.consume(3)
        output_path = root / "local-written.bin"
        with output_path.open("xb") as output:
            lease.materialize(output.write(b"a"))
            lease.materialize(output.write(b"bc"))
        assert lease.in_progress_bytes == 3
        with pytest.raises(quota.AuthoritySpoolQuotaError, match="SPOOL_QUOTA_MATERIALIZED_EXCEEDS_RESERVED"):
            lease.materialize(1)
        with pytest.raises(quota.AuthoritySpoolQuotaError, match="SPOOL_QUOTA_COMMIT_EXCEEDS_CONSUMED"):
            lease.commit(4)
        assert (root / quota.QUOTA_STATE_NAME).read_bytes() == before
        assert output_path.read_bytes() == b"abc"
        output_path.unlink()
        lease.cancel_after_cleanup()
    with pytest.raises(quota.AuthoritySpoolQuotaError, match="SPOOL_QUOTA_LEASE_ALREADY_FINISHED"):
        lease.consume(1)


def test_fresh_admission_keeps_membership_without_payload_directory_scan(tmp_path, monkeypatch):
    root = tmp_path / "fresh"
    payload = root / "payload" / "old.bin"
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b"existing invented payload bytes")
    real_scandir = quota.os.scandir
    def no_payload_scan(path):
        assert Path(path) != payload.parent, "fresh admission descended into payload bytes"
        return real_scandir(path)
    monkeypatch.setattr(quota.os, "scandir", no_payload_scan)
    with quota.publication_lease(root, max_generation_bytes=1, min_free_bytes=2**40) as lease:
        assert lease.committed_bytes == 0
        assert len(_state(root)["reservations"]) == 1
        lease.cancel_after_cleanup()
    assert payload.read_bytes() == b"existing invented payload bytes"
    assert _state(root)["reservations"] == []
