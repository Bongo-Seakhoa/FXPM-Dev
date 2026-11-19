from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
import multiprocessing
import os
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


def test_accounting_spans_publications_and_manifest_domains_above_legacy_cap(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    _publish(root, "historical/EURUSD/a.bin", b"123456", maximum=10)
    _publish(root, "current/EURUSD/over-cap.bin", b"12345", maximum=10)
    _publish(root, "current/EURUSD/b.bin", b"7890", maximum=10)
    assert (root / "historical/EURUSD/a.bin").read_bytes() == b"123456"
    assert (root / "current/EURUSD/over-cap.bin").read_bytes() == b"12345"
    assert (root / "current/EURUSD/b.bin").read_bytes() == b"7890"
    assert _state(root)["committed_bytes"] == 15
    assert _state(root)["reservations"] == []


def test_legacy_free_space_reserve_does_not_block_actual_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for name, free in (("below-old-boundary", 99), ("old-boundary", 100)):
        root = tmp_path / name
        root.mkdir()
        monkeypatch.setattr(quota.shutil, "disk_usage",
            lambda _path, free=free: SimpleNamespace(total=200, used=200-free, free=free))
        _publish(root, "object.bin", b"x" * 20, maximum=100, reserve=80)
        assert (root / "object.bin").read_bytes() == b"x" * 20
        assert _state(root)["committed_bytes"] == 20
        assert _state(root)["reservations"] == []


def test_simultaneous_reservations_remain_accounted_above_legacy_free_space_rule(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    monkeypatch.setattr(quota.shutil, "disk_usage",
        lambda _path: SimpleNamespace(total=200, used=100, free=100))
    with quota.publication_lease(root, max_generation_bytes=100, min_free_bytes=80) as first:
        first.consume(15)
        with quota.publication_lease(root, max_generation_bytes=100, min_free_bytes=80) as second:
            second.consume(5)
            second.consume(1)
            records = _state(root)["reservations"]
            assert len(records) == 2
            assert sum(row["reserved_bytes"] for row in records) == 21
            second.cancel_after_cleanup()
        assert sum(row["reserved_bytes"] for row in _state(root)["reservations"]) == 15
        first.cancel_after_cleanup()
    assert _state(root)["reservations"] == []
    assert _state(root)["committed_bytes"] == 0


def test_dirty_recovery_rescans_exact_unique_inodes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    first = root / "historical/EURUSD/object.bin"
    alias = root / "current/EURUSD/object.bin"

    with pytest.raises(RuntimeError, match="injected-death"):
        with quota.publication_lease(
            root,
            max_generation_bytes=100,
            min_free_bytes=0,
        ) as lease:
            lease.consume(7)
            first.parent.mkdir(parents=True)
            first.write_bytes(b"1234567")
            alias.parent.mkdir(parents=True)
            try:
                os.link(first, alias)
            except OSError as exc:
                pytest.skip(f"hard links unavailable: {type(exc).__name__}")
            raise RuntimeError("injected-death")

    assert _state(root)["status"] == "DIRTY"
    assert _state(root)["committed_bytes"] == 0
    with quota.publication_lease(
        root,
        max_generation_bytes=100,
        min_free_bytes=0,
    ) as recovered:
        assert recovered.committed_bytes == 7
        recovered.consume(3)
        extra = root / "current/EURUSD/extra.bin"
        extra.write_bytes(b"890")
        recovered.commit(3)

    assert _state(root)["status"] == "CLEAN"
    assert _state(root)["committed_bytes"] == 10


def test_legacy_limit_metadata_can_change_without_losing_over_cap_accounting(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    _publish(root, "object.bin", b"x" * 120, maximum=100, reserve=10)
    for maximum, reserve in ((101, 10), (100, 11)):
        with quota.publication_lease(root, max_generation_bytes=maximum, min_free_bytes=reserve) as lease:
            lease.commit(0)
        current = _state(root)
        assert current["max_generation_bytes"] == maximum
        assert current["min_free_bytes"] == reserve
        assert current["committed_bytes"] == 120
        assert current["reservations"] == []
        assert (root / "object.bin").read_bytes() == b"x" * 120


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


def test_concurrent_threads_account_all_publications_above_legacy_generation_cap(
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
    assert _state(root)["committed_bytes"] == 12
    assert _state(root)["reservations"] == []


@pytest.mark.skipif(
    os.environ.get("FXPM_SKIP_MULTIPROCESS_TESTS") == "1",
    reason="multiprocessing explicitly disabled by environment",
)
def test_concurrent_processes_account_all_publications_above_legacy_generation_cap(
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
    assert _state(root)["committed_bytes"] == 12
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
    assert _state(root)["committed_bytes"] == 24


@pytest.mark.skipif(
    os.environ.get("FXPM_SKIP_MULTIPROCESS_TESTS") == "1",
    reason="multiprocessing explicitly disabled by environment",
)
def test_killed_writer_temp_is_reconciled_before_new_admission(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    context = multiprocessing.get_context("spawn")
    ready_event = context.Event()
    worker = context.Process(
        target=_process_crash_with_materialized_temp,
        args=(str(root), ready_event),
    )
    worker.start()
    try:
        assert ready_event.wait(10)
        worker.join(10)
        assert worker.exitcode == 37
    finally:
        if worker.is_alive():
            worker.terminate()
            worker.join(5)

    assert _state(root)["status"] == "DIRTY"
    with quota.publication_lease(
        root,
        max_generation_bytes=10,
        min_free_bytes=0,
    ) as recovered:
        assert recovered.committed_bytes == 7
        recovered.consume(3)
        extra = root / "recovered.bin"
        extra.write_bytes(b"890")
        recovered.materialize(3)
        recovered.commit(3)

    assert (root / "tmp" / "crashed.part").read_bytes() == b"1234567"
    assert _state(root)["status"] == "CLEAN"
    assert _state(root)["committed_bytes"] == 10


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
        assert sum(
            int(record["reserved_bytes"]) for record in dirty["reservations"]
        ) == reservation_count
        for lease in reversed(leases):
            lease.cancel_after_cleanup()
    assert _state(root)["status"] == "CLEAN"


def test_rescan_entry_bound_is_applied_during_iteration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "spool"
    root.mkdir()
    for index in range(6):
        (root / f"{index}.bin").write_bytes(bytes([index]))
    original_scandir = quota.os.scandir
    pulls = 0

    class CountingScandir:
        def __init__(self, path: Path) -> None:
            self._entries = original_scandir(path)

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            self._entries.close()

        def __iter__(self):
            return self

        def __next__(self):
            nonlocal pulls
            entry = next(self._entries)
            pulls += 1
            return entry

    monkeypatch.setattr(quota, "MAX_RESCAN_ENTRIES", 3)
    monkeypatch.setattr(quota.os, "scandir", CountingScandir)
    with pytest.raises(
        quota.AuthoritySpoolQuotaError,
        match="SPOOL_QUOTA_RESCAN_ENTRY_BOUND_EXCEEDED",
    ):
        quota._scan_committed_bytes(root)
    assert pulls == 4


def test_rescan_fails_closed_on_reparse_entry(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    link = root / "redirected-domain"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {type(exc).__name__}")

    with pytest.raises(
        quota.AuthoritySpoolQuotaError,
        match="SPOOL_QUOTA_REPARSE_ENTRY_FORBIDDEN",
    ):
        with quota.publication_lease(
            root,
            max_generation_bytes=100,
            min_free_bytes=0,
        ):
            pytest.fail("reparse-containing root was admitted")
