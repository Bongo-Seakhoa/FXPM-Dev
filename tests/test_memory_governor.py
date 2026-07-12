from __future__ import annotations

import os
import time
from typing import Any, Iterable

import pytest

import pm_memory_governor as memory_governor_module
from pm_memory_governor import (
    MemoryGovernor,
    MemorySnapshot,
    MemoryTask,
    PeakMemoryEstimator,
    is_memory_failure,
    run_streamed_process_pool,
    system_memory_snapshot,
)


GIB = 1024 ** 3
MIB = 1024 ** 2
MULTIPROCESS = pytest.mark.skipif(
    os.environ.get("FXPM_SKIP_MULTIPROCESS_TESTS") == "1",
    reason="multiprocessing explicitly disabled by environment",
)


def _snapshot(
    *,
    total: int = 16 * GIB,
    available: int = 14 * GIB,
    parent: int = 256 * MIB,
) -> MemorySnapshot:
    return MemorySnapshot(total, available, parent)


def _constant_provider(snapshot: MemorySnapshot):
    return lambda: snapshot


def _identity(value: Any) -> Any:
    return value


def _return_pid() -> int:
    return os.getpid()


def _sleep_and_return(seconds: float, value: Any) -> Any:
    time.sleep(seconds)
    return value


def _raise_memory_error() -> None:
    raise MemoryError("synthetic allocation exception")


def _hard_exit() -> None:
    os._exit(7)


def _return_unpicklable() -> Any:
    return lambda: None


def _returned_failure() -> dict[str, Any]:
    return {
        "success": False,
        "error_message": "simulated ArrayMemoryError: unable to allocate",
    }


def _governor(
    *,
    requested_workers: int = 2,
    reserve_bytes: int = GIB,
    snapshot: MemorySnapshot | None = None,
    task_timeout_seconds: float = 30.0,
    worker_start_timeout_seconds: float = 30.0,
    admission_timeout_seconds: float = 0.0,
) -> MemoryGovernor:
    return MemoryGovernor(
        requested_workers=requested_workers,
        reserve_bytes=reserve_bytes,
        memory_provider=_constant_provider(snapshot or _snapshot()),
        task_timeout_seconds=task_timeout_seconds,
        worker_start_timeout_seconds=worker_start_timeout_seconds,
        admission_timeout_seconds=admission_timeout_seconds,
        poll_interval_seconds=0.005,
        termination_grace_seconds=0.2,
    )


def test_system_snapshot_reports_valid_physical_memory() -> None:
    snapshot = system_memory_snapshot()
    assert snapshot.total_bytes > 0
    assert 0 <= snapshot.available_bytes <= snapshot.total_bytes
    assert snapshot.process_rss_bytes >= 0


def test_memory_snapshot_rejects_impossible_available_memory() -> None:
    with pytest.raises(ValueError, match="cannot exceed"):
        MemorySnapshot(total_bytes=10, available_bytes=11, process_rss_bytes=1)


def test_memory_task_requires_explicit_positive_peak_estimate() -> None:
    with pytest.raises(ValueError, match="estimated_peak_bytes"):
        MemoryTask(key="EURUSD", estimated_peak_bytes=0)
    with pytest.raises(ValueError, match="key"):
        MemoryTask(key="", estimated_peak_bytes=MIB)


@pytest.mark.parametrize(
    "value",
    [
        MemoryError("x"),
        "numpy.core._exceptions._ArrayMemoryError: Unable to allocate 17.4 MiB",
        OSError(12, "Cannot allocate memory"),
        OSError(1455, "The paging file is too small"),
    ],
)
def test_memory_failure_classifier_covers_python_numpy_and_os(value: Any) -> None:
    assert is_memory_failure(value)


def test_memory_failure_classifier_rejects_generic_technical_failure() -> None:
    assert not is_memory_failure("regime provenance is incomplete")


def test_peak_estimator_is_bounded_and_never_reduces_estimate() -> None:
    estimator = PeakMemoryEstimator(max_samples=3, safety_multiplier=1.10)
    base = 100 * MIB
    estimator.observe(base, 80 * MIB)
    assert estimator.adjusted_bytes(base) == base
    estimator.observe(base, 150 * MIB)
    assert estimator.adjusted_bytes(base) == 165 * MIB
    for ratio in (1.2, 1.3, 1.4, 1.1):
        estimator.observe(base, int(base * ratio))
    assert estimator.sample_count == 3


def test_admission_never_credits_child_or_unrelated_process_rss() -> None:
    governor = _governor(
        reserve_bytes=GIB,
        snapshot=_snapshot(total=8 * GIB, available=4 * GIB, parent=GIB),
    )
    admitted, capacity, _ = governor._can_admit(
        2 * GIB,
        {1: 2 * GIB},
    )
    # Available minus reserve is 3 GiB. Existing + proposed reservations are
    # 4 GiB, so aggregate child RSS can never make this admissible.
    assert capacity == 3 * GIB
    assert not admitted


def test_permanently_oversized_task_fails_without_starting_process(monkeypatch) -> None:
    started = False

    def forbidden_start(*_args, **_kwargs):
        nonlocal started
        started = True
        raise AssertionError("process must not start")

    monkeypatch.setattr(memory_governor_module, "_launch_task_process", forbidden_start)
    governor = _governor(
        requested_workers=4,
        reserve_bytes=GIB,
        snapshot=_snapshot(total=2 * GIB, available=2 * GIB, parent=512 * MIB),
        admission_timeout_seconds=10.0,
    )
    outcome = next(
        governor.iter_completed(
            _identity,
            [MemoryTask(key="IMPOSSIBLE", estimated_peak_bytes=GIB)],
        )
    )
    assert outcome.status == "FAILED_MEMORY_BUDGET"
    assert not started


@MULTIPROCESS
def test_transient_external_pressure_is_polled_before_admission() -> None:
    calls = 0

    def provider() -> MemorySnapshot:
        nonlocal calls
        calls += 1
        available = 400 * MIB if calls < 3 else 4 * GIB
        return _snapshot(total=8 * GIB, available=available, parent=512 * MIB)

    governor = MemoryGovernor(
        requested_workers=1,
        reserve_bytes=GIB,
        memory_provider=provider,
        admission_timeout_seconds=1.0,
        task_timeout_seconds=30.0,
        poll_interval_seconds=0.005,
        termination_grace_seconds=0.2,
    )
    outcome = next(
        governor.iter_completed(
            _identity,
            [MemoryTask(key="RECOVERS", args=(7,), estimated_peak_bytes=256 * MIB)],
        )
    )
    assert outcome.status == "COMPLETED"
    assert outcome.value == 7
    assert calls >= 3


def test_process_start_failure_is_an_explicit_outcome_for_every_task(monkeypatch) -> None:
    def failed_start(*_args, **_kwargs):
        raise OSError("CreateProcess denied")

    monkeypatch.setattr(memory_governor_module, "_launch_task_process", failed_start)
    governor = _governor(requested_workers=4)
    tasks = [MemoryTask(key=f"S{i}", estimated_peak_bytes=64 * MIB) for i in range(5)]
    outcomes = list(governor.iter_completed(_identity, tasks))
    assert len(outcomes) == 5
    assert all(item.status == "FAILED_PROCESS_START" for item in outcomes)
    assert governor.stats.process_start_failures == 5
    assert governor.stats.submitted == 0


@MULTIPROCESS
def test_every_task_uses_a_fresh_process_and_pending_input_is_bounded() -> None:
    governor = _governor(requested_workers=3)
    produced: list[int] = []

    def tasks() -> Iterable[MemoryTask]:
        for index in range(9):
            produced.append(index)
            yield MemoryTask(key=f"S{index}", estimated_peak_bytes=64 * MIB)

    outcomes = governor.iter_completed(_return_pid, tasks())
    first = next(outcomes)
    assert first.status == "COMPLETED"
    assert len(produced) <= governor.requested_workers + 1
    completed = [first, *list(outcomes)]
    pids = [item.worker_pid for item in completed]
    assert len(completed) == 9
    assert len(set(pids)) == 9
    assert governor.stats.peak_inflight <= governor.requested_workers


@MULTIPROCESS
def test_child_memory_error_is_never_a_semantic_result() -> None:
    governor = _governor(requested_workers=1)
    outcome = next(
        governor.iter_completed(
            _raise_memory_error,
            [MemoryTask(key="AUDUSD", estimated_peak_bytes=256 * MIB)],
        )
    )
    assert outcome.status == "FAILED_MEMORY"
    assert outcome.technical_failure
    assert outcome.memory_failure
    assert "synthetic allocation exception" in outcome.error_message


@MULTIPROCESS
def test_abrupt_child_exit_is_explicit_and_never_promoted() -> None:
    governor = _governor(requested_workers=1)
    outcome = next(
        governor.iter_completed(
            _hard_exit,
            [MemoryTask(key="CRASH", estimated_peak_bytes=128 * MIB)],
        )
    )
    assert outcome.status == "FAILED_CHILD_OOM_OR_CRASH"
    assert outcome.technical_failure
    assert outcome.possible_child_oom


@MULTIPROCESS
def test_worker_start_deadline_is_distinct_and_enforceable() -> None:
    governor = _governor(
        requested_workers=1,
        worker_start_timeout_seconds=0.001,
    )
    outcome = next(
        governor.iter_completed(
            _return_pid,
            [MemoryTask(key="SLOW_SPAWN", estimated_peak_bytes=128 * MIB)],
        )
    )
    assert outcome.status == "FAILED_TIMEOUT_START"
    assert outcome.error_type == "WorkerStartTimeoutError"


@MULTIPROCESS
def test_task_deadline_terminates_hung_child_in_bounded_time() -> None:
    governor = _governor(
        requested_workers=1,
        task_timeout_seconds=1.0,
    )
    started = time.monotonic()
    outcomes = list(
        governor.iter_completed(
            _sleep_and_return,
            [
                MemoryTask(
                    key="HUNG",
                    args=(30.0, "never"),
                    estimated_peak_bytes=128 * MIB,
                ),
                MemoryTask(
                    key="NEXT",
                    args=(0.0, "next"),
                    estimated_peak_bytes=128 * MIB,
                ),
            ],
        )
    )
    elapsed = time.monotonic() - started
    assert [item.status for item in outcomes] == ["FAILED_TIMEOUT", "COMPLETED"]
    assert outcomes[0].technical_failure
    assert outcomes[1].value == "next"
    assert governor.stats.timeout_failures == 1
    assert elapsed < 5.0


@MULTIPROCESS
def test_unpicklable_worker_result_becomes_technical_serialization_failure() -> None:
    governor = _governor(requested_workers=1)
    outcome = next(
        governor.iter_completed(
            _return_unpicklable,
            [MemoryTask(key="BAD_RESULT", estimated_peak_bytes=128 * MIB)],
        )
    )
    assert outcome.status == "FAILED_TECHNICAL"
    assert outcome.error_type == "ResultSerializationError"


@MULTIPROCESS
def test_domain_result_is_not_reinterpreted_by_memory_scheduler() -> None:
    task = MemoryTask(key="AUDUSD", estimated_peak_bytes=128 * MIB)
    outcome = next(_governor(requested_workers=1).iter_completed(_returned_failure, [task]))
    assert outcome.status == "COMPLETED"
    assert outcome.value["success"] is False


@MULTIPROCESS
def test_governor_retains_only_constant_size_counters() -> None:
    governor = _governor(requested_workers=2)
    tasks = [
        MemoryTask(key=f"S{i}", args=(i,), estimated_peak_bytes=64 * MIB)
        for i in range(5)
    ]
    outcomes = list(governor.iter_completed(_identity, tasks))
    assert len(outcomes) == 5
    assert governor.stats.completed == 5
    assert not hasattr(governor.stats, "recent")


@MULTIPROCESS
def test_small_compatibility_api_streams_without_materializing_tasks() -> None:
    task_iter = (
        MemoryTask(key=f"S{i}", args=(i,), estimated_peak_bytes=64 * MIB)
        for i in range(3)
    )
    outcomes = list(
        run_streamed_process_pool(
            _identity,
            task_iter,
            requested_workers=2,
            reserve_bytes=GIB,
            memory_provider=_constant_provider(_snapshot()),
            task_timeout_seconds=30.0,
            admission_timeout_seconds=0.0,
            poll_interval_seconds=0.005,
            termination_grace_seconds=0.2,
        )
    )
    assert sorted(item.value for item in outcomes) == [0, 1, 2]


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("task_timeout_seconds", float("inf")),
        ("task_timeout_seconds", 0.0),
        ("worker_start_timeout_seconds", 0.0),
        ("poll_interval_seconds", -1.0),
        ("termination_grace_seconds", float("nan")),
    ],
)
def test_liveness_controls_must_be_finite(name: str, value: float) -> None:
    kwargs = {name: value}
    with pytest.raises(ValueError, match=name):
        MemoryGovernor(requested_workers=1, reserve_bytes=GIB, **kwargs)
