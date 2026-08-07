from __future__ import annotations

import os
import inspect
from typing import Any, Iterable

import pytest

import pm_memory_governor as memory_governor_module
from pm_memory_governor import (
    MemoryGovernor,
    MemoryTask,
    is_memory_failure,
    resolve_worker_count,
    run_streamed_process_pool,
)


MIB = 1024 ** 2
MULTIPROCESS = pytest.mark.skipif(
    os.environ.get("FXPM_SKIP_MULTIPROCESS_TESTS") == "1",
    reason="multiprocessing explicitly disabled by environment",
)


def _identity(value: Any) -> Any:
    return value


def _return_pid() -> int:
    return os.getpid()


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
) -> MemoryGovernor:
    return MemoryGovernor(
        requested_workers=requested_workers,
        poll_interval_seconds=0.005,
        termination_grace_seconds=0.2,
    )


def test_memory_task_has_no_ram_estimate_and_requires_a_key() -> None:
    assert tuple(inspect.signature(MemoryTask).parameters) == ("key", "args", "kwargs")
    with pytest.raises(ValueError, match="key"):
        MemoryTask(key="")


def test_removed_ram_policing_api_is_not_exposed() -> None:
    for name in (
        "MemorySnapshot",
        "PeakMemoryEstimator",
        "system_memory_snapshot",
    ):
        assert not hasattr(memory_governor_module, name)


def test_auto_worker_count_uses_os_logical_cpus_and_finite_work(monkeypatch) -> None:
    monkeypatch.setattr(memory_governor_module.os, "cpu_count", lambda: 12)
    assert resolve_worker_count(0) == 12
    assert resolve_worker_count(-1, task_count=50) == 12
    assert resolve_worker_count(0, task_count=5) == 5
    assert resolve_worker_count(8, task_count=0) == 0
    assert resolve_worker_count(3, task_count=10) == 3
    with pytest.raises(ValueError, match="task_count"):
        resolve_worker_count(2, task_count=-1)


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


def test_scheduler_launches_up_to_worker_count_without_ram_admission(
    monkeypatch,
) -> None:
    launches: list[str] = []

    class CompletedProcess:
        def __init__(self, pid: int) -> None:
            self.pid = pid

        def is_alive(self) -> bool:
            return False

        def join(self, timeout=0) -> None:
            return None

        def close(self) -> None:
            return None

    class CompletedConnection:
        def __init__(self, messages: list[Any]) -> None:
            self.messages = messages

        def poll(self, timeout=0) -> bool:
            return bool(self.messages)

        def recv(self) -> Any:
            return self.messages.pop(0)

        def close(self) -> None:
            return None

    def completed_start(_context, _worker, task):
        launches.append(task.key)
        pid = 1000 + len(launches)
        process = CompletedProcess(pid)
        connection = CompletedConnection(
            [
                memory_governor_module.WorkerStarted(task.key, pid),
                memory_governor_module.WorkerEnvelope(
                    task_key=task.key,
                    pid=pid,
                    value=task.args[0],
                    error_type="",
                    error_message="",
                    traceback_text="",
                    peak_rss_bytes=64 * MIB,
                    duration_seconds=0.01,
                ),
            ]
        )
        return process, connection

    monkeypatch.setattr(
        memory_governor_module,
        "_launch_task_process",
        completed_start,
    )
    governor = MemoryGovernor(
        requested_workers=2,
        poll_interval_seconds=0.005,
        termination_grace_seconds=0.2,
    )
    outcomes = list(
        governor.iter_completed(
            _identity,
            [
                MemoryTask(key="FIRST", args=(1,)),
                MemoryTask(key="SECOND", args=(2,)),
            ],
        )
    )
    assert [outcome.status for outcome in outcomes] == ["COMPLETED", "COMPLETED"]
    assert [outcome.value for outcome in outcomes] == [1, 2]
    assert launches == ["FIRST", "SECOND"]
    assert governor.stats.peak_inflight == 2


def test_process_start_failure_is_an_explicit_outcome_for_every_task(monkeypatch) -> None:
    def failed_start(*_args, **_kwargs):
        raise OSError("CreateProcess denied")

    monkeypatch.setattr(memory_governor_module, "_launch_task_process", failed_start)
    governor = _governor(requested_workers=4)
    tasks = [MemoryTask(key=f"S{i}") for i in range(5)]
    outcomes = list(governor.iter_completed(_identity, tasks))
    assert len(outcomes) == 5
    assert all(item.status == "FAILED_PROCESS_START" for item in outcomes)
    assert governor.stats.process_start_failures == 5
    assert governor.stats.submitted == 0


def test_governor_api_contains_no_elapsed_ceiling() -> None:
    parameters = inspect.signature(MemoryGovernor).parameters
    assert tuple(parameters) == (
        "requested_workers",
        "poll_interval_seconds",
        "termination_grace_seconds",
        "process_context",
    )


@MULTIPROCESS
def test_every_task_uses_a_fresh_process_and_pending_input_is_bounded() -> None:
    governor = _governor(requested_workers=3)
    produced: list[int] = []

    def tasks() -> Iterable[MemoryTask]:
        for index in range(9):
            produced.append(index)
            yield MemoryTask(key=f"S{index}")

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
            [MemoryTask(key="AUDUSD")],
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
            [MemoryTask(key="CRASH")],
        )
    )
    assert outcome.status == "FAILED_CHILD_OOM_OR_CRASH"
    assert outcome.technical_failure
    assert outcome.possible_child_oom


@MULTIPROCESS
def test_unpicklable_worker_result_becomes_technical_serialization_failure() -> None:
    governor = _governor(requested_workers=1)
    outcome = next(
        governor.iter_completed(
            _return_unpicklable,
            [MemoryTask(key="BAD_RESULT")],
        )
    )
    assert outcome.status == "FAILED_TECHNICAL"
    assert outcome.error_type == "ResultSerializationError"


@MULTIPROCESS
def test_domain_result_is_not_reinterpreted_by_memory_scheduler() -> None:
    task = MemoryTask(key="AUDUSD")
    outcome = next(_governor(requested_workers=1).iter_completed(_returned_failure, [task]))
    assert outcome.status == "COMPLETED"
    assert outcome.value["success"] is False


@MULTIPROCESS
def test_governor_retains_only_constant_size_counters() -> None:
    governor = _governor(requested_workers=2)
    tasks = [
        MemoryTask(key=f"S{i}", args=(i,))
        for i in range(5)
    ]
    outcomes = list(governor.iter_completed(_identity, tasks))
    assert len(outcomes) == 5
    assert governor.stats.completed == 5
    assert not hasattr(governor.stats, "recent")


@MULTIPROCESS
def test_small_compatibility_api_streams_without_materializing_tasks() -> None:
    task_iter = (
        MemoryTask(key=f"S{i}", args=(i,))
        for i in range(3)
    )
    outcomes = list(
        run_streamed_process_pool(
            _identity,
            task_iter,
            requested_workers=2,
            poll_interval_seconds=0.005,
            termination_grace_seconds=0.2,
        )
    )
    assert sorted(item.value for item in outcomes) == [0, 1, 2]


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("poll_interval_seconds", -1.0),
        ("poll_interval_seconds", 0.0),
        ("termination_grace_seconds", float("nan")),
    ],
)
def test_polling_and_cleanup_controls_must_be_finite(name: str, value: float) -> None:
    kwargs = {name: value}
    with pytest.raises(ValueError, match=name):
        MemoryGovernor(requested_workers=1, **kwargs)
