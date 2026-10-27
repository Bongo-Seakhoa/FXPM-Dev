from __future__ import annotations

import os
import inspect
from collections import deque
from pathlib import Path
import time
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


def _drain_witness(directory: str, key: str, kind: str) -> dict[str, Any]:
    """Invented nonfinancial worker; all three admitted jobs meet before faulting."""
    root = Path(directory)
    (root / f"{key}.started").write_text(str(os.getpid()), encoding="ascii")
    deadline = time.monotonic() + 15
    while len(tuple(root.glob("*.started"))) < 3:
        if time.monotonic() >= deadline:
            raise RuntimeError("SYNTHETIC_THREE_WORKER_BARRIER_NOT_REACHED")
        time.sleep(0.005)
    if kind == "raise":
        raise RuntimeError("SYNTHETIC_NEW_WORKER_FAILURE")
    if kind == "crash":
        os._exit(7)
    if kind == "domain_stop":
        return {"symbol": key, "new_stop": True}
    while not (root / "admission.closed").is_file():
        if time.monotonic() >= deadline:
            raise RuntimeError("SYNTHETIC_PARENT_DID_NOT_CLOSE_ADMISSION")
        time.sleep(0.005)
    (root / f"{key}.finished").write_text("done", encoding="ascii")
    return {"symbol": key, "new_stop": False}


def _short_durable_marker(directory: str, key: str) -> str:
    time.sleep(0.04)
    (Path(directory) / f"{key}.finished").write_text("done", encoding="ascii")
    return key


def test_declared_worker_target_uses_affinity_ceiling_and_preserves_explicit_count(monkeypatch) -> None:
    monkeypatch.setattr(memory_governor_module, "effective_cpu_count", lambda: 6)
    recommend = memory_governor_module.recommended_worker_count
    assert recommend(0, 30) == 5
    assert recommend(-1, 30) == 5
    assert recommend(0, 2) == 2
    assert recommend(6, 30) == 6
    assert recommend(0, 0) == 0
    assert recommend(0, 30, cpu_target_fraction=1.0) == 6
    with pytest.raises(ValueError, match="cpu_target_fraction"):
        recommend(0, 30, cpu_target_fraction=float("nan"))
    assert resolve_worker_count(6, task_count=30) == 6


def test_windows_affinity_query_declares_pointer_width_and_uses_mask_popcount(monkeypatch) -> None:
    ctypes = memory_governor_module.ctypes
    handle = 0x100000001

    class Call:
        def __init__(self, function):
            self.function = function
            self.argtypes = None
            self.restype = None

        def __call__(self, *args):
            return self.function(*args)

    def affinity(process, process_mask, system_mask):
        assert process == handle
        process_mask._obj.value = 0b111111
        system_mask._obj.value = 0b11111111
        return 1

    class Kernel:
        GetCurrentProcess = Call(lambda: handle)
        GetProcessAffinityMask = Call(affinity)

    kernel = Kernel()
    monkeypatch.setattr(ctypes, "WinDLL", lambda *_args, **_kwargs: kernel, raising=False)
    assert memory_governor_module._windows_affinity_cpu_count() == 6
    assert kernel.GetCurrentProcess.restype is ctypes.c_void_p
    assert kernel.GetProcessAffinityMask.argtypes == [
        ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t),
    ]


def test_closed_admission_reports_every_unstarted_task_without_launch(monkeypatch) -> None:
    def forbidden_start(*_args, **_kwargs):
        pytest.fail("closed admission launched a process")

    monkeypatch.setattr(memory_governor_module, "_launch_task_process", forbidden_start)
    governor = _governor(requested_workers=3)
    outcomes = list(governor.iter_completed(
        _identity, (MemoryTask(key=f"S{i}") for i in range(7)), admission_gate=lambda: False,
    ))
    assert [item.task_key for item in outcomes] == [f"S{i}" for i in range(7)]
    assert all(item.status == "NOT_RUN" and item.worker_pid == 0 for item in outcomes)
    assert governor.stats.submitted == 0
    assert governor.resource_stats.not_run == 7


def test_failed_child_cleanup_drains_healthy_sibling_without_targeting_it(monkeypatch) -> None:
    stopped = []

    class Process:
        def __init__(self, pid, alive):
            self.pid, self.alive = pid, alive

        def is_alive(self):
            return self.alive

        def join(self, timeout=0):
            pass

        def close(self):
            pass

    class Connection:
        def __init__(self, task, pid):
            self.bad = task.key == "BAD"
            self.messages = [memory_governor_module.WorkerEnvelope(
                task.key, pid, "SAVED_SUCCESS", "", "", "", 10, 0.01,
            )]

        def poll(self, timeout=0):
            if self.bad:
                raise EOFError("SYNTHETIC_PROTOCOL_FAILURE")
            return bool(self.messages)

        def recv(self):
            return self.messages.pop(0)

        def close(self):
            pass

    processes = {"BAD": Process(101, True), "GOOD": Process(102, False)}
    def launch(_context, _worker, task):
        assert task.key in processes
        process = processes[task.key]
        return process, Connection(task, process.pid)

    def stop(process, _grace):
        assert process.pid == 101, "healthy sibling was targeted by cleanup"
        stopped.append(process.pid)
        if len(stopped) == 1:
            return False
        process.alive = False
        return True

    monkeypatch.setattr(memory_governor_module, "_launch_task_process", launch)
    monkeypatch.setattr(memory_governor_module, "_stop_process", stop)
    governor = _governor(requested_workers=2)
    outcomes = list(governor.iter_completed(
        _identity, [MemoryTask(key=key) for key in ("BAD", "GOOD", "PENDING")],
        admission_gate=lambda: True,
    ))
    assert {item.task_key: item.status for item in outcomes} == {
        "BAD": "FAILED_CHILD_OOM_OR_CRASH", "GOOD": "COMPLETED", "PENDING": "NOT_RUN",
    }
    assert next(item.value for item in outcomes if item.task_key == "GOOD") == "SAVED_SUCCESS"
    assert stopped == [101, 101]
    assert not processes["BAD"].is_alive()
    assert governor.resource_stats.admission_stop_reason == "FAILED_CHILD_TERMINATION"


@MULTIPROCESS
@pytest.mark.parametrize("kind", ["raise", "crash", "domain_stop"])
def test_closed_admission_drains_real_siblings_and_retains_results(tmp_path, kind) -> None:
    governor = _governor(requested_workers=3)
    tasks = [
        MemoryTask(key=f"S{i}", args=(str(tmp_path), f"S{i}", kind if i == 0 else "ok"))
        for i in range(6)
    ]
    open_gate = True
    outcomes = []
    stream = governor.iter_completed(_drain_witness, tasks, admission_gate=lambda: open_gate)
    for outcome in stream:
        outcomes.append(outcome)
        if outcome.technical_failure or (type(outcome.value) is dict and outcome.value.get("new_stop")):
            open_gate = False
            (tmp_path / "admission.closed").write_text("closed", encoding="ascii")
    assert len(outcomes) == 6
    assert governor.stats.submitted == governor.stats.peak_inflight == 3
    assert {path.stem for path in tmp_path.glob("*.started")} == {"S0", "S1", "S2"}
    assert {path.stem for path in tmp_path.glob("*.finished")} == {"S1", "S2"}
    assert {item.task_key for item in outcomes if item.status == "NOT_RUN"} == {"S3", "S4", "S5"}
    assert all(item.worker_pid == 0 for item in outcomes if item.status == "NOT_RUN")
    assert {item.task_key for item in outcomes if item.status == "COMPLETED" and not item.value.get("new_stop")} == {"S1", "S2"}
    failure = next(item for item in outcomes if item.task_key == "S0")
    assert failure.status == {"raise": "FAILED_TECHNICAL", "crash": "FAILED_CHILD_OOM_OR_CRASH", "domain_stop": "COMPLETED"}[kind]
    assert governor.resource_stats.not_run == 3


@MULTIPROCESS
def test_task_iterator_exception_is_raised_only_after_active_job_finishes(tmp_path) -> None:
    def tasks():
        yield MemoryTask(key="STARTED", args=(str(tmp_path), "STARTED"))
        raise ValueError("SYNTHETIC_TASK_ITERATOR_FAILURE")

    governor = _governor(requested_workers=2)
    outcomes = []
    with pytest.raises(ValueError, match="SYNTHETIC_TASK_ITERATOR_FAILURE"):
        for outcome in governor.iter_completed(_short_durable_marker, tasks(), admission_gate=lambda: True):
            outcomes.append(outcome)
    assert [item.status for item in outcomes] == ["COMPLETED"]
    assert (tmp_path / "STARTED.finished").read_text(encoding="ascii") == "done"


def _invented_system_sample(**updates):
    sample = dict(
        physical_ram_bytes=1000, available_ram_bytes=0, system_commit_bytes=900,
        system_commit_limit_bytes=2000, system_page_fault_count=None, free_disk_bytes=123,
        system_cpu_total_seconds=10.0, system_cpu_idle_seconds=8.0, error_count=0,
    )
    sample.update(updates)
    return sample


def test_resource_samples_are_concurrent_bounded_and_ignore_other_apps_free_ram(monkeypatch) -> None:
    clock = [10.0]
    monkeypatch.setattr(memory_governor_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(memory_governor_module, "effective_cpu_count", lambda: 6)
    parent = os.getpid()
    counter = [0.0]

    def process(pid):
        return dict(
            pid=pid, identity=pid + 0.5, rss_bytes=70 if pid == parent else 40,
            private_commit_bytes=90 if pid == parent else 50, page_fault_count=3 if pid == parent else 7,
            cpu_seconds=counter[0], error_type="",
        )

    monkeypatch.setattr(memory_governor_module, "_sample_process_resources", process)
    monkeypatch.setattr(memory_governor_module, "_sample_system_resources", lambda _path: _invented_system_sample())
    rows = deque(maxlen=2)
    stats = memory_governor_module.ResourceStats()
    observer = memory_governor_module._ResourceObserver(
        memory_governor_module.GovernorResourcePolicy(sample_interval_seconds=0.01), rows.append, stats,
    )
    first = observer.observe((42,))
    assert first["own_rss_bytes"] == 110
    assert first["own_private_commit_bytes"] == 140
    assert first["worker_rss_bytes"] == 40
    assert first["page_fault_count"] == 10
    assert first["own_cpu_percent"] is None
    assert first["available_ram_bytes"] == 0
    assert not observer.hold(first, True)
    clock[0], counter[0] = 12.0, 1.0
    second = observer.observe((42,))
    assert second["own_cpu_percent"] == pytest.approx(100 / 6)
    assert second["cpu_coverage_complete"]
    assert second["sampling"] == "SYNCHRONOUS_SAMPLED_NOT_CONTINUOUS"
    clock[0] = 17.0
    third = observer.observe(())
    assert third["own_cpu_percent"] is None
    assert not third["cpu_coverage_complete"]
    assert len(rows) == 2
    assert tuple(observer.previous) == (parent,)
    assert stats.max_sample_gap_seconds == 5
    assert stats.peak_own_rss_bytes == 110
    assert not any(isinstance(value, (list, dict, set)) for value in vars(stats).values())


def test_unknown_measurements_fail_open_and_telemetry_cannot_mutate_hold_inputs(monkeypatch) -> None:
    def unavailable(_pid):
        raise OSError("SYNTHETIC_RESOURCE_QUERY_UNAVAILABLE")

    monkeypatch.setattr(memory_governor_module, "_sample_process_resources", unavailable)
    monkeypatch.setattr(memory_governor_module, "_sample_system_resources", lambda _path: _invented_system_sample())
    stats = memory_governor_module.ResourceStats()
    observer = memory_governor_module._ResourceObserver(
        memory_governor_module.GovernorResourcePolicy(), None, stats,
    )
    missing = observer.observe((42,))
    assert missing["own_rss_bytes"] is None
    assert not observer.hold(missing, True)
    assert stats.measurement_errors >= 2
    assert stats.ram_measurement_fail_open == 1
    assert stats.peak_own_rss_bytes is None

    monkeypatch.setattr(memory_governor_module, "_sample_process_resources", lambda pid: dict(
        pid=pid, identity=1, rss_bytes=900, private_commit_bytes=950,
        page_fault_count=1, cpu_seconds=0.0, error_type="",
    ))
    def corrupt_copy(row):
        row["own_rss_bytes"] = 0
        row["processes"][0]["rss_bytes"] = 0
        raise OSError("SYNTHETIC_TELEMETRY_SINK_FAILURE")

    observer.callback = corrupt_copy
    measured = observer.observe((), force=True)
    assert measured["own_rss_bytes"] == 900
    assert measured["processes"][0]["rss_bytes"] == 900
    assert observer.hold(measured, True)
    assert not observer.hold(measured, False)
    assert stats.telemetry_errors == 1
    disabled = memory_governor_module._ResourceObserver(
        memory_governor_module.GovernorResourcePolicy(soft_ram_hold=False), None,
        memory_governor_module.ResourceStats(),
    )
    assert not disabled.hold(measured, True)
    assert disabled.stats.hold_events == 0


@MULTIPROCESS
def test_tiny_own_ram_target_holds_next_launch_but_always_admits_idle(tmp_path) -> None:
    governor = _governor(requested_workers=3)
    rows = deque(maxlen=8)
    policy = memory_governor_module.GovernorResourcePolicy(
        soft_ram_target_fraction=1e-12, sample_interval_seconds=0.01, disk_path=str(tmp_path),
    )
    outcomes = list(governor.iter_completed(
        _short_durable_marker,
        [MemoryTask(key=f"S{i}", args=(str(tmp_path), f"S{i}")) for i in range(3)],
        admission_gate=lambda: True, resource_policy=policy, telemetry_callback=rows.append,
    ))
    assert [item.status for item in outcomes] == ["COMPLETED"] * 3
    assert governor.stats.submitted == 3
    assert governor.stats.peak_inflight == 1
    assert governor.resource_stats.hold_events >= 2
    assert governor.resource_stats.hold_seconds > 0
    assert governor.resource_stats.peak_worker_rss_bytes > 0
    assert any(row["role"] == "worker" and row["rss_bytes"] is not None
               for sample in rows for row in sample["processes"])
    if os.name == "nt":
        assert any(row["role"] == "worker" and row["private_commit_bytes"] > 0
                   for sample in rows for row in sample["processes"] if row["private_commit_bytes"] is not None)
    assert all((tmp_path / f"S{i}.finished").is_file() for i in range(3))
