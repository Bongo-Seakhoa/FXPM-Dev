"""Single-use process scheduling for FXPM jobs.

Each task runs in a fresh spawned OS process.  There is no reusable worker
pool, so a symbol can never inherit another symbol's Python or native caches.
At most ``requested_workers`` tasks plus one pending iterator item are held.

The worker count is the only concurrency bound.  A non-positive requested
count means all logical CPUs reported by the OS.  The scheduler neither
estimates task memory nor reserves RAM, polls free RAM, delays admission under
memory pressure, or rejects a task because of a memory budget.  The operating
system remains responsible for physical and virtual memory allocation.

There is deliberately no elapsed-time ceiling for queued, starting, or active
work.  A production run continues until every task reaches a terminal result
or its enclosing operator-controlled process is stopped.  Child exceptions,
crashes, allocation failures, and process-start failures are returned as
explicit technical outcomes rather than trading verdicts.
"""

from __future__ import annotations

import ctypes
from dataclasses import dataclass, field
import math
import multiprocessing
import os
import platform
import time
import traceback
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Tuple


class _ProcessMemoryCounters(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("PageFaultCount", ctypes.c_ulong),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


def _windows_process_memory() -> Tuple[int, int]:
    counters = _ProcessMemoryCounters()
    counters.cb = ctypes.sizeof(_ProcessMemoryCounters)
    process = ctypes.windll.kernel32.GetCurrentProcess()
    if not ctypes.windll.psapi.GetProcessMemoryInfo(
        process, ctypes.byref(counters), counters.cb
    ):
        raise OSError("GetProcessMemoryInfo failed")
    return int(counters.WorkingSetSize), int(counters.PeakWorkingSetSize)


def _process_memory_bytes() -> Tuple[int, int]:
    """Return ``(current, lifetime_peak)`` RSS with safe fallbacks."""
    current = 0
    try:
        import psutil  # type: ignore

        info = psutil.Process(os.getpid()).memory_info()
        current = int(getattr(info, "rss", 0) or 0)
        peak = max(
            current,
            int(getattr(info, "peak_wset", 0) or 0),
            int(getattr(info, "peak_rss", 0) or 0),
        )
        if peak > current:
            return current, peak
    except Exception:
        pass
    if os.name == "nt":
        try:
            return _windows_process_memory()
        except Exception:
            return 0, 0
    try:
        import resource

        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        peak = value if platform.system() == "Darwin" else value * 1024
        return current or peak, max(current, peak)
    except Exception:
        return current, current


def current_process_rss_bytes() -> int:
    return _process_memory_bytes()[0]


def peak_process_rss_bytes() -> int:
    return _process_memory_bytes()[1]


@dataclass(frozen=True)
class MemoryTask:
    """One complete, accuracy-preserving unit of child-process work."""

    key: str
    args: Tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.key):
            raise ValueError("task key must not be empty")
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "kwargs", dict(self.kwargs))


def resolve_worker_count(
    requested_workers: int,
    task_count: Optional[int] = None,
) -> int:
    """Resolve auto concurrency against OS logical CPUs and finite task work."""

    try:
        requested = int(requested_workers)
    except (TypeError, ValueError) as exc:
        raise ValueError("requested_workers must be an integer") from exc
    workers = requested if requested > 0 else max(1, int(os.cpu_count() or 1))
    if task_count is None:
        return workers
    try:
        count = int(task_count)
    except (TypeError, ValueError) as exc:
        raise ValueError("task_count must be a non-negative integer") from exc
    if count < 0:
        raise ValueError("task_count must be a non-negative integer")
    return min(workers, count)


@dataclass(frozen=True)
class WorkerStarted:
    task_key: str
    pid: int


@dataclass(frozen=True)
class WorkerEnvelope:
    """Pickle-safe terminal payload emitted by a single-use child."""

    task_key: str
    pid: int
    value: Any
    error_type: str
    error_message: str
    traceback_text: str
    peak_rss_bytes: int
    duration_seconds: float


@dataclass(frozen=True)
class TaskOutcome:
    """Terminal scheduler verdict for one task."""

    task_key: str
    status: str
    value: Any = None
    error_type: str = ""
    error_message: str = ""
    traceback_text: str = ""
    technical_failure: bool = False
    memory_failure: bool = False
    possible_child_oom: bool = False
    measured_peak_bytes: int = 0
    worker_pid: int = 0
    duration_seconds: float = 0.0


def _failure(
    task_key: str,
    status: str,
    error_type: str,
    message: str,
    *,
    value: Any = None,
    memory: bool = False,
    possible_oom: bool = False,
    measured: int = 0,
    pid: int = 0,
    duration: float = 0.0,
    traceback_text: str = "",
) -> TaskOutcome:
    return TaskOutcome(
        task_key=task_key,
        status=status,
        value=value,
        error_type=error_type,
        error_message=message,
        traceback_text=traceback_text,
        technical_failure=True,
        memory_failure=memory,
        possible_child_oom=possible_oom,
        measured_peak_bytes=measured,
        worker_pid=pid,
        duration_seconds=duration,
    )


_MEMORY_ERROR_MARKERS = (
    "memoryerror",
    "arraymemoryerror",
    "unable to allocate",
    "cannot allocate memory",
    "out of memory",
    "outofmemory",
    "std::bad_alloc",
    "bad allocation",
    "paging file is too small",
    "pagefile is too small",
    "winerror 1455",
    "errno 12",
)


def is_memory_failure(value: Any) -> bool:
    """Recognize Python, NumPy, OS, and Windows allocation failures."""

    if isinstance(value, MemoryError):
        return True
    if isinstance(value, OSError):
        if getattr(value, "errno", None) == 12 or getattr(value, "winerror", None) == 1455:
            return True
    text = str(value or "").strip().lower()
    return any(marker in text for marker in _MEMORY_ERROR_MARKERS)


def _measured_worker_call(worker: Callable[..., Any], task: MemoryTask) -> WorkerEnvelope:
    started = time.monotonic()
    current, peak = _process_memory_bytes()
    peak = max(current, peak)
    value: Any = None
    error_type = ""
    error_message = ""
    traceback_text = ""
    try:
        value = worker(*task.args, **dict(task.kwargs))
    except BaseException as exc:
        error_type = f"{type(exc).__module__}.{type(exc).__name__}"
        error_message = str(exc)
        traceback_text = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )[-12000:]
    current, lifetime_peak = _process_memory_bytes()
    peak = max(peak, current, lifetime_peak)
    return WorkerEnvelope(
        task_key=task.key,
        pid=os.getpid(),
        value=value,
        error_type=error_type,
        error_message=error_message,
        traceback_text=traceback_text,
        peak_rss_bytes=int(peak),
        duration_seconds=max(0.0, time.monotonic() - started),
    )


def _worker_process_entry(
    send_connection: Any,
    worker: Callable[..., Any],
    task: MemoryTask,
) -> None:
    try:
        send_connection.send(WorkerStarted(task.key, os.getpid()))
    except BaseException:
        send_connection.close()
        return
    envelope = _measured_worker_call(worker, task)
    try:
        send_connection.send(envelope)
    except BaseException as exc:
        fallback = WorkerEnvelope(
            task_key=task.key,
            pid=os.getpid(),
            value=None,
            error_type="ResultSerializationError",
            error_message=f"{type(exc).__name__}: {exc}",
            traceback_text="",
            peak_rss_bytes=envelope.peak_rss_bytes,
            duration_seconds=envelope.duration_seconds,
        )
        try:
            send_connection.send(fallback)
        except BaseException:
            pass
    finally:
        try:
            send_connection.close()
        except Exception:
            pass


def _launch_task_process(
    context: Any,
    worker: Callable[..., Any],
    task: MemoryTask,
) -> Tuple[Any, Any]:
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_worker_process_entry,
        args=(send_connection, worker, task),
        name=f"fxpm-{task.key[:40]}",
    )
    # A task may use native threads but may not create a nested multiprocessing
    # tree that would escape this scheduler's process cleanup.
    process.daemon = True
    try:
        process.start()
    except BaseException:
        receive_connection.close()
        send_connection.close()
        raise
    try:
        send_connection.close()
    except Exception:
        pass
    return process, receive_connection


def _stop_process(process: Any, grace_seconds: float) -> bool:
    """Bounded terminate/kill sequence; return whether the child is gone."""

    try:
        if not process.is_alive():
            process.join(timeout=0)
            return True
        process.terminate()
        process.join(timeout=grace_seconds)
        if process.is_alive() and hasattr(process, "kill"):
            process.kill()
            process.join(timeout=grace_seconds)
        return not process.is_alive()
    except Exception:
        return False


def _close_process(process: Any) -> None:
    try:
        if not process.is_alive():
            process.join(timeout=0)
            process.close()
    except Exception:
        pass


@dataclass
class _RunningTask:
    task: MemoryTask
    process: Any
    connection: Any
    started_at: float
    execution_started_at: Optional[float] = None
    envelope: Optional[WorkerEnvelope] = None
    receive_error: str = ""


def _receive_envelope(running: _RunningTask, timeout: float = 0.0) -> None:
    if running.envelope is not None or running.connection is None:
        return
    try:
        if not running.connection.poll(timeout):
            return
        while True:
            value = running.connection.recv()
            if isinstance(value, WorkerStarted):
                if value.task_key != running.task.key or value.pid != running.process.pid:
                    raise ValueError("worker-start identity mismatch")
                if running.execution_started_at is not None:
                    raise ValueError("duplicate worker-start message")
                running.execution_started_at = time.monotonic()
            elif isinstance(value, WorkerEnvelope):
                running.envelope = value
                running.connection.close()
                running.connection = None
                return
            else:
                raise TypeError(f"unexpected worker message {type(value).__name__}")
            if not running.connection.poll():
                return
    except Exception as exc:
        running.receive_error = f"{type(exc).__name__}: {exc}"


@dataclass
class GovernorStats:
    """Constant-size scheduler counters; outcomes stay with the caller."""

    submitted: int = 0
    completed: int = 0
    technical_failures: int = 0
    memory_failures: int = 0
    process_start_failures: int = 0
    peak_inflight: int = 0


class MemoryGovernor:
    """Run each task in a fresh process with OS-managed memory."""

    def __init__(
        self,
        *,
        requested_workers: int,
        poll_interval_seconds: float = 0.05,
        termination_grace_seconds: float = 1.0,
        process_context: Any = None,
    ):
        for name, value in (
            ("poll_interval_seconds", poll_interval_seconds),
            ("termination_grace_seconds", termination_grace_seconds),
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        self.requested_workers = resolve_worker_count(requested_workers)
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.termination_grace_seconds = float(termination_grace_seconds)
        self.process_context = process_context or multiprocessing.get_context("spawn")
        self.stats = GovernorStats()

    def _record(self, outcome: TaskOutcome) -> None:
        self.stats.completed += 1
        if outcome.technical_failure:
            self.stats.technical_failures += 1
        if outcome.memory_failure or outcome.possible_child_oom:
            self.stats.memory_failures += 1
        if outcome.status == "FAILED_PROCESS_START":
            self.stats.process_start_failures += 1

    def _from_envelope(self, running: _RunningTask) -> TaskOutcome:
        envelope = running.envelope
        if envelope is None:
            raise RuntimeError("missing worker envelope")
        if envelope.task_key != running.task.key or envelope.pid != running.process.pid:
            return _failure(
                running.task.key, "FAILED_TECHNICAL", "WorkerProtocolError",
                "worker envelope identity does not match the launched task",
                measured=envelope.peak_rss_bytes,
                pid=envelope.pid, duration=envelope.duration_seconds,
            )
        if envelope.error_type:
            memory_failure = is_memory_failure(envelope.error_type) or is_memory_failure(
                envelope.error_message
            )
            return _failure(
                envelope.task_key,
                "FAILED_MEMORY" if memory_failure else "FAILED_TECHNICAL",
                envelope.error_type, envelope.error_message,
                memory=memory_failure, measured=envelope.peak_rss_bytes,
                pid=envelope.pid, duration=envelope.duration_seconds,
                traceback_text=envelope.traceback_text,
            )
        return TaskOutcome(
            task_key=envelope.task_key,
            status="COMPLETED",
            value=envelope.value,
            measured_peak_bytes=envelope.peak_rss_bytes,
            worker_pid=envelope.pid,
            duration_seconds=envelope.duration_seconds,
        )

    def iter_completed(
        self,
        worker: Callable[..., Any],
        tasks: Iterable[MemoryTask],
    ) -> Iterator[TaskOutcome]:
        """Yield outcomes while retaining at most workers + one task."""

        task_iter = iter(tasks)
        pending: Optional[MemoryTask] = None
        exhausted = False
        active: dict[int, _RunningTask] = {}
        sequence = 0
        orphaned: list[Any] = []
        fatal_termination_failure = False

        def load_pending() -> None:
            nonlocal pending, exhausted
            if pending is not None or exhausted:
                return
            try:
                pending = next(task_iter)
            except StopIteration:
                exhausted = True

        def remove_running(token: int, running: _RunningTask) -> None:
            active.pop(token, None)
            if running.connection is not None:
                try:
                    running.connection.close()
                except Exception:
                    pass
                running.connection = None
            _close_process(running.process)

        try:
            load_pending()
            while pending is not None or active:
                progressed = False
                now = time.monotonic()
                for token, running in list(active.items()):
                    _receive_envelope(running)
                    alive = running.process.is_alive()
                    if (
                        not alive
                        and running.envelope is None
                        and not running.receive_error
                        and running.connection is not None
                    ):
                        _receive_envelope(running, 0.05)
                        if running.envelope is None and not running.receive_error:
                            running.receive_error = "child exited without a terminal envelope"

                    outcome: Optional[TaskOutcome] = None
                    if running.receive_error:
                        stopped = _stop_process(
                            running.process, self.termination_grace_seconds
                        )
                        if not stopped:
                            orphaned.append(running.process)
                            fatal_termination_failure = True
                        outcome = _failure(
                            running.task.key, "FAILED_CHILD_OOM_OR_CRASH",
                            "WorkerProtocolError", running.receive_error,
                            possible_oom=True,
                            pid=int(running.process.pid or 0),
                            duration=max(0.0, now - running.started_at),
                        )
                    elif running.envelope is not None and not alive:
                        outcome = self._from_envelope(running)

                    if outcome is not None:
                        remove_running(token, running)
                        self._record(outcome)
                        progressed = True
                        yield outcome

                if fatal_termination_failure:
                    for token, running in list(active.items()):
                        stopped = _stop_process(
                            running.process, self.termination_grace_seconds
                        )
                        outcome = _failure(
                            running.task.key, "ABORTED_SCHEDULER_FATAL",
                            "ProcessTerminationError",
                            (
                                "another failed child could not be terminated; "
                                f"this child termination {'succeeded' if stopped else 'also failed'}"
                            ),
                            pid=int(running.process.pid or 0),
                        )
                        if not stopped:
                            orphaned.append(running.process)
                        remove_running(token, running)
                        self._record(outcome)
                        yield outcome
                    load_pending()
                    while pending is not None:
                        outcome = _failure(
                            pending.key, "NOT_RUN_SCHEDULER_FATAL",
                            "ProcessTerminationError",
                            "not started because a failed child could not be terminated",
                        )
                        self._record(outcome)
                        yield outcome
                        pending = None
                        load_pending()
                    break

                load_pending()
                while pending is not None and len(active) < self.requested_workers:
                    started_at = time.monotonic()
                    try:
                        process, connection = _launch_task_process(
                            self.process_context, worker, pending
                        )
                    except Exception as exc:
                        outcome = _failure(
                            pending.key, "FAILED_PROCESS_START",
                            type(exc).__name__, str(exc),
                        )
                        self._record(outcome)
                        yield outcome
                        pending = None
                        progressed = True
                        load_pending()
                        continue

                    sequence += 1
                    running = _RunningTask(
                        task=pending,
                        process=process,
                        connection=connection,
                        started_at=started_at,
                    )
                    active[sequence] = running
                    self.stats.submitted += 1
                    self.stats.peak_inflight = max(
                        self.stats.peak_inflight, len(active)
                    )
                    pending = None
                    progressed = True
                    load_pending()

                if not progressed:
                    time.sleep(self.poll_interval_seconds)
        finally:
            for running in list(active.values()):
                _stop_process(running.process, self.termination_grace_seconds)
                if running.connection is not None:
                    try:
                        running.connection.close()
                    except Exception:
                        pass
                _close_process(running.process)
            for process in orphaned:
                _stop_process(process, self.termination_grace_seconds)
                _close_process(process)


def run_streamed_process_pool(
    worker: Callable[..., Any],
    tasks: Iterable[MemoryTask],
    *,
    requested_workers: int,
    **governor_kwargs: Any,
) -> Iterator[TaskOutcome]:
    """Compatibility entry point backed by single-use spawned processes."""

    governor = MemoryGovernor(
        requested_workers=requested_workers,
        **governor_kwargs,
    )
    yield from governor.iter_completed(worker, tasks)


__all__ = [
    "GovernorStats",
    "MemoryGovernor",
    "MemoryTask",
    "TaskOutcome",
    "WorkerEnvelope",
    "current_process_rss_bytes",
    "is_memory_failure",
    "peak_process_rss_bytes",
    "resolve_worker_count",
    "run_streamed_process_pool",
]
