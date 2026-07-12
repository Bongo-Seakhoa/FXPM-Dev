"""Fail-closed, bounded-memory process scheduling for FXPM jobs.

Each task runs in a fresh spawned OS process.  There is no reusable worker
pool, so a symbol can never inherit another symbol's Python or native caches.
At most ``requested_workers`` tasks plus one pending iterator item are held.

Callers reserve the complete host peak for each task, including child RSS, IPC
serialization, and parent-side result materialization.  Results should be
compact control records; large data artifacts stay child-side on bounded
storage.  Memory admission is conservative: current child RSS is never added back
to available memory.  This prevents idle or unrelated child processes from
being mistaken for reusable capacity.  The scheduler controls admission only;
it never truncates data, features, evidence windows, or search work.

Every started task has a finite deadline.  A timed-out child is terminated and
then killed if necessary.  Child exceptions, crashes, timeouts, admission
failures, and process-start failures are returned as explicit technical
outcomes rather than trading verdicts.
"""

from __future__ import annotations

from collections import deque
import ctypes
from dataclasses import dataclass, field
import math
import multiprocessing
import os
import platform
import time
import traceback
from typing import Any, Callable, Deque, Iterable, Iterator, Mapping, Optional, Tuple


@dataclass(frozen=True)
class MemorySnapshot:
    """Physical-memory measurements in bytes."""

    total_bytes: int
    available_bytes: int
    process_rss_bytes: int

    def __post_init__(self) -> None:
        for name in ("total_bytes", "available_bytes", "process_rss_bytes"):
            if int(getattr(self, name)) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.total_bytes <= 0:
            raise ValueError("total_bytes must be positive")
        if self.available_bytes > self.total_bytes:
            raise ValueError("available_bytes cannot exceed total_bytes")


MemoryProvider = Callable[[], MemorySnapshot]


class _MemoryStatusEx(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


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


def _windows_physical_memory() -> Tuple[int, int]:
    status = _MemoryStatusEx()
    status.dwLength = ctypes.sizeof(_MemoryStatusEx)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
        raise OSError("GlobalMemoryStatusEx failed")
    return int(status.ullTotalPhys), int(status.ullAvailPhys)


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


def system_memory_snapshot() -> MemorySnapshot:
    """Capture host physical memory and this scheduler process's RSS."""

    try:
        import psutil  # type: ignore

        vm = psutil.virtual_memory()
        rss = int(psutil.Process(os.getpid()).memory_info().rss)
        return MemorySnapshot(int(vm.total), int(vm.available), rss)
    except Exception:
        pass
    if os.name == "nt":
        total, available = _windows_physical_memory()
    else:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        total = page_size * int(os.sysconf("SC_PHYS_PAGES"))
        available = page_size * int(os.sysconf("SC_AVPHYS_PAGES"))
    return MemorySnapshot(total, min(total, available), current_process_rss_bytes())


@dataclass(frozen=True)
class MemoryTask:
    """One complete, accuracy-preserving unit of child-process work."""

    key: str
    args: Tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    estimated_peak_bytes: int = 0

    def __post_init__(self) -> None:
        if not str(self.key):
            raise ValueError("task key must not be empty")
        if int(self.estimated_peak_bytes) <= 0:
            raise ValueError("estimated_peak_bytes must be positive")
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "kwargs", dict(self.kwargs))


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
    estimated_peak_bytes: int = 0
    measured_peak_bytes: int = 0
    worker_pid: int = 0
    duration_seconds: float = 0.0


def _failure(
    task_key: str,
    status: str,
    error_type: str,
    message: str,
    estimate: int,
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
        estimated_peak_bytes=estimate,
        measured_peak_bytes=measured,
        worker_pid=pid,
        duration_seconds=duration,
    )


class PeakMemoryEstimator:
    """Raise future estimates when bounded observed RSS ratios require it."""

    def __init__(self, max_samples: int = 32, safety_multiplier: float = 1.10):
        if max_samples <= 0:
            raise ValueError("max_samples must be positive")
        if not math.isfinite(safety_multiplier) or safety_multiplier < 1.0:
            raise ValueError("safety_multiplier must be finite and >= 1")
        self._ratios: Deque[float] = deque(maxlen=int(max_samples))
        self.safety_multiplier = float(safety_multiplier)

    @property
    def sample_count(self) -> int:
        return len(self._ratios)

    def observe(self, estimated_bytes: int, measured_peak_bytes: int) -> None:
        if estimated_bytes <= 0 or measured_peak_bytes <= 0:
            return
        ratio = float(measured_peak_bytes) / float(estimated_bytes)
        if math.isfinite(ratio) and ratio > 0:
            self._ratios.append(ratio)

    def adjusted_bytes(self, task_estimate_bytes: int) -> int:
        base = int(task_estimate_bytes)
        if base <= 0:
            raise ValueError("task_estimate_bytes must be positive")
        if not self._ratios:
            return base
        ratio = max(1.0, max(self._ratios) * self.safety_multiplier)
        return int(math.ceil(base * ratio))


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
    # tree that would escape this process's deadline and memory reservation.
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
    estimate: int
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
    admission_failures: int = 0
    timeout_failures: int = 0
    process_start_failures: int = 0
    peak_inflight: int = 0


class MemoryGovernor:
    """Run complete tasks in fresh processes under a hard admission reserve."""

    def __init__(
        self,
        *,
        requested_workers: int,
        reserve_bytes: int,
        memory_provider: MemoryProvider = system_memory_snapshot,
        task_timeout_seconds: float = 4 * 60 * 60,
        worker_start_timeout_seconds: float = 120.0,
        admission_timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 0.05,
        termination_grace_seconds: float = 1.0,
        estimator_max_samples: int = 32,
        estimator_safety_multiplier: float = 1.10,
        process_context: Any = None,
    ):
        if requested_workers <= 0:
            raise ValueError("requested_workers must be positive")
        if reserve_bytes < 0:
            raise ValueError("reserve_bytes must be non-negative")
        for name, value, allow_zero in (
            ("task_timeout_seconds", task_timeout_seconds, False),
            ("worker_start_timeout_seconds", worker_start_timeout_seconds, False),
            ("admission_timeout_seconds", admission_timeout_seconds, True),
            ("poll_interval_seconds", poll_interval_seconds, False),
            ("termination_grace_seconds", termination_grace_seconds, False),
        ):
            if not math.isfinite(value) or value < 0 or (not allow_zero and value == 0):
                qualifier = "non-negative" if allow_zero else "positive"
                raise ValueError(f"{name} must be finite and {qualifier}")
        self.requested_workers = int(requested_workers)
        self.reserve_bytes = int(reserve_bytes)
        self.memory_provider = memory_provider
        self.task_timeout_seconds = float(task_timeout_seconds)
        self.worker_start_timeout_seconds = float(worker_start_timeout_seconds)
        self.admission_timeout_seconds = float(admission_timeout_seconds)
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.termination_grace_seconds = float(termination_grace_seconds)
        self.process_context = process_context or multiprocessing.get_context("spawn")
        self.estimator = PeakMemoryEstimator(
            max_samples=estimator_max_samples,
            safety_multiplier=estimator_safety_multiplier,
        )
        self.stats = GovernorStats()

    def _static_capacity_bytes(self, snapshot: MemorySnapshot) -> int:
        return max(
            0,
            int(snapshot.total_bytes)
            - self.reserve_bytes
            - int(snapshot.process_rss_bytes),
        )

    def _capacity_bytes(self, snapshot: MemorySnapshot) -> int:
        static_capacity = self._static_capacity_bytes(snapshot)
        # Deliberately do not add child RSS back.  Available physical memory is
        # the only dynamic credit, so idle/unrelated children can never inflate
        # the scheduler's budget.
        dynamic_capacity = max(0, int(snapshot.available_bytes) - self.reserve_bytes)
        return min(static_capacity, dynamic_capacity)

    def _can_admit(
        self,
        estimate_bytes: int,
        reservations: Mapping[int, int],
    ) -> Tuple[bool, int, MemorySnapshot]:
        snapshot = self.memory_provider()
        capacity = self._capacity_bytes(snapshot)
        reserved = sum(int(value) for value in reservations.values())
        return reserved + int(estimate_bytes) <= capacity, capacity, snapshot

    def _record(self, outcome: TaskOutcome) -> None:
        self.stats.completed += 1
        if outcome.technical_failure:
            self.stats.technical_failures += 1
        if outcome.memory_failure or outcome.possible_child_oom:
            self.stats.memory_failures += 1
        if outcome.status == "FAILED_MEMORY_BUDGET":
            self.stats.admission_failures += 1
        if outcome.status.startswith("FAILED_TIMEOUT"):
            self.stats.timeout_failures += 1
        if outcome.status == "FAILED_PROCESS_START":
            self.stats.process_start_failures += 1

    def _from_envelope(self, running: _RunningTask) -> TaskOutcome:
        envelope = running.envelope
        if envelope is None:
            raise RuntimeError("missing worker envelope")
        self.estimator.observe(running.estimate, envelope.peak_rss_bytes)
        if envelope.task_key != running.task.key or envelope.pid != running.process.pid:
            return _failure(
                running.task.key, "FAILED_TECHNICAL", "WorkerProtocolError",
                "worker envelope identity does not match the launched task",
                running.estimate, measured=envelope.peak_rss_bytes,
                pid=envelope.pid, duration=envelope.duration_seconds,
            )
        if envelope.error_type:
            memory_failure = is_memory_failure(envelope.error_type) or is_memory_failure(
                envelope.error_message
            )
            return _failure(
                envelope.task_key,
                "FAILED_MEMORY" if memory_failure else "FAILED_TECHNICAL",
                envelope.error_type, envelope.error_message, running.estimate,
                memory=memory_failure, measured=envelope.peak_rss_bytes,
                pid=envelope.pid, duration=envelope.duration_seconds,
                traceback_text=envelope.traceback_text,
            )
        return TaskOutcome(
            task_key=envelope.task_key,
            status="COMPLETED",
            value=envelope.value,
            estimated_peak_bytes=running.estimate,
            measured_peak_bytes=envelope.peak_rss_bytes,
            worker_pid=envelope.pid,
            duration_seconds=envelope.duration_seconds,
        )

    def _admission_failure(
        self,
        task: MemoryTask,
        estimate: int,
        capacity: int,
        snapshot: MemorySnapshot,
    ) -> TaskOutcome:
        return _failure(
            task.key, "FAILED_MEMORY_BUDGET", "MemoryAdmissionError",
            (
                f"memory admission denied for {task.key}: estimated host reservation "
                f"{estimate} B exceeds managed capacity {capacity} B "
                f"(available={snapshot.available_bytes} B, reserve={self.reserve_bytes} B)"
            ),
            estimate, memory=True,
        )

    def iter_completed(
        self,
        worker: Callable[..., Any],
        tasks: Iterable[MemoryTask],
    ) -> Iterator[TaskOutcome]:
        """Yield terminal outcomes while retaining at most workers + one task."""

        task_iter = iter(tasks)
        pending: Optional[MemoryTask] = None
        pending_since = 0.0
        exhausted = False
        active: dict[int, _RunningTask] = {}
        reservations: dict[int, int] = {}
        sequence = 0
        orphaned: list[Any] = []
        fatal_termination_failure = False

        def load_pending() -> None:
            nonlocal pending, pending_since, exhausted
            if pending is not None or exhausted:
                return
            try:
                pending = next(task_iter)
                pending_since = time.monotonic()
            except StopIteration:
                exhausted = True

        def remove_running(token: int, running: _RunningTask) -> None:
            active.pop(token, None)
            reservations.pop(token, None)
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
                    start_timed_out = (
                        running.execution_started_at is None
                        and now - running.started_at >= self.worker_start_timeout_seconds
                    )
                    task_timed_out = (
                        running.execution_started_at is not None
                        and now - running.execution_started_at >= self.task_timeout_seconds
                    )
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
                            running.estimate, possible_oom=True,
                            pid=int(running.process.pid or 0),
                            duration=max(0.0, now - running.started_at),
                        )
                    elif running.envelope is not None and not alive:
                        outcome = self._from_envelope(running)
                    elif start_timed_out or task_timed_out:
                        stopped = _stop_process(
                            running.process, self.termination_grace_seconds
                        )
                        if not stopped:
                            orphaned.append(running.process)
                            fatal_termination_failure = True
                        outcome = _failure(
                            running.task.key,
                            (
                                "FAILED_TIMEOUT_START"
                                if stopped and start_timed_out
                                else "FAILED_TIMEOUT"
                                if stopped
                                else "FAILED_TIMEOUT_TERMINATION"
                            ),
                            "WorkerStartTimeoutError" if start_timed_out else "TaskTimeoutError",
                            (
                                f"{'worker start' if start_timed_out else 'task'} exceeded "
                                f"{self.worker_start_timeout_seconds if start_timed_out else self.task_timeout_seconds:.3f} seconds; "
                                f"child termination {'succeeded' if stopped else 'failed'}"
                            ),
                            running.estimate,
                            measured=(
                                running.envelope.peak_rss_bytes
                                if running.envelope is not None
                                else 0
                            ),
                            pid=int(running.process.pid or 0),
                            duration=max(
                                0.0,
                                now
                                - (
                                    running.started_at
                                    if start_timed_out
                                    else running.execution_started_at or running.started_at
                                ),
                            ),
                        )

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
                                "another timed-out child could not be terminated; "
                                f"this child termination {'succeeded' if stopped else 'also failed'}"
                            ),
                            running.estimate, pid=int(running.process.pid or 0),
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
                            "not started because a timed-out child could not be terminated",
                            self.estimator.adjusted_bytes(pending.estimated_peak_bytes),
                        )
                        self._record(outcome)
                        yield outcome
                        pending = None
                        load_pending()
                    break

                load_pending()
                while pending is not None and len(active) < self.requested_workers:
                    estimate = self.estimator.adjusted_bytes(pending.estimated_peak_bytes)
                    admitted, capacity, snapshot = self._can_admit(estimate, reservations)
                    if not admitted:
                        if active:
                            break
                        impossible = estimate > self._static_capacity_bytes(snapshot)
                        expired = (
                            time.monotonic() - pending_since
                            >= self.admission_timeout_seconds
                        )
                        if not impossible and not expired:
                            break
                        outcome = self._admission_failure(
                            pending, estimate, capacity, snapshot
                        )
                        self._record(outcome)
                        yield outcome
                        pending = None
                        progressed = True
                        load_pending()
                        continue

                    started_at = time.monotonic()
                    try:
                        process, connection = _launch_task_process(
                            self.process_context, worker, pending
                        )
                    except Exception as exc:
                        outcome = _failure(
                            pending.key, "FAILED_PROCESS_START",
                            type(exc).__name__, str(exc), estimate,
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
                        estimate=estimate,
                        process=process,
                        connection=connection,
                        started_at=started_at,
                    )
                    active[sequence] = running
                    reservations[sequence] = estimate
                    self.stats.submitted += 1
                    self.stats.peak_inflight = max(
                        self.stats.peak_inflight, len(active)
                    )
                    pending = None
                    progressed = True
                    load_pending()

                if not progressed:
                    delay = self.poll_interval_seconds
                    if active:
                        clock = time.monotonic()
                        remaining = min(
                            (
                                self.worker_start_timeout_seconds
                                - (clock - running.started_at)
                                if running.execution_started_at is None
                                else self.task_timeout_seconds
                                - (clock - running.execution_started_at)
                            )
                            for running in active.values()
                        )
                        delay = min(delay, max(0.001, remaining))
                    elif pending is not None:
                        remaining = self.admission_timeout_seconds - (
                            time.monotonic() - pending_since
                        )
                        delay = min(delay, max(0.001, remaining))
                    time.sleep(max(0.001, delay))
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
    reserve_bytes: int,
    **governor_kwargs: Any,
) -> Iterator[TaskOutcome]:
    """Compatibility entry point backed by single-use spawned processes."""

    governor = MemoryGovernor(
        requested_workers=requested_workers,
        reserve_bytes=reserve_bytes,
        **governor_kwargs,
    )
    yield from governor.iter_completed(worker, tasks)


__all__ = [
    "GovernorStats",
    "MemoryGovernor",
    "MemorySnapshot",
    "MemoryTask",
    "PeakMemoryEstimator",
    "TaskOutcome",
    "WorkerEnvelope",
    "current_process_rss_bytes",
    "is_memory_failure",
    "peak_process_rss_bytes",
    "run_streamed_process_pool",
    "system_memory_snapshot",
]
