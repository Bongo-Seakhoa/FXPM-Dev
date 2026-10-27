"""Single-use process scheduling for FXPM jobs.

Each task runs in a fresh spawned OS process.  There is no reusable worker
pool, so a symbol can never inherit another symbol's Python or native caches.
At most ``requested_workers`` tasks plus one pending iterator item are held.

In the legacy API the worker count is the only concurrency bound.  A non-positive requested
count means all logical CPUs reported by the OS.  The scheduler neither
estimates task memory nor reserves RAM, polls free RAM, delays admission under
memory pressure, or rejects a task because of a memory budget.  The operating
system remains responsible for physical and virtual memory allocation.

The declared scheduler can opt into an admission gate, measured own-RSS soft
hold and synchronous telemetry through keyword arguments to ``iter_completed``.
Those extensions do not change the legacy two-argument scheduling contract.

There is deliberately no elapsed-time ceiling for queued, starting, or active
work.  A production run continues until every task reaches a terminal result
or its enclosing operator-controlled process is stopped.  Child exceptions,
crashes, allocation failures, and process-start failures are returned as
explicit technical outcomes rather than trading verdicts.
"""

from __future__ import annotations

import ctypes
import copy
from dataclasses import dataclass, field
import math
import multiprocessing
import os
import platform
import shutil
import time
import traceback
from datetime import datetime, timezone
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


def _windows_affinity_cpu_count() -> int:
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.argtypes = []
    kernel.GetCurrentProcess.restype = ctypes.c_void_p
    kernel.GetProcessAffinityMask.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(ctypes.c_size_t),
    ]
    kernel.GetProcessAffinityMask.restype = ctypes.c_int
    process_mask, system_mask = ctypes.c_size_t(), ctypes.c_size_t()
    if not kernel.GetProcessAffinityMask(
        kernel.GetCurrentProcess(), ctypes.byref(process_mask), ctypes.byref(system_mask)
    ) or not process_mask.value:
        raise OSError("GetProcessAffinityMask failed")
    return int(process_mask.value).bit_count()


def effective_cpu_count() -> int:
    """Read process affinity; do not change affinity or native thread settings."""
    try:
        if os.name == "nt":
            return _windows_affinity_cpu_count()
        return max(1, len(os.sched_getaffinity(0)))
    except (AttributeError, OSError, ValueError):
        return max(1, int(os.cpu_count() or 1))


def recommended_worker_count(
    requested_workers: int, task_count: int, *, cpu_target_fraction: float = 0.8,
) -> int:
    """Declared-entry recommendation; explicit positive requests remain explicit."""
    if type(requested_workers) is not int or type(task_count) is not int or task_count < 0:
        raise ValueError("requested_workers/task_count must be integers; task_count >= 0")
    if (
        type(cpu_target_fraction) not in {int, float}
        or not math.isfinite(cpu_target_fraction) or not 0 < cpu_target_fraction <= 1
    ):
        raise ValueError("cpu_target_fraction must be finite in (0, 1]")
    workers = requested_workers if requested_workers > 0 else max(
        1, math.ceil(cpu_target_fraction * effective_cpu_count())
    )
    return min(workers, task_count)


@dataclass(frozen=True, kw_only=True)
class GovernorResourcePolicy:
    """Opt-in measured soft target, never a forecast or allocation ceiling."""
    soft_ram_hold: bool = True
    soft_ram_target_fraction: float = 0.8
    sample_interval_seconds: float = 1.0
    disk_path: Optional[str] = None

    def __post_init__(self) -> None:
        if type(self.soft_ram_hold) is not bool:
            raise ValueError("soft_ram_hold must be bool")
        for name, value in (
            ("soft_ram_target_fraction", self.soft_ram_target_fraction),
            ("sample_interval_seconds", self.sample_interval_seconds),
        ):
            if type(value) not in {int, float} or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.soft_ram_target_fraction > 1:
            raise ValueError("soft_ram_target_fraction must be <= 1")
        if self.disk_path is not None and (type(self.disk_path) is not str or not self.disk_path):
            raise ValueError("disk_path must be a nonempty path or None")


@dataclass
class ResourceStats:
    """Constant-size sampled aggregates, separate from legacy GovernorStats."""
    samples: int = 0
    measurement_errors: int = 0
    ram_measurement_fail_open: int = 0
    telemetry_errors: int = 0
    admission_gate_errors: int = 0
    not_run: int = 0
    hold_events: int = 0
    hold_seconds: float = 0.0
    incomplete_samples: int = 0
    peak_own_rss_bytes: Optional[int] = None
    peak_worker_rss_bytes: Optional[int] = None
    peak_own_private_commit_bytes: Optional[int] = None
    peak_cpu_percent: Optional[float] = None
    max_sample_gap_seconds: float = 0.0
    admission_stop_reason: str = ""


class _ProcessMemoryCountersEx(ctypes.Structure):
    _fields_ = _ProcessMemoryCounters._fields_ + [("PrivateUsage", ctypes.c_size_t)]


class _FileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]

    def ticks(self) -> int:
        return (int(self.high) << 32) | int(self.low)


class _MemoryStatusEx(ctypes.Structure):
    _fields_ = [("length", ctypes.c_uint32), ("load", ctypes.c_uint32)] + [
        (name, ctypes.c_uint64) for name in (
            "total_phys", "avail_phys", "total_pagefile", "avail_pagefile",
            "total_virtual", "avail_virtual", "avail_extended_virtual",
        )
    ]


class _PerformanceInformation(ctypes.Structure):
    _fields_ = [("cb", ctypes.c_uint32)] + [
        (name, ctypes.c_size_t) for name in (
            "commit_total", "commit_limit", "commit_peak", "physical_total",
            "physical_available", "system_cache", "kernel_total", "kernel_paged",
            "kernel_nonpaged", "page_size",
        )
    ] + [(name, ctypes.c_uint32) for name in ("handles", "processes", "threads")]


def _sample_process_resources(pid: int) -> dict[str, Any]:
    if os.name != "nt":
        import psutil  # type: ignore
        process = psutil.Process(pid)
        memory, cpu = process.memory_info(), process.cpu_times()
        return {
            "pid": pid, "identity": process.create_time(), "rss_bytes": int(memory.rss),
            "private_commit_bytes": getattr(memory, "private", None),
            "page_fault_count": getattr(memory, "num_page_faults", None),
            "cpu_seconds": float(cpu.user + cpu.system), "error_type": "",
        }
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    kernel.OpenProcess.restype = ctypes.c_void_p
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel.CloseHandle.restype = ctypes.c_int
    kernel.GetProcessTimes.argtypes = [ctypes.c_void_p] + [ctypes.POINTER(_FileTime)] * 4
    kernel.GetProcessTimes.restype = ctypes.c_int
    psapi.GetProcessMemoryInfo.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(_ProcessMemoryCountersEx), ctypes.c_uint32,
    ]
    psapi.GetProcessMemoryInfo.restype = ctypes.c_int
    handle = kernel.OpenProcess(0x1000 | 0x0010, False, pid)
    if not handle:
        raise OSError("OpenProcess resource query failed")
    try:
        memory = _ProcessMemoryCountersEx()
        memory.cb = ctypes.sizeof(memory)
        created, exited, system, user = (_FileTime() for _ in range(4))
        if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(memory), memory.cb):
            raise OSError("GetProcessMemoryInfo EX failed")
        if not kernel.GetProcessTimes(
            handle, ctypes.byref(created), ctypes.byref(exited),
            ctypes.byref(system), ctypes.byref(user),
        ):
            raise OSError("GetProcessTimes failed")
        return {
            "pid": pid, "identity": created.ticks(), "rss_bytes": int(memory.WorkingSetSize),
            "private_commit_bytes": int(memory.PrivateUsage),
            "page_fault_count": int(memory.PageFaultCount),
            "cpu_seconds": (system.ticks() + user.ticks()) / 10_000_000,
            "error_type": "",
        }
    finally:
        kernel.CloseHandle(handle)


def _sample_system_resources(disk_path: Optional[str]) -> dict[str, Any]:
    result = {
        "physical_ram_bytes": None, "available_ram_bytes": None,
        "system_commit_bytes": None, "system_commit_limit_bytes": None,
        "system_page_fault_count": None, "free_disk_bytes": None,
        "system_cpu_total_seconds": None, "system_cpu_idle_seconds": None,
        "error_count": 0,
    }
    try:
        if os.name == "nt":
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            kernel.GlobalMemoryStatusEx.argtypes = [ctypes.POINTER(_MemoryStatusEx)]
            kernel.GlobalMemoryStatusEx.restype = ctypes.c_int
            psapi.GetPerformanceInfo.argtypes = [ctypes.POINTER(_PerformanceInformation), ctypes.c_uint32]
            psapi.GetPerformanceInfo.restype = ctypes.c_int
            kernel.GetSystemTimes.argtypes = [ctypes.POINTER(_FileTime)] * 3
            kernel.GetSystemTimes.restype = ctypes.c_int
            memory, performance = _MemoryStatusEx(), _PerformanceInformation()
            memory.length, performance.cb = ctypes.sizeof(memory), ctypes.sizeof(performance)
            if not kernel.GlobalMemoryStatusEx(ctypes.byref(memory)):
                raise OSError("GlobalMemoryStatusEx failed")
            result.update(physical_ram_bytes=int(memory.total_phys), available_ram_bytes=int(memory.avail_phys))
            if psapi.GetPerformanceInfo(ctypes.byref(performance), performance.cb):
                result.update(
                    system_commit_bytes=int(performance.commit_total * performance.page_size),
                    system_commit_limit_bytes=int(performance.commit_limit * performance.page_size),
                )
            else:
                result["error_count"] += 1
            idle, system, user = (_FileTime() for _ in range(3))
            if kernel.GetSystemTimes(ctypes.byref(idle), ctypes.byref(system), ctypes.byref(user)):
                result.update(
                    system_cpu_total_seconds=(system.ticks() + user.ticks()) / 10_000_000,
                    system_cpu_idle_seconds=idle.ticks() / 10_000_000,
                )
            else:
                result["error_count"] += 1
        else:
            import psutil  # type: ignore
            memory, cpu = psutil.virtual_memory(), psutil.cpu_times()
            result.update(
                physical_ram_bytes=int(memory.total), available_ram_bytes=int(memory.available),
                system_cpu_total_seconds=sum(getattr(cpu, name, 0) for name in (
                    "user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal",
                )),
                system_cpu_idle_seconds=float(cpu.idle + getattr(cpu, "iowait", 0)),
            )
    except Exception:
        result["error_count"] += 1
    try:
        result["free_disk_bytes"] = shutil.disk_usage(disk_path or os.getcwd()).free
    except Exception:
        result["error_count"] += 1
    return result


class _ResourceObserver:
    def __init__(self, policy: GovernorResourcePolicy, callback: Any, stats: ResourceStats):
        self.policy, self.callback, self.stats = policy, callback, stats
        self.last_at: Optional[float] = None
        self.last: dict[str, Any] = {}
        self.previous: dict[int, dict[str, Any]] = {}
        self.previous_system: dict[str, Any] = {}
        self.hold_started: Optional[float] = None
        self.cpu_count = effective_cpu_count()

    def observe(self, worker_pids: Iterable[int], *, force: bool = False) -> dict[str, Any]:
        pids = tuple(sorted({os.getpid(), *(int(pid) for pid in worker_pids if pid)}))
        now = time.monotonic()
        if (
            not force and self.last_at is not None and tuple(self.previous) == pids
            and now - self.last_at < self.policy.sample_interval_seconds
        ):
            return self.last
        gap = None if self.last_at is None else max(0.0, now - self.last_at)
        processes = []
        cpu_deltas = []
        faults_delta = []
        errors = 0
        for pid in pids:
            try:
                row = _sample_process_resources(pid)
            except Exception as exc:
                row = dict(pid=pid, identity=None, rss_bytes=None, private_commit_bytes=None,
                           page_fault_count=None, cpu_seconds=None, error_type=type(exc).__name__)
                errors += 1
            previous = self.previous.get(pid, {})
            same_process = row["identity"] is not None and row["identity"] == previous.get("identity")
            if same_process and row["cpu_seconds"] is not None and previous.get("cpu_seconds") is not None:
                cpu_deltas.append(max(0.0, row["cpu_seconds"] - previous["cpu_seconds"]))
            if same_process and row["page_fault_count"] is not None and previous.get("page_fault_count") is not None:
                faults_delta.append(max(0, row["page_fault_count"] - previous["page_fault_count"]))
            row["role"] = "parent" if pid == os.getpid() else "worker"
            processes.append(row)
        system = _sample_system_resources(self.policy.disk_path)
        errors += system.pop("error_count")
        def total(name: str, rows: list[dict[str, Any]]) -> Optional[int]:
            return sum(row[name] for row in rows) if all(row[name] is not None for row in rows) else None
        own_rss, private = total("rss_bytes", processes), total("private_commit_bytes", processes)
        worker_rss = total("rss_bytes", [row for row in processes if row["role"] == "worker"])
        same_roster = tuple(self.previous) == pids
        cpu_covered = same_roster and len(cpu_deltas) == len(pids)
        cpu = 100 * sum(cpu_deltas) / gap / self.cpu_count if gap and cpu_covered else None
        system_cpu = None
        if system["system_cpu_total_seconds"] is not None and self.previous_system.get("system_cpu_total_seconds") is not None:
            elapsed = system["system_cpu_total_seconds"] - self.previous_system["system_cpu_total_seconds"]
            if elapsed > 0:
                idle = system["system_cpu_idle_seconds"] - self.previous_system["system_cpu_idle_seconds"]
                system_cpu = 100 * max(0, elapsed - idle) / elapsed
        ram = system["physical_ram_bytes"]
        sample = {
            "schema_version": "fxpm.governor-resource-sample.v1",
            "observed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "sample_gap_seconds": gap, "sampling": "SYNCHRONOUS_SAMPLED_NOT_CONTINUOUS",
            "process_scope": "GOVERNOR_PARENT_AND_LIVE_TASK_WORKERS",
            "effective_cpu_count": self.cpu_count, "processes": processes,
            "own_rss_bytes": own_rss, "worker_rss_bytes": worker_rss,
            "own_private_commit_bytes": private, "own_cpu_percent": cpu,
            "system_cpu_percent": system_cpu,
            "page_fault_count": total("page_fault_count", processes),
            "page_fault_delta": sum(faults_delta) if same_roster and len(faults_delta) == len(pids) else None,
            "fault_metric": "TOTAL_PROCESS_PAGE_FAULTS_NOT_HARD_FAULTS",
            "cpu_coverage_complete": cpu_covered,
            "resident_coverage_complete": own_rss is not None,
            "soft_ram_target_bytes": None if not ram else int(ram * self.policy.soft_ram_target_fraction),
            **system,
        }
        self.stats.samples += 1
        self.stats.measurement_errors += errors
        self.stats.incomplete_samples += int(own_rss is None or cpu is None or private is None)
        self.stats.max_sample_gap_seconds = max(self.stats.max_sample_gap_seconds, gap or 0)
        if own_rss is not None:
            self.stats.peak_own_rss_bytes = max(self.stats.peak_own_rss_bytes or 0, own_rss)
        if worker_rss is not None:
            self.stats.peak_worker_rss_bytes = max(self.stats.peak_worker_rss_bytes or 0, worker_rss)
        if private is not None:
            self.stats.peak_own_private_commit_bytes = max(self.stats.peak_own_private_commit_bytes or 0, private)
        if cpu is not None:
            self.stats.peak_cpu_percent = max(self.stats.peak_cpu_percent or 0, cpu)
        self.previous = {row["pid"]: row for row in processes}
        self.previous_system, self.last_at, self.last = system, now, sample
        if self.callback is not None:
            try:
                self.callback(copy.deepcopy(sample))
            except Exception:
                self.stats.telemetry_errors += 1
        return sample

    def hold(self, sample: Mapping[str, Any], active: bool) -> bool:
        target, own_rss = sample["soft_ram_target_bytes"], sample["own_rss_bytes"]
        if self.policy.soft_ram_hold and active and (target is None or own_rss is None):
            self.stats.ram_measurement_fail_open += 1
        held = bool(self.policy.soft_ram_hold and active and target is not None
                    and own_rss is not None and own_rss >= target)
        now = time.monotonic()
        if held and self.hold_started is None:
            self.hold_started = now
            self.stats.hold_events += 1
        elif not held and self.hold_started is not None:
            self.stats.hold_seconds += max(0.0, now - self.hold_started)
            self.hold_started = None
        return held

    def close(self) -> None:
        if self.hold_started is not None:
            self.stats.hold_seconds += max(0.0, time.monotonic() - self.hold_started)
            self.hold_started = None


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
        *,
        admission_gate: Optional[Callable[[], bool]] = None,
        resource_policy: Optional[GovernorResourcePolicy] = None,
        telemetry_callback: Optional[Callable[[Mapping[str, Any]], None]] = None,
    ) -> Iterator[TaskOutcome]:
        """Yield before refill; opt-in consumers close admission and drain, not close().

        Telemetry callbacks must overwrite or bound their own retained rows. Sampling
        pauses while this generator is yielded; peaks are sampled, not continuous.
        """

        if admission_gate is not None and not callable(admission_gate):
            raise TypeError("admission_gate must be callable or None")
        if resource_policy is not None and type(resource_policy) is not GovernorResourcePolicy:
            raise TypeError("resource_policy must be GovernorResourcePolicy or None")
        if telemetry_callback is not None and not callable(telemetry_callback):
            raise TypeError("telemetry_callback must be callable or None")
        if telemetry_callback is not None and resource_policy is None:
            resource_policy = GovernorResourcePolicy(soft_ram_hold=False)
        self.resource_stats = ResourceStats()
        observer = None if resource_policy is None else _ResourceObserver(
            resource_policy, telemetry_callback, self.resource_stats,
        )

        task_iter = iter(tasks)
        pending: Optional[MemoryTask] = None
        exhausted = False
        active: dict[int, _RunningTask] = {}
        sequence = 0
        orphaned: list[Any] = []
        fatal_termination_failure = False
        admission_stopped = False
        task_iterator_error: Optional[Exception] = None

        def admission_open() -> bool:
            nonlocal admission_stopped
            if admission_stopped:
                return False
            if admission_gate is not None:
                try:
                    allowed = admission_gate()
                    if type(allowed) is not bool:
                        raise TypeError("admission_gate must return bool")
                except Exception as exc:
                    self.resource_stats.admission_gate_errors += 1
                    self.resource_stats.admission_stop_reason = "ADMISSION_GATE_" + type(exc).__name__
                    allowed = False
                if not allowed:
                    admission_stopped = True
                    if not self.resource_stats.admission_stop_reason:
                        self.resource_stats.admission_stop_reason = "ADMISSION_CLOSED"
            return not admission_stopped

        def live_worker_pids() -> tuple[int, ...]:
            processes = [running.process for running in active.values()] + orphaned
            pids = []
            for process in processes:
                try:
                    if process.pid and process.is_alive():
                        pids.append(int(process.pid))
                except Exception:
                    self.resource_stats.measurement_errors += 1
            return tuple(pids)

        def load_pending() -> None:
            nonlocal pending, exhausted, admission_stopped, task_iterator_error
            if pending is not None or exhausted:
                return
            try:
                pending = next(task_iter)
            except StopIteration:
                exhausted = True
            except Exception as exc:
                if admission_gate is None:
                    raise
                task_iterator_error = exc
                admission_stopped = exhausted = True
                self.resource_stats.admission_stop_reason = "TASK_ITERATOR_" + type(exc).__name__

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
                if observer is not None:
                    observer.observe(live_worker_pids())
                for token, running in list(active.items()):
                    _receive_envelope(running)
                    try:
                        alive = running.process.is_alive()
                    except Exception as exc:
                        if admission_gate is None:
                            raise
                        running.receive_error = "ProcessPollError: " + type(exc).__name__
                        alive = True
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
                            if admission_gate is not None:
                                admission_stopped = True
                                self.resource_stats.admission_stop_reason = "FAILED_CHILD_TERMINATION"
                                # Keep this child active until it really exits; drain siblings.
                                continue
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
                if not admission_open():
                    if observer is not None:
                        observer.hold(observer.last, False)
                    while pending is not None:
                        outcome = TaskOutcome(
                            task_key=pending.key, status="NOT_RUN",
                            error_type="AdmissionClosed",
                            error_message=self.resource_stats.admission_stop_reason,
                        )
                        self._record(outcome)
                        self.resource_stats.not_run += 1
                        yield outcome
                        pending = None
                        load_pending()
                    # Active siblings must finish before the iterator reaches finally.
                while pending is not None and len(active) < self.requested_workers:
                    if not admission_open():
                        break
                    if observer is not None:
                        sample = observer.observe(live_worker_pids())
                        if observer.hold(sample, bool(active)):
                            break
                    if not admission_open():
                        break
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
            if observer is not None:
                observer.observe((), force=True)
            if task_iterator_error is not None:
                raise task_iterator_error
        finally:
            if observer is not None:
                observer.close()
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
    "GovernorResourcePolicy",
    "MemoryGovernor",
    "MemoryTask",
    "ResourceStats",
    "TaskOutcome",
    "WorkerEnvelope",
    "current_process_rss_bytes",
    "effective_cpu_count",
    "is_memory_failure",
    "peak_process_rss_bytes",
    "recommended_worker_count",
    "resolve_worker_count",
    "run_streamed_process_pool",
]
