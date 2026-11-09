"""W5.1 stage monitor, design C (Original E4961 and E4974): exact counts and
exclusive seconds per stage, telemetry only.

Each symbol job runs in one spawned child through ``pm_memory_governor._measured_worker_call``.  That function is bound
by no release, declared source identity, execution contract or row code map, so one guarded opt-in start and stop
there, plus this unhashed module, measures the stages without editing any hashed module and without re-keying anything.
The mechanism is ``sys.monitoring`` (Python 3.12): local PY_START and PY_RETURN events on the code objects of a fixed
(module, qualname) stage map, and the global PY_UNWIND event for exits by exception.  The worker thread's stack turns
the events into inclusive and exclusive seconds and exact call counts; a nested stage's time is subtracted from its
parent, so exclusive seconds add up and the observation interval minus the attributed exclusive seconds is the
UNATTRIBUTED remainder. The footer records direct parent-to-child stage time, including same-stage nesting.
M5 account production itself stays UNATTRIBUTED because its module is unavailable at the native checkpoint.

Rules from the council's prechecks and reviews:

* Ordinary telemetry faults stay inside the observer. Process-control exceptions escape callbacks, leaving the observer
  inert for the caller's teardown. Interrupted start and stop attempt cleanup before propagating the exception.
* No import system changes and no imports of its own.  Targets whose module is already loaded resolve at start.  The
  others are discovered only at the two named post-import checkpoints of the actual cell routes,
  ``_declared_native_cell_contract_sha256`` (native route) and ``_declared_m5_cell_contract_sha256`` (M5 route), whose
  PY_START fires after the route's own imports and before any stage work. Native measurement requires native-finalization
  rows to reach the checkpoint. Cell entry (the two route functions) is
  tracked separately and only counts cells.  Each target's coverage is disclosed: RESOLVED_AT_START,
  RESOLVED_AT_CHECKPOINT with the checkpoint and the cell ordinal in which it fired, UNRESOLVED with its reason,
  LOADED_AFTER_LAST_CHECKPOINT, or MODULE_NEVER_IMPORTED.  Coverage is full from the first cell only when every target
  resolved at start or at a checkpoint in cell 1; otherwise the summary names the first fully covered cell, and when a
  checkpoint is absent or unreachable the coverage is refused as incomplete.
* Exact ownership of monitoring state.  Masks and callbacks are tracked by identity; a code object is published to the
  accounting only after its mask is installed; at stop the callbacks go inert first, every owned registration is
  cleared independently, and the tool id is released only when every clear succeeded.  A partial arm or teardown is
  recorded, never hidden.
* Worker-thread-only accounting, no clamping.  Only events on the thread that started the monitor are accounted; events
  from other threads are counted and ignored.  A negative or non-finite interval is never clamped: it is counted as an
  invalid interval, the sample is discarded and the timing is marked incomplete.  The observation interval uses the
  same clock between start and stop. Open frames, stack mismatches and callback faults also make timing incomplete.
* One fresh exclusive bounded file per child.  One ``os.write`` per line with its actual return counted; a short, zero
  or failed write marks the tail corrupt, stops output and suppresses the footer.  Cumulative rows are written only
  between top-level stage calls on the worker thread, serialized by a lock, at most once per interval.  The footer,
  written once by ``stop`` into space reserved from its exact shape, carries the roster, coverage, timing, faults and
  counters; the reader compares the roster to the fixed production map, joins every resolved target to its function row,
  reconciles the stage aggregates and refuses anything inconsistent, malformed or incomplete as such.
* The directory is the caller's: a dedicated, separately owned location outside the spool root, grid root, fence and
  receipt parents and the scheduler telemetry file.  Its ownership and non-overlap are launch conditions pinned apart
  from this module (E4965 C5); the footer records the directory so the pin can be checked.

Statistics are descriptive.  Nothing here decides a budget, gates a launch or touches a digested body.
"""

from __future__ import annotations

from datetime import datetime, timezone
import inspect
import json
import math
import os
import re
import secrets
import sys
import threading
import time
from typing import Any, Callable, Final

SCHEMA_VERSION: Final = "fxpm.stage-monitor.v4"
ROW_KIND: Final = "cumulative"
FOOTER_KIND: Final = "footer"
TOOL_NAME: Final = "fxpm-stage-monitor"
ENV_DIRECTORY: Final = "FXPM_STAGE_TIMERS_DIR"
CLOCK: Final = "time.perf_counter"
_SCHEDULER: Final = "pm_historical_scheduler"

# The fixed stage map (E4961 stage_map_sites): (stage, module, qualname).  Tests pass (stage, function) targets and
# bind their own expected map to the reader.
STAGE_MAP: Final[tuple[tuple[str, str, str], ...]] = (
    ("BAR_PREP", "pm_authoritative_search_execution", "fit_native_timeframe_for_search"),
    ("BAR_PREP", "pm_authoritative_search_execution", "_fit_verified_search_frame"),
    ("BAR_PREP", "pm_authoritative_search_execution", "_reverify_fitted_timeframe"),
    ("BAR_PREP", "pm_authoritative_search_execution", "reopen_bound_fitted_timeframe"),
    ("BAR_PREP", "pm_authoritative_search_execution", "_reopen_m5_bound_fitted_timeframe"),
    ("BAR_PREP", "pm_authoritative_search_execution", "fit_m5_derived_timeframe_for_search"),
    ("REGIME", "pm_regime", "MarketRegimeDetector.compute_regime_scores"),
    ("FEATURES", "pm_authoritative_search_execution", "_compute_fitted_strategy_features"),
    ("FEATURES", "pm_core", "FeatureComputer.compute_all"),
    ("REGIME", "pm_core", "FeatureComputer._append_regime_features"),
    ("SIGNALS", "pm_authoritative_search_execution", "_build_full_prefix_strategy_surface"),
    ("SLICES", "pm_authoritative_search_execution", "_run_tier_partition"),
    ("KERNEL", "pm_core", "Backtester.run"),
    ("SPOOL_IO", "pm_authoritative_search_execution", "_write_staged_attempt"),
    ("SPOOL_IO", "pm_authoritative_search_execution", "_reopen_staged_attempt"),
    ("SPOOL_IO", "pm_authoritative_search_execution", "_publish_declared_completed_unit"),
    ("SPOOL_IO", "pm_authority_spool", "SymbolAuthoritySink.put_json"),
    ("ACCOUNT_FINALIZATION", _SCHEDULER, "_declared_account"),
    ("ACCOUNT_FINALIZATION", _SCHEDULER, "_declared_native_finalize_account"),
    ("ACCOUNT_FINALIZATION", _SCHEDULER, "_declared_m5_finalize"),
    ("ACCOUNT_FINALIZATION", "pm_authority_spool", "retire_completed_generation"),
)
# Cell entries count cells (PY_START and PY_RETURN of the two cell routes); they never drive discovery.
CELL_ENTRIES: Final[tuple[tuple[str, str, str], ...]] = (
    ("NATIVE_CELL", _SCHEDULER, "_declared_run_one_cell"),
    ("M5_CELL", _SCHEDULER, "_declared_m5_run_one_cell"),
)
# Checkpoints drive discovery: their PY_START fires after the route's own imports and before any stage work.
CHECKPOINTS: Final[tuple[tuple[str, str, str], ...]] = (
    ("NATIVE", _SCHEDULER, "_declared_native_cell_contract_sha256"),
    ("M5", _SCHEDULER, "_declared_m5_cell_contract_sha256"),
)
STAGES: Final = tuple(dict.fromkeys(stage for stage, _, _ in STAGE_MAP))
UNATTRIBUTED: Final = "UNATTRIBUTED"

RESOLVED_AT_START: Final = "RESOLVED_AT_START"
RESOLVED_AT_CHECKPOINT: Final = "RESOLVED_AT_CHECKPOINT"
UNRESOLVED: Final = "UNRESOLVED"
LOADED_AFTER_LAST_CHECKPOINT: Final = "LOADED_AFTER_LAST_CHECKPOINT"
MODULE_NEVER_IMPORTED: Final = "MODULE_NEVER_IMPORTED"
PENDING: Final = "PENDING"
RESOLVED_STATUSES: Final = (RESOLVED_AT_START, RESOLVED_AT_CHECKPOINT)
COVERAGE_STATUSES: Final = RESOLVED_STATUSES + (UNRESOLVED, LOADED_AFTER_LAST_CHECKPOINT, MODULE_NEVER_IMPORTED)

MAX_LINE_BYTES: Final = 16384
FOOTER_MARGIN_BYTES: Final = 1024
DEFAULT_MAX_FILE_BYTES: Final = 16 * 1024 * 1024
MIN_MAX_FILE_BYTES: Final = 65536
MAX_READ_BYTES: Final = DEFAULT_MAX_FILE_BYTES + 2 * MAX_LINE_BYTES
DEFAULT_ROW_INTERVAL_SECONDS: Final = 30.0
_LABEL: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_COUNTER_TEMPLATE: Final = 10 ** 15
_SECONDS_TEMPLATE: Final = 123456789.123456
_REASON_TEMPLATE: Final = "x" * 200
_TOOL_IDS: Final = (2, 3, 4)  # PROFILER_ID first; 0 debugger, 1 coverage, 5 optimizer stay untouched
_OVERHEAD_SAMPLES: Final = 1000
_FAULT_KEYS: Final = ("callback_faults", "stack_mismatches", "invalid_intervals", "resolution_faults", "start_faults",
                      "stop_faults", "tool_id_unavailable", "other_thread_events")
_COUNTER_KEYS: Final = ("rows", "bytes_before_footer", "dropped_full", "write_errors", "short_writes")
_TOLERANCE: Final = 1e-6


class StageMonitorError(ValueError):
    """Raised by construction and by the reader only; never from a callback, start or stop."""


def _fail(code: str) -> None:
    raise StageMonitorError(code)


def _encode(body: Any) -> bytes:
    return (json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("ascii")


def _utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


# -- resolution ------------------------------------------------------------------------------------------------------
def target_name(target: Any) -> str:
    if len(target) == 3:
        return f"{target[1]}.{target[2]}"
    obj = target[1]
    return f"{getattr(obj, '__module__', '?')}.{getattr(obj, '__qualname__', repr(obj))}"


def expected_map(targets: Any) -> dict[str, str]:
    """The target-to-stage map a reader compares a footer against: name -> stage, in roster order."""
    return {target_name(t): str(t[0]) for t in targets}


def _code_of(obj: Any) -> Any:
    if isinstance(obj, (staticmethod, classmethod)):
        obj = obj.__func__
    if not inspect.isfunction(obj):
        raise TypeError("NOT_A_PLAIN_FUNCTION")
    if inspect.isgeneratorfunction(obj) or inspect.iscoroutinefunction(obj) or inspect.isasyncgenfunction(obj):
        raise TypeError("GENERATOR_OR_COROUTINE_FUNCTION")
    return obj.__code__


def _resolve_in_module(module: Any, qualname: str) -> Any:
    obj: Any = module
    for part in qualname.split("."):
        obj = inspect.getattr_static(obj, part)
    return _code_of(obj)


def resolve_targets(targets: Any, *, import_modules: bool = False) -> tuple[dict[Any, tuple[str, str]], list[dict[str, str]]]:
    """Static resolver: (code -> (stage, name), unresolved). Ordinary failures are listed with a reason."""
    resolved: dict[Any, tuple[str, str]] = {}
    unresolved: list[dict[str, str]] = []
    for target in targets:
        stage = str(target[0]) if len(target) else "?"
        name = "?"
        try:
            name = target_name(target)
            if len(target) == 3:
                module = sys.modules.get(target[1])
                if module is None and import_modules:
                    import importlib
                    module = importlib.import_module(target[1])
                if module is None:
                    raise LookupError("MODULE_NOT_IMPORTED")
                code = _resolve_in_module(module, target[2])
            elif len(target) == 2:
                code = _code_of(target[1])
            else:
                raise TypeError("TARGET_SHAPE_INVALID")
            if code in resolved:
                raise TypeError("DUPLICATE_CODE_OBJECT")
            if not stage or stage == "?":
                raise TypeError("STAGE_NAME_INVALID")
            resolved[code] = (stage, name)
        except Exception as exc:
            unresolved.append({"stage": stage, "name": name, "reason": f"{type(exc).__name__}: {exc}"[:200]})
    return resolved, unresolved


def _measure_clock_sampling_overhead(clock: Callable[[], float]) -> float:
    started = clock()
    for _ in range(_OVERHEAD_SAMPLES):
        clock()
    return max(0.0, (clock() - started) / _OVERHEAD_SAMPLES)


# -- bounded exclusive writer ----------------------------------------------------------------------------------------
class _BoundedWriter:
    """One exclusive fresh file; one os.write per line with its actual return counted; corrupt tail stops output."""

    def __init__(self, directory: str, label: str, max_file_bytes: int) -> None:
        if type(max_file_bytes) is not int or max_file_bytes < MIN_MAX_FILE_BYTES or max_file_bytes > DEFAULT_MAX_FILE_BYTES:
            _fail("STAGE_MONITOR_MAX_FILE_BYTES_INVALID")
        if not isinstance(label, str) or not _LABEL.match(label):
            _fail("STAGE_MONITOR_LABEL_INVALID")
        if not isinstance(directory, str) or not os.path.isdir(directory):
            _fail("STAGE_MONITOR_DIRECTORY_MISSING")
        self.directory = os.path.abspath(directory)
        self.file_name = f"stage_monitor.{label}.{os.getpid()}.{secrets.token_hex(6)}.jsonl"
        self.path = os.path.join(self.directory, self.file_name)
        self.max_file_bytes = max_file_bytes
        self.bytes_written = 0
        self.rows = 0
        self.write_errors = 0
        self.short_writes = 0
        self.dropped_full = 0
        self.corrupt_tail = False
        self.full = False
        self.closed = False
        self.footer_written = False
        self.footer_refused = False
        self.reserve_bytes = 0
        try:
            self._fd: int | None = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o644)
        except FileExistsError:
            _fail("STAGE_MONITOR_PATH_EXISTS")
        except OSError:
            _fail("STAGE_MONITOR_OPEN_FAILED")

    def set_reserve(self, reserve_bytes: int) -> None:
        if reserve_bytes * 2 > self.max_file_bytes:
            _fail("STAGE_MONITOR_FOOTER_RESERVE_EXCEEDS_BOUND")
        self.reserve_bytes = reserve_bytes

    def _append(self, line: bytes, *, footer: bool) -> bool:
        if self._fd is None or self.corrupt_tail or (self.closed and not footer):
            return False
        reserve = 0 if footer else self.reserve_bytes
        if self.bytes_written + len(line) + reserve > self.max_file_bytes:
            if footer:
                self.footer_refused = True
            else:
                self.full = True
                self.dropped_full += 1
            return False
        if len(line) > MAX_LINE_BYTES:
            self.write_errors += 1
            return False
        try:
            written = os.write(self._fd, line)
        except OSError:
            self.corrupt_tail = True
            self.write_errors += 1
            return False
        except BaseException:
            self.corrupt_tail = True  # an interrupted write has no confirmed byte count
            raise
        if type(written) is not int or written < 0:
            written = 0
        if not footer:
            self.bytes_written += min(written, len(line))
        if written != len(line):
            self.corrupt_tail = True
            self.short_writes += 1
            return False
        if not footer:
            self.rows += 1
        return True

    def write_row(self, body: dict[str, Any]) -> bool:
        try:
            return self._append(_encode(body), footer=False)
        except Exception:
            self.write_errors += 1
            return False

    def write_footer_and_close(self, body: dict[str, Any]) -> bool:
        if self.closed:
            return self.footer_written
        self.closed = True
        try:
            if not self.corrupt_tail:
                self.footer_written = self._append(_encode(body), footer=True)
        except Exception:
            self.write_errors += 1
        self.release()
        return self.footer_written

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                self.write_errors += 1

    def counters(self) -> dict[str, Any]:
        return {"rows": self.rows, "bytes_before_footer": self.bytes_written, "dropped_full": self.dropped_full,
                "write_errors": self.write_errors, "short_writes": self.short_writes, "corrupt_tail": self.corrupt_tail,
                "full": self.full, "max_file_bytes": self.max_file_bytes}


# -- the monitor -----------------------------------------------------------------------------------------------------
class StageMonitor:
    """Exact per-stage counts and exclusive seconds for one child process.  Construct, start, stop."""

    def __init__(self, label: str, directory: str, *, targets: Any = None, cell_entries: Any = None,
                 checkpoints: Any = None, max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
                 row_interval_seconds: float = DEFAULT_ROW_INTERVAL_SECONDS,
                 clock: Callable[[], float] = time.perf_counter) -> None:
        if not _finite(row_interval_seconds) or row_interval_seconds < 0:
            _fail("STAGE_MONITOR_ROW_INTERVAL_INVALID")
        self._targets = tuple(tuple(t) for t in (STAGE_MAP if targets is None else targets))
        self._cell_entries = tuple(tuple(t) for t in (CELL_ENTRIES if cell_entries is None else cell_entries))
        self._checkpoints = tuple(tuple(t) for t in (CHECKPOINTS if checkpoints is None else checkpoints))
        if not self._targets:
            _fail("STAGE_MONITOR_EMPTY_ROSTER")
        self.label = label
        self.pid = os.getpid()
        self._clock = clock
        self._writer = _BoundedWriter(directory, label, max_file_bytes)
        try:
            self._lock = threading.Lock()
            self._stack: list[list[Any]] = []
            self._nesting: dict[str, dict[str, list[int | float]]] = {}
            self._codes: dict[Any, str] = {}                 # published target code -> target name
            self._stats: dict[str, list[Any]] = {}            # target name -> [stage, count, inclusive, exclusive]
            self._coverage: dict[str, dict[str, Any]] = {}
            self._entry_codes: dict[Any, str] = {}           # cell entry code -> role
            self._checkpoint_codes: dict[Any, str] = {}      # checkpoint code -> role
            self._hooks: dict[str, dict[str, Any]] = {}       # cell entries and checkpoints, by name
            self._hook_names: dict[Any, str] = {}            # hook code -> hook name, for the fired counters
            self._installed_masks: set[Any] = set()
            self._installed_callbacks: set[int] = set()
            self._global_events_set = False
            self._stage_order = tuple(dict.fromkeys(str(t[0]) for t in self._targets))
            self.faults = {key: 0 for key in _FAULT_KEYS}
            self.tool_id: int | None = None
            self.tool_id_released: bool | None = None
            self.active = False
            self._inert = True
            self._interrupted = False
            self._active_at_stop: bool | None = None
            self.teardown_failed_steps: list[str] = []
            self.cells_started = 0
            self.cells_completed = 0
            self._pending_checkpoint: str | None = None
            self.open_frames_at_stop: int | None = None
            self.worker_thread_ident = threading.get_ident()
            self.row_interval_seconds = float(row_interval_seconds)
            self.clock_sampling_overhead_seconds = _measure_clock_sampling_overhead(clock)
            self._start_clock: float | None = None
            self._stop_clock: float | None = None
            self._last_row_at = 0.0
            for target in self._targets:
                self._coverage[target_name(target)] = {"stage": str(target[0]), "status": PENDING, "reason": None,
                                                       "cells_started_at_resolution": None, "checkpoint": None}
            for target in self._targets:
                self._resolve_target(target, RESOLVED_AT_START, checkpoint=None)
            for entry in self._cell_entries:
                self._resolve_hook(entry, self._entry_codes, "CELL_ENTRY")
            for checkpoint in self._checkpoints:
                self._resolve_hook(checkpoint, self._checkpoint_codes, "CHECKPOINT")
            self._writer.set_reserve(len(_encode(self._footer_body(template=True))) + FOOTER_MARGIN_BYTES)
        except BaseException:
            self._writer.release()
            raise

    # -- resolution --------------------------------------------------------------------------------------------------
    def pending_modules(self) -> set[str]:
        return {t[1] for t in self._targets if len(t) == 3 and self._coverage[target_name(t)]["status"] == PENDING}

    def _install_mask(self, code: Any) -> None:
        if self.active and self.tool_id is not None:
            events = sys.monitoring.events
            sys.monitoring.set_local_events(self.tool_id, code, events.PY_START | events.PY_RETURN)
            self._installed_masks.add(code)

    def _publish(self, target: Any, code: Any, status: str, checkpoint: str | None) -> None:
        name = target_name(target)
        if code in self._codes or code in self._entry_codes or code in self._checkpoint_codes:
            self._coverage[name] = {"stage": str(target[0]), "status": UNRESOLVED, "reason": "TypeError: DUPLICATE_CODE_OBJECT",
                                    "cells_started_at_resolution": None, "checkpoint": None}
            return
        self._install_mask(code)  # mask first, then publish, so no event fires for an unmasked code
        self._codes[code] = name
        self._stats[name] = [str(target[0]), 0, 0.0, 0.0]
        self._coverage[name] = {"stage": str(target[0]), "status": status, "reason": None,
                                "cells_started_at_resolution": self.cells_started if status == RESOLVED_AT_CHECKPOINT else 0,
                                "checkpoint": checkpoint}

    def _resolve_target(self, target: Any, status: str, *, checkpoint: str | None) -> None:
        name = target_name(target)
        try:
            if len(target) == 3:
                module = sys.modules.get(target[1])
                if module is None:
                    return  # pending until a checkpoint fires after the child's own import
                code = _resolve_in_module(module, target[2])
            elif len(target) == 2:
                code = _code_of(target[1])
            else:
                raise TypeError("TARGET_SHAPE_INVALID")
            self._publish(target, code, status, checkpoint)
        except Exception as exc:
            self._coverage[name] = {"stage": str(target[0]), "status": UNRESOLVED, "reason": f"{type(exc).__name__}: {exc}"[:200],
                                    "cells_started_at_resolution": None, "checkpoint": None}

    def _resolve_hook(self, hook: Any, table: dict[Any, str], kind: str) -> None:
        role = str(hook[0])
        name = target_name(hook)
        try:
            if len(hook) == 3:
                module = sys.modules.get(hook[1])
                if module is None:
                    raise LookupError("MODULE_NOT_IMPORTED")
                code = _resolve_in_module(module, hook[2])
            else:
                code = _code_of(hook[1])
            if code in self._codes or code in self._entry_codes or code in self._checkpoint_codes:
                raise TypeError("DUPLICATE_CODE_OBJECT")
            self._install_mask(code)
            table[code] = role
            self._hook_names[code] = name
            self._hooks[name] = {"kind": kind, "role": role, "status": "ARMED", "reason": None, "fired": 0}
        except Exception as exc:
            self._hooks[name] = {"kind": kind, "role": role, "status": UNRESOLVED, "reason": f"{type(exc).__name__}: {exc}"[:200],
                                 "fired": 0}

    def _discover(self, checkpoint: str) -> None:
        if not self.pending_modules():
            return
        for target in self._targets:
            if self._coverage[target_name(target)]["status"] == PENDING:
                self._resolve_target(target, RESOLVED_AT_CHECKPOINT, checkpoint=checkpoint)

    def _settle_coverage(self) -> None:
        for target in self._targets:
            name = target_name(target)
            if self._coverage[name]["status"] == PENDING:
                loaded = len(target) == 3 and target[1] in sys.modules
                self._coverage[name] = {"stage": str(target[0]),
                                        "status": LOADED_AFTER_LAST_CHECKPOINT if loaded else MODULE_NEVER_IMPORTED,
                                        "reason": None, "cells_started_at_resolution": None, "checkpoint": None}

    def _hook_name(self, code: Any) -> str:
        return self._hook_names.get(code, "?")

    # -- callbacks -----------------------------------------------------------------------------------------------
    def _mine(self) -> bool:
        if threading.get_ident() != self.worker_thread_ident:
            self.faults["other_thread_events"] += 1
            return False
        return True

    def _mark_interrupted(self) -> None:
        self._inert = True
        self._interrupted = True

    def _on_start(self, code: Any, instruction_offset: int) -> None:
        try:
            if self._inert or not self._mine():
                return None
            entry = self._entry_codes.get(code)
            if entry is not None:
                self.cells_started += 1
                self._pending_checkpoint = entry.removesuffix("_CELL")
                self._hooks[self._hook_name(code)]["fired"] += 1
                return None
            role = self._checkpoint_codes.get(code)
            if role is not None:
                if role == self._pending_checkpoint:
                    self._pending_checkpoint = None
                    self._hooks[self._hook_name(code)]["fired"] += 1
                    self._discover(role)
                return None
            if code in self._codes:
                self._stack.append([code, self._clock(), 0.0, {}])
        except Exception:
            self.faults["callback_faults"] += 1
        except BaseException:
            self._mark_interrupted()
            raise
        return None

    def _on_return(self, code: Any, instruction_offset: int, retval: Any) -> None:
        try:
            if self._inert or not self._mine():
                return None
            if code in self._entry_codes:
                self.cells_completed += 1
                self._pending_checkpoint = None
                return None
            if code in self._codes:
                self._finish(code)
        except Exception:
            self.faults["callback_faults"] += 1
        except BaseException:
            self._mark_interrupted()
            raise
        return None

    def _on_unwind(self, code: Any, instruction_offset: int, exception: Any) -> None:
        try:
            if self._inert or (code not in self._codes and code not in self._entry_codes) or not self._mine():
                return None
            if code in self._entry_codes:
                self._pending_checkpoint = None
            else:
                self._finish(code)
        except Exception:
            self.faults["callback_faults"] += 1
        except BaseException:
            self._mark_interrupted()
            raise
        return None

    def _finish(self, code: Any) -> None:
        stack = self._stack
        if not stack or stack[-1][0] is not code:
            self.faults["stack_mismatches"] += 1
            return
        entry = stack.pop()
        now = self._clock()
        inclusive = now - entry[1]
        exclusive = inclusive - entry[2]
        if not (_finite(inclusive) and _finite(exclusive)) or inclusive < 0 or exclusive < 0:
            self.faults["invalid_intervals"] += 1  # never clamped; the sample is discarded and timing is incomplete
            return
        record = self._stats[self._codes[code]]
        record[1] += 1
        record[2] += inclusive
        record[3] += exclusive
        stage = record[0]
        if entry[3]:
            children = self._nesting.setdefault(stage, {})
            for child, (count, seconds) in entry[3].items():
                edge = children.setdefault(child, [0, 0.0])
                edge[0] += count
                edge[1] += seconds
        if stack:
            parent = stack[-1]
            parent[2] += inclusive
            edge = parent[3].setdefault(stage, [0, 0.0])
            edge[0] += 1
            edge[1] += inclusive
        elif self.row_interval_seconds > 0 and now - self._last_row_at >= self.row_interval_seconds:
            self._last_row_at = now
            with self._lock:
                self._writer.write_row(self._row_body(now))

    # -- bodies --------------------------------------------------------------------------------------------------
    def _stage_table(self, template: bool = False) -> dict[str, dict[str, Any]]:
        table: dict[str, dict[str, Any]] = {}
        for stage in self._stage_order:
            table[stage] = {"count": _COUNTER_TEMPLATE if template else 0,
                            "inclusive_seconds": _SECONDS_TEMPLATE if template else 0.0,
                            "exclusive_seconds": _SECONDS_TEMPLATE if template else 0.0,
                            "status": "OBSERVED" if template else "UNOBSERVED"}
        if not template:
            for stage, count, inclusive, exclusive in self._stats.values():
                row = table[stage]
                row["count"] += count
                row["inclusive_seconds"] += inclusive
                row["exclusive_seconds"] += exclusive
            for row in table.values():
                row["inclusive_seconds"] = round(row["inclusive_seconds"], 6)
                row["exclusive_seconds"] = round(row["exclusive_seconds"], 6)
                row["status"] = "OBSERVED" if row["count"] > 0 else "UNOBSERVED"
        return table

    def _function_table(self, template: bool = False) -> dict[str, dict[str, Any]]:
        if template:
            return {name: {"stage": info["stage"], "count": _COUNTER_TEMPLATE, "inclusive_seconds": _SECONDS_TEMPLATE,
                           "exclusive_seconds": _SECONDS_TEMPLATE} for name, info in self._coverage.items()}
        return {name: {"stage": stage, "count": count, "inclusive_seconds": round(inclusive, 6),
                       "exclusive_seconds": round(exclusive, 6)} for name, (stage, count, inclusive, exclusive) in self._stats.items()}

    def _nesting_table(self, template: bool) -> dict[str, dict[str, dict[str, Any]]]:
        if template:
            return {parent: {child: {"count": _COUNTER_TEMPLATE, "inclusive_seconds": _SECONDS_TEMPLATE}
                             for child in self._stage_order} for parent in self._stage_order}
        return {parent: {child: {"count": count, "inclusive_seconds": round(seconds, 6)}
                         for child, (count, seconds) in children.items()} for parent, children in self._nesting.items()}

    def _coverage_table(self, template: bool = False) -> dict[str, dict[str, Any]]:
        if template:
            return {name: {"stage": info["stage"], "status": LOADED_AFTER_LAST_CHECKPOINT, "reason": _REASON_TEMPLATE,
                           "cells_started_at_resolution": _COUNTER_TEMPLATE, "checkpoint": _REASON_TEMPLATE[:40]}
                    for name, info in self._coverage.items()}
        return {name: dict(info) for name, info in self._coverage.items()}

    def _hook_table(self, template: bool = False) -> dict[str, dict[str, Any]]:
        if template:
            return {name: {"kind": info["kind"], "role": info["role"], "status": UNRESOLVED, "reason": _REASON_TEMPLATE,
                           "fired": _COUNTER_TEMPLATE} for name, info in self._hooks.items()}
        return {name: dict(info) for name, info in self._hooks.items()}

    def _timing(self, template: bool, task_duration_seconds: Any) -> dict[str, Any]:
        if template:
            return {"observation_interval_seconds": _SECONDS_TEMPLATE, "attributed_exclusive_seconds": _SECONDS_TEMPLATE,
                    UNATTRIBUTED: _SECONDS_TEMPLATE, "task_duration_seconds": _SECONDS_TEMPLATE,
                    "open_frames_at_stop": _COUNTER_TEMPLATE, "timing_complete": True,
                    "timing_incomplete_reason": _REASON_TEMPLATE, "scope": "WORKER_THREAD_ONLY"}
        attributed = sum(record[3] for record in self._stats.values())
        interval = None
        if self._start_clock is not None and self._stop_clock is not None and _finite(self._stop_clock - self._start_clock) \
                and self._stop_clock >= self._start_clock:
            interval = self._stop_clock - self._start_clock
        open_frames = self.open_frames_at_stop if self.open_frames_at_stop is not None else len(self._stack)
        reason = None
        if self._interrupted:
            reason = "INTERRUPTED"
        elif interval is None:
            reason = "NO_OBSERVATION_INTERVAL"
        elif open_frames:
            reason = "OPEN_FRAMES_AT_STOP"
        elif self.faults["stack_mismatches"]:
            reason = "STACK_MISMATCHES"
        elif self.faults["invalid_intervals"]:
            reason = "INVALID_INTERVALS"
        elif self.faults["callback_faults"]:
            reason = "CALLBACK_FAULTS"
        elif not _finite(attributed) or attributed > interval:
            reason = "ATTRIBUTED_EXCEEDS_INTERVAL"
        complete = reason is None
        interval = round(interval, 6) if interval is not None else None
        attributed = round(attributed, 6) if _finite(attributed) else None
        return {"observation_interval_seconds": interval,
                "attributed_exclusive_seconds": attributed,
                UNATTRIBUTED: round(interval - attributed, 6) if complete else None,
                "task_duration_seconds": round(float(task_duration_seconds), 6) if _finite(task_duration_seconds) else None,
                "open_frames_at_stop": open_frames, "timing_complete": complete, "timing_incomplete_reason": reason,
                "scope": "WORKER_THREAD_ONLY"}

    def _row_body(self, now: float) -> dict[str, Any]:
        return {"schema_version": SCHEMA_VERSION, "kind": ROW_KIND, "writer": self.label, "pid": self.pid,
                "at_utc": _utc(), "monitor_elapsed_seconds": round(max(0.0, now - (self._start_clock or now)), 6),
                "cells_started": self.cells_started, "stages": self._stage_table()}

    def _footer_body(self, *, template: bool = False, task_duration_seconds: Any = None) -> dict[str, Any]:
        wide = _COUNTER_TEMPLATE
        if template:
            faults = {key: wide for key in _FAULT_KEYS}
            counters = {"rows": wide, "bytes_before_footer": wide, "dropped_full": wide, "write_errors": wide,
                        "short_writes": wide, "corrupt_tail": False, "full": False, "max_file_bytes": self._writer.max_file_bytes}
            teardown = {"complete": True, "failed_steps": [_REASON_TEMPLATE[:60]] * 8, "tool_id_released": True}
        else:
            faults = dict(self.faults)
            counters = self._writer.counters()
            teardown = {"complete": not self.teardown_failed_steps, "failed_steps": list(self.teardown_failed_steps),
                        "tool_id_released": self.tool_id_released}
        active = True if template else (self._active_at_stop if self._active_at_stop is not None else self.active)
        return {
            "schema_version": SCHEMA_VERSION, "kind": FOOTER_KIND, "writer": self.label, "pid": self.pid,
            "worker_thread_ident": wide if template else self.worker_thread_ident,
            "file_name": self._writer.file_name, "directory": self._writer.directory,
            "tool_id": 9 if template else self.tool_id, "monitoring_active_at_stop": active, "clock": CLOCK,
            "clock_sampling_overhead_seconds": _SECONDS_TEMPLATE if template else round(self.clock_sampling_overhead_seconds, 9),
            "roster": [target_name(t) for t in self._targets],
            "roster_stages": {target_name(t): str(t[0]) for t in self._targets},
            "targets_total": len(self._targets), "stages_total": len(self._stage_order),
            "cells_started": wide if template else self.cells_started,
            "cells_completed": wide if template else self.cells_completed,
            "timing": self._timing(template, task_duration_seconds),
            "stages": self._stage_table(template=template),
            "stage_nesting": self._nesting_table(template),
            "functions": self._function_table(template=template),
            "coverage": self._coverage_table(template=template),
            "hooks": self._hook_table(template=template),
            "faults": faults, "teardown": teardown, "writer_counters": counters,
            "closed_at_utc": "2000-01-01T00:00:00+00:00" if template else _utc(),
        }

    # -- lifecycle -----------------------------------------------------------------------------------------------
    def start(self) -> bool:
        """Arm monitoring and return whether it is active. Tear down any partial arm before returning or propagating."""
        try:
            monitoring = sys.monitoring
            tool_id = None
            for candidate in _TOOL_IDS:
                if monitoring.get_tool(candidate) is None:
                    tool_id = candidate
                    break
            if tool_id is None:
                self.faults["tool_id_unavailable"] += 1
                return False
            monitoring.use_tool_id(tool_id, TOOL_NAME)
            self.tool_id = tool_id
            events = monitoring.events
            for event, callback in ((events.PY_START, self._on_start), (events.PY_RETURN, self._on_return),
                                    (events.PY_UNWIND, self._on_unwind)):
                monitoring.register_callback(tool_id, event, callback)
                self._installed_callbacks.add(event)
            for code in list(self._codes) + list(self._entry_codes) + list(self._checkpoint_codes):
                monitoring.set_local_events(tool_id, code, events.PY_START | events.PY_RETURN)
                self._installed_masks.add(code)
            monitoring.set_events(tool_id, events.PY_UNWIND)
            self._global_events_set = True
            self.active = True
            self._inert = False
            self._start_clock = self._clock()
            self._last_row_at = self._start_clock
            return True
        except Exception:
            self.faults["start_faults"] += 1
            self._inert = True
            self.active = False
            try:
                self._teardown()
            except BaseException:
                self._stop_after_interruption()
                raise
            return False
        except BaseException:
            self._stop_after_interruption()
            raise

    def _stop_after_interruption(self) -> None:
        self._mark_interrupted()
        try:
            self.stop()
        except BaseException:
            pass  # preserve the process-control exception already in flight

    def _teardown(self) -> None:
        """Make callbacks inert, clear every owned registration independently, release the id only when all cleared."""
        self._inert = True
        tool_id = self.tool_id
        if tool_id is None:
            return
        monitoring = sys.monitoring
        try:
            owned = monitoring.get_tool(tool_id) == TOOL_NAME
        except Exception:
            owned = False
        except BaseException:
            self.teardown_failed_steps.append("tool_ownership_check")
            self.tool_id_released = False
            raise
        if not owned:
            self.teardown_failed_steps.append("tool_id_not_owned")
            self.faults["stop_faults"] += 1
            self.tool_id_released = False
            self.tool_id = None
            return
        failed: list[str] = []
        interruption: BaseException | None = None

        def clear(step: str, action: Callable[..., Any], *args: Any) -> bool:
            nonlocal interruption
            try:
                action(*args)
            except Exception:
                failed.append(step)
                self.faults["stop_faults"] += 1
            except BaseException as exc:
                failed.append(step)
                if interruption is None:
                    interruption = exc
            else:
                return True
            return False

        if self._global_events_set and clear("global_events", monitoring.set_events, tool_id, 0):
            self._global_events_set = False
        for code in list(self._installed_masks):
            name = getattr(code, "co_qualname", getattr(code, "co_name", "?"))
            if clear(f"local_mask:{name}", monitoring.set_local_events, tool_id, code, 0):
                self._installed_masks.discard(code)
        for event in list(self._installed_callbacks):
            if clear(f"callback:{event}", monitoring.register_callback, tool_id, event, None):
                self._installed_callbacks.discard(event)

        def release_id() -> None:
            if monitoring.get_tool(tool_id) == TOOL_NAME:
                monitoring.free_tool_id(tool_id)
                self.tool_id_released = True
            else:
                failed.append("tool_id_not_owned")
                self.faults["stop_faults"] += 1
            self.tool_id = None

        self.tool_id_released = False
        if not failed:
            clear("free_tool_id", release_id)
        self.teardown_failed_steps.extend(failed)
        if interruption is not None:
            raise interruption

    def stop(self, task_duration_seconds: Any = None) -> dict[str, Any]:
        """Go inert, disarm, and attempt the footer and file close before propagating any process-control exception."""
        self._inert = True
        if self._active_at_stop is None:
            self._active_at_stop = self.active
        interruption: BaseException | None = None

        def attempt(action: Callable[[], Any]) -> None:
            nonlocal interruption
            try:
                action()
            except Exception:
                self.faults["stop_faults"] += 1
            except BaseException as exc:
                self._mark_interrupted()
                if interruption is None:
                    interruption = exc

        def sample_clock() -> None:
            self._stop_clock = self._clock()

        def emit_footer() -> None:
            with self._lock:
                self._writer.write_footer_and_close(self._footer_body(task_duration_seconds=task_duration_seconds))

        attempt(sample_clock)
        self.open_frames_at_stop = len(self._stack)
        attempt(self._teardown)
        self.active = False
        attempt(self._settle_coverage)
        attempt(emit_footer)
        attempt(self._writer.release)
        if interruption is not None:
            raise interruption
        return self.status()

    def status(self) -> dict[str, Any]:
        return {"writer": self.label, "pid": self.pid, "path": self._writer.path, "tool_id": self.tool_id,
                "tool_id_released": self.tool_id_released, "active": self.active, "faults": dict(self.faults),
                "writer_counters": self._writer.counters(), "footer_written": self._writer.footer_written,
                "footer_refused": self._writer.footer_refused, "coverage": self._coverage_table(),
                "hooks": self._hook_table(), "teardown_failed_steps": list(self.teardown_failed_steps),
                "cells_started": self.cells_started, "cells_completed": self.cells_completed}


def start(label: str, directory: str, **options: Any) -> StageMonitor:
    """Construct and arm a monitor. Process-control exceptions propagate after cleanup."""
    monitor = StageMonitor(label, directory, **options)
    monitor.start()
    return monitor


# -- reading ---------------------------------------------------------------------------------------------------------
_FOOTER_REQUIRED: Final = ("writer", "pid", "roster", "roster_stages", "targets_total", "stages_total", "timing", "stages",
                            "stage_nesting", "functions", "coverage", "hooks", "faults", "teardown", "writer_counters",
                           "monitoring_active_at_stop", "cells_started", "cells_completed")


def _metric_row_ok(info: Any) -> bool:
    return (type(info) is dict and type(info.get("count")) is int and info["count"] >= 0
            and _finite(info.get("inclusive_seconds")) and _finite(info.get("exclusive_seconds"))
            and info["inclusive_seconds"] >= 0 and info["exclusive_seconds"] >= 0
            and info["exclusive_seconds"] <= info["inclusive_seconds"] + _TOLERANCE)


def _check_row(row: dict[str, Any], expected: dict[str, str], number: int) -> None:
    stages = row.get("stages")
    if not isinstance(row.get("writer"), str) or type(row.get("pid")) is not int or type(stages) is not dict \
            or set(stages) != set(dict.fromkeys(expected.values())) or type(row.get("cells_started")) is not int \
            or not _finite(row.get("monitor_elapsed_seconds")):
        _fail(f"STAGE_MONITOR_ROW_INVALID:{number}")
    for name, info in stages.items():
        if not _metric_row_ok(info):
            _fail(f"STAGE_MONITOR_ROW_METRIC_INVALID:{number}:{name}")


def _check_nesting(nesting: Any, stages: dict[str, Any]) -> None:
    if type(nesting) is not dict or not set(nesting) <= set(stages):
        _fail("STAGE_MONITOR_FOOTER_NESTING_INVALID")
    incoming = {stage: [0, 0.0, 0] for stage in stages}
    for parent, stage in stages.items():
        children = nesting.get(parent, {})
        if type(children) is not dict or not set(children) <= set(stages) or (parent in nesting and not children):
            _fail("STAGE_MONITOR_FOOTER_NESTING_INVALID")
        nested_seconds = 0.0
        for child, edge in children.items():
            if type(edge) is not dict or set(edge) != {"count", "inclusive_seconds"} \
                    or type(edge["count"]) is not int or edge["count"] <= 0 \
                    or not _finite(edge["inclusive_seconds"]) or edge["inclusive_seconds"] < 0:
                _fail("STAGE_MONITOR_FOOTER_NESTING_INVALID")
            nested_seconds += edge["inclusive_seconds"]
            received = incoming[child]
            received[0] += edge["count"]
            received[1] += edge["inclusive_seconds"]
            received[2] += 1
        # Each edge and both stage totals were rounded independently.
        tolerance = _TOLERANCE * (1 + len(children))
        if (children and not stage["count"]) or abs(stage["inclusive_seconds"] - stage["exclusive_seconds"] - nested_seconds) > tolerance:
            _fail(f"STAGE_MONITOR_FOOTER_NESTING_MISMATCH:{parent}")
    for child, (count, seconds, parents) in incoming.items():
        if count > stages[child]["count"] or seconds > stages[child]["inclusive_seconds"] + _TOLERANCE * (1 + parents):
            _fail(f"STAGE_MONITOR_FOOTER_NESTING_MISMATCH:{child}")


def _check_footer(row: dict[str, Any], expected: dict[str, str], rows: int, offset: int, number: int,
                  expected_hooks: dict[str, tuple[str, str]] | None) -> None:
    for key in _FOOTER_REQUIRED:
        if key not in row:
            _fail(f"STAGE_MONITOR_FOOTER_INVALID:{number}:{key}")
    roster = row["roster"]
    if roster != list(expected) or row["roster_stages"] != expected or row["targets_total"] != len(expected):
        _fail("STAGE_MONITOR_FOOTER_ROSTER_MISMATCH")
    expected_stages = list(dict.fromkeys(expected.values()))
    coverage = row["coverage"]
    if type(coverage) is not dict or set(coverage) != set(expected):
        _fail("STAGE_MONITOR_FOOTER_COVERAGE_MISMATCH")
    resolved_names = []
    for name, info in coverage.items():
        if type(info) is not dict or info.get("status") not in COVERAGE_STATUSES or info.get("stage") != expected[name]:
            _fail(f"STAGE_MONITOR_FOOTER_COVERAGE_INVALID:{name}")
        status = info["status"]
        ordinal = info.get("cells_started_at_resolution")
        checkpoint = info.get("checkpoint")
        if status == RESOLVED_AT_START and not (ordinal == 0 and checkpoint is None):
            _fail(f"STAGE_MONITOR_FOOTER_COVERAGE_INVALID:{name}")
        if status == RESOLVED_AT_CHECKPOINT and not (type(ordinal) is int and ordinal >= 1 and isinstance(checkpoint, str) and checkpoint):
            _fail(f"STAGE_MONITOR_FOOTER_COVERAGE_INVALID:{name}")
        if status not in RESOLVED_STATUSES and not (ordinal is None and checkpoint is None):
            _fail(f"STAGE_MONITOR_FOOTER_COVERAGE_INVALID:{name}")
        if status == UNRESOLVED and not isinstance(info.get("reason"), str):
            _fail(f"STAGE_MONITOR_FOOTER_COVERAGE_INVALID:{name}")
        if status in RESOLVED_STATUSES:
            resolved_names.append(name)
    functions = row["functions"]
    if type(functions) is not dict or sorted(functions) != sorted(resolved_names):
        _fail("STAGE_MONITOR_FOOTER_FUNCTIONS_MISMATCH")
    stages = row["stages"]
    if type(stages) is not dict or set(stages) != set(expected_stages) or row["stages_total"] != len(expected_stages):
        _fail("STAGE_MONITOR_FOOTER_STAGES_INVALID")
    aggregate = {stage: [0, 0.0, 0.0] for stage in expected_stages}
    for name, info in functions.items():
        if not _metric_row_ok(info) or info.get("stage") != expected[name]:
            _fail(f"STAGE_MONITOR_FOOTER_METRIC_INVALID:{name}")
        agg = aggregate[info["stage"]]
        agg[0] += info["count"]
        agg[1] += info["inclusive_seconds"]
        agg[2] += info["exclusive_seconds"]
    for stage, info in stages.items():
        if not _metric_row_ok(info) or info.get("status") not in ("OBSERVED", "UNOBSERVED"):
            _fail(f"STAGE_MONITOR_FOOTER_METRIC_INVALID:{stage}")
        count, inclusive, exclusive = aggregate[stage]
        tolerance = _TOLERANCE * max(1, count)
        if info["count"] != count or abs(info["inclusive_seconds"] - inclusive) > tolerance \
                or abs(info["exclusive_seconds"] - exclusive) > tolerance or info["status"] != ("OBSERVED" if count else "UNOBSERVED"):
            _fail(f"STAGE_MONITOR_FOOTER_AGGREGATE_MISMATCH:{stage}")
    _check_nesting(row["stage_nesting"], stages)
    total_exclusive = sum(agg[2] for agg in aggregate.values())
    timing = row["timing"]
    if type(timing) is not dict or type(timing.get("timing_complete")) is not bool \
            or not _finite(timing.get("attributed_exclusive_seconds")) \
            or abs(timing["attributed_exclusive_seconds"] - total_exclusive) > _TOLERANCE * max(1, len(functions)) \
            or type(timing.get("open_frames_at_stop")) is not int:
        _fail("STAGE_MONITOR_FOOTER_TIMING_INVALID")
    if timing["timing_complete"]:
        interval = timing.get("observation_interval_seconds")
        residual = timing.get(UNATTRIBUTED)
        if not _finite(interval) or not _finite(residual) or interval < timing["attributed_exclusive_seconds"] - _TOLERANCE \
                or abs(residual - (interval - timing["attributed_exclusive_seconds"])) > _TOLERANCE \
                or timing["open_frames_at_stop"] != 0 or timing.get("timing_incomplete_reason") is not None:
            _fail("STAGE_MONITOR_FOOTER_TIMING_INVALID")
    elif not isinstance(timing.get("timing_incomplete_reason"), str) or timing.get(UNATTRIBUTED) is not None:
        _fail("STAGE_MONITOR_FOOTER_TIMING_INVALID")
    faults = row["faults"]
    if type(faults) is not dict or set(faults) != set(_FAULT_KEYS) or any(type(v) is not int or v < 0 for v in faults.values()):
        _fail("STAGE_MONITOR_FOOTER_FAULTS_INVALID")
    if timing["timing_complete"] and any(faults[k] for k in ("stack_mismatches", "invalid_intervals", "callback_faults")):
        _fail("STAGE_MONITOR_FOOTER_TIMING_INVALID")
    hooks = row["hooks"]
    if type(hooks) is not dict or (expected_hooks is not None and set(hooks) != set(expected_hooks)):
        _fail("STAGE_MONITOR_FOOTER_HOOKS_INVALID")
    for name, info in hooks.items():
        if type(info) is not dict or info.get("kind") not in ("CELL_ENTRY", "CHECKPOINT") \
                or not isinstance(info.get("role"), str) or info.get("status") not in ("ARMED", UNRESOLVED) \
                or type(info.get("fired")) is not int or info["fired"] < 0 \
                or (info["status"] != "ARMED" and info["fired"]):
            _fail(f"STAGE_MONITOR_FOOTER_HOOKS_INVALID:{name}")
        if expected_hooks is not None and (info["kind"], info["role"]) != expected_hooks[name]:
            _fail(f"STAGE_MONITOR_FOOTER_HOOKS_INVALID:{name}")
    counters = row["writer_counters"]
    if type(counters) is not dict or any(type(counters.get(k)) is not int or counters[k] < 0 for k in _COUNTER_KEYS) \
            or type(counters.get("corrupt_tail")) is not bool or type(counters.get("max_file_bytes")) is not int:
        _fail("STAGE_MONITOR_FOOTER_COUNTERS_INVALID")
    if counters["rows"] != rows or counters["bytes_before_footer"] != offset:
        _fail("STAGE_MONITOR_FOOTER_MISMATCH")
    if counters["corrupt_tail"]:
        _fail("STAGE_MONITOR_FOOTER_ON_CORRUPT_TAIL")
    teardown = row["teardown"]
    if type(teardown) is not dict or type(teardown.get("complete")) is not bool or type(teardown.get("failed_steps")) is not list \
            or teardown["complete"] != (not teardown["failed_steps"]):
        _fail("STAGE_MONITOR_FOOTER_TEARDOWN_INVALID")
    for key in ("cells_started", "cells_completed"):
        if type(row[key]) is not int or row[key] < 0:
            _fail(f"STAGE_MONITOR_FOOTER_INVALID:{number}:{key}")
    entry_counts, checkpoint_counts = _route_counts(hooks)
    if row["cells_started"] != sum(entry_counts.values()) or row["cells_completed"] > row["cells_started"]:
        _fail("STAGE_MONITOR_FOOTER_CELL_COUNTS_INVALID")
    if any(count > entry_counts.get(role, 0) for role, count in checkpoint_counts.items()):
        _fail("STAGE_MONITOR_FOOTER_CHECKPOINT_COUNTS_INVALID")
    for name, info in coverage.items():
        if info["status"] == RESOLVED_AT_CHECKPOINT and (
            info["cells_started_at_resolution"] > row["cells_started"] or not checkpoint_counts.get(info["checkpoint"])
        ):
            _fail(f"STAGE_MONITOR_FOOTER_COVERAGE_INVALID:{name}")
    if type(row["monitoring_active_at_stop"]) is not bool:
        _fail(f"STAGE_MONITOR_FOOTER_INVALID:{number}:monitoring_active_at_stop")


def _route_counts(hooks: dict[str, dict[str, Any]]) -> tuple[dict[str, int], dict[str, int]]:
    entries: dict[str, int] = {}
    checkpoints: dict[str, int] = {}
    for info in hooks.values():
        if info["kind"] == "CELL_ENTRY":
            counts, role = entries, info["role"].removesuffix("_CELL")
        else:
            counts, role = checkpoints, info["role"]
        counts[role] = counts.get(role, 0) + info["fired"]
    return entries, checkpoints


def read_monitor_file(path: str, expected: dict[str, str] | None = None,
                      max_read_bytes: int = MAX_READ_BYTES) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Parse one monitor file into (cumulative rows, footer or None) against the expected target-to-stage map (the
    production map by default).  Bounded; any defect refuses the whole file."""
    production_map = expected_map(STAGE_MAP)
    if expected is None:
        expected = production_map
    if type(expected) is not dict or not expected or any(not isinstance(k, str) or not isinstance(v, str) for k, v in expected.items()):
        _fail("STAGE_MONITOR_EXPECTED_MAP_INVALID")
    expected_hooks = None
    if expected == production_map:
        expected_hooks = {target_name(entry): ("CELL_ENTRY", entry[0]) for entry in CELL_ENTRIES}
        expected_hooks.update({target_name(cp): ("CHECKPOINT", cp[0]) for cp in CHECKPOINTS})
    if type(max_read_bytes) is not int or max_read_bytes < MAX_LINE_BYTES or max_read_bytes > MAX_READ_BYTES:
        _fail("STAGE_MONITOR_READ_BOUND_INVALID")
    if os.path.getsize(path) > max_read_bytes:
        _fail("STAGE_MONITOR_FILE_TOO_LARGE")
    with open(path, "rb") as fh:
        data = fh.read(max_read_bytes + 1)
    if len(data) > max_read_bytes:
        _fail("STAGE_MONITOR_FILE_TOO_LARGE")
    if data and not data.endswith(b"\n"):
        _fail("STAGE_MONITOR_TRUNCATED_TAIL")
    rows: list[dict[str, Any]] = []
    footer: dict[str, Any] | None = None
    offset = 0
    footer_length = 0
    for number, raw in enumerate(data.split(b"\n")[:-1] if data else [], start=1):
        if footer is not None:
            _fail(f"STAGE_MONITOR_ROW_AFTER_FOOTER:{number}")
        if len(raw) + 1 > MAX_LINE_BYTES:
            _fail(f"STAGE_MONITOR_LINE_TOO_LONG:{number}")
        try:
            row = json.loads(raw.decode("ascii"))
        except (UnicodeDecodeError, ValueError):
            _fail(f"STAGE_MONITOR_LINE_INVALID:{number}")
        if type(row) is not dict or row.get("schema_version") != SCHEMA_VERSION:
            _fail(f"STAGE_MONITOR_RECORD_INVALID:{number}")
        kind = row.get("kind")
        if kind == FOOTER_KIND:
            _check_footer(row, expected, len(rows), offset, number, expected_hooks)
            footer = row
            footer_length = len(raw) + 1
        elif kind == ROW_KIND:
            _check_row(row, expected, number)
            rows.append(row)
        else:
            _fail(f"STAGE_MONITOR_RECORD_INVALID:{number}")
        offset += len(raw) + 1
    if footer is not None:
        if footer["writer_counters"]["bytes_before_footer"] + footer_length != len(data):
            _fail("STAGE_MONITOR_FOOTER_SIZE_MISMATCH")
        if len(data) > footer["writer_counters"]["max_file_bytes"]:
            _fail("STAGE_MONITOR_FILE_EXCEEDS_DECLARED_BOUND")
    return rows, footer


def summarize(footer: dict[str, Any] | None) -> dict[str, Any]:
    """The W5.1 table from one validated footer, failing closed: complete only when every target resolved at start or at a
    checkpoint in cell 1, every checkpoint hook was armed, monitoring was active, timing is complete, teardown clean, no
    fault and a clean writer.  Otherwise the failed checks are named and, for late coverage, the first fully covered cell."""
    if footer is None:
        return {"schema_version": SCHEMA_VERSION + ".summary", "coverage": {"footer_present": False, "complete": False,
                 "incomplete_reason": "NO_FOOTER", "failed_checks": ["NO_FOOTER"]}, "stages": [], "stage_nesting": {},
                "scope": "DESCRIPTIVE_ONLY_NO_BUDGET_DECISION"}
    timing = footer["timing"]
    attributed = float(timing["attributed_exclusive_seconds"])
    table = []
    stage_order = dict.fromkeys(footer["roster_stages"][name] for name in footer["roster"])
    for stage in stage_order:
        row = footer["stages"][stage]
        count = int(row["count"])
        exclusive = float(row["exclusive_seconds"])
        table.append({"stage": stage, "count": count, "inclusive_seconds": row["inclusive_seconds"] if count else None,
                      "exclusive_seconds": exclusive if count else None,
                      "share_of_attributed": round(exclusive / attributed, 6) if attributed > 0 and count > 0 else None,
                      "status": "OBSERVED" if count > 0 else "UNOBSERVED"})
    coverage = footer["coverage"]
    not_covered = sorted(n for n, i in coverage.items() if i["status"] not in RESOLVED_STATUSES)
    late = {n: i["cells_started_at_resolution"] for n, i in coverage.items()
            if i["status"] == RESOLVED_AT_CHECKPOINT and i["cells_started_at_resolution"] > 1}
    hooks = footer["hooks"]
    checkpoints_unarmed = sorted(n for n, i in hooks.items() if i["kind"] == "CHECKPOINT" and i["status"] != "ARMED")
    entries_unarmed = sorted(n for n, i in hooks.items() if i["kind"] == "CELL_ENTRY" and i["status"] != "ARMED")
    entries, checkpoints = _route_counts(hooks)
    checkpoint_routes_missed = sorted(role for role, count in entries.items() if count > checkpoints.get(role, 0))
    full_from_cell = None
    if footer["cells_started"] and not (not_covered or checkpoints_unarmed or entries_unarmed or checkpoint_routes_missed):
        full_from_cell = max(late.values(), default=1)
    fault_total = sum(footer["faults"].values())
    counters = footer["writer_counters"]
    clean_writer = all(counters[k] == 0 for k in ("dropped_full", "write_errors", "short_writes"))
    checks = [
        ("FAULTS", fault_total == 0),
        ("TEARDOWN_INCOMPLETE", footer["teardown"]["complete"] is True),
        ("MONITORING_INACTIVE", footer["monitoring_active_at_stop"] is True),
        ("WRITER_COUNTERS", clean_writer),
        ("TIMING_" + str(timing.get("timing_incomplete_reason")), timing["timing_complete"] is True),
        ("CHECKPOINT_UNAVAILABLE", not checkpoints_unarmed),
        ("CELL_ENTRY_UNAVAILABLE", not entries_unarmed),
        ("NO_CELLS", footer["cells_started"] > 0 or footer["roster_stages"] != expected_map(STAGE_MAP)),
        ("CHECKPOINT_UNREACHED", not checkpoint_routes_missed),
        ("INCOMPLETE_COVERAGE", not not_covered),
        ("LATE_COVERAGE", not late),
    ]
    failed = [name for name, ok in checks if not ok]
    return {"schema_version": SCHEMA_VERSION + ".summary", "writer": footer["writer"], "pid": footer["pid"],
            "observation_interval_seconds": timing.get("observation_interval_seconds"),
            "task_duration_seconds": timing.get("task_duration_seconds"),
            "attributed_exclusive_seconds": attributed, UNATTRIBUTED: timing.get(UNATTRIBUTED),
            "cells_started": footer["cells_started"], "cells_completed": footer["cells_completed"],
            "stages": table, "stage_nesting": footer["stage_nesting"], "not_covered": not_covered, "late_coverage": late,
            "checkpoints_unarmed": checkpoints_unarmed,
            "fully_covered_from_cell_ordinal": full_from_cell,
            "checkpoint_routes_missed": checkpoint_routes_missed,
            "coverage": {"footer_present": True, "complete": not failed, "incomplete_reason": failed[0] if failed else None,
                         "failed_checks": failed, "faults": footer["faults"], "writer_counters": counters,
                         "teardown": footer["teardown"], "targets": coverage, "hooks": footer["hooks"],
                         "clock_sampling_overhead_seconds": footer.get("clock_sampling_overhead_seconds")},
            "scope": "DESCRIPTIVE_ONLY_NO_BUDGET_DECISION"}


__all__ = ["SCHEMA_VERSION", "STAGE_MAP", "CELL_ENTRIES", "CHECKPOINTS", "STAGES", "UNATTRIBUTED", "ENV_DIRECTORY",
           "StageMonitorError", "StageMonitor", "resolve_targets", "target_name", "expected_map", "start",
           "read_monitor_file", "summarize", "RESOLVED_AT_START", "RESOLVED_AT_CHECKPOINT", "UNRESOLVED",
           "LOADED_AFTER_LAST_CHECKPOINT", "MODULE_NEVER_IMPORTED", "PENDING"]
