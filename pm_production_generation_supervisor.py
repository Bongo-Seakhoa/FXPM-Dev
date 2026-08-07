"""Process-tree supervision for one production-generation dispatch.

The supervisor deliberately crosses the process boundary with canonical JSON
files and primitive command-line values only.  Application objects, locks,
native-authority objects, and process-local seals are reconstructed by the
dispatch hook in the worker process; none of them are pickled.

On Windows the worker is held behind a stdin gate until it has been assigned
to a kill-on-close Job Object.  Every descendant subsequently created by the
worker belongs to that Job, so an operator interruption can terminate the
whole tree rather than only the direct Python child.
"""

from __future__ import annotations

import argparse
import ctypes
from dataclasses import dataclass
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import signal
import stat
import subprocess
import sys
import time
from typing import Any, Callable, Mapping, Optional, Protocol


REQUEST_SCHEMA = "fxpm.production-generation-supervisor-request.v1"
WORKER_RESULT_SCHEMA = "fxpm.production-generation-worker-result.v1"
RESULT_SCHEMA = "fxpm.production-generation-supervisor-result.v1"

MAX_REQUEST_BYTES = 64 * 1024
MAX_WORKER_RESULT_BYTES = 64 * 1024
MAX_RESULT_BYTES = 64 * 1024
MAX_JSON_DEPTH = 24
MAX_JSON_NODES = 8_192
MAX_STRING_BYTES = 16 * 1024
POLL_SECONDS = 0.05
NORMAL_EXIT_DRAIN_SECONDS = 0.5
DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 10.0
MAX_SHUTDOWN_TIMEOUT_SECONDS = 60.0
_GATE_TOKEN = b"FXPM_SUPERVISOR_GO_V1\n"
_HEX = frozenset("0123456789abcdef")
_DISPATCH_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:"
    r"[A-Za-z_][A-Za-z0-9_]*$"
)

_REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "request_id",
        "dispatch_path",
        "payload",
        "content_sha256",
    }
)
_WORKER_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "request_id",
        "request_sha256",
        "status",
        "payload",
        "error",
        "worker_pid",
        "content_sha256",
    }
)
_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "request_id",
        "request_sha256",
        "status",
        "payload",
        "error",
        "worker_pid",
        "process_exit_code",
        "termination_complete",
        "worker_result_path",
        "content_sha256",
    }
)
_ERROR_FIELDS = frozenset({"code", "type", "message"})


class ProductionGenerationSupervisorError(RuntimeError):
    """Typed supervisor/protocol failure."""

    def __init__(self, code: str, *details: Any) -> None:
        self.code = str(code)
        self.details = tuple(str(value)[:512] for value in details[:8])
        suffix = ":" + "|".join(self.details) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise ProductionGenerationSupervisorError(code, *details)


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in _HEX for character in value)
    )


def _validate_dispatch_path(value: Any) -> str:
    dispatch = str(value or "")
    if len(dispatch) > 256 or _DISPATCH_RE.fullmatch(dispatch) is None:
        _fail("SUPERVISOR_DISPATCH_PATH_INVALID")
    return dispatch


def _validate_shutdown_timeout(value: Any) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.0 < float(value) <= MAX_SHUTDOWN_TIMEOUT_SECONDS
    ):
        _fail("SUPERVISOR_SHUTDOWN_TIMEOUT_INVALID")
    return float(value)


def _validate_json_value(value: Any) -> None:
    work: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while work:
        item, depth = work.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            _fail("SUPERVISOR_JSON_STRUCTURE_UNBOUNDED")
        if type(item) is str:
            if len(item.encode("utf-8")) > MAX_STRING_BYTES:
                _fail("SUPERVISOR_JSON_STRING_UNBOUNDED")
        elif type(item) is float:
            if not math.isfinite(item):
                _fail("SUPERVISOR_JSON_NONFINITE_NUMBER")
        elif type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    _fail("SUPERVISOR_JSON_KEY_INVALID")
                work.append((key, depth + 1))
                work.append((child, depth + 1))
        elif type(item) is list:
            work.extend((child, depth + 1) for child in item)
        elif item is not None and type(item) not in {bool, int}:
            _fail("SUPERVISOR_JSON_VALUE_INVALID", type(item).__name__)


def _canonical_bytes(value: Any, *, maximum: int) -> bytes:
    _validate_json_value(value)
    try:
        raw = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError, OverflowError) as exc:
        raise ProductionGenerationSupervisorError(
            "SUPERVISOR_JSON_CANONICALIZATION_FAILED",
            type(exc).__name__,
            str(exc),
        ) from exc
    if not 0 < len(raw) <= maximum:
        _fail("SUPERVISOR_JSON_SIZE_INVALID", len(raw), maximum)
    return raw


def _content_sha256(body: Mapping[str, Any], *, maximum: int) -> str:
    return hashlib.sha256(_canonical_bytes(dict(body), maximum=maximum)).hexdigest()


def _with_content_sha(body: Mapping[str, Any], *, maximum: int) -> dict[str, Any]:
    result = dict(body)
    result["content_sha256"] = _content_sha256(result, maximum=maximum)
    _canonical_bytes(result, maximum=maximum)
    return result


def _pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        if key in result:
            _fail("SUPERVISOR_JSON_DUPLICATE_KEY", key)
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    _fail("SUPERVISOR_JSON_NONFINITE_CONSTANT", value)


def _decode_canonical(raw: bytes, *, maximum: int) -> dict[str, Any]:
    if not 0 < len(raw) <= maximum:
        _fail("SUPERVISOR_JSON_SIZE_INVALID", len(raw), maximum)
    try:
        value = json.loads(
            raw.decode("ascii"),
            object_pairs_hook=_pairs,
            parse_constant=_reject_constant,
        )
    except ProductionGenerationSupervisorError:
        raise
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ProductionGenerationSupervisorError(
            "SUPERVISOR_JSON_DECODE_FAILED", type(exc).__name__
        ) from exc
    if type(value) is not dict:
        _fail("SUPERVISOR_JSON_OBJECT_REQUIRED")
    canonical = _canonical_bytes(value, maximum=maximum)
    if canonical != raw:
        _fail("SUPERVISOR_JSON_NOT_CANONICAL")
    return value


def _validate_envelope_digest(
    value: Mapping[str, Any],
    *,
    expected_fields: frozenset[str],
    maximum: int,
) -> dict[str, Any]:
    payload = dict(value)
    if set(payload) != expected_fields:
        _fail("SUPERVISOR_ENVELOPE_FIELDS_INVALID")
    digest = payload.pop("content_sha256", None)
    if not _is_sha256(digest) or digest != _content_sha256(payload, maximum=maximum):
        _fail("SUPERVISOR_ENVELOPE_DIGEST_INVALID")
    payload["content_sha256"] = digest
    return payload


def _bounded_error(code: str, error_type: str, message: str) -> dict[str, str]:
    def limited(value: Any, maximum: int) -> str:
        text = str(value or "")
        encoded = text.encode("utf-8", errors="replace")
        if len(encoded) <= maximum:
            return text
        return encoded[:maximum].decode("utf-8", errors="ignore")

    return {
        "code": limited(code, 256),
        "type": limited(error_type, 256),
        "message": limited(message, 2_048),
    }


def _validate_error(value: Any, *, required: bool) -> Optional[dict[str, str]]:
    if value is None:
        if required:
            _fail("SUPERVISOR_ERROR_REQUIRED")
        return None
    if type(value) is not dict or set(value) != _ERROR_FIELDS:
        _fail("SUPERVISOR_ERROR_INVALID")
    result = {key: str(value[key] or "") for key in _ERROR_FIELDS}
    if not result["code"] or not result["type"]:
        _fail("SUPERVISOR_ERROR_INVALID")
    _validate_json_value(result)
    return result


def _require_regular_parent(path: Path) -> Path:
    absolute = Path(os.path.abspath(path))
    parent = absolute.parent
    try:
        metadata = parent.stat()
    except OSError as exc:
        raise ProductionGenerationSupervisorError(
            "SUPERVISOR_PATH_PARENT_UNAVAILABLE", str(parent), type(exc).__name__
        ) from exc
    if not stat.S_ISDIR(metadata.st_mode):
        _fail("SUPERVISOR_PATH_PARENT_NOT_DIRECTORY", parent)
    return absolute


def _exclusive_write(path: Path, raw: bytes) -> None:
    target = _require_regular_parent(path)
    temporary = target.with_name(
        f".{target.name}.{os.getpid()}.{secrets.token_hex(16)}.tmp"
    )
    try:
        if target.exists() or target.is_symlink():
            _fail("SUPERVISOR_OUTPUT_ALREADY_EXISTS", str(target))
        with temporary.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.rename(temporary, target)
        except FileExistsError as exc:
            raise ProductionGenerationSupervisorError(
                "SUPERVISOR_OUTPUT_ALREADY_EXISTS", str(target)
            ) from exc
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _bounded_read(path: Path, *, maximum: int) -> bytes:
    target = _require_regular_parent(path)
    try:
        before = target.stat()
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > maximum
        ):
            _fail("SUPERVISOR_INPUT_FILE_INVALID", str(target))
        with target.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            raw = stream.read(maximum + 1)
            after = os.fstat(stream.fileno())
        final = target.stat()
    except ProductionGenerationSupervisorError:
        raise
    except OSError as exc:
        raise ProductionGenerationSupervisorError(
            "SUPERVISOR_INPUT_READ_FAILED", str(target), type(exc).__name__
        ) from exc
    identities = {
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns),
        (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns),
        (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
        (final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns),
    }
    if len(identities) != 1 or len(raw) != before.st_size:
        _fail("SUPERVISOR_INPUT_CHANGED_DURING_READ", str(target))
    return raw


def _build_request(
    *,
    request_id: str,
    dispatch_path: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not _is_sha256(request_id):
        _fail("SUPERVISOR_REQUEST_ID_INVALID")
    if type(payload) is not dict:
        _fail("SUPERVISOR_REQUEST_PAYLOAD_INVALID")
    body = {
        "schema_version": REQUEST_SCHEMA,
        "request_id": request_id,
        "dispatch_path": _validate_dispatch_path(dispatch_path),
        "payload": dict(payload),
    }
    return _with_content_sha(body, maximum=MAX_REQUEST_BYTES)


def _read_request(path: Path, *, expected_file_sha256: str) -> dict[str, Any]:
    if not _is_sha256(expected_file_sha256):
        _fail("SUPERVISOR_REQUEST_FILE_SHA256_INVALID")
    raw = _bounded_read(path, maximum=MAX_REQUEST_BYTES)
    if hashlib.sha256(raw).hexdigest() != expected_file_sha256:
        _fail("SUPERVISOR_REQUEST_FILE_SHA256_MISMATCH")
    value = _decode_canonical(raw, maximum=MAX_REQUEST_BYTES)
    value = _validate_envelope_digest(
        value,
        expected_fields=_REQUEST_FIELDS,
        maximum=MAX_REQUEST_BYTES,
    )
    if value.get("schema_version") != REQUEST_SCHEMA:
        _fail("SUPERVISOR_REQUEST_SCHEMA_INVALID")
    if not _is_sha256(value.get("request_id")):
        _fail("SUPERVISOR_REQUEST_ID_INVALID")
    value["dispatch_path"] = _validate_dispatch_path(value.get("dispatch_path"))
    if type(value.get("payload")) is not dict:
        _fail("SUPERVISOR_REQUEST_PAYLOAD_INVALID")
    return value


def reopen_supervised_request(path: Path) -> dict[str, Any]:
    """Strictly reopen one durable request without trusting caller metadata."""

    raw = _bounded_read(Path(path), maximum=MAX_REQUEST_BYTES)
    return _read_request(
        Path(path),
        expected_file_sha256=hashlib.sha256(raw).hexdigest(),
    )


def _build_worker_result(
    *,
    request_id: str,
    request_sha256: str,
    status: str,
    payload: Mapping[str, Any],
    error: Optional[Mapping[str, Any]],
    worker_pid: int,
) -> dict[str, Any]:
    if not _is_sha256(request_id) or not _is_sha256(request_sha256):
        _fail("SUPERVISOR_WORKER_RESULT_BINDING_INVALID")
    normalized_status = str(status or "").upper()
    if normalized_status not in {"SUCCEEDED", "FAILED"}:
        _fail("SUPERVISOR_WORKER_RESULT_STATUS_INVALID")
    if type(payload) is not dict:
        _fail("SUPERVISOR_WORKER_RESULT_PAYLOAD_INVALID")
    normalized_error = _validate_error(
        None if error is None else dict(error),
        required=normalized_status == "FAILED",
    )
    if normalized_status == "SUCCEEDED" and normalized_error is not None:
        _fail("SUPERVISOR_WORKER_RESULT_ERROR_UNEXPECTED")
    body = {
        "schema_version": WORKER_RESULT_SCHEMA,
        "request_id": request_id,
        "request_sha256": request_sha256,
        "status": normalized_status,
        "payload": dict(payload),
        "error": normalized_error,
        "worker_pid": int(worker_pid),
    }
    if body["worker_pid"] <= 0:
        _fail("SUPERVISOR_WORKER_PID_INVALID")
    return _with_content_sha(body, maximum=MAX_WORKER_RESULT_BYTES)


def _read_worker_result(
    path: Path,
    *,
    expected_request_id: str,
    expected_request_sha256: str,
    expected_worker_pid: int,
) -> dict[str, Any]:
    value = _decode_canonical(
        _bounded_read(path, maximum=MAX_WORKER_RESULT_BYTES),
        maximum=MAX_WORKER_RESULT_BYTES,
    )
    value = _validate_envelope_digest(
        value,
        expected_fields=_WORKER_RESULT_FIELDS,
        maximum=MAX_WORKER_RESULT_BYTES,
    )
    if value.get("schema_version") != WORKER_RESULT_SCHEMA:
        _fail("SUPERVISOR_WORKER_RESULT_SCHEMA_INVALID")
    if (
        value.get("request_id") != expected_request_id
        or value.get("request_sha256") != expected_request_sha256
        or value.get("worker_pid") != expected_worker_pid
    ):
        _fail("SUPERVISOR_WORKER_RESULT_BINDING_INVALID")
    status = str(value.get("status") or "").upper()
    if status not in {"SUCCEEDED", "FAILED"}:
        _fail("SUPERVISOR_WORKER_RESULT_STATUS_INVALID")
    if type(value.get("payload")) is not dict:
        _fail("SUPERVISOR_WORKER_RESULT_PAYLOAD_INVALID")
    value["error"] = _validate_error(
        value.get("error"), required=status == "FAILED"
    )
    if status == "SUCCEEDED" and value["error"] is not None:
        _fail("SUPERVISOR_WORKER_RESULT_ERROR_UNEXPECTED")
    return value


def _build_final_result(
    *,
    request_id: str,
    request_sha256: str,
    status: str,
    payload: Mapping[str, Any],
    error: Optional[Mapping[str, Any]],
    worker_pid: int,
    process_exit_code: Optional[int],
    termination_complete: bool,
    worker_result_path: Path,
) -> dict[str, Any]:
    normalized_status = str(status or "").upper()
    allowed = {
        "SUCCEEDED",
        "FAILED",
        "TERMINATION_FAILED",
        "CHILD_PROTOCOL_FAILED",
    }
    if normalized_status not in allowed:
        _fail("SUPERVISOR_RESULT_STATUS_INVALID")
    if type(payload) is not dict:
        _fail("SUPERVISOR_RESULT_PAYLOAD_INVALID")
    normalized_error = _validate_error(
        None if error is None else dict(error),
        required=normalized_status != "SUCCEEDED",
    )
    if normalized_status == "SUCCEEDED" and normalized_error is not None:
        _fail("SUPERVISOR_RESULT_ERROR_UNEXPECTED")
    body = {
        "schema_version": RESULT_SCHEMA,
        "request_id": request_id,
        "request_sha256": request_sha256,
        "status": normalized_status,
        "payload": dict(payload),
        "error": normalized_error,
        "worker_pid": int(worker_pid),
        "process_exit_code": (
            None if process_exit_code is None else int(process_exit_code)
        ),
        "termination_complete": bool(termination_complete),
        "worker_result_path": str(Path(worker_result_path)),
    }
    return _with_content_sha(body, maximum=MAX_RESULT_BYTES)


def read_supervisor_result(path: Path) -> "SupervisedDispatchResult":
    """Read and validate one terminal supervisor result file."""

    value = _decode_canonical(
        _bounded_read(path, maximum=MAX_RESULT_BYTES),
        maximum=MAX_RESULT_BYTES,
    )
    value = _validate_envelope_digest(
        value,
        expected_fields=_RESULT_FIELDS,
        maximum=MAX_RESULT_BYTES,
    )
    if value.get("schema_version") != RESULT_SCHEMA:
        _fail("SUPERVISOR_RESULT_SCHEMA_INVALID")
    if not _is_sha256(value.get("request_id")) or not _is_sha256(
        value.get("request_sha256")
    ):
        _fail("SUPERVISOR_RESULT_BINDING_INVALID")
    status = str(value.get("status") or "").upper()
    if status not in {
        "SUCCEEDED",
        "FAILED",
        "TERMINATION_FAILED",
        "CHILD_PROTOCOL_FAILED",
    }:
        _fail("SUPERVISOR_RESULT_STATUS_INVALID")
    if type(value.get("payload")) is not dict:
        _fail("SUPERVISOR_RESULT_PAYLOAD_INVALID")
    error = _validate_error(value.get("error"), required=status != "SUCCEEDED")
    if status == "SUCCEEDED" and error is not None:
        _fail("SUPERVISOR_RESULT_ERROR_UNEXPECTED")
    worker_pid = value.get("worker_pid")
    exit_code = value.get("process_exit_code")
    if (
        type(worker_pid) is not int
        or worker_pid <= 0
        or (exit_code is not None and type(exit_code) is not int)
        or type(value.get("termination_complete")) is not bool
        or type(value.get("worker_result_path")) is not str
    ):
        _fail("SUPERVISOR_RESULT_TYPES_INVALID")
    return SupervisedDispatchResult(
        request_id=value["request_id"],
        request_sha256=value["request_sha256"],
        status=status,
        payload=dict(value["payload"]),
        error=error,
        worker_pid=worker_pid,
        process_exit_code=exit_code,
        termination_complete=value["termination_complete"],
        worker_result_path=Path(value["worker_result_path"]),
        result_path=Path(path),
    )


@dataclass(frozen=True)
class SupervisedDispatchResult:
    """Bounded parent-side terminal result."""

    request_id: str
    request_sha256: str
    status: str
    payload: dict[str, Any]
    error: Optional[dict[str, str]]
    worker_pid: int
    process_exit_code: Optional[int]
    termination_complete: bool
    worker_result_path: Path
    result_path: Path

    @property
    def succeeded(self) -> bool:
        return self.status == "SUCCEEDED" and self.termination_complete

    @property
    def safe_to_reconcile(self) -> bool:
        """Recovery may start only after the complete process tree is gone."""

        return self.termination_complete


class _ProcessTreeGuard(Protocol):
    def assign(self, pid: int) -> None: ...

    def terminate(self, exit_code: int) -> None: ...

    def active_processes(self) -> int: ...

    def close(self) -> None: ...


if os.name == "nt":

    class _JobBasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", ctypes.c_uint32),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", ctypes.c_uint32),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", ctypes.c_uint32),
            ("SchedulingClass", ctypes.c_uint32),
        ]


    class _IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]


    class _JobExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _JobBasicLimitInformation),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]


    class _JobBasicAccountingInformation(ctypes.Structure):
        _fields_ = [
            ("TotalUserTime", ctypes.c_longlong),
            ("TotalKernelTime", ctypes.c_longlong),
            ("ThisPeriodTotalUserTime", ctypes.c_longlong),
            ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
            ("TotalPageFaultCount", ctypes.c_uint32),
            ("TotalProcesses", ctypes.c_uint32),
            ("ActiveProcesses", ctypes.c_uint32),
            ("TotalTerminatedProcesses", ctypes.c_uint32),
        ]


class _WindowsJob:
    """One kill-on-close Windows Job Object."""

    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
    _JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION = 1
    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
    _PROCESS_TERMINATE = 0x0001
    _PROCESS_SET_QUOTA = 0x0100
    _PROCESS_QUERY_LIMITED_INFORMATION = 0x1000

    def __init__(self) -> None:
        if os.name != "nt":
            _fail("SUPERVISOR_WINDOWS_JOB_UNAVAILABLE")
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self._kernel32 = kernel32
        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
        kernel32.CreateJobObjectW.restype = ctypes.c_void_p
        kernel32.SetInformationJobObject.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_uint32,
        ]
        kernel32.SetInformationJobObject.restype = ctypes.c_int
        kernel32.OpenProcess.argtypes = [
            ctypes.c_uint32,
            ctypes.c_int,
            ctypes.c_uint32,
        ]
        kernel32.OpenProcess.restype = ctypes.c_void_p
        kernel32.AssignProcessToJobObject.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
        ]
        kernel32.AssignProcessToJobObject.restype = ctypes.c_int
        kernel32.TerminateJobObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        kernel32.TerminateJobObject.restype = ctypes.c_int
        kernel32.QueryInformationJobObject.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        kernel32.QueryInformationJobObject.restype = ctypes.c_int
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_int

        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            self._raise_last_error("SUPERVISOR_JOB_CREATE_FAILED")
        self._handle: Optional[int] = int(handle)
        limits = _JobExtendedLimitInformation()
        limits.BasicLimitInformation.LimitFlags = (
            self._JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        )
        if not kernel32.SetInformationJobObject(
            ctypes.c_void_p(self._handle),
            self._JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(limits),
            ctypes.sizeof(limits),
        ):
            configure_error = ctypes.get_last_error()
            try:
                self.close()
            finally:
                self._raise_last_error(
                    "SUPERVISOR_JOB_CONFIGURE_FAILED", configure_error
                )

    def _raise_last_error(self, code: str, error: Optional[int] = None) -> None:
        if error is None:
            error = ctypes.get_last_error()
        raise ProductionGenerationSupervisorError(code, error)

    def _required_handle(self) -> int:
        if self._handle is None:
            _fail("SUPERVISOR_JOB_ALREADY_CLOSED")
        return self._handle

    def assign(self, pid: int) -> None:
        handle = self._required_handle()
        access = (
            self._PROCESS_TERMINATE
            | self._PROCESS_SET_QUOTA
            | self._PROCESS_QUERY_LIMITED_INFORMATION
        )
        process_handle = self._kernel32.OpenProcess(access, False, int(pid))
        if not process_handle:
            self._raise_last_error("SUPERVISOR_JOB_OPEN_PROCESS_FAILED")
        try:
            if not self._kernel32.AssignProcessToJobObject(
                ctypes.c_void_p(handle), process_handle
            ):
                self._raise_last_error("SUPERVISOR_JOB_ASSIGN_FAILED")
        finally:
            self._kernel32.CloseHandle(process_handle)

    def terminate(self, exit_code: int) -> None:
        if not self._kernel32.TerminateJobObject(
            ctypes.c_void_p(self._required_handle()),
            ctypes.c_uint32(int(exit_code) & 0xFFFFFFFF),
        ):
            self._raise_last_error("SUPERVISOR_JOB_TERMINATE_FAILED")

    def active_processes(self) -> int:
        accounting = _JobBasicAccountingInformation()
        if not self._kernel32.QueryInformationJobObject(
            ctypes.c_void_p(self._required_handle()),
            self._JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION,
            ctypes.byref(accounting),
            ctypes.sizeof(accounting),
            None,
        ):
            self._raise_last_error("SUPERVISOR_JOB_QUERY_FAILED")
        return int(accounting.ActiveProcesses)

    def close(self) -> None:
        handle = self._handle
        self._handle = None
        if handle is not None:
            if not self._kernel32.CloseHandle(ctypes.c_void_p(handle)):
                self._raise_last_error("SUPERVISOR_JOB_CLOSE_FAILED")


class _PosixProcessGroup:
    """Portable development/test fallback; production Windows uses a Job."""

    def __init__(self) -> None:
        self._pid: Optional[int] = None

    def assign(self, pid: int) -> None:
        self._pid = int(pid)
        if os.getpgid(self._pid) != self._pid:
            _fail("SUPERVISOR_PROCESS_GROUP_ASSIGN_FAILED")

    def terminate(self, exit_code: int) -> None:
        del exit_code
        if self._pid is not None:
            try:
                os.killpg(self._pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def active_processes(self) -> int:
        if self._pid is None:
            return 0
        try:
            os.killpg(self._pid, 0)
        except ProcessLookupError:
            return 0
        return 1

    def close(self) -> None:
        self._pid = None


def _create_process_tree_guard() -> _ProcessTreeGuard:
    return _WindowsJob() if os.name == "nt" else _PosixProcessGroup()


def _spawn_worker(
    command: list[str],
    *,
    environment: Mapping[str, str],
) -> subprocess.Popen[bytes]:
    kwargs: dict[str, Any] = {
        "stdin": subprocess.PIPE,
        "env": dict(environment),
        "shell": False,
        "close_fds": True,
    }
    if os.name == "nt":
        kwargs["creationflags"] = int(
            getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(command, **kwargs)


def _release_worker(process: subprocess.Popen[bytes]) -> None:
    stream = process.stdin
    if stream is None:
        _fail("SUPERVISOR_WORKER_GATE_UNAVAILABLE")
    stream.write(_GATE_TOKEN)
    stream.flush()
    stream.close()


def _wait_for_process(
    process: subprocess.Popen[bytes],
) -> int:
    """Wait indefinitely while remaining responsive to operator interruption."""

    while True:
        exit_code = process.poll()
        if exit_code is not None:
            return int(exit_code)
        time.sleep(POLL_SECONDS)


def _wait_guard_empty(
    guard: _ProcessTreeGuard,
    *,
    cutoff_monotonic: float,
) -> bool:
    while True:
        try:
            if guard.active_processes() == 0:
                return True
        except Exception:
            return False
        if time.monotonic() >= cutoff_monotonic:
            return False
        time.sleep(
            min(
                POLL_SECONDS,
                max(0.0, cutoff_monotonic - time.monotonic()),
            )
        )


def _bounded_wait_after_termination(
    process: subprocess.Popen[bytes],
    *,
    cutoff_monotonic: float,
) -> Optional[int]:
    remaining = max(0.0, cutoff_monotonic - time.monotonic())
    try:
        return int(process.wait(timeout=remaining))
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except OSError:
            pass
    remaining = max(0.0, cutoff_monotonic - time.monotonic())
    try:
        return int(process.wait(timeout=remaining))
    except subprocess.TimeoutExpired:
        return None


def _stop_unassigned_worker(
    process: subprocess.Popen[bytes],
    *,
    cutoff_monotonic: float,
) -> tuple[Optional[int], bool]:
    try:
        process.terminate()
    except OSError:
        pass
    exit_code = _bounded_wait_after_termination(
        process,
        cutoff_monotonic=cutoff_monotonic,
    )
    return exit_code, exit_code is not None


def _shutdown_cutoff(
    *,
    shutdown_timeout_seconds: float,
) -> float:
    return time.monotonic() + shutdown_timeout_seconds


def _worker_result_path(result_path: Path, request_id: str) -> Path:
    target = _require_regular_parent(result_path)
    return target.with_name(f".{target.name}.worker-{request_id}.json")


def run_supervised_dispatch(
    payload: Mapping[str, Any],
    *,
    dispatch_path: str,
    request_path: Path,
    result_path: Path,
    request_id: Optional[str] = None,
    shutdown_timeout_seconds: float = DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
    python_executable: Optional[str] = None,
    environment: Optional[Mapping[str, str]] = None,
) -> SupervisedDispatchResult:
    """Run one importable dispatch hook in a contained process tree.

    The hook must be a top-level ``module:function`` callable accepting
    ``payload`` and returning a plain JSON object.
    """

    if type(payload) is not dict:
        _fail("SUPERVISOR_REQUEST_PAYLOAD_INVALID")
    shutdown_timeout = _validate_shutdown_timeout(shutdown_timeout_seconds)
    identity = request_id or secrets.token_hex(32)
    if not _is_sha256(identity):
        _fail("SUPERVISOR_REQUEST_ID_INVALID")
    request_target = _require_regular_parent(request_path)
    result_target = _require_regular_parent(result_path)
    worker_target = _worker_result_path(result_target, identity)
    if len({request_target, result_target, worker_target}) != 3:
        _fail("SUPERVISOR_PATHS_NOT_DISTINCT")
    for target in (request_target, result_target, worker_target):
        if target.exists() or target.is_symlink():
            _fail("SUPERVISOR_OUTPUT_ALREADY_EXISTS", str(target))

    request = _build_request(
        request_id=identity,
        dispatch_path=dispatch_path,
        payload=payload,
    )
    request_raw = _canonical_bytes(request, maximum=MAX_REQUEST_BYTES)
    request_sha = hashlib.sha256(request_raw).hexdigest()
    _exclusive_write(request_target, request_raw)

    command = [
        str(python_executable or sys.executable),
        str(Path(__file__).resolve()),
        "--worker",
        "--request",
        str(request_target),
        "--request-sha256",
        request_sha,
        "--worker-result",
        str(worker_target),
    ]
    child_environment = dict(os.environ)
    if environment is not None:
        child_environment.update(
            {str(key): str(value) for key, value in environment.items()}
        )
    child_environment["PYTHONUNBUFFERED"] = "1"

    guard: Optional[_ProcessTreeGuard] = None
    process: Optional[subprocess.Popen[bytes]] = None
    termination_complete = False
    exit_code: Optional[int] = None
    terminal_status = "FAILED"
    terminal_payload: dict[str, Any] = {}
    terminal_error: Optional[dict[str, str]] = None
    try:
        guard = _create_process_tree_guard()
        process = _spawn_worker(command, environment=child_environment)
        try:
            guard.assign(int(process.pid))
        except BaseException as exc:
            shutdown_cutoff = _shutdown_cutoff(
                shutdown_timeout_seconds=shutdown_timeout,
            )
            exit_code, termination_complete = _stop_unassigned_worker(
                process,
                cutoff_monotonic=shutdown_cutoff,
            )
            terminal_error = _bounded_error(
                "SUPERVISOR_JOB_ASSIGN_FAILED",
                type(exc).__name__,
                str(exc),
            )
        else:
            _release_worker(process)
            exit_code = _wait_for_process(process)
            normal_drain_cutoff = (
                time.monotonic() + NORMAL_EXIT_DRAIN_SECONDS
            )
            termination_complete = _wait_guard_empty(
                guard,
                cutoff_monotonic=normal_drain_cutoff,
            )
            if not termination_complete:
                shutdown_cutoff = _shutdown_cutoff(
                    shutdown_timeout_seconds=shutdown_timeout,
                )
                try:
                    guard.terminate(0xE1)
                except BaseException as exc:
                    terminal_error = _bounded_error(
                        "SUPERVISOR_DESCENDANT_TERMINATE_FAILED",
                        type(exc).__name__,
                        str(exc),
                    )
                termination_complete = _wait_guard_empty(
                    guard,
                    cutoff_monotonic=shutdown_cutoff,
                )
                terminal_status = (
                    "FAILED" if termination_complete else "TERMINATION_FAILED"
                )
                terminal_error = terminal_error or _bounded_error(
                    "SUPERVISOR_DESCENDANTS_REMAINED",
                    "ProcessTerminationError",
                    "worker exited while descendant processes remained active",
                )
            else:
                try:
                    worker_result = _read_worker_result(
                        worker_target,
                        expected_request_id=identity,
                        expected_request_sha256=request_sha,
                        expected_worker_pid=int(process.pid),
                    )
                except BaseException as exc:
                    terminal_status = "CHILD_PROTOCOL_FAILED"
                    terminal_error = _bounded_error(
                        "SUPERVISOR_WORKER_RESULT_INVALID",
                        type(exc).__name__,
                        str(exc),
                    )
                else:
                    terminal_payload = dict(worker_result["payload"])
                    if (
                        exit_code == 0
                        and worker_result["status"] == "SUCCEEDED"
                    ):
                        terminal_status = "SUCCEEDED"
                        terminal_error = None
                    else:
                        terminal_status = "FAILED"
                        terminal_error = worker_result["error"] or _bounded_error(
                            "SUPERVISOR_WORKER_EXIT_FAILED",
                            "ChildProcessError",
                            f"worker exited with code {exit_code}",
                        )
    except BaseException as exc:
        if process is not None and process.poll() is None:
            shutdown_cutoff = _shutdown_cutoff(
                shutdown_timeout_seconds=shutdown_timeout,
            )
            if guard is not None:
                try:
                    guard.terminate(0xE2)
                except BaseException:
                    pass
                termination_complete = _wait_guard_empty(
                    guard,
                    cutoff_monotonic=shutdown_cutoff,
                )
            exit_code = _bounded_wait_after_termination(
                process,
                cutoff_monotonic=shutdown_cutoff,
            )
            termination_complete = bool(
                termination_complete and exit_code is not None
            )
        terminal_status = (
            "FAILED" if termination_complete else "TERMINATION_FAILED"
        )
        terminal_error = _bounded_error(
            "SUPERVISOR_RUNTIME_FAILED",
            type(exc).__name__,
            str(exc),
        )
    finally:
        if guard is not None:
            try:
                guard.close()
            except Exception:
                termination_complete = False
                if terminal_status == "SUCCEEDED":
                    terminal_status = "TERMINATION_FAILED"
                    terminal_error = _bounded_error(
                        "SUPERVISOR_JOB_CLOSE_FAILED",
                        "ProcessTerminationError",
                        "process-tree guard could not be closed safely",
                    )

    if process is None:
        _fail("SUPERVISOR_WORKER_NOT_STARTED")
    final = _build_final_result(
        request_id=identity,
        request_sha256=request_sha,
        status=terminal_status,
        payload=terminal_payload,
        error=terminal_error,
        worker_pid=int(process.pid),
        process_exit_code=exit_code,
        termination_complete=termination_complete,
        worker_result_path=worker_target,
    )
    _exclusive_write(
        result_target,
        _canonical_bytes(final, maximum=MAX_RESULT_BYTES),
    )
    return read_supervisor_result(result_target)


def _load_dispatch(path: str) -> Callable[..., Mapping[str, Any]]:
    dispatch_path = _validate_dispatch_path(path)
    module_name, function_name = dispatch_path.split(":", 1)
    module = importlib.import_module(module_name)
    function = getattr(module, function_name, None)
    if not callable(function):
        _fail("SUPERVISOR_DISPATCH_NOT_CALLABLE")
    return function


def _run_worker(
    *,
    request_path: Path,
    request_sha256: str,
    worker_result_path: Path,
) -> int:
    request = _read_request(
        request_path,
        expected_file_sha256=request_sha256,
    )
    gate = sys.stdin.buffer.read(len(_GATE_TOKEN))
    if gate != _GATE_TOKEN:
        return 91

    status = "SUCCEEDED"
    payload: dict[str, Any] = {}
    error: Optional[dict[str, str]] = None
    try:
        dispatch = _load_dispatch(request["dispatch_path"])
        returned = dispatch(dict(request["payload"]))
        if type(returned) is not dict:
            _fail("SUPERVISOR_DISPATCH_RESULT_INVALID")
        payload = dict(returned)
    except BaseException as exc:
        status = "FAILED"
        code = getattr(exc, "code", "SUPERVISOR_DISPATCH_FAILED")
        error = _bounded_error(str(code), type(exc).__name__, str(exc))

    result = _build_worker_result(
        request_id=request["request_id"],
        request_sha256=request_sha256,
        status=status,
        payload=payload,
        error=error,
        worker_pid=os.getpid(),
    )
    _exclusive_write(
        worker_result_path,
        _canonical_bytes(result, maximum=MAX_WORKER_RESULT_BYTES),
    )
    return 0 if status == "SUCCEEDED" else 2


def _worker_main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--request", required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--worker-result", required=True)
    args = parser.parse_args(argv)
    if not args.worker:
        return 90
    return _run_worker(
        request_path=Path(args.request),
        request_sha256=str(args.request_sha256),
        worker_result_path=Path(args.worker_result),
    )


__all__ = [
    "DEFAULT_SHUTDOWN_TIMEOUT_SECONDS",
    "MAX_REQUEST_BYTES",
    "MAX_RESULT_BYTES",
    "ProductionGenerationSupervisorError",
    "SupervisedDispatchResult",
    "read_supervisor_result",
    "run_supervised_dispatch",
    "reopen_supervised_request",
]


if __name__ == "__main__":
    raise SystemExit(_worker_main())
