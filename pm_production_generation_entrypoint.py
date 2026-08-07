"""Optimize-only CLI dispatch under the production Job supervisor.

This module is routing state only.  It carries one production generation into
a fresh process and returns a small, re-openable description of the CLI
result.  It never interprets trading evidence and never grants promotion
authority.
"""

from __future__ import annotations

import ctypes
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import stat
from typing import Any, Callable, Final, Iterator, Mapping, Optional, Sequence

import pm_production_generation_supervisor as generation_supervisor
from pm_storage import _exclusive_file_lock


ENTRYPOINT_REQUEST_SCHEMA: Final = (
    "fxpm.production-generation-entrypoint-request.v1"
)
ENTRYPOINT_RESULT_SCHEMA: Final = (
    "fxpm.production-generation-entrypoint-result.v1"
)
MAX_ARGV_ITEMS: Final = 512
MAX_ARG_BYTES: Final = 32 * 1024
MAX_IDENTITY_FILE_BYTES: Final = 256 * 1024**2
MAX_OWNER_RECORD_BYTES: Final = 16 * 1024
SUPERVISION_DIRECTORY_NAME: Final = (
    ".fxpm-production-generation-supervisor"
)
OWNER_RECORD_NAME: Final = "active-owner.json"
OWNER_RECORD_SCHEMA: Final = "fxpm.production-generation-owner.v1"
PRODUCTION_RUN_LOCK_SUFFIX: Final = ".production-optimization.run.lock"
MAX_SUPERVISION_ENTRIES: Final = 2
OWNER_TEMP_TOKEN_BYTES: Final = 16

_HEX: Final = frozenset("0123456789abcdef")
_RUN_LEASE_CAPABILITY: Final = object()
_REQUEST_FIELDS: Final = frozenset(
    {
        "schema_version",
        "argv",
        "generation_id",
        "active_config_path",
        "winner_ledger_path",
    }
)
_RESULT_FIELDS: Final = frozenset(
    {
        "schema_version",
        "generation_id",
        "active_config_path",
        "winner_ledger_path",
        "cli_exit_code",
        "winner_ledger_sha256",
        "active_manifest_sha256",
    }
)
_OWNER_FIELDS: Final = frozenset(
    {
        "schema_version",
        "generation_id",
        "owner_pid",
        "owner_creation_ticks",
        "active_config_path",
        "winner_ledger_path",
        "content_sha256",
    }
)
_OWNER_BODY_FIELDS: Final = _OWNER_FIELDS - {"content_sha256"}
_STALE_REQUEST_REQUIRED_FIELDS: Final = frozenset(
    {
        "schema_version",
        "request_id",
        "dispatch_path",
        "payload",
        "content_sha256",
    }
)
_RECOVERY_CALLBACK = Callable[[str], str]


class ProductionGenerationEntrypointError(RuntimeError):
    """Typed operational protocol failure; never an economic outcome."""

    def __init__(self, code: str, details: Sequence[Any] = ()) -> None:
        self.code = str(code)
        self.details = tuple(str(value)[:512] for value in tuple(details)[:12])
        suffix = ":" + "|".join(self.details) if self.details else ""
        super().__init__(self.code + suffix)


def _fail(code: str, *details: Any) -> None:
    raise ProductionGenerationEntrypointError(code, details)


def _generation_id(value: Any) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in _HEX for character in value)
    ):
        _fail("PRODUCTION_SUPERVISOR_GENERATION_ID_INVALID")
    return value


def _argv(value: Any) -> tuple[str, ...]:
    if type(value) not in {list, tuple} or not 0 < len(value) <= MAX_ARGV_ITEMS:
        _fail("PRODUCTION_SUPERVISOR_ARGV_INVALID")
    result: list[str] = []
    for item in value:
        if (
            type(item) is not str
            or not item
            or "\x00" in item
            or len(item.encode("utf-8")) > MAX_ARG_BYTES
        ):
            _fail("PRODUCTION_SUPERVISOR_ARGV_INVALID")
        result.append(item)
    if "--optimize" not in result or "--trade" in result:
        _fail("PRODUCTION_SUPERVISOR_ARGV_SCOPE_INVALID")
    return tuple(result)


def _absolute_file_path(
    value: Any,
    code: str,
    *,
    must_exist: bool,
) -> Path:
    if type(value) is not str or not value or value != value.strip():
        _fail(code)
    path = Path(value)
    if not path.is_absolute():
        _fail(code)
    try:
        resolved = path.resolve(strict=must_exist)
    except (OSError, RuntimeError) as exc:
        raise ProductionGenerationEntrypointError(
            code,
            (type(exc).__name__, str(exc)),
        ) from exc
    if must_exist and not resolved.is_file():
        _fail(code)
    return resolved


def _file_sha256(path: Path) -> str:
    target = Path(path)
    if not target.exists():
        return hashlib.sha256(b"").hexdigest()
    try:
        before = target.stat()
        if (
            not target.is_file()
            or before.st_size < 0
            or before.st_size > MAX_IDENTITY_FILE_BYTES
        ):
            _fail("PRODUCTION_SUPERVISOR_IDENTITY_FILE_INVALID", target)
        digest = hashlib.sha256()
        total = 0
        with target.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            while chunk := stream.read(1024 * 1024):
                total += len(chunk)
                if total > MAX_IDENTITY_FILE_BYTES:
                    _fail("PRODUCTION_SUPERVISOR_IDENTITY_FILE_INVALID", target)
                digest.update(chunk)
            after = os.fstat(stream.fileno())
        final = target.stat()
    except ProductionGenerationEntrypointError:
        raise
    except OSError as exc:
        raise ProductionGenerationEntrypointError(
            "PRODUCTION_SUPERVISOR_IDENTITY_FILE_INVALID",
            (str(target), type(exc).__name__),
        ) from exc
    identities = {
        (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns)
        for item in (before, opened, after, final)
    }
    if len(identities) != 1 or total != before.st_size:
        _fail("PRODUCTION_SUPERVISOR_IDENTITY_FILE_CHANGED", target)
    return digest.hexdigest()


def _active_manifest_path(ledger_path: Path) -> Path:
    return ledger_path.with_name(f"{ledger_path.stem}.generation.json")


def _canonical_owner_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        raw = json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError, OverflowError) as exc:
        raise ProductionGenerationEntrypointError(
            "PRODUCTION_SUPERVISOR_OWNER_RECORD_INVALID",
            (type(exc).__name__,),
        ) from exc
    if not 0 < len(raw) <= MAX_OWNER_RECORD_BYTES:
        _fail("PRODUCTION_SUPERVISOR_OWNER_RECORD_SIZE_INVALID")
    return raw


def _owner_digest(body: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_owner_bytes(body)).hexdigest()


def _unique_pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        if key in result:
            _fail("PRODUCTION_SUPERVISOR_OWNER_DUPLICATE_KEY", key)
        result[key] = value
    return result


def _process_creation_ticks(pid: int) -> Optional[int]:
    """Return a stable Windows creation FILETIME, or ``None`` if gone."""

    if type(pid) is not int or type(pid) is bool or pid <= 0:
        _fail("PRODUCTION_SUPERVISOR_OWNER_PID_INVALID")
    if os.name != "nt":
        process_path = Path("/proc") / str(pid)
        try:
            return int(process_path.stat().st_ctime_ns)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ProductionGenerationEntrypointError(
                "PRODUCTION_SUPERVISOR_OWNER_IDENTITY_UNAVAILABLE",
                (pid, type(exc).__name__),
            ) from exc

    from ctypes import wintypes

    process_query_limited_information = 0x1000
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [
        wintypes.DWORD,
        wintypes.BOOL,
        wintypes.DWORD,
    ]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(
        process_query_limited_information,
        False,
        pid,
    )
    if not handle:
        error = ctypes.get_last_error()
        if error in {87, 1168}:
            return None
        _fail(
            "PRODUCTION_SUPERVISOR_OWNER_IDENTITY_UNAVAILABLE",
            pid,
            error,
        )
    creation = wintypes.FILETIME()
    exit_time = wintypes.FILETIME()
    kernel_time = wintypes.FILETIME()
    user_time = wintypes.FILETIME()
    try:
        if not kernel32.GetProcessTimes(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel_time),
            ctypes.byref(user_time),
        ):
            _fail(
                "PRODUCTION_SUPERVISOR_OWNER_IDENTITY_UNAVAILABLE",
                pid,
                ctypes.get_last_error(),
            )
        return (int(creation.dwHighDateTime) << 32) | int(
            creation.dwLowDateTime
        )
    finally:
        kernel32.CloseHandle(handle)


def _owner_record(
    *,
    generation_id: str,
    active_config_path: Path,
    winner_ledger_path: Path,
) -> dict[str, Any]:
    owner_pid = os.getpid()
    creation_ticks = _process_creation_ticks(owner_pid)
    if creation_ticks is None or creation_ticks <= 0:
        _fail("PRODUCTION_SUPERVISOR_OWNER_IDENTITY_UNAVAILABLE")
    body: dict[str, Any] = {
        "schema_version": OWNER_RECORD_SCHEMA,
        "generation_id": _generation_id(generation_id),
        "owner_pid": owner_pid,
        "owner_creation_ticks": creation_ticks,
        "active_config_path": str(active_config_path),
        "winner_ledger_path": str(winner_ledger_path),
    }
    body["content_sha256"] = _owner_digest(body)
    return body


def _write_owner_record(path: Path, value: Mapping[str, Any]) -> None:
    raw = _canonical_owner_bytes(value)
    target = Path(path)
    temporary = target.with_name(
        f".{target.name}.{os.getpid()}."
        f"{secrets.token_hex(OWNER_TEMP_TOKEN_BYTES)}.tmp"
    )
    try:
        if target.exists() or target.is_symlink():
            raise FileExistsError(str(target))
        with temporary.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.rename(temporary, target)
    except OSError as exc:
        raise ProductionGenerationEntrypointError(
            "PRODUCTION_SUPERVISOR_OWNER_RECORD_WRITE_FAILED",
            (type(exc).__name__, str(target)),
        ) from exc
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _is_abandoned_owner_temporary(path: Path, root: Path) -> bool:
    candidate = Path(path)
    expected_root = Path(root).resolve(strict=True)
    prefix = f".{OWNER_RECORD_NAME}."
    suffix = ".tmp"
    name = candidate.name
    if (
        candidate.parent != expected_root
        or not name.startswith(prefix)
        or not name.endswith(suffix)
    ):
        return False
    identity = name[len(prefix) : -len(suffix)]
    fields = identity.split(".")
    if (
        len(fields) != 2
        or not fields[0].isdigit()
        or fields[0][0] not in "123456789"
        or len(fields[1]) != OWNER_TEMP_TOKEN_BYTES * 2
        or any(character not in _HEX for character in fields[1])
    ):
        return False
    try:
        metadata = candidate.lstat()
    except OSError:
        return False
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(
        not stat.S_ISLNK(metadata.st_mode)
        and not (
            int(getattr(metadata, "st_file_attributes", 0) or 0)
            & reparse_flag
        )
        and stat.S_ISREG(metadata.st_mode)
        and metadata.st_size <= MAX_OWNER_RECORD_BYTES
    )


def _read_owner_record(path: Path) -> dict[str, Any]:
    target = Path(path)
    try:
        before = target.lstat()
        reparse_flag = getattr(
            stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
        )
        if (
            stat.S_ISLNK(before.st_mode)
            or int(getattr(before, "st_file_attributes", 0) or 0)
            & reparse_flag
            or not stat.S_ISREG(before.st_mode)
            or not 0 < before.st_size <= MAX_OWNER_RECORD_BYTES
        ):
            _fail("PRODUCTION_SUPERVISOR_OWNER_RECORD_FILE_INVALID")
        raw = target.read_bytes()
        after = target.lstat()
        value = json.loads(
            raw.decode("ascii"),
            object_pairs_hook=_unique_pairs,
            parse_constant=lambda constant: _fail(
                "PRODUCTION_SUPERVISOR_OWNER_NONFINITE", constant
            ),
        )
    except ProductionGenerationEntrypointError:
        raise
    except (OSError, UnicodeError, ValueError) as exc:
        raise ProductionGenerationEntrypointError(
            "PRODUCTION_SUPERVISOR_OWNER_RECORD_READ_FAILED",
            (type(exc).__name__, str(target)),
        ) from exc
    if (
        before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or len(raw) != before.st_size
        or type(value) is not dict
        or set(value) != _OWNER_FIELDS
    ):
        _fail("PRODUCTION_SUPERVISOR_OWNER_RECORD_INVALID")
    body = dict(value)
    digest = body.pop("content_sha256", None)
    if (
        set(body) != _OWNER_BODY_FIELDS
        or body.get("schema_version") != OWNER_RECORD_SCHEMA
        or type(digest) is not str
        or digest != _owner_digest(body)
        or _canonical_owner_bytes(value) != raw
    ):
        _fail("PRODUCTION_SUPERVISOR_OWNER_RECORD_INVALID")
    _generation_id(body.get("generation_id"))
    if (
        type(body.get("owner_pid")) is not int
        or type(body.get("owner_pid")) is bool
        or body["owner_pid"] <= 0
        or type(body.get("owner_creation_ticks")) is not int
        or type(body.get("owner_creation_ticks")) is bool
        or body["owner_creation_ticks"] <= 0
    ):
        _fail("PRODUCTION_SUPERVISOR_OWNER_IDENTITY_INVALID")
    return value


def _supervision_root(winner_ledger_path: Path) -> Path:
    ledger = Path(winner_ledger_path).resolve()
    return ledger.parent / SUPERVISION_DIRECTORY_NAME


def _run_lock_path(winner_ledger_path: Path) -> Path:
    ledger = Path(winner_ledger_path).resolve()
    return ledger.with_name(
        f".{ledger.name}{PRODUCTION_RUN_LOCK_SUFFIX}"
    )


@dataclass(slots=True)
class _ProductionOptimizationRunLease:
    ledger_path: Path
    owner_pid: int
    capability: object
    active: bool = True


@contextmanager
def production_optimization_run_lease(
    winner_ledger_path: Path,
) -> Iterator[_ProductionOptimizationRunLease]:
    """Fail-fast top-level lease for one winner ledger's optimizer tree."""

    ledger_path = _absolute_file_path(
        str(winner_ledger_path),
        "PRODUCTION_SUPERVISOR_LEDGER_PATH_INVALID",
        must_exist=False,
    )
    lock_context = _exclusive_file_lock(
        _run_lock_path(ledger_path),
        timeout_seconds=0.0,
    )
    try:
        lock_context.__enter__()
    except TimeoutError as exc:
        raise ProductionGenerationEntrypointError(
            "PRODUCTION_SUPERVISOR_ALREADY_RUNNING",
            (str(ledger_path),),
        ) from exc
    lease = _ProductionOptimizationRunLease(
        ledger_path=ledger_path,
        owner_pid=os.getpid(),
        capability=_RUN_LEASE_CAPABILITY,
    )
    try:
        yield lease
    finally:
        lease.active = False
        lock_context.__exit__(None, None, None)


def _validate_run_lease(
    lease: Any,
    *,
    winner_ledger_path: Path,
) -> _ProductionOptimizationRunLease:
    if (
        type(lease) is not _ProductionOptimizationRunLease
        or lease.capability is not _RUN_LEASE_CAPABILITY
        or lease.active is not True
        or lease.owner_pid != os.getpid()
        or lease.ledger_path != winner_ledger_path
    ):
        _fail("PRODUCTION_SUPERVISOR_RUN_LEASE_INVALID")
    return lease


def _remove_request_directory(path: Path, expected_root: Path) -> None:
    directory = Path(path)
    root = Path(expected_root).resolve(strict=True)
    if directory.parent != root:
        _fail("PRODUCTION_SUPERVISOR_REQUEST_DIRECTORY_GEOMETRY_INVALID")
    try:
        metadata = directory.lstat()
    except FileNotFoundError:
        return
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    if (
        stat.S_ISLNK(metadata.st_mode)
        or int(getattr(metadata, "st_file_attributes", 0) or 0)
        & reparse_flag
        or not stat.S_ISDIR(metadata.st_mode)
        or directory.resolve(strict=True).parent != root
    ):
        _fail("PRODUCTION_SUPERVISOR_REQUEST_DIRECTORY_INVALID")
    shutil.rmtree(directory)
    if directory.exists() or directory.is_symlink():
        _fail("PRODUCTION_SUPERVISOR_REQUEST_DIRECTORY_REMOVE_FAILED")


def _remove_owner_record(path: Path, expected_root: Path) -> None:
    target = Path(path)
    root = Path(expected_root).resolve(strict=True)
    if target.parent != root:
        _fail("PRODUCTION_SUPERVISOR_OWNER_RECORD_GEOMETRY_INVALID")
    try:
        metadata = target.lstat()
    except FileNotFoundError:
        return
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    if (
        stat.S_ISLNK(metadata.st_mode)
        or int(getattr(metadata, "st_file_attributes", 0) or 0)
        & reparse_flag
        or not stat.S_ISREG(metadata.st_mode)
    ):
        _fail("PRODUCTION_SUPERVISOR_OWNER_RECORD_FILE_INVALID")
    target.unlink()


def _validate_recovery_status(value: Any) -> str:
    if (
        type(value) is not str
        or not value
        or len(value.encode("utf-8")) > 4096
    ):
        _fail("PRODUCTION_SUPERVISOR_RECOVERY_STATUS_INVALID")
    return value


def _reopen_stale_request_envelope(path: Path) -> dict[str, Any]:
    """Authenticate one canonical stale request while ignoring inert extensions."""

    try:
        raw = generation_supervisor._bounded_read(  # noqa: SLF001
            Path(path),
            maximum=generation_supervisor.MAX_REQUEST_BYTES,
        )
        value = generation_supervisor._decode_canonical(  # noqa: SLF001
            raw,
            maximum=generation_supervisor.MAX_REQUEST_BYTES,
        )
        if not _STALE_REQUEST_REQUIRED_FIELDS.issubset(value):
            _fail("PRODUCTION_SUPERVISOR_STALE_REQUEST_INVALID")
        value = generation_supervisor._validate_envelope_digest(  # noqa: SLF001
            value,
            expected_fields=frozenset(value),
            maximum=generation_supervisor.MAX_REQUEST_BYTES,
        )
    except ProductionGenerationEntrypointError:
        raise
    except generation_supervisor.ProductionGenerationSupervisorError as exc:
        raise ProductionGenerationEntrypointError(
            "PRODUCTION_SUPERVISOR_STALE_REQUEST_INVALID",
            (exc.code,),
        ) from exc
    if value.get("schema_version") != generation_supervisor.REQUEST_SCHEMA:
        _fail("PRODUCTION_SUPERVISOR_STALE_REQUEST_INVALID")
    return value


def _recover_stale_request(
    *,
    root: Path,
    active_config_path: Path,
    winner_ledger_path: Path,
    recovery_callback: _RECOVERY_CALLBACK,
) -> Optional[str]:
    entries = list(root.iterdir())
    if len(entries) > MAX_SUPERVISION_ENTRIES:
        _fail("PRODUCTION_SUPERVISOR_RETENTION_BOUND_EXCEEDED")
    if not entries:
        return None
    owner_path = root / OWNER_RECORD_NAME
    if not owner_path.exists() or owner_path.is_symlink():
        if (
            len(entries) == 1
            and _is_abandoned_owner_temporary(entries[0], root)
        ):
            entries[0].unlink()
            return None
        _fail("PRODUCTION_SUPERVISOR_REQUEST_DIRECTORY_INVALID")
    owner = _read_owner_record(owner_path)
    generation_id = _generation_id(owner["generation_id"])
    directory = root / f"generation-{generation_id}"
    allowed = {owner_path}
    if directory.exists() or directory.is_symlink():
        metadata = directory.lstat()
        reparse_flag = getattr(
            stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
        )
        if (
            stat.S_ISLNK(metadata.st_mode)
            or int(
                getattr(metadata, "st_file_attributes", 0) or 0
            )
            & reparse_flag
            or not stat.S_ISDIR(metadata.st_mode)
            or directory.resolve(strict=True).parent
            != root.resolve(strict=True)
        ):
            _fail("PRODUCTION_SUPERVISOR_REQUEST_DIRECTORY_INVALID")
        allowed.add(directory)
    if set(entries) != allowed:
        _fail("PRODUCTION_SUPERVISOR_REQUEST_DIRECTORY_INVALID")
    if (
        owner["active_config_path"] != str(active_config_path)
        or owner["winner_ledger_path"] != str(winner_ledger_path)
    ):
        _fail("PRODUCTION_SUPERVISOR_OWNER_BINDING_INVALID")
    observed_creation = _process_creation_ticks(owner["owner_pid"])
    if observed_creation == owner["owner_creation_ticks"]:
        _fail("PRODUCTION_SUPERVISOR_PRIOR_OWNER_STILL_LIVE")
    request_path = directory / "request.json"
    if directory.exists() and (
        request_path.exists() or request_path.is_symlink()
    ):
        envelope = _reopen_stale_request_envelope(
            request_path
        )
        payload = _validated_request(envelope.get("payload"))
        if (
            envelope.get("request_id") != generation_id
            or envelope.get("dispatch_path")
            != "pm_production_generation_entrypoint:dispatch_optimization_cli"
            or payload["generation_id"] != generation_id
            or payload["active_config_path"] != str(active_config_path)
            or payload["winner_ledger_path"] != str(winner_ledger_path)
        ):
            _fail("PRODUCTION_SUPERVISOR_STALE_REQUEST_BINDING_INVALID")
    recovery = _validate_recovery_status(
        recovery_callback(generation_id)
    )
    if directory.exists() or directory.is_symlink():
        _remove_request_directory(directory, root)
    _remove_owner_record(owner_path, root)
    return recovery


def _request_payload(
    *,
    argv: Sequence[str],
    generation_id: str,
    active_config_path: Path,
    winner_ledger_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": ENTRYPOINT_REQUEST_SCHEMA,
        "argv": list(_argv(tuple(argv))),
        "generation_id": _generation_id(generation_id),
        "active_config_path": str(
            _absolute_file_path(
                str(active_config_path),
                "PRODUCTION_SUPERVISOR_CONFIG_PATH_INVALID",
                must_exist=True,
            )
        ),
        "winner_ledger_path": str(
            _absolute_file_path(
                str(winner_ledger_path),
                "PRODUCTION_SUPERVISOR_LEDGER_PATH_INVALID",
                must_exist=False,
            )
        ),
    }


def _validated_request(payload: Any) -> dict[str, Any]:
    if type(payload) is not dict or set(payload) != _REQUEST_FIELDS:
        _fail("PRODUCTION_SUPERVISOR_REQUEST_FIELDS_INVALID")
    if payload.get("schema_version") != ENTRYPOINT_REQUEST_SCHEMA:
        _fail("PRODUCTION_SUPERVISOR_REQUEST_SCHEMA_INVALID")
    return _request_payload(
        argv=_argv(payload.get("argv")),
        generation_id=_generation_id(payload.get("generation_id")),
        active_config_path=_absolute_file_path(
            payload.get("active_config_path"),
            "PRODUCTION_SUPERVISOR_CONFIG_PATH_INVALID",
            must_exist=True,
        ),
        winner_ledger_path=_absolute_file_path(
            payload.get("winner_ledger_path"),
            "PRODUCTION_SUPERVISOR_LEDGER_PATH_INVALID",
            must_exist=False,
        ),
    )


def dispatch_optimization_cli(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Worker hook: run the normal optimize-only CLI."""

    request = _validated_request(dict(payload))
    from pm_main import main

    exit_code = main(
        list(request["argv"]),
        _supervised_generation_id=request["generation_id"],
    )
    if type(exit_code) is not int or type(exit_code) is bool or exit_code not in {
        0,
        1,
        2,
    }:
        _fail("PRODUCTION_SUPERVISOR_CHILD_EXIT_INVALID", exit_code)
    ledger_path = Path(request["winner_ledger_path"])
    return {
        "schema_version": ENTRYPOINT_RESULT_SCHEMA,
        "generation_id": request["generation_id"],
        "active_config_path": request["active_config_path"],
        "winner_ledger_path": request["winner_ledger_path"],
        "cli_exit_code": exit_code,
        "winner_ledger_sha256": _file_sha256(ledger_path),
        "active_manifest_sha256": _file_sha256(
            _active_manifest_path(ledger_path)
        ),
    }


@dataclass(frozen=True, slots=True)
class ProductionGenerationEntrypointResult:
    """Parent-side result after the complete child process tree is gone."""

    generation_id: str
    request_directory: Path
    supervisor: generation_supervisor.SupervisedDispatchResult
    cli_exit_code: Optional[int]
    stale_recovery_status: Optional[str]
    generation_recovery_status: Optional[str]
    request_directory_retained: bool

    @property
    def succeeded(self) -> bool:
        return (
            self.supervisor.succeeded
            and self.cli_exit_code == 0
        )

    @property
    def safe_to_reconcile(self) -> bool:
        return self.supervisor.safe_to_reconcile


def _validate_child_payload(
    payload: Any,
    *,
    request: Mapping[str, Any],
) -> int:
    if type(payload) is not dict or set(payload) != _RESULT_FIELDS:
        _fail("PRODUCTION_SUPERVISOR_CHILD_RESULT_FIELDS_INVALID")
    exit_code = payload.get("cli_exit_code")
    if (
        payload.get("schema_version") != ENTRYPOINT_RESULT_SCHEMA
        or payload.get("generation_id") != request["generation_id"]
        or payload.get("active_config_path") != request["active_config_path"]
        or payload.get("winner_ledger_path") != request["winner_ledger_path"]
        or type(exit_code) is not int
        or type(exit_code) is bool
        or exit_code not in {0, 1, 2}
    ):
        _fail("PRODUCTION_SUPERVISOR_CHILD_RESULT_BINDING_INVALID")
    ledger_path = Path(request["winner_ledger_path"])
    if (
        payload.get("winner_ledger_sha256") != _file_sha256(ledger_path)
        or payload.get("active_manifest_sha256")
        != _file_sha256(_active_manifest_path(ledger_path))
    ):
        _fail("PRODUCTION_SUPERVISOR_CHILD_LIVE_IDENTITY_MISMATCH")
    return int(exit_code)


def run_supervised_optimization_cli(
    *,
    argv: Sequence[str],
    active_config_path: Path,
    winner_ledger_path: Path,
    recovery_callback: _RECOVERY_CALLBACK,
    generation_id: Optional[str] = None,
    run_lease: Any = None,
) -> ProductionGenerationEntrypointResult:
    """Run the genuine optimize-only CLI once in a contained process tree."""

    if not callable(recovery_callback):
        _fail("PRODUCTION_SUPERVISOR_RECOVERY_CALLBACK_INVALID")
    identity = _generation_id(generation_id or secrets.token_hex(32))
    request = _request_payload(
        argv=argv,
        generation_id=identity,
        active_config_path=Path(active_config_path),
        winner_ledger_path=Path(winner_ledger_path),
    )
    config_path = Path(request["active_config_path"])
    ledger_path = Path(request["winner_ledger_path"])
    root = _supervision_root(ledger_path)
    owned_lease_context = None
    if run_lease is None:
        owned_lease_context = production_optimization_run_lease(
            ledger_path
        )
        run_lease = owned_lease_context.__enter__()
    else:
        _validate_run_lease(
            run_lease,
            winner_ledger_path=ledger_path,
        )
    try:
        root.mkdir(parents=True, exist_ok=True)
        stale_recovery = _recover_stale_request(
            root=root,
            active_config_path=config_path,
            winner_ledger_path=ledger_path,
            recovery_callback=recovery_callback,
        )
        owner_path = root / OWNER_RECORD_NAME
        owner = _owner_record(
            generation_id=identity,
            active_config_path=config_path,
            winner_ledger_path=ledger_path,
        )
        _write_owner_record(owner_path, owner)
        request_directory = root / f"generation-{identity}"
        try:
            request_directory.mkdir(mode=0o700)
        except FileExistsError as exc:
            raise ProductionGenerationEntrypointError(
                "PRODUCTION_SUPERVISOR_REQUEST_DIRECTORY_EXISTS",
                (str(request_directory),),
            ) from exc
        request_path = request_directory / "request.json"
        result_path = request_directory / "result.json"
        supervised = generation_supervisor.run_supervised_dispatch(
            request,
            dispatch_path=(
                "pm_production_generation_entrypoint:"
                "dispatch_optimization_cli"
            ),
            request_path=request_path,
            result_path=result_path,
            request_id=identity,
        )
        child_exit_code: Optional[int] = None
        if supervised.succeeded:
            child_exit_code = _validate_child_payload(
                supervised.payload,
                request=request,
            )
        generation_recovery: Optional[str] = None
        retained = True
        if supervised.safe_to_reconcile:
            generation_recovery = _validate_recovery_status(
                recovery_callback(identity)
            )
            _remove_request_directory(request_directory, root)
            _remove_owner_record(owner_path, root)
            retained = False
        return ProductionGenerationEntrypointResult(
            generation_id=identity,
            request_directory=request_directory,
            supervisor=supervised,
            cli_exit_code=child_exit_code,
            stale_recovery_status=stale_recovery,
            generation_recovery_status=generation_recovery,
            request_directory_retained=retained,
        )
    finally:
        if owned_lease_context is not None:
            owned_lease_context.__exit__(None, None, None)


__all__ = [
    "ENTRYPOINT_REQUEST_SCHEMA",
    "ENTRYPOINT_RESULT_SCHEMA",
    "ProductionGenerationEntrypointError",
    "ProductionGenerationEntrypointResult",
    "dispatch_optimization_cli",
    "production_optimization_run_lease",
    "run_supervised_optimization_cli",
]
