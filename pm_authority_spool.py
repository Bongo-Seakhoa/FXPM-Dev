"""Immutable, bounded transport for optimizer authority evidence."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
import os
import re
import stat
import unicodedata
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator, Mapping, Optional

import pm_authority_spool_quota as spool_quota


LEGACY_GENERATION_CONTEXT_SCHEMA = "fxpm.authority-spool-generation-context.v1"
GENERATION_CONTEXT_SCHEMA = "fxpm.authority-spool-generation-context.v2"
ARTIFACT_DESCRIPTOR_SCHEMA = "fxpm.authority-spool-artifact-descriptor.v1"
SYMBOL_MANIFEST_SCHEMA = "fxpm.authority-spool-symbol-manifest.v2"
COMPACT_RECEIPT_SCHEMA = "fxpm.optimizer-authority-spool-receipt.v3"

GENERAL_MANIFEST_DOMAIN = "GENERAL"
HISTORICAL_ANCHOR_MANIFEST_DOMAIN = "HISTORICAL_ANCHOR"
CURRENT_REFIT_MANIFEST_DOMAIN = "CURRENT_REFIT"

CANONICAL_JSON_FORMAT = "canonical-json"
CONTEXT_RELATIVE_PATH = "generation-context.json"
WORKSPACE_OWNER_RELATIVE_PATH = "generation-workspace.json"
RETIREMENT_RELATIVE_PATH = spool_quota.GENERATION_TERMINAL_NAME
RETIREMENT_INTENT_RELATIVE_PATH = "generation-retirement.intent.json"
RETIREMENT_COMPLETE_RELATIVE_PATH = "generation-archived.json"
WORKSPACE_OWNER_SCHEMA = "fxpm.owned-generation-workspace.v1"
RETIREMENT_SCHEMA = "fxpm.completed-generation-retirement.v1"
RETIREMENT_SCHEMA_V2 = "fxpm.completed-generation-retirement.v2"
RETIREMENT_SEGMENT_SCHEMA = "fxpm.completed-generation-retirement-segment.v1"
MAX_RETIREMENT_SEGMENT_RECORDS = 181_817
TERMINAL_REFUSAL_FENCE_SCHEMA = "fxpm.declared-m5-terminal-refusal-fence.v1"
TERMINAL_REFUSAL_RESULT_KIND = "TERMINAL_REFUSAL_SOURCE_NOT_COMPLETED"
POST_SOURCE_ABORT_FENCE_SCHEMA = "fxpm.declared-m5-post-source-abort-fence.v1"
POST_SOURCE_ABORT_RESULT_KIND = "POST_SOURCE_ABORT_SOURCE_NOT_COMPLETED"
NATIVE_COMPLETION_FENCE_SCHEMA = "fxpm.declared-broker-native-completion-fence.v1"
POST_SOURCE_ABORT_GRID_SOURCE_FIELDS = ("symbol", "timeframe", "activation_sha256", "bars_sha256",
    "sidecar_sha256", "provenance_sha256", "request_sha256", "spread_unit")
DEFAULT_STREAM_CHUNK_BYTES = 1024 * 1024
_RESERVATION_WINDOW_BYTES = 16 * DEFAULT_STREAM_CHUNK_BYTES
MAX_CONTEXT_BYTES = 64 * 1024
MAX_RECEIPT_BYTES = 64 * 1024
MAX_JSON_DEPTH = 64
MAX_JSON_NODES = 2_000_000
MAX_JSON_STRING_BYTES = 4 * 1024 * 1024

_HEX = frozenset("0123456789abcdef")
_FORMAT = re.compile(r"^[a-z0-9][a-z0-9._+-]{0,63}$")
_MANIFEST_DOMAINS = frozenset(
    {
        GENERAL_MANIFEST_DOMAIN,
        HISTORICAL_ANCHOR_MANIFEST_DOMAIN,
        CURRENT_REFIT_MANIFEST_DOMAIN,
    }
)
_DESCRIPTOR_FIELDS = frozenset(
    {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
)
_CONTEXT_FIELDS = frozenset(
    {
        "schema_version",
        "generation_id",
        "native_universe_sha256",
        "config_sha256",
        "limits",
    }
)
_LEGACY_LIMIT_FIELDS = frozenset(
    {
        "max_artifact_bytes",
        "max_symbol_bytes",
        "max_artifacts_per_symbol",
        "max_manifest_bytes",
        "max_json_bytes",
        "stream_chunk_bytes",
    }
)
_LIMIT_FIELDS = frozenset(
    {*_LEGACY_LIMIT_FIELDS, "max_generation_bytes", "min_free_bytes"}
)
_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "generation_id",
        "symbol",
        "manifest_domain",
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "artifact_count",
        "artifact_total_bytes",
        "artifact_roster_sha256",
        "artifacts",
    }
)
_ROSTER_FIELDS = frozenset({"artifact_id", "artifact_type", "descriptor"})
_RECEIPT_FIELDS = frozenset(
    {
        "schema_version",
        "generation_id",
        "symbol",
        "manifest_domain",
        "native_universe_sha256",
        "config_sha256",
        "generation_context_sha256",
        "symbol_manifest",
    }
)


class AuthoritySpoolError(ValueError):
    """Fail-closed spool error."""


def is_resource_capacity_error(error: BaseException) -> bool:
    """Recognize only the preserved typed disk-reserve failure, without I/O."""
    current = error
    seen = set()
    while isinstance(current, BaseException) and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, spool_quota.AuthoritySpoolQuotaError):
            return current.code == "SPOOL_QUOTA_FREE_SPACE_RESERVE_BREACHED"
        cause = current.__cause__
        if isinstance(current, AuthoritySpoolError):
            if (not isinstance(cause, spool_quota.AuthoritySpoolQuotaError)
                    or str(current) != str(cause)):
                return False
        current = cause
    return False


@dataclass(frozen=True)
class SpoolLimits:
    max_artifact_bytes: int = 64 * 1024**3
    max_symbol_bytes: int = 512 * 1024**3
    max_artifacts_per_symbol: int = 1_000_000
    max_manifest_bytes: int = 128 * 1024**2
    max_json_bytes: int = 64 * 1024**2
    stream_chunk_bytes: int = DEFAULT_STREAM_CHUNK_BYTES
    max_generation_bytes: int = 512 * 1024**3
    min_free_bytes: int = 0


@dataclass(frozen=True)
class GenerationSpoolContext:
    root: Path
    generation_id: str
    native_universe_sha256: str
    config_sha256: str
    context_sha256: str
    limits: SpoolLimits
    writable: bool


def _fail(code: str) -> None:
    raise AuthoritySpoolError(code)


def _is_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(char in _HEX for char in value)
    )


def _manifest_domain(value: Any, name: str = "MANIFEST_DOMAIN") -> str:
    if type(value) is not str or value not in _MANIFEST_DOMAINS:
        _fail(f"{name}_INVALID")
    return value


def _identity(value: Any, name: str, *, maximum_bytes: int = 512) -> str:
    if type(value) is not str or not value or value != value.strip():
        _fail(f"{name}_INVALID")
    if unicodedata.normalize("NFC", value) != value:
        _fail(f"{name}_INVALID")
    encoded = value.encode("utf-8")
    if len(encoded) > maximum_bytes or any(ord(char) < 32 or ord(char) == 127 for char in value):
        _fail(f"{name}_INVALID")
    return value


def _sha256(value: Any, name: str) -> str:
    if not _is_sha256(value):
        _fail(f"{name}_INVALID")
    return str(value)


def _positive_int(value: Any, name: str) -> int:
    if type(value) is not int or value <= 0:
        _fail(f"{name}_INVALID")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    if type(value) is not int or value < 0:
        _fail(f"{name}_INVALID")
    return value


def _validate_limits(limits: SpoolLimits) -> SpoolLimits:
    if type(limits) is not SpoolLimits:
        _fail("SPOOL_LIMITS_INVALID")
    values = asdict(limits)
    for name, value in values.items():
        if name == "min_free_bytes":
            _nonnegative_int(value, name.upper())
        else:
            _positive_int(value, name.upper())
    if limits.max_json_bytes > limits.max_artifact_bytes:
        _fail("MAX_JSON_BYTES_EXCEEDS_ARTIFACT_BOUND")
    if limits.max_manifest_bytes > limits.max_artifact_bytes:
        _fail("MAX_MANIFEST_BYTES_EXCEEDS_ARTIFACT_BOUND")
    if limits.stream_chunk_bytes > limits.max_artifact_bytes:
        _fail("STREAM_CHUNK_BYTES_EXCEEDS_ARTIFACT_BOUND")
    if limits.max_artifact_bytes > limits.max_generation_bytes:
        _fail("MAX_ARTIFACT_BYTES_EXCEEDS_GENERATION_BOUND")
    if limits.max_symbol_bytes > limits.max_generation_bytes:
        _fail("MAX_SYMBOL_BYTES_EXCEEDS_GENERATION_BOUND")
    return limits


def validate_spool_limits(limits: SpoolLimits) -> SpoolLimits:
    """Public exact-type validator used before production derives tighter limits."""

    return _validate_limits(limits)


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail(f"JSON_DUPLICATE_KEY:{key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    _fail(f"JSON_NONFINITE_CONSTANT:{value}")


def _bounded_json_value(value: Any) -> None:
    work: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while work:
        item, depth = work.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            _fail("JSON_STRUCTURE_UNBOUNDED")
        if type(item) is str:
            if len(item.encode("utf-8")) > MAX_JSON_STRING_BYTES:
                _fail("JSON_STRING_UNBOUNDED")
        elif type(item) is float:
            if not math.isfinite(item):
                _fail("JSON_NONFINITE_NUMBER")
        elif type(item) is int:
            if item.bit_length() > 4096:
                _fail("JSON_INTEGER_UNBOUNDED")
        elif type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    _fail("JSON_KEY_TYPE_INVALID")
                work.append((key, depth + 1))
                work.append((child, depth + 1))
        elif type(item) is list:
            work.extend((child, depth + 1) for child in item)
        elif item is not None and type(item) is not bool:
            _fail("JSON_VALUE_TYPE_INVALID")


def _canonical_json_bytes(value: Any, maximum_bytes: int) -> bytes:
    _bounded_json_value(value)
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise AuthoritySpoolError("JSON_CANONICALIZATION_FAILED") from exc
    if not 0 < len(encoded) <= maximum_bytes:
        _fail("JSON_SIZE_BOUND_EXCEEDED")
    return encoded


def _strict_json_bytes(encoded: bytes, maximum_bytes: int) -> Any:
    if not 0 < len(encoded) <= maximum_bytes:
        _fail("JSON_SIZE_BOUND_EXCEEDED")
    try:
        value = json.loads(
            encoded.decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_constant=_reject_constant,
        )
    except AuthoritySpoolError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise AuthoritySpoolError("JSON_INVALID") from exc
    if _canonical_json_bytes(value, maximum_bytes) != encoded:
        _fail("JSON_NOT_CANONICAL")
    return value


def _is_reparse(path: Path) -> bool:
    info = path.lstat()
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    return path.is_symlink() or bool(attributes & 0x400)


def _reject_reparse_chain(path: Path) -> None:
    absolute = Path(os.path.abspath(path))
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current = current / part
        if current.exists() or current.is_symlink():
            if _is_reparse(current):
                _fail("PATH_REPARSE_POINT_FORBIDDEN")


def _validate_root(root: Path, *, create: bool) -> Path:
    candidate = Path(root)
    if not candidate.is_absolute():
        _fail("SPOOL_ROOT_NOT_ABSOLUTE")
    candidate = Path(os.path.abspath(candidate))
    _reject_reparse_chain(candidate.parent)
    if not candidate.exists():
        if not create:
            _fail("SPOOL_ROOT_MISSING")
        try:
            os.mkdir(candidate)
        except FileExistsError:
            pass
    _reject_reparse_chain(candidate)
    try:
        info = candidate.lstat()
    except OSError as exc:
        raise AuthoritySpoolError("SPOOL_ROOT_UNAVAILABLE") from exc
    if _is_reparse(candidate) or not stat.S_ISDIR(info.st_mode):
        _fail("SPOOL_ROOT_NOT_SAFE_DIRECTORY")
    return candidate


def _relative_parts(relative_path: str) -> tuple[str, ...]:
    if type(relative_path) is not str or not relative_path:
        _fail("RELATIVE_PATH_INVALID")
    if "\\" in relative_path or "\x00" in relative_path:
        _fail("RELATIVE_PATH_INVALID")
    path = PurePosixPath(relative_path)
    if path.is_absolute() or str(path) != relative_path:
        _fail("RELATIVE_PATH_INVALID")
    if any(part in {"", ".", ".."} for part in path.parts):
        _fail("RELATIVE_PATH_INVALID")
    if any(re.fullmatch(r"[A-Za-z0-9._+-]+", part) is None for part in path.parts):
        _fail("RELATIVE_PATH_INVALID")
    return path.parts


def _local_path(root: Path, relative_path: str) -> Path:
    parts = _relative_parts(relative_path)
    candidate = root.joinpath(*parts)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise AuthoritySpoolError("PATH_OUTSIDE_SPOOL_ROOT") from exc
    return candidate


def _ensure_directory(root: Path, relative_path: str) -> Path:
    root = _validate_root(root, create=False)
    current = root
    for part in _relative_parts(relative_path):
        current = current / part
        try:
            os.mkdir(current)
        except FileExistsError:
            pass
        _reject_reparse_chain(current)
        info = current.lstat()
        if _is_reparse(current) or not stat.S_ISDIR(info.st_mode):
            _fail("PATH_COMPONENT_NOT_SAFE_DIRECTORY")
    return current


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    flags = os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0))
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _open_exclusive(path: Path) -> int:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | int(getattr(os, "O_BINARY", 0))
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise AuthoritySpoolError("TEMPORARY_FILE_COLLISION") from exc
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode):
        os.close(descriptor)
        _fail("TEMPORARY_FILE_NOT_REGULAR")
    return descriptor


def _write_all(descriptor: int, data: memoryview) -> None:
    offset = 0
    while offset < len(data):
        written = os.write(descriptor, data[offset:])
        if written <= 0:
            _fail("ARTIFACT_WRITE_INCOMPLETE")
        offset += written


def _file_identity(info: os.stat_result) -> tuple[int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def _safe_regular_file(root: Path, relative_path: str) -> Path:
    root = _validate_root(root, create=False)
    candidate = _local_path(root, relative_path)
    _reject_reparse_chain(candidate)
    try:
        info = candidate.lstat()
    except OSError as exc:
        raise AuthoritySpoolError("ARTIFACT_FILE_MISSING") from exc
    if _is_reparse(candidate) or not stat.S_ISREG(info.st_mode):
        _fail("ARTIFACT_FILE_NOT_SAFE_REGULAR")
    return candidate


def _stream_file(
    root: Path,
    relative_path: str,
    *,
    maximum_bytes: int,
    expected_size: Optional[int] = None,
    collect: bool = False,
    chunk_bytes: int = DEFAULT_STREAM_CHUNK_BYTES,
) -> tuple[str, int, Optional[bytes]]:
    path = _safe_regular_file(root, relative_path)
    path_before = path.lstat()
    flags = os.O_RDONLY | int(getattr(os, "O_BINARY", 0)) | int(
        getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags)
    digest = hashlib.sha256()
    total = 0
    chunks: Optional[list[bytes]] = [] if collect else None
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            _fail("ARTIFACT_FILE_NOT_REGULAR")
        if not 0 < before.st_size <= maximum_bytes:
            _fail("ARTIFACT_SIZE_BOUND_EXCEEDED")
        if expected_size is not None and before.st_size != expected_size:
            _fail("ARTIFACT_SIZE_MISMATCH")
        while True:
            chunk = os.read(descriptor, min(chunk_bytes, maximum_bytes + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                _fail("ARTIFACT_SIZE_BOUND_EXCEEDED")
            digest.update(chunk)
            if chunks is not None:
                chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    path_after = path.lstat()
    if (
        _file_identity(path_before) != _file_identity(before)
        or _file_identity(before) != _file_identity(after)
        or _file_identity(after) != _file_identity(path_after)
        or total != before.st_size
    ):
        _fail("ARTIFACT_CHANGED_DURING_READ")
    return digest.hexdigest(), total, b"".join(chunks) if chunks is not None else None


def _files_match(root: Path, relative_path: str, digest: str, size: int, chunk: int) -> bool:
    try:
        observed_hash, observed_size, _ = _stream_file(
            root,
            relative_path,
            maximum_bytes=size,
            expected_size=size,
            chunk_bytes=chunk,
        )
    except AuthoritySpoolError:
        return False
    return observed_hash == digest and observed_size == size


def _publish_temp_exclusive(root: Path, temp: Path, relative_path: str) -> None:
    target = _local_path(root, relative_path)
    parent = PurePosixPath(relative_path).parent.as_posix()
    if parent != ".":
        _ensure_directory(root, parent)
    try:
        os.link(temp, target, follow_symlinks=False)
    except FileExistsError:
        raise
    except OSError as exc:
        raise AuthoritySpoolError("ATOMIC_EXCLUSIVE_PUBLISH_FAILED") from exc
    _fsync_directory(target.parent)


def _temporary_path(root: Path) -> Path:
    directory = _ensure_directory(root, "tmp")
    return directory / f"{uuid.uuid4().hex}.part"


def _cancel_clean_reservation(
    lease: spool_quota.PublicationLease,
    *,
    temp: Optional[Path],
    target: Optional[Path],
    target_linked: bool,
) -> None:
    """Release only when cleanup proves no transaction-owned inode remains."""

    published = target_linked
    if not published and temp is not None and target is not None:
        try:
            published = os.path.samefile(temp, target)
        except OSError:
            pass
    cleaned = temp is None
    if temp is not None:
        try:
            temp.unlink(missing_ok=True)
            cleaned = not temp.exists()
        except OSError:
            cleaned = False
    if cleaned and not published:
        try:
            lease.cancel_after_cleanup()
        except spool_quota.AuthoritySpoolQuotaError:
            # The durable reservation remains fail-closed if cancellation
            # itself cannot be persisted; preserve the publication root cause.
            pass


def _write_streamed_object(
    context: GenerationSpoolContext,
    chunks: Iterable[bytes],
    *,
    format_name: str,
) -> dict[str, Any]:
    _validate_format(format_name)
    _assert_context_current(context)
    if context.writable is not True:
        _fail("GENERATION_CONTEXT_READ_ONLY")
    try:
        with spool_quota.publication_lease(
            context.root,
            max_generation_bytes=context.limits.max_generation_bytes,
            min_free_bytes=context.limits.min_free_bytes,
        ) as lease:
            temp = _temporary_path(context.root)
            descriptor = _open_exclusive(temp)
            digest = hashlib.sha256()
            total = 0
            pending = bytearray()
            target: Optional[Path] = None
            target_linked = False
            reserved_available = 0
            unreported_materialized = 0
            buffer_limit = min(
                context.limits.stream_chunk_bytes,
                DEFAULT_STREAM_CHUNK_BYTES,
            )

            def flush_pending() -> None:
                nonlocal reserved_available, unreported_materialized
                if not pending:
                    return
                encoded = bytes(pending)
                needed = max(0, len(encoded) - reserved_available)
                if needed:
                    remaining = (
                        context.limits.max_artifact_bytes
                        - lease.in_progress_bytes
                    )
                    requested = min(
                        remaining,
                        max(needed, _RESERVATION_WINDOW_BYTES),
                    )
                    try:
                        lease.consume(requested)
                    except spool_quota.AuthoritySpoolQuotaError as exc:
                        if (
                            requested == needed
                            or exc.code
                            not in {
                                "SPOOL_QUOTA_GENERATION_BYTES_EXCEEDED",
                                "SPOOL_QUOTA_FREE_SPACE_RESERVE_BREACHED",
                            }
                        ):
                            raise
                        lease.consume(needed)
                        requested = needed
                    reserved_available += requested
                _write_all(descriptor, memoryview(encoded))
                reserved_available -= len(encoded)
                unreported_materialized += len(encoded)
                if unreported_materialized >= _RESERVATION_WINDOW_BYTES:
                    lease.materialize(unreported_materialized)
                    unreported_materialized = 0
                pending.clear()

            try:
                try:
                    for raw in chunks:
                        if type(raw) not in {bytes, bytearray, memoryview}:
                            _fail("ARTIFACT_CHUNK_TYPE_INVALID")
                        view = memoryview(raw)
                        offset = 0
                        while offset < len(view):
                            available = (
                                buffer_limit - len(pending)
                            )
                            piece = view[
                                offset : offset + available
                            ]
                            projected = total + len(piece)
                            if projected > context.limits.max_artifact_bytes:
                                _fail("ARTIFACT_SIZE_BOUND_EXCEEDED")
                            pending.extend(piece)
                            digest.update(piece)
                            total = projected
                            offset += len(piece)
                            if (
                                len(pending) == buffer_limit
                            ):
                                flush_pending()
                    flush_pending()
                    if total <= 0:
                        _fail("ARTIFACT_EMPTY")
                    if unreported_materialized:
                        lease.materialize(unreported_materialized)
                        unreported_materialized = 0
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                content_hash = digest.hexdigest()
                relative_path = _content_relative_path(
                    content_hash, format_name
                )
                target = _local_path(context.root, relative_path)
                new_unique_bytes = 0
                if target.exists():
                    if not _files_match(
                        context.root,
                        relative_path,
                        content_hash,
                        total,
                        context.limits.stream_chunk_bytes,
                    ):
                        _fail("CONTENT_ADDRESS_CONFLICT")
                else:
                    try:
                        _publish_temp_exclusive(
                            context.root, temp, relative_path
                        )
                        target_linked = True
                        new_unique_bytes = total
                    except FileExistsError:
                        if not _files_match(
                            context.root,
                            relative_path,
                            content_hash,
                            total,
                            context.limits.stream_chunk_bytes,
                        ):
                            _fail("CONTENT_ADDRESS_CONFLICT")
                try:
                    temp.unlink()
                except OSError as exc:
                    raise AuthoritySpoolError(
                        "TEMPORARY_FILE_CLEANUP_FAILED"
                    ) from exc
                lease.commit(new_unique_bytes)
                return {
                    "schema_version": ARTIFACT_DESCRIPTOR_SCHEMA,
                    "relative_path": relative_path,
                    "sha256": content_hash,
                    "size_bytes": total,
                    "format": format_name,
                }
            except BaseException:
                _cancel_clean_reservation(
                    lease,
                    temp=temp,
                    target=target,
                    target_linked=target_linked,
                )
                raise
            finally:
                try:
                    temp.unlink(missing_ok=True)
                except OSError:
                    pass
    except spool_quota.AuthoritySpoolQuotaError as exc:
        raise AuthoritySpoolError(str(exc)) from exc


def _publish_exact_json(
    context: GenerationSpoolContext,
    relative_path: str,
    value: Any,
    maximum_bytes: int,
) -> dict[str, Any]:
    if (
        type(context) is not GenerationSpoolContext
        or context.writable is not True
    ):
        _fail("GENERATION_CONTEXT_READ_ONLY")
    encoded = _canonical_json_bytes(value, maximum_bytes)
    digest = hashlib.sha256(encoded).hexdigest()
    target = _local_path(context.root, relative_path)
    try:
        with spool_quota.publication_lease(
            context.root,
            max_generation_bytes=context.limits.max_generation_bytes,
            min_free_bytes=context.limits.min_free_bytes,
        ) as lease:
            temp: Optional[Path] = None
            target_linked = False
            try:
                new_unique_bytes = 0
                if target.exists():
                    observed_hash, observed_size, observed = _stream_file(
                        context.root,
                        relative_path,
                        maximum_bytes=maximum_bytes,
                        collect=True,
                        chunk_bytes=context.limits.stream_chunk_bytes,
                    )
                    if (
                        observed_hash != digest
                        or observed_size != len(encoded)
                        or observed != encoded
                    ):
                        _fail("IMMUTABLE_FILE_CONFLICT")
                else:
                    lease.consume(len(encoded))
                    temp = _temporary_path(context.root)
                    descriptor = _open_exclusive(temp)
                    try:
                        _write_all(descriptor, memoryview(encoded))
                        lease.materialize(len(encoded))
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
                    try:
                        _publish_temp_exclusive(
                            context.root, temp, relative_path
                        )
                        target_linked = True
                        new_unique_bytes = len(encoded)
                    except FileExistsError:
                        observed_hash, observed_size, observed = _stream_file(
                            context.root,
                            relative_path,
                            maximum_bytes=maximum_bytes,
                            expected_size=len(encoded),
                            collect=True,
                            chunk_bytes=context.limits.stream_chunk_bytes,
                        )
                        if (
                            observed_hash != digest
                            or observed_size != len(encoded)
                            or observed != encoded
                        ):
                            _fail("IMMUTABLE_FILE_CONFLICT")
                    try:
                        temp.unlink()
                    except OSError as exc:
                        raise AuthoritySpoolError(
                            "TEMPORARY_FILE_CLEANUP_FAILED"
                        ) from exc
                lease.commit(new_unique_bytes)
            except BaseException:
                _cancel_clean_reservation(
                    lease,
                    temp=temp,
                    target=target,
                    target_linked=target_linked,
                )
                raise
            finally:
                if temp is not None:
                    try:
                        temp.unlink(missing_ok=True)
                    except OSError:
                        pass
    except spool_quota.AuthoritySpoolQuotaError as exc:
        raise AuthoritySpoolError(str(exc)) from exc
    return {
        "schema_version": ARTIFACT_DESCRIPTOR_SCHEMA,
        "relative_path": relative_path,
        "sha256": digest,
        "size_bytes": len(encoded),
        "format": CANONICAL_JSON_FORMAT,
    }


def _validate_format(value: Any) -> str:
    if type(value) is not str or not _FORMAT.fullmatch(value):
        _fail("ARTIFACT_FORMAT_INVALID")
    return value


def _content_relative_path(digest: str, format_name: str) -> str:
    _sha256(digest, "CONTENT_SHA256")
    _validate_format(format_name)
    return f"objects/sha256/{digest[:2]}/{digest}.{format_name}"


def _manifest_relative_path(symbol: str, manifest_domain: str) -> str:
    domain = _manifest_domain(manifest_domain)
    key = hashlib.sha256(symbol.encode("utf-8")).hexdigest()
    domain_key = hashlib.sha256(domain.encode("ascii")).hexdigest()
    return f"symbols/{key}/domains/{domain_key}/manifest.json"


def _validate_descriptor(
    value: Any,
    *,
    expected_path: Optional[str] = None,
    content_addressed: bool,
) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _DESCRIPTOR_FIELDS:
        _fail("ARTIFACT_DESCRIPTOR_FIELDS_INVALID")
    if value.get("schema_version") != ARTIFACT_DESCRIPTOR_SCHEMA:
        _fail("ARTIFACT_DESCRIPTOR_SCHEMA_INVALID")
    relative_path = str(value.get("relative_path") or "")
    _relative_parts(relative_path)
    digest = _sha256(value.get("sha256"), "ARTIFACT_SHA256")
    size = _positive_int(value.get("size_bytes"), "ARTIFACT_SIZE_BYTES")
    format_name = _validate_format(value.get("format"))
    if expected_path is not None and relative_path != expected_path:
        _fail("ARTIFACT_DESCRIPTOR_PATH_MISMATCH")
    if content_addressed and relative_path != _content_relative_path(digest, format_name):
        _fail("CONTENT_ADDRESS_PATH_MISMATCH")
    return {
        "schema_version": ARTIFACT_DESCRIPTOR_SCHEMA,
        "relative_path": relative_path,
        "sha256": digest,
        "size_bytes": size,
        "format": format_name,
    }


def _limits_from_payload(
    value: Any,
    *,
    legacy: bool,
) -> SpoolLimits:
    expected_fields = _LEGACY_LIMIT_FIELDS if legacy else _LIMIT_FIELDS
    if type(value) is not dict or set(value) != expected_fields:
        _fail("SPOOL_LIMIT_FIELDS_INVALID")
    try:
        limits = SpoolLimits(
            **value,
            **(
                {
                    "max_generation_bytes": SpoolLimits().max_generation_bytes,
                    "min_free_bytes": 0,
                }
                if legacy
                else {}
            ),
        )
    except TypeError as exc:
        raise AuthoritySpoolError("SPOOL_LIMITS_INVALID") from exc
    return _validate_limits(limits)


def freeze_generation_context(
    root: Path,
    *,
    generation_id: str,
    native_universe_sha256: str,
    config_sha256: str,
    limits: Optional[SpoolLimits] = None,
    owned_workspace: Optional[Mapping[str, Any]] = None,
) -> GenerationSpoolContext:
    """Create or byte-identically reopen a generation context before workers."""

    safe_root = _validate_root(Path(root), create=True)
    workspace = None
    workspace_root_identity = None
    if owned_workspace is not None:
        if (type(owned_workspace) is not dict
                or set(owned_workspace) != {"cell_id", "cell_contract_sha256"}
                or not all(_is_sha256(value) for value in owned_workspace.values())):
            _fail("WORKSPACE_OWNER_DECLARATION_INVALID")
        if any(safe_root.iterdir()):
            _fail("WORKSPACE_ROOT_NOT_EMPTY")
        workspace = dict(owned_workspace)
        info = safe_root.stat(follow_symlinks=False)
        workspace_root_identity = [info.st_dev, info.st_ino]
    generation = _identity(generation_id, "GENERATION_ID")
    native_hash = _sha256(native_universe_sha256, "NATIVE_UNIVERSE_SHA256")
    config_hash = _sha256(config_sha256, "CONFIG_SHA256")
    active_limits = _validate_limits(limits or SpoolLimits())
    payload = {
        "schema_version": GENERATION_CONTEXT_SCHEMA,
        "generation_id": generation,
        "native_universe_sha256": native_hash,
        "config_sha256": config_hash,
        "limits": asdict(active_limits),
    }
    encoded = _canonical_json_bytes(payload, MAX_CONTEXT_BYTES)
    bootstrap = GenerationSpoolContext(
        root=safe_root,
        generation_id=generation,
        native_universe_sha256=native_hash,
        config_sha256=config_hash,
        context_sha256=hashlib.sha256(encoded).hexdigest(),
        limits=active_limits,
        writable=True,
    )
    if workspace is not None:
        _assert_retirement_root(safe_root, workspace_root_identity)
    _publish_exact_json(bootstrap, CONTEXT_RELATIVE_PATH, payload, MAX_CONTEXT_BYTES)
    if workspace is not None:
        _assert_retirement_root(safe_root, workspace_root_identity)
        _publish_exact_json(bootstrap, WORKSPACE_OWNER_RELATIVE_PATH, {
            "schema_version": WORKSPACE_OWNER_SCHEMA, "generation_id": generation,
            "generation_context_sha256": bootstrap.context_sha256,
            "creation_provenance": "ABSENT_OR_EMPTY_BEFORE_CONTEXT_CREATION", **workspace,
            "root": str(safe_root), "root_identity": workspace_root_identity,
        }, MAX_CONTEXT_BYTES)
    return reopen_generation_context(
        safe_root,
        expected_generation_id=generation,
        expected_native_universe_sha256=native_hash,
        expected_config_sha256=config_hash,
    )


def reopen_generation_context(
    root: Path,
    *,
    expected_generation_id: Optional[str] = None,
    expected_native_universe_sha256: Optional[str] = None,
    expected_config_sha256: Optional[str] = None,
) -> GenerationSpoolContext:
    """Strictly reopen the immutable generation context."""

    safe_root = _validate_root(Path(root), create=False)
    try:
        spool_quota.assert_generation_not_terminal(safe_root)
    except spool_quota.AuthoritySpoolQuotaError as exc:
        raise AuthoritySpoolError("GENERATION_WORKSPACE_TERMINAL") from exc
    digest, _, encoded = _stream_file(
        safe_root,
        CONTEXT_RELATIVE_PATH,
        maximum_bytes=MAX_CONTEXT_BYTES,
        collect=True,
    )
    assert encoded is not None
    payload = _strict_json_bytes(encoded, MAX_CONTEXT_BYTES)
    if type(payload) is not dict or set(payload) != _CONTEXT_FIELDS:
        _fail("GENERATION_CONTEXT_FIELDS_INVALID")
    schema_version = payload.get("schema_version")
    if schema_version not in {
        LEGACY_GENERATION_CONTEXT_SCHEMA,
        GENERATION_CONTEXT_SCHEMA,
    }:
        _fail("GENERATION_CONTEXT_SCHEMA_INVALID")
    generation = _identity(payload.get("generation_id"), "GENERATION_ID")
    native_hash = _sha256(
        payload.get("native_universe_sha256"), "NATIVE_UNIVERSE_SHA256"
    )
    config_hash = _sha256(payload.get("config_sha256"), "CONFIG_SHA256")
    legacy = schema_version == LEGACY_GENERATION_CONTEXT_SCHEMA
    active_limits = _limits_from_payload(
        payload.get("limits"),
        legacy=legacy,
    )
    if expected_generation_id is not None and generation != expected_generation_id:
        _fail("GENERATION_CONTEXT_IDENTITY_MISMATCH")
    if (
        expected_native_universe_sha256 is not None
        and native_hash != expected_native_universe_sha256
    ):
        _fail("GENERATION_CONTEXT_NATIVE_MISMATCH")
    if expected_config_sha256 is not None and config_hash != expected_config_sha256:
        _fail("GENERATION_CONTEXT_CONFIG_MISMATCH")
    return GenerationSpoolContext(
        root=safe_root,
        generation_id=generation,
        native_universe_sha256=native_hash,
        config_sha256=config_hash,
        context_sha256=digest,
        limits=active_limits,
        writable=not legacy,
    )


def _assert_context_current(context: GenerationSpoolContext) -> None:
    if type(context) is not GenerationSpoolContext:
        _fail("GENERATION_CONTEXT_OBJECT_INVALID")
    observed = reopen_generation_context(
        context.root,
        expected_generation_id=context.generation_id,
        expected_native_universe_sha256=context.native_universe_sha256,
        expected_config_sha256=context.config_sha256,
    )
    if (
        observed.context_sha256 != context.context_sha256
        or observed.limits != context.limits
        or observed.writable is not context.writable
    ):
        _fail("GENERATION_CONTEXT_CHANGED")


def write_canonical_json_artifact(
    context: GenerationSpoolContext,
    value: Any,
) -> dict[str, Any]:
    """Persist one canonical JSON artifact by content hash."""

    encoded = _canonical_json_bytes(value, context.limits.max_json_bytes)
    return _write_streamed_object(
        context,
        (encoded,),
        format_name=CANONICAL_JSON_FORMAT,
    )


def write_opaque_artifact(
    context: GenerationSpoolContext,
    chunks: Iterable[bytes],
    *,
    format: str,
) -> dict[str, Any]:
    """Persist every byte from an opaque stream without sampling or truncation."""

    format_name = _validate_format(format)
    if format_name == CANONICAL_JSON_FORMAT:
        _fail("OPAQUE_FORMAT_COLLIDES_WITH_CANONICAL_JSON")
    return _write_streamed_object(context, chunks, format_name=format_name)


@contextmanager
def reserve_transient_publication(
    context: GenerationSpoolContext,
) -> Iterator[spool_quota.PublicationLease]:
    """Open one governed multi-temp transaction for a future batch publisher.

    Call ``consume`` before each physical write and ``materialize`` after it.
    Finish with ``commit`` after content-address publication, or remove every
    transaction-owned byte before ``cancel_after_cleanup``.
    """

    _assert_context_current(context)
    if context.writable is not True:
        _fail("GENERATION_CONTEXT_READ_ONLY")
    try:
        with spool_quota.publication_lease(
            context.root,
            max_generation_bytes=context.limits.max_generation_bytes,
            min_free_bytes=context.limits.min_free_bytes,
        ) as reservation:
            yield reservation
    except spool_quota.AuthoritySpoolQuotaError as exc:
        raise AuthoritySpoolError(str(exc)) from exc


class SymbolAuthoritySink:
    """Bounded per-symbol artifact roster."""

    def __init__(
        self,
        context: GenerationSpoolContext,
        symbol: str,
        *,
        manifest_domain: str = GENERAL_MANIFEST_DOMAIN,
    ) -> None:
        _assert_context_current(context)
        self.context = context
        self.symbol = _identity(symbol, "SYMBOL", maximum_bytes=256)
        self.manifest_domain = _manifest_domain(manifest_domain)
        self._artifacts: dict[str, dict[str, Any]] = {}
        self._total_bytes = 0

    def register_artifact(
        self,
        artifact_id: str,
        artifact_type: str,
        descriptor: Mapping[str, Any],
    ) -> dict[str, Any]:
        artifact_key = _identity(artifact_id, "ARTIFACT_ID", maximum_bytes=1024)
        type_name = _identity(artifact_type, "ARTIFACT_TYPE", maximum_bytes=256)
        normalized = _validate_descriptor(
            dict(descriptor), content_addressed=True
        )
        if normalized["size_bytes"] > self.context.limits.max_artifact_bytes:
            _fail("ARTIFACT_SIZE_BOUND_EXCEEDED")
        _verify_artifact_file(self.context, normalized)
        entry = {
            "artifact_id": artifact_key,
            "artifact_type": type_name,
            "descriptor": normalized,
        }
        existing = self._artifacts.get(artifact_key)
        if existing is not None:
            if existing != entry:
                _fail("ARTIFACT_ID_CONFLICT")
            return dict(existing)
        if len(self._artifacts) >= self.context.limits.max_artifacts_per_symbol:
            _fail("ARTIFACT_COUNT_BOUND_EXCEEDED")
        projected = self._total_bytes + int(normalized["size_bytes"])
        if projected > self.context.limits.max_symbol_bytes:
            _fail("SYMBOL_BYTE_BOUND_EXCEEDED")
        self._artifacts[artifact_key] = entry
        self._total_bytes = projected
        return dict(entry)

    def put_json(
        self,
        artifact_id: str,
        artifact_type: str,
        value: Any,
    ) -> dict[str, Any]:
        descriptor = write_canonical_json_artifact(self.context, value)
        return self.register_artifact(artifact_id, artifact_type, descriptor)

    def put_opaque(
        self,
        artifact_id: str,
        artifact_type: str,
        chunks: Iterable[bytes],
        *,
        format: str,
    ) -> dict[str, Any]:
        format_name = _validate_format(format)
        if format_name == CANONICAL_JSON_FORMAT:
            _fail("OPAQUE_FORMAT_COLLIDES_WITH_CANONICAL_JSON")
        existing = self._artifacts.get(artifact_id) if type(artifact_id) is str else None
        if existing is not None:
            return self._reuse_registered_opaque(
                _identity(artifact_id, "ARTIFACT_ID", maximum_bytes=1024),
                _identity(artifact_type, "ARTIFACT_TYPE", maximum_bytes=256),
                chunks,
                format_name=format_name,
                existing=existing,
            )
        descriptor = write_opaque_artifact(self.context, chunks, format=format)
        return self.register_artifact(artifact_id, artifact_type, descriptor)

    def _reuse_registered_opaque(
        self,
        artifact_key: str,
        type_name: str,
        chunks: Iterable[bytes],
        *,
        format_name: str,
        existing: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Verify an exact same-sink replay without republishing its bytes."""

        _assert_context_current(self.context)
        if self.context.writable is not True:
            _fail("GENERATION_CONTEXT_READ_ONLY")
        digest = hashlib.sha256()
        total = 0
        for raw in chunks:
            if type(raw) not in {bytes, bytearray, memoryview}:
                _fail("ARTIFACT_CHUNK_TYPE_INVALID")
            view = memoryview(raw)
            projected = total + len(view)
            if projected > self.context.limits.max_artifact_bytes:
                _fail("ARTIFACT_SIZE_BOUND_EXCEEDED")
            digest.update(view)
            total = projected
        if total <= 0:
            _fail("ARTIFACT_EMPTY")
        content_hash = digest.hexdigest()
        descriptor = {
            "schema_version": ARTIFACT_DESCRIPTOR_SCHEMA,
            "relative_path": _content_relative_path(content_hash, format_name),
            "sha256": content_hash,
            "size_bytes": total,
            "format": format_name,
        }
        candidate = {
            "artifact_id": artifact_key,
            "artifact_type": type_name,
            "descriptor": descriptor,
        }
        if dict(existing) != candidate:
            _fail("ARTIFACT_ID_CONFLICT")
        _verify_artifact_file(self.context, descriptor)
        return dict(existing)

    def artifact_roster(self) -> list[dict[str, Any]]:
        """Return a bounded canonical copy without publishing a manifest."""

        _assert_context_current(self.context)
        roster = [self._artifacts[key] for key in sorted(self._artifacts)]
        encoded = _canonical_json_bytes(
            roster,
            self.context.limits.max_manifest_bytes,
        )
        reopened = _strict_json_bytes(
            encoded,
            self.context.limits.max_manifest_bytes,
        )
        if (
            type(reopened) is not list
            or len(reopened) != len(roster)
            or sum(
                int(item.get("descriptor", {}).get("size_bytes", -1))
                for item in reopened
                if type(item) is dict
            )
            != self._total_bytes
        ):
            _fail("UNSEALED_ARTIFACT_ROSTER_INVALID")
        return reopened

    def seal(self) -> dict[str, Any]:
        _assert_context_current(self.context)
        roster = [self._artifacts[key] for key in sorted(self._artifacts)]
        roster_sha = hashlib.sha256(
            _canonical_json_bytes(roster, self.context.limits.max_manifest_bytes)
        ).hexdigest()
        manifest = {
            "schema_version": SYMBOL_MANIFEST_SCHEMA,
            "generation_id": self.context.generation_id,
            "symbol": self.symbol,
            "manifest_domain": self.manifest_domain,
            "native_universe_sha256": self.context.native_universe_sha256,
            "config_sha256": self.context.config_sha256,
            "generation_context_sha256": self.context.context_sha256,
            "artifact_count": len(roster),
            "artifact_total_bytes": self._total_bytes,
            "artifact_roster_sha256": roster_sha,
            "artifacts": roster,
        }
        manifest_descriptor = _publish_exact_json(
            self.context,
            _manifest_relative_path(self.symbol, self.manifest_domain),
            manifest,
            self.context.limits.max_manifest_bytes,
        )
        return {
            "schema_version": COMPACT_RECEIPT_SCHEMA,
            "generation_id": self.context.generation_id,
            "symbol": self.symbol,
            "manifest_domain": self.manifest_domain,
            "native_universe_sha256": self.context.native_universe_sha256,
            "config_sha256": self.context.config_sha256,
            "generation_context_sha256": self.context.context_sha256,
            "symbol_manifest": manifest_descriptor,
        }


def open_symbol_sink(
    context: GenerationSpoolContext,
    symbol: str,
    *,
    manifest_domain: str = GENERAL_MANIFEST_DOMAIN,
) -> SymbolAuthoritySink:
    """Open a worker sink bound to one exact symbol and generation."""

    return SymbolAuthoritySink(
        context,
        symbol,
        manifest_domain=manifest_domain,
    )


def _verify_artifact_file(
    context: GenerationSpoolContext,
    descriptor: Mapping[str, Any],
) -> None:
    normalized = _validate_descriptor(dict(descriptor), content_addressed=True)
    if normalized["size_bytes"] > context.limits.max_artifact_bytes:
        _fail("ARTIFACT_SIZE_BOUND_EXCEEDED")
    collect = normalized["format"] == CANONICAL_JSON_FORMAT
    if collect and normalized["size_bytes"] > context.limits.max_json_bytes:
        _fail("JSON_SIZE_BOUND_EXCEEDED")
    digest, size, encoded = _stream_file(
        context.root,
        normalized["relative_path"],
        maximum_bytes=context.limits.max_artifact_bytes,
        expected_size=normalized["size_bytes"],
        collect=collect,
        chunk_bytes=context.limits.stream_chunk_bytes,
    )
    if digest != normalized["sha256"] or size != normalized["size_bytes"]:
        _fail("ARTIFACT_INTEGRITY_MISMATCH")
    if collect:
        assert encoded is not None
        _strict_json_bytes(encoded, context.limits.max_json_bytes)


def _receipt_payload(receipt: Any) -> dict[str, Any]:
    if type(receipt) is bytes:
        value = _strict_json_bytes(receipt, MAX_RECEIPT_BYTES)
    elif type(receipt) is str:
        value = _strict_json_bytes(receipt.encode("utf-8"), MAX_RECEIPT_BYTES)
    elif type(receipt) is dict:
        value = receipt
        _canonical_json_bytes(value, MAX_RECEIPT_BYTES)
    else:
        _fail("COMPACT_RECEIPT_NOT_OBJECT")
    if type(value) is not dict or set(value) != _RECEIPT_FIELDS:
        _fail("COMPACT_RECEIPT_FIELDS_INVALID")
    if value.get("schema_version") != COMPACT_RECEIPT_SCHEMA:
        _fail("COMPACT_RECEIPT_SCHEMA_INVALID")
    return value


def verify_symbol_receipt(
    root: Path,
    receipt: Any,
    *,
    expected_generation_id: str,
    expected_symbol: str,
    expected_native_universe_sha256: str,
    expected_config_sha256: str,
    expected_manifest_domain: Optional[str] = None,
) -> dict[str, Any]:
    """Reopen and verify a compact worker receipt and every roster artifact."""

    generation = _identity(expected_generation_id, "EXPECTED_GENERATION_ID")
    symbol = _identity(expected_symbol, "EXPECTED_SYMBOL", maximum_bytes=256)
    native_hash = _sha256(
        expected_native_universe_sha256, "EXPECTED_NATIVE_UNIVERSE_SHA256"
    )
    config_hash = _sha256(expected_config_sha256, "EXPECTED_CONFIG_SHA256")
    value = _receipt_payload(receipt)
    receipt_domain = _manifest_domain(value.get("manifest_domain"))
    manifest_domain = (
        receipt_domain
        if expected_manifest_domain is None
        else _manifest_domain(expected_manifest_domain, "EXPECTED_MANIFEST_DOMAIN")
    )
    if value.get("generation_id") != generation:
        _fail("COMPACT_RECEIPT_GENERATION_MISMATCH")
    if value.get("symbol") != symbol:
        _fail("COMPACT_RECEIPT_SYMBOL_MISMATCH")
    if receipt_domain != manifest_domain:
        _fail("COMPACT_RECEIPT_MANIFEST_DOMAIN_MISMATCH")
    if value.get("native_universe_sha256") != native_hash:
        _fail("COMPACT_RECEIPT_NATIVE_MISMATCH")
    if value.get("config_sha256") != config_hash:
        _fail("COMPACT_RECEIPT_CONFIG_MISMATCH")
    context = reopen_generation_context(
        root,
        expected_generation_id=generation,
        expected_native_universe_sha256=native_hash,
        expected_config_sha256=config_hash,
    )
    if value.get("generation_context_sha256") != context.context_sha256:
        _fail("COMPACT_RECEIPT_CONTEXT_MISMATCH")
    manifest_descriptor = _validate_descriptor(
        value.get("symbol_manifest"),
        expected_path=_manifest_relative_path(symbol, manifest_domain),
        content_addressed=False,
    )
    if manifest_descriptor["format"] != CANONICAL_JSON_FORMAT:
        _fail("SYMBOL_MANIFEST_FORMAT_INVALID")
    if manifest_descriptor["size_bytes"] > context.limits.max_manifest_bytes:
        _fail("SYMBOL_MANIFEST_SIZE_BOUND_EXCEEDED")
    digest, size, encoded = _stream_file(
        context.root,
        manifest_descriptor["relative_path"],
        maximum_bytes=context.limits.max_manifest_bytes,
        expected_size=manifest_descriptor["size_bytes"],
        collect=True,
        chunk_bytes=context.limits.stream_chunk_bytes,
    )
    if digest != manifest_descriptor["sha256"] or size != manifest_descriptor["size_bytes"]:
        _fail("SYMBOL_MANIFEST_INTEGRITY_MISMATCH")
    assert encoded is not None
    manifest = _strict_json_bytes(encoded, context.limits.max_manifest_bytes)
    if type(manifest) is not dict or set(manifest) != _MANIFEST_FIELDS:
        _fail("SYMBOL_MANIFEST_FIELDS_INVALID")
    if manifest.get("schema_version") != SYMBOL_MANIFEST_SCHEMA:
        _fail("SYMBOL_MANIFEST_SCHEMA_INVALID")
    observed_manifest_domain = _manifest_domain(manifest.get("manifest_domain"))
    if observed_manifest_domain != manifest_domain:
        _fail("SYMBOL_MANIFEST_DOMAIN_MISMATCH")
    expected_identities = {
        "generation_id": generation,
        "symbol": symbol,
        "native_universe_sha256": native_hash,
        "config_sha256": config_hash,
        "generation_context_sha256": context.context_sha256,
    }
    for name, expected in expected_identities.items():
        if manifest.get(name) != expected:
            _fail(f"SYMBOL_MANIFEST_{name.upper()}_MISMATCH")
    roster = manifest.get("artifacts")
    if type(roster) is not list:
        _fail("SYMBOL_MANIFEST_ROSTER_INVALID")
    if len(roster) > context.limits.max_artifacts_per_symbol:
        _fail("ARTIFACT_COUNT_BOUND_EXCEEDED")
    if manifest.get("artifact_count") != len(roster):
        _fail("SYMBOL_MANIFEST_COUNT_MISMATCH")
    observed_ids: list[str] = []
    total_bytes = 0
    for entry in roster:
        if type(entry) is not dict or set(entry) != _ROSTER_FIELDS:
            _fail("SYMBOL_MANIFEST_ROSTER_ENTRY_INVALID")
        artifact_id = _identity(entry.get("artifact_id"), "ARTIFACT_ID", maximum_bytes=1024)
        _identity(entry.get("artifact_type"), "ARTIFACT_TYPE", maximum_bytes=256)
        descriptor = _validate_descriptor(
            entry.get("descriptor"), content_addressed=True
        )
        observed_ids.append(artifact_id)
        total_bytes += descriptor["size_bytes"]
        if total_bytes > context.limits.max_symbol_bytes:
            _fail("SYMBOL_BYTE_BOUND_EXCEEDED")
        _verify_artifact_file(context, descriptor)
    if observed_ids != sorted(observed_ids) or len(observed_ids) != len(set(observed_ids)):
        _fail("SYMBOL_MANIFEST_ROSTER_ORDER_OR_DUPLICATE_INVALID")
    if manifest.get("artifact_total_bytes") != total_bytes:
        _fail("SYMBOL_MANIFEST_TOTAL_BYTES_MISMATCH")
    roster_sha = hashlib.sha256(
        _canonical_json_bytes(roster, context.limits.max_manifest_bytes)
    ).hexdigest()
    if manifest.get("artifact_roster_sha256") != roster_sha:
        _fail("SYMBOL_MANIFEST_ROSTER_HASH_MISMATCH")
    return manifest


def _retirement_control(root: Path, relative: str, maximum: int) -> tuple[dict, str]:
    digest, _, encoded = _stream_file(root, relative, maximum_bytes=maximum, collect=True)
    assert encoded is not None
    value = _strict_json_bytes(encoded, maximum)
    if type(value) is not dict or _canonical_json_bytes(value, maximum) != encoded:
        _fail("RETIREMENT_CONTROL_INVALID")
    return value, digest


def _assert_retirement_root(root: Path, identity: list[int], *, declared_root: Any = ...) -> None:
    _reject_reparse_chain(root)
    info = root.stat(follow_symlinks=False)
    if (not stat.S_ISDIR(info.st_mode) or type(identity) is not list or len(identity) != 2
            or any(type(value) is not int or value < 0 for value in identity)
            or identity[1] <= 0 or info.st_ino <= 0
            or [info.st_dev, info.st_ino] != identity
            or (declared_root is not ... and (type(declared_root) is not str
                or not Path(declared_root).is_absolute() or Path(declared_root) != root))):
        _fail("WORKSPACE_ROOT_IDENTITY_CHANGED")


def _retirement_publish(root: Path, relative: str, body: dict, maximum: int,
        *, root_owner: Optional[dict] = None) -> str:
    """Operational control only; caller holds the original quota lock."""
    encoded = _canonical_json_bytes(body, maximum)
    expected = hashlib.sha256(encoded).hexdigest()
    if root_owner is not None:
        root_identity = root_owner["root_identity"]
        _assert_retirement_root(root, root_identity, declared_root=root_owner["root"])
    else:
        info = root.stat(follow_symlinks=False)
        root_identity = [info.st_dev, info.st_ino]
    target = _local_path(root, relative)
    _reject_reparse_chain(target)
    if not target.exists():
        _assert_retirement_root(root, root_identity)
        temporary = _temporary_path(root)
        descriptor = _open_exclusive(temporary)
        try:
            _write_all(descriptor, memoryview(encoded))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        identity = _file_identity(temporary.stat(follow_symlinks=False))
        try:
            _assert_retirement_root(root, root_identity)
            _publish_temp_exclusive(root, temporary, relative)
        finally:
            if temporary.exists():
                _assert_retirement_root(root, root_identity)
                if _file_identity(temporary.stat(follow_symlinks=False)) != identity:
                    _fail("RETIREMENT_CONTROL_TEMP_CHANGED")
                temporary.unlink()
    observed, digest = _retirement_control(root, relative, maximum)
    if observed != body or digest != expected:
        _fail("RETIREMENT_CONTROL_CONFLICT")
    return digest


def read_owned_generation_workspace(context: GenerationSpoolContext) -> tuple[dict, str]:
    _assert_context_current(context)
    body, digest = _retirement_control(context.root, WORKSPACE_OWNER_RELATIVE_PATH, MAX_CONTEXT_BYTES)
    if (set(body) != {"schema_version", "generation_id", "generation_context_sha256", "creation_provenance",
            "cell_id", "cell_contract_sha256", "root", "root_identity"} or body["schema_version"] != WORKSPACE_OWNER_SCHEMA
            or body["generation_id"] != context.generation_id
            or body["generation_context_sha256"] != context.context_sha256
            or body["creation_provenance"] != "ABSENT_OR_EMPTY_BEFORE_CONTEXT_CREATION"
            or not _is_sha256(body["cell_id"]) or not _is_sha256(body["cell_contract_sha256"])):
        _fail("WORKSPACE_OWNER_RECORD_INVALID")
    _assert_retirement_root(context.root, body["root_identity"], declared_root=body["root"])
    return body, digest


def _retirement_external(path: Path, maximum: int) -> tuple[dict, str]:
    if not path.is_absolute():
        _fail("RETIREMENT_EXTERNAL_PATH_INVALID")
    return _retirement_control(_validate_root(path.parent, create=False), path.name, maximum)


def _retirement_terminal_refusal_fence(body: dict, *, context_sha256: str, generation_id: str,
        cell_id: str, workspace_sha256: str, working_root: Path) -> dict:
    """Caller-assured fresh refusal, never a source finish or financial capability."""
    import pm_end_grid as end_grid
    flags = {"qualification_authority", "capital_authority", "live_authority", "order_authority",
        "full_native_context_eligibility", "financial_consumer_ready"}
    if (set(body) != {"schema_version", "workspace_owner_sha256", "binding", "refusal",
            "source_disposition", "end_grid_rows", "code_sha256"} | flags
            or body["schema_version"] != TERMINAL_REFUSAL_FENCE_SCHEMA
            or body["workspace_owner_sha256"] != workspace_sha256
            or any(body.get(key) is not False for key in flags)):
        _fail("RETIREMENT_TERMINAL_REFUSAL_INVALID")
    disposition = body["source_disposition"]
    if (type(disposition) is not dict or disposition != {
            "stage": "FRESH_PRE_SOURCE_NO_ACQUISITION", "fresh_workspace": True,
            "resumed": False, "source_acquisition_attempted": False,
            "source_owner_created": False, "source_products_acquired": False}
            or disposition.get("fresh_workspace") is not True
            or any(disposition.get(key) is not False for key in
                ("resumed", "source_acquisition_attempted", "source_owner_created", "source_products_acquired"))):
        _fail("RETIREMENT_REFUSAL_SOURCE_STATE_INVALID")
    binding = body["binding"]
    if (type(binding) is not dict or set(binding) != {"generation_context_sha256", "run_id", "cell_id",
            "cell_contract_sha256", "search_process_receipt_sha256", "precommit_sha256"}
            or binding["generation_context_sha256"] != context_sha256 or binding["run_id"] != generation_id
            or binding["cell_id"] != cell_id
            or any(not _is_sha256(binding[key]) for key in
                ("generation_context_sha256", "cell_id", "cell_contract_sha256",
                 "search_process_receipt_sha256", "precommit_sha256"))):
        _fail("RETIREMENT_REFUSAL_WORKSPACE_BINDING_INVALID")
    owner, owner_sha = _retirement_control(working_root, WORKSPACE_OWNER_RELATIVE_PATH, MAX_CONTEXT_BYTES)
    _assert_retirement_root(working_root, owner.get("root_identity"), declared_root=owner.get("root"))
    if (owner_sha != workspace_sha256 or owner.get("generation_context_sha256") != context_sha256
            or owner.get("generation_id") != generation_id or owner.get("cell_id") != cell_id
            or owner.get("cell_contract_sha256") != binding["cell_contract_sha256"]
            or owner.get("creation_provenance") != "ABSENT_OR_EMPTY_BEFORE_CONTEXT_CREATION"):
        _fail("RETIREMENT_REFUSAL_WORKSPACE_BINDING_INVALID")
    refusal = body["refusal"]
    if (type(refusal) is not dict or set(refusal) != {"status", "scheduler_status", "reason_code",
            "recorded_at_utc", "slot_id", "symbol", "timeframe", "regime", "cycle_start_utc",
            "cycle_end_utc", "information_cutoff_utc"}
            or refusal["status"] != "REFUSED_SOURCE_NOT_COMPLETED"
            or refusal["scheduler_status"] not in ("NOT_RUN", "FAILED")
            or type(refusal["slot_id"]) is not str or not refusal["slot_id"]):
        _fail("RETIREMENT_TERMINAL_REFUSAL_INVALID")
    codes = body["code_sha256"]
    if (type(codes) is not dict or set(codes) != {
            "pm_historical_scheduler.py", "pm_end_grid.py", "pm_authority_spool.py"}
            or any(not _is_sha256(value) for value in codes.values())):
        _fail("RETIREMENT_TERMINAL_REFUSAL_INVALID")
    rows = body["end_grid_rows"]
    if type(rows) is not dict or set(rows) != {refusal["slot_id"]}:
        _fail("RETIREMENT_REFUSAL_GRID_READBACK_REQUIRED")
    reference = rows[refusal["slot_id"]]
    if (type(reference) is not dict or set(reference) != {"path", "row_sha256"}
            or type(reference["path"]) is not str or not _is_sha256(reference["row_sha256"])):
        _fail("RETIREMENT_REFUSAL_GRID_READBACK_REQUIRED")
    grid_path = Path(reference["path"])
    if not grid_path.is_absolute() or grid_path.suffix != ".jsonl":
        _fail("RETIREMENT_REFUSAL_GRID_READBACK_REQUIRED")
    grid_root = grid_path.parent.resolve()
    if grid_root.is_relative_to(working_root) or working_root.is_relative_to(grid_root):
        _fail("RETIREMENT_RETAINED_PATH_OVERLAP")
    matched = [row for row in end_grid.read_end_grid(grid_path.parent, grid_path.stem)
        if row.row_sha256 == reference["row_sha256"]]
    if len(matched) != 1:
        _fail("RETIREMENT_REFUSAL_GRID_READBACK_REQUIRED")
    row = matched[0]
    if (row.cell_id != cell_id or row.source_kind != "m5_derived" or row.outcome != "STOP"
            or row.reason_code != refusal["reason_code"] or row.cost_tier != "NONE"
            or row.process_edition_sha256 != binding["search_process_receipt_sha256"]
            or row.code_sha256 != codes or row.source_identity != {} or row.metrics != {}
            or any(getattr(row, key) is not None for key in ("source_contract_sha256", "view_key",
                "view_contract_sha256", "job_binding_sha256", "consumer_binding_sha256",
                "no_trade_run_receipt_sha256", "strategy_family", "configuration_sha256"))
            or any(getattr(row, key) != refusal[key] for key in
                ("symbol", "timeframe", "regime", "cycle_start_utc", "cycle_end_utc", "information_cutoff_utc"))
            or row.produced_at_utc != refusal["recorded_at_utc"]):
        _fail("RETIREMENT_REFUSAL_GRID_READBACK_REQUIRED")
    return body


def _retirement_abort_binding_digest(value: dict) -> str:
    # Match the original M5 owner's DATA digest, including escaped Unicode.
    _canonical_json_bytes(value, 4 * 1024**2)
    return hashlib.sha256(json.dumps(value, allow_nan=False, sort_keys=True,
        ensure_ascii=True, separators=(",", ":")).encode("ascii")).hexdigest()


def _retirement_abort_summary(report: dict) -> dict:
    summary = {"schema_version": report["schema_version"], "status": report["status"],
        "report_sha256": hashlib.sha256(_canonical_json_bytes(report, 4 * 1024**2)).hexdigest(),
        "job_binding_sha256": report["job_binding_sha256"], "view_plan_sha256": report["view_plan_sha256"],
        "source_identity": report["source_identity"],
        "source_identity_sha256": report["product_routes"][0]["source_identity_sha256"],
        "product_routes_sha256": hashlib.sha256(_canonical_json_bytes(report["product_routes"], 4 * 1024**2)).hexdigest(),
        "product_route_count": len(report["product_routes"]), "declared_view_count": len(report["declared_view_ids"]),
        "owner_closed": True, "source_complete": False, "job_complete": False, "retirement_authority": False}
    _canonical_json_bytes(summary, MAX_CONTEXT_BYTES)
    return summary


def _retirement_post_source_abort_fence(body: dict, *, context_sha256: str, generation_id: str,
        cell_id: str, workspace_sha256: str, working_root: Path) -> dict:
    """Validate a caller-earned unused-owner abort, never financial completion."""
    from datetime import datetime, timedelta
    import math
    import pm_end_grid as end_grid
    flags = {"qualification_authority", "capital_authority", "live_authority", "order_authority",
        "full_native_context_eligibility", "financial_consumer_ready"}
    if (set(body) != {"schema_version", "workspace_owner_sha256", "binding", "refusal",
            "source_disposition", "end_grid_rows", "code_sha256", "abort_report", "bound_fitted_header"} | flags
            or body["schema_version"] != POST_SOURCE_ABORT_FENCE_SCHEMA
            or body["workspace_owner_sha256"] != workspace_sha256
            or any(body.get(key) is not False for key in flags)):
        _fail("RETIREMENT_POST_SOURCE_ABORT_INVALID")
    if body["bound_fitted_header"] is not None:
        _fail("RETIREMENT_ABORT_HEADER_UNSUPPORTED")
    disposition = body["source_disposition"]
    expected_disposition = {"stage": "FRESH_POST_SOURCE_PRE_CONTROLLER_ABORT", "fresh_workspace": True,
        "resumed": False, "source_acquisition_attempted": True, "source_owner_created": True,
        "source_products_acquired": True, "controller_started": False}
    if (type(disposition) is not dict or set(disposition) != set(expected_disposition)
            or any(type(disposition[key]) is not type(value) or disposition[key] != value
                for key, value in expected_disposition.items())):
        _fail("RETIREMENT_ABORT_SOURCE_STATE_INVALID")
    binding = body["binding"]
    binding_fields = {"generation_context_sha256", "run_id", "cell_id", "cell_contract_sha256",
        "search_process_receipt_sha256", "precommit_sha256"}
    if (type(binding) is not dict or set(binding) != binding_fields
            or binding["generation_context_sha256"] != context_sha256 or binding["run_id"] != generation_id
            or binding["cell_id"] != cell_id
            or any(not _is_sha256(binding[key]) for key in binding_fields - {"run_id"})):
        _fail("RETIREMENT_ABORT_BINDING_INVALID")
    owner, owner_sha = _retirement_control(working_root, WORKSPACE_OWNER_RELATIVE_PATH, MAX_CONTEXT_BYTES)
    _assert_retirement_root(working_root, owner.get("root_identity"), declared_root=owner.get("root"))
    context, context_sha = _retirement_control(working_root, CONTEXT_RELATIVE_PATH, MAX_CONTEXT_BYTES)
    if (owner_sha != workspace_sha256 or owner.get("generation_context_sha256") != context_sha256
            or owner.get("generation_id") != generation_id or owner.get("cell_id") != cell_id
            or owner.get("cell_contract_sha256") != binding["cell_contract_sha256"]
            or owner.get("creation_provenance") != "ABSENT_OR_EMPTY_BEFORE_CONTEXT_CREATION"
            or context_sha != context_sha256):
        _fail("RETIREMENT_ABORT_BINDING_INVALID")
    refusal = body["refusal"]
    if (type(refusal) is not dict or set(refusal) != {"status", "scheduler_status", "reason_code",
            "recorded_at_utc", "slot_id", "symbol", "timeframe", "regime", "cycle_start_utc",
            "cycle_end_utc", "information_cutoff_utc"}
            or refusal["status"] != "REFUSED_SOURCE_NOT_COMPLETED"
            or refusal["scheduler_status"] not in ("NOT_RUN", "FAILED")
            or type(refusal["slot_id"]) is not str or not refusal["slot_id"]):
        _fail("RETIREMENT_POST_SOURCE_ABORT_INVALID")
    report = body["abort_report"]
    report_fields = {"schema_version", "status", "reason", "binding", "budgets", "job_binding_sha256",
        "view_plan_sha256", "source_identity", "product_routes", "declared_view_ids", "requests_all_pending",
        "active_leases_before_close", "observed_consumer_bindings", "value_cache_empty_before_close",
        "prefix_routes_revalidated", "job_view_read_observations", "owner_closed", "source_complete",
        "job_complete", "retirement_authority", "caller_obligations_not_authenticated"} | flags
    if (type(report) is not dict or set(report) != report_fields
            or report["schema_version"] != "fxpm.m5-abort-before-consumption.v1"
            or report["status"] != "ABORTED_SOURCE_BEFORE_CONSUMPTION"
            or type(report["reason"]) is not str or report["reason"] != refusal["reason_code"]
            or not 1 <= len(report["reason"]) <= 512
            or any(report.get(key) is not True for key in ("requests_all_pending", "value_cache_empty_before_close",
                "prefix_routes_revalidated", "owner_closed"))
            or type(report["active_leases_before_close"]) is not int or report["active_leases_before_close"] != 0
            or any(type(report[key]) is not list or report[key] for key in
                ("observed_consumer_bindings", "job_view_read_observations"))
            or any(report.get(key) is not False for key in flags | {"source_complete", "job_complete", "retirement_authority"})
            or report["caller_obligations_not_authenticated"] != ["ORIGINAL_DISPATCH_LEASE_HELD",
                "FRESH_PRE_CONTROLLER_PHASE", "ACTUAL_STOP_GRID_READBACK_AND_DISTINCT_RETIREMENT_FENCE"]):
        _fail("RETIREMENT_ABORT_REPORT_INVALID")
    job = report["binding"]
    job_fields = {"m5_view_contract_sha256", "generation_context_sha256", "config_sha256", "effective_config_sha256",
        "search_process_receipt_sha256", "precommit_sha256", "run_id", "cell_id", "view_budgets_sha256",
        "view_plan_sha256", "required_result_keys", "terminal_result_key"}
    if (type(job) is not dict or set(job) != job_fields
            or any(job[key] != binding[key] for key in binding_fields - {"cell_contract_sha256"})
            or job["config_sha256"] != context.get("config_sha256")
            or any(not _is_sha256(job[key]) for key in job_fields - {"run_id", "cell_id", "required_result_keys", "terminal_result_key"})
            or type(job["required_result_keys"]) is not list or not job["required_result_keys"]
            or any(type(key) is not str or not 1 <= len(key) <= 512 for key in job["required_result_keys"])
            or len(set(job["required_result_keys"])) != len(job["required_result_keys"])
            or job["terminal_result_key"] not in job["required_result_keys"]
            or report["view_plan_sha256"] != job["view_plan_sha256"]
            or report["job_binding_sha256"] != _retirement_abort_binding_digest(job)):
        _fail("RETIREMENT_ABORT_BINDING_INVALID")
    budgets = report["budgets"]
    budget_fields = {"max_source_rows", "max_source_bytes", "max_view_rows", "max_view_bytes", "max_cache_rows", "max_cache_bytes"}
    ids = report["declared_view_ids"]
    if (type(budgets) is not dict or set(budgets) != budget_fields
            or any(type(value) is not int or value <= 0 for value in budgets.values())
            or _retirement_abort_binding_digest(budgets) != job["view_budgets_sha256"]
            or type(ids) is not list or not ids or any(type(value) is not str or not 1 <= len(value) <= 512 for value in ids)
            or ids != sorted(set(ids))):
        _fail("RETIREMENT_ABORT_REPORT_INVALID")
    source = report["source_identity"]
    source_fields = {"symbol", "timeframe", "activation_id", "activation_sha256", "bars_sha256", "sidecar_sha256",
        "provenance_sha256", "request_sha256", "point", "spread_unit"}
    if (type(source) is not dict or set(source) != source_fields
            or source["symbol"] != refusal["symbol"] or source["timeframe"] != "M5" or source["spread_unit"] != "points"
            or type(source["activation_id"]) is not str or not 1 <= len(source["activation_id"]) <= 512
            or any(not _is_sha256(source[key]) for key in source_fields if key.endswith("sha256"))
            or type(source["point"]) not in (int, float)
            or (type(source["point"]) is float and not math.isfinite(source["point"])) or source["point"] <= 0):
        _fail("RETIREMENT_ABORT_SOURCE_IDENTITY_INVALID")
    routes = report["product_routes"]
    route_fields = {"cutoff_utc", "source_accessed_at_utc", "index_sha256", "source_identity_sha256",
        "activation_id", "activation_manifest_sha256", "activation_entry_sha256"}
    if type(routes) is not list or not routes:
        _fail("RETIREMENT_ABORT_ROUTE_INVALID")
    cutoffs = []
    source_sha = None
    for route in routes:
        if (type(route) is not dict or set(route) != route_fields
                or any(not _is_sha256(route[key]) for key in route_fields if key.endswith("sha256"))
                or route["activation_id"] != source["activation_id"]
                or route["activation_manifest_sha256"] != source["activation_sha256"]
                or (source_sha is not None and source_sha != route["source_identity_sha256"])):
            _fail("RETIREMENT_ABORT_ROUTE_INVALID")
        source_sha = route["source_identity_sha256"]
        stamps = []
        for key in ("cutoff_utc", "source_accessed_at_utc"):
            value = route[key]
            if type(value) is not str or value != value.strip():
                _fail("RETIREMENT_ABORT_ROUTE_INVALID")
            try:
                stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                _fail("RETIREMENT_ABORT_ROUTE_INVALID")
            if stamp.tzinfo is None or stamp.utcoffset() != timedelta(0):
                _fail("RETIREMENT_ABORT_ROUTE_INVALID")
            stamps.append(stamp)
        cutoffs.append(stamps[0])
    if cutoffs != sorted(set(cutoffs)):
        _fail("RETIREMENT_ABORT_ROUTE_INVALID")
    codes = body["code_sha256"]
    rows = body["end_grid_rows"]
    if (type(codes) is not dict or set(codes) != {"pm_historical_scheduler.py", "pm_end_grid.py", "pm_authority_spool.py"}
            or any(not _is_sha256(value) for value in codes.values())
            or type(rows) is not dict or set(rows) != {refusal["slot_id"]}):
        _fail("RETIREMENT_ABORT_GRID_READBACK_REQUIRED")
    reference = rows[refusal["slot_id"]]
    if (type(reference) is not dict or set(reference) != {"path", "row_sha256"}
            or type(reference["path"]) is not str or not _is_sha256(reference["row_sha256"])):
        _fail("RETIREMENT_ABORT_GRID_READBACK_REQUIRED")
    grid_path = Path(reference["path"])
    if not grid_path.is_absolute() or grid_path.suffix != ".jsonl":
        _fail("RETIREMENT_ABORT_GRID_READBACK_REQUIRED")
    grid_root = grid_path.parent.resolve()
    if grid_root.is_relative_to(working_root) or working_root.is_relative_to(grid_root):
        _fail("RETIREMENT_RETAINED_PATH_OVERLAP")
    matched = [row for row in end_grid.read_end_grid(grid_path.parent, grid_path.stem)
        if row.row_sha256 == reference["row_sha256"]]
    if len(matched) != 1:
        _fail("RETIREMENT_ABORT_GRID_READBACK_REQUIRED")
    row = matched[0]
    if (row.cell_id != cell_id or row.source_kind != "m5_derived" or row.outcome != "STOP" or row.cost_tier != "NONE"
            or row.reason_code != refusal["reason_code"] or row.process_edition_sha256 != binding["search_process_receipt_sha256"]
            or row.code_sha256 != codes or row.metrics != {} or row.job_binding_sha256 != report["job_binding_sha256"]
            or row.source_identity != {key: source[key] for key in POST_SOURCE_ABORT_GRID_SOURCE_FIELDS}
            or any(getattr(row, key) is not None for key in ("source_contract_sha256", "view_key", "view_contract_sha256",
                "consumer_binding_sha256", "no_trade_run_receipt_sha256", "strategy_family", "configuration_sha256"))
            or any(getattr(row, key) != refusal[key] for key in ("symbol", "timeframe", "regime",
                "cycle_start_utc", "cycle_end_utc", "information_cutoff_utc"))
            or row.produced_at_utc != refusal["recorded_at_utc"]):
        _fail("RETIREMENT_ABORT_GRID_READBACK_REQUIRED")
    _retirement_abort_summary(report)
    return body


def _retirement_native_view_contract(contract: dict, row: Any, *, binding: dict,
        completion: dict, slot_id: str, ordinal: int) -> None:
    from datetime import datetime, timedelta

    import pm_end_grid as end_grid

    invalid = "RETIREMENT_NATIVE_VIEW_CONTRACT_INVALID"
    if (set(contract) != {"schema_version", "view", "result", "cost_tier_evidence"}
            or contract["schema_version"] != "fxpm.broker-native-end-grid-reconstruction.v1"):
        _fail(invalid)
    view, result, evidence = contract["view"], contract["result"], contract["cost_tier_evidence"]
    source_keys = ("activation_id", "activation_manifest_sha256", "activation_entry_sha256", "native_bars_sha256")
    view_sha_fields = {"activation_manifest_sha256", "activation_entry_sha256", "native_bars_sha256",
        "continuity_policy_sha256", "continuity_geometry_sha256"}
    if (type(view) is not dict or set(view) != {
            "schema_version", "symbol", "timeframe", "cutoff_utc", "activation_id"} | view_sha_fields
            or view["schema_version"] != "fxpm.broker-native-end-grid-view.v1"
            or (view["symbol"], view["timeframe"]) != (row.symbol, row.timeframe)
            or row.source_identity != {key: view[key] for key in source_keys}
            or any(not _is_sha256(view[key]) for key in view_sha_fields)
            or hashlib.sha256(end_grid.canonical_bytes(view)).hexdigest() != row.view_key):
        _fail(invalid)
    expected_result = {"precommit_sha256": binding["precommit_sha256"], "cell_id": row.cell_id,
        "slot_id": slot_id, "ordinal": ordinal, "cycle_start_utc": row.cycle_start_utc,
        "cycle_end_utc": row.cycle_end_utc,
        "completion_status": "OWNER" if row.outcome == "WINNER" else "NO_TRADE",
        "reason_code": row.reason_code, "saved_cell_run_receipt_sha256": completion["cell_run_receipt_sha256"]}
    result_sha_fields = {"request_sha256", "slot_commit_sha256", "source_session_sha256", "native_prefix_index_sha256"}
    if (type(result) is not dict or set(result) != set(expected_result) | result_sha_fields | {"source_accessed_at_utc"}
            or type(result["ordinal"]) is not int
            or any(result[key] != value for key, value in expected_result.items())
            or any(not _is_sha256(result[key]) for key in result_sha_fields)):
        _fail(invalid)
    try:
        cutoff = datetime.fromisoformat(view["cutoff_utc"])
        accessed_at = datetime.fromisoformat(result["source_accessed_at_utc"])
    except (TypeError, ValueError):
        _fail(invalid)
    if (cutoff.utcoffset() != timedelta(0) or accessed_at.utcoffset() != timedelta(0)
            or cutoff != datetime.fromisoformat(row.information_cutoff_utc)):
        _fail(invalid)
    if row.outcome == "WINNER":
        semantics, descriptor_key = "VERIFIED_SELECTED_OWNER_TIER", "selected_tier_contract"
    else:
        semantics, descriptor_key = "BOUND_BASELINE_DESCRIPTOR_NO_SELECTED_WINNER", "bound_baseline_descriptor"
        if row.cost_tier != "P75":
            _fail(invalid)
    if (type(evidence) is not dict or set(evidence) != {"semantics", "tier", descriptor_key}
            or evidence["semantics"] != semantics or evidence["tier"] != row.cost_tier
            or type(evidence[descriptor_key]) is not dict or not evidence[descriptor_key]):
        _fail(invalid)


def _retirement_native_completion_fence(body: dict, *, context_sha256: str, generation_id: str,
        cell_id: str, workspace_sha256: str, working_root: Path, owner: Optional[dict]) -> dict:
    """Join caller-verified native completion identities to retained Grid evidence."""
    from datetime import datetime, timedelta

    import pm_end_grid as end_grid

    invalid = "RETIREMENT_NATIVE_COMPLETION_FENCE_INVALID"
    if (set(body) != {"schema_version", "workspace_owner_sha256", "binding", "completion",
            "selected_slots", "end_grid_rows"} | set(end_grid.AUTHORITY_FLAGS)
            or body["schema_version"] != NATIVE_COMPLETION_FENCE_SCHEMA
            or body["workspace_owner_sha256"] != workspace_sha256
            or any(body[key] is not False for key in end_grid.AUTHORITY_FLAGS)):
        _fail(invalid)
    binding = body["binding"]
    if (type(binding) is not dict or set(binding) != {"generation_context_sha256", "run_id", "cell_id",
            "cell_contract_sha256", "search_process_receipt_sha256", "precommit_sha256"}
            or (binding["generation_context_sha256"], binding["run_id"], binding["cell_id"])
                != (context_sha256, generation_id, cell_id)
            or any(not _is_sha256(binding[key]) for key in binding if key != "run_id")
            or type(owner) is not dict or owner.get("schema_version") != WORKSPACE_OWNER_SCHEMA
            or owner.get("creation_provenance") != "ABSENT_OR_EMPTY_BEFORE_CONTEXT_CREATION"
            or (owner.get("generation_context_sha256"), owner.get("generation_id"), owner.get("cell_id"),
                owner.get("cell_contract_sha256")) != (context_sha256, generation_id, cell_id, binding["cell_contract_sha256"])):
        _fail(invalid)
    completion = body["completion"]
    if (type(completion) is not dict or set(completion) != {"cell_run_receipt_sha256", "account_artifact_sha256"}
            or any(not _is_sha256(value) for value in completion.values())):
        _fail(invalid)
    slots, references = body["selected_slots"], body["end_grid_rows"]
    if type(slots) is not list or len(slots) != 13 or type(references) is not dict:
        _fail(invalid)
    time_keys = ("cycle_start_utc", "cycle_end_utc", "information_cutoff_utc")
    slot_ids, starts, grid_path = set(), set(), None
    previous_end = None
    for slot in slots:
        if (type(slot) is not dict or set(slot) != {"slot_id", "outcome", *time_keys}
                or slot["outcome"] not in ("WINNER", "NO_TRADE")
                or any(type(slot[key]) is not str for key in time_keys)):
            _fail(invalid)
        try:
            start = datetime.fromisoformat(slot["cycle_start_utc"])
            end = datetime.fromisoformat(slot["cycle_end_utc"])
        except ValueError:
            _fail(invalid)
        if (start.utcoffset() != timedelta(0) or end.utcoffset() != timedelta(0)
                or end - start != timedelta(days=28) or (previous_end is not None and start != previous_end)):
            _fail(invalid)
        previous_end = end
        slot_id = _identity(slot["slot_id"], "RETIREMENT_NATIVE_SLOT_ID")
        if slot_id in slot_ids:
            _fail(invalid)
        slot_ids.add(slot_id)
        starts.add(slot["cycle_start_utc"])
        reference = references.get(slot_id)
        if (type(reference) is not dict or set(reference) != {"path", "row_sha256"}
                or type(reference["path"]) is not str or not _is_sha256(reference["row_sha256"])):
            _fail("RETIREMENT_GRID_READBACK_REQUIRED")
        path = Path(reference["path"])
        if not path.is_absolute() or path.suffix != ".jsonl" or (grid_path is not None and path != grid_path):
            _fail("RETIREMENT_GRID_READBACK_REQUIRED")
        grid_path = path
    if set(references) != slot_ids:
        _fail(invalid)
    grid_root = grid_path.parent.resolve()
    if grid_root.is_relative_to(working_root) or working_root.is_relative_to(grid_root):
        _fail("RETIREMENT_RETAINED_PATH_OVERLAP")
    rows_by_start = {}
    for row in end_grid.read_end_grid(grid_path.parent, grid_path.stem):
        if row.cell_id == cell_id and row.process_edition_sha256 == binding["search_process_receipt_sha256"]:
            if row.cycle_start_utc not in starts or row.cycle_start_utc in rows_by_start:
                _fail("RETIREMENT_GRID_READBACK_REQUIRED")
            rows_by_start[row.cycle_start_utc] = row
    for ordinal, slot in enumerate(slots, start=1):
        row = rows_by_start.get(slot["cycle_start_utc"])
        if (row is None or row.row_sha256 != references[slot["slot_id"]]["row_sha256"]
                or row.schema_version != end_grid.END_GRID_SCHEMA_VERSION or row.source_kind != "broker_native"
                or row.outcome != slot["outcome"] or any(getattr(row, key) != slot[key] for key in time_keys)
                or row.job_binding_sha256 is not None or row.consumer_binding_sha256 is not None
                or row.view_contract_sha256 is None
                or (row.outcome == "NO_TRADE" and row.no_trade_run_receipt_sha256 != completion["cell_run_receipt_sha256"])):
            _fail("RETIREMENT_GRID_READBACK_REQUIRED")
        contract = end_grid.load_view_contract(grid_path.parent, row.view_contract_sha256)
        _retirement_native_view_contract(contract, row, binding=binding, completion=completion,
            slot_id=slot["slot_id"], ordinal=ordinal)
    return body


def _retirement_fence(path: Path, expected: str, *, context_sha256: str, generation_id: str,
        cell_id: str, workspace_sha256: str, working_root: Path, owner: Optional[dict] = None) -> dict:
    import pm_end_grid as end_grid
    if path.resolve().is_relative_to(working_root):
        _fail("RETIREMENT_RETAINED_PATH_OVERLAP")
    body, digest = _retirement_external(path, 4 * 1024**2)
    if body.get("schema_version") == POST_SOURCE_ABORT_FENCE_SCHEMA:
        if digest != expected:
            _fail("RETIREMENT_POST_SOURCE_ABORT_INVALID")
        return _retirement_post_source_abort_fence(body, context_sha256=context_sha256,
            generation_id=generation_id, cell_id=cell_id, workspace_sha256=workspace_sha256,
            working_root=working_root)
    if body.get("schema_version") == TERMINAL_REFUSAL_FENCE_SCHEMA:
        if digest != expected:
            _fail("RETIREMENT_TERMINAL_REFUSAL_INVALID")
        return _retirement_terminal_refusal_fence(body, context_sha256=context_sha256,
            generation_id=generation_id, cell_id=cell_id, workspace_sha256=workspace_sha256,
            working_root=working_root)
    if body.get("schema_version") == NATIVE_COMPLETION_FENCE_SCHEMA:
        if digest != expected:
            _fail("RETIREMENT_NATIVE_COMPLETION_FENCE_INVALID")
        return _retirement_native_completion_fence(body, context_sha256=context_sha256,
            generation_id=generation_id, cell_id=cell_id, workspace_sha256=workspace_sha256,
            working_root=working_root, owner=owner)
    summary = body.get("source_summary")
    if (digest != expected or body.get("schema_version") != "fxpm.declared-m5-development-source-fence.v1"
            or body.get("workspace_owner_sha256") != workspace_sha256
            or type(summary) is not dict or summary.get("owned_values_released") is not True
            or summary.get("release_eligible") is not True
            or summary.get("full_native_context_eligibility") is not False
            or summary.get("financial_consumer_ready") is not False
            or any(body.get(key) is not False for key in
                ("qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("RETIREMENT_FINAL_SOURCE_FENCE_INVALID")
    binding = summary.get("binding")
    if (type(binding) is not dict or binding.get("generation_context_sha256") != context_sha256
            or binding.get("run_id") != generation_id or binding.get("cell_id") != cell_id):
        _fail("RETIREMENT_FINAL_SOURCE_FENCE_INVALID")
    rows = body.get("end_grid_rows")
    if type(rows) is not dict or not rows:
        _fail("RETIREMENT_GRID_READBACK_REQUIRED")
    job_sha = hashlib.sha256(_canonical_json_bytes(binding, MAX_CONTEXT_BYTES)).hexdigest()
    for reference in rows.values():
        if (type(reference) is not dict or set(reference) != {"path", "row_sha256"}
                or type(reference["path"]) is not str or not _is_sha256(reference["row_sha256"])):
            _fail("RETIREMENT_GRID_READBACK_REQUIRED")
        grid_path = Path(reference["path"])
        if not grid_path.is_absolute() or grid_path.suffix != ".jsonl":
            _fail("RETIREMENT_GRID_READBACK_REQUIRED")
        grid_root = grid_path.parent.resolve()
        if grid_root.is_relative_to(working_root) or working_root.is_relative_to(grid_root):
            _fail("RETIREMENT_RETAINED_PATH_OVERLAP")
        matched = [row for row in end_grid.read_end_grid(grid_path.parent, grid_path.stem)
            if row.row_sha256 == reference["row_sha256"]]
        if len(matched) != 1 or matched[0].cell_id != cell_id or matched[0].job_binding_sha256 != job_sha:
            _fail("RETIREMENT_GRID_READBACK_REQUIRED")
        if matched[0].view_contract_sha256 is not None:
            end_grid.load_view_contract(grid_path.parent, matched[0].view_contract_sha256)
    return body


def read_generation_retirement(root: Path, *, expected_context_sha256: Optional[str] = None,
        expected_completed_fence_schema: Optional[str] = None) -> Optional[dict]:
    """Return recording metadata, optionally requiring a particular completed fence schema."""
    root = Path(root)
    if not root.exists():
        _reject_reparse_chain(root)
        return None
    root = _validate_root(root, create=False)
    if (root / WORKSPACE_OWNER_RELATIVE_PATH).exists():
        owner, _ = _retirement_control(root, WORKSPACE_OWNER_RELATIVE_PATH, MAX_CONTEXT_BYTES)
        _assert_retirement_root(root, owner.get("root_identity"), declared_root=owner.get("root"))
    try:
        (root / RETIREMENT_RELATIVE_PATH).lstat()
    except FileNotFoundError:
        return None
    marker, _ = _retirement_control(root, RETIREMENT_RELATIVE_PATH, MAX_CONTEXT_BYTES)
    marker_fields = {"schema_version", "status", "generation_context_sha256", "generation_id", "cell_id",
        "intent_sha256", "source_fence_path", "source_fence_sha256", "retirement_receipt_path", "workspace_owner_sha256"}
    if (set(marker) != marker_fields or marker.get("schema_version") not in {RETIREMENT_SCHEMA, RETIREMENT_SCHEMA_V2}
            or marker.get("status") != "RETIRING"
            or not _is_sha256(marker.get("generation_context_sha256"))
            or any(not _is_sha256(marker.get(key)) for key in
                ("cell_id", "intent_sha256", "source_fence_sha256", "workspace_owner_sha256"))
            or any(type(marker.get(key)) is not str or not Path(marker[key]).is_absolute()
                for key in ("source_fence_path", "retirement_receipt_path"))
            or (expected_context_sha256 is not None and marker["generation_context_sha256"] != expected_context_sha256)):
        _fail("RETIREMENT_MARKER_INVALID")
    context_body, context_sha = _retirement_control(root, CONTEXT_RELATIVE_PATH, MAX_CONTEXT_BYTES)
    owner, owner_sha = _retirement_control(root, WORKSPACE_OWNER_RELATIVE_PATH, MAX_CONTEXT_BYTES)
    if (context_sha != marker["generation_context_sha256"] or context_body.get("generation_id") != marker["generation_id"]
            or owner_sha != marker["workspace_owner_sha256"]):
        _fail("RETIREMENT_CONTEXT_CHANGED")
    try:
        (root / RETIREMENT_COMPLETE_RELATIVE_PATH).lstat()
    except FileNotFoundError:
        return dict(marker)
    complete, _ = _retirement_control(root, RETIREMENT_COMPLETE_RELATIVE_PATH, MAX_CONTEXT_BYTES)
    receipt, digest = _retirement_external(Path(marker["retirement_receipt_path"]), MAX_CONTEXT_BYTES)
    if (set(complete) != {"schema_version", "receipt_sha256", "generation_context_sha256"}
            or complete["schema_version"] != marker["schema_version"] or complete["receipt_sha256"] != digest
            or complete["generation_context_sha256"] != marker["generation_context_sha256"]
            or receipt.get("schema_version") != marker["schema_version"] or receipt.get("status") != "ARCHIVED_RECORD_ONLY"
            or receipt.get("generation_context_sha256") != marker["generation_context_sha256"]
            or receipt.get("payloads_retained") is not False or receipt.get("genuine_cold_reopen_available") is not False
            or any(receipt.get(key) is not False for key in
                ("qualification_authority", "capital_authority", "live_authority", "order_authority"))):
        _fail("RETIREMENT_COMPLETION_INVALID")
    if (marker["schema_version"] == RETIREMENT_SCHEMA_V2
            and (type(receipt.get("intent_segment_count")) is not int or receipt["intent_segment_count"] < 0)):
        _fail("RETIREMENT_COMPLETION_INVALID")
    fence = _retirement_fence(Path(marker["source_fence_path"]), marker["source_fence_sha256"],
        context_sha256=marker["generation_context_sha256"], generation_id=marker["generation_id"],
        cell_id=marker["cell_id"], workspace_sha256=marker["workspace_owner_sha256"], working_root=root, owner=owner)
    if expected_completed_fence_schema is not None and fence["schema_version"] != expected_completed_fence_schema:
        _fail("RETIREMENT_COMPLETED_FENCE_SCHEMA_MISMATCH")
    if fence.get("schema_version") == TERMINAL_REFUSAL_FENCE_SCHEMA:
        if (receipt.get("result_kind") != TERMINAL_REFUSAL_RESULT_KIND
                or receipt.get("terminal_refusal") != fence["refusal"]):
            _fail("RETIREMENT_COMPLETION_INVALID")
    elif fence.get("schema_version") == POST_SOURCE_ABORT_FENCE_SCHEMA:
        if (receipt.get("result_kind") != POST_SOURCE_ABORT_RESULT_KIND
                or receipt.get("terminal_refusal") != fence["refusal"]
                or receipt.get("source_abort") != _retirement_abort_summary(fence["abort_report"])
                or receipt.get("full_native_context_eligibility") is not False
                or receipt.get("financial_consumer_ready") is not False):
            _fail("RETIREMENT_COMPLETION_INVALID")
    elif "result_kind" in receipt or "terminal_refusal" in receipt:
        _fail("RETIREMENT_COMPLETION_INVALID")
    if fence["schema_version"] == NATIVE_COMPLETION_FENCE_SCHEMA:
        expected_receipt = {"generation_id": marker["generation_id"], "cell_id": marker["cell_id"],
            "cell_contract_sha256": fence["binding"]["cell_contract_sha256"],
            "workspace_owner_sha256": marker["workspace_owner_sha256"], "intent_sha256": marker["intent_sha256"],
            "precommit_sha256": fence["binding"]["precommit_sha256"], "end_grid_rows": fence["end_grid_rows"],
            "source_fence": {"path": marker["source_fence_path"], "sha256": marker["source_fence_sha256"]},
            "descriptor_assurance": "IDENTITY_ONLY_AFTER_WORKSPACE_RELEASE"}
        if any(receipt.get(key) != value for key, value in expected_receipt.items()):
            _fail("RETIREMENT_COMPLETION_INVALID")
    return {**receipt, "retirement_receipt_sha256": digest,
        "retirement_receipt_path": marker["retirement_receipt_path"]}


def _retirement_owned(relative: str, direct: frozenset[str], *, directory: bool) -> bool:
    if any(name in {"real_control", "STOP", ".git"} for name in PurePosixPath(relative).parts):
        return False
    if relative in direct or (directory and any(item.startswith(relative + "/") for item in direct)):
        return True
    if directory:
        return bool(re.fullmatch(r"objects(?:/sha256(?:/[a-f0-9]{2})?)?|symbols(?:/[a-f0-9]{64}(?:/domains(?:/[a-f0-9]{64})?)?)?|tmp", relative))
    object_path = re.fullmatch(r"objects/sha256/([a-f0-9]{2})/([a-f0-9]{64})\.[a-z0-9][a-z0-9._+\-]{0,63}", relative)
    if object_path is not None:
        return object_path[1] == object_path[2][:2]
    return bool(re.fullmatch(r"symbols/[a-f0-9]{64}/domains/[a-f0-9]{64}/manifest\.json|tmp/[a-f0-9]{32}\.part", relative))


def _retirement_record(record: Any) -> dict:
    if (type(record) is not dict or set(record) != {"relative_path", "identity", "bytes"}
            or type(record["relative_path"]) is not str
            or len(record["relative_path"].encode("utf-8")) > 4096
            or type(record["identity"]) is not list or len(record["identity"]) != 4
            or any(type(value) is not int for value in record["identity"])
            or record["identity"][0] < 0 or record["identity"][1] <= 0
            or type(record["bytes"]) is not int or record["bytes"] < 0
            or record["identity"][2] != record["bytes"]):
        _fail("RETIREMENT_SEGMENTS_INVALID")
    _relative_parts(record["relative_path"])
    return record


def _retirement_segment_descriptor(body: dict, maximum_bytes: int) -> dict:
    encoded = _canonical_json_bytes(body, maximum_bytes)
    digest = hashlib.sha256(encoded).hexdigest()
    records = body["files"]
    return {"relative_path": f"generation-retirement.segment.{digest}.json",
        "sha256": digest, "size_bytes": len(encoded), "count": len(records),
        "bytes": sum(record["bytes"] for record in records),
        "first_path": records[0]["relative_path"], "last_path": records[-1]["relative_path"]}


def plan_segmented_retirement(records: Iterable[dict], *, generation_context_sha256: str,
        arguments: dict, maximum_bytes: int, segment_max_records: int) -> dict:
    """Encode a deterministic, bounded retirement intent without filesystem access."""
    _sha256(generation_context_sha256, "RETIREMENT_BINDING_INVALID")
    if (type(segment_max_records) is not int
            or not 1 <= segment_max_records <= MAX_RETIREMENT_SEGMENT_RECORDS
            or type(maximum_bytes) is not int or maximum_bytes <= 0
            or type(arguments) is not dict
            or arguments.get("segment_max_records") != segment_max_records
            or any(type(arguments.get(key)) is not int or arguments[key] <= 0
                for key in ("max_paths", "max_nodes", "max_bytes"))):
        _fail("RETIREMENT_BOUNDS_REQUIRED")
    maximum_bytes = min(maximum_bytes, 64 * 1024**2)
    empty = {"schema_version": RETIREMENT_SEGMENT_SCHEMA,
        "generation_context_sha256": generation_context_sha256, "files": []}
    overhead = len(_canonical_json_bytes(empty, maximum_bytes))
    # Each unchanged V1 file record contributes eleven JSON nodes; the envelope seven.
    record_limit = min(segment_max_records, (MAX_JSON_NODES - 7) // 11)
    segments, pending = [], []
    pending_bytes, count, total_bytes, previous = overhead, 0, 0, ""

    def finish_segment() -> None:
        body = {**empty, "files": list(pending)}
        descriptor = _retirement_segment_descriptor(body, maximum_bytes)
        segments.append({**descriptor, "body": body})

    for value in records:
        record = _retirement_record(value)
        relative = record["relative_path"]
        if relative <= previous:
            _fail("RETIREMENT_SEGMENTS_INVALID")
        previous = relative
        count += 1
        total_bytes += record["bytes"]
        if count > arguments["max_paths"] or total_bytes > arguments["max_bytes"]:
            _fail("RETIREMENT_PAYLOAD_BOUND_EXCEEDED")
        encoded_size = len(_canonical_json_bytes(record, maximum_bytes))
        extra = encoded_size + bool(pending)
        if pending and (len(pending) >= record_limit or pending_bytes + extra > maximum_bytes):
            finish_segment()
            pending = []
            pending_bytes = overhead
            extra = encoded_size
        if pending_bytes + extra > maximum_bytes:
            _fail("JSON_SIZE_BOUND_EXCEEDED")
        pending.append(record)
        pending_bytes += extra
    if pending:
        finish_segment()
    intent = {"schema_version": RETIREMENT_SCHEMA_V2,
        "generation_context_sha256": generation_context_sha256, "arguments": arguments,
        "segments": [{key: value for key, value in segment.items() if key != "body"}
            for segment in segments],
        "retired_payload_files": count, "retired_logical_bytes": total_bytes}
    encoded = _canonical_json_bytes(intent, maximum_bytes)
    return {"intent": intent, "intent_sha256": hashlib.sha256(encoded).hexdigest(),
        "intent_size_bytes": len(encoded), "segments": tuple(segments)}


def _retirement_segment_records(segments: Iterable[dict]) -> Iterator[dict]:
    previous = ""
    context_sha = None
    for segment in segments:
        if type(segment) is not dict or "body" not in segment:
            _fail("RETIREMENT_SEGMENTS_INVALID")
        body = segment["body"]
        if (type(body) is not dict or set(body) != {"schema_version", "generation_context_sha256", "files"}
                or body["schema_version"] != RETIREMENT_SEGMENT_SCHEMA
                or not _is_sha256(body["generation_context_sha256"])
                or type(body["files"]) is not list or not body["files"]
                or len(body["files"]) > MAX_RETIREMENT_SEGMENT_RECORDS):
            _fail("RETIREMENT_SEGMENTS_INVALID")
        if context_sha is not None and context_sha != body["generation_context_sha256"]:
            _fail("RETIREMENT_SEGMENTS_INVALID")
        context_sha = body["generation_context_sha256"]
        for record in body["files"]:
            _retirement_record(record)
        descriptor = _retirement_segment_descriptor(body, 64 * 1024**2)
        if set(segment) != {*descriptor, "body"} or any(segment[key] != value for key, value in descriptor.items()):
            _fail("RETIREMENT_SEGMENTS_INVALID")
        for value in body["files"]:
            record = _retirement_record(value)
            if record["relative_path"] <= previous:
                _fail("RETIREMENT_SEGMENTS_INVALID")
            previous = record["relative_path"]
            yield record


def verify_segmented_retirement_inventory(current_records: Iterable[dict], segments: Iterable[dict]) -> None:
    """Accept only an unchanged, ordered survivor subset of the saved inventory."""
    expected = iter(_retirement_segment_records(segments))
    saved = next(expected, None)
    previous = ""
    for value in current_records:
        record = _retirement_record(value)
        relative = record["relative_path"]
        if relative <= previous:
            _fail("RETIREMENT_PAYLOAD_CHANGED")
        previous = relative
        while saved is not None and saved["relative_path"] < relative:
            saved = next(expected, None)
        if saved != record:
            _fail("RETIREMENT_PAYLOAD_CHANGED")
        saved = next(expected, None)
    for _ in expected:
        pass


def _retirement_v2_intent(root: Path, expected_sha: Optional[str], arguments: dict, maximum: int) -> tuple[dict, str]:
    intent, digest = _retirement_control(root, RETIREMENT_INTENT_RELATIVE_PATH, maximum)
    fields = {"schema_version", "generation_context_sha256", "arguments", "segments",
        "retired_payload_files", "retired_logical_bytes"}
    if (set(intent) != fields or intent["schema_version"] != RETIREMENT_SCHEMA_V2
            or not _is_sha256(intent["generation_context_sha256"])
            or intent["arguments"] != arguments or type(intent["segments"]) is not list
            or (expected_sha is not None and digest != expected_sha)
            or any(type(intent[key]) is not int or intent[key] < 0
                for key in ("retired_payload_files", "retired_logical_bytes"))):
        _fail("RETIREMENT_RESUME_BINDING_CHANGED")
    previous, count, total = "", 0, 0
    segment_fields = {"relative_path", "sha256", "size_bytes", "count", "bytes", "first_path", "last_path"}
    for segment in intent["segments"]:
        if (type(segment) is not dict or set(segment) != segment_fields
                or not _is_sha256(segment["sha256"])
                or segment["relative_path"] != f"generation-retirement.segment.{segment['sha256']}.json"
                or type(segment["size_bytes"]) is not int or not 0 < segment["size_bytes"] <= maximum
                or type(segment["count"]) is not int or not 1 <= segment["count"] <= arguments["segment_max_records"]
                or type(segment["bytes"]) is not int or segment["bytes"] < 0
                or any(type(segment[key]) is not str for key in ("first_path", "last_path"))
                or not previous < segment["first_path"] <= segment["last_path"]):
            _fail("RETIREMENT_SEGMENTS_INVALID")
        _relative_parts(segment["first_path"])
        _relative_parts(segment["last_path"])
        previous = segment["last_path"]
        count += segment["count"]
        total += segment["bytes"]
    if (count != intent["retired_payload_files"] or total != intent["retired_logical_bytes"]
            or count > arguments["max_paths"] or total > arguments["max_bytes"]):
        _fail("RETIREMENT_SEGMENTS_INVALID")
    return intent, digest


def _retirement_v2_segments(root: Path, intent: dict, maximum: int) -> Iterator[dict]:
    for descriptor in intent["segments"]:
        body, digest = _retirement_control(root, descriptor["relative_path"], maximum)
        if (digest != descriptor["sha256"] or set(body) != {"schema_version", "generation_context_sha256", "files"}
                or body.get("schema_version") != RETIREMENT_SEGMENT_SCHEMA
                or body.get("generation_context_sha256") != intent["generation_context_sha256"]
                or type(body.get("files")) is not list or not body["files"]):
            _fail("RETIREMENT_SEGMENTS_INVALID")
        for record in body["files"]:
            _retirement_record(record)
        if _retirement_segment_descriptor(body, maximum) != descriptor:
            _fail("RETIREMENT_SEGMENTS_INVALID")
        yield {**descriptor, "body": body}


def _retirement_control_os_error(exc: OSError, step: str) -> AuthoritySpoolError:
    diagnostics = {"step": step, "exception_type": type(exc).__name__[:80],
        "errno": exc.errno if type(exc.errno) is int else None,
        "winerror": getattr(exc, "winerror", None)}
    if type(diagnostics["winerror"]) is not int:
        diagnostics["winerror"] = None
    error = AuthoritySpoolError("RETIREMENT_CONTROL_WRITE_FAILED")
    error.diagnostics = diagnostics
    return error


def _retirement_publish_v2(root: Path, relative: str, body: dict, maximum: int,
        *, root_owner: Optional[dict] = None) -> str:
    encoded = _canonical_json_bytes(body, maximum)
    expected = hashlib.sha256(encoded).hexdigest()
    temporary, descriptor, identity = None, None, None
    step = "target_check"
    try:
        info = root.stat(follow_symlinks=False)
        root_identity = root_owner["root_identity"] if root_owner is not None else [info.st_dev, info.st_ino]
        declared_root = root_owner["root"] if root_owner is not None else str(root)
        _assert_retirement_root(root, root_identity, declared_root=declared_root)
        target = _local_path(root, relative)
        _reject_reparse_chain(target)
        if not target.exists():
            step = "temporary_open"
            temporary = _temporary_path(root)
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | int(getattr(os, "O_BINARY", 0))
            descriptor = os.open(temporary, flags, 0o600)
            try:
                step = "temporary_identity"
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    _fail("TEMPORARY_FILE_NOT_REGULAR")
                step = "temporary_write"
                _write_all(descriptor, memoryview(encoded))
                step = "temporary_fsync"
                os.fsync(descriptor)
            finally:
                try:
                    try:
                        identity = _file_identity(os.fstat(descriptor))
                    except OSError:
                        step = "temporary_identity"
                        raise
                finally:
                    try:
                        os.close(descriptor)
                    except OSError:
                        step = "temporary_close"
                        raise
                    finally:
                        descriptor = None
            step = "temporary_publish"
            _assert_retirement_root(root, root_identity, declared_root=declared_root)
            info = temporary.stat(follow_symlinks=False)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or _file_identity(info) != identity):
                _fail("RETIREMENT_CONTROL_TEMP_CHANGED")
            _publish_temp_exclusive(root, temporary, relative)
            step = "temporary_cleanup"
            _assert_retirement_root(root, root_identity, declared_root=declared_root)
            if _file_identity(temporary.stat(follow_symlinks=False)) != identity:
                _fail("RETIREMENT_CONTROL_TEMP_CHANGED")
            temporary.unlink()
            temporary = None
        step = "readback"
        _assert_retirement_root(root, root_identity, declared_root=declared_root)
        observed, digest = _retirement_control(root, relative, maximum)
        if observed != body or digest != expected:
            _fail("RETIREMENT_CONTROL_CONFLICT")
        return digest
    except OSError as exc:
        raise _retirement_control_os_error(exc, step) from exc
    except AuthoritySpoolError as exc:
        if isinstance(exc.__cause__, OSError):
            raise _retirement_control_os_error(exc.__cause__, step) from exc.__cause__
        raise
    finally:
        if temporary is not None and identity is not None and temporary.exists():
            try:
                _assert_retirement_root(root, root_identity, declared_root=declared_root)
                info = temporary.stat(follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or _file_identity(info) != identity:
                    _fail("RETIREMENT_CONTROL_TEMP_CHANGED")
                temporary.unlink()
            except OSError as exc:
                raise _retirement_control_os_error(exc, "temporary_cleanup") from exc


def _retirement_v2_cleanup(root: Path, intent_sha: str, arguments: dict, maximum: int, owner: dict) -> None:
    intent_path = _local_path(root, RETIREMENT_INTENT_RELATIVE_PATH)
    if not intent_path.exists():
        with os.scandir(root) as entries:
            if any(entry.name.startswith("generation-retirement.segment.") for entry in entries):
                _fail("RETIREMENT_SEGMENTS_INVALID")
        return
    intent, _ = _retirement_v2_intent(root, intent_sha, arguments, maximum)
    allowed = {segment["relative_path"] for segment in intent["segments"]}
    with os.scandir(root) as entries:
        if any(entry.name.startswith("generation-retirement.segment.") and entry.name not in allowed for entry in entries):
            _fail("RETIREMENT_SEGMENTS_INVALID")
    remaining = []
    for segment in intent["segments"]:
        path = _local_path(root, segment["relative_path"])
        if not path.exists():
            _reject_reparse_chain(path)
            continue
        safe = _safe_regular_file(root, segment["relative_path"])
        identity = _file_identity(safe.stat(follow_symlinks=False))
        _, digest = _retirement_control(root, segment["relative_path"], maximum)
        if digest != segment["sha256"]:
            _fail("RETIREMENT_INTENT_CHANGED")
        remaining.append((segment["relative_path"], identity))
    safe = _safe_regular_file(root, RETIREMENT_INTENT_RELATIVE_PATH)
    remaining.append((RETIREMENT_INTENT_RELATIVE_PATH, _file_identity(safe.stat(follow_symlinks=False))))
    for relative, identity in remaining:
        _assert_retirement_root(root, owner["root_identity"], declared_root=owner["root"])
        path = _safe_regular_file(root, relative)
        if _file_identity(path.stat(follow_symlinks=False)) != identity:
            _fail("RETIREMENT_INTENT_CHANGED")
        try:
            path.unlink()
        except OSError as exc:
            raise _retirement_control_os_error(exc, "intent_cleanup") from exc


def _retirement_v2_prepare(root: Path, *, existing: Optional[dict], context_body: dict,
        context_sha: str, owner: dict, arguments: dict, direct: frozenset[str],
        unit_lock_paths: tuple[str, ...], maximum: int) -> tuple[dict, str, tuple[str, ...]]:
    import shutil

    saved = None
    controls = ()
    intent_path = _local_path(root, RETIREMENT_INTENT_RELATIVE_PATH)
    if intent_path.exists():
        saved, saved_sha = _retirement_v2_intent(root,
            existing["intent_sha256"] if existing is not None else None, arguments, maximum)
        if saved["generation_context_sha256"] != context_sha:
            _fail("RETIREMENT_RESUME_BINDING_CHANGED")
        controls = tuple(segment["relative_path"] for segment in saved["segments"])
    elif existing is not None:
        _fail("RETIREMENT_INTENT_CHANGED")
    if existing is not None:
        return saved, saved_sha, controls
    records, _, root_bytes, nodes = _retirement_inventory(root, direct,
        max_paths=arguments["max_paths"], max_nodes=arguments["max_nodes"], max_bytes=arguments["max_bytes"],
        retained_unit_locks=unit_lock_paths, retained_controls=controls)
    plan = plan_segmented_retirement(records, generation_context_sha256=context_sha,
        arguments=arguments, maximum_bytes=maximum, segment_max_records=arguments["segment_max_records"])
    if saved is not None and plan["intent_sha256"] != saved_sha:
        _fail("RETIREMENT_PAYLOAD_CHANGED")
    present_bytes, present_nodes = 0, 0
    for relative in (RETIREMENT_INTENT_RELATIVE_PATH, *controls):
        path = _local_path(root, relative)
        if path.exists():
            present_bytes += _safe_regular_file(root, relative).stat(follow_symlinks=False).st_size
            present_nodes += 1
    control_bytes = plan["intent_size_bytes"] + sum(segment["size_bytes"] for segment in plan["segments"]) + 2 * MAX_CONTEXT_BYTES
    if (nodes + 3 + len(plan["segments"]) - present_nodes > arguments["max_nodes"]
            or root_bytes - present_bytes + control_bytes > min(arguments["max_bytes"], context_body["limits"]["max_generation_bytes"])
            or shutil.disk_usage(root).free - (control_bytes - present_bytes) < context_body["limits"]["min_free_bytes"]):
        _fail("RETIREMENT_CONTROL_CAPACITY")
    intent_sha = _retirement_publish_v2(root, RETIREMENT_INTENT_RELATIVE_PATH, plan["intent"], maximum, root_owner=owner)
    for segment in plan["segments"]:
        _retirement_publish_v2(root, segment["relative_path"], segment["body"], maximum, root_owner=owner)
    marker = {"schema_version": RETIREMENT_SCHEMA_V2, "status": "RETIRING", "generation_context_sha256": context_sha,
        "generation_id": owner["generation_id"], "cell_id": owner["cell_id"], "intent_sha256": intent_sha,
        **{key: arguments[key] for key in ("source_fence_path", "source_fence_sha256", "retirement_receipt_path", "workspace_owner_sha256")}}
    _retirement_publish_v2(root, RETIREMENT_RELATIVE_PATH, marker, MAX_CONTEXT_BYTES, root_owner=owner)
    return plan["intent"], intent_sha, tuple(segment["relative_path"] for segment in plan["segments"])


def _retirement_inventory(root: Path, direct: frozenset[str], *, max_paths: int, max_nodes: int,
        max_bytes: int, retained_unit_locks: tuple[str, ...] = (),
        retained_controls: tuple[str, ...] = ()) -> tuple[list[dict], list[dict], int, int]:
    keep = {CONTEXT_RELATIVE_PATH, WORKSPACE_OWNER_RELATIVE_PATH, RETIREMENT_RELATIVE_PATH,
        RETIREMENT_INTENT_RELATIVE_PATH, RETIREMENT_COMPLETE_RELATIVE_PATH,
        spool_quota.QUOTA_STATE_NAME, spool_quota.QUOTA_LOCK_NAME, "declared-scheduler.lock"}
    keep.update(retained_unit_locks)
    keep.update(retained_controls)
    records, directories, stack = [], [], [root]
    nodes, total = 0, 0
    while stack:
        parent = stack.pop()
        with os.scandir(parent) as stream:
            for entry in stream:
                nodes += 1
                if nodes > max_nodes:
                    _fail("RETIREMENT_NODE_BOUND_EXCEEDED")
                relative = Path(entry.path).relative_to(root).as_posix()
                if any(name in {"real_control", "STOP", ".git"} for name in PurePosixPath(relative).parts):
                    _fail("RETIREMENT_FOREIGN_FILE")
                path = _local_path(root, relative)
                info = path.stat(follow_symlinks=False)
                if _is_reparse(path) or stat.S_ISLNK(info.st_mode):
                    _fail("RETIREMENT_REPARSE_REFUSED")
                if stat.S_ISDIR(info.st_mode):
                    if not _retirement_owned(relative, direct, directory=True):
                        _fail("RETIREMENT_FOREIGN_DIRECTORY")
                    directories.append({"relative_path": relative, "identity": [info.st_dev, info.st_ino]})
                    stack.append(path)
                elif stat.S_ISREG(info.st_mode):
                    if info.st_nlink != 1:
                        _fail("RETIREMENT_HARDLINK_REFUSED")
                    total += info.st_size
                    if total > max_bytes:
                        _fail("RETIREMENT_PAYLOAD_BOUND_EXCEEDED")
                    if relative in keep:
                        continue
                    if not _retirement_owned(relative, direct, directory=False):
                        _fail("RETIREMENT_FOREIGN_FILE")
                    records.append({"relative_path": relative, "identity": list(_file_identity(info)), "bytes": info.st_size})
                    if len(records) > max_paths:
                        _fail("RETIREMENT_PAYLOAD_BOUND_EXCEEDED")
                else:
                    _fail("RETIREMENT_NONREGULAR_REFUSED")
    return sorted(records, key=lambda item: item["relative_path"]), directories, total, nodes


@contextmanager
def _retirement_unit_locks(root: Path, paths: tuple[str, ...], owner: dict) -> Iterator[None]:
    from contextlib import ExitStack
    import pm_native_storage as storage
    with ExitStack() as handles:
        for relative in paths:
            _assert_retirement_root(root, owner["root_identity"], declared_root=owner["root"])
            try:
                path = _safe_regular_file(root, relative)
                descriptor = os.open(path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
                handle = handles.enter_context(os.fdopen(descriptor, "r+b", buffering=0))
                _reject_reparse_chain(path)
                before, opened = path.stat(follow_symlinks=False), os.fstat(handle.fileno())
                if (not stat.S_ISREG(opened.st_mode) or opened.st_size != 1 or opened.st_nlink != 1
                        or _file_identity(before) != _file_identity(opened)):
                    _fail("RETIREMENT_UNIT_LOCK_UNVERIFIED")
                if not storage._try_kernel_lock(handle):
                    _fail("RETIREMENT_UNIT_WRITER_ACTIVE")
                handles.callback(storage._release_kernel_lock, handle)
                _assert_retirement_root(root, owner["root_identity"], declared_root=owner["root"])
                _reject_reparse_chain(path)
                current = path.stat(follow_symlinks=False)
                handle.seek(0)
                if _file_identity(current) != _file_identity(opened) or handle.read(1) != b"0":
                    _fail("RETIREMENT_UNIT_LOCK_UNVERIFIED")
            except OSError as exc:
                raise AuthoritySpoolError("RETIREMENT_UNIT_LOCK_UNVERIFIED") from exc
        yield


def retire_completed_generation(root: Path, *, expected_context_sha256: str, workspace_owner_sha256: str,
        source_fence_path: Path, source_fence_sha256: str, retirement_receipt_path: Path,
        protected_paths: tuple[Path, ...], owned_direct_files: tuple[str, ...],
        unit_lock_paths: tuple[str, ...],
        max_paths: int, max_nodes: int, max_bytes: int, segment_max_records: Optional[int] = None) -> dict:
    """Retire one new owned workspace after actual caller finish and retained readback.

    The fence is caller assurance, not a new financial seal. No native input,
    result replay, payload hash, cold archive, orphan reaping or case restart.
    """
    root = _validate_root(Path(root), create=False)
    if (segment_max_records is not None and (type(segment_max_records) is not int
            or not 1 <= segment_max_records <= MAX_RETIREMENT_SEGMENT_RECORDS)):
        _fail("RETIREMENT_BOUNDS_REQUIRED")
    retirement_schema = RETIREMENT_SCHEMA if segment_max_records is None else RETIREMENT_SCHEMA_V2
    if not (root / WORKSPACE_OWNER_RELATIVE_PATH).exists():
        _fail("RETIREMENT_OWNER_REQUIRED")
    owner_before_lock, owner_before_sha = _retirement_control(root, WORKSPACE_OWNER_RELATIVE_PATH, MAX_CONTEXT_BYTES)
    _assert_retirement_root(root, owner_before_lock.get("root_identity"), declared_root=owner_before_lock.get("root"))
    if owner_before_sha != workspace_owner_sha256 or owner_before_lock.get("generation_context_sha256") != expected_context_sha256:
        _fail("RETIREMENT_OWNER_REQUIRED")
    for value in (expected_context_sha256, workspace_owner_sha256, source_fence_sha256):
        _sha256(value, "RETIREMENT_BINDING_INVALID")
    if (type(protected_paths) is not tuple or not protected_paths
            or type(owned_direct_files) is not tuple
            or type(unit_lock_paths) is not tuple
            or any(type(value) is not int or value <= 0 for value in (max_paths, max_nodes, max_bytes))):
        _fail("RETIREMENT_BOUNDS_REQUIRED")
    if any(type(relative) is not str for relative in owned_direct_files):
        _fail("RETIREMENT_DIRECT_PATH_INVALID")
    direct = frozenset(owned_direct_files)
    if len(direct) != len(owned_direct_files) or len(direct) > max_paths:
        _fail("RETIREMENT_DIRECT_PATH_INVALID")
    for relative in direct:
        if type(relative) is not str or len(relative.encode("utf-8")) > 4096:
            _fail("RETIREMENT_DIRECT_PATH_INVALID")
        _relative_parts(relative)
    expected_unit_locks = {relative for relative in direct if PurePosixPath(relative).name.casefold() == "units.lock"}
    if (len(unit_lock_paths) > max_paths or any(type(relative) is not str for relative in unit_lock_paths)
            or len(set(unit_lock_paths)) != len(unit_lock_paths)
            or set(unit_lock_paths) != expected_unit_locks
            or any(relative not in direct
                or PurePosixPath(relative).name.casefold() != "units.lock" for relative in unit_lock_paths)):
        _fail("RETIREMENT_UNIT_LOCK_PATHS_INVALID")
    source_fence_path, retirement_receipt_path = Path(source_fence_path), Path(retirement_receipt_path)
    if source_fence_path.resolve() == retirement_receipt_path.resolve():
        _fail("RETIREMENT_RECEIPT_PATH_CONFLICT")
    _validate_root(retirement_receipt_path.parent, create=False)
    for path in (*protected_paths, source_fence_path, retirement_receipt_path):
        path = Path(path)
        if not path.is_absolute():
            _fail("RETIREMENT_PROTECTED_PATH_INVALID")
        _reject_reparse_chain(path)
        path = path.resolve()
        if path == root or path.is_relative_to(root) or root.is_relative_to(path):
            _fail("RETIREMENT_PROTECTED_PATH_OVERLAP")
    v2_arguments = {"source_fence_path": str(source_fence_path), "source_fence_sha256": source_fence_sha256,
        "retirement_receipt_path": str(retirement_receipt_path), "workspace_owner_sha256": workspace_owner_sha256,
        "protected_paths": [str(Path(value).resolve()) for value in protected_paths],
        "owned_direct_files": sorted(direct), "unit_lock_paths": list(unit_lock_paths),
        "max_paths": max_paths, "max_nodes": max_nodes, "max_bytes": max_bytes,
        "segment_max_records": segment_max_records}
    with spool_quota.generation_retirement_lock(root, max_root_entries=max_nodes), _retirement_unit_locks(root, unit_lock_paths, owner_before_lock):
        existing = read_generation_retirement(root, expected_context_sha256=expected_context_sha256)
        if existing is not None:
            marker, _ = _retirement_control(root, RETIREMENT_RELATIVE_PATH, MAX_CONTEXT_BYTES)
            if (marker["schema_version"] != retirement_schema
                    or marker["workspace_owner_sha256"] != workspace_owner_sha256
                    or marker["source_fence_path"] != str(source_fence_path)
                    or marker["source_fence_sha256"] != source_fence_sha256
                    or marker["retirement_receipt_path"] != str(retirement_receipt_path)):
                _fail("RETIREMENT_RESUME_BINDING_CHANGED")
        if existing is not None and existing["status"] == "ARCHIVED_RECORD_ONLY":
            if existing.get("retained_unit_locks") != list(unit_lock_paths):
                _fail("RETIREMENT_RESUME_BINDING_CHANGED")
            if segment_max_records is not None:
                context_body, _ = _retirement_control(root, CONTEXT_RELATIVE_PATH, MAX_CONTEXT_BYTES)
                maximum = min(context_body["limits"]["max_json_bytes"], 64 * 1024**2)
                _retirement_v2_cleanup(root, marker["intent_sha256"], v2_arguments, maximum, owner_before_lock)
            return existing
        if existing is None and retirement_receipt_path.exists():
            _fail("RETIREMENT_RECEIPT_ALREADY_EXISTS")
        context_body, context_sha = _retirement_control(root, CONTEXT_RELATIVE_PATH, MAX_CONTEXT_BYTES)
        if (context_sha != expected_context_sha256 or set(context_body) != _CONTEXT_FIELDS
                or context_body["schema_version"] != GENERATION_CONTEXT_SCHEMA):
            _fail("RETIREMENT_CONTEXT_CHANGED")
        _limits_from_payload(context_body["limits"], legacy=False)
        _identity(context_body["generation_id"], "GENERATION_ID")
        _sha256(context_body["native_universe_sha256"], "NATIVE_UNIVERSE_SHA256")
        _sha256(context_body["config_sha256"], "CONFIG_SHA256")
        if not (root / WORKSPACE_OWNER_RELATIVE_PATH).exists():
            _fail("RETIREMENT_OWNER_REQUIRED")
        owner, owner_sha = _retirement_control(root, WORKSPACE_OWNER_RELATIVE_PATH, MAX_CONTEXT_BYTES)
        if (set(owner) != {"schema_version", "generation_id", "generation_context_sha256", "creation_provenance",
                    "cell_id", "cell_contract_sha256", "root", "root_identity"}
                or owner_sha != workspace_owner_sha256 or owner.get("schema_version") != WORKSPACE_OWNER_SCHEMA
                or owner.get("generation_context_sha256") != context_sha
                or owner.get("generation_id") != context_body.get("generation_id")
                or not _is_sha256(owner.get("cell_id")) or not _is_sha256(owner.get("cell_contract_sha256"))
                or owner.get("creation_provenance") != "ABSENT_OR_EMPTY_BEFORE_CONTEXT_CREATION"):
            _fail("RETIREMENT_OWNER_REQUIRED")
        _assert_retirement_root(root, owner["root_identity"], declared_root=owner["root"])
        fence = _retirement_fence(source_fence_path, source_fence_sha256, context_sha256=context_sha,
            generation_id=owner["generation_id"], cell_id=owner["cell_id"], workspace_sha256=owner_sha,
            working_root=root, owner=owner)
        terminal_refusal = fence.get("schema_version") == TERMINAL_REFUSAL_FENCE_SCHEMA
        post_source_abort = fence.get("schema_version") == POST_SOURCE_ABORT_FENCE_SCHEMA
        native_completion = fence.get("schema_version") == NATIVE_COMPLETION_FENCE_SCHEMA
        if terminal_refusal and (unit_lock_paths or direct != frozenset({
                f"declared-slots/{fence['binding']['precommit_sha256']}/precommit.json"})):
            _fail("RETIREMENT_REFUSAL_SOURCE_STATE_INVALID")
        if post_source_abort and (unit_lock_paths or direct != frozenset({
                f"declared-slots/{fence['binding']['precommit_sha256']}/precommit.json"})):
            _fail("RETIREMENT_ABORT_SOURCE_STATE_INVALID")
        maximum = min(context_body["limits"]["max_json_bytes"], 64 * 1024**2)
        arguments = {"source_fence_path": str(source_fence_path), "source_fence_sha256": source_fence_sha256,
            "retirement_receipt_path": str(retirement_receipt_path), "workspace_owner_sha256": owner_sha,
            "protected_paths": [str(Path(value).resolve()) for value in protected_paths],
            "owned_direct_files": sorted(direct), "unit_lock_paths": list(unit_lock_paths),
            "max_paths": max_paths, "max_nodes": max_nodes, "max_bytes": max_bytes}
        controls = ()
        if segment_max_records is not None:
            arguments = v2_arguments
            intent, intent_sha, controls = _retirement_v2_prepare(root, existing=existing,
                context_body=context_body, context_sha=context_sha, owner=owner,
                arguments=arguments, direct=direct, unit_lock_paths=unit_lock_paths, maximum=maximum)
        elif existing is None:
            import shutil
            records, _, root_bytes, nodes = _retirement_inventory(root, direct, max_paths=max_paths, max_nodes=max_nodes,
                max_bytes=max_bytes, retained_unit_locks=unit_lock_paths)
            intent = {"schema_version": RETIREMENT_SCHEMA, "generation_context_sha256": context_sha,
                "arguments": arguments, "files": records}
            intent_bytes = len(_canonical_json_bytes(intent, maximum))
            control_bytes = intent_bytes + 2 * MAX_CONTEXT_BYTES
            if (nodes + 3 > max_nodes or root_bytes + control_bytes > min(max_bytes, context_body["limits"]["max_generation_bytes"])
                    or shutil.disk_usage(root).free - control_bytes < context_body["limits"]["min_free_bytes"]):
                _fail("RETIREMENT_CONTROL_CAPACITY")
            intent_sha = _retirement_publish(root, RETIREMENT_INTENT_RELATIVE_PATH, intent, maximum, root_owner=owner)
            marker = {"schema_version": RETIREMENT_SCHEMA, "status": "RETIRING", "generation_context_sha256": context_sha,
                "generation_id": owner["generation_id"], "cell_id": owner["cell_id"], "intent_sha256": intent_sha,
                **{key: arguments[key] for key in ("source_fence_path", "source_fence_sha256", "retirement_receipt_path", "workspace_owner_sha256")}}
            _retirement_publish(root, RETIREMENT_RELATIVE_PATH, marker, MAX_CONTEXT_BYTES, root_owner=owner)
        else:
            intent, intent_sha = _retirement_control(root, RETIREMENT_INTENT_RELATIVE_PATH, maximum)
            if (intent_sha != existing["intent_sha256"] or intent.get("arguments") != arguments
                    or intent.get("generation_context_sha256") != context_sha):
                _fail("RETIREMENT_RESUME_BINDING_CHANGED")
            records = intent["files"]
        inventory_options = {"retained_controls": controls} if segment_max_records is not None else {}
        current, _, _, _ = _retirement_inventory(root, direct, max_paths=max_paths, max_nodes=max_nodes,
            max_bytes=max_bytes, retained_unit_locks=unit_lock_paths, **inventory_options)
        if segment_max_records is not None:
            verify_segmented_retirement_inventory(current, _retirement_v2_segments(root, intent, maximum))
        else:
            expected = {record["relative_path"]: record for record in records}
            if any(expected.get(record["relative_path"]) != record for record in current):
                _fail("RETIREMENT_PAYLOAD_CHANGED")
        for record in current:
            _assert_retirement_root(root, owner["root_identity"], declared_root=owner["root"])
            path = _safe_regular_file(root, record["relative_path"])
            info = path.stat(follow_symlinks=False)
            if list(_file_identity(info)) != record["identity"] or info.st_nlink != 1:
                _fail("RETIREMENT_PAYLOAD_CHANGED")
            path.unlink()
        remaining, directories, _, _ = _retirement_inventory(root, direct, max_paths=max_paths, max_nodes=max_nodes,
            max_bytes=max_bytes, retained_unit_locks=unit_lock_paths, **inventory_options)
        if remaining:
            _fail("RETIREMENT_PAYLOAD_REMAINS")
        retained_directories = {parent.as_posix() for relative in unit_lock_paths for parent in PurePosixPath(relative).parents}
        for directory in sorted(directories, key=lambda value: len(PurePosixPath(value["relative_path"]).parts), reverse=True):
            if directory["relative_path"] in retained_directories:
                continue
            _assert_retirement_root(root, owner["root_identity"], declared_root=owner["root"])
            path = _local_path(root, directory["relative_path"])
            _reject_reparse_chain(path)
            current_directory = path.stat(follow_symlinks=False)
            if [current_directory.st_dev, current_directory.st_ino] != directory["identity"]:
                _fail("RETIREMENT_DIRECTORY_CHANGED")
            path.rmdir()
        receipt = {"schema_version": retirement_schema, "status": "ARCHIVED_RECORD_ONLY",
            "generation_context_sha256": context_sha, "generation_id": owner["generation_id"],
            "cell_id": owner["cell_id"], "cell_contract_sha256": owner["cell_contract_sha256"],
            "workspace_owner_sha256": owner_sha, "intent_sha256": intent_sha,
            "source_fence": {"path": str(source_fence_path), "sha256": source_fence_sha256},
            "precommit_sha256": (fence["binding"] if terminal_refusal or post_source_abort or native_completion
                else fence["source_summary"]["binding"]).get("precommit_sha256"),
            "end_grid_rows": fence["end_grid_rows"],
            "retired_payload_files": intent["retired_payload_files"] if segment_max_records is not None else len(records),
            "retired_logical_bytes": intent["retired_logical_bytes"] if segment_max_records is not None else sum(record["bytes"] for record in records),
            "payloads_retained": False, "genuine_cold_reopen_available": False,
            "descriptor_assurance": "IDENTITY_ONLY_AFTER_WORKSPACE_RELEASE",
            "retained_unit_locks": list(unit_lock_paths),
            "qualification_authority": False, "capital_authority": False,
            "live_authority": False, "order_authority": False}
        if segment_max_records is not None:
            receipt["intent_segment_count"] = len(intent["segments"])
        if terminal_refusal:
            receipt.update(result_kind=TERMINAL_REFUSAL_RESULT_KIND,
                terminal_refusal=dict(fence["refusal"]))
        if post_source_abort:
            receipt.update(result_kind=POST_SOURCE_ABORT_RESULT_KIND, terminal_refusal=dict(fence["refusal"]),
                source_abort=_retirement_abort_summary(fence["abort_report"]),
                full_native_context_eligibility=False, financial_consumer_ready=False)
        external_root = _validate_root(retirement_receipt_path.parent, create=False)
        _assert_retirement_root(root, owner["root_identity"], declared_root=owner["root"])
        publish = _retirement_publish if segment_max_records is None else _retirement_publish_v2
        receipt_sha = publish(external_root, retirement_receipt_path.name, receipt, MAX_CONTEXT_BYTES)
        publish(root, RETIREMENT_COMPLETE_RELATIVE_PATH, {"schema_version": retirement_schema,
            "generation_context_sha256": context_sha, "receipt_sha256": receipt_sha}, MAX_CONTEXT_BYTES, root_owner=owner)
        if segment_max_records is not None:
            _retirement_v2_cleanup(root, intent_sha, arguments, maximum, owner)
            return read_generation_retirement(root, expected_context_sha256=context_sha)
        intent_path = _safe_regular_file(root, RETIREMENT_INTENT_RELATIVE_PATH)
        if _retirement_control(root, RETIREMENT_INTENT_RELATIVE_PATH, maximum)[1] != intent_sha:
            _fail("RETIREMENT_INTENT_CHANGED")
        _assert_retirement_root(root, owner["root_identity"], declared_root=owner["root"])
        intent_path.unlink()
        return read_generation_retirement(root, expected_context_sha256=context_sha)


__all__ = [
    "ARTIFACT_DESCRIPTOR_SCHEMA",
    "AuthoritySpoolError",
    "CANONICAL_JSON_FORMAT",
    "COMPACT_RECEIPT_SCHEMA",
    "CURRENT_REFIT_MANIFEST_DOMAIN",
    "GENERATION_CONTEXT_SCHEMA",
    "GENERAL_MANIFEST_DOMAIN",
    "GenerationSpoolContext",
    "HISTORICAL_ANCHOR_MANIFEST_DOMAIN",
    "LEGACY_GENERATION_CONTEXT_SCHEMA",
    "NATIVE_COMPLETION_FENCE_SCHEMA",
    "RETIREMENT_SCHEMA_V2",
    "RETIREMENT_SEGMENT_SCHEMA",
    "MAX_RETIREMENT_SEGMENT_RECORDS",
    "SYMBOL_MANIFEST_SCHEMA",
    "SpoolLimits",
    "SymbolAuthoritySink",
    "freeze_generation_context",
    "is_resource_capacity_error",
    "open_symbol_sink",
    "reopen_generation_context",
    "reserve_transient_publication",
    "validate_spool_limits",
    "verify_symbol_receipt",
    "write_canonical_json_artifact",
    "write_opaque_artifact",
    "read_owned_generation_workspace",
    "read_generation_retirement",
    "retire_completed_generation",
    "plan_segmented_retirement",
    "verify_segmented_retirement_inventory",
]
