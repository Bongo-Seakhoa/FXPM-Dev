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
    _bounded_json_value(value)
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
) -> GenerationSpoolContext:
    """Create or byte-identically reopen a generation context before workers."""

    safe_root = _validate_root(Path(root), create=True)
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
    _publish_exact_json(bootstrap, CONTEXT_RELATIVE_PATH, payload, MAX_CONTEXT_BYTES)
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
]
