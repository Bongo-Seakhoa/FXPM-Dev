"""Bounded hot/cold lifecycle for immutable native-market evidence.

The storage layer is intentionally small and evidence-oriented.  One
canonical-root kernel lock serialises admission, reconciliation, retirement
and restore.  Every traversal, JSON document and archive has an explicit hard
bound.  Retirement is journalled and may start only after its content-addressed
cold object has been streamed, published and independently verified.

This module is storage authority only.  It does not grant source, statistical,
profitability, promotion or live-trading authority.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import math
import os
import shutil
import stat
import tarfile
import tempfile
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence


MIB = 1024 * 1024
PIN_SCHEMA = "fxpm-native-storage-pin-v1"
ARCHIVE_SCHEMA = "fxpm-native-lossless-archive-v1"
ARCHIVE_INDEX_SCHEMA = "fxpm-native-lossless-archive-index-v1"
DEFAULT_MAX_ARCHIVE_BYTES = 256 * 1024 * 1024 * 1024
DEFAULT_MAX_UNCOMPRESSED_BYTES = 1024 * 1024 * 1024 * 1024
DEFAULT_MAX_ARCHIVE_FILES = 100_000
DEFAULT_MAX_ARCHIVE_MANIFEST_BYTES = 64 * MIB
DEFAULT_MAX_DIRECTORY_ENTRIES = 10_000
DEFAULT_MAX_TREE_ENTRIES = 250_000
DEFAULT_MAX_JSON_NODES = 250_000
DEFAULT_LOCK_TIMEOUT_SECONDS = 30.0
LIFECYCLE_LOCK_NAME = ".native-storage.lifecycle.lock"
RETIRE_INTENT_SCHEMA = "fxpm-native-retirement-intent-v1"
RETIRE_COMPLETION_SCHEMA = "fxpm-native-retirement-completion-v1"
RUN_RESERVATION_SCHEMA = "fxpm-native-run-reservation-v1"
RUN_BEGIN_SCHEMA = "fxpm-native-run-begin-v1"
RUN_RESERVATION_COMPLETION_SCHEMA = "fxpm-native-run-reservation-completion-v1"
RUN_RESERVATION_CANCELLATION_SCHEMA = "fxpm-native-run-reservation-cancellation-v1"


class NativeStorageError(RuntimeError):
    """Base class for fail-closed native storage errors."""


class NativeStorageIntegrityError(NativeStorageError):
    """An evidence pointer, hash, archive or path is malformed."""


class NativeStorageBoundError(NativeStorageError):
    """A proposed operation would breach a configured hard bound."""


class NativeStorageProtectedError(NativeStorageError):
    """An archive request contains currently protected hot evidence."""


class NativeStorageBusyError(NativeStorageError):
    """The canonical native-storage lifecycle lock could not be acquired."""


class _DuplicateJSONKey(ValueError):
    pass


def _lexists(path: Path) -> bool:
    return os.path.lexists(os.fspath(path))


def _is_reparse(stat_result: os.stat_result) -> bool:
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(stat_result, "st_file_attributes", 0) & attribute)


def _safe_lstat(path: Path, *, require_regular: bool = False) -> os.stat_result:
    try:
        result = os.lstat(path)
    except OSError as exc:
        raise NativeStorageIntegrityError(f"required evidence is unavailable: {path}") from exc
    if stat.S_ISLNK(result.st_mode) or _is_reparse(result):
        raise NativeStorageIntegrityError(f"links/reparse points are forbidden: {path}")
    if require_regular:
        if not stat.S_ISREG(result.st_mode):
            raise NativeStorageIntegrityError(f"evidence must be a regular file: {path}")
        if int(result.st_nlink) != 1:
            raise NativeStorageIntegrityError(f"hard-linked evidence is forbidden: {path}")
    return result


def _canonical_directory(
    path: Path,
    *,
    label: str,
    create: bool = False,
) -> Path:
    supplied = Path(path).absolute()
    if create and not _lexists(supplied):
        existing = supplied
        missing: list[Path] = []
        while not _lexists(existing):
            missing.append(existing)
            if existing.parent == existing:
                raise NativeStorageIntegrityError(f"no safe ancestor for {label}")
            existing = existing.parent
        ancestor = _safe_lstat(existing)
        if not stat.S_ISDIR(ancestor.st_mode):
            raise NativeStorageIntegrityError(f"{label} ancestor is not a directory")
        for directory in reversed(missing):
            directory.mkdir(exist_ok=False)
    cursor = Path(supplied.anchor)
    for part in supplied.parts[1:]:
        cursor = cursor / part
        cursor_stat = _safe_lstat(cursor)
        if not stat.S_ISDIR(cursor_stat.st_mode):
            raise NativeStorageIntegrityError(f"{label} path crosses a non-directory")
    result = _safe_lstat(supplied)
    if not stat.S_ISDIR(result.st_mode):
        raise NativeStorageIntegrityError(f"{label} must be a directory")
    resolved = supplied.resolve()
    if resolved == Path(resolved.anchor).resolve():
        raise NativeStorageIntegrityError(f"{label} cannot be a filesystem root")
    return resolved


def _prospective_canonical_directory(path: Path, *, label: str) -> Path:
    """Validate every existing component without creating the missing leaf."""
    supplied = Path(path).absolute()
    cursor = Path(supplied.anchor)
    resolved_cursor = cursor.resolve()
    for part in supplied.parts[1:]:
        candidate = cursor / part
        if _lexists(candidate):
            result = _safe_lstat(candidate)
            if not stat.S_ISDIR(result.st_mode):
                raise NativeStorageIntegrityError(f"{label} crosses a non-directory")
            resolved_cursor = candidate.resolve()
        else:
            resolved_cursor = resolved_cursor / part
        cursor = candidate
    if resolved_cursor == Path(resolved_cursor.anchor).resolve():
        raise NativeStorageIntegrityError(f"{label} cannot be a filesystem root")
    return resolved_cursor


def _validate_disjoint_roots(native_root: Path, cold_root: Path) -> tuple[Path, Path]:
    native = _canonical_directory(native_root, label="native_root")
    cold = _prospective_canonical_directory(cold_root, label="cold_root")
    try:
        cold.relative_to(native)
    except ValueError:
        pass
    else:
        raise NativeStorageIntegrityError("hot and cold roots cannot overlap or nest")
    try:
        native.relative_to(cold)
    except ValueError:
        pass
    else:
        raise NativeStorageIntegrityError("hot and cold roots cannot overlap or nest")
    return native, cold


def _scandir_names(path: Path, *, max_entries: int) -> list[str]:
    """Return at most ``max_entries`` names after a cap+1 scandir probe."""
    if int(max_entries) < 1:
        raise ValueError("max_entries must be positive")
    names: list[str] = []
    try:
        with os.scandir(path) as scanner:
            iterator = iter(scanner)
            for index in range(int(max_entries) + 1):
                try:
                    entry = next(iterator)
                except StopIteration:
                    break
                if index == int(max_entries):
                    raise NativeStorageBoundError(
                        f"directory entry cap {max_entries} exceeded: {path}"
                    )
                names.append(entry.name)
    except NativeStorageError:
        raise
    except OSError as exc:
        raise NativeStorageIntegrityError(f"evidence directory is unreadable: {path}") from exc
    return sorted(names)


@dataclass(frozen=True)
class _TreeRecord:
    path: Path
    relative: str
    size: int
    identity: tuple[int, int, int, int, int]


@dataclass(frozen=True)
class TreeUsage:
    bytes: int
    files: int
    entries: int


def _file_identity(result: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        int(result.st_dev),
        int(result.st_ino),
        int(result.st_size),
        int(result.st_mtime_ns),
        int(result.st_nlink),
    )


def _walk_regular_files(
    root: Path,
    *,
    max_files: int,
    max_entries: int,
    max_directory_entries: int,
) -> list[_TreeRecord]:
    """Bounded, non-following tree walk using only context-managed scandir."""
    root = Path(root)
    root_stat = _safe_lstat(root)
    if stat.S_ISREG(root_stat.st_mode):
        if int(root_stat.st_nlink) != 1:
            raise NativeStorageIntegrityError(f"hard-linked evidence is forbidden: {root}")
        return [
            _TreeRecord(root, root.name, int(root_stat.st_size), _file_identity(root_stat))
        ]
    if not stat.S_ISDIR(root_stat.st_mode):
        raise NativeStorageIntegrityError(f"evidence tree root is not a directory: {root}")
    stack: list[tuple[Path, str]] = [(root, "")]
    records: list[_TreeRecord] = []
    observed_entries = 0
    while stack:
        directory, prefix = stack.pop()
        names = _scandir_names(directory, max_entries=max_directory_entries)
        observed_entries += len(names)
        if observed_entries > int(max_entries):
            raise NativeStorageBoundError(f"tree entry cap {max_entries} exceeded: {root}")
        child_directories: list[tuple[Path, str]] = []
        for name in names:
            child = directory / name
            child_stat = _safe_lstat(child)
            relative = f"{prefix}/{name}".lstrip("/")
            if stat.S_ISDIR(child_stat.st_mode):
                child_directories.append((child, relative))
            elif stat.S_ISREG(child_stat.st_mode):
                if int(child_stat.st_nlink) != 1:
                    raise NativeStorageIntegrityError(
                        f"hard-linked evidence is forbidden: {child}"
                    )
                records.append(
                    _TreeRecord(
                        child,
                        relative,
                        int(child_stat.st_size),
                        _file_identity(child_stat),
                    )
                )
                if len(records) > int(max_files):
                    raise NativeStorageBoundError(
                        f"tree file cap {max_files} exceeded: {root}"
                    )
            else:
                raise NativeStorageIntegrityError(
                    f"non-regular evidence entry is forbidden: {child}"
                )
        stack.extend(reversed(child_directories))
    return records


def _tree_usage(
    root: Path,
    *,
    max_files: int,
    max_entries: int,
    max_directory_entries: int,
) -> TreeUsage:
    root = Path(root)
    if not _lexists(root):
        return TreeUsage(0, 0, 0)
    records = _walk_regular_files(
        root,
        max_files=max_files,
        max_entries=max_entries,
        max_directory_entries=max_directory_entries,
    )
    return TreeUsage(
        bytes=sum(record.size for record in records),
        files=len(records),
        entries=len(records),
    )


def _read_bounded_bytes(path: Path, *, max_bytes: int) -> bytes:
    if int(max_bytes) < 1:
        raise ValueError("max_bytes must be positive")
    result = _safe_lstat(Path(path), require_regular=True)
    if result.st_size <= 0 or result.st_size > int(max_bytes):
        raise NativeStorageIntegrityError(f"evidence file has an unsafe size: {path}")
    payload = bytearray()
    try:
        with Path(path).open("rb") as handle:
            while len(payload) <= int(max_bytes):
                chunk = handle.read(min(MIB, int(max_bytes) + 1 - len(payload)))
                if not chunk:
                    break
                payload.extend(chunk)
    except OSError as exc:
        raise NativeStorageIntegrityError(f"evidence file is unreadable: {path}") from exc
    if not payload or len(payload) > int(max_bytes):
        raise NativeStorageIntegrityError(f"evidence file has an unsafe size: {path}")
    after = _safe_lstat(Path(path), require_regular=True)
    if _file_identity(after) != _file_identity(result):
        raise NativeStorageIntegrityError(f"evidence changed while it was read: {path}")
    return bytes(payload)


def _duplicate_rejecting_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, child in pairs:
        if key in value:
            raise _DuplicateJSONKey(key)
        value[key] = child
    return value


def _reject_json_constant(value: str) -> Any:
    raise ValueError(f"non-finite JSON constant: {value}")


def _decode_json_object(payload: bytes, *, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_duplicate_rejecting_pairs,
            parse_constant=_reject_json_constant,
        )
    except (
        _DuplicateJSONKey,
        UnicodeError,
        json.JSONDecodeError,
        RecursionError,
        ValueError,
    ) as exc:
        raise NativeStorageIntegrityError(f"{label} is unreadable or has duplicate keys") from exc
    if not isinstance(value, Mapping):
        raise NativeStorageIntegrityError(f"{label} must be an object")
    return value


def _try_kernel_lock(handle: io.BufferedRandom) -> bool:
    if os.name == "nt":
        import msvcrt

        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except (BlockingIOError, OSError):
        return False


def _release_kernel_lock(handle: io.BufferedRandom) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def native_storage_lifecycle_lock(
    native_root: Path,
    *,
    timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS,
) -> Iterator[Path]:
    """Hold the single persistent, kernel-backed lifecycle mutex for a root."""
    if float(timeout_seconds) < 0:
        raise ValueError("timeout_seconds cannot be negative")
    root = _canonical_directory(native_root, label="native_root", create=True)
    lock_path = root / LIFECYCLE_LOCK_NAME
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise NativeStorageIntegrityError("lifecycle lock cannot be opened safely") from exc
    with os.fdopen(descriptor, "r+b", buffering=0) as raw:
        path_stat = _safe_lstat(lock_path, require_regular=True)
        opened_stat = os.fstat(raw.fileno())
        if (int(path_stat.st_dev), int(path_stat.st_ino)) != (
            int(opened_stat.st_dev),
            int(opened_stat.st_ino),
        ):
            raise NativeStorageIntegrityError("lifecycle lock path changed during open")
        if opened_stat.st_size == 0:
            raw.write(b"\0")
            raw.flush()
            os.fsync(raw.fileno())
        deadline = time.monotonic() + float(timeout_seconds)
        while not _try_kernel_lock(raw):
            if time.monotonic() >= deadline:
                raise NativeStorageBusyError(
                    f"native storage lifecycle lock is busy: {lock_path}"
                )
            time.sleep(0.01)
        lock_stat = _safe_lstat(lock_path, require_regular=True)
        repinned_stat = os.fstat(raw.fileno())
        if (
            (int(lock_stat.st_dev), int(lock_stat.st_ino))
            != (int(repinned_stat.st_dev), int(repinned_stat.st_ino))
            or (int(opened_stat.st_dev), int(opened_stat.st_ino))
            != (int(repinned_stat.st_dev), int(repinned_stat.st_ino))
            or lock_stat.st_size != 1
        ):
            raise NativeStorageIntegrityError("lifecycle lock file is malformed")
        try:
            yield root
        finally:
            # Closing the descriptor also releases the kernel lock after an
            # abnormal process exit; the persistent file is never ownership.
            _release_kernel_lock(raw)


def sha256_file(path: Path, *, chunk_bytes: int = MIB) -> str:
    if int(chunk_bytes) < 1:
        raise ValueError("chunk_bytes must be positive")
    before = _safe_lstat(Path(path), require_regular=True)
    digest = hashlib.sha256()
    try:
        with Path(path).open("rb") as handle:
            while True:
                chunk = handle.read(int(chunk_bytes))
                if not chunk:
                    break
                digest.update(chunk)
    except OSError as exc:
        raise NativeStorageIntegrityError(f"evidence is unreadable: {path}") from exc
    after = _safe_lstat(Path(path), require_regular=True)
    if _file_identity(before) != _file_identity(after):
        raise NativeStorageIntegrityError(f"evidence changed while hashing: {path}")
    return digest.hexdigest()


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def _load_json(path: Path, *, max_bytes: int = 64 * MIB) -> Mapping[str, Any]:
    return _decode_json_object(
        _read_bounded_bytes(Path(path), max_bytes=max_bytes),
        label=f"evidence JSON {path}",
    )


def _is_sha256(value: Any) -> bool:
    text = str(value or "").lower()
    return len(text) == 64 and all(ch in "0123456789abcdef" for ch in text)


def _relative(native_root: Path, value: Any) -> Path:
    text = str(value or "").strip().replace("\\", "/")
    pure = PurePosixPath(text)
    if (
        not text
        or pure.is_absolute()
        or ".." in pure.parts
        or "." in pure.parts
        or any(":" in part for part in pure.parts)
    ):
        raise NativeStorageIntegrityError(f"unsafe native relative path: {value!r}")
    lexical = native_root / Path(*pure.parts)
    cursor = native_root
    for part in pure.parts:
        cursor = cursor / part
        if _lexists(cursor):
            _safe_lstat(cursor)
    candidate = lexical.resolve()
    try:
        candidate.relative_to(native_root)
    except ValueError as exc:
        raise NativeStorageIntegrityError(f"native path escapes its root: {value!r}") from exc
    return candidate


def _safe_archive_relative(value: Any) -> PurePosixPath:
    text = str(value or "")
    pure = PurePosixPath(text)
    if (
        not text
        or pure.is_absolute()
        or ".." in pure.parts
        or "." in pure.parts
        or any(":" in part or "\\" in part for part in pure.parts)
    ):
        raise NativeStorageIntegrityError("cold archive member path is unsafe")
    return pure


def _safe_native_payload_relative(value: Any) -> PurePosixPath:
    pure = _safe_archive_relative(value)
    if (
        pure.parts[0] in {".restore-staging", ".retire-staging"}
        or LIFECYCLE_LOCK_NAME in pure.parts
        or any(part.endswith(".tmp") for part in pure.parts)
        or (
            pure.parts[0] == "runs"
            and any(
                part.startswith(".") and part.endswith(".run_manifest.staging")
                for part in pure.parts[1:]
            )
        )
    ):
        raise NativeStorageIntegrityError("cold archive targets a lifecycle control path")
    return pure


def _rel(native_root: Path, path: Path) -> str:
    native_root = Path(native_root).resolve()
    lexical = Path(path).absolute()
    try:
        relative = lexical.relative_to(native_root)
    except ValueError as exc:
        raise NativeStorageIntegrityError(f"path is outside native root: {path}") from exc
    cursor = native_root
    for part in relative.parts:
        cursor = cursor / part
        if _lexists(cursor):
            _safe_lstat(cursor)
    try:
        lexical.resolve().relative_to(native_root)
    except ValueError as exc:
        raise NativeStorageIntegrityError(f"path escapes native root: {path}") from exc
    return relative.as_posix()


def _iter_relative_references(value: Any) -> Iterable[tuple[str, Any, Optional[Any]]]:
    """Yield bounded path/hash references without recursive Python calls."""
    stack: list[Any] = [value]
    observed = 0
    while stack:
        current = stack.pop()
        observed += 1
        if observed > DEFAULT_MAX_JSON_NODES:
            raise NativeStorageBoundError("evidence JSON node cap exceeded")
        if isinstance(current, Mapping):
            children: list[Any] = []
            for key, child in current.items():
                if str(key).endswith("_relative_path"):
                    prefix = str(key)[: -len("_relative_path")]
                    yield str(key), child, current.get(f"{prefix}_sha256")
                children.append(child)
            stack.extend(reversed(children))
        elif isinstance(current, list):
            stack.extend(reversed(current))


@dataclass
class ProtectionGraph:
    """Authenticated hot roots and every immutable file reachable from them."""

    native_root: Path
    protected: dict[str, set[str]] = field(default_factory=dict)
    edges: dict[str, set[str]] = field(default_factory=dict)
    expected_hashes: dict[str, set[str]] = field(default_factory=dict)
    observed_hashes: dict[str, str] = field(default_factory=dict)
    roots: set[str] = field(default_factory=set)
    warnings: list[str] = field(default_factory=list)
    max_files: int = DEFAULT_MAX_ARCHIVE_FILES
    max_entries: int = DEFAULT_MAX_TREE_ENTRIES
    max_directory_entries: int = DEFAULT_MAX_DIRECTORY_ENTRIES
    max_json_bytes: int = DEFAULT_MAX_ARCHIVE_MANIFEST_BYTES
    edge_count: int = 0

    def add_protection(self, path: Path, reason: str) -> None:
        relative = _rel(self.native_root, path)
        if relative not in self.protected and len(self.protected) >= self.max_entries:
            raise NativeStorageBoundError("global protection graph entry cap exceeded")
        self.protected.setdefault(relative, set()).add(str(reason))

    def add_edge(self, source: Path, target: Path, digest: Any = None) -> None:
        source_rel, target_rel = _rel(self.native_root, source), _rel(self.native_root, target)
        if target_rel not in self.edges.get(source_rel, set()):
            if self.edge_count >= self.max_entries:
                raise NativeStorageBoundError("global protection graph edge cap exceeded")
            self.edge_count += 1
        self.edges.setdefault(source_rel, set()).add(target_rel)
        if digest not in (None, ""):
            if not _is_sha256(digest):
                raise NativeStorageIntegrityError(
                    f"invalid SHA-256 binding from {source_rel} to {target_rel}"
                )
            observed = self.observed_hashes.get(target_rel)
            if observed is None:
                observed = sha256_file(target)
                self.observed_hashes[target_rel] = observed
            declared = str(digest).lower()
            if observed != declared:
                raise NativeStorageIntegrityError(
                    f"SHA-256 binding mismatch from {source_rel} to {target_rel}"
                )
            self.expected_hashes.setdefault(target_rel, set()).add(declared)

    def protect_tree(self, path: Path, reason: str) -> None:
        result = _safe_lstat(path)
        self.add_protection(path, reason)
        if stat.S_ISREG(result.st_mode):
            if int(result.st_nlink) != 1:
                raise NativeStorageIntegrityError(f"hard-linked evidence is forbidden: {path}")
            return
        if not stat.S_ISDIR(result.st_mode):
            raise NativeStorageIntegrityError(f"protected native evidence is unsafe: {path}")
        records = _walk_regular_files(
            path,
            max_files=self.max_files,
            max_entries=self.max_entries,
            max_directory_entries=self.max_directory_entries,
        )
        for record in records:
            self.add_protection(record.path, reason)

    def is_protected(self, path: Path) -> bool:
        candidate = _rel(self.native_root, path)
        prefix = candidate.rstrip("/") + "/"
        return candidate in self.protected or any(key.startswith(prefix) for key in self.protected)

    @property
    def protected_bytes(self) -> int:
        total = 0
        for relative in self.protected:
            path = self.native_root / Path(*PurePosixPath(relative).parts)
            result = _safe_lstat(path)
            if stat.S_ISREG(result.st_mode):
                if int(result.st_nlink) != 1:
                    raise NativeStorageIntegrityError(
                        f"hard-linked protected evidence is forbidden: {path}"
                    )
                total += int(result.st_size)
        return total


def _walk_json_references(
    graph: ProtectionGraph,
    path: Path,
    reason: str,
    *,
    seen: set[str],
) -> None:
    queue: deque[tuple[Path, str]] = deque([(Path(path), str(reason))])
    while queue:
        current, current_reason = queue.popleft()
        relative = _rel(graph.native_root, current)
        graph.add_protection(current, current_reason)
        if relative in seen:
            continue
        if len(seen) >= graph.max_files:
            raise NativeStorageBoundError("global JSON reference file cap exceeded")
        seen.add(relative)
        payload = _load_json(current, max_bytes=graph.max_json_bytes)
        for _field, raw_target, digest in _iter_relative_references(payload):
            target = _relative(graph.native_root, raw_target)
            if not _lexists(target):
                raise NativeStorageIntegrityError(
                    f"referenced native evidence is missing: {target}"
                )
            _safe_lstat(target, require_regular=True)
            graph.add_edge(current, target, digest)
            graph.add_protection(target, current_reason)
            if target.suffix.lower() == ".json" and _rel(
                graph.native_root, target
            ) not in seen:
                if len(seen) + len(queue) >= graph.max_files:
                    raise NativeStorageBoundError(
                        "global JSON reference file cap exceeded"
                    )
                queue.append((target, current_reason))


def _protect_active_pointer(
    graph: ProtectionGraph,
    pointer_path: Path,
    reason: str,
    *,
    seen: set[str],
) -> Optional[Mapping[str, Any]]:
    if not _lexists(pointer_path):
        return None
    pointer = _load_json(pointer_path, max_bytes=min(MIB, graph.max_json_bytes))
    graph.roots.add(_rel(graph.native_root, pointer_path))
    _walk_json_references(graph, pointer_path, reason, seen=seen)
    return pointer


def _activation_path_for_id(native_root: Path, activation_id: Any) -> Path:
    name = str(activation_id or "").strip()
    if not name or any(ch in name for ch in "/\\:") or name in {".", ".."}:
        raise NativeStorageIntegrityError("unsafe price activation id in terms evidence")
    return native_root / "activations" / f"{name}.json"


def _direct_children(
    root: Path,
    *,
    max_entries: int,
    directories_only: bool = False,
    suffix: Optional[str] = None,
) -> list[Path]:
    if not _lexists(root):
        return []
    _safe_lstat(root)
    children: list[Path] = []
    for name in _scandir_names(root, max_entries=max_entries):
        child = root / name
        result = _safe_lstat(child)
        if directories_only and not stat.S_ISDIR(result.st_mode):
            continue
        if suffix is not None and not name.endswith(suffix):
            continue
        children.append(child)
    return children


def _transient_roots(
    root: Path,
    *,
    max_entries: int,
    max_directory_entries: int,
) -> list[Path]:
    if not _lexists(root):
        return []
    stack = [root]
    found: list[Path] = []
    observed = 0
    while stack:
        directory = stack.pop()
        names = _scandir_names(directory, max_entries=max_directory_entries)
        observed += len(names)
        if observed > int(max_entries):
            raise NativeStorageBoundError("transient discovery entry cap exceeded")
        descend: list[Path] = []
        for name in names:
            child = directory / name
            result = _safe_lstat(child)
            if name.endswith((".staging", ".tmp")):
                found.append(child)
            elif stat.S_ISDIR(result.st_mode):
                descend.append(child)
            elif not stat.S_ISREG(result.st_mode):
                raise NativeStorageIntegrityError(f"unsafe transient evidence: {child}")
        stack.extend(reversed(descend))
    return sorted(found, key=lambda item: item.as_posix())


def build_protection_graph(
    native_root: Path,
    *,
    max_files: int = DEFAULT_MAX_ARCHIVE_FILES,
    max_entries: int = DEFAULT_MAX_TREE_ENTRIES,
    max_directory_entries: int = DEFAULT_MAX_DIRECTORY_ENTRIES,
    max_json_bytes: int = DEFAULT_MAX_ARCHIVE_MANIFEST_BYTES,
) -> ProtectionGraph:
    """Authenticate and map all evidence that must remain available in hot storage.

    Roots are the price and terms ACTIVE pointers, explicit immutable pins, active
    acquisition writers, unfinished runs, and audit directories that do not yet
    have an activation manifest.  The last category is intentionally fail-safe:
    the current audit writer has no shared lifecycle lock, so an apparently
    orphan audit may still be in progress.
    """
    native_root = _canonical_directory(native_root, label="native_root")
    graph = ProtectionGraph(
        native_root=native_root,
        max_files=int(max_files),
        max_entries=int(max_entries),
        max_directory_entries=int(max_directory_entries),
        max_json_bytes=int(max_json_bytes),
    )
    seen: set[str] = set()

    price_pointer = _protect_active_pointer(
        graph, native_root / "ACTIVE.json", "ACTIVE_PRICE", seen=seen
    )
    terms_pointer = _protect_active_pointer(
        graph, native_root / "terms" / "ACTIVE.json", "ACTIVE_TERMS", seen=seen
    )

    # Terms manifests bind the price activation by id/hash rather than by path.
    if terms_pointer:
        terms_manifest = _relative(native_root, terms_pointer.get("manifest_relative_path"))
        payload = _load_json(terms_manifest, max_bytes=graph.max_json_bytes)
        price_id = payload.get("price_activation_id")
        price_sha = payload.get("price_activation_manifest_sha256")
        price_manifest = _activation_path_for_id(native_root, price_id)
        if not _lexists(price_manifest):
            raise NativeStorageIntegrityError(
                "active terms evidence references a missing price activation"
            )
        _safe_lstat(price_manifest, require_regular=True)
        graph.add_edge(terms_manifest, price_manifest, price_sha)
        _walk_json_references(
            graph, price_manifest, "ACTIVE_TERMS_PRICE_BINDING", seen=seen
        )

    # Explicit pins are the only reliable way for winner books, replay packs and
    # promotion records outside data/native to retain historical evidence.
    pins_root = native_root / "PINS"
    if _lexists(pins_root):
        for pin_path in _direct_children(
            pins_root, max_entries=max_directory_entries, suffix=".json"
        ):
            if not stat.S_ISREG(_safe_lstat(pin_path).st_mode):
                raise NativeStorageIntegrityError(f"native pin is not a file: {pin_path}")
            pin = _load_json(
                pin_path, max_bytes=min(4 * MIB, graph.max_json_bytes)
            )
            if pin.get("schema_version") != PIN_SCHEMA:
                raise NativeStorageIntegrityError(f"unsupported native pin schema: {pin_path}")
            roots = pin.get("roots")
            if not isinstance(roots, list) or not roots:
                raise NativeStorageIntegrityError(f"native pin has no roots: {pin_path}")
            graph.roots.add(_rel(native_root, pin_path))
            graph.add_protection(pin_path, f"PIN:{pin_path.name}")
            for item in roots:
                if not isinstance(item, Mapping):
                    raise NativeStorageIntegrityError(f"malformed native pin root: {pin_path}")
                target = _relative(native_root, item.get("relative_path"))
                graph.add_edge(pin_path, target, item.get("sha256"))
                if target.suffix.lower() == ".json":
                    _walk_json_references(
                        graph, target, f"PIN:{pin_path.name}", seen=seen
                    )
                else:
                    graph.add_protection(target, f"PIN:{pin_path.name}")

    runs_root = native_root / "runs"
    if _lexists(runs_root):
        for run in _direct_children(
            runs_root, max_entries=max_directory_entries, directories_only=True
        ):
            if run.name.startswith("."):
                continue
            owner = runs_root / f".{run.name}.writer.owner.json"
            active_writer = False
            if _lexists(owner):
                owner_payload = _load_json(
                    owner, max_bytes=min(MIB, graph.max_json_bytes)
                )
                active_writer = owner_payload.get("active") is True
            manifest = run / "run_manifest.json"
            incomplete = True
            if _lexists(manifest):
                incomplete = (
                    _load_json(manifest, max_bytes=graph.max_json_bytes).get("complete")
                    is not True
                )
            if active_writer or incomplete:
                why = "ACTIVE_RUN_WRITER" if active_writer else "INCOMPLETE_RUN"
                graph.protect_tree(run, why)
                if _lexists(owner):
                    graph.add_protection(owner, why)
                lock = runs_root / f".{run.name}.writer.lock"
                if _lexists(lock):
                    graph.add_protection(lock, why)

        for transient in _direct_children(runs_root, max_entries=max_directory_entries):
            if transient.name.endswith((".staging", ".tmp")):
                graph.protect_tree(transient, "RUN_TRANSIENT")

    audits_root = native_root / "audits"
    activations_root = native_root / "activations"
    if _lexists(audits_root):
        for audit in _direct_children(
            audits_root, max_entries=max_directory_entries, directories_only=True
        ):
            activation_path = activations_root / f"{audit.name}.json"
            activation_published = False
            if _lexists(activation_path):
                activation_published = stat.S_ISREG(
                    _safe_lstat(activation_path, require_regular=True).st_mode
                )
            if not activation_published:
                graph.protect_tree(audit, "UNPUBLISHED_OR_INFLIGHT_AUDIT")
        for transient in _transient_roots(
            audits_root,
            max_entries=max_entries,
            max_directory_entries=max_directory_entries,
        ):
            graph.protect_tree(transient, "AUDIT_TRANSIENT")

    terms_root = native_root / "terms"
    if _lexists(terms_root):
        for transient in _transient_roots(
            terms_root,
            max_entries=max_entries,
            max_directory_entries=max_directory_entries,
        ):
            graph.protect_tree(transient, "TERMS_TRANSIENT")

    restore_staging = native_root / ".restore-staging"
    if _lexists(restore_staging):
        graph.protect_tree(restore_staging, "RESTORE_TRANSIENT")

    # A price pointer with no terms pointer is valid.  An active terms pointer
    # without an active price pointer is never coherent.
    if terms_pointer and not price_pointer:
        raise NativeStorageIntegrityError("active terms evidence exists without ACTIVE price evidence")
    return graph


def tree_bytes(
    root: Path,
    *,
    max_files: int = DEFAULT_MAX_ARCHIVE_FILES,
    max_entries: int = DEFAULT_MAX_TREE_ENTRIES,
    max_directory_entries: int = DEFAULT_MAX_DIRECTORY_ENTRIES,
) -> int:
    return _tree_usage(
        root,
        max_files=max_files,
        max_entries=max_entries,
        max_directory_entries=max_directory_entries,
    ).bytes


def _existing_ancestor(path: Path) -> Path:
    candidate = Path(path).resolve()
    while not _lexists(candidate):
        parent = candidate.parent
        if parent == candidate:
            raise NativeStorageIntegrityError(f"no existing filesystem ancestor for {path}")
        candidate = parent
    return candidate


def _ensure_safe_parent(root: Path, target: Path) -> None:
    try:
        relative = Path(target).absolute().relative_to(Path(root).resolve())
    except ValueError as exc:
        raise NativeStorageIntegrityError("publication target escapes its root") from exc
    cursor = Path(root).resolve()
    for part in relative.parts[:-1]:
        cursor = cursor / part
        if _lexists(cursor):
            result = _safe_lstat(cursor)
            if not stat.S_ISDIR(result.st_mode):
                raise NativeStorageIntegrityError("publication parent is unsafe")
        else:
            cursor.mkdir(exist_ok=False)


def _fsync_directory(path: Path) -> bool:
    """Durably flush a directory where the host OS exposes that primitive."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return False
    try:
        os.fsync(descriptor)
        return True
    except OSError:
        return False
    finally:
        os.close(descriptor)


def _remove_bounded_tree(
    root: Path,
    *,
    policy: "NativeStoragePolicy",
    after_unlink: Optional[Callable[[int], None]] = None,
) -> int:
    """Delete a known transient tree after identity-checking every leaf."""
    root = Path(root)
    if not _lexists(root):
        return 0
    root_stat = _safe_lstat(root)
    if not stat.S_ISDIR(root_stat.st_mode):
        raise NativeStorageIntegrityError("transient cleanup root is unsafe")
    records = _walk_regular_files(
        root,
        max_files=policy.max_hot_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
    )
    for index, record in enumerate(records, start=1):
        current = _safe_lstat(record.path, require_regular=True)
        if _file_identity(current) != record.identity:
            raise NativeStorageIntegrityError("transient changed before cleanup")
        record.path.unlink()
        if after_unlink is not None:
            after_unlink(index)
    directories: list[Path] = []
    stack = [root]
    observed = 0
    while stack:
        directory = stack.pop()
        directories.append(directory)
        names = _scandir_names(directory, max_entries=policy.max_directory_entries)
        observed += len(names)
        if observed > policy.max_scan_entries:
            raise NativeStorageBoundError("transient directory cap exceeded")
        for name in names:
            child = directory / name
            result = _safe_lstat(child)
            if stat.S_ISDIR(result.st_mode):
                stack.append(child)
            elif stat.S_ISREG(result.st_mode):
                raise NativeStorageIntegrityError("transient cleanup left an unexpected file")
            else:
                raise NativeStorageIntegrityError("transient cleanup found an unsafe entry")
    for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        directory.rmdir()
    return len(records)


@dataclass(frozen=True)
class NativeStoragePolicy:
    """Explicit hard limits; there are intentionally no risk-blind defaults."""

    max_hot_bytes: int
    max_cold_bytes: int
    min_filesystem_free_bytes: int
    max_complete_hot_runs: int
    max_inflight_runs: int = 1
    max_hot_files: int = DEFAULT_MAX_ARCHIVE_FILES
    max_cold_files: int = DEFAULT_MAX_ARCHIVE_FILES
    max_scan_entries: int = DEFAULT_MAX_TREE_ENTRIES
    max_directory_entries: int = DEFAULT_MAX_DIRECTORY_ENTRIES
    max_json_bytes: int = DEFAULT_MAX_ARCHIVE_MANIFEST_BYTES
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        for name in (
            "max_hot_bytes",
            "max_cold_bytes",
            "min_filesystem_free_bytes",
            "max_complete_hot_runs",
            "max_inflight_runs",
            "max_hot_files",
            "max_cold_files",
            "max_scan_entries",
            "max_directory_entries",
            "max_json_bytes",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if (
            isinstance(self.lock_timeout_seconds, bool)
            or not isinstance(self.lock_timeout_seconds, (int, float))
            or not math.isfinite(float(self.lock_timeout_seconds))
            or float(self.lock_timeout_seconds) < 0
        ):
            raise ValueError("lock_timeout_seconds cannot be negative")


@dataclass(frozen=True)
class GrowthDecision:
    allowed: bool
    expected_growth_bytes: int
    hot_bytes: int
    cold_bytes: int
    filesystem_free_bytes: int
    projected_hot_bytes: int
    projected_free_bytes: int
    complete_hot_runs: int
    inflight_runs: int
    expected_growth_files: int
    hot_files: int
    cold_files: int
    projected_hot_files: int
    reserved_hot_bytes: int
    reserved_hot_files: int
    reserved_run_slots: int
    reasons: tuple[str, ...]

    def require_allowed(self) -> "GrowthDecision":
        if not self.allowed:
            raise NativeStorageBoundError("; ".join(self.reasons))
        return self


@dataclass(frozen=True)
class _ReservationUsage:
    bytes: int = 0
    files: int = 0
    absent_run_slots: int = 0


def _reservation_directories(cold_root: Path) -> Mapping[str, Path]:
    transactions = cold_root / "transactions"
    return {
        "reservations": transactions / "run-reservations",
        "begins": transactions / "run-begins",
        "completions": transactions / "run-completions",
        "cancellations": transactions / "run-cancellations",
    }


def _load_run_reservation(
    path: Path, *, native_root: Path, policy: NativeStoragePolicy
) -> Mapping[str, Any]:
    value = _load_json(path, max_bytes=policy.max_json_bytes)
    required = {
        "schema_version",
        "reservation_id",
        "native_root_id",
        "run_id",
        "expected_growth_bytes",
        "expected_growth_files",
    }
    try:
        run_id = _valid_run_id(value.get("run_id"))
        if type(value.get("expected_growth_bytes")) is not int or type(
            value.get("expected_growth_files")
        ) is not int:
            raise TypeError
        expected_bytes = int(value["expected_growth_bytes"])
        expected_files = int(value["expected_growth_files"])
    except (NativeStorageIntegrityError, TypeError, ValueError, OverflowError) as exc:
        raise NativeStorageIntegrityError("run reservation fields are invalid") from exc
    basis = {
        "native_root_id": value.get("native_root_id"),
        "run_id": run_id,
        "expected_growth_bytes": expected_bytes,
        "expected_growth_files": expected_files,
    }
    reservation_id = hashlib.sha256(_canonical_bytes(basis)).hexdigest()
    if (
        set(value) != required
        or value.get("schema_version") != RUN_RESERVATION_SCHEMA
        or value.get("native_root_id") != _native_root_id(native_root)
        or expected_bytes < 1
        or expected_files < 1
        or value.get("reservation_id") != reservation_id
        or path.stem != reservation_id
    ):
        raise NativeStorageIntegrityError("run reservation binding is invalid")
    return value


def _validate_run_reservation_event(
    path: Path,
    reservation: Mapping[str, Any],
    *,
    schema: str,
    policy: NativeStoragePolicy,
) -> Mapping[str, Any]:
    value = _load_json(path, max_bytes=policy.max_json_bytes)
    required = {"schema_version", "reservation_id", "native_root_id", "run_id"}
    if (
        set(value) != required
        or value.get("schema_version") != schema
        or value.get("reservation_id") != reservation.get("reservation_id")
        or value.get("native_root_id") != reservation.get("native_root_id")
        or value.get("run_id") != reservation.get("run_id")
        or path.stem != reservation.get("reservation_id")
    ):
        raise NativeStorageIntegrityError("run reservation event is invalid")
    return value


def _active_run_reservations(
    native_root: Path,
    cold_root: Path,
    *,
    policy: NativeStoragePolicy,
) -> list[Mapping[str, Any]]:
    directories = _reservation_directories(cold_root)
    root = directories["reservations"]
    if not _lexists(root):
        return []
    paths = _direct_children(
        root, max_entries=policy.max_directory_entries, suffix=".json"
    )
    active: list[Mapping[str, Any]] = []
    seen_runs: set[str] = set()
    for path in paths:
        reservation = _load_run_reservation(
            path, native_root=native_root, policy=policy
        )
        reservation_id = str(reservation["reservation_id"])
        completion = directories["completions"] / f"{reservation_id}.json"
        cancellation = directories["cancellations"] / f"{reservation_id}.json"
        begin = directories["begins"] / f"{reservation_id}.json"
        terminal_events = int(_lexists(completion)) + int(_lexists(cancellation))
        if terminal_events > 1:
            raise NativeStorageIntegrityError("run reservation has conflicting terminal events")
        if _lexists(completion):
            _validate_run_reservation_event(
                completion,
                reservation,
                schema=RUN_RESERVATION_COMPLETION_SCHEMA,
                policy=policy,
            )
            continue
        if _lexists(cancellation):
            _validate_run_reservation_event(
                cancellation,
                reservation,
                schema=RUN_RESERVATION_CANCELLATION_SCHEMA,
                policy=policy,
            )
            continue
        if _lexists(begin):
            _validate_run_reservation_event(
                begin,
                reservation,
                schema=RUN_BEGIN_SCHEMA,
                policy=policy,
            )
        run_id = str(reservation["run_id"])
        if run_id in seen_runs:
            raise NativeStorageIntegrityError("multiple active reservations bind one run")
        seen_runs.add(run_id)
        active.append(reservation)
    return active


def _reservation_usage(
    native_root: Path,
    cold_root: Path,
    *,
    policy: NativeStoragePolicy,
) -> _ReservationUsage:
    reserved_bytes = reserved_files = absent_slots = 0
    for reservation in _active_run_reservations(
        native_root, cold_root, policy=policy
    ):
        run = native_root / "runs" / str(reservation["run_id"])
        used = TreeUsage(0, 0, 0)
        if _lexists(run):
            used = _tree_usage(
                run,
                max_files=policy.max_hot_files,
                max_entries=policy.max_scan_entries,
                max_directory_entries=policy.max_directory_entries,
            )
        else:
            absent_slots += 1
        reserved_bytes += max(
            0, int(reservation["expected_growth_bytes"]) - used.bytes
        )
        reserved_files += max(
            0, int(reservation["expected_growth_files"]) - used.files
        )
        if reserved_bytes > policy.max_hot_bytes or reserved_files > policy.max_hot_files:
            raise NativeStorageBoundError("active run reservations exceed a hot-store cap")
    return _ReservationUsage(reserved_bytes, reserved_files, absent_slots)


def _reject_acquisition_manifest_staging_residue(
    native_root: Path, *, policy: NativeStoragePolicy
) -> None:
    runs_root = native_root / "runs"
    if not _lexists(runs_root):
        return
    suffix = ".run_manifest.staging"
    for name in _scandir_names(runs_root, max_entries=policy.max_directory_entries):
        if not name.startswith(".") or not name.endswith(suffix):
            continue
        run_id = name[1 : -len(suffix)]
        try:
            run_id = _valid_run_id(run_id)
        except NativeStorageIntegrityError as exc:
            raise NativeStorageIntegrityError(
                "forged acquisition manifest staging filename"
            ) from exc
        residue = runs_root / name
        manifest = runs_root / run_id / "run_manifest.json"
        if not _lexists(manifest):
            raise NativeStorageIntegrityError(
                "orphan acquisition manifest staging residue is forbidden"
            )
        residue_stat = _safe_lstat(residue)
        manifest_stat = _safe_lstat(manifest)
        if (
            not stat.S_ISREG(residue_stat.st_mode)
            or not stat.S_ISREG(manifest_stat.st_mode)
            or int(residue_stat.st_nlink) != 2
            or int(manifest_stat.st_nlink) != 2
            or (int(residue_stat.st_dev), int(residue_stat.st_ino))
            != (int(manifest_stat.st_dev), int(manifest_stat.st_ino))
        ):
            raise NativeStorageIntegrityError(
                "forged acquisition manifest staging residue is forbidden"
            )
        raise NativeStorageProtectedError(
            "authenticated acquisition manifest staging residue requires explicit recovery"
        )


def preflight_growth(
    native_root: Path,
    cold_root: Path,
    *,
    expected_growth_bytes: int,
    expected_growth_files: int = 1,
    policy: NativeStoragePolicy,
    filesystem_free_bytes: Optional[int] = None,
) -> GrowthDecision:
    """Return a fail-before-growth decision without mutating either store."""
    if int(expected_growth_bytes) <= 0:
        raise ValueError("expected_growth_bytes must be positive")
    if int(expected_growth_files) <= 0:
        raise ValueError("expected_growth_files must be positive")
    native_root, cold_root = _validate_disjoint_roots(native_root, cold_root)
    _reject_acquisition_manifest_staging_residue(native_root, policy=policy)
    hot_usage = _tree_usage(
        native_root,
        max_files=policy.max_hot_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
    )
    cold_usage = _tree_usage(
        cold_root,
        max_files=policy.max_cold_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
    )
    hot, cold = hot_usage.bytes, cold_usage.bytes
    free = (
        int(filesystem_free_bytes)
        if filesystem_free_bytes is not None
        else int(shutil.disk_usage(native_root).free)
    )
    complete, inflight = 0, 0
    runs_root = native_root / "runs"
    if _lexists(runs_root):
        for run in _direct_children(
            runs_root,
            max_entries=policy.max_directory_entries,
            directories_only=True,
        ):
            if run.name.startswith("."):
                continue
            manifest = run / "run_manifest.json"
            if (
                _lexists(manifest)
                and _load_json(manifest, max_bytes=policy.max_json_bytes).get("complete")
                is True
            ):
                complete += 1
            else:
                inflight += 1
    reservations = _ReservationUsage()
    if _lexists(cold_root):
        reservations = _reservation_usage(
            native_root,
            _canonical_directory(cold_root, label="cold_root"),
            policy=policy,
        )
    projected_hot = hot + reservations.bytes + int(expected_growth_bytes)
    projected_hot_files = (
        hot_usage.files + reservations.files + int(expected_growth_files)
    )
    projected_free = free - reservations.bytes - int(expected_growth_bytes)
    reasons: list[str] = []
    if projected_hot > policy.max_hot_bytes:
        reasons.append(
            f"projected hot bytes {projected_hot} exceed cap {policy.max_hot_bytes}"
        )
    if cold > policy.max_cold_bytes:
        reasons.append(f"cold bytes {cold} exceed cap {policy.max_cold_bytes}")
    if projected_hot_files > policy.max_hot_files:
        reasons.append(
            f"projected hot files {projected_hot_files} exceed cap {policy.max_hot_files}"
        )
    if cold_usage.files > policy.max_cold_files:
        reasons.append(
            f"cold files {cold_usage.files} exceed cap {policy.max_cold_files}"
        )
    if projected_free < policy.min_filesystem_free_bytes:
        reasons.append(
            f"projected free bytes {projected_free} are below reserve "
            f"{policy.min_filesystem_free_bytes}"
        )
    projected_complete = complete + reservations.absent_run_slots + 1
    if projected_complete > policy.max_complete_hot_runs:
        reasons.append(
            f"projected complete hot runs {projected_complete} exceed cap "
            f"{policy.max_complete_hot_runs}"
        )
    projected_inflight = inflight + reservations.absent_run_slots + 1
    if projected_inflight > policy.max_inflight_runs:
        reasons.append(
            f"projected inflight runs {projected_inflight} exceed cap "
            f"{policy.max_inflight_runs}"
        )
    return GrowthDecision(
        allowed=not reasons,
        expected_growth_bytes=int(expected_growth_bytes),
        hot_bytes=hot,
        cold_bytes=cold,
        filesystem_free_bytes=free,
        projected_hot_bytes=projected_hot,
        projected_free_bytes=projected_free,
        complete_hot_runs=complete,
        inflight_runs=inflight,
        expected_growth_files=int(expected_growth_files),
        hot_files=hot_usage.files,
        cold_files=cold_usage.files,
        projected_hot_files=projected_hot_files,
        reserved_hot_bytes=reservations.bytes,
        reserved_hot_files=reservations.files,
        reserved_run_slots=reservations.absent_run_slots,
        reasons=tuple(reasons),
    )


def _archive_members(
    native_root: Path,
    paths: Iterable[Path],
    *,
    max_files: int,
    max_entries: int,
    max_directory_entries: int,
) -> list[tuple[str, Path]]:
    members: dict[str, Path] = {}
    for source_index, supplied in enumerate(paths):
        if source_index >= int(max_files):
            raise NativeStorageBoundError("archive source path cap exceeded")
        path = Path(supplied)
        if not path.is_absolute():
            path = native_root / path
        path_stat = _safe_lstat(path)
        path = path.absolute()
        relative = _rel(native_root, path)
        _safe_native_payload_relative(relative)
        if stat.S_ISREG(path_stat.st_mode):
            if int(path_stat.st_nlink) != 1:
                raise NativeStorageIntegrityError(f"hard-linked archive source: {path}")
            members[relative] = path
        elif stat.S_ISDIR(path_stat.st_mode):
            for record in _walk_regular_files(
                path,
                max_files=max_files,
                max_entries=max_entries,
                max_directory_entries=max_directory_entries,
            ):
                members[_rel(native_root, record.path)] = record.path
                _safe_native_payload_relative(_rel(native_root, record.path))
                if len(members) > int(max_files):
                    raise NativeStorageBoundError("archive file cap exceeded")
        else:
            raise NativeStorageIntegrityError(f"archive source is unsafe: {path}")
    if not members:
        raise NativeStorageIntegrityError("archive request contains no files")
    return sorted(members.items())


def _tar_info(name: str, size: int) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name=name)
    info.size = int(size)
    info.mtime = 0
    info.mode = 0o444
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    info.type = tarfile.REGTYPE
    return info


def _write_deterministic_archive(
    target: Path, manifest: Mapping[str, Any], members: list[tuple[str, Path]]
) -> None:
    with target.open("xb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as tar:
                manifest_bytes = _canonical_bytes(manifest)
                tar.addfile(
                    _tar_info("ARCHIVE_MANIFEST.json", len(manifest_bytes)),
                    io.BytesIO(manifest_bytes),
                )
                for relative, path in members:
                    with path.open("rb") as source:
                        tar.addfile(
                            _tar_info(
                                f"payload/{relative}",
                                _safe_lstat(path, require_regular=True).st_size,
                            ),
                            source,
                        )
        raw.flush()
        os.fsync(raw.fileno())


def verify_lossless_archive(
    path: Path,
    *,
    expected_sha256: Optional[str] = None,
    max_archive_bytes: int = DEFAULT_MAX_ARCHIVE_BYTES,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
    max_files: int = DEFAULT_MAX_ARCHIVE_FILES,
    max_manifest_bytes: int = DEFAULT_MAX_ARCHIVE_MANIFEST_BYTES,
) -> Mapping[str, Any]:
    """Stream and authenticate every archived byte; never extract during verification."""
    path = Path(path)
    if min(max_archive_bytes, max_uncompressed_bytes, max_files, max_manifest_bytes) < 1:
        raise ValueError("archive verification bounds must be positive")
    try:
        archive_size = _safe_lstat(path, require_regular=True).st_size
    except NativeStorageIntegrityError as exc:
        raise NativeStorageIntegrityError("cold archive is unavailable") from exc
    if archive_size <= 0 or archive_size > int(max_archive_bytes):
        raise NativeStorageIntegrityError("cold archive exceeds its verification byte bound")
    if expected_sha256 is not None:
        if not _is_sha256(expected_sha256) or sha256_file(path) != str(expected_sha256).lower():
            raise NativeStorageIntegrityError("cold archive SHA-256 mismatch")
    try:
        with tarfile.open(path, mode="r:gz") as tar:
            first = tar.next()
            if (
                first is None
                or first.name != "ARCHIVE_MANIFEST.json"
                or first.type not in (tarfile.REGTYPE, tarfile.AREGTYPE)
                or first.size <= 0
                or first.size > int(max_manifest_bytes)
            ):
                raise NativeStorageIntegrityError("cold archive manifest is missing")
            manifest_handle = tar.extractfile(first)
            if manifest_handle is None:
                raise NativeStorageIntegrityError("cold archive manifest is unreadable")
            manifest_payload = manifest_handle.read(int(first.size) + 1)
            if len(manifest_payload) != int(first.size):
                raise NativeStorageIntegrityError("cold archive manifest size is inconsistent")
            manifest = _decode_json_object(
                manifest_payload, label="cold archive manifest"
            )
            if (
                set(manifest)
                != {"schema_version", "files", "file_count", "uncompressed_bytes"}
                or manifest.get("schema_version") != ARCHIVE_SCHEMA
            ):
                raise NativeStorageIntegrityError("cold archive schema is invalid")
            declared = manifest.get("files")
            if (
                not isinstance(declared, list)
                or not declared
                or len(declared) > int(max_files)
            ):
                raise NativeStorageIntegrityError("cold archive file manifest is empty")
            expected = {
                str(item.get("relative_path")): item
                for item in declared
                if isinstance(item, Mapping)
            }
            if len(expected) != len(declared):
                raise NativeStorageIntegrityError("cold archive has duplicate or malformed file records")
            declared_total = 0
            for relative, record in expected.items():
                _safe_native_payload_relative(relative)
                try:
                    if set(record) != {"relative_path", "size", "sha256"} or isinstance(
                        record.get("size"), bool
                    ):
                        raise TypeError
                    size = int(record.get("size", -1))
                except (TypeError, ValueError, OverflowError) as exc:
                    raise NativeStorageIntegrityError(
                        "cold archive has an invalid member size"
                    ) from exc
                if size < 0 or not _is_sha256(record.get("sha256")):
                    raise NativeStorageIntegrityError(
                        "cold archive has an invalid file record"
                    )
                declared_total += size
                if declared_total > int(max_uncompressed_bytes):
                    raise NativeStorageIntegrityError(
                        "cold archive exceeds its uncompressed byte bound"
                    )
            try:
                if isinstance(manifest.get("file_count"), bool) or isinstance(
                    manifest.get("uncompressed_bytes"), bool
                ):
                    raise TypeError
                summary_count = int(manifest.get("file_count", -1))
                summary_bytes = int(manifest.get("uncompressed_bytes", -1))
            except (TypeError, ValueError, OverflowError) as exc:
                raise NativeStorageIntegrityError(
                    "cold archive summary is invalid"
                ) from exc
            if summary_count != len(expected) or summary_bytes != declared_total:
                raise NativeStorageIntegrityError("cold archive summary is inconsistent")
            observed_names: set[str] = set()
            while True:
                member = tar.next()
                if member is None:
                    break
                if (
                    member.type not in (tarfile.REGTYPE, tarfile.AREGTYPE)
                    or not member.name.startswith("payload/")
                ):
                    raise NativeStorageIntegrityError("cold archive contains a forbidden member")
                relative = member.name[len("payload/") :]
                _safe_native_payload_relative(relative)
                if relative not in expected:
                    raise NativeStorageIntegrityError("cold archive member path is unsafe")
                if relative in observed_names:
                    raise NativeStorageIntegrityError("cold archive member is duplicated")
                observed_names.add(relative)
                record = expected[relative]
                if int(record.get("size", -1)) != member.size:
                    raise NativeStorageIntegrityError("cold archive member size mismatch")
                handle = tar.extractfile(member)
                if handle is None:
                    raise NativeStorageIntegrityError("cold archive member is unreadable")
                digest = hashlib.sha256()
                while True:
                    chunk = handle.read(MIB)
                    if not chunk:
                        break
                    digest.update(chunk)
                if digest.hexdigest() != str(record.get("sha256") or "").lower():
                    raise NativeStorageIntegrityError("cold archive member SHA-256 mismatch")
            if observed_names != set(expected):
                raise NativeStorageIntegrityError("cold archive payload is incomplete")
            return manifest
    except NativeStorageIntegrityError:
        raise
    except (OSError, tarfile.TarError, UnicodeError, ValueError) as exc:
        raise NativeStorageIntegrityError("cold archive is unreadable or corrupt") from exc


def _content_addressed_manifest(
    archive: Path,
    object_id: str,
    *,
    policy: NativeStoragePolicy,
    expected_sha256: Optional[str] = None,
) -> Mapping[str, Any]:
    if not _is_sha256(object_id):
        raise NativeStorageIntegrityError("cold archive object id is invalid")
    manifest = verify_lossless_archive(
        archive,
        expected_sha256=expected_sha256,
        max_archive_bytes=DEFAULT_MAX_ARCHIVE_BYTES,
        max_uncompressed_bytes=DEFAULT_MAX_UNCOMPRESSED_BYTES,
        max_files=DEFAULT_MAX_ARCHIVE_FILES,
        max_manifest_bytes=policy.max_json_bytes,
    )
    observed_object_id = hashlib.sha256(_canonical_bytes(manifest)).hexdigest()
    if observed_object_id != str(object_id).lower():
        raise NativeStorageIntegrityError(
            "cold archive manifest does not match its content-addressed object id"
        )
    return manifest


def _verified_archive_index(
    cold_root: Path,
    object_id: str,
    *,
    policy: NativeStoragePolicy,
) -> tuple[Mapping[str, Any], Mapping[str, Any], Path]:
    if not _is_sha256(object_id):
        raise NativeStorageIntegrityError("cold archive object id is invalid")
    object_id = str(object_id).lower()
    index_path = cold_root / "indexes" / f"{object_id}.json"
    index = _load_json(index_path, max_bytes=min(MIB, policy.max_json_bytes))
    expected_relative = f"objects/{object_id}.tar.gz"
    required_index_keys = {
        "schema_version",
        "object_id",
        "archive_relative_path",
        "archive_sha256",
        "archive_bytes",
        "file_count",
        "uncompressed_bytes",
        "archive_directory_fsynced",
        "index_directory_fsync_supported",
    }
    if (
        set(index) != required_index_keys
        or index.get("schema_version") != ARCHIVE_INDEX_SCHEMA
        or index.get("object_id") != object_id
        or index.get("archive_relative_path") != expected_relative
        or not _is_sha256(index.get("archive_sha256"))
        or not isinstance(index.get("archive_directory_fsynced"), bool)
        or not isinstance(index.get("index_directory_fsync_supported"), bool)
    ):
        raise NativeStorageIntegrityError("cold archive index is invalid")
    archive = _relative(cold_root, expected_relative)
    manifest = _content_addressed_manifest(
        archive,
        object_id,
        policy=policy,
        expected_sha256=str(index["archive_sha256"]),
    )
    try:
        if any(
            type(index.get(field)) is not int
            for field in ("archive_bytes", "file_count", "uncompressed_bytes")
        ):
            raise TypeError
        archive_bytes = int(index.get("archive_bytes", -1))
        file_count = int(index.get("file_count", -1))
        uncompressed_bytes = int(index.get("uncompressed_bytes", -1))
    except (TypeError, ValueError, OverflowError) as exc:
        raise NativeStorageIntegrityError("cold archive index summary is invalid") from exc
    if (
        archive_bytes != _safe_lstat(archive, require_regular=True).st_size
        or file_count != int(manifest["file_count"])
        or uncompressed_bytes != int(manifest["uncompressed_bytes"])
    ):
        raise NativeStorageIntegrityError("cold archive index summary is inconsistent")
    return index, manifest, archive


def _cold_temp_object_id(name: str) -> str:
    if not name.startswith(".") or not name.endswith(".tmp"):
        raise NativeStorageIntegrityError("cold publication temp filename is invalid")
    parts = name[1:-4].split(".")
    if len(parts) != 2 or not _is_sha256(parts[0]) or not parts[1].isdigit():
        raise NativeStorageIntegrityError("cold publication temp filename is forged")
    return parts[0].lower()


def _reconcile_cold_publication_temps(
    cold_root: Path, *, policy: NativeStoragePolicy
) -> None:
    """Recover exact object/index temps; never delete unverified partial bytes."""
    objects = cold_root / "objects"
    indexes = cold_root / "indexes"
    if _lexists(objects):
        for child in _direct_children(
            objects, max_entries=policy.max_directory_entries
        ):
            if not child.name.startswith("."):
                continue
            object_id = _cold_temp_object_id(child.name)
            final = objects / f"{object_id}.tar.gz"
            if _lexists(final):
                child_stat = _safe_lstat(child)
                final_stat = _safe_lstat(final)
                same_inode = (
                    int(child_stat.st_dev),
                    int(child_stat.st_ino),
                ) == (int(final_stat.st_dev), int(final_stat.st_ino))
                if same_inode:
                    if int(child_stat.st_nlink) != 2:
                        raise NativeStorageIntegrityError(
                            "cold archive temp hardlink topology is invalid"
                        )
                    child.unlink()
                    _fsync_directory(objects)
                    _content_addressed_manifest(final, object_id, policy=policy)
                    continue
                temp_manifest = _content_addressed_manifest(
                    child, object_id, policy=policy
                )
                final_manifest = _content_addressed_manifest(
                    final, object_id, policy=policy
                )
                if temp_manifest != final_manifest or sha256_file(child) != sha256_file(
                    final
                ):
                    raise NativeStorageIntegrityError("cold archive temp conflicts with final")
                child.unlink()
                _fsync_directory(objects)
            else:
                _content_addressed_manifest(child, object_id, policy=policy)
                os.link(child, final)
                child.unlink()
                _fsync_directory(objects)
                _content_addressed_manifest(final, object_id, policy=policy)
    if _lexists(indexes):
        for child in _direct_children(
            indexes, max_entries=policy.max_directory_entries
        ):
            if not child.name.startswith("."):
                continue
            object_id = _cold_temp_object_id(child.name)
            final = indexes / f"{object_id}.json"
            if _lexists(final):
                child_stat = _safe_lstat(child)
                final_stat = _safe_lstat(final)
                same_inode = (
                    int(child_stat.st_dev),
                    int(child_stat.st_ino),
                ) == (int(final_stat.st_dev), int(final_stat.st_ino))
                if same_inode:
                    if int(child_stat.st_nlink) != 2:
                        raise NativeStorageIntegrityError(
                            "cold index temp hardlink topology is invalid"
                        )
                    child.unlink()
                    _fsync_directory(indexes)
                    _verified_archive_index(cold_root, object_id, policy=policy)
                    continue
                if _read_bounded_bytes(
                    child, max_bytes=policy.max_json_bytes
                ) != _read_bounded_bytes(final, max_bytes=policy.max_json_bytes):
                    raise NativeStorageIntegrityError("cold index temp conflicts with final")
                _verified_archive_index(cold_root, object_id, policy=policy)
                child.unlink()
                _fsync_directory(indexes)
            else:
                payload = _decode_json_object(
                    _read_bounded_bytes(child, max_bytes=policy.max_json_bytes),
                    label="cold index temp",
                )
                if payload.get("object_id") != object_id:
                    raise NativeStorageIntegrityError("cold index temp object id is invalid")
                os.link(child, final)
                child.unlink()
                _fsync_directory(indexes)
                _verified_archive_index(cold_root, object_id, policy=policy)


def _create_lossless_archive_locked(
    native_root: Path,
    cold_root: Path,
    source_paths: Iterable[Path],
    *,
    graph: Optional[ProtectionGraph] = None,
    policy: NativeStoragePolicy,
    filesystem_free_bytes: Optional[int] = None,
) -> Mapping[str, Any]:
    """Copy unprotected evidence into a deterministic verified cold object.

    Hot sources are intentionally retained.  A later retirement transaction may
    remove them only after all writers honour the same global storage lock.
    """
    native_root, cold_root_path = _validate_disjoint_roots(native_root, cold_root)
    if graph is not None and Path(graph.native_root).resolve() != native_root:
        raise NativeStorageIntegrityError("protection graph belongs to another native root")
    graph = build_protection_graph(
        native_root,
        max_files=policy.max_hot_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
        max_json_bytes=policy.max_json_bytes,
    )
    members = _archive_members(
        native_root,
        source_paths,
        max_files=policy.max_hot_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
    )
    protected = [relative for relative, path in members if graph.is_protected(path)]
    if protected:
        raise NativeStorageProtectedError(
            "archive request includes hot-protected evidence: " + ", ".join(protected[:5])
        )
    files = []
    for relative, path in members:
        source_stat = _safe_lstat(path, require_regular=True)
        files.append(
            {
                "relative_path": relative,
                "size": int(source_stat.st_size),
                "sha256": sha256_file(path),
            }
        )
    manifest: Mapping[str, Any] = {
        "schema_version": ARCHIVE_SCHEMA,
        "files": files,
        "file_count": len(files),
        "uncompressed_bytes": sum(int(item["size"]) for item in files),
    }
    if int(manifest["uncompressed_bytes"]) > policy.max_hot_bytes:
        raise NativeStorageBoundError(
            "archive payload exceeds the configured hot-store evidence bound"
        )
    object_id = hashlib.sha256(_canonical_bytes(manifest)).hexdigest()
    cold_root = cold_root_path
    objects = cold_root / "objects"
    indexes = cold_root / "indexes"
    final = objects / f"{object_id}.tar.gz"
    # TAR uses 512-byte records and Python pads to a 10 KiB record boundary.
    # DEFLATE's stored-block worst case is bounded by five bytes per 16,383
    # input bytes plus its header/trailer.  The extra MiB covers the index and
    # filesystem metadata conservatively without relying on compression ratio.
    manifest_size = len(_canonical_bytes(manifest))
    if manifest_size > policy.max_json_bytes:
        raise NativeStorageBoundError("archive manifest exceeds the policy JSON byte cap")
    tar_bytes = 512 + ((manifest_size + 511) // 512) * 512
    for _relative, source in members:
        source_size = _safe_lstat(source, require_regular=True).st_size
        tar_bytes += 512 + ((source_size + 511) // 512) * 512
    tar_bytes = ((tar_bytes + 1024 + 10239) // 10240) * 10240
    worst_case_archive_bytes = tar_bytes + ((tar_bytes + 16382) // 16383) * 5 + 18 + MIB
    if not _lexists(final):
        cold_usage = _tree_usage(
            cold_root,
            max_files=policy.max_cold_files,
            max_entries=policy.max_scan_entries,
            max_directory_entries=policy.max_directory_entries,
        )
        # Temporary and final are simultaneously addressable during the
        # create-only hard-link publication; reserve both logical paths.
        if cold_usage.bytes + (2 * worst_case_archive_bytes) + MIB > policy.max_cold_bytes:
            raise NativeStorageBoundError(
                "cold archive worst-case bytes would exceed the hard cold-store cap"
            )
        if cold_usage.files + 3 > policy.max_cold_files:
            raise NativeStorageBoundError(
                "cold archive publication would exceed the hard cold-store file cap"
            )
        free = (
            int(filesystem_free_bytes)
            if filesystem_free_bytes is not None
            else int(shutil.disk_usage(_existing_ancestor(cold_root)).free)
        )
        if free - worst_case_archive_bytes < policy.min_filesystem_free_bytes:
            raise NativeStorageBoundError(
                "cold archive worst-case bytes would breach the filesystem reserve"
            )
    cold_root = _canonical_directory(cold_root, label="cold_root", create=True)
    native_root, cold_root = _validate_disjoint_roots(native_root, cold_root)
    _ensure_safe_parent(cold_root, objects / "placeholder")
    _ensure_safe_parent(cold_root, indexes / "placeholder")
    if _lexists(final):
        observed_manifest = _content_addressed_manifest(
            final,
            object_id,
            policy=policy,
        )
        if observed_manifest != manifest:
            raise NativeStorageIntegrityError("content-addressed archive path collision")
    else:
        temporary = objects / f".{object_id}.{os.getpid()}.tmp"
        try:
            _write_deterministic_archive(temporary, manifest, members)
            observed_manifest = _content_addressed_manifest(
                temporary,
                object_id,
                policy=policy,
            )
            if observed_manifest != manifest:
                raise NativeStorageIntegrityError("staged archive manifest changed")
            try:
                os.link(temporary, final)
            except FileExistsError:
                observed_manifest = _content_addressed_manifest(
                    final,
                    object_id,
                    policy=policy,
                )
                if observed_manifest != manifest:
                    raise NativeStorageIntegrityError(
                        "content-addressed archive publication collision"
                    )
            except OSError:
                if _lexists(final):
                    observed_manifest = _content_addressed_manifest(
                        final,
                        object_id,
                        policy=policy,
                    )
                    if observed_manifest != manifest:
                        raise NativeStorageIntegrityError(
                            "content-addressed archive publication collision"
                        )
                else:
                    raise NativeStorageIntegrityError(
                        "cold store does not support create-only archive publication"
                    )
        finally:
            temporary.unlink(missing_ok=True)
    archive_directory_fsynced = _fsync_directory(objects)
    archive_sha = sha256_file(final)
    final_manifest = _content_addressed_manifest(
        final,
        object_id,
        policy=policy,
        expected_sha256=archive_sha,
    )
    if final_manifest != manifest:
        raise NativeStorageIntegrityError("published archive differs from requested evidence")
    index = {
        "schema_version": ARCHIVE_INDEX_SCHEMA,
        "object_id": object_id,
        "archive_relative_path": final.relative_to(cold_root).as_posix(),
        "archive_sha256": archive_sha,
        "archive_bytes": _safe_lstat(final, require_regular=True).st_size,
        "file_count": len(files),
        "uncompressed_bytes": manifest["uncompressed_bytes"],
        "archive_directory_fsynced": archive_directory_fsynced,
        "index_directory_fsync_supported": _fsync_directory(indexes),
    }
    index_path = indexes / f"{object_id}.json"
    encoded = _canonical_bytes(index)
    if _lexists(index_path):
        if _read_bounded_bytes(index_path, max_bytes=policy.max_json_bytes) != encoded:
            raise NativeStorageIntegrityError("cold archive index collision")
    else:
        temporary_index = indexes / f".{object_id}.{os.getpid()}.tmp"
        try:
            with temporary_index.open("xb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary_index, index_path)
            except FileExistsError:
                if _read_bounded_bytes(index_path, max_bytes=policy.max_json_bytes) != encoded:
                    raise NativeStorageIntegrityError("cold archive index collision")
            except OSError:
                if _lexists(index_path):
                    if _read_bounded_bytes(index_path, max_bytes=policy.max_json_bytes) != encoded:
                        raise NativeStorageIntegrityError("cold archive index collision")
                else:
                    raise NativeStorageIntegrityError(
                        "cold store does not support create-only index publication"
                    )
        finally:
            temporary_index.unlink(missing_ok=True)
    index_directory_fsynced = _fsync_directory(indexes)
    if index["index_directory_fsync_supported"] and not index_directory_fsynced:
        raise NativeStorageIntegrityError("cold index directory durability flush failed")
    final_usage = _tree_usage(
        cold_root,
        max_files=policy.max_cold_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
    )
    if final_usage.bytes > policy.max_cold_bytes or final_usage.files > policy.max_cold_files:
        raise NativeStorageBoundError("cold store exceeded a hard cap after publication")
    _verified_archive_index(cold_root, object_id, policy=policy)
    return index


def create_lossless_archive(
    native_root: Path,
    cold_root: Path,
    source_paths: Iterable[Path],
    *,
    graph: Optional[ProtectionGraph] = None,
    policy: NativeStoragePolicy,
    filesystem_free_bytes: Optional[int] = None,
) -> Mapping[str, Any]:
    """Kernel-serialised creation of one exact content-addressed cold object."""
    _validate_disjoint_roots(native_root, cold_root)
    with native_storage_lifecycle_lock(
        native_root, timeout_seconds=policy.lock_timeout_seconds
    ) as canonical_root:
        if _lexists(cold_root):
            canonical_cold = _canonical_directory(cold_root, label="cold_root")
            _validate_disjoint_roots(canonical_root, canonical_cold)
            _reconcile_cold_publication_temps(canonical_cold, policy=policy)
        return _create_lossless_archive_locked(
            canonical_root,
            cold_root,
            source_paths,
            graph=graph,
            policy=policy,
            filesystem_free_bytes=filesystem_free_bytes,
        )


def _publish_create_only_json(
    cold_root: Path,
    path: Path,
    value: Mapping[str, Any],
    *,
    policy: NativeStoragePolicy,
) -> None:
    encoded = _canonical_bytes(value)
    if len(encoded) > policy.max_json_bytes:
        raise NativeStorageBoundError("lifecycle receipt exceeds its JSON byte cap")
    if _lexists(path):
        if _read_bounded_bytes(path, max_bytes=policy.max_json_bytes) != encoded:
            raise NativeStorageIntegrityError(f"lifecycle receipt collision: {path}")
        return
    usage = _tree_usage(
        cold_root,
        max_files=policy.max_cold_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
    )
    if usage.bytes + (2 * len(encoded)) > policy.max_cold_bytes:
        raise NativeStorageBoundError("lifecycle receipt would exceed the cold byte cap")
    if usage.files + 2 > policy.max_cold_files:
        raise NativeStorageBoundError("lifecycle receipt would exceed the cold file cap")
    _ensure_safe_parent(cold_root, path)
    temporary = path.parent / f".{path.name}.{os.getpid()}.tmp"
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            pass
        except OSError as exc:
            if not _lexists(path):
                raise NativeStorageIntegrityError(
                    "cold store does not support create-only receipt publication"
                ) from exc
    finally:
        temporary.unlink(missing_ok=True)
    if _read_bounded_bytes(path, max_bytes=policy.max_json_bytes) != encoded:
        raise NativeStorageIntegrityError("published lifecycle receipt is not exact")
    _fsync_directory(path.parent)


def _validated_lifecycle_files(
    value: Any,
    *,
    run_id: str,
    policy: NativeStoragePolicy,
) -> dict[str, Mapping[str, Any]]:
    if not isinstance(value, list) or not value or len(value) > policy.max_hot_files:
        raise NativeStorageIntegrityError("retirement intent has an invalid file manifest")
    expected: dict[str, Mapping[str, Any]] = {}
    prefix = f"runs/{run_id}/"
    total = 0
    for item in value:
        if not isinstance(item, Mapping):
            raise NativeStorageIntegrityError("retirement intent file record is malformed")
        relative = str(item.get("relative_path") or "")
        _safe_archive_relative(relative)
        if not relative.startswith(prefix) or relative == prefix:
            raise NativeStorageIntegrityError("retirement intent escapes its run")
        try:
            if set(item) != {"relative_path", "size", "sha256"} or type(
                item.get("size")
            ) is not int:
                raise TypeError
            size = int(item.get("size", -1))
        except (TypeError, ValueError, OverflowError) as exc:
            raise NativeStorageIntegrityError("retirement intent file size is invalid") from exc
        if size < 0 or not _is_sha256(item.get("sha256")) or relative in expected:
            raise NativeStorageIntegrityError("retirement intent file record is invalid")
        total += size
        if total > policy.max_hot_bytes:
            raise NativeStorageBoundError("retirement intent exceeds the hot byte cap")
        expected[relative] = item
    return expected


def _load_retirement_intent(
    path: Path, *, policy: NativeStoragePolicy
) -> Mapping[str, Any]:
    value = _load_json(path, max_bytes=policy.max_json_bytes)
    transaction_id = str(value.get("transaction_id") or "")
    run_id = str(value.get("run_id") or "")
    required_keys = {
        "schema_version",
        "transaction_id",
        "native_root_id",
        "run_id",
        "run_relative_path",
        "staging_relative_path",
        "object_id",
        "archive_relative_path",
        "archive_sha256",
        "archive_manifest_sha256",
        "files",
        "file_count",
        "uncompressed_bytes",
    }
    if (
        set(value) != required_keys
        or
        value.get("schema_version") != RETIRE_INTENT_SCHEMA
        or not _is_sha256(transaction_id)
        or path.stem != transaction_id
        or not _is_sha256(value.get("native_root_id"))
        or not run_id
        or any(character in run_id for character in "/\\:")
        or run_id in {".", ".."}
        or not _is_sha256(value.get("object_id"))
        or not _is_sha256(value.get("archive_sha256"))
        or not _is_sha256(value.get("archive_manifest_sha256"))
    ):
        raise NativeStorageIntegrityError(f"retirement intent is invalid: {path}")
    expected_run = f"runs/{run_id}"
    expected_stage = f".retire-staging/{transaction_id}/{run_id}"
    expected_archive = f"objects/{str(value['object_id']).lower()}.tar.gz"
    if (
        value.get("run_relative_path") != expected_run
        or value.get("staging_relative_path") != expected_stage
        or value.get("archive_relative_path") != expected_archive
    ):
        raise NativeStorageIntegrityError("retirement intent path binding is invalid")
    basis = {
        "native_root_id": value["native_root_id"],
        "run_relative_path": expected_run,
        "object_id": value["object_id"],
        "archive_sha256": value["archive_sha256"],
    }
    if hashlib.sha256(_canonical_bytes(basis)).hexdigest() != transaction_id:
        raise NativeStorageIntegrityError("retirement transaction id binding is invalid")
    files = _validated_lifecycle_files(value.get("files"), run_id=run_id, policy=policy)
    try:
        if type(value.get("file_count")) is not int or type(
            value.get("uncompressed_bytes")
        ) is not int:
            raise TypeError
        declared_count = int(value.get("file_count", -1))
        declared_bytes = int(value.get("uncompressed_bytes", -1))
    except (TypeError, ValueError, OverflowError) as exc:
        raise NativeStorageIntegrityError("retirement intent summary is invalid") from exc
    if declared_count != len(files) or declared_bytes != sum(
        int(item["size"]) for item in files.values()
    ):
        raise NativeStorageIntegrityError("retirement intent summary is inconsistent")
    return value


def _verify_intent_archive(
    cold_root: Path,
    intent: Mapping[str, Any],
    *,
    policy: NativeStoragePolicy,
) -> Mapping[str, Any]:
    object_id = str(intent["object_id"])
    index, manifest, archive = _verified_archive_index(
        cold_root, object_id, policy=policy
    )
    if (
        index.get("archive_sha256") != intent.get("archive_sha256")
        or index.get("archive_relative_path") != intent.get("archive_relative_path")
        or _rel(cold_root, archive) != intent.get("archive_relative_path")
    ):
        raise NativeStorageIntegrityError("retirement cold index is inconsistent")
    if hashlib.sha256(_canonical_bytes(manifest)).hexdigest() != intent.get(
        "archive_manifest_sha256"
    ):
        raise NativeStorageIntegrityError("retirement archive manifest binding failed")
    if (
        manifest.get("files") != intent.get("files")
        or int(manifest.get("file_count", -1)) != int(intent.get("file_count", -2))
        or int(manifest.get("uncompressed_bytes", -1))
        != int(intent.get("uncompressed_bytes", -2))
    ):
        raise NativeStorageIntegrityError("retirement archive files differ from intent")
    return manifest


def _verify_run_tree(
    root: Path,
    *,
    origin_prefix: str,
    expected: Mapping[str, Mapping[str, Any]],
    allow_subset: bool,
    policy: NativeStoragePolicy,
) -> list[_TreeRecord]:
    records = _walk_regular_files(
        root,
        max_files=policy.max_hot_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
    )
    observed: set[str] = set()
    for record in records:
        relative = f"{origin_prefix}/{record.relative}".rstrip("/")
        declared = expected.get(relative)
        if declared is None or relative in observed:
            raise NativeStorageIntegrityError("retirement tree contains an unexpected file")
        if record.size != int(declared["size"]) or sha256_file(record.path) != str(
            declared["sha256"]
        ).lower():
            raise NativeStorageIntegrityError("retirement tree bytes differ from intent")
        observed.add(relative)
    if not allow_subset and observed != set(expected):
        raise NativeStorageIntegrityError("retirement hot run is incomplete")
    return records


def _valid_run_id(run_id: Any) -> str:
    value = str(run_id or "").strip()
    if not value or value in {".", ".."} or any(ch in value for ch in "/\\:"):
        raise NativeStorageIntegrityError("unsafe native run id")
    return value


def _native_root_id(native_root: Path) -> str:
    canonical = os.path.normcase(str(Path(native_root).resolve())).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _retirement_paths(
    native_root: Path, cold_root: Path, intent: Mapping[str, Any]
) -> tuple[Path, Path, Path]:
    run = _relative(native_root, intent["run_relative_path"])
    stage = _relative(native_root, intent["staging_relative_path"])
    completion = (
        cold_root
        / "transactions"
        / "completed"
        / f"{intent['transaction_id']}.json"
    )
    return run, stage, completion


def _publish_completion(
    cold_root: Path,
    intent: Mapping[str, Any],
    *,
    policy: NativeStoragePolicy,
) -> Mapping[str, Any]:
    completion = {
        "schema_version": RETIRE_COMPLETION_SCHEMA,
        "transaction_id": intent["transaction_id"],
        "native_root_id": intent["native_root_id"],
        "run_id": intent["run_id"],
        "object_id": intent["object_id"],
        "archive_sha256": intent["archive_sha256"],
        "retired_files": intent["file_count"],
        "retired_bytes": intent["uncompressed_bytes"],
    }
    path = (
        cold_root
        / "transactions"
        / "completed"
        / f"{intent['transaction_id']}.json"
    )
    _publish_create_only_json(cold_root, path, completion, policy=policy)
    return completion


def _validate_completion(
    path: Path,
    intent: Mapping[str, Any],
    *,
    policy: NativeStoragePolicy,
) -> Mapping[str, Any]:
    completion = _load_json(path, max_bytes=policy.max_json_bytes)
    required = {
        "schema_version",
        "transaction_id",
        "native_root_id",
        "run_id",
        "object_id",
        "archive_sha256",
        "retired_files",
        "retired_bytes",
    }
    try:
        if type(completion.get("retired_files")) is not int or type(
            completion.get("retired_bytes")
        ) is not int:
            raise TypeError
        retired_files = int(completion.get("retired_files", -1))
        retired_bytes = int(completion.get("retired_bytes", -1))
    except (TypeError, ValueError, OverflowError) as exc:
        raise NativeStorageIntegrityError(
            "retirement completion receipt has invalid numeric fields"
        ) from exc
    if (
        set(completion) != required
        or completion.get("schema_version") != RETIRE_COMPLETION_SCHEMA
        or completion.get("transaction_id") != intent.get("transaction_id")
        or completion.get("native_root_id") != intent.get("native_root_id")
        or completion.get("run_id") != intent.get("run_id")
        or completion.get("object_id") != intent.get("object_id")
        or completion.get("archive_sha256") != intent.get("archive_sha256")
        or retired_files != int(intent.get("file_count", -2))
        or retired_bytes != int(intent.get("uncompressed_bytes", -2))
    ):
        raise NativeStorageIntegrityError("retirement completion receipt is invalid")
    return completion


def _checkpoint(callback: Optional[Callable[[str], None]], name: str) -> None:
    if callback is not None:
        callback(name)


def _finish_retirement_locked(
    native_root: Path,
    cold_root: Path,
    intent: Mapping[str, Any],
    *,
    policy: NativeStoragePolicy,
    crash_injector: Optional[Callable[[str], None]] = None,
    permit_hot_rename: bool,
) -> Mapping[str, Any]:
    if intent.get("native_root_id") != _native_root_id(native_root):
        raise NativeStorageIntegrityError("retirement intent belongs to another native root")
    run, stage, completion_path = _retirement_paths(native_root, cold_root, intent)
    if _lexists(completion_path):
        completion = _validate_completion(completion_path, intent, policy=policy)
        _verify_intent_archive(cold_root, intent, policy=policy)
        if _lexists(stage):
            raise NativeStorageIntegrityError(
                "completed retirement still has a staging tree"
            )
        if _lexists(run):
            expected = _validated_lifecycle_files(
                intent["files"], run_id=str(intent["run_id"]), policy=policy
            )
            _verify_run_tree(
                run,
                origin_prefix=f"runs/{intent['run_id']}",
                expected=expected,
                allow_subset=False,
                policy=policy,
            )
        return completion

    expected = _validated_lifecycle_files(
        intent["files"], run_id=str(intent["run_id"]), policy=policy
    )
    archive_error: Optional[NativeStorageIntegrityError] = None
    try:
        _verify_intent_archive(cold_root, intent, policy=policy)
    except NativeStorageIntegrityError as exc:
        archive_error = exc

    run_exists, stage_exists = _lexists(run), _lexists(stage)
    if archive_error is not None:
        if run_exists and not stage_exists:
            _verify_run_tree(
                run,
                origin_prefix=f"runs/{intent['run_id']}",
                expected=expected,
                allow_subset=False,
                policy=policy,
            )
            raise NativeStorageIntegrityError(
                "retirement archive is invalid; verified hot run was preserved"
            ) from archive_error
        if stage_exists and not run_exists:
            staged = _verify_run_tree(
                stage,
                origin_prefix=f"runs/{intent['run_id']}",
                expected=expected,
                allow_subset=False,
                policy=policy,
            )
            if len(staged) == len(expected):
                _ensure_safe_parent(native_root, run)
                os.rename(stage, run)
                transaction_directory = stage.parent
                if _lexists(transaction_directory) and not _scandir_names(
                    transaction_directory, max_entries=policy.max_directory_entries
                ):
                    transaction_directory.rmdir()
                raise NativeStorageIntegrityError(
                    "retirement archive is invalid; full staged run was restored hot"
                ) from archive_error
        raise NativeStorageIntegrityError(
            "retirement archive is invalid and no complete hot copy remains"
        ) from archive_error

    if run_exists and stage_exists:
        _verify_run_tree(
            run,
            origin_prefix=f"runs/{intent['run_id']}",
            expected=expected,
            allow_subset=False,
            policy=policy,
        )
        _verify_run_tree(
            stage,
            origin_prefix=f"runs/{intent['run_id']}",
            expected=expected,
            allow_subset=True,
            policy=policy,
        )
        _verify_intent_archive(cold_root, intent, policy=policy)
        _remove_bounded_tree(stage, policy=policy)
        transaction_directory = stage.parent
        if _lexists(transaction_directory) and not _scandir_names(
            transaction_directory, max_entries=policy.max_directory_entries
        ):
            transaction_directory.rmdir()
        return _publish_completion(cold_root, intent, policy=policy)

    if run_exists:
        if not permit_hot_rename:
            return {
                "transaction_id": intent["transaction_id"],
                "state": "verified_hot_and_archive",
            }
        graph = build_protection_graph(
            native_root,
            max_files=policy.max_hot_files,
            max_entries=policy.max_scan_entries,
            max_directory_entries=policy.max_directory_entries,
            max_json_bytes=policy.max_json_bytes,
        )
        if graph.is_protected(run):
            raise NativeStorageProtectedError(
                f"run became protected before retirement: {intent['run_id']}"
            )
        _verify_run_tree(
            run,
            origin_prefix=f"runs/{intent['run_id']}",
            expected=expected,
            allow_subset=False,
            policy=policy,
        )
        _verify_intent_archive(cold_root, intent, policy=policy)
        _ensure_safe_parent(native_root, stage)
        if _lexists(stage):
            raise NativeStorageIntegrityError("retirement staging collision")
        os.rename(run, stage)
        _checkpoint(crash_injector, "hot_renamed")
        stage_exists = True

    if stage_exists:
        _verify_intent_archive(cold_root, intent, policy=policy)
        _verify_run_tree(
            stage,
            origin_prefix=f"runs/{intent['run_id']}",
            expected=expected,
            allow_subset=True,
            policy=policy,
        )
        _checkpoint(crash_injector, "before_stage_cleanup")
        _remove_bounded_tree(
            stage,
            policy=policy,
            after_unlink=lambda _index: _checkpoint(
                crash_injector, "stage_file_unlinked"
            ),
        )
        transaction_directory = stage.parent
        if _lexists(transaction_directory) and not _scandir_names(
            transaction_directory, max_entries=policy.max_directory_entries
        ):
            transaction_directory.rmdir()
        _checkpoint(crash_injector, "stage_cleanup_complete")
    elif run_exists:
        raise NativeStorageIntegrityError("retirement run state changed unexpectedly")

    _verify_intent_archive(cold_root, intent, policy=policy)
    completion = _publish_completion(cold_root, intent, policy=policy)
    _checkpoint(crash_injector, "completion_published")
    return completion


def _intent_paths(cold_root: Path, *, policy: NativeStoragePolicy) -> list[Path]:
    root = cold_root / "transactions" / "intents"
    if not _lexists(root):
        return []
    paths = _direct_children(
        root, max_entries=policy.max_directory_entries, suffix=".json"
    )
    if len(paths) > policy.max_complete_hot_runs + policy.max_inflight_runs + policy.max_cold_files:
        raise NativeStorageBoundError("retirement intent count is unsafe")
    return paths


def _reconcile_locked(
    native_root: Path,
    cold_root: Path,
    *,
    policy: NativeStoragePolicy,
    crash_injector: Optional[Callable[[str], None]] = None,
) -> list[Mapping[str, Any]]:
    cold_root = _canonical_directory(cold_root, label="cold_root", create=True)
    _validate_disjoint_roots(native_root, cold_root)
    _reconcile_cold_publication_temps(cold_root, policy=policy)
    results: list[Mapping[str, Any]] = []
    for path in _intent_paths(cold_root, policy=policy):
        intent = _load_retirement_intent(path, policy=policy)
        results.append(
            _finish_retirement_locked(
                native_root,
                cold_root,
                intent,
                policy=policy,
                crash_injector=crash_injector,
                permit_hot_rename=True,
            )
        )
    return results


def reconcile_native_storage(
    native_root: Path,
    cold_root: Path,
    *,
    policy: NativeStoragePolicy,
    crash_injector: Optional[Callable[[str], None]] = None,
) -> tuple[Mapping[str, Any], ...]:
    """Verify and deterministically finish every durable retirement intent."""
    _validate_disjoint_roots(native_root, cold_root)
    with native_storage_lifecycle_lock(
        native_root, timeout_seconds=policy.lock_timeout_seconds
    ) as canonical_root:
        return tuple(
            _reconcile_locked(
                canonical_root,
                cold_root,
                policy=policy,
                crash_injector=crash_injector,
            )
        )


def _eligible_complete_runs(
    native_root: Path,
    *,
    graph: ProtectionGraph,
    policy: NativeStoragePolicy,
) -> list[Path]:
    runs_root = native_root / "runs"
    candidates: list[Path] = []
    if not _lexists(runs_root):
        return candidates
    for run in _direct_children(
        runs_root,
        max_entries=policy.max_directory_entries,
        directories_only=True,
    ):
        if run.name.startswith("."):
            continue
        manifest_path = run / "run_manifest.json"
        if not _lexists(manifest_path):
            continue
        manifest = _load_json(manifest_path, max_bytes=policy.max_json_bytes)
        if manifest.get("complete") is not True:
            continue
        manifest_run_id = manifest.get("run_id")
        if manifest_run_id not in (None, run.name):
            raise NativeStorageIntegrityError("complete run manifest id is inconsistent")
        owner = runs_root / f".{run.name}.writer.owner.json"
        if _lexists(owner) and _load_json(owner, max_bytes=policy.max_json_bytes).get(
            "active"
        ) is True:
            continue
        if not graph.is_protected(run):
            candidates.append(run)
    return sorted(candidates, key=lambda item: item.name)


def _retire_one_locked(
    native_root: Path,
    cold_root: Path,
    run: Path,
    *,
    policy: NativeStoragePolicy,
    crash_injector: Optional[Callable[[str], None]] = None,
) -> Mapping[str, Any]:
    run_id = _valid_run_id(run.name)
    graph = build_protection_graph(
        native_root,
        max_files=policy.max_hot_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
        max_json_bytes=policy.max_json_bytes,
    )
    manifest = run / "run_manifest.json"
    if not _lexists(manifest) or _load_json(
        manifest, max_bytes=policy.max_json_bytes
    ).get("complete") is not True:
        raise NativeStorageProtectedError(f"run is incomplete/inflight: {run_id}")
    if graph.is_protected(run):
        raise NativeStorageProtectedError(f"run is active or pinned: {run_id}")
    index = _create_lossless_archive_locked(
        native_root,
        cold_root,
        [run],
        graph=graph,
        policy=policy,
    )
    verified_index, archive_manifest, _archive = _verified_archive_index(
        cold_root, str(index["object_id"]), policy=policy
    )
    if verified_index != index:
        raise NativeStorageIntegrityError("retirement archive index changed")
    _checkpoint(crash_injector, "archive_verified")
    transaction_basis = {
        "native_root_id": _native_root_id(native_root),
        "run_relative_path": f"runs/{run_id}",
        "object_id": index["object_id"],
        "archive_sha256": index["archive_sha256"],
    }
    transaction_id = hashlib.sha256(_canonical_bytes(transaction_basis)).hexdigest()
    intent: Mapping[str, Any] = {
        "schema_version": RETIRE_INTENT_SCHEMA,
        "transaction_id": transaction_id,
        "native_root_id": transaction_basis["native_root_id"],
        "run_id": run_id,
        "run_relative_path": f"runs/{run_id}",
        "staging_relative_path": f".retire-staging/{transaction_id}/{run_id}",
        "object_id": index["object_id"],
        "archive_relative_path": index["archive_relative_path"],
        "archive_sha256": index["archive_sha256"],
        "archive_manifest_sha256": hashlib.sha256(
            _canonical_bytes(archive_manifest)
        ).hexdigest(),
        "files": archive_manifest["files"],
        "file_count": archive_manifest["file_count"],
        "uncompressed_bytes": archive_manifest["uncompressed_bytes"],
    }
    intent_path = (
        cold_root / "transactions" / "intents" / f"{transaction_id}.json"
    )
    _publish_create_only_json(cold_root, intent_path, intent, policy=policy)
    _checkpoint(crash_injector, "intent_published")
    verified_intent = _load_retirement_intent(intent_path, policy=policy)
    return _finish_retirement_locked(
        native_root,
        cold_root,
        verified_intent,
        policy=policy,
        crash_injector=crash_injector,
        permit_hot_rename=True,
    )


def retire_complete_native_runs(
    native_root: Path,
    cold_root: Path,
    *,
    policy: NativeStoragePolicy,
    max_runs: int = 1,
    run_ids: Optional[Sequence[str]] = None,
    crash_injector: Optional[Callable[[str], None]] = None,
) -> tuple[Mapping[str, Any], ...]:
    """Retire deterministic complete, unprotected runs under the one OS lock."""
    if int(max_runs) < 1:
        raise ValueError("max_runs must be positive")
    _validate_disjoint_roots(native_root, cold_root)
    requested: Optional[set[str]] = None
    if run_ids is not None:
        requested = set()
        for index, item in enumerate(run_ids):
            if index >= policy.max_directory_entries:
                raise NativeStorageBoundError("requested run id cap exceeded")
            requested.add(_valid_run_id(item))
    with native_storage_lifecycle_lock(
        native_root, timeout_seconds=policy.lock_timeout_seconds
    ) as canonical_root:
        cold = _canonical_directory(cold_root, label="cold_root", create=True)
        _reconcile_locked(canonical_root, cold, policy=policy)
        graph = build_protection_graph(
            canonical_root,
            max_files=policy.max_hot_files,
            max_entries=policy.max_scan_entries,
            max_directory_entries=policy.max_directory_entries,
            max_json_bytes=policy.max_json_bytes,
        )
        candidates = _eligible_complete_runs(
            canonical_root, graph=graph, policy=policy
        )
        if requested is not None:
            present_names = {
                child.name
                for child in _direct_children(
                    canonical_root / "runs",
                    max_entries=policy.max_directory_entries,
                    directories_only=True,
                )
                if not child.name.startswith(".")
            }
            protected_or_inflight = requested & present_names - {
                candidate.name for candidate in candidates
            }
            if protected_or_inflight:
                raise NativeStorageProtectedError(
                    "requested runs are active, pinned or inflight: "
                    + ", ".join(sorted(protected_or_inflight))
                )
            candidates = [candidate for candidate in candidates if candidate.name in requested]
        results: list[Mapping[str, Any]] = []
        for candidate in candidates[: int(max_runs)]:
            results.append(
                _retire_one_locked(
                    canonical_root,
                    cold,
                    candidate,
                    policy=policy,
                    crash_injector=crash_injector,
                )
            )
        return tuple(results)


def retire_native_run(
    native_root: Path,
    cold_root: Path,
    run_id: str,
    *,
    policy: NativeStoragePolicy,
    crash_injector: Optional[Callable[[str], None]] = None,
) -> Optional[Mapping[str, Any]]:
    """Narrow explicit-run retirement; return ``None`` if already absent."""
    results = retire_complete_native_runs(
        native_root,
        cold_root,
        policy=policy,
        max_runs=1,
        run_ids=[run_id],
        crash_injector=crash_injector,
    )
    return results[0] if results else None


def _reservation_by_id(
    native_root: Path,
    cold_root: Path,
    reservation_id: str,
    *,
    policy: NativeStoragePolicy,
) -> Mapping[str, Any]:
    if not _is_sha256(reservation_id):
        raise NativeStorageIntegrityError("run reservation id is invalid")
    path = (
        _reservation_directories(cold_root)["reservations"]
        / f"{str(reservation_id).lower()}.json"
    )
    return _load_run_reservation(path, native_root=native_root, policy=policy)


def _reservation_event(
    reservation: Mapping[str, Any], schema_version: str
) -> Mapping[str, Any]:
    return {
        "schema_version": schema_version,
        "reservation_id": reservation["reservation_id"],
        "native_root_id": reservation["native_root_id"],
        "run_id": reservation["run_id"],
    }


def admit_native_run(
    native_root: Path,
    cold_root: Path,
    run_id: str,
    *,
    expected_growth_bytes: int,
    expected_growth_files: int,
    policy: NativeStoragePolicy,
    retire_if_needed: bool = False,
    max_retirements: int = 1,
    filesystem_free_bytes: Optional[int] = None,
) -> Mapping[str, Any]:
    """Atomically persist capacity for a future long-running acquisition.

    The returned immutable reservation remains active without holding the OS
    lock.  The writer must call :func:`begin_native_run` before writing and
    :func:`complete_native_run_reservation` after publishing its complete run.
    There is no timeout or unsafe stale-owner stealing.
    """
    run_id = _valid_run_id(run_id)
    if type(expected_growth_bytes) is not int or expected_growth_bytes < 1:
        raise ValueError("expected_growth_bytes must be a positive integer")
    if type(expected_growth_files) is not int or expected_growth_files < 1:
        raise ValueError("expected_growth_files must be a positive integer")
    if type(max_retirements) is not int or max_retirements < 0:
        raise ValueError("max_retirements must be a non-negative integer")
    _validate_disjoint_roots(native_root, cold_root)
    with native_storage_lifecycle_lock(
        native_root, timeout_seconds=policy.lock_timeout_seconds
    ) as canonical_root:
        cold = _canonical_directory(cold_root, label="cold_root", create=True)
        _validate_disjoint_roots(canonical_root, cold)
        _reconcile_locked(canonical_root, cold, policy=policy)
        _reject_acquisition_manifest_staging_residue(canonical_root, policy=policy)
        if _lexists(canonical_root / "runs" / run_id):
            raise NativeStorageProtectedError("run id already exists in hot storage")
        basis = {
            "native_root_id": _native_root_id(canonical_root),
            "run_id": run_id,
            "expected_growth_bytes": expected_growth_bytes,
            "expected_growth_files": expected_growth_files,
        }
        reservation_id = hashlib.sha256(_canonical_bytes(basis)).hexdigest()
        reservation: Mapping[str, Any] = {
            "schema_version": RUN_RESERVATION_SCHEMA,
            "reservation_id": reservation_id,
            **basis,
        }
        active = _active_run_reservations(canonical_root, cold, policy=policy)
        for existing in active:
            if existing["run_id"] == run_id:
                if existing != reservation:
                    raise NativeStorageProtectedError(
                        "run id already has a different active reservation"
                    )
                return existing

        retirements = 0
        while True:
            decision = preflight_growth(
                canonical_root,
                cold,
                expected_growth_bytes=expected_growth_bytes,
                expected_growth_files=expected_growth_files,
                policy=policy,
                filesystem_free_bytes=filesystem_free_bytes,
            )
            if decision.allowed:
                break
            non_remediable = (
                any("cold " in reason for reason in decision.reasons)
                or any("inflight runs" in reason for reason in decision.reasons)
            )
            if (
                not retire_if_needed
                or non_remediable
                or retirements >= max_retirements
            ):
                decision.require_allowed()
            graph = build_protection_graph(
                canonical_root,
                max_files=policy.max_hot_files,
                max_entries=policy.max_scan_entries,
                max_directory_entries=policy.max_directory_entries,
                max_json_bytes=policy.max_json_bytes,
            )
            candidates = _eligible_complete_runs(
                canonical_root, graph=graph, policy=policy
            )
            if not candidates:
                decision.require_allowed()
            _retire_one_locked(
                canonical_root, cold, candidates[0], policy=policy
            )
            retirements += 1
        path = (
            _reservation_directories(cold)["reservations"]
            / f"{reservation_id}.json"
        )
        _publish_create_only_json(cold, path, reservation, policy=policy)
        _fsync_directory(path.parent)
        return _load_run_reservation(
            path, native_root=canonical_root, policy=policy
        )


reserve_native_run = admit_native_run


def begin_native_run(
    native_root: Path,
    cold_root: Path,
    reservation_id: str,
    *,
    policy: NativeStoragePolicy,
) -> Mapping[str, Any]:
    """Durably mark one reservation as handed to its acquisition writer."""
    _validate_disjoint_roots(native_root, cold_root)
    with native_storage_lifecycle_lock(
        native_root, timeout_seconds=policy.lock_timeout_seconds
    ) as canonical_root:
        cold = _canonical_directory(cold_root, label="cold_root")
        reservation = _reservation_by_id(
            canonical_root, cold, reservation_id, policy=policy
        )
        active_ids = {
            item["reservation_id"]
            for item in _active_run_reservations(canonical_root, cold, policy=policy)
        }
        if reservation["reservation_id"] not in active_ids:
            raise NativeStorageProtectedError("run reservation is no longer active")
        event = _reservation_event(reservation, RUN_BEGIN_SCHEMA)
        path = (
            _reservation_directories(cold)["begins"]
            / f"{reservation['reservation_id']}.json"
        )
        _publish_create_only_json(cold, path, event, policy=policy)
        _fsync_directory(path.parent)
        return _validate_run_reservation_event(
            path,
            reservation,
            schema=RUN_BEGIN_SCHEMA,
            policy=policy,
        )


def complete_native_run_reservation(
    native_root: Path,
    cold_root: Path,
    reservation_id: str,
    *,
    policy: NativeStoragePolicy,
) -> Mapping[str, Any]:
    """Release reserved capacity only after exact complete-run verification."""
    _validate_disjoint_roots(native_root, cold_root)
    with native_storage_lifecycle_lock(
        native_root, timeout_seconds=policy.lock_timeout_seconds
    ) as canonical_root:
        cold = _canonical_directory(cold_root, label="cold_root")
        reservation = _reservation_by_id(
            canonical_root, cold, reservation_id, policy=policy
        )
        directories = _reservation_directories(cold)
        begin_path = directories["begins"] / f"{reservation['reservation_id']}.json"
        if not _lexists(begin_path):
            raise NativeStorageProtectedError("run reservation was never begun")
        _validate_run_reservation_event(
            begin_path, reservation, schema=RUN_BEGIN_SCHEMA, policy=policy
        )
        run = canonical_root / "runs" / str(reservation["run_id"])
        manifest = run / "run_manifest.json"
        if (
            not _lexists(manifest)
            or _load_json(manifest, max_bytes=policy.max_json_bytes).get("complete")
            is not True
        ):
            raise NativeStorageProtectedError("reserved run is not complete")
        _reject_acquisition_manifest_staging_residue(canonical_root, policy=policy)
        usage = _tree_usage(
            run,
            max_files=policy.max_hot_files,
            max_entries=policy.max_scan_entries,
            max_directory_entries=policy.max_directory_entries,
        )
        if (
            usage.bytes > int(reservation["expected_growth_bytes"])
            or usage.files > int(reservation["expected_growth_files"])
        ):
            raise NativeStorageBoundError("run exceeded its durable reservation")
        total = _tree_usage(
            canonical_root,
            max_files=policy.max_hot_files,
            max_entries=policy.max_scan_entries,
            max_directory_entries=policy.max_directory_entries,
        )
        if total.bytes > policy.max_hot_bytes or total.files > policy.max_hot_files:
            raise NativeStorageBoundError("completed run exceeds a hot-store cap")
        _require_run_state_caps(_hot_run_states(canonical_root, policy=policy), policy)
        event = _reservation_event(reservation, RUN_RESERVATION_COMPLETION_SCHEMA)
        path = directories["completions"] / f"{reservation['reservation_id']}.json"
        _publish_create_only_json(cold, path, event, policy=policy)
        _fsync_directory(path.parent)
        return _validate_run_reservation_event(
            path,
            reservation,
            schema=RUN_RESERVATION_COMPLETION_SCHEMA,
            policy=policy,
        )


def cancel_native_run_reservation(
    native_root: Path,
    cold_root: Path,
    reservation_id: str,
    *,
    policy: NativeStoragePolicy,
) -> Mapping[str, Any]:
    """Explicitly cancel an unbegun reservation; no timeout or stealing."""
    _validate_disjoint_roots(native_root, cold_root)
    with native_storage_lifecycle_lock(
        native_root, timeout_seconds=policy.lock_timeout_seconds
    ) as canonical_root:
        cold = _canonical_directory(cold_root, label="cold_root")
        reservation = _reservation_by_id(
            canonical_root, cold, reservation_id, policy=policy
        )
        directories = _reservation_directories(cold)
        if _lexists(
            directories["begins"] / f"{reservation['reservation_id']}.json"
        ) or _lexists(canonical_root / "runs" / str(reservation["run_id"])):
            raise NativeStorageProtectedError("begun/run-present reservation cannot be cancelled")
        event = _reservation_event(reservation, RUN_RESERVATION_CANCELLATION_SCHEMA)
        path = directories["cancellations"] / f"{reservation['reservation_id']}.json"
        _publish_create_only_json(cold, path, event, policy=policy)
        _fsync_directory(path.parent)
        return _validate_run_reservation_event(
            path,
            reservation,
            schema=RUN_RESERVATION_CANCELLATION_SCHEMA,
            policy=policy,
        )


def _hot_run_states(
    native_root: Path, *, policy: NativeStoragePolicy
) -> dict[str, bool]:
    """Map run id to complete(True)/inflight(False) under bounded traversal."""
    states: dict[str, bool] = {}
    runs_root = native_root / "runs"
    if not _lexists(runs_root):
        return states
    for run in _direct_children(
        runs_root,
        max_entries=policy.max_directory_entries,
        directories_only=True,
    ):
        if run.name.startswith("."):
            continue
        run_id = _valid_run_id(run.name)
        manifest = run / "run_manifest.json"
        states[run_id] = bool(
            _lexists(manifest)
            and _load_json(manifest, max_bytes=policy.max_json_bytes).get("complete")
            is True
        )
    return states


def _require_run_state_caps(states: Mapping[str, bool], policy: NativeStoragePolicy) -> None:
    complete = sum(state is True for state in states.values())
    inflight = sum(state is False for state in states.values())
    if complete > policy.max_complete_hot_runs:
        raise NativeStorageBoundError("restore would exceed the complete hot-run cap")
    if inflight > policy.max_inflight_runs:
        raise NativeStorageBoundError("restore would exceed the inflight hot-run cap")


def _projected_restore_run_states(
    native_root: Path,
    staging: Path,
    manifest: Mapping[str, Any],
    *,
    policy: NativeStoragePolicy,
) -> Mapping[str, bool]:
    states = _hot_run_states(native_root, policy=policy)
    touched: set[str] = set()
    for record in manifest["files"]:
        pure = _safe_native_payload_relative(record["relative_path"])
        if len(pure.parts) >= 2 and pure.parts[0] == "runs":
            if pure.parts[1].startswith("."):
                continue
            touched.add(_valid_run_id(pure.parts[1]))
            if len(touched) > policy.max_directory_entries:
                raise NativeStorageBoundError("restored run-id cap exceeded")
    for run_id in sorted(touched):
        destination_manifest = native_root / "runs" / run_id / "run_manifest.json"
        staged_manifest = staging / "runs" / run_id / "run_manifest.json"
        if _lexists(destination_manifest):
            complete = (
                _load_json(
                    destination_manifest, max_bytes=policy.max_json_bytes
                ).get("complete")
                is True
            )
        elif _lexists(staged_manifest):
            complete = (
                _load_json(staged_manifest, max_bytes=policy.max_json_bytes).get(
                    "complete"
                )
                is True
            )
        else:
            complete = False
        states[run_id] = complete
    _require_run_state_caps(states, policy)
    return states


def _restore_lossless_archive_locked(
    native_root: Path,
    cold_root: Path,
    object_id: str,
    *,
    policy: NativeStoragePolicy,
    filesystem_free_bytes: Optional[int] = None,
) -> Mapping[str, Any]:
    """Verify, stage, then no-clobber publish every file from a cold object.

    Publication is idempotent: an existing byte-identical file is accepted, but
    any collision fails closed.  The staging tree remains outside all production
    resolver paths until every member has passed its hash check.
    """
    if not _is_sha256(object_id):
        raise NativeStorageIntegrityError("cold archive object id is invalid")
    object_id = str(object_id).lower()
    native_root, cold_root = _validate_disjoint_roots(native_root, cold_root)
    cold_root = _canonical_directory(cold_root, label="cold_root")
    cold_usage = _tree_usage(
        cold_root,
        max_files=policy.max_cold_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
    )
    if cold_usage.bytes > policy.max_cold_bytes:
        raise NativeStorageBoundError("cold store exceeds its hard byte cap")
    _index, manifest, archive = _verified_archive_index(
        cold_root, object_id, policy=policy
    )
    if int(manifest["uncompressed_bytes"]) > policy.max_hot_bytes:
        raise NativeStorageBoundError("restore would exceed the hard hot-store byte cap")
    if int(manifest["file_count"]) > policy.max_hot_files:
        raise NativeStorageBoundError("restore would exceed the hard hot-store file cap")
    missing: dict[str, Mapping[str, Any]] = {}
    missing_bytes = 0
    for record in manifest["files"]:
        relative = str(record["relative_path"])
        pure = _safe_native_payload_relative(relative)
        destination = native_root / Path(*pure.parts)
        if _lexists(destination):
            destination_stat = _safe_lstat(destination, require_regular=True)
            if (
                destination_stat.st_size != int(record["size"])
                or sha256_file(destination) != record["sha256"]
            ):
                raise NativeStorageIntegrityError(
                    f"restore would overwrite non-identical evidence: {relative}"
                )
        else:
            missing[relative] = record
            missing_bytes += int(record["size"])
    missing_files = len(missing)
    hot_usage = _tree_usage(
        native_root,
        max_files=policy.max_hot_files,
        max_entries=policy.max_scan_entries,
        max_directory_entries=policy.max_directory_entries,
    )
    if hot_usage.bytes + missing_bytes > policy.max_hot_bytes:
        raise NativeStorageBoundError("restore would exceed the hard hot-store byte cap")
    if hot_usage.files + missing_files > policy.max_hot_files:
        raise NativeStorageBoundError("restore would exceed the hard hot-store file cap")
    free = (
        int(filesystem_free_bytes)
        if filesystem_free_bytes is not None
        else int(shutil.disk_usage(native_root).free)
    )
    # Staged files are renamed into place, not copied or hard-linked, so each
    # missing byte has exactly one hot-store path throughout publication.
    if free - missing_bytes < policy.min_filesystem_free_bytes:
        raise NativeStorageBoundError("restore staging would breach the filesystem reserve")
    staging_parent = native_root / ".restore-staging"
    _ensure_safe_parent(native_root, staging_parent / "placeholder")
    staging = Path(tempfile.mkdtemp(prefix=f"{object_id}.", dir=staging_parent))
    try:
        declared_names = {item["relative_path"] for item in manifest["files"]}
        with tarfile.open(archive, mode="r:gz") as tar:
            first = tar.next()
            if (
                first is None
                or first.name != "ARCHIVE_MANIFEST.json"
                or first.type not in (tarfile.REGTYPE, tarfile.AREGTYPE)
            ):
                raise NativeStorageIntegrityError("cold archive manifest is missing")
            observed: set[str] = set()
            while True:
                member = tar.next()
                if member is None:
                    break
                if (
                    member.type not in (tarfile.REGTYPE, tarfile.AREGTYPE)
                    or not member.name.startswith("payload/")
                ):
                    raise NativeStorageIntegrityError("cold archive contains a forbidden member")
                relative = member.name[len("payload/") :]
                pure = _safe_native_payload_relative(relative)
                if relative in observed or relative not in declared_names:
                    raise NativeStorageIntegrityError("cold archive member path is unsafe")
                observed.add(relative)
                if relative not in missing:
                    continue
                target = staging / Path(*pure.parts)
                _ensure_safe_parent(staging, target)
                source = tar.extractfile(member)
                if source is None:
                    raise NativeStorageIntegrityError("cold archive member is unreadable")
                with target.open("xb") as handle:
                    shutil.copyfileobj(source, handle, length=MIB)
                    handle.flush()
                    os.fsync(handle.fileno())
                record = missing[relative]
                target_stat = _safe_lstat(target, require_regular=True)
                if target_stat.st_size != int(record["size"]) or sha256_file(target) != record["sha256"]:
                    raise NativeStorageIntegrityError("staged cold evidence failed verification")
            if observed != declared_names:
                raise NativeStorageIntegrityError("cold archive payload is incomplete")
        _projected_restore_run_states(
            native_root, staging, manifest, policy=policy
        )
        restored = 0
        for relative, record in missing.items():
            source = staging / Path(*PurePosixPath(relative).parts)
            destination = native_root / Path(*PurePosixPath(relative).parts)
            _ensure_safe_parent(native_root, destination)
            if _lexists(destination):
                raise NativeStorageIntegrityError("restore publication collision")
            os.rename(source, destination)
            restored += 1
        for record in manifest["files"]:
            destination = native_root / Path(*PurePosixPath(record["relative_path"]).parts)
            if sha256_file(destination) != record["sha256"]:
                raise NativeStorageIntegrityError("restored cold evidence failed final verification")
        final_usage = _tree_usage(
            native_root,
            max_files=policy.max_hot_files,
            max_entries=policy.max_scan_entries,
            max_directory_entries=policy.max_directory_entries,
        )
        if final_usage.bytes > policy.max_hot_bytes or final_usage.files > policy.max_hot_files:
            raise NativeStorageBoundError("restore exceeded a final hot-store cap")
        _require_run_state_caps(_hot_run_states(native_root, policy=policy), policy)
        return {"object_id": object_id, "restored_files": restored, "file_count": len(manifest["files"])}
    finally:
        _remove_bounded_tree(staging, policy=policy)


def restore_lossless_archive(
    native_root: Path,
    cold_root: Path,
    object_id: str,
    *,
    policy: NativeStoragePolicy,
    filesystem_free_bytes: Optional[int] = None,
) -> Mapping[str, Any]:
    """Kernel-serialised verify-before-publish exact-byte restoration."""
    _validate_disjoint_roots(native_root, cold_root)
    with native_storage_lifecycle_lock(
        native_root, timeout_seconds=policy.lock_timeout_seconds
    ) as canonical_root:
        cold = _canonical_directory(cold_root, label="cold_root")
        _validate_disjoint_roots(canonical_root, cold)
        _reconcile_locked(canonical_root, cold, policy=policy)
        return _restore_lossless_archive_locked(
            canonical_root,
            cold,
            object_id,
            policy=policy,
            filesystem_free_bytes=filesystem_free_bytes,
        )
