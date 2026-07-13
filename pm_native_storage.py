"""Lossless storage controls for immutable native-market evidence.

This module is deliberately independent from acquisition and activation.  It
provides the primitives those writers must call once they share a global
storage lock:

* authenticate the hot evidence roots and derive their complete protection
  graph;
* reject growth before a run is created when either the byte budget or the
  filesystem reserve would be breached; and
* create deterministic, content-addressed cold archives and restore them via a
  verify-before-publish staging tree.

No function in this module deletes hot evidence.  Retirement needs a shared
lock honoured by acquisition, audit and terms activation; until that lock is
integrated, deleting an apparently inactive run has an unavoidable TOCTOU race
with ``ACTIVE.json`` publication.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import shutil
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping, Optional


MIB = 1024 * 1024
PIN_SCHEMA = "fxpm-native-storage-pin-v1"
ARCHIVE_SCHEMA = "fxpm-native-lossless-archive-v1"
ARCHIVE_INDEX_SCHEMA = "fxpm-native-lossless-archive-index-v1"
DEFAULT_MAX_ARCHIVE_BYTES = 256 * 1024 * 1024 * 1024
DEFAULT_MAX_UNCOMPRESSED_BYTES = 1024 * 1024 * 1024 * 1024
DEFAULT_MAX_ARCHIVE_FILES = 100_000
DEFAULT_MAX_ARCHIVE_MANIFEST_BYTES = 64 * MIB


class NativeStorageError(RuntimeError):
    """Base class for fail-closed native storage errors."""


class NativeStorageIntegrityError(NativeStorageError):
    """An evidence pointer, hash, archive or path is malformed."""


class NativeStorageBoundError(NativeStorageError):
    """A proposed operation would breach a configured hard bound."""


class NativeStorageProtectedError(NativeStorageError):
    """An archive request contains currently protected hot evidence."""


def sha256_file(path: Path, *, chunk_bytes: int = MIB) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(chunk_bytes)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def _load_json(path: Path, *, max_bytes: int = 64 * MIB) -> Mapping[str, Any]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise NativeStorageIntegrityError(f"required evidence is unavailable: {path}") from exc
    if size <= 0 or size > max_bytes:
        raise NativeStorageIntegrityError(f"evidence JSON has an unsafe size: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise NativeStorageIntegrityError(f"evidence JSON is unreadable: {path}") from exc
    if not isinstance(value, Mapping):
        raise NativeStorageIntegrityError(f"evidence JSON must be an object: {path}")
    return value


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
        if cursor.is_symlink():
            raise NativeStorageIntegrityError(
                f"symlinks are forbidden in native evidence paths: {value!r}"
            )
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


def _rel(native_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(native_root).as_posix()
    except ValueError as exc:
        raise NativeStorageIntegrityError(f"path is outside native root: {path}") from exc


def _iter_relative_references(value: Any) -> Iterable[tuple[str, Any, Optional[Any]]]:
    """Yield (field, relative path, declared digest) from nested manifests."""
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key).endswith("_relative_path"):
                prefix = str(key)[: -len("_relative_path")]
                yield str(key), child, value.get(f"{prefix}_sha256")
            yield from _iter_relative_references(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_relative_references(child)


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

    def add_protection(self, path: Path, reason: str) -> None:
        self.protected.setdefault(_rel(self.native_root, path), set()).add(str(reason))

    def add_edge(self, source: Path, target: Path, digest: Any = None) -> None:
        source_rel, target_rel = _rel(self.native_root, source), _rel(self.native_root, target)
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
        if path.is_symlink():
            raise NativeStorageIntegrityError(f"symlinks are forbidden in native evidence: {path}")
        if path.is_file():
            self.add_protection(path, reason)
            return
        if not path.is_dir():
            raise NativeStorageIntegrityError(f"protected native evidence is missing: {path}")
        self.add_protection(path, reason)
        for child in path.rglob("*"):
            if child.is_symlink():
                raise NativeStorageIntegrityError(
                    f"symlinks are forbidden in native evidence: {child}"
                )
            if child.is_file() or child.is_dir():
                self.add_protection(child, reason)

    def is_protected(self, path: Path) -> bool:
        candidate = _rel(self.native_root, path)
        prefix = candidate.rstrip("/") + "/"
        return candidate in self.protected or any(key.startswith(prefix) for key in self.protected)

    @property
    def protected_bytes(self) -> int:
        total = 0
        for relative in self.protected:
            path = self.native_root / Path(*PurePosixPath(relative).parts)
            if path.is_file():
                total += path.stat().st_size
        return total


def _walk_json_references(
    graph: ProtectionGraph,
    path: Path,
    reason: str,
    *,
    seen: set[str],
) -> None:
    relative = _rel(graph.native_root, path)
    graph.add_protection(path, reason)
    if relative in seen:
        return
    seen.add(relative)
    payload = _load_json(path)
    for _field, raw_target, digest in _iter_relative_references(payload):
        target = _relative(graph.native_root, raw_target)
        if not target.is_file():
            raise NativeStorageIntegrityError(f"referenced native evidence is missing: {target}")
        graph.add_edge(path, target, digest)
        graph.add_protection(target, reason)
        if target.suffix.lower() == ".json":
            _walk_json_references(graph, target, reason, seen=seen)


def _protect_active_pointer(
    graph: ProtectionGraph,
    pointer_path: Path,
    reason: str,
    *,
    seen: set[str],
) -> Optional[Mapping[str, Any]]:
    if not pointer_path.exists():
        return None
    pointer = _load_json(pointer_path, max_bytes=MIB)
    graph.roots.add(_rel(graph.native_root, pointer_path))
    _walk_json_references(graph, pointer_path, reason, seen=seen)
    return pointer


def _activation_path_for_id(native_root: Path, activation_id: Any) -> Path:
    name = str(activation_id or "").strip()
    if not name or any(ch in name for ch in "/\\:") or name in {".", ".."}:
        raise NativeStorageIntegrityError("unsafe price activation id in terms evidence")
    return native_root / "activations" / f"{name}.json"


def build_protection_graph(native_root: Path) -> ProtectionGraph:
    """Authenticate and map all evidence that must remain available in hot storage.

    Roots are the price and terms ACTIVE pointers, explicit immutable pins, active
    acquisition writers, unfinished runs, and audit directories that do not yet
    have an activation manifest.  The last category is intentionally fail-safe:
    the current audit writer has no shared lifecycle lock, so an apparently
    orphan audit may still be in progress.
    """
    supplied_root = Path(native_root)
    if supplied_root.is_symlink():
        raise NativeStorageIntegrityError("native_root itself cannot be a symlink")
    native_root = supplied_root.resolve()
    if (
        not native_root.is_dir()
        or native_root.is_symlink()
        or native_root == Path(native_root.anchor).resolve()
    ):
        raise NativeStorageIntegrityError(
            "native_root must be an explicit existing non-root directory"
        )
    graph = ProtectionGraph(native_root=native_root)
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
        payload = _load_json(terms_manifest)
        price_id = payload.get("price_activation_id")
        price_sha = payload.get("price_activation_manifest_sha256")
        price_manifest = _activation_path_for_id(native_root, price_id)
        if not price_manifest.is_file():
            raise NativeStorageIntegrityError(
                "active terms evidence references a missing price activation"
            )
        graph.add_edge(terms_manifest, price_manifest, price_sha)
        _walk_json_references(
            graph, price_manifest, "ACTIVE_TERMS_PRICE_BINDING", seen=seen
        )

    # Explicit pins are the only reliable way for winner books, replay packs and
    # promotion records outside data/native to retain historical evidence.
    pins_root = native_root / "PINS"
    if pins_root.exists():
        for pin_path in sorted(pins_root.glob("*.json")):
            pin = _load_json(pin_path, max_bytes=4 * MIB)
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
    if runs_root.exists():
        for run in sorted(path for path in runs_root.iterdir() if path.is_dir()):
            owner = runs_root / f".{run.name}.writer.owner.json"
            active_writer = False
            if owner.exists():
                owner_payload = _load_json(owner, max_bytes=MIB)
                active_writer = owner_payload.get("active") is True
            manifest = run / "run_manifest.json"
            incomplete = True
            if manifest.exists():
                incomplete = _load_json(manifest).get("complete") is not True
            if active_writer or incomplete:
                why = "ACTIVE_RUN_WRITER" if active_writer else "INCOMPLETE_RUN"
                graph.protect_tree(run, why)
                if owner.exists():
                    graph.add_protection(owner, why)
                lock = runs_root / f".{run.name}.writer.lock"
                if lock.exists():
                    graph.add_protection(lock, why)

        for transient in sorted(runs_root.glob(".*")):
            if transient.name.endswith((".staging", ".tmp")):
                graph.protect_tree(transient, "RUN_TRANSIENT")

    audits_root = native_root / "audits"
    activations_root = native_root / "activations"
    if audits_root.exists():
        for audit in sorted(path for path in audits_root.iterdir() if path.is_dir()):
            if not (activations_root / f"{audit.name}.json").is_file():
                graph.protect_tree(audit, "UNPUBLISHED_OR_INFLIGHT_AUDIT")
        for transient in sorted(audits_root.rglob(".*")):
            if transient.name.endswith((".staging", ".tmp")):
                graph.protect_tree(transient, "AUDIT_TRANSIENT")

    terms_root = native_root / "terms"
    if terms_root.exists():
        for transient in sorted(terms_root.rglob(".*")):
            if transient.name.endswith((".staging", ".tmp")):
                graph.protect_tree(transient, "TERMS_TRANSIENT")

    restore_staging = native_root / ".restore-staging"
    if restore_staging.exists():
        graph.protect_tree(restore_staging, "RESTORE_TRANSIENT")

    # A price pointer with no terms pointer is valid.  An active terms pointer
    # without an active price pointer is never coherent.
    if terms_pointer and not price_pointer:
        raise NativeStorageIntegrityError("active terms evidence exists without ACTIVE price evidence")
    return graph


def tree_bytes(root: Path) -> int:
    root = Path(root)
    if not root.exists():
        return 0
    if root.is_file():
        return root.stat().st_size
    total = 0
    for path in root.rglob("*"):
        if path.is_symlink():
            raise NativeStorageIntegrityError(f"symlinks are forbidden in evidence storage: {path}")
        if path.is_file():
            total += path.stat().st_size
    return total


def _existing_ancestor(path: Path) -> Path:
    candidate = Path(path).resolve()
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            raise NativeStorageIntegrityError(f"no existing filesystem ancestor for {path}")
        candidate = parent
    return candidate


@dataclass(frozen=True)
class NativeStoragePolicy:
    """Explicit hard limits; there are intentionally no risk-blind defaults."""

    max_hot_bytes: int
    max_cold_bytes: int
    min_filesystem_free_bytes: int
    max_complete_hot_runs: int
    max_inflight_runs: int = 1

    def __post_init__(self) -> None:
        for name in (
            "max_hot_bytes",
            "max_cold_bytes",
            "min_filesystem_free_bytes",
            "max_complete_hot_runs",
            "max_inflight_runs",
        ):
            if int(getattr(self, name)) < 1:
                raise ValueError(f"{name} must be positive")


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
    reasons: tuple[str, ...]

    def require_allowed(self) -> "GrowthDecision":
        if not self.allowed:
            raise NativeStorageBoundError("; ".join(self.reasons))
        return self


def preflight_growth(
    native_root: Path,
    cold_root: Path,
    *,
    expected_growth_bytes: int,
    policy: NativeStoragePolicy,
    filesystem_free_bytes: Optional[int] = None,
) -> GrowthDecision:
    """Return a fail-before-growth decision without mutating either store."""
    if int(expected_growth_bytes) <= 0:
        raise ValueError("expected_growth_bytes must be positive")
    native_root, cold_root = Path(native_root).resolve(), Path(cold_root).resolve()
    hot = tree_bytes(native_root)
    cold = tree_bytes(cold_root)
    free = (
        int(filesystem_free_bytes)
        if filesystem_free_bytes is not None
        else int(shutil.disk_usage(native_root).free)
    )
    complete, inflight = 0, 0
    runs_root = native_root / "runs"
    if runs_root.exists():
        for run in (path for path in runs_root.iterdir() if path.is_dir()):
            manifest = run / "run_manifest.json"
            if manifest.exists() and _load_json(manifest).get("complete") is True:
                complete += 1
            else:
                inflight += 1
    projected_hot = hot + int(expected_growth_bytes)
    projected_free = free - int(expected_growth_bytes)
    reasons: list[str] = []
    if projected_hot > policy.max_hot_bytes:
        reasons.append(
            f"projected hot bytes {projected_hot} exceed cap {policy.max_hot_bytes}"
        )
    if cold > policy.max_cold_bytes:
        reasons.append(f"cold bytes {cold} exceed cap {policy.max_cold_bytes}")
    if projected_free < policy.min_filesystem_free_bytes:
        reasons.append(
            f"projected free bytes {projected_free} are below reserve "
            f"{policy.min_filesystem_free_bytes}"
        )
    if complete + 1 > policy.max_complete_hot_runs:
        reasons.append(
            f"projected complete hot runs {complete + 1} exceed cap "
            f"{policy.max_complete_hot_runs}"
        )
    if inflight + 1 > policy.max_inflight_runs:
        reasons.append(
            f"projected inflight runs {inflight + 1} exceed cap {policy.max_inflight_runs}"
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
        reasons=tuple(reasons),
    )


def _archive_members(native_root: Path, paths: Iterable[Path]) -> list[tuple[str, Path]]:
    members: dict[str, Path] = {}
    for supplied in paths:
        path = Path(supplied)
        if not path.is_absolute():
            path = native_root / path
        if path.is_symlink():
            raise NativeStorageIntegrityError(
                f"symlinks are forbidden in native archives: {path}"
            )
        path = path.resolve()
        relative = _rel(native_root, path)
        if path.is_symlink() or not path.exists():
            raise NativeStorageIntegrityError(f"archive source is missing or a symlink: {path}")
        if path.is_file():
            members[relative] = path
        else:
            for child in path.rglob("*"):
                if child.is_symlink():
                    raise NativeStorageIntegrityError(
                        f"symlinks are forbidden in native archives: {child}"
                    )
                if child.is_file():
                    members[_rel(native_root, child)] = child
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
                            _tar_info(f"payload/{relative}", path.stat().st_size), source
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
        archive_size = path.stat().st_size
    except OSError as exc:
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
            manifest = json.loads(manifest_handle.read().decode("utf-8"))
            if not isinstance(manifest, Mapping) or manifest.get("schema_version") != ARCHIVE_SCHEMA:
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
                _safe_archive_relative(relative)
                try:
                    size = int(record.get("size", -1))
                except (TypeError, ValueError) as exc:
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
            if (
                int(manifest.get("file_count", -1)) != len(expected)
                or int(manifest.get("uncompressed_bytes", -1)) != declared_total
            ):
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
                _safe_archive_relative(relative)
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
    except (OSError, tarfile.TarError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise NativeStorageIntegrityError("cold archive is unreadable or corrupt") from exc


def create_lossless_archive(
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
    native_root, cold_root = Path(native_root).resolve(), Path(cold_root).resolve()
    graph = graph or build_protection_graph(native_root)
    members = _archive_members(native_root, source_paths)
    protected = [relative for relative, path in members if graph.is_protected(path)]
    if protected:
        raise NativeStorageProtectedError(
            "archive request includes hot-protected evidence: " + ", ".join(protected[:5])
        )
    files = [
        {
            "relative_path": relative,
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for relative, path in members
    ]
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
    objects = cold_root / "objects"
    indexes = cold_root / "indexes"
    final = objects / f"{object_id}.tar.gz"
    # TAR uses 512-byte records and Python pads to a 10 KiB record boundary.
    # DEFLATE's stored-block worst case is bounded by five bytes per 16,383
    # input bytes plus its header/trailer.  The extra MiB covers the index and
    # filesystem metadata conservatively without relying on compression ratio.
    manifest_size = len(_canonical_bytes(manifest))
    tar_bytes = 512 + ((manifest_size + 511) // 512) * 512
    for _relative, source in members:
        tar_bytes += 512 + ((source.stat().st_size + 511) // 512) * 512
    tar_bytes = ((tar_bytes + 1024 + 10239) // 10240) * 10240
    worst_case_archive_bytes = tar_bytes + ((tar_bytes + 16382) // 16383) * 5 + 18 + MIB
    if not final.exists():
        cold_bytes = tree_bytes(cold_root)
        if cold_bytes + worst_case_archive_bytes > policy.max_cold_bytes:
            raise NativeStorageBoundError(
                "cold archive worst-case bytes would exceed the hard cold-store cap"
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
    objects.mkdir(parents=True, exist_ok=True)
    indexes.mkdir(parents=True, exist_ok=True)
    if final.exists():
        verify_lossless_archive(
            final,
            max_archive_bytes=policy.max_cold_bytes,
            max_uncompressed_bytes=policy.max_hot_bytes,
        )
    else:
        temporary = objects / f".{object_id}.{os.getpid()}.tmp"
        try:
            _write_deterministic_archive(temporary, manifest, members)
            verify_lossless_archive(
                temporary,
                max_archive_bytes=policy.max_cold_bytes,
                max_uncompressed_bytes=policy.max_hot_bytes,
            )
            try:
                os.link(temporary, final)
            except FileExistsError:
                verify_lossless_archive(
                    final,
                    max_archive_bytes=policy.max_cold_bytes,
                    max_uncompressed_bytes=policy.max_hot_bytes,
                )
            except OSError:
                # os.replace is atomic and safe because the content-addressed
                # destination was checked immediately before publication.
                if final.exists():
                    verify_lossless_archive(
                        final,
                        max_archive_bytes=policy.max_cold_bytes,
                        max_uncompressed_bytes=policy.max_hot_bytes,
                    )
                else:
                    os.replace(temporary, final)
        finally:
            temporary.unlink(missing_ok=True)
    archive_sha = sha256_file(final)
    verify_lossless_archive(
        final,
        expected_sha256=archive_sha,
        max_archive_bytes=policy.max_cold_bytes,
        max_uncompressed_bytes=policy.max_hot_bytes,
    )
    index = {
        "schema_version": ARCHIVE_INDEX_SCHEMA,
        "object_id": object_id,
        "archive_relative_path": final.relative_to(cold_root).as_posix(),
        "archive_sha256": archive_sha,
        "archive_bytes": final.stat().st_size,
        "file_count": len(files),
        "uncompressed_bytes": manifest["uncompressed_bytes"],
    }
    index_path = indexes / f"{object_id}.json"
    encoded = _canonical_bytes(index)
    if index_path.exists():
        if index_path.read_bytes() != encoded:
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
                if index_path.read_bytes() != encoded:
                    raise NativeStorageIntegrityError("cold archive index collision")
            except OSError:
                if index_path.exists():
                    if index_path.read_bytes() != encoded:
                        raise NativeStorageIntegrityError("cold archive index collision")
                else:
                    os.replace(temporary_index, index_path)
        finally:
            temporary_index.unlink(missing_ok=True)
    return index


def restore_lossless_archive(
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
    native_root, cold_root = Path(native_root).resolve(), Path(cold_root).resolve()
    index_path = cold_root / "indexes" / f"{object_id}.json"
    index = _load_json(index_path, max_bytes=MIB)
    if index.get("schema_version") != ARCHIVE_INDEX_SCHEMA or index.get("object_id") != object_id:
        raise NativeStorageIntegrityError("cold archive index is invalid")
    try:
        indexed_uncompressed = int(index.get("uncompressed_bytes", -1))
    except (TypeError, ValueError) as exc:
        raise NativeStorageIntegrityError("cold archive index size is invalid") from exc
    if indexed_uncompressed < 0:
        raise NativeStorageIntegrityError("cold archive index size is invalid")
    if indexed_uncompressed > policy.max_hot_bytes:
        raise NativeStorageBoundError("restore would exceed the hard hot-store byte cap")
    archive = _relative(cold_root, index.get("archive_relative_path"))
    manifest = verify_lossless_archive(
        archive,
        expected_sha256=index.get("archive_sha256"),
        max_archive_bytes=policy.max_cold_bytes,
        max_uncompressed_bytes=policy.max_hot_bytes,
    )
    native_root.mkdir(parents=True, exist_ok=True)
    missing_bytes = 0
    for record in manifest["files"]:
        relative = str(record["relative_path"])
        destination = native_root / Path(*PurePosixPath(relative).parts)
        if not destination.exists():
            missing_bytes += int(record["size"])
    if tree_bytes(native_root) + missing_bytes > policy.max_hot_bytes:
        raise NativeStorageBoundError("restore would exceed the hard hot-store byte cap")
    free = (
        int(filesystem_free_bytes)
        if filesystem_free_bytes is not None
        else int(shutil.disk_usage(native_root).free)
    )
    # Staging and destination coexist until publication, so reserve the full
    # missing payload twice. Hard-links normally avoid the second allocation,
    # but the copy fallback must remain safe on every supported filesystem.
    if free - (2 * missing_bytes) < policy.min_filesystem_free_bytes:
        raise NativeStorageBoundError("restore staging would breach the filesystem reserve")
    staging_parent = native_root / ".restore-staging"
    staging_parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f"{object_id}.", dir=staging_parent))
    try:
        with tarfile.open(archive, mode="r:gz") as tar:
            records = {item["relative_path"]: item for item in manifest["files"]}
            first = tar.next()
            if first is None or first.name != "ARCHIVE_MANIFEST.json":
                raise NativeStorageIntegrityError("cold archive manifest is missing")
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
                pure = _safe_archive_relative(relative)
                if relative not in records:
                    raise NativeStorageIntegrityError("cold archive member path is unsafe")
                target = staging / Path(*pure.parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                source = tar.extractfile(member)
                if source is None:
                    raise NativeStorageIntegrityError("cold archive member is unreadable")
                with target.open("xb") as handle:
                    shutil.copyfileobj(source, handle, length=MIB)
                    handle.flush()
                    os.fsync(handle.fileno())
                record = records[relative]
                if target.stat().st_size != int(record["size"]) or sha256_file(target) != record["sha256"]:
                    raise NativeStorageIntegrityError("staged cold evidence failed verification")
        # Check every collision before publishing the first new file.
        for record in manifest["files"]:
            relative = record["relative_path"]
            destination = native_root / Path(*PurePosixPath(relative).parts)
            if destination.exists() and (
                not destination.is_file()
                or destination.stat().st_size != int(record["size"])
                or sha256_file(destination) != record["sha256"]
            ):
                raise NativeStorageIntegrityError(
                    f"restore would overwrite non-identical evidence: {relative}"
                )
        restored = 0
        for record in manifest["files"]:
            relative = record["relative_path"]
            source = staging / Path(*PurePosixPath(relative).parts)
            destination = native_root / Path(*PurePosixPath(relative).parts)
            if destination.exists():
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(source, destination)
            except FileExistsError:
                if sha256_file(destination) != record["sha256"]:
                    raise NativeStorageIntegrityError("restore publication collision")
            except OSError:
                # Same-volume staging normally supports a hard-link.  The
                # fallback uses exclusive creation and never overwrites.
                try:
                    with source.open("rb") as src, destination.open("xb") as dst:
                        shutil.copyfileobj(src, dst, length=MIB)
                        dst.flush()
                        os.fsync(dst.fileno())
                except FileExistsError:
                    if sha256_file(destination) != record["sha256"]:
                        raise NativeStorageIntegrityError("restore publication collision")
            restored += 1
        for record in manifest["files"]:
            destination = native_root / Path(*PurePosixPath(record["relative_path"]).parts)
            if sha256_file(destination) != record["sha256"]:
                raise NativeStorageIntegrityError("restored cold evidence failed final verification")
        return {"object_id": object_id, "restored_files": restored, "file_count": len(manifest["files"])}
    finally:
        shutil.rmtree(staging, ignore_errors=True)
