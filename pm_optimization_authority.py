"""Raw, fail-closed verification for one optimization generation."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import stat
from dataclasses import asdict, fields
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping, Optional, Sequence


RESULT_SCHEMA = "fxpm.live-readiness-authority-result.v2"
CHAIN_RECEIPT_SCHEMA = "fxpm.optimization-authority-chain-receipt.v1"
GENERATION_SCHEMA = "fxpm.optimization-generation.v2"
TIMEFRAMES = ("M5", "M15", "M30", "H1", "H4", "D1")
REGIMES = ("TREND", "RANGE", "BREAKOUT", "CHOP")
ARTIFACTS = (
    "regime_support",
    "causal_process",
    "historical_book",
    "current_process_book",
    "current_refit_execution",
    "synchronized_portfolio_replay",
    "capital_calibration",
)
INDEX_SCHEMAS = {
    name: f"fxpm.{name.replace('_', '-')}-authority-index.v1" for name in ARTIFACTS
}
RAW_COMPONENTS = ("native_data_state", *ARTIFACTS)
MANAGEMENT_CONTEXT_SCHEMA = "fxpm.demo-management-only-context.v1"
MANAGEMENT_STATUS = "MANAGEMENT_ONLY"

MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_LEDGER_BYTES = 512 * 1024 * 1024
MAX_CONFIG_BYTES = 4 * 1024 * 1024
MAX_INDEX_BYTES = 64 * 1024 * 1024
MAX_TOTAL_INDEX_BYTES = 256 * 1024 * 1024
MAX_JSON_DEPTH = 64
MAX_JSON_NODES = 2_000_000
MAX_STRING_BYTES = 4 * 1024 * 1024
MAX_SYMBOLS = 1_000
READ_CHUNK_BYTES = 1024 * 1024
MAX_MANAGEMENT_CLOSURE_FILES = 20_000
MAX_MANAGEMENT_CLOSURE_BYTES = 2 * 1024**4
MAX_MANAGEMENT_EXTERNAL_FILE_BYTES = 16 * 1024**3

_MANAGEMENT_ROLES = frozenset(
    {
        "current_refit_artifact",
        "capital_replay_artifact",
        "native_bars",
        "native_sidecar",
    }
)
_MANAGEMENT_CONTEXT_FIELDS = frozenset(
    {
        "schema_version",
        "capsule_id",
        "generation_id",
        "scope",
        "allow_expired_entry_management",
        "allow_inactive_generation_pointer",
        "allow_new_entries",
        "allow_position_increase",
        "allow_stop_widening",
        "captured_source_root",
        "captured_activation_manifest_path",
        "captured_activation_manifest_sha256",
        "external_closure",
        "selected_native_source_bindings",
    }
)
_MANAGEMENT_CLOSURE_FIELDS = frozenset(
    {"role", "logical_relative_path", "absolute_path", "size_bytes", "sha256"}
)
_MANAGEMENT_NATIVE_FIELDS = frozenset(
    {
        "activation_id",
        "activation_manifest_sha256",
        "bars_logical_relative_path",
        "bars_size_bytes",
        "bars_sha256",
        "sidecar_logical_relative_path",
        "sidecar_size_bytes",
        "sidecar_sha256",
    }
)

_HEX = frozenset("0123456789abcdef")
_DESCRIPTOR_FIELDS = frozenset(
    {"relative_path", "sha256", "size_bytes", "schema_version"}
)
_CHAIN_FIELDS = frozenset(
    {
        "schema_version",
        "generation_id",
        "authorization_scope",
        "config_relative_path",
        "config_sha256",
        "ledger_sha256",
        "native_activation_id",
        "native_activation_manifest_sha256",
        "requested_symbols",
        "authority_artifact_sha256_by_name",
        "receipt_sha256",
    }
)
_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "status",
        "errors",
        "raw_reverification",
        "selected_cell_ids",
        "possible_slot_count",
        "source_unavailable_cell_ids",
        "regime_unobserved_cell_ids",
        "no_local_winner_cell_ids",
        "insufficient_evidence_cell_ids",
        "technical_cell_ids",
        "unsearched_cell_ids",
        "missing_observed_cell_ids",
        "native_activation_id",
        "native_activation_manifest_sha256",
        "ledger_sha256",
        "selected_capital_envelope",
        "capital_config_matches",
        "capital_execution_enforcement_reverified",
        "funded_live_authorized",
    }
)


def _valid_requested_symbol(value: Any) -> bool:
    if type(value) is not str or not value or value != value.strip():
        return False
    if not 1 <= len(value.encode("utf-8")) <= 128:
        return False
    return all(
        character.isascii() and (character.isalnum() or character in {"-", ".", "_"})
        for character in value
    )


class _Invalid(ValueError):
    pass


def _is_sha(value: Any) -> bool:
    return (
        type(value) is str and len(value) == 64 and all(char in _HEX for char in value)
    )


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_sha(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _reject_constant(value: str) -> None:
    raise _Invalid(f"JSON_NONFINITE_CONSTANT:{value}")


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _Invalid(f"JSON_DUPLICATE_KEY:{key}")
        result[key] = value
    return result


def _bounded_value(value: Any) -> None:
    work: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while work:
        item, depth = work.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise _Invalid("JSON_STRUCTURE_UNBOUNDED")
        if type(item) is float and not math.isfinite(item):
            raise _Invalid("JSON_NONFINITE_NUMBER")
        if type(item) is str and len(item.encode("utf-8")) > MAX_STRING_BYTES:
            raise _Invalid("JSON_STRING_UNBOUNDED")
        if type(item) is dict:
            work.extend((key, depth + 1) for key in item)
            work.extend((child, depth + 1) for child in item.values())
        elif type(item) is list:
            work.extend((child, depth + 1) for child in item)
        elif item is not None and type(item) not in {str, int, float, bool}:
            raise _Invalid("JSON_VALUE_TYPE_INVALID")


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
                raise _Invalid("PATH_REPARSE_POINT_FORBIDDEN")


def _safe_path(root: Path, candidate: Path, *, directory: bool = False) -> Path:
    _reject_reparse_chain(root)
    _reject_reparse_chain(candidate)
    root = root.resolve(strict=True)
    resolved = candidate.resolve(strict=True)
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise _Invalid("PATH_OUTSIDE_AUTHORITY_ROOT") from exc
    current = root
    if _is_reparse(current):
        raise _Invalid("PATH_REPARSE_POINT_FORBIDDEN")
    for part in relative.parts:
        current = current / part
        if _is_reparse(current):
            raise _Invalid("PATH_REPARSE_POINT_FORBIDDEN")
    if directory:
        if not resolved.is_dir():
            raise _Invalid("PATH_NOT_DIRECTORY")
    elif not resolved.is_file():
        raise _Invalid("PATH_NOT_REGULAR_FILE")
    return resolved


def _snapshot(
    path: Path,
    maximum_bytes: int,
    *,
    root: Optional[Path] = None,
) -> tuple[Any, str, int, Path]:
    candidate = Path(path)
    if root is not None:
        candidate = _safe_path(root, candidate)
    else:
        _reject_reparse_chain(candidate)
        candidate = candidate.resolve(strict=True)
    path_before = candidate.lstat()
    path_identity_before = (
        path_before.st_dev,
        path_before.st_ino,
        path_before.st_size,
        path_before.st_mtime_ns,
    )
    if not stat.S_ISREG(path_before.st_mode):
        raise _Invalid("PATH_NOT_REGULAR_FILE")
    if int(getattr(path_before, "st_file_attributes", 0) or 0) & 0x400:
        raise _Invalid("PATH_REPARSE_POINT_FORBIDDEN")
    with candidate.open("rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise _Invalid("PATH_NOT_REGULAR_FILE")
        if not 0 < before.st_size <= maximum_bytes:
            raise _Invalid("FILE_SIZE_INVALID")
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = handle.read(min(READ_CHUNK_BYTES, maximum_bytes + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                raise _Invalid("FILE_SIZE_INVALID")
            digest.update(chunk)
            chunks.append(chunk)
        after = os.fstat(handle.fileno())
    path_after = candidate.lstat()
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    path_identity_after = (
        path_after.st_dev,
        path_after.st_ino,
        path_after.st_size,
        path_after.st_mtime_ns,
    )
    if (
        path_identity_before != identity_before
        or identity_before != identity_after
        or identity_after != path_identity_after
        or total != before.st_size
    ):
        raise _Invalid("FILE_CHANGED_DURING_READ")
    try:
        value = json.loads(
            b"".join(chunks).decode("utf-8"),
            object_pairs_hook=_pairs,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _Invalid("JSON_INVALID") from exc
    _bounded_value(value)
    return value, digest.hexdigest(), total, candidate


def _unique(errors: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(str(error) for error in errors if str(error)))


def _error(errors: list[str], code: str, detail: Any = None) -> None:
    errors.append(code if detail in (None, "") else f"{code}:{detail}")


def _relative_path(
    root: Path, value: Any, label: str, *, directory: bool = False
) -> Path:
    if type(value) is not str or not value:
        raise _Invalid(f"{label}_PATH_INVALID")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise _Invalid(f"{label}_PATH_INVALID")
    return _safe_path(root, root / relative, directory=directory)


def _safe_directory(root: Path, candidate: Path) -> Path:
    return _safe_path(root, candidate, directory=True)


def _logical_relative(value: Any, *, allow_root: bool = False) -> str:
    if type(value) is not str or not value or "\\" in value:
        raise _Invalid("MANAGEMENT_LOGICAL_PATH_INVALID")
    if allow_root and value == ".":
        return ""
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or any(part in {"", ".", ".."} or ":" in part for part in path.parts)
        or path.as_posix() != value
    ):
        raise _Invalid("MANAGEMENT_LOGICAL_PATH_INVALID")
    return value


def _stream_fingerprint(path: Path, maximum_bytes: int) -> tuple[int, str]:
    path_before = path.lstat()
    path_identity_before = (
        path_before.st_dev,
        path_before.st_ino,
        path_before.st_size,
        path_before.st_mtime_ns,
    )
    if (
        not stat.S_ISREG(path_before.st_mode)
        or path_before.st_nlink != 1
        or not 0 < path_before.st_size <= maximum_bytes
        or int(getattr(path_before, "st_file_attributes", 0) or 0) & 0x400
    ):
        raise _Invalid("MANAGEMENT_EXTERNAL_FILE_INVALID")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        before = os.fstat(handle.fileno())
        remaining = before.st_size
        while remaining:
            chunk = handle.read(min(READ_CHUNK_BYTES, remaining))
            if not chunk:
                raise _Invalid("MANAGEMENT_EXTERNAL_FILE_TRUNCATED")
            digest.update(chunk)
            remaining -= len(chunk)
        if handle.read(1):
            raise _Invalid("MANAGEMENT_EXTERNAL_FILE_GREW")
        after = os.fstat(handle.fileno())
    path_after = path.lstat()
    identities = {
        path_identity_before,
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns),
        (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
        (
            path_after.st_dev,
            path_after.st_ino,
            path_after.st_size,
            path_after.st_mtime_ns,
        ),
    }
    if len(identities) != 1:
        raise _Invalid("MANAGEMENT_EXTERNAL_FILE_CHANGED")
    return before.st_size, digest.hexdigest()


class _ManagementClosure:
    def __init__(self, source_root: Path, raw: Any) -> None:
        if type(raw) is not dict or not 1 <= len(raw) <= MAX_MANAGEMENT_CLOSURE_FILES:
            raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_ROSTER_INVALID")
        self.source_root = _safe_directory(source_root, source_root)
        self.descriptors: dict[str, dict[str, Any]] = {}
        self.by_role_path: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
        self.consumed: set[str] = set()
        identities: dict[str, tuple[int, str]] = {}
        total = 0
        for closure_id, item in raw.items():
            if (
                not _is_sha(closure_id)
                or type(item) is not dict
                or set(item) != _MANAGEMENT_CLOSURE_FIELDS
                or item.get("role") not in _MANAGEMENT_ROLES
                or type(item.get("size_bytes")) is not int
                or not 0 < item["size_bytes"] <= MAX_MANAGEMENT_EXTERNAL_FILE_BYTES
                or not _is_sha(item.get("sha256"))
            ):
                raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_DESCRIPTOR_INVALID")
            logical = _logical_relative(item.get("logical_relative_path"))
            body = {
                "role": item["role"],
                "logical_relative_path": logical,
                "size_bytes": item["size_bytes"],
                "sha256": item["sha256"],
            }
            if _canonical_sha(body) != closure_id:
                raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_ID_INVALID")
            expected = _safe_path(self.source_root, self.source_root / Path(logical))
            absolute = item.get("absolute_path")
            if type(absolute) is not str or absolute != str(expected):
                raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_ABSOLUTE_PATH_INVALID")
            key = (item["role"], logical)
            if key in self.by_role_path:
                raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_ROLE_PATH_DUPLICATE")
            first_path_use = logical not in identities
            previous = identities.setdefault(
                logical, (item["size_bytes"], item["sha256"])
            )
            if previous != (item["size_bytes"], item["sha256"]):
                raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_PATH_CONFLICT")
            if first_path_use:
                total += item["size_bytes"]
            if total > MAX_MANAGEMENT_CLOSURE_BYTES:
                raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_BYTES_EXCEEDED")
            copied = dict(item)
            self.descriptors[closure_id] = copied
            self.by_role_path[key] = (closure_id, copied)
        self.verify_all()

    def verify_all(self) -> None:
        for item in self.descriptors.values():
            path = _safe_path(
                self.source_root,
                self.source_root / Path(item["logical_relative_path"]),
            )
            size, digest = _stream_fingerprint(path, MAX_MANAGEMENT_EXTERNAL_FILE_BYTES)
            if size != item["size_bytes"] or digest != item["sha256"]:
                raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_HASH_MISMATCH")

    def consume_file(
        self,
        role: str,
        logical: str,
        *,
        size_bytes: Optional[int] = None,
        sha256: Optional[str] = None,
    ) -> Path:
        key = (role, _logical_relative(logical))
        found = self.by_role_path.get(key)
        if found is None:
            raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_FILE_MISSING")
        closure_id, item = found
        if size_bytes is not None and item["size_bytes"] != size_bytes:
            raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_SIZE_MISMATCH")
        if sha256 is not None and item["sha256"] != sha256:
            raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_HASH_MISMATCH")
        self.consumed.add(closure_id)
        return _safe_path(self.source_root, Path(item["absolute_path"]))

    def consume_directory(
        self,
        role: str,
        logical_root: str,
        *,
        required_logicals: Optional[set[str]] = None,
    ) -> Path:
        logical_root = _logical_relative(logical_root, allow_root=True)
        root = _safe_directory(
            self.source_root,
            self.source_root / Path(logical_root or "."),
        )
        prefix = f"{logical_root}/" if logical_root else ""
        expected = {
            item["logical_relative_path"]
            for item in self.descriptors.values()
            if item["role"] == role and item["logical_relative_path"].startswith(prefix)
        }
        if required_logicals is not None and expected != required_logicals:
            raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_DECLARED_ROSTER_MISMATCH")
        for closure_id, item in self.descriptors.items():
            if item["role"] == role and item["logical_relative_path"] in expected:
                self.consumed.add(closure_id)
        return root

    def require_complete(self) -> None:
        if self.consumed != set(self.descriptors):
            raise _Invalid("MANAGEMENT_EXTERNAL_CLOSURE_UNCONSUMED")


def _capsule_root(path: Path, capsule_id: str) -> Path:
    resolved = path.resolve(strict=True)
    for parent in (resolved.parent, *resolved.parents):
        if parent.name == capsule_id:
            relative = resolved.relative_to(parent)
            if relative.parts and relative.parts[0] == "closure":
                return _safe_directory(parent, parent)
    raise _Invalid("MANAGEMENT_CAPSULE_ROOT_INVALID")


def _management_context(
    value: Any,
    *,
    manifest_path: Path,
    requested_symbols: Sequence[str],
) -> tuple[Optional[dict[str, Any]], list[str]]:
    errors: list[str] = []
    try:
        if type(value) is not dict or set(value) != _MANAGEMENT_CONTEXT_FIELDS:
            raise _Invalid("MANAGEMENT_CONTEXT_FIELDS_INVALID")
        if (
            value.get("schema_version") != MANAGEMENT_CONTEXT_SCHEMA
            or not _is_sha(value.get("capsule_id"))
            or type(value.get("generation_id")) is not str
            or not value["generation_id"]
            or value.get("scope") != MANAGEMENT_STATUS
            or value.get("allow_expired_entry_management") is not True
            or value.get("allow_inactive_generation_pointer") is not True
            or value.get("allow_new_entries") is not False
            or value.get("allow_position_increase") is not False
            or value.get("allow_stop_widening") is not False
            or not _is_sha(value.get("captured_activation_manifest_sha256"))
        ):
            raise _Invalid("MANAGEMENT_CONTEXT_POLICY_INVALID")
        source_raw = value.get("captured_source_root")
        if type(source_raw) is not str or not Path(source_raw).is_absolute():
            raise _Invalid("MANAGEMENT_CAPTURED_SOURCE_ROOT_INVALID")
        source_root = _safe_directory(Path(source_raw), Path(source_raw))
        if source_raw != str(source_root):
            raise _Invalid("MANAGEMENT_CAPTURED_SOURCE_ROOT_INVALID")
        capsule_id = value["capsule_id"]
        capsule_root = _capsule_root(Path(manifest_path), capsule_id)
        capsule, _, _, _ = _snapshot(
            capsule_root / "capsule.json", MAX_INDEX_BYTES, root=capsule_root
        )
        if type(capsule) is not dict or capsule.get("capsule_id") != capsule_id:
            raise _Invalid("MANAGEMENT_CAPSULE_DOCUMENT_INVALID")
        capsule_body = dict(capsule)
        capsule_body.pop("capsule_id")
        if (
            _canonical_sha(capsule_body) != capsule_id
            or capsule.get("schema_version") != "fxpm.demo-management-authority.v1"
            or capsule.get("generation_id") != value["generation_id"]
            or capsule.get("source_root") != source_raw
            or capsule.get("requested_symbols") != list(requested_symbols)
        ):
            raise _Invalid("MANAGEMENT_CAPSULE_BINDING_INVALID")
        permissions = capsule.get("permissions")
        if (
            type(permissions) is not dict
            or permissions.get("open_new_positions") is not False
            or permissions.get("increase_positions") is not False
            or permissions.get("widen_stops") is not False
        ):
            raise _Invalid("MANAGEMENT_CAPSULE_PERMISSIONS_INVALID")
        closure = _ManagementClosure(source_root, value.get("external_closure"))
        persisted_closure = {
            closure_id: {
                key: item[key]
                for key in ("role", "logical_relative_path", "size_bytes", "sha256")
            }
            for closure_id, item in value["external_closure"].items()
        }
        if capsule.get("external_closure") != persisted_closure or capsule.get(
            "external_closure_sha256"
        ) != _canonical_sha(persisted_closure):
            raise _Invalid("MANAGEMENT_CAPSULE_CLOSURE_BINDING_INVALID")
        native_bindings = value.get("selected_native_source_bindings")
        if type(native_bindings) is not dict or not native_bindings:
            raise _Invalid("MANAGEMENT_NATIVE_BINDING_ROSTER_INVALID")
        for cell_id, binding in native_bindings.items():
            if (
                type(cell_id) is not str
                or cell_id.count("|") != 1
                or type(binding) is not dict
                or set(binding) != _MANAGEMENT_NATIVE_FIELDS
                or not _is_sha(binding.get("activation_manifest_sha256"))
                or any(
                    not _is_sha(binding.get(name))
                    for name in ("bars_sha256", "sidecar_sha256")
                )
                or any(
                    type(binding.get(name)) is not int or binding[name] <= 0
                    for name in ("bars_size_bytes", "sidecar_size_bytes")
                )
            ):
                raise _Invalid("MANAGEMENT_NATIVE_BINDING_INVALID")
            _logical_relative(binding["bars_logical_relative_path"])
            _logical_relative(binding["sidecar_logical_relative_path"])
        if capsule.get(
            "selected_native_source_bindings"
        ) != native_bindings or capsule.get(
            "selected_native_source_bindings_sha256"
        ) != _canonical_sha(native_bindings):
            raise _Invalid("MANAGEMENT_CAPSULE_NATIVE_BINDING_INVALID")
        activation_raw = value.get("captured_activation_manifest_path")
        if type(activation_raw) is not str or not Path(activation_raw).is_absolute():
            raise _Invalid("MANAGEMENT_CAPTURED_ACTIVATION_PATH_INVALID")
        activation_path = _safe_path(capsule_root, Path(activation_raw))
        activation, activation_sha, _, _ = _snapshot(
            activation_path, MAX_INDEX_BYTES, root=capsule_root
        )
        if activation_sha != value["captured_activation_manifest_sha256"]:
            raise _Invalid("MANAGEMENT_CAPTURED_ACTIVATION_HASH_MISMATCH")
        snapshots = capsule.get("source_snapshots")
        descriptor = (
            snapshots.get("native_activation_manifest")
            if type(snapshots) is dict
            else None
        )
        if type(descriptor) is not dict:
            raise _Invalid("MANAGEMENT_CAPSULE_ACTIVATION_DESCRIPTOR_MISSING")
        descriptor_path = _safe_path(
            capsule_root,
            capsule_root / Path(descriptor.get("capsule_relative_path", "")),
        )
        if (
            descriptor_path != activation_path
            or descriptor.get("sha256") != activation_sha
            or capsule.get("native_activation_manifest_sha256") != activation_sha
        ):
            raise _Invalid("MANAGEMENT_CAPSULE_ACTIVATION_BINDING_INVALID")
        return {
            "value": dict(value),
            "capsule": capsule,
            "capsule_root": capsule_root,
            "source_root": source_root,
            "closure": closure,
            "activation": activation,
            "activation_path": activation_path,
            "activation_sha256": activation_sha,
            "native_bindings": native_bindings,
        }, []
    except Exception as exc:
        _error(errors, "MANAGEMENT_CONTEXT_RAW_REVERIFY_FAILED", type(exc).__name__)
        if isinstance(exc, _Invalid):
            errors.append(str(exc))
        return None, _unique(errors)


def _book_decision(value: Any) -> tuple[Any, dict[str, Any], list[str]]:
    errors: list[str] = []
    if type(value) is not dict:
        return None, {}, ["BOOK_DECISION_NOT_OBJECT"]
    body = dict(value)
    receipt = body.pop("receipt_sha256", None)
    if not _is_sha(receipt):
        return None, {}, ["BOOK_DECISION_RECEIPT_INVALID"]
    try:
        from pm_book_multiplicity import (
            BookMultiplicityDecision,
            verify_historical_book_multiplicity_decision,
        )

        raw = json.dumps(
            body,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        if hashlib.sha256(raw).hexdigest() != receipt:
            errors.append("BOOK_DECISION_RECEIPT_MISMATCH")
        decision = BookMultiplicityDecision(raw, receipt)
        if not verify_historical_book_multiplicity_decision(decision):
            errors.append("BOOK_DECISION_RAW_REVERIFY_FAILED")
    except Exception as exc:
        decision = None
        _error(errors, "BOOK_DECISION_RAW_REVERIFY_FAILED", type(exc).__name__)
    return decision, body, _unique(errors)


def _index(
    indexes: Mapping[str, Any], name: str, fields_exact: set[str]
) -> tuple[Optional[dict[str, Any]], list[str]]:
    value = indexes.get(name)
    if type(value) is not dict:
        return None, [f"AUTHORITY_INDEX_NOT_OBJECT:{name}"]
    if set(value) != fields_exact or value.get("schema_version") != INDEX_SCHEMAS[name]:
        return None, [f"AUTHORITY_INDEX_SCHEMA_OR_FIELDS_INVALID:{name}"]
    return value, []


def _load_indexes(
    *,
    manifest: Mapping[str, Any],
    manifest_path: Path,
    supplied_paths: Mapping[str, Path],
) -> tuple[dict[str, Any], dict[str, str], list[str]]:
    errors: list[str] = []
    root = manifest_path.parent.resolve(strict=True)
    descriptors = manifest.get("authority_artifacts")
    if type(descriptors) is not dict or set(descriptors) != set(ARTIFACTS):
        return {}, {}, ["AUTHORITY_ARTIFACT_ROSTER_INVALID"]
    if type(supplied_paths) is not dict or set(supplied_paths) != set(ARTIFACTS):
        return {}, {}, ["AUTHORITY_INDEX_PATH_ROSTER_INVALID"]
    loaded: dict[str, Any] = {}
    hashes: dict[str, str] = {}
    total = 0
    for name in ARTIFACTS:
        descriptor = descriptors.get(name)
        if type(descriptor) is not dict or set(descriptor) != _DESCRIPTOR_FIELDS:
            _error(errors, "AUTHORITY_ARTIFACT_DESCRIPTOR_INVALID", name)
            continue
        if descriptor.get("schema_version") != INDEX_SCHEMAS[name]:
            _error(errors, "AUTHORITY_ARTIFACT_SCHEMA_INVALID", name)
            continue
        relative = descriptor.get("relative_path")
        try:
            expected_path = _relative_path(root, relative, f"AUTHORITY_{name}")
            supplied = _safe_path(root, Path(supplied_paths[name]))
            if supplied != expected_path:
                raise _Invalid("SUPPLIED_PATH_MISMATCH")
            value, observed_sha, size, _ = _snapshot(
                expected_path, MAX_INDEX_BYTES, root=root
            )
        except Exception as exc:
            _error(
                errors, "AUTHORITY_ARTIFACT_UNREADABLE", f"{name}:{type(exc).__name__}"
            )
            continue
        declared_size = descriptor.get("size_bytes")
        declared_sha = descriptor.get("sha256")
        if type(declared_size) is not int or declared_size != size:
            _error(errors, "AUTHORITY_ARTIFACT_SIZE_MISMATCH", name)
            continue
        if not _is_sha(declared_sha) or declared_sha != observed_sha:
            _error(errors, "AUTHORITY_ARTIFACT_SHA256_MISMATCH", name)
            continue
        total += size
        loaded[name] = value
        hashes[name] = observed_sha
    if total > MAX_TOTAL_INDEX_BYTES:
        errors.append("AUTHORITY_ARTIFACT_TOTAL_SIZE_EXCEEDED")
    return loaded, hashes, _unique(errors)


def _native_reverify(
    *,
    config: Any,
    config_path: Path,
    requested_symbols: Sequence[str],
    supplied_universe: Any,
) -> tuple[Optional[Any], list[str]]:
    errors: list[str] = []
    try:
        from pm_native_activation import resolve_active_native_universe

        data_dir = Path(getattr(config, "data_dir", "data"))
        if not data_dir.is_absolute():
            data_dir = config_path.parent / data_dir
        _reject_reparse_chain(data_dir)
        _reject_reparse_chain(data_dir / "native")
        universe = resolve_active_native_universe(
            data_dir,
            requested_symbols,
            required_timeframes=TIMEFRAMES,
            production_contract_required=True,
        )
        if not universe.valid or not universe.explicit_contract:
            errors.extend(str(value) for value in universe.errors)
        if universe.activation_id != getattr(
            supplied_universe, "activation_id", None
        ) or universe.activation_manifest_sha256 != getattr(
            supplied_universe, "activation_manifest_sha256", None
        ):
            errors.append("NATIVE_UNIVERSE_CALLER_SNAPSHOT_MISMATCH")
        for symbol in requested_symbols:
            for timeframe in TIMEFRAMES:
                if universe.cell_status(symbol, timeframe) != getattr(
                    supplied_universe, "cell_status", lambda *_: "unbound"
                )(symbol, timeframe):
                    errors.append(
                        f"NATIVE_CELL_CALLER_SNAPSHOT_MISMATCH:{symbol}|{timeframe}"
                    )
        manifest_path = Path(universe.activation_manifest_path)
        manifest, manifest_sha, _, _ = _snapshot(
            manifest_path, MAX_INDEX_BYTES, root=manifest_path.parent
        )
        if (
            type(manifest) is not dict
            or manifest_sha != universe.activation_manifest_sha256
            or manifest.get("activation_id") != universe.activation_id
        ):
            errors.append("NATIVE_ACTIVATION_MANIFEST_IDENTITY_INVALID")
        pointer_path = data_dir.resolve() / "native" / "ACTIVE.json"
        pointer, _, _, _ = _snapshot(
            pointer_path, MAX_MANIFEST_BYTES, root=pointer_path.parent
        )
        if (
            type(pointer) is not dict
            or pointer.get("activation_id") != universe.activation_id
            or pointer.get("manifest_sha256") != universe.activation_manifest_sha256
        ):
            errors.append("NATIVE_ACTIVE_POINTER_IDENTITY_INVALID")
    except Exception as exc:
        universe = None
        _error(errors, "NATIVE_DATA_STATE_RAW_REVERIFY_FAILED", type(exc).__name__)
    return universe, _unique(errors)


def _management_native_reverify(
    *,
    state: Mapping[str, Any],
    requested_symbols: Sequence[str],
    supplied_universe: Any,
) -> tuple[Optional[Any], list[str]]:
    errors: list[str] = []
    try:
        from pm_native_activation import (
            ACTIVATION_MANIFEST_SCHEMA,
            NATIVE_AUTHENTICITY_DECISION_BASIS,
            NATIVE_EVIDENCE_CANDIDATE,
            NATIVE_EVIDENCE_PARTITIONS,
            NATIVE_PENDING,
            NATIVE_SESSION_COVERAGE_METHOD,
            NATIVE_SOURCE_UNAVAILABLE,
            NATIVE_TECHNICAL_ERROR,
            UNIVERSE_AUTHENTICITY_SCHEMA,
            ActiveNativeUniverse,
        )

        manifest = state.get("activation")
        context = state["value"]
        activation_sha = state["activation_sha256"]
        if type(manifest) is not dict:
            raise _Invalid("MANAGEMENT_ACTIVATION_MANIFEST_INVALID")
        activation_id = manifest.get("activation_id")
        contract = manifest.get("universe_eligibility")
        entries = manifest.get("entries")
        if (
            manifest.get("schema_version") != ACTIVATION_MANIFEST_SCHEMA
            or str(manifest.get("status") or "").lower() != "active"
            or manifest.get("production_authoritative") is not True
            or manifest.get("native_is_signal_authority") is not True
            or manifest.get("silent_fallback_allowed") is not False
            or type(activation_id) is not str
            or not activation_id
            or type(contract) is not dict
            or contract.get("schema_version") != UNIVERSE_AUTHENTICITY_SCHEMA
            or type(entries) is not dict
        ):
            raise _Invalid("MANAGEMENT_ACTIVATION_CONTRACT_INVALID")
        if set(contract) != {
            "schema_version",
            "decision_basis",
            "performance_inputs_used",
            "trading_verdicts_emitted",
            "coverage_is_diagnostic_only",
            "fixed_coverage_activation_threshold",
            "session_coverage_method",
            "session_coverage_partitions",
            "requested_symbols",
            "required_timeframes",
            "cell_statuses",
            "terminal_states",
            "nonterminal_states",
            "evidence_candidate_timeframes_by_symbol",
            "source_unavailable_timeframes_by_symbol",
            "symbols_with_evidence_candidates",
            "eligible_symbols",
            "excluded_symbols",
            "statistical_sufficiency_authority",
        }:
            raise _Invalid("MANAGEMENT_ACTIVATION_AUTHENTICITY_FIELDS_INVALID")
        declared_terminal_states = list(contract.get("terminal_states") or [])
        declared_nonterminal_states = list(contract.get("nonterminal_states") or [])
        legacy_terminal_states = [
            NATIVE_EVIDENCE_CANDIDATE,
            NATIVE_SOURCE_UNAVAILABLE,
        ]
        current_terminal_states = [
            NATIVE_EVIDENCE_CANDIDATE,
            NATIVE_SOURCE_UNAVAILABLE,
            NATIVE_TECHNICAL_ERROR,
        ]
        if (
            contract.get("decision_basis") != NATIVE_AUTHENTICITY_DECISION_BASIS
            or contract.get("performance_inputs_used") is not False
            or contract.get("trading_verdicts_emitted") is not False
            or contract.get("coverage_is_diagnostic_only") is not True
            or contract.get("fixed_coverage_activation_threshold") is not None
            or contract.get("session_coverage_method") != NATIVE_SESSION_COVERAGE_METHOD
            or list(contract.get("session_coverage_partitions") or [])
            != list(NATIVE_EVIDENCE_PARTITIONS)
            or tuple(declared_terminal_states)
            not in {tuple(legacy_terminal_states), tuple(current_terminal_states)}
            or (
                declared_terminal_states == legacy_terminal_states
                and declared_nonterminal_states
                != [NATIVE_TECHNICAL_ERROR, NATIVE_PENDING]
            )
            or (
                declared_terminal_states == current_terminal_states
                and declared_nonterminal_states != [NATIVE_PENDING]
            )
            or contract.get("excluded_symbols") != {}
            or contract.get("statistical_sufficiency_authority")
            != "authenticated_horizon_ess_trade_power_and_multiplicity_v1"
            or manifest.get("coverage_activation_threshold_removed") is not True
            or manifest.get("minimum_session_coverage") is not None
        ):
            raise _Invalid("MANAGEMENT_ACTIVATION_AUTHENTICITY_POLICY_INVALID")
        if (
            list(contract.get("requested_symbols") or []) != list(requested_symbols)
            or list(contract.get("required_timeframes") or []) != list(TIMEFRAMES)
            or set(entries) != set(requested_symbols)
        ):
            raise _Invalid("MANAGEMENT_ACTIVATION_ROSTER_INVALID")
        raw_statuses = contract.get("cell_statuses")
        if type(raw_statuses) is not dict or set(raw_statuses) != set(
            requested_symbols
        ):
            raise _Invalid("MANAGEMENT_ACTIVATION_STATUS_ROSTER_INVALID")
        statuses: dict[str, dict[str, str]] = {}
        candidate_projection: dict[str, list[str]] = {}
        unavailable_projection: dict[str, list[str]] = {}
        for symbol in requested_symbols:
            raw_grid = raw_statuses.get(symbol)
            symbol_entries = entries.get(symbol)
            if (
                type(raw_grid) is not dict
                or type(symbol_entries) is not dict
                or set(raw_grid) != set(TIMEFRAMES)
                or set(symbol_entries) != set(TIMEFRAMES)
            ):
                raise _Invalid("MANAGEMENT_ACTIVATION_CELL_GRID_INVALID")
            statuses[symbol] = {}
            candidate_projection[symbol] = []
            unavailable_projection[symbol] = []
            for timeframe in TIMEFRAMES:
                cell = symbol_entries.get(timeframe)
                status = str(raw_grid.get(timeframe) or "").lower()
                if (
                    type(cell) is not dict
                    or status not in set(declared_terminal_states)
                    or str(cell.get("status") or "").lower() != status
                    or cell.get("symbol") != symbol
                    or cell.get("timeframe") != timeframe
                    or cell.get("tradeable") is not False
                    or cell.get("statistical_sufficiency_receipt_required") is not True
                    or str(cell.get("universe_eligibility_status") or "").lower()
                    != status
                ):
                    raise _Invalid("MANAGEMENT_ACTIVATION_CELL_INVALID")
                statuses[symbol][timeframe] = status
                if status == NATIVE_EVIDENCE_CANDIDATE:
                    candidate_projection[symbol].append(timeframe)
                    if (
                        not _is_sha(cell.get("bars_sha256"))
                        or not _is_sha(cell.get("sidecar_sha256"))
                        or type(cell.get("bars_relative_path")) is not str
                        or type(cell.get("sidecar_relative_path")) is not str
                    ):
                        raise _Invalid("MANAGEMENT_ACTIVATION_SOURCE_BINDING_INVALID")
                elif status == NATIVE_SOURCE_UNAVAILABLE:
                    unavailable_projection[symbol].append(timeframe)
                else:
                    if (
                        cell.get("audit_status") != "PASS"
                        or cell.get("bars_sha256") is not None
                        or not _is_sha(cell.get("sidecar_sha256"))
                        or type(cell.get("bars_relative_path")) is not str
                        or type(cell.get("sidecar_relative_path")) is not str
                        or str(cell.get("consensus_status") or "").upper() != "PENDING"
                        or str(cell.get("resolution_status") or "").upper() != "PENDING"
                    ):
                        raise _Invalid(
                            "MANAGEMENT_ACTIVATION_TERMINAL_TECHNICAL_BINDING_INVALID"
                        )
        candidate_symbols = [
            symbol for symbol in requested_symbols if candidate_projection[symbol]
        ]
        if (
            contract.get("evidence_candidate_timeframes_by_symbol")
            != candidate_projection
            or contract.get("source_unavailable_timeframes_by_symbol")
            != unavailable_projection
            or contract.get("symbols_with_evidence_candidates") != candidate_symbols
            or contract.get("eligible_symbols") != candidate_symbols
        ):
            raise _Invalid("MANAGEMENT_ACTIVATION_AUTHENTICITY_PROJECTION_INVALID")
        snapshots = state["capsule"].get("source_snapshots")
        activation_descriptor = snapshots.get("native_activation_manifest")
        activation_logical = _logical_relative(
            activation_descriptor.get("source_relative_path")
        )
        activation_logical_path = PurePosixPath(activation_logical)
        if (
            activation_logical_path.parent.name != "activations"
            or activation_logical_path.name != f"{activation_id}.json"
        ):
            raise _Invalid("MANAGEMENT_ACTIVATION_LOGICAL_PATH_INVALID")
        native_prefix = activation_logical_path.parent.parent
        bindings = state["native_bindings"]
        for group, binding in bindings.items():
            symbol, timeframe = group.split("|", 1)
            if symbol not in requested_symbols or timeframe not in TIMEFRAMES:
                raise _Invalid("MANAGEMENT_NATIVE_BINDING_CELL_INVALID")
            cell = entries[symbol][timeframe]
            if statuses[symbol][timeframe] != NATIVE_EVIDENCE_CANDIDATE:
                raise _Invalid("MANAGEMENT_NATIVE_BINDING_SOURCE_UNAVAILABLE")
            bars_relative = _logical_relative(cell.get("bars_relative_path"))
            sidecar_relative = _logical_relative(cell.get("sidecar_relative_path"))
            expected_bars = (native_prefix / PurePosixPath(bars_relative)).as_posix()
            expected_sidecar = (
                native_prefix / PurePosixPath(sidecar_relative)
            ).as_posix()
            if (
                binding.get("activation_id") != activation_id
                or binding.get("activation_manifest_sha256") != activation_sha
                or binding.get("bars_logical_relative_path") != expected_bars
                or binding.get("sidecar_logical_relative_path") != expected_sidecar
                or binding.get("bars_sha256") != cell.get("bars_sha256")
                or binding.get("sidecar_sha256") != cell.get("sidecar_sha256")
            ):
                raise _Invalid("MANAGEMENT_NATIVE_BINDING_MANIFEST_MISMATCH")
            closure = state["closure"]
            closure.consume_file(
                "native_bars",
                expected_bars,
                size_bytes=binding["bars_size_bytes"],
                sha256=binding["bars_sha256"],
            )
            closure.consume_file(
                "native_sidecar",
                expected_sidecar,
                size_bytes=binding["sidecar_size_bytes"],
                sha256=binding["sidecar_sha256"],
            )
        if (
            context.get("captured_activation_manifest_sha256") != activation_sha
            or state["capsule"].get("native_activation_id") != activation_id
            or getattr(supplied_universe, "activation_id", None) != activation_id
            or getattr(supplied_universe, "activation_manifest_sha256", None)
            != activation_sha
        ):
            raise _Invalid("MANAGEMENT_NATIVE_CALLER_OR_CAPSULE_BINDING_MISMATCH")
        universe = ActiveNativeUniverse()
        universe.activation_id = activation_id
        universe.activation_manifest_path = state["activation_path"]
        universe.activation_manifest_sha256 = activation_sha
        universe.entries = dict(entries)
        universe.explicit_contract = True
        universe.exact_symbol_identity = True
        universe.eligibility_schema = UNIVERSE_AUTHENTICITY_SCHEMA
        universe.required_timeframes = TIMEFRAMES
        universe.requested_symbols = tuple(requested_symbols)
        universe.cell_statuses = statuses
        universe.eligible_symbols = tuple(
            symbol
            for symbol in requested_symbols
            if any(
                status == NATIVE_EVIDENCE_CANDIDATE
                for status in statuses[symbol].values()
            )
        )
        universe.symbols_with_active_cells = universe.eligible_symbols
        universe.excluded_symbols = {}
        universe.fully_excluded_symbols = ()
        return universe, []
    except Exception as exc:
        _error(errors, "MANAGEMENT_NATIVE_RAW_REVERIFY_FAILED", type(exc).__name__)
        if isinstance(exc, _Invalid):
            errors.append(str(exc))
        return None, _unique(errors)


def _book_partition(
    body: Mapping[str, Any], requested_symbols: Sequence[str]
) -> tuple[dict[str, set[str]], list[str]]:
    import pm_book_multiplicity as book

    errors: list[str] = []
    if (
        body.get("valid") is not True
        or body.get("optimization_run_complete") is not True
    ):
        errors.append("CURRENT_BOOK_NOT_COMPLETE_VALID")
    possible = {
        f"{symbol}|{timeframe}|{regime}"
        for symbol in requested_symbols
        for timeframe in TIMEFRAMES
        for regime in REGIMES
    }
    rosters = {
        "selected_cell_ids": set(),
        "source_unavailable_cell_ids": set(),
        "regime_unobserved_cell_ids": set(),
        "no_local_winner_cell_ids": set(),
        "insufficient_evidence_cell_ids": set(),
        "technical_cell_ids": set(),
        "unsearched_cell_ids": set(),
        "missing_observed_cell_ids": set(),
    }
    state_field = {
        "WINNER": "selected_cell_ids",
        "SOURCE_UNAVAILABLE": "source_unavailable_cell_ids",
        "REGIME_UNOBSERVED": "regime_unobserved_cell_ids",
        "NO_WINNER": "no_local_winner_cell_ids",
        "INSUFFICIENT_EVIDENCE": "insufficient_evidence_cell_ids",
        book.TERMINAL_TECHNICAL: "technical_cell_ids",
        "UNSEARCHED": "unsearched_cell_ids",
    }
    seen: set[str] = set()
    cells = body.get("cells")
    if type(cells) is not list:
        cells = []
        errors.append("CURRENT_BOOK_CELL_ROSTER_INVALID")
    for cell in cells:
        if type(cell) is not dict:
            errors.append("CURRENT_BOOK_CELL_INVALID")
            continue
        symbol = cell.get("symbol")
        timeframe = cell.get("timeframe")
        regime = cell.get("regime")
        if symbol not in requested_symbols:
            continue
        slot = f"{symbol}|{timeframe}|{regime}"
        if slot not in possible or slot in seen:
            _error(errors, "CURRENT_BOOK_CELL_IDENTITY_INVALID", slot)
            continue
        seen.add(slot)
        target = state_field.get(str(cell.get("historical_state") or "").upper())
        if target is None:
            _error(errors, "CURRENT_BOOK_CELL_STATE_INVALID", slot)
            rosters["technical_cell_ids"].add(slot)
        else:
            rosters[target].add(slot)
    missing = possible - seen
    rosters["missing_observed_cell_ids"].update(missing)
    if missing:
        errors.append("CURRENT_BOOK_POSSIBLE_SLOT_ROSTER_INCOMPLETE")
    if rosters["unsearched_cell_ids"]:
        errors.append("CURRENT_BOOK_UNSEARCHED_CELLS_PRESENT")
    return rosters, _unique(errors)


def _fit_by_group(body: Mapping[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    raw = body.get("regime_fits")
    if type(raw) is not list:
        return result
    for item in raw:
        if type(item) is not dict or set(item) != {"payload", "receipt_sha256"}:
            continue
        payload = item.get("payload")
        if type(payload) is not dict:
            continue
        key = (payload.get("symbol"), payload.get("timeframe"))
        if type(key[0]) is str and type(key[1]) is str and key not in result:
            result[key] = item
    return result


def _validate_detector_ledger_record(
    record: Any,
    *,
    symbol: str,
    timeframe: str,
    fit: Mapping[str, Any],
    activation_id: Any,
    activation_manifest_sha256: Any,
    native_entry: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    ledger_fields = {
        "schema_version",
        "regime_fit_contract_version",
        "regime_fit_receipt_sha256",
        "detector_record",
        "detector_record_sha256",
        "ledger_record_sha256",
    }
    detector_fields = {
        "schema_version",
        "symbol",
        "timeframe",
        "source_group_id",
        "source_group_decision_sha256",
        "source_contract_sha256",
        "source_group_evidence_sha256",
        "activation_id",
        "activation_manifest_sha256",
        "native_source_sha256",
        "fit_cutoff_utc",
        "detector_version",
        "detector_code_sha256",
        "parameters_canonical_json",
        "parameters_sha256",
        "record_sha256",
    }
    if type(record) is not dict or set(record) != ledger_fields:
        return [f"REGIME_DETECTOR_LEDGER_FIELDS_INVALID:{symbol}|{timeframe}"]
    try:
        import pm_book_multiplicity as book
        import pm_causal_process as causal

        if (
            record["schema_version"]
            != causal.REGIME_DETECTOR_LEDGER_RECORD_SCHEMA_VERSION
            or record["regime_fit_contract_version"] != book.REGIME_FIT_CONTRACT_VERSION
            or record["regime_fit_receipt_sha256"] != fit.get("receipt_sha256")
        ):
            errors.append(
                f"REGIME_DETECTOR_LEDGER_CONTRACT_INVALID:{symbol}|{timeframe}"
            )
        detector = record.get("detector_record")
        fit_payload = fit.get("payload") if type(fit.get("payload")) is dict else {}
        embedded = fit_payload.get("regime_detector_record")
        if type(detector) is not dict or set(detector) != detector_fields:
            errors.append(f"REGIME_DETECTOR_RECORD_FIELDS_INVALID:{symbol}|{timeframe}")
            detector = {}
        if detector != embedded:
            errors.append(f"REGIME_DETECTOR_RECORD_BOOK_SPLICE:{symbol}|{timeframe}")
        if (
            detector.get("schema_version") != book.REGIME_DETECTOR_RECORD_SCHEMA_VERSION
            or detector.get("symbol") != symbol
            or detector.get("timeframe") != timeframe
            or detector.get("activation_id") != activation_id
            or detector.get("activation_manifest_sha256") != activation_manifest_sha256
        ):
            errors.append(
                f"REGIME_DETECTOR_RECORD_IDENTITY_INVALID:{symbol}|{timeframe}"
            )
        for field in (
            "source_group_decision_sha256",
            "source_contract_sha256",
            "source_group_evidence_sha256",
            "activation_manifest_sha256",
            "native_source_sha256",
            "detector_code_sha256",
            "parameters_sha256",
            "record_sha256",
        ):
            if not _is_sha(detector.get(field)):
                errors.append(
                    f"REGIME_DETECTOR_RECORD_HASH_INVALID:{symbol}|{timeframe}:{field}"
                )
        parameters_text = detector.get("parameters_canonical_json")
        if type(parameters_text) is not str or not parameters_text.isascii():
            errors.append(f"REGIME_DETECTOR_PARAMETERS_INVALID:{symbol}|{timeframe}")
        else:
            parameters = json.loads(
                parameters_text,
                object_pairs_hook=_pairs,
                parse_constant=_reject_constant,
            )
            if _canonical_bytes(parameters).decode("utf-8") != parameters_text:
                errors.append(
                    f"REGIME_DETECTOR_PARAMETERS_NONCANONICAL:{symbol}|{timeframe}"
                )
            if hashlib.sha256(
                parameters_text.encode("ascii")
            ).hexdigest() != detector.get("parameters_sha256"):
                errors.append(
                    f"REGIME_DETECTOR_PARAMETERS_HASH_MISMATCH:{symbol}|{timeframe}"
                )
        detector_body = dict(detector)
        detector_body.pop("record_sha256", None)
        if _canonical_sha(detector_body) != detector.get("record_sha256"):
            errors.append(f"REGIME_DETECTOR_RECORD_HASH_MISMATCH:{symbol}|{timeframe}")
        if record.get("detector_record_sha256") != detector.get("record_sha256"):
            errors.append(f"REGIME_DETECTOR_LEDGER_RECORD_SPLICE:{symbol}|{timeframe}")
        ledger_body = dict(record)
        ledger_body.pop("ledger_record_sha256", None)
        if _canonical_sha(ledger_body) != record.get("ledger_record_sha256"):
            errors.append(f"REGIME_DETECTOR_LEDGER_HASH_MISMATCH:{symbol}|{timeframe}")
        bars_sha = native_entry.get("bars_sha256")
        if _is_sha(bars_sha) and detector.get("native_source_sha256") != bars_sha:
            errors.append(
                f"REGIME_DETECTOR_NATIVE_SOURCE_MISMATCH:{symbol}|{timeframe}"
            )
    except Exception as exc:
        _error(
            errors,
            "REGIME_DETECTOR_LEDGER_RAW_REVERIFY_FAILED",
            f"{symbol}|{timeframe}:{type(exc).__name__}",
        )
    return _unique(errors)


def _regime_support_reverify(
    *,
    index_value: Any,
    current_book_body: Mapping[str, Any],
    current_book_receipt: Optional[str],
    requested_symbols: Sequence[str],
    native_universe: Any,
) -> tuple[dict[str, dict[str, str]], list[str]]:
    exact = {
        "schema_version",
        "current_book_multiplicity_receipt_sha256",
        "native_activation_id",
        "native_activation_manifest_sha256",
        "detector_ledger_records",
    }
    index, errors = _index({"regime_support": index_value}, "regime_support", exact)
    bindings: dict[str, dict[str, str]] = {}
    if index is None:
        return bindings, errors
    if (
        index["current_book_multiplicity_receipt_sha256"] != current_book_receipt
        or index["native_activation_id"]
        != getattr(native_universe, "activation_id", None)
        or index["native_activation_manifest_sha256"]
        != getattr(native_universe, "activation_manifest_sha256", None)
    ):
        errors.append("REGIME_SUPPORT_AUTHORITY_BINDING_MISMATCH")
    records = index.get("detector_ledger_records")
    if type(records) is not list or len(records) > len(requested_symbols) * len(
        TIMEFRAMES
    ):
        return bindings, _unique(errors + ["REGIME_DETECTOR_LEDGER_ROSTER_INVALID"])
    fit_map = _fit_by_group(current_book_body)
    seen: set[tuple[str, str]] = set()
    for row in records:
        if type(row) is not dict or set(row) != {
            "symbol",
            "timeframe",
            "train_regime_support",
            "regime_detector_ledger_record",
        }:
            errors.append("REGIME_DETECTOR_LEDGER_ROW_INVALID")
            continue
        symbol = row.get("symbol")
        timeframe = row.get("timeframe")
        key = (symbol, timeframe)
        if (
            symbol not in requested_symbols
            or timeframe not in TIMEFRAMES
            or key in seen
        ):
            _error(
                errors,
                "REGIME_DETECTOR_LEDGER_IDENTITY_INVALID",
                f"{symbol}|{timeframe}",
            )
            continue
        seen.add(key)
        fit = fit_map.get(key)
        fit_payload = fit.get("payload") if type(fit) is dict else {}
        support = row.get("train_regime_support")
        if type(support) is not dict or support != fit_payload.get(
            "train_regime_support"
        ):
            _error(errors, "REGIME_TRAIN_SUPPORT_BOOK_SPLICE", f"{symbol}|{timeframe}")
            continue
        entries = getattr(native_universe, "entries", {}) or {}
        symbol_entries = entries.get(symbol) if type(entries) is dict else {}
        native_entry = (
            symbol_entries.get(timeframe) if type(symbol_entries) is dict else {}
        )
        errors.extend(
            _validate_detector_ledger_record(
                row.get("regime_detector_ledger_record"),
                symbol=symbol,
                timeframe=timeframe,
                fit=fit or {},
                activation_id=getattr(native_universe, "activation_id", None),
                activation_manifest_sha256=getattr(
                    native_universe, "activation_manifest_sha256", None
                ),
                native_entry=native_entry if type(native_entry) is dict else {},
            )
        )
        ledger_record = row.get("regime_detector_ledger_record")
        if type(ledger_record) is dict:
            detector = ledger_record.get("detector_record")
            if type(detector) is dict:
                bindings[f"{symbol}|{timeframe}"] = {
                    "regime_fit_receipt_sha256": str(
                        ledger_record.get("regime_fit_receipt_sha256") or ""
                    ),
                    "regime_detector_record_sha256": str(
                        detector.get("record_sha256") or ""
                    ),
                    "regime_detector_parameters_sha256": str(
                        detector.get("parameters_sha256") or ""
                    ),
                    "regime_detector_code_sha256": str(
                        detector.get("detector_code_sha256") or ""
                    ),
                    "regime_detector_ledger_record_sha256": str(
                        ledger_record.get("ledger_record_sha256") or ""
                    ),
                    "train_regime_support_receipt_sha256": str(
                        support.get("receipt_sha256") or ""
                    ),
                }
    required_groups = {
        (symbol, timeframe)
        for symbol in requested_symbols
        for timeframe in TIMEFRAMES
        if getattr(native_universe, "cell_status", lambda *_: "unbound")(
            symbol, timeframe
        )
        not in {"source_unavailable", "excluded_no_trade", "technical_error"}
    }
    if seen != required_groups:
        errors.append("REGIME_DETECTOR_LEDGER_ROSTER_INCOMPLETE")
    return bindings, _unique(errors)


def _causal_reverify(index_value: Any) -> tuple[Optional[dict[str, Any]], list[str]]:
    exact = {
        "schema_version",
        "historical_process_decision",
        "precommit_payload",
        "evidence_payload",
        "reconciliation_payload",
        "historical_book_receipt_sha256",
    }
    index, errors = _index({"causal_process": index_value}, "causal_process", exact)
    if index is None:
        return None, errors
    decision = index.get("historical_process_decision")
    if type(decision) is not dict:
        return None, errors + ["CAUSAL_PROCESS_DECISION_NOT_OBJECT"]
    try:
        from pm_causal_process import verify_historical_process_decision

        verified = verify_historical_process_decision(
            decision,
            index.get("precommit_payload"),
            index.get("evidence_payload"),
            index.get("reconciliation_payload"),
        )
        if verified is not True:
            errors.append("CAUSAL_PROCESS_DECISION_RAW_REVERIFY_FAILED")
        if decision.get("decision") != "OFFLINE_PROCESS_VALIDATION_PASS":
            errors.append("CAUSAL_PROCESS_DECISION_NOT_PASS")
    except Exception as exc:
        _error(
            errors, "CAUSAL_PROCESS_DECISION_RAW_REVERIFY_FAILED", type(exc).__name__
        )
    return decision, _unique(errors)


def _historical_book_reverify(
    index_value: Any,
) -> tuple[Optional[Any], dict[str, Any], list[str]]:
    exact = {"schema_version", "book_decision"}
    index, errors = _index({"historical_book": index_value}, "historical_book", exact)
    if index is None:
        return None, {}, errors
    decision, body, book_errors = _book_decision(index.get("book_decision"))
    return decision, body, _unique([*errors, *book_errors])


def _current_book_reverify(
    index_value: Any,
    *,
    causal_decision: Optional[Mapping[str, Any]],
) -> tuple[Optional[Any], dict[str, Any], Optional[dict[str, Any]], list[str]]:
    exact = {
        "schema_version",
        "accepted_book_generation_receipt",
        "current_book_decision",
        "historical_process_decision_sha256",
    }
    index, errors = _index(
        {"current_process_book": index_value}, "current_process_book", exact
    )
    if index is None:
        return None, {}, None, errors
    decision, body, book_errors = _book_decision(index.get("current_book_decision"))
    errors.extend(book_errors)
    if body and (
        body.get("valid") is not True
        or body.get("optimization_run_complete") is not True
    ):
        errors.append("CURRENT_BOOK_NOT_COMPLETE_VALID")
    accepted = index.get("accepted_book_generation_receipt")
    payload: Optional[dict[str, Any]] = None
    try:
        from pm_model_lifecycle import (
            CURRENT_PROCESS_BOOK_RECEIPT_TYPE,
            canonical_receipt_sha256,
            open_typed_receipt,
        )

        opened, _ = open_typed_receipt(
            accepted, expected_receipt_type=CURRENT_PROCESS_BOOK_RECEIPT_TYPE
        )
        payload = dict(opened)
        causal_sha = (
            canonical_receipt_sha256(dict(causal_decision))
            if type(causal_decision) is dict
            else None
        )
        if (
            causal_sha is None
            or index.get("historical_process_decision_sha256") != causal_sha
            or payload.get("historical_process_decision_sha256") != causal_sha
        ):
            errors.append("CURRENT_BOOK_CAUSAL_PROCESS_BINDING_MISMATCH")
        if decision is None or payload.get(
            "book_multiplicity_receipt_sha256"
        ) != getattr(decision, "receipt_sha256", None):
            errors.append("CURRENT_BOOK_MULTIPLICITY_RECEIPT_MISMATCH")
        winners = sum(
            type(cell) is dict and cell.get("historical_state") == "WINNER"
            for cell in body.get("cells", [])
        )
        if payload.get("selected_winner_count") != winners:
            errors.append("CURRENT_BOOK_SELECTED_WINNER_COUNT_MISMATCH")
        if payload.get("precommitted_cell_count") != len(body.get("cells", [])):
            errors.append("CURRENT_BOOK_PRECOMMITTED_ROSTER_MISMATCH")
        if payload.get("full_cell_outcome_roster_sha256") != body.get(
            "book_generation_sha256"
        ):
            errors.append("CURRENT_BOOK_FULL_OUTCOME_ROSTER_BINDING_MISMATCH")
    except Exception as exc:
        _error(errors, "CURRENT_BOOK_RECEIPT_RAW_REVERIFY_FAILED", type(exc).__name__)
    return decision, body, payload, _unique(errors)


def _current_refit_reverify(
    index_value: Any,
    *,
    root: Path,
    causal_decision: Optional[Mapping[str, Any]],
    accepted_book_receipt: Any,
    current_cutoff: Any,
    raw_path_override: Optional[Path] = None,
    artifact_root_override: Optional[Path] = None,
) -> tuple[Optional[Mapping[str, Any]], Optional[dict[str, Any]], list[str]]:
    exact = {
        "schema_version",
        "current_refit_receipt",
        "raw_manifest_relative_path",
        "artifact_root_relative_path",
    }
    index, errors = _index(
        {"current_refit_execution": index_value}, "current_refit_execution", exact
    )
    if index is None:
        return None, None, errors
    receipt = index.get("current_refit_receipt")
    raw_manifest: Optional[Mapping[str, Any]] = None
    try:
        raw_path = raw_path_override or _relative_path(
            root, index.get("raw_manifest_relative_path"), "CURRENT_REFIT_RAW_MANIFEST"
        )
        artifact_root = artifact_root_override or _relative_path(
            root,
            index.get("artifact_root_relative_path"),
            "CURRENT_REFIT_ARTIFACT_ROOT",
            directory=True,
        )
        raw_manifest, _, _, _ = _snapshot(
            raw_path,
            16 * 1024 * 1024,
            root=(raw_path.parent if raw_path_override is not None else root),
        )
        from pm_current_refit_execution import verify_raw_current_refit_execution

        verification = verify_raw_current_refit_execution(
            receipt, raw_manifest, artifact_root=artifact_root
        )
        if (
            type(verification) is not dict
            or verification.get("verified") is not True
            or verification.get("errors") != []
        ):
            reported = (
                verification.get("errors")
                if type(verification) is dict
                and type(verification.get("errors")) is list
                else ["CURRENT_REFIT_EXECUTION_REVERIFIER_REQUIRED"]
            )
            errors.extend(str(value) for value in reported)
    except Exception as exc:
        _error(
            errors, "CURRENT_REFIT_EXECUTION_RAW_REVERIFY_FAILED", type(exc).__name__
        )
    try:
        from pm_model_lifecycle import build_process_validated_current_refit_lineage

        lineage = build_process_validated_current_refit_lineage(
            historical_process_decision=causal_decision,
            accepted_book_generation_receipt=accepted_book_receipt,
            current_refit_receipt=receipt,
            as_of_utc=current_cutoff,
        )
        if (
            type(lineage) is not dict
            or lineage.get("contract_status") != "PROBATION_ONLY"
            or lineage.get("probation_candidate_eligible") is not True
            or lineage.get("contract_errors")
        ):
            errors.append("CURRENT_REFIT_LINEAGE_RAW_REVERIFY_FAILED")
    except Exception as exc:
        lineage = None
        _error(errors, "CURRENT_REFIT_LINEAGE_RAW_REVERIFY_FAILED", type(exc).__name__)
    return receipt if type(receipt) is dict else None, lineage, _unique(errors)


def _management_refit_roots(
    *,
    index_value: Any,
    captured_root: Path,
    state: Mapping[str, Any],
) -> tuple[Optional[Path], Optional[Path], list[str]]:
    errors: list[str] = []
    try:
        if type(index_value) is not dict:
            raise _Invalid("MANAGEMENT_CURRENT_REFIT_INDEX_INVALID")
        raw_logical = _logical_relative(index_value.get("raw_manifest_relative_path"))
        raw_path = state["closure"].consume_file(
            "current_refit_artifact",
            raw_logical,
        )
        raw_manifest, _, _, _ = _snapshot(
            raw_path, 16 * 1024 * 1024, root=captured_root
        )
        descriptors = raw_manifest.get("artifact_descriptors")
        if type(descriptors) is not list or not descriptors:
            raise _Invalid("MANAGEMENT_CURRENT_REFIT_DESCRIPTOR_ROSTER_INVALID")
        logical_root = _logical_relative(
            index_value.get("artifact_root_relative_path"), allow_root=True
        )
        prefix = f"{logical_root}/" if logical_root else ""
        required: set[str] = {raw_logical}
        for descriptor in descriptors:
            if type(descriptor) is not dict:
                raise _Invalid("MANAGEMENT_CURRENT_REFIT_DESCRIPTOR_INVALID")
            relative = _logical_relative(descriptor.get("relative_path"))
            logical = f"{prefix}{relative}"
            if logical in required:
                raise _Invalid("MANAGEMENT_CURRENT_REFIT_DESCRIPTOR_DUPLICATE")
            required.add(logical)
            found = state["closure"].by_role_path.get(
                ("current_refit_artifact", logical)
            )
            if (
                found is None
                or found[1].get("size_bytes") != descriptor.get("size_bytes")
                or found[1].get("sha256") != descriptor.get("content_sha256")
            ):
                raise _Invalid("MANAGEMENT_CURRENT_REFIT_DESCRIPTOR_CLOSURE_MISMATCH")
        artifact_root = state["closure"].consume_directory(
            "current_refit_artifact",
            logical_root or ".",
            required_logicals=required,
        )
        return raw_path, artifact_root, []
    except Exception as exc:
        _error(errors, "MANAGEMENT_CURRENT_REFIT_CLOSURE_FAILED", type(exc).__name__)
        if isinstance(exc, _Invalid):
            errors.append(str(exc))
        return None, None, _unique(errors)


def _management_capital_root(
    *,
    index_value: Any,
    state: Mapping[str, Any],
) -> tuple[Optional[Path], list[str]]:
    errors: list[str] = []
    try:
        if type(index_value) is not dict:
            raise _Invalid("MANAGEMENT_CAPITAL_INDEX_INVALID")
        logical = _logical_relative(
            index_value.get("replay_evidence_root_relative_path"), allow_root=True
        )
        prefix = f"{logical}/" if logical else ""
        envelope = index_value.get("replay_evidence_receipt")
        body = envelope.get("body") if type(envelope) is dict else None
        if type(body) is not dict:
            raise _Invalid("MANAGEMENT_CAPITAL_REPLAY_RECEIPT_INVALID")
        required: set[str] = set()
        if body.get("storage_mode") == "CONTENT_ADDRESSED_ATTEMPT_SHARDS_V1":
            descriptors = body.get("attempt_shards")
            if type(descriptors) is not list or not 1 <= len(descriptors) <= 512:
                raise _Invalid("MANAGEMENT_CAPITAL_SHARD_ROSTER_INVALID")
            for index, descriptor in enumerate(descriptors):
                if type(descriptor) is not dict or set(descriptor) != {
                    "shard_index",
                    "relative_path",
                    "file_sha256",
                    "byte_size",
                    "attempt_count",
                    "daily_observation_count",
                    "first_attempt_id",
                    "last_attempt_id",
                }:
                    raise _Invalid("MANAGEMENT_CAPITAL_SHARD_DESCRIPTOR_INVALID")
                relative = _logical_relative(descriptor.get("relative_path"))
                digest = descriptor.get("file_sha256")
                size = descriptor.get("byte_size")
                if (
                    descriptor.get("shard_index") != index
                    or relative != f"attempt_shards/attempts_{index:04d}_{digest}.json"
                    or not _is_sha(digest)
                    or type(size) is not int
                    or size <= 0
                ):
                    raise _Invalid("MANAGEMENT_CAPITAL_SHARD_DESCRIPTOR_INVALID")
                full = f"{prefix}{relative}"
                required.add(full)
                found = state["closure"].by_role_path.get(
                    ("capital_replay_artifact", full)
                )
                if (
                    found is None
                    or found[1].get("size_bytes") != size
                    or found[1].get("sha256") != digest
                ):
                    raise _Invalid("MANAGEMENT_CAPITAL_SHARD_CLOSURE_MISMATCH")
        elif "attempt_shards" in body:
            raise _Invalid("MANAGEMENT_CAPITAL_REPLAY_STORAGE_MODE_INVALID")
        root = state["closure"].consume_directory(
            "capital_replay_artifact",
            logical or ".",
            required_logicals=required,
        )
        return root, []
    except Exception as exc:
        _error(errors, "MANAGEMENT_CAPITAL_CLOSURE_FAILED", type(exc).__name__)
        if isinstance(exc, _Invalid):
            errors.append(str(exc))
        return None, _unique(errors)


def _ledger_selected(ledger: Mapping[str, Any]) -> tuple[set[str], list[str]]:
    try:
        from pm_live_readiness import _sparse_current_refit_projection

        symbols = (
            ledger.get("symbols") if type(ledger.get("symbols")) is dict else ledger
        )
        requested = []
        if type(symbols) is dict:
            for raw_symbol, payload in symbols.items():
                symbol = (
                    payload.get("symbol")
                    if type(payload) is dict and type(payload.get("symbol")) is str
                    else raw_symbol
                )
                requested.append(str(symbol).strip().upper())
        return _sparse_current_refit_projection(ledger, requested)
    except Exception as exc:
        return set(), [f"LEDGER_CURRENT_REFIT_PROJECTION_FAILED:{type(exc).__name__}"]


def _selected_binding_errors(
    *,
    ledger: Mapping[str, Any],
    current_book_body: Mapping[str, Any],
    selected: set[str],
    partition: Mapping[str, set[str]],
    lineage: Optional[Mapping[str, Any]],
    detector_bindings: Mapping[str, Mapping[str, str]],
) -> list[str]:
    errors: list[str] = []
    if selected != partition.get("selected_cell_ids", set()):
        errors.append("PHYSICAL_LEDGER_CURRENT_BOOK_WINNER_ROSTER_MISMATCH")
    cells = lineage.get("current_refit_cells") if type(lineage) is dict else None
    if type(cells) is not list:
        return errors + ["CURRENT_REFIT_CELL_ROSTER_UNAVAILABLE"]
    refit_slots = {
        f"{cell.get('symbol')}|{cell.get('timeframe')}|{cell.get('regime')}"
        for cell in cells
        if type(cell) is dict
    }
    if refit_slots != selected:
        errors.append("CURRENT_REFIT_PHYSICAL_LEDGER_ROSTER_MISMATCH")
    refit_by_slot = {
        f"{cell.get('symbol')}|{cell.get('timeframe')}|{cell.get('regime')}": cell
        for cell in cells
        if type(cell) is dict
    }
    book_by_slot = {
        f"{cell.get('symbol')}|{cell.get('timeframe')}|{cell.get('regime')}": cell
        for cell in current_book_body.get("cells", [])
        if type(cell) is dict
    }
    ledger_root = (
        ledger.get("symbols")
        if type(ledger.get("symbols")) is dict
        else ledger.get("symbol_configs")
        if type(ledger.get("symbol_configs")) is dict
        else ledger
    )
    ledger_by_symbol = {}
    if type(ledger_root) is dict:
        for raw_symbol, payload in ledger_root.items():
            if type(payload) is dict:
                canonical = str(payload.get("symbol") or raw_symbol).strip().upper()
                ledger_by_symbol[canonical] = payload
    for slot in selected:
        symbol, timeframe, regime = slot.split("|", 2)
        binding = detector_bindings.get(f"{symbol}|{timeframe}")
        if (
            type(binding) is not dict
            or not _is_sha(binding.get("regime_fit_receipt_sha256"))
            or not _is_sha(binding.get("regime_detector_record_sha256"))
            or not _is_sha(binding.get("regime_detector_parameters_sha256"))
            or not _is_sha(binding.get("regime_detector_code_sha256"))
            or not _is_sha(binding.get("regime_detector_ledger_record_sha256"))
            or not _is_sha(binding.get("train_regime_support_receipt_sha256"))
        ):
            _error(errors, "SELECTED_CELL_REGIME_DETECTOR_AUTHORITY_MISSING", slot)
            continue
        refit = refit_by_slot.get(slot)
        if type(refit) is not dict:
            continue
        book_cell = book_by_slot.get(slot)
        if (
            type(book_cell) is not dict
            or book_cell.get("historical_state") != "WINNER"
            or book_cell.get("winner_decision_sha256")
            != refit.get("winner_decision_sha256")
            or book_cell.get("historical_book_entry_sha256")
            != refit.get("winner_book_entry_sha256")
        ):
            _error(errors, "CURRENT_REFIT_CURRENT_BOOK_WINNER_BINDING_MISMATCH", slot)
        refit_binding_fields = (
            "historical_process_decision_sha256",
            "accepted_book_generation_receipt_sha256",
            "current_refit_cell_id",
            "winner_decision_sha256",
            "winner_book_entry_sha256",
            "source_snapshot_sha256",
            "parameter_sha256",
            "output_artifact_sha256",
            "regime_fit_receipt_sha256",
            "regime_detector_record_sha256",
            "regime_detector_parameters_sha256",
            "regime_detector_code_sha256",
        )
        for name in refit_binding_fields[-4:]:
            if refit.get(name) != binding.get(name):
                _error(
                    errors,
                    "CURRENT_REFIT_REGIME_DETECTOR_BINDING_MISMATCH",
                    f"{slot}:{name}",
                )
        symbol_payload = ledger_by_symbol.get(symbol)
        grid = (
            symbol_payload.get("regime_configs")
            if type(symbol_payload) is dict
            else None
        )
        timeframe_payload = grid.get(timeframe) if type(grid) is dict else None
        cell = (
            timeframe_payload.get(regime) if type(timeframe_payload) is dict else None
        )
        artifact = cell.get("artifact_meta") if type(cell) is dict else None
        physical_binding = (
            artifact.get("current_refit_binding") if type(artifact) is dict else None
        )
        expected = {name: refit.get(name) for name in refit_binding_fields}
        if type(cell) is not dict or cell.get("strategy_name") != refit.get(
            "strategy_id"
        ):
            _error(errors, "PHYSICAL_LEDGER_CURRENT_REFIT_STRATEGY_MISMATCH", slot)
        if type(physical_binding) is not dict or physical_binding != expected:
            _error(errors, "PHYSICAL_LEDGER_CURRENT_REFIT_BINDING_MISMATCH", slot)
    return _unique(errors)


def _config_number(config: Any, name: str, default: float = float("nan")) -> float:
    try:
        value = float(getattr(config, name))
    except (AttributeError, TypeError, ValueError, OverflowError):
        return default
    return value if math.isfinite(value) else default


def _management_native_portfolio_contract(
    expected_lineage: Any,
    native_universe: Any,
    requested_symbols: Sequence[str],
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    if type(expected_lineage) is not dict:
        return {}, ["MANAGEMENT_PORTFOLIO_NATIVE_LINEAGE_INVALID"]
    bars: dict[str, str] = {}
    sidecars: dict[str, str] = {}
    entries = getattr(native_universe, "entries", {})
    for symbol in requested_symbols:
        symbol_entries = entries.get(symbol) if type(entries) is dict else None
        if type(symbol_entries) is not dict:
            errors.append(f"MANAGEMENT_PORTFOLIO_NATIVE_SYMBOL_MISSING:{symbol}")
            continue
        for timeframe in TIMEFRAMES:
            entry = symbol_entries.get(timeframe)
            status = getattr(native_universe, "cell_status", lambda *_: "unbound")(
                symbol, timeframe
            )
            if type(entry) is not dict:
                errors.append(
                    f"MANAGEMENT_PORTFOLIO_NATIVE_CELL_MISSING:{symbol}|{timeframe}"
                )
            elif status == "evidence_candidate":
                cell_id = f"{symbol}|{timeframe}"
                if not _is_sha(entry.get("bars_sha256")) or not _is_sha(
                    entry.get("sidecar_sha256")
                ):
                    errors.append(f"MANAGEMENT_PORTFOLIO_NATIVE_HASH_INVALID:{cell_id}")
                else:
                    bars[cell_id] = entry["bars_sha256"]
                    sidecars[cell_id] = entry["sidecar_sha256"]
    expected_bars = expected_lineage.get("native_bars_sha256_by_source")
    expected_sidecars = expected_lineage.get("native_sidecar_sha256_by_source")
    if expected_bars != dict(sorted(bars.items())):
        errors.append("MANAGEMENT_PORTFOLIO_NATIVE_BARS_ROSTER_MISMATCH")
    if expected_sidecars != dict(sorted(sidecars.items())):
        errors.append("MANAGEMENT_PORTFOLIO_NATIVE_SIDECAR_ROSTER_MISMATCH")
    roster_sha = expected_lineage.get("native_activation_roster_sha256")
    if not _is_sha(roster_sha):
        errors.append("MANAGEMENT_PORTFOLIO_NATIVE_ROSTER_HASH_INVALID")
    contract = {
        "activation_id": getattr(native_universe, "activation_id", None),
        "activation_manifest_sha256": getattr(
            native_universe, "activation_manifest_sha256", None
        ),
        "activation_roster_sha256": roster_sha,
        "bars_sha256_by_source": dict(sorted(bars.items())),
        "sidecar_sha256_by_source": dict(sorted(sidecars.items())),
    }
    return contract, _unique(errors)


def _replay_reverify(
    index_value: Any,
    *,
    ledger: Mapping[str, Any],
    requested_symbols: Sequence[str],
    current_book_decision: Any,
    causal_index: Optional[Mapping[str, Any]],
    accepted_book_receipt: Any,
    current_refit_receipt: Any,
    current_refit_index: Optional[Mapping[str, Any]],
    current_cutoff: Any,
    root: Path,
    config: Any,
    config_payload: Mapping[str, Any],
    data_dir: Path,
    raw_path_override: Optional[Path] = None,
    artifact_root_override: Optional[Path] = None,
    captured_native_universe: Any = None,
) -> tuple[Optional[Mapping[str, Any]], Optional[Mapping[str, Any]], list[str]]:
    exact = {
        "schema_version",
        "evidence",
        "expected_lineage",
        "expected_policies",
        "validation_policy",
    }
    index, errors = _index(
        {"synchronized_portfolio_replay": index_value},
        "synchronized_portfolio_replay",
        exact,
    )
    if index is None:
        return None, None, errors
    if causal_index is None or current_refit_index is None:
        return None, None, errors + ["PORTFOLIO_REPLAY_UPSTREAM_AUTHORITY_MISSING"]
    try:
        from pm_portfolio_replay import (
            active_native_portfolio_contract,
            build_current_refit_replay_roster,
            portfolio_cost_contract,
            portfolio_policy_from_config_payload,
            validate_funded_portfolio_replay_evidence,
        )

        raw_path = raw_path_override or _relative_path(
            root,
            current_refit_index.get("raw_manifest_relative_path"),
            "CURRENT_REFIT_RAW_MANIFEST",
        )
        raw_manifest, _, _, _ = _snapshot(
            raw_path,
            16 * 1024 * 1024,
            root=(raw_path.parent if raw_path_override is not None else root),
        )
        artifact_root = artifact_root_override or _relative_path(
            root,
            current_refit_index.get("artifact_root_relative_path"),
            "CURRENT_REFIT_ARTIFACT_ROOT",
            directory=True,
        )
        roster, roster_errors = build_current_refit_replay_roster(
            ledger,
            requested_symbols,
            current_book_decision=current_book_decision,
            book_evidence_family=None,
            historical_process_decision=causal_index["historical_process_decision"],
            causal_precommit_payload=causal_index["precommit_payload"],
            causal_evidence_payload=causal_index["evidence_payload"],
            causal_reconciliation_payload=causal_index["reconciliation_payload"],
            accepted_book_generation_receipt=accepted_book_receipt,
            current_refit_receipt=current_refit_receipt,
            as_of_utc=current_cutoff,
            current_refit_raw_manifest=raw_manifest,
            current_refit_artifact_root=artifact_root,
        )
        errors.extend(str(value) for value in roster_errors)
        if captured_native_universe is None:
            native_contract, native_errors = active_native_portfolio_contract(
                data_dir, requested_symbols, timeframes=TIMEFRAMES
            )
        else:
            native_contract, native_errors = _management_native_portfolio_contract(
                index.get("expected_lineage"),
                captured_native_universe,
                requested_symbols,
            )
        errors.extend(str(value) for value in native_errors)
        expected_lineage = index.get("expected_lineage")
        if type(expected_lineage) is not dict:
            expected_lineage = {}
            errors.append("PORTFOLIO_REPLAY_EXPECTED_LINEAGE_INVALID")
        derived_lineage = {
            "native_activation_id": native_contract.get("activation_id"),
            "native_activation_manifest_sha256": native_contract.get(
                "activation_manifest_sha256"
            ),
            "native_activation_roster_sha256": native_contract.get(
                "activation_roster_sha256"
            ),
            "native_bars_sha256_by_source": native_contract.get(
                "bars_sha256_by_source"
            ),
            "native_sidecar_sha256_by_source": native_contract.get(
                "sidecar_sha256_by_source"
            ),
            "physical_ledger_sha256": roster.get("physical_ledger_sha256"),
        }
        current_lineage = roster.get("current_refit_lineage")
        if type(current_lineage) is dict:
            derived_lineage.update(current_lineage)
        for name, value in derived_lineage.items():
            if expected_lineage.get(name) != value:
                _error(errors, "PORTFOLIO_REPLAY_LINEAGE_MISMATCH", name)
        raw_policies = index.get("expected_policies")
        expected_policies: dict[float, Mapping[str, Any]] = {}
        if type(raw_policies) is not dict or not raw_policies:
            errors.append("PORTFOLIO_REPLAY_POLICY_ROSTER_INVALID")
        else:
            for key, policy in raw_policies.items():
                try:
                    balance = float(key)
                    if (
                        not math.isfinite(balance)
                        or balance <= 0.0
                        or balance in expected_policies
                    ):
                        raise ValueError
                    derived = asdict(
                        portfolio_policy_from_config_payload(config_payload, balance)
                    )
                    if type(policy) is not dict or policy != derived:
                        _error(errors, "PORTFOLIO_REPLAY_POLICY_CONFIG_MISMATCH", key)
                    expected_policies[balance] = policy
                except Exception:
                    _error(errors, "PORTFOLIO_REPLAY_POLICY_INVALID", key)
        policy = index.get("validation_policy")
        policy_fields = {
            "minimum_calendar_days",
            "minimum_accepted_positions",
            "minimum_profit_factor",
            "minimum_return_pct",
            "maximum_drawdown_pct",
            "require_mtm",
            "maximum_bootstrap_ruin_upper_95pct",
            "maximum_bootstrap_p95_drawdown_pct",
            "minimum_bootstrap_daily_return_observations",
        }
        if type(policy) is not dict or set(policy) != policy_fields:
            errors.append("PORTFOLIO_REPLAY_VALIDATION_POLICY_INVALID")
            policy = {}
        config_policy = {
            "minimum_calendar_days": _config_number(
                config, "production_portfolio_replay_min_calendar_days"
            ),
            "minimum_accepted_positions": int(
                _config_number(config, "production_portfolio_replay_min_positions", -1)
            ),
            "minimum_profit_factor": _config_number(
                config, "production_portfolio_replay_min_profit_factor"
            ),
            "minimum_return_pct": _config_number(
                config, "production_portfolio_replay_min_return_pct"
            ),
            "maximum_drawdown_pct": _config_number(
                config, "production_portfolio_replay_max_drawdown_pct"
            ),
            "require_mtm": bool(
                getattr(config, "production_portfolio_replay_require_mtm", False)
            ),
        }
        for name, expected in config_policy.items():
            if policy.get(name) != expected:
                _error(
                    errors, "PORTFOLIO_REPLAY_VALIDATION_POLICY_CONFIG_MISMATCH", name
                )
        evidence_errors = validate_funded_portfolio_replay_evidence(
            index.get("evidence"),
            expected_lineage=expected_lineage,
            expected_roster=roster,
            expected_cost_contract=portfolio_cost_contract(config_payload),
            expected_policies=expected_policies,
            minimum_calendar_days=policy.get("minimum_calendar_days", float("nan")),
            minimum_accepted_positions=policy.get("minimum_accepted_positions", -1),
            minimum_profit_factor=policy.get("minimum_profit_factor", float("nan")),
            minimum_return_pct=policy.get("minimum_return_pct", float("nan")),
            maximum_drawdown_pct=policy.get("maximum_drawdown_pct", float("nan")),
            require_mtm=policy.get("require_mtm") is True,
            maximum_bootstrap_ruin_upper_95pct=policy.get(
                "maximum_bootstrap_ruin_upper_95pct", float("nan")
            ),
            maximum_bootstrap_p95_drawdown_pct=policy.get(
                "maximum_bootstrap_p95_drawdown_pct", float("nan")
            ),
            minimum_bootstrap_daily_return_observations=policy.get(
                "minimum_bootstrap_daily_return_observations", float("nan")
            ),
        )
        errors.extend(str(value) for value in evidence_errors)
    except Exception as exc:
        roster = None
        _error(errors, "PORTFOLIO_REPLAY_RAW_REVERIFY_FAILED", type(exc).__name__)
    return index.get("evidence"), roster, _unique(errors)


def _decode_keys(value: Any, label: str) -> tuple[dict[str, bytes], list[str]]:
    if type(value) is not dict:
        return {}, [f"{label}_KEY_ROSTER_INVALID"]
    decoded: dict[str, bytes] = {}
    errors: list[str] = []
    for key_id, encoded in value.items():
        try:
            if type(key_id) is not str or not key_id or type(encoded) is not str:
                raise ValueError
            raw = base64.b64decode(encoded, validate=True)
            if len(raw) != 32:
                raise ValueError
            decoded[key_id] = raw
        except Exception:
            _error(errors, f"{label}_PUBLIC_KEY_INVALID", key_id)
    return decoded, _unique(errors)


def _capital_reverify(
    index_value: Any,
    *,
    root: Path,
    config: Any,
    validation_time_utc: Any,
    require_funded_authorization: bool,
    current_lineage: Optional[Mapping[str, Any]],
    replay_evidence: Optional[Mapping[str, Any]],
    replay_root_override: Optional[Path] = None,
) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]], list[str]]:
    exact = {
        "schema_version",
        "artifact",
        "expected_lineage",
        "constraints",
        "expected_selected_policy_point_id",
        "replay_evidence_root_relative_path",
        "trusted_authority_public_keys",
        "trusted_replay_public_keys",
        "trusted_precommitment_public_keys",
        "precommitment_receipt",
        "replay_evidence_receipt",
    }
    index, errors = _index(
        {"capital_calibration": index_value}, "capital_calibration", exact
    )
    if index is None:
        return None, None, errors
    artifact = index.get("artifact")
    expected_lineage = index.get("expected_lineage")
    if type(artifact) is not dict or type(expected_lineage) is not dict:
        return None, None, errors + ["CAPITAL_CALIBRATION_INDEX_PAYLOAD_INVALID"]
    try:
        from pm_cap_calibration import (
            CapCalibrationConstraints,
            canonical_json_sha256,
            validate_cap_calibration_artifact,
        )

        constraints_raw = index.get("constraints")
        if type(constraints_raw) is not dict:
            raise ValueError("constraints")
        constraints = CapCalibrationConstraints(**constraints_raw)
        constraints.validate()
        authority_keys, key_errors = _decode_keys(
            index.get("trusted_authority_public_keys"), "CAP_AUTHORITY"
        )
        replay_keys, replay_key_errors = _decode_keys(
            index.get("trusted_replay_public_keys"), "CAP_REPLAY"
        )
        precommit_keys, precommit_key_errors = _decode_keys(
            index.get("trusted_precommitment_public_keys"), "CAP_PRECOMMITMENT"
        )
        errors.extend([*key_errors, *replay_key_errors, *precommit_key_errors])
        replay_root = replay_root_override or _relative_path(
            root,
            index.get("replay_evidence_root_relative_path"),
            "CAPITAL_REPLAY_EVIDENCE_ROOT",
            directory=True,
        )
        if type(current_lineage) is dict:
            known = {
                "book_generation_receipt_sha256": current_lineage.get(
                    "accepted_book_generation_receipt_sha256"
                ),
                "refit_receipt_sha256": current_lineage.get(
                    "current_refit_receipt_sha256"
                ),
                "refit_execution_sha256": current_lineage.get(
                    "current_refit_execution_sha256"
                ),
                "output_book_sha256": current_lineage.get(
                    "current_refit_output_book_sha256"
                ),
                "winner_roster_sha256": current_lineage.get(
                    "current_selected_winner_roster_sha256"
                ),
            }
            for name, expected in known.items():
                if expected_lineage.get(name) != expected:
                    _error(errors, "CAPITAL_CURRENT_REFIT_LINEAGE_MISMATCH", name)
        if type(replay_evidence) is dict:
            replay_sha = canonical_json_sha256(replay_evidence)
            if expected_lineage.get("replay_output_sha256") != replay_sha:
                errors.append("CAPITAL_REPLAY_OUTPUT_BINDING_MISMATCH")
        calibration_lineage = dict(expected_lineage)
        calibration_lineage.pop("replay_output_sha256", None)
        # The signed selected_policy_point_id plus the signature / precommitment
        # / replay chain already bind which precommitted candidate was selected
        # (cap_calibration validates that selection against the signed roster).
        # The static config cap must NOT veto an intentionally precommitted
        # selection such as 33.25/50/75/90 (seq1644/1684); passing None here
        # keeps the signed selected point authoritative instead of forcing it to
        # coincide with max_total_open_risk_pct.
        common = dict(
            artifact=artifact,
            expected_lineage=calibration_lineage,
            expected_selected_cap_pct=None,
            expected_selected_policy_point_id=index.get(
                "expected_selected_policy_point_id"
            ),
            expected_constraints=constraints,
            trusted_authority_public_keys=authority_keys,
            replay_evidence_root=replay_root,
            trusted_replay_public_keys=replay_keys,
            validation_time_utc=validation_time_utc,
            precommitment_receipt=index.get("precommitment_receipt"),
            replay_evidence_receipt=index.get("replay_evidence_receipt"),
            trusted_precommitment_public_keys=precommit_keys,
        )
        cap_errors = validate_cap_calibration_artifact(
            **common,
            require_external_authorization=False,
        )
        if require_funded_authorization:
            cap_errors.extend(
                validate_cap_calibration_artifact(
                    **common,
                    require_external_authorization=True,
                )
            )
        errors.extend(str(value) for value in cap_errors)
        selection = artifact.get("selection")
        envelope = (
            selection.get("selected_capital_envelope")
            if type(selection) is dict
            else None
        )
        if type(envelope) is not dict:
            envelope = None
            errors.append("CAPITAL_SELECTED_ENVELOPE_MISSING")
    except Exception as exc:
        envelope = None
        _error(errors, "CAPITAL_CALIBRATION_RAW_REVERIFY_FAILED", type(exc).__name__)
    return artifact, envelope, _unique(errors)


def _capital_config_errors(envelope: Any) -> list[str]:
    # Post-selection, the signed roster member and its seven-axis envelope are
    # admission truth: cap_calibration binds the selected point to the signed
    # precommitted candidate roster, and static config only constrained roster
    # CONSTRUCTION upstream.  Re-checking the envelope against static risk fields
    # here would be a hidden post-selection veto against an intentionally
    # precommitted candidate (e.g. 33.25/50/75/90), which seq1644/1684 forbid.
    # So this validates the envelope's schema and its OWN internal coherence
    # only, never equality with static config.
    required = {
        "max_total_stop_risk_pct",
        "max_factor_stop_risk_pct",
        "max_same_symbol_stop_risk_pct",
        "max_margin_utilization_pct",
        "min_margin_level_pct",
        "min_free_margin_pct",
        "gap_stress_multiple",
    }
    if type(envelope) is not dict or set(envelope) != required:
        return ["CAPITAL_SELECTED_ENVELOPE_SCHEMA_INVALID"]
    errors: list[str] = []
    for name in sorted(required):
        value = envelope.get(name)
        if (
            type(value) not in {int, float}
            or type(value) is bool
            or not math.isfinite(float(value))
            or float(value) <= 0.0
        ):
            _error(errors, "CAPITAL_SELECTED_ENVELOPE_AXIS_INVALID", name)
    if errors:
        return _unique(errors)
    total = float(envelope["max_total_stop_risk_pct"])
    factor = float(envelope["max_factor_stop_risk_pct"])
    same_symbol = float(envelope["max_same_symbol_stop_risk_pct"])
    if not 0.0 < same_symbol <= factor <= total < 100.0:
        errors.append("CAPITAL_SELECTED_ENVELOPE_HIERARCHY_INVALID")
    margin = float(envelope["max_margin_utilization_pct"])
    level = float(envelope["min_margin_level_pct"])
    free = float(envelope["min_free_margin_pct"])
    if not 0.0 < margin <= 100.0:
        errors.append("CAPITAL_SELECTED_ENVELOPE_MARGIN_UTILIZATION_INVALID")
    elif not math.isclose(margin, 10_000.0 / level, rel_tol=0.0, abs_tol=1e-9):
        errors.append("CAPITAL_SELECTED_ENVELOPE_MARGIN_LEVEL_MISMATCH")
    if not math.isclose(free, 100.0 - margin, rel_tol=0.0, abs_tol=1e-9):
        errors.append("CAPITAL_SELECTED_ENVELOPE_FREE_MARGIN_MISMATCH")
    if float(envelope["gap_stress_multiple"]) < 1.0:
        errors.append("CAPITAL_SELECTED_ENVELOPE_GAP_STRESS_INVALID")
    return _unique(errors)


def _capital_execution_errors(envelope: Any) -> list[str]:
    if type(envelope) is not dict:
        return ["CAPITAL_EXECUTION_ENVELOPE_UNAVAILABLE"]
    try:
        import inspect

        import pm_main
        import pm_risk_admission as risk

        if (
            pm_main.evaluate_gross_stop_risk is not risk.evaluate_gross_stop_risk
            or pm_main.evaluate_projected_margin is not risk.evaluate_projected_margin
        ):
            raise _Invalid("LIVE_RISK_FUNCTION_IDENTITY_MISMATCH")
        source = inspect.getsource(pm_main.LiveTrader._execute_entry)
        # SEAM 4 (seq1720): admission reads the bound selected envelope, so the
        # caller binding is proven by the accessor + the seven-axis field names,
        # NOT the retired static config tokens.
        required_tokens = (
            "_bound_capital_admission_envelope",
            "max_total_stop_risk_pct",
            "max_factor_stop_risk_pct",
            "max_same_symbol_stop_risk_pct",
            "min_margin_level_pct",
            "evaluate_gross_stop_risk",
            "evaluate_projected_margin",
        )
        forbidden_static_tokens = (
            "max_total_open_risk_pct",
            "max_factor_group_open_risk_pct",
        )
        if any(token not in source for token in required_tokens) or any(
            token in source for token in forbidden_static_tokens
        ):
            raise _Invalid("LIVE_RISK_CALLER_BINDING_MISSING")
        total = float(envelope["max_total_stop_risk_pct"])
        factor = float(envelope["max_factor_stop_risk_pct"])
        exposure = risk.StopRiskExposure("EURUSD", total, "fx", "open")
        candidate = risk.StopRiskExposure("USDJPY", 0.01, "fx", "candidate")
        decision = risk.evaluate_gross_stop_risk(
            [exposure],
            candidate,
            risk.GrossRiskLimits(total, factor),
        )
        if decision.allow or decision.code != "TOTAL_STOP_RISK_CAP":
            raise _Invalid("LIVE_TOTAL_STOP_RISK_CAP_NOT_ENFORCED")
        factor_decision = risk.evaluate_gross_stop_risk(
            [risk.StopRiskExposure("EURUSD", factor, "fx", "factor-open")],
            risk.StopRiskExposure("GBPUSD", 0.01, "fx", "factor-candidate"),
            risk.GrossRiskLimits(max(total, factor + 1.0), factor),
        )
        if (
            factor_decision.allow
            or factor_decision.code != "FACTOR_GROUP_STOP_RISK_CAP"
        ):
            raise _Invalid("LIVE_FACTOR_STOP_RISK_CAP_NOT_ENFORCED")
        # Same-symbol admission now reads the BOUND selected envelope, not
        # static config; bind it on a probe trader so the cap under test is the
        # envelope's own max_same_symbol_stop_risk_pct (seq1720).
        trader = object.__new__(pm_main.LiveTrader)
        trader._is_current_refit_demo = True
        trader._current_refit_demo_capital_envelope = dict(envelope)
        same_symbol_env = float(envelope["max_same_symbol_stop_risk_pct"])
        same_symbol_allowed, _ = trader._check_symbol_combined_risk_cap(
            "EURUSD",
            same_symbol_env + 0.01,
            "EURUSD",
            positions_snapshot=[],
            account_info=type("_Account", (), {"equity": 100.0})(),
        )
        if same_symbol_allowed:
            raise _Invalid("LIVE_SAME_SYMBOL_STOP_RISK_CAP_NOT_ENFORCED")
        # A candidate strictly UNDER the selected cap must be admitted - proving
        # the selected limit is honoured, not a hidden static pre-veto.
        under_allowed, _ = trader._check_symbol_combined_risk_cap(
            "EURUSD",
            max(same_symbol_env - 0.01, same_symbol_env / 2.0),
            "EURUSD",
            positions_snapshot=[],
            account_info=type("_Account", (), {"equity": 100.0})(),
        )
        if not under_allowed:
            raise _Invalid("LIVE_SAME_SYMBOL_SELECTED_LIMIT_UNDER_VETOED")
        margin = risk.evaluate_projected_margin(
            equity=100.0,
            current_used_margin=float(envelope["max_margin_utilization_pct"]),
            candidate_required_margin=0.01,
            max_margin_utilization_pct=float(envelope["max_margin_utilization_pct"]),
        )
        if margin.allow or margin.code != "PROJECTED_MARGIN_UTILIZATION_CAP":
            raise _Invalid("LIVE_MARGIN_CAP_NOT_ENFORCED")
        return []
    except Exception as exc:
        return [f"CAPITAL_EXECUTION_ENFORCEMENT_REVERIFY_FAILED:{type(exc).__name__}"]


def _empty_result(
    requested_symbols: Sequence[str],
    *,
    ledger_sha256: Optional[str] = None,
    activation_id: Optional[str] = None,
    activation_sha256: Optional[str] = None,
) -> dict[str, Any]:
    possible = sorted(
        f"{symbol}|{timeframe}|{regime}"
        for symbol in requested_symbols
        for timeframe in TIMEFRAMES
        for regime in REGIMES
    )
    return {
        "schema_version": RESULT_SCHEMA,
        "status": "BLOCKED",
        "errors": [],
        "raw_reverification": {name: False for name in RAW_COMPONENTS},
        "selected_cell_ids": [],
        "possible_slot_count": len(possible),
        "source_unavailable_cell_ids": [],
        "regime_unobserved_cell_ids": [],
        "no_local_winner_cell_ids": [],
        "insufficient_evidence_cell_ids": [],
        "technical_cell_ids": [],
        "unsearched_cell_ids": [],
        "missing_observed_cell_ids": possible,
        "native_activation_id": activation_id,
        "native_activation_manifest_sha256": activation_sha256,
        "ledger_sha256": ledger_sha256,
        "selected_capital_envelope": {},
        "capital_config_matches": False,
        "capital_execution_enforcement_reverified": False,
        "funded_live_authorized": False,
    }


def verify_live_readiness_authority_chain(
    *,
    manifest: Mapping[str, Any],
    manifest_path: Path,
    authority_index_paths: Mapping[str, Path],
    ledger: Mapping[str, Any],
    ledger_path: Path,
    config: Any,
    config_payload: Mapping[str, Any],
    config_path: Path,
    requested_symbols: Sequence[str],
    native_universe: Any,
    validation_time_utc: Any,
    require_funded_authorization: bool,
    management_only_context: Any = None,
) -> dict[str, Any]:
    """Recompute every authority fact from bounded raw files."""
    errors: list[str] = []
    if type(require_funded_authorization) is not bool:
        raise TypeError("require_funded_authorization must be bool")
    if (
        not isinstance(requested_symbols, (tuple, list))
        or not 1 <= len(requested_symbols) <= MAX_SYMBOLS
        or any(not _valid_requested_symbol(symbol) for symbol in requested_symbols)
        or len(set(requested_symbols)) != len(requested_symbols)
        or len({symbol.casefold() for symbol in requested_symbols})
        != len(requested_symbols)
    ):
        raise ValueError("REQUESTED_SYMBOL_ROSTER_INVALID")
    symbols = tuple(requested_symbols)
    management_requested = management_only_context is not None
    management_state: Optional[dict[str, Any]] = None
    result = _empty_result(
        symbols,
        activation_id=getattr(native_universe, "activation_id", None),
        activation_sha256=getattr(native_universe, "activation_manifest_sha256", None),
    )
    try:
        validation_time = (
            validation_time_utc
            if type(validation_time_utc) is datetime
            else datetime.fromisoformat(str(validation_time_utc).replace("Z", "+00:00"))
        )
        if validation_time.tzinfo is None or validation_time.utcoffset() is None:
            raise ValueError
        validation_time = validation_time.astimezone(timezone.utc)
    except Exception as exc:
        _error(errors, "AUTHORITY_VALIDATION_TIME_INVALID", type(exc).__name__)
        validation_time = datetime.now(timezone.utc)

    manifest_value: Any = None
    root: Optional[Path] = None
    manifest_sha: Optional[str] = None
    try:
        manifest_path = Path(manifest_path)
        root = Path(os.path.abspath(manifest_path)).parent
        manifest_value, manifest_sha, _, manifest_path = _snapshot(
            manifest_path, MAX_MANIFEST_BYTES, root=root
        )
        if type(manifest_value) is not dict or manifest_value != dict(manifest):
            errors.append("AUTHORITY_MANIFEST_CALLER_SNAPSHOT_MISMATCH")
        if manifest_value.get("schema_version") != GENERATION_SCHEMA:
            errors.append("AUTHORITY_MANIFEST_SCHEMA_INVALID")
        if str(manifest_value.get("generation_status") or "").upper() != "PROMOTED":
            errors.append("AUTHORITY_GENERATION_NOT_PROMOTED")
        if manifest_value.get("technical_failures"):
            errors.append("AUTHORITY_GENERATION_TECHNICAL_FAILURES_PRESENT")
    except Exception as exc:
        _error(errors, "AUTHORITY_MANIFEST_RAW_READ_FAILED", type(exc).__name__)

    ledger_value: Any = None
    ledger_sha: Optional[str] = None
    if root is not None:
        try:
            ledger_value, ledger_sha, _, resolved_ledger = _snapshot(
                Path(ledger_path), MAX_LEDGER_BYTES, root=root
            )
            if type(ledger_value) is not dict or ledger_value != dict(ledger):
                errors.append("AUTHORITY_LEDGER_CALLER_SNAPSHOT_MISMATCH")
            if str(manifest_value.get("ledger_sha256") or "").lower() != ledger_sha:
                errors.append("AUTHORITY_LEDGER_MANIFEST_SHA256_MISMATCH")
            if resolved_ledger != Path(ledger_path).resolve(strict=True):
                errors.append("AUTHORITY_LEDGER_PATH_MISMATCH")
        except Exception as exc:
            _error(errors, "AUTHORITY_LEDGER_RAW_READ_FAILED", type(exc).__name__)
    result["ledger_sha256"] = ledger_sha

    indexes: dict[str, Any] = {}
    index_hashes: dict[str, str] = {}
    if root is not None and type(manifest_value) is dict:
        indexes, index_hashes, index_errors = _load_indexes(
            manifest=manifest_value,
            manifest_path=manifest_path,
            supplied_paths=authority_index_paths,
        )
        errors.extend(index_errors)

    receipt = (
        manifest_value.get("authority_chain_receipt")
        if type(manifest_value) is dict
        else None
    )
    receipt_valid = True
    config_value: Any = None
    config_sha: Optional[str] = None
    if type(receipt) is not dict or set(receipt) != _CHAIN_FIELDS:
        errors.append("AUTHORITY_CHAIN_RECEIPT_FIELDS_INVALID")
        receipt_valid = False
    else:
        body = dict(receipt)
        receipt_sha = body.pop("receipt_sha256", None)
        if (
            receipt.get("schema_version") != CHAIN_RECEIPT_SCHEMA
            or not _is_sha(receipt_sha)
            or _canonical_sha(body) != receipt_sha
        ):
            errors.append("AUTHORITY_CHAIN_RECEIPT_HASH_OR_SCHEMA_INVALID")
            receipt_valid = False
        expected_scope = (
            "FUNDED_LIVE" if require_funded_authorization else "DEMO_PROBATION"
        )
        if receipt.get("authorization_scope") != expected_scope:
            errors.append("AUTHORITY_CHAIN_RECEIPT_SCOPE_MISMATCH")
            receipt_valid = False
        if (
            receipt.get("generation_id") != manifest_value.get("generation_id")
            or receipt.get("ledger_sha256") != ledger_sha
            or receipt.get("requested_symbols") != list(symbols)
            or receipt.get("native_activation_id")
            != getattr(native_universe, "activation_id", None)
            or receipt.get("native_activation_manifest_sha256")
            != getattr(native_universe, "activation_manifest_sha256", None)
        ):
            errors.append("AUTHORITY_CHAIN_RECEIPT_GENERATION_BINDING_MISMATCH")
            receipt_valid = False
        declared_hashes = receipt.get("authority_artifact_sha256_by_name")
        if type(declared_hashes) is not dict or declared_hashes != index_hashes:
            errors.append("AUTHORITY_CHAIN_RECEIPT_ARTIFACT_BINDING_MISMATCH")
            receipt_valid = False
        if root is not None:
            try:
                expected_config = _relative_path(
                    root, receipt.get("config_relative_path"), "AUTHORITY_CONFIG"
                )
                if expected_config != Path(config_path).resolve(strict=True):
                    raise _Invalid("CONFIG_CALLER_PATH_MISMATCH")
                config_value, config_sha, _, _ = _snapshot(
                    expected_config, MAX_CONFIG_BYTES, root=root
                )
                if type(config_value) is not dict or config_value != dict(
                    config_payload
                ):
                    errors.append("AUTHORITY_CONFIG_CALLER_SNAPSHOT_MISMATCH")
                    receipt_valid = False
                if receipt.get("config_sha256") != config_sha:
                    errors.append("AUTHORITY_CONFIG_SHA256_MISMATCH")
                    receipt_valid = False
            except Exception as exc:
                _error(errors, "AUTHORITY_CONFIG_RAW_READ_FAILED", type(exc).__name__)
                receipt_valid = False

    management_errors: list[str] = []
    if management_requested:
        management_state, management_errors = _management_context(
            management_only_context,
            manifest_path=Path(manifest_path),
            requested_symbols=symbols,
        )
        if require_funded_authorization:
            management_errors.append("MANAGEMENT_FUNDED_SCOPE_FORBIDDEN")
        if (
            management_state is not None
            and type(manifest_value) is dict
            and management_state["value"].get("generation_id")
            != manifest_value.get("generation_id")
        ):
            management_errors.append("MANAGEMENT_GENERATION_BINDING_MISMATCH")
        errors.extend(management_errors)

    chain_ok = (
        receipt_valid
        and type(manifest_value) is dict
        and type(ledger_value) is dict
        and type(config_value) is dict
        and len(indexes) == len(ARTIFACTS)
        and (not management_requested or management_state is not None)
        and not management_errors
    )

    selected, selected_errors = (
        (set(), ["LEDGER_NOT_RAW_REVERIFIED"])
        if type(ledger_value) is not dict
        else __import__("pm_live_readiness")._sparse_current_refit_projection(
            ledger_value, symbols
        )
    )
    errors.extend(selected_errors)

    if management_requested:
        if management_state is None:
            verified_native, native_errors = (
                None,
                ["MANAGEMENT_NATIVE_CONTEXT_UNAVAILABLE"],
            )
        else:
            verified_native, native_errors = _management_native_reverify(
                state=management_state,
                requested_symbols=symbols,
                supplied_universe=native_universe,
            )
    else:
        verified_native, native_errors = _native_reverify(
            config=config,
            config_path=Path(config_path),
            requested_symbols=symbols,
            supplied_universe=native_universe,
        )
    if management_state is not None:
        expected_groups = {"|".join(cell_id.split("|")[:2]) for cell_id in selected}
        if set(management_state["native_bindings"]) != expected_groups:
            native_errors.append("MANAGEMENT_NATIVE_SELECTED_ROSTER_MISMATCH")
        if management_state["capsule"].get("selected_cell_ids") != sorted(selected):
            native_errors.append("MANAGEMENT_CAPSULE_SELECTED_ROSTER_MISMATCH")
    errors.extend(native_errors)
    result["raw_reverification"]["native_data_state"] = chain_ok and not native_errors
    if verified_native is not None:
        result["native_activation_id"] = verified_native.activation_id
        result["native_activation_manifest_sha256"] = (
            verified_native.activation_manifest_sha256
        )

    historical_decision, historical_body, historical_errors = _historical_book_reverify(
        indexes.get("historical_book")
    )
    if historical_body and (
        historical_body.get("valid") is not True
        or historical_body.get("optimization_run_complete") is not True
    ):
        historical_errors.append("HISTORICAL_BOOK_NOT_COMPLETE_VALID")
    errors.extend(historical_errors)

    causal_index = (
        indexes.get("causal_process")
        if type(indexes.get("causal_process")) is dict
        else None
    )
    causal_decision, causal_errors = _causal_reverify(indexes.get("causal_process"))
    if causal_index is not None and historical_decision is not None:
        if causal_index.get("historical_book_receipt_sha256") != getattr(
            historical_decision, "receipt_sha256", None
        ):
            causal_errors.append("CAUSAL_HISTORICAL_BOOK_BINDING_MISMATCH")
    errors.extend(causal_errors)

    current_decision, current_body, accepted_payload, current_errors = (
        _current_book_reverify(
            indexes.get("current_process_book"), causal_decision=causal_decision
        )
    )
    current_index = (
        indexes.get("current_process_book")
        if type(indexes.get("current_process_book")) is dict
        else {}
    )
    accepted_receipt = current_index.get("accepted_book_generation_receipt")
    errors.extend(current_errors)
    if current_decision is not None:
        partition, partition_errors = _book_partition(current_body, symbols)
        errors.extend(partition_errors)
    else:
        partition = {
            name: set(result[name])
            for name in (
                "selected_cell_ids",
                "source_unavailable_cell_ids",
                "regime_unobserved_cell_ids",
                "no_local_winner_cell_ids",
                "insufficient_evidence_cell_ids",
                "technical_cell_ids",
                "unsearched_cell_ids",
                "missing_observed_cell_ids",
            )
        }

    detector_bindings, regime_errors = (
        _regime_support_reverify(
            index_value=indexes.get("regime_support"),
            current_book_body=current_body,
            current_book_receipt=getattr(current_decision, "receipt_sha256", None),
            requested_symbols=symbols,
            native_universe=verified_native,
        )
        if verified_native is not None
        else ({}, ["REGIME_SUPPORT_NATIVE_AUTHORITY_MISSING"])
    )
    errors.extend(regime_errors)

    refit_index = (
        indexes.get("current_refit_execution")
        if type(indexes.get("current_refit_execution")) is dict
        else None
    )
    current_cutoff = (
        accepted_payload.get("current_cutoff_utc") if accepted_payload else None
    )
    refit_raw_override: Optional[Path] = None
    refit_root_override: Optional[Path] = None
    refit_closure_errors: list[str] = []
    if management_state is not None:
        refit_raw_override, refit_root_override, refit_closure_errors = (
            _management_refit_roots(
                index_value=indexes.get("current_refit_execution"),
                captured_root=root or Path.cwd(),
                state=management_state,
            )
        )
    if management_requested and (
        management_state is None
        or refit_raw_override is None
        or refit_root_override is None
    ):
        refit_receipt, current_lineage, refit_errors = (
            None,
            None,
            [
                *refit_closure_errors,
                "MANAGEMENT_CURRENT_REFIT_RAW_CLOSURE_UNAVAILABLE",
            ],
        )
    else:
        refit_receipt, current_lineage, refit_errors = _current_refit_reverify(
            indexes.get("current_refit_execution"),
            root=root or Path.cwd(),
            causal_decision=causal_decision,
            accepted_book_receipt=accepted_receipt,
            current_cutoff=current_cutoff,
            raw_path_override=refit_raw_override,
            artifact_root_override=refit_root_override,
        )
    errors.extend(refit_errors)
    binding_errors = _selected_binding_errors(
        ledger=ledger_value if type(ledger_value) is dict else {},
        current_book_body=current_body,
        selected=selected,
        partition=partition,
        lineage=current_lineage,
        detector_bindings=detector_bindings,
    )
    errors.extend(binding_errors)

    if verified_native is not None:
        native_unavailable = {
            f"{symbol}|{timeframe}|{regime}"
            for symbol in symbols
            for timeframe in TIMEFRAMES
            if verified_native.cell_status(symbol, timeframe) == "source_unavailable"
            for regime in REGIMES
        }
        if partition.get("source_unavailable_cell_ids", set()) != native_unavailable:
            errors.append("CURRENT_BOOK_NATIVE_SOURCE_AVAILABILITY_MISMATCH")
        native_technical = {
            f"{symbol}|{timeframe}|{regime}"
            for symbol in symbols
            for timeframe in TIMEFRAMES
            if verified_native.cell_status(symbol, timeframe) == "technical_error"
            for regime in REGIMES
        }
        if partition.get("technical_cell_ids", set()) != native_technical:
            errors.append("CURRENT_BOOK_NATIVE_TECHNICAL_AVAILABILITY_MISMATCH")

    config_data_dir = Path(getattr(config, "data_dir", "data"))
    if not config_data_dir.is_absolute():
        config_data_dir = Path(config_path).resolve().parent / config_data_dir
    if management_requested and (
        management_state is None
        or refit_raw_override is None
        or refit_root_override is None
    ):
        replay_evidence, replay_roster, replay_errors = (
            None,
            None,
            ["MANAGEMENT_PORTFOLIO_REPLAY_RAW_CLOSURE_UNAVAILABLE"],
        )
    else:
        replay_evidence, replay_roster, replay_errors = _replay_reverify(
            indexes.get("synchronized_portfolio_replay"),
            ledger=ledger_value if type(ledger_value) is dict else {},
            requested_symbols=symbols,
            current_book_decision=current_decision,
            causal_index=causal_index,
            accepted_book_receipt=accepted_receipt,
            current_refit_receipt=refit_receipt,
            current_refit_index=refit_index,
            current_cutoff=current_cutoff,
            root=root or Path.cwd(),
            config=config,
            config_payload=(
                config_value if type(config_value) is dict else config_payload
            ),
            data_dir=config_data_dir,
            raw_path_override=refit_raw_override,
            artifact_root_override=refit_root_override,
            captured_native_universe=(
                verified_native if management_requested else None
            ),
        )
    errors.extend(replay_errors)

    capital_root_override: Optional[Path] = None
    capital_closure_errors: list[str] = []
    if management_state is not None:
        capital_root_override, capital_closure_errors = _management_capital_root(
            index_value=indexes.get("capital_calibration"), state=management_state
        )
    if management_requested and (
        management_state is None or capital_root_override is None
    ):
        envelope, capital_errors = (
            None,
            [
                *capital_closure_errors,
                "MANAGEMENT_CAPITAL_RAW_CLOSURE_UNAVAILABLE",
            ],
        )
    else:
        _, envelope, capital_errors = _capital_reverify(
            indexes.get("capital_calibration"),
            root=root or Path.cwd(),
            config=config,
            validation_time_utc=validation_time,
            require_funded_authorization=require_funded_authorization,
            current_lineage=current_lineage,
            replay_evidence=replay_evidence,
            replay_root_override=capital_root_override,
        )
    errors.extend(capital_errors)
    config_capital_errors = _capital_config_errors(envelope)
    execution_capital_errors = _capital_execution_errors(envelope)
    errors.extend(config_capital_errors)
    errors.extend(execution_capital_errors)

    management_integrity_errors: list[str] = []
    if management_state is not None:
        try:
            management_state["closure"].require_complete()
            management_state["closure"].verify_all()
        except Exception as exc:
            _error(
                management_integrity_errors,
                "MANAGEMENT_EXTERNAL_CLOSURE_FINAL_REVERIFY_FAILED",
                type(exc).__name__,
            )
            if isinstance(exc, _Invalid):
                management_integrity_errors.append(str(exc))
        errors.extend(management_integrity_errors)
        if management_integrity_errors:
            chain_ok = False

    component_errors = {
        "historical_book": historical_errors,
        "causal_process": causal_errors,
        "current_process_book": [
            *current_errors,
            *(
                []
                if type(current_lineage) is dict
                else ["CURRENT_PROCESS_LINEAGE_INVALID"]
            ),
        ],
        "regime_support": regime_errors,
        "current_refit_execution": refit_errors,
        "synchronized_portfolio_replay": replay_errors,
        "capital_calibration": capital_errors,
    }
    for name, failures in component_errors.items():
        result["raw_reverification"][name] = chain_ok and not failures
    if management_integrity_errors:
        result["raw_reverification"]["native_data_state"] = False

    for name, values in partition.items():
        result[name] = sorted(values)
    result["selected_capital_envelope"] = dict(envelope or {})
    result["capital_config_matches"] = (
        not config_capital_errors and envelope is not None
    )
    result["capital_execution_enforcement_reverified"] = (
        not execution_capital_errors and envelope is not None
    )
    final_errors = _unique(errors)
    if not final_errors and all(result["raw_reverification"].values()):
        if management_requested:
            result["status"] = MANAGEMENT_STATUS
            result["funded_live_authorized"] = False
        elif require_funded_authorization:
            result["status"] = "FUNDED_AUTHORIZED"
            result["funded_live_authorized"] = True
        else:
            result["status"] = "DEMO_PROBATION_AUTHORIZED"
            result["funded_live_authorized"] = False
    result["errors"] = final_errors
    if set(result) != _RESULT_FIELDS:
        raise RuntimeError("AUTHORITY_RESULT_INTERNAL_SCHEMA_INVALID")
    return result


def _pipeline_config(config_payload: Mapping[str, Any], config_path: Path) -> Any:
    from pm_core import PipelineConfig

    pipeline = config_payload.get("pipeline")
    if type(pipeline) is not dict:
        raise _Invalid("CONFIG_PIPELINE_NOT_OBJECT")
    allowed = {item.name for item in fields(PipelineConfig)}
    config = PipelineConfig(
        **{key: value for key, value in pipeline.items() if key in allowed}
    )
    setattr(config, "_deployment_config_path", str(config_path))
    return config


def verify_optimization_authority_chain(
    *,
    manifest: Mapping[str, Any],
    outcome: Any,
    ledger_path: Path,
) -> list[str]:
    """Reverify a promoted generation; a receipt alone never grants authority."""
    errors: list[str] = []
    try:
        manifest_path = Path(str(getattr(outcome, "manifest_path", "") or ""))
        root = Path(os.path.abspath(manifest_path)).parent
        observed_manifest, _, _, manifest_path = _snapshot(
            manifest_path, MAX_MANIFEST_BYTES, root=root
        )
        if type(observed_manifest) is not dict or observed_manifest != dict(manifest):
            return ["OPTIMIZATION_AUTHORITY_MANIFEST_SNAPSHOT_MISMATCH"]
        receipt = observed_manifest.get("authority_chain_receipt")
        if type(receipt) is not dict or set(receipt) != _CHAIN_FIELDS:
            return ["OPTIMIZATION_AUTHORITY_CHAIN_RECEIPT_INVALID"]
        config_path = _relative_path(
            root, receipt.get("config_relative_path"), "OPTIMIZATION_CONFIG"
        )
        config_payload, config_sha, _, _ = _snapshot(
            config_path, MAX_CONFIG_BYTES, root=root
        )
        if type(config_payload) is not dict or config_sha != receipt.get(
            "config_sha256"
        ):
            return ["OPTIMIZATION_AUTHORITY_CONFIG_SNAPSHOT_INVALID"]
        ledger, ledger_sha, _, resolved_ledger = _snapshot(
            Path(ledger_path), MAX_LEDGER_BYTES, root=root
        )
        if type(ledger) is not dict or ledger_sha != receipt.get("ledger_sha256"):
            return ["OPTIMIZATION_AUTHORITY_LEDGER_SNAPSHOT_INVALID"]
        config = _pipeline_config(config_payload, config_path)
        requested = receipt.get("requested_symbols")
        if type(requested) is not list:
            return ["OPTIMIZATION_AUTHORITY_REQUESTED_SYMBOLS_INVALID"]
        from pm_native_activation import resolve_active_native_universe

        data_dir = Path(getattr(config, "data_dir", "data"))
        if not data_dir.is_absolute():
            data_dir = config_path.parent / data_dir
        universe = resolve_active_native_universe(
            data_dir,
            requested,
            required_timeframes=TIMEFRAMES,
            production_contract_required=True,
        )
        descriptors = observed_manifest.get("authority_artifacts")
        if type(descriptors) is not dict or set(descriptors) != set(ARTIFACTS):
            return ["OPTIMIZATION_AUTHORITY_ARTIFACT_ROSTER_INVALID"]
        paths = {
            name: _relative_path(
                root,
                descriptors[name].get("relative_path")
                if type(descriptors.get(name)) is dict
                else None,
                f"OPTIMIZATION_{name}",
            )
            for name in ARTIFACTS
        }
        require_funded = receipt.get("authorization_scope") == "FUNDED_LIVE"
        result = verify_live_readiness_authority_chain(
            manifest=observed_manifest,
            manifest_path=manifest_path,
            authority_index_paths=paths,
            ledger=ledger,
            ledger_path=resolved_ledger,
            config=config,
            config_payload=config_payload,
            config_path=config_path,
            requested_symbols=tuple(requested),
            native_universe=universe,
            validation_time_utc=datetime.now(timezone.utc),
            require_funded_authorization=require_funded,
        )
        errors.extend(result.get("errors") or [])
        expected_status = (
            "FUNDED_AUTHORIZED" if require_funded else "DEMO_PROBATION_AUTHORIZED"
        )
        if result.get("status") != expected_status:
            errors.append("OPTIMIZATION_AUTHORITY_CHAIN_NOT_AUTHORIZED")
        if str(getattr(outcome, "generation_id", "") or "") != str(
            observed_manifest.get("generation_id") or ""
        ):
            errors.append("OPTIMIZATION_AUTHORITY_OUTCOME_GENERATION_MISMATCH")
    except Exception as exc:
        _error(
            errors, "OPTIMIZATION_AUTHORITY_CHAIN_REVERIFY_FAILED", type(exc).__name__
        )
    return _unique(errors)


__all__ = [
    "ARTIFACTS",
    "CHAIN_RECEIPT_SCHEMA",
    "INDEX_SCHEMAS",
    "RAW_COMPONENTS",
    "RESULT_SCHEMA",
    "verify_live_readiness_authority_chain",
    "verify_optimization_authority_chain",
]
