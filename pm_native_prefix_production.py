"""Canonical, bounded cutoff prefixes from activated broker-native history.

This module is the production bridge between the immutable native activation
and ``pm_training_horizon``.  It never resamples data and never accepts caller
supplied source digests.  An AVAILABLE product is the byte-for-byte CSV prefix
of one independently activated native leaf containing every, and only, bar
whose authenticated close is at or before the explicit cutoff.

The persisted index and every subordinate object are content addressed in the
shared authority spool.  Verification reopens the source activation,
acquisition provenance, prefix CSV, segment ledger, and all descriptors from
bytes.  The returned horizon projection grants no search, promotion, capital,
or trading authority.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Final, Iterator, Mapping, Sequence

import numpy as np
import pandas as pd

import pm_authority_spool as authority_spool
from pm_native_activation import (
    ACTIVATION_MANIFEST_SCHEMA,
    NATIVE_BARS_SCHEMA,
    NATIVE_EVIDENCE_CANDIDATE,
    ActiveNativeResolution,
    resolve_active_native_artifact,
)
from pm_native_request_contract import (
    ACQUISITION_CONTRACT_VERSION,
    COMMON_REQUEST_KEYS,
    PRODUCTION_ARTIFACT_COLUMNS,
    PRODUCTION_TIMEFRAME_ORDER,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    TIMEFRAME_SECONDS,
    NativeRequestContractError,
    canonical_source_identity,
    validate_authenticated_available_history_boundary,
)
from pm_native_csv_validation import NativeCsvValidationError, scan_native_csv_stream


UTC: Final = timezone.utc

PREFIX_INDEX_SCHEMA: Final = "fxpm.activated-native-prefix-index.v1"
PREFIX_SCHEMA_VERSION: Final = "fxpm.activated-native-cutoff-prefix.v1"
PREFIX_CONTINUITY_POLICY_SCHEMA: Final = (
    "fxpm.activated-native-prefix-continuity-policy.v1"
)
PREFIX_CONTINUITY_RECEIPT_SCHEMA: Final = (
    "fxpm.activated-native-prefix-continuity-receipt.v1"
)
PREFIX_TERMINAL_RECEIPT_SCHEMA: Final = (
    "fxpm.activated-native-prefix-terminal-receipt.v1"
)
PREFIX_CUTOFF_TERMINAL_RECEIPT_SCHEMA: Final = (
    "fxpm.activated-native-prefix-cutoff-terminal-receipt.v1"
)
PREFIX_AUTHORITY_SCOPE: Final = (
    "NATIVE_CUTOFF_PREFIX_MECHANICAL_EVIDENCE_ONLY_NO_TRADING_AUTHORITY"
)
# Keep this byte-exact with ``pm_core.NATIVE_SOURCE_BINDING_ATTR`` without
# importing pm_core (which would create an authority-layer import cycle).  The
# value is attached only after the activated index and prefix bytes have been
# replayed below; Backtester uses it to require the matching authenticated
# broker-terms overlay for financing costs.
NATIVE_SOURCE_BINDING_ATTR: Final = "_fxpm_native_source_binding_v1"

AVAILABLE: Final = "AVAILABLE"
SOURCE_UNAVAILABLE: Final = "SOURCE_UNAVAILABLE"
SOURCE_UNAVAILABLE_AT_CUTOFF: Final = "SOURCE_UNAVAILABLE_AT_CUTOFF"
TECHNICAL_FAILURE: Final = "TECHNICAL_FAILURE"
SOURCE_STATES: Final = frozenset((AVAILABLE, SOURCE_UNAVAILABLE, TECHNICAL_FAILURE))

CSV_HEADER: Final[tuple[str, ...]] = ("time", *PRODUCTION_ARTIFACT_COLUMNS)
MAX_SOURCE_BYTES: Final = 64 * 1024**3
MAX_SOURCE_ROWS: Final = 50_000_000
MAX_PROVENANCE_BYTES: Final = 16 * 1024**2
MAX_ACTIVATION_BYTES: Final = 256 * 1024**2
MAX_AUDIT_SIDECAR_BYTES: Final = 64 * 1024**2
MAX_INDEX_BYTES: Final = 4 * 1024**2
MAX_JSON_NODES: Final = 300_000
MAX_JSON_DEPTH: Final = 48
MAX_STRING_BYTES: Final = 4 * 1024**2
MAX_LINE_BYTES: Final = 16 * 1024
IO_CHUNK_BYTES: Final = 1024 * 1024
MAX_OPEN_FRAME_ROWS: Final = 10_000_000
MAX_OPEN_FRAME_BYTES: Final = 4 * 1024**3
_PRODUCT_SEAL: Final = object()

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_SYMBOL_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$", re.ASCII)
_UTC_RE: Final = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
    re.ASCII,
)

_DESCRIPTOR_KEYS: Final = frozenset(
    {"schema_version", "relative_path", "sha256", "size_bytes", "format"}
)
_INDEX_KEYS: Final = frozenset(
    {
        "schema_version",
        "authority_scope",
        "source_state",
        "source_reason",
        "symbol",
        "timeframe",
        "cutoff_utc",
        "source_accessed_at_utc",
        "activation",
        "source",
        "prefix",
        "continuity",
        "terminal_receipt",
        "grants_search_authority",
        "grants_trading_authority",
    }
)
_ACTIVATION_KEYS: Final = frozenset(
    {
        "activation_id",
        "manifest_relative_path",
        "manifest_sha256",
        "entry_sha256",
        "run_manifest_relative_path",
        "run_manifest_sha256",
    }
)
_SOURCE_KEYS: Final = frozenset(
    {
        "bars_relative_path",
        "bars_sha256",
        "bars_bytes",
        "bars_rows",
        "audit_sidecar_relative_path",
        "audit_sidecar_sha256",
        "audit_sidecar_bytes",
        "provenance_relative_path",
        "provenance_sha256",
        "provenance_bytes",
        "request_map_sha256",
        "source_identity_sha256",
        "source_identity",
        "instrument_snapshot",
        "request_start_utc",
        "request_end_exclusive_utc",
    }
)
_PREFIX_KEYS: Final = frozenset(
    {
        "schema",
        "content",
        "segments",
        "prefix_rows",
        "prefix_start_utc",
        "prefix_end_exclusive_utc",
        "prefix_first_open_utc",
        "prefix_last_open_utc",
        "prefix_last_close_utc",
    }
)
_CONTINUITY_KEYS: Final = frozenset(
    {
        "policy",
        "receipt",
        "geometry_sha256",
        "internal_status",
        "boundary_status",
        "unexplained_discontinuity_count",
        "segment_count",
        "gap_count",
        "requires_preserved_segment_ledger",
    }
)
_INSTRUMENT_SNAPSHOT_KEYS: Final = frozenset(
    {
        "asset_class",
        "digits",
        "parity_tick_tolerance",
        "path",
        "point",
        "status",
        "symbol",
        "trade_contract_size",
        "trade_tick_size",
        "trade_tick_value",
    }
)


class NativePrefixError(ValueError):
    """Deterministic fail-closed native-prefix error."""

    def __init__(self, code: str) -> None:
        self.code = str(code)
        super().__init__(self.code)


@dataclass(frozen=True)
class NativePrefixProduct:
    """A byte-verified content-addressed prefix product."""

    source_state: str
    index: Mapping[str, Any]
    index_descriptor: Mapping[str, Any]
    index_sha256: str
    native_prefix: Mapping[str, Any]
    instrument_snapshot: Mapping[str, Any] | None
    prefix_path: Path | None
    prefix_descriptor: Mapping[str, Any] | None
    artifact_descriptors: tuple[Mapping[str, Any], ...]
    _seal: object = field(repr=False, compare=False)
    _data_dir: Path | None = field(default=None, repr=False, compare=False)
    _production_contract_required: bool = field(
        default=True,
        repr=False,
        compare=False,
    )


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class _SourceBinding:
    activation_id: str
    activation_manifest_relative_path: str
    activation_manifest_sha256: str
    activation_entry_sha256: str
    run_manifest_relative_path: str
    run_manifest_sha256: str
    entry: Mapping[str, Any]
    bars_relative_path: str
    bars_path: Path
    bars_sha256: str
    bars_bytes: int
    bars_rows: int
    audit_sidecar_relative_path: str
    audit_sidecar_sha256: str
    audit_sidecar_bytes: int
    provenance_relative_path: str
    provenance_path: Path
    provenance_sha256: str
    provenance_bytes: int
    request_map_sha256: str
    source_identity_sha256: str
    source_identity: Mapping[str, Any]
    instrument_snapshot: Mapping[str, Any]
    request_start_utc: str
    request_end_exclusive_utc: str
    provenance: Mapping[str, Any]
    run_manifest: Mapping[str, Any]
    run_cell: Mapping[str, Any]


@dataclass(frozen=True)
class _FirstSourceBar:
    opened: datetime
    closed: datetime


@dataclass(frozen=True)
class _AvailableHistoryBoundary:
    proof: Mapping[str, Any]
    proof_sha256: str
    first_open_utc: str
    first_close_utc: str


@dataclass(frozen=True)
class _PrefixFacts:
    source_sha256: str
    source_bytes: int
    source_rows: int
    prefix_rows: int
    first_open_utc: str
    last_open_utc: str
    last_close_utc: str
    segment_count: int
    gap_count: int
    segment_sha256: str
    segment_bytes: int


def _fail(code: str) -> None:
    raise NativePrefixError(code)


def _canonical_bytes(value: Any) -> bytes:
    _bounded_json(value)
    try:
        raw = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise NativePrefixError("PREFIX_JSON_CANONICALIZATION_FAILED") from exc
    if not raw or len(raw) > MAX_INDEX_BYTES:
        _fail("PREFIX_JSON_SIZE_BOUND_EXCEEDED")
    return raw


def _sha_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _bounded_json(value: Any) -> None:
    work: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while work:
        item, depth = work.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            _fail("PREFIX_JSON_STRUCTURE_UNBOUNDED")
        if type(item) is str:
            if len(item.encode("utf-8")) > MAX_STRING_BYTES:
                _fail("PREFIX_JSON_STRING_UNBOUNDED")
        elif type(item) is float:
            if not math.isfinite(item):
                _fail("PREFIX_JSON_NONFINITE")
        elif type(item) is int:
            if item.bit_length() > 4096:
                _fail("PREFIX_JSON_INTEGER_UNBOUNDED")
        elif type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    _fail("PREFIX_JSON_KEY_INVALID")
                work.append((key, depth + 1))
                work.append((child, depth + 1))
        elif type(item) in {list, tuple}:
            work.extend((child, depth + 1) for child in item)
        elif item is not None and type(item) is not bool:
            _fail("PREFIX_JSON_VALUE_INVALID")


def _duplicate_rejector(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail(f"PREFIX_JSON_DUPLICATE_KEY:{key}")
        result[key] = value
    return result


def _strict_json(raw: bytes, *, maximum_bytes: int, canonical: bool) -> dict[str, Any]:
    if not raw or len(raw) > maximum_bytes:
        _fail("PREFIX_JSON_SIZE_BOUND_EXCEEDED")
    try:
        parsed = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_duplicate_rejector,
            parse_constant=lambda value: _fail(f"PREFIX_JSON_CONSTANT:{value}"),
        )
    except NativePrefixError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise NativePrefixError("PREFIX_JSON_INVALID") from exc
    _bounded_json(parsed)
    if type(parsed) is not dict:
        _fail("PREFIX_JSON_OBJECT_REQUIRED")
    if canonical and _canonical_bytes(parsed) != raw:
        _fail("PREFIX_JSON_NOT_CANONICAL")
    return parsed


def _is_sha(value: Any) -> bool:
    return type(value) is str and _SHA256_RE.fullmatch(value) is not None


def _digest(value: Any, code: str) -> str:
    if not _is_sha(value):
        _fail(code)
    return str(value)


def _symbol(value: Any) -> str:
    if type(value) is not str or _SYMBOL_RE.fullmatch(value) is None:
        _fail("PREFIX_SYMBOL_INVALID")
    return value


def _timeframe(value: Any) -> str:
    if type(value) is not str or value not in PRODUCTION_TIMEFRAME_ORDER:
        _fail("PREFIX_TIMEFRAME_INVALID")
    return value


def _parse_utc(value: Any, code: str) -> datetime:
    if type(value) is not str or _UTC_RE.fullmatch(value) is None:
        _fail(code)
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise NativePrefixError(code) from exc


def _csv_utc(value: Any, code: str) -> datetime:
    if type(value) is not str or len(value) > 64:
        _fail(code)
    normalized = value.strip().replace(" ", "T")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise NativePrefixError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        _fail(code)
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        _fail("PREFIX_TIMESTAMP_NAIVE")
    normalized = value.astimezone(UTC)
    if normalized.microsecond:
        _fail("PREFIX_TIMESTAMP_SUBSECOND")
    return normalized.strftime("%Y-%m-%dT%H:%M:%SZ")


def _identity(info: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=int(info.st_dev),
        inode=int(info.st_ino),
        size=int(info.st_size),
        mtime_ns=int(info.st_mtime_ns),
    )


def _is_reparse(path: Path, info: os.stat_result | None = None) -> bool:
    current = info or path.lstat()
    attributes = int(getattr(current, "st_file_attributes", 0) or 0)
    return path.is_symlink() or bool(attributes & 0x400)


def _safe_relative(root: Path, value: Any, code: str) -> tuple[str, Path]:
    if type(value) is not str or not value or "\\" in value or "\x00" in value:
        _fail(code)
    relative = PurePosixPath(value)
    if (
        relative.is_absolute()
        or str(relative) != value
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        _fail(code)
    path = root.joinpath(*relative.parts)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise NativePrefixError(code) from exc
    current = root
    for part in relative.parts:
        current = current / part
        if current.exists() or current.is_symlink():
            try:
                info = current.lstat()
            except OSError as exc:
                raise NativePrefixError(code) from exc
            if _is_reparse(current, info):
                _fail(code)
    return value, path


def _open_regular(
    path: Path, *, maximum_bytes: int, code: str
) -> tuple[BinaryIO, _FileIdentity]:
    try:
        before = path.lstat()
        if _is_reparse(path, before) or not stat.S_ISREG(before.st_mode):
            _fail(f"{code}_NOT_SAFE_REGULAR")
        if not 0 < int(before.st_size) <= maximum_bytes:
            _fail(f"{code}_SIZE_INVALID")
        flags = (
            os.O_RDONLY
            | int(getattr(os, "O_BINARY", 0))
            | int(getattr(os, "O_NOFOLLOW", 0))
        )
        descriptor = os.open(path, flags)
        opened = os.fdopen(descriptor, "rb", buffering=IO_CHUNK_BYTES)
        observed = os.fstat(opened.fileno())
    except NativePrefixError:
        raise
    except OSError as exc:
        raise NativePrefixError(f"{code}_OPEN_FAILED") from exc
    if not stat.S_ISREG(observed.st_mode) or _identity(observed) != _identity(before):
        opened.close()
        _fail(f"{code}_IDENTITY_CHANGED")
    return opened, _identity(observed)


def _verify_open_identity(
    path: Path, handle: BinaryIO, original: _FileIdentity, code: str
) -> None:
    try:
        handle_identity = _identity(os.fstat(handle.fileno()))
        path_identity = _identity(path.lstat())
    except OSError as exc:
        raise NativePrefixError(code) from exc
    if handle_identity != original or path_identity != original:
        _fail(code)


def _read_regular(path: Path, *, maximum_bytes: int, code: str) -> tuple[bytes, str]:
    handle, identity = _open_regular(path, maximum_bytes=maximum_bytes, code=code)
    try:
        # Read only the already-fstat-bounded object size.  Asking BufferedReader
        # for a transport ceiling such as 64 GiB can itself request a 64 GiB
        # allocation even when the object contains only a few bytes.
        raw = handle.read(identity.size + 1)
        if len(raw) != identity.size or len(raw) > maximum_bytes:
            _fail(f"{code}_SIZE_CHANGED")
        _verify_open_identity(path, handle, identity, f"{code}_IDENTITY_CHANGED")
    finally:
        handle.close()
    return raw, hashlib.sha256(raw).hexdigest()


def _descriptor(value: Any, *, expected_format: str | None = None) -> dict[str, Any]:
    if type(value) is not dict or set(value) != _DESCRIPTOR_KEYS:
        _fail("PREFIX_DESCRIPTOR_FIELDS_INVALID")
    if value.get("schema_version") != authority_spool.ARTIFACT_DESCRIPTOR_SCHEMA:
        _fail("PREFIX_DESCRIPTOR_SCHEMA_INVALID")
    digest = _digest(value.get("sha256"), "PREFIX_DESCRIPTOR_SHA256_INVALID")
    size = value.get("size_bytes")
    if type(size) is not int or not 0 < size <= MAX_SOURCE_BYTES:
        _fail("PREFIX_DESCRIPTOR_SIZE_INVALID")
    format_name = value.get("format")
    if type(format_name) is not str or not re.fullmatch(
        r"[A-Za-z0-9._+-]+", format_name
    ):
        _fail("PREFIX_DESCRIPTOR_FORMAT_INVALID")
    if expected_format is not None and format_name != expected_format:
        _fail("PREFIX_DESCRIPTOR_FORMAT_MISMATCH")
    relative = value.get("relative_path")
    expected_relative = f"objects/sha256/{digest[:2]}/{digest}.{format_name}"
    if relative != expected_relative:
        _fail("PREFIX_DESCRIPTOR_CONTENT_ADDRESS_INVALID")
    return dict(value)


def _read_spool_descriptor(
    context: authority_spool.GenerationSpoolContext,
    descriptor: Any,
    *,
    expected_format: str,
    collect: bool,
) -> tuple[dict[str, Any], bytes | None]:
    normalized = _descriptor(descriptor, expected_format=expected_format)
    if normalized["size_bytes"] > context.limits.max_artifact_bytes:
        _fail("PREFIX_DESCRIPTOR_EXCEEDS_SPOOL_BOUND")
    _, path = _safe_relative(
        Path(context.root), normalized["relative_path"], "PREFIX_SPOOL_PATH_INVALID"
    )
    handle, identity = _open_regular(
        path,
        maximum_bytes=context.limits.max_artifact_bytes,
        code="PREFIX_SPOOL_OBJECT",
    )
    digest = hashlib.sha256()
    chunks: list[bytes] | None = [] if collect else None
    total = 0
    try:
        while True:
            chunk = handle.read(IO_CHUNK_BYTES)
            if not chunk:
                break
            total += len(chunk)
            if total > context.limits.max_artifact_bytes:
                _fail("PREFIX_SPOOL_OBJECT_SIZE_EXCEEDED")
            digest.update(chunk)
            if chunks is not None:
                chunks.append(chunk)
        _verify_open_identity(path, handle, identity, "PREFIX_SPOOL_OBJECT_CHANGED")
    finally:
        handle.close()
    if total != normalized["size_bytes"] or digest.hexdigest() != normalized["sha256"]:
        _fail("PREFIX_SPOOL_OBJECT_INTEGRITY_MISMATCH")
    return normalized, b"".join(chunks) if chunks is not None else None


def _source_schema_payload() -> dict[str, Any]:
    return {
        "schema_version": PREFIX_SCHEMA_VERSION,
        "csv_header": list(CSV_HEADER),
        "timeframes": list(PRODUCTION_TIMEFRAME_ORDER),
        "timeframe_seconds": {
            value: TIMEFRAME_SECONDS[value] for value in PRODUCTION_TIMEFRAME_ORDER
        },
        "selection_rule": "include_iff_authenticated_bar_close_utc_lte_cutoff_utc",
        "row_rule": "byte_identical_ordered_prefix_of_one_activated_native_leaf",
        "segment_rule": "new_segment_iff_current_open_utc_gt_previous_close_utc",
        "overlap_rule": "current_open_utc_lt_previous_close_utc_is_technical_failure",
        "timestamp_rule": "utc_second_precision_half_open_bars",
        "resampling_allowed": False,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }


def _manifest_binding(
    data_dir: Path,
    resolution: ActiveNativeResolution,
    symbol: str,
    timeframe: str,
) -> tuple[Path, str, dict[str, Any], dict[str, Any], str, str]:
    native_root = (Path(data_dir) / "native").resolve()
    path = resolution.activation_manifest_path
    if path is None:
        _fail("PREFIX_ACTIVATION_MANIFEST_UNBOUND")
    try:
        relative = path.resolve(strict=False).relative_to(native_root).as_posix()
    except (OSError, ValueError) as exc:
        raise NativePrefixError("PREFIX_ACTIVATION_MANIFEST_PATH_INVALID") from exc
    _, safe_path = _safe_relative(
        native_root, relative, "PREFIX_ACTIVATION_MANIFEST_PATH_INVALID"
    )
    raw, digest = _read_regular(
        safe_path,
        maximum_bytes=MAX_ACTIVATION_BYTES,
        code="PREFIX_ACTIVATION_MANIFEST",
    )
    if resolution.activation_manifest_sha256 != digest:
        _fail("PREFIX_ACTIVATION_MANIFEST_RESOLUTION_MISMATCH")
    manifest = _strict_json(raw, maximum_bytes=MAX_ACTIVATION_BYTES, canonical=False)
    if manifest.get("schema_version") != ACTIVATION_MANIFEST_SCHEMA:
        _fail("PREFIX_ACTIVATION_MANIFEST_SCHEMA_INVALID")
    if str(manifest.get("status") or "").lower() != "active":
        _fail("PREFIX_ACTIVATION_MANIFEST_NOT_ACTIVE")
    activation_id = str(manifest.get("activation_id") or "")
    if not activation_id or activation_id != str(resolution.activation_id or ""):
        _fail("PREFIX_ACTIVATION_ID_MISMATCH")
    entries = manifest.get("entries")
    symbol_entries = entries.get(symbol) if isinstance(entries, Mapping) else None
    entry = (
        symbol_entries.get(timeframe) if isinstance(symbol_entries, Mapping) else None
    )
    if not isinstance(entry, Mapping):
        _fail("PREFIX_ACTIVATION_ENTRY_MISSING")
    entry_dict = dict(entry)
    if entry_dict.get("symbol") != symbol or entry_dict.get("timeframe") != timeframe:
        _fail("PREFIX_ACTIVATION_ENTRY_CELL_MISMATCH")
    entry_sha = _sha_json(entry_dict)
    return native_root, relative, manifest, entry_dict, digest, entry_sha


def _source_binding(
    data_dir: Path,
    resolution: ActiveNativeResolution,
    symbol: str,
    timeframe: str,
) -> _SourceBinding:
    native_root, manifest_relative, manifest, entry, manifest_sha, entry_sha = (
        _manifest_binding(data_dir, resolution, symbol, timeframe)
    )
    if str(entry.get("status") or "").lower() not in {
        "active",
        NATIVE_EVIDENCE_CANDIDATE,
    }:
        _fail("PREFIX_ACTIVATION_ENTRY_NOT_AVAILABLE")
    bars_relative, bars_path = _safe_relative(
        native_root, entry.get("bars_relative_path"), "PREFIX_SOURCE_BARS_PATH_INVALID"
    )
    bars_sha = _digest(entry.get("bars_sha256"), "PREFIX_SOURCE_BARS_SHA_INVALID")
    if (
        resolution.data_path is None
        or resolution.data_path.resolve() != bars_path.resolve()
    ):
        _fail("PREFIX_SOURCE_BARS_RESOLUTION_MISMATCH")
    bars_info = bars_path.lstat()
    if _is_reparse(bars_path, bars_info) or not stat.S_ISREG(bars_info.st_mode):
        _fail("PREFIX_SOURCE_BARS_NOT_REGULAR")
    bars_bytes = int(bars_info.st_size)
    if not 0 < bars_bytes <= MAX_SOURCE_BYTES:
        _fail("PREFIX_SOURCE_BARS_SIZE_INVALID")

    sidecar_relative, sidecar_path = _safe_relative(
        native_root,
        entry.get("sidecar_relative_path"),
        "PREFIX_AUDIT_SIDECAR_PATH_INVALID",
    )
    if (
        resolution.sidecar_path is None
        or resolution.sidecar_path.resolve() != sidecar_path.resolve()
    ):
        _fail("PREFIX_AUDIT_SIDECAR_RESOLUTION_MISMATCH")
    sidecar_raw, sidecar_sha = _read_regular(
        sidecar_path,
        maximum_bytes=MAX_AUDIT_SIDECAR_BYTES,
        code="PREFIX_AUDIT_SIDECAR",
    )
    if sidecar_sha != _digest(
        entry.get("sidecar_sha256"), "PREFIX_AUDIT_SIDECAR_SHA_INVALID"
    ):
        _fail("PREFIX_AUDIT_SIDECAR_SHA_MISMATCH")
    sidecar = _strict_json(
        sidecar_raw, maximum_bytes=MAX_AUDIT_SIDECAR_BYTES, canonical=False
    )
    if sidecar.get("schema_version") != NATIVE_BARS_SCHEMA:
        _fail("PREFIX_AUDIT_SIDECAR_SCHEMA_INVALID")
    if sidecar.get("activation_id") != resolution.activation_id:
        _fail("PREFIX_AUDIT_SIDECAR_ACTIVATION_MISMATCH")
    provenance_relative, provenance_path = _safe_relative(
        native_root,
        sidecar.get("source_provenance_relative_path"),
        "PREFIX_SOURCE_PROVENANCE_PATH_INVALID",
    )
    provenance_raw, provenance_sha = _read_regular(
        provenance_path,
        maximum_bytes=MAX_PROVENANCE_BYTES,
        code="PREFIX_SOURCE_PROVENANCE",
    )
    if provenance_sha != _digest(
        sidecar.get("source_provenance_sha256"),
        "PREFIX_SOURCE_PROVENANCE_SHA_INVALID",
    ):
        _fail("PREFIX_SOURCE_PROVENANCE_SHA_MISMATCH")
    provenance = _strict_json(
        provenance_raw, maximum_bytes=MAX_PROVENANCE_BYTES, canonical=False
    )
    expected_scalars = {
        "schema_version": NATIVE_BARS_SCHEMA,
        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "canonical_symbol": symbol,
        "timeframe": timeframe,
        "source_kind": "broker_native",
        "timezone": "UTC",
        "timestamp_semantics": "bar_open_utc",
        "bar_interval_semantics": "half_open_[bar_open,bar_close)",
        "bar_close_column": "_BAR_CLOSE_TS",
    }
    for key, expected in expected_scalars.items():
        if provenance.get(key) != expected:
            _fail(f"PREFIX_SOURCE_PROVENANCE_FIELD_MISMATCH:{key}")
    if provenance.get("columns") != list(PRODUCTION_ARTIFACT_COLUMNS):
        _fail("PREFIX_SOURCE_PROVENANCE_COLUMNS_MISMATCH")
    artifact = provenance.get("artifact")
    if not isinstance(artifact, Mapping):
        _fail("PREFIX_SOURCE_PROVENANCE_ARTIFACT_INVALID")
    if (
        artifact.get("relative_path") != "bars.csv"
        or artifact.get("sha256") != bars_sha
    ):
        _fail("PREFIX_SOURCE_PROVENANCE_ARTIFACT_MISMATCH")
    bars_rows = artifact.get("rows")
    if type(bars_rows) is not int or not 0 < bars_rows <= MAX_SOURCE_ROWS:
        _fail("PREFIX_SOURCE_ROWS_INVALID")
    if artifact.get("bytes") != bars_bytes or provenance.get("rows") != bars_rows:
        _fail("PREFIX_SOURCE_PROVENANCE_SIZE_MISMATCH")
    if provenance.get("sha256") != bars_sha:
        _fail("PREFIX_SOURCE_PROVENANCE_BARS_SHA_MISMATCH")

    instrument = provenance.get("instrument")
    if type(instrument) is not dict or set(instrument) != _INSTRUMENT_SNAPSHOT_KEYS:
        _fail("PREFIX_SOURCE_INSTRUMENT_SNAPSHOT_INVALID")
    if (
        instrument.get("status") != "available"
        or str(instrument.get("symbol") or "").upper() != symbol
        or type(instrument.get("asset_class")) is not str
        or not instrument["asset_class"]
        or type(instrument.get("path")) is not str
        or not instrument["path"]
        or type(instrument.get("digits")) is not int
        or not 0 <= instrument["digits"] <= 16
    ):
        _fail("PREFIX_SOURCE_INSTRUMENT_IDENTITY_INVALID")
    for name in (
        "parity_tick_tolerance",
        "point",
        "trade_contract_size",
        "trade_tick_size",
        "trade_tick_value",
    ):
        value = instrument.get(name)
        if (
            type(value) not in {int, float}
            or not math.isfinite(float(value))
            or value <= 0
        ):
            _fail(f"PREFIX_SOURCE_INSTRUMENT_VALUE_INVALID:{name}")

    request = provenance.get("request")
    if not isinstance(request, Mapping):
        _fail("PREFIX_SOURCE_REQUEST_INVALID")
    request_map_sha = _digest(
        request.get("timeframe_request_map_sha256"),
        "PREFIX_SOURCE_REQUEST_MAP_SHA_INVALID",
    )
    interval = request.get("source_request_interval")
    if not isinstance(interval, Mapping) or set(interval) != {
        "start_utc",
        "end_exclusive_utc",
    }:
        _fail("PREFIX_SOURCE_REQUEST_INTERVAL_INVALID")
    request_start = _iso(
        _csv_utc(interval.get("start_utc"), "PREFIX_SOURCE_REQUEST_START_INVALID")
    )
    request_end = _iso(
        _csv_utc(interval.get("end_exclusive_utc"), "PREFIX_SOURCE_REQUEST_END_INVALID")
    )
    if _parse_utc(request_start, "PREFIX_SOURCE_REQUEST_START_INVALID") >= _parse_utc(
        request_end, "PREFIX_SOURCE_REQUEST_END_INVALID"
    ):
        _fail("PREFIX_SOURCE_REQUEST_INTERVAL_INVALID")
    try:
        source_identity = canonical_source_identity(
            provenance.get("source_environment")
        )
    except Exception as exc:
        raise NativePrefixError("PREFIX_SOURCE_IDENTITY_INVALID") from exc
    if not isinstance(source_identity, Mapping):
        _fail("PREFIX_SOURCE_IDENTITY_INVALID")
    identity_sha = _digest(
        source_identity.get("identity_sha256"), "PREFIX_SOURCE_IDENTITY_SHA_INVALID"
    )
    run_manifest_relative_value = manifest.get("run_manifest_relative_path")
    run_manifest_sha_value = manifest.get("run_manifest_sha256")
    if not isinstance(run_manifest_relative_value, str) or not _is_sha(
        run_manifest_sha_value
    ):
        _fail("PREFIX_RUN_MANIFEST_BINDING_MISSING")
    run_manifest_relative, run_manifest_path = _safe_relative(
        native_root,
        run_manifest_relative_value,
        "PREFIX_RUN_MANIFEST_PATH_INVALID",
    )
    run_manifest_raw, run_manifest_sha = _read_regular(
        run_manifest_path,
        maximum_bytes=MAX_ACTIVATION_BYTES,
        code="PREFIX_RUN_MANIFEST",
    )
    if run_manifest_sha != run_manifest_sha_value:
        _fail("PREFIX_RUN_MANIFEST_SHA_MISMATCH")
    run_manifest = _strict_json(
        run_manifest_raw,
        maximum_bytes=MAX_ACTIVATION_BYTES,
        canonical=False,
    )
    run_request = run_manifest.get("request")
    if (
        not isinstance(run_request, Mapping)
        or run_request.get("timeframe_request_map_sha256") != request_map_sha
    ):
        _fail("PREFIX_RUN_MANIFEST_REQUEST_MAP_MISMATCH")
    try:
        run_identity = canonical_source_identity(run_manifest.get("source_environment"))
    except Exception as exc:
        raise NativePrefixError("PREFIX_RUN_MANIFEST_SOURCE_IDENTITY_INVALID") from exc
    if run_identity != source_identity:
        _fail("PREFIX_MIXED_ACCOUNT_PROVENANCE")
    matching_results = [
        item
        for item in (run_manifest.get("results") or [])
        if isinstance(item, Mapping)
        and item.get("symbol") == symbol
        and item.get("timeframe") == timeframe
    ]
    if len(matching_results) != 1:
        _fail("PREFIX_RUN_MANIFEST_CELL_BINDING_INVALID")
    run_cell = matching_results[0]
    if run_cell.get("sha256") != bars_sha or run_cell.get("rows") != bars_rows:
        _fail("PREFIX_RUN_MANIFEST_ARTIFACT_MISMATCH")
    return _SourceBinding(
        activation_id=str(resolution.activation_id),
        activation_manifest_relative_path=manifest_relative,
        activation_manifest_sha256=manifest_sha,
        activation_entry_sha256=entry_sha,
        run_manifest_relative_path=run_manifest_relative,
        run_manifest_sha256=run_manifest_sha,
        entry=entry,
        bars_relative_path=bars_relative,
        bars_path=bars_path,
        bars_sha256=bars_sha,
        bars_bytes=bars_bytes,
        bars_rows=bars_rows,
        audit_sidecar_relative_path=sidecar_relative,
        audit_sidecar_sha256=sidecar_sha,
        audit_sidecar_bytes=len(sidecar_raw),
        provenance_relative_path=provenance_relative,
        provenance_path=provenance_path,
        provenance_sha256=provenance_sha,
        provenance_bytes=len(provenance_raw),
        request_map_sha256=request_map_sha,
        source_identity_sha256=identity_sha,
        source_identity=dict(source_identity),
        instrument_snapshot=dict(instrument),
        request_start_utc=request_start,
        request_end_exclusive_utc=request_end,
        provenance=dict(provenance),
        run_manifest=dict(run_manifest),
        run_cell=dict(run_cell),
    )


def _parse_csv_line(
    raw: bytes, line_number: int
) -> tuple[list[str], datetime, datetime]:
    if not raw or len(raw) > MAX_LINE_BYTES or not raw.endswith((b"\n", b"\r")):
        _fail(f"PREFIX_SOURCE_LINE_INVALID:{line_number}")
    try:
        decoded = raw.decode("utf-8", errors="strict")
        rows = list(csv.reader([decoded]))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise NativePrefixError(f"PREFIX_SOURCE_CSV_INVALID:{line_number}") from exc
    if len(rows) != 1 or len(rows[0]) != len(CSV_HEADER):
        _fail(f"PREFIX_SOURCE_CSV_FIELDS_INVALID:{line_number}")
    row = rows[0]
    opened = _csv_utc(row[0], f"PREFIX_SOURCE_OPEN_INVALID:{line_number}")
    closed = _csv_utc(row[-1], f"PREFIX_SOURCE_CLOSE_INVALID:{line_number}")
    if not opened < closed:
        _fail(f"PREFIX_SOURCE_BAR_INTERVAL_INVALID:{line_number}")
    for index in (1, 2, 3, 4):
        try:
            price = float(row[index])
        except ValueError as exc:
            raise NativePrefixError(
                f"PREFIX_SOURCE_PRICE_INVALID:{line_number}"
            ) from exc
        if not math.isfinite(price) or price <= 0:
            _fail(f"PREFIX_SOURCE_PRICE_INVALID:{line_number}")
    open_price, high, low, close_price = map(float, row[1:5])
    if high < max(open_price, low, close_price) or low > min(
        open_price, high, close_price
    ):
        _fail(f"PREFIX_SOURCE_OHLC_INVALID:{line_number}")
    for index in (5, 6, 7):
        try:
            value = float(row[index])
        except ValueError as exc:
            raise NativePrefixError(
                f"PREFIX_SOURCE_DISCRETE_INVALID:{line_number}"
            ) from exc
        if not math.isfinite(value) or value < 0 or not value.is_integer():
            _fail(f"PREFIX_SOURCE_DISCRETE_INVALID:{line_number}")
    source_first = _csv_utc(row[8], f"PREFIX_SOURCE_FIRST_TS_INVALID:{line_number}")
    source_last = _csv_utc(row[9], f"PREFIX_SOURCE_LAST_TS_INVALID:{line_number}")
    if source_first > source_last or not source_first <= opened <= source_last:
        _fail(f"PREFIX_SOURCE_RANGE_INVALID:{line_number}")
    return row, opened, closed


def _first_source_bar(source: _SourceBinding) -> _FirstSourceBar:
    """Read the first activated bar without classifying missing earlier history."""

    handle, identity = _open_regular(
        source.bars_path,
        maximum_bytes=MAX_SOURCE_BYTES,
        code="PREFIX_SOURCE_FIRST_BAR",
    )
    try:
        header = handle.readline(MAX_LINE_BYTES + 1)
        try:
            decoded = next(csv.reader([header.decode("utf-8", errors="strict")]))
        except (UnicodeDecodeError, csv.Error, StopIteration) as exc:
            raise NativePrefixError("PREFIX_SOURCE_HEADER_INVALID") from exc
        if tuple(decoded) != CSV_HEADER:
            _fail("PREFIX_SOURCE_HEADER_INVALID")
        raw = handle.readline(MAX_LINE_BYTES + 1)
        if not raw:
            _fail("PREFIX_SOURCE_FIRST_BAR_MISSING")
        _row, opened, closed = _parse_csv_line(raw, 2)
        _verify_open_identity(
            source.bars_path,
            handle,
            identity,
            "PREFIX_SOURCE_CHANGED_DURING_FIRST_BAR_READ",
        )
        return _FirstSourceBar(opened=opened, closed=closed)
    finally:
        handle.close()


def _available_history_boundary_required(
    source: _SourceBinding,
    *,
    expected_first: _FirstSourceBar,
) -> bool:
    """Detect an available suffix whose proof must be replayed.

    A terminal prefix that ends before the strategy interval is a legitimate
    audit/warmup shortfall and does not require an available-suffix proof.  A
    proof is mandatory once that terminal geometry intersects strategy, and an
    explicitly supplied proof is always verified rather than trusted.
    """

    pagination = source.provenance.get("pagination")
    boundary_shortfall = (
        source.provenance.get("failure_status") == "boundary_history_shortfall"
        or source.run_cell.get("failure_status") == "boundary_history_shortfall"
    )
    if not isinstance(pagination, Mapping):
        if not boundary_shortfall:
            return False
        request = source.provenance.get("request")
        strategy_interval = (
            request.get("strategy_evidence_interval")
            if isinstance(request, Mapping)
            else None
        )
        if not isinstance(strategy_interval, Mapping):
            return True
        try:
            strategy_start = _csv_utc(
                strategy_interval.get("start_utc"),
                "PREFIX_AVAILABLE_HISTORY_STRATEGY_START_INVALID",
            )
        except NativePrefixError:
            return True
        return expected_first.opened > strategy_start
    if pagination.get("available_history_boundary") is not None:
        return True
    terminal_pages = pagination.get("terminal_prefix_unresolved_pages")
    coverage = source.provenance.get("coverage")
    terminal_marker = (
        boundary_shortfall
        or pagination.get("stop_reason") == "terminal_prefix_unresolved"
        or (isinstance(terminal_pages, list) and bool(terminal_pages))
        or (
            isinstance(coverage, Mapping)
            and coverage.get("terminal_prefix_unresolved_page_count") not in {0, None}
        )
    )
    if not terminal_marker:
        return False
    request = source.provenance.get("request")
    strategy_interval = (
        request.get("strategy_evidence_interval")
        if isinstance(request, Mapping)
        else None
    )
    if not isinstance(strategy_interval, Mapping):
        return True
    try:
        strategy_start = _csv_utc(
            strategy_interval.get("start_utc"),
            "PREFIX_AVAILABLE_HISTORY_STRATEGY_START_INVALID",
        )
    except NativePrefixError:
        return True
    if isinstance(terminal_pages, list) and terminal_pages:
        terminal_page = terminal_pages[0]
        if not isinstance(terminal_page, Mapping):
            return True
        try:
            terminal_end = _csv_utc(
                terminal_page.get("end_exclusive_utc"),
                "PREFIX_AVAILABLE_HISTORY_TERMINAL_END_INVALID",
            )
        except NativePrefixError:
            return True
        return terminal_end > strategy_start
    return expected_first.opened > strategy_start


def _available_history_boundary(
    source: _SourceBinding,
    *,
    symbol: str,
    timeframe: str,
    expected_first: _FirstSourceBar,
) -> _AvailableHistoryBoundary:
    """Recompute the public V5 available-suffix proof from pinned bytes."""

    manifest_request = source.run_manifest.get("request")
    provenance_request = source.provenance.get("request")
    pagination = source.provenance.get("pagination")
    if (
        not isinstance(manifest_request, Mapping)
        or manifest_request.get("timeframe_request_map_schema_version")
        != TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
        or not isinstance(provenance_request, Mapping)
        or not isinstance(pagination, Mapping)
    ):
        _fail("PREFIX_AVAILABLE_HISTORY_BOUNDARY_CONTRACT_REQUIRED")
    common_request = {
        key: manifest_request[key]
        for key in COMMON_REQUEST_KEYS
        if key in manifest_request
    }
    scan_request: Mapping[str, Any] = common_request
    if pagination.get("stop_reason") in {
        "terminal_prefix_unresolved",
        "technical_prefix_unresolved",
    }:
        attempts = pagination.get("page_attempts")
        intervals = common_request.get("timeframe_source_request_intervals")
        if (
            not isinstance(attempts, list)
            or not attempts
            or not isinstance(attempts[0], Mapping)
            or not isinstance(intervals, Mapping)
            or not isinstance(intervals.get(timeframe), Mapping)
        ):
            _fail("PREFIX_AVAILABLE_HISTORY_BOUNDARY_SCAN_REQUEST_INVALID")
        bounded_intervals = {
            str(key): dict(value) if isinstance(value, Mapping) else value
            for key, value in intervals.items()
        }
        interval = dict(intervals[timeframe])
        interval["start_utc"] = attempts[0].get("start_utc")
        bounded_intervals[timeframe] = interval
        scan_request = {
            **common_request,
            "timeframe_source_request_intervals": bounded_intervals,
        }

    handle, identity = _open_regular(
        source.bars_path,
        maximum_bytes=MAX_SOURCE_BYTES,
        code="PREFIX_AVAILABLE_HISTORY_SOURCE",
    )
    try:
        scan = scan_native_csv_stream(
            handle,
            expected_bytes=source.bars_bytes,
            timeframe=timeframe,
            common_request=scan_request,
            pagination=pagination,
            maximum_bytes=MAX_SOURCE_BYTES,
            maximum_rows=MAX_SOURCE_ROWS,
            maximum_line_bytes=MAX_LINE_BYTES,
        )
        _verify_open_identity(
            source.bars_path,
            handle,
            identity,
            "PREFIX_AVAILABLE_HISTORY_SOURCE_CHANGED",
        )
    except NativeCsvValidationError as exc:
        raise NativePrefixError(
            f"PREFIX_AVAILABLE_HISTORY_SOURCE_INVALID:{exc.code}"
        ) from exc
    finally:
        handle.close()
    observation = scan.artifact
    if (
        observation.sha256 != source.bars_sha256
        or observation.bytes != source.bars_bytes
        or observation.rows != source.bars_rows
        or observation.first_open_utc != _iso(expected_first.opened)
    ):
        _fail("PREFIX_AVAILABLE_HISTORY_SOURCE_BINDING_MISMATCH")
    try:
        proof = validate_authenticated_available_history_boundary(
            pagination,
            observation,
            symbol=symbol,
            broker_symbol=source.provenance.get("broker_symbol"),
            timeframe=timeframe,
            leaf_request=provenance_request,
            common_request=common_request,
            leaf_source_environment=source.provenance.get("source_environment"),
            manifest_source_environment=source.run_manifest.get("source_environment"),
            expected_retries=manifest_request.get("retries"),
        )
    except NativeRequestContractError as exc:
        detail = exc.errors[0] if getattr(exc, "errors", ()) else type(exc).__name__
        raise NativePrefixError(
            f"PREFIX_AVAILABLE_HISTORY_BOUNDARY_INVALID:{detail}"
        ) from exc
    if proof.get("boundary_open_utc") != _iso(expected_first.opened):
        _fail("PREFIX_AVAILABLE_HISTORY_BOUNDARY_FIRST_BAR_MISMATCH")
    return _AvailableHistoryBoundary(
        proof=proof,
        proof_sha256=_sha_json(proof),
        first_open_utc=_iso(expected_first.opened),
        first_close_utc=_iso(expected_first.closed),
    )


def _segment_line(
    ordinal: int,
    start_open: datetime,
    last_open: datetime,
    end_close: datetime,
    first_row: int,
    last_row: int,
) -> bytes:
    return (
        _canonical_bytes(
            {
                "ordinal": ordinal,
                "start_open_utc": _iso(start_open),
                "last_open_utc": _iso(last_open),
                "end_exclusive_utc": _iso(end_close),
                "first_prefix_row": first_row,
                "last_prefix_row": last_row,
                "rows": last_row - first_row + 1,
            }
        )
        + b"\n"
    )


class _PrefixStream:
    """Single-use source scanner that yields exact prefix bytes."""

    def __init__(
        self,
        source: _SourceBinding,
        cutoff: datetime,
        segment_path: Path,
    ) -> None:
        self.source = source
        self.cutoff = cutoff
        self.segment_path = segment_path
        self.facts: _PrefixFacts | None = None

    def chunks(self) -> Iterator[bytes]:
        source_handle, source_identity = _open_regular(
            self.source.bars_path,
            maximum_bytes=MAX_SOURCE_BYTES,
            code="PREFIX_SOURCE_BARS",
        )
        source_digest = hashlib.sha256()
        source_bytes = 0
        source_rows = 0
        prefix_rows = 0
        first_open: datetime | None = None
        last_open: datetime | None = None
        last_close: datetime | None = None
        previous_source_open: datetime | None = None
        previous_prefix_close: datetime | None = None
        segment_start_open: datetime | None = None
        segment_last_open: datetime | None = None
        segment_last_close: datetime | None = None
        segment_first_row = 0
        segment_count = 0
        gap_count = 0
        segment_digest = hashlib.sha256()
        segment_bytes = 0
        segment_file = self.segment_path.open("xb", buffering=IO_CHUNK_BYTES)
        try:
            header = source_handle.readline(MAX_LINE_BYTES + 1)
            source_digest.update(header)
            source_bytes += len(header)
            try:
                decoded_header = next(
                    csv.reader([header.decode("utf-8", errors="strict")])
                )
            except (UnicodeDecodeError, csv.Error, StopIteration) as exc:
                raise NativePrefixError("PREFIX_SOURCE_HEADER_INVALID") from exc
            if tuple(decoded_header) != CSV_HEADER:
                _fail("PREFIX_SOURCE_HEADER_INVALID")
            yield header
            line_number = 1
            while True:
                raw = source_handle.readline(MAX_LINE_BYTES + 1)
                if not raw:
                    break
                line_number += 1
                source_digest.update(raw)
                source_bytes += len(raw)
                if source_bytes > MAX_SOURCE_BYTES:
                    _fail("PREFIX_SOURCE_SIZE_EXCEEDED")
                _row, opened, closed = _parse_csv_line(raw, line_number)
                source_rows += 1
                if source_rows > MAX_SOURCE_ROWS:
                    _fail("PREFIX_SOURCE_ROWS_EXCEEDED")
                if previous_source_open is not None and opened <= previous_source_open:
                    _fail(f"PREFIX_SOURCE_ROW_ORDER_INVALID:{line_number}")
                previous_source_open = opened
                if closed > self.cutoff:
                    continue
                prefix_rows += 1
                if first_open is None:
                    first_open = opened
                    segment_start_open = opened
                    segment_first_row = 1
                if previous_prefix_close is not None:
                    if opened < previous_prefix_close:
                        _fail(f"PREFIX_SOURCE_BAR_OVERLAP:{line_number}")
                    if opened > previous_prefix_close:
                        assert (
                            segment_start_open is not None
                            and segment_last_open is not None
                            and segment_last_close is not None
                        )
                        segment_count += 1
                        gap_count += 1
                        segment = _segment_line(
                            segment_count,
                            segment_start_open,
                            segment_last_open,
                            segment_last_close,
                            segment_first_row,
                            prefix_rows - 1,
                        )
                        segment_file.write(segment)
                        segment_digest.update(segment)
                        segment_bytes += len(segment)
                        segment_start_open = opened
                        segment_first_row = prefix_rows
                last_open = opened
                last_close = closed
                previous_prefix_close = closed
                segment_last_open = opened
                segment_last_close = closed
                yield raw
            if prefix_rows <= 0:
                _fail("PREFIX_NO_CLOSED_ROWS_AT_CUTOFF")
            assert (
                first_open is not None
                and last_open is not None
                and last_close is not None
                and segment_start_open is not None
                and segment_last_open is not None
                and segment_last_close is not None
            )
            segment_count += 1
            segment = _segment_line(
                segment_count,
                segment_start_open,
                segment_last_open,
                segment_last_close,
                segment_first_row,
                prefix_rows,
            )
            segment_file.write(segment)
            segment_digest.update(segment)
            segment_bytes += len(segment)
            segment_file.flush()
            os.fsync(segment_file.fileno())
            _verify_open_identity(
                self.source.bars_path,
                source_handle,
                source_identity,
                "PREFIX_SOURCE_CHANGED_DURING_SCAN",
            )
            if source_digest.hexdigest() != self.source.bars_sha256:
                _fail("PREFIX_SOURCE_BARS_SHA_MISMATCH")
            if (
                source_bytes != self.source.bars_bytes
                or source_rows != self.source.bars_rows
            ):
                _fail("PREFIX_SOURCE_BARS_SIZE_OR_ROWS_MISMATCH")
            self.facts = _PrefixFacts(
                source_sha256=source_digest.hexdigest(),
                source_bytes=source_bytes,
                source_rows=source_rows,
                prefix_rows=prefix_rows,
                first_open_utc=_iso(first_open),
                last_open_utc=_iso(last_open),
                last_close_utc=_iso(last_close),
                segment_count=segment_count,
                gap_count=gap_count,
                segment_sha256=segment_digest.hexdigest(),
                segment_bytes=segment_bytes,
            )
        finally:
            segment_file.close()
            source_handle.close()


def _file_chunks(path: Path) -> Iterator[bytes]:
    handle, identity = _open_regular(
        path, maximum_bytes=MAX_SOURCE_BYTES, code="PREFIX_TEMPORARY_OBJECT"
    )
    try:
        while True:
            chunk = handle.read(IO_CHUNK_BYTES)
            if not chunk:
                break
            yield chunk
        _verify_open_identity(path, handle, identity, "PREFIX_TEMPORARY_OBJECT_CHANGED")
    finally:
        handle.close()


def _source_mapping(source: _SourceBinding) -> dict[str, Any]:
    return {
        "bars_relative_path": source.bars_relative_path,
        "bars_sha256": source.bars_sha256,
        "bars_bytes": source.bars_bytes,
        "bars_rows": source.bars_rows,
        "audit_sidecar_relative_path": source.audit_sidecar_relative_path,
        "audit_sidecar_sha256": source.audit_sidecar_sha256,
        "audit_sidecar_bytes": source.audit_sidecar_bytes,
        "provenance_relative_path": source.provenance_relative_path,
        "provenance_sha256": source.provenance_sha256,
        "provenance_bytes": source.provenance_bytes,
        "request_map_sha256": source.request_map_sha256,
        "source_identity_sha256": source.source_identity_sha256,
        "source_identity": dict(source.source_identity),
        "instrument_snapshot": dict(source.instrument_snapshot),
        "request_start_utc": source.request_start_utc,
        "request_end_exclusive_utc": source.request_end_exclusive_utc,
    }


def _activation_mapping(source: _SourceBinding) -> dict[str, Any]:
    return {
        "activation_id": source.activation_id,
        "manifest_relative_path": source.activation_manifest_relative_path,
        "manifest_sha256": source.activation_manifest_sha256,
        "entry_sha256": source.activation_entry_sha256,
        "run_manifest_relative_path": source.run_manifest_relative_path,
        "run_manifest_sha256": source.run_manifest_sha256,
    }


def _native_prefix_from_index(
    index: Mapping[str, Any], index_sha: str
) -> dict[str, Any]:
    source_state = index["source_state"]
    activation = index["activation"]
    source = index["source"]
    continuity = index["continuity"]
    prefix = index["prefix"]
    if source_state == AVAILABLE:
        schema_sha = prefix["schema"]["sha256"]
        content_sha = prefix["content"]["sha256"]
        artifact_sha = source["bars_sha256"]
        provenance_sha = source["provenance_sha256"]
        artifact_rows = source["bars_rows"]
        prefix_rows = prefix["prefix_rows"]
        projected_index_sha = index_sha
        prefix_start = prefix["prefix_start_utc"]
        prefix_end = prefix["prefix_end_exclusive_utc"]
        first_open = prefix["prefix_first_open_utc"]
        last_open = prefix["prefix_last_open_utc"]
        last_close = prefix["prefix_last_close_utc"]
        terminal_sha = None
    else:
        schema_sha = content_sha = artifact_sha = provenance_sha = None
        artifact_rows = prefix_rows = None
        projected_index_sha = None
        prefix_start = prefix_end = first_open = last_open = last_close = None
        terminal_sha = index["terminal_receipt"]["sha256"]
    return {
        "source_state": source_state,
        "source_reason": index["source_reason"],
        "symbol": index["symbol"],
        "timeframe": index["timeframe"],
        "activation_id": activation["activation_id"],
        "activation_manifest_sha256": activation["manifest_sha256"],
        "activation_entry_sha256": activation["entry_sha256"],
        "artifact_sha256": artifact_sha,
        "provenance_sha256": provenance_sha,
        "request_map_sha256": source["request_map_sha256"],
        "source_identity_sha256": source["source_identity_sha256"],
        "continuity_policy_sha256": continuity["policy"]["sha256"],
        "continuity_receipt_sha256": continuity["receipt"]["sha256"],
        "continuity_geometry_sha256": continuity["geometry_sha256"],
        "source_terminal_receipt_sha256": terminal_sha,
        "source_accessed_at_utc": index["source_accessed_at_utc"],
        "request_start_utc": source["request_start_utc"],
        "request_end_exclusive_utc": source["request_end_exclusive_utc"],
        "prefix_start_utc": prefix_start,
        "prefix_end_exclusive_utc": prefix_end,
        "prefix_first_open_utc": first_open,
        "prefix_last_open_utc": last_open,
        "prefix_last_close_utc": last_close,
        "artifact_rows": artifact_rows,
        "prefix_rows": prefix_rows,
        "prefix_schema_sha256": schema_sha,
        "prefix_index_sha256": projected_index_sha,
        "prefix_content_sha256": content_sha,
        "internal_status": continuity["internal_status"],
        "boundary_status": continuity["boundary_status"],
        "unexplained_discontinuity_count": continuity[
            "unexplained_discontinuity_count"
        ],
        "segments_sha256": prefix["segments"]["sha256"],
        "requires_preserved_segment_ledger": continuity[
            "requires_preserved_segment_ledger"
        ],
    }


def _temp_path(root: Path) -> Path:
    handle = tempfile.NamedTemporaryFile(
        mode="xb",
        prefix="native-prefix-segments-",
        suffix=".part",
        dir=root,
        delete=False,
    )
    path = Path(handle.name)
    handle.close()
    path.unlink()
    return path


def produce_activated_native_prefix(
    *,
    data_dir: Path,
    context: authority_spool.GenerationSpoolContext,
    symbol: str,
    timeframe: str,
    cutoff_utc: str,
    source_accessed_at_utc: str,
    production_contract_required: bool = True,
) -> NativePrefixProduct:
    """Produce and immediately byte-verify one activated native cutoff prefix."""

    canonical_symbol = _symbol(symbol)
    canonical_timeframe = _timeframe(timeframe)
    cutoff = _parse_utc(cutoff_utc, "PREFIX_CUTOFF_INVALID")
    accessed = _parse_utc(source_accessed_at_utc, "PREFIX_SOURCE_ACCESSED_INVALID")
    if accessed > datetime.now(tz=UTC) + timedelta(minutes=5):
        _fail("PREFIX_SOURCE_ACCESSED_IN_FUTURE")
    if type(context) is not authority_spool.GenerationSpoolContext:
        _fail("PREFIX_SPOOL_CONTEXT_REQUIRED")
    try:
        resolution = resolve_active_native_artifact(
            Path(data_dir),
            canonical_symbol,
            canonical_timeframe,
            production_contract_required=production_contract_required,
        )
    except Exception as exc:
        raise NativePrefixError("PREFIX_ACTIVATION_RESOLUTION_EXCEPTION") from exc
    if not resolution.valid:
        state = (
            SOURCE_UNAVAILABLE
            if resolution.errors == ["NATIVE_ACTIVE_CELL_SOURCE_UNAVAILABLE"]
            else TECHNICAL_FAILURE
        )
        return _produce_terminal_prefix(
            data_dir=Path(data_dir),
            context=context,
            resolution=resolution,
            symbol=canonical_symbol,
            timeframe=canonical_timeframe,
            cutoff_utc=_iso(cutoff),
            source_accessed_at_utc=_iso(accessed),
            source_state=state,
            production_contract_required=production_contract_required,
        )
    source = _source_binding(
        Path(data_dir), resolution, canonical_symbol, canonical_timeframe
    )
    request_start = _parse_utc(
        source.request_start_utc, "PREFIX_SOURCE_REQUEST_START_INVALID"
    )
    request_end = _parse_utc(
        source.request_end_exclusive_utc, "PREFIX_SOURCE_REQUEST_END_INVALID"
    )
    if not request_start < cutoff <= request_end:
        _fail("PREFIX_CUTOFF_OUTSIDE_AUTHENTICATED_REQUEST")
    first_source_bar = _first_source_bar(source)
    available_boundary: _AvailableHistoryBoundary | None = None
    if cutoff < first_source_bar.closed or _available_history_boundary_required(
        source,
        expected_first=first_source_bar,
    ):
        available_boundary = _available_history_boundary(
            source,
            symbol=canonical_symbol,
            timeframe=canonical_timeframe,
            expected_first=first_source_bar,
        )
    if cutoff < first_source_bar.closed:
        assert available_boundary is not None
        return _produce_cutoff_unavailable_prefix(
            data_dir=Path(data_dir),
            context=context,
            source=source,
            symbol=canonical_symbol,
            timeframe=canonical_timeframe,
            cutoff_utc=_iso(cutoff),
            source_accessed_at_utc=_iso(accessed),
            boundary=available_boundary,
            production_contract_required=production_contract_required,
        )
    boundary_status = "LEFT_TRUNCATED" if available_boundary is not None else "COMPLETE"

    schema_descriptor = authority_spool.write_canonical_json_artifact(
        context, _source_schema_payload()
    )
    segment_temp = _temp_path(Path(context.root))
    stream = _PrefixStream(source, cutoff, segment_temp)
    try:
        content_descriptor = authority_spool.write_opaque_artifact(
            context, stream.chunks(), format="csv"
        )
        facts = stream.facts
        if facts is None:
            _fail("PREFIX_SCAN_DID_NOT_COMPLETE")
        segments_descriptor = authority_spool.write_opaque_artifact(
            context, _file_chunks(segment_temp), format="jsonl"
        )
    except authority_spool.AuthoritySpoolError as exc:
        raise NativePrefixError(f"PREFIX_SPOOL_FAILURE:{exc}") from exc
    finally:
        try:
            segment_temp.unlink(missing_ok=True)
        except OSError:
            pass
    if (
        segments_descriptor["sha256"] != facts.segment_sha256
        or segments_descriptor["size_bytes"] != facts.segment_bytes
    ):
        _fail("PREFIX_SEGMENT_DESCRIPTOR_MISMATCH")

    policy = {
        "schema_version": PREFIX_CONTINUITY_POLICY_SCHEMA,
        "symbol": canonical_symbol,
        "timeframe": canonical_timeframe,
        "cutoff_utc": _iso(cutoff),
        "activation_manifest_sha256": source.activation_manifest_sha256,
        "activation_entry_sha256": source.activation_entry_sha256,
        "source_artifact_sha256": source.bars_sha256,
        "source_provenance_sha256": source.provenance_sha256,
        "source_identity_sha256": source.source_identity_sha256,
        "request_map_sha256": source.request_map_sha256,
        "prefix_schema_sha256": schema_descriptor["sha256"],
        "prefix_content_sha256": content_descriptor["sha256"],
        "prefix_rows": facts.prefix_rows,
        "nominal_timeframe_seconds": TIMEFRAME_SECONDS[canonical_timeframe],
        "overlap_allowed": False,
        "resampling_allowed": False,
    }
    policy_descriptor = authority_spool.write_canonical_json_artifact(context, policy)
    geometry = {
        "schema_version": PREFIX_CONTINUITY_RECEIPT_SCHEMA,
        "symbol": canonical_symbol,
        "timeframe": canonical_timeframe,
        "prefix_content_sha256": content_descriptor["sha256"],
        "prefix_rows": facts.prefix_rows,
        "first_open_utc": facts.first_open_utc,
        "last_open_utc": facts.last_open_utc,
        "last_close_utc": facts.last_close_utc,
        "segment_count": facts.segment_count,
        "gap_count": facts.gap_count,
        "segments_sha256": segments_descriptor["sha256"],
    }
    geometry_sha = _sha_json(geometry)
    internal = (
        "CONTINUOUS"
        if facts.gap_count == 0
        else "EXPECTED_AUTHENTICATED_DISCONTINUITIES"
    )
    continuity_receipt = {
        **geometry,
        "policy_sha256": policy_descriptor["sha256"],
        "geometry_sha256": geometry_sha,
        "internal_status": internal,
        "boundary_status": boundary_status,
        "unexplained_discontinuity_count": 0,
        "requires_preserved_segment_ledger": facts.gap_count > 0,
        "source_activation_authenticated": True,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    receipt_descriptor = authority_spool.write_canonical_json_artifact(
        context, continuity_receipt
    )
    index = {
        "schema_version": PREFIX_INDEX_SCHEMA,
        "authority_scope": PREFIX_AUTHORITY_SCOPE,
        "source_state": AVAILABLE,
        "source_reason": None,
        "symbol": canonical_symbol,
        "timeframe": canonical_timeframe,
        "cutoff_utc": _iso(cutoff),
        "source_accessed_at_utc": _iso(accessed),
        "activation": _activation_mapping(source),
        "source": _source_mapping(source),
        "prefix": {
            "schema": schema_descriptor,
            "content": content_descriptor,
            "segments": segments_descriptor,
            "prefix_rows": facts.prefix_rows,
            "prefix_start_utc": facts.first_open_utc,
            "prefix_end_exclusive_utc": facts.last_close_utc,
            "prefix_first_open_utc": facts.first_open_utc,
            "prefix_last_open_utc": facts.last_open_utc,
            "prefix_last_close_utc": facts.last_close_utc,
        },
        "continuity": {
            "policy": policy_descriptor,
            "receipt": receipt_descriptor,
            "geometry_sha256": geometry_sha,
            "internal_status": internal,
            "boundary_status": boundary_status,
            "unexplained_discontinuity_count": 0,
            "segment_count": facts.segment_count,
            "gap_count": facts.gap_count,
            "requires_preserved_segment_ledger": facts.gap_count > 0,
        },
        "terminal_receipt": None,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    index_descriptor = authority_spool.write_canonical_json_artifact(context, index)
    return verify_activated_native_prefix(
        data_dir=Path(data_dir),
        context=context,
        index_descriptor=index_descriptor,
        expected_index_sha256=index_descriptor["sha256"],
        expected_symbol=canonical_symbol,
        expected_timeframe=canonical_timeframe,
        expected_cutoff_utc=_iso(cutoff),
        expected_activation_manifest_sha256=source.activation_manifest_sha256,
        production_contract_required=production_contract_required,
        require_current_activation=True,
    )


def _terminal_seed(
    *,
    state: str,
    symbol: str,
    timeframe: str,
    cutoff_utc: str,
    errors: Sequence[str],
) -> dict[str, Any]:
    return {
        "schema_version": PREFIX_TERMINAL_RECEIPT_SCHEMA,
        "source_state": state,
        "symbol": symbol,
        "timeframe": timeframe,
        "cutoff_utc": cutoff_utc,
        "resolver_errors": list(errors),
        "binding_mode": "FAIL_CLOSED_DERIVED_SENTINELS_NOT_SOURCE_ASSERTIONS",
    }


def _produce_terminal_prefix(
    *,
    data_dir: Path,
    context: authority_spool.GenerationSpoolContext,
    resolution: ActiveNativeResolution,
    symbol: str,
    timeframe: str,
    cutoff_utc: str,
    source_accessed_at_utc: str,
    source_state: str,
    production_contract_required: bool,
) -> NativePrefixProduct:
    if source_state not in {SOURCE_UNAVAILABLE, TECHNICAL_FAILURE}:
        _fail("PREFIX_TERMINAL_STATE_INVALID")
    errors = tuple(str(value) for value in resolution.errors) or (
        "NATIVE_ACTIVE_RESOLUTION_EMPTY_FAILURE",
    )
    seed = _terminal_seed(
        state=source_state,
        symbol=symbol,
        timeframe=timeframe,
        cutoff_utc=cutoff_utc,
        errors=errors,
    )
    seed_sha = _sha_json(seed)
    activation_id = str(
        resolution.activation_id or f"technical-unbound-{seed_sha[:16]}"
    )
    manifest_sha = (
        resolution.activation_manifest_sha256
        if _is_sha(resolution.activation_manifest_sha256)
        else _sha_json({"domain": "unbound-activation-manifest", "seed": seed})
    )
    entry_sha = _sha_json({"domain": "unbound-activation-entry", "seed": seed})
    manifest_relative = None
    run_manifest_relative = None
    run_manifest_sha = _sha_json({"domain": "unbound-run-manifest", "seed": seed})
    if resolution.activation_manifest_path is not None:
        try:
            manifest_relative = (
                resolution.activation_manifest_path.resolve(strict=False)
                .relative_to((data_dir / "native").resolve())
                .as_posix()
            )
            raw, observed_manifest_sha = _read_regular(
                resolution.activation_manifest_path,
                maximum_bytes=MAX_ACTIVATION_BYTES,
                code="PREFIX_TERMINAL_ACTIVATION_MANIFEST",
            )
            manifest = _strict_json(
                raw, maximum_bytes=MAX_ACTIVATION_BYTES, canonical=False
            )
            entries = manifest.get("entries")
            symbol_entries = (
                entries.get(symbol) if isinstance(entries, Mapping) else None
            )
            entry = (
                symbol_entries.get(timeframe)
                if isinstance(symbol_entries, Mapping)
                else None
            )
            if observed_manifest_sha == manifest_sha and isinstance(entry, Mapping):
                entry_sha = _sha_json(dict(entry))
            raw_run_relative = manifest.get("run_manifest_relative_path")
            raw_run_sha = manifest.get("run_manifest_sha256")
            if isinstance(raw_run_relative, str) and _is_sha(raw_run_sha):
                run_manifest_relative = raw_run_relative
                run_manifest_sha = str(raw_run_sha)
        except NativePrefixError:
            if source_state == SOURCE_UNAVAILABLE:
                raise
            manifest_relative = None
    request_end = _parse_utc(cutoff_utc, "PREFIX_CUTOFF_INVALID")
    request_start = request_end - timedelta(seconds=TIMEFRAME_SECONDS[timeframe])
    request_map_sha = _sha_json({"domain": "terminal-request-map", "seed": seed})
    identity_sha = _sha_json({"domain": "terminal-source-identity", "seed": seed})
    continuity_policy = {
        "schema_version": PREFIX_CONTINUITY_POLICY_SCHEMA,
        "terminal": True,
        "seed_sha256": seed_sha,
    }
    policy_descriptor = authority_spool.write_canonical_json_artifact(
        context, continuity_policy
    )
    terminal_segments_descriptor = authority_spool.write_canonical_json_artifact(
        context,
        {
            "schema_version": PREFIX_CONTINUITY_RECEIPT_SCHEMA,
            "terminal": True,
            "seed_sha256": seed_sha,
            "segment_count": 0,
        },
    )
    segments_sha = terminal_segments_descriptor["sha256"]
    geometry_sha = _sha_json(
        {"domain": "terminal-geometry", "seed": seed, "segments_sha256": segments_sha}
    )
    continuity_receipt = {
        "schema_version": PREFIX_CONTINUITY_RECEIPT_SCHEMA,
        "terminal": True,
        "source_state": source_state,
        "seed_sha256": seed_sha,
        "policy_sha256": policy_descriptor["sha256"],
        "geometry_sha256": geometry_sha,
        "segments_sha256": segments_sha,
    }
    continuity_descriptor = authority_spool.write_canonical_json_artifact(
        context, continuity_receipt
    )
    terminal_receipt = {
        **seed,
        "activation_id": activation_id,
        "activation_manifest_sha256": manifest_sha,
        "activation_entry_sha256": entry_sha,
        "run_manifest_relative_path": run_manifest_relative,
        "run_manifest_sha256": run_manifest_sha,
        "request_map_sha256": request_map_sha,
        "source_identity_sha256": identity_sha,
        "continuity_policy_sha256": policy_descriptor["sha256"],
        "continuity_receipt_sha256": continuity_descriptor["sha256"],
        "continuity_geometry_sha256": geometry_sha,
        "segments_sha256": segments_sha,
        "grants_no_trade_authority": False,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    terminal_descriptor = authority_spool.write_canonical_json_artifact(
        context, terminal_receipt
    )
    internal = "UNEXPLAINED" if source_state == TECHNICAL_FAILURE else "CONTINUOUS"
    unexplained = 1 if source_state == TECHNICAL_FAILURE else 0
    reason = f"{source_state}:{errors[0]}"
    index = {
        "schema_version": PREFIX_INDEX_SCHEMA,
        "authority_scope": PREFIX_AUTHORITY_SCOPE,
        "source_state": source_state,
        "source_reason": reason,
        "symbol": symbol,
        "timeframe": timeframe,
        "cutoff_utc": cutoff_utc,
        "source_accessed_at_utc": source_accessed_at_utc,
        "activation": {
            "activation_id": activation_id,
            "manifest_relative_path": manifest_relative,
            "manifest_sha256": manifest_sha,
            "entry_sha256": entry_sha,
            "run_manifest_relative_path": run_manifest_relative,
            "run_manifest_sha256": run_manifest_sha,
        },
        "source": {
            "bars_relative_path": None,
            "bars_sha256": None,
            "bars_bytes": None,
            "bars_rows": None,
            "audit_sidecar_relative_path": None,
            "audit_sidecar_sha256": None,
            "audit_sidecar_bytes": None,
            "provenance_relative_path": None,
            "provenance_sha256": None,
            "provenance_bytes": None,
            "request_map_sha256": request_map_sha,
            "source_identity_sha256": identity_sha,
            "source_identity": None,
            "instrument_snapshot": None,
            "request_start_utc": _iso(request_start),
            "request_end_exclusive_utc": cutoff_utc,
        },
        "prefix": {
            "schema": None,
            "content": None,
            "segments": terminal_segments_descriptor,
            "prefix_rows": None,
            "prefix_start_utc": None,
            "prefix_end_exclusive_utc": None,
            "prefix_first_open_utc": None,
            "prefix_last_open_utc": None,
            "prefix_last_close_utc": None,
        },
        "continuity": {
            "policy": policy_descriptor,
            "receipt": continuity_descriptor,
            "geometry_sha256": geometry_sha,
            "internal_status": internal,
            "boundary_status": "RIGHT_TRUNCATED",
            "unexplained_discontinuity_count": unexplained,
            "segment_count": 0,
            "gap_count": 0,
            "requires_preserved_segment_ledger": False,
        },
        "terminal_receipt": terminal_descriptor,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    index_descriptor = authority_spool.write_canonical_json_artifact(context, index)
    return verify_activated_native_prefix(
        data_dir=data_dir,
        context=context,
        index_descriptor=index_descriptor,
        expected_index_sha256=index_descriptor["sha256"],
        expected_symbol=symbol,
        expected_timeframe=timeframe,
        expected_cutoff_utc=cutoff_utc,
        expected_activation_manifest_sha256=manifest_sha,
        production_contract_required=production_contract_required,
        require_current_activation=True,
    )


def _cutoff_terminal_seed(
    *,
    source: _SourceBinding,
    symbol: str,
    timeframe: str,
    cutoff_utc: str,
    boundary: _AvailableHistoryBoundary,
) -> dict[str, Any]:
    return {
        "schema_version": PREFIX_CUTOFF_TERMINAL_RECEIPT_SCHEMA,
        "source_state": SOURCE_UNAVAILABLE,
        "source_reason": SOURCE_UNAVAILABLE_AT_CUTOFF,
        "symbol": symbol,
        "timeframe": timeframe,
        "cutoff_utc": cutoff_utc,
        "binding_mode": "AUTHENTICATED_AVAILABLE_SUFFIX_NO_CLOSED_ROW_AT_CUTOFF",
        "activation_id": source.activation_id,
        "activation_manifest_sha256": source.activation_manifest_sha256,
        "activation_entry_sha256": source.activation_entry_sha256,
        "run_manifest_relative_path": source.run_manifest_relative_path,
        "run_manifest_sha256": source.run_manifest_sha256,
        "source_artifact_sha256": source.bars_sha256,
        "source_provenance_sha256": source.provenance_sha256,
        "source_artifact_rows": source.bars_rows,
        "request_map_sha256": source.request_map_sha256,
        "source_identity_sha256": source.source_identity_sha256,
        "request_start_utc": source.request_start_utc,
        "request_end_exclusive_utc": source.request_end_exclusive_utc,
        "available_history_boundary_sha256": boundary.proof_sha256,
        "available_history_boundary_kind": boundary.proof.get("boundary_kind"),
        "available_history_boundary_confirmation_method": boundary.proof.get(
            "confirmation_method"
        ),
        "market_listing_date_asserted": boundary.proof.get(
            "market_listing_date_asserted"
        ),
        "statistical_sufficiency_assessed": boundary.proof.get(
            "statistical_sufficiency_assessed"
        ),
        "first_authenticated_bar_open_utc": boundary.first_open_utc,
        "first_authenticated_bar_close_utc": boundary.first_close_utc,
        "cutoff_relation": "STRICTLY_BEFORE_FIRST_AUTHENTICATED_BAR_CLOSE",
    }


def _produce_cutoff_unavailable_prefix(
    *,
    data_dir: Path,
    context: authority_spool.GenerationSpoolContext,
    source: _SourceBinding,
    symbol: str,
    timeframe: str,
    cutoff_utc: str,
    source_accessed_at_utc: str,
    boundary: _AvailableHistoryBoundary,
    production_contract_required: bool,
) -> NativePrefixProduct:
    """Seal a source-bound no-closed-row state for a pre-boundary cutoff."""

    cutoff = _parse_utc(cutoff_utc, "PREFIX_CUTOFF_INVALID")
    first_close = _parse_utc(
        boundary.first_close_utc,
        "PREFIX_AVAILABLE_HISTORY_FIRST_CLOSE_INVALID",
    )
    if not cutoff < first_close:
        _fail("PREFIX_AVAILABLE_HISTORY_CUTOFF_RELATION_INVALID")
    seed = _cutoff_terminal_seed(
        source=source,
        symbol=symbol,
        timeframe=timeframe,
        cutoff_utc=cutoff_utc,
        boundary=boundary,
    )
    seed_sha = _sha_json(seed)
    continuity_policy = {
        "schema_version": PREFIX_CONTINUITY_POLICY_SCHEMA,
        "terminal": True,
        "terminal_kind": SOURCE_UNAVAILABLE_AT_CUTOFF,
        "seed_sha256": seed_sha,
    }
    policy_descriptor = authority_spool.write_canonical_json_artifact(
        context, continuity_policy
    )
    terminal_segments_descriptor = authority_spool.write_canonical_json_artifact(
        context,
        {
            "schema_version": PREFIX_CONTINUITY_RECEIPT_SCHEMA,
            "terminal": True,
            "terminal_kind": SOURCE_UNAVAILABLE_AT_CUTOFF,
            "seed_sha256": seed_sha,
            "segment_count": 0,
        },
    )
    geometry_sha = _sha_json(
        {
            "domain": "authenticated-cutoff-terminal-geometry",
            "seed": seed,
            "segments_sha256": terminal_segments_descriptor["sha256"],
        }
    )
    continuity_receipt = {
        "schema_version": PREFIX_CONTINUITY_RECEIPT_SCHEMA,
        "terminal": True,
        "terminal_kind": SOURCE_UNAVAILABLE_AT_CUTOFF,
        "source_state": SOURCE_UNAVAILABLE,
        "seed_sha256": seed_sha,
        "policy_sha256": policy_descriptor["sha256"],
        "geometry_sha256": geometry_sha,
        "segments_sha256": terminal_segments_descriptor["sha256"],
    }
    continuity_descriptor = authority_spool.write_canonical_json_artifact(
        context, continuity_receipt
    )
    terminal_receipt = {
        **seed,
        "continuity_policy_sha256": policy_descriptor["sha256"],
        "continuity_receipt_sha256": continuity_descriptor["sha256"],
        "continuity_geometry_sha256": geometry_sha,
        "segments_sha256": terminal_segments_descriptor["sha256"],
        "grants_no_trade_authority": False,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    terminal_descriptor = authority_spool.write_canonical_json_artifact(
        context, terminal_receipt
    )
    index = {
        "schema_version": PREFIX_INDEX_SCHEMA,
        "authority_scope": PREFIX_AUTHORITY_SCOPE,
        "source_state": SOURCE_UNAVAILABLE,
        "source_reason": SOURCE_UNAVAILABLE_AT_CUTOFF,
        "symbol": symbol,
        "timeframe": timeframe,
        "cutoff_utc": cutoff_utc,
        "source_accessed_at_utc": source_accessed_at_utc,
        "activation": _activation_mapping(source),
        "source": _source_mapping(source),
        "prefix": {
            "schema": None,
            "content": None,
            "segments": terminal_segments_descriptor,
            "prefix_rows": None,
            "prefix_start_utc": None,
            "prefix_end_exclusive_utc": None,
            "prefix_first_open_utc": None,
            "prefix_last_open_utc": None,
            "prefix_last_close_utc": None,
        },
        "continuity": {
            "policy": policy_descriptor,
            "receipt": continuity_descriptor,
            "geometry_sha256": geometry_sha,
            "internal_status": "CONTINUOUS",
            "boundary_status": "LEFT_TRUNCATED",
            "unexplained_discontinuity_count": 0,
            "segment_count": 0,
            "gap_count": 0,
            "requires_preserved_segment_ledger": False,
        },
        "terminal_receipt": terminal_descriptor,
        "grants_search_authority": False,
        "grants_trading_authority": False,
    }
    index_descriptor = authority_spool.write_canonical_json_artifact(context, index)
    return verify_activated_native_prefix(
        data_dir=data_dir,
        context=context,
        index_descriptor=index_descriptor,
        expected_index_sha256=index_descriptor["sha256"],
        expected_symbol=symbol,
        expected_timeframe=timeframe,
        expected_cutoff_utc=cutoff_utc,
        expected_activation_manifest_sha256=source.activation_manifest_sha256,
        production_contract_required=production_contract_required,
        require_current_activation=True,
    )


def _validate_index_shape(index: dict[str, Any]) -> None:
    if set(index) != _INDEX_KEYS or index.get("schema_version") != PREFIX_INDEX_SCHEMA:
        _fail("PREFIX_INDEX_FIELDS_OR_SCHEMA_INVALID")
    if index.get("authority_scope") != PREFIX_AUTHORITY_SCOPE:
        _fail("PREFIX_INDEX_SCOPE_INVALID")
    if index.get("source_state") not in SOURCE_STATES:
        _fail("PREFIX_INDEX_SOURCE_STATE_INVALID")
    if (
        type(index.get("activation")) is not dict
        or set(index["activation"]) != _ACTIVATION_KEYS
    ):
        _fail("PREFIX_INDEX_ACTIVATION_INVALID")
    if type(index.get("source")) is not dict or set(index["source"]) != _SOURCE_KEYS:
        _fail("PREFIX_INDEX_SOURCE_INVALID")
    if type(index.get("prefix")) is not dict or set(index["prefix"]) != _PREFIX_KEYS:
        _fail("PREFIX_INDEX_PREFIX_INVALID")
    if (
        type(index.get("continuity")) is not dict
        or set(index["continuity"]) != _CONTINUITY_KEYS
    ):
        _fail("PREFIX_INDEX_CONTINUITY_INVALID")
    if (
        index.get("grants_search_authority") is not False
        or index.get("grants_trading_authority") is not False
    ):
        _fail("PREFIX_INDEX_AUTHORITY_INVALID")


def _compare_available_prefix(
    *,
    source: _SourceBinding,
    content_path: Path,
    cutoff: datetime,
    segments_descriptor: Mapping[str, Any],
) -> _PrefixFacts:
    segment_temp = _temp_path(content_path.parent)
    # Reuse the exact source scanner and compare its yielded prefix bytes to
    # the persisted content rather than trusting producer counters.
    stream = _PrefixStream(source, cutoff, segment_temp)
    prefix_handle, prefix_identity = _open_regular(
        content_path, maximum_bytes=MAX_SOURCE_BYTES, code="PREFIX_CONTENT"
    )
    prefix_digest = hashlib.sha256()
    prefix_bytes = 0
    try:
        for expected in stream.chunks():
            observed = prefix_handle.read(len(expected))
            if observed != expected:
                _fail("PREFIX_CONTENT_NOT_EXACT_SOURCE_PREFIX")
            prefix_digest.update(observed)
            prefix_bytes += len(observed)
        if prefix_handle.read(1):
            _fail("PREFIX_CONTENT_HAS_EXTRA_BYTES")
        _verify_open_identity(
            content_path, prefix_handle, prefix_identity, "PREFIX_CONTENT_CHANGED"
        )
    finally:
        prefix_handle.close()
    facts = stream.facts
    if facts is None:
        _fail("PREFIX_VERIFY_SCAN_DID_NOT_COMPLETE")
    normalized_segments = _descriptor(
        dict(segments_descriptor), expected_format="jsonl"
    )
    try:
        raw, segment_sha = _read_regular(
            segment_temp,
            maximum_bytes=MAX_SOURCE_BYTES,
            code="PREFIX_RECOMPUTED_SEGMENTS",
        )
        if (
            segment_sha != normalized_segments["sha256"]
            or len(raw) != normalized_segments["size_bytes"]
        ):
            _fail("PREFIX_SEGMENT_LEDGER_RECOMPUTATION_MISMATCH")
    finally:
        segment_temp.unlink(missing_ok=True)
    return facts


def verify_activated_native_prefix(
    *,
    data_dir: Path,
    context: authority_spool.GenerationSpoolContext,
    index_descriptor: Mapping[str, Any],
    expected_index_sha256: str,
    expected_symbol: str,
    expected_timeframe: str,
    expected_cutoff_utc: str,
    expected_activation_manifest_sha256: str,
    production_contract_required: bool = True,
    require_current_activation: bool = False,
) -> NativePrefixProduct:
    """Reopen and independently verify a native-prefix product from bytes."""

    symbol = _symbol(expected_symbol)
    timeframe = _timeframe(expected_timeframe)
    cutoff = _parse_utc(expected_cutoff_utc, "PREFIX_EXPECTED_CUTOFF_INVALID")
    expected_index = _digest(expected_index_sha256, "PREFIX_EXPECTED_INDEX_SHA_INVALID")
    expected_activation = _digest(
        expected_activation_manifest_sha256,
        "PREFIX_EXPECTED_ACTIVATION_SHA_INVALID",
    )
    normalized_index, index_raw = _read_spool_descriptor(
        context,
        dict(index_descriptor),
        expected_format=authority_spool.CANONICAL_JSON_FORMAT,
        collect=True,
    )
    if normalized_index["sha256"] != expected_index:
        _fail("PREFIX_INDEX_SHA_MISMATCH")
    assert index_raw is not None
    index = _strict_json(index_raw, maximum_bytes=MAX_INDEX_BYTES, canonical=True)
    _validate_index_shape(index)
    if index.get("symbol") != symbol or index.get("timeframe") != timeframe:
        _fail("PREFIX_INDEX_CELL_MISMATCH")
    if index.get("cutoff_utc") != _iso(cutoff):
        _fail("PREFIX_INDEX_CUTOFF_MISMATCH")
    _parse_utc(index.get("source_accessed_at_utc"), "PREFIX_INDEX_ACCESS_TIME_INVALID")
    activation = index["activation"]
    if activation.get("manifest_sha256") != expected_activation:
        _fail("PREFIX_INDEX_ACTIVATION_SHA_MISMATCH")

    state = index["source_state"]
    descriptors: list[Mapping[str, Any]] = [normalized_index]
    instrument_snapshot: Mapping[str, Any] | None = None
    verified_prefix_path: Path | None = None
    verified_prefix_descriptor: Mapping[str, Any] | None = None
    if state == AVAILABLE:
        if (
            index.get("source_reason") is not None
            or index.get("terminal_receipt") is not None
        ):
            _fail("PREFIX_AVAILABLE_TERMINAL_FIELDS_INVALID")
        resolution = resolve_active_native_artifact(
            Path(data_dir),
            symbol,
            timeframe,
            production_contract_required=production_contract_required,
        )
        if require_current_activation and not resolution.valid:
            _fail("PREFIX_CURRENT_ACTIVATION_INVALID")
        if not resolution.valid:
            # Pinned historical activations remain verifiable after ACTIVE.json
            # advances only when their exact manifest and leaf bytes remain.
            resolution = ActiveNativeResolution(
                symbol=symbol,
                timeframe=timeframe,
                data_path=(
                    Path(data_dir) / "native" / index["source"]["bars_relative_path"]
                ),
                sidecar_path=(
                    Path(data_dir)
                    / "native"
                    / index["source"]["audit_sidecar_relative_path"]
                ),
                activation_id=activation["activation_id"],
                activation_manifest_path=(
                    Path(data_dir) / "native" / activation["manifest_relative_path"]
                ),
                activation_manifest_sha256=activation["manifest_sha256"],
                entry={},
            )
        source = _source_binding(Path(data_dir), resolution, symbol, timeframe)
        instrument_snapshot = dict(source.instrument_snapshot)
        if (
            _activation_mapping(source) != activation
            or _source_mapping(source) != index["source"]
        ):
            _fail("PREFIX_INDEX_SOURCE_BINDING_MISMATCH")
        first_source_bar = _first_source_bar(source)
        if cutoff < first_source_bar.closed:
            _fail("PREFIX_AVAILABLE_BEFORE_FIRST_AUTHENTICATED_BAR_CLOSE")
        available_boundary: _AvailableHistoryBoundary | None = None
        if _available_history_boundary_required(
            source,
            expected_first=first_source_bar,
        ):
            available_boundary = _available_history_boundary(
                source,
                symbol=symbol,
                timeframe=timeframe,
                expected_first=first_source_bar,
            )
        boundary_status = (
            "LEFT_TRUNCATED" if available_boundary is not None else "COMPLETE"
        )
        prefix = index["prefix"]
        schema_descriptor, schema_raw = _read_spool_descriptor(
            context,
            prefix["schema"],
            expected_format=authority_spool.CANONICAL_JSON_FORMAT,
            collect=True,
        )
        if schema_raw != _canonical_bytes(_source_schema_payload()):
            _fail("PREFIX_SCHEMA_BYTES_MISMATCH")
        content_descriptor, _ = _read_spool_descriptor(
            context, prefix["content"], expected_format="csv", collect=False
        )
        segments_descriptor, _ = _read_spool_descriptor(
            context, prefix["segments"], expected_format="jsonl", collect=False
        )
        _, content_path = _safe_relative(
            Path(context.root),
            content_descriptor["relative_path"],
            "PREFIX_CONTENT_PATH_INVALID",
        )
        verified_prefix_path = content_path
        verified_prefix_descriptor = content_descriptor
        facts = _compare_available_prefix(
            source=source,
            content_path=content_path,
            cutoff=cutoff,
            segments_descriptor=segments_descriptor,
        )
        if facts.prefix_rows != prefix["prefix_rows"] or (
            facts.first_open_utc,
            facts.last_open_utc,
            facts.last_close_utc,
        ) != (
            prefix["prefix_start_utc"],
            prefix["prefix_last_open_utc"],
            prefix["prefix_last_close_utc"],
        ):
            _fail("PREFIX_INDEX_GEOMETRY_MISMATCH")
        if (
            prefix["prefix_first_open_utc"] != facts.first_open_utc
            or prefix["prefix_end_exclusive_utc"] != facts.last_close_utc
        ):
            _fail("PREFIX_INDEX_BOUNDARY_MISMATCH")
        policy_descriptor, policy_raw = _read_spool_descriptor(
            context,
            index["continuity"]["policy"],
            expected_format=authority_spool.CANONICAL_JSON_FORMAT,
            collect=True,
        )
        receipt_descriptor, receipt_raw = _read_spool_descriptor(
            context,
            index["continuity"]["receipt"],
            expected_format=authority_spool.CANONICAL_JSON_FORMAT,
            collect=True,
        )
        segments_descriptor, _segments_raw = _read_spool_descriptor(
            context,
            index["prefix"]["segments"],
            expected_format="jsonl",
            collect=False,
        )
        assert policy_raw is not None and receipt_raw is not None
        policy = _strict_json(policy_raw, maximum_bytes=MAX_INDEX_BYTES, canonical=True)
        receipt = _strict_json(
            receipt_raw, maximum_bytes=MAX_INDEX_BYTES, canonical=True
        )
        expected_policy = {
            "schema_version": PREFIX_CONTINUITY_POLICY_SCHEMA,
            "symbol": symbol,
            "timeframe": timeframe,
            "cutoff_utc": _iso(cutoff),
            "activation_manifest_sha256": source.activation_manifest_sha256,
            "activation_entry_sha256": source.activation_entry_sha256,
            "source_artifact_sha256": source.bars_sha256,
            "source_provenance_sha256": source.provenance_sha256,
            "source_identity_sha256": source.source_identity_sha256,
            "request_map_sha256": source.request_map_sha256,
            "prefix_schema_sha256": schema_descriptor["sha256"],
            "prefix_content_sha256": content_descriptor["sha256"],
            "prefix_rows": facts.prefix_rows,
            "nominal_timeframe_seconds": TIMEFRAME_SECONDS[timeframe],
            "overlap_allowed": False,
            "resampling_allowed": False,
        }
        if policy != expected_policy:
            _fail("PREFIX_CONTINUITY_POLICY_MISMATCH")
        geometry = {
            "schema_version": PREFIX_CONTINUITY_RECEIPT_SCHEMA,
            "symbol": symbol,
            "timeframe": timeframe,
            "prefix_content_sha256": content_descriptor["sha256"],
            "prefix_rows": facts.prefix_rows,
            "first_open_utc": facts.first_open_utc,
            "last_open_utc": facts.last_open_utc,
            "last_close_utc": facts.last_close_utc,
            "segment_count": facts.segment_count,
            "gap_count": facts.gap_count,
            "segments_sha256": segments_descriptor["sha256"],
        }
        geometry_sha = _sha_json(geometry)
        internal = (
            "CONTINUOUS"
            if facts.gap_count == 0
            else "EXPECTED_AUTHENTICATED_DISCONTINUITIES"
        )
        expected_receipt = {
            **geometry,
            "policy_sha256": policy_descriptor["sha256"],
            "geometry_sha256": geometry_sha,
            "internal_status": internal,
            "boundary_status": boundary_status,
            "unexplained_discontinuity_count": 0,
            "requires_preserved_segment_ledger": facts.gap_count > 0,
            "source_activation_authenticated": True,
            "grants_search_authority": False,
            "grants_trading_authority": False,
        }
        if receipt != expected_receipt:
            _fail("PREFIX_CONTINUITY_RECEIPT_MISMATCH")
        expected_continuity = {
            "policy": policy_descriptor,
            "receipt": receipt_descriptor,
            "geometry_sha256": geometry_sha,
            "internal_status": internal,
            "boundary_status": boundary_status,
            "unexplained_discontinuity_count": 0,
            "segment_count": facts.segment_count,
            "gap_count": facts.gap_count,
            "requires_preserved_segment_ledger": facts.gap_count > 0,
        }
        if index["continuity"] != expected_continuity:
            _fail("PREFIX_INDEX_CONTINUITY_MISMATCH")
        descriptors.extend(
            (
                schema_descriptor,
                content_descriptor,
                segments_descriptor,
                policy_descriptor,
                receipt_descriptor,
            )
        )
    else:
        if type(index.get("source_reason")) is not str or not index["source_reason"]:
            _fail("PREFIX_TERMINAL_REASON_INVALID")
        segments_descriptor, segments_raw = _read_spool_descriptor(
            context,
            index["prefix"]["segments"],
            expected_format=authority_spool.CANONICAL_JSON_FORMAT,
            collect=True,
        )
        terminal_descriptor, terminal_raw = _read_spool_descriptor(
            context,
            index["terminal_receipt"],
            expected_format=authority_spool.CANONICAL_JSON_FORMAT,
            collect=True,
        )
        policy_descriptor, policy_raw = _read_spool_descriptor(
            context,
            index["continuity"]["policy"],
            expected_format=authority_spool.CANONICAL_JSON_FORMAT,
            collect=True,
        )
        receipt_descriptor, receipt_raw = _read_spool_descriptor(
            context,
            index["continuity"]["receipt"],
            expected_format=authority_spool.CANONICAL_JSON_FORMAT,
            collect=True,
        )
        assert (
            segments_raw is not None
            and terminal_raw is not None
            and policy_raw is not None
            and receipt_raw is not None
        )
        terminal = _strict_json(
            terminal_raw, maximum_bytes=MAX_INDEX_BYTES, canonical=True
        )
        if terminal.get("schema_version") == PREFIX_CUTOFF_TERMINAL_RECEIPT_SCHEMA:
            try:
                observed_resolution = resolve_active_native_artifact(
                    Path(data_dir),
                    symbol,
                    timeframe,
                    production_contract_required=production_contract_required,
                )
            except Exception as exc:
                if require_current_activation:
                    raise NativePrefixError(
                        "PREFIX_CUTOFF_TERMINAL_ACTIVATION_RESOLUTION_EXCEPTION"
                    ) from exc
                observed_resolution = ActiveNativeResolution(
                    symbol=symbol,
                    timeframe=timeframe,
                    errors=["PINNED_ACTIVATION_REOPEN_REQUIRED"],
                )
            if require_current_activation and not observed_resolution.valid:
                _fail("PREFIX_CUTOFF_TERMINAL_CURRENT_ACTIVATION_INVALID")
            if not observed_resolution.valid or (
                not require_current_activation
                and observed_resolution.activation_manifest_sha256
                != activation["manifest_sha256"]
            ):
                observed_resolution = ActiveNativeResolution(
                    symbol=symbol,
                    timeframe=timeframe,
                    data_path=(
                        Path(data_dir)
                        / "native"
                        / index["source"]["bars_relative_path"]
                    ),
                    sidecar_path=(
                        Path(data_dir)
                        / "native"
                        / index["source"]["audit_sidecar_relative_path"]
                    ),
                    activation_id=activation["activation_id"],
                    activation_manifest_path=(
                        Path(data_dir) / "native" / activation["manifest_relative_path"]
                    ),
                    activation_manifest_sha256=activation["manifest_sha256"],
                    entry={},
                )
            source = _source_binding(
                Path(data_dir), observed_resolution, symbol, timeframe
            )
            if (
                _activation_mapping(source) != activation
                or _source_mapping(source) != index["source"]
            ):
                _fail("PREFIX_CUTOFF_TERMINAL_SOURCE_BINDING_MISMATCH")
            first = _first_source_bar(source)
            boundary = _available_history_boundary(
                source,
                symbol=symbol,
                timeframe=timeframe,
                expected_first=first,
            )
            if not cutoff < first.closed:
                _fail("PREFIX_CUTOFF_TERMINAL_HAS_CLOSED_SOURCE_ROW")
            seed = _cutoff_terminal_seed(
                source=source,
                symbol=symbol,
                timeframe=timeframe,
                cutoff_utc=_iso(cutoff),
                boundary=boundary,
            )
            seed_sha = _sha_json(seed)
            expected_segments = {
                "schema_version": PREFIX_CONTINUITY_RECEIPT_SCHEMA,
                "terminal": True,
                "terminal_kind": SOURCE_UNAVAILABLE_AT_CUTOFF,
                "seed_sha256": seed_sha,
                "segment_count": 0,
            }
            if (
                _strict_json(
                    segments_raw,
                    maximum_bytes=MAX_INDEX_BYTES,
                    canonical=True,
                )
                != expected_segments
            ):
                _fail("PREFIX_CUTOFF_TERMINAL_SEGMENTS_MISMATCH")
            expected_policy = {
                "schema_version": PREFIX_CONTINUITY_POLICY_SCHEMA,
                "terminal": True,
                "terminal_kind": SOURCE_UNAVAILABLE_AT_CUTOFF,
                "seed_sha256": seed_sha,
            }
            if (
                _strict_json(
                    policy_raw,
                    maximum_bytes=MAX_INDEX_BYTES,
                    canonical=True,
                )
                != expected_policy
            ):
                _fail("PREFIX_CUTOFF_TERMINAL_POLICY_MISMATCH")
            geometry_sha = _sha_json(
                {
                    "domain": "authenticated-cutoff-terminal-geometry",
                    "seed": seed,
                    "segments_sha256": segments_descriptor["sha256"],
                }
            )
            expected_continuity_receipt = {
                "schema_version": PREFIX_CONTINUITY_RECEIPT_SCHEMA,
                "terminal": True,
                "terminal_kind": SOURCE_UNAVAILABLE_AT_CUTOFF,
                "source_state": SOURCE_UNAVAILABLE,
                "seed_sha256": seed_sha,
                "policy_sha256": policy_descriptor["sha256"],
                "geometry_sha256": geometry_sha,
                "segments_sha256": segments_descriptor["sha256"],
            }
            if (
                _strict_json(
                    receipt_raw,
                    maximum_bytes=MAX_INDEX_BYTES,
                    canonical=True,
                )
                != expected_continuity_receipt
            ):
                _fail("PREFIX_CUTOFF_TERMINAL_CONTINUITY_RECEIPT_MISMATCH")
            expected_terminal = {
                **seed,
                "continuity_policy_sha256": policy_descriptor["sha256"],
                "continuity_receipt_sha256": receipt_descriptor["sha256"],
                "continuity_geometry_sha256": geometry_sha,
                "segments_sha256": segments_descriptor["sha256"],
                "grants_no_trade_authority": False,
                "grants_search_authority": False,
                "grants_trading_authority": False,
            }
            if terminal != expected_terminal:
                _fail("PREFIX_CUTOFF_TERMINAL_RECEIPT_MISMATCH")
            expected_prefix = {
                "schema": None,
                "content": None,
                "segments": segments_descriptor,
                "prefix_rows": None,
                "prefix_start_utc": None,
                "prefix_end_exclusive_utc": None,
                "prefix_first_open_utc": None,
                "prefix_last_open_utc": None,
                "prefix_last_close_utc": None,
            }
            expected_continuity = {
                "policy": policy_descriptor,
                "receipt": receipt_descriptor,
                "geometry_sha256": geometry_sha,
                "internal_status": "CONTINUOUS",
                "boundary_status": "LEFT_TRUNCATED",
                "unexplained_discontinuity_count": 0,
                "segment_count": 0,
                "gap_count": 0,
                "requires_preserved_segment_ledger": False,
            }
            if (
                state != SOURCE_UNAVAILABLE
                or index.get("source_reason") != SOURCE_UNAVAILABLE_AT_CUTOFF
                or index["prefix"] != expected_prefix
                or index["continuity"] != expected_continuity
            ):
                _fail("PREFIX_CUTOFF_TERMINAL_INDEX_STATE_MISMATCH")
            descriptors.extend(
                (
                    terminal_descriptor,
                    policy_descriptor,
                    receipt_descriptor,
                    segments_descriptor,
                )
            )
            return NativePrefixProduct(
                source_state=state,
                index=index,
                index_descriptor=normalized_index,
                index_sha256=expected_index,
                native_prefix=_native_prefix_from_index(index, expected_index),
                instrument_snapshot=None,
                prefix_path=None,
                prefix_descriptor=None,
                artifact_descriptors=tuple(descriptors),
                _seal=_PRODUCT_SEAL,
                _data_dir=Path(data_dir).resolve(strict=False),
                _production_contract_required=production_contract_required,
            )
        if require_current_activation:
            try:
                observed_resolution = resolve_active_native_artifact(
                    Path(data_dir),
                    symbol,
                    timeframe,
                    production_contract_required=production_contract_required,
                )
            except Exception as exc:
                raise NativePrefixError(
                    "PREFIX_TERMINAL_ACTIVATION_RESOLUTION_EXCEPTION"
                ) from exc
            observed_state = (
                AVAILABLE
                if observed_resolution.valid
                else (
                    SOURCE_UNAVAILABLE
                    if observed_resolution.errors
                    == ["NATIVE_ACTIVE_CELL_SOURCE_UNAVAILABLE"]
                    else TECHNICAL_FAILURE
                )
            )
            if observed_state != state or list(observed_resolution.errors) != list(
                terminal.get("resolver_errors") or []
            ):
                _fail("PREFIX_TERMINAL_SOURCE_STATE_REPLAY_MISMATCH")
        seed = _terminal_seed(
            state=state,
            symbol=symbol,
            timeframe=timeframe,
            cutoff_utc=_iso(cutoff),
            errors=terminal.get("resolver_errors") or (),
        )
        seed_sha = _sha_json(seed)
        if terminal.get("seed_sha256", seed_sha) != seed_sha:
            _fail("PREFIX_TERMINAL_SEED_MISMATCH")
        expected_segments = {
            "schema_version": PREFIX_CONTINUITY_RECEIPT_SCHEMA,
            "terminal": True,
            "seed_sha256": seed_sha,
            "segment_count": 0,
        }
        if (
            _strict_json(
                segments_raw,
                maximum_bytes=MAX_INDEX_BYTES,
                canonical=True,
            )
            != expected_segments
        ):
            _fail("PREFIX_TERMINAL_SEGMENTS_MISMATCH")
        expected_policy = {
            "schema_version": PREFIX_CONTINUITY_POLICY_SCHEMA,
            "terminal": True,
            "seed_sha256": seed_sha,
        }
        if (
            _strict_json(
                policy_raw,
                maximum_bytes=MAX_INDEX_BYTES,
                canonical=True,
            )
            != expected_policy
        ):
            _fail("PREFIX_TERMINAL_POLICY_MISMATCH")
        geometry_sha = _sha_json(
            {
                "domain": "terminal-geometry",
                "seed": seed,
                "segments_sha256": segments_descriptor["sha256"],
            }
        )
        expected_continuity_receipt = {
            "schema_version": PREFIX_CONTINUITY_RECEIPT_SCHEMA,
            "terminal": True,
            "source_state": state,
            "seed_sha256": seed_sha,
            "policy_sha256": policy_descriptor["sha256"],
            "geometry_sha256": geometry_sha,
            "segments_sha256": segments_descriptor["sha256"],
        }
        if (
            _strict_json(
                receipt_raw,
                maximum_bytes=MAX_INDEX_BYTES,
                canonical=True,
            )
            != expected_continuity_receipt
        ):
            _fail("PREFIX_TERMINAL_CONTINUITY_RECEIPT_MISMATCH")
        if index["prefix"] != {
            "schema": None,
            "content": None,
            "segments": segments_descriptor,
            "prefix_rows": None,
            "prefix_start_utc": None,
            "prefix_end_exclusive_utc": None,
            "prefix_first_open_utc": None,
            "prefix_last_open_utc": None,
            "prefix_last_close_utc": None,
        }:
            _fail("PREFIX_TERMINAL_PREFIX_STATE_INVALID")
        expected_continuity = {
            "policy": policy_descriptor,
            "receipt": receipt_descriptor,
            "geometry_sha256": geometry_sha,
            "internal_status": "UNEXPLAINED"
            if state == TECHNICAL_FAILURE
            else "CONTINUOUS",
            "boundary_status": "RIGHT_TRUNCATED",
            "unexplained_discontinuity_count": 1 if state == TECHNICAL_FAILURE else 0,
            "segment_count": 0,
            "gap_count": 0,
            "requires_preserved_segment_ledger": False,
        }
        if index["continuity"] != expected_continuity:
            _fail("PREFIX_TERMINAL_CONTINUITY_STATE_INVALID")
        expected_terminal_subset = {
            "schema_version": PREFIX_TERMINAL_RECEIPT_SCHEMA,
            "source_state": state,
            "symbol": symbol,
            "timeframe": timeframe,
            "cutoff_utc": _iso(cutoff),
            "resolver_errors": terminal.get("resolver_errors"),
            "binding_mode": "FAIL_CLOSED_DERIVED_SENTINELS_NOT_SOURCE_ASSERTIONS",
        }
        for key, value in expected_terminal_subset.items():
            if terminal.get(key) != value:
                _fail("PREFIX_TERMINAL_RECEIPT_MISMATCH")
        if (
            terminal.get("activation_manifest_sha256") != activation["manifest_sha256"]
            or terminal.get("activation_entry_sha256") != activation["entry_sha256"]
        ):
            _fail("PREFIX_TERMINAL_ACTIVATION_BINDING_MISMATCH")
        if (
            terminal.get("run_manifest_relative_path")
            != activation["run_manifest_relative_path"]
            or terminal.get("run_manifest_sha256") != activation["run_manifest_sha256"]
        ):
            _fail("PREFIX_TERMINAL_RUN_MANIFEST_BINDING_MISMATCH")
        if (
            terminal.get("request_map_sha256") != index["source"]["request_map_sha256"]
            or terminal.get("source_identity_sha256")
            != index["source"]["source_identity_sha256"]
        ):
            _fail("PREFIX_TERMINAL_SOURCE_BINDING_MISMATCH")
        if terminal.get("segments_sha256") != segments_descriptor["sha256"]:
            _fail("PREFIX_TERMINAL_SEGMENT_BINDING_MISMATCH")
        descriptors.extend(
            (
                terminal_descriptor,
                policy_descriptor,
                receipt_descriptor,
                segments_descriptor,
            )
        )

    native_prefix = _native_prefix_from_index(index, expected_index)
    return NativePrefixProduct(
        source_state=state,
        index=index,
        index_descriptor=normalized_index,
        index_sha256=expected_index,
        native_prefix=native_prefix,
        instrument_snapshot=instrument_snapshot,
        prefix_path=verified_prefix_path,
        prefix_descriptor=verified_prefix_descriptor,
        artifact_descriptors=tuple(descriptors),
        _seal=_PRODUCT_SEAL,
        _data_dir=Path(data_dir).resolve(strict=False),
        _production_contract_required=production_contract_required,
    )


def reverify_activated_native_prefix_product(
    product: NativePrefixProduct,
    context: authority_spool.GenerationSpoolContext,
    *,
    require_current_activation: bool = False,
) -> NativePrefixProduct:
    """Reopen a sealed product against its verifier-owned native root.

    Downstream consumers must not guess a data directory for a source-bound
    terminal product.  The directory and production-contract mode retained by
    the successful verifier are non-authoritative routing facts: every index,
    activation, provenance, and source byte is still independently replayed by
    :func:`verify_activated_native_prefix` below.
    """

    if type(product) is not NativePrefixProduct or product._seal is not _PRODUCT_SEAL:
        _fail("PREFIX_PRODUCT_NOT_VERIFIER_SEALED")
    if type(context) is not authority_spool.GenerationSpoolContext:
        _fail("PREFIX_SPOOL_CONTEXT_REQUIRED")
    if not isinstance(product._data_dir, Path):
        _fail("PREFIX_PRODUCT_NATIVE_ROOT_UNBOUND")
    try:
        canonical_data_dir = product._data_dir.resolve(strict=False)
    except OSError as exc:
        raise NativePrefixError("PREFIX_PRODUCT_NATIVE_ROOT_INVALID") from exc
    if not product._data_dir.is_absolute() or product._data_dir != canonical_data_dir:
        _fail("PREFIX_PRODUCT_NATIVE_ROOT_INVALID")
    if type(product._production_contract_required) is not bool:
        _fail("PREFIX_PRODUCT_PRODUCTION_CONTRACT_MODE_INVALID")
    if (
        type(product.index) is not dict
        or type(product.index_descriptor) is not dict
        or type(product.native_prefix) is not dict
    ):
        _fail("PREFIX_PRODUCT_INDEX_INVALID")
    index = dict(product.index)
    _validate_index_shape(index)
    activation = index["activation"]
    if (
        product.index_sha256 != _sha_json(index)
        or dict(product.index_descriptor).get("sha256") != product.index_sha256
        or dict(product.native_prefix)
        != _native_prefix_from_index(index, product.index_sha256)
    ):
        _fail("PREFIX_PRODUCT_INDEX_REPLAY_MISMATCH")
    return verify_activated_native_prefix(
        data_dir=product._data_dir,
        context=context,
        index_descriptor=product.index_descriptor,
        expected_index_sha256=product.index_sha256,
        expected_symbol=str(index.get("symbol") or ""),
        expected_timeframe=str(index.get("timeframe") or ""),
        expected_cutoff_utc=str(index.get("cutoff_utc") or ""),
        expected_activation_manifest_sha256=str(
            activation.get("manifest_sha256") or ""
        ),
        production_contract_required=product._production_contract_required,
        require_current_activation=require_current_activation,
    )


def open_verified_prefix_frame(product: NativePrefixProduct) -> pd.DataFrame:
    """Reopen one sealed AVAILABLE prefix as an isolated native bar frame.

    The content-addressed index and CSV descriptor are replayed before parsing;
    the same regular-file handle is hashed, parsed, and identity-checked so a
    path swap cannot splice caller data into the execution frame.  No
    resampling, timestamp synthesis, or provenance relabelling occurs here.
    """

    if type(product) is not NativePrefixProduct or product._seal is not _PRODUCT_SEAL:
        _fail("PREFIX_PRODUCT_NOT_VERIFIER_SEALED")
    if product.source_state != AVAILABLE:
        _fail("PREFIX_FRAME_REQUIRES_AVAILABLE_SOURCE")
    if type(product.index) is not dict:
        # Products return a dict; rejecting arbitrary Mapping implementations
        # avoids mutable proxy behavior during the replay checks below.
        _fail("PREFIX_PRODUCT_INDEX_INVALID")
    index = dict(product.index)
    _validate_index_shape(index)
    index_raw = _canonical_bytes(index)
    index_descriptor = _descriptor(
        dict(product.index_descriptor),
        expected_format=authority_spool.CANONICAL_JSON_FORMAT,
    )
    if (
        product.index_sha256 != hashlib.sha256(index_raw).hexdigest()
        or index_descriptor["sha256"] != product.index_sha256
        or index_descriptor["size_bytes"] != len(index_raw)
        or dict(product.native_prefix)
        != _native_prefix_from_index(index, product.index_sha256)
        or dict(product.instrument_snapshot or {})
        != dict(index["source"].get("instrument_snapshot") or {})
    ):
        _fail("PREFIX_PRODUCT_INDEX_REPLAY_MISMATCH")
    prefix = index["prefix"]
    descriptor = _descriptor(dict(product.prefix_descriptor), expected_format="csv")
    if (
        descriptor != prefix["content"]
        or product.prefix_path is None
        or prefix["prefix_rows"] != product.native_prefix["prefix_rows"]
        or type(prefix["prefix_rows"]) is not int
        or not 1 <= prefix["prefix_rows"] <= MAX_OPEN_FRAME_ROWS
        or descriptor["size_bytes"] > MAX_OPEN_FRAME_BYTES
    ):
        _fail("PREFIX_PRODUCT_CONTENT_BINDING_INVALID")
    normalized_artifacts = tuple(dict(value) for value in product.artifact_descriptors)
    if (
        index_descriptor not in normalized_artifacts
        or descriptor not in normalized_artifacts
    ):
        _fail("PREFIX_PRODUCT_DESCRIPTOR_ROSTER_INCOMPLETE")
    path = Path(product.prefix_path)
    handle, identity = _open_regular(
        path,
        maximum_bytes=MAX_OPEN_FRAME_BYTES,
        code="PREFIX_FRAME_OBJECT",
    )
    try:
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = handle.read(IO_CHUNK_BYTES)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_OPEN_FRAME_BYTES:
                _fail("PREFIX_FRAME_BYTE_BOUND_EXCEEDED")
            digest.update(chunk)
        if (
            total != descriptor["size_bytes"]
            or digest.hexdigest() != descriptor["sha256"]
        ):
            _fail("PREFIX_FRAME_CONTENT_MISMATCH")
        handle.seek(0)
        try:
            frame = pd.read_csv(
                handle,
                dtype={
                    "time": "string",
                    "Open": "float64",
                    "High": "float64",
                    "Low": "float64",
                    "Close": "float64",
                    "Volume": "float64",
                    "Spread": "float64",
                    "RealVolume": "float64",
                    "_SOURCE_FIRST_TS": "string",
                    "_SOURCE_LAST_TS": "string",
                    "_BAR_CLOSE_TS": "string",
                },
                keep_default_na=False,
                low_memory=False,
                nrows=prefix["prefix_rows"] + 1,
            )
        except Exception as exc:
            raise NativePrefixError("PREFIX_FRAME_CSV_PARSE_FAILED") from exc
        _verify_open_identity(path, handle, identity, "PREFIX_FRAME_OBJECT_CHANGED")
    finally:
        handle.close()
    if tuple(frame.columns) != CSV_HEADER or len(frame) != prefix["prefix_rows"]:
        _fail("PREFIX_FRAME_ROW_OR_COLUMN_MISMATCH")
    time_columns = ("time", "_SOURCE_FIRST_TS", "_SOURCE_LAST_TS", "_BAR_CLOSE_TS")
    parsed_times: dict[str, pd.DatetimeIndex] = {}
    for name in time_columns:
        parsed = pd.DatetimeIndex(
            pd.to_datetime(frame[name], utc=True, errors="coerce")
        )
        if parsed.isna().any():
            _fail("PREFIX_FRAME_TIMESTAMP_INVALID")
        parsed_times[name] = parsed
    opens = parsed_times["time"]
    closes = parsed_times["_BAR_CLOSE_TS"]
    source_first = parsed_times["_SOURCE_FIRST_TS"]
    source_last = parsed_times["_SOURCE_LAST_TS"]
    if (
        not opens.is_monotonic_increasing
        or opens.has_duplicates
        or bool((opens >= closes).any())
        or bool((source_first > source_last).any())
        or bool((opens < source_first).any())
        or bool((opens > source_last).any())
        or closes[-1] > pd.Timestamp(index["cutoff_utc"])
        or _iso(opens[0].to_pydatetime()) != prefix["prefix_first_open_utc"]
        or _iso(opens[-1].to_pydatetime()) != prefix["prefix_last_open_utc"]
        or _iso(closes[-1].to_pydatetime()) != prefix["prefix_last_close_utc"]
    ):
        _fail("PREFIX_FRAME_TEMPORAL_GEOMETRY_INVALID")
    numeric_columns = ("Open", "High", "Low", "Close", "Volume", "Spread", "RealVolume")
    numeric = frame.loc[:, numeric_columns].to_numpy(dtype=np.float64, copy=False)
    if (
        not np.isfinite(numeric).all()
        or bool((frame[["Open", "High", "Low", "Close"]].to_numpy() <= 0).any())
        or bool((frame[["Volume", "Spread", "RealVolume"]].to_numpy() < 0).any())
        or bool(
            (
                frame[["Volume", "Spread", "RealVolume"]].to_numpy()
                != np.floor(frame[["Volume", "Spread", "RealVolume"]].to_numpy())
            ).any()
        )
        or bool(
            (
                frame["High"].to_numpy()
                < frame[["Open", "Low", "Close"]].max(axis=1).to_numpy()
            ).any()
        )
        or bool(
            (
                frame["Low"].to_numpy()
                > frame[["Open", "High", "Close"]].min(axis=1).to_numpy()
            ).any()
        )
    ):
        _fail("PREFIX_FRAME_NUMERICAL_VALUES_INVALID")
    frame["_SOURCE_FIRST_TS"] = source_first
    frame["_SOURCE_LAST_TS"] = source_last
    frame["_BAR_CLOSE_TS"] = closes
    frame = frame.drop(columns=["time"])
    frame.index = opens
    frame.index.name = "time"
    result = frame.copy(deep=True)
    activation = index["activation"]
    source = index["source"]
    result.attrs[NATIVE_SOURCE_BINDING_ATTR] = {
        "symbol": str(index["symbol"]).upper(),
        "timeframe": str(index["timeframe"]).upper(),
        "activation_id": str(activation["activation_id"]),
        "activation_manifest_sha256": str(activation["manifest_sha256"]).lower(),
        "bars_sha256": str(source["bars_sha256"]).lower(),
        "sidecar_sha256": str(source["audit_sidecar_sha256"]).lower(),
    }
    return result


__all__ = [
    "AVAILABLE",
    "NATIVE_SOURCE_BINDING_ATTR",
    "PREFIX_CUTOFF_TERMINAL_RECEIPT_SCHEMA",
    "PREFIX_INDEX_SCHEMA",
    "PREFIX_SCHEMA_VERSION",
    "SOURCE_UNAVAILABLE",
    "SOURCE_UNAVAILABLE_AT_CUTOFF",
    "TECHNICAL_FAILURE",
    "NativePrefixError",
    "NativePrefixProduct",
    "open_verified_prefix_frame",
    "produce_activated_native_prefix",
    "reverify_activated_native_prefix_product",
    "verify_activated_native_prefix",
]
