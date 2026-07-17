"""Bounded, page-geometry-invariant continuity evidence for native bars.

This module audits one immutable ``bars.csv`` leaf by streaming its exact raw
bytes once.  It never infers continuity from acquisition pages and it never
loads bar history into a dataframe or an in-memory row collection.

The result is technical evidence only.  It grants no trading, optimization,
statistical-sufficiency, promotion, capital, or live-execution authority.
Production integration is intentionally outside this standalone contract.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import math
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, Mapping, Sequence

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from pm_native_request_contract import (
    canonical_source_identity as _canonical_source_identity,
)


UTC: Final = timezone.utc

CONTINUITY_CONTRACT_VERSION: Final = "fxpm.native-continuity.v1"
CONTINUITY_POLICY_SCHEMA_VERSION: Final = "fxpm.native-continuity.policy.v1"
CALENDAR_RECEIPT_SCHEMA_VERSION: Final = "fxpm.native-calendar-explanation.v1"
CALENDAR_AUTHORITY_KEY_ID: Final = "fxpm-native-calendar-authority-2026-07-v1"
CALENDAR_AUTHORITY_PUBLIC_KEY_BASE64: Final = (
    "YKkPxGWIXROFixQonJm693zph8ZaoekERAm/7IqoXEg="
)
CALENDAR_SIGNATURE_DOMAIN: Final = b"fxpm.native-calendar-explanation.v1\x00"

PROVENANCE_SCHEMA_VERSION: Final = "fxpm-native-bars-v1"
ACQUISITION_CONTRACT_VERSION: Final = "2026-07-12-native-calendar-acquisition-v3"

TIMEFRAME_SECONDS: Final[Mapping[str, int]] = MappingProxyType(
    {
        "M5": 300,
        "M15": 900,
        "M30": 1_800,
        "H1": 3_600,
        "H4": 14_400,
        "D1": 86_400,
    }
)
ARTIFACT_COLUMNS: Final[tuple[str, ...]] = (
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Spread",
    "RealVolume",
    "_SOURCE_FIRST_TS",
    "_SOURCE_LAST_TS",
    "_BAR_CLOSE_TS",
)
CSV_HEADER: Final[tuple[str, ...]] = ("time", *ARTIFACT_COLUMNS)

# Resource limits are parser safety authorities, never evidence/sufficiency
# thresholds.  They are deliberately not caller configurable.
MAX_ARTIFACT_BYTES: Final = 64 * 1024 * 1024 * 1024
MAX_ARTIFACT_ROWS: Final = 50_000_000
MAX_PROVENANCE_BYTES: Final = 4 * 1024 * 1024
MAX_CSV_LINE_BYTES: Final = 16 * 1024
IO_BUFFER_BYTES: Final = 1024 * 1024
MAX_DISCONTINUITIES: Final = 10_000
MAX_CALENDAR_RECEIPTS: Final = MAX_DISCONTINUITIES
MAX_CANONICAL_DEPTH: Final = 32
MAX_CANONICAL_NODES: Final = 200_000
MAX_CONTAINER_ITEMS: Final = 20_000
MAX_CANONICAL_STRING_CHARS: Final = 65_536
MAX_CANONICAL_INTEGER_BITS: Final = 256
MAX_INTERVAL_SECONDS: Final = 50 * 366 * 86_400

_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_EXPLANATION_CODES: Final[frozenset[str]] = frozenset(
    {
        "SESSION_CLOSED_WEEKEND",
        "SESSION_CLOSED_HOLIDAY",
        "SESSION_SCHEDULE_TRANSITION",
        "MARKET_SUSPENSION",
    }
)

_ENVELOPE_KEYS: Final = frozenset(
    {"schema_version", "key_id", "body", "signature_base64"}
)
_RECEIPT_BODY_KEYS: Final = frozenset(
    {
        "contract_version",
        "symbol",
        "timeframe",
        "cadence_seconds",
        "artifact_sha256",
        "provenance_sha256",
        "source_identity_sha256",
        "request_map_sha256",
        "request_start_utc",
        "request_end_exclusive_utc",
        "left_open_utc",
        "right_open_utc",
        "observed_delta_seconds",
        "discontinuity_kind",
        "skipped_nominal_open_count",
        "explanation_code",
        "calendar_release_id",
        "calendar_release_sha256",
        "issued_at_utc",
    }
)


def _runtime_authority_intact(
    *,
    _timeframes: Mapping[str, int] = TIMEFRAME_SECONDS,
    _timeframe_items: tuple[tuple[str, int], ...] = tuple(TIMEFRAME_SECONDS.items()),
    _artifact_columns: tuple[str, ...] = ARTIFACT_COLUMNS,
    _csv_header: tuple[str, ...] = CSV_HEADER,
    _contract_version: str = CONTINUITY_CONTRACT_VERSION,
    _policy_version: str = CONTINUITY_POLICY_SCHEMA_VERSION,
    _receipt_version: str = CALENDAR_RECEIPT_SCHEMA_VERSION,
    _authority_key_id: str = CALENDAR_AUTHORITY_KEY_ID,
    _authority_public_key: str = CALENDAR_AUTHORITY_PUBLIC_KEY_BASE64,
    _signature_domain: bytes = CALENDAR_SIGNATURE_DOMAIN,
    _provenance_version: str = PROVENANCE_SCHEMA_VERSION,
    _acquisition_version: str = ACQUISITION_CONTRACT_VERSION,
    _limits: tuple[int, ...] = (
        MAX_ARTIFACT_BYTES,
        MAX_ARTIFACT_ROWS,
        MAX_PROVENANCE_BYTES,
        MAX_CSV_LINE_BYTES,
        IO_BUFFER_BYTES,
        MAX_DISCONTINUITIES,
        MAX_CALENDAR_RECEIPTS,
        MAX_CANONICAL_DEPTH,
        MAX_CANONICAL_NODES,
        MAX_CONTAINER_ITEMS,
        MAX_CANONICAL_STRING_CHARS,
        MAX_CANONICAL_INTEGER_BITS,
        MAX_INTERVAL_SECONDS,
    ),
    _explanation_codes: frozenset[str] = _EXPLANATION_CODES,
    _envelope_keys: frozenset[str] = _ENVELOPE_KEYS,
    _body_keys: frozenset[str] = _RECEIPT_BODY_KEYS,
    _symbol_pattern: re.Pattern[str] = _SYMBOL_RE,
    _sha256_pattern: re.Pattern[str] = _SHA256_RE,
    _safe_id_pattern: re.Pattern[str] = _SAFE_ID_RE,
    _source_identity_function: Any = _canonical_source_identity,
    _public_key_type: Any = Ed25519PublicKey,
    _invalid_signature_type: Any = InvalidSignature,
) -> bool:
    """Detect runtime rebinding of cadence, trust-root, or parser authority."""

    try:
        return bool(
            TIMEFRAME_SECONDS is _timeframes
            and tuple(TIMEFRAME_SECONDS.items()) == _timeframe_items
            and ARTIFACT_COLUMNS == _artifact_columns
            and CSV_HEADER == _csv_header
            and CONTINUITY_CONTRACT_VERSION == _contract_version
            and CONTINUITY_POLICY_SCHEMA_VERSION == _policy_version
            and CALENDAR_RECEIPT_SCHEMA_VERSION == _receipt_version
            and CALENDAR_AUTHORITY_KEY_ID == _authority_key_id
            and CALENDAR_AUTHORITY_PUBLIC_KEY_BASE64 == _authority_public_key
            and CALENDAR_SIGNATURE_DOMAIN == _signature_domain
            and PROVENANCE_SCHEMA_VERSION == _provenance_version
            and ACQUISITION_CONTRACT_VERSION == _acquisition_version
            and (
                MAX_ARTIFACT_BYTES,
                MAX_ARTIFACT_ROWS,
                MAX_PROVENANCE_BYTES,
                MAX_CSV_LINE_BYTES,
                IO_BUFFER_BYTES,
                MAX_DISCONTINUITIES,
                MAX_CALENDAR_RECEIPTS,
                MAX_CANONICAL_DEPTH,
                MAX_CANONICAL_NODES,
                MAX_CONTAINER_ITEMS,
                MAX_CANONICAL_STRING_CHARS,
                MAX_CANONICAL_INTEGER_BITS,
                MAX_INTERVAL_SECONDS,
            )
            == _limits
            and _EXPLANATION_CODES is _explanation_codes
            and _ENVELOPE_KEYS is _envelope_keys
            and _RECEIPT_BODY_KEYS is _body_keys
            and _SYMBOL_RE is _symbol_pattern
            and _SHA256_RE is _sha256_pattern
            and _SAFE_ID_RE is _safe_id_pattern
            and _canonical_source_identity is _source_identity_function
            and Ed25519PublicKey is _public_key_type
            and InvalidSignature is _invalid_signature_type
        )
    except BaseException:
        return False


class NativeContinuityError(ValueError):
    """A deterministic, fail-closed continuity contract violation."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class InternalContinuityStatus(str, Enum):
    CONTIGUOUS = "CONTIGUOUS"
    AUTHENTICATED_EXPECTED_DISCONTINUITIES = "AUTHENTICATED_EXPECTED_DISCONTINUITIES"
    UNEXPLAINED_INTERNAL_DISCONTINUITIES = "UNEXPLAINED_INTERNAL_DISCONTINUITIES"


class BoundaryStatus(str, Enum):
    COMPLETE_REQUEST_SPAN = "COMPLETE_REQUEST_SPAN"
    LEADING_BOUNDARY_OR_PRELISTING_SHORTFALL = (
        "LEADING_BOUNDARY_OR_PRELISTING_SHORTFALL"
    )
    TRAILING_BOUNDARY_SHORTFALL = "TRAILING_BOUNDARY_SHORTFALL"
    BOTH_BOUNDARY_SHORTFALLS = "BOTH_BOUNDARY_SHORTFALLS"


class DiscontinuityClassification(str, Enum):
    EXPECTED_AUTHENTICATED = "EXPECTED_AUTHENTICATED"
    UNEXPLAINED = "UNEXPLAINED"


@dataclass(frozen=True)
class NativeContinuityPolicy:
    """Out-of-band exact expectations fixed before artifact inspection.

    ``policy_sha256`` is a corruption digest, not a signature or trust
    credential.  The caller/integration remains responsible for sourcing this
    policy from an authenticated manifest and trusted source identity.
    """

    schema_version: str
    symbol: str
    timeframe: str
    cadence_seconds: int
    artifact_sha256: str
    artifact_bytes: int
    artifact_rows: int
    provenance_sha256: str
    provenance_bytes: int
    source_identity_sha256: str
    request_map_sha256: str
    request_start_utc: str
    request_end_exclusive_utc: str
    policy_sha256: str


@dataclass(frozen=True)
class ContiguousSegment:
    ordinal: int
    start_open_utc: str
    end_exclusive_utc: str
    first_row_number: int
    last_row_number: int
    rows: int


@dataclass(frozen=True)
class NativeDiscontinuity:
    ordinal: int
    left_open_utc: str
    right_open_utc: str
    observed_delta_seconds: int
    cadence_seconds: int
    discontinuity_kind: str
    skipped_nominal_open_count: int
    classification: DiscontinuityClassification
    explanation_code: str | None
    calendar_release_id: str | None
    calendar_receipt_sha256: str | None


@dataclass(frozen=True)
class BoundaryShortfall:
    side: str
    requested_boundary_utc: str
    observed_boundary_utc: str
    duration_seconds: int
    classification: str


@dataclass(frozen=True)
class NativeContinuityResult:
    contract_version: str
    policy_sha256: str
    symbol: str
    timeframe: str
    cadence_seconds: int
    artifact_sha256: str
    artifact_bytes: int
    artifact_rows: int
    provenance_sha256: str
    source_identity_sha256: str
    request_map_sha256: str
    first_open_utc: str
    last_open_utc: str
    internal_status: InternalContinuityStatus
    boundary_status: BoundaryStatus
    segments: tuple[ContiguousSegment, ...]
    discontinuities: tuple[NativeDiscontinuity, ...]
    boundary_shortfalls: tuple[BoundaryShortfall, ...]
    expected_discontinuity_count: int
    unexplained_discontinuity_count: int
    geometry_sha256: str
    audit_sha256: str
    technical_continuity_passed: bool
    blocks_later_technical_activation: bool
    requested_span_complete: bool
    requires_preserved_segment_ledger: bool
    grants_trading_authority: bool = False
    grants_statistical_sufficiency_authority: bool = False


@dataclass(frozen=True)
class _VerifiedCalendarExplanation:
    body: Mapping[str, Any]
    receipt_sha256: str


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int
    size: int
    mtime_ns: int


def _fail(code: str) -> None:
    raise NativeContinuityError(code)


def _sha256(value: Any, *, code: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        _fail(code)
    return value


def _positive_int(value: Any, *, maximum: int, code: str) -> int:
    if type(value) is not int or value <= 0 or value > maximum:
        _fail(code)
    return value


def _nonnegative_int(value: Any, *, maximum: int, code: str) -> int:
    if type(value) is not int or value < 0 or value > maximum:
        _fail(code)
    return value


def _canonical_symbol(value: Any) -> str:
    if not isinstance(value, str):
        _fail("CONTINUITY_SYMBOL_INVALID")
    normalized = value.strip().upper()
    if value != normalized or _SYMBOL_RE.fullmatch(normalized) is None:
        _fail("CONTINUITY_SYMBOL_INVALID")
    return normalized


def _canonical_timeframe(value: Any) -> str:
    if not isinstance(value, str):
        _fail("CONTINUITY_TIMEFRAME_INVALID")
    normalized = value.strip().upper()
    if value != normalized or normalized not in TIMEFRAME_SECONDS:
        _fail("CONTINUITY_TIMEFRAME_INVALID")
    return normalized


def _parse_utc_second(value: Any, *, code: str) -> datetime:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 64
        or value != value.strip()
    ):
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise NativeContinuityError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        _fail(code)
    parsed = parsed.astimezone(UTC)
    if parsed.microsecond != 0:
        _fail(code)
    return parsed


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _bounded_normalize(value: Any) -> Any:
    """Return JSON-compatible data while enforcing depth/node/cycle caps."""

    nodes = 0
    active: set[int] = set()

    def visit(item: Any, depth: int) -> Any:
        nonlocal nodes
        nodes += 1
        if nodes > MAX_CANONICAL_NODES:
            _fail("CONTINUITY_CANONICAL_NODE_CAP_EXCEEDED")
        if depth > MAX_CANONICAL_DEPTH:
            _fail("CONTINUITY_CANONICAL_DEPTH_CAP_EXCEEDED")
        if item is None or type(item) is bool:
            return item
        if type(item) is int:
            if item.bit_length() > MAX_CANONICAL_INTEGER_BITS:
                _fail("CONTINUITY_CANONICAL_INTEGER_CAP_EXCEEDED")
            return item
        if type(item) is float:
            if not math.isfinite(item):
                _fail("CONTINUITY_CANONICAL_FLOAT_INVALID")
            return item
        if isinstance(item, str):
            if len(item) > MAX_CANONICAL_STRING_CHARS:
                _fail("CONTINUITY_CANONICAL_STRING_CAP_EXCEEDED")
            return item
        # Only inert built-in containers are accepted.  User-defined Mapping
        # or Sequence implementations can lie about length or iterate forever.
        if type(item) is dict:
            identity = id(item)
            if identity in active:
                _fail("CONTINUITY_CANONICAL_CYCLE_DETECTED")
            if len(item) > MAX_CONTAINER_ITEMS:
                _fail("CONTINUITY_CANONICAL_CONTAINER_CAP_EXCEEDED")
            active.add(identity)
            try:
                normalized: dict[str, Any] = {}
                for key, child in item.items():
                    if (
                        not isinstance(key, str)
                        or len(key) > MAX_CANONICAL_STRING_CHARS
                    ):
                        _fail("CONTINUITY_CANONICAL_KEY_INVALID")
                    if key in normalized:
                        _fail("CONTINUITY_CANONICAL_DUPLICATE_KEY")
                    normalized[key] = visit(child, depth + 1)
                return normalized
            finally:
                active.remove(identity)
        if type(item) in (list, tuple):
            identity = id(item)
            if identity in active:
                _fail("CONTINUITY_CANONICAL_CYCLE_DETECTED")
            if len(item) > MAX_CONTAINER_ITEMS:
                _fail("CONTINUITY_CANONICAL_CONTAINER_CAP_EXCEEDED")
            active.add(identity)
            try:
                return [visit(child, depth + 1) for child in item]
            finally:
                active.remove(identity)
        _fail("CONTINUITY_CANONICAL_TYPE_INVALID")

    return visit(value, 0)


def _canonical_bytes(value: Any) -> bytes:
    normalized = _bounded_normalize(value)
    try:
        return json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise NativeContinuityError("CONTINUITY_CANONICAL_ENCODING_FAILED") from exc


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _source_identity_sha256(value: Any) -> str:
    try:
        canonical = _canonical_source_identity(value)
    except BaseException as exc:
        raise NativeContinuityError("CONTINUITY_SOURCE_IDENTITY_INVALID") from exc
    return _canonical_sha256(canonical)


def _policy_payload(policy: NativeContinuityPolicy) -> dict[str, Any]:
    return {
        "schema_version": policy.schema_version,
        "symbol": policy.symbol,
        "timeframe": policy.timeframe,
        "cadence_seconds": policy.cadence_seconds,
        "artifact_sha256": policy.artifact_sha256,
        "artifact_bytes": policy.artifact_bytes,
        "artifact_rows": policy.artifact_rows,
        "provenance_sha256": policy.provenance_sha256,
        "provenance_bytes": policy.provenance_bytes,
        "source_identity_sha256": policy.source_identity_sha256,
        "request_map_sha256": policy.request_map_sha256,
        "request_start_utc": policy.request_start_utc,
        "request_end_exclusive_utc": policy.request_end_exclusive_utc,
    }


def build_native_continuity_policy(
    *,
    symbol: Any,
    timeframe: Any,
    expected_artifact_sha256: Any,
    expected_artifact_bytes: Any,
    expected_artifact_rows: Any,
    expected_provenance_sha256: Any,
    expected_provenance_bytes: Any,
    expected_source_environment: Any,
    expected_request_map_sha256: Any,
    request_start_utc: Any,
    request_end_exclusive_utc: Any,
) -> NativeContinuityPolicy:
    """Build an immutable exact expectation without minting any authority."""

    if not _runtime_authority_intact():
        _fail("CONTINUITY_RUNTIME_AUTHORITY_REBOUND")
    canonical_symbol = _canonical_symbol(symbol)
    canonical_timeframe = _canonical_timeframe(timeframe)
    start = _parse_utc_second(
        request_start_utc, code="CONTINUITY_REQUEST_START_INVALID"
    )
    end = _parse_utc_second(
        request_end_exclusive_utc,
        code="CONTINUITY_REQUEST_END_INVALID",
    )
    if start >= end or (end - start).total_seconds() > MAX_INTERVAL_SECONDS:
        _fail("CONTINUITY_REQUEST_INTERVAL_INVALID")
    provisional = NativeContinuityPolicy(
        schema_version=CONTINUITY_POLICY_SCHEMA_VERSION,
        symbol=canonical_symbol,
        timeframe=canonical_timeframe,
        cadence_seconds=TIMEFRAME_SECONDS[canonical_timeframe],
        artifact_sha256=_sha256(
            expected_artifact_sha256,
            code="CONTINUITY_ARTIFACT_SHA256_INVALID",
        ),
        artifact_bytes=_positive_int(
            expected_artifact_bytes,
            maximum=MAX_ARTIFACT_BYTES,
            code="CONTINUITY_ARTIFACT_BYTES_INVALID",
        ),
        artifact_rows=_positive_int(
            expected_artifact_rows,
            maximum=MAX_ARTIFACT_ROWS,
            code="CONTINUITY_ARTIFACT_ROWS_INVALID",
        ),
        provenance_sha256=_sha256(
            expected_provenance_sha256,
            code="CONTINUITY_PROVENANCE_SHA256_INVALID",
        ),
        provenance_bytes=_positive_int(
            expected_provenance_bytes,
            maximum=MAX_PROVENANCE_BYTES,
            code="CONTINUITY_PROVENANCE_BYTES_INVALID",
        ),
        source_identity_sha256=_source_identity_sha256(expected_source_environment),
        request_map_sha256=_sha256(
            expected_request_map_sha256,
            code="CONTINUITY_REQUEST_MAP_SHA256_INVALID",
        ),
        request_start_utc=_iso(start),
        request_end_exclusive_utc=_iso(end),
        policy_sha256="",
    )
    return NativeContinuityPolicy(
        **{
            **_policy_payload(provisional),
            "policy_sha256": _canonical_sha256(_policy_payload(provisional)),
        }
    )


def _validate_policy(value: Any) -> NativeContinuityPolicy:
    if type(value) is not NativeContinuityPolicy:
        _fail("CONTINUITY_POLICY_REQUIRED")
    if value.schema_version != CONTINUITY_POLICY_SCHEMA_VERSION:
        _fail("CONTINUITY_POLICY_SCHEMA_INVALID")
    symbol = _canonical_symbol(value.symbol)
    timeframe = _canonical_timeframe(value.timeframe)
    if value.cadence_seconds != TIMEFRAME_SECONDS[timeframe]:
        _fail("CONTINUITY_POLICY_CADENCE_MISMATCH")
    _sha256(value.artifact_sha256, code="CONTINUITY_ARTIFACT_SHA256_INVALID")
    _sha256(value.provenance_sha256, code="CONTINUITY_PROVENANCE_SHA256_INVALID")
    _sha256(
        value.source_identity_sha256, code="CONTINUITY_SOURCE_IDENTITY_SHA256_INVALID"
    )
    _sha256(value.request_map_sha256, code="CONTINUITY_REQUEST_MAP_SHA256_INVALID")
    _positive_int(
        value.artifact_bytes,
        maximum=MAX_ARTIFACT_BYTES,
        code="CONTINUITY_ARTIFACT_BYTES_INVALID",
    )
    _positive_int(
        value.artifact_rows,
        maximum=MAX_ARTIFACT_ROWS,
        code="CONTINUITY_ARTIFACT_ROWS_INVALID",
    )
    _positive_int(
        value.provenance_bytes,
        maximum=MAX_PROVENANCE_BYTES,
        code="CONTINUITY_PROVENANCE_BYTES_INVALID",
    )
    start = _parse_utc_second(
        value.request_start_utc, code="CONTINUITY_REQUEST_START_INVALID"
    )
    end = _parse_utc_second(
        value.request_end_exclusive_utc,
        code="CONTINUITY_REQUEST_END_INVALID",
    )
    if start >= end or (end - start).total_seconds() > MAX_INTERVAL_SECONDS:
        _fail("CONTINUITY_REQUEST_INTERVAL_INVALID")
    if symbol != value.symbol or timeframe != value.timeframe:
        _fail("CONTINUITY_POLICY_CANONICAL_IDENTITY_MISMATCH")
    _sha256(value.policy_sha256, code="CONTINUITY_POLICY_SHA256_INVALID")
    if _canonical_sha256(_policy_payload(value)) != value.policy_sha256:
        _fail("CONTINUITY_POLICY_DIGEST_MISMATCH")
    return value


def _file_identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=int(value.st_dev),
        inode=int(value.st_ino),
        size=int(value.st_size),
        mtime_ns=int(value.st_mtime_ns),
    )


def _unsafe_file_attributes(value: os.stat_result) -> bool:
    attributes = int(getattr(value, "st_file_attributes", 0) or 0)
    return bool(attributes & 0x400)  # Windows FILE_ATTRIBUTE_REPARSE_POINT


def _open_exact_regular(path: Path, *, expected_size: int, code_prefix: str):
    try:
        if path.is_symlink():
            _fail(f"{code_prefix}_UNSAFE_PATH")
        before_path = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(before_path.st_mode) or _unsafe_file_attributes(
            before_path
        ):
            _fail(f"{code_prefix}_UNSAFE_PATH")
        if int(before_path.st_size) != expected_size:
            _fail(f"{code_prefix}_BYTE_COUNT_MISMATCH")
        # A fixed-size buffer preserves bounded memory while avoiding one OS
        # read syscall per CSV row on multi-year M5 artifacts.
        handle = path.open("rb", buffering=IO_BUFFER_BYTES)
    except NativeContinuityError:
        raise
    except (OSError, ValueError) as exc:
        raise NativeContinuityError(f"{code_prefix}_OPEN_FAILED") from exc
    opened = handle.__enter__()
    try:
        identity = _file_identity(os.fstat(opened.fileno()))
        if identity != _file_identity(before_path):
            _fail(f"{code_prefix}_IDENTITY_CHANGED")
    except BaseException:
        handle.__exit__(None, None, None)
        raise
    return handle, opened, identity


def _verify_path_identity(
    path: Path, opened: Any, original: _FileIdentity, *, code: str
) -> None:
    try:
        current_handle = _file_identity(os.fstat(opened.fileno()))
        current_path = _file_identity(path.stat(follow_symlinks=False))
    except OSError as exc:
        raise NativeContinuityError(code) from exc
    if current_handle != original or current_path != original:
        _fail(code)


def _read_provenance(
    path: Path,
    policy: NativeContinuityPolicy,
) -> tuple[dict[str, Any], _FileIdentity]:
    context, handle, identity = _open_exact_regular(
        path,
        expected_size=policy.provenance_bytes,
        code_prefix="CONTINUITY_PROVENANCE",
    )
    try:
        raw = handle.read(MAX_PROVENANCE_BYTES + 1)
        if len(raw) != policy.provenance_bytes or len(raw) > MAX_PROVENANCE_BYTES:
            _fail("CONTINUITY_PROVENANCE_BYTE_COUNT_MISMATCH")
        if hashlib.sha256(raw).hexdigest() != policy.provenance_sha256:
            _fail("CONTINUITY_PROVENANCE_SHA256_MISMATCH")
        _verify_path_identity(
            path,
            handle,
            identity,
            code="CONTINUITY_PROVENANCE_IDENTITY_CHANGED",
        )
    finally:
        context.__exit__(None, None, None)

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                _fail("CONTINUITY_PROVENANCE_DUPLICATE_JSON_KEY")
            result[key] = value
        return result

    try:
        decoded = raw.decode("utf-8", errors="strict")
        parsed = json.loads(decoded, object_pairs_hook=reject_duplicate_keys)
    except NativeContinuityError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise NativeContinuityError("CONTINUITY_PROVENANCE_JSON_INVALID") from exc
    normalized = _bounded_normalize(parsed)
    if not isinstance(normalized, dict):
        _fail("CONTINUITY_PROVENANCE_OBJECT_REQUIRED")
    return normalized, identity


def _exact_interval(value: Any, *, code: str) -> tuple[str, str]:
    if not isinstance(value, Mapping) or set(value) != {
        "start_utc",
        "end_exclusive_utc",
    }:
        _fail(code)
    start = _parse_utc_second(value["start_utc"], code=code)
    end = _parse_utc_second(value["end_exclusive_utc"], code=code)
    if start >= end:
        _fail(code)
    return _iso(start), _iso(end)


def _validate_provenance_before_scan(
    provenance: Mapping[str, Any],
    policy: NativeContinuityPolicy,
) -> None:
    expected_scalars = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "canonical_symbol": policy.symbol,
        "timeframe": policy.timeframe,
        "timeframe_seconds": policy.cadence_seconds,
        "source_kind": "broker_native",
        "timezone": "UTC",
        "timestamp_semantics": "bar_open_utc",
        "bar_interval_semantics": "half_open_[bar_open,bar_close)",
        "bar_close_column": "_BAR_CLOSE_TS",
    }
    for field, expected in expected_scalars.items():
        if provenance.get(field) != expected:
            _fail(f"CONTINUITY_PROVENANCE_FIELD_MISMATCH:{field}")
    if provenance.get("symbol", policy.symbol) != policy.symbol:
        _fail("CONTINUITY_PROVENANCE_FIELD_MISMATCH:symbol")

    artifact = provenance.get("artifact")
    if not isinstance(artifact, Mapping) or set(artifact) != {
        "relative_path",
        "sha256",
        "bytes",
        "rows",
    }:
        _fail("CONTINUITY_PROVENANCE_ARTIFACT_INVALID")
    expected_artifact = {
        "relative_path": "bars.csv",
        "sha256": policy.artifact_sha256,
        "bytes": policy.artifact_bytes,
        "rows": policy.artifact_rows,
    }
    if dict(artifact) != expected_artifact:
        _fail("CONTINUITY_PROVENANCE_ARTIFACT_MISMATCH")
    if provenance.get("sha256") != policy.artifact_sha256:
        _fail("CONTINUITY_PROVENANCE_ARTIFACT_SHA256_MISMATCH")
    if provenance.get("rows") != policy.artifact_rows:
        _fail("CONTINUITY_PROVENANCE_ARTIFACT_ROWS_MISMATCH")
    if provenance.get("columns") != list(ARTIFACT_COLUMNS):
        _fail("CONTINUITY_PROVENANCE_COLUMNS_MISMATCH")

    observed_source_sha256 = _source_identity_sha256(
        provenance.get("source_environment")
    )
    if observed_source_sha256 != policy.source_identity_sha256:
        _fail("CONTINUITY_PROVENANCE_SOURCE_IDENTITY_MISMATCH")

    request = provenance.get("request")
    if not isinstance(request, Mapping):
        _fail("CONTINUITY_PROVENANCE_REQUEST_INVALID")
    if request.get("cell_timeframe") != policy.timeframe:
        _fail("CONTINUITY_PROVENANCE_REQUEST_TIMEFRAME_MISMATCH")
    if request.get("timeframe_request_map_sha256") != policy.request_map_sha256:
        _fail("CONTINUITY_PROVENANCE_REQUEST_MAP_MISMATCH")
    expected_interval = (policy.request_start_utc, policy.request_end_exclusive_utc)
    if (
        _exact_interval(
            request.get("source_request_interval"),
            code="CONTINUITY_PROVENANCE_SOURCE_INTERVAL_INVALID",
        )
        != expected_interval
    ):
        _fail("CONTINUITY_PROVENANCE_SOURCE_INTERVAL_MISMATCH")
    if request.get("native_fetch_start_utc") != policy.request_start_utc:
        _fail("CONTINUITY_PROVENANCE_NATIVE_START_MISMATCH")
    if (
        request.get("native_fetch_end_exclusive_utc")
        != policy.request_end_exclusive_utc
    ):
        _fail("CONTINUITY_PROVENANCE_NATIVE_END_MISMATCH")
    expected_alignment = (
        "broker_observed_successor_or_nominal_close_v1"
        if policy.timeframe in {"H4", "D1"}
        else "fixed_utc_native_boundary_v1"
    )
    if request.get("alignment_mode") != expected_alignment:
        _fail("CONTINUITY_PROVENANCE_ALIGNMENT_MODE_MISMATCH")


def _validate_provenance_after_scan(
    provenance: Mapping[str, Any],
    policy: NativeContinuityPolicy,
    *,
    first_open_utc: str,
    last_open_utc: str,
) -> None:
    first = _parse_utc_second(first_open_utc, code="CONTINUITY_FIRST_OPEN_INVALID")
    last = _parse_utc_second(last_open_utc, code="CONTINUITY_LAST_OPEN_INVALID")
    if (
        _parse_utc_second(
            provenance.get("start_ts"),
            code="CONTINUITY_PROVENANCE_START_TS_INVALID",
        )
        != first
    ):
        _fail("CONTINUITY_PROVENANCE_START_TS_MISMATCH")
    if (
        _parse_utc_second(
            provenance.get("end_ts"),
            code="CONTINUITY_PROVENANCE_END_TS_INVALID",
        )
        != last
    ):
        _fail("CONTINUITY_PROVENANCE_END_TS_MISMATCH")
    request = provenance.get("request")
    if (
        not isinstance(request, Mapping)
        or _parse_utc_second(
            request.get("last_fully_closed_bar_open_utc"),
            code="CONTINUITY_PROVENANCE_LAST_CLOSED_OPEN_INVALID",
        )
        != last
    ):
        _fail("CONTINUITY_PROVENANCE_LAST_CLOSED_OPEN_MISMATCH")
    if first < _parse_utc_second(
        policy.request_start_utc,
        code="CONTINUITY_REQUEST_START_INVALID",
    ):
        _fail("CONTINUITY_ARTIFACT_OPEN_BEFORE_REQUEST")
    request_end = _parse_utc_second(
        policy.request_end_exclusive_utc,
        code="CONTINUITY_REQUEST_END_INVALID",
    )
    if last + timedelta(seconds=policy.cadence_seconds) > request_end:
        _fail("CONTINUITY_ARTIFACT_BAR_CLOSE_AFTER_REQUEST")


def _receipt_common_expected(policy: NativeContinuityPolicy) -> dict[str, Any]:
    return {
        "contract_version": CONTINUITY_CONTRACT_VERSION,
        "symbol": policy.symbol,
        "timeframe": policy.timeframe,
        "cadence_seconds": policy.cadence_seconds,
        "artifact_sha256": policy.artifact_sha256,
        "provenance_sha256": policy.provenance_sha256,
        "source_identity_sha256": policy.source_identity_sha256,
        "request_map_sha256": policy.request_map_sha256,
        "request_start_utc": policy.request_start_utc,
        "request_end_exclusive_utc": policy.request_end_exclusive_utc,
    }


def _verify_calendar_receipt(
    value: Any,
    policy: NativeContinuityPolicy,
    *,
    _public_key_bytes: bytes = base64.b64decode(
        CALENDAR_AUTHORITY_PUBLIC_KEY_BASE64,
        validate=True,
    ),
    _public_key_type: Any = Ed25519PublicKey,
    _invalid_signature: Any = InvalidSignature,
) -> _VerifiedCalendarExplanation:
    normalized = _bounded_normalize(value)
    if not isinstance(normalized, dict) or set(normalized) != _ENVELOPE_KEYS:
        _fail("CONTINUITY_CALENDAR_RECEIPT_ENVELOPE_INVALID")
    if normalized["schema_version"] != CALENDAR_RECEIPT_SCHEMA_VERSION:
        _fail("CONTINUITY_CALENDAR_RECEIPT_SCHEMA_INVALID")
    if normalized["key_id"] != CALENDAR_AUTHORITY_KEY_ID:
        _fail("CONTINUITY_CALENDAR_RECEIPT_KEY_ID_INVALID")
    body = normalized["body"]
    if not isinstance(body, dict) or set(body) != _RECEIPT_BODY_KEYS:
        _fail("CONTINUITY_CALENDAR_RECEIPT_BODY_INVALID")
    for field, expected in _receipt_common_expected(policy).items():
        if body.get(field) != expected:
            _fail(f"CONTINUITY_CALENDAR_RECEIPT_BINDING_MISMATCH:{field}")

    left = _parse_utc_second(
        body.get("left_open_utc"),
        code="CONTINUITY_CALENDAR_RECEIPT_LEFT_OPEN_INVALID",
    )
    right = _parse_utc_second(
        body.get("right_open_utc"),
        code="CONTINUITY_CALENDAR_RECEIPT_RIGHT_OPEN_INVALID",
    )
    if left >= right:
        _fail("CONTINUITY_CALENDAR_RECEIPT_OPEN_ORDER_INVALID")
    observed_delta = _positive_int(
        body.get("observed_delta_seconds"),
        maximum=MAX_INTERVAL_SECONDS,
        code="CONTINUITY_CALENDAR_RECEIPT_DELTA_INVALID",
    )
    if observed_delta != int((right - left).total_seconds()):
        _fail("CONTINUITY_CALENDAR_RECEIPT_DELTA_MISMATCH")
    expected_kind = (
        "LONGER_THAN_NOMINAL"
        if observed_delta > policy.cadence_seconds
        else "SHORTER_THAN_NOMINAL"
    )
    if (
        observed_delta == policy.cadence_seconds
        or body.get("discontinuity_kind") != expected_kind
    ):
        _fail("CONTINUITY_CALENDAR_RECEIPT_KIND_INVALID")
    expected_skipped = max(0, (observed_delta - 1) // policy.cadence_seconds)
    if (
        _nonnegative_int(
            body.get("skipped_nominal_open_count"),
            maximum=MAX_ARTIFACT_ROWS,
            code="CONTINUITY_CALENDAR_RECEIPT_SKIPPED_COUNT_INVALID",
        )
        != expected_skipped
    ):
        _fail("CONTINUITY_CALENDAR_RECEIPT_SKIPPED_COUNT_MISMATCH")
    if body.get("explanation_code") not in _EXPLANATION_CODES:
        _fail("CONTINUITY_CALENDAR_RECEIPT_EXPLANATION_INVALID")
    if (
        expected_kind == "SHORTER_THAN_NOMINAL"
        and body.get("explanation_code") != "SESSION_SCHEDULE_TRANSITION"
    ):
        _fail("CONTINUITY_CALENDAR_RECEIPT_EXPLANATION_KIND_MISMATCH")
    release_id = body.get("calendar_release_id")
    if not isinstance(release_id, str) or _SAFE_ID_RE.fullmatch(release_id) is None:
        _fail("CONTINUITY_CALENDAR_RECEIPT_RELEASE_ID_INVALID")
    _sha256(
        body.get("calendar_release_sha256"),
        code="CONTINUITY_CALENDAR_RECEIPT_RELEASE_SHA256_INVALID",
    )
    issued = _parse_utc_second(
        body.get("issued_at_utc"),
        code="CONTINUITY_CALENDAR_RECEIPT_ISSUED_AT_INVALID",
    )
    if issued < _parse_utc_second(
        policy.request_end_exclusive_utc,
        code="CONTINUITY_REQUEST_END_INVALID",
    ):
        _fail("CONTINUITY_CALENDAR_RECEIPT_PREMATURE")

    signature_text = normalized.get("signature_base64")
    if not isinstance(signature_text, str) or len(signature_text) > 128:
        _fail("CONTINUITY_CALENDAR_RECEIPT_SIGNATURE_INVALID")
    try:
        signature = base64.b64decode(signature_text, validate=True)
    except (TypeError, ValueError) as exc:
        raise NativeContinuityError(
            "CONTINUITY_CALENDAR_RECEIPT_SIGNATURE_INVALID"
        ) from exc
    if len(signature) != 64 or len(_public_key_bytes) != 32:
        _fail("CONTINUITY_CALENDAR_RECEIPT_SIGNATURE_INVALID")
    body_bytes = _canonical_bytes(body)
    try:
        _public_key_type.from_public_bytes(_public_key_bytes).verify(
            signature,
            CALENDAR_SIGNATURE_DOMAIN + body_bytes,
        )
    except (_invalid_signature, TypeError, ValueError) as exc:
        raise NativeContinuityError(
            "CONTINUITY_CALENDAR_RECEIPT_SIGNATURE_INVALID"
        ) from exc
    return _VerifiedCalendarExplanation(
        body=MappingProxyType(body),
        receipt_sha256=hashlib.sha256(_canonical_bytes(normalized)).hexdigest(),
    )


def _prepare_calendar_receipts(
    values: Any,
    policy: NativeContinuityPolicy,
) -> dict[tuple[str, str], _VerifiedCalendarExplanation]:
    if type(values) not in (list, tuple):
        _fail("CONTINUITY_CALENDAR_RECEIPTS_COLLECTION_INVALID")
    if len(values) > MAX_CALENDAR_RECEIPTS:
        _fail("CONTINUITY_CALENDAR_RECEIPT_CAP_EXCEEDED")
    prepared: dict[tuple[str, str], _VerifiedCalendarExplanation] = {}
    for value in values:
        receipt = _verify_calendar_receipt(value, policy)
        key = (
            str(receipt.body["left_open_utc"]),
            str(receipt.body["right_open_utc"]),
        )
        if key in prepared:
            _fail("CONTINUITY_CALENDAR_RECEIPT_DUPLICATE")
        prepared[key] = receipt
    return prepared


def _csv_row(raw: bytes, *, expected_columns: int, row_number: int) -> list[str]:
    if len(raw) > MAX_CSV_LINE_BYTES:
        _fail("CONTINUITY_CSV_LINE_CAP_EXCEEDED")
    if not raw.endswith(b"\n") or b"\r" in raw or b"\x00" in raw:
        _fail("CONTINUITY_CSV_LINE_ENDING_INVALID")
    try:
        text = raw[:-1].decode("utf-8", errors="strict")
        parsed = list(csv.reader([text], strict=True))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise NativeContinuityError("CONTINUITY_CSV_ROW_INVALID") from exc
    if len(parsed) != 1 or len(parsed[0]) != expected_columns:
        _fail(f"CONTINUITY_CSV_COLUMN_COUNT_INVALID:{row_number}")
    return parsed[0]


def _segment_payload(value: ContiguousSegment) -> dict[str, Any]:
    return {
        "ordinal": value.ordinal,
        "start_open_utc": value.start_open_utc,
        "end_exclusive_utc": value.end_exclusive_utc,
        "first_row_number": value.first_row_number,
        "last_row_number": value.last_row_number,
        "rows": value.rows,
    }


def _discontinuity_payload(value: NativeDiscontinuity) -> dict[str, Any]:
    return {
        "ordinal": value.ordinal,
        "left_open_utc": value.left_open_utc,
        "right_open_utc": value.right_open_utc,
        "observed_delta_seconds": value.observed_delta_seconds,
        "cadence_seconds": value.cadence_seconds,
        "discontinuity_kind": value.discontinuity_kind,
        "skipped_nominal_open_count": value.skipped_nominal_open_count,
        "classification": value.classification.value,
        "explanation_code": value.explanation_code,
        "calendar_release_id": value.calendar_release_id,
        "calendar_receipt_sha256": value.calendar_receipt_sha256,
    }


def _discontinuity_geometry_payload(value: NativeDiscontinuity) -> dict[str, Any]:
    """Pagination/evidence-independent geometry for one adjacent-open break."""

    return {
        "ordinal": value.ordinal,
        "left_open_utc": value.left_open_utc,
        "right_open_utc": value.right_open_utc,
        "observed_delta_seconds": value.observed_delta_seconds,
        "cadence_seconds": value.cadence_seconds,
        "discontinuity_kind": value.discontinuity_kind,
        "skipped_nominal_open_count": value.skipped_nominal_open_count,
    }


def _boundary_payload(value: BoundaryShortfall) -> dict[str, Any]:
    return {
        "side": value.side,
        "requested_boundary_utc": value.requested_boundary_utc,
        "observed_boundary_utc": value.observed_boundary_utc,
        "duration_seconds": value.duration_seconds,
        "classification": value.classification,
    }


def _scan_bars(
    path: Path,
    policy: NativeContinuityPolicy,
    receipts: Mapping[tuple[str, str], _VerifiedCalendarExplanation],
) -> tuple[
    str,
    int,
    int,
    str,
    str,
    tuple[ContiguousSegment, ...],
    tuple[NativeDiscontinuity, ...],
]:
    context, handle, identity = _open_exact_regular(
        path,
        expected_size=policy.artifact_bytes,
        code_prefix="CONTINUITY_ARTIFACT",
    )
    digest = hashlib.sha256()
    byte_count = 0
    row_count = 0
    previous: datetime | None = None
    first: datetime | None = None
    segment_start: datetime | None = None
    segment_first_row = 1
    segments: list[ContiguousSegment] = []
    discontinuities: list[NativeDiscontinuity] = []
    used_receipts: set[tuple[str, str]] = set()
    cadence = timedelta(seconds=policy.cadence_seconds)
    try:
        header_raw = handle.readline(MAX_CSV_LINE_BYTES + 1)
        if not header_raw:
            _fail("CONTINUITY_CSV_HEADER_MISSING")
        digest.update(header_raw)
        byte_count += len(header_raw)
        header = _csv_row(
            header_raw,
            expected_columns=len(CSV_HEADER),
            row_number=0,
        )
        if tuple(header) != CSV_HEADER:
            _fail("CONTINUITY_CSV_HEADER_MISMATCH")

        while True:
            raw = handle.readline(MAX_CSV_LINE_BYTES + 1)
            if not raw:
                break
            digest.update(raw)
            byte_count += len(raw)
            row_count += 1
            if row_count > policy.artifact_rows or row_count > MAX_ARTIFACT_ROWS:
                _fail("CONTINUITY_ARTIFACT_ROW_CAP_EXCEEDED")
            row = _csv_row(
                raw,
                expected_columns=len(CSV_HEADER),
                row_number=row_count,
            )
            opened = _parse_utc_second(
                row[0],
                code=f"CONTINUITY_BAR_OPEN_INVALID:{row_count}",
            )
            if first is None:
                first = opened
                segment_start = opened
            if previous is not None:
                if opened == previous:
                    _fail(f"CONTINUITY_DUPLICATE_BAR_OPEN:{row_count}")
                if opened < previous:
                    _fail(f"CONTINUITY_NON_MONOTONIC_BAR_OPEN:{row_count}")
                delta_seconds = int((opened - previous).total_seconds())
                if delta_seconds != policy.cadence_seconds:
                    if len(discontinuities) >= MAX_DISCONTINUITIES:
                        _fail("CONTINUITY_DISCONTINUITY_CAP_EXCEEDED")
                    assert segment_start is not None
                    segments.append(
                        ContiguousSegment(
                            ordinal=len(segments) + 1,
                            start_open_utc=_iso(segment_start),
                            end_exclusive_utc=_iso(previous + cadence),
                            first_row_number=segment_first_row,
                            last_row_number=row_count - 1,
                            rows=(row_count - 1) - segment_first_row + 1,
                        )
                    )
                    left_iso = _iso(previous)
                    right_iso = _iso(opened)
                    key = (left_iso, right_iso)
                    receipt = receipts.get(key)
                    skipped = max(
                        0,
                        (delta_seconds - 1) // policy.cadence_seconds,
                    )
                    kind = (
                        "LONGER_THAN_NOMINAL"
                        if delta_seconds > policy.cadence_seconds
                        else "SHORTER_THAN_NOMINAL"
                    )
                    if receipt is not None:
                        body = receipt.body
                        if (
                            body["observed_delta_seconds"] != delta_seconds
                            or body["discontinuity_kind"] != kind
                            or body["skipped_nominal_open_count"] != skipped
                        ):
                            _fail("CONTINUITY_CALENDAR_RECEIPT_EXACT_COVERAGE_MISMATCH")
                        used_receipts.add(key)
                        classification = (
                            DiscontinuityClassification.EXPECTED_AUTHENTICATED
                        )
                        explanation_code = str(body["explanation_code"])
                        release_id = str(body["calendar_release_id"])
                        receipt_sha256 = receipt.receipt_sha256
                    else:
                        classification = DiscontinuityClassification.UNEXPLAINED
                        explanation_code = None
                        release_id = None
                        receipt_sha256 = None
                    discontinuities.append(
                        NativeDiscontinuity(
                            ordinal=len(discontinuities) + 1,
                            left_open_utc=left_iso,
                            right_open_utc=right_iso,
                            observed_delta_seconds=delta_seconds,
                            cadence_seconds=policy.cadence_seconds,
                            discontinuity_kind=kind,
                            skipped_nominal_open_count=skipped,
                            classification=classification,
                            explanation_code=explanation_code,
                            calendar_release_id=release_id,
                            calendar_receipt_sha256=receipt_sha256,
                        )
                    )
                    segment_start = opened
                    segment_first_row = row_count
            previous = opened

        _verify_path_identity(
            path,
            handle,
            identity,
            code="CONTINUITY_ARTIFACT_IDENTITY_CHANGED",
        )
    finally:
        context.__exit__(None, None, None)

    if first is None or previous is None or segment_start is None:
        _fail("CONTINUITY_ARTIFACT_HAS_NO_ROWS")
    if byte_count != policy.artifact_bytes:
        _fail("CONTINUITY_ARTIFACT_BYTE_COUNT_MISMATCH")
    observed_sha256 = digest.hexdigest()
    if observed_sha256 != policy.artifact_sha256:
        _fail("CONTINUITY_ARTIFACT_SHA256_MISMATCH")
    if row_count != policy.artifact_rows:
        _fail("CONTINUITY_ARTIFACT_ROW_COUNT_MISMATCH")
    unused = set(receipts) - used_receipts
    if unused:
        _fail("CONTINUITY_CALENDAR_RECEIPT_UNUSED")
    segments.append(
        ContiguousSegment(
            ordinal=len(segments) + 1,
            start_open_utc=_iso(segment_start),
            end_exclusive_utc=_iso(previous + cadence),
            first_row_number=segment_first_row,
            last_row_number=row_count,
            rows=row_count - segment_first_row + 1,
        )
    )
    return (
        observed_sha256,
        byte_count,
        row_count,
        _iso(first),
        _iso(previous),
        tuple(segments),
        tuple(discontinuities),
    )


def _boundaries(
    policy: NativeContinuityPolicy,
    *,
    first_open_utc: str,
    last_open_utc: str,
) -> tuple[BoundaryStatus, tuple[BoundaryShortfall, ...]]:
    start = _parse_utc_second(
        policy.request_start_utc, code="CONTINUITY_REQUEST_START_INVALID"
    )
    end = _parse_utc_second(
        policy.request_end_exclusive_utc,
        code="CONTINUITY_REQUEST_END_INVALID",
    )
    first = _parse_utc_second(first_open_utc, code="CONTINUITY_FIRST_OPEN_INVALID")
    last_close = _parse_utc_second(
        last_open_utc, code="CONTINUITY_LAST_OPEN_INVALID"
    ) + timedelta(seconds=policy.cadence_seconds)
    values: list[BoundaryShortfall] = []
    if first > start:
        values.append(
            BoundaryShortfall(
                side="LEADING",
                requested_boundary_utc=_iso(start),
                observed_boundary_utc=_iso(first),
                duration_seconds=int((first - start).total_seconds()),
                classification="BOUNDARY_OR_PRELISTING_SHORTFALL_NOT_INTERNAL_GAP",
            )
        )
    if last_close < end:
        values.append(
            BoundaryShortfall(
                side="TRAILING",
                requested_boundary_utc=_iso(end),
                observed_boundary_utc=_iso(last_close),
                duration_seconds=int((end - last_close).total_seconds()),
                classification="TRAILING_BOUNDARY_SHORTFALL_NOT_INTERNAL_GAP",
            )
        )
    sides = {value.side for value in values}
    if not sides:
        status = BoundaryStatus.COMPLETE_REQUEST_SPAN
    elif sides == {"LEADING"}:
        status = BoundaryStatus.LEADING_BOUNDARY_OR_PRELISTING_SHORTFALL
    elif sides == {"TRAILING"}:
        status = BoundaryStatus.TRAILING_BOUNDARY_SHORTFALL
    else:
        status = BoundaryStatus.BOTH_BOUNDARY_SHORTFALLS
    return status, tuple(values)


def audit_native_continuity(
    *,
    bars_path: str | os.PathLike[str],
    provenance_path: str | os.PathLike[str],
    policy: NativeContinuityPolicy,
    calendar_receipts: Sequence[Mapping[str, Any]] = (),
) -> NativeContinuityResult:
    """Stream and classify one exact immutable native artifact.

    No pagination or caller-selected tolerance is accepted.  Every adjacent
    open is compared with the timeframe's fixed native cadence.  A mismatch is
    expected only when an externally signed receipt covers those exact two
    opens and the exact artifact/source/request identities.
    """

    if not _runtime_authority_intact():
        _fail("CONTINUITY_RUNTIME_AUTHORITY_REBOUND")
    trusted = _validate_policy(policy)
    bars = Path(bars_path)
    provenance_file = Path(provenance_path)
    if bars.name != "bars.csv" or provenance_file.name != "provenance.json":
        _fail("CONTINUITY_LEAF_FILENAMES_INVALID")
    try:
        if bars.parent.resolve(strict=True) != provenance_file.parent.resolve(
            strict=True
        ):
            _fail("CONTINUITY_LEAF_PARENT_MISMATCH")
    except NativeContinuityError:
        raise
    except OSError as exc:
        raise NativeContinuityError("CONTINUITY_LEAF_PARENT_INVALID") from exc

    provenance, provenance_identity = _read_provenance(provenance_file, trusted)
    _validate_provenance_before_scan(provenance, trusted)
    receipts = _prepare_calendar_receipts(calendar_receipts, trusted)
    (
        artifact_sha256,
        artifact_bytes,
        artifact_rows,
        first_open_utc,
        last_open_utc,
        segments,
        discontinuities,
    ) = _scan_bars(bars, trusted, receipts)
    _validate_provenance_after_scan(
        provenance,
        trusted,
        first_open_utc=first_open_utc,
        last_open_utc=last_open_utc,
    )
    try:
        if (
            _file_identity(provenance_file.stat(follow_symlinks=False))
            != provenance_identity
        ):
            _fail("CONTINUITY_PROVENANCE_IDENTITY_CHANGED_DURING_AUDIT")
    except NativeContinuityError:
        raise
    except OSError as exc:
        raise NativeContinuityError(
            "CONTINUITY_PROVENANCE_IDENTITY_CHANGED_DURING_AUDIT"
        ) from exc

    boundary_status, boundary_shortfalls = _boundaries(
        trusted,
        first_open_utc=first_open_utc,
        last_open_utc=last_open_utc,
    )
    expected_count = sum(
        value.classification is DiscontinuityClassification.EXPECTED_AUTHENTICATED
        for value in discontinuities
    )
    unexplained_count = sum(
        value.classification is DiscontinuityClassification.UNEXPLAINED
        for value in discontinuities
    )
    if unexplained_count:
        internal_status = InternalContinuityStatus.UNEXPLAINED_INTERNAL_DISCONTINUITIES
    elif expected_count:
        internal_status = (
            InternalContinuityStatus.AUTHENTICATED_EXPECTED_DISCONTINUITIES
        )
    else:
        internal_status = InternalContinuityStatus.CONTIGUOUS

    geometry_payload = {
        "contract_version": CONTINUITY_CONTRACT_VERSION,
        "symbol": trusted.symbol,
        "timeframe": trusted.timeframe,
        "cadence_seconds": trusted.cadence_seconds,
        "artifact_sha256": artifact_sha256,
        "artifact_bytes": artifact_bytes,
        "artifact_rows": artifact_rows,
        "first_open_utc": first_open_utc,
        "last_open_utc": last_open_utc,
        "segments": [_segment_payload(value) for value in segments],
        "discontinuities": [
            _discontinuity_geometry_payload(value) for value in discontinuities
        ],
        "boundary_shortfalls": [
            _boundary_payload(value) for value in boundary_shortfalls
        ],
    }
    geometry_sha256 = _canonical_sha256(geometry_payload)
    technical_passed = unexplained_count == 0
    requested_span_complete = boundary_status is BoundaryStatus.COMPLETE_REQUEST_SPAN
    audit_payload = {
        "contract_version": CONTINUITY_CONTRACT_VERSION,
        "policy_sha256": trusted.policy_sha256,
        "provenance_sha256": trusted.provenance_sha256,
        "source_identity_sha256": trusted.source_identity_sha256,
        "request_map_sha256": trusted.request_map_sha256,
        "geometry_sha256": geometry_sha256,
        "discontinuity_evidence": [
            _discontinuity_payload(value) for value in discontinuities
        ],
        "internal_status": internal_status.value,
        "boundary_status": boundary_status.value,
        "technical_continuity_passed": technical_passed,
        "blocks_later_technical_activation": not technical_passed,
        "requested_span_complete": requested_span_complete,
        "grants_trading_authority": False,
        "grants_statistical_sufficiency_authority": False,
    }
    return NativeContinuityResult(
        contract_version=CONTINUITY_CONTRACT_VERSION,
        policy_sha256=trusted.policy_sha256,
        symbol=trusted.symbol,
        timeframe=trusted.timeframe,
        cadence_seconds=trusted.cadence_seconds,
        artifact_sha256=artifact_sha256,
        artifact_bytes=artifact_bytes,
        artifact_rows=artifact_rows,
        provenance_sha256=trusted.provenance_sha256,
        source_identity_sha256=trusted.source_identity_sha256,
        request_map_sha256=trusted.request_map_sha256,
        first_open_utc=first_open_utc,
        last_open_utc=last_open_utc,
        internal_status=internal_status,
        boundary_status=boundary_status,
        segments=segments,
        discontinuities=discontinuities,
        boundary_shortfalls=boundary_shortfalls,
        expected_discontinuity_count=expected_count,
        unexplained_discontinuity_count=unexplained_count,
        geometry_sha256=geometry_sha256,
        audit_sha256=_canonical_sha256(audit_payload),
        technical_continuity_passed=technical_passed,
        blocks_later_technical_activation=not technical_passed,
        requested_span_complete=requested_span_complete,
        requires_preserved_segment_ledger=bool(discontinuities or boundary_shortfalls),
    )


__all__ = [
    "ARTIFACT_COLUMNS",
    "BoundaryShortfall",
    "BoundaryStatus",
    "CALENDAR_AUTHORITY_KEY_ID",
    "CALENDAR_RECEIPT_SCHEMA_VERSION",
    "CONTINUITY_CONTRACT_VERSION",
    "CSV_HEADER",
    "ContiguousSegment",
    "DiscontinuityClassification",
    "InternalContinuityStatus",
    "MAX_ARTIFACT_BYTES",
    "MAX_ARTIFACT_ROWS",
    "MAX_CALENDAR_RECEIPTS",
    "MAX_CANONICAL_DEPTH",
    "MAX_CANONICAL_NODES",
    "MAX_CONTAINER_ITEMS",
    "MAX_CSV_LINE_BYTES",
    "MAX_DISCONTINUITIES",
    "MAX_PROVENANCE_BYTES",
    "NativeContinuityError",
    "NativeContinuityPolicy",
    "NativeContinuityResult",
    "NativeDiscontinuity",
    "TIMEFRAME_SECONDS",
    "audit_native_continuity",
    "build_native_continuity_policy",
]
