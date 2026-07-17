"""Detached, verify-only authority for resealing complete native V5 runs.

The acquisition writer's manifest pins bar hashes and rows, but it does not pin
provenance bytes.  This module materialises a bounded V3 signing body that binds
all 600 publishable success/partial cells to the exact manifest, bars,
provenance, source identity, request map, and per-timeframe source intervals.
Unresolved acquisition failures are retryable attempt receipts outside a final
manifest; they cannot be promoted by this production reseal contract.  A final
run is resealable only after every retry receipt and its reserved directory have
been removed by the acquisition writer.

Materialisation grants no authority.  A reseal becomes trusted only after an
out-of-process Ed25519 authority signs the exact domain-separated body and this
module independently re-reads every source artifact while verifying it.  No
secret signing material or signing operation exists here.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, BinaryIO, Final, Iterator, Mapping, Sequence

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from pm_native_csv_validation import (
    NativeCsvValidationError,
    scan_native_csv_stream,
)
from pm_native_request_contract import (
    ACQUISITION_CONTRACT_VERSION,
    MANIFEST_SCHEMA_VERSION,
    MAX_TOTAL_ARTIFACT_BYTES,
    PRODUCTION_ARTIFACT_COLUMNS,
    PRODUCTION_SYMBOL_ORDER,
    PRODUCTION_SYMBOL_ORDER_SHA256,
    PRODUCTION_TIMEFRAME_ORDER,
    PRODUCTION_TIMEFRAME_ORDER_SHA256,
    PROVENANCE_SCHEMA_VERSION,
    TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION,
    TrustedV5Policy,
    V5ArtifactObservation,
    NativeRequestContractError,
    build_v5_cell_validation_context,
    build_trusted_v5_policy,
    canonical_source_identity,
    canonical_json_bytes,
    validate_source_identity_match,
    validate_v5_cell_status,
    validate_v5_leaf_request,
    validate_v5_production_manifest,
)


UTC: Final = timezone.utc

RUN_RESEAL_CONTRACT_VERSION: Final = "fxpm.native_history.run_reseal.v3"
RUN_RESEAL_EXPECTED_AUTHORITY_SCHEMA_VERSION: Final = (
    "fxpm.native_history.run_reseal_expected_authority.v1"
)
RUN_RESEAL_SIGNING_REQUEST_SCHEMA_VERSION: Final = (
    "fxpm.native_history.run_reseal_signing_request.v2"
)
RUN_RESEAL_ENVELOPE_SCHEMA_VERSION: Final = "fxpm.native_history.run_reseal_envelope.v3"
RUN_RESEAL_TRUST_ROOT_SCHEMA_VERSION: Final = (
    "fxpm.native_history.run_reseal_trust_root.v1"
)
RUN_RESEAL_VERIFICATION_RECEIPT_SCHEMA_VERSION: Final = (
    "fxpm.native_history.run_reseal_verification_receipt.v1"
)
RUN_RESEAL_SIGNATURE_DOMAIN: Final = b"fxpm.native-history.run-reseal.v3\x00"
RUN_RESEAL_SIGNATURE_ALGORITHM: Final = "Ed25519"
RUN_RESEAL_AUTHORITY_SCOPE: Final = (
    "externally_pinned_native_run_exact_bytes_source_request_and_result_bindings_only"
)

PRODUCTION_CELL_COUNT: Final = len(PRODUCTION_SYMBOL_ORDER) * len(
    PRODUCTION_TIMEFRAME_ORDER
)
# Parser and filesystem limits are safety bounds, not evidence thresholds.
MAX_MANIFEST_BYTES: Final = 16 * 1024 * 1024
MAX_RESUME_SEED_BYTES: Final = 16 * 1024 * 1024
MAX_PROVENANCE_BYTES: Final = 4 * 1024 * 1024
MAX_ARTIFACT_BYTES: Final = 64 * 1024 * 1024 * 1024
MAX_TOTAL_PROVENANCE_BYTES: Final = PRODUCTION_CELL_COUNT * MAX_PROVENANCE_BYTES
MAX_EXTERNAL_JSON_BYTES: Final = 4 * 1024 * 1024
MAX_EXTERNAL_JSON_DEPTH: Final = 16
MAX_EXTERNAL_JSON_STRUCTURAL_TOKENS: Final = 100_000
MAX_CSV_LINE_BYTES: Final = 16 * 1024
MAX_ARTIFACT_ROWS: Final = 50_000_000
IO_BUFFER_BYTES: Final = 1024 * 1024

_FILE_ATTRIBUTE_REPARSE_POINT: Final = 0x400
_RUN_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_KEY_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_RESUME_SEED_FILENAME: Final = "resume_contract.json"
_RESUME_SEED_SCHEMA_VERSION: Final = "fxpm.native_history.resume_seed.v2"
_ATTEMPT_RECEIPTS_DIR: Final = "_attempts"
_RESUME_SEED_KEYS: Final = frozenset(
    {
        "schema_version",
        "immutable_record",
        "run_id",
        "acquisition_started_utc",
        "request",
        "source_identity",
        "symbols",
        "timeframes",
        "production_authoritative",
    }
)

_ENVELOPE_KEYS: Final = frozenset(
    {"schema_version", "key_id", "body", "signature_base64"}
)
_TRUST_ROOT_KEYS: Final = frozenset({"schema_version", "key_id", "public_key_base64"})
_EXPECTED_AUTHORITY_MAPPING_KEYS: Final = frozenset(
    {
        "schema_version",
        "run_id",
        "origin_manifest_sha256",
        "trusted_policy_sha256",
        "source_identity",
        "source_identity_sha256",
        "common_request",
        "requested_information_cutoff_utc",
        "common_as_of_utc",
        "timeframe_request_map_sha256",
        "symbols",
        "timeframes",
        "retries",
        "authority_sha256",
    }
)
_SIGNING_REQUEST_KEYS: Final = frozenset(
    {
        "schema_version",
        "key_id",
        "signature_algorithm",
        "signature_domain_base64",
        "canonicalization",
        "signature_payload_base64",
        "signature_payload_sha256",
        "body",
        "body_sha256",
        "grants_trading_authority",
        "grants_statistical_sufficiency_authority",
        "grants_profitability_authority",
    }
)
_BODY_KEYS: Final = frozenset(
    {
        "contract_version",
        "authority_scope",
        "run_id",
        "origin_manifest_schema_version",
        "origin_manifest_relative_path",
        "origin_manifest_sha256",
        "origin_manifest_bytes",
        "resume_seed_schema_version",
        "resume_seed_relative_path",
        "resume_seed_sha256",
        "resume_seed_bytes",
        "expected_authority_schema_version",
        "expected_authority_sha256",
        "trusted_policy_sha256",
        "expected_requested_information_cutoff_utc",
        "expected_common_as_of_utc",
        "acquisition_started_utc",
        "acquisition_completed_utc",
        "source_identity",
        "source_identity_sha256",
        "common_request",
        "common_request_sha256",
        "timeframe_request_map_sha256",
        "symbols",
        "symbol_order_sha256",
        "timeframes",
        "timeframe_order_sha256",
        "retries",
        "cells",
        "cells_sha256",
        "cell_count",
        "artifact_cell_count",
        "issued_at_utc",
        "grants_trading_authority",
        "grants_statistical_sufficiency_authority",
        "grants_profitability_authority",
    }
)

_PROHIBITED_AUTHORITY_FIELDS: Final = frozenset(
    {
        "tradeable",
        "production_eligible",
        "funded_live_eligible",
        "statistical_authority",
        "statistical_sufficiency_receipt",
        "promotion_authorization",
        "no_trade",
        "grants_trading_authority",
        "grants_statistical_sufficiency_authority",
        "grants_profitability_authority",
        "economic_verdict",
        "source_unavailable_proof",
        "eligible_for_live_trading",
        "profitable",
    }
)

# Current publishable acquisition-writer schema after
# ``NativeHistoryStore.commit_leaf``.
_PUBLISHABLE_PROVENANCE_KEYS: Final = frozenset(
    {
        "schema_version",
        "acquisition_contract_version",
        "immutable_record",
        "run_id",
        "status",
        "failure_status",
        "eligible_for_optimization",
        "eligible_for_downstream_validation",
        "statistical_sufficiency_assessed",
        "canonical_symbol",
        "symbol",
        "broker_symbol",
        "timeframe",
        "source_kind",
        "timezone",
        "timeframe_seconds",
        "request",
        "source_environment",
        "instrument",
        "acquisition_started_utc",
        "acquisition_completed_utc",
        "pagination",
        "coverage",
        "expected_session_coverage_by_partition",
        "observed_session_bars_by_partition",
        "expected_session_bars_by_partition",
        "expected_session_coverage_details_by_partition",
        "quality",
        "columns",
        "timestamp_semantics",
        "bar_interval_semantics",
        "bar_close_column",
        "output_storage_resource_snapshot",
        "artifact",
        "sha256",
        "rows",
        "start_ts",
        "end_ts",
        "complete",
    }
)
_CELL_KEYS: Final = frozenset(
    {
        "ordinal",
        "cell_id",
        "symbol",
        "timeframe",
        "relative_leaf",
        "manifest_result_sha256",
        "acquisition_status",
        "failure_status",
        "resume_disposition",
        "request_start_utc",
        "request_end_exclusive_utc",
        "artifact_present",
        "bars_relative_path",
        "bars_sha256",
        "bars_bytes",
        "bars_rows",
        "first_open_utc",
        "last_open_utc",
        "last_close_utc",
        "provenance_relative_path",
        "provenance_sha256",
        "provenance_bytes",
        "provenance_request_interval_bound",
    }
)


class NativeRunResealError(ValueError):
    """A deterministic, fail-closed detached-reseal violation."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class RunResealTrustRoot:
    """Public verification authority supplied out of band."""

    schema_version: str
    key_id: str
    public_key_base64: str


@dataclass(frozen=True)
class RunResealExpectedAuthority:
    """Immutable caller-supplied authority for one exact acquisition run.

    The candidate manifest is never allowed to construct this object.  The
    caller must pin both the independent V5 policy and the exact immutable
    manifest bytes before materialisation or verification begins.
    """

    schema_version: str
    run_id: str
    origin_manifest_sha256: str
    trusted_policy: TrustedV5Policy
    trusted_policy_sha256: str
    source_identity_json: bytes
    source_identity_sha256: str
    requested_information_cutoff_utc: str
    common_as_of_utc: str
    timeframe_request_map_sha256: str
    authority_sha256: str


_VERIFIED_RUN_RESEAL_SEAL = object()


class VerifiedRunReseal:
    """Calculator-constructed technical lineage; never a trading verdict."""

    __slots__ = (
        "schema_version",
        "run_id",
        "key_id",
        "body_sha256",
        "envelope_sha256",
        "origin_manifest_sha256",
        "expected_authority_sha256",
        "trusted_policy_sha256",
        "cell_count",
        "artifact_cell_count",
        "issued_at_utc",
        "validated_at_utc",
        "body",
        "grants_trading_authority",
        "grants_statistical_sufficiency_authority",
        "grants_profitability_authority",
        "_sealed",
    )

    def __init__(
        self,
        *,
        schema_version: str,
        run_id: str,
        key_id: str,
        body_sha256: str,
        envelope_sha256: str,
        origin_manifest_sha256: str,
        expected_authority_sha256: str,
        trusted_policy_sha256: str,
        cell_count: int,
        artifact_cell_count: int,
        issued_at_utc: str,
        validated_at_utc: str,
        body: Mapping[str, Any],
        _seal: object | None = None,
    ) -> None:
        if _seal is not _VERIFIED_RUN_RESEAL_SEAL:
            raise TypeError("VerifiedRunReseal is verifier-constructed only")
        values = {
            "schema_version": schema_version,
            "run_id": run_id,
            "key_id": key_id,
            "body_sha256": body_sha256,
            "envelope_sha256": envelope_sha256,
            "origin_manifest_sha256": origin_manifest_sha256,
            "expected_authority_sha256": expected_authority_sha256,
            "trusted_policy_sha256": trusted_policy_sha256,
            "cell_count": cell_count,
            "artifact_cell_count": artifact_cell_count,
            "issued_at_utc": issued_at_utc,
            "validated_at_utc": validated_at_utc,
            "body": body,
            "grants_trading_authority": False,
            "grants_statistical_sufficiency_authority": False,
            "grants_profitability_authority": False,
        }
        for name, value in values.items():
            object.__setattr__(self, name, value)
        object.__setattr__(self, "_sealed", True)

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_sealed", False):
            raise AttributeError("VerifiedRunReseal is immutable")
        object.__setattr__(self, name, value)

    def to_receipt(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "key_id": self.key_id,
            "body_sha256": self.body_sha256,
            "envelope_sha256": self.envelope_sha256,
            "origin_manifest_sha256": self.origin_manifest_sha256,
            "expected_authority_sha256": self.expected_authority_sha256,
            "trusted_policy_sha256": self.trusted_policy_sha256,
            "cell_count": self.cell_count,
            "artifact_cell_count": self.artifact_cell_count,
            "issued_at_utc": self.issued_at_utc,
            "validated_at_utc": self.validated_at_utc,
            "grants_trading_authority": False,
            "grants_statistical_sufficiency_authority": False,
            "grants_profitability_authority": False,
        }

    def __reduce__(self) -> Any:
        raise TypeError("VerifiedRunReseal cannot be deserialized as authority")


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int
    links: int
    mode: int


@dataclass(frozen=True)
class _DirectoryIdentity:
    device: int
    inode: int
    mtime_ns: int
    ctime_ns: int
    links: int
    mode: int


@dataclass(frozen=True)
class _PreflightFile:
    path: Path
    identity: _FileIdentity
    maximum_bytes: int


@dataclass(frozen=True)
class _BarsObservation:
    sha256: str
    bytes: int
    rows: int
    first_open_utc: str
    last_open_utc: str
    last_close_utc: str
    quality: Mapping[str, Any]
    artifact: V5ArtifactObservation


def _fail(code: str) -> None:
    raise NativeRunResealError(code)


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _safe_identifier(value: Any, *, code: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        _fail(code)
    return value


def _parse_utc(value: Any, *, code: str) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        _fail(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise NativeRunResealError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(code)
    return parsed.astimezone(UTC)


def _iso_utc(value: Any, *, code: str) -> str:
    parsed = _parse_utc(value, code=code)
    return parsed.isoformat().replace("+00:00", "Z")


def _iso_second(value: Any, *, code: str) -> str:
    parsed = _parse_utc(value, code=code)
    if parsed.microsecond:
        _fail(code)
    return parsed.isoformat().replace("+00:00", "Z")


def _canonical_bytes(value: Any) -> bytes:
    try:
        return canonical_json_bytes(value)
    except NativeRequestContractError as exc:
        raise NativeRunResealError("RUN_RESEAL_CANONICAL_VALUE_INVALID") from exc


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _deep_freeze(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant: {value}")


def _unique_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _preflight_external_json_structure(raw: bytes) -> None:
    """Reject hostile depth/width before ``json.loads`` allocates the tree."""

    depth = 0
    structural_tokens = 0
    in_string = False
    escaped = False
    for byte in raw:
        if in_string:
            if escaped:
                escaped = False
            elif byte == 0x5C:  # backslash
                escaped = True
            elif byte == 0x22:  # double quote
                in_string = False
            continue
        if byte == 0x22:
            in_string = True
            continue
        if byte in {0x5B, 0x7B}:  # [ {
            depth += 1
            structural_tokens += 1
            if depth > MAX_EXTERNAL_JSON_DEPTH:
                _fail("RUN_RESEAL_EXTERNAL_JSON_DEPTH_CAP_EXCEEDED")
        elif byte in {0x5D, 0x7D}:  # ] }
            depth -= 1
        elif byte in {0x2C, 0x3A}:  # comma, colon
            structural_tokens += 1
        if structural_tokens > MAX_EXTERNAL_JSON_STRUCTURAL_TOKENS:
            _fail("RUN_RESEAL_EXTERNAL_JSON_NODE_CAP_EXCEEDED")


def _strict_json(raw: bytes, *, code: str) -> Mapping[str, Any]:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (
        MemoryError,
        OverflowError,
        RecursionError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValueError,
    ) as exc:
        raise NativeRunResealError(code) from exc
    if not isinstance(value, Mapping):
        _fail(code)
    _canonical_bytes(value)
    return value


def _validate_resume_seed(
    value: Any,
    *,
    run_id: str,
    common_request: Mapping[str, Any],
    source_identity: Mapping[str, Any],
    acquisition_started_utc: str,
) -> None:
    if not isinstance(value, Mapping) or set(value) != _RESUME_SEED_KEYS:
        _fail("RUN_RESEAL_RESUME_SEED_SCHEMA_INVALID")
    if (
        value.get("schema_version") != _RESUME_SEED_SCHEMA_VERSION
        or value.get("immutable_record") is not True
        or value.get("run_id") != run_id
    ):
        _fail("RUN_RESEAL_RESUME_SEED_INVALID")
    request = value.get("request")
    if not isinstance(request, Mapping) or _canonical_bytes(
        request
    ) != _canonical_bytes(common_request):
        _fail("RUN_RESEAL_RESUME_SEED_REQUEST_MISMATCH")
    try:
        validate_source_identity_match(value.get("source_identity"), source_identity)
    except NativeRequestContractError as exc:
        raise NativeRunResealError(
            "RUN_RESEAL_RESUME_SEED_SOURCE_IDENTITY_MISMATCH"
        ) from exc
    if tuple(value.get("symbols") or ()) != tuple(PRODUCTION_SYMBOL_ORDER) or tuple(
        value.get("timeframes") or ()
    ) != tuple(PRODUCTION_TIMEFRAME_ORDER):
        _fail("RUN_RESEAL_RESUME_SEED_ROSTER_MISMATCH")
    if value.get("production_authoritative") is not True:
        _fail("RUN_RESEAL_RESUME_SEED_PRODUCTION_INTENT_INVALID")
    if _iso_utc(
        value.get("acquisition_started_utc"),
        code="RUN_RESEAL_RESUME_SEED_ACQUISITION_STARTED_INVALID",
    ) != _iso_utc(
        acquisition_started_utc,
        code="RUN_RESEAL_ACQUISITION_STARTED_INVALID",
    ):
        _fail("RUN_RESEAL_RESUME_SEED_ACQUISITION_STARTED_MISMATCH")


def _expected_authority_payload(
    *,
    run_id: str,
    origin_manifest_sha256: str,
    trusted_policy_sha256: str,
    source_identity_sha256: str,
    requested_information_cutoff_utc: str,
    common_as_of_utc: str,
    timeframe_request_map_sha256: str,
) -> dict[str, Any]:
    return {
        "schema_version": RUN_RESEAL_EXPECTED_AUTHORITY_SCHEMA_VERSION,
        "run_id": run_id,
        "origin_manifest_sha256": origin_manifest_sha256,
        "trusted_policy_sha256": trusted_policy_sha256,
        "source_identity_sha256": source_identity_sha256,
        "requested_information_cutoff_utc": requested_information_cutoff_utc,
        "common_as_of_utc": common_as_of_utc,
        "timeframe_request_map_sha256": timeframe_request_map_sha256,
    }


def build_run_reseal_expected_authority(
    *,
    expected_run_id: Any,
    expected_origin_manifest_sha256: Any,
    trusted_policy: Any,
    expected_trusted_policy_sha256: Any,
    expected_source_environment: Any,
    expected_source_identity_sha256: Any,
    expected_requested_information_cutoff_utc: Any,
    expected_common_as_of_utc: Any,
    expected_timeframe_request_map_sha256: Any,
) -> RunResealExpectedAuthority:
    """Build the out-of-band pins required at every reseal trust boundary."""

    run_id = _safe_identifier(
        expected_run_id,
        code="RUN_RESEAL_EXPECTED_RUN_ID_INVALID",
        pattern=_RUN_ID_RE,
    )
    if not _is_sha256(expected_origin_manifest_sha256):
        _fail("RUN_RESEAL_EXPECTED_MANIFEST_HASH_INVALID")
    if type(trusted_policy) is not TrustedV5Policy:
        _fail("RUN_RESEAL_EXPECTED_TRUSTED_POLICY_REQUIRED")
    if (
        not _is_sha256(expected_trusted_policy_sha256)
        or expected_trusted_policy_sha256 != trusted_policy.policy_sha256
    ):
        _fail("RUN_RESEAL_EXPECTED_TRUSTED_POLICY_HASH_MISMATCH")
    try:
        common = _strict_json(
            trusted_policy.common_request_json,
            code="RUN_RESEAL_EXPECTED_TRUSTED_POLICY_INVALID",
        )
        supplied_source = canonical_source_identity(expected_source_environment)
        policy_source = _strict_json(
            trusted_policy.source_identity_json,
            code="RUN_RESEAL_EXPECTED_TRUSTED_POLICY_INVALID",
        )
    except NativeRequestContractError as exc:
        raise NativeRunResealError(
            "RUN_RESEAL_EXPECTED_TRUSTED_POLICY_INVALID"
        ) from exc
    source_json = _canonical_bytes(supplied_source)
    if source_json != trusted_policy.source_identity_json or (
        _canonical_bytes(policy_source) != source_json
    ):
        _fail("RUN_RESEAL_EXPECTED_SOURCE_IDENTITY_POLICY_MISMATCH")
    source_sha256 = _canonical_sha256(supplied_source)
    if (
        not _is_sha256(expected_source_identity_sha256)
        or expected_source_identity_sha256 != source_sha256
    ):
        _fail("RUN_RESEAL_EXPECTED_SOURCE_IDENTITY_HASH_MISMATCH")
    cutoff = _iso_second(
        expected_requested_information_cutoff_utc,
        code="RUN_RESEAL_EXPECTED_CUTOFF_INVALID",
    )
    as_of = _iso_second(
        expected_common_as_of_utc,
        code="RUN_RESEAL_EXPECTED_AS_OF_INVALID",
    )
    if common.get("requested_information_cutoff_utc") != cutoff:
        _fail("RUN_RESEAL_EXPECTED_CUTOFF_POLICY_MISMATCH")
    if common.get("common_as_of_utc") != as_of:
        _fail("RUN_RESEAL_EXPECTED_AS_OF_POLICY_MISMATCH")
    if (
        not _is_sha256(expected_timeframe_request_map_sha256)
        or common.get("timeframe_request_map_sha256")
        != expected_timeframe_request_map_sha256
    ):
        _fail("RUN_RESEAL_EXPECTED_REQUEST_MAP_HASH_MISMATCH")
    try:
        rebuilt_policy = build_trusted_v5_policy(
            expected_symbols=trusted_policy.symbols,
            expected_source_environment=supplied_source,
            requested_information_cutoff_utc=cutoff,
            timeframe_order=trusted_policy.timeframes,
            retries=trusted_policy.retries,
            timeframe_window_years=common.get("timeframe_window_years"),
            horizon_years=common.get("horizon_years"),
            embargo_days=common.get("embargo_days"),
            warmup_days=common.get("warmup_days"),
            safety_days=common.get("safety_days"),
            boundary_tolerance_days=common.get("boundary_tolerance_days"),
        )
    except NativeRequestContractError as exc:
        raise NativeRunResealError(
            "RUN_RESEAL_EXPECTED_TRUSTED_POLICY_INVALID"
        ) from exc
    if rebuilt_policy != trusted_policy:
        _fail("RUN_RESEAL_EXPECTED_TRUSTED_POLICY_NONCANONICAL")
    payload = _expected_authority_payload(
        run_id=run_id,
        origin_manifest_sha256=expected_origin_manifest_sha256,
        trusted_policy_sha256=expected_trusted_policy_sha256,
        source_identity_sha256=source_sha256,
        requested_information_cutoff_utc=cutoff,
        common_as_of_utc=as_of,
        timeframe_request_map_sha256=expected_timeframe_request_map_sha256,
    )
    return RunResealExpectedAuthority(
        schema_version=RUN_RESEAL_EXPECTED_AUTHORITY_SCHEMA_VERSION,
        run_id=run_id,
        origin_manifest_sha256=expected_origin_manifest_sha256,
        trusted_policy=trusted_policy,
        trusted_policy_sha256=expected_trusted_policy_sha256,
        source_identity_json=source_json,
        source_identity_sha256=source_sha256,
        requested_information_cutoff_utc=cutoff,
        common_as_of_utc=as_of,
        timeframe_request_map_sha256=expected_timeframe_request_map_sha256,
        authority_sha256=_canonical_sha256(payload),
    )


def _validated_expected_authority(value: Any) -> RunResealExpectedAuthority:
    if type(value) is not RunResealExpectedAuthority:
        _fail("RUN_RESEAL_EXPECTED_AUTHORITY_REQUIRED")
    if value.schema_version != RUN_RESEAL_EXPECTED_AUTHORITY_SCHEMA_VERSION:
        _fail("RUN_RESEAL_EXPECTED_AUTHORITY_SCHEMA_INVALID")
    source = _strict_json(
        value.source_identity_json,
        code="RUN_RESEAL_EXPECTED_SOURCE_IDENTITY_INVALID",
    )
    rebuilt = build_run_reseal_expected_authority(
        expected_run_id=value.run_id,
        expected_origin_manifest_sha256=value.origin_manifest_sha256,
        trusted_policy=value.trusted_policy,
        expected_trusted_policy_sha256=value.trusted_policy_sha256,
        expected_source_environment=source,
        expected_source_identity_sha256=value.source_identity_sha256,
        expected_requested_information_cutoff_utc=(
            value.requested_information_cutoff_utc
        ),
        expected_common_as_of_utc=value.common_as_of_utc,
        expected_timeframe_request_map_sha256=value.timeframe_request_map_sha256,
    )
    if rebuilt != value:
        _fail("RUN_RESEAL_EXPECTED_AUTHORITY_NONCANONICAL")
    return value


def run_reseal_expected_authority_to_mapping(
    value: RunResealExpectedAuthority,
) -> dict[str, Any]:
    authority = _validated_expected_authority(value)
    common = _strict_json(
        authority.trusted_policy.common_request_json,
        code="RUN_RESEAL_EXPECTED_TRUSTED_POLICY_INVALID",
    )
    source = _strict_json(
        authority.source_identity_json,
        code="RUN_RESEAL_EXPECTED_SOURCE_IDENTITY_INVALID",
    )
    return {
        "schema_version": authority.schema_version,
        "run_id": authority.run_id,
        "origin_manifest_sha256": authority.origin_manifest_sha256,
        "trusted_policy_sha256": authority.trusted_policy_sha256,
        "source_identity": dict(source),
        "source_identity_sha256": authority.source_identity_sha256,
        "common_request": dict(common),
        "requested_information_cutoff_utc": (
            authority.requested_information_cutoff_utc
        ),
        "common_as_of_utc": authority.common_as_of_utc,
        "timeframe_request_map_sha256": authority.timeframe_request_map_sha256,
        "symbols": list(authority.trusted_policy.symbols),
        "timeframes": list(authority.trusted_policy.timeframes),
        "retries": authority.trusted_policy.retries,
        "authority_sha256": authority.authority_sha256,
    }


def run_reseal_expected_authority_from_mapping(
    value: Any,
) -> RunResealExpectedAuthority:
    if not isinstance(value, Mapping) or set(value) != _EXPECTED_AUTHORITY_MAPPING_KEYS:
        _fail("RUN_RESEAL_EXPECTED_AUTHORITY_MAPPING_SCHEMA_INVALID")
    if value.get("schema_version") != RUN_RESEAL_EXPECTED_AUTHORITY_SCHEMA_VERSION:
        _fail("RUN_RESEAL_EXPECTED_AUTHORITY_MAPPING_SCHEMA_INVALID")
    common = value.get("common_request")
    source = value.get("source_identity")
    if not isinstance(common, Mapping) or not isinstance(source, Mapping):
        _fail("RUN_RESEAL_EXPECTED_AUTHORITY_MAPPING_SCHEMA_INVALID")
    try:
        policy = build_trusted_v5_policy(
            expected_symbols=value.get("symbols"),
            expected_source_environment=source,
            requested_information_cutoff_utc=value.get(
                "requested_information_cutoff_utc"
            ),
            timeframe_order=value.get("timeframes"),
            retries=value.get("retries"),
            timeframe_window_years=common.get("timeframe_window_years"),
            horizon_years=common.get("horizon_years"),
            embargo_days=common.get("embargo_days"),
            warmup_days=common.get("warmup_days"),
            safety_days=common.get("safety_days"),
            boundary_tolerance_days=common.get("boundary_tolerance_days"),
        )
    except NativeRequestContractError as exc:
        raise NativeRunResealError(
            "RUN_RESEAL_EXPECTED_AUTHORITY_POLICY_INVALID"
        ) from exc
    if _canonical_bytes(common) != policy.common_request_json:
        _fail("RUN_RESEAL_EXPECTED_AUTHORITY_COMMON_REQUEST_MISMATCH")
    authority = build_run_reseal_expected_authority(
        expected_run_id=value.get("run_id"),
        expected_origin_manifest_sha256=value.get("origin_manifest_sha256"),
        trusted_policy=policy,
        expected_trusted_policy_sha256=value.get("trusted_policy_sha256"),
        expected_source_environment=source,
        expected_source_identity_sha256=value.get("source_identity_sha256"),
        expected_requested_information_cutoff_utc=value.get(
            "requested_information_cutoff_utc"
        ),
        expected_common_as_of_utc=value.get("common_as_of_utc"),
        expected_timeframe_request_map_sha256=value.get("timeframe_request_map_sha256"),
    )
    if value.get("authority_sha256") != authority.authority_sha256:
        _fail("RUN_RESEAL_EXPECTED_AUTHORITY_HASH_MISMATCH")
    return authority


def _unsafe_file_attributes(value: os.stat_result) -> bool:
    return bool(
        stat.S_ISLNK(value.st_mode)
        or int(getattr(value, "st_file_attributes", 0) or 0)
        & _FILE_ATTRIBUTE_REPARSE_POINT
    )


def _file_identity(value: os.stat_result) -> _FileIdentity:
    return _FileIdentity(
        device=int(value.st_dev),
        inode=int(value.st_ino),
        size=int(value.st_size),
        mtime_ns=int(value.st_mtime_ns),
        ctime_ns=int(value.st_ctime_ns),
        links=int(value.st_nlink),
        mode=int(value.st_mode),
    )


def _directory_identity(value: os.stat_result) -> _DirectoryIdentity:
    return _DirectoryIdentity(
        device=int(value.st_dev),
        inode=int(value.st_ino),
        mtime_ns=int(value.st_mtime_ns),
        ctime_ns=int(value.st_ctime_ns),
        links=int(value.st_nlink),
        mode=int(value.st_mode),
    )


def _same_open_file_identity(left: _FileIdentity, right: _FileIdentity) -> bool:
    """Compare path and open-handle identities across Windows stat semantics.

    CPython can report ``st_ctime_ns`` at different precision for ``lstat`` and
    ``fstat`` on NTFS.  Device+inode pins the object while size, mtime, links,
    and mode pin the preflight state.  Path-to-path end checks still compare the
    complete dataclass, including ctime.
    """

    return (
        left.device,
        left.inode,
        left.size,
        left.mtime_ns,
        left.links,
        left.mode,
    ) == (
        right.device,
        right.inode,
        right.size,
        right.mtime_ns,
        right.links,
        right.mode,
    )


class _DirectoryGuard:
    def __init__(self, native_root: Path) -> None:
        self.native_root = native_root
        self._identities: dict[Path, _DirectoryIdentity] = {}
        self.guard_chain(native_root)

    def guard_chain(self, target: Path) -> None:
        absolute = Path(os.path.abspath(os.fspath(target)))
        try:
            absolute.relative_to(self.native_root)
        except ValueError:
            if absolute != self.native_root:
                _fail("RUN_RESEAL_PATH_OUTSIDE_NATIVE_ROOT")
        relative = absolute.relative_to(self.native_root)
        current = self.native_root
        self._guard_one(current)
        for part in relative.parts:
            current = current / part
            self._guard_one(current)

    def _guard_one(self, path: Path) -> None:
        if path in self._identities:
            return
        try:
            observed = path.lstat()
        except OSError as exc:
            raise NativeRunResealError("RUN_RESEAL_DIRECTORY_MISSING") from exc
        if _unsafe_file_attributes(observed) or not stat.S_ISDIR(observed.st_mode):
            _fail("RUN_RESEAL_UNSAFE_DIRECTORY")
        self._identities[path] = _directory_identity(observed)

    def verify(self) -> None:
        for path, expected in self._identities.items():
            try:
                observed = path.lstat()
            except OSError as exc:
                raise NativeRunResealError(
                    "RUN_RESEAL_DIRECTORY_IDENTITY_CHANGED"
                ) from exc
            if (
                _unsafe_file_attributes(observed)
                or not stat.S_ISDIR(observed.st_mode)
                or _directory_identity(observed) != expected
            ):
                _fail("RUN_RESEAL_DIRECTORY_IDENTITY_CHANGED")


def _bounded_directory_entries(
    path: Path,
    *,
    maximum_entries: int,
    code: str,
) -> dict[str, Path]:
    """Enumerate at most ``maximum_entries`` children without eager listdir."""

    if type(maximum_entries) is not int or maximum_entries < 0:
        _fail(f"{code}_ENTRY_CAP_INVALID")
    entries: dict[str, Path] = {}
    observed_count = 0
    duplicate_seen = False
    try:
        with os.scandir(path) as iterator:
            for entry in iterator:
                observed_count += 1
                if observed_count > maximum_entries:
                    _fail(f"{code}_ENTRY_CAP_EXCEEDED")
                if entry.name in entries:
                    duplicate_seen = True
                else:
                    entries[entry.name] = path / entry.name
    except OSError as exc:
        raise NativeRunResealError(f"{code}_ENUMERATION_FAILED") from exc
    if duplicate_seen:
        _fail(f"{code}_DUPLICATE_ENTRY")
    return entries


def _validate_run_topology(run_root: Path, *, guard: _DirectoryGuard) -> None:
    """Validate the exact final tree and reject all retry-attempt residue."""

    guard.guard_chain(run_root)
    allowed_root = {
        *PRODUCTION_SYMBOL_ORDER,
        "run_manifest.json",
        _RESUME_SEED_FILENAME,
        _ATTEMPT_RECEIPTS_DIR,
    }
    root_entries = _bounded_directory_entries(
        run_root,
        maximum_entries=len(allowed_root),
        code="RUN_RESEAL_RUN_ROOT",
    )
    if _ATTEMPT_RECEIPTS_DIR in root_entries:
        _fail("RUN_RESEAL_ATTEMPT_RECEIPTS_PRESENT")
    required = {
        *PRODUCTION_SYMBOL_ORDER,
        "run_manifest.json",
        _RESUME_SEED_FILENAME,
    }
    unexpected = set(root_entries) - required
    missing = required - set(root_entries)
    if unexpected or missing:
        _fail("RUN_RESEAL_RUN_ROOT_SCHEMA_INVALID")

    expected_timeframes = set(PRODUCTION_TIMEFRAME_ORDER)
    expected_leaf_files = {"bars.csv", "provenance.json"}
    for symbol in PRODUCTION_SYMBOL_ORDER:
        symbol_path = root_entries[symbol]
        guard.guard_chain(symbol_path)
        timeframe_entries = _bounded_directory_entries(
            symbol_path,
            maximum_entries=len(PRODUCTION_TIMEFRAME_ORDER),
            code="RUN_RESEAL_SYMBOL_DIRECTORY",
        )
        if set(timeframe_entries) != expected_timeframes:
            _fail("RUN_RESEAL_SYMBOL_DIRECTORY_SCHEMA_INVALID")
        for timeframe in PRODUCTION_TIMEFRAME_ORDER:
            leaf = timeframe_entries[timeframe]
            guard.guard_chain(leaf)
            leaf_entries = _bounded_directory_entries(
                leaf,
                maximum_entries=len(expected_leaf_files),
                code="RUN_RESEAL_LEAF_DIRECTORY",
            )
            if set(leaf_entries) != expected_leaf_files:
                _fail("RUN_RESEAL_LEAF_DIRECTORY_SCHEMA_INVALID")


def _secure_native_root(value: str | os.PathLike[str]) -> Path:
    root = Path(os.path.abspath(os.fspath(value)))
    if not root.is_absolute():
        _fail("RUN_RESEAL_NATIVE_ROOT_INVALID")
    current = Path(root.anchor)
    for part in root.parts[1:]:
        current = current / part
        try:
            observed = current.lstat()
        except OSError as exc:
            raise NativeRunResealError("RUN_RESEAL_NATIVE_ROOT_INVALID") from exc
        if _unsafe_file_attributes(observed) or not stat.S_ISDIR(observed.st_mode):
            _fail("RUN_RESEAL_NATIVE_ROOT_UNSAFE")
    return root


def _preflight_regular_file(
    path: Path,
    *,
    maximum_bytes: int,
    guard: _DirectoryGuard,
    seen_identities: set[tuple[int, int]],
    code: str,
) -> _PreflightFile:
    guard.guard_chain(path.parent)
    try:
        observed = path.lstat()
    except OSError as exc:
        raise NativeRunResealError(f"{code}_MISSING") from exc
    identity = _file_identity(observed)
    if (
        _unsafe_file_attributes(observed)
        or not stat.S_ISREG(observed.st_mode)
        or identity.links != 1
    ):
        _fail(f"{code}_UNSAFE")
    if not 0 < identity.size <= maximum_bytes:
        _fail(f"{code}_SIZE_INVALID")
    key = (identity.device, identity.inode)
    if key in seen_identities:
        _fail("RUN_RESEAL_FILE_IDENTITY_ALIAS")
    seen_identities.add(key)
    return _PreflightFile(path=path, identity=identity, maximum_bytes=maximum_bytes)


@contextmanager
def _open_preflighted(value: _PreflightFile, *, code: str) -> Iterator[BinaryIO]:
    flags = os.O_RDONLY | int(getattr(os, "O_BINARY", 0))
    flags |= int(getattr(os, "O_NOFOLLOW", 0))
    try:
        descriptor = os.open(value.path, flags)
    except OSError as exc:
        raise NativeRunResealError(f"{code}_OPEN_FAILED") from exc
    try:
        observed = os.fstat(descriptor)
        if not _same_open_file_identity(_file_identity(observed), value.identity):
            _fail(f"{code}_IDENTITY_CHANGED")
        with os.fdopen(descriptor, "rb", buffering=IO_BUFFER_BYTES) as handle:
            descriptor = -1
            yield handle
            after = os.fstat(handle.fileno())
            if not _same_open_file_identity(_file_identity(after), value.identity):
                _fail(f"{code}_IDENTITY_CHANGED")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        final = value.path.lstat()
    except OSError as exc:
        raise NativeRunResealError(f"{code}_IDENTITY_CHANGED") from exc
    if _file_identity(final) != value.identity or _unsafe_file_attributes(final):
        _fail(f"{code}_IDENTITY_CHANGED")


def _read_preflighted(value: _PreflightFile, *, code: str) -> bytes:
    with _open_preflighted(value, code=code) as handle:
        raw = handle.read(value.maximum_bytes + 1)
        if len(raw) != value.identity.size or len(raw) > value.maximum_bytes:
            _fail(f"{code}_SIZE_CHANGED")
        if handle.read(1):
            _fail(f"{code}_SIZE_CHANGED")
    return raw


def load_bounded_json_file(
    path: str | os.PathLike[str],
    *,
    maximum_bytes: int = MAX_EXTERNAL_JSON_BYTES,
    code: str = "RUN_RESEAL_EXTERNAL_JSON_INVALID",
) -> Mapping[str, Any]:
    """Read one external JSON object without following aliases or hardlinks."""

    if (
        type(maximum_bytes) is not int
        or maximum_bytes <= 0
        or maximum_bytes > MAX_EXTERNAL_JSON_BYTES
    ):
        _fail("RUN_RESEAL_EXTERNAL_JSON_BYTE_CAP_INVALID")
    target = Path(os.path.abspath(os.fspath(path)))
    root = _secure_native_root(target.parent)
    guard = _DirectoryGuard(root)
    preflight = _preflight_regular_file(
        target,
        maximum_bytes=maximum_bytes,
        guard=guard,
        seen_identities=set(),
        code=code,
    )
    raw = _read_preflighted(preflight, code=code)
    _preflight_external_json_structure(raw)
    return _strict_json(raw, code=code)


def _verify_preflighted_files(values: Sequence[_PreflightFile]) -> None:
    for value in values:
        try:
            observed = value.path.lstat()
        except OSError as exc:
            raise NativeRunResealError("RUN_RESEAL_SOURCE_IDENTITY_CHANGED") from exc
        if (
            _unsafe_file_attributes(observed)
            or _file_identity(observed) != value.identity
        ):
            _fail("RUN_RESEAL_SOURCE_IDENTITY_CHANGED")


def _scan_bars(
    value: _PreflightFile,
    *,
    provenance: Mapping[str, Any],
    common_request: Mapping[str, Any],
    timeframe: str,
) -> _BarsObservation:
    """Securely stream one pinned artifact through the shared V5 scanner."""

    pagination = provenance.get("pagination")
    if not isinstance(pagination, Mapping):
        _fail("RUN_RESEAL_PAGE_LEDGER_INVALID")
    with _open_preflighted(value, code="RUN_RESEAL_BARS") as handle:
        try:
            scanned = scan_native_csv_stream(
                handle,
                expected_bytes=value.identity.size,
                timeframe=timeframe,
                common_request=common_request,
                pagination=pagination,
                maximum_bytes=value.maximum_bytes,
                maximum_rows=MAX_ARTIFACT_ROWS,
                maximum_line_bytes=MAX_CSV_LINE_BYTES,
            )
        except NativeCsvValidationError as exc:
            raise NativeRunResealError(f"RUN_RESEAL_{exc.code}") from exc
    artifact = scanned.artifact
    return _BarsObservation(
        sha256=artifact.sha256,
        bytes=artifact.bytes,
        rows=artifact.rows,
        first_open_utc=artifact.first_open_utc,
        last_open_utc=artifact.last_open_utc,
        last_close_utc=artifact.last_close_utc,
        quality=scanned.quality,
        artifact=artifact,
    )


def _reject_authority_claim_fields(value: Any, *, record_name: str) -> None:
    if not isinstance(value, Mapping):
        _fail(f"RUN_RESEAL_{record_name}_SCHEMA_INVALID")
    for field in sorted(set(value).intersection(_PROHIBITED_AUTHORITY_FIELDS)):
        _fail(f"RUN_RESEAL_{record_name}_AUTHORITY_FIELD_PRESENT:{field}")


def _validate_publishable_provenance_schema(
    provenance: Mapping[str, Any],
    *,
    observation: _BarsObservation,
    manifest: Mapping[str, Any],
) -> None:
    if set(provenance) != _PUBLISHABLE_PROVENANCE_KEYS:
        _fail("RUN_RESEAL_PUBLISHABLE_PROVENANCE_SCHEMA_INVALID")
    started = _parse_utc(
        provenance.get("acquisition_started_utc"),
        code="RUN_RESEAL_PROVENANCE_ACQUISITION_TIME_INVALID",
    )
    completed = _parse_utc(
        provenance.get("acquisition_completed_utc"),
        code="RUN_RESEAL_PROVENANCE_ACQUISITION_TIME_INVALID",
    )
    run_started = _parse_utc(
        manifest.get("acquisition_started_utc"),
        code="RUN_RESEAL_ACQUISITION_STARTED_INVALID",
    )
    run_completed = _parse_utc(
        manifest.get("acquisition_completed_utc"),
        code="RUN_RESEAL_ACQUISITION_COMPLETED_INVALID",
    )
    if not (run_started <= started <= completed <= run_completed):
        _fail("RUN_RESEAL_PROVENANCE_ACQUISITION_TIME_OUTSIDE_RUN")
    quality = provenance.get("quality")
    if not isinstance(quality, Mapping) or dict(quality) != dict(observation.quality):
        _fail("RUN_RESEAL_PROVENANCE_QUALITY_MISMATCH")


def _validate_provenance_common(
    provenance: Mapping[str, Any],
    *,
    run_id: str,
    symbol: str,
    timeframe: str,
    result: Mapping[str, Any],
    source_environment: Any,
) -> None:
    _reject_authority_claim_fields(provenance, record_name="PROVENANCE")
    _reject_authority_claim_fields(result, record_name="MANIFEST_RESULT")
    expected = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "acquisition_contract_version": ACQUISITION_CONTRACT_VERSION,
        "immutable_record": True,
        "run_id": run_id,
        "symbol": symbol,
        "canonical_symbol": symbol,
        "timeframe": timeframe,
        "source_kind": "broker_native",
        "timezone": "UTC",
    }
    for field, value in expected.items():
        if provenance.get(field) != value:
            _fail(f"RUN_RESEAL_PROVENANCE_FIELD_MISMATCH:{field}")
    if provenance.get("status") != result.get("status"):
        _fail("RUN_RESEAL_PROVENANCE_STATUS_MISMATCH")
    if provenance.get("failure_status") != result.get("failure_status"):
        _fail("RUN_RESEAL_PROVENANCE_FAILURE_STATUS_MISMATCH")
    try:
        validate_source_identity_match(
            provenance.get("source_environment"), source_environment
        )
    except NativeRequestContractError as exc:
        raise NativeRunResealError(
            "RUN_RESEAL_PROVENANCE_SOURCE_IDENTITY_MISMATCH"
        ) from exc


def _expected_interval(
    common_request: Mapping[str, Any], timeframe: str
) -> tuple[dict[str, str], str, str]:
    intervals = common_request.get("timeframe_source_request_intervals")
    interval = intervals.get(timeframe) if isinstance(intervals, Mapping) else None
    if not isinstance(interval, Mapping) or set(interval) != {
        "start_utc",
        "end_exclusive_utc",
    }:
        _fail("RUN_RESEAL_SOURCE_INTERVAL_INVALID")
    start = _iso_second(
        interval.get("start_utc"), code="RUN_RESEAL_SOURCE_INTERVAL_INVALID"
    )
    end = _iso_second(
        interval.get("end_exclusive_utc"),
        code="RUN_RESEAL_SOURCE_INTERVAL_INVALID",
    )
    if _parse_utc(start, code="RUN_RESEAL_SOURCE_INTERVAL_INVALID") >= _parse_utc(
        end, code="RUN_RESEAL_SOURCE_INTERVAL_INVALID"
    ):
        _fail("RUN_RESEAL_SOURCE_INTERVAL_INVALID")
    return {"start_utc": start, "end_exclusive_utc": end}, start, end


def _cell_payload(
    *,
    ordinal: int,
    run_id: str,
    symbol: str,
    timeframe: str,
    result: Mapping[str, Any],
    manifest: Mapping[str, Any],
    trusted_policy: Any,
    validation_context: Any,
    common_request: Mapping[str, Any],
    source_environment: Any,
    provenance_file: _PreflightFile,
    bars_file: _PreflightFile | None,
) -> dict[str, Any]:
    provenance_raw = _read_preflighted(provenance_file, code="RUN_RESEAL_PROVENANCE")
    provenance = _strict_json(provenance_raw, code="RUN_RESEAL_PROVENANCE_JSON_INVALID")
    _validate_provenance_common(
        provenance,
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        result=result,
        source_environment=source_environment,
    )
    expected_interval, request_start, request_end = _expected_interval(
        common_request, timeframe
    )
    request = provenance.get("request")
    request_bound = False
    if isinstance(request, Mapping):
        observed_interval = request.get("source_request_interval")
        if not isinstance(observed_interval, Mapping) or set(observed_interval) != {
            "start_utc",
            "end_exclusive_utc",
        }:
            _fail("RUN_RESEAL_PROVENANCE_SOURCE_INTERVAL_MISMATCH")
        normalized_observed = {
            "start_utc": _iso_second(
                observed_interval.get("start_utc"),
                code="RUN_RESEAL_PROVENANCE_SOURCE_INTERVAL_MISMATCH",
            ),
            "end_exclusive_utc": _iso_second(
                observed_interval.get("end_exclusive_utc"),
                code="RUN_RESEAL_PROVENANCE_SOURCE_INTERVAL_MISMATCH",
            ),
        }
        if normalized_observed != expected_interval:
            _fail("RUN_RESEAL_PROVENANCE_SOURCE_INTERVAL_MISMATCH")
        if request.get("timeframe_request_map_sha256") != common_request.get(
            "timeframe_request_map_sha256"
        ):
            _fail("RUN_RESEAL_PROVENANCE_REQUEST_MAP_MISMATCH")
        request_bound = True

    status = str(result.get("status") or "")
    if status not in {"success", "partial"}:
        _fail("RUN_RESEAL_UNRESOLVED_FAILURE_CELL_NOT_RESEALABLE")
    artifact_present = True
    leaf_relative = f"{symbol}/{timeframe}"
    provenance_relative = f"runs/{run_id}/{leaf_relative}/provenance.json"
    bars_relative: str | None = None
    observation: _BarsObservation | None = None
    if bars_file is None:
        _fail("RUN_RESEAL_BARS_PREFLIGHT_MISSING")
    if not request_bound:
        _fail("RUN_RESEAL_ARTIFACT_REQUEST_BINDING_MISSING")
    try:
        validate_v5_leaf_request(request, common_request, timeframe)
    except NativeRequestContractError as exc:
        raise NativeRunResealError(
            "RUN_RESEAL_PROVENANCE_LEAF_REQUEST_INVALID"
        ) from exc
    observation = _scan_bars(
        bars_file,
        provenance=provenance,
        common_request=common_request,
        timeframe=timeframe,
    )
    try:
        validate_v5_cell_status(
            provenance,
            result,
            common_request,
            symbol=symbol,
            timeframe=timeframe,
            manifest_source_environment=source_environment,
            artifact_observation=observation.artifact,
            expected_retries=trusted_policy.retries,
            validation_context=validation_context,
        )
    except NativeRequestContractError as exc:
        detail = exc.errors[0] if exc.errors else "UNKNOWN"
        raise NativeRunResealError(
            f"RUN_RESEAL_V5_CELL_INVALID:{symbol}:{timeframe}:{detail}"
        ) from exc
    _validate_publishable_provenance_schema(
        provenance,
        observation=observation,
        manifest=manifest,
    )
    bars_relative = f"runs/{run_id}/{leaf_relative}/bars.csv"
    artifact = provenance.get("artifact")
    expected_artifact = {
        "relative_path": "bars.csv",
        "sha256": observation.sha256,
        "bytes": observation.bytes,
        "rows": observation.rows,
    }
    if not isinstance(artifact, Mapping) or dict(artifact) != expected_artifact:
        _fail("RUN_RESEAL_PROVENANCE_ARTIFACT_MISMATCH")
    if (
        provenance.get("sha256") != observation.sha256
        or provenance.get("rows") != observation.rows
        or result.get("sha256") != observation.sha256
        or result.get("rows") != observation.rows
    ):
        _fail("RUN_RESEAL_ARTIFACT_RESULT_BINDING_MISMATCH")
    if provenance.get("columns") != list(PRODUCTION_ARTIFACT_COLUMNS):
        _fail("RUN_RESEAL_PROVENANCE_COLUMNS_MISMATCH")
    if (
        _iso_second(
            provenance.get("start_ts"), code="RUN_RESEAL_PROVENANCE_START_INVALID"
        )
        != observation.first_open_utc
        or _iso_second(
            provenance.get("end_ts"), code="RUN_RESEAL_PROVENANCE_END_INVALID"
        )
        != observation.last_open_utc
    ):
        _fail("RUN_RESEAL_PROVENANCE_OBSERVED_INTERVAL_MISMATCH")

    return {
        "ordinal": ordinal,
        "cell_id": f"{symbol}|{timeframe}",
        "symbol": symbol,
        "timeframe": timeframe,
        "relative_leaf": leaf_relative,
        "manifest_result_sha256": _canonical_sha256(result),
        "acquisition_status": result.get("status"),
        "failure_status": result.get("failure_status"),
        "resume_disposition": result.get("resume_disposition"),
        "request_start_utc": request_start,
        "request_end_exclusive_utc": request_end,
        "artifact_present": artifact_present,
        "bars_relative_path": bars_relative,
        "bars_sha256": observation.sha256,
        "bars_bytes": observation.bytes,
        "bars_rows": observation.rows,
        "first_open_utc": observation.first_open_utc,
        "last_open_utc": observation.last_open_utc,
        "last_close_utc": observation.last_close_utc,
        "provenance_relative_path": provenance_relative,
        "provenance_sha256": hashlib.sha256(provenance_raw).hexdigest(),
        "provenance_bytes": len(provenance_raw),
        "provenance_request_interval_bound": request_bound,
    }


def materialize_run_reseal_body(
    native_root: str | os.PathLike[str],
    *,
    expected_authority: RunResealExpectedAuthority,
    issued_at_utc: Any,
) -> dict[str, Any]:
    """Stream an externally pinned V5 run into a non-authoritative body."""

    authority = _validated_expected_authority(expected_authority)
    canonical_run_id = authority.run_id
    root = _secure_native_root(native_root)
    guard = _DirectoryGuard(root)
    seen_identities: set[tuple[int, int]] = set()
    run_root = root / "runs" / canonical_run_id
    manifest_path = run_root / "run_manifest.json"
    manifest_file = _preflight_regular_file(
        manifest_path,
        maximum_bytes=MAX_MANIFEST_BYTES,
        guard=guard,
        seen_identities=seen_identities,
        code="RUN_RESEAL_MANIFEST",
    )
    manifest_raw = _read_preflighted(manifest_file, code="RUN_RESEAL_MANIFEST")
    manifest_sha256 = hashlib.sha256(manifest_raw).hexdigest()
    if manifest_sha256 != authority.origin_manifest_sha256:
        _fail("RUN_RESEAL_EXPECTED_MANIFEST_HASH_MISMATCH")
    manifest = _strict_json(manifest_raw, code="RUN_RESEAL_MANIFEST_JSON_INVALID")
    if manifest.get("run_id") != canonical_run_id:
        _fail("RUN_RESEAL_MANIFEST_RUN_ID_MISMATCH")
    policy = authority.trusted_policy
    try:
        bound = validate_v5_production_manifest(manifest, policy)
    except NativeRequestContractError as exc:
        detail = exc.errors[0] if exc.errors else "UNKNOWN"
        raise NativeRunResealError(f"RUN_RESEAL_V5_MANIFEST_INVALID:{detail}") from exc

    common_request = bound["common_request"]
    source_identity = bound["source_environment"]
    results_by_cell = bound["results_by_cell"]
    try:
        validation_context = build_v5_cell_validation_context(
            common_request,
            source_identity,
            policy.retries,
        )
    except NativeRequestContractError as exc:
        detail = exc.errors[0] if exc.errors else "UNKNOWN"
        raise NativeRunResealError(
            f"RUN_RESEAL_V5_CELL_CONTEXT_INVALID:{detail}"
        ) from exc
    if any(
        result.get("status") not in {"success", "partial"}
        for result in results_by_cell.values()
    ):
        _fail("RUN_RESEAL_UNRESOLVED_FAILURE_CELL_NOT_RESEALABLE")
    _validate_run_topology(run_root, guard=guard)
    request = manifest["request"]
    if _canonical_bytes(source_identity) != authority.source_identity_json:
        _fail("RUN_RESEAL_EXPECTED_SOURCE_IDENTITY_MISMATCH")
    if _canonical_sha256(source_identity) != authority.source_identity_sha256:
        _fail("RUN_RESEAL_EXPECTED_SOURCE_IDENTITY_HASH_MISMATCH")
    if (
        common_request.get("requested_information_cutoff_utc")
        != authority.requested_information_cutoff_utc
    ):
        _fail("RUN_RESEAL_EXPECTED_CUTOFF_MISMATCH")
    if common_request.get("common_as_of_utc") != authority.common_as_of_utc:
        _fail("RUN_RESEAL_EXPECTED_AS_OF_MISMATCH")
    if (
        common_request.get("timeframe_request_map_sha256")
        != authority.timeframe_request_map_sha256
    ):
        _fail("RUN_RESEAL_EXPECTED_REQUEST_MAP_MISMATCH")
    if request.get("timeframe_request_map_schema_version") != (
        TIMEFRAME_REQUEST_MAP_SCHEMA_VERSION
    ):
        _fail("RUN_RESEAL_REQUEST_MAP_SCHEMA_INVALID")
    if tuple(request.get("symbols") or ()) != tuple(PRODUCTION_SYMBOL_ORDER):
        _fail("RUN_RESEAL_SYMBOL_ORDER_INVALID")
    if tuple(request.get("timeframes") or ()) != tuple(PRODUCTION_TIMEFRAME_ORDER):
        _fail("RUN_RESEAL_TIMEFRAME_ORDER_INVALID")

    acquisition_started = _iso_utc(
        manifest.get("acquisition_started_utc"),
        code="RUN_RESEAL_ACQUISITION_STARTED_INVALID",
    )
    acquisition_completed = _iso_utc(
        manifest.get("acquisition_completed_utc"),
        code="RUN_RESEAL_ACQUISITION_COMPLETED_INVALID",
    )
    if _parse_utc(
        acquisition_started, code="RUN_RESEAL_ACQUISITION_STARTED_INVALID"
    ) > _parse_utc(
        acquisition_completed, code="RUN_RESEAL_ACQUISITION_COMPLETED_INVALID"
    ):
        _fail("RUN_RESEAL_ACQUISITION_INTERVAL_INVALID")
    resume_seed_file = _preflight_regular_file(
        run_root / _RESUME_SEED_FILENAME,
        maximum_bytes=MAX_RESUME_SEED_BYTES,
        guard=guard,
        seen_identities=seen_identities,
        code="RUN_RESEAL_RESUME_SEED",
    )
    resume_seed_raw = _read_preflighted(resume_seed_file, code="RUN_RESEAL_RESUME_SEED")
    resume_seed = _strict_json(
        resume_seed_raw, code="RUN_RESEAL_RESUME_SEED_JSON_INVALID"
    )
    _validate_resume_seed(
        resume_seed,
        run_id=canonical_run_id,
        common_request=common_request,
        source_identity=source_identity,
        acquisition_started_utc=acquisition_started,
    )

    preflight: dict[tuple[str, str], tuple[_PreflightFile, _PreflightFile]] = {}
    total_provenance_bytes = 0
    total_artifact_bytes = 0
    for symbol in PRODUCTION_SYMBOL_ORDER:
        for timeframe in PRODUCTION_TIMEFRAME_ORDER:
            leaf = root / "runs" / canonical_run_id / symbol / timeframe
            provenance_file = _preflight_regular_file(
                leaf / "provenance.json",
                maximum_bytes=MAX_PROVENANCE_BYTES,
                guard=guard,
                seen_identities=seen_identities,
                code="RUN_RESEAL_PROVENANCE",
            )
            total_provenance_bytes += provenance_file.identity.size
            bars_file = _preflight_regular_file(
                leaf / "bars.csv",
                maximum_bytes=MAX_ARTIFACT_BYTES,
                guard=guard,
                seen_identities=seen_identities,
                code="RUN_RESEAL_BARS",
            )
            total_artifact_bytes += bars_file.identity.size
            if total_provenance_bytes > MAX_TOTAL_PROVENANCE_BYTES:
                _fail("RUN_RESEAL_TOTAL_PROVENANCE_CAP_EXCEEDED")
            if total_artifact_bytes > MAX_TOTAL_ARTIFACT_BYTES:
                _fail("RUN_RESEAL_TOTAL_ARTIFACT_CAP_EXCEEDED")
            preflight[(symbol, timeframe)] = (provenance_file, bars_file)

    cells: list[dict[str, Any]] = []
    ordinal = 0
    for symbol in PRODUCTION_SYMBOL_ORDER:
        for timeframe in PRODUCTION_TIMEFRAME_ORDER:
            ordinal += 1
            provenance_file, bars_file = preflight[(symbol, timeframe)]
            cells.append(
                _cell_payload(
                    ordinal=ordinal,
                    run_id=canonical_run_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    result=results_by_cell[(symbol, timeframe)],
                    manifest=manifest,
                    trusted_policy=policy,
                    validation_context=validation_context,
                    common_request=common_request,
                    source_environment=source_identity,
                    provenance_file=provenance_file,
                    bars_file=bars_file,
                )
            )
    if len(cells) != PRODUCTION_CELL_COUNT:
        _fail("RUN_RESEAL_CELL_COUNT_INVALID")

    issued = _iso_second(issued_at_utc, code="RUN_RESEAL_ISSUED_AT_INVALID")
    if _parse_utc(issued, code="RUN_RESEAL_ISSUED_AT_INVALID") < _parse_utc(
        acquisition_completed, code="RUN_RESEAL_ACQUISITION_COMPLETED_INVALID"
    ):
        _fail("RUN_RESEAL_ISSUED_BEFORE_ACQUISITION_COMPLETED")
    artifact_count = sum(cell["artifact_present"] is True for cell in cells)
    if artifact_count != PRODUCTION_CELL_COUNT:
        _fail("RUN_RESEAL_INTERNAL_PUBLISHABLE_CELL_COUNT_INVALID")
    body = {
        "contract_version": RUN_RESEAL_CONTRACT_VERSION,
        "authority_scope": RUN_RESEAL_AUTHORITY_SCOPE,
        "run_id": canonical_run_id,
        "origin_manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "origin_manifest_relative_path": (f"runs/{canonical_run_id}/run_manifest.json"),
        "origin_manifest_sha256": manifest_sha256,
        "origin_manifest_bytes": len(manifest_raw),
        "resume_seed_schema_version": _RESUME_SEED_SCHEMA_VERSION,
        "resume_seed_relative_path": (
            f"runs/{canonical_run_id}/{_RESUME_SEED_FILENAME}"
        ),
        "resume_seed_sha256": hashlib.sha256(resume_seed_raw).hexdigest(),
        "resume_seed_bytes": len(resume_seed_raw),
        "expected_authority_schema_version": authority.schema_version,
        "expected_authority_sha256": authority.authority_sha256,
        "trusted_policy_sha256": authority.trusted_policy_sha256,
        "expected_requested_information_cutoff_utc": (
            authority.requested_information_cutoff_utc
        ),
        "expected_common_as_of_utc": authority.common_as_of_utc,
        "acquisition_started_utc": acquisition_started,
        "acquisition_completed_utc": acquisition_completed,
        "source_identity": source_identity,
        "source_identity_sha256": _canonical_sha256(source_identity),
        "common_request": common_request,
        "common_request_sha256": _canonical_sha256(common_request),
        "timeframe_request_map_sha256": common_request["timeframe_request_map_sha256"],
        "symbols": list(PRODUCTION_SYMBOL_ORDER),
        "symbol_order_sha256": PRODUCTION_SYMBOL_ORDER_SHA256,
        "timeframes": list(PRODUCTION_TIMEFRAME_ORDER),
        "timeframe_order_sha256": PRODUCTION_TIMEFRAME_ORDER_SHA256,
        "retries": request["retries"],
        "cells": cells,
        "cells_sha256": _canonical_sha256(cells),
        "cell_count": len(cells),
        "artifact_cell_count": artifact_count,
        "issued_at_utc": issued,
        "grants_trading_authority": False,
        "grants_statistical_sufficiency_authority": False,
        "grants_profitability_authority": False,
    }
    if set(body) != _BODY_KEYS:
        _fail("RUN_RESEAL_INTERNAL_BODY_SCHEMA_INVALID")
    all_files = [manifest_file, resume_seed_file]
    for provenance_file, bars_file in preflight.values():
        all_files.append(provenance_file)
        all_files.append(bars_file)
    _verify_preflighted_files(all_files)
    _validate_run_topology(run_root, guard=guard)
    guard.verify()
    return body


def build_run_reseal_signing_request(
    native_root: str | os.PathLike[str],
    *,
    expected_authority: RunResealExpectedAuthority,
    authority_key_id: str,
    issued_at_utc: Any,
) -> dict[str, Any]:
    """Build an unsigned request for an external authority."""

    key_id = _safe_identifier(
        authority_key_id,
        code="RUN_RESEAL_KEY_ID_INVALID",
        pattern=_KEY_ID_RE,
    )
    body = materialize_run_reseal_body(
        native_root,
        expected_authority=expected_authority,
        issued_at_utc=issued_at_utc,
    )
    canonical_body = _canonical_bytes(body)
    signature_payload = _run_reseal_signature_payload(key_id, body)
    request = {
        "schema_version": RUN_RESEAL_SIGNING_REQUEST_SCHEMA_VERSION,
        "key_id": key_id,
        "signature_algorithm": RUN_RESEAL_SIGNATURE_ALGORITHM,
        "signature_domain_base64": base64.b64encode(RUN_RESEAL_SIGNATURE_DOMAIN).decode(
            "ascii"
        ),
        "canonicalization": "pm_native_request_contract.canonical_json_bytes.v1",
        "signature_payload_base64": base64.b64encode(signature_payload).decode("ascii"),
        "signature_payload_sha256": hashlib.sha256(signature_payload).hexdigest(),
        "body": body,
        "body_sha256": hashlib.sha256(canonical_body).hexdigest(),
        "grants_trading_authority": False,
        "grants_statistical_sufficiency_authority": False,
        "grants_profitability_authority": False,
    }
    if set(request) != _SIGNING_REQUEST_KEYS:
        _fail("RUN_RESEAL_INTERNAL_SIGNING_REQUEST_SCHEMA_INVALID")
    return request


def _run_reseal_signature_payload(
    key_id: Any,
    body: Any,
) -> bytes:
    """Bind the authority identity as well as the reseal body."""

    canonical_key_id = _safe_identifier(
        key_id,
        code="RUN_RESEAL_KEY_ID_INVALID",
        pattern=_KEY_ID_RE,
    )
    return RUN_RESEAL_SIGNATURE_DOMAIN + _canonical_bytes(
        {"key_id": canonical_key_id, "body": body}
    )


def build_run_reseal_trust_root(
    *, key_id: Any, public_key_base64: Any
) -> RunResealTrustRoot:
    canonical_key_id = _safe_identifier(
        key_id, code="RUN_RESEAL_KEY_ID_INVALID", pattern=_KEY_ID_RE
    )
    if not isinstance(public_key_base64, str) or len(public_key_base64) > 128:
        _fail("RUN_RESEAL_PUBLIC_KEY_INVALID")
    try:
        public_key = base64.b64decode(public_key_base64, validate=True)
    except (TypeError, ValueError) as exc:
        raise NativeRunResealError("RUN_RESEAL_PUBLIC_KEY_INVALID") from exc
    if len(public_key) != 32:
        _fail("RUN_RESEAL_PUBLIC_KEY_INVALID")
    return RunResealTrustRoot(
        schema_version=RUN_RESEAL_TRUST_ROOT_SCHEMA_VERSION,
        key_id=canonical_key_id,
        public_key_base64=public_key_base64,
    )


def run_reseal_trust_root_from_mapping(value: Any) -> RunResealTrustRoot:
    if not isinstance(value, Mapping) or set(value) != _TRUST_ROOT_KEYS:
        _fail("RUN_RESEAL_TRUST_ROOT_SCHEMA_INVALID")
    if value.get("schema_version") != RUN_RESEAL_TRUST_ROOT_SCHEMA_VERSION:
        _fail("RUN_RESEAL_TRUST_ROOT_SCHEMA_INVALID")
    return build_run_reseal_trust_root(
        key_id=value.get("key_id"),
        public_key_base64=value.get("public_key_base64"),
    )


def _validate_signed_body_shape(
    body: Any,
    *,
    expected_authority: RunResealExpectedAuthority,
) -> Mapping[str, Any]:
    if not isinstance(body, Mapping) or set(body) != _BODY_KEYS:
        _fail("RUN_RESEAL_BODY_SCHEMA_INVALID")
    if body.get("contract_version") != RUN_RESEAL_CONTRACT_VERSION:
        _fail("RUN_RESEAL_BODY_CONTRACT_INVALID")
    if body.get("authority_scope") != RUN_RESEAL_AUTHORITY_SCOPE:
        _fail("RUN_RESEAL_BODY_AUTHORITY_SCOPE_INVALID")
    authority = _validated_expected_authority(expected_authority)
    expected_pins = {
        "run_id": authority.run_id,
        "origin_manifest_sha256": authority.origin_manifest_sha256,
        "expected_authority_schema_version": authority.schema_version,
        "expected_authority_sha256": authority.authority_sha256,
        "trusted_policy_sha256": authority.trusted_policy_sha256,
        "source_identity_sha256": authority.source_identity_sha256,
        "expected_requested_information_cutoff_utc": (
            authority.requested_information_cutoff_utc
        ),
        "expected_common_as_of_utc": authority.common_as_of_utc,
        "timeframe_request_map_sha256": authority.timeframe_request_map_sha256,
    }
    for field, expected in expected_pins.items():
        if body.get(field) != expected:
            _fail(f"RUN_RESEAL_BODY_EXPECTED_AUTHORITY_MISMATCH:{field}")
    run_id = authority.run_id
    if (
        body.get("origin_manifest_schema_version") != MANIFEST_SCHEMA_VERSION
        or body.get("origin_manifest_relative_path")
        != f"runs/{run_id}/run_manifest.json"
        or not _is_sha256(body.get("origin_manifest_sha256"))
        or type(body.get("origin_manifest_bytes")) is not int
        or body.get("origin_manifest_bytes") <= 0
    ):
        _fail("RUN_RESEAL_BODY_ORIGIN_MANIFEST_BINDING_INVALID")
    if (
        body.get("resume_seed_schema_version") != _RESUME_SEED_SCHEMA_VERSION
        or body.get("resume_seed_relative_path")
        != f"runs/{run_id}/{_RESUME_SEED_FILENAME}"
        or not _is_sha256(body.get("resume_seed_sha256"))
        or type(body.get("resume_seed_bytes")) is not int
        or body.get("resume_seed_bytes") <= 0
    ):
        _fail("RUN_RESEAL_BODY_RESUME_SEED_BINDING_INVALID")
    source_identity = _strict_json(
        authority.source_identity_json,
        code="RUN_RESEAL_EXPECTED_SOURCE_IDENTITY_INVALID",
    )
    if _canonical_bytes(body.get("source_identity")) != _canonical_bytes(
        source_identity
    ):
        _fail("RUN_RESEAL_BODY_EXPECTED_SOURCE_IDENTITY_MISMATCH")
    common = _strict_json(
        authority.trusted_policy.common_request_json,
        code="RUN_RESEAL_EXPECTED_TRUSTED_POLICY_INVALID",
    )
    if _canonical_bytes(body.get("common_request")) != _canonical_bytes(common):
        _fail("RUN_RESEAL_BODY_EXPECTED_COMMON_REQUEST_MISMATCH")
    if any(
        body.get(field) is not False
        for field in (
            "grants_trading_authority",
            "grants_statistical_sufficiency_authority",
            "grants_profitability_authority",
        )
    ):
        _fail("RUN_RESEAL_BODY_FORBIDDEN_AUTHORITY")
    if tuple(body.get("symbols") or ()) != tuple(PRODUCTION_SYMBOL_ORDER):
        _fail("RUN_RESEAL_BODY_SYMBOL_ORDER_INVALID")
    if body.get("symbol_order_sha256") != PRODUCTION_SYMBOL_ORDER_SHA256:
        _fail("RUN_RESEAL_BODY_SYMBOL_ORDER_HASH_INVALID")
    if tuple(body.get("timeframes") or ()) != tuple(PRODUCTION_TIMEFRAME_ORDER):
        _fail("RUN_RESEAL_BODY_TIMEFRAME_ORDER_INVALID")
    if body.get("timeframe_order_sha256") != PRODUCTION_TIMEFRAME_ORDER_SHA256:
        _fail("RUN_RESEAL_BODY_TIMEFRAME_ORDER_HASH_INVALID")
    cells = body.get("cells")
    if not isinstance(cells, list) or len(cells) != PRODUCTION_CELL_COUNT:
        _fail("RUN_RESEAL_BODY_CELL_COUNT_INVALID")
    if body.get("cell_count") != PRODUCTION_CELL_COUNT:
        _fail("RUN_RESEAL_BODY_CELL_COUNT_INVALID")
    expected_identities = [
        (index, symbol, timeframe)
        for index, (symbol, timeframe) in enumerate(
            (
                (symbol, timeframe)
                for symbol in PRODUCTION_SYMBOL_ORDER
                for timeframe in PRODUCTION_TIMEFRAME_ORDER
            ),
            start=1,
        )
    ]
    for cell, (ordinal, symbol, timeframe) in zip(cells, expected_identities):
        if not isinstance(cell, Mapping) or set(cell) != _CELL_KEYS:
            _fail("RUN_RESEAL_BODY_CELL_SCHEMA_INVALID")
        if (
            cell.get("ordinal") != ordinal
            or cell.get("symbol") != symbol
            or cell.get("timeframe") != timeframe
            or cell.get("cell_id") != f"{symbol}|{timeframe}"
            or cell.get("relative_leaf") != f"{symbol}/{timeframe}"
        ):
            _fail("RUN_RESEAL_BODY_CELL_ORDER_INVALID")
        if not _is_sha256(cell.get("manifest_result_sha256")):
            _fail("RUN_RESEAL_BODY_RESULT_HASH_INVALID")
        if not _is_sha256(cell.get("provenance_sha256")):
            _fail("RUN_RESEAL_BODY_PROVENANCE_HASH_INVALID")
        if (
            cell.get("acquisition_status") not in {"success", "partial"}
            or cell.get("artifact_present") is not True
            or not _is_sha256(cell.get("bars_sha256"))
            or type(cell.get("bars_bytes")) is not int
            or cell.get("bars_bytes") <= 0
            or type(cell.get("bars_rows")) is not int
            or cell.get("bars_rows") <= 0
            or cell.get("provenance_request_interval_bound") is not True
        ):
            _fail("RUN_RESEAL_BODY_PUBLISHABLE_CELL_INVALID")
    if body.get("cells_sha256") != _canonical_sha256(cells):
        _fail("RUN_RESEAL_BODY_CELLS_HASH_INVALID")
    artifact_count = sum(cell.get("artifact_present") is True for cell in cells)
    if body.get("artifact_cell_count") != artifact_count:
        _fail("RUN_RESEAL_BODY_CELL_COUNTS_INVALID")
    if artifact_count != PRODUCTION_CELL_COUNT:
        _fail("RUN_RESEAL_BODY_UNRESOLVED_FAILURE_CELL")
    return body


_RUNTIME_GLOBAL_IDENTITIES: Final = tuple(
    (name, globals()[name])
    for name in (
        "_safe_identifier",
        "_parse_utc",
        "_iso_utc",
        "_iso_second",
        "_canonical_bytes",
        "_canonical_sha256",
        "_deep_freeze",
        "_preflight_external_json_structure",
        "_strict_json",
        "_validate_resume_seed",
        "_expected_authority_payload",
        "build_run_reseal_expected_authority",
        "_validated_expected_authority",
        "run_reseal_expected_authority_to_mapping",
        "run_reseal_expected_authority_from_mapping",
        "_unsafe_file_attributes",
        "_file_identity",
        "_directory_identity",
        "_same_open_file_identity",
        "MAX_TOTAL_ARTIFACT_BYTES",
        "_bounded_directory_entries",
        "_validate_run_topology",
        "_secure_native_root",
        "_preflight_regular_file",
        "_open_preflighted",
        "_read_preflighted",
        "_verify_preflighted_files",
        "_scan_bars",
        "_reject_authority_claim_fields",
        "_validate_publishable_provenance_schema",
        "_validate_provenance_common",
        "_expected_interval",
        "_cell_payload",
        "materialize_run_reseal_body",
        "_run_reseal_signature_payload",
        "build_run_reseal_trust_root",
        "_validate_signed_body_shape",
        "validate_v5_production_manifest",
        "validate_v5_cell_status",
        "validate_v5_leaf_request",
        "validate_source_identity_match",
        "build_v5_cell_validation_context",
        "scan_native_csv_stream",
        "NativeCsvValidationError",
        "build_trusted_v5_policy",
        "canonical_json_bytes",
        "Ed25519PublicKey",
        "InvalidSignature",
        "VerifiedRunReseal",
    )
)
_RUNTIME_MEMBER_IDENTITIES: Final = (
    (_DirectoryGuard, "guard_chain", _DirectoryGuard.guard_chain),
    (_DirectoryGuard, "_guard_one", _DirectoryGuard._guard_one),
    (_DirectoryGuard, "verify", _DirectoryGuard.verify),
    (VerifiedRunReseal, "__init__", VerifiedRunReseal.__init__),
    (VerifiedRunReseal, "__setattr__", VerifiedRunReseal.__setattr__),
)


def _run_reseal_runtime_intact() -> bool:
    try:
        return all(
            globals().get(name) is expected
            for name, expected in _RUNTIME_GLOBAL_IDENTITIES
        ) and all(
            getattr(owner, name, None) is expected
            for owner, name, expected in _RUNTIME_MEMBER_IDENTITIES
        )
    except BaseException:
        return False


def verify_run_reseal_envelope(
    native_root: str | os.PathLike[str],
    envelope: Any,
    *,
    trust_root: RunResealTrustRoot,
    expected_authority: RunResealExpectedAuthority,
    validation_time_utc: Any,
    _runtime_integrity: Any = _run_reseal_runtime_intact,
) -> VerifiedRunReseal:
    """Verify an external envelope and independently replay every exact pin."""

    if (
        globals().get("_run_reseal_runtime_intact") is not _runtime_integrity
        or not _runtime_integrity()
    ):
        _fail("RUN_RESEAL_RUNTIME_INTEGRITY_INVALID")
    if type(trust_root) is not RunResealTrustRoot:
        _fail("RUN_RESEAL_TRUST_ROOT_REQUIRED")
    trusted = build_run_reseal_trust_root(
        key_id=trust_root.key_id,
        public_key_base64=trust_root.public_key_base64,
    )
    if trust_root.schema_version != RUN_RESEAL_TRUST_ROOT_SCHEMA_VERSION:
        _fail("RUN_RESEAL_TRUST_ROOT_SCHEMA_INVALID")
    if not isinstance(envelope, Mapping) or set(envelope) != _ENVELOPE_KEYS:
        _fail("RUN_RESEAL_ENVELOPE_SCHEMA_INVALID")
    if envelope.get("schema_version") != RUN_RESEAL_ENVELOPE_SCHEMA_VERSION:
        _fail("RUN_RESEAL_ENVELOPE_SCHEMA_INVALID")
    if envelope.get("key_id") != trusted.key_id:
        _fail("RUN_RESEAL_ENVELOPE_KEY_ID_MISMATCH")
    authority = _validated_expected_authority(expected_authority)
    body = _validate_signed_body_shape(
        envelope.get("body"), expected_authority=authority
    )
    canonical_run_id = _safe_identifier(
        body.get("run_id"), code="RUN_RESEAL_RUN_ID_INVALID", pattern=_RUN_ID_RE
    )
    signature_text = envelope.get("signature_base64")
    if not isinstance(signature_text, str) or len(signature_text) > 128:
        _fail("RUN_RESEAL_SIGNATURE_INVALID")
    try:
        signature = base64.b64decode(signature_text, validate=True)
        public_key = base64.b64decode(trusted.public_key_base64, validate=True)
    except (TypeError, ValueError) as exc:
        raise NativeRunResealError("RUN_RESEAL_SIGNATURE_INVALID") from exc
    if len(signature) != 64 or len(public_key) != 32:
        _fail("RUN_RESEAL_SIGNATURE_INVALID")
    try:
        Ed25519PublicKey.from_public_bytes(public_key).verify(
            signature,
            _run_reseal_signature_payload(trusted.key_id, body),
        )
    except (InvalidSignature, TypeError, ValueError) as exc:
        raise NativeRunResealError("RUN_RESEAL_SIGNATURE_INVALID") from exc

    validation_time = _iso_second(
        validation_time_utc,
        code="RUN_RESEAL_VALIDATION_TIME_INVALID",
    )
    if _parse_utc(
        body.get("issued_at_utc"), code="RUN_RESEAL_ISSUED_AT_INVALID"
    ) > _parse_utc(validation_time, code="RUN_RESEAL_VALIDATION_TIME_INVALID"):
        _fail("RUN_RESEAL_FUTURE_ISSUE_TIME")

    replayed = materialize_run_reseal_body(
        native_root,
        expected_authority=authority,
        issued_at_utc=body.get("issued_at_utc"),
    )
    if _canonical_bytes(replayed) != _canonical_bytes(body):
        _fail("RUN_RESEAL_SIGNED_BODY_REPLAY_MISMATCH")
    envelope_sha256 = _canonical_sha256(envelope)
    return VerifiedRunReseal(
        schema_version=RUN_RESEAL_VERIFICATION_RECEIPT_SCHEMA_VERSION,
        run_id=canonical_run_id,
        key_id=trusted.key_id,
        body_sha256=_canonical_sha256(body),
        envelope_sha256=envelope_sha256,
        origin_manifest_sha256=str(body["origin_manifest_sha256"]),
        expected_authority_sha256=str(body["expected_authority_sha256"]),
        trusted_policy_sha256=str(body["trusted_policy_sha256"]),
        cell_count=int(body["cell_count"]),
        artifact_cell_count=int(body["artifact_cell_count"]),
        issued_at_utc=str(body["issued_at_utc"]),
        validated_at_utc=validation_time,
        body=_deep_freeze(body),
        _seal=_VERIFIED_RUN_RESEAL_SEAL,
    )


__all__ = [
    "MAX_EXTERNAL_JSON_BYTES",
    "MAX_EXTERNAL_JSON_DEPTH",
    "MAX_EXTERNAL_JSON_STRUCTURAL_TOKENS",
    "NativeRunResealError",
    "RUN_RESEAL_AUTHORITY_SCOPE",
    "RUN_RESEAL_CONTRACT_VERSION",
    "RUN_RESEAL_ENVELOPE_SCHEMA_VERSION",
    "RUN_RESEAL_EXPECTED_AUTHORITY_SCHEMA_VERSION",
    "RUN_RESEAL_SIGNATURE_ALGORITHM",
    "RUN_RESEAL_SIGNATURE_DOMAIN",
    "RUN_RESEAL_SIGNING_REQUEST_SCHEMA_VERSION",
    "RUN_RESEAL_TRUST_ROOT_SCHEMA_VERSION",
    "RUN_RESEAL_VERIFICATION_RECEIPT_SCHEMA_VERSION",
    "RunResealTrustRoot",
    "RunResealExpectedAuthority",
    "VerifiedRunReseal",
    "build_run_reseal_signing_request",
    "build_run_reseal_expected_authority",
    "build_run_reseal_trust_root",
    "load_bounded_json_file",
    "materialize_run_reseal_body",
    "run_reseal_trust_root_from_mapping",
    "run_reseal_expected_authority_from_mapping",
    "run_reseal_expected_authority_to_mapping",
    "verify_run_reseal_envelope",
]
