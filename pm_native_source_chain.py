"""Lean, bounded resolver for one preserved native origin and one current overlay.

This module intentionally has no dependency on the deprecated composite/lifecycle
authorities.  It verifies only the selected symbol/timeframe artifacts, then streams
their CSVs in bounded chunks.  A chain is immutable and contains at most two
segments: the preserved origin and a replace-in-place current overlay.

The preserved V1 acquisition is useful historical evidence, but its leading prefix
is explicitly ``LEGACY_PREFIX_UNVERIFIED``.  That limitation is never interpreted as
broker absence or as an economic/no-trade decision.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Iterator, Mapping, Optional, Sequence

import pandas as pd


DESCRIPTOR_SCHEMA = "fxpm.native_source_chain.v1"
RECEIPT_SCHEMA = "fxpm.native_source_chain_receipt.v1"
ORIGIN_MANIFEST_SCHEMA = "fxpm.native_history_run_manifest.v1"
ORIGIN_PROVENANCE_SCHEMA = "fxpm-native-bars-v1"
OVERLAY_MANIFEST_SCHEMA = "fxpm.native_overlay_run_manifest.v1"
OVERLAY_PROVENANCE_SCHEMA = "fxpm.native_overlay_leaf.v1"
LEGACY_PREFIX_LIMITATION = "LEGACY_PREFIX_UNVERIFIED"

ORIGIN_ROLE = "PRESERVED_ORIGIN"
OVERLAY_ROLE = "CURRENT_OVERLAY"
NATIVE_TIMEFRAMES = frozenset({"M5", "M15", "M30", "H1", "H4", "D1"})
LEGACY_ORIGIN_SYMBOL_ALIASES = {
    "Platinum": "PLATINUM",
    "Palladium": "PALLADIUM",
}
EXECUTION_COLUMNS = (
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
PRICE_COLUMNS = ("Open", "High", "Low", "Close")
INTEGER_COLUMNS = ("Volume", "Spread", "RealVolume")
TIMESTAMP_COLUMNS = ("_SOURCE_FIRST_TS", "_SOURCE_LAST_TS", "_BAR_CLOSE_TS")

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9_.-]{1,128}")
_MAX_DESCRIPTOR_BYTES = 64 * 1024


class NativeSourceChainError(RuntimeError):
    """Base class carrying a stable, non-economic failure code."""

    code = "SOURCE_CHAIN_ERROR"

    def __init__(self, message: str, *, technical_status: Optional[str] = None):
        super().__init__(message)
        self.technical_status = technical_status


class SourceIntegrityError(NativeSourceChainError):
    code = "SOURCE_INTEGRITY_ERROR"


class SourceRevisionConflict(NativeSourceChainError):
    code = "SOURCE_REVISION_CONFLICT"


class SourceChainGap(NativeSourceChainError):
    code = "SOURCE_CHAIN_GAP"


class BaselineRefreshRequired(NativeSourceChainError):
    code = "BASELINE_REFRESH_REQUIRED"


class SourceUnavailable(NativeSourceChainError):
    code = "SOURCE_UNAVAILABLE"


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _strict_json(raw: bytes, label: str) -> Mapping[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def no_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON constant: {value}")

    try:
        decoded = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=no_duplicates,
            parse_constant=no_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise SourceIntegrityError(f"{label} is not strict UTF-8 JSON") from exc
    if not isinstance(decoded, Mapping):
        raise SourceIntegrityError(f"{label} is not a JSON object")
    return decoded


def _normal_label(value: Any, name: str) -> str:
    if not isinstance(value, str) or value != value.strip() or not value:
        raise SourceIntegrityError(f"{name} must be a non-empty canonical string")
    if len(value) > 256 or any(ord(character) < 32 for character in value):
        raise SourceIntegrityError(f"{name} is not a bounded canonical string")
    return value


def _normal_identifier(value: Any, name: str) -> str:
    result = _normal_label(value, name)
    if _IDENTIFIER_RE.fullmatch(result) is None:
        raise SourceIntegrityError(f"{name} is not a canonical identifier")
    return result


def _normal_sha256(value: Any, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise SourceIntegrityError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _normal_nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SourceIntegrityError(f"{name} must be a nonnegative integer")
    return value


def _normal_positive_int(value: Any, name: str) -> int:
    result = _normal_nonnegative_int(value, name)
    if result == 0:
        raise SourceIntegrityError(f"{name} must be positive")
    return result


def _normal_relative_path(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise SourceIntegrityError(f"{name} is not a canonical POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or path.as_posix() != value:
        raise SourceIntegrityError(f"{name} must be a canonical relative path")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise SourceIntegrityError(f"{name} contains traversal components")
    return value


def _account_text(value: Any, name: str) -> str:
    if isinstance(value, bool):
        raise SourceIntegrityError(f"{name} is invalid")
    if isinstance(value, int):
        value = str(value)
    return _normal_label(value, name)


def _timestamp_ns(value: Any, name: str) -> int:
    if not isinstance(value, (str, datetime, pd.Timestamp)):
        raise SourceIntegrityError(f"{name} is not a timestamp")
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SourceIntegrityError(f"{name} is not a valid timestamp") from exc
    if pd.isna(timestamp) or timestamp.tzinfo is None:
        raise SourceIntegrityError(f"{name} must be timezone-aware")
    try:
        return int(timestamp.tz_convert("UTC").value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SourceIntegrityError(f"{name} is outside supported UTC range") from exc


def _ns_text(value: int) -> str:
    return pd.Timestamp(value, unit="ns", tz="UTC").isoformat()


@dataclass(frozen=True)
class ArtifactPin:
    relative_path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "relative_path",
            _normal_relative_path(self.relative_path, "artifact relative_path"),
        )
        object.__setattr__(self, "sha256", _normal_sha256(self.sha256, "artifact sha256"))
        object.__setattr__(self, "bytes", _normal_nonnegative_int(self.bytes, "artifact bytes"))

    def to_mapping(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "bytes": self.bytes,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ArtifactPin":
        return cls(
            relative_path=value.get("relative_path"),
            sha256=value.get("sha256"),
            bytes=value.get("bytes"),
        )


@dataclass(frozen=True)
class NativeCellIdentity:
    broker: str
    server: str
    account_id: str
    canonical_symbol: str
    broker_symbol: str
    timeframe: str
    source_kind: str = "broker_native"
    origin_symbol: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "broker", _normal_label(self.broker, "broker"))
        object.__setattr__(self, "server", _normal_label(self.server, "server"))
        object.__setattr__(
            self, "account_id", _account_text(self.account_id, "account_id")
        )
        object.__setattr__(
            self,
            "canonical_symbol",
            _normal_identifier(self.canonical_symbol, "canonical_symbol"),
        )
        object.__setattr__(
            self,
            "broker_symbol",
            _normal_identifier(self.broker_symbol, "broker_symbol"),
        )
        if self.timeframe not in NATIVE_TIMEFRAMES:
            raise SourceIntegrityError("timeframe must be an exact native timeframe")
        if self.source_kind != "broker_native":
            raise SourceIntegrityError("source_kind must be broker_native")
        origin_symbol = self.origin_symbol or self.canonical_symbol
        origin_symbol = _normal_identifier(origin_symbol, "origin_symbol")
        permitted_origin = LEGACY_ORIGIN_SYMBOL_ALIASES.get(
            self.canonical_symbol, self.canonical_symbol
        )
        if origin_symbol != permitted_origin:
            raise SourceIntegrityError(
                "origin_symbol is not the exact canonical symbol or an authenticated legacy alias"
            )
        object.__setattr__(self, "origin_symbol", origin_symbol)

    def to_mapping(self) -> dict[str, str]:
        return {
            "broker": self.broker,
            "server": self.server,
            "account_id": self.account_id,
            "canonical_symbol": self.canonical_symbol,
            "broker_symbol": self.broker_symbol,
            "timeframe": self.timeframe,
            "source_kind": self.source_kind,
            "origin_symbol": self.origin_symbol,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "NativeCellIdentity":
        return cls(**{key: value.get(key) for key in cls.__dataclass_fields__})


@dataclass(frozen=True)
class SourceChainLimits:
    chunk_rows: int = 25_000
    max_origin_rows: int = 2_000_000
    max_origin_bytes: int = 512 * 1024 * 1024
    max_overlay_rows: int = 250_000
    max_overlay_bytes: int = 128 * 1024 * 1024
    max_output_rows: int = 2_000_000
    max_manifest_bytes: int = 16 * 1024 * 1024
    max_provenance_bytes: int = 4 * 1024 * 1024

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            object.__setattr__(self, name, _normal_positive_int(getattr(self, name), name))
        if self.chunk_rows > 100_000:
            raise SourceIntegrityError("chunk_rows exceeds the fixed 100000-row ceiling")

    def to_mapping(self) -> dict[str, int]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SourceChainLimits":
        return cls(**{key: value.get(key) for key in cls.__dataclass_fields__})


@dataclass(frozen=True)
class SourceSegment:
    role: str
    run_id: str
    manifest: ArtifactPin
    provenance: ArtifactPin
    bars: Optional[ArtifactPin]
    rows: int
    status: str
    failure_status: Optional[str]
    limitation: Optional[str]

    def __post_init__(self) -> None:
        if self.role not in {ORIGIN_ROLE, OVERLAY_ROLE}:
            raise SourceIntegrityError("segment role is invalid")
        object.__setattr__(self, "run_id", _normal_identifier(self.run_id, "run_id"))
        if not isinstance(self.manifest, ArtifactPin) or not isinstance(
            self.provenance, ArtifactPin
        ):
            raise SourceIntegrityError("segment JSON artifacts must be ArtifactPin values")
        if self.bars is not None and not isinstance(self.bars, ArtifactPin):
            raise SourceIntegrityError("segment bars must be an ArtifactPin or null")
        object.__setattr__(self, "rows", _normal_nonnegative_int(self.rows, "rows"))
        object.__setattr__(self, "status", _normal_label(self.status, "status"))
        if "NO_TRADE" in self.status.upper():
            raise SourceIntegrityError("economic NO_TRADE states are forbidden in source data")
        if self.status not in {"success", "partial", "failed"}:
            raise SourceIntegrityError("segment status is not a source-data state")
        if self.failure_status is not None:
            object.__setattr__(
                self,
                "failure_status",
                _normal_label(self.failure_status, "failure_status"),
            )
            if "NO_TRADE" in self.failure_status.upper():
                raise SourceIntegrityError(
                    "economic NO_TRADE states are forbidden in source data"
                )
        if self.status == "success" and self.failure_status is not None:
            raise SourceIntegrityError("successful source segment cannot carry a failure status")
        if self.status == "failed" and self.failure_status is None:
            raise SourceIntegrityError("failed source segment must carry a technical status")
        if self.role == ORIGIN_ROLE:
            if self.limitation != LEGACY_PREFIX_LIMITATION:
                raise SourceIntegrityError(
                    "preserved origin must declare LEGACY_PREFIX_UNVERIFIED"
                )
            if self.bars is None or self.rows == 0:
                raise SourceIntegrityError("preserved origin must pin non-empty bars")
        elif self.limitation is not None:
            raise SourceIntegrityError("current overlay must not relabel a legacy limitation")
        if self.bars is None and self.rows != 0:
            raise SourceIntegrityError("a segment without bars must declare zero rows")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "run_id": self.run_id,
            "manifest": self.manifest.to_mapping(),
            "provenance": self.provenance.to_mapping(),
            "bars": None if self.bars is None else self.bars.to_mapping(),
            "rows": self.rows,
            "status": self.status,
            "failure_status": self.failure_status,
            "limitation": self.limitation,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SourceSegment":
        bars = value.get("bars")
        return cls(
            role=value.get("role"),
            run_id=value.get("run_id"),
            manifest=ArtifactPin.from_mapping(value.get("manifest") or {}),
            provenance=ArtifactPin.from_mapping(value.get("provenance") or {}),
            bars=None if bars is None else ArtifactPin.from_mapping(bars),
            rows=value.get("rows"),
            status=value.get("status"),
            failure_status=value.get("failure_status"),
            limitation=value.get("limitation"),
        )


@dataclass(frozen=True)
class NativeSourceChainDescriptor:
    identity: NativeCellIdentity
    segments: tuple[SourceSegment, ...]
    limits: SourceChainLimits = SourceChainLimits()
    schema_version: str = DESCRIPTOR_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != DESCRIPTOR_SCHEMA:
            raise SourceIntegrityError("source-chain descriptor schema is invalid")
        if not isinstance(self.identity, NativeCellIdentity):
            raise SourceIntegrityError("descriptor identity is invalid")
        if not isinstance(self.limits, SourceChainLimits):
            raise SourceIntegrityError("descriptor limits are invalid")
        segments = tuple(self.segments)
        object.__setattr__(self, "segments", segments)
        if not segments or len(segments) > 2:
            raise BaselineRefreshRequired(
                "a source chain must have one origin and at most one current overlay"
            )
        if segments[0].role != ORIGIN_ROLE:
            raise SourceIntegrityError("the first source-chain segment must be the origin")
        if len(segments) == 2 and segments[1].role != OVERLAY_ROLE:
            raise SourceIntegrityError("the second segment must be the current overlay")
        if len({segment.run_id for segment in segments}) != len(segments):
            raise SourceIntegrityError("source-chain run IDs must be unique")
        raw = self.canonical_bytes()
        if len(raw) > _MAX_DESCRIPTOR_BYTES:
            raise BaselineRefreshRequired("source-chain descriptor byte cap exceeded")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "identity": self.identity.to_mapping(),
            "segments": [segment.to_mapping() for segment in self.segments],
            "limits": self.limits.to_mapping(),
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.to_mapping())

    @property
    def sha256(self) -> str:
        return _sha256_bytes(self.canonical_bytes())

    @classmethod
    def from_bytes(cls, raw: bytes) -> "NativeSourceChainDescriptor":
        if len(raw) > _MAX_DESCRIPTOR_BYTES:
            raise BaselineRefreshRequired("source-chain descriptor byte cap exceeded")
        value = _strict_json(raw, "source-chain descriptor")
        if value.get("schema_version") != DESCRIPTOR_SCHEMA:
            raise SourceIntegrityError("source-chain descriptor schema is invalid")
        identity = value.get("identity")
        segments = value.get("segments")
        limits = value.get("limits")
        if not isinstance(identity, Mapping) or not isinstance(segments, list) or not isinstance(
            limits, Mapping
        ):
            raise SourceIntegrityError("source-chain descriptor shape is invalid")
        return cls(
            identity=NativeCellIdentity.from_mapping(identity),
            segments=tuple(SourceSegment.from_mapping(item) for item in segments),
            limits=SourceChainLimits.from_mapping(limits),
        )


@dataclass(frozen=True)
class NativeSourceChainReceipt:
    descriptor_sha256: str
    identity: NativeCellIdentity
    request_start_utc: str
    request_end_exclusive_utc: str
    rows: int
    origin_rows_owned: int
    overlay_rows_owned: int
    overlap_rows_compared: int
    verified_bytes: int
    maximum_chunk_rows_observed: int
    first_open_utc: str
    last_open_utc: str
    last_close_utc: str
    overlay_first_owned_open_utc: Optional[str]
    overlay_first_owned_close_utc: Optional[str]
    origin_status: str
    origin_failure_status: Optional[str]
    overlay_status: Optional[str]
    overlay_failure_status: Optional[str]
    limitations: tuple[str, ...]
    segment_pins: tuple[Mapping[str, Any], ...]
    schema_version: str = RECEIPT_SCHEMA

    def to_mapping(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "descriptor_sha256": self.descriptor_sha256,
            "identity": self.identity.to_mapping(),
            "request": {
                "start_utc": self.request_start_utc,
                "end_exclusive_utc": self.request_end_exclusive_utc,
            },
            "result": {
                "status": "SOURCE_AVAILABLE",
                "rows": self.rows,
                "origin_rows_owned": self.origin_rows_owned,
                "overlay_rows_owned": self.overlay_rows_owned,
                "overlap_rows_compared": self.overlap_rows_compared,
                "verified_bytes": self.verified_bytes,
                "maximum_chunk_rows_observed": self.maximum_chunk_rows_observed,
                "first_open_utc": self.first_open_utc,
                "last_open_utc": self.last_open_utc,
                "last_close_utc": self.last_close_utc,
                "overlay_first_owned_open_utc": self.overlay_first_owned_open_utc,
                "overlay_first_owned_close_utc": self.overlay_first_owned_close_utc,
            },
            "source_status": {
                "origin_status": self.origin_status,
                "origin_failure_status": self.origin_failure_status,
                "overlay_status": self.overlay_status,
                "overlay_failure_status": self.overlay_failure_status,
            },
            "limitations": list(self.limitations),
            "segment_pins": [dict(item) for item in self.segment_pins],
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.to_mapping())

    @property
    def sha256(self) -> str:
        return _sha256_bytes(self.canonical_bytes())


@dataclass(frozen=True)
class ResolvedNativeCell:
    frame: pd.DataFrame
    receipt: NativeSourceChainReceipt


@dataclass(frozen=True)
class _VerifiedSegment:
    descriptor: SourceSegment
    provenance: Mapping[str, Any]
    bars_path: Optional[Path]
    actual_first_open_ns: Optional[int]
    actual_last_open_ns: Optional[int]
    actual_last_close_ns: Optional[int]
    request_start_ns: Optional[int]
    request_end_ns: Optional[int]


@dataclass(frozen=True)
class _CanonicalBar:
    open_ns: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    spread: int
    real_volume: int
    source_first_ns: int
    source_last_ns: int
    close_ns: int

    def comparison_tuple(self) -> tuple[Any, ...]:
        return (
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.spread,
            self.real_volume,
            self.source_first_ns,
            self.source_last_ns,
            self.close_ns,
        )


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SourceIntegrityError(f"{name} must be a JSON object")
    return value


def _source_identity_from_account(source_environment: Any) -> tuple[str, str, str]:
    environment = _mapping(source_environment, "source_environment")
    account = _mapping(environment.get("account"), "source_environment.account")
    return (
        _normal_label(account.get("company"), "account company"),
        _normal_label(account.get("server"), "account server"),
        _account_text(account.get("login"), "account login"),
    )


def _source_identity_from_overlay(value: Any) -> tuple[str, str, str, str]:
    identity = _mapping(value, "source_identity")
    return (
        _normal_label(identity.get("broker"), "source_identity broker"),
        _normal_label(identity.get("server"), "source_identity server"),
        _account_text(identity.get("account_id"), "source_identity account_id"),
        _normal_label(identity.get("source_kind"), "source_identity source_kind"),
    )


class NativeSourceChainResolver:
    """Verify and resolve one symbol/timeframe without scanning other cells."""

    def __init__(
        self,
        native_root: Path | str,
        descriptor: NativeSourceChainDescriptor,
    ) -> None:
        self.native_root = Path(native_root).resolve(strict=True)
        if not self.native_root.is_dir():
            raise SourceIntegrityError("native_root must be a directory")
        if not isinstance(descriptor, NativeSourceChainDescriptor):
            raise SourceIntegrityError("descriptor must be NativeSourceChainDescriptor")
        self.descriptor = descriptor
        self._maximum_chunk_rows_observed = 0
        self._verified_bytes = 0
        self._enforce_declared_caps()

    def _enforce_declared_caps(self) -> None:
        limits = self.descriptor.limits
        for segment in self.descriptor.segments:
            if segment.role == ORIGIN_ROLE:
                max_rows, max_bytes = limits.max_origin_rows, limits.max_origin_bytes
            else:
                max_rows, max_bytes = limits.max_overlay_rows, limits.max_overlay_bytes
            if segment.rows > max_rows:
                raise BaselineRefreshRequired(
                    f"{segment.role} declared row cap exceeded"
                )
            if segment.bars is not None and segment.bars.bytes > max_bytes:
                raise BaselineRefreshRequired(
                    f"{segment.role} declared byte cap exceeded"
                )
            if segment.manifest.bytes > limits.max_manifest_bytes:
                raise BaselineRefreshRequired("manifest byte cap exceeded")
            if segment.provenance.bytes > limits.max_provenance_bytes:
                raise BaselineRefreshRequired("provenance byte cap exceeded")

    def _path(self, relative: str) -> Path:
        canonical = _normal_relative_path(relative, "artifact path")
        path = self.native_root.joinpath(*PurePosixPath(canonical).parts)
        if path.is_symlink() or not path.is_file():
            raise SourceIntegrityError(f"pinned artifact is absent or not a regular file: {canonical}")
        resolved = path.resolve(strict=True)
        try:
            resolved.relative_to(self.native_root)
        except ValueError as exc:
            raise SourceIntegrityError(f"pinned artifact escapes native root: {canonical}") from exc
        return resolved

    def _verify_pin(self, pin: ArtifactPin, cap: int, label: str) -> tuple[Path, bytes]:
        if pin.bytes > cap:
            raise BaselineRefreshRequired(f"{label} byte cap exceeded")
        path = self._path(pin.relative_path)
        before = path.stat()
        if int(before.st_size) != pin.bytes:
            raise SourceIntegrityError(f"{label} byte count does not match its pin")
        digest = hashlib.sha256()
        raw_parts: Optional[list[bytes]] = [] if label != "bars" else None
        with path.open("rb") as handle:
            while True:
                block = handle.read(1024 * 1024)
                if not block:
                    break
                digest.update(block)
                if raw_parts is not None:
                    raw_parts.append(block)
        after = path.stat()
        if (
            int(after.st_size) != int(before.st_size)
            or int(after.st_mtime_ns) != int(before.st_mtime_ns)
        ):
            raise SourceIntegrityError(f"{label} changed while it was verified")
        if digest.hexdigest() != pin.sha256:
            raise SourceIntegrityError(f"{label} SHA-256 does not match its pin")
        self._verified_bytes += pin.bytes
        return path, b"" if raw_parts is None else b"".join(raw_parts)

    def _expected_paths(self, segment: SourceSegment) -> tuple[str, str, str]:
        identity = self.descriptor.identity
        if segment.role == ORIGIN_ROLE:
            prefix = f"runs/{segment.run_id}"
            symbol = identity.origin_symbol
        else:
            prefix = f"overlays/runs/{segment.run_id}"
            symbol = identity.canonical_symbol
        leaf = f"{symbol}/{identity.timeframe}"
        return (
            f"{prefix}/run_manifest.json",
            f"{prefix}/{leaf}/provenance.json",
            f"{prefix}/{leaf}/bars.csv",
        )

    def _assert_source_identity(self, observed: Sequence[str], label: str) -> None:
        identity = self.descriptor.identity
        expected = (identity.broker, identity.server, identity.account_id)
        if tuple(observed[:3]) != expected:
            raise SourceIntegrityError(f"{label} source identity does not match the chain")
        if len(observed) == 4 and observed[3] != identity.source_kind:
            raise SourceIntegrityError(f"{label} source kind does not match the chain")

    def _selected_result(
        self, manifest: Mapping[str, Any], *, overlay: bool
    ) -> Mapping[str, Any]:
        results = manifest.get("results")
        if not isinstance(results, list):
            raise SourceIntegrityError("run manifest results must be a list")
        identity = self.descriptor.identity
        selected: list[Mapping[str, Any]] = []
        for raw in results:
            if not isinstance(raw, Mapping):
                continue
            symbol_key = "canonical_symbol" if overlay else "symbol"
            expected_symbol = identity.canonical_symbol if overlay else identity.origin_symbol
            if raw.get(symbol_key) == expected_symbol and raw.get(
                "timeframe"
            ) == identity.timeframe:
                selected.append(raw)
        if len(selected) != 1:
            raise SourceIntegrityError(
                "run manifest must bind exactly one selected symbol/timeframe result"
            )
        return selected[0]

    def _validate_common_leaf(
        self,
        segment: SourceSegment,
        provenance: Mapping[str, Any],
        result: Mapping[str, Any],
        *,
        overlay: bool,
    ) -> None:
        identity = self.descriptor.identity
        run_field = provenance.get("run_id")
        if run_field != segment.run_id:
            raise SourceIntegrityError("provenance run_id does not match the segment")
        canonical = provenance.get("canonical_symbol") if overlay else provenance.get("symbol")
        expected_symbol = identity.canonical_symbol if overlay else identity.origin_symbol
        if canonical != expected_symbol:
            raise SourceIntegrityError("provenance canonical symbol is not an exact case match")
        if provenance.get("broker_symbol") != identity.broker_symbol:
            raise SourceIntegrityError("provenance broker symbol is not an exact case match")
        if provenance.get("timeframe") != identity.timeframe:
            raise SourceIntegrityError("provenance timeframe is not the exact native timeframe")
        if provenance.get("immutable_record") is not True:
            raise SourceIntegrityError("leaf provenance is not immutable")
        if not overlay and provenance.get("source_kind") != identity.source_kind:
            raise SourceIntegrityError("origin provenance source kind is not broker_native")
        if overlay and result.get("broker_symbol") != identity.broker_symbol:
            raise SourceIntegrityError("overlay result broker symbol is not an exact case match")
        if provenance.get("status") != segment.status or result.get("status") != segment.status:
            raise SourceIntegrityError("segment status is not hash-bound consistently")
        if provenance.get("failure_status") != segment.failure_status or result.get(
            "failure_status"
        ) != segment.failure_status:
            raise SourceIntegrityError("segment failure status is not hash-bound consistently")
        leaf_symbol = identity.canonical_symbol if overlay else identity.origin_symbol
        relative_leaf = f"{leaf_symbol}/{identity.timeframe}"
        if result.get("relative_leaf") != relative_leaf:
            raise SourceIntegrityError("selected result relative_leaf is spliced")
        artifact = provenance.get("artifact")
        if segment.bars is None:
            if artifact is not None or segment.rows != 0:
                raise SourceIntegrityError("bars-free segment has inconsistent artifact metadata")
            if _normal_nonnegative_int(result.get("rows"), "result rows") != 0:
                raise SourceIntegrityError("bars-free selected result declares rows")
            if overlay and (
                result.get("provenance_sha256") != segment.provenance.sha256
                or result.get("bars_sha256") is not None
                or result.get("bytes") != 0
            ):
                raise SourceIntegrityError(
                    "bars-free overlay result pins are inconsistent"
                )
            return
        artifact_map = _mapping(artifact, "provenance artifact")
        if artifact_map.get("relative_path") != "bars.csv":
            raise SourceIntegrityError("leaf bars relative path is not canonical")
        checks = [
            artifact_map.get("sha256") == segment.bars.sha256,
            artifact_map.get("bytes") == segment.bars.bytes,
            artifact_map.get("rows") == segment.rows,
            result.get("rows") == segment.rows,
        ]
        if not overlay:
            # The legacy native V1 leaf duplicated these fields at top level.
            # Overlay V1 deliberately has a single leaf artifact authority and
            # an independently pinned run result instead of repeating them.
            checks.extend(
                [
                    provenance.get("sha256") == segment.bars.sha256,
                    provenance.get("rows") == segment.rows,
                    result.get("sha256") == segment.bars.sha256,
                ]
            )
        if not all(checks):
            raise SourceIntegrityError("bars hash/row/byte pins are inconsistent")
        if overlay:
            if result.get("bars_sha256") != segment.bars.sha256 or result.get(
                "bytes"
            ) != segment.bars.bytes:
                raise SourceIntegrityError("overlay manifest bars pin is inconsistent")
            if result.get("provenance_sha256") != segment.provenance.sha256:
                raise SourceIntegrityError("overlay manifest provenance pin is inconsistent")

    def _validate_baseline_binding(
        self,
        baseline: Any,
        origin: SourceSegment,
        label: str,
        *,
        leaf_specific: bool,
    ) -> Mapping[str, Any]:
        value = _mapping(baseline, f"{label} baseline")
        identity = self.descriptor.identity
        expected_leaf = f"{identity.origin_symbol}/{identity.timeframe}"
        checks = {
            "run_id": origin.run_id,
            "manifest_relative_path": origin.manifest.relative_path,
            "manifest_sha256": origin.manifest.sha256,
        }
        if leaf_specific:
            checks.update(
                {
                    "relative_leaf": expected_leaf,
                    "provenance_sha256": origin.provenance.sha256,
                    "bars_sha256": origin.bars.sha256 if origin.bars else None,
                    "rows": origin.rows,
                    "bytes": origin.bars.bytes if origin.bars else 0,
                }
            )
        missing = [key for key, expected in checks.items() if value.get(key) != expected]
        if missing:
            raise SourceIntegrityError(f"{label} baseline binding is spliced: {missing}")
        return value

    def _verify_segment(
        self, segment: SourceSegment, origin: SourceSegment
    ) -> _VerifiedSegment:
        manifest_path, provenance_path, bars_path = self._expected_paths(segment)
        if segment.manifest.relative_path != manifest_path:
            raise SourceIntegrityError("segment manifest path does not match its role/run")
        if segment.provenance.relative_path != provenance_path:
            raise SourceIntegrityError("segment provenance path does not match its selected leaf")
        if segment.bars is not None and segment.bars.relative_path != bars_path:
            raise SourceIntegrityError("segment bars path does not match its selected leaf")
        limits = self.descriptor.limits
        _, manifest_raw = self._verify_pin(
            segment.manifest, limits.max_manifest_bytes, "manifest"
        )
        _, provenance_raw = self._verify_pin(
            segment.provenance, limits.max_provenance_bytes, "provenance"
        )
        manifest = _strict_json(manifest_raw, "run manifest")
        provenance = _strict_json(provenance_raw, "leaf provenance")
        overlay = segment.role == OVERLAY_ROLE
        expected_manifest_schema = OVERLAY_MANIFEST_SCHEMA if overlay else ORIGIN_MANIFEST_SCHEMA
        expected_leaf_schema = OVERLAY_PROVENANCE_SCHEMA if overlay else ORIGIN_PROVENANCE_SCHEMA
        if manifest.get("schema_version") != expected_manifest_schema:
            raise SourceIntegrityError("run manifest schema does not match the segment role")
        if provenance.get("schema_version") != expected_leaf_schema:
            raise SourceIntegrityError("leaf provenance schema does not match the segment role")
        if manifest.get("run_id") != segment.run_id:
            raise SourceIntegrityError("run manifest run_id does not match the segment")
        if manifest.get("complete") is not True or manifest.get("immutable_record") is not True:
            raise SourceIntegrityError("run manifest is not complete and immutable")
        result = self._selected_result(manifest, overlay=overlay)
        if overlay:
            self._assert_source_identity(
                _source_identity_from_overlay(manifest.get("source_identity")),
                "overlay manifest",
            )
            self._assert_source_identity(
                _source_identity_from_overlay(provenance.get("source_identity")),
                "overlay provenance",
            )
            self._validate_baseline_binding(
                manifest.get("baseline"),
                origin,
                "manifest",
                leaf_specific=False,
            )
            baseline = self._validate_baseline_binding(
                provenance.get("baseline"),
                origin,
                "provenance",
                leaf_specific=True,
            )
        else:
            self._assert_source_identity(
                _source_identity_from_account(manifest.get("source_environment")),
                "origin manifest",
            )
            self._assert_source_identity(
                _source_identity_from_account(provenance.get("source_environment")),
                "origin provenance",
            )
            baseline = {}
        self._validate_common_leaf(segment, provenance, result, overlay=overlay)
        resolved_bars: Optional[Path] = None
        if segment.bars is not None:
            max_bytes = limits.max_overlay_bytes if overlay else limits.max_origin_bytes
            resolved_bars, _ = self._verify_pin(segment.bars, max_bytes, "bars")
        raw_coverage = provenance.get("coverage")
        if raw_coverage is None and overlay and segment.bars is None:
            coverage: Mapping[str, Any] = {}
        else:
            coverage = _mapping(raw_coverage, "provenance coverage")
        actual_first_open = coverage.get("actual_first_open_utc")
        actual_last_open = coverage.get("actual_last_open_utc")
        actual_last_close = coverage.get("actual_last_close_utc")
        first_open_ns = None if actual_first_open is None else _timestamp_ns(
            actual_first_open, "actual_first_open_utc"
        )
        last_open_ns = None if actual_last_open is None else _timestamp_ns(
            actual_last_open, "actual_last_open_utc"
        )
        last_close_ns = None if actual_last_close is None else _timestamp_ns(
            actual_last_close, "actual_last_close_utc"
        )
        if overlay:
            if baseline.get("actual_last_open_utc") is None or baseline.get(
                "actual_last_close_utc"
            ) is None:
                raise SourceIntegrityError("overlay baseline omits actual closed-bar seam")
            request = _mapping(provenance.get("request"), "overlay request")
            request_start_ns = _timestamp_ns(request.get("start_utc"), "overlay request start")
            request_end_ns = _timestamp_ns(
                request.get("end_exclusive_utc"), "overlay request end"
            )
            if request_start_ns >= request_end_ns:
                raise SourceIntegrityError("overlay request interval is not half-open")
        else:
            request_start_ns = None
            request_end_ns = None
        return _VerifiedSegment(
            descriptor=segment,
            provenance=provenance,
            bars_path=resolved_bars,
            actual_first_open_ns=first_open_ns,
            actual_last_open_ns=last_open_ns,
            actual_last_close_ns=last_close_ns,
            request_start_ns=request_start_ns,
            request_end_ns=request_end_ns,
        )

    @staticmethod
    def _decimal(value: Any, name: str) -> Decimal:
        try:
            result = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise SourceIntegrityError(f"{name} is not a canonical finite number") from exc
        if not result.is_finite():
            raise SourceIntegrityError(f"{name} is not finite")
        try:
            binary = float(result)
        except (ValueError, OverflowError) as exc:
            raise SourceIntegrityError(f"{name} is outside finite float range") from exc
        if not math.isfinite(binary):
            raise SourceIntegrityError(f"{name} is outside finite float range")
        return Decimal(0) if result == 0 else result.normalize()

    @staticmethod
    def _integer(value: Any, name: str) -> int:
        decimal = NativeSourceChainResolver._decimal(value, name)
        integral = decimal.to_integral_value()
        if decimal != integral:
            raise SourceIntegrityError(f"{name} is not integral")
        result = int(integral)
        if result < 0 or result > 2**63 - 1:
            raise SourceIntegrityError(f"{name} is outside the nonnegative int64 range")
        return result

    @classmethod
    def _canonical_bar(cls, raw: Mapping[str, Any]) -> _CanonicalBar:
        open_ns = _timestamp_ns(raw.get("time"), "bar time")
        source_first = _timestamp_ns(raw.get("_SOURCE_FIRST_TS"), "_SOURCE_FIRST_TS")
        source_last = _timestamp_ns(raw.get("_SOURCE_LAST_TS"), "_SOURCE_LAST_TS")
        close_ns = _timestamp_ns(raw.get("_BAR_CLOSE_TS"), "_BAR_CLOSE_TS")
        if source_first != open_ns or not (source_first <= source_last < close_ns):
            raise SourceIntegrityError("bar source interval semantics are invalid")
        prices = [cls._decimal(raw.get(column), column) for column in PRICE_COLUMNS]
        if prices[1] < max(prices[0], prices[3]) or prices[2] > min(
            prices[0], prices[3]
        ) or prices[1] < prices[2]:
            raise SourceIntegrityError("OHLC invariants are invalid")
        integers = [cls._integer(raw.get(column), column) for column in INTEGER_COLUMNS]
        return _CanonicalBar(
            open_ns,
            *prices,
            *integers,
            source_first,
            source_last,
            close_ns,
        )

    def _iter_bars(self, segment: _VerifiedSegment) -> Iterator[_CanonicalBar]:
        if segment.bars_path is None:
            return
        required = {"time", *EXECUTION_COLUMNS}
        observed_rows = 0
        prior_open: Optional[int] = None
        try:
            chunks = pd.read_csv(
                segment.bars_path,
                dtype=str,
                keep_default_na=False,
                na_filter=False,
                chunksize=self.descriptor.limits.chunk_rows,
            )
            with chunks:
                for chunk in chunks:
                    self._maximum_chunk_rows_observed = max(
                        self._maximum_chunk_rows_observed, int(len(chunk))
                    )
                    if len(chunk) > self.descriptor.limits.chunk_rows:
                        raise BaselineRefreshRequired("CSV parser exceeded the chunk row cap")
                    if len(set(chunk.columns)) != len(chunk.columns) or not required.issubset(
                        set(chunk.columns)
                    ):
                        raise SourceIntegrityError(
                            "bars CSV has missing or duplicate execution columns"
                        )
                    for raw in chunk.to_dict(orient="records"):
                        bar = self._canonical_bar(raw)
                        if prior_open is not None and bar.open_ns <= prior_open:
                            raise SourceIntegrityError(
                                "bars CSV timestamps are not unique and strictly increasing"
                            )
                        if segment.request_start_ns is not None and (
                            bar.open_ns < segment.request_start_ns
                            or bar.close_ns > segment.request_end_ns
                        ):
                            raise SourceIntegrityError(
                                "overlay bar escapes its exact transport interval"
                            )
                        prior_open = bar.open_ns
                        observed_rows += 1
                        yield bar
        except (
            pd.errors.EmptyDataError,
            pd.errors.ParserError,
            UnicodeDecodeError,
            ValueError,
        ) as exc:
            if isinstance(exc, NativeSourceChainError):
                raise
            raise SourceIntegrityError("bars CSV cannot be parsed deterministically") from exc
        if observed_rows != segment.descriptor.rows:
            raise SourceIntegrityError("bars CSV row count does not match its pin")

    @staticmethod
    def _technical_overlay_status(segment: SourceSegment) -> Optional[str]:
        if segment.status == "failed" or segment.bars is None:
            return segment.failure_status or segment.status
        if segment.failure_status in {
            "terminal_prefix_unresolved",
            "technical_history_gap",
            "fetch_failed",
            "write_failed",
            "data_quality_failed",
            "unexpected_acquisition_failure",
            "symbol_resolution_failed",
        }:
            return segment.failure_status
        return None

    @staticmethod
    def _frame(rows: Sequence[_CanonicalBar]) -> pd.DataFrame:
        data = {
            "time": pd.to_datetime([row.open_ns for row in rows], unit="ns", utc=True),
            "Open": [float(row.open) for row in rows],
            "High": [float(row.high) for row in rows],
            "Low": [float(row.low) for row in rows],
            "Close": [float(row.close) for row in rows],
            "Volume": [row.volume for row in rows],
            "Spread": [row.spread for row in rows],
            "RealVolume": [row.real_volume for row in rows],
            "_SOURCE_FIRST_TS": pd.to_datetime(
                [row.source_first_ns for row in rows], unit="ns", utc=True
            ),
            "_SOURCE_LAST_TS": pd.to_datetime(
                [row.source_last_ns for row in rows], unit="ns", utc=True
            ),
            "_BAR_CLOSE_TS": pd.to_datetime(
                [row.close_ns for row in rows], unit="ns", utc=True
            ),
        }
        frame = pd.DataFrame(data)
        frame.set_index("time", inplace=True)
        frame.index.name = "time"
        return frame

    def resolve(self, start_utc: Any, end_exclusive_utc: Any) -> ResolvedNativeCell:
        start_ns = _timestamp_ns(start_utc, "requested start")
        end_ns = _timestamp_ns(end_exclusive_utc, "requested end")
        if start_ns >= end_ns:
            raise SourceIntegrityError("requested interval must be non-empty and half-open")
        self._maximum_chunk_rows_observed = 0
        self._verified_bytes = 0
        origin_descriptor = self.descriptor.segments[0]
        origin = self._verify_segment(origin_descriptor, origin_descriptor)
        overlay: Optional[_VerifiedSegment] = None
        if len(self.descriptor.segments) == 2:
            overlay_descriptor = self.descriptor.segments[1]
            overlay = self._verify_segment(overlay_descriptor, origin_descriptor)
            technical = self._technical_overlay_status(overlay_descriptor)
            if technical is not None:
                raise SourceUnavailable(
                    "current overlay is technically unavailable; the source chain was not relabelled economically",
                    technical_status=technical,
                )
        if origin.bars_path is None:
            raise SourceUnavailable("preserved origin bars are unavailable")

        # Only the bounded overlap audit set and the caller's requested rows are
        # retained.  Rows outside those two purposes are streamed and discarded.
        overlay_by_open: dict[int, _CanonicalBar] = {}
        overlay_requested: list[_CanonicalBar] = []
        overlay_first: Optional[_CanonicalBar] = None
        overlay_last: Optional[_CanonicalBar] = None
        if overlay is not None:
            if origin.actual_last_open_ns is None:
                raise SourceIntegrityError("origin omits its actual last open timestamp")
            for bar in self._iter_bars(overlay):
                if overlay_first is None:
                    overlay_first = bar
                overlay_last = bar
                if bar.open_ns <= origin.actual_last_open_ns:
                    overlay_by_open[bar.open_ns] = bar
                if start_ns <= bar.open_ns < end_ns:
                    overlay_requested.append(bar)
                    if len(overlay_requested) > self.descriptor.limits.max_output_rows:
                        raise BaselineRefreshRequired("resolved output row cap exceeded")
            if overlay_first is None or overlay_last is None:
                raise SourceUnavailable(
                    "current overlay contains no bars", technical_status="empty_overlay"
                )

        origin_owned: list[_CanonicalBar] = []
        overlap_seen: set[int] = set()
        origin_first: Optional[_CanonicalBar] = None
        origin_last: Optional[_CanonicalBar] = None
        for bar in self._iter_bars(origin):
            if origin_first is None:
                origin_first = bar
            origin_last = bar
            if overlay_first is not None and bar.open_ns >= overlay_first.open_ns:
                replacement = overlay_by_open.get(bar.open_ns)
                if replacement is None:
                    if overlay_last is not None and bar.open_ns <= overlay_last.open_ns:
                        raise SourceChainGap(
                            "overlay omits a timestamp present in the origin overlap"
                        )
                else:
                    overlap_seen.add(bar.open_ns)
                    if bar.comparison_tuple() != replacement.comparison_tuple():
                        raise SourceRevisionConflict(
                            "origin and overlay disagree on an execution-relevant overlap value"
                        )
            if (
                (overlay_first is None or bar.open_ns < overlay_first.open_ns)
                and start_ns <= bar.open_ns < end_ns
            ):
                origin_owned.append(bar)
                if (
                    len(origin_owned) + len(overlay_requested)
                    > self.descriptor.limits.max_output_rows
                ):
                    raise BaselineRefreshRequired("resolved output row cap exceeded")
        if origin_first is None or origin_last is None:
            raise SourceUnavailable("preserved origin contains no bars")
        if (
            origin.actual_first_open_ns != origin_first.open_ns
            or origin.actual_last_open_ns != origin_last.open_ns
            or origin.actual_last_close_ns != origin_last.close_ns
        ):
            raise SourceIntegrityError(
                "origin provenance actual first/last coverage does not match the bars artifact"
            )

        overlay_owned: list[_CanonicalBar] = []
        if overlay_first is not None and overlay_last is not None and overlay is not None:
            baseline = _mapping(overlay.provenance.get("baseline"), "overlay baseline")
            if _timestamp_ns(
                baseline.get("actual_last_open_utc"), "baseline actual last open"
            ) != origin_last.open_ns or _timestamp_ns(
                baseline.get("actual_last_close_utc"), "baseline actual last close"
            ) != origin_last.close_ns:
                raise SourceIntegrityError(
                    "overlay seam is not bound to the origin's actual last closed bar"
                )
            if overlay_first.open_ns > origin_last.open_ns:
                raise SourceChainGap("origin and current overlay have no closed-bar overlap")
            if overlay_last.open_ns < origin_last.open_ns:
                raise SourceChainGap("current overlay does not reach the origin tail")
            if overlay.request_start_ns != overlay_first.open_ns:
                raise SourceIntegrityError(
                    "overlay request start is not its first admitted overlap bar"
                )
            overlay_overlap_keys = set(overlay_by_open)
            if not overlap_seen or overlap_seen != overlay_overlap_keys:
                raise SourceChainGap(
                    "origin and overlay do not have an exact timestamp set across their overlap"
                )
            if (
                overlay.actual_first_open_ns != overlay_first.open_ns
                or overlay.actual_last_open_ns != overlay_last.open_ns
                or overlay.actual_last_close_ns != overlay_last.close_ns
            ):
                raise SourceIntegrityError(
                    "overlay provenance actual first/last coverage does not match the bars artifact"
                )
            overlay_owned = overlay_requested

        output = origin_owned + overlay_owned
        if not output:
            raise SourceUnavailable(
                "requested interval contains no source bars",
                technical_status="requested_interval_empty",
            )
        if len(output) > self.descriptor.limits.max_output_rows:
            raise BaselineRefreshRequired("resolved output row cap exceeded")
        if any(left.open_ns >= right.open_ns for left, right in zip(output, output[1:])):
            raise SourceChainGap("resolved source chain contains a duplicate or reversed seam")
        frame = self._frame(output)
        segment_pins = tuple(
            {
                "role": segment.role,
                "run_id": segment.run_id,
                "manifest_sha256": segment.manifest.sha256,
                "provenance_sha256": segment.provenance.sha256,
                "bars_sha256": None if segment.bars is None else segment.bars.sha256,
                "rows": segment.rows,
                "bytes": 0 if segment.bars is None else segment.bars.bytes,
            }
            for segment in self.descriptor.segments
        )
        receipt = NativeSourceChainReceipt(
            descriptor_sha256=self.descriptor.sha256,
            identity=self.descriptor.identity,
            request_start_utc=_ns_text(start_ns),
            request_end_exclusive_utc=_ns_text(end_ns),
            rows=len(output),
            origin_rows_owned=len(origin_owned),
            overlay_rows_owned=len(overlay_owned),
            overlap_rows_compared=len(overlap_seen),
            verified_bytes=self._verified_bytes,
            maximum_chunk_rows_observed=self._maximum_chunk_rows_observed,
            first_open_utc=_ns_text(output[0].open_ns),
            last_open_utc=_ns_text(output[-1].open_ns),
            last_close_utc=_ns_text(output[-1].close_ns),
            overlay_first_owned_open_utc=(
                None if overlay_first is None else _ns_text(overlay_first.open_ns)
            ),
            overlay_first_owned_close_utc=(
                None if overlay_first is None else _ns_text(overlay_first.close_ns)
            ),
            origin_status=origin_descriptor.status,
            origin_failure_status=origin_descriptor.failure_status,
            overlay_status=(None if overlay is None else overlay.descriptor.status),
            overlay_failure_status=(
                None if overlay is None else overlay.descriptor.failure_status
            ),
            limitations=(LEGACY_PREFIX_LIMITATION,),
            segment_pins=segment_pins,
        )
        return ResolvedNativeCell(frame=frame, receipt=receipt)


__all__ = [
    "ArtifactPin",
    "BaselineRefreshRequired",
    "DESCRIPTOR_SCHEMA",
    "EXECUTION_COLUMNS",
    "LEGACY_PREFIX_LIMITATION",
    "NativeCellIdentity",
    "NativeSourceChainDescriptor",
    "NativeSourceChainError",
    "NativeSourceChainReceipt",
    "NativeSourceChainResolver",
    "ORIGIN_ROLE",
    "OVERLAY_ROLE",
    "ResolvedNativeCell",
    "SourceChainGap",
    "SourceChainLimits",
    "SourceIntegrityError",
    "SourceRevisionConflict",
    "SourceSegment",
    "SourceUnavailable",
]
